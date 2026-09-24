"""Cross-shard coordination for Groq's shared free-tier quota (2026-09 ROUND 7).

THE PROBLEM THIS CLOSES: Groq-O and Groq-C's rate limits (30 RPM / 8K TPM /
1K RPD PER ACCOUNT, per console.groq.com/docs/rate-limits) are real,
account-wide quotas — but classifier.py's per-provider throttle
(_last_call_times / min_call_interval) is purely in-process memory, with
zero visibility across the ~20 separate GitHub Actions shard runners that
can all be classifying concurrently. AI_RATE_SHARDS (config.py) papered
over this with a static guess — divide the interval by an assumed shard
count — which is either too conservative (throttles hard even when few
shards actually need Groq at that moment) or not conservative enough (more
real concurrent callers than the guessed number -> live 429s).

THE FIX (explicit user design, 2026-09): a tiny shared Supabase table
turns "guess how many shards might be calling right now" into "only ONE
shard is ever actually allowed to call Groq at a time." At any moment, at
most one shard holds the "Groq slot" — while holding it, that shard fires
requests at BOTH Groq-O and Groq-C together (they're independent accounts
with independent budgets, so one shard using both concurrently is a
deliberate feature, not a bug — the account owner confirmed this is
intentional: "one shard has both accounts at that time"). Every other
shard's Groq-eligible work for that round is skipped by _ai_call() and
falls through classifier.py's EXISTING cross-provider failover (still
routes to OpenAI/NVIDIA or a later cascade round) rather than blocking.

Per-account math backing the pacing this unlocks (config.py's
_GROQ_BASE_INTERVAL): worst case every batch is a full 6,000 chars (~1,500
tokens at ~4 chars/token). 8,000 TPM / 1,500 tokens/call = 5.33 calls/min
safely fit under the cap — floored to 5/min (12s/call) leaves real margin
(7,500 of 8,000 TPM used) rather than cutting it exactly to the limit.
Since only one shard is ever active on Groq at a time now, that interval
no longer needs AI_RATE_SHARDS' cross-shard division — the LOCK is what
keeps concurrent callers off Groq, not a shared-out interval guess.

Two Supabase objects this module assumes already exist (see
Main/sql/groq_coordination.sql — run that once in the Supabase SQL editor
before this ships to production; nothing here creates them):
  - table `groq_lock` (single row, id=1): who currently holds the slot.
  - table `groq_daily_usage` + RPC `bump_groq_daily_usage`: an atomic,
    cross-shard running count of today's Groq requests per account, so the
    1,000/day cap (RPD) is enforced globally instead of each shard
    discovering it independently through trial-and-error 429s.
  - RPCs `try_claim_groq_lock` / `renew_groq_lock` / `release_groq_lock`:
    atomic (single-UPDATE, Postgres-row-locked) claim/renew/release —
    "atomic" matters here specifically to avoid a check-then-write race
    where two shards both see the slot as free in the same instant.

FAILS OPEN throughout: if the lock table/RPCs aren't reachable yet (not
migrated, a transient network blip), every function here logs ONCE and
returns a "proceed anyway" value rather than deadlocking the whole
classification run or crashing it — falling back to whatever
AI_RATE_SHARDS pacing is already configured as the safety net under that
degraded (rare, Supabase-outage-only) condition.
"""
import logging
import os
import random
import threading
import time
import uuid
from datetime import date

from supabase_handler import _rpc, _rpc_void, SupabaseFetchError

log = logging.getLogger(__name__)

# A holder that hasn't renewed within this many seconds is presumed dead
# (crashed shard, killed runner) and the lock is reclaimable by anyone —
# this is what stops one dead shard from freezing Groq for everyone else
# for the rest of the run.
_STALE_SECONDS = 180

# How long a shard will queue for the lock before giving up THIS ROUND and
# falling back to classifier.py's existing cross-provider failover instead
# — never blocks forever.
_MAX_WAIT_SECONDS = 45

# Jittered poll interval while waiting — deliberately randomized (not a
# fixed per-shard offset) so many shards waiting on the same lock don't
# fall into a synchronized "keep landing on the same instant" pattern.
_POLL_MIN, _POLL_MAX = 2.0, 5.0

_warned_unreachable = False


def _make_shard_id() -> str:
    """A unique-enough identifier for THIS process, stable for its whole
    lifetime. Doesn't need to be the literal --shard N CLI arg — it only
    has to be unique per process so the lock can tell "still me" apart
    from "a different shard," which GITHUB_RUN_ID + GITHUB_JOB + pid +
    a short random suffix already guarantees without any new CLI wiring."""
    run = os.environ.get("GITHUB_RUN_ID", "local")
    job = os.environ.get("GITHUB_JOB", "job")
    return f"{run}-{job}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


SHARD_ID = _make_shard_id()

# Reference-counted so multiple concurrent Groq batches from the SAME
# shard (e.g. groq-o and groq-c both have work this round) share ONE
# remote claim instead of fighting each other for it or releasing early
# while a sibling batch is still mid-flight.
_refcount = 0
_refcount_lock = threading.Lock()


def _warn_unreachable_once(e) -> None:
    global _warned_unreachable
    if not _warned_unreachable:
        log.warning(f"Groq lock table unreachable ({e}) — proceeding WITHOUT cross-shard "
                    f"Groq coordination this run (see Main/sql/groq_coordination.sql — "
                    f"has it been run in Supabase yet?). Falling back to whatever "
                    f"AI_RATE_SHARDS pacing is configured as the safety net.")
        _warned_unreachable = True


def try_acquire(shard_id: str = None) -> bool:
    """Single non-blocking attempt. True = this shard now holds the slot
    (or already did — idempotent). False = someone else holds it and it
    isn't stale yet. Fails OPEN (returns True) if the lock table itself is
    unreachable, so a Supabase hiccup degrades to "uncoordinated" instead
    of "stuck.\""""
    shard_id = shard_id or SHARD_ID
    try:
        return bool(_rpc("try_claim_groq_lock",
                          {"p_shard": shard_id, "p_stale_seconds": _STALE_SECONDS}))
    except SupabaseFetchError as e:
        _warn_unreachable_once(e)
        return True


def renew(shard_id: str = None) -> None:
    """Best-effort heartbeat while holding the slot — a missed renewal
    just risks another shard reclaiming it a little early via the
    staleness check, not a correctness bug."""
    try:
        _rpc_void("renew_groq_lock", {"p_shard": shard_id or SHARD_ID})
    except Exception:
        pass


def release(shard_id: str = None) -> None:
    """Best-effort release — if this fails, the staleness timeout
    self-heals it within _STALE_SECONDS anyway."""
    try:
        _rpc_void("release_groq_lock", {"p_shard": shard_id or SHARD_ID})
    except Exception:
        pass


def acquire_blocking(shard_id: str = None, max_wait: float = _MAX_WAIT_SECONDS) -> bool:
    """Poll for the slot with jittered backoff, giving up (returning
    False) after max_wait seconds rather than blocking forever."""
    shard_id = shard_id or SHARD_ID
    if try_acquire(shard_id):
        return True
    deadline = time.monotonic() + max_wait
    while time.monotonic() < deadline:
        time.sleep(random.uniform(_POLL_MIN, _POLL_MAX))
        if try_acquire(shard_id):
            return True
    return False


def enter_critical_section() -> bool:
    """Enter this process's Groq critical section — acquires the
    cross-shard slot on the FIRST concurrent caller and just increments a
    refcount for any sibling calls already inside it (e.g. groq-o and
    groq-c batches running at once in the same shard). Returns False if
    the slot couldn't be claimed within the wait budget — caller should
    treat that exactly like any other "this provider isn't available right
    now" failure and let the existing failover pick up the work."""
    global _refcount
    with _refcount_lock:
        if _refcount > 0:
            _refcount += 1
            return True
        acquired = acquire_blocking()
        if acquired:
            _refcount = 1
        return acquired


def exit_critical_section() -> None:
    global _refcount
    with _refcount_lock:
        _refcount = max(0, _refcount - 1)
        release_now = _refcount == 0
    if release_now:
        release()


def bump_daily(account: str, n: int = 1) -> int | None:
    """Atomically add n to today's cross-shard request count for `account`
    (groq-o/groq-c). Returns the new running total, or None if the call
    failed — callers should treat None as "unknown," NOT as "under limit,"
    and rely on the existing per-process _exhausted_providers_today circuit
    breaker as the fallback in that case."""
    try:
        return _rpc("bump_groq_daily_usage",
                     {"p_account": account, "p_today": date.today().isoformat(), "p_n": n})
    except SupabaseFetchError as e:
        log.debug(f"Groq daily-usage counter unreachable for {account}: {e}")
        return None
