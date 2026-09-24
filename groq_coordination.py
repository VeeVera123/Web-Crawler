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

FAILS CLOSED for the lock, fast: if the lock table/RPCs aren't reachable
within a few seconds (not migrated, a transient network blip, Supabase
under load), try_acquire()/acquire_blocking() treat that as "can't confirm
exclusivity" and refuse the lock rather than granting it — logging once,
not crashing. This is a 2026-09 HOTFIX correction: an earlier version
failed OPEN (granted the lock anyway) on the theory that Groq coordination
failing shouldn't block the whole run — but Groq is optional here, the
existing cross-provider failover already reroutes to OpenAI/NVIDIA the
moment a provider can't be used, so there's no deadlock risk in refusing.
Failing open was actually what let multiple shards believe they'd each
claimed exclusive access during a real Supabase slow patch — confirmed
live. bump_daily() still returns None (not a false "under the cap" signal)
on failure, which is the correct non-amplifying direction for a counter.
Every coordination call also uses a short timeout + minimal retry (NOT
supabase_handler's durability-tuned 4-attempt/30s-per-attempt retry),
specifically so a slow patch is detected in a few seconds rather than up
to two minutes — both to react faster and to avoid piling extra retry
load onto an already-struggling Supabase project.
"""
import logging
import os
import random
import threading
import time
import uuid
from datetime import date

import requests as http_requests

from supabase_handler import REST, HEADERS

log = logging.getLogger(__name__)

# 2026-09 ROUND 7 HOTFIX: real production evidence — a wall of concurrent
# 429s across shards even AFTER the lock shipped, with the account owner
# directly confirming multiple shards were firing at once. Root cause:
# try_acquire()/bump_daily() were built on supabase_handler._rpc(), which
# is deliberately generous (4 attempts, each with its own 30s timeout, plus
# growing backoff between them — correct for a durability-critical write
# like a job upsert, where you WANT to keep trying rather than lose data).
# That's exactly the WRONG trade-off for a coordination check: worst case,
# a single try_acquire() call could take well over a minute to finally
# give up — and this module's fail-open design then treats that timeout as
# "Supabase must be down, proceed anyway." Under real load, several shards
# checking at once can all hit that same slow patch, all wait through it,
# and all fail open within moments of each other — which looks exactly
# like "nobody was waiting," because functionally nobody was: the lock
# never got a chance to say no before every caller gave up on it.
#
# Fix: a dedicated, DELIBERATELY IMPATIENT caller for coordination RPCs
# only — a short per-attempt timeout and just one retry. A coordination
# check should get a fast yes/no, or fail fast so the real polling loop in
# acquire_blocking() gets to do its job (several genuine re-checks spread
# over its 45s budget) instead of burning that whole budget inside a
# single over-patient attempt. This also cuts how much EXTRA load this
# module adds to Supabase during a slow patch — 20 shards each retrying 4
# times is far worse for an already-struggling project than 20 shards
# retrying once and backing off.
_FAST_TIMEOUT_SECONDS = 4
_FAST_MAX_ATTEMPTS = 2
_FAST_RETRY_DELAY = 0.5


def _fast_rpc(fn: str, params: dict):
    """POST to a scalar-returning RPC with a short timeout and minimal
    retry — see the ROUND 7 HOTFIX note above for why this deliberately
    does NOT reuse supabase_handler._rpc."""
    last_err = None
    for attempt in range(_FAST_MAX_ATTEMPTS):
        try:
            r = http_requests.post(f"{REST}/rpc/{fn}", headers=HEADERS, json=params,
                                    timeout=_FAST_TIMEOUT_SECONDS)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < _FAST_MAX_ATTEMPTS - 1:
                time.sleep(_FAST_RETRY_DELAY)
    raise RuntimeError(f"groq coordination RPC {fn} failed fast: {last_err}")


def _fast_rpc_void(fn: str, params: dict) -> None:
    """Same as _fast_rpc but for a void-returning function (204, empty
    body) — best-effort, swallows its own failure (renew/release are
    already best-effort by design elsewhere in this module)."""
    try:
        http_requests.post(f"{REST}/rpc/{fn}", headers=HEADERS, json=params,
                            timeout=_FAST_TIMEOUT_SECONDS)
    except Exception:
        pass

# 2026-09 ROUND 8 (explicit user design): shortened from 180s. The holder
# renews on EVERY Groq call it makes (see enter_critical_section below),
# and at Groq's own worst-case pacing (12s/call, 5/min at a full 6,000-char
# batch) a genuinely active holder logs a fresh renewal at least once a
# minute even in the slowest realistic case — so "no renewal in over a
# minute" is a real, meaningful signal that the holder is gone (crashed,
# killed runner), not just between calls. A dead holder now only blocks
# everyone else for about a minute instead of three.
_STALE_SECONDS = 60

# 2026-09 ROUND 8: how long a shard will queue for the lock before giving
# up THIS ROUND and falling back to classifier.py's existing
# cross-provider failover instead — never blocks forever. Long enough to
# reliably span one full staleness cycle (a shard that starts waiting just
# after a stale takeover check still gets another shot once the NEXT
# staleness window closes) without waiting indefinitely.
_MAX_WAIT_SECONDS = 70

# 2026-09 ROUND 8 (explicit user design: "maybe every 20 seconds, it comes
# and asks: is this still in use"): a waiting shard doesn't need to poll
# tightly — the lock only ever frees up either when the holder finishes
# (unpredictable) or the staleness window elapses (a known ~60s cadence),
# so checking every ~20s catches both without hammering Supabase with
# pointless polls from every waiting shard. Jittered (not a fixed offset)
# so many shards waiting on the same lock don't all check in the same
# instant.
_POLL_MIN, _POLL_MAX = 15.0, 25.0

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
        log.warning(f"Groq lock table unreachable/slow ({e}) — treating Groq as "
                    f"UNAVAILABLE for this call (fails CLOSED, not open) so it reroutes "
                    f"through the existing cross-provider failover to OpenAI/NVIDIA "
                    f"instead of risking multiple shards firing at Groq uncoordinated. "
                    f"See Main/sql/groq_coordination.sql — has it been run in Supabase "
                    f"yet? Further occurrences this run are logged at debug only.")
        _warned_unreachable = True
    else:
        log.debug(f"Groq lock table unreachable/slow again ({e}) — failing closed as above.")


def try_acquire(shard_id: str = None) -> bool:
    """Single non-blocking attempt. True = this shard now holds the slot
    (or already did — idempotent). False = someone else holds it and it
    isn't stale yet, OR the lock table itself couldn't be reached in time.

    2026-09 ROUND 7 HOTFIX: this used to fail OPEN (return True) when the
    lock table was unreachable, reasoning that Groq coordination failing
    shouldn't freeze the whole classification run. That reasoning was
    backwards for THIS specific lock: Groq is optional — the existing
    cross-provider failover already reroutes to OpenAI/NVIDIA the instant
    a provider can't be used, so there is no deadlock risk in refusing the
    lock. Failing open, in contrast, is exactly what let multiple shards
    believe they'd each claimed exclusive access during a real Supabase
    slow patch (confirmed live: several 429s within the same second across
    shards right after a run of 'Read timed out' RPC failures) — the one
    scenario this whole module exists to prevent. Now fails CLOSED: an
    unreachable/slow lock table means "can't confirm exclusivity, so don't
    proceed" — Groq just sits out that call, other providers absorb it.
    """
    shard_id = shard_id or SHARD_ID
    try:
        return bool(_fast_rpc("try_claim_groq_lock",
                               {"p_shard": shard_id, "p_stale_seconds": _STALE_SECONDS}))
    except Exception as e:
        _warn_unreachable_once(e)
        return False


def renew(shard_id: str = None) -> None:
    """Best-effort heartbeat while holding the slot — a missed renewal
    just risks another shard reclaiming it a little early via the
    staleness check, not a correctness bug."""
    _fast_rpc_void("renew_groq_lock", {"p_shard": shard_id or SHARD_ID})


def release(shard_id: str = None) -> None:
    """Best-effort release — if this fails, the staleness timeout
    self-heals it within _STALE_SECONDS anyway."""
    _fast_rpc_void("release_groq_lock", {"p_shard": shard_id or SHARD_ID})


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
    now" failure and let the existing failover pick up the work.

    2026-09 ROUND 7 HOTFIX: also renews the lock on every entry, including
    the "already held, just incrementing" path. This was a real gap —
    nothing was renewing claimed_at before, so a shard actively using the
    lock across many sequential calls (the normal case, one batch after
    another) would still eventually look "stale" to try_claim_groq_lock's
    staleness check purely from time passing, even while genuinely still
    in use, letting another shard steal it mid-use. Renewing here means
    "still actively using it" is refreshed every time this shard actually
    makes a Groq call — which happens far more often than the staleness
    window, as long as there's ongoing Groq work."""
    global _refcount
    with _refcount_lock:
        if _refcount > 0:
            _refcount += 1
            renew()
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
    breaker as the fallback in that case. Uses the same fast/low-retry
    caller as the lock (see ROUND 7 HOTFIX note above) so a slow Supabase
    patch doesn't also turn this into a multi-attempt, multi-second stall
    on every single Groq call."""
    try:
        return _fast_rpc("bump_groq_daily_usage",
                          {"p_account": account, "p_today": date.today().isoformat(), "p_n": n})
    except Exception as e:
        log.debug(f"Groq daily-usage counter unreachable for {account}: {e}")
        return None


def mark_exhausted_shared(account: str, reason: str) -> None:
    """2026-09 ROUND 8 (explicit user design: 'that groq account is tagged
    unusable and no hit is made to it again' — by anyone, not just the
    shard that discovered it): tell every other shard this account is done
    for today (hit the 1,000/day cap, or a hard non-retryable API error —
    a revoked key, an account suspension) via the shared daily-usage row.
    Mirrors classifier.py's LOCAL _exhausted_providers_today, just made
    visible cross-shard. Best-effort: a failed write here isn't dangerous
    — the existing per-process circuit breaker still protects the shard
    that made this call, and every other shard just falls back to
    rediscovering the same failure on its own, exactly like before this
    existed."""
    _fast_rpc_void("mark_groq_exhausted",
                    {"p_account": account, "p_today": date.today().isoformat(), "p_reason": reason})


def is_exhausted_shared(account: str) -> bool:
    """Best-effort cross-shard check — returns False (not exhausted, go
    ahead and try) on ANY failure to reach Supabase. That's the safe
    direction for THIS check specifically: a false negative here costs at
    most one wasted attempt that the normal per-call error handling
    already absorbs, unlike the lock's fail-closed check where a false
    positive risks a rate-limit storm."""
    try:
        return bool(_fast_rpc("is_groq_exhausted",
                               {"p_account": account, "p_today": date.today().isoformat()}))
    except Exception:
        return False
