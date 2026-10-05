"""
Two-stage classifier — multi-provider architecture.

Role classification:     Groq-O + Groq-C (free tiers, concurrent)
Location classification: Groq-O + Groq-C + NVIDIA NIM + OpenAI GPT-4.1 nano (concurrent)
  (2026-09 ROUND 4: Gemini and Mistral both removed entirely, replaced by
  a second independent Groq account — explicit user instruction, after
  Gemini's daily-quota problems and Mistral's 403-then-429 saga. See
  config.py, the source of truth, for exact models/keys/reasoning.)

(See config.py's module docstring for the full, current provider roster
and the reasoning behind each swap — this list drifts as providers get
added/moved, so config.py is the source of truth if this ever looks stale.)

Falls back to single-provider mode if only LLM_PROVIDER is set.

  Stage 1 — keyword filter for CSM/AM role titles (fast, no API)
  Stage 2 — AI for ambiguous titles (batched, multi-provider concurrent)

Then a separate location filter:
  Stage 3 — keyword check for Africa/Global locations
  Stage 4 — AI for ambiguous locations (multi-provider concurrent)

2026-09: cross-provider failover. Each stage's work is still split
round-robin across its providers up front (see ai_classify_roles()/
ai_classify_locations()), but now if one provider's batch fails outright
(exhausts its own MAX_RETRIES=3 attempts, or its client can't be built),
that batch's items are reassigned across the OTHER providers for that
stage and retried once, instead of immediately defaulting to
False/'uncertain'. With three providers per stage, "one fails" genuinely
means "the other two pick it up" — this is what NVIDIA was re-added
alongside (see config.py) rather than as a lone third option that just
adds more capacity.
"""

import re
import time
import heapq
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import ROLE_PROVIDERS, LOCATION_PROVIDERS, LOCATION_PROVIDER, LLM_PROVIDER, _PROVIDER_FILTER
import geo
import groq_coordination

# 2026-09 ROUND 7: which provider names are "Groq" for cross-shard lock
# purposes — both independent accounts share the coordination this module
# provides (see groq_coordination.py's module docstring for the full
# design: only one shard is ever allowed to fire at Groq at a time).
_GROQ_NAMES = {"groq-o", "groq-c"}

# 2026-09 ROUND 7: once a Groq account's cross-shard daily count (tracked
# in Supabase, see groq_coordination.bump_daily) reaches this, every shard
# reroutes to OpenAI/NVIDIA for the rest of the day — the account owner's
# explicit rule ("once its gotten to 1k, they all stop"), enforced GLOBALLY
# instead of each shard discovering the real 1,000 RPD cap independently
# through trial-and-error 429s.
_GROQ_DAILY_CAP = 1_000

log = logging.getLogger(__name__)

# ── Provider-specific AI client setup ─────────────────────
# 3 attempts per provider (not more) before a batch is considered that
# provider's failure and handed to the other providers for the same
# stage — see the module docstring's "cross-provider failover" note and
# _run_batches_with_failover() below.
MAX_RETRIES = 3
RETRY_BASE_DELAY = 5  # seconds

# 2026-10 (explicit user request: "if an LLM did not see it, have it wait
# and then try one more time after which you should discard it should it
# fail"): how long ai_classify_locations' last-chance retry waits before
# forcing one final attempt at every job no provider ever actually
# reviewed. Deliberately short — this is a bounded, single extra wait per
# run (only triggered when no_ai_read is non-empty), not a substitute for
# a daily-quota reset, which this wait cannot fix regardless of length.
_LAST_CHANCE_RETRY_WAIT_SECONDS = 45


def _make_client(provider: dict):
    """Create an OpenAI-compatible client for a provider config dict.

    2026-09 fix: max_retries=0 — the openai-python SDK retries failed
    requests itself by default (max_retries=2, i.e. up to 3 real HTTP
    attempts per .create() call, with its own short backoff — that's what
    the "Retrying request in 0.49s" log lines actually are, not anything
    _ai_call() below logs). That sat UNDERNEATH _ai_call()'s own
    MAX_RETRIES=3 outer loop with no coordination between the two, so a
    single logical "attempt" there could silently fire up to 3 real
    requests — confirmed live: a Gemini 429 burst that should have been 3
    attempts (per the module docstring) actually sent 5+ requests in the
    same second before _ai_call() even logged its own "attempt 3" error.
    That's 3x (or more) the intended request volume against a
    rate/quota-limited provider, and 3x the wall-clock spent retrying
    before this stage's own cross-provider failover ever gets a chance to
    kick in. max_retries=0 makes _ai_call()'s loop the ONLY retry layer,
    matching what its own docstring already claims."""
    from openai import OpenAI
    return OpenAI(api_key=provider["api_key"], base_url=provider["base_url"], max_retries=0)


# Pre-create clients for all configured providers
_role_clients = {}
for _p in ROLE_PROVIDERS:
    try:
        _role_clients[_p["name"]] = _make_client(_p)
    except Exception as e:
        log.warning(f"Failed to create client for {_p['name']}: {e}")

_location_clients = {}
for _p in LOCATION_PROVIDERS:
    try:
        _location_clients[_p["name"]] = _make_client(_p)
    except Exception as e:
        log.warning(f"Failed to create location client ({_p['name']}): {e}")

# Per-provider rate limiting.
_last_call_times = {p["name"]: 0.0 for p in ROLE_PROVIDERS}
for _p in LOCATION_PROVIDERS:
    _last_call_times[_p["name"]] = 0.0

# 2026-09 ROUND 8 FIX (real production evidence: two "groq-o rate limit
# hit" WARNINGs logged at the EXACT same timestamp, for two DIFFERENT
# batches, even after the cross-shard lock started working correctly).
# _last_call_times was only "thread-safe via dict" in the sense that dict
# reads/writes themselves don't corrupt memory — the actual
# check-elapsed-then-sleep-then-record sequence below in _ai_call was NOT
# atomic. Two threads processing two batches for the SAME provider
# concurrently (normal — a shard's own ThreadPoolExecutor can easily have
# 2+ groq-o batches in flight at once) could both read the same stale
# _last_call_times[name], both compute "elapsed >= interval, go ahead",
# and both fire within the same fraction of a second — exactly what a
# per-account pacing interval exists to prevent, and exactly what the
# paired-429 evidence shows happening. A per-provider lock around that
# whole check-sleep-record sequence makes it atomic: the second thread
# now genuinely waits for the first to finish updating the timestamp
# before it even computes its own "elapsed," instead of racing it.
_pacing_locks = {name: threading.Lock() for name in _last_call_times}
_pacing_locks_lock = threading.Lock()  # guards creating a lock for a name not seen at import time


def _pacing_lock_for(name: str) -> threading.Lock:
    lock = _pacing_locks.get(name)
    if lock is not None:
        return lock
    with _pacing_locks_lock:
        lock = _pacing_locks.get(name)
        if lock is None:
            lock = threading.Lock()
            _pacing_locks[name] = lock
        return lock

# 2026-09: once a provider hits its DAILY quota (not an ordinary transient
# rate limit — see is_daily_limit below), retrying it again later in the
# SAME run is guaranteed to fail immediately: a daily quota only resets
# once a day, never mid-run. Previously every subsequent batch still got
# sent to that provider anyway, each wasting a real HTTP round trip and
# re-logging the identical "daily quota reached" error line — confirmed
# live (the same "gemini daily quota reached" message repeating for the
# rest of a run). Tracked at MODULE level, shared by role AND location
# classification (a provider name like "nvidia" is the same underlying
# account/quota for both stages), reset only by a fresh process start —
# which matches how this project actually runs (one process per CI job).
_exhausted_providers_today: set[str] = set()
_exhausted_providers_lock = threading.Lock()

# 2026-09 ROUND 6: same log-spam fix as _mark_exhausted's own dedup, but
# for the ordinary (recoverable) per-minute rate-limit case just below,
# which deliberately does NOT call _mark_exhausted (a per-minute quota
# refills, so the provider isn't permanently blacklisted) — but was still
# logging a fresh WARNING line on every single call that hit it, which is
# exactly what produced the wall of repeated "rate limit hit"/"exhausted
# retries" lines in a real run's log when a tight-quota provider like Groq
# got hit with many small batches at once. First occurrence per provider
# per run still logs at WARNING; every repeat logs at DEBUG only.
_rate_limit_warned_today: set[str] = set()
_rate_limit_warned_lock = threading.Lock()


def _mark_exhausted(name: str, reason: str) -> None:
    """2026-09 (explicit user request): 'should any provider fail, it's
    immediately logged, no more traffic goes to it, and its remaining
    work is rerouted to the other free providers.' Originally this only
    fired for a confirmed DAILY quota error; now it fires for ANY reason
    _ai_call gives up on a provider — exhausted retries, a hard non-
    retryable API error, all of it. Logs once per provider per run (not
    once per failed batch — a struggling provider can fail many batches
    in a row, and this project's own logs got noisy from that before),
    then every later call short-circuits instantly with no network hit.

    2026-09 ROUND 8 (explicit user design: "that groq account is tagged
    unusable and no hit is made to it again" — meaning by every shard, not
    just this one): for a Groq account, also writes this exhaustion to the
    shared cross-shard table (see groq_coordination.mark_exhausted_shared)
    so every OTHER shard stops wasting attempts on the same dead account
    instead of each independently rediscovering it. Best-effort — if that
    write fails, this shard is still protected by the local set below, and
    every other shard just falls back to discovering it on its own,
    exactly like before this cross-shard propagation existed."""
    with _exhausted_providers_lock:
        already_known = name in _exhausted_providers_today
        _exhausted_providers_today.add(name)
    if not already_known:
        log.error(f"{name} failed ({reason}) — no more traffic to {name} for the rest of this run, "
                  f"rerouting its remaining work to other providers")
        if name in _GROQ_NAMES:
            groq_coordination.mark_exhausted_shared(name, reason)


def _provider_is_exhausted(name: str, *, sync_shared: bool = True) -> bool:
    """Return whether a provider must receive NO new traffic in this run.

    This is the single circuit-breaker query used by both scheduling and the
    final request guard.  Local exhaustion is checked first because it is
    authoritative for this process and costs no network round trip.  Groq
    exhaustion is then checked in the shared Supabase breaker so a different
    shard's decision becomes visible BEFORE work is assigned to this provider.
    """
    with _exhausted_providers_lock:
        if name in _exhausted_providers_today:
            return True

    if sync_shared and name in _GROQ_NAMES and groq_coordination.is_exhausted_shared(name):
        _mark_exhausted(name, "cross-shard: another shard marked this account exhausted today")
        return True
    return False


def _available_providers(providers: list[dict]) -> list[dict]:
    """Return only providers that are currently allowed to receive new work.

    IMPORTANT: this runs immediately before every scheduling/assignment pass.
    `_ai_call()` still performs its own final check because a provider can die
    after scheduling but before the worker actually reaches the network.
    """
    return [p for p in providers if not _provider_is_exhausted(p["name"])]


def _ai_call(provider: dict, client, system_prompt: str, user_msg: str, max_tokens: int = 500,
              force: bool = False) -> str | None:
    """Call an OpenAI-compatible provider with retry on rate limit.
    Returns response text or None on failure.

    force=True (2026-10, explicit user request — see ai_classify_locations'
    last-chance retry) skips both exhaustion guards below so a provider
    already marked dead by _mark_exhausted still gets one real network
    attempt. Only ever passed by that one deliberate, bounded last-chance
    retry — every other call site leaves this False, so _mark_exhausted's
    normal "no more traffic for the rest of this run" behavior is
    completely unaffected everywhere else.

    2026-09 ROUND 7: for Groq (both accounts), this call only actually
    proceeds while this process holds the cross-shard Groq lock (see
    groq_coordination.py). This replaces AI_RATE_SHARDS' static guess with
    real coordination: at most one shard fires at Groq at a time, so
    config.py's Groq min_call_interval is now a single-shard-safe value on
    its own, not something that needs dividing by an assumed shard count
    anymore.

    2026-09 ROUND 10 (explicit user design: "it does nothing else till its
    turn when it starts sending requests again"): a Groq call no longer
    gives up after a short wait and falls back to cross-provider failover
    just because another shard currently holds the slot — it blocks until
    it's this shard's turn (see groq_coordination.enter_critical_section
    and _MAX_WAIT_SECONDS' comment for the now-last-resort safety valve).
    A single "Waiting for groq." line is logged once per call that
    actually has to wait, not on every poll — this replaces the old
    "AI ... classification failed (groq-o)... rerouting to another
    provider" WARNING that used to repeat every ~75-85s in production
    logs purely from this lock-timeout case, which was never a real API
    failure and shouldn't have been logged or treated like one.
    """
    name = provider["name"]
    is_groq = name in _GROQ_NAMES

    # FIRST GUARD: never start work for a provider already known dead.
    if not force and _provider_is_exhausted(name):
        return None

    if is_groq:
        def _log_waiting_for_groq():
            log.info(f"{name}: Waiting for groq.")
        if not groq_coordination.enter_critical_section(on_wait=_log_waiting_for_groq):
            log.warning(f"{name}: Groq coordination unavailable for an extended period — "
                        f"rerouting this call to another provider (see groq_coordination.py's "
                        f"_MAX_WAIT_SECONDS — this is not ordinary lock contention, something is "
                        f"actually wrong with the shared lock table/RPCs)")
            return None

    try:
        # SECOND GUARD: this closes the critical race that the old code had:
        # worker A checked "not exhausted", then waited for the Groq lock;
        # worker B discovered the quota exhaustion and marked it shared; A
        # subsequently acquired the lock and fired anyway.  The shared check
        # MUST happen again after the lock is held and immediately before any
        # pacing/counter/network work.
        if not force and _provider_is_exhausted(name):
            return None

        interval = provider.get("min_call_interval", 0.0)

        for attempt in range(MAX_RETRIES):
            # 2026-09 ROUND 9 FIX (real production evidence: a real crawl's
            # log showed groq-o firing "attempt 1" retries only 5-7 SECONDS
            # apart, repeatedly, even though min_call_interval=12s and the
            # cross-shard lock confirmed only one shard was ever calling
            # Groq at a time). Root cause: this pacing gate used to run
            # ONCE, before the retry loop started — so it correctly spaced
            # out the FIRST attempt of each _ai_call() invocation, but once
            # inside the loop, a 429 on attempt 1 slept only
            # RETRY_BASE_DELAY*(attempt+1) (5s, then 10s — see below) and
            # retried WITHOUT ever re-checking the provider's own minimum
            # spacing. Two compounding effects: (1) that retry itself fired
            # sooner than the account's real per-minute quota allows,
            # guaranteeing another 429; and (2) because _last_call_times[name]
            # was only ever stamped ONCE per _ai_call() call (right before
            # attempt 1), a DIFFERENT concurrent call to the same provider
            # would see that stale timestamp, correctly conclude its own
            # 12s had elapsed, and fire for real WHILE this call's retry was
            # also in flight — doubling up on requests inside the same
            # rate-limit window. Moving the pacing gate INSIDE the loop (run
            # before every real HTTP attempt, not just the first) fixes
            # both: every actual network call — fresh or retry — always
            # waits out the full interval since the most recent attempt to
            # this provider by ANY thread, and _last_call_times is kept
            # current for every attempt, not just the first.
            with _pacing_lock_for(name):
                if interval > 0:
                    elapsed = time.time() - _last_call_times.get(name, 0.0)
                    if elapsed < interval:
                        time.sleep(interval - elapsed)
                _last_call_times[name] = time.time()

            # 2026-09 ROUND 9: enter_critical_section() (and the renew it
            # does) only runs ONCE per _ai_call() invocation, at the very
            # top — a single call can now legitimately take multiple paced
            # retries in a row (up to MAX_RETRIES * interval, well over
            # 30s), and nothing was refreshing the remote lock's
            # claimed_at during that whole stretch. _STALE_SECONDS=60
            # means a genuinely still-active shard whose own retries (plus
            # real network latency on top of the pacing waits) happened to
            # run long could look abandoned to another shard's staleness
            # check and have the lock legitimately stolen out from under
            # it mid-call — the exact "even in the slightest" collision
            # this renews away. Cheap and best-effort (renew() already
            # swallows its own failures), so doing it on every attempt
            # instead of just once per call costs nothing when nothing's
            # wrong and closes this gap when something is slow.
            if is_groq:
                groq_coordination.renew()
                # 2026-09 ROUND 9 FIX: this used to bump the daily counter
                # exactly ONCE per _ai_call() invocation, before the retry
                # loop even started — but a single invocation can make up
                # to MAX_RETRIES real HTTP requests against Groq's actual
                # account (each 429/retry IS a real request that counts
                # against the real 1,000/day RPD cap, whether or not it
                # succeeded). Undercounting by up to 3x here meant the
                # cross-shard counter could sit well under 1,000 while the
                # ACCOUNT'S real usage had already reached it — letting
                # every shard keep sending real requests right at the
                # boundary instead of rerouting to OpenAI/NVIDIA before
                # hitting it, which is its own source of unexpected 429s.
                # Bumping once per actual attempt (here, right before it
                # fires) keeps the tracked count honest against reality.
                new_count = groq_coordination.bump_daily(name, 1)
                if new_count is not None and new_count >= _GROQ_DAILY_CAP:
                    _mark_exhausted(name, f"cross-shard daily count reached {new_count}/{_GROQ_DAILY_CAP}")
                    return None

            try:
                resp = client.chat.completions.create(
                    model=provider["model"],
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_msg},
                    ],
                    temperature=0,
                    max_tokens=max_tokens,
                )
                content = resp.choices[0].message.content
                if content is None:
                    if attempt < MAX_RETRIES - 1:
                        # 2026-09 ROUND 9: no separate fixed sleep here
                        # either, same reasoning as the rate-limit path
                        # above — the pacing gate at the top of the next
                        # loop iteration already enforces the provider's
                        # real min_call_interval before this retry actually
                        # fires, so an extra fixed sleep on top of that was
                        # just wasted latency stacked on the real wait, not
                        # additional safety.
                        log.warning(f"{name} returned null content (attempt {attempt + 1}/{MAX_RETRIES}) "
                                    f"— waiting for the next paced slot before retrying")
                        continue
                    _mark_exhausted(name, "returned null content after all retries")
                    return None
                return content.strip()
            except Exception as e:
                error_str = str(e)
                error_lower = error_str.lower()
                is_rate_limit = "429" in error_str or "413" in error_str or "rate" in error_lower
                # 2026-09 fix: confirmed live this missed Gemini's actual
                # free-tier daily-quota error entirely — its real wording is
                # "Quota exceeded for metric: ...generate_content_free_tier_
                # requests" with quotaId "GenerateRequestsPerDayPerProjectPer
                # Model-FreeTier", none of which contains "tokens per day" or
                # the bare word "daily" (case-insensitively) that this check
                # used to require. Because it fell through as an ordinary
                # rate limit instead, _ai_call() kept retrying with backoff
                # for a quota that a few seconds' wait can never fix within
                # the same UTC day — wasting all MAX_RETRIES attempts (and,
                # combined with the max_retries=0 fix above, needlessly
                # delaying cross-provider failover) on a call guaranteed to
                # fail again immediately. Broadened to also catch "per day"
                # and "quota exceeded" — still narrow enough not to misfire
                # on an ordinary transient rate-limit message (those say
                # "rate limit"/"too many requests", never "quota exceeded"
                # or a per-day quota window).
                is_daily_limit = (
                    "tokens per day" in error_lower
                    or "requests per day" in error_lower
                    or "per day" in error_lower
                    or "perday" in error_lower.replace(" ", "").replace("_", "")
                    or ("quota exceeded" in error_lower and "day" in error_lower)
                    or "daily" in error_lower
                )

                if is_daily_limit:
                    # 2026-10 BUG FIX (explicit user report: Groq getting
                    # marked exhausted after only ~100 requests, nowhere
                    # near the 1,000/day request cap this file tracks --
                    # but a manual reset gets it working again the SAME
                    # day, which a genuinely exhausted account couldn't
                    # do). The generic "daily quota reached" reason this
                    # used to log is useless for telling apart Groq's TWO
                    # separate daily caps (1,000 requests/day vs 200,000
                    # TOKENS/day -- the latter isn't tracked by this file's
                    # own counter at all, and LOCATION_SYSTEM_PROMPT alone
                    # is ~2,067 tokens per call, so ~100 location calls can
                    # plausibly hit 200K tokens/day well before 1,000
                    # requests) from a genuine false positive (an ordinary
                    # per-minute rate-limit message that happens to contain
                    # a substring like "daily"/"per day" and gets
                    # misclassified by the loose checks above). Logging
                    # Groq's own verbatim error text makes that
                    # diagnosable from the run's log instead of requiring
                    # a guess.
                    log.error(f"{name} hit a daily-limit response: {error_str[:500]}")
                    _mark_exhausted(name, "daily quota reached")
                    return None
                if is_rate_limit and attempt < MAX_RETRIES - 1:
                    # 2026-09 ROUND 9: no separate fixed backoff sleep here
                    # any more — the pacing gate at the top of the next loop
                    # iteration already guarantees this retry waits out the
                    # provider's own min_call_interval since the most recent
                    # attempt (by ANY thread), which is both the correct
                    # wait AND enough on its own; a redundant extra sleep
                    # here on top of that (the old behavior) was strictly
                    # wasted latency, never extra safety. See the pacing
                    # gate's own comment above for the full before/after.
                    log.warning(f"{name} rate limit hit (attempt {attempt + 1}/{MAX_RETRIES}) — "
                                f"waiting for the next paced slot before retrying")
                    continue
                # 2026-09 FIX (real production evidence, not hypothetical):
                # investigated after the pipeline owner reported Groq "failing
                # massively" despite the model being confirmed live and not
                # deprecated (checked against Groq's own docs). Root cause:
                # an ordinary per-minute rate limit (429) that survives
                # MAX_RETRIES attempts used to fall into the SAME
                # _mark_exhausted call as a genuine daily-quota exhaustion or
                # a hard non-retryable error — permanently blacklisting the
                # provider for the REST of this process's run, even though a
                # per-minute quota (Groq's real pool: 8K TPM) fully refills
                # within a minute. Groq's quota is shared across role AND
                # location classification AND — pre-ROUND-7 — all
                # AI_RATE_SHARDS concurrent crawl-shard processes (see
                # config.py's provider comments); ROUND 7 replaces that
                # static division with the real cross-shard lock above, but
                # this per-call recoverable-vs-permanent distinction still
                # matters regardless of how many shards are involved. A
                # daily-quota error (is_daily_limit above) genuinely can't
                # recover mid-run, so permanently blacklisting is correct
                # there — but a rate limit that merely outlasted this call's
                # retries is NOT the same thing, and should just fail THIS
                # call (existing cross-provider failover already handles
                # that) so the provider gets tried again on the next batch,
                # once the per-minute window has reset.
                if is_rate_limit:
                    with _rate_limit_warned_lock:
                        first_time = name not in _rate_limit_warned_today
                        _rate_limit_warned_today.add(name)
                    if first_time:
                        log.warning(f"{name} rate limit exhausted retries for this call — "
                                    f"NOT blacklisting (per-minute quota, will retry on next "
                                    f"batch; further per-minute rate-limit hits for {name} "
                                    f"this run are logged at debug level only)")
                    else:
                        log.debug(f"{name} rate limit exhausted retries for this call "
                                  f"(repeat this run, suppressed at warning level)")
                    return None
                # Every remaining path is a genuine give-up on this provider
                # for this call that ISN'T a recoverable rate limit — some
                # other non-retryable API error (bad auth, invalid request,
                # model error, etc.). This still marks the provider exhausted
                # for the rest of the run, since there's no reason to expect
                # those to self-resolve within the same process.
                _mark_exhausted(name, f"API error: {e}")
                return None
        # Every retry attempt returned null content or was itself a rate limit
        # that got retried — if we fall out of the loop entirely without an
        # exception, that means MAX_RETRIES null-content attempts (already
        # handled above, returns before reaching here) or (2026-09 fix, see
        # above) exhausted rate-limit retries with no exception on the final
        # attempt. Either way this is the same "don't permanently blacklist a
        # per-minute rate limit" fix — not a hard failure worth a circuit break.
        return None
    finally:
        # 2026-09 ROUND 7: always release this process's hold on the
        # cross-shard Groq slot (refcounted — only actually releases the
        # remote lock once every concurrent Groq call in this process has
        # finished), no matter which return path above was taken.
        if is_groq:
            groq_coordination.exit_critical_section()


# ═══════════════════════════════════════════════════════
# STAGE 1 & 2: ROLE CLASSIFICATION
# ═══════════════════════════════════════════════════════

# 2026-09: split into 4 named category lists (CS/AM/PM/OM) so every job
# can be tagged with which of the 4 this project actually recruits for —
# see classify_role_category() below and its "role_category" DB column
# (explicit user request). INCLUDE_KEYWORDS itself is unchanged (still the
# flat concatenation every existing keyword_classify_role() call site
# already expects) — this is a pure refactor of how the list is BUILT,
# not a behavior change to role keyword matching.
CS_KEYWORDS = [
    # Customer/Client Success
    r"customer\s*success", r"client\s*success", r"partner\s*success",
    r"merchant\s*success", r"\bcsm\b", r"success\s*manager",
    r"success\s*lead", r"success\s*specialist", r"success\s*director",
    r"success\s*associate", r"success\s*consultant", r"success\s*advisor",
    r"success\s*architect", r"success\s*coach", r"success\s*executive",
    r"head\s*of\s*.*success",

    # Customer/Client Support/Service (manager-level, not agents)
    r"customer\s*support\s*(manager|lead|director|head)",
    r"client\s*support\s*(manager|lead|director|head)",
    r"customer\s*service\s*(manager|lead|director|head|representative|rep\b)",
    r"client\s*service\s*(manager|lead|director|head)",

    # Customer/Client Experience
    r"customer\s*experience", r"client\s*experience",
    r"\bcx\s*(manager|lead|specialist|director|strategist)",

    # Customer/Client Relationship
    r"customer\s*relationship", r"client\s*relationship",
    r"relationship\s*manager",

    # Customer/Client Engagement
    r"customer\s*engagement", r"client\s*engagement",

    # Customer/Client Care
    r"customer\s*care", r"client\s*care",

    # Customer/Client Advocate
    r"customer\s*advocate", r"client\s*advocate",

    # Retention / Renewal (customer-success-adjacent, not its own category)
    r"customer\s*retention", r"client\s*retention",
    r"retention\s*(manager|lead|specialist|director)",
    r"renewal\s*(manager|lead|specialist|director)",

    # Onboarding / Implementation (customer-facing, same reasoning)
    r"customer\s*onboarding", r"client\s*onboarding",
    r"onboarding\s*(manager|lead|specialist)",
    r"implementation\s*(manager|lead|specialist|consultant)",
]

AM_KEYWORDS = [
    # Account Management
    r"account\s*manager", r"account\s*management",
    r"client\s*account\s*manag", r"customer\s*account\s*manag",
    r"key\s*account\s*manag", r"strategic\s*account\s*manag",
    r"enterprise\s*account\s*manag", r"technical\s*account\s*manag",
    r"\btam\b", r"named\s*account\s*manag",
    r"regional\s*account\s*manag", r"national\s*account\s*manag",
    r"global\s*account\s*manag", r"account\s*lead", r"account\s*director",
    r"senior\s*account\s*manag", r"junior\s*account\s*manag",
    r"account\s*executive\s*.*(?:success|retention|renewal)",
]

PM_KEYWORDS = [
    # Project Management
    r"project\s*manag(?:er|ement)", r"project\s*lead\b",
    r"project\s*director", r"project\s*coordinator",
    r"project\s*specialist", r"project\s*consultant",
    r"\bpmo\b", r"program\s*manag(?:er|ement)", r"program\s*lead\b",
    r"program\s*director", r"program\s*coordinator",
    r"technical\s*project\s*manag", r"it\s*project\s*manag",
    r"digital\s*project\s*manag", r"senior\s*project\s*manag",
    r"junior\s*project\s*manag",
]

OM_KEYWORDS = [
    # Operations Manager / Management
    r"operations\s*manag(?:er|ement)", r"operations\s*lead\b",
    r"operations\s*director", r"operations\s*coordinator",
    r"operations\s*specialist", r"operations\s*analyst",
    r"\bops\s*manag(?:er|ement)\b", r"\bops\s*lead\b",
    r"business\s*operations\s*manag", r"business\s*operations\s*lead",
    r"regional\s*operations\s*manag", r"national\s*operations\s*manag",
    r"global\s*operations\s*manag", r"senior\s*operations\s*manag",
    r"junior\s*operations\s*manag", r"head\s*of\s*.*operations",
]

INCLUDE_KEYWORDS = CS_KEYWORDS + AM_KEYWORDS + PM_KEYWORDS + OM_KEYWORDS

# Checked in this order — a title matching more than one category's
# regexes (rare, e.g. a hybrid "Customer Success / Account Manager" title)
# gets whichever category is listed first here. CS first since it's the
# largest, most foundational bucket (support/experience/retention/
# onboarding all fold into it); AM/PM/OM follow in the same order as
# their own keyword blocks above.
_ROLE_CATEGORY_RE = [
    ("CS", [re.compile(kw, re.I) for kw in CS_KEYWORDS]),
    ("AM", [re.compile(kw, re.I) for kw in AM_KEYWORDS]),
    ("PM", [re.compile(kw, re.I) for kw in PM_KEYWORDS]),
    ("OM", [re.compile(kw, re.I) for kw in OM_KEYWORDS]),
]


def classify_role_category(title: str) -> str:
    """2026-09 (explicit user request): tag every included job as CS
    (Customer Success), AM (Account Management), PM (Project Management),
    or OM (Operations Management) — a separate DB column, not a
    replacement for the existing include/exclude role filter.

    Pure regex against the SAME keyword groups keyword_classify_role()
    already uses to decide include/exclude (see CS_KEYWORDS/AM_KEYWORDS/
    PM_KEYWORDS/OM_KEYWORDS above) — this never re-decides whether a role
    belongs in this pipeline at all, only which of the 4 buckets an
    already-included role falls into. Returns "" (unknown) for a title
    that keyword_classify_role only included via a broader AI verdict and
    doesn't match any of these narrower category regexes itself — that's
    expected for a genuinely novel title phrasing the AI caught but the
    keyword lists didn't, and is a real, honestly-reported gap rather
    than a guess."""
    if not title:
        return ""
    for category, patterns in _ROLE_CATEGORY_RE:
        if any(rx.search(title) for rx in patterns):
            return category
    return ""

EXCLUDE_KEYWORDS = [
    # Engineering / technical build roles
    r"\bengineer\b", r"\bengineering\b", r"\bdeveloper\b", r"\bdev\b",
    r"\bsoftware\b", r"\bsre\b", r"\bdevops\b", r"\bbackend\b",
    r"\bfrontend\b", r"\bfull[\s-]?stack\b", r"\bdata\s*engineer\b",
    r"\bplatform\b(?!.*success)(?!.*account)",
    r"\binfrastructure\b", r"\barchitect\b(?!.*success)(?!.*account)",

    # Sales (hunting roles, not AM)
    r"\bsdr\b", r"\bbdr\b", r"business\s*development\s*rep",
    r"demand\s*gen", r"sales\s*rep\b(?!.*account)",
    r"inside\s*sales(?!.*account)", r"outside\s*sales(?!.*account)",

    # IT Support (desktop/hardware, not customer success)
    r"(it|desktop|hardware|network|systems?)\s*support",
    r"support\s*(developer|programmer)\b(?!.*customer)(?!.*client)",

    # Marketing / Product / Design / HR / Finance / Legal
    r"\bmarketing\b", r"content\s*(manager|writer|strategist)",
    r"product\s*(manager|designer|owner|lead|director)",
    r"\bux\b|\bui\b", r"\bhr\b|human\s*resources",
    r"\bfinance\b|\baccounting\b", r"\blegal\b|\bcompliance\b",
    r"recruiter|recruiting|talent\s*acquisition",
]

INCLUDE_RE = [re.compile(kw, re.I) for kw in INCLUDE_KEYWORDS]
EXCLUDE_RE = [re.compile(kw, re.I) for kw in EXCLUDE_KEYWORDS]


def keyword_classify_role(title: str) -> str:
    """Returns 'include', 'exclude', or 'unsure'."""
    has_exclude = any(rx.search(title) for rx in EXCLUDE_RE)
    has_include = any(rx.search(title) for rx in INCLUDE_RE)

    if has_exclude and not has_include:
        return "exclude"
    if has_include and not has_exclude:
        return "include"
    if has_include and has_exclude:
        return "unsure"
    return "exclude"


ROLE_SYSTEM_PROMPT = """\
You are a job title classifier. Decide if each title is a Customer Success, \
Account Management, Project Management, or Operations Manager/Management \
role.

YES if the role is any variation of:
- Customer Success Manager/Lead/Specialist/Director/Associate/Consultant
- Account Manager (key/strategic/enterprise/technical/named/regional/global)
- Customer/Client Support Manager or Representative
- Customer/Client Service Manager or Representative
- Customer/Client Experience (CX) Manager
- Customer/Client Relationship Manager
- Customer/Client Engagement Manager
- Customer/Client Care Manager
- Retention/Renewal Manager
- Onboarding/Implementation Manager (customer-facing)
- Project Manager/Lead/Coordinator/Director/PMO (technical/IT/digital/senior/junior)
- Program Manager/Lead/Coordinator/Director
- Operations Manager/Lead/Director/Coordinator/Specialist/Analyst (business/regional/national/global operations)
- Head of Operations

NO if the role is:
- Any kind of Engineer or Developer
- Sales (SDR, BDR, Account Executive, demand gen)
- IT/Desktop/Hardware Support
- Marketing, Design, HR, Finance, Legal
- Product Manager/Owner (this is a distinct role from Project Manager — a
  "Product Manager" is NO even though a "Project Manager" is YES)

Respond ONLY with lines like:
1 YES
2 NO"""


def _classify_role_batch(batch: list[str], provider: dict, client) -> tuple[dict[str, bool], bool]:
    """Classify a single batch of titles using a specific provider.

    Returns (results, call_ok) — same call_ok contract as
    _classify_location_batch (False only when the underlying API call
    itself failed, e.g. exhausted retries or a daily quota). 2026-09 fix:
    this used to return a bare dict defaulting every title to False on a
    failed call, indistinguishable from a real AI verdict — so
    ai_classify_roles' failover logic (which only ever triggered on a
    raised exception) never saw these as failures at all, and a title
    just silently landed as 'excluded' instead of being rerouted to
    another provider."""
    numbered = "\n".join(f"{j+1}. {t}" for j, t in enumerate(batch))
    user_msg = f"Titles:\n{numbered}"
    max_tokens = max(500, len(batch) * 4)
    text = _ai_call(provider, client, ROLE_SYSTEM_PROMPT, user_msg, max_tokens=max_tokens)

    results = {}
    if text is None:
        if _provider_is_exhausted(provider["name"]):
            log.debug(f"Role batch for {provider['name']} abandoned after provider was exhausted; "
                      f"the batch is being rerouted")
        else:
            log.warning(f"AI role classification failed ({provider['name']}) for batch of {len(batch)}, "
                        f"rerouting to another provider")
        for t in batch:
            results[t] = False
        return results, False

    for line in text.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2:
            try:
                idx = int(parts[0]) - 1
            except ValueError:
                continue
            if 0 <= idx < len(batch):
                results[batch[idx]] = parts[1].upper().startswith("YES")

    for t in batch:
        if t not in results:
            results[t] = False
    return results, True


def _build_role_batches(titles: list[str], max_chars: int = 400_000) -> list[list[str]]:
    """Build role classification batches based on character limits.

    No fixed role cap — batches are purely character-budget driven.
    Each title is counted as its length + overhead for numbering/formatting.
    """
    OVERHEAD_PER_TITLE = 20   # "NNN. " + newline + buffer
    batches = []
    current_batch = []
    current_chars = 0

    for title in titles:
        title_chars = len(title) + OVERHEAD_PER_TITLE
        if current_batch and current_chars + title_chars > max_chars:
            batches.append(current_batch)
            current_batch = []
            current_chars = 0
        current_batch.append(title)
        current_chars += title_chars

    if current_batch:
        batches.append(current_batch)

    return batches


def _get_role_client(provider: dict):
    client = _role_clients.get(provider["name"])
    if not client:
        try:
            client = _make_client(provider)
            _role_clients[provider["name"]] = client
        except Exception as e:
            log.error(f"Cannot create client for {provider['name']}: {e}")
            return None
    return client


def ai_classify_roles(titles: list[str]) -> dict[str, bool]:
    """Send ambiguous titles to AI for role classification.

    Multi-provider mode: splits titles across Cerebras/Groq/NVIDIA,
    batches per provider's context window, runs all concurrently.

    Single-provider fallback: uses whichever provider is configured.

    Cross-provider failover (2026-09 ROUND 8, matches ai_classify_locations'
    cascade): if a provider's batch fails outright (after its own
    MAX_RETRIES=3 attempts inside _ai_call, or its client can't be built),
    that title keeps getting reassigned to whatever provider hasn't been
    tried for it yet — cycling through every configured provider if that's
    what it takes — until it either succeeds or every single one has
    genuinely failed it. A title is only ever defaulted to False once
    there is truly nowhere left to send it.

    Returns {title: is_relevant}. On genuine exhaustion of every provider
    for a title: defaults to False (exclude).
    """
    if not titles:
        return {}

    configured_providers = list(ROLE_PROVIDERS)
    if not configured_providers:
        # Legacy single-provider fallback
        from config import LLM_API_KEY, LLM_MODEL, LLM_BASE_URL
        configured_providers = [{
            "name": LLM_PROVIDER,
            "api_key": LLM_API_KEY,
            "model": LLM_MODEL,
            "base_url": LLM_BASE_URL,
            "max_batch_chars": 6_000 if LLM_PROVIDER == "cerebras" else 400_000,
            "min_call_interval": 12.5 if LLM_PROVIDER == "cerebras" else 0.0,
        }]

    # Circuit-breaker filtering happens BEFORE any batch is assigned.
    # A provider that another shard has already killed never enters this
    # round's work queue in the first place.
    providers = _available_providers(configured_providers)
    if not providers:
        log.warning("Role AI: all configured providers are currently unavailable; "
                    "no AI requests will be scheduled for this round")
        return {t: False for t in titles}

    # ── Split titles round-robin across live providers ──
    provider_titles = {p["name"]: [] for p in providers}
    for i, title in enumerate(titles):
        p = providers[i % len(providers)]
        provider_titles[p["name"]].append(title)

    # ── Build batches per provider (respecting each provider's context limits) ──
    all_work = []  # list of (provider, client, batch)
    for p in providers:
        p_titles = provider_titles[p["name"]]
        if not p_titles:
            continue
        client = _get_role_client(p)
        if not client:
            # Provider unusable from the start (bad key, client-creation
            # error) — treat its whole slice as failed immediately so it
            # goes through the same failover path as a mid-run failure,
            # instead of silently dropping these titles.
            all_work.append((p, None, p_titles))
            continue
        batches = _build_role_batches(p_titles, max_chars=p["max_batch_chars"])
        for batch in batches:
            all_work.append((p, client, batch))

    # Surface missing providers the same way ai_classify_locations does —
    # a title excluded here defaults straight to False (non-match) with
    # no other signal, so if a provider's API key is missing this run,
    # that's the single most useful line for explaining an unexpectedly
    # low role-match count.
    # 2026-09 ROUND 4: Gemini and Mistral removed entirely, replaced by a
    # second independent Groq account — see config.py's module docstring.
    # 2026-09 ROUND 6: openai/nvidia added as ADDITIONAL possible role
    # providers (config.py's USE_OPENAI/USE_NVIDIA provider-filter
    # feature — see that module's docstring) — previously role
    # classification only ever had groq-o/groq-c, so this set is widened
    # to match. `_PROVIDER_FILTER` (also from config.py) is subtracted out
    # of what counts as "missing" below — if the user deliberately ticked
    # "Use OpenAI" only, groq-o/groq-c being absent from `providers` is the
    # INTENDED outcome, not something to warn about as if a key were
    # missing.
    _known_role_providers = {"groq-o", "groq-c", "nvidia", "openai"}
    _active = {p["name"] for p in providers}
    _expected = (_known_role_providers & _PROVIDER_FILTER) if _PROVIDER_FILTER else _known_role_providers
    _missing = _expected - _active
    if _missing:
        log.warning(f"Role AI running with {len(_active)}/{len(_expected)} "
                    f"providers ({', '.join(sorted(_active)) or 'none'}) — missing "
                    f"{', '.join(sorted(_missing))} (no API key set). Less "
                    f"failover if one of the active providers struggles.")

    provider_summary = ", ".join(
        f"{p['name']}:{len(provider_titles[p['name']])}" for p in providers
    )
    log.info(f"Role classification: {len(titles)} titles → {len(all_work)} batches "
             f"across {len(providers)} providers ({provider_summary})")

    results = {}
    failed_batches = []  # [(failed_provider_name, batch_titles), ...]
    no_ai_read: set[str] = set()  # titles never actually reviewed by a provider

    def _run_round(work):
        # 2026-09 ROUND 6: same log-collapsing fix as ai_classify_locations'
        # _run_round — one aggregated error line per provider per round
        # instead of one log.error() per failed batch (identical spam risk
        # here: a struggling provider with many small title batches used
        # to print one line each).
        batch_errors: dict[str, list] = {}
        with ThreadPoolExecutor(max_workers=max(1, len(providers))) as pool:
            future_map = {}
            for provider, client, batch in work:
                if client is None:
                    failed_batches.append((provider["name"], batch))
                    no_ai_read.update(batch)
                    continue
                f = pool.submit(_classify_role_batch, batch, provider, client)
                future_map[f] = (provider["name"], batch)

            for future in as_completed(future_map):
                pname, batch = future_map[future]
                try:
                    batch_results, call_ok = future.result()
                    if call_ok:
                        results.update(batch_results)
                        no_ai_read.difference_update(batch)
                    else:
                        # 2026-09 fix: same bug class as ai_classify_locations
                        # — a graceful call failure (call_ok=False, no
                        # exception) never used to reach failed_batches, so
                        # it skipped the failover round entirely and just
                        # kept _classify_role_batch's default-False verdict.
                        failed_batches.append((pname, batch))
                        no_ai_read.update(batch)
                except Exception as e:
                    entry = batch_errors.setdefault(pname, [0, ""])
                    entry[0] += 1
                    entry[1] = str(e)
                    failed_batches.append((pname, batch))
                    no_ai_read.update(batch)

        for pname, (count, last_err) in batch_errors.items():
            log.error(f"Role classification: {pname} failed on {count} "
                      f"batch(es) this round (last error: {last_err}) — "
                      f"rerouting to other providers")

    _run_round(all_work)

    # ── Failover cascade (2026-09 ROUND 8, explicit user request: "No role
    # should be written to uncertain or discarded without going through an
    # AI. they all must go through one.") — mirrors ai_classify_locations'
    # ROUND 5 cascade: a title keeps getting routed to whatever untried,
    # non-exhausted provider is left until it either succeeds or every
    # configured provider has genuinely been tried and failed for THAT
    # title. Replaces the OLD "one failover round then default to False"
    # behavior, which could silently exclude a title after a single retry
    # even with 2+ untried providers still sitting available — e.g. 4
    # configured providers, the first fails, the one failover retry lands
    # on a second one that's ALSO struggling, and the title died there even
    # though a 3rd and 4th provider were never even tried.
    #
    # `tried` tracks, per title, which provider names have already been
    # attempted — the loop terminates naturally once no still-failing
    # title has any untried, viable candidate left (bounded by
    # len(providers) rounds; a hard round cap is kept anyway as defense in
    # depth, same as the location cascade).
    tried: dict[str, set] = {}
    _cascade_round = 0
    while failed_batches and len(providers) > 1 and _cascade_round < len(providers):
        _cascade_round += 1
        failing = []  # titles still needing a home this round
        for failed_pname, batch in failed_batches:
            for title in batch:
                tried.setdefault(title, set()).add(failed_pname)
                failing.append(title)
        failed_batches = []

        # Pick the next candidate provider for each still-failing title:
        # anything NOT already tried for that title AND not already known
        # dead for the rest of this run (same _exhausted_providers_today
        # circuit breaker ai_classify_locations' cascade uses — including,
        # as of ROUND 8, a Groq account another SHARD marked exhausted).
        # Round-robins across each title's own remaining candidates via a
        # shared counter so load spreads across the survivors.
        assignments: dict[str, dict] = {}
        k = 0
        live_providers = _available_providers(providers)
        for title in failing:
            candidates = [
                p for p in live_providers
                if p["name"] not in tried[title]
            ]
            if not candidates:
                # Genuinely exhausted for THIS title — every provider has
                # now either been tried and failed, or was already known
                # dead. Falls through to the final default-False loop
                # below, same as before, but only reached here once every
                # option has truly been used up.
                continue
            assignments[title] = candidates[k % len(candidates)]
            k += 1

        if not assignments:
            break  # nothing left that has anywhere new to go

        by_provider: dict[str, list] = {}
        for title in failing:
            p = assignments.get(title)
            if p is None:
                continue
            by_provider.setdefault(p["name"], []).append(title)

        retry_work = []
        for pname, p_titles in by_provider.items():
            p = next(pp for pp in providers if pp["name"] == pname)
            client = _get_role_client(p)
            if not client:
                retry_work.append((p, None, p_titles))
                continue
            for sub_batch in _build_role_batches(p_titles, max_chars=p["max_batch_chars"]):
                retry_work.append((p, client, sub_batch))

        log.info(
            f"Role classification cascade round {_cascade_round}: "
            f"retrying {sum(len(v) for v in by_provider.values())} still-"
            f"failing title(s) across {len(by_provider)} provider(s) "
            f"({', '.join(sorted(by_provider))})"
        )
        if retry_work:
            _run_round(retry_work)
        # Loop repeats: anything that failed again lands back in
        # failed_batches and gets picked up next iteration, still
        # excluding every provider already tried for that specific title.

    # Genuinely exhausted for a title only after every provider has
    # actually been tried (or the single-provider case, where there was
    # never anywhere else to send it) — default to False (exclude), same
    # fallback semantics as before this round, but only reached once every
    # configured option has truly been used up.
    for _pname, batch in failed_batches:
        for t in batch:
            results.setdefault(t, False)

    # Any title never touched by any provider at all (shouldn't happen,
    # but matches the old function's "always return every title" contract)
    for t in titles:
        if t not in results:
            results[t] = False
            no_ai_read.add(t)

    if no_ai_read:
        log.warning(f"Role AI: {len(no_ai_read)}/{len(titles)} titles never reached a "
                    f"provider and were excluded by default (see warnings above) — "
                    f"NOT a genuine non-match verdict.")

    return results


# ═══════════════════════════════════════════════════════
# STAGE 3 & 4: LOCATION FILTER (Global hiring only)
# ═══════════════════════════════════════════════════════

# ── Immediate MATCH keywords (global/worldwide hiring signals only) ──

GLOBAL_KEYWORDS = [
    # Remote + global qualifier (separator OPTIONAL — catches "Remote Global",
    # "Remote - Global", "Remote/Worldwide", "Remote (Anywhere)", etc.)
    r"\bremote\s*[\-–—/,()]?\s*global\b",
    r"\bremote\s*[\-–—/,()]?\s*worldwide\b",
    r"\bremote\s*[\-–—/,()]?\s*anywhere\b",
    r"\bremote\s*[\-–—/,()]?\s*international\b",
    r"\bremote\s*[\-–—/,()]?\s*wfa\b",
    r"\bremote\s*[\-–—/,()]?\s*everywhere\b",
    r"\bremote\s*[\-–—/,()]?\s*distributed\b",
    r"\bremote\s*[\-–—/,()]?\s*(all|any)\s*location\b",
    r"\bremote\s*[\-–—/,()]?\s*(all|any)\s*countr\w*\b",
    # Qualifier + remote (handles "Global (Remote)", "Worldwide - Remote", etc.)
    r"\bglobal\s*[\-–—/,()]?\s*remote\b",
    r"\bworldwide\s*[\-–—/,()]?\s*remote\b",
    r"\binternational\s*[\-–—/,()]?\s*remote\b",
    r"\banywhere\s*[\-–—/,()]?\s*remote\b",
    r"\bdistributed\s*[\-–—/,()]?\s*remote\b",
    # Bare "global"/"worldwide"/etc. qualifiers (not necessarily paired
    # with "remote" in the string — e.g. "100% Global", "Fully Global")
    r"\b(100%|fully|truly|genuinely)\s*global(?:ly)?\b",
    r"\b(100%|fully|truly|genuinely)\s*worldwide\b",
    r"\bglobal(?:ly)?\s*[\-–—/,()]?\s*hiring\b",
    r"\bhiring\s*global(?:ly)?\b",
    r"\bglobal\s*hire\b",
    r"\bglobal\s*hires\b",
    r"\bworld\s*[\-\s]*wide\b",
    r"\baround\s+the\s+(?:world|globe)\b",
    r"\baround\s*the\s*(world|globe)\b",
    r"\baround\s+the\s+(?:world|globe)\b",
    r"\baround\s+the\s+(?:world|globe)\b",
    r"\bworld\s*[\-\s]*wide\b",
    r"\bearth\b",
    r"\bplanet\s*earth\b",
    r"\bglobal\s*citizens?\b",
    r"\bglobal\s*workforce\b",
    r"\bglobal\s*operations?\b",
    r"\bglobal\s*presence\b",
    r"\bglobal\s*network\b",
    r"\bglobal\s*reach\b",
    r"\bglobal\s*coverage\b",
    r"\bglobal\s*scale\b",
    r"\bpan[\-\s]*global\b",
    r"\ball\s*regions?\b",
    r"\bany\s*region\b",
    r"\bmulti[\-\s]*continent\b",
    r"\bcross[\-\s]*continental\b",
    r"\bcross[\-\s]*border\b",
    r"\bmulti[\-\s]*national\b",
    r"\btransnational\b",
    r"\ball\s*over\s*the\s*world\b",
    r"\banywhere\s*(in|on)\s*(the\s*)?(world|earth|globe)\b",
    r"\baround\s*the\s*(world|globe)\b",
    # Explicit phrases
    r"\bwork\s*from\s*anywhere\b",
    r"\bwfa\b",
    r"\bhire\s*(globally|worldwide|anywhere)\b",
    r"\bhiring\s*(globally|worldwide|anywhere)\b",
    r"\bopen\s*to\s*(all|any)\s*location",
    r"\bopen\s*to\s*(all|any)\s*countr",
    r"\blocation\s*[\-–—:]?\s*anywhere\b",
    r"\blocation\s*[\-–—:]?\s*flexible\b",
    r"\blocation\s*[\-\s]*free\b",
    # 2026-09 ROUND 5 BUG FIX (explicit user-provided global hiring lingo
    # list): these two used bare \s* with NO hyphen alternative, so the
    # extremely common HYPHENATED spellings "location-agnostic" and
    # "location-independent" (a literal hyphen character, which \s* never
    # matches since it isn't whitespace) never matched at all — only the
    # spaced-out "location agnostic"/"location independent" did. Fixed the
    # same way "location[\s\-]*free" a few lines above already did it
    # correctly.
    r"\blocation[\s\-]*agnostic\b",
    r"\blocation[\s\-]*independent\b",
    r"\bgeo[\-\s]*flexible\b",
    r"\bgeo[\-\s]*agnostic\b",
    r"\bborderless\b",
    r"\bunrestricted\s*location\b",
    r"\bno\s*location\s*restrictions?\b",
    r"\b(fully\s*)?distributed\b",
    r"\bdistributed\s*team\b",
    r"\bdistributed\s*workforce\b",
    r"\b(global|international)\s*team\b",
    r"\ball\s*geograph",
    r"\bany\s*country\b",
    r"\ball\s*countries\b",
    r"\bany\s*location\b",
    r"\ball\s*locations?\b",
    r"\bno\s*location\s*(requirement|restriction|preference)s?\b",
    # 2026-09 ROUND 5 BUG FIX: these two required a trailing word boundary
    # right after the singular "restriction", which FAILS on the plural
    # "restrictions" (no word-boundary between the "n" and the "s") — the
    # user's own list uses the plural ("no geographic restrictions", "no
    # location restrictions"). Made the trailing "s" optional.
    r"\bno\s*geographic\s*restrictions?\b",
    r"\bno\s*country\s*restrictions?\b",
    # 2026-09 ROUND 5 (explicit user-provided global hiring lingo list —
    # "no geographic limitation"/"no location limitation" is the same idea
    # as "restriction" with a different noun, not previously covered at
    # all).
    r"\bno\s*(geographic|location)\s*limitations?\b",
    r"\bno\s*restrictions?\s*on\s*where\s*you\s*live\b",
    r"\bwe\s*(?:don'?t|do\s*not)\s*restrict\s*where\s*you\s*(?:work|live)\b",
    # Time-zone framed global signals — "any time zone" / "regardless of
    # time zone" is a strong proxy for "we don't restrict by geography"
    r"\btime[\-\s]*zone\s*agnostic\b",
    r"\bany\s*time\s*zone\b",
    r"\bany\s*timezone\b",
    # Explicit "we don't care where you are" phrasings
    r"\bregardless\s*of\s*(location|country|time\s*zone|timezone|geography)\b",
    r"\birrespective\s*of\s*(location|country)\b",
    # 2026-09 ROUND 5 (explicit user-provided global hiring lingo list):
    # "wherever"/"where you live" framed variants distinct from the
    # "regardless of"/"irrespective of" preposition-led phrasings above.
    r"\bregardless\s*of\s*where\s*you\s*live\b",
    r"\bwherever\s*(?:you|they)\s*(?:are|live)(?:\s*located)?\b",
    r"\bcountry[\-\s]*agnostic\b",
    r"\bwork\s*from\s*any\s*(country|location)\b",
    # "hire/candidates/applicants ... worldwide/globally/anywhere" phrasings
    # not already covered by the hire/hiring-globally patterns above
    r"\bhire\s*talent\s*(globally|worldwide|from\s*anywhere)\b",
    r"\bglobal\s*talent\b",
    r"\bglobal\s*talent\s*pool\b",
    r"\bopen\s*to\s*(candidates|applicants)\s*(worldwide|globally|from\s*anywhere|in\s*any\s*country)\b",
    # 2026-09 expansion (explicit user request: over-expand this vocabulary,
    # zero tolerance for excluding a genuine global-hiring role — false
    # positives are acceptable, false negatives are not).
    r"\bopen\s*to\s*remote\s*(candidates|applicants)\s*(anywhere|worldwide|globally)\b",
    r"\bwork\s*remotely\s*from\s*(any|anywhere)\b",
    r"\bhire\s*(across|in)\s*(over\s*)?\d+\+?\s*countries\b",
    r"\bteam\s*(members?)?\s*(across|in|spanning)\s*(over\s*)?\d+\+?\s*countries\b",
    r"\boperat\w*\s*in\s*(over\s*)?\d+\+?\s*countries\b",
    r"\bemployer\s*of\s*record\b",
    r"\b(via\s*)?(deel|remote\.com|oyster\s*hr|papaya\s*global|multiplier|velocity\s*global|rippling\s*eor|justworks)\b",
    r"\bhire\s*without\s*borders\b",
    r"\bborderless\s*hiring\b",
    r"\bglobally\s*distributed\b",
    r"\bworldwide\s*team\b",
    r"\bglobal[\-\s]*first\b",
    r"\bremote[\-\s]*first\b",
    r"\bdigital\s*nomad\b",
    r"\bacross\s*(all\s*)?time\s*zones\b",
    r"\ball\s*time\s*zones\b",
    r"\bwork\s*from\s*any\s*part\s*of\s*the\s*world\b",
    r"\bglobal\s*remote\s*team\b",
    r"\bremote[\-\s]*native\b",
    # 2026-09 ROUND 5 (explicit user-provided global hiring lingo list,
    # sourced from OpenAI research per the user's established workflow of
    # posing a research question externally and handing back the answer —
    # see this file's own history of the Mistral/OpenRouter and restrictive-
    # language research questions for the same pattern). Cross-checked
    # against the existing ~100 entries above; only the genuinely MISSING
    # phrasings are added here rather than duplicating coverage that
    # already exists (e.g. "hire anywhere in the world" already matches
    # the existing bare "hire...anywhere" entry as a substring, so it's
    # not re-added).
    r"\bwork\s*anywhere\b",
    r"\bfrom\s*anywhere\b",
    r"\banywhere\s*(globally|worldwide)\b",
    r"\blocation\s*(?:doesn'?t|does\s*not)\s*matter\b",
    r"\bglobally\s*remote\b",
    r"\bglobal\s*remote\s*workforce\b",
    r"\bglobal\s*remote\s*(?:position|role|opportunity)\b",
    r"\bdistributed\s*(?:worldwide|globally)\b",
    r"\bremote\s*by\s*design\b",
    r"\bborn\s*remote\b",
    # Hyphenated "work-from-anywhere" (a literal hyphen, not whitespace) —
    # the existing "\bwork\s*from\s*anywhere\b" entry above only matches
    # the spaced-out form; this compound-adjective form ("work-from-
    # anywhere company/culture") needs its own hyphen-aware pattern.
    r"\bwork[\s\-]*from[\s\-]*anywhere\b",
    r"\bopen\s*(?:globally|worldwide)\b",
    r"\bapplications?\s*accepted\s*worldwide\b",
    r"\b(?:applicants|candidates)\s*worldwide\s*welcome\b",
    r"\b(?:located|based)\s*anywhere\b",
    r"\bacross\s*the\s*globe\b",
    r"\btalent,?\s*not\s*(?:location|geography)\b",
    r"\bgeography\s*is\s*not\s*a\s*barrier\b",

    # 2026-09 ROUND 6 (explicit user-provided global hiring lingo list,
    # this time cross-checked by the user directly against this file's own
    # ~130-pattern vocabulary rather than a generic list — only genuinely
    # missing phrasing families are added here; see the user's own message
    # for the full family-by-family comparison this is drawn from).
    #
    # "hire/recruit/employ ... anywhere/from anywhere" — the existing
    # "\bhire\s*(globally|worldwide|anywhere)\b" (ROUND-1-era, above) only
    # matches when the qualifier IMMEDIATELY follows the verb (\s* is
    # whitespace-only) — "hire FROM anywhere" or "hire PEOPLE/TALENT
    # anywhere" both have an extra word in between and were real gaps.
    r"\b(?:hire|hiring|recruit|recruiting|employ|employment)\s*"
    r"(?:people|talent)?\s*(?:from\s*)?(?:anywhere|everywhere)\b",
    r"\bopen\s*to\s*talent\s*(?:everywhere|globally|worldwide|from\s*anywhere)\b",
    r"\b(?:candidates?|applicants?)\s*from\s*everywhere\b",
    r"\b(?:employees?|team\s*members?)\s*(?:can|may)\s*be\s*anywhere\b",
    r"\byou\s*(?:can|may)\s*(?:live|be\s*based)\s*anywhere\b",
    # "wherever" family — the existing bare "\bwherever\s*(?:you|they)\s*
    # (?:are|live)\b" doesn't match the "you're" contraction, and "work
    # from wherever" (no "you are/live" tail at all) wasn't covered either.
    r"\bwherever\s*you'?re\s*(?:located|based)\b",
    r"\bwork\s*from\s*wherever\b",
    # "no matter where" / "regardless where" / "irrespective where" (the
    # existing regardless-of/irrespective-of patterns above require "of" —
    # real postings drop it) family.
    r"\bno\s*matter\s*where\s*you\s*(?:live|are)\b",
    r"\bno\s*matter\s*where\s*you'?re\s*(?:located|based)\b",
    r"\b(?:regardless|irrespective)\s*where\s*you\s*(?:live|are)\b",
    # "X is irrelevant" / "X doesn't matter" family, broadened from the
    # existing location-only "doesn't matter" pattern to geography/country,
    # plus the "irrelevant" synonym the existing vocabulary had no coverage
    # for at all.
    r"\b(?:location|geography|country)\s*(?:doesn'?t|does\s*not)\s*matter\b",
    r"\b(?:your\s*)?(?:location|geography|country)\s*is\s*irrelevant\b",
    r"\bwhere\s*you\s*(?:live|are\s*based|are\s*located)\s*is\s*irrelevant\b",
    # geography/location/country -neutral/-independent/-free (existing
    # vocabulary only had location-agnostic/location-independent/geo-
    # agnostic/geo-flexible/location-free — not the "-neutral" spelling, not
    # "geography"/"country" as the noun, and not "geographically
    # independent").
    r"\b(?:geography|location|country)[\s\-]*neutral\b",
    r"\b(?:geograph(?:y|ically)|country)[\s\-]*independent\b",
    r"\b(?:geography|country)[\s\-]*free\b",
    # "no barrier(s)" / "will not be a barrier" — existing vocabulary only
    # had the single fixed phrase "geography is not a barrier".
    r"\bno\s*(?:geographic|geographical|location|country)\s*barriers?\b",
    r"\b(?:geography|location)\s*is\s*no\s*barrier\b",
    r"\b(?:location|geography)\s*will\s*not\s*be\s*a\s*barrier\b",
    # "no/without geographic limits/boundaries" — existing vocabulary only
    # had the plural "limitations", not "limits", and no "boundaries" form.
    r"\bno\s*(?:geographic|geographical|location|country)\s*limits?\b",
    r"\bno\s*(?:geographic|geographical|location|country)\s*boundaries\b",
    r"\bwithout\s*(?:geographic|geographical|location)\s*"
    r"(?:limits?|boundaries|borders?|restrictions?)\b",
    # "across/throughout the (entire) world/globe", "every/all countries" —
    # existing vocabulary only had "around the world/globe" and "across the
    # globe", not the bare "across the world" or any "throughout" form.
    r"\bacross\s*the\s*(?:entire\s*)?world\b",
    r"\bthroughout\s*the\s*(?:entire\s*)?(?:world|globe)\b",
    r"\bacross\s*every\s*country\b",
    r"\bacross\s*all\s*countries\b",
    r"\bin\s*every\s*country\b",
    r"\bfrom\s*every\s*country\b",
    # "internationally distributed" (existing only had "globally
    # distributed") and "distributed across countries/continents".
    r"\binternationally\s*distributed\b",
    r"\bdistributed\s*across\s*(?:countries|continents)\b",
    # "global talent base/network/community", "international/worldwide
    # talent pool" — existing only had "global talent"/"global talent
    # pool". Also excluded from the free-text safety net below, same
    # treatment as their existing siblings (company-description marketing
    # language, not an explicit per-role hiring-scope statement).
    r"\bglobal\s*talent\s*(?:base|network|community)\b",
    r"\b(?:international|worldwide)\s*talent\s*pool\b",
    r"\btalent\s*pool\s*without\s*borders\b",
    # "anywhere on earth/the planet", "any corner of the world", "any
    # location in the world/globally" — existing only had "any part of the
    # world".
    r"\banywhere\s*on\s*(?:earth|the\s*planet)\b",
    r"\bany\s*corner\s*of\s*the\s*world\b",
    r"\bany\s*location\s*(?:in\s*the\s*world|globally)\b",
    # "remote globally" (adverb form — the existing remote+qualifier
    # patterns above require "global" the adjective, immediately after
    # "remote", and \b fails right before "-ly"), "remote from
    # anywhere/any country/around the world" (an intervening "from" the
    # existing remote+qualifier patterns' bare \s* can't match), "remote in
    # every country".
    r"\bremote\s*[\-–—/,()]?\s*globally\b",
    r"\b(?:fully\s*|100%\s*)?remote\s*from\s*"
    r"(?:anywhere|any\s*country|around\s*the\s*world)\b",
    r"\bremote\s*in\s*every\s*country\b",
    # "hiring/recruiting/employment WITHOUT geographic restrictions" — the
    # existing "no geographic/country restrictions" patterns above require
    # "no", not "without".
    r"\b(?:hiring|recruiting|employment)\s*without\s*"
    r"(?:geographic|geographical|location|country)\s*restrictions?\b",
    # "geographically/location/country unrestricted" (reversed word order
    # from the existing "unrestricted location") and "geographically/
    # location open", "open internationally", "open to the world".
    r"\b(?:geographically|location|country)\s*unrestricted\b",
    r"\b(?:geographically|location)\s*open\b",
    r"\bopen\s*internationally\b",
    r"\bopen\s*to\s*the\s*world\b",
    # "eligibility"/"eligible" family — not covered at all before.
    r"\b(?:global|worldwide|international)\s*eligibility\b",
    r"\bglobally\s*eligible\b",
    r"\beligible\s*(?:worldwide|globally|anywhere)\b",
    r"\beligible\s*in\s*any\s*country\b",
    # "no boundaries"/"without borders" family, broader than the existing
    # "hire without borders"/"borderless hiring" fixed phrases.
    r"\bwithout\s*borders\b",
    r"\bno\s*borders\b",
    r"\bbeyond\s*borders\b",
    r"\bacross\s*(?:national\s*)?borders\b",
    r"\bcross[\-\s]*border\s*hiring\b",
    r"\bborder[\-\s]*free\s*hiring\b",
    r"\bborderless\s*employment\b",
]

GLOBAL_RE = [re.compile(kw, re.I) for kw in GLOBAL_KEYWORDS]


# Additional high-precision global hiring language. These are deliberately
# phrased around the ROLE/CANDIDATE, rather than generic company-global words.
# They are used by the final evidence gate as well as the keyword layer.
_EXTRA_GLOBAL_HIRING_PATTERNS = [
    r"\b(?:this|the)\s+(?:role|position|job|opportunity)\s+(?:can|may|could)\s+be\s+based\s+anywhere\b",
    r"\b(?:this|the)\s+(?:role|position|job|opportunity)\s+is\s+(?:fully\s+)?remote\s+(?:worldwide|globally)\b",
    r"\b(?:this|the)\s+(?:role|position|job|opportunity)\s+is\s+open\s+(?:worldwide|globally|anywhere)\b",
    r"\b(?:this|the)\s+(?:role|position|job|opportunity)\s+(?:can|may)\s+be\s+performed\s+from\s+anywhere\b",
    r"\b(?:candidates?|applicants?|employees?|team\s+members?)\s+(?:can|may)\s+be\s+(?:located|based)\s+anywhere\b",
    r"\b(?:candidates?|applicants?)\s+(?:from|located\s+in|based\s+in)\s+(?:anywhere|any\s+country|all\s+countries|the\s+world)\b",
    r"\b(?:open|available)\s+to\s+(?:candidates?|applicants?)\s+(?:from\s+)?(?:anywhere|around\s+the\s+world|worldwide|globally)\b",
    r"\b(?:we|company|organization|organisation)\s+(?:can|may|will)\s+(?:hire|employ|recruit)\s+(?:people|talent|candidates?|employees?)\s+(?:from\s+)?(?:anywhere|any\s+country|worldwide|globally)\b",
    r"\b(?:we|company|organization|organisation)\s+(?:hire|hiring|recruit|recruiting)\s+(?:from\s+)?(?:anywhere|all\s+over\s+the\s+world|around\s+the\s+world|worldwide|globally)\b",
    r"\b(?:remote|work)\s+(?:from\s+)?(?:anywhere|any\s+country|all\s+countries|around\s+the\s+world|the\s+world)\b",
    r"\b(?:work|working)\s+from\s+(?:any\s+country|anywhere\s+in\s+the\s+world|anywhere\s+worldwide)\b",
    r"\b(?:no|without)\s+(?:geographic|geographical|location|country)\s+(?:restriction|restrictions|limitation|limitations)\b",
    r"\b(?:no|without)\s+restrictions?\s+(?:on|as\s+to)\s+where\s+(?:you|candidates?|employees?)\s+(?:live|reside|are\s+based|work)\b",
    r"\b(?:location|geography|country)\s+(?:does\s+not|doesn't)\s+matter\s+(?:for|to)\s+(?:this\s+role|the\s+role|us|hiring)\b",
    r"\b(?:regardless|irrespective)\s+of\s+where\s+(?:you|the\s+candidate|candidates?|employees?)\s+(?:live|reside|are\s+based|work)\b",
    r"\b(?:regardless|irrespective)\s+of\s+(?:your|their|the)\s+(?:location|country|geography)\b",
    r"\b(?:location|geography|country)[\s\-]*(?:agnostic|independent|neutral)\b",
    r"\b(?:geographically|location)[\s\-]*(?:agnostic|independent)\b",
    r"\b(?:globally|worldwide)\s+(?:remote|distributed)\s+(?:role|position|job|opportunity)\b",
    r"\b(?:applications?|applicants?|candidates?)\s+(?:are\s+)?(?:accepted|welcome|welcomed)\s+worldwide\b",
    r"\b(?:applications?|applicants?|candidates?)\s+(?:are\s+)?(?:accepted|welcome|welcomed)\s+from\s+anywhere\b",
    r"\b(?:hire|hiring|recruit|recruiting)\s+(?:in|from|across)\s+(?:all\s+countries|every\s+country|any\s+country)\b",
    r"\b(?:eligible|available|open)\s+(?:to|for)\s+(?:people|candidates?|applicants?)\s+(?:in|from)\s+(?:any\s+country|all\s+countries)\b",
    r"\b(?:100%|fully|completely)\s+remote\s+(?:anywhere|worldwide|globally)\b",
    r"\b(?:remote|distributed)\s+(?:role|position|opportunity)\s+(?:open|available)\s+(?:worldwide|globally|anywhere)\b",
]
_EXTRA_GLOBAL_HIRING_RE = [re.compile(p, re.I) for p in _EXTRA_GLOBAL_HIRING_PATTERNS]

# 2026-09 fix: a subset of GLOBAL_KEYWORDS, for _text_has_global_evidence's
# post-AI safety net ONLY (see that function's docstring) — real case that
# exposed this: an Ashby posting (jobs.ashbyhq.com/vesta/1f031efa-...,
# "Senior Manager, Customer Success") whose description described the
# COMPANY as "distributed"/a "global team" while its actual hiring was
# scoped to specific hub cities (New York, San Francisco) — confirmed via
# the live posting. That still let the AI's match_global verdict survive
# the safety net, because entries like bare "distributed", "global team",
# "global workforce", "global presence", "global network", "global
# talent", or "earth" are marketing language describing what a COMPANY
# *is*, not a statement of where a ROLE can be based — and the safety net
# deliberately has no residue-stripping (unlike the strict FIELD-level
# check above, rule 4, which requires the phrase be the ONLY thing left
# in a short location string — genuinely low false-positive risk there).
# Applied to a whole free-form job description instead, those same loose,
# single-phrase entries are exactly the ones a "distributed team, but
# hiring is hub-city-only" posting will trip. Excluded here: any entry
# that only describes the company/team's nature rather than an explicit
# hiring-scope policy ("hire globally," "open to candidates worldwide,"
# "work from anywhere," "no location restriction," "time zone agnostic,"
# etc. all stay — those remain unambiguous hiring-policy statements).
_SAFETY_NET_EXCLUDED_GLOBAL_KEYWORDS = {
    r"\bworld\s*[\-\s]*wide\b",
    r"\baround\s+the\s+(?:world|globe)\b",
    r"\baround\s*the\s*(world|globe)\b",
    r"\bearth\b",
    r"\bplanet\s*earth\b",
    r"\bglobal\s*citizens?\b",
    r"\bglobal\s*workforce\b",
    r"\bglobal\s*operations?\b",
    r"\bglobal\s*presence\b",
    r"\bglobal\s*network\b",
    r"\bglobal\s*reach\b",
    r"\bglobal\s*coverage\b",
    r"\bglobal\s*scale\b",
    r"\b(fully\s*)?distributed\b",
    r"\bdistributed\s*team\b",
    r"\bdistributed\s*workforce\b",
    r"\b(global|international)\s*team\b",
    r"\bglobal\s*talent\b",
    r"\bglobal\s*talent\s*pool\b",
    # 2026-09 additions: same reasoning — these describe what the COMPANY
    # is/does, not an explicit statement that THIS role's hiring is open
    # globally (the exact Ashby false-positive pattern documented above),
    # so they stay out of the stricter free-text safety net while still
    # counting for the FIELD-level residue check and the base keyword list.
    r"\bteam\s*(members?)?\s*(across|in|spanning)\s*(over\s*)?\d+\+?\s*countries\b",
    r"\boperat\w*\s*in\s*(over\s*)?\d+\+?\s*countries\b",
    r"\bglobally\s*distributed\b",
    r"\bworldwide\s*team\b",
    r"\bglobal[\-\s]*first\b",
    r"\bremote[\-\s]*first\b",
    r"\bdigital\s*nomad\b",
    r"\bglobal\s*remote\s*team\b",
    r"\bremote[\-\s]*native\b",
    # 2026-09 ROUND 5 additions (explicit user-provided global hiring
    # lingo list): same reasoning as the block above — these describe what
    # the COMPANY/workforce is or does in general ("we have a global remote
    # workforce", "we're remote by design", "we were born remote", "our
    # people are across the globe") rather than an explicit statement that
    # THIS role's hiring is open globally. They stay in the base keyword
    # list (field-level residue check is strict enough to keep them safe
    # there) and count for the guard functions, just not for the looser
    # free-text safety net.
    r"\bglobal\s*remote\s*workforce\b",
    r"\bdistributed\s*(?:worldwide|globally)\b",
    r"\bremote\s*by\s*design\b",
    r"\bborn\s*remote\b",
    r"\bacross\s*the\s*globe\b",
    # 2026-09 ROUND 6 additions: same reasoning — company/talent-pool
    # marketing language, not an explicit per-role hiring-scope statement.
    r"\bglobal\s*talent\s*(?:base|network|community)\b",
    r"\b(?:international|worldwide)\s*talent\s*pool\b",
}
_SAFETY_NET_GLOBAL_RE = [re.compile(kw, re.I) for kw in GLOBAL_KEYWORDS
                         if kw not in _SAFETY_NET_EXCLUDED_GLOBAL_KEYWORDS]

STANDALONE_GLOBAL_RE = re.compile(
    r"^\s*(global|worldwide|world\s*wide|anywhere|international|wfa|earth|planet\s*earth|"
    r"distributed|borderless|everywhere|"
    r"remote\s*[\-–—/,()]?\s*(global|worldwide|anywhere|international|wfa|distributed|everywhere))\s*$", re.I
)

# ── Non-geographic words in location fields ──────────
NON_GEO_WORDS_RE = re.compile(
    r"\b("
    r"remote|fully|completely|"                             # remote modifiers
    r"full[\-\s]*time|part[\-\s]*time|"                     # employment types
    r"contract(?:or|ual)?|permanent|temporary|temp|"
    r"freelance|intern(?:ship)?|hourly|salaried|"
    r"direct[\-\s]*hire|regular|casual|seasonal|"
    r"fte|pte|"                                             # abbreviations
    r"worker|job|position|role|opening|opportunity|"        # job words
    r"n/?a|not\s*specified|unspecified|tbd|"                # placeholders
    r"flexible|open|based|home|general|"                    # generic qualifiers
    r"monday|tuesday|wednesday|thursday|friday|"            # schedule words
    r"saturday|sunday|weekday|weekend|"
    r"shift|schedule|day|night|evening|morning|"
    r"hours|hrs|am|pm|to|and|or|the|a|an|at|for|of|"       # connectors/articles
    r"immediate|urgent|asap|new|multiple|"                  # posting qualifiers
    r"available|hiring|now|apply"                            # action words
    r")\b",
    re.I,
)

# Words to strip ONLY in global-keyword residue check
# Connector/filler vocabulary used across GLOBAL_KEYWORDS phrase templates
# ("open to candidates worldwide", "work from any country", "any time
# zone", ...). The residue check strips out the exact substring that
# matched a keyword pattern, but when TWO patterns overlap the same text
# (e.g. the narrow "\bany\s*country\b" firing inside the longer "open to
# applicants in any country"), only one match wins and the other pattern's
# leftover connector words ("open", "to", "applicants", "work", "from")
# would otherwise sit there as fake "residue" and wrongly downgrade a
# genuine global match to no_match. This list is deliberately just
# connector/filler words from OUR OWN phrase templates — never real
# country/city names — so the "is there an actual place name left over"
# protection those phrases exist for stays intact.
GLOBAL_FILLER_RE = re.compile(
    r"\b("
    r"location|locations|agnostic|independent|geo|flexible|team|"
    r"multiple|countries|country|regions|region|restrictions?|requirement|preference|"
    r"geographic|geography|any|all|no|talent|pool|candidates|applicants|open|hire|hiring|"
    r"globally|time|zone|timezone|regardless|irrespective|of|welcome|eligible|"
    r"in|work|from|limitations?|barrier|employment|not|matter|does|doesn'?t"
    r")\b",
    re.I,
)

# 2026-09 ROUND 5 (explicit user-provided EMEA-wide hiring lingo list):
# connector/filler vocabulary specific to this project's "EMEA-wide"
# phrasing family ("EMEA-wide", "across EMEA", "throughout EMEA", "all
# EMEA countries", "any EMEA country", "across the EMEA region") — used
# ONLY by the location-FIELD EMEA residue check (step 3 above) so a field
# value like "EMEA - All Countries" or "EMEA Wide" doesn't leave "all
# countries"/"wide" sitting as false residue and get wrongly rejected as a
# qualified (non-bare) EMEA value.
_EMEA_FILLER_RE = re.compile(
    r"\b(wide|region|regions|across|throughout|all|any|countries|country|markets?)\b",
    re.I,
)

# Placeholder values that mean "no location given"
PLACEHOLDER_LOC_RE = re.compile(
    r"^\s*(not\s*specified|n/?a|tbd|to\s*be\s*determined|"
    r"unspecified|see\s*description|see\s*below|"
    r"multiple\s*locations?|various\s*locations?|"
    r"[—\-–\.]+)\s*$",
    re.I,
)

# A location field that says nothing but "Remote" (no place attached) —
# shared by _enrich_location_from_title and _keyword_classify_location_
# detail's step-5 gate below, both of which need the identical "is this
# genuinely bare" test.
_BARE_REMOTE_LOC_VALUES = ("remote", "remote worker", "remote job", "fully remote")


def _is_bare_location(loc: str) -> bool:
    """True when `loc` (already .strip()ped by the caller, or not — this
    strips again defensively) carries no real place information at all:
    blank, a recognized placeholder, or bare "Remote" with nothing else
    attached."""
    stripped = (loc or "").strip()
    stripped_lower = stripped.lower()
    return (
        not stripped_lower
        or stripped_lower in _BARE_REMOTE_LOC_VALUES
        or bool(PLACEHOLDER_LOC_RE.match(stripped))
    )


# ── Title-based location enrichment ───────────────────
# Country/region codes AND global-hiring words, recognized only when
# structurally delimited in a title (trailing "- US", leading "EMEA:",
# parenthesised "(APAC)", "- Global", etc.) — never as a bare word floating
# anywhere in the title. That distinction matters: "Global"/"International"
# frequently describe SENIORITY OR SCOPE OF ACCOUNTS, not hiring
# eligibility ("Global Head of Customer Success", "International Account
# Manager" both routinely mean "manages global/international accounts from
# one specific office", not "we'll hire you from anywhere"). Requiring a
# delimiter (dash/pipe/colon/parens) at the START or END of the title is
# what distinguishes an actual "Title - Region" suffix/prefix convention
# from an ordinary descriptive word inside the title text.
#
# Region acronyms (EMEA/APAC/LATAM/ANZ/NAM/MENA) are NOT all "global"
# signals — APAC/LATAM/ANZ/NAM/MENA are single-region RESTRICTIONS, same as
# "US" or "UK". Everything this regex extracts is handed to the same
# strict-allowlist pipeline that already knows EMEA/Africa/Global are
# acceptable and everything else isn't — no separate "is this global"
# judgment is made here.
_TITLE_CODES = (
    r"US|USA|UK|EU|EMEA|APAC|LATAM|ANZ|NAM|AMER|MENA|CA|AU|IN|DE|FR|NL|SG|HK|JP|BR|MX|PH|NG|KE|ZA|AE|SA|IL|"
    r"PL|CZ|RO|BG|HU|IE|ES|IT|PT|SE|NO|DK|FI|CH|AT|BE|NZ"
)
# Global/Africa-hiring words allowed in the same delimiter-anchored
# positions as the codes above (e.g. "CSM - Global", "Account Manager -
# Worldwide", "Distributed - Support Engineer").
_TITLE_GLOBAL_WORDS = r"Global|Worldwide|International|Africa|Distributed|Anywhere|Borderless"
_TITLE_CODES_OR_GLOBAL = _TITLE_CODES + r"|" + _TITLE_GLOBAL_WORDS

# 2026-09 ROUND 3 (explicit user correction: "if it has multiple locations
# and africa thats good. Like say: MENA, AMER, Africa, EMEA, Latam. thats
# acceptable too."): a title naming 2+ DISTINCT region acronyms/global-words
# together is evidence of broad multi-region reach, not a single-region
# restriction — same principle as _has_multi_region_breadth below, just
# scoped to this title-extraction subsystem's own acronym set (which
# includes country codes _has_multi_region_breadth deliberately excludes,
# so this is kept as its own regex rather than reusing that one).
_TITLE_MULTI_REGION_WORDS_RE = re.compile(
    r"\b(?:EMEA|APAC|LATAM|ANZ|NAM|AMER|MENA|" + _TITLE_GLOBAL_WORDS + r")\b", re.I,
)

_TITLE_LOCATION_RE = re.compile(
    r"(?:"
    # code immediately followed by a remote/based/only qualifier, anywhere
    r"\b(" + _TITLE_CODES + r")\s*[\-–—/]?\s*(?:remote|based|only)\b"
    r"|"
    r"(?:remote)\s*[\-–—/,()]*\s*"
    r"(US|USA|UK|EU|EMEA|APAC|LATAM|India|United\s+States|United\s+Kingdom|Canada|Australia|Germany|France|Netherlands)"
    r"|"
    # parenthesised code/global-word anywhere in the title, e.g. "CSM (US)",
    # "AM (EMEA) - Enterprise", "Support Engineer (Global)"
    r"\(\s*(" + _TITLE_CODES_OR_GLOBAL + r"|India|Canada|Australia|Nigeria|Kenya|South\s+Africa)\s*\)"
    r"|"
    # bare code/global-word at the very END of the title after a delimiter —
    # the "CSM - US" / "CSM - Global" pattern that plain remote/based/only-
    # suffix matching above misses entirely, since there's no qualifier
    # word at all, just the code or global-hiring word itself
    r"[\-–—|:,]\s*(" + _TITLE_CODES_OR_GLOBAL + r")\s*(?:Only|Based|Remote)?\s*$"
    r"|"
    # bare code/global-word at the very START of the title before a
    # delimiter, e.g. "US - Customer Success Manager", "EMEA: Account
    # Manager", "Global: Customer Success Manager"
    r"^\s*(" + _TITLE_CODES_OR_GLOBAL + r")\s*[\-–—|:]"
    r"|"
    r"\b(New\s+York|San\s+Francisco|Los\s+Angeles|Chicago|Boston|Seattle|Austin|Denver|Atlanta|Dallas|Miami|"
    r"London|Berlin|Paris|Amsterdam|Toronto|Sydney|Singapore|Dubai|Mumbai|Bangalore|"
    r"California|Texas|Florida|Virginia|Pennsylvania|Illinois|Ohio|Georgia|"
    r"North\s+Carolina|New\s+Jersey|Massachusetts|Maryland|Colorado|Washington|Oregon|Arizona|Michigan|Minnesota)"
    r"\b"
    r")",
    re.I,
)


def _enrich_location_from_title(loc: str, title: str) -> str:
    """If location is bare 'Remote' or empty, extract geographic hints from
    title — e.g. a title like "CSM - US" or "Account Manager (EMEA)" often
    carries the actual hiring-eligibility signal an ATS never put in the
    structured location field at all. Deliberately gated to the BARE-location
    case only (not applied when location already has real content): the main
    classification pipeline's EMEA/Global residue checks are sensitive to any
    extra text sitting in `loc`, so blending title text into an
    already-populated, already-qualified location risks a false-negative
    (e.g. downgrading a genuine EMEA match because of unrelated leftover
    title text). When location is bare/blank, there's nothing to blend with —
    the title is the ONLY signal available, so it's used outright."""
    if not title:
        return loc

    if not _is_bare_location(loc):
        return loc

    # 2026-09 ROUND 3 (explicit user correction): a title naming 2+
    # distinct region acronyms/global-words together (e.g. "Regional
    # Account Manager - MENA, AMER, Africa, EMEA, Latam") signals broad
    # multi-region reach. Extracting just ONE of them below (whichever
    # _TITLE_LOCATION_RE happens to match, typically the last one before
    # the title ends) would incorrectly narrow an intentionally-broad
    # posting down to a single restrictive region. Leave `loc` unchanged
    # (still bare/blank) in that case — it then falls through to the
    # normal bare-Remote 'unsure' bucket (or stays blank/'unsure') instead
    # of being hard-rejected over one arbitrarily-picked region name.
    if len({m.group(0).lower() for m in _TITLE_MULTI_REGION_WORDS_RE.finditer(title)}) >= 2:
        return loc

    match = _TITLE_LOCATION_RE.search(title)
    if match:
        geo = next((g for g in match.groups() if g), None)
        if geo:
            geo = geo.strip()
            if "remote" in loc.strip().lower():
                return f"Remote, {geo}"
            return geo

    return loc


# ── Description-body location enrichment (2026-09, explicit user report)──
# Real postings that motivated this: viaquestinc.com's Paycor/Gnewton
# listing, whose page reads "Location: Bowling Green, OH" as plain
# table-cell text — a genuine labeled location this project's Crawl II
# scraper simply never captured into job["location"] at all (see
# crawl_ii.py's _strip_html/_HEURISTIC_LOCATION_RE fix for the scraper-
# level half of this — an unescaped "&nbsp;" sitting between the label and
# the value broke that regex). This is the classifier-level half: even
# where a scraper never gets it right (Crawl III's stapply.ai source has
# no structured location field AT ALL for some postings — explicit user
# report: "for some of them, the location is in the JD and for some
# reason we don't seem to be seeing that"), a labeled location mention
# sitting in the raw description body text is still real, recoverable
# evidence. Deliberately broader label vocabulary than
# crawl_ii.py's own heuristic-page-only version (this runs for EVERY job
# in EVERY pipeline via _keyword_classify_location_detail/classify_rank4,
# not just Crawl II's heuristic-extraction path) — "location status",
# "workplace setting", "work from", "based in", explicit user-requested
# additions, alongside the existing "primary/job/work location" set.
_DESC_LOCATION_LABEL_RE = re.compile(
    r"\b(?:primary\s*location|job\s*location|work\s*location|office\s*location|"
    r"location\s*(?:status|type)?|workplace\s*(?:setting|type|location)|"
    r"work\s*(?:from|site)|based\s*in)\s*"
    # 2026-09 BUG FIX (explicit user-commissioned adversarial fuzz test,
    # real discovered false negative): a colon may directly abut the value
    # ("Location:Bowling Green, OH") with no ambiguity, but a bare hyphen
    # must have whitespace on both sides to count as a label separator --
    # otherwise a compound word like "Location-agnostic" (a GLOBAL_KEYWORDS
    # member) gets misparsed as label "Location" + value "agnostic", which
    # then hijacks the bare location field away from ever being recognized
    # as the global keyword it actually is.
    r"(?:\s*:\s*|\s+-\s+)"
    r"(?:&nbsp;|\s)*"
    r"([A-Za-z][^\n|]{1,60}?)"
    r"(?=\s+(?:Apply|Department|Job\s*Type|Employment|Requirements|Responsibilities|"
    r"Qualifications|About|Benefits|Salary|Schedule|Description|Overview|Summary|"
    r"Remote\s*Status|Workplace\s*(?:Setting|Type)|Job\s*Id|#\s*of\s*Openings|"
    r"Who\s|What\s|We\s|Click|View|Full[- ]?Time|Part[- ]?Time|Posted|Date|Category)\b"
    r"|[.;|]\s|\n|$)",
    re.I,
)


def _enrich_location_from_description(loc: str, description_snippet: str) -> str:
    """Mirrors _enrich_location_from_title, but pulls from a location/
    workplace LABEL sitting in the raw description body text instead of
    the title — see _DESC_LOCATION_LABEL_RE's module comment above for the
    real postings this closes. Same bare-only gate as the title version
    (and runs strictly AFTER it — title enrichment already had first
    chance): only fires when `loc` is still genuinely bare, so a real
    field or title-derived value is never overridden by description
    text. The extracted value still has to survive the normal Africa/
    EMEA/Global/bare-Remote classification below like any other location
    text — this only recovers a value to test, it never decides
    match/no-match itself."""
    if not description_snippet or not _is_bare_location(loc):
        return loc
    m = _DESC_LOCATION_LABEL_RE.search(description_snippet)
    if not m:
        return loc
    value = re.sub(r"&nbsp;|\s+", " ", m.group(1)).strip(" ,.-|")
    if not value or len(value) > 60:
        return loc
    return value


# ── Location priority tiers (for sort order on upsert) ────
# 2026-09: jobs.location_priority was widened from int to text (still
# holding just "1"/"2"/"3" for now) so Phase 2 of the ranking revamp can
# introduce "3a"/"3b"/"4a"/"4b" sub-tiers without another schema migration
# — see the Supabase migration location_priority_to_alphanumeric_text.
# These three constants are plain strings for that reason, not because the
# tier semantics changed. Lower/earlier tier = higher priority.
PRIORITY_GLOBAL = "1"   # ONLY a strictly, unambiguously worldwide/anywhere/
                       # global-hiring signal — never just "several regions",
                       # however many.
PRIORITY_AFRICA = "2"   # 2026-09 BUG FIX (explicit user instruction: precise
                       # definition of what earns each priority number) —
                       # covers FOUR cases, not just "Africa or bare EMEA":
                       #   1. Africa as a continent (not a single member
                       #      country)
                       #   2. Bare EMEA, no narrower qualifier
                       #   3. EMEA + 2 or more OTHER business regions
                       #      together (e.g. "EMEA, LATAM, AMER")
                       #   4. 2 or more business regions together WITHOUT
                       #      EMEA (e.g. "LATAM, AMER", "APAC, AMER, LATAM")
                       # Cases 3/4 used to be bucketed at PRIORITY_UNSURE
                       # ("genuine multi-region scope... intentionally kept"
                       # at the uncertain tier) — that was wrong per the
                       # corrected policy: multi-region breadth (2+ distinct
                       # regions, whether or not EMEA is one of them) is
                       # exactly what this tier is for, not a reason to
                       # under-rank it as merely "uncertain". See
                       # _has_multi_region_breadth's two call sites below and
                       # LOCATION_SYSTEM_PROMPT's MATCH_AFRICA section.
PRIORITY_UNSURE = "3"   # allowed fallback tier, but a NARROW one: only when
                       # the posting is truly, truly without ANY location
                       # restriction signal AND does not meet the
                       # PRIORITY_AFRICA multi-region bar above either — bare
                       # Remote/N/A/blank locations, or genuine ambiguity
                       # after reading the whole posting. A job naming 2+
                       # distinct business regions is PRIORITY_AFRICA (2),
                       # never this tier — see PRIORITY_AFRICA's comment.
                       #
                       # 2026-09 (classification revamp Phase 2, explicit
                       # user request): split into two sub-tiers below.
                       # PRIORITY_UNSURE itself is kept defined (unused by
                       # new code, nothing else in this file relies on
                       # removing it) only so any stale caller/DB row still
                       # referencing plain "3" doesn't break.
PRIORITY_UNSURE_BLANK = "3a"  # sent to the AI stage for MISSING/INCOMPLETE
                       # data — no location field at all (the AI is asked to
                       # find global/EMEA/Africa language spread across the
                       # JD instead), or an ATS/pipeline (e.g. Crawl III's
                       # stapply.ai CSVs) that has no application-question
                       # data to inspect in the first place. Per explicit
                       # user policy: the AI's verdict here NEVER promotes a
                       # job to PRIORITY_GLOBAL/PRIORITY_AFRICA — only the
                       # regex stage above does that ("I trust regex
                       # more... only regex passes makes this go into rank 1
                       # or 2"). A job the AI reviews lands at 3a/3b
                       # regardless of what it concludes, UNLESS the AI
                       # finds a genuine NEGATIVE/restrictive signal regex
                       # missed, in which case it's dropped instead (same as
                       # a keyword no_match).
PRIORITY_UNSURE_SILENT = "3b"  # location field, description, AND
                       # application questions are ALL present on this job
                       # — genuinely complete data — and ALL are silent on
                       # geography (bare "Remote"/N/A location, nothing in
                       # the JD or questions positively or negatively
                       # signals global/EMEA/Africa/country-specific
                       # hiring). Distinct from 3a: this tier means "we had
                       # everything and it still doesn't say," not "we
                       # don't have enough to know."


# ── Africa-continent detection ────────────────────────────
# Deliberately a NARROW, standalone check — just "does a real African
# country's full name appear as a whole word" — rather than routing
# through geo.extract_countries()'s full multi-country machinery (state
# codes, ISO2 prefixes, trailing-country-code rules, etc.). None of that
# apparatus is needed here: no African country name in this project's
# gazetteer collides with a US state/Canadian province name the way
# "Mexico"/"Wales"/"Ontario" did, so a bare word-boundary match is safe.
# 2026-10 correction: two real collisions - "Benin City" (a city in Nigeria,
# so "Benin City, Nigeria" counted as TWO African countries and was admitted
# as Rank 2) and "Papua New Guinea" (not Guinea).
_AFRICAN_COUNTRY_RE = re.compile(
    r"(?<!Papua New )\b(" + "|".join(re.escape(c) for c in sorted(geo.AFRICAN_COUNTRIES, key=len, reverse=True)) + r")\b(?!\s+City\b)",
    re.I,
)


def _keyword_classify_location_detail(job: dict) -> tuple[str, int | None, str | None]:
    """
    Returns (result, priority, unsure_reason) where result is 'match',
    'no_match', or 'unsure'; priority (PRIORITY_GLOBAL / PRIORITY_AFRICA /
    None) is only meaningful when result == 'match'; unsure_reason is only
    meaningful when result == 'unsure' and is one of:
      'blank'       — location field was empty/placeholder. This is the
                      ambiguous case: could be a genuinely unlisted
                      location, OR a scraper extraction bug silently
                      leaving the field blank (confirmed live twice —
                      Inabia/JazzHR and Sonepar/SuccessFactors, both
                      2026-09 — where the real posting was flat-out
                      country-specific but a markup-parsing miss in the
                      scraper produced location=""). Since a blank field
                      can't be told apart from an extraction failure, this
                      reason is held to a HIGHER bar downstream: it must
                      get an AI verdict backed by real evidence
                      (match_global/match_africa) to survive — a plain
                      "AI looked and still couldn't tell" is NOT enough
                      and gets dropped, unlike 'bare_remote' below. See
                      crawl_i.py/crawl_ii.py's location-filter functions.
      'bare_remote' — location field explicitly said "Remote" with no
                      other qualifier. This IS a real signal the company
                      itself provided (not a data gap), just one that
                      doesn't say which region — kept at the same
                      benefit-of-the-doubt policy as before (AI-uncertain
                      still survives at PRIORITY_UNSURE).

    STRICT ALLOWLIST, rewritten 2026-08. The only ways a job can survive
    this filter:
      1. An explicit GLOBAL_KEYWORDS phrase (global/worldwide/
         international/distributed/anywhere/... — ~80 variants).
      2. Africa as a continent — the literal word "Africa", or 2+
         DIFFERENT African countries named together (proof of
         continent-wide reach, not just "based in one African country").
      3. EMEA alone, with no city/country qualifier attached.
    Everything else is rejected immediately — including a location that
    lists several real places, no matter how many, if none of the above
    three signals is present. The previous version tried to infer "global"
    from counting distinct countries in the text (2+ countries = Global);
    that inference kept getting fooled by real-world place-name collisions
    (US towns sharing a name with a country, state/province codes that are
    also ISO2 country codes, etc.) and was letting single-country US/CA/AU
    postings through. This version doesn't try to infer anything — it
    only trusts an explicit keyword, or genuine multi-country Africa
    evidence, or explicit EMEA text. A location with NO qualifying
    keyword is rejected outright, UNLESS it's blank/placeholder or a bare
    "Remote" with nothing else attached — those two cases alone go to
    'unsure' so the AI stage gets a look at genuinely ambiguous listings,
    rather than every non-matching job being silently AI-reviewed.
    """
    # ── 0. HARD OVERRIDE: explicit "we can't/won't sponsor" language
    # anywhere in the title/description always means NO_MATCH, checked
    # BEFORE the location field or the AI stage ever gets a say. See
    # has_hard_no_sponsorship_signal's docstring for the real posting
    # (a company-wide "we hire globally" claim doesn't override a
    # specific role's own "no sponsorship, must already be authorized"
    # statement) that slipped past classification without this. ──
    if has_hard_no_sponsorship_signal(job):
        return "no_match", None, None

    # A concrete role/candidate place always outranks a broad word such as
    # EMEA in the title or company copy.
    if has_role_specific_place_restriction_signal(job):
        return "no_match", None, None

    # ── 0.5. HARD OVERRIDE: scraper-reported workplace_type says this
    # specific posting is Hybrid/On-site/In-office/In-person, regardless
    # of what the bare location field claims (e.g. location="Remote" but
    # workplace_type="Hybrid"). See has_non_remote_workplace_type's
    # docstring for the real Infor/Pinpoint posting this closes. ──
    if has_non_remote_workplace_type(job):
        return "no_match", None, None

    # ── 0.55. HARD OVERRIDE (2026-09, explicit user report, real postings:
    # Kraft Heinz's Eightfold listing — page explicitly said "Hybrid
    # Working" nowhere near a structured workplace_type field or a title
    # suffix — and viaquestinc.com's Paycor/Gnewton listing, whose page
    # literally reads "Remote Status: On-Site" as body text, not a field
    # this project's scrapers were reading into workplace_type at all).
    # has_non_remote_workplace_type (0.5, above) only ever sees a
    # scraper-populated `workplace_type` FIELD; has_non_remote_title_signal
    # (0.6, below) only ever sees the TITLE. Neither one, nor
    # has_office_attendance_signal (0.88, below — narrowly scoped to
    # "N days/week in office" attendance-frequency phrasing), catches a
    # bare workplace-type LABEL sitting in the free-text description
    # itself ("Remote Status:", "Workplace setting:", "Location status:",
    # "Work from:", ... followed by Hybrid/On-site/In-office/In-person),
    # or an unambiguous standalone phrase like "Hybrid Working". See
    # has_non_remote_labeled_text_signal's docstring for the full label
    # vocabulary and the false-positive guards. ──
    if has_non_remote_labeled_text_signal(job):
        return "no_match", None, None

    # ── 0.6. HARD OVERRIDE (2026-09, explicit user request): the TITLE
    # itself carries a physical-presence qualifier ("... (Hybrid)",
    # "... - Onsite"), independent of the workplace_type field above. See
    # has_non_remote_title_signal's docstring. ──
    if has_non_remote_title_signal(job):
        return "no_match", None, None

    # ── 0.75. HARD OVERRIDE: an affirmative country-specific work-
    # authorization requirement (or a flagged work-auth/visa/sponsorship
    # application question), independent of whether the posting also
    # says anything about sponsorship. See has_hard_country_specific_
    # auth_signal's docstring for the two real JazzHR postings this
    # closes — one of which mentioned no "sponsor" wording at all. ──
    if has_hard_country_specific_auth_signal(job):
        return "no_match", None, None

    # 2026-10: application questions that bind the candidate to a place
    # (relocate / commute / reside / authorized-to-work-in / clearance ...).
    if has_restrictive_geo_question_signal(job):
        return "no_match", None, None

    # ── 0.76. HARD OVERRIDE (2026-09, explicit user-commissioned
    # adversarial fuzz test — ~4,820 generated restrictive phrasings run
    # directly against this pipeline): sponsorship/work-permit/residency
    # phrasing tied to a named country — a distinct phrasing family from
    # has_hard_country_specific_auth_signal just above (that one only
    # catches "authorized/eligible/entitled/permitted to work in
    # <country>," not "require visa sponsorship to work in <country>,"
    # "need a work permit for <country>," or "maintain residence in
    # <country>"). This exact check already existed for Rank 4 only; see
    # has_country_tied_sponsorship_permit_residency_signal's docstring for
    # why applying it everywhere closes a real, large gap — dozens of
    # ordinary screening-question phrasings were reaching 'unsure' instead
    # of a deterministic no_match for every job outside Rank 4's narrow
    # gate. ──
    if has_country_tied_sponsorship_permit_residency_signal(job):
        return "no_match", None, None

    # ── 0.77. HARD OVERRIDE (2026-09, explicit user report, real posting:
    # Twilio's Greenhouse "Senior Manager, Customer Success" — location
    # named Australia, screening questions asked "the country in which
    # this role is located"/"where this role is listed" without naming it
    # directly). See has_referential_auth_question_with_named_place_
    # signal's docstring — distinct from has_hard_country_specific_auth_
    # signal just above, which requires the country to be named IN THE
    # QUESTION TEXT ITSELF; this catches a question that instead refers to
    # wherever the job's own location already says, which is only
    # disqualifying when this job actually names a real, specific,
    # non-broad place. ──
    if has_referential_auth_question_with_named_place_signal(job):
        return "no_match", None, None

    # ── 0.8. HARD OVERRIDE (2026-09, real posting: RethinkCare's "Senior
    # Client Success Manager", JazzHR/rethink.applytojob.com): location
    # field said bare "Remote" — a real, honest signal, not an extraction
    # bug — but the DESCRIPTION separately stated "Remote opportunities
    # are available to candidates who reside in the following states:
    # AL, AZ, CT, FL, ... [30 US states]." That's a hard US-only
    # eligibility restriction, but bare "Remote" alone used to sail this
    # straight into the AI stage, and the AI came back "uncertain" (not
    # "no_match") rather than reading and flagging that state list itself
    # — which then hit this project's own "bare_remote AI-uncertain is
    # kept at PRIORITY_UNSURE" policy and got written to the jobs table
    # anyway. An enumerated list of specific U.S. state codes is
    # unambiguous, deterministic evidence a posting is NOT global — no
    # need to leave this up to an LLM's read of the full description when
    # a cheap keyword check can catch it every time. See
    # has_state_list_restriction_signal's docstring. ──
    if has_state_list_restriction_signal(job):
        return "no_match", None, None

    # ── 0.85. HARD OVERRIDE (2026-09, real posting: OpenSesame's "Sales
    # Operations Manager, Direct Sales", Greenhouse): the description's own
    # words state the role must be based/located in, or worked from, one
    # specific named country ("This position can be based anywhere in the
    # US") — a hard country-wide restriction independent of both the
    # work-authorization phrasing 0.75 catches and the enumerated-state-list
    # phrasing 0.8 catches. See has_hard_country_based_restriction_signal's
    # docstring. ──
    if has_hard_country_based_restriction_signal(job):
        return "no_match", None, None
    if has_extra_restrictive_geography_signal(job):
        return "no_match", None, None

    # ── 0.86. HARD OVERRIDE (2026-09, cross-LLM review, real posting:
    # Stripe's "Program Manager, Security GRC"): Greenhouse's own metadata
    # names a specific, non-global place even though the location FIELD
    # this project reads said nothing more specific than "Remote". See
    # has_hard_metadata_location_signal's docstring. ──
    if has_hard_metadata_location_signal(job):
        return "no_match", None, None

    # ── 0.865. HARD OVERRIDE (2026-09, explicit user request: "track the
    # location symbol and what location sits by it ... particularly
    # useful ... in the case of in-house ATSs"): a map-pin/location glyph
    # sitting directly next to a specific, non-global place name in the
    # raw posting (in-house career pages that don't expose a structured
    # location field commonly still show this visually) is the same kind
    # of "the posting named a place through a channel this project's
    # normal location-field extraction never sees" signal as the
    # Greenhouse-metadata check just above. See
    # has_hard_location_symbol_signal's docstring. ──
    if has_hard_location_symbol_signal(job):
        return "no_match", None, None

    # ── 0.87. HARD OVERRIDE (2026-09, cross-LLM review, real postings:
    # Arcwood's "Account Manager - Louisiana", OpenProject's "(Senior)
    # Account Manager - Europe", HeroDevs' "Channel Account Manager,
    # EMEA"): the TITLE itself carries the only region/state restriction,
    # independent of the location field or description body. See
    # has_title_region_restriction_signal's docstring. ──
    if has_title_region_restriction_signal(job):
        return "no_match", None, None

    # ── 0.88. HARD OVERRIDE (2026-09, explicit user instruction, real
    # posting: Together AI's Greenhouse listing, job 5070981007): a
    # screening question requiring in-office attendance a specific number
    # of days per week, or naming a specific office/city as a physical
    # attendance requirement — "Are you willing to work four days per week
    # in our San Francisco office?" — independent of the location field
    # (which just said "San Francisco" with no other qualifier) and of the
    # workplace_type/title-suffix checks above (this project had no
    # detector at all for a body-text/application-question ATTENDANCE
    # REQUIREMENT phrased as a question, only for an explicit
    # Hybrid/On-site/In-office FIELD value or title suffix). This question
    # only reached description_snippet at all after the 2026-09 fix to
    # ats_scrapers.py's _format_screening_questions() — see that function's
    # docstring: previously every non-work-authorization-shaped screening
    # question, including this one, was silently dropped before
    # classifier.py ever saw it. See has_office_attendance_signal's
    # docstring. ──
    if has_office_attendance_signal(job):
        return "no_match", None, None

    # ── 0.89. HARD OVERRIDE (2026-09, explicit user-provided taxonomy of
    # restrictive job-posting language): a company-capability statement
    # ("we don't have a legal entity in your country"), an explicit
    # exclusion ("not open to candidates outside the US"), or a curated
    # country-list phrasing ("the following countries only") — none of
    # which match the classic "must reside/be based in <country>" shape the
    # overrides above already catch. See
    # has_entity_or_exclusion_restriction_signal's docstring. ──
    if has_entity_or_exclusion_restriction_signal(job):
        return "no_match", None, None

    # ── 0.895. HARD OVERRIDE (2026-09, explicit user-provided taxonomy of
    # restrictive job-posting language): a residence-verb-governed timezone
    # requirement ("must be located in a US timezone" — distinct from safe
    # "overlap with our hours" scheduling wording), an explicit relocation
    # requirement naming a place, or a hyphenated "<place>-based candidates
    # only" construction. See
    # has_timezone_relocation_or_hyphenated_restriction_signal's docstring. ──
    if has_timezone_relocation_or_hyphenated_restriction_signal(job):
        return "no_match", None, None

    # ── 0.896. HARD OVERRIDE (2026-09, explicit user report, real posting:
    # SideCar Health's Greenhouse application question "Do you have a
    # Texas State Health and Life insurance license?"): a US-state-specific
    # professional/occupational license question. See
    # has_state_specific_license_signal's docstring. ──
    if has_state_specific_license_signal(job):
        return "no_match", None, None

    # ── 0.897. HARD OVERRIDE (2026-09, explicit user instruction: "roles
    # with say CSM - GERMAN speaking, French speaking should be excluded
    # too"): a hard-requirement language-fluency qualifier, in the title or
    # in a non-"nice to have" part of the description/application
    # questions. See has_language_fluency_restriction_signal's docstring
    # for the full title-vs-description and hard-vs-soft-section policy. ──
    if has_language_fluency_restriction_signal(job):
        return "no_match", None, None

    # 2026-09: use `or ""`, not `.get(key, "")` — a job dict sourced from
    # Supabase (a NULL column) or a scraper that found no location has the
    # key PRESENT with value None, not missing, so the "" default here
    # never kicked in and `raw_loc + " " + raw_country` crashed with
    # "unsupported operand type(s) for +: 'NoneType' and 'str'" the moment
    # either field was None. This is the same safe idiom already used
    # everywhere else in this file (see e.g. line ~1253 below).
    raw_loc = job.get("location") or ""
    raw_country = job.get("country") or ""
    if isinstance(raw_loc, list):
        raw_loc = ", ".join(str(x) for x in raw_loc)
    if isinstance(raw_country, list):
        raw_country = ", ".join(str(x) for x in raw_country)
    loc = (raw_loc + " " + raw_country).strip()

    title = job.get("title", "")
    loc = _enrich_location_from_title(loc, title)
    loc = _enrich_location_from_description(loc, job.get("description_snippet") or "")
    loc_lower = loc.lower()

    # ── 1. Empty / placeholder → UNSURE (send to AI) ──────
    if not loc.strip() or PLACEHOLDER_LOC_RE.match(loc):
        return "unsure", PRIORITY_UNSURE, "blank"

    has_remote = bool(re.search(r"\bremote\b", loc_lower))

    # ── 2. Africa as a continent ──────────────────────────
    # Literal "Africa" anywhere → match. Otherwise, 2+ DIFFERENT African
    # countries named together is real evidence of continent-wide
    # African hiring — a single African country alone ("Nigeria",
    # "South Africa") is REJECTED, because that's "based in one African
    # country," not "hiring across Africa."
    #
    # BUG FIXED 2026-08: "South Africa" is itself a single African
    # country whose official name CONTAINS the word "Africa" as its own
    # token — \bafrica\b matched inside it and let a single-country
    # "South Africa" / "Cape Town, South Africa" posting through as a
    # continent-wide match, exactly the failure mode this function's own
    # docstring says must be rejected. Fix: strip every "South Africa"
    # occurrence out of the text before testing for a bare "Africa"
    # continent mention, so only a genuine standalone "Africa" (or a
    # regional phrase like "West Africa", "Sub-Saharan Africa", "Africa
    # (Remote)") still counts as the continent signal. "South Africa" the
    # country still gets its fair shot at matching below via the 2+
    # distinct-countries rule, same as any other single African country.
    africa_continent_check = re.sub(r"\bsouth[\s\-]+africa\b", " ", loc_lower)
    if re.search(r"\bafrica\b", africa_continent_check):
        return "match", PRIORITY_AFRICA, None

    african_hits = {m.group(1).lower() for m in _AFRICAN_COUNTRY_RE.finditer(loc)}
    if len(african_hits) >= 2:
        # 2026-10: 2+ African countries alone isn't enough — require a
        # remote signal to confirm this is an Africa-wide remote role, not
        # local hiring across specific African cities/offices.
        wt = job.get("workplace_type") or ""
        title_str = job.get("title") or ""
        has_remote_signal = (
            has_remote
            or bool(_REMOTE_WORKPLACE_RE.search(wt))
            or bool(re.search(r"\bremote\b", title_str, re.I))
        )
        if has_remote_signal:
            return "match", PRIORITY_AFRICA, None

    # ── 2.5. Multi-region breadth in the LOCATION FIELD itself → match
    # (2026-09 ROUND 5 FALSE-NEGATIVE FIX, found during this session's
    # closing test sweep, explicit user request to hunt for exactly this
    # class of bug): the user's own long-standing policy (see
    # _REGION_ONLY_WORDS_RE's module comment: "if it has multiple
    # locations and africa thats good. Like say: MENA, AMER, Africa, EMEA,
    # Latam. thats acceptable too.") already treats 2+ distinct business
    # regions named together as evidence of broad multi-region reach — but
    # that logic (_has_multi_region_breadth) was previously only wired
    # into the DESCRIPTION/TITLE hard-override guards (has_hard_country_
    # based_restriction_signal, has_title_region_restriction_signal), never
    # into this function's own LOCATION-FIELD classification. A job whose
    # location field literally read "APAC, EMEA" or "MENA, AMER, EMEA,
    # Latam" — genuine, textbook multi-region breadth — fell through to
    # step 3 below, where the strict "EMEA must have NO other residue"
    # rule saw the other region names as disqualifying residue and
    # rejected the job as no_match: exactly backwards, since 2+ regions is
    # stronger evidence of broad hiring than bare EMEA alone, not weaker.
    # Checked BEFORE the bare-EMEA residue check for that reason — this is
    # a superset case, not a competing one.
    #
    # 2026-09 BUG FIX (explicit user instruction, precise priority-number
    # policy): this used to return PRIORITY_UNSURE here despite this exact
    # block's OWN comment already saying "bucketed at PRIORITY_AFRICA" —
    # a real comment/code mismatch, not just a wording gap. Corrected to
    # match both the comment's original intent and the user's explicit
    # rule: 2+ distinct business regions named together (EMEA + others, OR
    # 2+ regions with no EMEA at all — "LATAM, AMER", "APAC, AMER, LATAM")
    # is PRIORITY_AFRICA (2), the same tier bare EMEA/Africa-continent use
    # — broader than a single region, narrower than an explicit
    # "global"/"worldwide" claim, but a genuine MATCH, not merely
    # "uncertain, kept anyway". See PRIORITY_AFRICA's own comment above.
    if _has_multi_region_breadth(loc):
        return "match", PRIORITY_AFRICA, None

    # ── 3. EMEA → match ONLY if no country/city qualifier ─
    if re.search(r"\bemea\b", loc_lower):
        check = re.sub(r"\bemea\b", "", loc_lower)
        check = NON_GEO_WORDS_RE.sub("", check)
        # 2026-09 ROUND 5 (explicit user-provided EMEA-wide hiring lingo
        # list): also strip the connector/filler words this project's own
        # "EMEA-wide" phrasing family uses ("EMEA-wide", "across EMEA",
        # "throughout EMEA", "all EMEA countries", "any EMEA country") —
        # without this, a location FIELD value like "EMEA - All Countries"
        # or "EMEA Wide" left "all countries"/"wide" as residue and was
        # wrongly rejected as a qualified (non-bare) EMEA value, even
        # though none of those words name an actual place.
        check = _EMEA_FILLER_RE.sub("", check)
        check = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9&|]+", " ", check).strip()
        if not check:
            # EMEA (Europe/Middle East/Africa) includes Africa but is
            # broader than "global" — bucketed with Africa, not Global.
            return "match", PRIORITY_AFRICA, None
        return "no_match", None, None

    # ── 4. Explicit Global/Worldwide/International/Distributed/
    # Anywhere/... keyword (see GLOBAL_KEYWORDS, ~80 variants) ──
    # Residue check: strip out the EXACT substring(s) that matched a
    # keyword, then confirm nothing else (a real city/country name) is
    # left over — "Global (Remote, US Only)" should NOT match just
    # because "Global" appears; the leftover "us only" gives it away.
    if STANDALONE_GLOBAL_RE.search(loc.strip()):
        return "match", PRIORITY_GLOBAL, None

    check = loc_lower
    matched_any = False
    for rx in GLOBAL_RE:
        if rx.search(check):
            matched_any = True
            check = rx.sub(" ", check)
    if matched_any:
        check = NON_GEO_WORDS_RE.sub("", check)
        check = GLOBAL_FILLER_RE.sub("", check)
        check = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9&]+", " ", check).strip()
        if not check:
            return "match", PRIORITY_GLOBAL, None
        return "no_match", None, None

    # ── 5. Positive evidence in the JD can rescue a bare Remote field ──
    # The location field itself is ambiguous, but the description/title may
    # contain a concrete hiring-scope statement. Evaluate that evidence
    # BEFORE treating bare Remote as merely uncertain.
    #
    # 2026-09 BUG FIX (explicit user report, two real postings: Fresha's
    # "Account Manager (Amsterdam) - Danish Speaking" and leva-eu.com's
    # "Projectmanager, OEMbikes - Amsterdam, North Holland (NL)" — both
    # landed at PRIORITY_AFRICA via THIS step, clearance="regex", even
    # though the title itself names a specific city and (for the Fresha
    # posting) the role explicitly wants a Danish speaker for one
    # location): this step used to run unconditionally whenever the
    # location-FIELD checks (steps 2-4) didn't match — including when
    # `loc` was NOT ambiguous at all, but a real, specific, already-
    # extracted place (here, "Amsterdam" — pulled in by
    # _enrich_location_from_title from the title text, since both
    # postings' own `location` field was blank). A company's JD commonly
    # carries loose "EMEA"/"global" language elsewhere in the page
    # (department tags, About-Us boilerplate, benefits copy) that has
    # nothing to do with THIS specific posting's actual place — step 5's
    # own docstring already says "the location field itself is
    # ambiguous", but nothing enforced that before firing. Gated now to
    # only run when `loc` is genuinely bare (blank, a placeholder, or a
    # plain "Remote" with nothing else attached) — the exact same test
    # _enrich_location_from_title uses to decide whether title text was
    # even worth blending in. A `loc` that already names a real place
    # (from the location field OR the title) is a real, specific signal
    # that must be trusted over unrelated free text elsewhere in the JD —
    # it falls through to the "REJECT everything else" step below instead
    # of getting a second, looser chance here.
    if _is_bare_location(loc):
        full_text = (job.get("title") or "") + " " + (job.get("description_snippet") or "")
        if _text_has_global_evidence(full_text):
            return "match", PRIORITY_GLOBAL, None
        if _text_has_africa_or_emea_evidence(full_text):
            return "match", PRIORITY_AFRICA, None
        # 2026-09 BUG FIX: see the identical fix + rationale on the
        # location-FIELD multi-region check above (step 2.5) — same policy
        # correction applies here for multi-region evidence found in the JD
        # TEXT instead of the location field: PRIORITY_AFRICA (2), not
        # PRIORITY_UNSURE (3).
        if _has_multi_region_breadth(full_text):
            return "match", PRIORITY_AFRICA, None

    # ── 5. Bare "Remote" with nothing else qualifying it → UNSURE
    # (send to AI). Any OTHER text attached to "remote" (a city, a
    # country, "hybrid", "US only", etc.) is a real qualifier and gets
    # rejected outright, per the strict-allowlist policy above. ──
    if has_remote:
        stripped = NON_GEO_WORDS_RE.sub("", loc_lower)
        stripped = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9]+", " ", stripped).strip()
        if not stripped:
            return "unsure", PRIORITY_UNSURE, "bare_remote"
        return "no_match", None, None

    # ── 6. REJECT everything else outright ────────────────
    # No Global/EMEA/Africa keyword, not blank, not bare "Remote" — this
    # is a job tied to a specific place (or places) with no explicit
    # broad-hiring signal, so it's rejected without going to the AI.
    return "no_match", None, None


def _text_has_global_evidence(text: str) -> bool:
    """Loose (no residue-stripping) check: does ANY of the same GLOBAL_
    KEYWORDS/STANDALONE_GLOBAL_RE evidence used by the deterministic
    location-field classifier appear anywhere in free-form text (title +
    description)? Used only as a post-AI sanity check — see
    ai_classify_locations' "Post-AI safety net" section — so it's
    deliberately permissive (no requirement that the match be the ONLY
    thing in the text, unlike the strict location-field residue check)."""
    if not text:
        return False
    t = text.lower()
    if STANDALONE_GLOBAL_RE.search(text.strip()):
        return True
    return any(rx.search(t) for rx in _SAFETY_NET_GLOBAL_RE) or any(rx.search(t) for rx in _EXTRA_GLOBAL_HIRING_RE)


def _text_has_africa_or_emea_evidence(text: str) -> bool:
    """Same idea as _text_has_global_evidence but for the Africa/EMEA
    tier: literal 'Africa' (continent, excluding 'South Africa'), 2+
    distinct African countries, or bare 'EMEA' anywhere in the text."""
    if not text:
        return False
    t = text.lower()
    africa_check = re.sub(r"\bsouth[\s\-]+africa\b", " ", t)
    if re.search(r"\bafrica\b", africa_check):
        return True
    if len({m.group(1).lower() for m in _AFRICAN_COUNTRY_RE.finditer(text)}) >= 2:
        return True
    return bool(re.search(r"\bemea\b", t))


def keyword_classify_location(job: dict) -> str:
    """
    Returns 'match', 'no_match', or 'unsure'.

    MATCH = truly global hiring signals (anywhere, worldwide,
    international, WFA, EMEA alone, Africa as continent).

    Thin wrapper over _keyword_classify_location_detail() for callers that
    only need the verdict, not the priority tier (e.g. ats_scrapers.py's
    application-question enrichment, which only checks for "unsure").
    """
    result, _, _ = _keyword_classify_location_detail(job)
    return result


LOCATION_SYSTEM_PROMPT = """\
You decide whether a job posting should be included in a list of roles \
open to candidates working remotely from ANYWHERE in the world, from \
across the EMEA region (Europe/Middle East/Africa), from anywhere on \
the African continent, OR across two or more business regions together \
(such as AMER + LATAM, APAC + AMER, EMEA + APAC, or EMEA + LATAM + AMER — \
ANY 2 or more of AMER/LATAM/APAC/EMEA/MENA/ANZ/NAM/DACH named together, \
whether or not EMEA is one of them). A role with no geographic \
restriction signal at all is also allowed and must be labeled UNCERTAIN \
(priority 3). A single narrow region such as APAC or LATAM BY ITSELF \
(nothing else named alongside it), a single country, or a single \
city/state is not allowed.

Every job you're shown here already has an ambiguous LOCATION field \
(bare "Remote", blank, or a placeholder like "N/A") — the location field \
gave no usable signal, which is exactly why it's being sent to you. Your \
only source of truth is the JOB TITLE and the full DESCRIPTION text \
below, which is provided IN FULL (not truncated) specifically so you can \
find the real eligibility language wherever it appears in the posting — \
including in application-question text that may be appended at the end \
of the description (e.g. "Application Question: Are you authorized to \
work in the US?" is itself evidence of a country restriction, not just \
a form field). A short or jargon-heavy description is not by itself a \
reason to say MATCH_GLOBAL or MATCH_AFRICA — read for real content, and \
if there genuinely isn't any after reading everything provided, say \
UNCERTAIN rather than guessing.

Respond with exactly one of these four labels per job. The three tiers \
below (MATCH_GLOBAL, MATCH_AFRICA, UNCERTAIN) are STRICT and mutually \
exclusive by exactly how broad the posting's hiring scope is — read all \
three definitions before choosing, since "broad enough to keep" is not \
the same question as "which tier":

MATCH_GLOBAL (priority 1) — ONLY for STRICTLY, unambiguously worldwide \
roles. Positive evidence of genuinely global hiring:
- Description or title explicitly says "global", "worldwide", "anywhere \
  in the world", "international", "work from anywhere", "distributed \
  team", "location-agnostic", "hire in any country", or a clear \
  equivalent
- Hiring across many countries spanning multiple continents in a way \
  that clearly means "wherever you are" (not just "a few offices" or a \
  specific named list of 2-4 business regions — a named list of regions, \
  however many, is the MATCH_AFRICA tier below, not this one, unless the \
  posting ALSO makes an explicit worldwide/anywhere claim on top of it)
- No geographic restrictions AND the role/company context clearly \
  supports global openness (e.g. "our fully remote team spans 30+ \
  countries across 6 continents")
Do NOT use MATCH_GLOBAL just because a posting names several regions. \
"We hire across EMEA, LATAM, APAC, and AMER" names four regions — that \
is MATCH_AFRICA (priority 2), not global, unless the posting separately \
states an actual worldwide/anywhere claim.

DO NOT treat as MATCH_GLOBAL evidence — generic company-branding/EEO \
boilerplate that is NOT about candidate eligibility at all:
- "We hire globally" / "we hire talent globally" / "hiring globally" \
  used as a mission-statement or About-Us-style sentence about the \
  COMPANY's general reach, not this specific role's eligibility (e.g. \
  buried in "About [Company]" copy, or paired with "without bias" / \
  "regardless of background" — that phrasing pattern is Equal \
  Opportunity Employer language about WHO can apply once eligible, not \
  a statement about WHERE candidates may be located)
- Any "diversity"/"inclusion"/"equal opportunity" sentence that mentions \
  a broad geography in passing — these are about non-discrimination, \
  not location eligibility, and must not be read as one
- The safest test: does the sentence say something concrete about WHERE \
  a candidate for THIS role may be based (a place, a region, "anywhere \
  you are"), or does it just use "global"/"worldwide" as flavor text \
  about company culture/reach/values? Only the former counts as \
  evidence. If genuinely unsure which it is, that sentence contributes \
  nothing — keep reading for something more concrete, and if nothing \
  concrete exists anywhere in the posting, say UNCERTAIN.

MATCH_AFRICA (priority 2) — covers FOUR distinct cases. This tier is \
BROADER than just "Africa or bare EMEA" — read all four:
1. The African continent, not a single member country: description or \
   title explicitly says "Africa" (as a hiring region, not just "we \
   have a Cape Town office") or names 2+ different African countries as \
   places the company hires from. A single African country alone (e.g. \
   "based in Nigeria", "Kenya office only") is NOT enough — that's one \
   country, not the continent.
2. Bare EMEA: description or title says "EMEA" with no further \
   single-country/city qualifier narrowing it back down to one place.
3. EMEA PLUS 2 or more OTHER business regions named together (e.g. \
   "EMEA, LATAM, AMER" or "EMEA and APAC and LATAM").
4. 2 or more business regions named together WITHOUT EMEA (e.g. "LATAM, \
   AMER", "APAC, AMER, LATAM"). Genuine multi-region breadth is \
   MATCH_AFRICA even when EMEA isn't one of the named regions — it \
   takes 2 or more DIFFERENT regions together to qualify. A SINGLE \
   region alone (just "APAC" alone, just "LATAM" alone, with nothing \
   else named) is NOT this tier — see NO_MATCH's "restricted to a \
   single region" bullet below.

NO_MATCH — evidence of a country- or narrow-region-specific restriction:
- "must be authorized/eligible to work in [country]"
- "US/UK/EU work authorization required"
- "W-2 employment", "W2 only", "must have SSN"
- "no visa sponsorship", "cannot sponsor", "will not sponsor" — and every
  paraphrase of this, not just those exact words. Real postings say this
  an enormous number of ways, and ALL of the following mean the same
  thing and are ALL grounds for NO_MATCH:
    "we're not able to sponsor visas at this time"
    "we are unable to sponsor an employment visa"
    "not able to offer visa sponsorship for this role"
    "won't be able to sponsor a work visa"
    "we don't currently offer visa sponsorship"
    "sponsorship isn't something we're able to provide"
    "visa sponsorship is not available for this position"
    "we're not in a position to sponsor work visas"
  A REAL EXAMPLE THAT WAS MISSED BEFORE (do not repeat this mistake):
  a posting said "You must be authorized to work in the US; we're not
  able to sponsor visas at this time." and was WRONGLY marked as
  globally open — read the whole sentence, not just for the literal
  words "cannot sponsor", and treat "not able to" + "sponsor" as the
  exact same signal as "cannot sponsor".
- "must reside in [state/country]", "must be located in [place]"
- "this role is based in [country]" without a global/EMEA/Africa-wide \
  remote option
- Restricted to APAC, LATAM, ANZ, NAM, DACH, or any other single region \
  narrower than "worldwide" or "EMEA/Africa"
- Country-specific benefits as requirements (401k, PAYE, tax residency)
- Time zone requirements that exclude most of the world \
  (e.g. "PST/EST hours required", "US business hours only")
- Says "remote" but then lists specific countries you must be located in
- An application question about work authorization/visa sponsorship for \
  one specific country, with no global/EMEA/Africa language elsewhere
- Description context makes it obvious the role is for one country \
  (e.g. references to US-specific regulations, UK employment law)
- A short unlabeled location tag naming a single US state, Canadian \
  province, or other sub-national region near the title or at the top \
  of the posting (e.g. "Remote, California", "Remote - Ontario", \
  "Remote (Texas)") — this is a real, specific eligibility restriction \
  even though it isn't introduced by the word "Location:". Treat ANY \
  single state/province named this way as equivalent to "must be \
  authorized to work in [that place]" unless the description elsewhere \
  contains genuine MATCH_GLOBAL/MATCH_AFRICA-level language that clearly \
  overrides it (a company having ONE state-restricted team while \
  another sentence says "we hire from anywhere" is rare — read the \
  whole posting before assuming an override exists)
  A REAL EXAMPLE THAT WAS MISSED BEFORE (do not repeat this mistake): a \
  posting titled "Senior Project Manager" had "Remote, California" \
  directly under the title with no further label, and was WRONGLY let \
  through as globally open because no sentence anywhere used the exact \
  words "must be located in" — the bare state tag itself IS the \
  restriction; don't wait for boilerplate phrasing to confirm it.
- ANY mention that this specific posting is Hybrid, On-site/Onsite, \
  In-office, or In-person — a labeled field or line ("Remote Status: \
  On-Site", "Workplace type: Hybrid", "Workplace setting: On-site", \
  "Work from: Office"), a plain sentence ("This is a Hybrid Working \
  role", "This position is on-site"), or a screening question about \
  in-office attendance. This OVERRIDES any global/EMEA/Africa/worldwide \
  language found ELSEWHERE in the same posting — a company can genuinely \
  be a global, distributed employer while THIS SPECIFIC role still \
  requires physical presence at one location; the per-role workplace-type \
  statement is what governs THIS job, not the company's general reach. \
  The only exception: a value that names Hybrid/On-site alongside \
  "Remote" together (e.g. "Hybrid or Remote", "Remote/On-site options \
  available") describes a genuine remote OPTION existing alongside \
  on-site ones, not a hybrid-only requirement — that is not disqualifying \
  by itself.
  A REAL EXAMPLE THAT WAS MISSED BEFORE (do not repeat this mistake): a \
  posting's location field said bare "Remote" and the description \
  contained generic "we operate globally" company language, but the same \
  page separately and explicitly said "Hybrid Working" with a specific \
  country named as the office location — this is NO_MATCH regardless of \
  the "global" boilerplate; the per-role workplace-type statement is the \
  real, governing signal here, not the company-wide language.

UNCERTAIN (priority 3) — ALLOWED, but the NARROWEST tier: use this ONLY \
when the posting is truly, truly without ANY geographic restriction \
signal AND does not meet the MATCH_AFRICA multi-region bar above either. \
This is not a safe default for "broad but I'm not sure how broad" — a \
posting naming 2+ business regions together is MATCH_AFRICA (see case 4 \
above), never UNCERTAIN, no matter how the regions are combined:
- Location is blank, N/A, unspecified, or otherwise absent, and the JD does \
  not reveal a restriction
- Location is simply "Remote" with no geographic qualifier and the JD does \
  not reveal a restriction
- Ambiguous language remains after reading the entire posting, but there is \
  no concrete country/region restriction AND no multi-region/EMEA/Africa/ \
  global evidence either

IMPORTANT: When there is no description or no clear signal, say \
UNCERTAIN. Do NOT default to MATCH_GLOBAL or MATCH_AFRICA — only use \
those when you see real positive evidence, per the definitions above. \
But do NOT default to UNCERTAIN either when a posting clearly meets one \
of the four MATCH_AFRICA cases (Africa continent, bare EMEA, EMEA + \
other regions, or 2+ regions without EMEA) — that specific, nameable \
evidence is exactly what MATCH_AFRICA is for, not a reason to hedge. \
When genuinely in doubt after checking all four MATCH_AFRICA cases and \
the NO_MATCH list above, then say UNCERTAIN.

Respond ONLY with lines like:
1 MATCH_GLOBAL
2 MATCH_AFRICA
3 NO_MATCH
4 UNCERTAIN"""


def _classify_location_batch(batch_jobs: list[dict], provider: dict, client,
                              force: bool = False) -> tuple[list[str], bool]:
    """Classify a single batch of jobs by location using a specific provider.

    force=True is threaded straight through to _ai_call (see its own
    docstring) — only ai_classify_locations' last-chance retry ever passes
    this.

    Descriptions are sent IN FULL — no per-job truncation happens anywhere
    in this pipeline any more (see _assign_jobs_by_desc_length's ROUND 7
    note: a job is routed to whichever provider's max_batch_chars can hold
    its full description, never cut to fit). No further per-batch slicing.
    Previously this re-truncated every job's description to an EVEN SPLIT of
    max_user_chars across the whole batch (e.g. a full-size batch could cut
    each job down to ~4K chars regardless of how short the batch's other
    descriptions were), which silently chopped real JDs mid-sentence even
    when the batch as a whole was nowhere near max_user_chars — exactly the
    kind of truncation that can hide the eligibility language the AI is
    being asked to find. _build_dynamic_batches already guarantees the
    batch's TOTAL character count stays under the provider's real budget
    (max_batch_chars), so no additional re-slicing is needed here.

    Returns (labels, call_ok). call_ok is False only when the underlying
    API call itself failed (see _ai_call) — every job in the batch then
    defaults to 'uncertain' for a reason that has nothing to do with the
    job itself. This is tracked separately from a genuine model-returned
    UNCERTAIN verdict (call_ok=True) so ai_classify_locations' summary can
    tell "the AI looked and couldn't tell" apart from "the AI never
    actually got called" — the same bare 'uncertain' label meant either
    one before this distinction existed, making a low classification
    count impossible to diagnose from the log alone.
    """
    numbered_lines = []
    for j, job in enumerate(batch_jobs):
        desc = job.get("description_snippet", "")
        desc_note = desc if desc else "[No description available]"
        numbered_lines.append(
            f"{j+1}. Title: {job['title']} | Company: {job.get('company', 'Unknown')} | "
            f"Location: {job.get('location', 'Remote')} | "
            f"Description: {desc_note}"
        )
    user_msg = f"Classify these {len(batch_jobs)} jobs:\n{chr(10).join(numbered_lines)}"

    # Output tokens must scale with batch size — one response line per job
    # (e.g. "47 NO_MATCH"). A fixed cap here silently truncates the response
    # once a batch has more jobs than the cap can cover, and every job past
    # the cutoff keeps its default "uncertain" label. ~8 tokens/line + buffer.
    max_tokens = max(1500, len(batch_jobs) * 8 + 200)
    text = _ai_call(provider, client, LOCATION_SYSTEM_PROMPT, user_msg, max_tokens=max_tokens, force=force)

    batch_results = ["uncertain"] * len(batch_jobs)
    if text is None:
        # 2026-09 final circuit-breaker logging fix: once a provider is
        # known dead, this is a reroute event, not a new AI failure. Do not
        # emit one warning per already-doomed batch.
        if _provider_is_exhausted(provider["name"]):
            log.debug(f"Location batch for {provider['name']} abandoned after provider was exhausted; "
                      f"the batch is being rerouted")
        else:
            log.warning(f"AI location classification failed ({provider['name']}) "
                        f"for batch of {len(batch_jobs)}, rerouting to another provider")
        # 2026-09 (explicit user fix — real production log evidence): this
        # used to say "...keeping as uncertain", which is simply false in
        # the common case — ai_classify_locations' cascade (see its own
        # docstring) reroutes this exact batch to another untried provider
        # moments later, and only writes a final 'uncertain' if EVERY
        # provider has genuinely been tried and failed. Matches
        # _classify_role_batch's already-correct wording for the identical
        # situation on the role side.
        return batch_results, False

    for line in text.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) >= 1:
            try:
                idx = int(parts[0].rstrip(".")) - 1
            except ValueError:
                continue
            if 0 <= idx < len(batch_jobs):
                label = parts[1].upper() if len(parts) > 1 else ""
                # Check longest/most-specific prefixes first — "NO_MATCH" and
                # "MATCH_AFRICA" both start with characters "MATCH" is also a
                # prefix of, so order matters here.
                if label.startswith("NO_MATCH"):
                    batch_results[idx] = "no_match"
                elif label.startswith("MATCH_AFRICA"):
                    batch_results[idx] = "match_africa"
                elif label.startswith("MATCH_GLOBAL"):
                    batch_results[idx] = "match_global"
                elif label.startswith("MATCH"):
                    # Model didn't use a category suffix — treat as Global,
                    # matching this prompt's pre-2026-08 behavior.
                    batch_results[idx] = "match_global"
                elif label.startswith("UNCERTAIN"):
                    batch_results[idx] = "uncertain"

    return batch_results, True


def _dynamic_job_cap(jobs: list[dict], provider_name: str | None = None) -> int:
    """5-10 jobs per batch by default (2026-09, raised from the earlier 5-7
    range per explicit user request: a real run classifying 201 jobs
    produced 82 separate batches/API calls — "that eats into requests and
    calls per day" — because the OLD ceiling of 7 for short-description
    jobs was needlessly tight; there was never a confirmed failure mode at
    8-10 jobs/batch for short descriptions, only at much larger flat
    batches (100+, see MAX_JOBS_PER_BATCH's history below) or
    long-description batches specifically. The floor stays at 5 for
    long-JD batches — that's the tier the original hallucination fix
    actually targeted (a cheap model losing attention across many FULL job
    descriptions in one call), and the user's own instruction here
    explicitly kept "5 minimum for long JDs."

    Scaled down toward the floor as the jobs being batched have longer
    descriptions — more text per job in one call means less of the
    model's attention per job. Based on the AVERAGE description length
    across the jobs being batched, since the cap applies to the batch as
    a whole, not any single job.

    2026-09 ROUND 7: provider-aware. NVIDIA raised to a higher 15-40 tier
    per explicit user instruction ("increase max with caution... never
    truncate a job") — its real context window (1,000,000 tokens) is
    orders of magnitude past whatever this cap was ever actually
    protecting against, so a meaningfully higher ceiling still leaves a
    same-shaped "fewer jobs per batch as descriptions get longer" curve
    (still guarding against attention dilution, just at a higher altitude)
    rather than removing the cap outright. Groq and OpenAI keep the
    original 5-10 tiers: Groq's real ceiling is its char budget anyway
    (this job-count cap rarely binds there before max_batch_chars does),
    and OpenAI's gpt-4.1-nano benchmarks as the weakest reasoner of the
    three models in active use here, so its cap is left unchanged rather
    than raised alongside NVIDIA's.

    NOTE: this caps job COUNT per batch, but each provider's own
    max_batch_chars (config.py) is a separate, independent ceiling that's
    checked first in _build_dynamic_batches — Groq's max_batch_chars=6,000
    (~1,500 tokens, sized to fit its real 8K-tokens-per-minute quota) will
    still force smaller batches than this job-count cap allows whenever
    descriptions aren't trivially short, REGARDLESS of raising this cap
    further. That's Groq's genuine rate-limit ceiling, not an oversight.
    """
    if provider_name == "nvidia":
        if not jobs:
            return 40
        total = sum(len(j.get("description_snippet") or "") for j in jobs)
        avg = total / len(jobs)
        if avg <= 2_000:
            return 40
        if avg <= 8_000:
            return 25
        return 15
    if not jobs:
        return 10
    total = sum(len(j.get("description_snippet") or "") for j in jobs)
    avg = total / len(jobs)
    if avg <= 2_000:
        return 10
    if avg <= 8_000:
        return 7
    return 5


def _build_dynamic_batches(jobs: list[dict], max_batch_chars: int,
                            provider_name: str | None = None) -> list[tuple[int, list[dict]]]:
    """Build batches dynamically based on description length.

    Char-budget driven, BUT also capped by job count. Many jobs have no
    description ("[No description available]" is only ~30 chars), so a
    pure char-budget batch can silently balloon to hundreds/thousands of
    jobs. The model's response is one line per job, and output tokens are
    scaled to job count (see _classify_location_batch) — so job count,
    not character count, is what actually bounds a safely-sized response.

    2026-09 ROUND 7 (explicit user instruction: "never ever truncate a
    job"): this no longer re-truncates any job's description. A job's full
    text is always used — _assign_jobs_by_desc_length (the caller) already
    guarantees nothing longer than a provider's own max_batch_chars is
    routed to that provider in the first place, falling back to whichever
    configured provider has the LARGEST budget for the rare case where a
    single job's own description exceeds every provider's per-request
    budget on its own. That means this function only ever sees jobs that
    already fit — a job here that alone exceeds max_batch_chars becomes a
    solo, over-budget batch rather than being cut short; that's the
    intended trade-off (send the whole job, even if the one request runs a
    little over the nominal budget) over silently hiding part of a JD's
    eligibility/restriction language from the classifier.
    """
    OVERHEAD_PER_JOB = 120
    # 120 -> 10 -> 5-7 dynamic (2026-09, explicit user request): a batch of
    # up to 120 jobs in one bulk "one-line-verdict-per-job" call is exactly
    # the shape that let cheap/small models cut corners — this is the same
    # call shape as the confirmed live failure where gpt-4.1-nano
    # hallucinated a match_global verdict with zero supporting text
    # anywhere in the job (see classifier.py's "Post-AI safety net" section
    # in ai_classify_locations for the real posting this closes). First
    # reduced to a flat 10, now made dynamic (5-7, or 15-40 for NVIDIA —
    # see _dynamic_job_cap) so long-description batches get even more of
    # the model's attention per job than a flat cap would give them.
    # Deliberately NOT applied to role classification (_build_role_batches,
    # a separate function) — role verdicts are just a short title, not a
    # full JD, so the same bulk-call risk doesn't apply there; left
    # unchanged per explicit instruction.
    MAX_JOBS_PER_BATCH = _dynamic_job_cap(jobs, provider_name=provider_name)

    batches = []
    current_batch = []
    current_chars = 0
    start_idx = 0

    for i, job in enumerate(jobs):
        desc = job.get("description_snippet") or ""
        desc_len = len(desc)
        job_chars = desc_len + OVERHEAD_PER_JOB

        if current_batch and (
            current_chars + job_chars > max_batch_chars
            or len(current_batch) >= MAX_JOBS_PER_BATCH
        ):
            batches.append((start_idx, current_batch))
            start_idx = i
            current_batch = []
            current_chars = 0

        current_batch.append(job)
        current_chars += job_chars

    if current_batch:
        batches.append((start_idx, current_batch))

    return batches


def _assign_jobs_by_desc_length(indexed_jobs: list[tuple[int, dict]],
                                 providers: list[dict]) -> dict[str, list[tuple[int, dict]]]:
    """2026-09: length-aware provider assignment — see the fix comment at
    ai_classify_locations' call site for the full "55 jobs -> 27 batches"
    root-cause story. Gives every provider the same ~equal SHARE of jobs
    plain round robin would (off-by-one at most), but picks WHICH jobs
    each provider gets: the smallest-max_batch_chars providers get the
    shortest descriptions, so a tight per-batch char budget (e.g. Groq's
    6,000) can actually fit several jobs per batch instead of being
    starved to ~1 job/batch by an unlucky draw of long descriptions.
    Providers with effectively unlimited budgets (OpenAI/NVIDIA, several
    million chars) absorb whichever long-JD jobs land on them without any
    batching penalty either way.

    Used for both the initial assignment and each failover-cascade round
    (with `providers` narrowed to that round's still-viable candidates) —
    same reasoning applies at every stage: don't let a tight-budget
    provider get stuck with long jobs it can't batch efficiently.

    Returns {provider_name: [(orig_idx, job), ...]}, same shape the old
    round-robin dict produced, so no downstream code needed to change.

    2026-09 fix during testing: an earlier version of this function gave
    each provider a single CONTIGUOUS slice of the length-sorted job list
    (smallest-budget providers first). That mis-balanced providers that
    happen to share the SAME budget (e.g. groq-o and groq-c both at
    6,000) — the first one in sort order (a stable sort, so really just
    whichever came first in LOCATION_PROVIDERS) claimed the very
    shortest slice, leaving the second same-budget provider a
    noticeably-less-short slice purely from list position, not anything
    about its actual capacity. Confirmed live: 14 jobs each, but 3
    batches for one and 8 for the other. Replaced with a min-heap that
    always hands the next-shortest remaining job to whichever
    still-has-room provider currently has (lowest budget, fewest jobs
    assigned so far) — the second tiebreaker interleaves equal-budget
    providers evenly instead of giving one of them a whole contiguous
    block before the other gets a look in.

    2026-09 ROUND 7 (explicit user design: "any JD <= 6k characters is
    Groq-eligible; anything over that only goes to NVIDIA/OpenAI; if
    they're all under, split equally; never truncate a job"): a job is
    now only assignable to a provider whose OWN max_batch_chars is big
    enough to hold that job's full description alone. Since the heap
    always tries the SMALLEST-budget provider for a job first, this
    naturally means short jobs land on Groq first (as before) while a job
    too long for Groq's ~6,000-char budget skips straight past it to
    NVIDIA/OpenAI instead of being force-fit there — and a job that's
    short enough to fit everywhere still gets the same equal-share
    treatment as before. This is what actually GUARANTEES "never above
    6k to groq" and "never truncate" together: a job is either routed to
    a provider that can hold it whole, or (the one pathological
    fallback, e.g. a single description bigger than every configured
    provider's budget) handed to whichever provider has the LARGEST
    budget regardless of its current share, rather than dropped or cut.
    """
    assignments = {p["name"]: [] for p in providers}
    n = len(indexed_jobs)
    if n == 0 or not providers:
        return assignments

    # Shortest description first.
    order = sorted(range(n), key=lambda k: len(indexed_jobs[k][1].get("description_snippet") or ""))
    base, extra = divmod(n, len(providers))
    # Tighter-budget providers get the (slightly larger, if any) remainder
    # share too — they're the ones that most need every job in their share
    # to be short — same reasoning as before, just computed up front here.
    providers_by_budget_asc = sorted(providers, key=lambda p: p["max_batch_chars"])
    shares = {p["name"]: base + (1 if i < extra else 0)
              for i, p in enumerate(providers_by_budget_asc)}

    counts = {p["name"]: 0 for p in providers}
    # Heap entries: (max_batch_chars, assigned_count_so_far, tie-break, provider).
    # Smallest budget wins first; among equal budgets, whichever has been
    # assigned FEWEST jobs so far wins next — that's what interleaves
    # same-budget providers instead of giving one a whole block before the
    # other. tie-break (insertion order) only matters for the initial,
    # all-zero heap state so equal-everything providers still get a
    # deterministic, stable order.
    heap = [(p["max_batch_chars"], 0, i, p) for i, p in enumerate(providers)]
    heapq.heapify(heap)

    for k in order:
        orig_idx, job = indexed_jobs[k]
        job_len = len(job.get("description_snippet") or "")
        # Providers skipped for THIS job only (too small to hold it whole)
        # go back on the heap unchanged afterward — still eligible for a
        # shorter job later, unlike a share-full provider which is
        # permanently dropped below.
        skipped = []
        assigned_ok = False
        while heap:
            budget, assigned_so_far, tie, p = heapq.heappop(heap)
            if counts[p["name"]] >= shares[p["name"]]:
                continue  # share full — permanently dropped, same as before
            if job_len > p["max_batch_chars"]:
                skipped.append((budget, assigned_so_far, tie, p))
                continue
            assignments[p["name"]].append((orig_idx, job))
            counts[p["name"]] += 1
            assigned_ok = True
            if counts[p["name"]] < shares[p["name"]]:
                heapq.heappush(heap, (budget, counts[p["name"]], tie, p))
            break
        for entry in skipped:
            heapq.heappush(heap, entry)
        if not assigned_ok:
            # Every provider with room left in its share is too small for
            # this one job's own description — genuinely nowhere it fits
            # as a solo request under the normal share-based assignment.
            # Never drop it and never truncate it: hand it to whichever
            # CONFIGURED provider has the largest budget, share limit or
            # not (this is the rare case, e.g. one description far bigger
            # than usual — not the normal path for most jobs).
            biggest = max(providers, key=lambda pp: pp["max_batch_chars"])
            assignments[biggest["name"]].append((orig_idx, job))
            counts[biggest["name"]] = counts.get(biggest["name"], 0) + 1
    return assignments


def _get_location_client(provider: dict):
    client = _location_clients.get(provider["name"])
    if not client:
        try:
            client = _make_client(provider)
            _location_clients[provider["name"]] = client
        except Exception as e:
            log.error(f"Cannot create location client for {provider['name']}: {e}")
            return None
    return client


def ai_classify_locations(jobs: list[dict]) -> list[tuple[str, str | None]]:
    """
    Send ambiguous jobs (bare "Remote") to AI for location classification.
    Uses LOCATION_PROVIDERS (Gemini + OpenAI + NVIDIA) concurrently.

    Jobs are round-robin split across providers, batched per provider's
    context window, and all batches run concurrently.

    Cross-provider failover (2026-09 ROUND 5, explicit user request: "when
    a provider fails for the location phase, it should not be dropped at
    any cost ... routed to another provider ... only the LLM can finally
    say this is truly uncertain"): if a provider's batch fails outright
    (after its own MAX_RETRIES=3 attempts inside _ai_call, or its client
    can't be built), that batch's jobs are reassigned to a provider that
    hasn't yet been tried FOR THOSE SPECIFIC JOBS and retried — and this
    now CASCADES: if that next provider also fails, the same jobs are
    reassigned again to yet another untried provider, and so on, looping
    until either (a) a job succeeds on some provider, or (b) every entry
    in LOCATION_PROVIDERS has genuinely been tried and failed for that
    job. A job is NEVER given up on after just one failover round any
    more — it keeps cycling through whatever providers it hasn't already
    been sent to (per-job tracking, so the same job is never resent to a
    provider that already failed it) until the provider list for that job
    is exhausted. The only two ways a job ends up labeled 'uncertain' are
    therefore: (1) every provider was tried and every one of them failed
    on a technical level (exception, call_ok=False, no client) — this
    returns ('uncertain', None); or (2) some provider's call actually
    SUCCEEDED (call_ok=True) and the model itself genuinely verdicted
    "uncertain" — this returns ('uncertain', provider_name), i.e.
    provider_name is NOT None. Case (2) is the only "truly uncertain" the
    user's instruction refers to; case (1) is a plumbing failure, not a
    verdict, and callers must not treat the two as equivalent (see the
    provider_name is None note below, which still holds).

    Returns a list of (label, provider_name) tuples in the same order as
    `jobs` — label is one of 'match_global', 'match_africa', 'no_match',
    or 'uncertain'; provider_name is whichever LOCATION_PROVIDERS entry
    actually produced that label (None if EVERY provider in
    LOCATION_PROVIDERS was tried and failed for that job — see the
    cascade above; this is the only case where the label defaults to
    ('uncertain', None) rather than reflecting a genuine LLM verdict).

    2026-09: fixed to actually return tuples — crawl_i.py's
    filter_locations() and crawl_ii.py's _filter_locations() have both
    always unpacked this as `(label, provider_name)` (to track which
    provider classified each job, avoiding a separate round-robin
    re-derivation that could drift out of sync), but this function was
    still returning bare label strings, which crashed identically in
    both callers with "too many values to unpack (expected 2)" — a
    length-N string doesn't unpack into 2 values unless N happens to be
    2. Caught live via a production crawl_i.py run once the location
    filter actually sent unsure jobs to the AI stage.

    IMPORTANT for callers, re: `provider_name is None` (AI-classification-
    stage audit, 2026-09): this happens when EVERY entry in
    LOCATION_PROVIDERS failed/was rate-limited/was exhausted for this
    job (including every round of the cascade above) — see _mark_exhausted's
    circuit breaker just above, which, once a provider gives up even
    once in a run, marks it dead for calls for the REST of that run with
    zero further network hits. Under real production load (10+ concurrent
    ATS shards racing a handful of shared free-tier keys — Groq's real
    pool is only 8K TPM, shared with role classification too, see
    config.py), it's entirely plausible for one or more providers to trip
    this breaker within the first few batches of a shard's ~3 hour run,
    after which every remaining ambiguous job in that shard gets
    ('uncertain', None) for the rest of the run. A caller that treats
    `provider_name is None` as equivalent to "no_match" therefore risks
    silently discarding real "Remote" signals purely because of upstream
    rate-limiting, not because of anything about the job itself — this
    was confirmed live as the root cause of csm_roles (10,000-17,000/
    shard) collapsing to global_jobs of 11-75/shard (scan_reports,
    Supabase project mqkcmkwpfvpajzjrbdji), and is now fixed in both
    crawl_i.py's filter_locations() and crawl_ii.py's _filter_locations():
    a bare_remote job is kept at PRIORITY_UNSURE regardless of whether
    provider_name is None, since the job's own location field already
    carries a real signal that doesn't depend on the AI confirming it.
    (A BLANK location field is intentionally NOT covered by that fix —
    it's still held to the stricter "needs a real match_global/
    match_africa verdict" bar, since a blank field can't be told apart
    from a scraper extraction bug; see those functions' docstrings.)
    """
    if not jobs:
        return []

    configured_providers = list(LOCATION_PROVIDERS)
    providers = _available_providers(configured_providers)
    if not providers:
        log.warning("Location AI: all configured providers are currently unavailable; "
                    "no AI requests will be scheduled for this round")
        return [("uncertain", None)] * len(jobs)

    # 2026-09: surface missing providers up front. LOCATION_PROVIDERS
    # silently drops any provider whose API key env var isn't set (see
    # config.py's _make_provider) — running location classification on
    # fewer providers than expected isn't wrong, but it cuts both capacity
    # and failover coverage a lot, and previously the only sign of it was
    # the raw HTTP request log for whichever provider(s) were actually left
    # — nothing said the others were missing. This is the single most
    # likely explanation for "why did so few jobs get a real AI verdict
    # this run."
    # 2026-09 ROUND 4: "gemini" and "mistral" deliberately excluded — both
    # removed entirely (see config.py's module docstring), replaced by a
    # second independent Groq account ("groq-c"). If GROQ_API_KEY_C isn't
    # set yet, "groq-c" will show up in _missing below — that's expected
    # until the account owner adds that secret, not a bug.
    # 2026-09 ROUND 6: same _PROVIDER_FILTER-aware adjustment as
    # ai_classify_roles() above — deliberately excluding a provider via
    # the USE_OPENAI/USE_NVIDIA checkboxes isn't a "missing API key",
    # so it shouldn't produce a misleading "missing" warning.
    _known_location_providers = {"nvidia", "openai", "groq-o", "groq-c"}
    _active = {p["name"] for p in providers}
    _expected = (_known_location_providers & _PROVIDER_FILTER) if _PROVIDER_FILTER else _known_location_providers
    _missing = _expected - _active
    if _missing:
        log.warning(f"Location AI running with {len(_active)}/{len(_expected)} "
                    f"providers ({', '.join(sorted(_active)) or 'none'}) — missing "
                    f"{', '.join(sorted(_missing))} (no API key set). Lower "
                    f"throughput and less failover if one of the active "
                    f"providers struggles.")

    # ── Length-aware assignment (2026-09, real production bug: 55 jobs
    # were sharding into 27 batches — nearly 1 job/batch — when the
    # "5-10 jobs/batch" dynamic cap says that should have been ~6 at most).
    # Root cause: plain index-based round robin (`providers[i % N]`) hands
    # each provider an arbitrary MIX of short and long descriptions with
    # zero regard for that provider's own char budget. Groq's real
    # rate-limit-driven max_batch_chars is only 6,000 (see config.py) while
    # a real JD can run into the tens of thousands of characters (see
    # ats_scrapers.py's _snippet(), whose cap is a 500,000-char defensive
    # ceiling, not a normal operating size) — so
    # whenever a job with a real multi-thousand-char JD landed on Groq, its
    # batch was capped at 1 job long before _dynamic_job_cap's 5-10 job
    # ceiling ever mattered, while OpenAI/NVIDIA (3.2-3.3M char budgets)
    # sat far under their own job-count cap. Round robin was blind to this
    # — it split job COUNT evenly, not job SIZE vs. each provider's actual
    # capacity.
    #
    # Fix: sort jobs by description length and deal the SHORTEST
    # descriptions to the SMALLEST-budget providers first (still giving
    # every provider its normal ~equal share of jobs — this doesn't change
    # who gets how MANY jobs, only WHICH ones), so a tight-budget provider
    # like Groq gets jobs that can actually pack 5+ per batch instead of a
    # random draw that starves it into 1-job batches. Long-JD jobs land on
    # whichever providers can actually absorb a full batch of them.
    # Descriptions themselves are NOT truncated or altered by this — this
    # only changes provider assignment, so the "send full descriptions,
    # don't hide buried eligibility language" fix from earlier rounds is
    # untouched.
    provider_assignments = _assign_jobs_by_desc_length(list(enumerate(jobs)), providers)

    # ── Build batches per provider ──
    all_work = []  # (provider, client, [(orig_idx, job)...], batch_jobs)
    for p in providers:
        assigned = provider_assignments[p["name"]]
        if not assigned:
            continue
        client = _get_location_client(p)
        if not client:
            # Whole slice unusable from the start — route through the same
            # failure/failover path as a mid-run failure below.
            all_work.append((p, None, [idx for idx, _ in assigned], [job for _, job in assigned]))
            continue
        assigned_jobs = [job for _, job in assigned]
        assigned_indices = [idx for idx, _ in assigned]
        batches = _build_dynamic_batches(assigned_jobs, p["max_batch_chars"], provider_name=p["name"])
        for start_idx, batch in batches:
            batch_orig_indices = assigned_indices[start_idx:start_idx + len(batch)]
            all_work.append((p, client, batch_orig_indices, batch))

    provider_summary = ", ".join(
        f"{p['name']}:{len(provider_assignments[p['name']])}" for p in providers
    )
    log.info(f"Location classification: {len(jobs)} jobs → {len(all_work)} batches "
             f"across {len(providers)} providers ({provider_summary})")

    results: list[tuple[str, str | None]] = [("uncertain", None)] * len(jobs)
    failed_batches = []  # [(failed_provider_name, orig_indices, batch_jobs), ...]
    # Indices that ended up 'uncertain' because their batch's AI call never
    # actually succeeded (see _classify_location_batch's call_ok) — as
    # opposed to the model genuinely reading the job and saying UNCERTAIN.
    # Cleared whenever a later attempt (the failover round) succeeds for
    # that index, so this only reflects the FINAL outcome.
    no_ai_read: set[int] = set()

    def _run_round(work, force=False):
        # force=True (last-chance retry only, see below) is passed straight
        # through to _classify_location_batch/_ai_call so an already-
        # exhausted provider still gets one real network attempt.
        # 2026-09 ROUND 6 (explicit user request, real production log dump:
        # ~30 separate WARNING lines in the same second, one per failed
        # batch, e.g. "groq-c failed on a 1-job batch — reassigning to
        # openai, nvidia, groq-o" repeated over and over): a struggling
        # provider used to get one log.error() line PER BATCH exception in
        # this round — when a tight-budget provider like Groq gets a burst
        # of small batches (the exact scenario the length-aware assignment
        # fix above reduces but doesn't fully eliminate, e.g. if a provider
        # is ALREADY exhausted mid-round), that's one line per batch,
        # completely swamping the log for what is really just "this one
        # provider is having a bad round." Collapsed to ONE aggregated
        # line per provider per round, logged after the round finishes
        # rather than inline per batch — still names the actual error
        # (last one seen for that provider) and the batch/job counts, just
        # once instead of N times.
        batch_errors: dict[str, list] = {}  # pname -> [count, last_error_str]
        with ThreadPoolExecutor(max_workers=max(1, len(providers))) as pool:
            future_map = {}
            for provider, client, orig_indices, batch in work:
                if client is None:
                    failed_batches.append((provider["name"], orig_indices, batch))
                    no_ai_read.update(orig_indices)
                    continue
                f = pool.submit(_classify_location_batch, batch, provider, client, force)
                future_map[f] = (provider["name"], orig_indices, batch)

            for future in as_completed(future_map):
                pname, orig_indices, batch = future_map[future]
                try:
                    batch_results, call_ok = future.result()
                    if call_ok:
                        for j, label in enumerate(batch_results):
                            results[orig_indices[j]] = (label, pname)
                        no_ai_read.difference_update(orig_indices)
                    else:
                        # 2026-09 fix: this is the actual bug behind
                        # "keeps as uncertain instead of rerouting to
                        # another provider" — a call that FAILED (bad key,
                        # daily quota, exhausted retries — anything
                        # _ai_call itself gave up on, returning call_ok=
                        # False with no exception raised) used to just
                        # write the default 'uncertain' straight into
                        # results and move on. Only a raised Python
                        # exception (below) ever reached failed_batches,
                        # so a graceful-but-failed call NEVER went through
                        # the cross-provider failover a few lines down —
                        # confirmed live (Gemini hitting its daily quota
                        # kept landing jobs as bare 'uncertain' instead of
                        # being retried on OpenAI/NVIDIA). Route it through
                        # the identical failed_batches path an exception
                        # takes; results already defaults to
                        # ('uncertain', None) so nothing needs writing
                        # here — only the failover round (or, if that also
                        # fails, the final default) decides the outcome.
                        failed_batches.append((pname, orig_indices, batch))
                        no_ai_read.update(orig_indices)
                except Exception as e:
                    entry = batch_errors.setdefault(pname, [0, ""])
                    entry[0] += 1
                    entry[1] = str(e)
                    failed_batches.append((pname, orig_indices, batch))
                    no_ai_read.update(orig_indices)

        for pname, (count, last_err) in batch_errors.items():
            log.error(f"Location classification: {pname} failed on {count} "
                      f"batch(es) this round (last error: {last_err}) — "
                      f"rerouting to other providers")

    _run_round(all_work)

    # ── Failover cascade (2026-09 ROUND 5, explicit user request) ──────
    # A job must NEVER be dropped to 'uncertain' just because ONE provider
    # had a technical failure — it has to keep getting routed to whatever
    # providers it hasn't been tried on yet, cycling through the ENTIRE
    # LOCATION_PROVIDERS list if that's what it takes, and only actually
    # settle on ('uncertain', None) once literally every provider has been
    # tried and genuinely failed for that specific job. This replaces the
    # old "one extra round then give up" behavior, which silently
    # defaulted a job to 'uncertain' after exactly one failover retry even
    # though 2+ untried providers might still have been available (e.g. 4
    # configured providers, first one fails, failover retry lands on the
    # second one which is ALSO struggling — the job used to die there even
    # though a 3rd and 4th provider were sitting untried).
    #
    # `tried` tracks, per original job index, which provider names have
    # already been attempted for that job (starting with whichever
    # provider round 0 assigned it to) — this is what lets the loop keep
    # cascading a job through EVERY remaining provider without ever
    # resending it to one that already failed it. The loop terminates
    # naturally once no still-failing job has any untried, viable
    # candidate left (bounded by len(providers) rounds — tried only grows,
    # never shrinks, and providers is a finite list) — a `len(providers)`
    # round safety cap is kept anyway as defense in depth against a future
    # change accidentally breaking that invariant.
    tried: dict[int, set] = {}
    _cascade_round = 0
    while failed_batches and len(providers) > 1 and _cascade_round < len(providers):
        _cascade_round += 1
        failing = []  # [(orig_idx, job), ...] still needing a home this round
        for failed_pname, orig_indices, batch in failed_batches:
            for orig_idx, job in zip(orig_indices, batch):
                tried.setdefault(orig_idx, set()).add(failed_pname)
                failing.append((orig_idx, job))
        failed_batches = []

        # Pick the next candidate provider for each still-failing job:
        # anything NOT already tried for that job AND not already known
        # dead for the rest of this run (the pre-existing circuit breaker,
        # _mark_exhausted/_exhausted_providers_today — skipping those here
        # avoids burning a whole extra cascade round on a provider that's
        # guaranteed to short-circuit to None anyway). Round-robins across
        # each job's own remaining candidates via a shared counter so load
        # spreads across the survivors instead of piling onto one.
        assignments: dict[int, dict] = {}
        k = 0
        live_providers = _available_providers(providers)
        for orig_idx, job in failing:
            candidates = [
                p for p in live_providers
                if p["name"] not in tried[orig_idx]
            ]
            if not candidates:
                # Genuinely exhausted for THIS job — every provider has
                # now either been tried and failed, or was already known
                # dead. Leave it at the default ('uncertain', None); it's
                # already in no_ai_read from its earlier failure(s) above,
                # so nothing further to record. This is case (1) from the
                # docstring, never conflated with a real LLM verdict.
                continue
            assignments[orig_idx] = candidates[k % len(candidates)]
            k += 1

        if not assignments:
            break  # nothing left that has anywhere new to go

        by_provider: dict[str, list] = {}
        for orig_idx, job in failing:
            p = assignments.get(orig_idx)
            if p is None:
                continue
            by_provider.setdefault(p["name"], []).append((orig_idx, job))

        retry_work = []
        for pname, items in by_provider.items():
            p = next(pp for pp in providers if pp["name"] == pname)
            assigned_jobs = [job for _, job in items]
            assigned_indices = [idx for idx, _ in items]
            client = _get_location_client(p)
            if not client:
                retry_work.append((p, None, assigned_indices, assigned_jobs))
                continue
            for start_idx, sub_batch in _build_dynamic_batches(assigned_jobs, p["max_batch_chars"], provider_name=p["name"]):
                sub_orig_indices = assigned_indices[start_idx:start_idx + len(sub_batch)]
                retry_work.append((p, client, sub_orig_indices, sub_batch))

        log.info(
            f"Location classification cascade round {_cascade_round}: "
            f"retrying {sum(len(v) for v in by_provider.values())} still-"
            f"failing job(s) across {len(by_provider)} provider(s) "
            f"({', '.join(sorted(by_provider))})"
        )
        if retry_work:
            _run_round(retry_work)
        # Loop repeats: anything that failed again lands back in
        # failed_batches and gets picked up next iteration, still
        # excluding every provider already tried for that specific job.

    # ── Last-chance retry (2026-10, explicit user request: "if an LLM did
    # not see it, have it wait and then try one more time after which you
    # should discard it should it fail"). The cascade above only ever
    # reassigns a job to a provider it HASN'T tried yet, and skips any
    # provider _mark_exhausted already marked dead for the rest of this
    # run — so once every configured provider has given up on a job once
    # (most likely with few providers active, e.g. a run that only
    # enabled one of NVIDIA/OpenAI/Groq), that job lands on
    # ('uncertain', None) with zero further chance in this run, no matter
    # how transient the real cause was. This is the deliberate, bounded
    # exception: wait briefly, then force exactly ONE more real network
    # attempt per still-unreviewed job against every configured provider,
    # bypassing _mark_exhausted's breaker for this single attempt only
    # (see _ai_call's force= param). Whatever still comes back
    # ('uncertain', None) after this is genuinely never going to be
    # reviewed this run — callers (crawl_i.py/crawl_ii.py) now discard
    # those instead of keeping them under an "ai_unreviewed" clearance.
    if no_ai_read:
        retry_indices = sorted(no_ai_read)
        log.warning(f"Location classification: {len(retry_indices)} job(s) were never "
                    f"reviewed by any provider — waiting {_LAST_CHANCE_RETRY_WAIT_SECONDS}s "
                    f"for one final forced retry before they're discarded")
        time.sleep(_LAST_CHANCE_RETRY_WAIT_SECONDS)

        retry_jobs = [jobs[i] for i in retry_indices]
        retry_assignments = _assign_jobs_by_desc_length(list(enumerate(retry_jobs)), providers)
        last_chance_work = []
        for p in providers:
            assigned = retry_assignments[p["name"]]
            if not assigned:
                continue
            client = _get_location_client(p)
            if not client:
                continue
            assigned_jobs = [job for _, job in assigned]
            assigned_local_idx = [idx for idx, _ in assigned]
            for start_idx, batch in _build_dynamic_batches(assigned_jobs, p["max_batch_chars"], provider_name=p["name"]):
                batch_local_idx = assigned_local_idx[start_idx:start_idx + len(batch)]
                batch_orig_idx = [retry_indices[i] for i in batch_local_idx]
                last_chance_work.append((p, client, batch_orig_idx, batch))

        if last_chance_work:
            recovered_before = len(no_ai_read)
            _run_round(last_chance_work, force=True)
            recovered = recovered_before - len(no_ai_read)
            log.info(f"Location classification: last-chance retry recovered a real review "
                     f"for {recovered}/{len(retry_indices)} job(s); "
                     f"{len(no_ai_read)} still never reviewed — will be discarded")

    # ── FINAL AI AUTHORITY GATE ───────────────────────────────────────
    results = _apply_location_ai_authority_gate(jobs, results)

    # 2026-09: simplified summary. "Classified X/Y" previously conflated
    # two very different things under one 'uncertain' bucket: a job the
    # model actually read and couldn't decide on, vs. a job whose batch's
    # API call never went through at all (see no_ai_read above) — both
    # printed identically, so a bad run (e.g. 2 of 3 providers missing)
    # looked the same as a normal one full of genuinely ambiguous
    # postings. Uncertain jobs are NOT dropped either way — they still
    # go through, just at lower confidence — so the summary says that
    # plainly instead of leaving it to be inferred.
    labels = [label for label, _ in results]
    no_read = len(no_ai_read)
    genuinely_uncertain = labels.count("uncertain") - no_read
    log.info(f"Locations: {labels.count('match_global')} global, "
             f"{labels.count('match_africa')} Africa, "
             f"{labels.count('no_match')} excluded, "
             f"{genuinely_uncertain} uncertain (retained at priority 3)"
             + (f", {no_read} skipped — AI never reached them (see warnings above)"
                if no_read else ""))
    return results


# ── Visa Sponsorship Detection ──────────────────────────
#
# 2026-09: rewritten from a single "negation must sit immediately before
# the sponsor phrase" regex to a sentence-scoped detector, after a real
# false negative reached production: a live posting
# (harmonyworks.com/careers/customer-success-manager) said
#     "You must be authorized to work in the US; we're not able to
#      sponsor visas at this time."
# and was still classified as globally open. The old _VISA_NO_RE pattern
# was `(no|not|unable|...)\s*(provide\s*)?(visa\s*sponsor|...)` — it
# required the negation word to sit right before "sponsor" (at most one
# "provide" in between), so "not ABLE TO sponsor" — with "able to"
# wedged in the middle — never matched. Real postings phrase this an
# enormous number of ways ("not able to", "won't be able to", "aren't
# currently able to", "not in a position to", "unable to at this time",
# "don't currently offer sponsorship", "sponsorship isn't something we
# provide", ...) and a rigid adjacency regex will always be one phrasing
# behind the next one encountered in the wild.
#
# The new approach: split into sentences, and for each sentence that
# mentions the sponsorship/work-permit *topic* at all, check whether that
# SAME sentence also carries negation or unavailability language
# ANYWHERE in it (not glued to the topic word). "sponsor"/"sponsorship"
# is specific enough as a word (job postings essentially never use it in
# any other sense) that "topic + negation share a sentence" is a strong,
# low-false-positive signal — far more resilient to paraphrasing than
# trying to enumerate every possible negation-to-verb construction.

_SPONSOR_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\r?\n+")

# 2026-09 audit fix: a sentence is also split on coordinating conjunctions
# ("and"/"but"/"or") and semicolons into separate CLAUSES before the
# topic/negation check runs (see _sponsorship_sentence_has_negative_signal
# below for why). This mirrors the sentence-boundary reasoning above one
# level down: "topic + negation share a SENTENCE" was already looser than
# real adjacency, but a compound sentence routinely joins two entirely
# unrelated clauses ("We sponsor employee resource groups and do not
# discriminate against any protected class") where the negation belongs
# to the second clause, not the sponsorship one.
_SPONSOR_CLAUSE_SPLIT_RE = re.compile(r"\s*(?:,\s*(?:and|but|or)\s+|\s+(?:and|but)\s+|;\s*)\s*", re.I)

_SPONSOR_TOPIC_RE = re.compile(
    # 2026-10 (adversarial-coverage pass, real failing cases: "No
    # immigration support will be provided", "We cannot provide
    # immigration support"): "immigration support" is a direct synonym
    # for visa sponsorship in HR copy — real postings use it exactly
    # that way. Also added bare "visa sponsorship" (two-word) as a
    # topic — the single-word "sponsorship" alt below already covers
    # most cases, but keeping it explicit guards against a future
    # tightening of that alt accidentally dropping this phrasing.
    r"\bsponsor(?:ship|ed|ing|s)?\b|\bwork\s*permits?\b"
    r"|\bimmigration\s*(?:sponsorship|support)\b|\bvisa\s+sponsorship\b",
    re.I,
)

# 2026-09 audit fix: "sponsor" is NOT specific to visa/immigration
# sponsorship the way the module comment above originally assumed — real,
# ordinary job-posting boilerplate uses it for: the ERISA "Plan Sponsor"
# of a 401(k)/benefits plan, a company "sponsoring" employee resource
# groups / Pride / a conference / a charity / a meetup as a DEI or
# culture blurb, and "conference-sponsorship" professional-development
# stipend programs. None of these have anything to do with work-visa
# eligibility, but every one of them is exactly the kind of sentence that
# ALSO contains an unrelated negation word nearby (an EEO "does not
# discriminate" clause is near-universal, and pairs naturally with a DEI
# sponsor mention in the same sentence). Any _SPONSOR_TOPIC_RE hit whose
# immediate context matches this is not treated as the genuine topic —
# see _sponsorship_sentence_has_negative_signal.
_SPONSOR_NON_VISA_RE = re.compile(
    r"\bplan\s+sponsor\b"
    r"|\b(?:proud|official|corporate|event|title)\s+sponsor\b"
    r"|\bsponsor(?:s|ed|ing)?\s+(?:of\s+)?(?:the\s+|a\s+|an\s+)?(?:local\s+)?"
    r"(?:pride|parade|meet-?up|conference|hackathon|charity|non-?profit|scholarship|"
    r"employee\s+resource\s+groups?|erg\b)"
    r"|\bconference[\s-]?sponsorship\b",
    re.I,
)

_SPONSOR_NEGATION_RE = re.compile(
    r"\b(no|not|cannot|can\'t|can’t|won\'t|won’t|will\s*not|unable|never|"
    r"doesn\'t|does\s*not|don\'t|do\s*not|isn\'t|is\s*not|aren\'t|are\s*not|"
    r"n\'t)\b",
    re.I,
)

# 2026-09 BUG FIX (explicit user-commissioned top-to-bottom audit,
# confirmed via direct testing, not a guess): bare "without" used to be
# part of _SPONSOR_NEGATION_RE's blanket word list, treating ANY "without"
# anywhere in the same clause as "sponsorship" as proof sponsorship is
# being negated. But "without" overwhelmingly modifies something OTHER
# than sponsorship in real postings — "visa sponsorship without any
# restrictions," "we will sponsor your visa without hesitation," "full
# visa sponsorship, without exception, to all qualified applicants,"
# "sponsorship is available without any geographic limitation" — all of
# these are POSITIVE sponsorship statements that were being hard-rejected
# as if they said the opposite, purely because the clause also happened
# to contain the word "without" modifying "restrictions"/"hesitation"/
# "exception"/"limitation" instead. Fixed by removing the blanket
# "without" trigger and adding this much narrower, CONTEXT-SPECIFIC
# pattern instead: "without" only counts as a sponsorship negation when
# it's directly followed by the sponsorship/visa/work-permit noun itself
# (optionally through "a"/"any") -- "work without sponsorship," "without
# a visa," "without any work permit" -- which is how a genuine negative
# statement actually reads, and which the five false-positive phrasings
# above never do.
_SPONSOR_WITHOUT_TOPIC_RE = re.compile(
    r"\bwithout\s+(?:a\s+|any\s+)?(?:visa\s+|work\s+)?"
    r"(?:sponsorship|visa|work\s+permit|immigration\s+support)\b",
    re.I,
)

# Negation that shows up AFTER the topic word instead of before it
# ("sponsorship is not available", "visa sponsorship: unavailable").
_SPONSOR_UNAVAILABLE_RE = re.compile(
    r"\bunavailable\b|\bnot\s+(?:currently\s+|presently\s+)?(?:offered|provided|available|possible|"
    r"something\s+(?:we|the\s+company)\s+(?:can\s+)?(?:offer|provide|do))\b",
    re.I,
)

# 2026-09 BUG FIX (same audit pass, same reasoning as the "without" fix
# above — a negation-shaped WORD standing in for actual negation, instead
# of checking what it's actually negating): "We CAN'T WAIT to sponsor the
# right candidate's visa!" contains "can't," which _SPONSOR_NEGATION_RE
# matches, but "can't wait" is an idiom meaning "excited to," not a
# negation of the sponsorship that follows it. Stripped out of the clause
# before the negation check runs (see _sponsorship_sentence_has_negative_
# signal below) rather than added as yet another standalone word the
# negation regex has to avoid, since "can't" by itself is still a perfectly
# good, common, genuine negation word everywhere else.
_SPONSOR_IDIOM_EXCEPTION_RE = re.compile(r"\bcan\W?t\s+wait\b", re.I)

_VISA_YES_RE = re.compile(
    r"visa\s*sponsor|sponsor.*visa|relocation\s*(support|assist|package)"
    r"|work\s*permit\s*(support|assist|provid)"
    r"|immigration\s*(support|assist)"
    r"|we\s*(do|can|will)\s*(provide|offer)\s*(visa\s*)?sponsorship"
    r"|sponsorship\s*(is\s*)?(available|offered|provided)",
    re.I,
)


def _sponsorship_sentence_has_negative_signal(text: str) -> bool:
    """True if any sentence/CLAUSE in `text` mentions the sponsorship/
    work-permit topic (in a genuine visa/immigration sense — see
    _SPONSOR_NON_VISA_RE) AND carries negation or unavailability
    language somewhere in that same sentence/clause, in any order or
    distance apart.

    2026-09 audit fix: sentences are now also split into clauses on
    "and"/"but"/"or"/";" before the check runs, and a bare _SPONSOR_
    TOPIC_RE hit is discarded when its immediate surrounding text
    matches a known non-visa sense of "sponsor" (plan sponsor, event/
    conference/ERG sponsor, etc.). Without this, a routine compound EEO/
    DEI sentence like "We sponsor employee resource groups and do not
    discriminate against any protected class" or "As Plan Sponsor, the
    Company does not guarantee continuation of the 401(k) match" would
    hard-reject the job even though neither clause says anything about
    visa/work-authorization sponsorship — the same class of bug as the
    has_hard_country_specific_auth_signal fix above (a topic-adjacent
    word standing in for the thing that's actually disqualifying).
    """
    if not text:
        return False
    for sentence in _SPONSOR_SENTENCE_SPLIT_RE.split(text):
        for clause in _SPONSOR_CLAUSE_SPLIT_RE.split(sentence):
            if not clause or not clause.strip():
                continue
            # 2026-10 (adversarial-coverage pass, real failing case:
            # "Must be able to work without a visa"): _SPONSOR_WITHOUT_
            # TOPIC_RE is itself a complete negative-sponsorship pattern
            # ("without a visa"/"without any work permit") — the "without
            # X" phrasing carries both the negation AND the topic word in
            # one go, so requiring _SPONSOR_TOPIC_RE to also match
            # separately as a gate (which it doesn't for bare "visa" by
            # design, to avoid matching generic visa discussion) was
            # dropping legitimate "work without a visa" statements. A
            # direct hit on the "without <topic>" pattern is enough on
            # its own.
            if _SPONSOR_WITHOUT_TOPIC_RE.search(clause):
                return True
            has_genuine_topic = False
            for m in _SPONSOR_TOPIC_RE.finditer(clause):
                window = clause[max(0, m.start() - 20):m.end() + 30]
                if _SPONSOR_NON_VISA_RE.search(window):
                    continue
                has_genuine_topic = True
                break
            if not has_genuine_topic:
                continue
            # Strip idiom exceptions ("can't wait") before testing for a
            # negation word — see _SPONSOR_IDIOM_EXCEPTION_RE's module
            # comment: "can't" in "can't wait to sponsor your visa!" isn't
            # negating the sponsorship that follows it.
            negation_check_text = _SPONSOR_IDIOM_EXCEPTION_RE.sub(" ", clause)
            if (_SPONSOR_NEGATION_RE.search(negation_check_text)
                    or _SPONSOR_UNAVAILABLE_RE.search(clause)):
                return True
    return False


def detect_visa_sponsorship(job: dict) -> str:
    """Scan description + title for visa sponsorship signals.
    Returns 'yes', 'no', or 'unknown'."""
    text = (
        (job.get("description_snippet") or "")
        + " " + (job.get("title") or "")
        + " " + (job.get("location") or "")
    )
    if not text.strip():
        return "unknown"

    if _sponsorship_sentence_has_negative_signal(text):
        return "no"
    if _VISA_YES_RE.search(text):
        return "yes"
    return "unknown"


def has_hard_no_sponsorship_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does this job's title/
    description contain an explicit "we won't/can't sponsor" (or
    equivalent) statement? Used to force NO_MATCH in location
    classification regardless of what the location field says or what
    the AI stage might otherwise decide — a company-wide "we hire
    globally" claim doesn't change the fact that THIS specific posting
    told applicants it needs existing work authorization with no
    sponsorship. Running this before the AI stage also means these
    clear-cut cases never depend on the AI getting it right (or even
    running successfully) at all — see _keyword_classify_location_detail
    for where this is wired in, and the module comment above
    _sponsorship_sentence_has_negative_signal for the real false negative
    this closes."""
    text = (job.get("description_snippet") or "") + " " + (job.get("title") or "")
    return _sponsorship_sentence_has_negative_signal(text)


# ── Country-specific work-authorization hard override (2026-09) ──────
# Real cases this closes, both live JazzHR postings (confirmed via direct
# fetch of the actual posting, 2026-09):
#   1. americanincomelifeaokevinblomquist.applytojob.com/apply/he4qxTnPSB/...
#      said "Must be legally authorized to work in the United States." —
#      an AFFIRMATIVE authorization requirement with NO mention of the
#      word "sponsor" anywhere, so has_hard_no_sponsorship_signal (which
#      requires the sponsorship/work-permit TOPIC word to co-occur with
#      negation in the same sentence) never fires on it at all. This
#      posting was still classified location_priority=3 (kept, "unsure")
#      instead of being excluded outright.
#   2. Any JazzHR/other ATS posting whose application form has a
#      screening question like "Are you legally authorized to work in
#      the United States?" — ats_scrapers.py's enrich_application_
#      questions() already detects exactly these (via its own
#      _WORK_AUTH_RE) and appends them into description_snippet as
#      "Application Question: ..." lines specifically so classification
#      can see them, but nothing was actually checking for that marker
#      as a hard signal — it was only ever extra context handed to the
#      AI stage, which could (and did) still ignore it.
# A country-specific work-authorization requirement is a STRONGER and
# more literal signal than "no sponsorship" — plenty of real postings
# never mention sponsorship at all and simply state the authorization
# requirement as a plain fact. Per explicit product requirement: this
# must force the job out of consideration entirely (no_match), not just
# demote it to the "unsure" tier — a country-specific authorization
# question/statement should never even reach PRIORITY_UNSURE, let alone
# PRIORITY_GLOBAL.
# 2026-09: country list broadened from the original 8-entry US/UK/Canada/
# Australia/NZ/Ireland/Germany/EU set — this pipeline scrapes ~38 ATS
# platforms across a genuinely global set of employers, and the original
# list missed live-confirmed real postings restricted to other countries
# (e.g. a JumpCloud posting requiring "located in and authorized to work
# in India" had no country-list entry to match against at all — a
# pre-existing miss, independent of the phrasing-rigidity bug below).
_COUNTRY_AUTH_NAMES_RE_FRAGMENT = (
    # 2026-09 BUG FIX (explicit user report, real-world screening-question
    # phrasing: "Would you require sponsorship to work with us?"): the
    # original bare "u\.?s\.?a?\.?" made EVERY period and the "a" optional,
    # so its minimal possible match was just the two bare letters "us" --
    # which collided with the ordinary English pronoun "us" ("work with
    # us", "join us", "work for us"). Confirmed live: this sentence has no
    # country named anywhere, referentially or otherwise, yet several
    # sponsorship/auth checks were reading "us" as the country code and
    # hard-rejecting a job that should have stayed 'unsure' (benefit of
    # the doubt) on a bare/Remote location, or been correctly admitted on
    # a broad one. Fixed by requiring the bare, period-less 2-letter form
    # to be written in EXACT UPPERCASE ("US", never "us"/"Us"/"uS") via a
    # scoped case-sensitive sub-pattern -- real postings always write the
    # country abbreviation in caps, so this loses no real coverage, while
    # every period-containing variant ("U.S.", "U.S.A.") and the full
    # "USA"/"United States" forms stay case-insensitive as before, since
    # none of those collide with any ordinary English word in any casing.
    r"\b(?-i:US)\b|u\.s\.?a?\.?|usa|united\s+states(?:\s+of\s+america)?|u\.?k\.?|united\s+kingdom|"
    r"canada|australia|new\s+zealand|(?:republic\s+of\s+)?ireland|germany|"
    r"european\s+union|\beu\b|"
    r"india|philippines|nigeria|kenya|south\s+africa|singapore|mexico|brazil|"
    r"netherlands|france|spain|italy|sweden|norway|denmark|finland|poland|"
    r"portugal|switzerland|austria|belgium|japan|china|u\.?a\.?e\.?|"
    r"united\s+arab\s+emirates|egypt|ghana|"
    # 2026-09 NEW (2nd cross-LLM review, real postings: BlueVoyant's "Must
    # be authorized to work in the Republic of Ireland" — the ORIGINAL
    # "ireland" entry above never allowed the "Republic of" prefix real
    # postings actually use; Autopay's "100% remote for Mexico, Columbia,
    # Venezuela and Guatemala, Argentina, Honduras, DR, Brazil applicants";
    # Think Academy MY's "Remote Customer Service Representative (Malaysia
    # Based)"): broadened country coverage well beyond the original 8/30-
    # country lists, since this pipeline scrapes ~38 ATS platforms across a
    # genuinely global employer base and every prior list kept getting
    # caught out by a country nobody had added yet.
    r"colombia|venezuela|guatemala|argentina|honduras|dominican\s+republic|"
    r"malaysia|indonesia|vietnam|thailand|pakistan|chile|peru|ecuador|"
    r"costa\s+rica|panama|israel|turkey|ukraine|russia|romania|hungary|"
    r"czech\s+republic|greece|south\s+korea|taiwan|hong\s+kong|morocco|"
    # 2026-09 NEW (real postings surfaced by a cross-LLM review of live JD
    # links — see the module comment above _COUNTRY_BASED_RESTRICTION_RE
    # for the full evidence): CONTINENT/REGION names used the exact same
    # way a country name is in this project's postings — "remote within
    # Europe", "Account Manager - Europe", "Channel Account Manager,
    # EMEA" (HeroDevs), "(Senior) Account Manager - Europe" (OpenProject) —
    # none of these are "global/worldwide", but none is a single ISO
    # country either. Added here (not a separate fragment) since every
    # regex that consumes this fragment treats a hit the same way: "this
    # posting names a specific, non-global place" -> hard reject.
    #
    # 2026-09 ROUND 4 (explicit, urgent user correction): "emea" was
    # REMOVED from this line entirely. This project's own base location-
    # FIELD logic (see the "── 3. EMEA → match ONLY if no country/city
    # qualifier ──" block in _keyword_classify_location_detail) has ALWAYS
    # treated a bare "EMEA" with no further qualifier as ACCEPTED — the
    # exact same PRIORITY_AFRICA tier as the Africa continent, since "EMEA
    # (Europe/Middle East/Africa) includes Africa but is broader than
    # global." Putting "emea" in THIS fragment as well directly
    # contradicted that project-wide policy: a title like "Channel Account
    # Manager, EMEA" or description text like "authorized to work in EMEA"
    # was being hard-rejected by these overrides before ever reaching the
    # location-field logic that would have correctly accepted it. Per the
    # user, directly: "EMEA is fucking allowed... We accept... jobs hiring
    # in Africa as a continent and ones hiring in the EMEA region." Plain
    # "europe" (the continent, NOT the EMEA acronym) is deliberately KEPT
    # here — this project's base location-field logic does NOT give bare
    # "Europe" the same accepted treatment it gives Africa/EMEA/Global, so
    # "Account Manager - Europe" (OpenProject, still a real posting this
    # closes) is correctly still a hard reject.
    r"europe|apac|latam|asia[\s\-]?pacific|"
    # 2026-09 ROUND 2 (explicit user-provided taxonomy of restrictive
    # region names, not tied to one specific posting this time — a
    # structured brainstorm of phrasing families rather than individual
    # live JD evidence, unlike every entry above): the remaining common
    # region/bloc names this project's postings use the same way —
    # "North America only", "hire exclusively within APAC" (already
    # covered), "MENA only", "DACH", "Benelux", "Nordics", "ANZ"
    # (Australia/New Zealand shorthand).
    # 2026-09 ROUND 3 (explicit user correction): bare "africa" and
    # "sub-saharan africa" were REMOVED from this list — Africa already has
    # its own dedicated POSITIVE handling as continent-wide evidence
    # (PRIORITY_AFRICA, see the "── 2. Africa as a continent ──" block in
    # _keyword_classify_location_detail), and treating it as a restrictive
    # region name here directly contradicted that: a title/description
    # naming Africa (alone, or alongside other regions like "MENA, AMER,
    # Africa, EMEA, Latam" — the user's own example of an ACCEPTABLE
    # multi-region posting) was getting hard-rejected by this fragment
    # before ever reaching the location-field logic that would have
    # recognized it as continent-wide/global-ish evidence. See also
    # _has_multi_region_breadth below, which handles the general case of
    # 2+ DIFFERENT regions named together (not just Africa) the same way
    # 2+ different African countries already counts as continent evidence
    # rather than a single-country restriction.
    r"north\s+america|americas|amers?|mena|middle\s+east|"
    r"anz|dach|benelux|nordics?|"
    # 2026-09 ROUND 6 (explicit user-provided region-vocabulary taxonomy,
    # cross-checked against this file's existing region coverage — the
    # user's own summary: "AMER/AMERICAS and multi-region combinations
    # should remain allowed/uncertain rather than automatically rejected"
    # and explicitly "do NOT make [EMEA/Africa] restrictive" — both
    # already true of the existing code and unchanged here; every entry
    # below is a genuinely NEW single-region name, added the same way
    # every other entry in this fragment already works: restrictive when
    # named ALONE, but see _REGION_ONLY_WORDS_RE/_has_multi_region_breadth
    # below — 2+ of these (or these + an existing region) named together
    # is still broad multi-region evidence, not a restriction.
    #
    # Europe sub-regions — same treatment as the existing bare "europe"
    # entry above (restrictive alone; Europe itself was never given
    # Africa/EMEA's special accepted status).
    r"western\s+europe|eastern\s+europe|central\s+europe|southern\s+europe|"
    r"northern\s+europe|"
    # EU/EEA family — synonyms of the existing "european union"/bare "eu"
    # entries above.
    r"european\s+economic\s+area|eea|"
    # Gulf region.
    r"gulf\s+cooperation\s+council|gcc|gulf|"
    # 2026-09 JUDGMENT CALL (flagged for review): African SUB-regions —
    # NOT bare "Africa" or "Sub-Saharan Africa", both of which keep their
    # existing dedicated accepted/non-restrictive treatment (ROUND 3
    # above; the user's own message re-confirmed Sub-Saharan Africa's
    # "existing special treatment" rather than asking to change it). A
    # named SUB-region (West/East/Central/Southern/North Africa) is
    # treated as restrictive-if-alone instead, the same relationship
    # "North America" already has to the broader (non-restrictive-by-
    # name-alone) "Americas" — i.e. the continent-wide claim stays
    # accepted, but a specific sub-region within it is still a real
    # narrowing. Not explicitly confirmed by the user for this exact
    # sub-case; correct this mapping if that reading is wrong.
    r"southern\s+africa|west\s+africa|east\s+africa|central\s+africa|"
    r"north\s+africa|"
    # Asia sub-regions, plus bare "asia" itself (parallel to "europe" above
    # — no continent gets automatic accepted status except Africa/EMEA).
    # Ordered longest/most-specific first (south-east/southeast/south/
    # east/central asia) before the bare "asia" fallback, so a compound
    # sub-region name doesn't get shadowed by the generic single-word
    # alternative when this fragment is embedded with a leading \b (regex
    # alternation picks the FIRST successful alternative at a position,
    # not the longest) — same discipline this file's "americas" already
    # keeps ordered before "amer"/"amers" in _REGION_ONLY_WORDS_RE below.
    r"south[\s\-]?east\s+asia|south\s+asia|east\s+asia|central\s+asia|asia|"
    r"oceania|pacific|"
    r"central\s+america|south\s+america|caribbean|"
    r"cee|cis|"
    r"japac|apj|"
    # Compound country-pair groupings — treated as a single named place
    # (like any 2-country whitelist), not a "business region" for
    # _has_multi_region_breadth purposes, so deliberately NOT added to
    # _REGION_ONLY_WORDS_RE below.
    r"uk\s*(?:&|and)\s*ireland|british\s+isles"
)

# 2026-09 BUG FIX (explicit user report, real posting: International EOR's
# Greenhouse listing — "Are you legally authorized to work in London,
# England, United Kingdom?"): every "(?:in|within) (?:the)? <country>"
# construction built on _COUNTRY_AUTH_NAMES_RE_FRAGMENT throughout this
# file assumed the country name sits IMMEDIATELY after the preposition
# (modulo a bare "the"), which is true for "...in the United States" but
# false for the extremely common real-world shape where a city and/or
# subdivision name comes first and the country comes LAST: "London,
# England, United Kingdom", "Austin, Texas, United States", "Toronto,
# Ontario, Canada", "Sydney, New South Wales, Australia". Confirmed via a
# systematic test sweep (not a guess) that this gap affected the
# authorization/eligibility/work-rights family hardest — those
# alternatives have ZERO gap tolerance at all between the preposition and
# the country — while the sponsorship/permit/residency family
# (_RANK4_COUNTRY_TIED_RESTRICTION_RE) only accidentally survives a SHORT
# city name by sheer luck of its generic 30-character trailing window,
# and still fails on anything longer (a 2-segment "City, Region, " chain,
# or a long intervening aside).
#
# This fragment matches 0-2 "<word(s)>, " segments (each up to 4 words)
# that can sit between a preposition and the country name it's actually
# naming. Deliberately bounded on BOTH word count and segment count, and
# REQUIRES each segment to end in a literal comma immediately followed by
# either the next segment or the country itself — that's how a real place
# chain is written in practice, and essentially never how unrelated prose
# happens to read, so this doesn't meaningfully widen what these checks
# accept beyond genuine "city, subdivision, country" phrasing. Applied to
# the authorization (_COUNTRY_AUTH_RE), sponsorship/permit/residency
# (_RANK4_COUNTRY_TIED_RESTRICTION_RE), and residence-verb
# (_COUNTRY_BASED_RESTRICTION_RE) families — the three places a candidate-
# facing eligibility question or JD sentence names a country this way.
_PLACE_CHAIN_PREFIX_FRAGMENT = (
    r"(?:[A-Za-z][\w'.\-]*(?:\s+[A-Za-z][\w'.\-]*){0,3}\s*,\s*){0,2}"
)

# 2026-09 ROUND 3 (explicit user correction, quoted directly: "if it has
# multiple locations and africa thats good. Like say: MENA, AMER, Africa,
# EMEA, Latam. thats acceptable too."): naming 2+ DISTINCT business regions
# together is evidence of BROAD multi-region hiring, not a restriction to
# one place — the exact same principle this file already applies to 2+
# different African countries counting as continent-wide evidence rather
# than "based in one African country." Used below to suppress a region-name
# match that would otherwise fire on a list that actually proves the
# opposite of what a single region name implies. Deliberately does NOT
# include "africa" (never restrictive to begin with, see the comment above)
# or full country names (a list of several individual COUNTRIES, e.g. "USA,
# Canada, Mexico only," is still exactly the curated-whitelist restriction
# _COUNTRY_WHITELIST_PHRASE_RE/_COUNTRY_LIST_ONLY_RE exist to catch — this
# guard is specifically about BUSINESS-REGION names, not country lists).
_REGION_ONLY_WORDS_RE = re.compile(
    r"\b(?:europe|emea|apac|latam|asia[\s\-]?pacific|north\s+america|"
    r"americas|amer|amers|mena|middle\s+east|anz|dach|benelux|nordics?|"
    # 2026-09 ROUND 6: same new business-region set added to
    # _COUNTRY_AUTH_NAMES_RE_FRAGMENT above — kept in sync so "hiring in
    # Western Europe, Gulf, and CIS" (3 distinct new regions) or "EMEA,
    # Southeast Asia, and Caribbean" (mixing an existing + new regions)
    # both count as multi-region breadth. UK & Ireland/British Isles
    # deliberately excluded — see that fragment's own comment.
    r"western\s+europe|eastern\s+europe|central\s+europe|southern\s+europe|"
    r"northern\s+europe|european\s+economic\s+area|eea|"
    r"gulf\s+cooperation\s+council|gcc|gulf|"
    r"southern\s+africa|west\s+africa|east\s+africa|central\s+africa|"
    r"north\s+africa|"
    r"south[\s\-]?east\s+asia|south\s+asia|east\s+asia|central\s+asia|asia|"
    r"oceania|pacific|central\s+america|south\s+america|caribbean|"
    r"cee|cis|japac|apj)\b",
    re.I,
)


def _has_multi_region_breadth(text: str) -> bool:
    """True if `text` names 2+ DISTINCT business regions (see
    _REGION_ONLY_WORDS_RE's module comment) — evidence of broad multi-
    region reach that should NOT be treated as a single-region
    restriction."""
    hits = {m.group(0).lower() for m in _REGION_ONLY_WORDS_RE.finditer(text or "")}
    return len(hits) >= 2
# 2026-09 FIX (live-sample validation, real postings): the ORIGINAL regex
# required "authorized...to work in <country>" with no words allowed in
# between, so it missed extremely common real phrasing variants —
# Calendly: "authorized to work LAWFULLY in the United States"; Tines/
# Renaissance Learning: "authorized to work in the United States FOR ANY
# EMPLOYER" reordered as "must be authorized to work for any employer in
# the U.S."; Upbound: a screening question literally titled "Is your work
# authorization a U.S. Citizen?" (a citizenship phrasing this regex never
# covered at all, in any version). Confirmed live via WebFetch against
# each posting's real application-question/description text. Fixed by (1)
# allowing "lawfully"/"for any employer" as optional interposed words
# between "authorized to work" and "in <country>", in either order, and
# (2) adding a standalone "<country-adjective> citizen(ship)" pattern.
_COUNTRY_AUTH_RE = re.compile(
    # 2026-10 BUG FIX (explicit user report, real live posting:
    # https://job-boards.greenhouse.io/insurityllc/jobs/4297878009 — Q1:
    # "Are you legally eligible for employment in the United States?"
    # exactly the kind of textbook US-only question this check exists to
    # catch, yet it slipped through entirely). Root cause: every "to\s+
    # work" alt below hardcoded the preposition "work" after the
    # eligible/authorized verb — "eligible FOR EMPLOYMENT in" (a direct
    # and common synonym for "eligible TO WORK in") never matched any
    # alt. Replaced the fixed "to\s+work" bridge with a (?:to\s+work|
    # for\s+(?:employment|hire)|to\s+be\s+(?:employed|hired)|to\s+accept
    # \s+(?:employment|a\s+position)|to\s+hold\s+employment) group that
    # covers every real-world equivalent of "work in <country>" without
    # loosening the surrounding anchor (still requires the eligible/
    # authorized/entitled/permitted/allowed verb AND an "in <country>"
    # tail; this isn't a bare "employment in" catch-all).
    r"\b(?:must\s+(?:be|have|currently\s+be)\s+)?(?:currently\s+)?"
    r"(?:legally\s+|lawfully\s+)?(?:authorized|authorised|eligible|entitled|permitted|allowed|able)\s+"
    r"(?:to\s+(?:work|be\s+(?:employed|hired)|accept\s+(?:employment|a\s+position)|hold\s+employment)"
    r"|for\s+(?:employment|hire))\s+"
    r"(?:lawfully\s+)?(?:for\s+(?:any|an)\s+employer\s+)?(?:lawfully\s+)?(?:in|within)\s+"
    r"(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\b(?:us|u\.s\.|uk|u\.k\.|canadian|australian|british|indian|german|irish)\s+work\s+authoriz"
    r"|\bwork\s+authoriz\w*\s+(?:status\s+)?(?:in|for)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    # 2026-09 NEW (real posting: Aptive's "Program Manager," iCIMS —
    # "Legal authorization to work in the U.S." — a NOUN-phrase statement,
    # not the "authorized to work in" VERB-phrase question every other
    # alternative above expects). Confirmed via a cross-LLM review of live
    # JD text; this exact phrasing never matched any prior alternative.
    r"|\bauthoriz(?:ation|ations)\s+to\s+work\s+(?:in|within)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    # 2026-10: noun-phrase "eligibility/eligible/authorization for
    # employment in <country>" variant — same shape as the "authorization
    # to work in" alt just above, but with the "for employment"
    # preposition the Insurity-style questions use. Keeps the structure
    # of the alt above (noun-phrase + "in <country>" tail), so no bare
    # "employment in Germany" sentence matches it on its own.
    r"|\b(?:eligibility|eligible|authoriz(?:ation|ations)|authoriz(?:ed|able))\s+for\s+employment\s+(?:in|within)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bmust\s+(?:currently\s+)?reside\s+in\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bright\s+to\s+work\s+in\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    # 2026-09 NEW (explicit user report, real posting: Ofload's Workable
    # screening question "Do you have full unrestricted work rights for
    # Australia?"): every alternative above requires singular "right"
    # (never plural "rights") and "in"/"within" (never "for") — this real,
    # common phrasing uses BOTH the plural and "for", and matched nothing.
    r"|\bwork\s+rights?\s+(?:in|for)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bmust\s+have\s+(?:a\s+)?valid\s+(?:us|u\.s\.|uk|canadian|australian|indian)\s+work\s+(?:visa|permit)\b"
    # 2026-10 NEW (same Insurity posting, Q2): "Do you now or will you
    # in the future require [COMPANY] to petition for, sponsor, or
    # transfer a nonimmigrant or immigrant employment visa in order for
    # you to work in the United States?" — a direct, pre-filled-with-
    # country sponsorship-requirement question. Nothing matched this
    # before: has_hard_no_sponsorship_signal requires sponsor+negation
    # in the same clause (none here — "require sponsor" is affirmative,
    # not negated), and every _COUNTRY_AUTH_RE alt above anchors on
    # eligible/authorized/entitled/permitted/allowed/rights/reside/
    # citizen — none of which this phrasing uses. New alt: a question
    # that pairs "require/need [company?] to [petition for/sponsor/
    # transfer] [a?] visa/sponsorship/work permit [in order] to work in
    # <country>" is itself a country-specific authorization signal —
    # the question wouldn't be worded this way for a role that would
    # consider applicants without that country's work authorization.
    # Deliberately scoped to "to work in <country>" tail so a generic
    # country-agnostic "do you require sponsorship for employment visa
    # status" (no country named) does NOT fire this — same no-country-
    # named-in-the-question-itself discipline every other alt uses.
    r"|\brequire\s+(?:[A-Za-z][\w\s'.\-]*?\s+)?(?:to\s+)?"
    r"(?:petition(?:\s+for)?|sponsor|transfer)\s+"
    r"(?:[^.?\n]*?)\b(?:visa|sponsorship|work\s+permit|immigration\s+support)\b"
    r"(?:[^.?\n]*?)\bto\s+work\s+(?:in|within)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    # Simpler synonym family: "require/need visa sponsorship to work in <country>".
    r"|\b(?:require|need|seek)\s+(?:[a-z\s]*\s+)?(?:visa\s+sponsorship|sponsorship|a\s+visa|a\s+work\s+permit|immigration\s+support)\s+"
    r"(?:[^.?\n]*?)\bto\s+work\s+(?:in|within)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    # 2026-09 BUG FIX (explicit user report, real posting: Weploy's
    # Greenhouse application question "Please specify whether you are an
    # AU or NZ citizen or Permanent Resident:"). Two gaps the existing
    # demonym list below never covered: (1) bare 2-letter country CODES
    # ("AU", "NZ") rather than full demonym words ("Australian"), and no
    # "New Zealand(er)" demonym at all; (2) "Permanent Resident" as an
    # equally-restrictive noun alongside "citizen" -- a residency-status
    # question, not just a citizenship one. The regex is non-anchored, so
    # "AU or NZ citizen" already matches once "nz" is in the list (the
    # word "citizen" sits directly after "NZ"); still adding "au" for the
    # same phrasing in the other order ("NZ or AU citizen").
    r"|\b(?:au|nz|new\s+zealand(?:er)?)\s+(?:citizen(?:ship)?|permanent\s+resident)\b"
    r"|\b(?:u\.?s\.?a?\.?|united\s+states|u\.?k\.?|united\s+kingdom|canadian|australian|british|irish|german|indian)\s+"
    r"(?:citizen(?:ship)?|permanent\s+resident)\b"
    r"|\bpermanent\s+resident\s+of\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b",
    re.I,
)

# 2026-09 NEW (explicit user report, real posting: Twilio's Greenhouse
# listing, "Senior Manager, Customer Success" — location field named a
# specific country, Australia, but the screening questions read "What is
# the source of your right to work where this role is listed?" and "Are
# you legally authorized to work in the country in which this role is
# located?" — neither names a country IN THE QUESTION ITSELF, so
# _COUNTRY_AUTH_RE above never matches either one). These are a distinct
# phrasing family: instead of naming a country directly, they REFER to
# wherever the role's own location/listing already says — which is every
# bit as real a restriction as a directly-named country WHEN this job's
# location is already known to be one specific, narrow place (that's the
# whole point of a question phrased this way: "you already told us where
# this role is, so tell us if you can legally work there"). Explicit user
# instruction: "regex/classifier should ask if it already named a
# location... if a location is named and it asks these kinds of
# questions... then it should not be let in." See
# _rank4_has_country_tied_restrictive_question (Rank 4 always has a known
# place by definition) and has_referential_auth_question_with_named_place_
# signal (the universal version, for Rank 1/2/3a/3b, which separately
# checks whether a real place is already named) for the two call sites.
# 2026-10 BUG FIX (explicit user report, real posting: Ping Identity's
# Greenhouse "Manager, Customer Success", location "UK - Remote" -> admitted
# at Rank 4/4a, job-boards.greenhouse.io/pingidentity/jobs/8807392002 — its
# application questions were "Will you now or in the future require
# sponsorship to work in the country where this job is located?" and "Upon
# hire, can you provide verification of your identity and legal right to
# work in the country where this job is located?"). Neither matched any
# alternative below: the first-listed alternative only handled "right to
# work WHERE this role is located" (no "in the country" bridge) and the
# second required the verb "authoriz*" specifically — so "legal right to
# work in the country where this job is located" and "require sponsorship
# to work in the country where this job is located" both slipped through.
# Rather than adding one more one-off alternative per phrasing, these two
# fragments split every referential question into its two real parts —
# (1) an authorization/sponsorship/right-to-work HEAD and (2) a PLACE TAIL
# that refers back to wherever the role itself is located/listed/based (or
# where "you are applying") — and the alternative that follows matches any
# head followed (within one sentence) by any tail. The same safety gates as
# every other alternative apply unchanged: both call sites only treat a hit
# as disqualifying when the job's OWN location already resolved to one
# narrow place, and _referential_auth_hit() additionally skips a sentence
# that is only company-benefit framing ("we provide work permit support to
# help you work in the country where this role is located").
_REFERENTIAL_AUTH_HEAD_FRAGMENT = (
    r"(?:(?:legal(?:ly)?\s+|lawful(?:ly)?\s+)?rights?\s+to\s+work"
    r"|(?:legally\s+|lawfully\s+|currently\s+)?(?:authori[sz]ed|eligible|entitled|permitted|allowed|able|qualified)"
    r"\s+(?:to\s+(?:work|be\s+employed|be\s+hired|accept\s+employment|take\s+up\s+employment)|for\s+employment)"
    r"|work\s+(?:authori[sz]ation|permit|visa|rights?)"
    r"|employment\s+(?:authori[sz]ation|eligibility|visa)"
    r"|(?:require|requires|need|needs)\s+(?:(?:visa|work|employment|immigration)\s+)?(?:sponsorship|support)"
    r"|(?:require|requires|need|needs)\s+an?\s+(?:(?:work|employment)\s+)?(?:visa|permit)"
    r"|(?:citizenship|residen(?:cy|ce)|immigration)\s+status"
    r"|verif\w*\s+(?:of\s+)?(?:your\s+)?(?:identity|eligibility|work\s+authori[sz]ation))"
)
_REFERENTIAL_PLACE_TAIL_FRAGMENT = (
    r"(?:(?:in|within|at|for)\s+)?(?:the\s+)?"
    r"(?:(?:country|countries|location|jurisdiction|region|place|state|city)\s+)?"
    r"(?:(?:where|wherever|in\s+which|that)\s+(?:this\s+|the\s+|your\s+|our\s+)?"
    r"(?:role|job|position|opportunity|vacancy|posting|work|employment|office)\s+"
    r"(?:is\s+|will\s+be\s+|would\s+be\s+|are\s+)?(?:located|based|listed|situated|posted|advertised|performed|held)"
    r"|(?:where|in\s+which)\s+you\s+(?:are|will\s+be|would\s+be|'re)\s+(?:applying|working|based|located|employed|hired)"
    r"|of\s+(?:this|the)\s+(?:role|job|position|opportunity)(?:'s\s+location)?)\b"
)

_REFERENTIAL_AUTH_QUESTION_RE = re.compile(
    r"\b" + _REFERENTIAL_AUTH_HEAD_FRAGMENT + r"[^.!?\n]{0,80}?\b" + _REFERENTIAL_PLACE_TAIL_FRAGMENT
    + r"|\b(?:right|eligib\w*|authoriz(?:ed|ation)?|permitted?)\s+to\s+work\s+"
    r"(?:where|wherever)\s+(?:this\s+)?(?:role|position|job)\s+(?:is\s+)?"
    r"(?:listed|located|based)\b"
    r"|\bauthoriz\w*\s+to\s+work\s+in\s+the\s+countr(?:y|ies)\s+(?:in\s+which|where)\s+"
    r"(?:this\s+)?(?:role|position|job)\s+(?:is\s+)?(?:located|based|listed)\b"
    r"|\bsource\s+of\s+your\s+right\s+to\s+work\s+where\s+(?:this\s+)?(?:role|position|job)\s+"
    r"(?:is\s+)?(?:listed|located|based)\b"
    r"|\bwork\s+(?:rights?|authoriz\w*)\s+for\s+(?:the\s+|this\s+)?(?:role|position|job)'?s?\s+"
    r"(?:location|country)\b"
    r"|\beligib(?:le|ility)\s+to\s+work\s+(?:in|at)\s+(?:the\s+)?(?:location|country)\s+"
    r"(?:of|for)\s+(?:this\s+)?(?:role|position|job)\b"
    # 2026-09 BUG FIX (explicit user report, real posting: ShipBob's
    # Greenhouse listing, location "Sydney, New South Wales, Australia" —
    # application question just "What's your citizenship / employment
    # eligibility?", no "where this role is located" wording at all, so
    # none of the alternatives above ever matched it). A question this
    # terse never explicitly refers to the role's own location the way
    # every pattern above requires -- but the underlying intent is
    # identical: a company asking a Sydney applicant their "citizenship /
    # employment eligibility" obviously means "eligible to work in
    # Australia," same as if it had spelled that out. Added a genuinely
    # BARE citizenship/work-authorization/eligibility status question
    # (no place named anywhere, not even referentially) as its own
    # alternative -- the SAME "is a real, specific, non-broad place
    # already named" gate every caller of this regex already applies
    # (has_referential_auth_question_with_named_place_signal /
    # _rank4_has_country_tied_restrictive_question) is what keeps this
    # from firing on a genuinely location-agnostic job; it only ever
    # disqualifies when the job's OWN location field already resolved to
    # one narrow place, exactly like the existing alternatives above.
    # Requires an interrogative/imperative question framing (what's/what
    # is/please specify/confirm/indicate/are you/do you have) so this
    # doesn't also match plain DEI/company-values prose that merely
    # mentions "citizenship" in passing.
    r"|\b(?:what(?:'s|\s+is)|please\s+(?:specify|confirm|indicate|state))\s+your\s+"
    r"citizenship\s*(?:[/,]|\s+(?:or|and)\s+)?\s*(?:work\s+)?(?:employment\s+)?"
    r"(?:authoriz\w*|eligib\w*|status)\b"
    r"|\b(?:are\s+you|do\s+you\s+have)\b[^.!?\n]{0,30}\b(?:citizenship|citizenship\s+status|"
    r"work\s+authoriz\w*|employment\s+eligib\w*)\b"
    # 2026-09 BUG FIX (explicit user report: "would it accept a situation
    # where a company says USA as location but then asks: would you
    # require sponsorship to work with us. Here, USA is not mentioned in
    # the application questions"). Same referential gap as the bare-
    # citizenship alternative just above, but for a bare SPONSORSHIP
    # question instead -- "Would you require sponsorship to work with
    # us?" names no country anywhere, not even referentially ("where this
    # role is located"), it just refers to "us"/"here"/"our team" (the
    # employer itself). Confirmed via direct testing this was a real gap:
    # has_country_tied_sponsorship_permit_residency_signal/
    # _rank4_has_country_tied_restrictive_question both require the
    # country NAMED in the question text itself, with no referential
    # fallback at all -- so this question was falling through untouched
    # on a job whose location field already said e.g. "United States".
    # Same safety gate as every alternative above: both call sites
    # (has_referential_auth_question_with_named_place_signal /
    # _rank4_has_country_tied_restrictive_question) only treat a match
    # here as disqualifying when the job's OWN location already resolved
    # to one narrow, specific place -- a genuinely bare/blank/Remote/
    # broad location is unaffected, same benefit-of-the-doubt policy as
    # the bare citizenship question above.
    r"|\b(?:would|will|do|does)\s+(?:you|the\s+candidate|the\s+applicant)\b[^.!?\n]{0,60}"
    r"\b(?:require|need)\b[^.!?\n]{0,30}"
    r"(?:visa\s+)?sponsorship\b[^.!?\n]{0,60}\bto\s+(?:work\s+(?:with|for)|join)\s+"
    r"(?:us|here|this\s+(?:company|team|organization)|our\s+(?:company|team|organization))\b",
    re.I,
)

# 2026-09 NEW (real posting: OpenSesame's "Sales Operations Manager, Direct
# Sales", Greenhouse — job-boards.greenhouse.io/opensesame/jobs/8161867):
# location field was bare "Remote" (a real, honest signal), but the
# description's own "Location Requirements" section read: "This position
# can be based anywhere in the US." That's an explicit, unambiguous
# country-wide restriction stated by the company itself — nothing about
# work AUTHORIZATION (so _COUNTRY_AUTH_RE above, which only matches
# "authorized/eligible/entitled/permitted TO WORK in <country>" phrasing,
# never fires on it), and no enumerated state list either (so
# has_state_list_restriction_signal doesn't catch it). This is a distinct
# phrasing family — "based (anywhere) in <country>" / "must work from
# <country>" / "must be located in <country>" — describing where the
# CANDIDATE has to physically be, not what work authorization they must
# hold. Deliberately scanned sentence-by-sentence with a guard against
# "team/office/HQ/company is based in X" (describing the COMPANY's own
# location, not a candidate eligibility rule) — real postings often
# mention where a hiring manager's team sits without that being a
# restriction on where applicants may live.
# 2026-09 BROADENED (cross-LLM review of ~30 live JD links, dispatched
# specifically to find phrasing this project's regexes still missed — see
# the real postings quoted below): the ORIGINAL version of this regex only
# matched a MODAL-VERB-prefixed "based"/"located" ("must be based",
# "can be based", "is based") plus "must work from" — but real postings
# overwhelmingly phrase the SAME restriction as a bare screening QUESTION
# with no modal at all:
#   Lumivero:  "Do you currently reside in the US full-time?"
#   Infinx:    "Do you currently live in the US?"
#   WeVote:    "Are you currently located in the United States?"
#   Spinwheel: "Do you currently live in either the US or Canada?"
# None of these contain "based" or "must" — "reside"/"live"/"located" were
# entirely absent from the old verb list, so none of them matched. Fixed
# by broadening to every verb form real postings actually use (reside/
# residing/resides, live/living/lives, located, based) with NO modal-verb
# requirement — the verb immediately followed by "in"/"within" a named
# place is itself the signal, regardless of what (if anything) precedes
# it. Also added:
#  - "either" as an optional word between "in" and "the" (Spinwheel's
#    "in either the US or Canada" — the literal "the US" substring alone
#    is what matches, but "either" sits between "in" and "the" and would
#    otherwise break the match).
#  - a distinct "remote within/in <place>" alternative — GreenSlate's
#    "This role is remote within the United States" and Storm Ideas'
#    "This role is fully remote within Canada" name the place right after
#    "remote", not after a based/located/reside/live verb at all.
#  - "working from (anywhere in)? <place>", modal-free — Storm Ideas'
#    "Fully remote working from anywhere in Egypt!" has no "must".
# Deliberately NOT added: timezone phrases ("Pacific Time Zone", "US
# Eastern Time") are NOT treated as a place name — a cross-LLM review
# specifically flagged that Storm Ideas runs the EXACT SAME "Pacific-time-
# aligned" hours for both a Canada-restricted role and an Egypt-restricted
# role, so timezone alone proves nothing about required physical location
# and must stay a separate, unimplemented signal rather than being folded
# in here.
# 2026-09: full (not abbreviated) US state names, shared between the
# residence-restriction regex below and the title-suffix check further
# down — a single US state, spelled out in full, is exactly as reliable a
# "not global" signal as a named country (Ninth Brain: "currently reside
# in Michigan"; Ellevation: "must live in California"), and spelling it
# out avoids the false-positive risk a 2-letter abbreviation would carry
# ("IN", "OR", "HI" as ordinary English words).
_US_STATE_FULL_NAMES_FRAGMENT = (
    r"alabama|alaska|arizona|arkansas|california|colorado|connecticut|"
    r"delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|"
    r"kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|"
    r"mississippi|missouri|montana|nebraska|nevada|new\s+hampshire|"
    r"new\s+jersey|new\s+mexico|new\s+york|north\s+carolina|north\s+dakota|"
    r"ohio|oklahoma|oregon|pennsylvania|rhode\s+island|south\s+carolina|"
    r"south\s+dakota|tennessee|texas|utah|vermont|virginia|washington|"
    r"west\s+virginia|wisconsin|wyoming"
)
# Real, full 50-state-plus-DC set (2-letter USPS abbreviations). Moved up
# here (originally defined much further down, alongside
# has_state_list_restriction_signal, which still uses it unchanged — a
# function body only looks up a module global when it actually RUNS, not
# at definition time, so that use was never affected by this move) so
# _has_metro_area_state_abbr_signal below (see its own module comment,
# right after _COUNTRY_BASED_RESTRICTION_RE) can validate against it; that
# function is called from code compiled further down this same file and
# needs this set to already exist by then.
_US_STATE_ABBRS = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
}
# Countries/regions PLUS full US state names — used only by the residence-
# verb regex below (a candidate can be told to "live in California" just
# as validly as "live in Canada"), NOT by _COUNTRY_AUTH_RE's work-
# authorization noun/verb forms above (a US state isn't a work-
# authorization jurisdiction, so "authorized to work in Michigan" isn't a
# real phrasing this project has seen and isn't worth the added risk).
_RESIDENCE_PLACE_RE_FRAGMENT = _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r"|" + _US_STATE_FULL_NAMES_FRAGMENT

# 2026-09 FURTHER BROADENED (2nd cross-LLM review, real postings):
#  - "permanently" as another optional interposed word — Scribe's "with a
#    requirement to be based permanently in the United States or Canada"
#    has neither "anywhere"/"only"/"solely"/"primarily" between "based"
#    and "in", it has "permanently", which the prior version didn't allow.
#  - "remote from <place>" as a THIRD "remote ___ <place>" shape alongside
#    the existing "remote in/within <place>" — Tendril's "fully remote
#    from Mexico".
#  - "remote (?:<place>)" PARENTHETICAL shape — Kitsch's "remote
#    (Philippines)" names the place in parens right after "remote" with no
#    preposition at all.
#  - a full US state name is now an acceptable place (see
#    _RESIDENCE_PLACE_RE_FRAGMENT above) — Ninth Brain's "currently reside
#    in Michigan", Ellevation's "must live in California".
_COUNTRY_BASED_RESTRICTION_RE = re.compile(
    r"\b(?:reside|residing|resides|live|living|lives|located|based)\s+"
    r"(?:anywhere\s+)?(?:permanently\s+)?(?:only\s+|solely\s+|primarily\s+)?(?:in|within)\s+"
    r"(?:either\s+)?(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    r"|\bremote\s+(?:in|within|from)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    r"|\bremote\s*\(\s*" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\s*\)"
    r"|\bwork(?:ing)?\s+from\s+"
    r"(?:anywhere\s+)?(?:only\s+|solely\s+|primarily\s+)?(?:in\s+)?(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    # 2026-09 ROUND 6 (explicit user-provided restrictive-language
    # taxonomy): "physically" as a modal PREFIX before the residence verb —
    # the existing verb group above only allows optional words BETWEEN the
    # verb and "in"/"within" ("permanently", "only", "solely", "primarily"),
    # never a word before the verb itself, so "must be PHYSICALLY located
    # in X" / "physically reside in X" were both real gaps.
    r"|\b(?:must\s+(?:be\s+)?)?physically\s+(?:located|based|reside)\s+"
    r"(?:in|within)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    # "must have/maintain a primary/permanent residence in X", "primary/
    # permanent residence in X required" — a distinct noun-phrase shape
    # ("residence", not the residence VERB the main clause above expects).
    r"|\bmust\s+(?:have|maintain)\s+(?:a\s+)?(?:permanent|primary)\s+residence\s+"
    r"(?:in|within)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    r"|\b(?:permanent|primary)\s+residence\s+(?:in|within)\s+(?:the\s+)?"
    r"" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\s+(?:is\s+)?required\b"
    # Tax/legal residence/residency — a distinct jurisdictional concept
    # from physical/permanent residence above, but phrased the same
    # restrictive way in real postings.
    r"|\b(?:legal|tax)\s+residen(?:ce|cy)\s+(?:in|within)\s+(?:the\s+)?"
    r"" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\s+(?:is\s+)?required\b"
    r"|\bmust\s+be\s+a\s+tax\s+resident\s+of\s+(?:the\s+)?"
    r"" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    r"|\bmust\s+maintain\s+tax\s+residency\s+(?:in|within)\s+(?:the\s+)?"
    r"" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    # 2026-09 BUG FIX (explicit user report, real posting: CentralReach's
    # Greenhouse "Customer Success Lead" — "We prefer candidates who can
    # work in a hybrid capacity from one of our corporate offices in
    # Holmdel, New Jersey or Fort Lauderdale, Florida. However, we will
    # consider remote candidates located in other U.S. states for the
    # right individual." Confirmed live via WebFetch that this posting's
    # location field ALSO names those two specific cities directly, so
    # the main location-field pipeline already correctly rejects it — but
    # classify_rank4's 4b check, which naively scans title+description
    # for ANY mention of an eligible country/region as automatic positive
    # evidence, was reading the bare "U.S." inside this sentence as a
    # "mixed signal, saving grace" and wrongly admitting it. The real
    # problem: "located in OTHER U.S. states" is itself a genuine,
    # unambiguous country-wide restriction (any US state still means
    # "must be in the US," not "open beyond the US") that the residence-
    # verb pattern above never caught, because "other" sits between the
    # verb and the place name where only "the" was ever allowed, and the
    # place itself is a generic "some US state" reference rather than one
    # specific named state. Fixing the ROOT restriction-detection gap
    # here (rather than special-casing Rank 4's own logic) means this
    # posting is now correctly rejected everywhere, not just at Rank 4.
    r"|\b(?:reside|residing|resides|live|living|lives|located|based)\s+"
    r"(?:anywhere\s+)?in\s+(?:any\s+|another\s+|other\s+)?"
    r"(?:us|u\.s\.a?\.?|american)\s+states?\b"
    # 2026-09 BUG FIX (explicit user-commissioned adversarial fuzz test,
    # ~4,820 generated restrictive phrasings): three more confirmed
    # leaks. "Live AND WORK in <country>" (the interposed "and work"
    # breaks the plain residence-verb clause at the top of this regex,
    # same class of gap as "located in OTHER U.S. states" above). A bare
    # NOUN-phrase "resident of/in <country>" (distinct from the verb
    # "reside in X" the top clause already covers, and from the existing
    # "tax resident of X" clause above, which requires the word "tax").
    # "Work REMOTELY FROM <country>" (the existing "remote (in|within|
    # from) X" clause requires bare "remote", not "work remotely from").
    r"|\blive\s+and\s+work\s+in\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    r"|\b(?:a\s+)?resident\s+(?:of|in)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    r"|\bwork\s+remotely\s+(?:only\s+)?from\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    # 2026-09 BUG FIX (same fuzz-test batch): passive-voice "worked from
    # <country>" (distinct from the active "work remotely from X" above --
    # no "remotely," and the verb is passive: "This role must be WORKED
    # FROM the United States") and a bare noun-phrase "citizen of <country>"
    # (distinct from the existing "<demonym> citizen" pattern elsewhere,
    # which requires a demonym like "U.S. citizen" rather than "citizen of
    # the United States").
    r"|\bworked\s+from\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    r"|\b(?:a\s+)?citizen\s+of\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b",
    re.I,
)

# 2026-09 BUG FIX (explicit user report, real posting: SideCar Health's
# Greenhouse application question "Do you currently reside in the
# Dallas/Fort Worth, TX area?"): a residence-verb question naming a
# CITY/METRO area immediately followed by a 2-letter US state abbreviation
# — neither a full country name nor a full US state name
# _RESIDENCE_PLACE_RE_FRAGMENT recognizes, so _COUNTRY_BASED_RESTRICTION_RE
# above missed it. The city/metro text itself is deliberately unconstrained
# (just "non-comma words", since metro names vary too much to enumerate —
# "Dallas/Fort Worth", "the Bay Area", "Research Triangle").
#
# Kept as a SEPARATE regex + validation function, deliberately NOT folded
# into _COUNTRY_BASED_RESTRICTION_RE's `re.I` alternatives: a bare 2-letter
# token is only a trustworthy state signal when it's actually WRITTEN in
# uppercase in the real text ("TX", not "tx") — real postings always
# write it that way, but _US_STATE_FULL_NAMES_FRAGMENT's own module
# comment already flagged the exact failure mode a case-INSENSITIVE 2-letter
# match risks ("IN"/"OR"/"HI" as ordinary English words). Caught live in
# this fix's own testing: "Please reside in a place that is calm, in order
# to focus" case-insensitively matched "in" (after the comma) as the
# abbreviation for Indiana. Fix: match case-insensitively (so the verb
# itself can still be capitalized at a sentence start) but capture the
# 2-letter token and check it against _US_STATE_ABBRS — a Python set of
# literal uppercase strings — using the EXACT case the token was written
# in (re.I affects what the engine matches, not what a captured group
# preserves), so a lowercase/mixed-case incidental match like "in"/"Or"
# fails that membership check while a real "TX"/"CA" passes.
_METRO_AREA_STATE_ABBR_RE = re.compile(
    r"\b(?:reside|residing|resides|live|living|lives|located|based)\s+"
    r"(?:currently\s+)?(?:anywhere\s+)?(?:in|within)\s+(?:the\s+)?"
    r"[A-Za-z][\w.'-]*(?:[\s/][A-Za-z][\w.'-]*){0,4}\s*,\s*([A-Za-z]{2})\b"
    r"(?:\s+area)?",
    re.I,
)


def _has_metro_area_state_abbr_signal(text: str) -> bool:
    return any(m.group(1) in _US_STATE_ABBRS for m in _METRO_AREA_STATE_ABBR_RE.finditer(text))

# 2026-09 NEW (2nd cross-LLM review, real postings: rePurpose Global's
# "This position is remote-only for East Coast, US candidates."; Autopay's
# "This position is 100% remote for Mexico, Columbia, Venezuela and
# Guatemala, Argentina, Honduras, DR, Brazil applicants."): a distinct
# sentence SHAPE — "remote (?:-only)? for ... <place> ... candidates/
# applicants/residents" — that names the place with neither a residence
# verb NOR an "in/within" preposition at all ("for X candidates", not "for
# candidates in X"), and Autopay's version interposes a whole list of
# OTHER country names between "for" and the ones this project's fragment
# recognizes. Rather than try to match the place immediately after "for"
# (which breaks on exactly this kind of list), this checks the SENTENCE
# as a whole: does it contain the "remote ... for" trigger AND, anywhere
# in that same sentence, at least one recognized place name AND one of
# "candidates"/"applicants"/"residents"? All three conditions together are
# specific enough to avoid false-positiving on an unrelated "remote-first
# culture" sentence that happens to also mention a country in passing.
_REMOTE_FOR_TRIGGER_RE = re.compile(r"\bremote[\s\-]*(?:only\s+)?for\b", re.I)
_CANDIDATE_WORD_RE = re.compile(r"\b(?:candidates?|applicants?|residents?)\b", re.I)
_ANY_RESIDENCE_PLACE_RE = re.compile(r"\b(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b", re.I)

_TEAM_OR_COMPANY_CONTEXT_RE = re.compile(
    r"\b(?:teams?|offices?|headquarters|hq|compan(?:y|ies)|organizations?|"
    r"organisations?|orgs?|departments?|divisions?|studios?|founders?)\b",
    re.I,
)

# 2026-09 BUG FIX (explicit user report, real posting: CentralReach's
# Greenhouse "Customer Success Lead" — "...we will consider remote
# candidates located in other U.S. states for the right individual." The
# naive `re.split(r"(?<=[.!?])\s+|\n+", text)` idiom every sentence-level
# hard-override check below uses treats the period INSIDE "U.S." as a
# sentence end, silently splitting this into "...located in other U.S."
# + "states for the right individual." — cutting the actual restrictive
# phrase in half, so neither fragment matched any restriction regex and
# this US-only posting sailed through every check undetected (confirmed
# live: has_hard_country_based_restriction_signal returned False on the
# real text). Same risk for "U.K.", "U.A.E.", or any other short
# dot-separated abbreviation appearing mid-sentence. Shared by every
# sentence-splitting call site in this file (all now use this instead of
# the raw re.split) so the fix applies everywhere at once, not just the
# one check that surfaced it.
_ABBREVIATION_SUFFIX_RE = re.compile(r"\b[A-Za-z]\.[A-Za-z]?\.?$")


def _split_into_sentences(text: str) -> list[str]:
    """Splits `text` into sentences on '.', '!', '?', or a newline, then
    re-joins a split that landed right after a short (1-2 letter,
    dot-separated) ALL-CAPS-style abbreviation shape (U.S., U.K., U.A.E.,
    ...) — a real sentence essentially never ends with a bare 1-2-letter
    abbreviation immediately before a LOWERCASE continuation, so that
    combination is treated as a false split and merged back together."""
    raw_parts = re.split(r"(?<=[.!?])\s+|\n+", text)
    parts: list[str] = []
    for part in raw_parts:
        if parts and part[:1].islower() and _ABBREVIATION_SUFFIX_RE.search(parts[-1]):
            parts[-1] = parts[-1] + " " + part
        else:
            parts.append(part)
    return parts

# 2026-09 NEW (cross-LLM review, real posting: Prolific's Montreal listing —
# job-boards.eu.greenhouse.io/prolificacademicltd — "currently based in,
# and can verify right to work from one of the following countries" / "we
# can only onboard successful participants who are currently based in ...
# the specified regions"): a CURATED COUNTRY/REGION WHITELIST is, by
# construction, not "global/worldwide" — the fact that the whitelist might
# be long (Prolific's own page even offers a "REST OF WORLD" catch-all
# option elsewhere) doesn't change that THIS specific screening flow only
# onboards from a defined list, not literally anywhere. Matched on the
# distinctive whitelist PHRASING itself rather than trying to enumerate
# whatever list of countries a company might name, since the list itself
# is never fully knowable from the description text alone.
_COUNTRY_WHITELIST_PHRASE_RE = re.compile(
    r"\bone\s+of\s+the\s+following\s+countries\b"
    r"|\bthe\s+specified\s+regions?\b"
    r"|\bfollowing\s+list\s+of\s+(?:countries|regions|locations)\b"
    r"|\bcurrently\s+based\s+in,?\s+and\s+can\s+verify\s+right\s+to\s+work\b",
    re.I,
)


# 2026-09 STRICT GEOGRAPHY GATE: a role-specific physical/base-location
# statement is disqualifying even when the same posting also contains a broad
# EMEA/Africa/global word elsewhere. This closes postings such as
# "full-time position based in Darmstadt" whose title happens to contain
# "EMEA". It intentionally requires role/candidate context, so statements
# such as "our HQ is based in Darmstadt" are not treated as candidate
# restrictions.
_ROLE_SPECIFIC_PLACE_RE = re.compile(
    # 2026-09 BUG FIX (explicit user-commissioned top-to-bottom audit,
    # confirmed via direct testing): this whole regex compiles with re.I,
    # which ALSO case-folds a `[A-Z]` character class -- so the leading
    # `[A-Z]` here, meant to require the captured place start with an
    # actual capital letter (a proper noun), was silently matching a
    # LOWERCASE letter too. "This role is based in a hybrid of creativity
    # and structure" (pure marketing fluff, no place named at all) was
    # being captured and hard-rejected as if "a hybrid of creativity and
    # structure" were a real place. Fixed with a scoped case-sensitive
    # sub-pattern, `(?-i:[A-Z])`, at every "must start a capitalized word"
    # position -- this turns OFF case-insensitivity for just that one
    # character while the rest of the pattern (keywords like "role"/
    # "based"/"located") stays governed by the outer re.I as before.
    r"\b(?:this\s+)?(?:role|position|job|opening|opportunity|\"?role\"?)\b"
    r".{0,100}?\b(?:is\s+)?(?:based|located|situated)\s+(?:in|at)\s+"
    r"(?-i:[A-Z])[A-Za-zÀ-ÖØ-öø-ÿ.'-]*(?:\s+(?-i:[A-Z])[A-Za-zÀ-ÖØ-öø-ÿ.'-]*){0,4}"
    r"(?:\s*,\s*(?-i:[A-Z])[A-Za-zÀ-ÖØ-öø-ÿ.'-]*)?",
    re.I,
)

_CANDIDATE_PLACE_RE = re.compile(
    # 2026-09 BUG FIX — same re.I-vs-[A-Z] false positive as
    # _ROLE_SPECIFIC_PLACE_RE just above (see its module comment): "The
    # candidate must be based in a culture of excellence" was matching
    # as if "a culture of excellence" were a real place name.
    r"\b(?:must\s+be|should\s+be|is|are|work|working|work\s+remotely|remote\s+role)"
    r".{0,80}?\b(?:based|located|reside|residing|living|live)\s+(?:in|from)\s+"
    r"(?-i:[A-Z])[A-Za-zÀ-ÖØ-öø-ÿ.'-]*(?:\s+(?-i:[A-Z])[A-Za-zÀ-ÖØ-öøÿ.'-]*){0,4}",
    re.I,
)

# Explicitly accepted broad place names. Anything else captured by the
# role/candidate patterns above is treated as a specific geographic
# restriction.
#
# NOTE this list mixes two different things, by original design: regions
# that are genuinely ACCEPTED outright (EMEA/Africa/Global/Worldwide/
# International/Anywhere), and regions that are themselves restrictive-if-
# alone elsewhere in this file (APAC/LATAM/AMER/Americas/MENA) but are
# listed here so THIS SPECIFIC sentence-scan detector treats them as "a
# recognized region name, defer to the region-specific/multi-region-aware
# checks elsewhere" rather than misfiring as if it had found a concrete
# CITY. 2026-09 ROUND 6: every new region name added to
# _COUNTRY_AUTH_NAMES_RE_FRAGMENT/_REGION_ONLY_WORDS_RE above is added
# here for the same reason — without it, this detector's capitalized-word
# capture (_ROLE_SPECIFIC_PLACE_RE/_CANDIDATE_PLACE_RE, which stops at the
# first lowercase connector like "and") would treat "based in Western
# Europe and Gulf" as if only "Western Europe" were named and hard-reject
# it here, before the multi-region-breadth-aware checks later in the
# override chain (has_entity_or_exclusion_restriction_signal's
# _HIRING_LIMITED_TO_PLACE_RE, _has_multi_region_breadth guards, etc.)
# ever get a chance to recognize the "and Gulf" as broadening evidence.
_BROAD_REGION_VALUE_RE = re.compile(
    r"\b(?:EMEA|Africa|Sub[-\s]?Saharan\s+Africa|Global|Worldwide|"
    r"International|Anywhere|APAC|LATAM|AMER|Americas|MENA|"
    r"Western\s+Europe|Eastern\s+Europe|Central\s+Europe|Southern\s+Europe|"
    r"Northern\s+Europe|European\s+Economic\s+Area|EEA|"
    r"Gulf\s+Cooperation\s+Council|GCC|Gulf|"
    r"Southern\s+Africa|West\s+Africa|East\s+Africa|Central\s+Africa|"
    r"North\s+Africa|"
    r"South[\s\-]?East\s+Asia|South\s+Asia|East\s+Asia|Central\s+Asia|Asia|"
    r"Oceania|Pacific|South\s+America|Central\s+America|Caribbean|"
    r"CEE|CIS|JAPAC|APJ|UK\s*(?:&|and)\s*Ireland|British\s+Isles)\b", re.I,
)

def has_role_specific_place_restriction_signal(job: dict) -> bool:
    """Return True when the posting ties THIS role/candidate to a specific
    city, country, state, or other concrete place. Broad accepted regions
    such as EMEA/Africa/global are not rejected by this detector.

    This is deliberately role-scoped; company/HQ/team-location sentences
    are ignored. The check is used before and after AI classification so an
    LLM cannot override an explicit physical hiring restriction."""
    title = job.get("title") or ""
    desc = job.get("description_snippet") or ""
    location = job.get("location") or ""
    text = title + " " + desc

    # Structured location is already a role-specific ATS field. If it is a
    # concrete value and not one of the accepted broad scopes, reject it.
    loc = str(location).strip()
    if loc and not PLACEHOLDER_LOC_RE.match(loc):
        normalized = re.sub(r"[\s,|/()\-–—]+", " ", loc).strip()
        # 2026-10 (adversarial-coverage pass, real bug: "Remote - Global",
        # "Global (Remote)", "Remote, Worldwide", "Anywhere - Remote" were
        # all hard-rejected here): the fixed accepted-set below only knew
        # the bare words "remote"/"global"/... ALONE, so any location
        # combining a work-mode filler word with an accepted broad scope
        # normalized to e.g. "Remote Global", missed the set, found just
        # one region word, and fell through to `return True` (restrictive)
        # -- a false rejection of an explicitly global posting. Strip the
        # work-mode filler first and compare what's left.
        _loc_core = re.sub(
            r"\b(?:remote|fully|work\s+from\s+home|wfh|home\s*based|virtual|"
            r"telecommute|telecommuting|distributed)\b",
            " ", normalized, flags=re.I)
        _loc_core = re.sub(r"\s+", " ", _loc_core).strip().lower()
        if normalized and normalized.lower() not in {
            "remote", "fully remote", "remote worker", "remote job",
            "global", "worldwide", "international", "anywhere",
            "emea", "africa", "sub saharan africa",
        } and _loc_core not in {
            "global", "worldwide", "international", "anywhere",
            "emea", "africa", "sub saharan africa",
        }:
            # Multi-region structured locations are explicitly allowed.
            regions = {m.group(0).lower() for m in re.finditer(
                r"\b(?:EMEA|Africa|Sub[-\s]?Saharan\s+Africa|Global|Worldwide|International|Anywhere|"
                r"APAC|LATAM|AMER|AMERs|Americas|MENA|"
                # 2026-09 ROUND 6: new business regions, ordered
                # specific-before-generic (e.g. "South East Asia"/"South
                # Asia" before bare "Asia") the same way "Asia[-\s]?Pacific"
                # already had to be ordered before bare "Asia" below — this
                # is a finditer() scan, not fullmatch, so alternation order
                # determines which alternative wins at a given position.
                r"Western\s+Europe|Eastern\s+Europe|Central\s+Europe|"
                r"Southern\s+Europe|Northern\s+Europe|"
                r"European\s+Economic\s+Area|EEA|"
                r"Gulf\s+Cooperation\s+Council|GCC|Gulf|"
                r"Southern\s+Africa|West\s+Africa|East\s+Africa|"
                r"Central\s+Africa|North\s+Africa|"
                r"South[\s\-]?East\s+Asia|South\s+Asia|East\s+Asia|Central\s+Asia|"
                r"Asia[-\s]?Pacific|Asia|"
                r"Oceania|Pacific|Caribbean|"
                r"CEE|CIS|JAPAC|APJ|"
                r"Europe|"
                r"North\s+America|South\s+America|Central\s+America|"
                r"ANZ|DACH|Benelux|Nordics?)\b",
                normalized, re.I)}
            # A location consisting of two or more business regions is an
            # allowed multi-region scope, regardless of whether EMEA is one
            # of them. A single narrow region such as LATAM or APAC remains
            # restrictive under the project's allowlist.
            if len(regions) >= 2:
                return False
            # Two or more distinct African countries are treated as
            # continent-wide African hiring evidence, not a single-country
            # restriction.
            africa_hits = {m.group(1).lower() for m in _AFRICAN_COUNTRY_RE.finditer(normalized)}
            if len(africa_hits) >= 2:
                return False
            if len(regions) == 1 and normalized.lower() == next(iter(regions)):
                return True if next(iter(regions)) in {"emea", "africa", "global", "worldwide", "international", "anywhere", "sub-saharan africa"} else True
            if len(regions) == 1:
                return True
            return True

    for sentence in _split_into_sentences(text):
        if not sentence.strip():
            continue
        # Never treat company/HQ/team/office descriptions as candidate
        # restrictions.
        if _TEAM_OR_COMPANY_CONTEXT_RE.search(sentence):
            continue
        for rx in (_ROLE_SPECIFIC_PLACE_RE, _CANDIDATE_PLACE_RE):
            for m in rx.finditer(sentence):
                value = m.group(0)
                # Strip the structural words and inspect the place portion.
                tail = re.split(r"\b(?:based|located|reside|residing|living|live)\s+(?:in|from|at)\s+", value, flags=re.I)[-1].strip(" .,:;()")
                if tail and not _BROAD_REGION_VALUE_RE.fullmatch(tail):
                    # 2026-09 BUG FIX (verified live, real gap: "This role
                    # is based in APAC and LATAM." — and "...in EMEA and
                    # Africa." — were both hard-rejected). Both regexes
                    # above are compiled with re.I, so their [A-Z] capture
                    # classes ALSO match lowercase letters under case-
                    # insensitive folding — the capture does NOT stop at a
                    # lowercase connector like "and" the way it looks like
                    # it would from the pattern alone; "based in APAC and
                    # LATAM" captures the full "APAC and LATAM" as tail,
                    # same as "based in EMEA and Africa" captures "EMEA and
                    # Africa".
                    #
                    # The bug was in what happened next: the OLD code split
                    # the tail on whitespace/commas, confirmed SOME word
                    # was a recognized broad region, then did
                    # `if len(words) > 1: return True` unconditionally —
                    # which fires even when EVERY word is a broad region
                    # ("APAC and LATAM" -> ["APAC","and","LATAM"], 3 words,
                    # always returns True regardless of what those words
                    # actually were). The comment above that line ("if the
                    # tail contains a broad region PLUS a concrete place,
                    # the concrete place still wins") describes the
                    # intended behavior, but the code never actually
                    # checked for a genuine concrete place among the
                    # remaining words.
                    #
                    # Fixed by removing every recognized broad-region SPAN
                    # (via .sub(), not a naive whitespace split — a
                    # whitespace split breaks a multi-word region name like
                    # "Western Europe" into ["Western","Europe"], neither
                    # of which fullmatches the 2-word pattern on its own)
                    # and every connector word from the tail; whatever's
                    # left over is inspected. Nothing left over means the
                    # tail was ENTIRELY recognized region names + connectors
                    # ("APAC and LATAM", "EMEA and Africa", "Western Europe
                    # and Gulf") — accepted multi-region evidence, not a
                    # restriction. Real text left over ("APAC and Austin"
                    # leaves "Austin"; "New York and Boston" leaves "New
                    # York Boston") means a genuine concrete place is
                    # mixed in — still correctly restrictive.
                    remainder = _BROAD_REGION_VALUE_RE.sub(" ", tail)
                    remainder = re.sub(r"\b(?:and|or)\b|&", " ", remainder, flags=re.I)
                    remainder = re.sub(r"[,\s]+", " ", remainder).strip()
                    if remainder:
                        return True
    return False


# Strict positive hiring-scope evidence. Company reach, product reach,
# "global team", "millions worldwide", etc. are NOT eligibility evidence.
_STRICT_GLOBAL_ELIGIBILITY_RE = re.compile(
    r"\b(?:remote\s+)?(?:worldwide|global|international|anywhere|everywhere)\s+(?:hiring|hire|recruit(?:ing)?|open|available|eligible|candidates?|applicants?)\b"
    r"|\b(?:work|working|work\s+remotely|hire|hiring|recruit|recruiting|employ|employment|open)\s+(?:from|in|to)\s+(?:anywhere|any\s+country|any\s+location|the\s+world|worldwide|globally)\b"
    r"|\b(?:open|available|eligible)\s+to\s+(?:candidates?|applicants?|employees?)\s+(?:worldwide|globally|anywhere|from\s+any\s+country)\b"
    r"|\b(?:no|without)\s+(?:geographic|location|country|regional)\s+(?:restriction|restrictions|limitation|limitations)\b",
    re.I,
)

_STRICT_BROAD_REGION_RE = re.compile(
    r"\b(?:remote|work|working|hire|hiring|recruit|recruiting|employment|employ|candidates?|applicants?|based|located|available|open)\b"
    r".{0,80}\b(?:EMEA|Africa|Sub[-\s]?Saharan\s+Africa)\b"
    r"|\b(?:EMEA|Africa|Sub[-\s]?Saharan\s+Africa)\b.{0,80}\b(?:remote|work|working|hire|hiring|recruit|recruiting|employment|employ|candidates?|applicants?|based|located|available|open)\b",
    re.I,
)



# High-precision restrictive language. These patterns require a candidate/role
# eligibility construction; they intentionally do NOT match generic mentions
# such as "US customers", "our London office", or "global operations".
_EXTRA_RESTRICTIVE_PATTERNS = [
    # Explicit candidate/resident-only constructions, including the common
    # "for US residents only" form which has no residence verb such as
    # "must reside in".
    r"\b(?:for|to)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:(?:US|U\.S\.|UK|Canada|Australia|Germany|France|Ireland)|" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\s+(?:residents?|candidates?|applicants?)\s+only\b",
    r"\b(?:US|U\.S\.|UK|Canada|Australia|Germany|France|Ireland)\s+(?:residents?|candidates?|applicants?)\s+only\b",
    # 2026-10 (adversarial-coverage pass, real-shape failing case: "This
    # role is open only to US-based candidates" / "UK-based applicants
    # only"). The alt just above requires a whitespace between the country
    # and the "candidates/applicants/residents" noun; a hyphenated
    # "US-based"/"UK-based"/"Canada-based"/... adjective form (equally
    # common in real postings, especially "<country>-based" compound
    # modifiers) never matched any prior alt. Scoped to the same curated
    # country list used just above, so "<countryfragment>-based" doesn't
    # accidentally catch "home-based" or "office-based".
    r"\bremote\s*[,;:/\-–—(]?\s*(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\s+only\b",
    r"\b(?:remote|work\s+remotely)\s*[,;:/\-–—(]?\s*(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _US_STATE_FULL_NAMES_FRAGMENT + r")\s+only\b",
    r"\b(?:role|position|job|opportunity)\s+(?:is\s+)?(?:remote\s+)?(?:only|exclusively)\s+(?:for|in|from)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b",
    r"\b(?:must|need(?:s)?|required|required\s+to)\s+(?:be\s+)?(?:based|located|resident|residing|living)\s+(?:in|within)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:must|need(?:s)?|required|required\s+to)\s+(?:live|reside|work|be\s+located|be\s+based)\s+(?:in|within)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:only|exclusively)\s+(?:open|available)\s+to\s+(?:candidates?|applicants?|employees?|people)\s+(?:in|from|based\s+in)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:open|available)\s+(?:only|exclusively)\s+(?:in|to\s+candidates?\s+in|for\s+candidates?\s+in)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:remote|fully\s+remote)\s+(?:only\s+)?(?:in|within)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:remote|work)\s+(?:is\s+)?(?:only|exclusively)\s+(?:available|permitted|allowed)\s+(?:in|from)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:we|company|organization|organisation)\s+(?:can|may|will)\s+only\s+(?:hire|employ|recruit)\s+(?:in|from)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:we|company|organization|organisation)\s+(?:only|exclusively)\s+(?:hire|employ|recruit)\s+(?:in|from)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:role|position|job|opportunity)\s+(?:is|will be)\s+(?:based|located)\s+(?:in|within)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:role|position|job|opportunity)\s+(?:is|will be)\s+(?:only|exclusively)\s+(?:available|open)\s+(?:in|to)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:candidates?|applicants?)\s+must\s+(?:be\s+)?(?:authorized|authorised|eligible|entitled|permitted)\s+to\s+work\s+(?:in|from)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:right|rights)\s+to\s+work\s+(?:in|from)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:work|employment)\s+authorization\s+(?:in|for)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:employment|work)\s+eligib(?:ility|le)\s+(?:in|for)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:only|exclusively)\s+(?:hire|employ|recruit)\s+(?:people|talent|candidates?|applicants?)\s+(?:in|from)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:candidates?|applicants?)\s+(?:must|need\s+to)\s+be\s+(?:within|inside)\s+(?:the\s+)?(?-i:[A-Z])[^.;,\n]{0,79}",
    r"\b(?:timezone|time\s+zone)\s+(?:requirement|restriction|limited|only)\b.{0,100}\b(?:US|U\.S\.|UK|Europe|EMEA|APAC|LATAM|AMER|Pacific|Eastern|Central|Mountain)\b",
    r"\b(?:candidates?|applicants?|employees?)\s+(?:must|need(?:\s+to)?|are\s+required\s+to)\s+(?:be\s+)?(?:in|within)\s+(?:a\s+)?(?:US|U\.S\.|UK|European|EMEA|APAC|LATAM|AMER|Pacific|Eastern|Central|Mountain)\s+(?:time\s+)?zones?\b",
    r"\b(?:must|need(?:\s+to)?|required\s+to)\s+(?:work|be\s+available)\s+(?:during|within)\s+(?:US|U\.S\.|UK|European|EMEA|APAC|LATAM|AMER|Pacific|Eastern|Central|Mountain)\s+(?:business\s+hours|hours|time\s+zone)\b",
    r"\b(?:must|need\s+to)\s+(?:be\s+)?(?:within|in)\s+(?:the\s+)?(?:same|specified)\s+time\s*zone\b",
]
_EXTRA_RESTRICTIVE_RE = [re.compile(p, re.I) for p in _EXTRA_RESTRICTIVE_PATTERNS]

# 2026-10 (adversarial-coverage pass, real-shape failing case: "This role
# is open only to US-based candidates" / "UK-based applicants only"): the
# hyphenated "<country>-based candidates only" shapes. Kept in their own
# list (not folded into _EXTRA_RESTRICTIVE_PATTERNS) because, unlike every
# pattern there, they can trail a negated lead-in ("This role is NOT
# restricted to US-based candidates only") and so are only applied when
# _NEGATED_RESTRICTION_RE doesn't match the same sentence — see
# has_extra_restrictive_geography_signal.
_EXTRA_RESTRICTIVE_BASED_ONLY_RE = [re.compile(p, re.I) for p in (
    r"\b(?:open|available)\s+only\s+to\s+(?:US|U\.S\.|UK|Canada|Australia|Germany|France|Ireland)[\s\-]*based\s+(?:residents?|candidates?|applicants?)\b",
    r"\b(?:US|U\.S\.|UK|Canada|Australia|Germany|France|Ireland|USA|United[\s\-]States)[\s\-]+based\s+(?:residents?|candidates?|applicants?)\s+only\b",
    r"\bonly\s+(?:hiring|hire|open)\s+(?:to\s+)?(?:US|U\.S\.|UK|Canada|Australia|Germany|France|Ireland)[\s\-]*based\s+(?:residents?|candidates?|applicants?)\b",
)]
_NEGATED_RESTRICTION_RE = re.compile(
    r"\b(?:not|no\s+longer|isn'?t|aren'?t|never|without|neither|nor)\b[^.!?\n]{0,40}?\brestrict\w*\b",
    re.I,
)

# 2026-10 (generated fuzz suite, test_filters_fuzz_every_filter.py — real
# phrasing gaps found by running ~2,300 generated TP/FP cases through the
# actual Rank 4 gate): hiring-scope phrasings none of the existing
# restriction regexes covered. Each one still names a concrete PLACE (a
# country from the project-wide vocabulary, a "following countries" list,
# a timezone, a destination city, a state licence) so none fires on a bare
# "hiring"/"outside"/"relocation" mention. Applied sentence-by-sentence in
# has_extra_restrictive_geography_signal behind the same negated-
# restriction guard as the hyphenated "-based only" list above.
_SCOPE_PLACE = (r"(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b")
_EXTRA_RESTRICTIVE_SCOPE_RE = [re.compile(p, re.I) for p in (
    # "This role is restricted/limited (only) to the United States."
    r"\b(?:role|position|job|opportunity|hiring|employment|vacancy|posting)\s+(?:is\s+|are\s+)?"
    r"(?:restricted|limited)\s+(?:only\s+)?to\s+" + _SCOPE_PLACE,
    # "We are not hiring outside the UK." / "We don't hire anyone outside Canada."
    r"\b(?:not|never)\s+hiring\s+(?:anyone\s+|candidates\s+|applicants\s+|people\s+)?outside\s+(?:of\s+)?" + _SCOPE_PLACE,
    r"\b(?:do\s+not|don'?t|cannot|can'?t|will\s+not|won'?t|unable\s+to)\s+(?:currently\s+)?"
    r"(?:hire|employ|recruit)\s+(?:anyone\s+|candidates\s+|applicants\s+|people\s+)?outside\s+(?:of\s+)?" + _SCOPE_PLACE,
    # "Candidates outside the US will not be considered." (subject-first order)
    r"\b(?:candidates|applicants|applications|anyone|people|those)\s+(?:located\s+|based\s+|residing\s+|living\s+|applying\s+)?"
    r"(?:from\s+)?outside\s+(?:of\s+)?" + _SCOPE_PLACE + r"[^!?\n]{0,40}?"
    r"\b(?:not\s+be\s+(?:considered|accepted|reviewed|processed|hired)|cannot\s+be\s+(?:considered|accepted|hired)|"
    r"(?:are|is)\s+(?:not\s+eligible|ineligible)|will\s+be\s+(?:rejected|declined|disqualified))\b",
    # "We can only hire candidates who are in the United States."
    r"\b(?:can|will|do|does)\s+only\s+(?:hire|employ|consider|accept)\s+(?:candidates|applicants|people|those|individuals)\s+"
    r"(?:who\s+are\s+|that\s+are\s+|who\s+live\s+|who\s+reside\s+)?(?:located\s+|based\s+|residing\s+|living\s+|currently\s+)?"
    r"(?:in|within|from)\s+" + _SCOPE_PLACE,
    # "Only candidates in the following countries will be considered: ..."
    r"\bonly\s+(?:candidates|applicants)\s+(?:in|from|based\s+in|located\s+in|residing\s+in)\s+the\s+following\s+countries\b",
    # "We can only employ in countries where we have an EOR / entity."
    r"\bcan\s+only\s+(?:employ|hire)\s+(?:in|within)\s+countries\s+where\s+(?:we|the\s+company)\s+have\b",
    # "Must be in a European timezone." / "Must reside in a Pacific time zone."
    r"\b(?:must|should|need\s+to|required\s+to)\s+(?:be\s+)?(?:in|within|reside\s+in|live\s+in|be\s+located\s+in|be\s+based\s+in)\s+"
    r"(?:a\s+|an\s+|the\s+)?(?:us|u\.s\.|uk|u\.k\.|north\s+american?|european?|apac|latam|pacific|eastern|central|mountain|"
    r"gmt|est|cst|mst|pst|cet)\s*time\s*zones?\b",
    # Passive relocation: "Relocation to New York is required."
    r"\brelocation\s+to\s+(?:the\s+|our\s+)?(?-i:[A-Z])[\w\s,]{0,39}?\s+(?:is\s+|will\s+be\s+)?"
    r"(?:required|mandatory|necessary|a\s+must)\b",
    # State professional licences phrased with extra words: "valid Texas
    # insurance producer license", "California real estate broker license".
    r"\b(?:" + _US_STATE_FULL_NAMES_FRAGMENT + r")\s+(?:state\s+)?"
    r"(?:(?:insurance|real\s+estate|nursing|contractor|cosmetology|bar|securities|mortgage|notary|adjuster|"
    r"pharmacy|medical|teaching|producer|broker|health|life|property|casualty)\s+){1,4}licens[ei]\w*\b",
)]

def _apply_location_ai_authority_gate(jobs: list[dict], results: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Apply the final geographic decision contract.

    The LLM is authoritative for the ranking: MATCH_GLOBAL, MATCH_AFRICA,
    UNCERTAIN, and NO_MATCH are preserved exactly as returned unless the
    posting contains an explicit, deterministic, role-specific geographic
    restriction. Deterministic evidence is therefore a hard veto only; the
    absence of regex evidence is never a reason to downgrade an LLM result.
    """
    for i, (label, provider_name) in enumerate(results):
        if label not in ("match_global", "match_africa", "uncertain", "no_match"):
            continue
        job = jobs[i]
        if (has_role_specific_place_restriction_signal(job)
                or has_extra_restrictive_geography_signal(job)):
            if label != "no_match":
                log.debug(f"Hard geographic restriction overrides AI {label} for "
                          f"{job.get('url', job.get('title', '?'))!r}")
            results[i] = ("no_match", provider_name)
    return results


def has_extra_restrictive_geography_signal(job: dict) -> bool:
    text = (job.get("title") or "") + " " + (job.get("description_snippet") or "")
    for sentence in _split_into_sentences(text):
        if not sentence.strip():
            continue
        # Broad accepted geography in the same sentence can legitimately
        # qualify a region list; don't treat "remote for EMEA candidates,
        # including UK/Germany" as a single-country restriction.
        if _text_has_global_evidence(sentence) or _text_has_africa_or_emea_evidence(sentence):
            continue
        # 2026-09 BUG FIX (verified live, real gap: "This role is based in
        # APAC and LATAM." was hard-rejected). _EXTRA_RESTRICTIVE_RE's
        # "based in .../only in ..." patterns capture the place as a
        # generic [^.;,\n]{1,80} span — "APAC and LATAM" matches that span
        # whole, with no check for whether it's actually naming 2+
        # distinct business regions rather than one place. The guard above
        # only catches an explicit global/EMEA/Africa phrase in the same
        # sentence, not general multi-region breadth (e.g. two non-EMEA/
        # Africa regions named together) — add that guard too, same as
        # has_hard_country_based_restriction_signal/has_hard_country_
        # specific_auth_signal/has_role_specific_place_restriction_signal
        # already do.
        if _has_multi_region_breadth(sentence):
            continue
        # 2026-10 (caught by test_top_to_bottom_audit.py after the
        # "<country>-based candidates only" alternatives were added): a
        # NEGATED restriction ("This role is NOT restricted to US-based
        # candidates only", "We are NO LONGER restricted to ...") is the
        # opposite of a restriction. Only the hyphenated "-based ... only"
        # alternatives are guarded here, since they are the ones that can
        # trail a "restricted to" lead-in; every other alternative's
        # behavior is untouched.
        if any(rx.search(sentence) for rx in _EXTRA_RESTRICTIVE_RE):
            return True
        if (not _NEGATED_RESTRICTION_RE.search(sentence)
                and any(rx.search(sentence) for rx in _EXTRA_RESTRICTIVE_BASED_ONLY_RE)):
            return True
        if (not _NEGATED_RESTRICTION_RE.search(sentence)
                and any(rx.search(sentence) for rx in _EXTRA_RESTRICTIVE_SCOPE_RE)):
            return True
    return False

def has_strict_accepted_geography_evidence(job: dict) -> bool:
    """True only when the posting contains evidence of one of the allowed
    hiring scopes. This deliberately rejects generic company-global language
    and is the final gate after AI classification."""
    loc = str(job.get("location") or "").strip()
    title = job.get("title") or ""
    desc = job.get("description_snippet") or ""
    text = title + " " + desc

    # Structured ATS location is high-confidence evidence.
    if loc and not PLACEHOLDER_LOC_RE.match(loc):
        if re.fullmatch(r"\s*(?:global|worldwide|international|anywhere|emea|africa|sub[-\s]?saharan\s+africa)\s*", loc, re.I):
            return True
        if len({m.group(0).lower() for m in re.finditer(
            r"\b(?:EMEA|Africa|Global|Worldwide|International|Anywhere|APAC|LATAM|AMER|Americas|MENA)\b", loc, re.I)}) >= 2:
            return True

    if _STRICT_GLOBAL_ELIGIBILITY_RE.search(text) or any(rx.search(text) for rx in _EXTRA_GLOBAL_HIRING_RE):
        return True
    if _STRICT_BROAD_REGION_RE.search(text):
        # Multi-region breadth is acceptable when the regions are actually
        # stated as hiring/eligibility scope.
        return True
    if _has_multi_region_breadth(text):
        # Only accept 2+ regions when they occur in a hiring/location context,
        # not merely because an employer describes its global business.
        region_context = re.search(
            r"\b(?:hire|hiring|recruit|recruiting|work|working|remote|candidates?|applicants?|locations?|based|open|available)\b.{0,120}\b(?:EMEA|APAC|LATAM|AMER|MENA|Africa|Europe|Asia[-\s]?Pacific|Americas|North\s+America)\b",
            text, re.I,
        ) or re.search(
            r"\b(?:EMEA|APAC|LATAM|AMER|MENA|Africa|Europe|Asia[-\s]?Pacific|Americas|North\s+America)\b.{0,120}\b(?:hire|hiring|recruit|recruiting|work|working|remote|candidates?|applicants?|locations?|based|open|available)\b",
            text, re.I,
        )
        if region_context:
            return True
    return False


def has_hard_country_based_restriction_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter, sibling to
    has_hard_country_specific_auth_signal above but for a different
    phrasing family: does this job's description (or an appended
    application question) state — in its own words, not necessarily an
    "authorized to work" question — that the ROLE/CANDIDATE must reside,
    live, be located, be based, or be working from one specific named
    country/region, or must fall within a curated country/region
    whitelist? See the module comments above _COUNTRY_BASED_RESTRICTION_RE
    and _COUNTRY_WHITELIST_PHRASE_RE for the real postings (OpenSesame,
    Lumivero, Infinx, WeVote, Spinwheel, GreenSlate, Storm Ideas,
    Prolific) this closes.

    Checked sentence-by-sentence (split on '.', '!', '?', or a newline) so
    a sentence describing where the COMPANY's team/office/HQ sits (a
    common, unrelated statement in remote job postings) doesn't
    false-positive this into rejecting an otherwise genuinely open-to-
    anyone role. The whitelist-phrase check is intentionally NOT run
    through this same team/office guard — none of its phrasings have any
    plausible "describing the company, not the candidate" reading.

    2026-09 ROUND 3 (explicit user correction): a sentence naming 2+
    DISTINCT business regions together ("open to residents of MENA, AMER,
    EMEA, or Latam") is evidence of broad multi-region reach, not a
    restriction — see _has_multi_region_breadth's docstring.
    """
    desc = job.get("description_snippet") or ""
    text = desc + " " + (job.get("title") or "")
    if not text.strip():
        return False
    if _COUNTRY_WHITELIST_PHRASE_RE.search(text):
        return True
    for sentence in _split_into_sentences(text):
        if not sentence.strip():
            continue
        if _TEAM_OR_COMPANY_CONTEXT_RE.search(sentence):
            continue
        if _has_multi_region_breadth(sentence):
            continue
        if _COUNTRY_BASED_RESTRICTION_RE.search(sentence):
            return True
        if _has_metro_area_state_abbr_signal(sentence):
            return True
        if (_REMOTE_FOR_TRIGGER_RE.search(sentence) and _CANDIDATE_WORD_RE.search(sentence)
                and _ANY_RESIDENCE_PLACE_RE.search(sentence)
                and not _text_has_global_evidence(sentence)
                # 2026-09 ROUND 5 (explicit user-provided EMEA-wide hiring
                # lingo list): EMEA/Africa evidence in the same sentence is
                # just as strong a "don't reject" guard as global evidence
                # — "remote for EMEA candidates, including UK/Germany/UAE"
                # names real countries but is still an EMEA-wide (accepted)
                # posting, not a single-country restriction.
                and not _text_has_africa_or_emea_evidence(sentence)):
            return True
    return False


# 2026-09 NEW (cross-LLM review, real posting: Stripe's "Program Manager,
# Security GRC" — stripe.com/jobs/search?gh_jid=8078131): Greenhouse's own
# metadata can carry a specific, non-global location even when the bare
# location FIELD this project reads says "Remote" — ats_scrapers.py's
# _fetch_greenhouse_questions() already appends a "Metadata Location: ..."
# line to description_snippet whenever Greenhouse's own job metadata has a
# location/location_country field (see its own comment: "Also check
# metadata for location hints"), but nothing downstream ever READ that
# line — it just sat in the text unused. This is a highly reliable,
# STRUCTURED signal (it's the ATS's own metadata field, not free-form
# prose to parse), so it gets its own direct check rather than folding it
# into the prose-oriented regexes above: if the metadata value doesn't
# carry any of the same global/worldwide evidence the location-FIELD
# classifier itself accepts, it's exactly as disqualifying as the field
# saying that value directly.
_METADATA_LOCATION_LINE_RE = re.compile(r"^Metadata Location:\s*(.+)$", re.M)

# 2026-09 NEW (explicit user request: "track the location symbol and what
# location sits by it ... this would particularly be useful ... in the
# case of in-house ATSs"): ats_scrapers.py's _snippet() now appends a
# "Location Symbol: <text>" line whenever the raw posting HTML/text has a
# map-pin/location glyph immediately next to a place name (see
# _extract_location_symbol_lines there) — same "structured-enough signal,
# check it directly" treatment as _METADATA_LOCATION_LINE_RE above, since
# a glyph-adjacent place name is a deliberate visual cue a company put
# there for the exact same reason the location FIELD exists, just not
# exposed through whatever API/DOM field this project's scrapers read.
# 2026-10: one line only ([^|\n]) - the old [^|]+ ran on past the end of the
# line into the next "Application Question:" text when no "|" followed. And
# unrendered-template / loading-screen junk scraped off an application form
# ("{{display_location}}", "Loading application form" - both seen on real
# Rank 4 rows) is not a location, so it must never count as one.
_LOCATION_SYMBOL_LINE_RE = re.compile(r"Location Symbol:[ \t]*([^|\n]+)")
_LOCATION_SYMBOL_JUNK_RE = re.compile(
    r"\{\{|\}\}|\{%|<%|\$\{|\bloading\b|\bplaceholder\b|\bundefined\b|\bnull\b", re.I)


def has_hard_location_symbol_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does an appended "Location
    Symbol: ..." line (see ats_scrapers.py's _extract_location_symbol_lines)
    name a specific, non-global place? Same contract/logic as
    has_hard_metadata_location_signal just above, applied to the
    glyph-adjacent text instead of ATS metadata — both are "the posting
    itself named a specific place through a channel the location FIELD
    didn't capture" signals, so they're deliberately kept as separate,
    parallel checks rather than merged into one, in case only one of the
    two ever needs tuning later."""
    desc = job.get("description_snippet") or ""
    if not desc:
        return False
    for m in _LOCATION_SYMBOL_LINE_RE.finditer(desc):
        value = m.group(1).strip()
        if not value or _LOCATION_SYMBOL_JUNK_RE.search(value):
            continue
        if _has_multi_region_breadth(value):
            continue
        if not _text_has_global_evidence(value):
            return True
    return False


def has_hard_metadata_location_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does an appended "Metadata
    Location: ..." line (see ats_scrapers.py's _fetch_greenhouse_questions)
    name a specific, non-global place? See the module comment above
    _METADATA_LOCATION_LINE_RE for the real Stripe posting this closes."""
    desc = job.get("description_snippet") or ""
    if not desc:
        return False
    for m in _METADATA_LOCATION_LINE_RE.finditer(desc):
        value = m.group(1).strip()
        if not value:
            continue
        # Two or more named business regions are an allowed multi-region
        # scope, even when neither is EMEA (e.g. AMER + LATAM, APAC + AMER).
        # Do not mistake that for a single-region restriction.
        if _has_multi_region_breadth(value):
            continue
        if not _text_has_global_evidence(value):
            return True
    return False


# 2026-09 NEW (cross-LLM review, real postings: Arcwood's "Account Manager
# - Louisiana" (iCIMS), OpenProject's "(Senior) Account Manager - Europe"
# (Personio), HeroDevs' "Channel Account Manager, EMEA"): several ATSs
# encode the ONLY location restriction in the TITLE itself, as a trailing
# " - <place>" / ", <place>" qualifier, with the location FIELD saying
# nothing more specific than bare "Remote" or "US-" — so nothing in the
# description/application-question checks above ever sees a restriction
# at all. Deliberately scoped to a SMALL, unambiguous set of full region
# names and full (not abbreviated) US state names, matched only as the
# LAST segment of the title after a dash/comma — this avoids the false-
# positive risk a bare state abbreviation would carry (a title containing
# "IN" or "OR" as ordinary English words) since these are spelled out in
# full and only recognized in the one title position real postings
# actually use for this.
# 2026-09: reuses _US_STATE_FULL_NAMES_FRAGMENT (defined above, alongside
# _COUNTRY_BASED_RESTRICTION_RE) instead of duplicating the 50-state list a
# second time.
_TITLE_REGION_SUFFIX_NAMES = (
    r"europe|apac|latam|asia[\s\-]?pacific|australia|"
    # 2026-09 ROUND 2 (same user-provided region taxonomy as
    # _COUNTRY_AUTH_NAMES_RE_FRAGMENT above — kept in sync so a title
    # suffix like "Account Manager - MENA" or "- DACH" is caught the same
    # way "- Europe" already is). 2026-09 ROUND 3: bare "africa"/
    # "sub-saharan africa" REMOVED. 2026-09 ROUND 4 (explicit, urgent user
    # correction): bare "emea" ALSO REMOVED — see the ROUND 4 comment above
    # _COUNTRY_AUTH_NAMES_RE_FRAGMENT's "europe|apac|latam|..." line for the
    # full reasoning: this project's base location-field logic has ALWAYS
    # accepted bare EMEA (no city/country qualifier) at the same tier as
    # the Africa continent, so a title suffix like "Channel Account
    # Manager, EMEA" (HeroDevs) must NOT be hard-rejected here either.
    r"north\s+america|americas|mena|middle\s+east|"
    r"anz|dach|benelux|nordics?|"
    + _US_STATE_FULL_NAMES_FRAGMENT
)
_TITLE_REGION_SUFFIX_RE = re.compile(
    r"[\-–—,]\s*(?:" + _TITLE_REGION_SUFFIX_NAMES + r")\s*$", re.I,
)

# 2026-09 NEW (2nd cross-LLM review, real posting: Think Academy MY's
# "Remote Customer Service Representative (Malaysia Based)"): a SECOND
# title shape — a "(<Country> Based)" parenthetical — that isn't a
# trailing " - <place>"/", <place>" suffix at all, so _TITLE_REGION_SUFFIX_RE
# above never matched it. Checked anywhere in the title, not just at the
# end, since a parenthetical qualifier can appear mid-title too.
_TITLE_COUNTRY_PAREN_RE = re.compile(
    r"\(\s*" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\s+based\s*\)", re.I,
)


def has_title_region_restriction_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does the job TITLE end with a
    " - <region/state>" or ", <region/state>" qualifier, or contain a
    "(<Country> Based)" parenthetical, naming a specific, non-global
    place? See the module comments above _TITLE_REGION_SUFFIX_NAMES and
    _TITLE_COUNTRY_PAREN_RE for the real Arcwood/OpenProject/HeroDevs/
    Think Academy MY postings this closes.

    2026-09 ROUND 3 (explicit user correction): a title naming 2+ DISTINCT
    business regions together ("Account Manager - MENA, AMER, EMEA, Latam")
    is evidence of broad multi-region reach, not a single-region
    restriction — see _has_multi_region_breadth's docstring. Checked before
    the suffix/paren regexes so a multi-region title never gets rejected
    just because one of its several regions happens to sit last."""
    title = job.get("title") or ""
    # A title containing two or more business regions is broad multi-region
    # hiring, not a single-region restriction. Keep it at priority 3.
    if _has_multi_region_breadth(title):
        return False
    if not title.strip():
        return False
    if _has_multi_region_breadth(title):
        return False
    return bool(_TITLE_REGION_SUFFIX_RE.search(title) or _TITLE_COUNTRY_PAREN_RE.search(title))


# Marker ats_scrapers.py's enrich_application_questions() appends before
# a work-authorization-flavored screening question (see its own
# _WORK_AUTH_RE / _format_auth_questions) — every line carrying this
# marker is, by construction, already known to be about authorization/
# visa/sponsorship, so its mere presence is itself a hard signal
# regardless of the exact wording used in that specific question.
_APPLICATION_AUTH_QUESTION_MARKER = "Application Question:"

# _US_STATE_ABBRS (used below to validate a comma-separated run of 2-letter
# tokens is genuinely a list of U.S. states, not a coincidental run of
# unrelated 2-letter acronyms) now lives up near _US_STATE_FULL_NAMES_FRAGMENT
# — moved there so _has_metro_area_state_abbr_signal can validate against
# it from code that compiles earlier in this file.
# 3+ comma-separated 2-letter uppercase tokens, anywhere in the text.
_STATE_ABBR_RUN_RE = re.compile(r"\b([A-Z]{2}(?:\s*,\s*[A-Z]{2}){2,})\b")
# 3+ FULL U.S. state names joined by commas / "and" / "or" (see the
# 2026-10 note in has_state_list_restriction_signal).
_STATE_FULL_NAME_RUN_RE = re.compile(
    r"\b(?:" + _US_STATE_FULL_NAMES_FRAGMENT + r")\b"
    r"(?:\s*,\s*(?:(?:and|or)\s+)?|\s+(?:and|or)\s+)"
    r"\b(?:" + _US_STATE_FULL_NAMES_FRAGMENT + r")\b"
    r"(?:\s*,\s*(?:(?:and|or)\s+)?|\s+(?:and|or)\s+)"
    r"\b(?:" + _US_STATE_FULL_NAMES_FRAGMENT + r")\b",
    re.I,
)
_STATE_LIST_COMPANY_FOOTPRINT_RE = re.compile(
    r"\b(?:offices?|headquarters|hq|branches|studios|campus(?:es)?|locations|facilities)\b", re.I)
_STATE_LIST_FRAMING_RE = re.compile(
    r"\b(?:hire|hiring|reside|resid\w+|live|living|located|based|eligib\w+|"
    r"candidates?|applicants?|only|following\s+states|these\s+states|in\s+one\s+of)\b",
    re.I,
)


def has_state_list_restriction_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does this job's description
    contain a comma-separated run of 3+ genuine U.S. state abbreviations
    (e.g. "available to candidates who reside in the following states:
    AL, AZ, CT, FL, GA, ...")? An enumerated state list is, by
    construction, a hard U.S.-only (and often not even nationwide —
    usually a SUBSET of states) eligibility restriction — unambiguous
    evidence a posting is not global, regardless of what a separate bare
    "Remote" location field says.

    Real posting this closes: RethinkCare's "Senior Client Success
    Manager" (rethink.applytojob.com, JazzHR) had location="Remote" (a
    real, honestly-reported field — not an extraction bug) but its
    description read "Remote opportunities are available to candidates
    who reside in the following states: AL, AZ, CT, FL, GA, HI, IA, IL,
    IN, KY, LA, MD, MA, MI, MN, MO, MT, NC, NE, NH, NJ, NV, OH, OK, OR,
    PA, RI, TN, TX, VA, WA, WI, WY" — 30 explicitly named states, nothing
    close to global. The AI location stage reviewed this job and returned
    "uncertain" rather than catching the list itself, which then hit this
    project's "bare Remote + AI-uncertain is kept at PRIORITY_UNSURE"
    policy and got written to the jobs table. Requires 80%+ of the tokens
    in a matched run to be REAL state abbreviations (not just any
    2-letter run) to avoid false-positiving on an unrelated acronym list.
    """
    desc = job.get("description_snippet") or ""
    text = desc + " " + (job.get("title") or "")
    if not text.strip():
        return False
    for m in _STATE_ABBR_RUN_RE.finditer(text):
        tokens = [t.strip() for t in m.group(1).split(",")]
        valid = [t for t in tokens if t in _US_STATE_ABBRS]
        if len(valid) >= 3 and len(valid) / len(tokens) >= 0.8:
            # 2026-10 (generated fuzz suite): "We have offices in CA, TX,
            # NY, FL, WA." is a company-footprint sentence, not a hiring
            # restriction. Skip a run whose own lead-in sentence talks
            # about offices/HQ/locations AND carries no hiring/residency
            # framing at all — any framing word ("candidates who reside
            # in states where we have offices: ...") keeps it a hit.
            lead = re.split(r"[.!?\n]", text[max(0, m.start() - 160):m.start()])[-1]
            if (_STATE_LIST_COMPANY_FOOTPRINT_RE.search(lead)
                    and not _STATE_LIST_FRAMING_RE.search(lead)):
                continue
            return True
    # 2026-10 (adversarial-coverage pass, real-shape failing case: "We can
    # only hire in these states: California, Texas, New York, Washington."):
    # the abbreviation-run check above never sees a list spelled out as
    # FULL state names. Same restriction, same reasoning — but unlike a
    # run of 2-letter codes, a list of full names also shows up in plain
    # company prose ("our offices are in California, Texas and New York"),
    # so a hit additionally has to sit in a sentence with hiring/residency
    # framing and NOT in company/office/customer context.
    for sentence in _split_into_sentences(text):
        if not sentence.strip() or _TEAM_OR_COMPANY_CONTEXT_RE.search(sentence):
            continue
        if _STATE_FULL_NAME_RUN_RE.search(sentence) and _STATE_LIST_FRAMING_RE.search(sentence):
            return True
    return False


# 2026-09 NEW (explicit user report, real posting: SideCar Health's
# Greenhouse application question "Do you have a Texas State Health and
# Life insurance license?"): a STATE-ISSUED PROFESSIONAL/OCCUPATIONAL
# LICENSE question is, in practice, exactly as restrictive as naming the
# state directly — insurance, nursing, real-estate, contractor, and
# similar licenses are issued and valid per-state in the US, so a company
# screening for one is screening for candidates who are either already
# licensed there (overwhelmingly means they live/work in that state
# today) or willing to become licensed there, either way a state-specific
# eligibility bar no different in kind from "must reside in Texas." This
# posting's own location field had nothing else flagging it — the
# question was the ONLY restrictive signal anywhere.
_STATE_PROFESSIONAL_LICENSE_RE = re.compile(
    r"\b(?:" + _US_STATE_FULL_NAMES_FRAGMENT + r")\s+state\s+[\w\s/&-]{0,40}?"
    r"licens[ei]\w*\b"
    r"|\blicensed\s+in\s+(?:the\s+state\s+of\s+)?(?:" + _US_STATE_FULL_NAMES_FRAGMENT + r")\b"
    r"|\b(?:" + _US_STATE_FULL_NAMES_FRAGMENT + r")\s+(?:insurance|real\s+estate|nursing|"
    r"contractor|cosmetology|bar)\s+licens[ei]\w*\b",
    re.I,
)


def has_state_specific_license_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does this job's description or
    application-question text ask about a US-state-specific professional/
    occupational license (e.g. "Texas State Health and Life insurance
    license", "licensed in the state of California")? See
    _STATE_PROFESSIONAL_LICENSE_RE's module comment above for why this is
    treated as a hard state-tied restriction, same severity as naming the
    state directly in a residence requirement."""
    desc = job.get("description_snippet") or ""
    text = desc + " " + (job.get("title") or "")
    if not text.strip():
        return False
    return bool(_STATE_PROFESSIONAL_LICENSE_RE.search(text))


def has_hard_country_specific_auth_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does this job's description
    (including any appended application-question text) or title contain
    an AFFIRMATIVE country-specific work-authorization requirement, or a
    country-specific work-authorization/visa/sponsorship screening
    question flagged by enrich_application_questions()? Forces NO_MATCH —
    see the module comment above _COUNTRY_AUTH_RE for the two real
    postings this closes.

    2026-09 CRITICAL FIX (real production data, not a hypothetical): the
    PRIOR version of this function treated the mere PRESENCE of ANY
    "Application Question: ..." line as an automatic hard no_match — full
    stop, regardless of what that question actually said. Those lines are
    appended by ats_scrapers.py's enrich_application_questions() whenever a
    screening question matches _WORK_AUTH_RE, which matches ubiquitous,
    industry-standard EEO/I-9 compliance screening language — "Are you
    legally authorized to work in the country in which you are applying?",
    "Will you now or in the future require sponsorship for employment visa
    status?" — that the overwhelming majority of US-headquartered
    companies ask on EVERY job application via one company-wide question
    set, REGARDLESS of whether that specific posting is actually
    restricted to one country. Asking the question proves nothing about
    the required answer or about this role's actual eligibility.
    Live scan_reports data confirmed the damage directly: role-matched
    ("csm_roles") counts of 10,000-17,000 per crawl_i shard were
    collapsing to 11-75 surviving jobs ("global_jobs") after the location
    filter — a >99% rejection rate, across every ATS platform, not just
    the handful of genuinely country-restricted postings this override
    was written to catch. This is almost certainly the dominant cause of
    the daily job count collapsing from 1500+ to under 300.
    Fix: only treat an application-question hit as a hard signal when the
    QUESTION TEXT ITSELF names a specific country via _COUNTRY_AUTH_RE —
    "Are you legally authorized to work in the United States?" still
    triggers this (it names a country), but the generic, country-agnostic
    "Are you legally authorized to work in the country in which you are
    applying?" no longer does, since it says nothing about which country
    this particular job actually requires.

    2026-09 ROUND 3 (explicit user correction): a question or description
    naming 2+ DISTINCT business regions together ("authorized to work in
    MENA, AMER, EMEA, or Latam") is evidence of broad multi-region reach,
    not a restriction — see _has_multi_region_breadth's docstring. Checked
    per-line/per-text before the country/region regex fires.
    """
    desc = job.get("description_snippet") or ""
    for line in desc.splitlines():
        if (line.startswith(_APPLICATION_AUTH_QUESTION_MARKER) and _COUNTRY_AUTH_RE.search(line)
                and not _has_multi_region_breadth(line)):
            return True
    text = desc + " " + (job.get("title") or "")
    if _has_multi_region_breadth(text):
        return False
    return bool(_COUNTRY_AUTH_RE.search(text))


def _referential_auth_hit(text: str) -> bool:
    """True if any sentence in `text` matches _REFERENTIAL_AUTH_QUESTION_RE
    AND isn't purely company-benefit framing ("we provide work permit
    support to help you work in the country where this role is located" —
    a benefit offered, not a requirement placed on the applicant). Same
    benefit-vs-requirement discipline has_country_tied_sponsorship_permit_
    residency_signal already applies to its own regex."""
    if not text or not text.strip():
        return False
    for sentence in _split_into_sentences(text):
        if not sentence.strip() or not _REFERENTIAL_AUTH_QUESTION_RE.search(sentence):
            continue
        if (_RANK4_BENEFIT_FRAMING_RE.search(sentence)
                and not _RANK4_REQUIREMENT_FRAMING_RE.search(sentence)):
            continue
        return True
    return False


def has_referential_auth_question_with_named_place_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter — the universal (Rank 1/2/3a/3b)
    counterpart to _rank4_has_country_tied_restrictive_question's Rank-4-
    only version. See _REFERENTIAL_AUTH_QUESTION_RE's module comment for
    the real Twilio posting this closes.

    Unlike Rank 4 (which by definition is only ever evaluating a job whose
    location already resolved to one specific bare country/region), a job
    reaching THIS check could have any location value at all — so this
    function does its own "is a real, specific place already named"
    check on the RAW location field before treating a referential question
    as disqualifying. Explicit user instruction: "regex/classifier should
    ask if it already named a location... if a location is named and it
    asks these kinds of questions... then it should not be let in."

    A referential question is NOT disqualifying when:
    - No real place is named at all (blank, placeholder, or bare
      "Remote") — "wherever this role is located" is uninformative when
      the location field itself never says, same as the existing
      country-agnostic-question policy for a job with no location signal.
    - The named place is ALREADY a broad, accepted scope (an explicit
      Global/Worldwide claim, EMEA, Africa, or 2+ business regions
      together) — the referential question then ties to that broad scope,
      not a single narrow country, which is exactly as acceptable as the
      existing country-agnostic-question policy already treats it.
    """
    desc = job.get("description_snippet") or ""
    text = desc + " " + (job.get("title") or "")
    if not _referential_auth_hit(text):
        return False

    raw_loc = job.get("location") or ""
    raw_country = job.get("country") or ""
    if isinstance(raw_loc, list):
        raw_loc = ", ".join(str(x) for x in raw_loc)
    if isinstance(raw_country, list):
        raw_country = ", ".join(str(x) for x in raw_country)
    loc = (raw_loc + " " + raw_country).strip()
    if _is_bare_location(loc):
        return False
    if STANDALONE_GLOBAL_RE.search(loc) or re.search(r"\bemea\b", loc, re.I) or re.search(r"\bafrica\b", loc, re.I):
        return False
    if _has_multi_region_breadth(loc):
        return False
    return True


# 2026-09 NEW (explicit user instruction, real posting: Together AI's
# Greenhouse listing, job 5070981007 — "Are you willing to work four days
# per week in our San Francisco office?"). Distinct from
# _NON_REMOTE_WORKPLACE_RE below: that one matches a scraper-reported
# workplace_type FIELD value (Hybrid/On-site/In-office/In-person) or a
# title suffix, not free-text body/application-question phrasing asking
# whether the candidate is willing to attend an office N days a week.
# A number-of-days-per-week-in-office question is unambiguous evidence the
# role is NOT fully remote, regardless of what the bare location field
# says — Together AI's own location field here was just "San Francisco"
# with no other qualifier, so nothing else in this pipeline would have
# caught it. Only reaches this function's input at all since the 2026-09
# fix to ats_scrapers.py's _format_screening_questions() stopped dropping
# non-work-authorization-shaped screening questions before they ever
# reached description_snippet.
_OFFICE_ATTENDANCE_RE = re.compile(
    r"\b(?:\d+|one|two|three|four|five|six|seven)\s*(?:-|\s)?days?\s*"
    r"(?:a|per)\s*week\s*(?:in|at|from)\s*(?:our|the|this|your)?\s*"
    r"[\w\s]{0,30}?\boffice\b"
    # 2026-09 BUG FIX (explicit user report, real posting: audyence's
    # Rippling listing — "Are you willing and able to work at our Austin
    # office 4 days a week?"): the interposed "and able"/"and available"
    # between "willing" and "to" broke this alternative outright (it
    # required "willing" directly followed by "to"); confirmed missed
    # live via WebFetch. Also note the frequency ("4 days a week") comes
    # AFTER "office" in this real phrasing, not before it the way the
    # first alternative above expects — this alternative doesn't require
    # a frequency at all, so it still matches regardless of where one
    # sits.
    r"|\bwilling\s+(?:and\s+(?:able|available)\s+)?to\s+(?:work|come|be)\s+"
    r"(?:in|to|at)\s+(?:our|the|this|your)?\s*[\w\s]{0,30}?\boffice\b"
    r"|\brequired?\s+to\s+(?:be\s+)?(?:in|at)\s+(?:the|our|a)\s+office\b"
    r"|\bin[\s\-]office\s+\d+\s*days?\b"
    # 2026-09 BUG FIX (explicit user-commissioned adversarial fuzz test):
    # "Must be able to work FROM an office" -- a distinct shape from
    # "work AT/IN/TO ... office" the "willing to work..." alternative
    # above requires; "from an office" reads the same physical-presence
    # way but was missed entirely.
    r"|\bable\s+to\s+work\s+from\s+(?:an?\s+|our\s+|the\s+)?office\b"
    r"|\bmust\s+work\s+from\s+(?:an?\s+|our\s+|the\s+)?office\b"
    # 2026-09 BUG FIX (explicit user report, real posting: Project A
    # Services GmbH & Co KG's Greenhouse application question "Are you
    # open to working fully onsite in Berlin?"): an INTERROGATIVE
    # willingness question built around "onsite"/"in-office"/"in-person"/
    # "hybrid" directly, with no "office" noun at all — every alternative
    # above requires the literal word "office" to appear, so a question
    # phrased around "onsite" instead (a one-word on/off-site qualifier,
    # not "in our office") fell through untouched. Allows up to two filler
    # words between "work(ing)" and the qualifier ("working FULLY onsite"),
    # same reasoning as the "willing to work..." alternative above's
    # `[\w\s]{0,30}?` slack before "office".
    r"|\b(?:open|willing|able)\s+to\s+work(?:ing)?\s+(?:\w+\s+){0,2}"
    r"(?:on[\s\-]?site|in[\s\-]?office|in[\s\-]?person|hybrid)\b"
    # 2026-10 (adversarial-coverage pass, six real-shape failing phrasings:
    # "You will be in the office 3 days per week", "Required to be onsite 4
    # days a week", "Must be in the office at least 3 days a week", "4 days
    # per week in-office required", "This role requires 4 days a week
    # onsite in New York", "Hybrid schedule: 3 days in office, 2 remote").
    # The original alternatives all expect the frequency BEFORE "in the
    # office" or a "willing/able to" lead-in; the reversed word order
    # (location qualifier first, frequency second -- or vice versa with
    # onsite/in-office/in-person instead of the literal word "office") was
    # never covered. Every new alternative still REQUIRES a per-week
    # frequency (or an explicit remote-days contrast for the "N days in
    # office" form) so a bare "visit the office" / "quarterly in-office
    # retreat" / "first 3 days of onboarding" doesn't match.
    r"|\b(?:in|at)\s+(?:the|our|an?)\s+office\b[^.!?\n]{0,40}?"
    r"\b(?:\d+|one|two|three|four|five)\s*(?:-|\s)?days?\s*(?:a|per|each|every|/)\s*week\b"
    r"|\b(?:on[\s\-]?site|in[\s\-]?office|in[\s\-]?person)\b[^.!?\n]{0,30}?"
    r"\b(?:\d+|one|two|three|four|five)\s*(?:-|\s)?days?\s*(?:a|per|each|every|/)\s*week\b"
    r"|\b(?:\d+|one|two|three|four|five)\s*(?:-|\s)?days?\s*(?:a|per|each|every|/)\s*week\b[^.!?\n]{0,20}?"
    r"\b(?:on[\s\-]?site|in[\s\-]?office|in[\s\-]?person)\b"
    r"|\b(?:\d+|one|two|three|four|five)\s*days?\s+(?:in|at)\s+(?:the\s+|our\s+)?office\b[^.!?\n]{0,30}?"
    r"\b(?:remote|from\s+home|wfh|a\s+week|per\s+week)\b"
    r"|\b(?:hybrid|schedule)\b[^.!?\n]{0,40}?"
    r"\b(?:\d+|one|two|three|four|five)\s*days?\s+(?:in|at)\s+(?:the\s+|our\s+)?office\b",
    re.I,
)


def has_office_attendance_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does this job's description or
    application-question text (see _OFFICE_ATTENDANCE_RE's module comment
    above) require in-person office attendance a specific number of days
    per week, or explicitly ask the candidate's willingness to be in a
    physical office? This is a hard "not fully remote" signal independent
    of the workplace_type field and title-suffix checks elsewhere in this
    file, both of which only catch a STRUCTURED field/title value, not a
    free-text attendance requirement buried in a screening question."""
    desc = job.get("description_snippet") or ""
    text = desc + " " + (job.get("title") or "")
    return bool(_OFFICE_ATTENDANCE_RE.search(text))


# 2026-09 NEW (explicit user instruction: "roles with say CSM - GERMAN
# speaking, French speaking should be excluded too. If its phrased as a
# hard requirement in the requirements section it should not make it in.
# [if] its added as something that would be nice/the bonus section, then
# let it in."). A role that genuinely requires fluency in a specific
# non-English language is, in practice, tied to that language's market/
# region the same way a named-country residency requirement is — a
# company screening CS/AM candidates for German fluency is screening for
# people serving German-speaking customers, not actually open anywhere in
# the world. A TITLE qualifier ("CSM - German Speaking") is always
# definitional (titles don't carry a "nice to have" nuance) so it's a
# hard reject unconditionally; a description/application-question mention
# only rejects when it sits in a HARD-requirement context, not a nice-to-
# have/bonus one, per the user's own explicit distinction above.
_LANGUAGE_NAMES_FRAGMENT = (
    r"german|french|spanish|italian|portuguese|dutch|flemish|polish|"
    r"swedish|norwegian|danish|finnish|russian|ukrainian|czech|slovak|"
    r"romanian|hungarian|greek|turkish|arabic|hebrew|japanese|korean|"
    r"mandarin|cantonese|chinese|hindi|thai|vietnamese|indonesian|bahasa|"
    r"tagalog|filipino|bulgarian|croatian|serbian|lithuanian|latvian|"
    r"estonian|farsi|persian|urdu|bengali|malay|swahili|afrikaans"
)
_TITLE_LANGUAGE_SPEAKING_RE = re.compile(
    r"\b(?:" + _LANGUAGE_NAMES_FRAGMENT + r")[\s\-]speak(?:ing|er)\b", re.I,
)
_LANGUAGE_FLUENCY_RE = re.compile(
    r"\b(?:" + _LANGUAGE_NAMES_FRAGMENT + r")[\s\-]speak(?:ing|er)\b"
    r"|\b(?:fluent|fluency|proficient|proficiency)\s+(?:in\s+|with\s+)?"
    r"(?:" + _LANGUAGE_NAMES_FRAGMENT + r")\b"
    r"|\bnative\s+(?:" + _LANGUAGE_NAMES_FRAGMENT + r")\s*(?:speaker)?\b"
    r"|\bmust\s+(?:speak|be\s+fluent\s+in)\s+(?:" + _LANGUAGE_NAMES_FRAGMENT + r")\b"
    r"|\b(?:" + _LANGUAGE_NAMES_FRAGMENT + r")\s+language\s+(?:skills?|proficiency|fluency)\b",
    re.I,
)
# Section/line-level softeners — a standalone header line ("Nice to
# Haves:", "Bonus:") flips every language mention AFTER it (until the next
# hard-requirement header) from a reject into neutral; an inline qualifier
# on the SAME line/sentence as the language mention ("German speaking is a
# plus") softens just that one mention regardless of which section it's
# physically under.
_NICE_TO_HAVE_HEADER_RE = re.compile(
    r"^(?:nice[\s\-]to[\s\-]haves?|bonus(?:\s+points?)?|"
    r"preferred(?:\s+qualifications?|\s+skills?)?|a\s+plus|pluses|"
    r"desirable(?:\s+skills?)?|good\s+to\s+have|optional(?:\s+skills?)?)\s*:?\s*$",
    re.I,
)
_HARD_REQUIREMENT_HEADER_RE = re.compile(
    r"^(?:requirements?|required\s+(?:skills?|qualifications?)|must[\s\-]haves?|"
    r"minimum\s+qualifications?|basic\s+qualifications?|qualifications?|"
    r"what\s+you(?:'ll|\s+will)?\s+(?:need|bring)|"
    r"what\s+we(?:'re|\s+are)?\s+looking\s+for)\s*:?\s*$",
    re.I,
)
_INLINE_LANGUAGE_SOFTENER_RE = re.compile(
    r"\bis\s+a\s+plus\b|\ba\s+plus\b|\bbonus\b|\bpreferred\b|\bdesirable\b|"
    r"\bnice\s+to\s+have\b|\boptional\b|\bnot\s+required\b|\bnot\s+mandatory\b",
    re.I,
)


def has_language_fluency_restriction_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter — see the module comment above
    _LANGUAGE_NAMES_FRAGMENT for the full policy. A title qualifier
    ("CSM - German Speaking") is always a hard reject. A description/
    application-question mention only rejects when it's a HARD
    requirement, not a nice-to-have/bonus one — tracked by walking the
    text clause-by-clause (reusing _split_into_sentences, which already
    splits on newlines, so a standalone section-header line is its own
    entry) and flipping an in-soft-section flag on a recognized nice-to-
    have/hard-requirement header, plus checking each individual mention's
    own clause for an inline softener regardless of section."""
    title = job.get("title") or ""
    if isinstance(title, str) and _TITLE_LANGUAGE_SPEAKING_RE.search(title):
        return True
    desc = job.get("description_snippet") or ""
    if not isinstance(desc, str) or not desc.strip():
        return False
    in_soft_section = False
    for clause in _split_into_sentences(desc):
        stripped = clause.strip(" \t -*•#>")
        if not stripped:
            continue
        if len(stripped) <= 60 and _NICE_TO_HAVE_HEADER_RE.match(stripped):
            in_soft_section = True
            continue
        if len(stripped) <= 60 and _HARD_REQUIREMENT_HEADER_RE.match(stripped):
            in_soft_section = False
            continue
        if not _LANGUAGE_FLUENCY_RE.search(stripped):
            continue
        if in_soft_section or _INLINE_LANGUAGE_SOFTENER_RE.search(stripped):
            continue
        return True
    return False


# 2026-09 NEW (explicit user-provided taxonomy of restrictive job-posting
# language, not tied to one specific posting this time — same brainstorm as
# the region-name broadening near _COUNTRY_AUTH_NAMES_RE_FRAGMENT above).
# The user's own design constraint, quoted directly: "The biggest thing I'd
# avoid is making `country`, `region`, `EMEA`, `Europe`, `Africa`, `US`,
# `Canada`, `remote`, `EOR`, `PEO`, `payroll`, or `timezone` independently
# restrictive. They need to participate in an eligibility/location
# construction." Every regex below is built to that rule: none of them fire
# on a bare keyword alone, only on a full multi-word construction that
# unambiguously states an eligibility restriction.
#
# Part 1: "we can't/won't employ you there" wording — a company saying it
# has no legal entity/payroll capability in the candidate's country, or can
# only employ through a curated EOR/PEO country list, is JUST AS restrictive
# as a hard country requirement even though it's phrased as a company
# capability statement rather than a candidate requirement. This is
# UNCONDITIONAL (no team/office-context guard, no place-name requirement,
# just like _COUNTRY_WHITELIST_PHRASE_RE above) because none of these
# phrasings have a plausible "describing the company in general, unrelated
# to hiring" reading — they only ever appear in an eligibility context.
_ENTITY_PAYROLL_RESTRICTION_RE = re.compile(
    r"\b(?:do\s+not|don'?t|does\s+not|doesn'?t)\s+(?:currently\s+)?have\s+(?:a\s+|an\s+)?"
    r"(?:legal\s+)?entity\s+in\b"
    r"|\bno\s+legal\s+entity\s+in\b"
    r"|\bunable\s+to\s+(?:employ|hire|onboard)\s+(?:you\s+|candidates\s+|applicants\s+)?"
    r"(?:in|outside|from)\b"
    r"|\bcan\s+only\s+(?:employ|hire)\b[\w\s]{0,30}?\bwhere\s+"
    r"(?:we|the\s+company|\w+)\s+(?:have|has)\s+(?:an?\s+)?(?:legal\s+|local\s+)?entity\b"
    r"|\b(?:must|will)\s+be\s+employed\s+through\s+(?:our|a|an)\s+(?:eor|peo)\b"
    r"|\bonly\s+(?:able\s+to\s+)?onboard(?:ed)?\s+(?:candidates\s+)?through\s+(?:our|a|an)\s+(?:eor|peo)\b"
    r"|\bcountries\s+(?:where\s+)?(?:we|the\s+company)\s+(?:currently\s+)?(?:have|has)\s+"
    r"(?:an?\s+)?(?:eor|peo|payroll)\s+(?:partner|provider|entity|presence)\b"
    # 2026-09 ROUND 6 (explicit user-provided restrictive-language taxonomy,
    # cross-checked against this file's existing coverage). Same "full
    # multi-word construction only" design rule as the block above — none
    # of these fire on bare "EOR"/"payroll"/"entity" alone.
    #
    # Negative-capability modal broadening: the existing "unable to
    # employ/hire/onboard ... in/outside/from" clause above doesn't match
    # the equally common "cannot"/"can't"/"do not"/"don't" phrasing of the
    # exact same statement ("we cannot employ in Brazil", "we don't hire
    # from India").
    r"|\b(?:cannot|can'?t|(?:do|does)\s+not|(?:don|doesn)'?t)\s+(?:currently\s+)?"
    r"(?:employ|hire|onboard)\s+(?:you\s+|candidates\s+|applicants\s+)?(?:in|outside|from)\b"
    # "must be employed in/through our <place> entity" — the existing
    # EOR/PEO-only version above doesn't cover a named-country entity
    # phrasing.
    r"|\bmust\s+be\s+employed\s+(?:in|within)\s+(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bemployment\s+through\s+(?:our|a|an)\s+(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\s+entity\s+only\b"
    # Payroll-specific (distinct from the existing "EOR/PEO/payroll
    # partner/provider/entity/presence" clause above, which requires
    # "countries where we have" phrasing specifically).
    r"|\bpayroll\s+(?:is\s+)?only\s+(?:available|supported)\s+(?:in|for)\b"
    r"|\bpayroll\s+(?:is\s+)?(?:available|supported)\s+only\s+(?:in|for)\b"
    r"|\bpayroll\s+(?:is\s+)?restricted\s+to\b"
    r"|\bwe\s+can\s+only\s+payroll\s+employees\s+in\b"
    r"|\bwe\s+can\s+only\s+employ\s+people\s+in\b"
    # EOR-specific broadening — the existing clauses above only cover "must
    # be employed through an EOR" and "onboarded through an EOR"; they
    # don't cover EOR *coverage/availability* being scoped to a country
    # list, which is the more common real phrasing.
    r"|\beor\s+(?:is\s+)?only\s+available\s+(?:in|for)\b"
    r"|\beor\s+(?:is\s+)?available\s+only\s+(?:in|for)\b"
    r"|\beor\s+coverage\s+(?:is\s+)?(?:only\s+)?(?:in|for)\b"
    r"|\beor[\s\-]supported\s+countries\s+only\b"
    r"|\beor\s+countries\s+only\b"
    r"|\bwe\s+can\s+(?:hire|employ)\s+through\s+(?:an?\s+)?eor\s+only\s+in\b"
    r"|\bwe\s+only\s+support\s+employment\s+through\s+(?:an?\s+)?eor\s+in\b"
    r"|\bemployment\s+through\s+an?\s+eor\s+is\s+limited\s+to\b"
    # 2026-09 BUG FIX (explicit user-commissioned adversarial fuzz test):
    # "we cannot employ people WHERE WE LACK a legal entity" -- "lack" as
    # an alternative to "do not/don't have", the existing negative-
    # capability clause above only covers.
    r"|\b(?:cannot|can'?t|(?:do|does)\s+not|(?:don|doesn)'?t)\s+employ\s+"
    r"(?:people\s+|candidates\s+|applicants\s+|you\s+)?where\s+(?:we|the\s+company)\s+"
    r"(?:lack|do\s+not\s+have|don'?t\s+have)\s+(?:a\s+|an\s+)?(?:legal\s+|local\s+)?entity\b"
    # 2026-09 BUG FIX (same fuzz test): a VAGUE "approved/supported
    # country" gate -- doesn't name a specific country (the payroll/EOR
    # clauses above all require one), but is the exact same restrictive
    # shape as the existing "countries where we have an eor/peo/payroll
    # partner" clause, just phrased around an unnamed "supported"/
    # "approved" list instead. "Must be in a supported payroll country",
    # "Only countries supported by our payroll provider are eligible",
    # "Applicants must be in countries covered by our EOR", "Candidates
    # must be in an approved country" were all confirmed leaking.
    r"|\bmust\s+be\s+in\s+(?:a\s+|an\s+)?(?:supported|approved)\s+"
    r"(?:payroll\s+)?countr(?:y|ies)\b"
    r"|\b(?:only\s+)?countries\s+(?:supported\s+by|covered\s+by)\s+(?:our\s+|the\s+)?"
    r"(?:payroll\s+provider|eor|peo)\b[^.!?\n]{0,20}\b(?:are\s+)?eligible\b"
    r"|\bmust\s+be\s+in\s+countries\s+covered\s+by\s+(?:our\s+|the\s+)?eor\b"
    r"|\bmust\s+be\s+in\s+an?\s+approved\s+countr(?:y|ies)\b",
    re.I,
)

# Part 2: explicit exclusion phrasing — "not open/available to candidates
# OUTSIDE of <place/list>" is the mirror image of "only open to candidates
# IN <place>": both restrict eligibility to one place, just phrased from the
# opposite direction. Requires the "outside" construction paired with an
# eligibility verb (open/available/accept/consider), not a bare "outside"
# anywhere in the text.
_EXCLUSION_OUTSIDE_RE = re.compile(
    # 2026-09 BUG FIX (explicit user-commissioned top-to-bottom audit,
    # confirmed via direct testing): both alternatives below never
    # verified that anything resembling a PLACE actually follows "outside
    # (of)?" — "We cannot accept applications from outside of our normal
    # review process," "...outside of our interview timeline," "...not
    # open to candidates outside of standard business hours," "...outside
    # of a reasonable commute to our values" were all hard-rejected even
    # though none of them name a geographic exclusion at all. Fixed the
    # same way as _EXTRA_RESTRICTIVE_PATTERNS/_RELOCATION_REQUIRED_RE
    # above: require the tail to start with an actual capitalized word, via
    # a scoped case-sensitive sub-pattern since this whole regex compiles
    # with re.I (which also case-folds a bare [A-Z] class).
    r"\b(?:not|isn'?t|is\s+not)\s+(?:currently\s+)?"
    r"(?:open|available|accepting\s+applications)\s+(?:to|for)\s+"
    r"(?:candidates|applicants)?[\w\s]{0,20}?\boutside\s+(?:of\s+)?(?:the\s+)?(?-i:[A-Z])[\w\s,&]{0,39}"
    r"|\b(?:cannot|can'?t|do\s+not|don'?t)\s+(?:accept|consider)\s+"
    r"(?:applications|candidates|applicants)\s+(?:located\s+|based\s+)?(?:from\s+)?outside\s+(?:of\s+)?"
    r"(?:the\s+)?(?-i:[A-Z])[\w\s,&]{0,39}"
    r"|\bunable\s+to\s+consider\s+(?:candidates|applicants)\s+(?:located\s+|based\s+)?outside\b"
    # 2026-09 BUG FIX (explicit user-commissioned adversarial fuzz test):
    # the REVERSED sentence order — "Applicants OUTSIDE the United States
    # ARE NOT ELIGIBLE" names "outside <place>" FIRST, then the exclusion
    # verb ("are not eligible") comes AFTER — every alternative above
    # requires the exclusion verb first, then "outside".
    r"|\boutside\s+(?:of\s+)?(?:the\s+)?[\w\s,&]{0,40}?\s+(?:are|is)\s+not\s+eligible\b",
    re.I,
)

# Part 3: a bare "the following countries only" / "restricted to the
# following countries" construction — the curated-list phrasing itself,
# independent of whether the enumerated list happens to be long. Sibling to
# _COUNTRY_WHITELIST_PHRASE_RE above, kept separate since these are a
# distinct phrasing family (explicit "only"/"restricted" wording rather than
# a "can verify right to work" screening-flow phrasing).
_COUNTRY_LIST_ONLY_RE = re.compile(
    r"\bthe\s+following\s+countries\s+only\b"
    r"|\brestricted\s+to\s+(?:the\s+)?following\s+countries\b"
    r"|\bonly\s+open\s+to\s+(?:candidates|applicants)\s+in\s+(?:the\s+)?following\s+countries\b"
    r"|\bwe\s+only\s+hire\s+in\s+the\s+following\s+countries\b"
    # 2026-09 ROUND 6 (explicit user-provided restrictive-language
    # taxonomy): "we CAN only hire" (an extra modal the original pattern
    # above didn't allow) and the sibling "employ" verb.
    r"|\bwe\s+can\s+only\s+(?:hire|employ)\s+in\s+the\s+following\s+countries\b"
    # "we currently hire/employ in the following countries" — a bare
    # declarative statement of the list, no "only"/"restricted" wording,
    # but — same reasoning already applied to
    # _COUNTRY_WHITELIST_PHRASE_RE's "one of the following countries"
    # above — "the following countries" is itself an inherently closed-list
    # construction in practice, not an illustrative example.
    r"|\bwe\s+currently\s+(?:hire|employ)\s+in\s+the\s+following\s+countries\b"
    # "eligible countries/locations are limited to/include only X".
    r"|\beligible\s+(?:countries|locations)\s+(?:are\s+)?limited\s+to\b"
    r"|\beligible\s+(?:countries|locations)\s+include\s+only\b"
    # "we accept applicants/candidates only from X" / "applications
    # (are) (accepted/open) only from X" / "applications are
    # limited/restricted to X".
    r"|\bwe\s+accept\s+(?:applicants|candidates)\s+only\s+from\b"
    r"|\bapplications?\s+(?:are\s+)?(?:accepted|open)\s+only\s+(?:from|in)\b"
    r"|\bapplications?\s+only\s+accepted\s+from\b"
    r"|\bapplications?\s+(?:are\s+)?(?:limited|restricted)\s+to\b",
    re.I,
)

# 2026-09 ROUND 6 (explicit user-provided restrictive-language taxonomy):
# "hiring/employment is limited/restricted to <place>", "we hire/employ/
# recruit only/exclusively in <place>", "remote is available/restricted/
# limited only in <place>", "this remote role is only available in
# <place>" — all place-anchored (unlike _COUNTRY_LIST_ONLY_RE above, which
# is anchored on the "following countries" PHRASING itself, these name the
# actual place), so reuse _COUNTRY_AUTH_NAMES_RE_FRAGMENT the same way
# _COUNTRY_BASED_RESTRICTION_RE does. Sibling to that regex, kept separate
# since these are an "eligibility IS restricted to X" shape rather than a
# "candidate must reside/be based in X" shape.
_HIRING_LIMITED_TO_PLACE_RE = re.compile(
    r"\b(?:hiring|employment)\s+(?:is\s+)?(?:limited|restricted)\s+to\s+(?:the\s+)?"
    + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bwe\s+(?:hire|employ|recruit)\s+(?:only|exclusively)\s+in\s+(?:the\s+)?"
    + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bremote\s+(?:is\s+|positions?\s+are\s+|work\s+is\s+)?(?:available\s+only|"
    r"restricted\s+to|limited\s+to|only\s+available)\s+(?:in\s+)?(?:the\s+)?"
    + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bthis\s+remote\s+(?:role|position|opportunity)\s+is\s+only\s+available\s+in\s+"
    r"(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bavailable\s+(?:only|exclusively)\s+within\s+(?:the\s+)?"
    + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    # NOTE: deliberately NOT a bare "restricted/limited to <place>" with no
    # subject — real postings use that exact shape for things unrelated to
    # hiring eligibility (e.g. "international travel is limited to Germany
    # and France for client visits"), which would false-positive-reject a
    # job that says nothing about candidate location. "geographically"
    # anchors it to an eligibility statement instead; "hiring is limited/
    # restricted to X" and "employment is limited/restricted to X" above
    # already cover the other common real subjects.
    r"|\b(?:geographically\s+(?:restricted|limited)|(?:restricted|limited)\s+geographically)\s+to\s+"
    r"(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    # 2026-09 BUG FIX (explicit user-commissioned adversarial fuzz test):
    # "This position IS RESTRICTED TO candidates in <country>" — a real,
    # distinct subject shape ("this position/role IS restricted to
    # candidates in X") from "hiring/employment is limited/restricted to
    # X" above, which has no "candidates in" clause at all.
    r"|\b(?:this\s+)?(?:position|role|job)\s+is\s+restricted\s+to\s+candidates?\s+in\s+"
    r"(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    # 2026-09 BUG FIX (explicit user-commissioned adversarial fuzz test,
    # ~4,820 generated restrictive phrasings): a bare, standalone "<place>
    # only." — "US only.", "APAC only.", "Germany only." — with NO subject
    # at all (not "hiring is"/"we hire"/"remote is") was leaking through
    # every check above and the entity/exclusion family below. This is a
    # DIFFERENT risk profile from the bare "restricted/limited to <place>"
    # shape the NOTE above deliberately excludes (real risk: "travel is
    # limited to Germany for client visits") — "<place> only" as its own
    # short, self-contained clause (end-anchored: only fires right before
    # a sentence boundary or end of text, never mid-sentence where a
    # legitimate non-eligibility reading like "Germany only for client
    # visits" could continue past it) essentially never appears in real
    # postings for anything other than a hiring-eligibility restriction —
    # it's the exact same shape a LOCATION FIELD value like "US only"
    # already gets hard-rejected for (see _keyword_classify_location_
    # detail's own residue check), just appearing as JD body text instead
    # of the structured field.
    r"|\b" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\s+only\s*(?=[.!?\n]|$)",
    re.I,
)


def has_entity_or_exclusion_restriction_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does this job's description state
    — via a company-capability statement ("we don't have a legal entity in
    your country", "can only employ where we have an EOR/PEO"), an explicit
    exclusion ("not open to candidates outside the US"), a curated-list
    phrasing ("the following countries only"), or a place-anchored "hiring
    is limited to X"/"remote is only available in X" statement — that
    eligibility is restricted to a specific place or list, even though none
    of these phrasings look like the classic "must reside/be based in
    <country>" shape the other hard overrides already catch? See the module
    comments above _ENTITY_PAYROLL_RESTRICTION_RE, _EXCLUSION_OUTSIDE_RE,
    _COUNTRY_LIST_ONLY_RE, and _HIRING_LIMITED_TO_PLACE_RE. Per the user's
    explicit design constraint, none of these fire on bare "EOR"/"PEO"/
    "payroll"/"country"/"region" alone — only on the full construction.

    2026-09 ROUND 6: _HIRING_LIMITED_TO_PLACE_RE is checked sentence-by-
    sentence with a _has_multi_region_breadth guard (same reasoning as
    has_hard_country_specific_auth_signal above it in this file) — "hiring
    is limited to LATAM, EMEA, and APAC" names a single region-fragment
    word ("latam") but is actually broad multi-region evidence, not a
    single-place restriction, and must not be hard-rejected.
    """
    desc = job.get("description_snippet") or ""
    text = desc + " " + (job.get("title") or "")
    if not text.strip():
        return False
    if _ENTITY_PAYROLL_RESTRICTION_RE.search(text) or _COUNTRY_LIST_ONLY_RE.search(text):
        return True
    for sentence in _split_into_sentences(text):
        if not sentence.strip():
            continue
        if _TEAM_OR_COMPANY_CONTEXT_RE.search(sentence):
            continue
        if _EXCLUSION_OUTSIDE_RE.search(sentence):
            return True
        if _has_multi_region_breadth(sentence):
            continue
        if _HIRING_LIMITED_TO_PLACE_RE.search(sentence):
            return True
    return False


# Part 4: timezone wording TIED TO a residence/location requirement — per
# the user's explicit distinction, "must have significant overlap with our
# team's working hours" is SAFE (describes scheduling, not eligibility),
# but "must be located/based/reside in a US timezone" or "in the same
# timezone as our HQ" is RESTRICTIVE (uses a residence verb, just naming a
# timezone instead of a country). The regex only fires on a residence verb
# immediately governing "timezone" — a sentence that also contains "overlap"
# is guarded off entirely, since every "overlap"-based phrasing this
# project has seen is the safe scheduling-only shape, never a residence
# requirement.
#
# 2026-09 ROUND 5 (explicit, urgent user correction re: EMEA): "emea" was
# REMOVED from the optional region-qualifier list below — this project
# treats EMEA the same as the Africa continent, an ACCEPTED broad-hiring
# tier, not a restriction (see the ROUND 4 comment above
# _COUNTRY_AUTH_NAMES_RE_FRAGMENT). "must be located in an EMEA timezone"
# is functionally the same statement as "must be based in EMEA" — which is
# explicitly allowed — so it must not be treated as restrictive here
# either, for the same consistency reason "africa"/"emea" were pulled out
# of every other restrictive-region fragment in this file.
_TIMEZONE_LOCATION_RE = re.compile(
    r"\b(?:located|based|reside|residing|resides|live|living|lives|work(?:ing)?)\s+"
    r"(?:in|within)\s+(?:a\s+|an\s+|the\s+)?"
    # NOTE: the generic "[a-z]{2,4}" catch-all below (for arbitrary 2-4
    # letter timezone abbreviations this list doesn't explicitly name)
    # would otherwise still swallow "emea"/"africa" as if they were just
    # another short code — the negative lookaheads keep those excluded
    # consistent with removing them from the explicit list above.
    r"(?:us|u\.s\.|uk|u\.k\.|north american?|european?|apac|latam|"
    r"gmt|est|cst|mst|pst|cet|(?!emea\b)(?!africa\b)[a-z]{2,4})?\s*time\s*zone\b"
    r"|\b(?:located|based|reside|residing|resides|live|living|lives)\s+"
    r"in\s+the\s+same\s+time\s*zone\s+as\b"
    # 2026-09 BUG FIX (explicit user-commissioned adversarial fuzz test):
    # bare DECLARATIVE timezone statements with no residence verb at all
    # — "US business hours only.", "Pacific Time zone required." — a
    # distinct shape from every alternative above, which all require a
    # residence/work verb ("located/based/reside/work... in/within").
    r"|\b(?:us|u\.s\.|uk|u\.k\.|north american?|european?|apac|latam|pacific|eastern|"
    r"central|mountain|gmt|est|cst|mst|pst|cet)\s+(?:business\s+)?hours\s+only\b"
    r"|\b(?:gmt|est|cst|mst|pst|cet|pacific|eastern|central|mountain)\s*time\s*"
    r"(?:zone)?\s+required\b",
    re.I,
)

# Part 5: a narrow, explicit relocation REQUIREMENT naming a specific place
# — "willing to relocate to the United States" / "must relocate to our
# Austin office" — distinct from a company merely mentioning relocation
# assistance/packages exist (which says nothing about restricting who may
# apply from where and is intentionally NOT matched here).
_RELOCATION_REQUIRED_RE = re.compile(
    # 2026-09 BUG FIX (explicit user-commissioned top-to-bottom audit,
    # confirmed via direct testing): the generic "[\w\s,]{0,40}" tail
    # never required the destination to actually look like a place —
    # "willing to relocate to a new city for this role," "...to wherever
    # opportunity takes you," "...to pursue growth opportunities," "...to
    # advance your career" were all being hard-rejected as if a SPECIFIC
    # place had been named, when none of them name one at all (relocating
    # "to a new city" says nothing about WHICH city). Same fix family as
    # _EXTRA_RESTRICTIVE_PATTERNS/_ROLE_SPECIFIC_PLACE_RE/_CANDIDATE_
    # PLACE_RE above: require the tail to start with an actual capitalized
    # word via a scoped case-sensitive sub-pattern (this whole regex
    # compiles with re.I, which ALSO case-folds a bare [A-Z] class, so
    # that scoping is load-bearing, not cosmetic).
    r"\b(?:willing|must\s+be\s+willing|required|willingness)\s+to\s+relocate\s+to\s+"
    r"(?:the\s+|our\s+)?(?-i:[A-Z])[\w\s,]{0,39}"
    r"|\bmust\s+relocate\s+to\s+(?:the\s+|our\s+)?(?-i:[A-Z])[\w\s,]{0,39}"
    r"|\brequires?\s+relocation\s+to\s+(?:the\s+|our\s+)?(?-i:[A-Z])[\w\s,]{0,39}",
    re.I,
)

# Part 6: hyphenated "<place>-based candidates/applicants only" — the same
# eligibility restriction as _COUNTRY_BASED_RESTRICTION_RE's "based in
# <place>" shape, just written as a compound adjective ("US-based
# candidates only") instead of a verb phrase. Requires the trailing
# "only" (or leading "only") so a merely descriptive "we have a US-based
# team" doesn't fire — that's already handled by the team-context guard
# below regardless, but the "only" requirement keeps this regex itself
# narrow.
_HYPHENATED_BASED_ONLY_RE = re.compile(
    r"\b(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")[\s\-]based\s+"
    r"(?:candidates?|applicants?|employees?|team\s+members?)?\s*only\b"
    r"|\bonly\s+(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")[\s\-]based\s+"
    r"(?:candidates?|applicants?|employees?)\b",
    re.I,
)

# 2026-09 ROUND 6 (explicit user-provided restrictive-language taxonomy —
# flagged directly: "Your existing hyphenated detector specifically
# requires `only` in its main form, so it is deliberately narrower" — a
# major missing construction is the SAME hyphenated-place-based phrasing
# without "only" at all, in a "for <place>-based applicants" shape — "for
# US-based applicants", "for Canada-based candidates", "for UK-based
# employees"). Deliberately requires the leading "for" (not just a bare
# "<place>-based applicants" anywhere in text) — "for" is what turns this
# into an eligibility-defining statement ("this role is FOR ...") rather
# than a merely descriptive mention ("our largely US-based team..."),
# keeping it as narrow as the "only"-anchored version above.
_FOR_PLACE_BASED_RE = re.compile(
    r"\bfor\s+(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")[\s\-]based\s+"
    r"(?:candidates?|applicants?|employees?|team\s+members?)\b",
    re.I,
)

# 2026-09 BUG FIX (explicit user-commissioned top-to-bottom audit,
# confirmed via direct testing): "This role is NOT restricted to US-based
# candidates only" is an explicitly INCLUSIVE statement (openly reassuring
# applicants the role ISN'T US-only) but _HYPHENATED_BASED_ONLY_RE matches
# "US-based candidates only" inside it regardless, with no check for a
# preceding negation — hard-rejecting a job that was explicitly telling
# candidates the opposite. Same bug class as the sponsorship "without"
# fix above: a negation-shaped clause being read for its positive claim
# instead of what it's actually negating.
_HYPHENATED_BASED_NEGATION_RE = re.compile(
    r"\bnot\s+(?:restricted|limited|exclusive(?:ly)?)\s+to\b"
    r"|\bisn'?t\s+(?:restricted|limited)\s+to\b"
    r"|\bno\s+longer\s+(?:restricted|limited)\s+to\b"
    r"|\bnot\s+just\s+(?:open\s+)?(?:for|to)\b",
    re.I,
)


def has_timezone_relocation_or_hyphenated_restriction_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does this job's description state
    a residence-verb-governed timezone requirement ("must be located in a
    US timezone" — see _TIMEZONE_LOCATION_RE's module comment for why this
    is distinct from safe "overlap" scheduling wording), an explicit
    relocation requirement naming a place (_RELOCATION_REQUIRED_RE), a
    hyphenated "<place>-based candidates only" construction
    (_HYPHENATED_BASED_ONLY_RE), or the same hyphenated shape without
    "only" in a "for <place>-based applicants" construction
    (_FOR_PLACE_BASED_RE)? Checked sentence-by-sentence with the same
    team/office-context guard and global-evidence guard as
    has_hard_country_based_restriction_signal above, since all of these
    phrasings could in principle appear in a sentence describing the
    COMPANY's own timezone/location rather than a candidate requirement.
    _FOR_PLACE_BASED_RE additionally gets a _has_multi_region_breadth
    guard (the others in this function predate that guard and aren't
    touched here) since "for US-based, UK-based, or Germany-based
    applicants" names 3 places but is broad multi-region hiring, not a
    single-place restriction.
    """
    desc = job.get("description_snippet") or ""
    text = desc + " " + (job.get("title") or "")
    if not text.strip():
        return False
    for sentence in _split_into_sentences(text):
        if not sentence.strip():
            continue
        if "overlap" in sentence.lower():
            continue
        if _text_has_global_evidence(sentence):
            continue
        # 2026-09 ROUND 5 (explicit user-provided EMEA-wide hiring lingo
        # list): same reasoning as has_hard_country_based_restriction_signal
        # above — EMEA/Africa evidence in the sentence is as strong a
        # "don't reject" guard as explicit global evidence.
        if _text_has_africa_or_emea_evidence(sentence):
            continue
        # NOTE: the team/office-context guard used elsewhere in this file
        # (_TEAM_OR_COMPANY_CONTEXT_RE) is deliberately NOT applied to
        # _TIMEZONE_LOCATION_RE / _RELOCATION_REQUIRED_RE here — both
        # legitimately reference "our office"/"our HQ team" as the
        # relocation target or comparison basis ("relocate to our Austin
        # office", "same timezone as our headquarters team"), and both
        # regexes already require a residence/relocation verb governing the
        # place, which a genuine company-describes-itself sentence
        # ("our HQ is in Austin") doesn't have.
        if _TIMEZONE_LOCATION_RE.search(sentence) or _RELOCATION_REQUIRED_RE.search(sentence):
            return True
        # 2026-09 BUG FIX: skip a sentence that explicitly NEGATES the
        # hyphenated restriction ("not restricted to US-based candidates
        # only") before testing either hyphenated pattern — see
        # _HYPHENATED_BASED_NEGATION_RE's own module comment.
        if _HYPHENATED_BASED_NEGATION_RE.search(sentence):
            continue
        # _HYPHENATED_BASED_ONLY_RE ("US-based candidates only") has no
        # office/team-referencing shape, so the team-context guard is kept
        # here to stay consistent with the rest of the file's pattern.
        if not _TEAM_OR_COMPANY_CONTEXT_RE.search(sentence) and _HYPHENATED_BASED_ONLY_RE.search(sentence):
            return True
        if (not _TEAM_OR_COMPANY_CONTEXT_RE.search(sentence)
                and not _has_multi_region_breadth(sentence)
                and _FOR_PLACE_BASED_RE.search(sentence)):
            return True
    return False


# Disqualifying workplace_type tokens: a scraper-reported physical-presence
# requirement (Hybrid / On-site / In-office / In-person). Real values seen
# across ats_scrapers.py's ~15 populating call sites: Lever ("remote",
# "hybrid", "on-site" — lowercase enum), JOIN ("ONSITE", "REMOTE", "HYBRID"
# — uppercase enum), Ashby/Rippling/Recruitee/SmartRecruiters/Pinpoint
# ("Remote"/"Hybrid"/"Onsite"/"" free text or boolean-derived), Workday
# ("remoteType", company-specific free text), Oracle Cloud HCM
# ("WorkplaceTypeDisplay", free text), Zoho ("Remote_Job"/"Work_Mode").
# Personio's field is mislabeled (it's actually the XML feed's
# "schedule" — full-time/part-time — not a real workplace-type signal);
# left as-is here since those values never match either pattern below,
# so they're harmless no-ops for this check, not false signals.
_NON_REMOTE_WORKPLACE_RE = re.compile(
    r"\b(hybrid|on[\s\-]?site|in[\s\-]?office|in[\s\-]?person)\b", re.I
)
# 2026-10 (adversarial-coverage pass): "On-Premise"/"On-Premises"/"On
# Premise" — enterprise/IT shorthand some companies put in a structured
# workplace_type FIELD instead of "On-site" (same physical-presence
# meaning). Deliberately a SEPARATE regex used only by
# has_non_remote_workplace_type (a dedicated field whose whole job is to
# say where the work happens): _NON_REMOTE_WORKPLACE_RE itself is also
# scanned against job TITLES, where "On-Premise" very commonly names the
# PRODUCT ("Account Executive - On-Premise Software"), not an attendance
# requirement, and widening that shared regex would have mass-rejected
# those.
_NON_REMOTE_WORKPLACE_FIELD_RE = re.compile(
    r"\b(hybrid|on[\s\-]?site|in[\s\-]?office|in[\s\-]?person|on[\s\-]?premises?)\b", re.I
)
_REMOTE_WORKPLACE_RE = re.compile(r"\bremote\b", re.I)


def has_non_remote_title_signal(job: dict) -> bool:
    """2026-09 (explicit user request): a job whose TITLE itself carries a
    physical-presence qualifier — "Account Manager (Hybrid)", "Customer
    Success Manager - Onsite", "Project Manager (In-Office)" — is a real,
    company-stated hiring-scope signal exactly like has_non_remote_
    workplace_type's structured workplace_type field, just expressed in
    the title text instead of a dedicated field. Same hard override, same
    reasoning: a company that tags the ROLE ITSELF as hybrid/on-site is
    telling you this specific posting requires physical presence,
    regardless of what a separate location/workplace_type field says (or
    doesn't say — this also catches titles with no other location signal
    at all, which previously fell through to 'unsure' and got kept, e.g.
    the GFL Environmental Indianapolis, IN case that prompted this).
    Same "remote alongside it" exception as the workplace_type check: a
    title mentioning both ("Hybrid/Remote") is not excluded here — that's
    not a hybrid-only requirement."""
    title = job.get("title") or ""
    if not isinstance(title, str) or not title:
        return False
    if _REMOTE_WORKPLACE_RE.search(title):
        return False
    return bool(_NON_REMOTE_WORKPLACE_RE.search(title))


def has_non_remote_workplace_type(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does the scraper-captured
    workplace_type field say this specific posting requires physical
    presence (Hybrid/On-site/In-office/In-person), regardless of what the
    bare `location` field claims?

    Real case this closes: an Infor posting on Pinpoint
    (careers.infor.com/en/postings/769bef61-...) had location="Remote"
    but Pinpoint's own `workplace_type_text` field said "Hybrid" — the
    posting page shows this as a "Workplace type: Hybrid" badge, with NO
    country-restriction prose anywhere on the page (verified directly
    against the live posting, not inferred). classifier.py was capturing
    workplace_type on every scraper but never reading it, so the
    "Remote" location field alone let this straight through as
    match_global. workplace_type is scraper/platform-STRUCTURED data
    (an explicit field the ATS itself populates), not prose the AI has
    to interpret — same category of signal as the sponsorship hard
    override above, so it gets the same treatment: a hard, pre-AI
    override that doesn't depend on the AI getting it right.

    A job whose workplace_type lists BOTH a disqualifying value and
    "remote" (e.g. Rippling's "Hybrid, Remote" when a company posts one
    requisition across multiple locations of different types) is NOT
    excluded here — that's a genuine remote option existing alongside
    on-site ones, not a hybrid-only requirement. Only fires when a
    disqualifying token is present with no remote token alongside it.
    Blank/missing workplace_type (the common case — most scrapers don't
    populate it) or a value that matches neither pattern (e.g.
    Personio's mislabeled "Full-time") is not a signal either way and
    falls through to the existing location-keyword/AI classification.
    """
    wt = job.get("workplace_type", "")
    if not wt or not isinstance(wt, str):
        return False
    if _REMOTE_WORKPLACE_RE.search(wt):
        return False
    return bool(_NON_REMOTE_WORKPLACE_FIELD_RE.search(wt))


# 2026-09 (explicit user report, two real postings): neither
# has_non_remote_workplace_type (structured workplace_type FIELD only) nor
# has_non_remote_title_signal (TITLE only) nor has_office_attendance_signal
# (narrowly scoped to "N days/week in office" attendance-frequency
# phrasing) catches a workplace-type LABEL sitting as plain body text in
# the description itself:
#   - Kraft Heinz's Eightfold posting (kraftheinz.eightfold.ai/careers/
#     job/1970324837481684): the page explicitly said "Hybrid Working"
#     with a map-pin location of "Australia" — location field this
#     project stored was bare "Remote", no workplace_type at all.
#   - viaquestinc.com's Paycor/Gnewton posting: the page reads "Remote
#     Status: On-Site" as plain table-cell text (`<td id="gnewtonJob
#     RemoteStatus"><b>Remote Status:</b> On-Site</td>`, verified live)
#     that this project's Crawl II scraper never mapped to a
#     workplace_type field at all.
# Explicit user request: "expand location wording to include: location
# status, workplace setting:, location:, work from:, and many more."
# Label vocabulary here intentionally excludes a bare "location:" (that
# one's for _DESC_LOCATION_LABEL_RE's PLACE-recovering job above, not a
# disqualifying-VALUE check — "Location: Bowling Green, OH" isn't itself
# hybrid/onsite evidence, the city name is what disqualifies it, via the
# normal keyword path once _enrich_location_from_description recovers it).
_WORKPLACE_LABEL_RE = re.compile(
    r"\b(?:remote\s*status|workplace\s*(?:setting|type)|location\s*(?:status|type)|"
    r"work\s*(?:from|mode|arrangement|style|site)|working\s*(?:arrangement|style|model))"
    r"\s*[:\-]\s*(?:&nbsp;|\s)*([^\n.;|]{1,40})",
    re.I,
)
# Broader than _NON_REMOTE_WORKPLACE_RE by one token ("office" bare) —
# deliberately scoped to ONLY the short captured label-VALUE text above
# (never the full description), where a terse one-word answer like "Work
# from: Office" is common and unambiguous in that narrow context, unlike
# scanning the whole JD for the bare word "office" (which would false-
# positive on "our office culture", "back-office support", etc.).
_WORKPLACE_LABEL_VALUE_RE = re.compile(
    r"\b(hybrid|on[\s\-]?site|in[\s\-]?office|in[\s\-]?person|office)\b", re.I
)
# A small set of unambiguous standalone phrases — deliberately NOT a bare
# "\bhybrid\b" scan (too overloaded: "hybrid cloud", "hybrid event",
# "hybrid car" are all common, unrelated JD boilerplate) — only a
# workplace-descriptor word directly paired with "working"/a role-model
# word, which has no plausible non-workplace reading.
_STANDALONE_NON_REMOTE_PHRASE_RE = re.compile(
    r"\bhybrid\s*working\b|\bworking\s*hybrid\b|\bon[\s\-]?site\s*working\b|"
    r"\bin[\s\-]?office\s*working\b|\bin[\s\-]?person\s*working\b|"
    r"\bhybrid\s*work\s*(?:model|arrangement|environment|policy|schedule)\b"
    # 2026-09 NEW (explicit user report, real posting: European Dynamics'
    # Workable "Customer relationship manager" listing, Brussels — the
    # exact live text reads "The work will be carried out either in the
    # company's premises or on site at customer premises," a physical-
    # workplace descriptor with no "Hybrid"/"Onsite" label anywhere and no
    # "...working" pairing either — confirmed via direct fetch of the real
    # page, since this job was admitted at Rank 4b despite being fully
    # onsite). "Premises" is a deliberately narrow anchor word here —
    # formal, near-unambiguous for "a physical business location" (unlike
    # bare "office", which risks "back-office support"-style false
    # positives) — so pairing it with "on-site at" or "carried out
    # in/at ... premises" is safe without the "...working" requirement
    # the other alternatives above need.
    r"|\bon[\s\-]?site\s+at\s+(?:the\s+|our\s+|your\s+|customer\s+|client\s+)?"
    r"[\w\s]{0,20}?\bpremises\b"
    r"|\b(?:carried\s+out|performed|conducted)\s+(?:either\s+)?(?:in|at)\s+"
    r"(?:the\s+|our\s+)?(?:company'?s?\s+)?premises\b"
    # 2026-09 NEW (explicit user report, real posting: CentralReach's
    # Greenhouse "Customer Success Lead" — "We prefer candidates who can
    # work in a hybrid capacity from one of our corporate offices..." —
    # a genuinely new phrasing variant, "hybrid CAPACITY" instead of
    # "hybrid working"/"hybrid work model". Defense-in-depth: this exact
    # posting is already excluded via has_hard_country_based_restriction_
    # signal (see _COUNTRY_BASED_RESTRICTION_RE's "other U.S. states" fix)
    # regardless, but the phrase itself is worth recognizing on its own.
    r"|\bhybrid\s+capacity\b"
    # 2026-09 BUG FIX (explicit user-commissioned adversarial fuzz test):
    # "This is an on-site role", "This position is onsite" -- a workplace-
    # type word paired directly with "role"/"position"/"job" (either
    # order), with no "...working" pairing and no labeled field at all.
    # Same narrow-word-pairing safety reasoning as the "...working"
    # alternatives above (never a bare "\bhybrid\b"/"\bonsite\b" scan on
    # its own).
    r"|\bon[\s\-]?site\s+(?:role|position|job)\b|\bin[\s\-]?office\s+(?:role|position|job)\b|"
    r"\bin[\s\-]?person\s+(?:role|position|job)\b|"
    r"\b(?:role|position|job)\s+is\s+(?:on[\s\-]?site|in[\s\-]?office|in[\s\-]?person)\b",
    re.I,
)


def has_non_remote_labeled_text_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does the job's raw description
    or title carry a workplace-type LABEL (see _WORKPLACE_LABEL_RE) whose
    value is disqualifying (Hybrid/On-site/In-office/In-person/Office),
    or an unambiguous standalone phrase like "Hybrid Working"
    (_STANDALONE_NON_REMOTE_PHRASE_RE)? See this function's module
    comment above for the two real postings this closes. Same "remote
    alongside it" exception as has_non_remote_workplace_type/
    has_non_remote_title_signal: a labeled value that also mentions
    "remote" (e.g. "Work mode: Hybrid or Remote") is NOT excluded here —
    that's a genuine remote option, not a hybrid-only requirement."""
    desc = job.get("description_snippet") or ""
    title = job.get("title") or ""
    text = f"{desc} {title}" if isinstance(desc, str) and isinstance(title, str) else ""
    if not text.strip():
        return False

    if _STANDALONE_NON_REMOTE_PHRASE_RE.search(text):
        return True

    for m in _WORKPLACE_LABEL_RE.finditer(text):
        value = m.group(1)
        if _REMOTE_WORKPLACE_RE.search(value):
            continue
        if _WORKPLACE_LABEL_VALUE_RE.search(value):
            return True
    return False


# ── Rank 4 (2026-09, explicit user request — classification revamp
# Phase 2): CS/AM-only admission for a BARE country/region/continent
# location (or a title/JD naming one while the location field is some
# other concrete place) that the pipeline above would otherwise reject
# outright with no further look — as long as neither the description
# nor the application questions confirm an actual country/continent-tied
# restriction (work authorization, residence, permit, visa sponsorship
# tied to a place, enumerated state list, hybrid/on-site, timezone/
# relocation, entity/exclusion wording, etc.).
#
# Deliberately a SEPARATE, additive pass, not a change to any hard-
# override function above: those are unconditional, all-role, all-ATS
# checks proven over many real postings, and continue to apply exactly
# as before for every job of every role/platform, Rank 4 candidates
# included (see _RANK4_GENUINE_RESTRICTION_CHECKS below — the same
# functions, reused as-is). Only the "location/title bare-names a single
# specific place with nothing further" signal is treated differently
# here, and only for CS/AM roles from a platform confirmed (2026-09 live
# audit, ats_capability_probe-style: 25-30 real companies sampled per
# platform through the actual production pipeline) to reliably return
# BOTH a location and application-question value on the same job —
# Workday/iCIMS/ADP/BambooHR/Oracle Cloud HCM/SmartRecruiters/
# Zoho/HRMDirect/Taleo/Paylocity/JOIN/BreezyHR/Jobvite all either have a
# confirmed-broken/auth-walled question fetcher or too high a missing-
# questions rate to trust for a tier whose entire admission logic
# depends on application questions being genuinely ABSENT, not merely
# unfetched. Per explicit user instruction, this tier is Crawl I only —
# Crawl II's heuristic in-house scraper and Crawl III's stapply.ai CSV
# consumer have no reliable application-question signal at all, so
# neither can ever satisfy this tier's own admission requirement.
RANK4_ELIGIBLE_ATS = {
    "Greenhouse", "Workable", "Personio", "JazzHR", "Teamtailor",
    "Recruitee", "Lever", "PageUp", "isolvedhire", "Pinpoint",
    "Rippling", "Ashby",
}
# 2026-10: Ashby added once its question fetcher worked again (GraphQL; the old
# posting API returned 401). Live probe, 30 random boards / 72 jobs: location on
# 72/72, form fetch OK on 72/72, >=1 non-boilerplate question on 58/72.
# 2026-10: Eploy removed — no dedicated question fetcher (only the
# generic wild fallback, which is unreliable for Eploy's server-rendered
# HTML forms), so Rank 4's core premise ("we confirmed the questions are
# silent on this") cannot be trusted for Eploy jobs.

PRIORITY_MIXED_COUNTRY = "4a"  # bare country/region/continent location,
                       # no restrictive tie confirmed
PRIORITY_MIXED_SIGNAL = "4b"   # title/description names a region/country
                       # while the location field itself is a different,
                       # more specific place (e.g. a city) — no
                       # restrictive tie confirmed

# Rank-4-specific guard (2026-09, explicit user instruction: "you should
# expand these restrictive class of questions"). None of the existing hard
# overrides above catch a sponsorship/work-permit/residency question or
# statement that ties itself to a SPECIFIC named country —
# has_hard_country_specific_auth_signal's _COUNTRY_AUTH_RE only matches
# "authorized/eligible/permitted TO WORK in <country>" and citizenship
# phrasing, not sponsorship/permit/residency phrasing. This is exactly the
# distinction the user's own Rank-4b example turns on (Notabene: "Will you
# now or in the future require sponsorship for a work visa?" is fine
# ONLY because it names no country — the identical question naming a
# country is a genuine restriction and must exclude the job).
_RANK4_COUNTRY_TIED_RESTRICTION_RE = re.compile(
    r"\b(?:visa\s*)?sponsorship\b[^.!?\n]{0,60}\b(?:for|to|in|within)\b[^.!?\n]{0,30}\b"
    + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:"
    + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\b" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b[^.!?\n]{0,60}\b(?:visa\s*)?sponsorship\b"
    r"|\bsponsor\w*\s+(?:a\s+|your\s+)?(?:work\s+)?visa\b[^.!?\n]{0,60}\b(?:for|to|in|within)\b[^.!?\n]{0,30}\b"
    + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:"
    + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bwork\s+permit\b[^.!?\n]{0,60}\b(?:for|to|in|within)\b[^.!?\n]{0,30}\b" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bresidenc(?:e|y)\b[^.!?\n]{0,60}\b(?:for|to|in|within)\b[^.!?\n]{0,30}\b" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    # 2026-09 BUG FIX (explicit user-commissioned adversarial fuzz test):
    # "Do you have a valid work visa for the United States?" -- bare
    # "work visa," no "sponsorship" word at all, which every alternative
    # above requires.
    r"|\bwork\s+visa\b[^.!?\n]{0,60}\b(?:for|to|in|within)\b[^.!?\n]{0,30}\b" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    # 2026-09 BUG FIX (explicit user report, real posting: Pave's
    # Greenhouse "Account Manager" listing — "Do you now, or will you in
    # the future, require sponsorship for employment visa status (e.g.,
    # H-1B visa status, etc.) to work legally for our Company in the
    # United States?"): this is structurally the EXACT shape the module
    # comment above already calls disqualifying ("the identical question
    # naming a country is a genuine restriction") -- "sponsorship ... to
    # work ... in <country>" -- but every alternative above measures the
    # country's distance from "sponsorship" using a SINGLE preposition
    # immediately after "sponsorship" itself, and here the real country-
    # naming clause ("to work legally for our Company in the United
    # States") sits 100+ characters later, past an intervening "(e.g.,
    # H-1B visa status, etc.)" aside. Two distinct fixes bundled into one
    # new alternative: (1) anchor on "to work ... in <country>" directly
    # — the actual authorization-shaped clause — rather than a bare
    # preposition, since that's reliably present close to the country name
    # regardless of how verbose the preceding sponsorship clause is; (2)
    # use a plain `.` gap instead of this regex's usual `[^.!?\n]` filler,
    # since "e.g." and "etc." each contain a literal period that
    # `[^.!?\n]` refuses to cross even though neither one is an actual
    # SENTENCE boundary (_split_into_sentences's own split regex requires
    # the period to be followed directly by whitespace, which "e.g.," and
    # "etc.)" aren't) — this function already operates one already-split
    # sentence at a time, so there's no cross-sentence-bleed risk from
    # loosening the gap just here. A benefit-phrased equivalent ("we offer
    # visa sponsorship to work in Canada") still correctly falls through
    # to this same function's existing _RANK4_BENEFIT_FRAMING_RE/
    # _RANK4_REQUIREMENT_FRAMING_RE guard unaffected, since that check
    # runs on the whole sentence after this regex already matches it.
    r"|\bsponsorship\b.{0,100}?\bto\s+work\s+(?:legally\s+)?(?:for\s+.{0,40}?)?"
    r"\bin\b\s*(?:the\s+)?" + _PLACE_CHAIN_PREFIX_FRAGMENT + r"(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b",
    re.I,
)


_RANK4_BENEFIT_FRAMING_RE = re.compile(
    r"\bwe\s+(?:can\s+|will\s+|would\s+)?(?:offer|provide|help|assist|support)\b"
    r"|\b(?:relocation|visa|sponsorship|residency|immigration)\s+(?:assistance|support|help)\b"
    r"|\bhelp(?:ing)?\s+(?:you\s+|candidates?\s+)?(?:secur\w*|obtain\w*|get\w*)\b",
    re.I,
)
_RANK4_REQUIREMENT_FRAMING_RE = re.compile(
    r"\bmust\b|\brequire[sd]?\b|\bneed(?:s|ed)?\s+to\b|\bcurrently\s+(?:hold|have)\b"
    r"|\bdo\s+you\s+(?:have|hold|currently)\b|\bare\s+you\b|\bwill\s+you\b"
    r"|\bable\s+to\s+provide\b|\bvalid\b",
    re.I,
)


def has_country_tied_sponsorship_permit_residency_signal(job: dict) -> bool:
    """Universal hard override (applies to every rank — 1/2/3a/3b/4) —
    sponsorship/work-permit/residency phrasing tied to a NAMED country in
    the same sentence. Distinct phrasing family from has_hard_country_
    specific_auth_signal's _COUNTRY_AUTH_RE (that one only matches
    "authorized/eligible/entitled/permitted TO WORK in <country>" and
    citizenship phrasing — not sponsorship/permit/residency phrasing).
    See _RANK4_COUNTRY_TIED_RESTRICTION_RE's module comment for the real
    posting that originally motivated the regex itself.

    2026-09 BUG FIX (explicit user-commissioned adversarial fuzz test,
    ~4,820 generated restrictive phrasings run directly against the
    deterministic pipeline): this exact regex already existed and was
    already battle-tested — but was ONLY ever wired into Rank 4's
    exclusion checks (_rank4_has_country_tied_restrictive_question,
    below, now a thin wrapper around this function). Every OTHER job —
    any non-CS/AM role, any ATS not in RANK4_ELIGIBLE_ATS, or simply a
    bare-Remote job that never reaches Rank 4 logic at all — had NO
    deterministic check for this entire phrasing family and fell through
    to the AI stage instead, directly contradicting this project's own
    core design principle ("regex decides, the LLM never does" for
    anything that doesn't genuinely need judgment). Confirmed live: "Do
    you hold a valid work permit for the United States?", "require visa
    sponsorship to work in the United States?", "maintain residence in
    the United States?" and dozens of close variants were all reaching
    'unsure'/bare_remote instead of a deterministic no_match for every
    job outside Rank 4's narrow gate.

    Safe to apply universally with no extra "is a place already named"
    gate (unlike has_referential_auth_question_with_named_place_signal)
    — the country name is always required directly in the same sentence
    as the restrictive phrase, never implied from elsewhere.

    Checked sentence-by-sentence with the same multi-region-breadth
    exception every other hard override uses (a sentence naming 2+
    distinct business regions together is broad reach, not a
    single-country tie).

    2026-09 false-positive fix (caught by this session's own adversarial
    test pass, not a guess): "We offer full relocation assistance and
    help securing residency permits for candidates moving to Germany"
    matched the residency+country pattern even though it's a BENEFIT the
    company is offering, not a requirement the candidate must already
    meet — the exact same "topic-adjacent word standing in for the thing
    that's actually disqualifying" bug class this file's other checks
    (has_hard_no_sponsorship_signal's negation requirement,
    _sponsorship_sentence_has_negative_signal's non-visa-sense guard)
    already fixed once each. A sentence with benefit/assistance framing
    and no actual requirement/question framing is not a restriction."""
    desc = job.get("description_snippet") or ""
    text = desc + " " + (job.get("title") or "")
    if not text.strip():
        return False
    for sentence in _split_into_sentences(text):
        if not sentence.strip():
            continue
        if _has_multi_region_breadth(sentence):
            continue
        if not _RANK4_COUNTRY_TIED_RESTRICTION_RE.search(sentence):
            continue
        if (_RANK4_BENEFIT_FRAMING_RE.search(sentence)
                and not _RANK4_REQUIREMENT_FRAMING_RE.search(sentence)):
            continue
        return True
    return False


def _rank4_has_country_tied_restrictive_question(job: dict) -> bool:
    """Rank-4-only guard — see has_country_tied_sponsorship_permit_
    residency_signal (the universal version, applied to every rank) for
    the shared sponsorship/permit/residency+named-country logic. This
    Rank-4-specific wrapper ADDS the referential-question check
    (_REFERENTIAL_AUTH_QUESTION_RE) on top: Rank 4 is only ever
    evaluating a job whose location already resolved to one specific
    bare country/region (that's this whole tier's premise), so a
    referential question ("authorized to work where this role is
    located") is automatically tied to that known place there — unlike
    the universal context, which needs has_referential_auth_question_
    with_named_place_signal's own "is a place actually named" gate
    instead."""
    desc = job.get("description_snippet") or ""
    text = desc + " " + (job.get("title") or "")
    if not text.strip():
        return False
    if _referential_auth_hit(text):
        return True
    return has_country_tied_sponsorship_permit_residency_signal(job)


# ── 2026-10: geography-binding APPLICATION QUESTIONS (explicit user report:
# Hightouch's Ashby form asked "Are you willing to relocate to NYC, San
# Francisco or Denver?" / "Are you authorized to work for any employer in the
# U.S?" while the posting said "location independent ... remote-first").
# Only "Application Question:" lines are read, never JD prose, so a JD that
# merely mentions a place can't trip it. A question counts when it BINDS the
# candidate to a place: work authorization / citizenship / visa tied to a
# place, residence, relocation, commuting, on-site or hybrid attendance,
# local licensing, a named time zone's working hours, or a security
# clearance. Ordinary questions that only mention a place ("experience with
# customers in Germany") don't match. check_restrictive_questions.py is the
# regression corpus for this detector. ──
_Q_EXTRA_COUNTRIES = (
    r"argentina|colombia|chile|peru|uruguay|ecuador|venezuela|costa\s+rica|panama|puerto\s+rico|jamaica|"
    r"dominican\s+republic|israel|turkey|t[uü]rkiye|ukraine|russia|belarus|romania|bulgaria|serbia|croatia|"
    r"slovenia|slovakia|czech(?:ia|\s+republic)?|hungary|greece|cyprus|malta|luxembourg|iceland|estonia|latvia|"
    r"lithuania|england|scotland|wales|northern\s+ireland|vietnam|thailand|malaysia|indonesia|pakistan|"
    r"bangladesh|sri\s+lanka|nepal|taiwan|hong\s+kong|south\s+korea|korea|saudi\s+arabia|qatar|kuwait|bahrain|"
    r"oman|jordan|lebanon|morocco|tunisia|algeria|ethiopia|tanzania|uganda|zambia|zimbabwe|rwanda|senegal|"
    r"cameroon|kazakhstan|georgia\s+\(country\)|armenia|azerbaijan|uzbekistan"
)
_Q_REGIONS = (
    r"europe|north\s+america|latin\s+america|latam|apac|asia|oceania|middle\s+east|nordics?|benelux|dach|anz|"
    r"the\s+americas|south\s+america|central\s+america|caribbean|scandinavia|gulf"
)
_Q_PROVINCES = (
    r"ontario|quebec|qu[eé]bec|british\s+columbia|alberta|manitoba|saskatchewan|nova\s+scotia|new\s+brunswick|"
    r"newfoundland|prince\s+edward\s+island"
)
_Q_CITIES = (
    r"new\s+york(?:\s+city)?|nyc|manhattan|brooklyn|san\s+francisco|sf|bay\s+area|silicon\s+valley|san\s+jose|"
    r"oakland|los\s+angeles|san\s+diego|sacramento|seattle|portland|denver|boulder|salt\s+lake\s+city|phoenix|"
    r"scottsdale|las\s+vegas|austin|dallas|houston|san\s+antonio|fort\s+worth|atlanta|miami|orlando|tampa|"
    r"jacksonville|charlotte|raleigh|durham|nashville|memphis|louisville|columbus|cleveland|cincinnati|"
    r"pittsburgh|philadelphia|baltimore|washington,?\s+d\.?c\.?|d\.?c\.?|boston|cambridge|providence|hartford|"
    r"chicago|milwaukee|minneapolis|st\.?\s+louis|kansas\s+city|omaha|detroit|indianapolis|new\s+orleans|"
    r"honolulu|anchorage|tri-?state|dmv|dfw|atx|pnw|socal|norcal|"
    r"toronto|vancouver|montr[eé]al|ottawa|calgary|edmonton|winnipeg|gta|"
    r"london|manchester|birmingham|leeds|bristol|edinburgh|glasgow|cardiff|belfast|dublin|cork|galway|"
    r"berlin|munich|m[uü]nchen|hamburg|frankfurt|cologne|k[oö]ln|stuttgart|d[uü]sseldorf|paris|lyon|marseille|"
    r"toulouse|madrid|barcelona|valencia|lisbon|porto|rome|milan|turin|amsterdam|rotterdam|the\s+hague|utrecht|"
    r"eindhoven|brussels|antwerp|zurich|z[uü]rich|geneva|basel|vienna|prague|warsaw|krak[oó]w|wroc[lł]aw|"
    r"budapest|bucharest|sofia|athens|stockholm|gothenburg|oslo|copenhagen|helsinki|tallinn|riga|vilnius|"
    r"sydney|melbourne|brisbane|perth|adelaide|canberra|auckland|wellington|christchurch|singapore|tokyo|osaka|"
    r"seoul|beijing|shanghai|shenzhen|bangalore|bengaluru|mumbai|delhi|new\s+delhi|hyderabad|chennai|pune|gurgaon|"
    r"gurugram|noida|kolkata|manila|jakarta|bangkok|kuala\s+lumpur|ho\s+chi\s+minh|hanoi|dubai|abu\s+dhabi|"
    r"riyadh|doha|tel\s+aviv|istanbul|cairo|lagos|abuja|nairobi|accra|johannesburg|cape\s+town|durban|"
    r"mexico\s+city|guadalajara|monterrey|bogot[aá]|medell[ií]n|lima|santiago|buenos\s+aires|s[aã]o\s+paulo|"
    r"rio\s+de\s+janeiro|montevideo|panama\s+city|san\s+juan"
)
_Q_ABBR_PLACES = r"(?-i:US|USA|UK|EU|EEA|UAE|NYC|SF|DC|GTA|DMV|DFW|ATX|PNW|SoCal|NorCal)"
_Q_PLACE = (
    r"(?:(?:the\s+)?(?:greater\s+)?(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r"|" + _Q_EXTRA_COUNTRIES + r"|"
    + _Q_REGIONS + r"|" + _Q_PROVINCES + r"|" + _US_STATE_FULL_NAMES_FRAGMENT + r"|" + _Q_CITIES + r"|"
    + _Q_ABBR_PLACES + r")(?:\s+(?:area|metro|region|office|offices|hub))?)"
)
_Q_PLACE_RE = re.compile(r"\b" + _Q_PLACE + r"(?![\w])", re.I)

_Q_AUTH_CUE = (
    r"authori[sz]ed|authori[sz]ation|eligib\w+|legal(?:ly)?\s+(?:able|allowed|entitled|permitted|eligible|"
    r"authori[sz]ed|work|employ|reside|resident)|right\s+to\s+(?:work|be\s+employed|employment)|"
    r"(?:work|employment)\s+(?:permit|visa|authori[sz]ation|status|eligibility)|permit(?:ted)?\s+to\s+work|"
    r"(?:valid|current|hold|have|possess|need|require)\s+(?:a\s+|an\s+)?(?:valid\s+)?(?:work\s+|employment\s+)?"
    r"(?:visa|permit)|visa\s+(?:that\s+)?(?:permits|allows|entitles|status|holder)|citizen\w*|nationals?\b|"
    r"permanent\s+resident|green\s*card|residency|resident\s+status|immigration\s+status|"
    r"proof\s+of\s+(?:eligib\w+|right|identity)|sponsorship|(?:employed|employment)\s+in"
)
_Q_AUTH_CUE_RE = re.compile(r"\b(?:" + _Q_AUTH_CUE + r")", re.I)
_Q_YOU_RE = re.compile(r"\b(?:you|your|i|i'm|i\s+am|we|applicant|candidate)\b|^\s*(?:please\s+)?(?:confirm|indicate|state)\b", re.I)

_GEO_QUESTION_FRAMES = tuple(re.compile(p, re.I) for p in (
    # relocation (a place is NOT required: being asked to relocate means the role is tied to a location)
    r"\b(?:willing|open|able|prepared|ready|comfortable|happy|amenable|agreeable|keen|interested|available|"
    r"consider\w*|plan\w*|can|could|would)\b[^?.]{0,40}?\b(?:relocat\w+|reloc\b|mov(?:e|ing)\s+(?:to|house|home|closer|near))",
    r"\brelocation\s+(?:to|is\s+required|required)\b",
    # commuting / proximity / on-site attendance
    r"\bcommut(?:e|es|ed|ing|able)\b",
    r"\bwithin\s+(?:\d+|a\s+\w+)\s*(?:miles?|mi|km|kilomet\w+|minutes?|mins?|hours?)\s+(?:of|from|drive)\b",
    r"\b(?:live|living|located|reside|residing|based)\s+(?:in\s+or\s+)?(?:near|around|close\s+to|nearby|within\s+\w+\s+of)\b",
    r"\b(?:are\s+you|be|being)\s+(?:a\s+)?local\b|\blocal\s+to\b|\blocals?\s+only\b",
    r"\b(?:work|working|come|coming|be|being|report|reporting|attend|attending|present|show\s+up)\b[^?.]{0,30}?"
    r"\b(?:on[- ]?site|in[- ]?person|in[- ]?office|in\s+the\s+office|(?:from|at|to|into)\s+(?:our|the|a)\s+(?:\w+\s+){0,2}office)\b",
    r"\bhybrid\s+(?:schedule|work\w*|role|model|arrangement|position|environment|setup|basis)\b",
    r"\b\d+\s*days?\s*(?:a|per|each|/)\s*week\b[^?.]{0,20}\b(?:in|at|from)\b[^?.]{0,15}\boffice\b",
    r"\boffice\s+(?:days|attendance|presence)\b",
    # security clearance
    r"\b(?:security|secret|top[- ]secret|ts/sci|public[- ]trust)\s+clearance\b|\bclearance\s+(?:level|status)\b",
))
_Q_RESIDENCE_FRAME = re.compile(
    r"\b(?:reside|resides|residing|resident|live|living|located|based|domiciled|home\s+base|citizen|native)\b",
    re.I)
_Q_ABILITY_FRAME = re.compile(
    r"\b(?:able|willing|available|ready|prepared|comfortable|happy|can|could|will|would|be|are|do|have|"
    r"working|work|works|working|working)\b", re.I)
_Q_WORK_VERB = re.compile(r"\b(?:work|working|works|employ\w*|operate|operating|be\s+based|based|located|reside|live|stay)\b", re.I)
_Q_LICENSE_CUE = re.compile(r"\b(?:licen[sc]e[ds]?|licensure|registered|registration|certified|certification|bar|admitted|credential\w*)\b", re.I)
_Q_BACKGROUND_CUE = re.compile(r"\b(?:background|credit|drug|criminal)\s+(?:check|screen\w*|test)\b|\b(?:driver'?s?|driving)\s+licen[cs]e\b|\bpassport\b", re.I)
_Q_ZONE = (
    r"(?:(?:us|u\.s\.|north\s+american|european|australian|indian|uk|eu)\s+)?"
    r"(?:eastern|pacific|mountain|central|atlantic|alaska(?:n)?|hawaii(?:an)?|greenwich)(?:\s+standard)?(?:\s+time)?|"
    r"est|edt|cst|cdt|mst|mdt|pst|pdt|cet|cest|bst|ist|aest|aedt|jst|kst|sgt|gmt|utc|"
    r"utc\s*[+\-−±]?\s*\d{1,2}|gmt\s*[+\-−±]\s*\d{1,2}|"
    r"(?:us|u\.s\.|eu|european|uk|australian|indian|nz|north\s+american)\s+(?:business\s+|working\s+|office\s+)?"
    r"(?:hours|time\s*zones?|time)"
)
_Q_ZONE_RE = re.compile(r"\b(?:" + _Q_ZONE + r")\b", re.I)
_Q_HOURS_CUE = re.compile(r"\b(?:hours|time\s*zones?|overlap|business\s+hours|working\s+hours|shift|schedule|coverage|cover)\b|\btime\b", re.I)
_Q_CAP_PLACE_RE = re.compile(
    r"\b(?:do|are|will|can)\s+you\s+(?:currently\s+|now\s+)?(?:live|living|reside|residing|located|based)\s+"
    r"(?:in|within|near|around)\s+(?:or\s+(?:near|around)\s+)?(?:the\s+)?(?-i:[A-Z][\w.'’-]*)", re.I)


# Wording that means the question is ABOUT the place, not binding the candidate to it.
_Q_NON_BINDING_RE = re.compile(
    r"\b(?:experience\w*|interested|company|companies|customers?|clients?|market|markets|industry|"
    r"headquarter\w*|familiar\w*|knowledge|worked\s+(?:with|in|at|for)|years?|exposure|regulat\w+|"
    r"across|multiple|various|different|flexib\w+|distributed)\b", re.I)
_Q_RESIDENCE_BOUND_RE = re.compile(
    r"\b(?:you|your|i|i'm|applicant|candidate)\b[^?.]{0,40}?\b(?:reside|residing|live|living|located|based|"
    r"domiciled|resident|citizen|national|native)\b[^?.]{0,30}?\b(?:in|within|near|around|at|of)\b", re.I)


def _geo_question_hit(q: str) -> bool:
    """True when this single application-question text binds the candidate to a place."""
    if not q or len(q) > 600:
        return False
    if any(rx.search(q) for rx in _GEO_QUESTION_FRAMES):
        return True
    if _Q_CAP_PLACE_RE.search(q):
        return True
    you = _Q_YOU_RE.search(q)
    if _Q_PLACE_RE.search(q):
        # authorization / citizenship / visa / sponsorship + a named place, framed at the candidate
        if you and _Q_AUTH_CUE_RE.search(q):
            return True
        if you and not _Q_NON_BINDING_RE.search(q):
            # residence / location of the candidate ("Do you live in X", "Are you a X resident")
            if _Q_RESIDENCE_BOUND_RE.search(q) or re.search(
                    r"\b(?:resident|citizen|national|native|local)\s+of\b|\b(?:based|resident|local)\b(?=\s*\?)|"
                    r"\ba\s+\w+(?:\s+\w+)?\s+resident\b", q, re.I):
                return True
            # "able/willing to work in/from <place>"
            if _Q_ABILITY_FRAME.search(q) and re.search(
                    r"\b(?:work|working|be\s+based|operate|operating)\s+(?:in|from|out\s+of|at|within)\b", q, re.I):
                return True
            # state / province licensing, background checks, local passport or driver's licence
            if _Q_LICENSE_CUE.search(q) or _Q_BACKGROUND_CUE.search(q):
                return True
    # named time zone + availability / hours wording aimed at the candidate
    if you and _Q_ZONE_RE.search(q) and _Q_HOURS_CUE.search(q) and not _Q_NON_BINDING_RE.search(q):
        return True
    return False


def has_restrictive_geo_question_signal(job: dict) -> bool:
    """Deterministic hard filter over the job's application questions only —
    see the module comment above _Q_EXTRA_COUNTRIES."""
    desc = job.get("description_snippet") or ""
    if _APPLICATION_AUTH_QUESTION_MARKER not in desc:
        return False
    for line in desc.split("\n"):
        line = line.strip()
        if line.startswith(_APPLICATION_AUTH_QUESTION_MARKER) and _geo_question_hit(
                line[len(_APPLICATION_AUTH_QUESTION_MARKER):].strip()):
            return True
    return False


_RANK4_GENUINE_RESTRICTION_CHECKS = (
    has_hard_no_sponsorship_signal,
    has_non_remote_workplace_type,
    has_non_remote_title_signal,
    has_non_remote_labeled_text_signal,
    has_hard_country_specific_auth_signal,
    has_restrictive_geo_question_signal,
    has_state_list_restriction_signal,
    has_hard_country_based_restriction_signal,
    has_hard_metadata_location_signal,
    has_hard_location_symbol_signal,
    has_office_attendance_signal,
    has_entity_or_exclusion_restriction_signal,
    has_timezone_relocation_or_hyphenated_restriction_signal,
    has_extra_restrictive_geography_signal,
    _rank4_has_country_tied_restrictive_question,
    has_state_specific_license_signal,
    has_language_fluency_restriction_signal,
)

# 2026-09 (explicit user instruction, verbatim list): Rank 4's OWN,
# DELIBERATELY NARROWER country allowlist — distinct from the project-wide
# _COUNTRY_AUTH_NAMES_RE_FRAGMENT above, which is used everywhere ELSE in
# this file as "does this text name ANY specific place" for EXCLUSION
# purposes (a JD naming Japan/Brazil/India as a restriction has to be
# caught regardless of whether Japan/Brazil/India is a market this
# project ever wants Rank 4 ADMITTING) and must stay broad — narrowing
# THAT shared fragment would silently break every OTHER hard-override
# check that reuses it. Rank 4 admission is the opposite direction: only
# these specific countries are accepted as a bare Rank 4 location. "US,
# UK, Canada, Australia, Germany, Ireland, Singapore, Luxembourg, Norway,
# Switzerland, Denmark, Netherlands, Iceland, Sweden, Italy" — two of
# these (Luxembourg, Iceland) aren't even in the shared fragment at all.
# "Europe" and "North America" (continents/regions, not individual
# countries) and the broader business-region set (LATAM, AMER, APAC,
# etc.) are handled by _RANK4_PLACE_RE's OWN separate additions below,
# unchanged — explicit user confirmation that regions stay as-is
# ("regions are allowed too, like LATAM, AMER, etc.").
_RANK4_ELIGIBLE_COUNTRIES_RE_FRAGMENT = (
    r"u\.?s\.?a?\.?|united\s+states(?:\s+of\s+america)?|u\.?k\.?|united\s+kingdom|"
    r"canada|australia|germany|(?:republic\s+of\s+)?ireland|singapore|"
    r"luxembourg|norway|switzerland|denmark|netherlands|iceland|sweden|italy"
)

# Every non-EMEA/non-Africa business region and continent this file
# already recognizes elsewhere (_REGION_ONLY_WORDS_RE's vocabulary, minus
# EMEA/Africa — those are already PRIORITY_AFRICA, handled long before
# this tier is ever reached) — UNCHANGED from before the country-allowlist
# narrowing above. US-state/Canadian-province names are deliberately NOT
# included — per explicit user instruction ("states do not qualify
# here"), a bare state alone never matches this allowlist and so never
# reaches 4a/4b.
_RANK4_PLACE_INNER_FRAGMENT = (
    _RANK4_ELIGIBLE_COUNTRIES_RE_FRAGMENT + r"|"
    r"european\s+union|\beu\b|apac|latam|amers?|americas|mena|middle\s+east|"
    r"anz|dach|benelux|nordics?|"
    r"western\s+europe|eastern\s+europe|central\s+europe|southern\s+europe|"
    r"northern\s+europe|"
    r"gulf\s+cooperation\s+council|gcc|gulf|"
    r"south[\s\-]?east\s+asia|south\s+asia|east\s+asia|central\s+asia|asia|"
    r"oceania|pacific|north\s+america|central\s+america|south\s+america|"
    r"caribbean|cee|cis|japac|apj|europe"
)
_RANK4_PLACE_RE = re.compile(r"\b(?:" + _RANK4_PLACE_INNER_FRAGMENT + r")\b", re.I)

# 2026-09 BUG FIX (explicit user report, real production data: bare-city
# locations naming a DISALLOWED country/city — "India", "Shanghai", "South
# Africa", "Bengaluru, India Office" — were being admitted at 4b). Root
# cause: classify_rank4's 4b "title/description independently name an
# allowed region" check ran a bare, unscoped _RANK4_PLACE_RE.search(
# full_text) over the ENTIRE title+description blob — so ANY mention of a
# recognized region word anywhere (routine "About us" company boilerplate
# like "we have teams across EMEA, APAC, and the Americas", totally
# unrelated to what region THIS specific role is hired for) counted as a
# "mixed signal" and admitted an otherwise-disallowed location. This is the
# exact same failure mode already fixed for the main pipeline's Rank 1/2/3
# JD-rescue step and for _STRICT_BROAD_REGION_RE's EMEA/Africa-only
# version — generalized here to Rank 4's full country/region vocabulary:
# require a hiring-context verb within 80 chars of the matched place, so
# generic company-description mentions no longer count as evidence.
#
# 2026-09 (explicit user policy, verbatim: "a JD saying based in one our
# (allowed country/region) offices should not be allowed... an enforcement
# stating in the sentence/jd that they require you to stay/reside there is
# a no"): "based" and "located" removed from this list. Those two words
# don't just mean "this text happens to be about hiring" the way "hire"/
# "role"/"candidates" do -- "based in <region>"/"located in <region>" is
# itself a residency ENFORCEMENT, the opposite of supporting evidence for
# admission. Keeping them here would have let "this role is based in our
# EMEA offices" count as 4b's "mixed signal, let it in" evidence, when the
# user's policy is the reverse: that exact phrasing should DISQUALIFY the
# job. See has_rank4_region_residency_enforcement_signal below, which
# catches "based in <region>" as its own dedicated rejection instead.
_RANK4_HIRING_CONTEXT_WORDS_FRAGMENT = (
    r"remote|work|working|hire|hiring|recruit|recruiting|employment|employ|"
    r"candidates?|applicants?|available|open|role|position"
)
_RANK4_HIRING_CONTEXT_WORDS_RE = re.compile(r"\b(?:" + _RANK4_HIRING_CONTEXT_WORDS_FRAGMENT + r")\b", re.I)
_RANK4_STRICT_PLACE_RE = re.compile(
    r"\b(?:" + _RANK4_HIRING_CONTEXT_WORDS_FRAGMENT + r")\b"
    r".{0,80}\b(?:" + _RANK4_PLACE_INNER_FRAGMENT + r")\b"
    r"|\b(?:" + _RANK4_PLACE_INNER_FRAGMENT + r")\b"
    r".{0,80}\b(?:" + _RANK4_HIRING_CONTEXT_WORDS_FRAGMENT + r")\b",
    re.I,
)
# 2026-10: 4b-specific variant that excludes "Americas"/"AMER" as rescue
# signals. A JD mentioning "Americas" when the location is a specific US
# city doesn't prove the role is open outside the US — too broad to count
# as a mixed signal for 4b admission.
_RANK4_4B_PLACE_INNER_FRAGMENT = (
    _RANK4_ELIGIBLE_COUNTRIES_RE_FRAGMENT + r"|"
    r"european\s+union|\beu\b|apac|latam|mena|middle\s+east|"
    r"anz|dach|benelux|nordics?|"
    r"western\s+europe|eastern\s+europe|central\s+europe|southern\s+europe|"
    r"northern\s+europe|"
    r"gulf\s+cooperation\s+council|gcc|gulf|"
    r"south[\s\-]?east\s+asia|south\s+asia|east\s+asia|central\s+asia|asia|"
    r"oceania|pacific|central\s+america|south\s+america|"
    r"caribbean|cee|cis|japac|apj|europe"
)
_RANK4_4B_STRICT_PLACE_RE = re.compile(
    r"\b(?:" + _RANK4_HIRING_CONTEXT_WORDS_FRAGMENT + r")\b"
    r".{0,80}\b(?:" + _RANK4_4B_PLACE_INNER_FRAGMENT + r")\b"
    r"|\b(?:" + _RANK4_4B_PLACE_INNER_FRAGMENT + r")\b"
    r".{0,80}\b(?:" + _RANK4_HIRING_CONTEXT_WORDS_FRAGMENT + r")\b",
    re.I,
)

# 2026-09 NEW (explicit user instruction, verbatim examples: "locations
# like these too: Sydney, Australia and London, United Kingdom"): a CITY
# named alongside one of Rank 4's eligible countries is at least as
# specific/acceptable as the bare country alone — currently rejected
# outright by the 4a "remainder must be empty" check above, since the
# city name itself is never in _RANK4_PLACE_RE's vocabulary (cities
# aren't recognized place names anywhere in this file; only countries/
# regions/continents are). Deliberately anchored to the WHOLE location
# string (^...$, not a bare .search) and requires the country to be the
# LAST thing in the string — this only recognizes the clean "City,
# Country" shape itself, not a country name merely appearing somewhere in
# a longer, possibly-restrictive sentence (which the 12+ hard-override
# checks already ran and cleared before classify_rank4 ever reaches this
# point anyway). Excludes a US state name in the "city" position (e.g.
# "California, United States") — same "states do not qualify" policy as
# _RANK4_PLACE_RE above; a state is not a city.
_RANK4_CITY_COMMA_COUNTRY_RE = re.compile(
    r"^\s*(?!(?:" + _US_STATE_FULL_NAMES_FRAGMENT + r")\s*,)"
    r"[A-Za-z][A-Za-z.'\-]*(?:\s+[A-Za-z][A-Za-z.'\-]*){0,2}\s*,\s*"
    r"(?:" + _RANK4_ELIGIBLE_COUNTRIES_RE_FRAGMENT + r")\s*$",
    re.I,
)

# 2026-10 BUG FIX (real production leak, explicit user report: a live
# crawl wrote rows with location "Remote, New York", "Denver, CO; New
# York City, NY; San Francisco, CA", "Boston, Massachusetts; Chicago,
# Illinois; ... Remote, New Jersey", and a raw European street address,
# "AT002 Industriestraße 2, 5303 Thalgau" — none of them a legitimate
# Rank 4 admission. Root cause: once the 4a checks above both fail (this
# location is neither a bare eligible country/region nor a clean "City,
# eligible-country" pair), classify_rank4() falls through to 4b's
# Global/EMEA/Africa-family check, which tests `loc`/`title` for broad
# evidence (_text_has_global_evidence, _TITLE_MULTI_REGION_WORDS_RE,
# etc.) with NO check at all that `loc` itself isn't already a flatly
# disallowed shape. A title merely containing the word "Global" ("Global
# Account Manager, Strategics (New York)") was enough to grant 4b despite
# the location field unambiguously naming a specific US state — 4b's own
# premise (title/JD supplies a broader claim while the location field is
# merely a narrower-but-still-ELIGIBLE place, e.g. "EMEA" + "London")
# requires the location to at least be eligible on its own; it was never
# meant to let title/JD language override a location field that's
# already concretely disqualifying.
#
# Checked once, right after 4a fails, and blocks BOTH 4b paths below
# (not just the Global/EMEA/Africa one) — a location this clearly
# specific shouldn't be rescuable via _RANK4_STRICT_PLACE_RE either.
# Deliberately scoped to what real evidence showed, not every
# conceivable disqualifying shape (e.g. Canadian provinces aren't
# covered — no real posting has shown that gap yet):
#   1. A US state, spelled out in full anywhere in `loc` ("Remote, New
#      York", "Boston, Massachusetts; Chicago, Illinois; ...").
#   2. A US state abbreviation immediately after a "<word(s)>, " prefix,
#      validated against the real, closed _US_STATE_ABBRS set (not a
#      bare re.I alternation of 2-letter codes, which would also match
#      "ca"/"ny" inside ordinary words like "Canada"/"many" — same
#      case-sensitivity discipline as _has_metro_area_state_abbr_signal).
#   3. A raw street address: a short digit run (a postal code, 4-6
#      digits) sitting directly in front of a word (the city name) --
#      "5303 Thalgau" -- a shape no legitimate bare country/region/city
#      name ever takes.
_LOCATION_FIELD_US_STATE_FULL_RE = re.compile(
    r"\b(?:" + _US_STATE_FULL_NAMES_FRAGMENT + r")\b", re.I
)
_LOCATION_FIELD_STATE_ABBR_RE = re.compile(
    r"\b[A-Za-z][\w.'-]*(?:\s+[A-Za-z][\w.'-]*){0,3}\s*,\s*([A-Za-z]{2})\b"
)
_RANK4_RAW_STREET_ADDRESS_RE = re.compile(
    r"\b\d{1,5}\s*,?\s*\d{4,6}\s+[A-Za-zÀ-ÖØ-öø-ÿ]"
)


def _rank4_location_field_is_hard_disqualified(loc: str) -> bool:
    if _LOCATION_FIELD_US_STATE_FULL_RE.search(loc):
        return True
    if any(m.group(1) in _US_STATE_ABBRS for m in _LOCATION_FIELD_STATE_ABBR_RE.finditer(loc)):
        return True
    if _RANK4_RAW_STREET_ADDRESS_RE.search(loc):
        return True
    return False


# 2026-10 POLICY CHANGE (explicit user instruction: "even though jobs in
# Kenya and co mention global hiring, they should still not make it. They
# must be in the list of allowed countries/regions/continents to do
# so."): a bare country NOT on Rank 4's own curated eligible list
# (_RANK4_PLACE_INNER_FRAGMENT — US/UK/Canada/Australia/Germany/Ireland/
# Singapore/Luxembourg/Norway/Switzerland/Denmark/Netherlands/Iceland/
# Sweden/Italy, plus the named business regions) used to still be
# rescuable at 4b purely on genuine "we hire globally"-shaped JD text —
# confirmed live for Kenya/Brazil/South Africa/India-named postings. That
# is no longer allowed: real broad-hiring language in the JD is NOT
# sufficient on its own if the location field itself names a country
# outside the curated list.
#
# Reuses _COUNTRY_AUTH_NAMES_RE_FRAGMENT — the project-wide, much BROADER
# "does this text name any real country" vocabulary already used
# everywhere else in this file for exclusion purposes — checked against
# `remainder`, not raw `loc`: `remainder` already has every Rank-4-
# eligible place name stripped out (computed just above, before either 4a
# check), so a bare eligible region abbreviation that also happens to sit
# in the broader fragment (APAC/LATAM/Europe are in both) never
# false-positives here — by the time this runs, 4a has already failed,
# which only happens when `remainder` is non-empty, i.e. real leftover
# text _RANK4_PLACE_RE doesn't recognize. If THAT leftover text itself
# names a real country, this is a hard, unconditional reject — no
# title/JD language can override it. A bare city with no country word at
# all ("London") leaves no such match and is unaffected, preserving the
# legitimate "title: CSM, EMEA / location: London" 4b case.
# 2026-10 (pipeline audit, real production rows): the project-wide fragment
# above is a BLOCKLIST of ~80 countries, not a complete list, so the guard
# never fired for countries missing from it — live Rank 4 rows with location
# "El Salvador" (8), "Jamaica" (6) and "Nicaragua" (5), all 4b, all admitted
# on a title/JD "LATAM"/global-hiring cue. The policy ("must be in the list
# of allowed countries/regions/continents") is an ALLOWLIST rule, so the
# guard needs the full vocabulary of sovereign states and common territories,
# not just the countries someone happened to add. Used ONLY by this guard
# (the shared fragment feeds ~20 other restriction checks and is left
# alone). Curated Rank 4 countries are deliberately absent: by the time
# this regex runs they have already been stripped from `remainder`.
# Ambiguous words that are also ordinary names/places (Georgia = US state,
# handled by the state guard that also returns None; Jordan, Chad) are fine
# here because the location FIELD is the only text inspected.
_RANK4_WORLD_COUNTRIES_FRAGMENT = (
    r"afghanistan|albania|algeria|andorra|angola|antigua|argentina|armenia|aruba|"
    r"azerbaijan|bahamas|bahrain|bangladesh|barbados|belarus|belize|benin|bermuda|"
    r"bhutan|bolivia|bosnia|botswana|brunei|bulgaria|burkina\s+faso|burundi|"
    r"cabo\s+verde|cape\s+verde|cambodia|cameroon|cayman|central\s+african\s+republic|"
    r"chad|comoros|congo|cook\s+islands|croatia|cuba|cura[cç]ao|cyprus|"
    r"djibouti|dominica|ecuador|el\s+salvador|equatorial\s+guinea|eritrea|estonia|"
    r"eswatini|swaziland|ethiopia|fiji|gabon|gambia|georgia|ghana|gibraltar|greenland|"
    r"grenada|guadeloupe|guam|guatemala|guernsey|guinea|guinea-bissau|guyana|haiti|"
    r"honduras|iran|iraq|isle\s+of\s+man|ivory\s+coast|c[oô]te\s+d.ivoire|jamaica|"
    r"jersey|jordan|kazakhstan|kiribati|kosovo|kuwait|kyrgyzstan|laos|latvia|lebanon|"
    r"lesotho|liberia|libya|liechtenstein|lithuania|macau|macao|madagascar|malawi|"
    r"maldives|mali|malta|marshall\s+islands|martinique|mauritania|mauritius|"
    r"micronesia|moldova|monaco|mongolia|montenegro|mozambique|myanmar|burma|namibia|"
    r"nauru|nepal|new\s+caledonia|nicaragua|niger|north\s+korea|north\s+macedonia|"
    r"macedonia|oman|palau|palestine|papua\s+new\s+guinea|paraguay|puerto\s+rico|"
    r"reunion|r[eé]union|rwanda|saint\s+lucia|st\.?\s+lucia|samoa|san\s+marino|"
    r"sao\s+tome|saudi\s+arabia|senegal|seychelles|sierra\s+leone|slovakia|slovenia|"
    r"solomon\s+islands|somalia|south\s+sudan|sri\s+lanka|sudan|suriname|syria|"
    r"tajikistan|tanzania|timor|togo|tonga|trinidad|tunisia|turkmenistan|tuvalu|"
    r"uganda|uruguay|uzbekistan|vanuatu|vatican|yemen|zambia|zimbabwe|"
    r"serbia|bosnia\s+and\s+herzegovina|qatar|"
    r"united\s+arab\s+emirates|uae|belgium|austria|finland|portugal|poland|"
    r"hungary|greece|romania|czechia|czech|ukraine|russia|turkey|t[uü]rkiye|"
    r"japan|china|taiwan|hong\s+kong|south\s+korea|korea|vietnam|thailand|"
    r"indonesia|philippines|malaysia|pakistan|india|israel|egypt|morocco|nigeria|"
    r"kenya|ghana|south\s+africa|brazil|mexico|colombia|chile|peru|venezuela|"
    r"costa\s+rica|panama|dominican\s+republic|new\s+zealand|france|spain"
)
_RANK4_ANY_NAMED_COUNTRY_RE = re.compile(
    r"\b(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r"|" + _RANK4_WORLD_COUNTRIES_FRAGMENT + r")\b", re.I)

# 2026-09 NEW (explicit user policy, verbatim: "a JD saying based in one
# our (allowed country/region) offices should not be allowed. Regions are
# allowed. The aim of rank 4 is that maybe if they did not ask an app
# question they may be willing to allow you work from anywhere and don't
# really need u to work from there. So them asking you to be based there
# is a no. Just bare location field is what we are taking, an enforcement
# stating in the sentence/jd that they require you to stay/reside there is
# a no."): a region/country named PASSIVELY in the title or JD ("CSM -
# LATAM", "we hire across EMEA") is still legitimate 4b evidence — but a
# sentence that ENFORCES physical presence ("this role is based in our
# EMEA offices", "must reside in APAC") is a real residency requirement,
# exactly as disqualifying as naming one specific country, even though the
# place itself is otherwise an accepted broad region. Region/business-
# family words were never part of any restriction vocabulary before this —
# they were only ever treated as ACCEPTED evidence elsewhere in this file
# (_RANK4_PLACE_RE, _has_multi_region_breadth) — so this is a genuinely new
# check, not a tightened existing one. Rank-4-specific (not universal):
# Rank 1/2 never let JD/title text override what the location field
# itself already resolved to, so a new country-NAME exclusion wouldn't
# apply there the same way; this is scoped to where the user described it
# — Rank 4's own "is this JD text actually a mixed signal, or a real tie"
# judgment call.
_RANK4_REGION_RESIDENCY_ENFORCEMENT_PLACE_FRAGMENT = (
    _RANK4_PLACE_INNER_FRAGMENT + r"|emea|africa"
)
_RANK4_REGION_RESIDENCY_ENFORCEMENT_RE = re.compile(
    r"\bbased\s+in\s+(?:the\s+|one\s+of\s+our\s+|our\s+)?"
    r"(?:" + _RANK4_REGION_RESIDENCY_ENFORCEMENT_PLACE_FRAGMENT + r")\b"
    r"|\b(?:must|required\s+to|need(?:s)?\s+to|has\s+to)\s+(?:reside|stay|live)\s+in\s+"
    r"(?:the\s+)?(?:" + _RANK4_REGION_RESIDENCY_ENFORCEMENT_PLACE_FRAGMENT + r")\b"
    r"|\bresiden(?:ce|cy)\s+(?:in|within)\s+(?:the\s+)?"
    r"(?:" + _RANK4_REGION_RESIDENCY_ENFORCEMENT_PLACE_FRAGMENT + r")\s+(?:is\s+)?required\b",
    re.I,
)


# 2026-09 BUG FIX (found via this project's own existing adversarial
# test suite, immediately after adding the check above): "our globally
# distributed team includes engineers based in Germany, India, and
# Brazil" also matches "based in Germany" -- but this is company-wide
# boilerplate describing WHO ALREADY WORKS HERE, not a requirement for
# THIS candidate/role. The user's own policy example ("a JD saying based
# in one of our <region> offices should not be allowed") describes the
# ROLE/POSITION itself, not the existing workforce. A sentence naming the
# company's existing people (team/engineers/employees/staff/workforce/
# colleagues/workers) in the same breath as "based in <place>" is that
# company-description shape, not a residency enforcement on the
# candidate, and is excluded here the same way "our global network"
# marketing copy is excluded elsewhere in this file.
_RANK4_WORKFORCE_DESCRIPTION_RE = re.compile(
    r"\b(?:team|engineers?|employees?|staff|workforce|colleagues?|workers?|people)\b",
    re.I,
)


def has_rank4_region_residency_enforcement_signal(job: dict) -> bool:
    """Rank-4-only guard — see _RANK4_REGION_RESIDENCY_ENFORCEMENT_RE's
    module comment above for the full policy this implements. Sentence-
    scoped (same idea as has_extra_restrictive_geography_signal/the
    boilerplate-guard sentence loop above) so a restrictive sentence in
    one part of the JD doesn't get diluted by unrelated text elsewhere."""
    text = (job.get("title") or "") + " " + (job.get("description_snippet") or "")
    if not text.strip():
        return False
    for sentence in _split_into_sentences(text):
        if not _RANK4_REGION_RESIDENCY_ENFORCEMENT_RE.search(sentence):
            continue
        if _RANK4_WORKFORCE_DESCRIPTION_RE.search(sentence):
            continue
        return True
    return False


def classify_rank4(job: dict) -> tuple[str | None, str | None]:
    """Returns (priority, reason) — priority is PRIORITY_MIXED_COUNTRY,
    PRIORITY_MIXED_SIGNAL, or None (not eligible for this tier).

    Caller is responsible for the eligibility gate — role_category in
    ("CS", "AM"), job["source_ats"] in RANK4_ELIGIBLE_ATS, the
    ENABLE_RANK4_COUNTRY_SPECIFIC config toggle, and location AND
    application-question text both genuinely present on THIS job — this
    function only decides the location-shape/restriction question once
    that gate has already passed, and assumes the job already failed the
    main _keyword_classify_location_detail pipeline above (i.e. is NOT
    already a Rank 1/2/3 match)."""
    for check in _RANK4_GENUINE_RESTRICTION_CHECKS:
        if check(job):
            return None, None
    # Defined later in this file (after _RANK4_GENUINE_RESTRICTION_CHECKS'
    # own definition, which it can't be folded into without reordering a
    # lot of code it depends on) — see has_rank4_region_residency_
    # enforcement_signal's own docstring for the policy this implements.
    if has_rank4_region_residency_enforcement_signal(job):
        return None, None

    raw_loc = job.get("location") or ""
    raw_country = job.get("country") or ""
    if isinstance(raw_loc, list):
        raw_loc = ", ".join(str(x) for x in raw_loc)
    if isinstance(raw_country, list):
        raw_country = ", ".join(str(x) for x in raw_country)
    loc = (raw_loc + " " + raw_country).strip()
    title = job.get("title", "")
    loc = _enrich_location_from_title(loc, title)
    loc = _enrich_location_from_description(loc, job.get("description_snippet") or "")
    if not loc.strip() or PLACEHOLDER_LOC_RE.match(loc):
        return None, None

    # 4a: the location field, once every recognized place-name span is
    # removed, has nothing left over — it's ENTIRELY made of one or more
    # allowed country/region/continent names (+ connectors). Same
    # "remainder" technique has_role_specific_place_restriction_signal's
    # body-text check already uses (see that function's docstring for
    # the real bug this shape of check fixed there).
    # 2026-09 BUG FIX (explicit user report, real production data:
    # "Germany Remote" — an eligible bare country plus a generic
    # work-modality qualifier, not a second competing place — was landing
    # in 4b/being dropped instead of 4a). NON_GEO_WORDS_RE (already used
    # by the main pipeline's own EMEA/Global residue checks) strips
    # "remote", "office"-adjacent generic qualifiers, and other non-place
    # filler; without it, "remote" survived as leftover residue and made
    # this look like a second, un-recognized place rather than the single
    # clean country signal it actually is.
    remainder = _RANK4_PLACE_RE.sub(" ", loc)
    remainder = re.sub(r"\b(?:and|or)\b|&", " ", remainder, flags=re.I)
    remainder = NON_GEO_WORDS_RE.sub(" ", remainder)
    remainder = re.sub(r"[,\s/|()\-–—]+", " ", remainder).strip()
    if not remainder and _RANK4_PLACE_RE.search(loc):
        return PRIORITY_MIXED_COUNTRY, "bare_country_or_region"

    # 2026-10 POLICY CHANGE: the location field names a real country that
    # isn't on Rank 4's own curated list (see _RANK4_ANY_NAMED_COUNTRY_RE's
    # module comment) -- a hard, unconditional reject regardless of what
    # the title/JD separately claims. Checked here, right after the bare-
    # country 4a check fails, so it also covers the "City, <disallowed
    # country>" shape before the city+country 4a check below gets a
    # chance to NOT match it anyway (that check only ever matches an
    # ELIGIBLE country, so this isn't redundant with it -- it's closing
    # off the DIFFERENT, broader path through 4b that follows).
    # 2026-10: scan the location with ONLY the curated places stripped, not
    # `remainder` — NON_GEO_WORDS_RE (applied to build `remainder`) also
    # strips filler words such as "new" and "of", which mangled multi-word
    # country names before this check could see them ("New Zealand" ->
    # "Zealand", "Isle of Man" -> "Isle Man", "New Caledonia" -> "Caledonia")
    # and let those three through at 4b.
    if _RANK4_ANY_NAMED_COUNTRY_RE.search(_RANK4_PLACE_RE.sub(" ", loc)):
        return None, None

    # 4a (city variant): "City, Country" — e.g. "Sydney, Australia",
    # "London, United Kingdom" — see _RANK4_CITY_COMMA_COUNTRY_RE's module
    # comment. A city named alongside an eligible country is at least as
    # specific/acceptable as the bare country alone, and would otherwise
    # be rejected by the "remainder must be empty" check just above (the
    # city name itself is never in _RANK4_PLACE_RE's vocabulary).
    if _RANK4_CITY_COMMA_COUNTRY_RE.match(loc.strip()):
        return PRIORITY_MIXED_COUNTRY, "city_in_eligible_country"

    # Neither 4a shape matched -- before letting 4b's title/JD-driven
    # checks have a say, rule out a location field that's already
    # concretely disqualifying on its own (a US state, an enumerated
    # list containing one, or a raw street address). See
    # _rank4_location_field_is_hard_disqualified's own module comment
    # for the real leaked postings this closes -- no title/JD language
    # should be able to rescue a location field this specific.
    if _rank4_location_field_is_hard_disqualified(loc):
        return None, None

    # 4b: the location field is something ELSE (a city, e.g.) but the
    # title/description independently name an allowed region/country — a
    # real signal pointing a different, more specific direction, not a
    # contradiction (the genuine-restriction checks above already ruled
    # out an actual confirmed tie, e.g. a country-specific work-auth
    # question).
    #
    # 2026-09 BUG FIX (explicit user report, real production data: bare
    # DISALLOWED locations -- "India", "Shanghai", "South Africa",
    # "Bengaluru, India Office" -- were being admitted at 4b). This used to
    # be a bare _RANK4_PLACE_RE.search(full_text) over the WHOLE title+
    # description blob, with no requirement that the matched place have
    # anything to do with THIS role's own hiring scope -- routine "About
    # us" company boilerplate ("we have teams across EMEA, APAC, and the
    # Americas") was enough to admit a location that isn't even on Rank
    # 4's own allowlist. _RANK4_STRICT_PLACE_RE requires a hiring-context
    # verb within 80 chars of the matched place (same proximity-guard
    # pattern _STRICT_BROAD_REGION_RE already uses for EMEA/Africa
    # elsewhere in this file), so generic company-description mentions no
    # longer count as evidence.
    full_text = (job.get("title") or "") + " " + (job.get("description_snippet") or "")
    if _RANK4_4B_STRICT_PLACE_RE.search(full_text):
        return PRIORITY_MIXED_SIGNAL, "mixed_title_or_jd_signal"

    # 2026-09 NEW (explicit user instruction, verbatim: "if the title is:
    # CSM, EMEA or global or Africa or variations of these, and location
    # is London, its let in because we allow EMEA, global/Africa are
    # things we accept... the same applies vice versa where title is CSM
    # London and location says EMEA/Global/Africa... to be let into 4b you
    # must have a mixed signal, something saying yes and no"): the SAME 4b
    # admission, for the Global/EMEA/Africa keyword family specifically.
    # Deliberately NOT folded into _RANK4_PLACE_RE itself — that family
    # already has its own dedicated, stricter Rank 1/2 handling upstream
    # in _keyword_classify_location_detail, and a job only ever reaches
    # this point because a BARE, unqualified "EMEA"/"Global"/"Africa"
    # location field would already have been Rank 1/2 before Rank 4 is
    # ever attempted — so a hit here always means the signal was
    # qualified/mixed with something else (a city, e.g. "EMEA, London"),
    # never a clean standalone claim.
    #
    # Checks three places, matching each half of the user's example:
    #   - `loc` (the location field itself, post-enrichment) — the "vice
    #     versa" case: location says something like "EMEA, London" that
    #     failed the strict bare-EMEA residue check upstream, but the
    #     EMEA half is still real, independent evidence sitting right
    #     there in the same field.
    #   - `title` via _TITLE_MULTI_REGION_WORDS_RE — the same regex
    #     _enrich_location_from_title already uses to spot "CSM - EMEA"-
    #     style suffixes; a short, curated field where a bare region/
    #     global word ("CSM - Global", "CSM (Africa)") is safe to trust
    #     without needing the fuller phrase body text requires.
    #   - `description_snippet` via the main pipeline's own (deliberately
    #     more conservative, phrase-requiring) JD-evidence functions, to
    #     avoid marketing-copy false positives like "our global network"
    #     that a bare-word scan of free body text would catch.
    #
    # 2026-09 BUG FIX (explicit user report, same production data as the
    # _RANK4_STRICT_PLACE_RE fix above): the full_text half of this check
    # had the identical unscoped-boilerplate flaw -- generic "About us"
    # copy like "we have teams across EMEA, APAC, and the Americas" isn't
    # excluded by _text_has_global_evidence/_has_multi_region_breadth's
    # own marketing-boilerplate guard (that guard only excludes a curated
    # set of specific phrases like "global network", not named-region
    # company-wide claims), so it was still admitting disallowed locations
    # (India, Shanghai, South Africa) at 4b even after the fix above closed
    # the _RANK4_PLACE_RE path. Scoped the full_text half to sentences that
    # also contain a hiring-context word, same proximity-guard idea as
    # _RANK4_STRICT_PLACE_RE -- "we hire globally across every continent"
    # still counts (has "hire"), but "we have teams across EMEA, APAC, and
    # the Americas" as pure company description no longer does.
    full_text_has_scoped_broad_evidence = False
    for sentence in _split_into_sentences(full_text):
        if not sentence.strip() or not _RANK4_HIRING_CONTEXT_WORDS_RE.search(sentence):
            continue
        if (_text_has_global_evidence(sentence) or _text_has_africa_or_emea_evidence(sentence)
                or _has_multi_region_breadth(sentence)):
            full_text_has_scoped_broad_evidence = True
            break

    if (_text_has_global_evidence(loc) or _text_has_africa_or_emea_evidence(loc)
            or _has_multi_region_breadth(loc)
            or _TITLE_MULTI_REGION_WORDS_RE.search(title)
            or full_text_has_scoped_broad_evidence):
        return PRIORITY_MIXED_SIGNAL, "mixed_global_emea_africa_signal"

    return None, None
