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

# Per-provider rate limiting (thread-safe via dict — each provider has its own timestamp)
_last_call_times = {p["name"]: 0.0 for p in ROLE_PROVIDERS}
for _p in LOCATION_PROVIDERS:
    _last_call_times[_p["name"]] = 0.0

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
    then every later call short-circuits instantly with no network hit."""
    with _exhausted_providers_lock:
        already_known = name in _exhausted_providers_today
        _exhausted_providers_today.add(name)
    if not already_known:
        log.error(f"{name} failed ({reason}) — no more traffic to {name} for the rest of this run, "
                  f"rerouting its remaining work to other providers")


def _ai_call(provider: dict, client, system_prompt: str, user_msg: str, max_tokens: int = 500) -> str | None:
    """Call an OpenAI-compatible provider with retry on rate limit.
    Returns response text or None on failure.

    2026-09 ROUND 7: for Groq (both accounts), this call only actually
    proceeds while this process holds the cross-shard Groq lock (see
    groq_coordination.py) — if it can't be claimed within that module's
    wait budget, this returns None immediately, exactly like any other
    provider failure, so the EXISTING cross-provider failover picks up the
    work on OpenAI/NVIDIA (or a later cascade round) without any special
    casing needed at the call sites. This replaces AI_RATE_SHARDS' static
    guess with real coordination: at most one shard fires at Groq at a
    time, so config.py's Groq min_call_interval is now a single-shard-safe
    value on its own, not something that needs dividing by an assumed
    shard count anymore.
    """
    name = provider["name"]
    if name in _exhausted_providers_today:
        # Already confirmed out for this run (see _mark_exhausted) — skip
        # the wasted network call and the repeat log line entirely.
        # Callers see this exactly like any other failed call (None), so
        # the existing cross-provider failover path still applies.
        return None

    is_groq = name in _GROQ_NAMES
    if is_groq and not groq_coordination.enter_critical_section():
        log.debug(f"{name}: couldn't claim the cross-shard Groq slot in time — "
                  f"skipping this call (existing failover will retry it on another provider)")
        return None

    try:
        if is_groq:
            new_count = groq_coordination.bump_daily(name, 1)
            if new_count is not None and new_count >= _GROQ_DAILY_CAP:
                _mark_exhausted(name, f"cross-shard daily count reached {new_count}/{_GROQ_DAILY_CAP}")
                return None

        interval = provider.get("min_call_interval", 0.0)

        if interval > 0:
            elapsed = time.time() - _last_call_times.get(name, 0.0)
            if elapsed < interval:
                time.sleep(interval - elapsed)
        _last_call_times[name] = time.time()

        for attempt in range(MAX_RETRIES):
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
                    log.warning(f"{name} returned null content (attempt {attempt + 1})")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_BASE_DELAY)
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
                    _mark_exhausted(name, "daily quota reached")
                    return None
                if is_rate_limit and attempt < MAX_RETRIES - 1:
                    delay = RETRY_BASE_DELAY * (attempt + 1)
                    log.warning(f"{name} rate limit hit, retrying in {delay}s (attempt {attempt + 1})")
                    time.sleep(delay)
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
        log.warning(f"AI role classification failed ({provider['name']}) for batch of {len(batch)}, rerouting to another provider")
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

    Cross-provider failover: if a provider's batch fails outright (after
    its own MAX_RETRIES=3 attempts inside _ai_call, or its client can't be
    built), that batch's titles are reassigned across the OTHER providers
    and retried once before giving up on them — see the module docstring.

    Returns {title: is_relevant}. On failure of every provider for a
    title: defaults to False (exclude).
    """
    if not titles:
        return {}

    providers = ROLE_PROVIDERS
    if not providers:
        # Legacy single-provider fallback
        from config import LLM_API_KEY, LLM_MODEL, LLM_BASE_URL
        providers = [{
            "name": LLM_PROVIDER,
            "api_key": LLM_API_KEY,
            "model": LLM_MODEL,
            "base_url": LLM_BASE_URL,
            "max_batch_chars": 6_000 if LLM_PROVIDER == "cerebras" else 400_000,
            "min_call_interval": 12.5 if LLM_PROVIDER == "cerebras" else 0.0,
        }]

    # ── Split titles round-robin across providers ──
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

    # ── Failover: reassign each failed provider's batch to the OTHER
    # providers for this stage and retry once. Only one failover round —
    # this is "the other providers pick it up", not an endless cascade. ──
    if failed_batches and len(providers) > 1:
        retry_work = []
        # 2026-09 ROUND 6: one aggregated line for the WHOLE failover round
        # instead of one log.warning() per failed batch (same spam class as
        # the _run_round fix above — a provider with many small failed
        # batches used to print one "reassigning to..." line each).
        _reassign_total_titles = 0
        _reassign_from = set()
        _reassign_to = set()
        for failed_pname, batch in failed_batches:
            survivors = [p for p in providers if p["name"] != failed_pname]
            if not survivors:
                for t in batch:
                    results.setdefault(t, False)
                continue
            _reassign_total_titles += len(batch)
            _reassign_from.add(failed_pname)
            _reassign_to.update(p["name"] for p in survivors)
            sub_assignments = {p["name"]: [] for p in survivors}
            for i, title in enumerate(batch):
                p = survivors[i % len(survivors)]
                sub_assignments[p["name"]].append(title)
            for p in survivors:
                p_titles = sub_assignments[p["name"]]
                if not p_titles:
                    continue
                client = _get_role_client(p)
                if not client:
                    retry_work.append((p, None, p_titles))
                    continue
                for sub_batch in _build_role_batches(p_titles, max_chars=p["max_batch_chars"]):
                    retry_work.append((p, client, sub_batch))

        if _reassign_total_titles:
            log.warning(
                f"Role classification: {', '.join(sorted(_reassign_from))} failed on "
                f"{_reassign_total_titles} title(s) total this round — reassigning to "
                f"{', '.join(sorted(_reassign_to))}"
            )

        failed_batches = []
        if retry_work:
            _run_round(retry_work)
        # Anything that failed AGAIN on the failover round is defaulted —
        # no second failover cascade.
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
]

GLOBAL_RE = [re.compile(kw, re.I) for kw in GLOBAL_KEYWORDS]

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

    loc_stripped = loc.strip().lower()
    is_bare = (
        not loc_stripped
        or loc_stripped in ("remote", "remote worker", "remote job", "fully remote")
        or PLACEHOLDER_LOC_RE.match(loc)
    )
    if not is_bare:
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
            if loc_stripped and "remote" in loc_stripped:
                return f"Remote, {geo}"
            return geo

    return loc


# ── Location priority tiers (for sort order on upsert) ────
# Lower number = higher priority. Populates jobs.location_priority (the
# column already existed in the schema, unused, before this).
PRIORITY_GLOBAL = 1   # explicit worldwide/anywhere/global-hiring signal
PRIORITY_AFRICA = 2   # Africa (continent) or bare EMEA match
PRIORITY_UNSURE = 3   # kept as a plausible match, but geographic scope
                       # wasn't confirmed by keyword OR AI evidence


# ── Africa-continent detection ────────────────────────────
# Deliberately a NARROW, standalone check — just "does a real African
# country's full name appear as a whole word" — rather than routing
# through geo.extract_countries()'s full multi-country machinery (state
# codes, ISO2 prefixes, trailing-country-code rules, etc.). None of that
# apparatus is needed here: no African country name in this project's
# gazetteer collides with a US state/Canadian province name the way
# "Mexico"/"Wales"/"Ontario" did, so a bare word-boundary match is safe.
_AFRICAN_COUNTRY_RE = re.compile(
    r"\b(" + "|".join(re.escape(c) for c in sorted(geo.AFRICAN_COUNTRIES, key=len, reverse=True)) + r")\b",
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

    # ── 0.5. HARD OVERRIDE: scraper-reported workplace_type says this
    # specific posting is Hybrid/On-site/In-office/In-person, regardless
    # of what the bare location field claims (e.g. location="Remote" but
    # workplace_type="Hybrid"). See has_non_remote_workplace_type's
    # docstring for the real Infor/Pinpoint posting this closes. ──
    if has_non_remote_workplace_type(job):
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

    # ── 0.86. HARD OVERRIDE (2026-09, cross-LLM review, real posting:
    # Stripe's "Program Manager, Security GRC"): Greenhouse's own metadata
    # names a specific, non-global place even though the location FIELD
    # this project reads said nothing more specific than "Remote". See
    # has_hard_metadata_location_signal's docstring. ──
    if has_hard_metadata_location_signal(job):
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
    loc_lower = loc.lower()

    # ── 1. Empty / placeholder → UNSURE (send to AI) ──────
    if not loc.strip() or PLACEHOLDER_LOC_RE.match(loc):
        return "unsure", None, "blank"

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
    # a superset case, not a competing one. Bucketed at PRIORITY_AFRICA,
    # the same tier bare EMEA already uses (broader than a single region,
    # narrower than an explicit "global"/"worldwide" claim).
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

    # ── 5. Bare "Remote" with nothing else qualifying it → UNSURE
    # (send to AI). Any OTHER text attached to "remote" (a city, a
    # country, "hybrid", "US only", etc.) is a real qualifier and gets
    # rejected outright, per the strict-allowlist policy above. ──
    if has_remote:
        stripped = NON_GEO_WORDS_RE.sub("", loc_lower)
        stripped = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9]+", " ", stripped).strip()
        if not stripped:
            return "unsure", None, "bare_remote"
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
    return any(rx.search(t) for rx in _SAFETY_NET_GLOBAL_RE)


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
across the EMEA region (Europe/Middle East/Africa), or from anywhere on \
the African continent. Everything else — including roles genuinely open \
to remote candidates but restricted to a single country or a narrower \
region (APAC, LATAM, one specific country, etc.) — must be excluded.

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

Respond with exactly one of these four labels per job:

MATCH_GLOBAL — positive evidence of genuinely worldwide hiring:
- Description or title explicitly says "global", "worldwide", "anywhere \
  in the world", "international", "work from anywhere", "distributed \
  team", "location-agnostic", "hire in any country", or a clear \
  equivalent
- Hiring across many countries spanning multiple continents (not just \
  "a few offices" — genuine "we hire wherever you are" language)
- No geographic restrictions AND the role/company context clearly \
  supports global openness (e.g. "our fully remote team spans 30+ \
  countries across 6 continents")

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

MATCH_AFRICA — positive evidence of hiring across the African continent \
(as a continent, not a single African country) or across the EMEA region:
- Description or title explicitly says "Africa" (as a hiring region, \
  not just "we have a Cape Town office") or names 2+ different African \
  countries as places the company hires from
- Description or title says "EMEA" with no further single-country/city \
  qualifier narrowing it back down to one place
- A single African country alone (e.g. "based in Nigeria", "Kenya \
  office only") is NOT enough — that's one country, not the continent

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

UNCERTAIN — cannot determine either way after reading everything given:
- No description available, or description genuinely says nothing about \
  location/eligibility
- Ambiguous or conflicting signals that don't clearly resolve to one of \
  the above

IMPORTANT: When there is no description or no clear signal, say \
UNCERTAIN. Do NOT default to MATCH_GLOBAL or MATCH_AFRICA — only use \
those when you see real positive evidence, per the definitions above. \
When in doubt, UNCERTAIN.

Respond ONLY with lines like:
1 MATCH_GLOBAL
2 MATCH_AFRICA
3 NO_MATCH
4 UNCERTAIN"""


def _classify_location_batch(batch_jobs: list[dict], provider: dict, client) -> tuple[list[str], bool]:
    """Classify a single batch of jobs by location using a specific provider.

    Descriptions are sent IN FULL (only bounded by MAX_DESC_CHARS, applied
    once already in _build_dynamic_batches) — no further per-batch slicing.
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
    text = _ai_call(provider, client, LOCATION_SYSTEM_PROMPT, user_msg, max_tokens=max_tokens)

    batch_results = ["uncertain"] * len(batch_jobs)
    if text is None:
        log.warning(f"AI location classification failed ({provider['name']}) "
                     f"for batch of {len(batch_jobs)}, keeping as uncertain")
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

    providers = LOCATION_PROVIDERS

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
    # descriptions here go up to 30,000 chars each (MAX_DESC_CHARS) — so
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

    def _run_round(work):
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
                f = pool.submit(_classify_location_batch, batch, provider, client)
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
        for orig_idx, job in failing:
            candidates = [
                p for p in providers
                if p["name"] not in tried[orig_idx]
                and p["name"] not in _exhausted_providers_today
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

        log.warning(
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

    # ── Post-AI safety net (2026-09) ──────────────────────────────────
    # Real case this closes: a JazzHR posting (starlims.applytojob.com/
    # apply/tY0FHXuKkf/Account-Manager-Expansions) with a blank location
    # field and a description containing NO global/worldwide/anywhere
    # language whatsoever (confirmed via direct fetch of the live
    # posting) was still returned as match_global by the AI stage,
    # landing it at PRIORITY_GLOBAL — the highest-trust tier — with
    # literally zero supporting evidence anywhere in the job's own text.
    # PRIORITY_GLOBAL/PRIORITY_AFRICA are meant to mean "we have real
    # positive evidence", so a match the AI itself can't back with any
    # of the same keyword evidence the deterministic stage already
    # trusts is downgraded to 'uncertain' rather than accepted at face
    # value — this doesn't drop the job, it just stops an unsupported
    # AI claim from outranking genuinely-confirmed matches. A job can
    # still reach match_global/match_africa normally when the AI finds
    # real evidence the keyword regexes don't happen to cover; this only
    # catches the case where the AI's own verdict has NO textual backing
    # at all.
    for i, (label, provider_name) in enumerate(results):
        if label not in ("match_global", "match_africa"):
            continue
        job = jobs[i]
        text = (job.get("title") or "") + " " + (job.get("description_snippet") or "")
        if label == "match_global" and not _text_has_global_evidence(text):
            log.debug(f"Downgrading unsupported match_global → uncertain for "
                      f"{job.get('url', job.get('title', '?'))!r} (no global "
                      f"keyword evidence in title/description)")
            results[i] = ("uncertain", provider_name)
        elif label == "match_africa" and not _text_has_africa_or_emea_evidence(text):
            log.debug(f"Downgrading unsupported match_africa → uncertain for "
                      f"{job.get('url', job.get('title', '?'))!r} (no Africa/"
                      f"EMEA keyword evidence in title/description)")
            results[i] = ("uncertain", provider_name)

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
             f"{genuinely_uncertain} uncertain (kept, lower confidence)"
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
    r"\bsponsor(?:ship|ed|ing|s)?\b|\bwork\s*permits?\b|\bimmigration\s*sponsorship\b",
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
    r"without|n\'t)\b",
    re.I,
)

# Negation that shows up AFTER the topic word instead of before it
# ("sponsorship is not available", "visa sponsorship: unavailable").
_SPONSOR_UNAVAILABLE_RE = re.compile(
    r"\bunavailable\b|\bnot\s+(?:currently\s+|presently\s+)?(?:offered|provided|available|possible|"
    r"something\s+(?:we|the\s+company)\s+(?:can\s+)?(?:offer|provide|do))\b",
    re.I,
)

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
            has_genuine_topic = False
            for m in _SPONSOR_TOPIC_RE.finditer(clause):
                window = clause[max(0, m.start() - 20):m.end() + 30]
                if _SPONSOR_NON_VISA_RE.search(window):
                    continue
                has_genuine_topic = True
                break
            if not has_genuine_topic:
                continue
            if _SPONSOR_NEGATION_RE.search(clause) or _SPONSOR_UNAVAILABLE_RE.search(clause):
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
    r"u\.?s\.?a?\.?|united\s+states(?:\s+of\s+america)?|u\.?k\.?|united\s+kingdom|"
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
    r"north\s+america|americas|mena|middle\s+east|"
    r"anz|dach|benelux|nordics?"
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
    r"americas|mena|middle\s+east|anz|dach|benelux|nordics?)\b",
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
    r"\b(?:must\s+(?:be|have|currently\s+be)\s+)?(?:currently\s+)?"
    r"(?:legally\s+)?(?:authorized|authorised|eligible|entitled|permitted)\s+to\s+work\s+"
    r"(?:lawfully\s+)?(?:for\s+(?:any|an)\s+employer\s+)?(?:lawfully\s+)?(?:in|within)\s+"
    r"(?:the\s+)?(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\b(?:us|u\.s\.|uk|u\.k\.|canadian|australian|british|indian|german|irish)\s+work\s+authoriz"
    r"|\bwork\s+authoriz\w*\s+(?:in|for)\s+(?:the\s+)?(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    # 2026-09 NEW (real posting: Aptive's "Program Manager," iCIMS —
    # "Legal authorization to work in the U.S." — a NOUN-phrase statement,
    # not the "authorized to work in" VERB-phrase question every other
    # alternative above expects). Confirmed via a cross-LLM review of live
    # JD text; this exact phrasing never matched any prior alternative.
    r"|\bauthoriz(?:ation|ations)\s+to\s+work\s+(?:in|within)\s+(?:the\s+)?(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bmust\s+(?:currently\s+)?reside\s+in\s+(?:the\s+)?(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bright\s+to\s+work\s+in\s+(?:the\s+)?(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\b"
    r"|\bmust\s+have\s+(?:a\s+)?valid\s+(?:us|u\.s\.|uk|canadian|australian|indian)\s+work\s+(?:visa|permit)\b"
    r"|\b(?:u\.?s\.?a?\.?|united\s+states|u\.?k\.?|united\s+kingdom|canadian|australian|irish|german|indian)\s+"
    r"citizen(?:ship)?\b",
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
    r"(?:either\s+)?(?:the\s+)?(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    r"|\bremote\s+(?:in|within|from)\s+(?:the\s+)?(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b"
    r"|\bremote\s*\(\s*(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\s*\)"
    r"|\bwork(?:ing)?\s+from\s+"
    r"(?:anywhere\s+)?(?:only\s+|solely\s+|primarily\s+)?(?:in\s+)?(?:the\s+)?(?:" + _RESIDENCE_PLACE_RE_FRAGMENT + r")\b",
    re.I,
)

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
    for sentence in re.split(r"(?<=[.!?])\s+|\n+", text):
        if not sentence.strip():
            continue
        if _TEAM_OR_COMPANY_CONTEXT_RE.search(sentence):
            continue
        if _has_multi_region_breadth(sentence):
            continue
        if _COUNTRY_BASED_RESTRICTION_RE.search(sentence):
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
        if value and not _text_has_global_evidence(value):
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
    r"\(\s*(?:" + _COUNTRY_AUTH_NAMES_RE_FRAGMENT + r")\s+based\s*\)", re.I,
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

# 2026-09: real, full 50-state-plus-DC set (2-letter USPS abbreviations)
# used by has_state_list_restriction_signal below to validate a
# comma-separated run of 2-letter tokens is genuinely a list of U.S.
# states, not a coincidental run of unrelated 2-letter acronyms.
_US_STATE_ABBRS = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
}
# 3+ comma-separated 2-letter uppercase tokens, anywhere in the text.
_STATE_ABBR_RUN_RE = re.compile(r"\b([A-Z]{2}(?:\s*,\s*[A-Z]{2}){2,})\b")


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
            return True
    return False


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
    r"|\bwilling\s+to\s+(?:work|come|be)\s+(?:in|to|at)\s+(?:our|the|this|your)?\s*"
    r"[\w\s]{0,30}?\boffice\b"
    r"|\brequired?\s+to\s+(?:be\s+)?(?:in|at)\s+(?:the|our|a)\s+office\b"
    r"|\bin[\s\-]office\s+\d+\s*days?\b",
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
    r"(?:we|the\s+company|\w+)\s+(?:have|has)\s+(?:an?\s+)?(?:legal\s+)?entity\b"
    r"|\b(?:must|will)\s+be\s+employed\s+through\s+(?:our|a|an)\s+(?:eor|peo)\b"
    r"|\bonly\s+(?:able\s+to\s+)?onboard(?:ed)?\s+(?:candidates\s+)?through\s+(?:our|a|an)\s+(?:eor|peo)\b"
    r"|\bcountries\s+(?:where\s+)?(?:we|the\s+company)\s+(?:currently\s+)?(?:have|has)\s+"
    r"(?:an?\s+)?(?:eor|peo|payroll)\s+(?:partner|provider|entity|presence)\b",
    re.I,
)

# Part 2: explicit exclusion phrasing — "not open/available to candidates
# OUTSIDE of <place/list>" is the mirror image of "only open to candidates
# IN <place>": both restrict eligibility to one place, just phrased from the
# opposite direction. Requires the "outside" construction paired with an
# eligibility verb (open/available/accept/consider), not a bare "outside"
# anywhere in the text.
_EXCLUSION_OUTSIDE_RE = re.compile(
    r"\b(?:not|isn'?t|is\s+not)\s+(?:currently\s+)?"
    r"(?:open|available|accepting\s+applications)\s+(?:to|for)\s+"
    r"(?:candidates|applicants)?[\w\s]{0,20}?\boutside\s+(?:of\s+)?(?:the\s+)?[\w\s,&]{0,40}"
    r"|\b(?:cannot|can'?t|do\s+not|don'?t)\s+(?:accept|consider)\s+"
    r"(?:applications|candidates|applicants)\s+(?:located\s+|based\s+)?(?:from\s+)?outside\s+(?:of\s+)?"
    r"|\bunable\s+to\s+consider\s+(?:candidates|applicants)\s+(?:located\s+|based\s+)?outside\b",
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
    r"|\bwe\s+only\s+hire\s+in\s+the\s+following\s+countries\b",
    re.I,
)


def has_entity_or_exclusion_restriction_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does this job's description state
    — via a company-capability statement ("we don't have a legal entity in
    your country", "can only employ where we have an EOR/PEO"), an explicit
    exclusion ("not open to candidates outside the US"), or a curated-list
    phrasing ("the following countries only") — that eligibility is
    restricted to a specific place or list, even though none of these
    phrasings look like the classic "must reside/be based in <country>"
    shape the other hard overrides already catch? See the module comments
    above _ENTITY_PAYROLL_RESTRICTION_RE, _EXCLUSION_OUTSIDE_RE, and
    _COUNTRY_LIST_ONLY_RE. Per the user's explicit design constraint, none
    of these fire on bare "EOR"/"PEO"/"payroll"/"country"/"region" alone —
    only on the full construction.
    """
    desc = job.get("description_snippet") or ""
    text = desc + " " + (job.get("title") or "")
    if not text.strip():
        return False
    if _ENTITY_PAYROLL_RESTRICTION_RE.search(text) or _COUNTRY_LIST_ONLY_RE.search(text):
        return True
    for sentence in re.split(r"(?<=[.!?])\s+|\n+", text):
        if not sentence.strip():
            continue
        if _TEAM_OR_COMPANY_CONTEXT_RE.search(sentence):
            continue
        if _EXCLUSION_OUTSIDE_RE.search(sentence):
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
    r"in\s+the\s+same\s+time\s*zone\s+as\b",
    re.I,
)

# Part 5: a narrow, explicit relocation REQUIREMENT naming a specific place
# — "willing to relocate to the United States" / "must relocate to our
# Austin office" — distinct from a company merely mentioning relocation
# assistance/packages exist (which says nothing about restricting who may
# apply from where and is intentionally NOT matched here).
_RELOCATION_REQUIRED_RE = re.compile(
    r"\b(?:willing|must\s+be\s+willing|required|willingness)\s+to\s+relocate\s+to\s+"
    r"(?:the\s+)?[\w\s,]{0,40}"
    r"|\bmust\s+relocate\s+to\s+(?:the\s+)?[\w\s,]{0,40}"
    r"|\brequires?\s+relocation\s+to\s+(?:the\s+)?[\w\s,]{0,40}",
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


def has_timezone_relocation_or_hyphenated_restriction_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does this job's description state
    a residence-verb-governed timezone requirement ("must be located in a
    US timezone" — see _TIMEZONE_LOCATION_RE's module comment for why this
    is distinct from safe "overlap" scheduling wording), an explicit
    relocation requirement naming a place (_RELOCATION_REQUIRED_RE), or a
    hyphenated "<place>-based candidates only" construction
    (_HYPHENATED_BASED_ONLY_RE)? Checked sentence-by-sentence with the same
    team/office-context guard and global-evidence guard as
    has_hard_country_based_restriction_signal above, since all three of
    these phrasings could in principle appear in a sentence describing the
    COMPANY's own timezone/location rather than a candidate requirement.
    """
    desc = job.get("description_snippet") or ""
    text = desc + " " + (job.get("title") or "")
    if not text.strip():
        return False
    for sentence in re.split(r"(?<=[.!?])\s+|\n+", text):
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
        # _HYPHENATED_BASED_ONLY_RE ("US-based candidates only") has no
        # office/team-referencing shape, so the team-context guard is kept
        # here to stay consistent with the rest of the file's pattern.
        if not _TEAM_OR_COMPANY_CONTEXT_RE.search(sentence) and _HYPHENATED_BASED_ONLY_RE.search(sentence):
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
    return bool(_NON_REMOTE_WORKPLACE_RE.search(wt))
