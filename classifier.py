"""
Two-stage classifier — multi-provider architecture.

Role classification:     Cerebras + Groq + NVIDIA NIM (free tiers, concurrent)
Location classification: Gemini + OpenAI GPT-4.1 nano + NVIDIA NIM (concurrent)

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
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import ROLE_PROVIDERS, LOCATION_PROVIDERS, LOCATION_PROVIDER, LLM_PROVIDER
import geo

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
    Returns response text or None on failure."""
    name = provider["name"]
    if name in _exhausted_providers_today:
        # Already confirmed out for this run (see _mark_exhausted) — skip
        # the wasted network call and the repeat log line entirely.
        # Callers see this exactly like any other failed call (None), so
        # the existing cross-provider failover path still applies.
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
            # Every remaining path is a genuine give-up on this provider
            # for this call — exhausted retries on an ordinary rate limit,
            # or any other non-retryable API error. Per the circuit-breaker
            # policy above, this now marks the provider exhausted for the
            # rest of the run too, not just this one batch.
            _mark_exhausted(name, f"API error: {e}")
            return None
    _mark_exhausted(name, "exhausted all retries")
    return None


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
    _known_role_providers = {"gemini", "groq"}
    _active = {p["name"] for p in providers}
    _missing = _known_role_providers - _active
    if _missing:
        log.warning(f"Role AI running with {len(_active)}/2 providers "
                    f"({', '.join(sorted(_active)) or 'none'}) — missing "
                    f"{', '.join(sorted(_missing))} (no API key set). No "
                    f"failover if this one struggles.")

    provider_summary = ", ".join(
        f"{p['name']}:{len(provider_titles[p['name']])}" for p in providers
    )
    log.info(f"Role classification: {len(titles)} titles → {len(all_work)} batches "
             f"across {len(providers)} providers ({provider_summary})")

    results = {}
    failed_batches = []  # [(failed_provider_name, batch_titles), ...]
    no_ai_read: set[str] = set()  # titles never actually reviewed by a provider

    def _run_round(work):
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
                    log.error(f"Role classification error ({pname}): {e}")
                    failed_batches.append((pname, batch))
                    no_ai_read.update(batch)

    _run_round(all_work)

    # ── Failover: reassign each failed provider's batch to the OTHER
    # providers for this stage and retry once. Only one failover round —
    # this is "the other providers pick it up", not an endless cascade. ──
    if failed_batches and len(providers) > 1:
        retry_work = []
        for failed_pname, batch in failed_batches:
            survivors = [p for p in providers if p["name"] != failed_pname]
            if not survivors:
                for t in batch:
                    results.setdefault(t, False)
                continue
            log.warning(
                f"Role classification: {failed_pname} failed on a "
                f"{len(batch)}-title batch — reassigning to "
                f"{', '.join(p['name'] for p in survivors)}"
            )
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
    r"\blocation\s*agnostic\b",
    r"\blocation\s*independent\b",
    r"\bgeo[\-\s]*flexible\b",
    r"\bgeo[\-\s]*agnostic\b",
    r"\bborderless\b",
    r"\bunrestricted\s*location\b",
    r"\bno\s*location\s*restriction\b",
    r"\b(fully\s*)?distributed\b",
    r"\bdistributed\s*team\b",
    r"\bdistributed\s*workforce\b",
    r"\b(global|international)\s*team\b",
    r"\ball\s*geograph",
    r"\bany\s*country\b",
    r"\ball\s*countries\b",
    r"\bany\s*location\b",
    r"\ball\s*locations?\b",
    r"\bno\s*location\s*(requirement|restriction|preference)\b",
    r"\bno\s*geographic\s*restriction\b",
    r"\bno\s*country\s*restriction\b",
    # Time-zone framed global signals — "any time zone" / "regardless of
    # time zone" is a strong proxy for "we don't restrict by geography"
    r"\btime[\-\s]*zone\s*agnostic\b",
    r"\bany\s*time\s*zone\b",
    r"\bany\s*timezone\b",
    # Explicit "we don't care where you are" phrasings
    r"\bregardless\s*of\s*(location|country|time\s*zone|timezone)\b",
    r"\birrespective\s*of\s*(location|country)\b",
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
    r"multiple|countries|country|regions|region|restriction|requirement|preference|"
    r"geographic|any|all|no|talent|pool|candidates|applicants|open|hire|hiring|"
    r"globally|time|zone|timezone|regardless|irrespective|of|welcome|eligible|"
    r"in|work|from"
    r")\b",
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
    r"US|USA|UK|EU|EMEA|APAC|LATAM|ANZ|NAM|MENA|CA|AU|IN|DE|FR|NL|SG|HK|JP|BR|MX|PH|NG|KE|ZA|AE|SA|IL|"
    r"PL|CZ|RO|BG|HU|IE|ES|IT|PT|SE|NO|DK|FI|CH|AT|BE|NZ"
)
# Global/Africa-hiring words allowed in the same delimiter-anchored
# positions as the codes above (e.g. "CSM - Global", "Account Manager -
# Worldwide", "Distributed - Support Engineer").
_TITLE_GLOBAL_WORDS = r"Global|Worldwide|International|Africa|Distributed|Anywhere|Borderless"
_TITLE_CODES_OR_GLOBAL = _TITLE_CODES + r"|" + _TITLE_GLOBAL_WORDS

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

    # ── 3. EMEA → match ONLY if no country/city qualifier ─
    if re.search(r"\bemea\b", loc_lower):
        check = re.sub(r"\bemea\b", "", loc_lower)
        check = NON_GEO_WORDS_RE.sub("", check)
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


def _dynamic_job_cap(jobs: list[dict]) -> int:
    """5-7 jobs per batch (2026-09, explicit user request: 'reduce batch
    size for location classification to between 5 to 7 max, dynamically
    sized based on the number of characters so the LLMs does not lose
    context'). Scaled down toward 5 as the jobs being batched have longer
    descriptions — more text per job in one call means less of the model's
    attention per job, which is the exact confirmed-live failure mode
    (see MAX_JOBS_PER_BATCH's history below). Based on the AVERAGE
    description length across the jobs being batched, since the cap
    applies to the batch as a whole, not any single job.
    """
    if not jobs:
        return 7
    total = sum(len(j.get("description_snippet") or "") for j in jobs)
    avg = total / len(jobs)
    if avg <= 2_000:
        return 7
    if avg <= 8_000:
        return 6
    return 5


def _build_dynamic_batches(jobs: list[dict], max_batch_chars: int) -> list[tuple[int, list[dict]]]:
    """Build batches dynamically based on description length.

    Char-budget driven, BUT also capped by job count. Many jobs have no
    description ("[No description available]" is only ~30 chars), so a
    pure char-budget batch can silently balloon to hundreds/thousands of
    jobs. The model's response is one line per job, and output tokens are
    scaled to job count (see _classify_location_batch) — so job count,
    not character count, is what actually bounds a safely-sized response.
    """
    OVERHEAD_PER_JOB = 120
    # Matches ats_scrapers._snippet's default cap — descriptions are already
    # bounded there, so this is just a defensive re-assertion, not the
    # primary truncation point. 30,000 chars is large enough that no real
    # job description is ever actually cut off by it.
    MAX_DESC_CHARS = 30_000
    # 120 -> 10 -> 5-7 dynamic (2026-09, explicit user request): a batch of
    # up to 120 jobs in one bulk "one-line-verdict-per-job" call is exactly
    # the shape that let cheap/small models cut corners — this is the same
    # call shape as the confirmed live failure where gpt-4.1-nano
    # hallucinated a match_global verdict with zero supporting text
    # anywhere in the job (see classifier.py's "Post-AI safety net" section
    # in ai_classify_locations for the real posting this closes). First
    # reduced to a flat 10, now made dynamic (5-7, via _dynamic_job_cap)
    # so long-description batches get even more of the model's attention
    # per job than a flat cap would give them. Even at the floor (5/batch),
    # a real run's ~74 unsure jobs on Gemini/OpenAI is ~15 calls and ~141
    # on NVIDIA is ~29 calls per run — still comfortably inside every
    # provider's RPM budget. Deliberately NOT applied to role classification
    # (_build_role_batches, a separate function) — role verdicts are just a
    # short title, not a full JD, so the same bulk-call risk doesn't apply
    # there; left unchanged per explicit instruction.
    MAX_JOBS_PER_BATCH = _dynamic_job_cap(jobs)

    batches = []
    current_batch = []
    current_chars = 0
    start_idx = 0

    for i, job in enumerate(jobs):
        desc = job.get("description_snippet") or ""
        if len(desc) > MAX_DESC_CHARS:
            job["description_snippet"] = desc[:MAX_DESC_CHARS]
            desc = job["description_snippet"]
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

    Cross-provider failover: if a provider's batch fails outright (after
    its own MAX_RETRIES=3 attempts inside _ai_call, or its client can't be
    built), that batch's jobs are reassigned across the OTHER providers
    and retried once before falling back to 'uncertain' — see the module
    docstring.

    Returns a list of (label, provider_name) tuples in the same order as
    `jobs` — label is one of 'match_global', 'match_africa', 'no_match',
    or 'uncertain'; provider_name is whichever LOCATION_PROVIDERS entry
    actually produced that label (None if every provider failed for that
    job, including the failover round). On failure: defaults to
    ('uncertain', None) (include with flag).

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
    """
    if not jobs:
        return []

    providers = LOCATION_PROVIDERS

    # 2026-09: surface missing providers up front. LOCATION_PROVIDERS
    # silently drops any of gemini/openai/nvidia whose API key env var
    # isn't set (see config.py's _make_provider) — running location
    # classification on 1 provider instead of 3 isn't wrong, but it cuts
    # both capacity and failover coverage a lot, and previously the only
    # sign of it was the raw HTTP request log for whichever provider(s)
    # were actually left — nothing said the others were missing. This is
    # the single most likely explanation for "why did so few jobs get a
    # real AI verdict this run."
    _known_location_providers = {"nvidia", "openai", "groq"}
    _active = {p["name"] for p in providers}
    _missing = _known_location_providers - _active
    if _missing:
        log.warning(f"Location AI running with {len(_active)}/3 providers "
                    f"({', '.join(sorted(_active)) or 'none'}) — missing "
                    f"{', '.join(sorted(_missing))} (no API key set). Lower "
                    f"throughput and no failover if this one struggles.")

    # ── Round-robin assign jobs to providers (tracking original indices) ──
    provider_assignments = {p["name"]: [] for p in providers}  # name → [(orig_idx, job)]
    for i, job in enumerate(jobs):
        p = providers[i % len(providers)]
        provider_assignments[p["name"]].append((i, job))

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
        batches = _build_dynamic_batches(assigned_jobs, p["max_batch_chars"])
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
                    log.error(f"Location classification error ({pname}): {e}")
                    failed_batches.append((pname, orig_indices, batch))
                    no_ai_read.update(orig_indices)

    _run_round(all_work)

    # ── Failover: reassign each failed provider's jobs to the OTHER
    # providers for this stage and retry once — one round only. ──
    if failed_batches and len(providers) > 1:
        retry_work = []
        for failed_pname, orig_indices, batch in failed_batches:
            survivors = [p for p in providers if p["name"] != failed_pname]
            if not survivors:
                continue  # results already default to ('uncertain', None)
            log.warning(
                f"Location classification: {failed_pname} failed on a "
                f"{len(batch)}-job batch — reassigning to "
                f"{', '.join(p['name'] for p in survivors)}"
            )
            sub_assignments = {p["name"]: [] for p in survivors}  # name -> [(orig_idx, job)]
            for i, (orig_idx, job) in enumerate(zip(orig_indices, batch)):
                p = survivors[i % len(survivors)]
                sub_assignments[p["name"]].append((orig_idx, job))
            for p in survivors:
                assigned = sub_assignments[p["name"]]
                if not assigned:
                    continue
                client = _get_location_client(p)
                assigned_jobs = [job for _, job in assigned]
                assigned_indices = [idx for idx, _ in assigned]
                if not client:
                    retry_work.append((p, None, assigned_indices, assigned_jobs))
                    continue
                for start_idx, sub_batch in _build_dynamic_batches(assigned_jobs, p["max_batch_chars"]):
                    sub_orig_indices = assigned_indices[start_idx:start_idx + len(sub_batch)]
                    retry_work.append((p, client, sub_orig_indices, sub_batch))

        failed_batches = []
        if retry_work:
            _run_round(retry_work)
        # Anything that failed AGAIN on the failover round stays
        # ('uncertain', None) — no second failover cascade.

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

_SPONSOR_TOPIC_RE = re.compile(
    r"\bsponsor(?:ship|ed|ing|s)?\b|\bwork\s*permits?\b|\bimmigration\s*sponsorship\b",
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
    """True if any sentence/line in `text` mentions the sponsorship/work-
    permit topic AND carries negation or unavailability language
    somewhere in that same sentence, in any order or distance apart."""
    if not text:
        return False
    for sentence in _SPONSOR_SENTENCE_SPLIT_RE.split(text):
        if not _SPONSOR_TOPIC_RE.search(sentence):
            continue
        if _SPONSOR_NEGATION_RE.search(sentence) or _SPONSOR_UNAVAILABLE_RE.search(sentence):
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
_COUNTRY_AUTH_RE = re.compile(
    r"\b(?:must\s+(?:be|have|currently\s+be)\s+)?(?:currently\s+)?"
    r"(?:legally\s+)?(?:authorized|authorised|eligible|entitled|permitted)\s+to\s+work\s+in\s+"
    r"(?:the\s+)?(?:u\.?s\.?a?\.?|united\s+states(?:\s+of\s+america)?|u\.?k\.?|"
    r"united\s+kingdom|canada|australia|new\s+zealand|ireland|germany|"
    r"european\s+union|\beu\b)\b"
    r"|\b(?:us|u\.s\.|uk|u\.k\.|canadian|australian|british)\s+work\s+authoriz"
    r"|\bwork\s+authoriz\w*\s+(?:in|for)\s+(?:the\s+)?(?:us|u\.s\.|usa|united\s+states|uk|canada|australia)\b"
    r"|\bmust\s+(?:currently\s+)?reside\s+in\s+(?:the\s+)?(?:us|usa|united\s+states|uk|canada|australia)\b"
    r"|\bright\s+to\s+work\s+in\s+(?:the\s+)?(?:us|usa|united\s+states|uk|canada|australia)\b"
    r"|\bmust\s+have\s+(?:a\s+)?valid\s+(?:us|u\.s\.|uk|canadian|australian)\s+work\s+(?:visa|permit)\b",
    re.I,
)

# Marker ats_scrapers.py's enrich_application_questions() appends before
# a work-authorization-flavored screening question (see its own
# _WORK_AUTH_RE / _format_auth_questions) — every line carrying this
# marker is, by construction, already known to be about authorization/
# visa/sponsorship, so its mere presence is itself a hard signal
# regardless of the exact wording used in that specific question.
_APPLICATION_AUTH_QUESTION_MARKER = "Application Question:"


def has_hard_country_specific_auth_signal(job: dict) -> bool:
    """Deterministic, pre-AI hard filter: does this job's description
    (including any appended application-question text) or title contain
    an AFFIRMATIVE country-specific work-authorization requirement, or a
    work-authorization/visa/sponsorship screening question flagged by
    enrich_application_questions()? Forces NO_MATCH — see the module
    comment above _COUNTRY_AUTH_RE for the two real postings this closes.
    """
    desc = job.get("description_snippet") or ""
    if _APPLICATION_AUTH_QUESTION_MARKER in desc:
        return True
    text = desc + " " + (job.get("title") or "")
    return bool(_COUNTRY_AUTH_RE.search(text))


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
