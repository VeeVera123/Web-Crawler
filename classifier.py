"""
Two-stage classifier — multi-provider architecture.

Role classification:  Cerebras + Groq (free tiers, concurrent)
Location classification: Gemini + OpenAI GPT-4.1 nano (concurrent)

Falls back to single-provider mode if only LLM_PROVIDER is set.

  Stage 1 — keyword filter for CSM/AM role titles (fast, no API)
  Stage 2 — AI for ambiguous titles (batched, multi-provider concurrent)

Then a separate location filter:
  Stage 3 — keyword check for Africa/Global locations
  Stage 4 — AI for ambiguous locations (OpenAI only)
"""

import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import ROLE_PROVIDERS, LOCATION_PROVIDERS, LOCATION_PROVIDER, LLM_PROVIDER
import geo

log = logging.getLogger(__name__)

# ── Provider-specific AI client setup ─────────────────────
MAX_RETRIES = 4
RETRY_BASE_DELAY = 5  # seconds


def _make_client(provider: dict):
    """Create an OpenAI-compatible client for a provider config dict."""
    from openai import OpenAI
    return OpenAI(api_key=provider["api_key"], base_url=provider["base_url"])


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


def _ai_call(provider: dict, client, system_prompt: str, user_msg: str, max_tokens: int = 500) -> str | None:
    """Call an OpenAI-compatible provider with retry on rate limit.
    Returns response text or None on failure."""
    name = provider["name"]
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
                return None
            return content.strip()
        except Exception as e:
            error_str = str(e)
            is_rate_limit = "429" in error_str or "413" in error_str or "rate" in error_str.lower()
            is_daily_limit = "tokens per day" in error_str.lower() or "daily" in error_str.lower()

            if is_daily_limit:
                log.error(f"{name} daily token limit reached — skipping remaining AI calls")
                return None
            if is_rate_limit and attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (attempt + 1)
                log.warning(f"{name} rate limit hit, retrying in {delay}s (attempt {attempt + 1})")
                time.sleep(delay)
                continue
            log.error(f"{name} API error (attempt {attempt + 1}): {e}")
            return None
    return None


# ═══════════════════════════════════════════════════════
# STAGE 1 & 2: ROLE CLASSIFICATION
# ═══════════════════════════════════════════════════════

INCLUDE_KEYWORDS = [
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

    # Retention / Renewal
    r"customer\s*retention", r"client\s*retention",
    r"retention\s*(manager|lead|specialist|director)",
    r"renewal\s*(manager|lead|specialist|director)",

    # Onboarding / Implementation (customer-facing)
    r"customer\s*onboarding", r"client\s*onboarding",
    r"onboarding\s*(manager|lead|specialist)",
    r"implementation\s*(manager|lead|specialist|consultant)",
]

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
You are a job title classifier. Decide if each title is a Customer Success \
or Account Management role.

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

NO if the role is:
- Any kind of Engineer or Developer
- Sales (SDR, BDR, Account Executive, demand gen)
- IT/Desktop/Hardware Support
- Marketing, Product, Design, HR, Finance, Legal

Respond ONLY with lines like:
1 YES
2 NO"""


def _classify_role_batch(batch: list[str], provider: dict, client) -> dict[str, bool]:
    """Classify a single batch of titles using a specific provider."""
    numbered = "\n".join(f"{j+1}. {t}" for j, t in enumerate(batch))
    user_msg = f"Titles:\n{numbered}"
    max_tokens = max(500, len(batch) * 4)
    text = _ai_call(provider, client, ROLE_SYSTEM_PROMPT, user_msg, max_tokens=max_tokens)

    results = {}
    if text is None:
        log.warning(f"AI role classification failed ({provider['name']}) for batch of {len(batch)}, defaulting to exclude")
        for t in batch:
            results[t] = False
        return results

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
    return results


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


def ai_classify_roles(titles: list[str]) -> dict[str, bool]:
    """Send ambiguous titles to AI for role classification.

    Multi-provider mode: splits titles across Cerebras/Groq,
    batches per provider's context window, runs all concurrently.

    Single-provider fallback: uses whichever provider is configured.

    Returns {title: is_relevant}. On failure: defaults to False (exclude).
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
        client = _role_clients.get(p["name"])
        if not client:
            try:
                client = _make_client(p)
                _role_clients[p["name"]] = client
            except Exception as e:
                log.error(f"Cannot create client for {p['name']}: {e}")
                continue
        batches = _build_role_batches(p_titles, max_chars=p["max_batch_chars"])
        for batch in batches:
            all_work.append((p, client, batch))

    provider_summary = ", ".join(
        f"{p['name']}:{len(provider_titles[p['name']])}" for p in providers
    )
    log.info(f"Role classification: {len(titles)} titles → {len(all_work)} batches "
             f"across {len(providers)} providers ({provider_summary})")

    results = {}

    # Run ALL batches concurrently (each provider's batches interleave)
    with ThreadPoolExecutor(max_workers=len(providers)) as pool:
        future_map = {}
        for provider, client, batch in all_work:
            f = pool.submit(_classify_role_batch, batch, provider, client)
            future_map[f] = (provider["name"], batch)

        for future in as_completed(future_map):
            pname, batch = future_map[future]
            try:
                batch_results = future.result()
                results.update(batch_results)
            except Exception as e:
                log.error(f"Role classification error ({pname}): {e}")
                for t in batch:
                    results[t] = False

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
    r"\b(100%|fully|truly|genuinely)\s*global\b",
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
]

GLOBAL_RE = [re.compile(kw, re.I) for kw in GLOBAL_KEYWORDS]

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


def _keyword_classify_location_detail(job: dict) -> tuple[str, int | None]:
    """
    Returns (result, priority) where result is 'match', 'no_match', or
    'unsure', and priority (PRIORITY_GLOBAL / PRIORITY_AFRICA / None) is
    only meaningful when result == 'match'.

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
    raw_loc = job.get("location", "")
    raw_country = job.get("country", "")
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
        return "unsure", None

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
        return "match", PRIORITY_AFRICA

    african_hits = {m.group(1).lower() for m in _AFRICAN_COUNTRY_RE.finditer(loc)}
    if len(african_hits) >= 2:
        return "match", PRIORITY_AFRICA

    # ── 3. EMEA → match ONLY if no country/city qualifier ─
    if re.search(r"\bemea\b", loc_lower):
        check = re.sub(r"\bemea\b", "", loc_lower)
        check = NON_GEO_WORDS_RE.sub("", check)
        check = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9&|]+", " ", check).strip()
        if not check:
            # EMEA (Europe/Middle East/Africa) includes Africa but is
            # broader than "global" — bucketed with Africa, not Global.
            return "match", PRIORITY_AFRICA
        return "no_match", None

    # ── 4. Explicit Global/Worldwide/International/Distributed/
    # Anywhere/... keyword (see GLOBAL_KEYWORDS, ~80 variants) ──
    # Residue check: strip out the EXACT substring(s) that matched a
    # keyword, then confirm nothing else (a real city/country name) is
    # left over — "Global (Remote, US Only)" should NOT match just
    # because "Global" appears; the leftover "us only" gives it away.
    if STANDALONE_GLOBAL_RE.search(loc.strip()):
        return "match", PRIORITY_GLOBAL

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
            return "match", PRIORITY_GLOBAL
        return "no_match", None

    # ── 5. Bare "Remote" with nothing else qualifying it → UNSURE
    # (send to AI). Any OTHER text attached to "remote" (a city, a
    # country, "hybrid", "US only", etc.) is a real qualifier and gets
    # rejected outright, per the strict-allowlist policy above. ──
    if has_remote:
        stripped = NON_GEO_WORDS_RE.sub("", loc_lower)
        stripped = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9]+", " ", stripped).strip()
        if not stripped:
            return "unsure", None
        return "no_match", None

    # ── 6. REJECT everything else outright ────────────────
    # No Global/EMEA/Africa keyword, not blank, not bare "Remote" — this
    # is a job tied to a specific place (or places) with no explicit
    # broad-hiring signal, so it's rejected without going to the AI.
    return "no_match", None


def keyword_classify_location(job: dict) -> str:
    """
    Returns 'match', 'no_match', or 'unsure'.

    MATCH = truly global hiring signals (anywhere, worldwide,
    international, WFA, EMEA alone, Africa as continent).

    Thin wrapper over _keyword_classify_location_detail() for callers that
    only need the verdict, not the priority tier (e.g. ats_scrapers.py's
    application-question enrichment, which only checks for "unsure").
    """
    result, _ = _keyword_classify_location_detail(job)
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
- "no visa sponsorship", "cannot sponsor", "will not sponsor"
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


def _classify_location_batch(batch_jobs: list[dict], provider: dict, client) -> list[str]:
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
        return batch_results

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

    return batch_results


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
    MAX_JOBS_PER_BATCH = 120  # keeps AI response comfortably within token limits

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


def ai_classify_locations(jobs: list[dict]) -> list[str]:
    """
    Send ambiguous jobs (bare "Remote") to AI for location classification.
    Uses LOCATION_PROVIDERS (Gemini + OpenAI) concurrently.

    Jobs are round-robin split across providers, batched per provider's
    context window, and all batches run concurrently.

    Returns list of 'match_global', 'match_africa', 'no_match', or
    'uncertain' in same order. On rate limit/failure: defaults to
    'uncertain' (include with flag).
    """
    if not jobs:
        return []

    providers = LOCATION_PROVIDERS

    # ── Round-robin assign jobs to providers (tracking original indices) ──
    provider_assignments = {p["name"]: [] for p in providers}  # name → [(orig_idx, job)]
    for i, job in enumerate(jobs):
        p = providers[i % len(providers)]
        provider_assignments[p["name"]].append((i, job))

    # ── Build batches per provider ──
    all_work = []  # (provider, client, [(orig_idx, job)...])
    for p in providers:
        assigned = provider_assignments[p["name"]]
        if not assigned:
            continue
        client = _location_clients.get(p["name"])
        if not client:
            try:
                client = _make_client(p)
                _location_clients[p["name"]] = client
            except Exception as e:
                log.error(f"Cannot create location client for {p['name']}: {e}")
                continue
        # Build batches from assigned jobs
        assigned_jobs = [job for _, job in assigned]
        assigned_indices = [idx for idx, _ in assigned]
        batches = _build_dynamic_batches(assigned_jobs, p["max_batch_chars"])
        for start_idx, batch in batches:
            # Map batch start_idx back to original indices
            batch_orig_indices = assigned_indices[start_idx:start_idx + len(batch)]
            all_work.append((p, client, batch, batch_orig_indices))

    provider_summary = ", ".join(
        f"{p['name']}:{len(provider_assignments[p['name']])}" for p in providers
    )
    log.info(f"Location classification: {len(jobs)} jobs → {len(all_work)} batches "
             f"across {len(providers)} providers ({provider_summary})")

    results = ["uncertain"] * len(jobs)

    # Run all batches concurrently
    with ThreadPoolExecutor(max_workers=len(providers)) as pool:
        future_map = {}
        for provider, client, batch, orig_indices in all_work:
            f = pool.submit(_classify_location_batch, batch, provider, client)
            future_map[f] = (provider["name"], batch, orig_indices)

        for future in as_completed(future_map):
            pname, batch, orig_indices = future_map[future]
            try:
                batch_results = future.result()
                for j, label in enumerate(batch_results):
                    results[orig_indices[j]] = label
            except Exception as e:
                log.error(f"Location classification error ({pname}): {e}")

    classified = sum(1 for r in results if r != "uncertain")
    log.info(f"AI classified {classified}/{len(jobs)} locations "
             f"({results.count('match_global')} match_global, "
             f"{results.count('match_africa')} match_africa, "
             f"{results.count('no_match')} no_match, "
             f"{results.count('uncertain')} uncertain/unclassified)")

    return results


# ── Visa Sponsorship Detection ──────────────────────────

_VISA_YES_RE = re.compile(
    r"visa\s*sponsor|sponsor.*visa|relocation\s*(support|assist|package)"
    r"|work\s*permit\s*(support|assist|provid)"
    r"|immigration\s*(support|assist)"
    r"|we\s*sponsor"
    r"|sponsorship\s*(available|offered|provided)",
    re.I,
)

_VISA_NO_RE = re.compile(
    r"(no|not|unable|cannot|can\'t|won\'t|will\s*not)\s*(provide\s*)?(visa\s*sponsor|sponsor.*visa|work\s*permit|immigration\s*sponsor)"
    r"|must\s*(be\s*)?(authorized|eligible)\s*to\s*work"
    r"|without\s*(visa\s*)?sponsor"
    r"|visa\s*sponsorship\s*(is\s*)?(not|un)available"
    r"|not\s*offer.*sponsorship",
    re.I,
)


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

    if _VISA_NO_RE.search(text):
        return "no"
    if _VISA_YES_RE.search(text):
        return "yes"
    return "unknown"
