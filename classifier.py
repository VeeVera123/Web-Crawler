"""
Two-stage classifier — unified for all LLM providers.
Provider is selected via LLM_PROVIDER in config.py / env.

  Stage 1 — keyword filter for CSM/AM role titles (fast, no API)
  Stage 2 — AI for ambiguous titles (batched)

Then a separate location filter:
  Stage 3 — keyword check for Africa/Global locations
  Stage 4 — AI for ambiguous locations + description scanning
"""

import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import LLM_PROVIDER, LLM_API_KEY, LLM_MODEL, LLM_BASE_URL, AI_BATCH_SIZE, AI_PARALLEL_REQUESTS

log = logging.getLogger(__name__)

# ── Provider-specific AI client setup ─────────────────────
MAX_RETRIES = 4
RETRY_BASE_DELAY = 5  # seconds

# Proactive rate-limit throttle (Cerebras free tier: ~5 req/min)
_last_call_time = 0.0
if LLM_PROVIDER == "cerebras":
    _MIN_CALL_INTERVAL = 12.5   # 12.5s = ~5 req/min (free tier)
elif LLM_PROVIDER == "openai":
    _MIN_CALL_INTERVAL = 5.0    # 5s = ~12 req/min (Tier 1: 200K TPM)
else:
    _MIN_CALL_INTERVAL = 0.0

if LLM_PROVIDER == "anthropic":
    import anthropic
    _client = anthropic.Anthropic(api_key=LLM_API_KEY)
else:
    # Cerebras, Groq, OpenAI, and any OpenAI-compatible provider
    from openai import OpenAI
    _client = OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)


def _ai_call(system_prompt: str, user_msg: str, max_tokens: int = 500) -> str | None:
    """Call the configured LLM with retry on rate limit.
    Anthropic uses prompt caching (5 min TTL).
    Returns response text or None on failure."""
    global _last_call_time
    if _MIN_CALL_INTERVAL > 0:
        elapsed = time.time() - _last_call_time
        if elapsed < _MIN_CALL_INTERVAL:
            time.sleep(_MIN_CALL_INTERVAL - elapsed)
    _last_call_time = time.time()

    if LLM_PROVIDER == "anthropic":
        return _ai_call_anthropic(system_prompt, user_msg, max_tokens)
    else:
        return _ai_call_openai_compat(system_prompt, user_msg, max_tokens)


def _ai_call_anthropic(system_prompt: str, user_msg: str, max_tokens: int) -> str | None:
    """Anthropic Claude with prompt caching + retry."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = _client.messages.create(
                model=LLM_MODEL,
                max_tokens=max_tokens,
                system=[{
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": user_msg}],
            )
            usage = resp.usage
            cached = getattr(usage, "cache_read_input_tokens", 0) or 0
            created = getattr(usage, "cache_creation_input_tokens", 0) or 0
            if cached > 0:
                log.debug(f"Cache hit: {cached} tokens read from cache")
            elif created > 0:
                log.debug(f"Cache write: {created} tokens written to cache")
            return resp.content[0].text.strip()
        except anthropic.RateLimitError as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (attempt + 1)
                log.warning(f"Anthropic rate limit hit, retrying in {delay}s (attempt {attempt + 1})")
                time.sleep(delay)
                continue
            log.error(f"Anthropic rate limit exhausted: {e}")
            return None
        except anthropic.APIError as e:
            log.error(f"Anthropic API error (attempt {attempt + 1}): {e}")
            return None
        except Exception as e:
            log.error(f"Anthropic unexpected error (attempt {attempt + 1}): {e}")
            return None
    return None


def _ai_call_openai_compat(system_prompt: str, user_msg: str, max_tokens: int) -> str | None:
    """OpenAI-compatible provider (Cerebras, Groq, OpenAI, etc.) with retry."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = _client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0,
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            error_str = str(e)
            is_rate_limit = "429" in error_str or "rate" in error_str.lower()
            is_daily_limit = "tokens per day" in error_str.lower() or "daily" in error_str.lower()

            if is_daily_limit:
                log.error(f"{LLM_PROVIDER} daily token limit reached — skipping remaining AI calls")
                return None
            if is_rate_limit and attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (attempt + 1)
                log.warning(f"{LLM_PROVIDER} rate limit hit, retrying in {delay}s (attempt {attempt + 1})")
                time.sleep(delay)
                continue
            log.error(f"{LLM_PROVIDER} API error (attempt {attempt + 1}): {e}")
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


def _classify_role_batch(batch: list[str]) -> dict[str, bool]:
    """Classify a single batch of titles. Returns {title: is_relevant}."""
    numbered = "\n".join(f"{j+1}. {t}" for j, t in enumerate(batch))
    user_msg = f"Titles:\n{numbered}"
    text = _ai_call(ROLE_SYSTEM_PROMPT, user_msg, max_tokens=500)

    results = {}
    if text is None:
        log.warning(f"AI role classification failed for batch of {len(batch)}, defaulting to exclude")
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


def ai_classify_roles(titles: list[str]) -> dict[str, bool]:
    """Send ambiguous titles to AI. Returns {title: is_relevant}.
    Uses AI_PARALLEL_REQUESTS concurrent calls for Groq/Haiku.
    On failure: defaults to False (exclude)."""
    if not titles:
        return {}

    batches = [titles[i:i + AI_BATCH_SIZE] for i in range(0, len(titles), AI_BATCH_SIZE)]
    results = {}

    if AI_PARALLEL_REQUESTS <= 1:
        for batch in batches:
            results.update(_classify_role_batch(batch))
    else:
        with ThreadPoolExecutor(max_workers=AI_PARALLEL_REQUESTS) as pool:
            futures = {pool.submit(_classify_role_batch, b): b for b in batches}
            for future in as_completed(futures):
                try:
                    results.update(future.result())
                except Exception as e:
                    log.error(f"Parallel role classification error: {e}")
                    for t in futures[future]:
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
    # Qualifier + remote (handles "Global (Remote)", "Worldwide - Remote", etc.)
    r"\bglobal\s*[\-–—/,()]?\s*remote\b",
    r"\bworldwide\s*[\-–—/,()]?\s*remote\b",
    r"\binternational\s*[\-–—/,()]?\s*remote\b",
    r"\banywhere\s*[\-–—/,()]?\s*remote\b",
    # Explicit phrases
    r"\bwork\s*from\s*anywhere\b",
    r"\bwfa\b",
    r"\bhire\s*(globally|worldwide|anywhere)\b",
    r"\bhiring\s*(globally|worldwide|anywhere)\b",
    r"\bopen\s*to\s*(all|any)\s*location",
    r"\bopen\s*to\s*(all|any)\s*countr",
    r"\blocation\s*[\-–—:]?\s*anywhere\b",
    r"\blocation\s*[\-–—:]?\s*flexible\b",
    r"\blocation\s*agnostic\b",
    r"\blocation\s*independent\b",
    r"\bgeo[\-\s]*flexible\b",
    r"\bgeo[\-\s]*agnostic\b",
    r"\bborderless\b",
    r"\b(fully\s*)?distributed\b",
    r"\b(global|international)\s*team\b",
    r"\bmultiple\s*(countries|locations|regions)\b",
    r"\ball\s*geograph",
    r"\bany\s*country\b",
    r"\bno\s*location\s*(requirement|restriction|preference)\b",
    r"\bno\s*geographic\s*restriction\b",
]

GLOBAL_RE = [re.compile(kw, re.I) for kw in GLOBAL_KEYWORDS]

STANDALONE_GLOBAL_RE = re.compile(
    r"^\s*(global|worldwide|anywhere|international|wfa|earth|"
    r"remote\s*[\-–—/,()]?\s*(global|worldwide|anywhere|international|wfa))\s*$", re.I
)

# ── Non-geographic words in location fields ──────────
# Instead of enumerating every country/city/region (fragile, always
# misses some), we strip non-geographic filler from the location string.
# If anything remains after stripping "remote" + these words, it's a
# geographic qualifier → no_match.  This catches UAE, Americas, etc.
# without maintaining country/city lists.

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
    r"hours|hrs|am|pm|to|and|or|the|a|an|at|for|of|"       # connectors/articles (NOT "in" — it's a US state code)
    r"immediate|urgent|asap|new|multiple|"                  # posting qualifiers
    r"available|hiring|now|apply"                            # action words
    r")\b",
    re.I,
)

# Words to strip ONLY in global-keyword residue check (not for remote qualifier check)
GLOBAL_STRIP_RE = re.compile(
    r"\b("
    r"global|worldwide|international|anywhere|everywhere|"  # global keywords
    r"wfa|distributed|borderless|work|from"                 # global modifiers
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
# Patterns in job titles that indicate geographic restriction
# These are checked when the location field is empty or just "Remote"
_TITLE_LOCATION_RE = re.compile(
    r"(?:"
    # "USA-Remote", "US Remote", "UK - Remote", "US-Based"
    r"\b(US|USA|UK|EU|CA|AU|IN|DE|FR|NL|SG|HK|JP|BR|MX|PH|NG|KE|ZA|AE|SA|IL|PL|CZ|RO|BG|HU|IE|ES|IT|PT|SE|NO|DK|FI|CH|AT|BE|NZ)"
    r"\s*[\-–—/]?\s*(?:remote|based|only)"
    r"|"
    # "Remote - US", "Remote (USA)", "Remote, United States"
    r"(?:remote)\s*[\-–—/,()]*\s*"
    r"(US|USA|UK|EU|India|United\s+States|United\s+Kingdom|Canada|Australia|Germany|France|Netherlands)"
    r"|"
    # "(US)", "(UK)", "(USA)" at end of title
    r"\(\s*(US|USA|UK|EU|India|Canada|Australia)\s*\)\s*$"
    r"|"
    # "New York", "San Francisco", "London", state names in title
    r"\b(New\s+York|San\s+Francisco|Los\s+Angeles|Chicago|Boston|Seattle|Austin|Denver|Atlanta|Dallas|Miami|"
    r"London|Berlin|Paris|Amsterdam|Toronto|Sydney|Singapore|Dubai|Mumbai|Bangalore|"
    r"California|Texas|Florida|Virginia|Pennsylvania|Illinois|Ohio|Georgia|"
    r"North\s+Carolina|New\s+Jersey|Massachusetts|Maryland|Colorado|Washington|Oregon|Arizona|Michigan|Minnesota)"
    r"\b"
    r")",
    re.I,
)


def _enrich_location_from_title(loc: str, title: str) -> str:
    """If location is bare 'Remote' or empty, extract geographic hints from title.
    This catches patterns like 'USA-Remote', 'UK Remote', city/state names.
    Returns enriched location string."""
    if not title:
        return loc

    loc_stripped = loc.strip().lower()
    # Only enrich if location is empty, placeholder, or bare "remote"
    is_bare = (
        not loc_stripped
        or loc_stripped in ("remote", "remote worker", "remote job", "fully remote")
        or PLACEHOLDER_LOC_RE.match(loc)
    )
    if not is_bare:
        return loc

    # Also check for global signals in title (EMEA, Global, Worldwide, Africa)
    _title_global = re.search(
        r"\b(EMEA|Global\s*Remote|Remote\s*Global|Worldwide|International|Africa)\b",
        title, re.I
    )
    if _title_global:
        geo = _title_global.group(1).strip()
        if loc_stripped and "remote" in loc_stripped:
            return f"Remote, {geo}"
        return geo

    match = _TITLE_LOCATION_RE.search(title)
    if match:
        # Get the matched group (whichever one matched)
        geo = next((g for g in match.groups() if g), None)
        if geo:
            geo = geo.strip()
            if loc_stripped and "remote" in loc_stripped:
                return f"Remote, {geo}"
            return geo

    return loc


def keyword_classify_location(job: dict) -> str:
    """
    Returns 'match', 'no_match', or 'unsure'.

    MATCH = truly global hiring signals (anywhere, worldwide,
    international, WFA, EMEA alone, Africa as continent).

    Flow:
      1. Empty / placeholder location → UNSURE (send to AI)
      2. Global/Anywhere/EMEA/Africa(continent) → MATCH
      3. Hybrid / onsite → REJECT
      4. Has "remote" → strip non-geo words; if anything geographic
         remains → REJECT, else bare "remote" → UNSURE
      5. Has location text but no "remote" → REJECT (onsite)
    """
    raw_loc = job.get("location", "")
    raw_country = job.get("country", "")
    # Ensure both are strings (some APIs return lists)
    if isinstance(raw_loc, list):
        raw_loc = ", ".join(str(x) for x in raw_loc)
    if isinstance(raw_country, list):
        raw_country = ", ".join(str(x) for x in raw_country)
    loc = (raw_loc + " " + raw_country).strip()

    # ── 0. Enrich location from title if location is bare/empty ──
    # Many ATS set location="Remote" but the title contains the real
    # qualifier: "USA-Remote", "UK Remote", "EMEA", etc.
    title = job.get("title", "")
    loc = _enrich_location_from_title(loc, title)

    loc_lower = loc.lower()

    # ── 1. Empty / placeholder → UNSURE ──────────────────
    if not loc.strip() or PLACEHOLDER_LOC_RE.match(loc):
        return "unsure"

    has_remote = bool(re.search(r"\bremote\b", loc_lower))

    # ── 2a. South Africa is a COUNTRY → always reject ──
    # Must check before Africa-continent check (South Africa contains "Africa")
    if re.search(r"\bsouth\s+africa\b", loc_lower):
        return "no_match"

    # ── 2b. Africa (the continent) → always MATCH ──
    # This is what we're scanning for. If Africa is explicitly mentioned,
    # the job is Africa-eligible regardless of other qualifiers.
    if re.search(r"\bafrica\b", loc_lower):
        return "match"

    # ── 2c. EMEA → match only if no country/city qualifier ──
    if re.search(r"\bemea\b", loc_lower):
        check = re.sub(r"\bemea\b", "", loc_lower)
        check = NON_GEO_WORDS_RE.sub("", check)
        check = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9&|]+", " ", check).strip()
        if not check:
            return "match"
        return "no_match"

    # ── 2d. Global/Worldwide/International/WFA/Anywhere ──
    # Match only if no geographic qualifier (country, city, OR continent/region)
    has_global = any(rx.search(loc_lower) for rx in GLOBAL_RE) or STANDALONE_GLOBAL_RE.search(loc.strip())
    if has_global:
        check = GLOBAL_STRIP_RE.sub("", loc_lower)
        check = NON_GEO_WORDS_RE.sub("", check)
        check = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9&]+", " ", check).strip()
        if not check:
            return "match"
        # Has geographic qualifier → no_match (e.g. "WFA-India", "Anywhere (Europe)")
        return "no_match"

    # ── 3. REJECT: Hybrid / Onsite ──────────────────────
    if re.search(r"\bhybrid\b", loc_lower):
        return "no_match"
    if re.search(r"\bon[\-\s]*site\b", loc_lower):
        return "no_match"
    if re.search(r"\bin[\-\s]*person\b", loc_lower):
        return "no_match"

    # ── 4. Remote + qualifier check ─────────────────────
    # This replaces the old country/city enumeration approach.
    # Strip "remote" and non-geographic filler words. If anything
    # remains, it's a geographic qualifier (country, city, region,
    # state code, etc.) → no_match.
    if has_remote:
        stripped = NON_GEO_WORDS_RE.sub("", loc_lower)
        # Strip separators, digits (like "00-Remote Worker-N/A"), punctuation
        stripped = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9]+", " ", stripped).strip()

        if not stripped:
            # Bare "Remote" / "Remote / Full-Time" / "Remote Worker - N/A"
            return "unsure"
        # Has geographic content → country-restricted remote
        return "no_match"

    # ── 5. REJECT: Has location text but no "remote" → onsite ──
    return "no_match"


LOCATION_SYSTEM_PROMPT = """\
You classify whether a job is open to candidates working remotely \
from ANYWHERE in the world (truly global hiring, not country-specific).

These jobs say "Remote" with no country qualifier. Your task: \
check the DESCRIPTION for evidence of global hiring OR geographic restrictions.

MATCH — positive evidence the role is genuinely global:
- Description explicitly says "global", "worldwide", "anywhere", \
  "international", "work from anywhere", "distributed team"
- Hiring across multiple continents or many countries
- No geographic restrictions AND the role/company context clearly \
  suggests global openness (e.g. "our team spans 30+ countries")

NO_MATCH — evidence of country or region restriction:
- "must be authorized/eligible to work in [country]"
- "US/UK/EU work authorization required"
- "W-2 employment", "W2 only", "must have SSN"
- "no visa sponsorship", "cannot sponsor", "will not sponsor"
- "must reside in [state/country]", "must be located in [place]"
- "this role is based in [country]" without global remote option
- Country-specific benefits as requirements (401k, PAYE, tax residency)
- Description context makes it obvious the role is for one country \
  (e.g. references to US-specific regulations, UK employment law)

UNCERTAIN — cannot determine either way:
- No description available
- Description does not mention location requirements at all
- Ambiguous or conflicting signals

IMPORTANT: When there is no description or no clear signal, say UNCERTAIN. \
Do NOT default to MATCH. Only say MATCH when you see positive evidence \
of global/worldwide hiring. When in doubt, UNCERTAIN.

Respond ONLY with lines like:
1 MATCH
2 NO_MATCH
3 UNCERTAIN"""


def _classify_location_batch(batch_jobs: list[dict], max_user_chars: int) -> list[str]:
    """Classify a single batch of jobs by location. Returns list of labels."""
    DESC_OVERHEAD = 120
    max_desc = max(500, (max_user_chars - len(batch_jobs) * DESC_OVERHEAD) // len(batch_jobs))
    numbered_lines = []
    for j, job in enumerate(batch_jobs):
        desc = job.get("description_snippet", "")
        if desc:
            desc_note = desc[:max_desc] + ("…" if len(desc) > max_desc else "")
        else:
            desc_note = "[No description available]"
        numbered_lines.append(
            f"{j+1}. Title: {job['title']} | Company: {job.get('company', 'Unknown')} | "
            f"Location: {job.get('location', 'Remote')} | "
            f"Description: {desc_note}"
        )
    user_msg = f"Classify these {len(batch_jobs)} jobs:\n{chr(10).join(numbered_lines)}"

    text = _ai_call(LOCATION_SYSTEM_PROMPT, user_msg, max_tokens=1500)

    batch_results = ["uncertain"] * len(batch_jobs)
    if text is None:
        log.warning(f"AI location classification failed for batch of {len(batch_jobs)}, keeping as uncertain")
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
                if label.startswith("NO_MATCH"):
                    batch_results[idx] = "no_match"
                elif label.startswith("MATCH"):
                    batch_results[idx] = "match"
                elif label.startswith("UNCERTAIN"):
                    batch_results[idx] = "uncertain"

    return batch_results


def _build_dynamic_batches(jobs: list[dict], max_user_chars: int) -> list[tuple[int, list[dict]]]:
    """Build batches dynamically based on description length.

    Rules:
      - Total batch ≤ 400k characters (80% of 500k budget)
      - Max 50 roles per batch, even if character space remains
      - Max 8k characters per individual role description (truncated)
      - Smaller JDs → more roles packed in (up to 50)
      - Min 2 roles per batch
    """
    OVERHEAD_PER_JOB = 120   # title, company, location formatting
    MAX_DESC_CHARS = 8000    # truncate individual JDs beyond 8k
    MIN_BATCH = 2
    MAX_BATCH = 50           # hard cap: 50 roles per batch
    TARGET_CHARS = int(max_user_chars * 0.8)  # 400k chars

    batches = []
    current_batch = []
    current_chars = 0
    start_idx = 0

    for i, job in enumerate(jobs):
        desc = job.get("description_snippet") or ""
        # Truncate oversized descriptions to 8k chars
        if len(desc) > MAX_DESC_CHARS:
            job["description_snippet"] = desc[:MAX_DESC_CHARS]
            desc = job["description_snippet"]
        desc_len = len(desc)
        job_chars = desc_len + OVERHEAD_PER_JOB

        # Would this job push us over budget? (but always allow MIN_BATCH)
        if current_batch and (current_chars + job_chars > TARGET_CHARS
                              or len(current_batch) >= MAX_BATCH):
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
    AI checks descriptions for hidden geographic restrictions.
    Uses AI_PARALLEL_REQUESTS concurrent calls for Groq/Haiku.
    Returns list of 'match', 'no_match', or 'uncertain' in same order.
    On rate limit/failure: defaults to 'uncertain' (include with flag).

    Batch size is DYNAMIC — more short-description jobs per request,
    fewer long ones. This saves tokens while maintaining quality.
    """
    if not jobs:
        return []

    # Cap description length per provider's context window.
    if LLM_PROVIDER == "cerebras":
        max_user_chars = 28000
    else:
        max_user_chars = 500000  # Groq/Anthropic: no practical limit

    # Build dynamic batches based on actual content size
    batches = _build_dynamic_batches(jobs, max_user_chars)
    log.info(f"Dynamic batching: {len(jobs)} jobs → {len(batches)} batches "
             f"(sizes: {[len(b) for _, b in batches]})")
    results = ["uncertain"] * len(jobs)

    if AI_PARALLEL_REQUESTS <= 1:
        for start_idx, batch in batches:
            batch_results = _classify_location_batch(batch, max_user_chars)
            for j, label in enumerate(batch_results):
                results[start_idx + j] = label
            if start_idx + AI_BATCH_SIZE < len(jobs):
                time.sleep(2)
    else:
        with ThreadPoolExecutor(max_workers=AI_PARALLEL_REQUESTS) as pool:
            future_to_idx = {
                pool.submit(_classify_location_batch, batch, max_user_chars): start_idx
                for start_idx, batch in batches
            }
            for future in as_completed(future_to_idx):
                start_idx = future_to_idx[future]
                try:
                    batch_results = future.result()
                    for j, label in enumerate(batch_results):
                        results[start_idx + j] = label
                except Exception as e:
                    log.error(f"Parallel location classification error: {e}")

    classified = sum(1 for r in results if r != "uncertain")
    log.info(f"AI classified {classified}/{len(jobs)} locations "
             f"({results.count('match')} match, {results.count('no_match')} no_match, "
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

    # Check "no" patterns first — they're more specific
    if _VISA_NO_RE.search(text):
        return "no"
    if _VISA_YES_RE.search(text):
        return "yes"
    return "unknown"
