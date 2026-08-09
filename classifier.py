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
from config import LLM_PROVIDER, LLM_API_KEY, LLM_MODEL, AI_BATCH_SIZE

log = logging.getLogger(__name__)

# ── Provider-specific AI client setup ─────────────────────
MAX_RETRIES = 2
RETRY_BASE_DELAY = 3  # seconds

if LLM_PROVIDER == "anthropic":
    import anthropic
    _client = anthropic.Anthropic(api_key=LLM_API_KEY)
elif LLM_PROVIDER == "groq":
    from groq import Groq
    _client = Groq(api_key=LLM_API_KEY)


def _ai_call(system_prompt: str, user_msg: str, max_tokens: int = 500) -> str | None:
    """Call the configured LLM with retry on rate limit.
    Anthropic uses prompt caching (5 min TTL).
    Returns response text or None on failure."""
    if LLM_PROVIDER == "anthropic":
        return _ai_call_anthropic(system_prompt, user_msg, max_tokens)
    elif LLM_PROVIDER == "groq":
        return _ai_call_groq(system_prompt, user_msg, max_tokens)
    return None


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


def _ai_call_groq(system_prompt: str, user_msg: str, max_tokens: int) -> str | None:
    """Groq with retry on rate limit."""
    combined_prompt = f"{system_prompt}\n\n{user_msg}"
    for attempt in range(MAX_RETRIES):
        try:
            resp = _client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": combined_prompt}],
                temperature=0,
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            error_str = str(e)
            is_rate_limit = "429" in error_str or "rate" in error_str.lower()
            is_daily_limit = "tokens per day" in error_str.lower() or "tpd" in error_str.lower()

            if is_daily_limit:
                log.error("Groq daily token limit reached — skipping remaining AI calls")
                return None
            if is_rate_limit and attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (attempt + 1)
                log.warning(f"Groq rate limit hit, retrying in {delay}s (attempt {attempt + 1})")
                time.sleep(delay)
                continue
            log.error(f"Groq API error (attempt {attempt + 1}): {e}")
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


def ai_classify_roles(titles: list[str]) -> dict[str, bool]:
    """Send ambiguous titles to AI. Returns {title: is_relevant}.
    On failure: defaults to False (exclude)."""
    if not titles:
        return {}

    results = {}
    for i in range(0, len(titles), AI_BATCH_SIZE):
        batch = titles[i:i + AI_BATCH_SIZE]
        numbered = "\n".join(f"{j+1}. {t}" for j, t in enumerate(batch))
        user_msg = f"Titles:\n{numbered}"

        text = _ai_call(ROLE_SYSTEM_PROMPT, user_msg, max_tokens=500)

        if text is None:
            log.warning(f"AI role classification failed for batch of {len(batch)}, defaulting to exclude")
            for t in batch:
                results[t] = False
            continue

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


# ═══════════════════════════════════════════════════════
# STAGE 3 & 4: LOCATION FILTER (Africa / Global)
# ═══════════════════════════════════════════════════════

AFRICAN_COUNTRIES = {
    "algeria", "angola", "benin", "botswana", "burkina faso", "burundi",
    "cabo verde", "cape verde", "cameroon", "central african republic", "chad",
    "comoros", "congo", "cote d'ivoire", "ivory coast", "djibouti", "egypt",
    "equatorial guinea", "eritrea", "eswatini", "swaziland", "ethiopia",
    "gabon", "gambia", "ghana", "guinea", "guinea-bissau", "kenya", "lesotho",
    "liberia", "libya", "madagascar", "malawi", "mali", "mauritania",
    "mauritius", "morocco", "mozambique", "namibia", "niger", "nigeria",
    "rwanda", "sao tome", "senegal", "seychelles", "sierra leone", "somalia",
    "south africa", "south sudan", "sudan", "tanzania", "togo", "tunisia",
    "uganda", "zambia", "zimbabwe",
}

GLOBAL_KEYWORDS = [
    r"\bremote\s*[\-–—/,]\s*global\b",
    r"\bremote\s*[\-–—/,]\s*worldwide\b",
    r"\bremote\s*[\-–—/,]\s*anywhere\b",
    r"\bremote\s*[\-–—/,]\s*international\b",
    r"\bglobal\s*[\-–—/,]?\s*remote\b",
    r"\bworldwide\s*[\-–—/,]?\s*remote\b",
    r"\bwork\s*from\s*anywhere\b",
    r"\bhire\s*(globally|worldwide|anywhere)\b",
    r"\bopen\s*to\s*(all|any)\s*location",
    r"\blocation\s*[\-–—:]?\s*anywhere\b",
]

GLOBAL_RE = [re.compile(kw, re.I) for kw in GLOBAL_KEYWORDS]

STANDALONE_GLOBAL_RE = re.compile(
    r"^\s*(global|worldwide|anywhere)\s*$", re.I
)

US_LOCATION_PATTERNS = [
    r"\bnew\s*york\b", r"\bsan\s*francisco\b", r"\blos\s*angeles\b",
    r"\bchicago\b", r"\bseattle\b", r"\bboston\b", r"\baustin\b",
    r"\bdenver\b", r"\batlanta\b", r"\bportland\b", r"\bphoenix\b",
    r"\bdallas\b", r"\bhouston\b", r"\bmiami\b", r"\bdc\b",
    r"\bwashington\b", r"\bphiladelphia\b", r"\bminneapolis\b",
    r"\braleigh\b", r"\bsalt\s*lake\b", r"\bdetroit\b", r"\btampa\b",
    r"\bunited\s*states\b", r"\busa\b", r"\b\(us\)\b", r"\bus\s*only\b",
    r"\busa\s*only\b", r"\bus\s*remote\b", r"\bremote\s*[\-–—,]?\s*us\b",
    r"\bremote\s*[\-–—,]?\s*usa\b", r"\bremote\s*[\-–—,]?\s*united\s*states\b",
    r"\bnorth\s*america\b", r"\bnorth\s*america\s*only\b",
    r"\bus\s*based\b", r"\busa\s*based\b", r"\bbased\s*in\s*(the\s*)?us\b",
    r"\bmust\s*be\s*(in|located\s*in)\s*(the\s*)?us\b",
    r"\bcalifornia\b", r"\btexas\b", r"\bflorida\b", r"\billinois\b",
    r"\bpennsylvania\b", r"\bgeorgia\b", r"\bmassachusetts\b",
    r"\bcolorado\b", r"\bvirginia\b", r"\bmaryland\b", r"\boregon\b",
    r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b",
]
US_LOCATION_RE = [re.compile(p, re.I) for p in US_LOCATION_PATTERNS]

UK_LOCATION_PATTERNS = [
    r"\bunited\s*kingdom\b", r"\b\(uk\)\b", r"\buk\s*only\b",
    r"\buk\s*remote\b", r"\bremote\s*[\-–—,]?\s*uk\b",
    r"\buk\s*based\b", r"\bbased\s*in\s*(the\s*)?uk\b",
    r"\blondon\b", r"\bmanchester\b", r"\bbirmingham\b", r"\bleeds\b",
    r"\bbristol\b", r"\bedinburgh\b", r"\bglasgow\b", r"\bcardiff\b",
    r"\bengland\b", r"\bscotland\b", r"\bwales\b",
]
UK_LOCATION_RE = [re.compile(p, re.I) for p in UK_LOCATION_PATTERNS]

OTHER_NON_AFRICA_PATTERNS = [
    r"\bcanada\b", r"\btoronto\b", r"\bvancouver\b",
    r"\bgermany\b", r"\bberlin\b", r"\bmunich\b",
    r"\bfrance\b", r"\bparis\b",
    r"\baustralia\b", r"\bsydney\b", r"\bmelbourne\b",
    r"\bjapan\b", r"\btokyo\b",
    r"\bindia\b", r"\bbangalore\b", r"\bmumbai\b", r"\bhyderabad\b",
    r"\bsingapore\b",
    r"\bdublin\b", r"\bireland\b",
    r"\bnetherlands\b", r"\bamsterdam\b",
    r"\bspain\b", r"\bmadrid\b", r"\bbarcelona\b",
    r"\bitaly\b", r"\bmilan\b",
    r"\bsweden\b", r"\bstockholm\b",
    r"\bdenmark\b", r"\bcopenhagen\b",
    r"\bswitzerland\b", r"\bzurich\b",
    r"\baustr(ia|alian)\b",
    r"\bisrael\b", r"\btel\s*aviv\b",
    r"\bchina\b", r"\bbeijing\b", r"\bshanghai\b",
    r"\bbrazil\b", r"\bsao\s*paulo\b",
    r"\bmexico\b", r"\bmexico\s*city\b",
    r"\bargentina\b", r"\bbogota\b", r"\bcolombia\b",
    r"\bkorea\b", r"\bseoul\b",
    r"\bapac\b", r"\blatam\b",
    r"\bseur\b", r"\bneur\b", r"\bdach\b", r"\banz\b",
    r"\beurope\s*only\b", r"\beu\s*only\b",
]
OTHER_NON_AFRICA_RE = [re.compile(p, re.I) for p in OTHER_NON_AFRICA_PATTERNS]

REMOTE_COUNTRY_RE = re.compile(
    r"\bremote\s*[\-–—/,]\s*(us|usa|united\s*states|uk|united\s*kingdom|canada|australia|germany|france|india|europe|apac|latam|dach|anz)\b",
    re.I,
)


def keyword_classify_location(job: dict) -> str:
    """
    Returns 'match' (Africa/global), 'no_match', or 'unsure'.
    Strict: only matches explicit Africa or truly global roles.
    Bare 'Remote' = no_match (most companies mean US remote).
    """
    loc = (job.get("location", "") + " " + job.get("country", "")).lower()
    desc = job.get("description_snippet", "").lower()

    if "africa" in loc:
        return "match"
    for country in AFRICAN_COUNTRIES:
        if country in loc:
            return "match"

    if "africa" in desc or "nigeria" in desc or "lagos" in desc or "nairobi" in desc:
        for country in AFRICAN_COUNTRIES:
            if country in desc:
                return "match"

    if any(rx.search(loc) for rx in US_LOCATION_RE):
        return "no_match"

    if any(rx.search(loc) for rx in UK_LOCATION_RE):
        return "no_match"

    if REMOTE_COUNTRY_RE.search(loc):
        return "no_match"

    if any(rx.search(loc) for rx in GLOBAL_RE):
        return "match"

    if STANDALONE_GLOBAL_RE.search(loc.strip()):
        return "match"

    if re.search(r"\bemea\b", loc, re.I):
        return "unsure"

    if any(rx.search(loc) for rx in OTHER_NON_AFRICA_RE):
        return "no_match"

    if re.search(r"\bremote\b", loc, re.I):
        global_desc_signals = [
            r"open\s*to\s*(candidates\s*)?(globally|worldwide|anywhere)",
            r"hire\s*(in\s*)?(\d+\+?\s*)?countries",
            r"work\s*from\s*anywhere",
            r"location[\s:]+anywhere",
            r"distributed\s*team.*global",
        ]
        for pattern in global_desc_signals:
            if re.search(pattern, desc, re.I):
                return "match"

        us_desc_signals = [
            r"us\s*work\s*authorization",
            r"united\s*states\s*work\s*authorization",
            r"must\s*be\s*(authorized|eligible)\s*to\s*work\s*in\s*(the\s*)?u\.?s",
            r"w-?2\s*(employee|employment)",
            r"this\s*(role|position)\s*is\s*(based\s*in|located\s*in)\s*(the\s*)?(us|united\s*states|uk|united\s*kingdom)",
        ]
        for pattern in us_desc_signals:
            if re.search(pattern, desc, re.I):
                return "no_match"

        return "unsure"

    if loc.strip():
        return "no_match"

    return "unsure"


LOCATION_SYSTEM_PROMPT = """\
You are a strict job location classifier. For each job below, determine if \
someone based in AFRICA (specifically Nigeria) could realistically apply.

MATCH only if:
- Location explicitly mentions an African country (Nigeria, Kenya, South Africa, etc.)
- Location says "Global", "Worldwide", "Anywhere", "Work from anywhere"
- Description explicitly says they hire globally or in Africa
- Location says "Remote - Global" or similar compound global phrase

NO_MATCH if:
- Location is a specific US city/state (New York, California, etc.)
- Location says just "Remote" with no global qualifier (this usually means US)
- Location is UK, Canada, Europe, EMEA, APAC, Australia, India, etc.
- Description says "US work authorization required" or similar
- Any specific non-African country or region is mentioned
- Description mentions US-only benefits (401k, H1B, etc.)

When in doubt, say NO_MATCH. We only want roles a person in Africa can actually get.

Respond ONLY with lines like:
1 MATCH (reason)
2 NO_MATCH (reason)"""


def ai_classify_locations(jobs: list[dict]) -> list[str]:
    """
    Send ambiguous jobs to AI for location classification.
    Returns list of 'match', 'no_match', or 'uncertain' in same order.
    On failure: defaults to 'no_match' (exclude).
    """
    if not jobs:
        return []

    results = ["no_match"] * len(jobs)

    for i in range(0, len(jobs), AI_BATCH_SIZE):
        batch = jobs[i:i + AI_BATCH_SIZE]
        numbered_lines = []
        for j, job in enumerate(batch):
            numbered_lines.append(
                f"{j+1}. Title: {job['title']} | Location: {job['location']} | "
                f"Country: {job.get('country', '')} | "
                f"Description: {job.get('description_snippet', '')[:500]}"
            )
        user_msg = f"Jobs:\n{chr(10).join(numbered_lines)}"

        text = _ai_call(LOCATION_SYSTEM_PROMPT, user_msg, max_tokens=1000)

        if text is None:
            log.warning(f"AI location classification failed for batch of {len(batch)}, defaulting to exclude")
            continue

        for line in text.splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) >= 1:
                try:
                    idx = int(parts[0].rstrip(".")) - 1
                except ValueError:
                    continue
                if 0 <= idx < len(batch):
                    label = parts[1].upper() if len(parts) > 1 else ""
                    if label.startswith("MATCH"):
                        results[i + idx] = "match"
                    elif label.startswith("NO_MATCH"):
                        results[i + idx] = "no_match"
                    else:
                        results[i + idx] = "no_match"

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
