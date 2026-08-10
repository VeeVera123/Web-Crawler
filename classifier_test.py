"""
classifier_test.py — Copy of classifier.py with NO rate throttle.
Changes from classifier.py:
  1. Imports from config_test instead of config
  2. _MIN_CALL_INTERVAL = 0 (no throttle)
  3. Everything else identical
"""

import re
import time
import logging
from config_test import LLM_PROVIDER, LLM_API_KEY, LLM_MODEL, LLM_BASE_URL, AI_BATCH_SIZE

log = logging.getLogger(__name__)

# ── Provider-specific AI client setup ─────────────────────
MAX_RETRIES = 4
RETRY_BASE_DELAY = 5  # seconds

# Cerebras free tier: ~5 req/min. Space calls 13s apart to avoid 429s.
_last_call_time = 0.0
_MIN_CALL_INTERVAL = 13.0

if LLM_PROVIDER == "anthropic":
    import anthropic
    _client = anthropic.Anthropic(api_key=LLM_API_KEY)
else:
    from openai import OpenAI
    _client = OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)


def _ai_call(system_prompt: str, user_msg: str, max_tokens: int = 500) -> str | None:
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
    r"customer\s*success", r"client\s*success", r"partner\s*success",
    r"merchant\s*success", r"\bcsm\b", r"success\s*manager",
    r"success\s*lead", r"success\s*specialist", r"success\s*director",
    r"success\s*associate", r"success\s*consultant", r"success\s*advisor",
    r"success\s*architect", r"success\s*coach", r"success\s*executive",
    r"head\s*of\s*.*success",
    r"customer\s*support\s*(manager|lead|director|head)",
    r"client\s*support\s*(manager|lead|director|head)",
    r"customer\s*service\s*(manager|lead|director|head|representative|rep\b)",
    r"client\s*service\s*(manager|lead|director|head)",
    r"customer\s*experience", r"client\s*experience",
    r"\bcx\s*(manager|lead|specialist|director|strategist)",
    r"customer\s*relationship", r"client\s*relationship",
    r"relationship\s*manager",
    r"customer\s*engagement", r"client\s*engagement",
    r"customer\s*care", r"client\s*care",
    r"customer\s*advocate", r"client\s*advocate",
    r"account\s*manager", r"account\s*management",
    r"client\s*account\s*manag", r"customer\s*account\s*manag",
    r"key\s*account\s*manag", r"strategic\s*account\s*manag",
    r"enterprise\s*account\s*manag", r"technical\s*account\s*manag",
    r"\btam\b", r"named\s*account\s*manag",
    r"regional\s*account\s*manag", r"national\s*account\s*manag",
    r"global\s*account\s*manag", r"account\s*lead", r"account\s*director",
    r"senior\s*account\s*manag", r"junior\s*account\s*manag",
    r"account\s*executive\s*.*(?:success|retention|renewal)",
    r"customer\s*retention", r"client\s*retention",
    r"retention\s*(manager|lead|specialist|director)",
    r"renewal\s*(manager|lead|specialist|director)",
    r"customer\s*onboarding", r"client\s*onboarding",
    r"onboarding\s*(manager|lead|specialist)",
    r"implementation\s*(manager|lead|specialist|consultant)",
]

EXCLUDE_KEYWORDS = [
    r"\bengineer\b", r"\bengineering\b", r"\bdeveloper\b", r"\bdev\b",
    r"\bsoftware\b", r"\bsre\b", r"\bdevops\b", r"\bbackend\b",
    r"\bfrontend\b", r"\bfull[\s-]?stack\b", r"\bdata\s*engineer\b",
    r"\bplatform\b(?!.*success)(?!.*account)",
    r"\binfrastructure\b", r"\barchitect\b(?!.*success)(?!.*account)",
    r"\bsdr\b", r"\bbdr\b", r"business\s*development\s*rep",
    r"demand\s*gen", r"sales\s*rep\b(?!.*account)",
    r"inside\s*sales(?!.*account)", r"outside\s*sales(?!.*account)",
    r"(it|desktop|hardware|network|systems?)\s*support",
    r"support\s*(developer|programmer)\b(?!.*customer)(?!.*client)",
    r"\bmarketing\b", r"content\s*(manager|writer|strategist)",
    r"product\s*(manager|designer|owner|lead|director)",
    r"\bux\b|\bui\b", r"\bhr\b|human\s*resources",
    r"\bfinance\b|\baccounting\b", r"\blegal\b|\bcompliance\b",
    r"recruiter|recruiting|talent\s*acquisition",
]

INCLUDE_RE = [re.compile(kw, re.I) for kw in INCLUDE_KEYWORDS]
EXCLUDE_RE = [re.compile(kw, re.I) for kw in EXCLUDE_KEYWORDS]


def keyword_classify_role(title: str) -> str:
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
# STAGE 3 & 4: LOCATION FILTER
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
    r"\blocation\s*[\-–—:]?\s*flexible\b",
    r"\blocation\s*agnostic\b",
    r"\b(fully\s*)?distributed\b",
    r"\b(global|international)\s*team\b",
    r"\bmultiple\s*(countries|locations|regions)\b",
]
GLOBAL_RE = [re.compile(kw, re.I) for kw in GLOBAL_KEYWORDS]

STANDALONE_GLOBAL_RE = re.compile(
    r"^\s*(global|worldwide|anywhere|international|remote\s*[\-–—/,]\s*global)\s*$", re.I
)

REMOTE_COUNTRY_RE = re.compile(
    r"\bremote\s*[\-–—/,(]\s*"
    r"(us|usa|united\s*states|uk|united\s*kingdom|canada|australia|germany|"
    r"france|india|europe|apac|latam|dach|anz|spain|italy|netherlands|"
    r"sweden|denmark|switzerland|japan|singapore|brazil|mexico|korea|"
    r"ireland|austria|belgium|norway|finland|poland|portugal|czech|"
    r"israel|new\s*zealand|argentina|colombia|chile|philippines|"
    r"thailand|vietnam|malaysia|indonesia|taiwan|hong\s*kong)"
    r"\)?\b",
    re.I,
)

MAJOR_CITIES = [
    r"new\s*york", r"san\s*francisco", r"los\s*angeles", r"chicago",
    r"seattle", r"boston", r"austin", r"denver", r"atlanta", r"portland",
    r"phoenix", r"dallas", r"houston", r"miami", r"washington",
    r"philadelphia", r"minneapolis", r"raleigh", r"salt\s*lake",
    r"detroit", r"tampa", r"san\s*diego", r"san\s*jose", r"nashville",
    r"charlotte", r"columbus", r"indianapolis", r"pittsburgh",
    r"london", r"manchester", r"birmingham", r"leeds", r"bristol",
    r"edinburgh", r"glasgow", r"cardiff", r"liverpool", r"cambridge",
    r"oxford",
    r"berlin", r"munich", r"hamburg", r"paris", r"lyon", r"amsterdam",
    r"rotterdam", r"madrid", r"barcelona", r"milan", r"rome",
    r"stockholm", r"copenhagen", r"zurich", r"vienna", r"warsaw",
    r"prague", r"brussels", r"lisbon", r"oslo", r"helsinki",
    r"dublin",
    r"toronto", r"vancouver", r"montreal", r"sydney", r"melbourne",
    r"tokyo", r"singapore", r"bangalore", r"mumbai", r"hyderabad",
    r"tel\s*aviv", r"beijing", r"shanghai", r"sao\s*paulo",
    r"mexico\s*city", r"buenos\s*aires", r"bogota", r"seoul",
]
MAJOR_CITIES_RE = [re.compile(r"\b" + c + r"\b", re.I) for c in MAJOR_CITIES]

COUNTRY_NAMES = [
    r"united\s*states", r"usa", r"\bus\b", r"united\s*kingdom", r"\buk\b",
    r"canada", r"australia", r"germany", r"france", r"netherlands",
    r"spain", r"italy", r"sweden", r"denmark", r"switzerland",
    r"austria", r"belgium", r"norway", r"finland", r"poland",
    r"portugal", r"czech", r"ireland", r"israel",
    r"japan", r"singapore", r"india", r"china", r"south\s*korea",
    r"brazil", r"mexico", r"argentina", r"colombia", r"chile",
    r"new\s*zealand", r"philippines", r"thailand", r"vietnam",
    r"malaysia", r"indonesia", r"taiwan", r"hong\s*kong",
    r"england", r"scotland", r"wales",
    r"california", r"texas", r"florida", r"illinois",
    r"pennsylvania", r"massachusetts", r"colorado", r"virginia",
    r"maryland", r"oregon", r"georgia", r"north\s*carolina",
    r"new\s*jersey", r"ohio", r"michigan", r"arizona", r"washington",
]
COUNTRY_NAMES_RE = [re.compile(r"\b" + c + r"\b", re.I) for c in COUNTRY_NAMES]

REGION_RE = re.compile(
    r"\b(apac|latam|dach|anz|seur|neur|emea|mena|cee|nordics|"
    r"north\s*america|south\s*america|latin\s*america|"
    r"asia\s*pacific|middle\s*east)\b",
    re.I,
)

ONLY_RE = re.compile(
    r"\b(us|usa|uk|canada|australia|europe|eu|"
    r"united\s*states|united\s*kingdom|germany|france|india|"
    r"north\s*america|apac|latam)\s*only\b"
    r"|\bonly\s*in\s*(the\s*)?\w+"
    r"|\b\w+\s*based\s*only\b",
    re.I,
)

US_STATE_ABBR_RE = re.compile(
    r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|"
    r"MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|"
    r"SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b"
)


def keyword_classify_location(job: dict) -> str:
    loc = (job.get("location", "") + " " + job.get("country", "")).lower()
    desc = job.get("description_snippet", "").lower()
    has_remote = bool(re.search(r"\bremote\b", loc, re.I))

    if "africa" in loc:
        return "match"
    for country in AFRICAN_COUNTRIES:
        if country in loc:
            return "match"
    if any(rx.search(loc) for rx in GLOBAL_RE):
        return "match"
    if STANDALONE_GLOBAL_RE.search(loc.strip()):
        return "match"
    if re.search(r"\bemea\b", loc, re.I):
        return "match"

    africa_desc_terms = ["africa", "nigeria", "lagos", "nairobi", "kenya",
                         "south africa", "ghana", "accra", "cape town",
                         "johannesburg"]
    if any(term in desc for term in africa_desc_terms):
        return "match"

    if REMOTE_COUNTRY_RE.search(loc):
        return "no_match"
    if ONLY_RE.search(loc):
        return "no_match"

    if has_remote:
        has_city = any(rx.search(loc) for rx in MAJOR_CITIES_RE)
        has_country = any(rx.search(loc) for rx in COUNTRY_NAMES_RE)
        has_state_abbr = US_STATE_ABBR_RE.search(loc)
        if has_city or has_country or has_state_abbr:
            return "no_match"
        if REGION_RE.search(loc) and not re.search(r"\bemea\b", loc, re.I):
            return "no_match"
        return "unsure"

    if loc.strip():
        return "no_match"

    return "unsure"


LOCATION_SYSTEM_PROMPT = """\
You are a job location classifier for a global remote job board. \
For each job, determine if the role could realistically hire someone \
working remotely from anywhere in the world.

These jobs all say "Remote" in the location field with no country qualifier. \
Your job is to check the DESCRIPTION for hidden geographic restrictions.

MATCH if:
- Description has NO mention of country-specific work authorization
- Description does NOT require residency in a specific country
- Description does NOT say "must be authorized to work in [country]"
- Description mentions distributed team, global, or international
- No description available (benefit of the doubt — many ATS platforms \
  like Workday and iCIMS don't include descriptions in the API)
- Company appears to be from a developed country (US, UK, EU, etc.) \
  and nothing restricts the role geographically

NO_MATCH if:
- Description says "must be authorized/eligible to work in the US/UK/etc."
- Description says "US/UK work authorization required"
- Description mentions "W-2 employment", "W2 only"
- Description says "no visa sponsorship", "cannot sponsor", \
  "unable to sponsor", "will not sponsor"
- Description says "must reside in [specific country/state]"
- Description says "this role is based in [country]" without remote option
- Application requires SSN, national ID, or country-specific documentation
- Description mentions country-specific benefits as requirements \
  (401k, PAYE, specific tax residency)
- Description says "must be located in" a specific country/region

Be thorough — scan the ENTIRE description for any restriction signal. \
Many companies bury "must be authorized to work in the US" deep in the text.

When there is NO description and NO restriction signal: say MATCH. \
We'd rather include a questionable role than miss a global opportunity.

Respond ONLY with lines like:
1 MATCH
2 NO_MATCH"""


def ai_classify_locations(jobs: list[dict]) -> list[str]:
    if not jobs:
        return []

    results = ["uncertain"] * len(jobs)

    # Cerebras free tier: 8K context. System prompt ~500 tokens (~2K chars).
    # Budget ~24K chars for user message. Cap description per job so batch fits.
    MAX_USER_CHARS = 24000
    DESC_OVERHEAD = 120  # title + company + location metadata per job

    for i in range(0, len(jobs), AI_BATCH_SIZE):
        batch = jobs[i:i + AI_BATCH_SIZE]
        max_desc = max(200, (MAX_USER_CHARS - len(batch) * DESC_OVERHEAD) // len(batch))
        numbered_lines = []
        for j, job in enumerate(batch):
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
        user_msg = f"Classify these {len(batch)} jobs:\n{chr(10).join(numbered_lines)}"

        log.info(f"  [AI] Sending batch of {len(batch)} to LLM (desc cap: {max_desc} chars)")
        text = _ai_call(LOCATION_SYSTEM_PROMPT, user_msg, max_tokens=1500)

        if text is None:
            log.warning(f"AI location classification failed for batch of {len(batch)}, keeping as uncertain")
            continue

        log.info(f"  [AI] Response: {text.strip()}")

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
    r"(no|not|cannot|can't|won't|will\s*not|unable\s*to)\s*(provide\s*)?sponsor"
    r"|without\s*sponsor"
    r"|sponsorship\s*(is\s*)?(not|un)available",
    re.I,
)


def detect_visa_sponsorship(job: dict) -> str:
    desc = job.get("description_snippet", "")
    if not desc:
        return "unknown"
    if _VISA_NO_RE.search(desc):
        return "no"
    if _VISA_YES_RE.search(desc):
        return "yes"
    return "unknown"
