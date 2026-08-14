"""
Two-stage classifier — multi-provider architecture, fully async.
Role classification:  Cerebras + Groq + Gemini (free tiers, concurrent)
Location classification: OpenAI GPT-4.1 nano (paid, smartest, 1M context)

Race-condition fix: per-provider AsyncRateLimiter (asyncio.Lock) replaces the
thread-unsafe `_last_call_times` dict + time.sleep() pattern.
"""
import re
import time
import asyncio
import logging

from openai import AsyncOpenAI
from config import (ROLE_PROVIDERS, LOCATION_PROVIDER, LLM_PROVIDER,
                    ROLE_BATCH_CONCURRENCY, LOCATION_BATCH_CONCURRENCY)

log = logging.getLogger(__name__)

MAX_RETRIES = 4
RETRY_BASE_DELAY = 5  # seconds


# ── Task-safe rate limiter (fixes the race condition) ─────
class AsyncRateLimiter:
    """Enforce a minimum interval between calls, safe across concurrent tasks.

    The lock only reserves the next allowed timestamp; the actual sleep happens
    outside the lock so many tasks can wait in parallel and fire in order.
    """

    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def acquire(self):
        if self.min_interval <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            self._next_allowed = max(now, self._next_allowed) + self.min_interval
        if wait > 0:
            await asyncio.sleep(wait)


# ── Provider-specific async AI clients ───────────────────
def _make_client(provider: dict) -> AsyncOpenAI:
    """Create an async OpenAI-compatible client for a provider config dict."""
    return AsyncOpenAI(api_key=provider["api_key"], base_url=provider["base_url"])


_role_clients: dict[str, AsyncOpenAI] = {}
for _p in ROLE_PROVIDERS:
    try:
        _role_clients[_p["name"]] = _make_client(_p)
    except Exception as e:
        log.warning(f"Failed to create client for {_p['name']}: {e}")

_location_client: AsyncOpenAI | None = None
if LOCATION_PROVIDER:
    try:
        _location_client = _make_client(LOCATION_PROVIDER)
    except Exception as e:
        log.warning(f"Failed to create location client ({LOCATION_PROVIDER['name']}): {e}")

# Per-provider rate limiters (task-safe)
_rate_limiters: dict[str, AsyncRateLimiter] = {
    p["name"]: AsyncRateLimiter(p.get("min_call_interval", 0.0)) for p in ROLE_PROVIDERS
}
if LOCATION_PROVIDER:
    _rate_limiters[LOCATION_PROVIDER["name"]] = AsyncRateLimiter(
        LOCATION_PROVIDER.get("min_call_interval", 0.0))


async def _ai_call(provider: dict, client, system_prompt: str,
                   user_msg: str, max_tokens: int = 500) -> str | None:
    """Call an OpenAI-compatible provider with rate limiting + retry."""
    name = provider["name"]
    limiter = _rate_limiters.get(name)
    if limiter:
        await limiter.acquire()

    for attempt in range(MAX_RETRIES):
        try:
            resp = await client.chat.completions.create(
                model=provider["model"],
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
                log.error(f"{name} daily token limit reached — skipping remaining AI calls")
                return None
            if is_rate_limit and attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (attempt + 1)
                log.warning(f"{name} rate limit hit, retrying in {delay}s (attempt {attempt + 1})")
                await asyncio.sleep(delay)
                if limiter:
                    await limiter.acquire()
                continue
            log.error(f"{name} API error (attempt {attempt + 1}): {e}")
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


ROLE_SYSTEM_PROMPT = """You are a job title classifier. Decide if each title is a Customer Success 
or Account Management role.
YES if the role is any variation of:
Customer Success Manager/Lead/Specialist/Director/Associate/Consultant
Account Manager (key/strategic/enterprise/technical/named/regional/global)
Customer/Client Support Manager or Representative
Customer/Client Service Manager or Representative
Customer/Client Experience (CX) Manager
Customer/Client Relationship Manager
Customer/Client Engagement Manager
Customer/Client Care Manager
Retention/Renewal Manager
Onboarding/Implementation Manager (customer-facing)
NO if the role is:
Any kind of Engineer or Developer
Sales (SDR, BDR, Account Executive, demand gen)
IT/Desktop/Hardware Support
Marketing, Product, Design, HR, Finance, Legal
Respond ONLY with lines like:
1 YES
2 NO"""


async def _classify_role_batch(batch: list[str], provider: dict, client) -> dict[str, bool]:
    """Classify a single batch of titles using a specific provider."""
    numbered = "\n".join(f"{j+1}. {t}" for j, t in enumerate(batch))
    user_msg = f"Titles:\n{numbered}"
    max_tokens = max(500, len(batch) * 4)
    text = await _ai_call(provider, client, ROLE_SYSTEM_PROMPT, user_msg, max_tokens=max_tokens)
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
    """Build role classification batches based on character limits."""
    OVERHEAD_PER_TITLE = 20
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


async def ai_classify_roles(titles: list[str]) -> dict[str, bool]:
    """Send ambiguous titles to AI for role classification (fully async)."""
    if not titles:
        return {}

    providers = ROLE_PROVIDERS
    if not providers:
        from config import LLM_API_KEY, LLM_MODEL, LLM_BASE_URL
        providers = [{
            "name": LLM_PROVIDER,
            "api_key": LLM_API_KEY,
            "model": LLM_MODEL,
            "base_url": LLM_BASE_URL,
            "max_batch_chars": 6_000 if LLM_PROVIDER == "cerebras" else 400_000,
            "min_call_interval": 12.5 if LLM_PROVIDER == "cerebras" else 0.0,
        }]
        # Ensure a limiter + client exist for the legacy provider
        _rate_limiters.setdefault(providers[0]["name"],
                                  AsyncRateLimiter(providers[0]["min_call_interval"]))

    provider_titles = {p["name"]: [] for p in providers}
    for i, title in enumerate(titles):
        p = providers[i % len(providers)]
        provider_titles[p["name"]].append(title)

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
                _rate_limiters.setdefault(p["name"],
                                          AsyncRateLimiter(p.get("min_call_interval", 0.0)))
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
    sem = asyncio.Semaphore(ROLE_BATCH_CONCURRENCY)

    async def _run(provider, client, batch):
        async with sem:
            return await _classify_role_batch(batch, provider, client)

    tasks = [_run(provider, client, batch) for provider, client, batch in all_work]
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    for (provider, _client, batch), outcome in zip(all_work, outcomes):
        if isinstance(outcome, Exception):
            log.error(f"Role classification error ({provider['name']}): {outcome}")
            for t in batch:
                results[t] = False
        else:
            results.update(outcome)
    return results


# ═══════════════════════════════════════════════════════
# STAGE 3 & 4: LOCATION FILTER (Global hiring only)
# ═══════════════════════════════════════════════════════
GLOBAL_KEYWORDS = [
    r"\bremote\s*[-–—/,()]?\s*global\b",
    r"\bremote\s*[-–—/,()]?\s*worldwide\b",
    r"\bremote\s*[-–—/,()]?\s*anywhere\b",
    r"\bremote\s*[-–—/,()]?\s*international\b",
    r"\bremote\s*[-–—/,()]?\s*wfa\b",
    r"\bremote\s*[-–—/,()]?\s*everywhere\b",
    r"\bglobal\s*[-–—/,()]?\s*remote\b",
    r"\bworldwide\s*[-–—/,()]?\s*remote\b",
    r"\binternational\s*[-–—/,()]?\s*remote\b",
    r"\banywhere\s*[-–—/,()]?\s*remote\b",
    r"\bwork\s*from\s*anywhere\b",
    r"\bwfa\b",
    r"\bhire\s*(globally|worldwide|anywhere)\b",
    r"\bhiring\s*(globally|worldwide|anywhere)\b",
    r"\bopen\s*to\s*(all|any)\s*location",
    r"\bopen\s*to\s*(all|any)\s*countr",
    r"\blocation\s*[-–—:]?\s*anywhere\b",
    r"\blocation\s*[-–—:]?\s*flexible\b",
    r"\blocation\s*agnostic\b",
    r"\blocation\s*independent\b",
    r"\bgeo[-\s]*flexible\b",
    r"\bgeo[-\s]*agnostic\b",
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
    r"remote\s*[-–—/,()]?\s*(global|worldwide|anywhere|international|wfa))\s*$", re.I
)

NON_GEO_WORDS_RE = re.compile(
    r"\b("
    r"remote|fully|completely|"
    r"full[-\s]*time|part[-\s]*time|"
    r"contract(?:or|ual)?|permanent|temporary|temp|"
    r"freelance|intern(?:ship)?|hourly|salaried|"
    r"direct[-\s]*hire|regular|casual|seasonal|"
    r"fte|pte|"
    r"worker|job|position|role|opening|opportunity|"
    r"n/?a|not\s*specified|unspecified|tbd|"
    r"flexible|open|based|home|general|"
    r"monday|tuesday|wednesday|thursday|friday|"
    r"saturday|sunday|weekday|weekend|"
    r"shift|schedule|day|night|evening|morning|"
    r"hours|hrs|am|pm|to|and|or|the|a|an|at|for|of|"
    r"immediate|urgent|asap|new|multiple|"
    r"available|hiring|now|apply"
    r")\b",
    re.I,
)
GLOBAL_STRIP_RE = re.compile(
    r"\b("
    r"global|worldwide|international|anywhere|everywhere|"
    r"wfa|distributed|borderless|work|from"
    r")\b",
    re.I,
)
PLACEHOLDER_LOC_RE = re.compile(
    r"^\s*(not\s*specified|n/?a|tbd|to\s*be\s*determined|"
    r"unspecified|see\s*description|see\s*below|"
    r"multiple\s*locations?|various\s*locations?|"
    r"[—\-–.]+)\s*$",
    re.I,
)

_TITLE_LOCATION_RE = re.compile(
    r"(?:"
    r"\b(US|USA|UK|EU|CA|AU|IN|DE|FR|NL|SG|HK|JP|BR|MX|PH|NG|KE|ZA|AE|SA|IL|PL|CZ|RO|BG|HU|IE|ES|IT|PT|SE|NO|DK|FI|CH|AT|BE|NZ)"
    r"\s*[-–—/]?\s*(?:remote|based|only)"
    r"|"
    r"(?:remote)\s*[-–—/,()]\s*"
    r"(US|USA|UK|EU|India|United\s+States|United\s+Kingdom|Canada|Australia|Germany|France|Netherlands)"
    r"|"
    r"(\s*(US|USA|UK|EU|India|Canada|Australia)\s*)\s*$"
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
    """If location is bare 'Remote' or empty, extract geographic hints from title."""
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
        geo = next((g for g in match.groups() if g), None)
        if geo:
            geo = geo.strip()
            if loc_stripped and "remote" in loc_stripped:
                return f"Remote, {geo}"
            return geo
    return loc


def keyword_classify_location(job: dict) -> str:
    """Returns 'match', 'no_match', or 'unsure'."""
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

    if not loc.strip() or PLACEHOLDER_LOC_RE.match(loc):
        return "unsure"
    has_remote = bool(re.search(r"\bremote\b", loc_lower))

    if re.search(r"\bsouth\s+africa\b", loc_lower):
        return "no_match"
    if re.search(r"\bafrica\b", loc_lower):
        return "match"
    if re.search(r"\bemea\b", loc_lower):
        check = re.sub(r"\bemea\b", "", loc_lower)
        check = NON_GEO_WORDS_RE.sub("", check)
        check = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9&|]+", " ", check).strip()
        if not check:
            return "match"
        return "no_match"

    has_global = any(rx.search(loc_lower) for rx in GLOBAL_RE) or STANDALONE_GLOBAL_RE.search(loc.strip())
    if has_global:
        check = GLOBAL_STRIP_RE.sub("", loc_lower)
        check = NON_GEO_WORDS_RE.sub("", check)
        check = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9&]+", " ", check).strip()
        if not check:
            return "match"
        return "no_match"

    if re.search(r"\bhybrid\b", loc_lower):
        return "no_match"
    if re.search(r"\bon[\-\s]*site\b", loc_lower):
        return "no_match"
    if re.search(r"\bin[\-\s]*person\b", loc_lower):
        return "no_match"

    if has_remote:
        stripped = NON_GEO_WORDS_RE.sub("", loc_lower)
        stripped = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9]+", " ", stripped).strip()
        if not stripped:
            return "unsure"
        return "no_match"

    return "no_match"


LOCATION_SYSTEM_PROMPT = """You classify whether a job is open to candidates working remotely 
from ANYWHERE in the world (truly global hiring, not country-specific).
These jobs say "Remote" with no country qualifier. Your task: 
check the DESCRIPTION for evidence of global hiring OR geographic restrictions.
MATCH — positive evidence the role is genuinely global:
Description explicitly says "global", "worldwide", "anywhere", "international", "work from anywhere", "distributed team"
Hiring across multiple continents or many countries
No geographic restrictions AND the role/company context clearly 
suggests global openness (e.g. "our team spans 30+ countries")
NO_MATCH — evidence of country or region restriction:
"must be authorized/eligible to work in [country]"
"US/UK/EU work authorization required"
"W-2 employment", "W2 only", "must have SSN"
"no visa sponsorship", "cannot sponsor", "will not sponsor"
"must reside in [state/country]", "must be located in [place]"
"this role is based in [country]" without global remote option
Country-specific benefits as requirements (401k, PAYE, tax residency)
Description context makes it obvious the role is for one country 
(e.g. references to US-specific regulations, UK employment law)
UNCERTAIN — cannot determine either way:
No description available
Description does not mention location requirements at all
Ambiguous or conflicting signals
IMPORTANT: When there is no description or no clear signal, say UNCERTAIN. 
Do NOT default to MATCH. Only say MATCH when you see positive evidence 
of global/worldwide hiring. When in doubt, UNCERTAIN.
Respond ONLY with lines like:
1 MATCH
2 NO_MATCH
3 UNCERTAIN"""


async def _classify_location_batch(batch_jobs: list[dict], max_user_chars: int) -> list[str]:
    """Classify a single batch of jobs by location using LOCATION_PROVIDER."""
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
    text = await _ai_call(LOCATION_PROVIDER, _location_client,
                          LOCATION_SYSTEM_PROMPT, user_msg, max_tokens=1500)

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


def _build_dynamic_batches(jobs: list[dict], max_batch_chars: int) -> list[tuple[int, list[dict]]]:
    """Build batches dynamically based on description length."""
    OVERHEAD_PER_JOB = 120
    MAX_DESC_CHARS = 8000
    batches = []
    current_batch = []
    current_chars = 0
    start_idx = 0
    for i, job in enumerate(jobs):
        desc = job.get("description_snippet") or ""
        if len(desc) > MAX_DESC_CHARS:
            job["description_snippet"] = desc[:MAX_DESC_CHARS]
            desc = job["description_snippet"]
        job_chars = len(desc) + OVERHEAD_PER_JOB
        if current_batch and current_chars + job_chars > max_batch_chars:
            batches.append((start_idx, current_batch))
            start_idx = i
            current_batch = []
            current_chars = 0
        current_batch.append(job)
        current_chars += job_chars
    if current_batch:
        batches.append((start_idx, current_batch))
    return batches


async def ai_classify_locations(jobs: list[dict]) -> list[str]:
    """Send ambiguous jobs (bare "Remote") to the location provider (async).

    Batches run concurrently but the per-provider AsyncRateLimiter enforces the
    minimum call interval, replacing the old sequential loop + time.sleep(2).
    """
    if not jobs:
        return []
    provider = LOCATION_PROVIDER
    max_batch_chars = provider["max_batch_chars"]
    max_user_chars = 500_000
    batches = _build_dynamic_batches(jobs, max_batch_chars)
    log.info(f"Location classification ({provider['name']}): {len(jobs)} jobs → {len(batches)} batches "
             f"(sizes: {[len(b) for _, b in batches]})")

    results = ["uncertain"] * len(jobs)
    sem = asyncio.Semaphore(LOCATION_BATCH_CONCURRENCY)

    async def _run(start_idx, batch):
        async with sem:
            return start_idx, await _classify_location_batch(batch, max_user_chars)

    outcomes = await asyncio.gather(*[_run(si, batch) for si, batch in batches],
                                    return_exceptions=True)
    for outcome in outcomes:
        if isinstance(outcome, Exception):
            log.error(f"Location classification error: {outcome}")
            continue
        start_idx, batch_results = outcome
        for j, label in enumerate(batch_results):
            results[start_idx + j] = label

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
    r"(no|not|unable|cannot|can't|won't|will\s*not)\s*(provide\s*)?(visa\s*sponsor|sponsor.*visa|work\s*permit|immigration\s*sponsor)"
    r"|must\s*(be\s*)?(authorized|eligible)\s*to\s*work"
    r"|without\s*(visa\s*)?sponsor"
    r"|visa\s*sponsorship\s*(is\s*)?(not|un)available"
    r"|not\s*offer.*sponsorship",
    re.I,
)


def detect_visa_sponsorship(job: dict) -> str:
    """Scan description + title for visa sponsorship signals."""
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
