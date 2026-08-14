"""
Two-stage classifier — multi-provider architecture. (Async I/O Version)
"""

import re
import time
import asyncio
import logging
from config import ROLE_PROVIDERS, LOCATION_PROVIDER, LLM_PROVIDER

log = logging.getLogger(__name__)

# ── Provider-specific AI client setup ─────────────────────
MAX_RETRIES = 4
RETRY_BASE_DELAY = 5


def _make_client(provider: dict):
    from openai import AsyncOpenAI
    return AsyncOpenAI(api_key=provider["api_key"], base_url=provider["base_url"])


_role_clients = {}
_provider_locks = {}

for _p in ROLE_PROVIDERS:
    try:
        _role_clients[_p["name"]] = _make_client(_p)
        _provider_locks[_p["name"]] = asyncio.Lock()
    except Exception as e:
        log.warning(f"Failed to create client for {_p['name']}: {e}")

_location_client = None
if LOCATION_PROVIDER:
    try:
        _location_client = _make_client(LOCATION_PROVIDER)
        _provider_locks[LOCATION_PROVIDER["name"]] = asyncio.Lock()
    except Exception as e:
        log.warning(f"Failed to create location client ({LOCATION_PROVIDER['name']}): {e}")

_last_call_times = {p["name"]: 0.0 for p in ROLE_PROVIDERS}
if LOCATION_PROVIDER:
    _last_call_times[LOCATION_PROVIDER["name"]] = 0.0


async def _ai_call(provider: dict, client, system_prompt: str, user_msg: str, max_tokens: int = 500) -> str | None:
    name = provider["name"]
    interval = provider.get("min_call_interval", 0.0)

    if interval > 0:
        async with _provider_locks[name]:
            elapsed = time.time() - _last_call_times.get(name, 0.0)
            if elapsed < interval:
                await asyncio.sleep(interval - elapsed)
            _last_call_times[name] = time.time()

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
    has_exclude = any(rx.search(title) for rx in EXCLUDE_RE)
    has_include = any(rx.search(title) for rx in INCLUDE_RE)
    if has_exclude and not has_include: return "exclude"
    if has_include and not has_exclude: return "include"
    if has_include and has_exclude: return "unsure"
    return "exclude"


ROLE_SYSTEM_PROMPT = """\
You are a job title classifier. Decide if each title is a Customer Success \
or Account Management role.

YES if the role is any variation of CSM or Account Manager...
NO if the role is Engineering, Sales, IT, HR, etc.

Respond ONLY with lines like:
1 YES
2 NO"""

async def _classify_role_batch(batch: list[str], provider: dict, client) -> dict[str, bool]:
    numbered = "\n".join(f"{j+1}. {t}" for j, t in enumerate(batch))
    user_msg = f"Titles:\n{numbered}"
    max_tokens = max(500, len(batch) * 4)
    text = await _ai_call(provider, client, ROLE_SYSTEM_PROMPT, user_msg, max_tokens=max_tokens)

    results = {}
    if text is None:
        log.warning(f"AI role classification failed ({provider['name']}) for batch of {len(batch)}")
        for t in batch: results[t] = False
        return results

    for line in text.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2:
            try: idx = int(parts[0]) - 1
            except ValueError: continue
            if 0 <= idx < len(batch):
                results[batch[idx]] = parts[1].upper().startswith("YES")

    for t in batch:
        if t not in results: results[t] = False
    return results

def _build_role_batches(titles: list[str], max_chars: int = 400_000) -> list[list[str]]:
    OVERHEAD_PER_TITLE = 20
    batches, current_batch = [], []
    current_chars = 0

    for title in titles:
        title_chars = len(title) + OVERHEAD_PER_TITLE
        if current_batch and current_chars + title_chars > max_chars:
            batches.append(current_batch)
            current_batch = []
            current_chars = 0
        current_batch.append(title)
        current_chars += title_chars
    if current_batch: batches.append(current_batch)
    return batches

async def ai_classify_roles(titles: list[str]) -> dict[str, bool]:
    if not titles: return {}
    providers = ROLE_PROVIDERS
    provider_titles = {p["name"]: [] for p in providers}
    for i, title in enumerate(titles):
        provider_titles[providers[i % len(providers)]["name"]].append(title)

    all_work = []
    for p in providers:
        p_titles = provider_titles[p["name"]]
        if not p_titles: continue
        client = _role_clients.get(p["name"])
        for batch in _build_role_batches(p_titles, max_chars=p["max_batch_chars"]):
            all_work.append((p, client, batch))

    results = {}
    tasks = [_classify_role_batch(batch, provider, client) for provider, client, batch in all_work]
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

    for res, (p, client, batch) in zip(batch_results, all_work):
        if isinstance(res, Exception):
            log.error(f"Role classification error ({p['name']}): {res}")
            for t in batch: results[t] = False
        else:
            results.update(res)
    return results


# ═══════════════════════════════════════════════════════
# STAGE 3 & 4: LOCATION FILTER
# ═══════════════════════════════════════════════════════

GLOBAL_KEYWORDS = [
    r"\bremote\s*[\-–—/,()]?\s*global\b", r"\bremote\s*[\-–—/,()]?\s*worldwide\b",
    r"\bremote\s*[\-–—/,()]?\s*anywhere\b", r"\bwork\s*from\s*anywhere\b",
    r"\bhire\s*(globally|worldwide|anywhere)\b", r"\blocation\s*agnostic\b",
    r"\bborderless\b", r"\ball\s*geograph", r"\bany\s*country\b"
]
GLOBAL_RE = [re.compile(kw, re.I) for kw in GLOBAL_KEYWORDS]
STANDALONE_GLOBAL_RE = re.compile(r"^\s*(global|worldwide|anywhere|international|wfa|earth|remote\s*[\-–—/,()]?\s*(global|worldwide|anywhere|international|wfa))\s*$", re.I)
NON_GEO_WORDS_RE = re.compile(r"\b(remote|full[\-\s]*time|contract|flexible|open|based|hiring|now)\b", re.I)
GLOBAL_STRIP_RE = re.compile(r"\b(global|worldwide|international|anywhere|wfa)\b", re.I)
PLACEHOLDER_LOC_RE = re.compile(r"^\s*(not\s*specified|n/?a|tbd|unspecified|see\s*description|[—\-–\.]+)\s*$", re.I)

_TITLE_LOCATION_RE = re.compile(r"(?:\b(US|USA|UK|EU|CA|AU|IN)\s*[\-–—/]?\s*(?:remote|based|only)|\(\s*(US|USA|UK|EU|India)\s*\)\s*$)", re.I)

def _enrich_location_from_title(loc: str, title: str) -> str:
    if not title: return loc
    loc_stripped = loc.strip().lower()
    if not (not loc_stripped or "remote" in loc_stripped or PLACEHOLDER_LOC_RE.match(loc)): return loc

    _title_global = re.search(r"\b(EMEA|Global\s*Remote|Remote\s*Global|Worldwide|International|Africa)\b", title, re.I)
    if _title_global:
        geo = _title_global.group(1).strip()
        return f"Remote, {geo}" if "remote" in loc_stripped else geo
    return loc

def keyword_classify_location(job: dict) -> str:
    loc = (str(job.get("location", "")) + " " + str(job.get("country", ""))).strip()
    loc = _enrich_location_from_title(loc, job.get("title", ""))
    loc_lower = loc.lower()

    if not loc.strip() or PLACEHOLDER_LOC_RE.match(loc): return "unsure"
    has_remote = bool(re.search(r"\bremote\b", loc_lower))
    if re.search(r"\bsouth\s+africa\b", loc_lower): return "no_match"
    if re.search(r"\bafrica\b", loc_lower): return "match"
    if re.search(r"\bemea\b", loc_lower): return "match" if not re.sub(r"[\s/\-–—,|()·•:;\[\]0-9&|]+", " ", NON_GEO_WORDS_RE.sub("", re.sub(r"\bemea\b", "", loc_lower))).strip() else "no_match"
    
    if any(rx.search(loc_lower) for rx in GLOBAL_RE) or STANDALONE_GLOBAL_RE.search(loc.strip()): return "match"
    if re.search(r"\b(hybrid|on[\-\s]*site|in[\-\s]*person)\b", loc_lower): return "no_match"

    if has_remote:
        stripped = re.sub(r"[\s/\-–—,|()·•:;\[\]0-9]+", " ", NON_GEO_WORDS_RE.sub("", loc_lower)).strip()
        return "unsure" if not stripped else "no_match"
    return "no_match"

LOCATION_SYSTEM_PROMPT = """\
You classify whether a job is open to candidates working remotely from ANYWHERE in the world.
Respond ONLY with lines like:
1 MATCH
2 NO_MATCH
3 UNCERTAIN"""

async def _classify_location_batch(batch_jobs: list[dict], max_user_chars: int) -> list[str]:
    max_desc = max(500, (max_user_chars - len(batch_jobs) * 120) // len(batch_jobs))
    numbered_lines = []
    for j, job in enumerate(batch_jobs):
        desc = job.get("description_snippet", "")
        desc_note = desc[:max_desc] + "…" if desc else "[No description available]"
        numbered_lines.append(f"{j+1}. Title: {job['title']} | Loc: {job.get('location', 'Remote')} | Desc: {desc_note}")
    
    user_msg = f"Classify these {len(batch_jobs)} jobs:\n{chr(10).join(numbered_lines)}"
    text = await _ai_call(LOCATION_PROVIDER, _location_client, LOCATION_SYSTEM_PROMPT, user_msg, max_tokens=1500)
    
    batch_results = ["uncertain"] * len(batch_jobs)
    if not text: return batch_results

    for line in text.splitlines():
        parts = line.strip().split(None, 1)
        if parts:
            try: idx = int(parts[0].rstrip(".")) - 1
            except ValueError: continue
            if 0 <= idx < len(batch_jobs):
                lbl = parts[1].upper() if len(parts) > 1 else ""
                batch_results[idx] = "no_match" if lbl.startswith("NO") else "match" if lbl.startswith("MATCH") else "uncertain"
    return batch_results

def _build_dynamic_batches(jobs: list[dict], max_batch_chars: int) -> list[tuple[int, list[dict]]]:
    batches, current_batch, current_chars, start_idx = [], [], 0, 0
    for i, job in enumerate(jobs):
        desc = job.get("description_snippet", "")[:8000]
        job["description_snippet"] = desc
        job_chars = len(desc) + 120
        if current_batch and current_chars + job_chars > max_batch_chars:
            batches.append((start_idx, current_batch))
            start_idx = i; current_batch = []; current_chars = 0
        current_batch.append(job); current_chars += job_chars
    if current_batch: batches.append((start_idx, current_batch))
    return batches

async def ai_classify_locations(jobs: list[dict]) -> list[str]:
    if not jobs: return []
    provider = LOCATION_PROVIDER
    batches = _build_dynamic_batches(jobs, provider["max_batch_chars"])
    results = ["uncertain"] * len(jobs)
    
    for start_idx, batch in batches:
        batch_results = await _classify_location_batch(batch, 500_000)
        for j, label in enumerate(batch_results): results[start_idx + j] = label
        if len(batches) > 1: await asyncio.sleep(2)
    return results

_VISA_YES_RE = re.compile(r"visa\s*sponsor|sponsor.*visa|relocation\s*(support|assist|package)", re.I)
_VISA_NO_RE = re.compile(r"(no|not|unable|cannot|can\'t|won\'t|will\s*not)\s*(provide\s*)?(visa\s*sponsor|sponsor.*visa)", re.I)

def detect_visa_sponsorship(job: dict) -> str:
    text = f"{job.get('description_snippet', '')} {job.get('title', '')} {job.get('location', '')}"
    if not text.strip(): return "unknown"
    if _VISA_NO_RE.search(text): return "no"
    if _VISA_YES_RE.search(text): return "yes"
    return "unknown"
