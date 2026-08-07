"""
Two-stage classifier:
  Stage 1 — keyword filter for CSM/AM role titles (fast, no API)
  Stage 2 — Groq AI for ambiguous titles (batched)

Then a separate location filter:
  Stage 3 — keyword check for Africa/Global locations
  Stage 4 — Groq AI for ambiguous locations + description scanning
"""

import re
import logging
from groq import Groq
from config import GROQ_API_KEY, GROQ_MODEL, AI_BATCH_SIZE

log = logging.getLogger(__name__)

client = Groq(api_key=GROQ_API_KEY)


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
    return "exclude"  # no match at all = not a CSM/AM role


ROLE_AI_PROMPT = """\
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

Titles:
{titles}

Respond ONLY with lines like:
1 YES
2 NO
"""


def ai_classify_roles(titles: list[str]) -> dict[str, bool]:
    """Send ambiguous titles to Groq. Returns {title: is_relevant}."""
    if not titles:
        return {}

    results = {}
    for i in range(0, len(titles), AI_BATCH_SIZE):
        batch = titles[i:i + AI_BATCH_SIZE]
        numbered = "\n".join(f"{j+1}. {t}" for j, t in enumerate(batch))
        prompt = ROLE_AI_PROMPT.format(titles=numbered)

        try:
            resp = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=500,
            )
            text = resp.choices[0].message.content.strip()
            for line in text.splitlines():
                parts = line.strip().split(None, 1)
                if len(parts) == 2:
                    idx = int(parts[0]) - 1
                    if 0 <= idx < len(batch):
                        results[batch[idx]] = parts[1].upper().startswith("YES")
        except Exception as e:
            log.error(f"Groq role classification error: {e}")
            for t in batch:
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
    r"\bglobal\b", r"\bworldwide\b", r"\banywhere\b",
    r"\bremote\s*[\-–—]?\s*global\b", r"\bglobally\b",
    r"\binternational\b", r"\bwork\s*from\s*anywhere\b",
    r"\bremote\b(?!.*\bunited\s*states\b)(?!.*\bus\s*only\b)(?!.*\busa\s*only\b)",
]

GLOBAL_RE = [re.compile(kw, re.I) for kw in GLOBAL_KEYWORDS]

US_ONLY_PATTERNS = [
    r"\bunited\s*states\s*only\b", r"\bus[\s-]*only\b",
    r"\busa\s*only\b", r"\bmust\s*be\s*(located|based)\s*in\s*the\s*(us|united\s*states)\b",
    r"\bauthori[sz]ed\s*to\s*work\s*in\s*the\s*(us|united\s*states)\b",
]
US_ONLY_RE = [re.compile(p, re.I) for p in US_ONLY_PATTERNS]


def keyword_classify_location(job: dict) -> str:
    """
    Returns 'match' (Africa/global), 'no_match', or 'unsure'.
    Checks location fields only.
    """
    loc = (job.get("location", "") + " " + job.get("country", "")).lower()

    # Direct Africa match
    if "africa" in loc:
        return "match"
    for country in AFRICAN_COUNTRIES:
        if country in loc:
            return "match"

    # Global/worldwide/anywhere
    if any(rx.search(loc) for rx in GLOBAL_RE):
        # But check for US-only signals
        full_text = loc + " " + job.get("description_snippet", "").lower()
        if any(rx.search(full_text) for rx in US_ONLY_RE):
            return "no_match"
        return "match"

    # "Remote" without country restriction could be global
    if re.search(r"\bremote\b", loc, re.I):
        # If location says "Remote" with a specific non-African country, it's not global
        specific_non_africa = re.search(
            r"remote.*\b(united states|usa|us|uk|united kingdom|canada|germany|france|australia|india|japan|china)\b",
            loc, re.I
        )
        if specific_non_africa:
            return "no_match"
        # Bare "Remote" is ambiguous
        return "unsure"

    # Specific non-African location
    if loc.strip():
        return "no_match"

    # No location info at all
    return "unsure"


LOCATION_AI_PROMPT = """\
You are a job location classifier. For each job below, determine if the role \
could be filled by someone based in Africa or if it's open globally \
(not restricted to a specific non-African country).

Look at the location, country, and description for clues like:
- "Remote - Global", "Work from anywhere", "We hire in 30+ countries"
- Mentions of African countries or "Africa"
- "Open to candidates worldwide"
- Conversely: "Must be based in the US", "US work authorization required"

Jobs:
{jobs}

Respond ONLY with lines like:
1 MATCH (reason)
2 NO_MATCH (reason)
3 UNCERTAIN (reason)
"""


def ai_classify_locations(jobs: list[dict]) -> list[str]:
    """
    Send ambiguous jobs to Groq for location classification.
    Returns list of 'match', 'no_match', or 'uncertain' in same order.
    """
    if not jobs:
        return []

    results = ["uncertain"] * len(jobs)

    for i in range(0, len(jobs), AI_BATCH_SIZE):
        batch = jobs[i:i + AI_BATCH_SIZE]
        numbered_lines = []
        for j, job in enumerate(batch):
            numbered_lines.append(
                f"{j+1}. Title: {job['title']} | Location: {job['location']} | "
                f"Country: {job.get('country', '')} | "
                f"Description: {job.get('description_snippet', '')[:500]}"
            )
        prompt = LOCATION_AI_PROMPT.format(jobs="\n".join(numbered_lines))

        try:
            resp = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=1000,
            )
            text = resp.choices[0].message.content.strip()
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
                            results[i + idx] = "uncertain"
        except Exception as e:
            log.error(f"Groq location classification error: {e}")

    return results
