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
    r"\bremote\s*[\-\u2013\u2014/,]\s*global\b",
    r"\bremote\s*[\-\u2013\u2014/,]\s*worldwide\b",
    r"\bremote\s*[\-\u2013\u2014/,]\s*anywhere\b",
    r"\bremote\s*[\-\u2013\u2014/,]\s*international\b",
    r"\bglobal\s*[\-\u2013\u2014/,]?\s*remote\b",
    r"\bworldwide\s*[\-\u2013\u2014/,]?\s*remote\b",
    r"\bwork\s*from\s*anywhere\b",
    r"\bhire\s*(globally|worldwide|anywhere)\b",
    r"\bopen\s*to\s*(all|any)\s*location",
    r"\blocation\s*[\-\u2013\u2014:]?\s*anywhere\b",
]

GLOBAL_RE = [re.compile(kw, re.I) for kw in GLOBAL_KEYWORDS]

# Standalone "global" or "worldwide" in the location field
STANDALONE_GLOBAL_RE = re.compile(
    r"^\s*(global|worldwide|anywhere)\s*$", re.I
)

# US cities / states / regions that should be excluded
US_LOCATION_PATTERNS = [
    r"\bnew\s*york\b", r"\bsan\s*francisco\b", r"\blos\s*angeles\b",
    r"\bchicago\b", r"\bseattle\b", r"\bboston\b", r"\baustin\b",
    r"\bdenver\b", r"\batlanta\b", r"\bportland\b", r"\bphoenix\b",
    r"\bdallas\b", r"\bhouston\b", r"\bmiami\b", r"\bdc\b",
    r"\bwashington\b", r"\bphiladelphia\b", r"\bminneapolis\b",
    r"\braleigh\b", r"\bsalt\s*lake\b", r"\bdetroit\b", r"\btampa\b",
    r"\bunited\s*states\b", r"\busa\b", r"\b\(us\)\b", r"\bus\s*only\b",
    r"\busa\s*only\b", r"\bus\s*remote\b", r"\bremote\s*[\-\u2013\u2014,]?\s*us\b",
    r"\bremote\s*[\-\u2013\u2014,]?\s*usa\b", r"\bremote\s*[\-\u2013\u2014,]?\s*united\s*states\b",
    r"\bnorth\s*america\b",
    # US states
    r"\bcalifornia\b", r"\btexas\b", r"\bflorida\b", r"\billinois\b",
    r"\bpennsylvania\b", r"\bgeorgia\b", r"\bmassachusetts\b",
    r"\bcolorado\b", r"\bvirginia\b", r"\bmaryland\b", r"\boregon\b",
    r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b",
]
US_LOCATION_RE = [re.compile(p, re.I) for p in US_LOCATION_PATTERNS]

# Other non-African specific locations
OTHER_NON_AFRICA_PATTERNS = [
    r"\bunited\s*kingdom\b", r"\buk\b", r"\blondon\b", r"\bcanada\b",
    r"\btoronto\b", r"\bvancouver\b", r"\bgermany\b", r"\bberlin\b",
    r"\bfrance\b", r"\bparis\b", r"\baustralia\b", r"\bsydney\b",
    r"\bjapan\b", r"\btokyo\b", r"\bindia\b", r"\bbangalore\b",
    r"\bsingapore\b", r"\bdublin\b", r"\bireland\b", r"\bnetherlands\b",
    r"\bamsterdam\b", r"\bspain\b", r"\bitaly\b", r"\bsweden\b",
    r"\bstockholm\b", r"\bdenmark\b", r"\bswitzerland\b", r"\baustr(ia|alian)\b",
    r"\bisrael\b", r"\btel\s*aviv\b", r"\bchina\b", r"\bbeijing\b",
    r"\bshanghai\b", r"\bbrazil\b", r"\bsao\s*paulo\b", r"\bmexico\b",
    r"\bargentina\b", r"\bcolombia\b", r"\bkorea\b", r"\bseoul\b",
    r"\beurope\b", r"\bemea\b", r"\bapac\b", r"\blatam\b",
    r"\bseur\b", r"\bneur\b", r"\bdach\b", r"\banz\b",
]
OTHER_NON_AFRICA_RE = [re.compile(p, re.I) for p in OTHER_NON_AFRICA_PATTERNS]


def keyword_classify_location(job: dict) -> str:
    """
    Returns 'match' (Africa/global), 'no_match', or 'unsure'.
    Strict: only matches explicit Africa or truly global roles.
    Bare 'Remote' = no_match (most companies mean US remote).
    """
    loc = (job.get("location", "") + " " + job.get("country", "")).lower()
    desc = job.get("description_snippet", "").lower()

    # -- 1. Direct Africa match --
    if "africa" in loc:
        return "match"
    for country in AFRICAN_COUNTRIES:
        if country in loc:
            return "match"

    # Check description for Africa mentions too
    if "africa" in desc or "nigeria" in desc or "lagos" in desc or "nairobi" in desc:
        for country in AFRICAN_COUNTRIES:
            if country in desc:
                return "match"

    # -- 2. Explicit US/non-Africa location -> reject immediately --
    if any(rx.search(loc) for rx in US_LOCATION_RE):
        return "no_match"
    if any(rx.search(loc) for rx in OTHER_NON_AFRICA_RE):
        return "no_match"

    # -- 3. Truly global signals (compound phrases only) --
    if any(rx.search(loc) for rx in GLOBAL_RE):
        return "match"

    # Check if location is standalone "Global" or "Worldwide"
    if STANDALONE_GLOBAL_RE.search(loc.strip()):
        return "match"

    # -- 4. "Remote" alone = no_match (most mean US remote) --
    if re.search(r"\bremote\b", loc, re.I):
        # Check if description has strong global signals
        global_desc_signals = [
            r"open\s*to\s*(candidates\s*)?(globally|worldwide|anywhere)",
            r"hire\s*(in\s*)?(\d+\+?\s*)?countries",
            r"work\s*from\s*anywhere",
            r"location[\s:]+anywhere",
            r"distributed\s*team.*global",
            r"remote.*global",
        ]
        for pattern in global_desc_signals:
            if re.search(pattern, desc, re.I):
                return "match"
        return "no_match"

    # -- 5. Specific non-African location in any form --
    if loc.strip():
        return "no_match"

    # -- 6. No location at all -- check description --
    global_desc_signals = [
        r"open\s*to\s*(candidates\s*)?(globally|worldwide|anywhere)",
        r"hire\s*(in\s*)?(\d+\+?\s*)?countries",
        r"work\s*from\s*anywhere",
    ]
    for pattern in global_desc_signals:
        if re.search(pattern, desc, re.I):
            return "match"

    return "no_match"


LOCATION_AI_PROMPT = """\
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

When in doubt, say NO_MATCH. We only want roles a person in Africa can actually get.

Jobs:
{jobs}

Respond ONLY with lines like:
1 MATCH (reason)
2 NO_MATCH (reason)
"""


def ai_classify_locations(jobs: list[dict]) -> list[str]:
    """
    Send ambiguous jobs to Groq for location classification.
    Returns list of 'match', 'no_match', or 'uncertain' in same order.
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
                        else:
                            results[i + idx] = "no_match"
        except Exception as e:
            log.error(f"Groq location classification error: {e}")

    return results
