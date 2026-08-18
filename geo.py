"""
Lightweight country/continent gazetteer — shared by ats_scrapers.py (to
resolve ADP's requisitionLocations, which carry no explicit country field,
only strings like "Miami, FL, US") and classifier.py (to tell a genuine
multi-country/multi-continent job listing apart from a job that just has
several city/state locations within ONE country).

Not a full ISO-3166 implementation — covers the countries this project's
scrapers actually encounter (all of Africa in full, plus the countries
that show up regularly in North America/Europe/Asia/Oceania/South America
job postings). Extend COUNTRY_ALIASES/COUNTRY_CONTINENT if a gap shows up.

── 2026-08 regression fix ──────────────────────────────────────────────
The previous version of this module put US state / Canadian province
2-letter codes (AL, CA, GA, IN, NH, NS, ...) directly into the same
unrestricted "match this alias ANYWHERE in the text" regex used for real
country names. That's ambiguous on its face — "CA" is both the US state
California AND the ISO-2 code for Canada; "IN" is both Indiana and an
ordinary English word/preposition — and it also let a country's full name
match as a substring of a *different* place's full name (e.g. "Mexico"
inside "New Mexico", a US state), which is what caused single-country US
jobs to get miscounted as 2-country ("Mexico" + "United States") and
wrongly promoted to PRIORITY_GLOBAL.

Fixed by:
  1. Never adding bare 2-letter codes to the free-matching alias regex.
     They're only trusted in two narrow, high-confidence shapes:
       a) ", XX" — comma immediately followed by a 2-letter code
          (classic "City, ST" / "City, Province" shape)
       b) "XX-YY-" at the start of a location token — iCIMS-style
          "CA-SK-Saskatoon" / "US-NY-Malta" ISO2-country + region prefix
  2. Masking out full US-state / Canadian-province NAMES (e.g. "New
     Mexico", "New York") from the text before running the general
     country-name regex, so a state name that happens to contain a
     country's name as a trailing word can never be double-counted as
     that country.
  3. Matching short (<=3 char) country codes/abbreviations (US, UK, USA,
     UAE, DRC, ...) case-SENSITIVELY, so ordinary lowercase prose words
     ("join us", "check in") can't be mistaken for country codes.
"""

import re

# ══════════════════════════════════════════════════════════
# Canonical country -> continent
# ══════════════════════════════════════════════════════════

COUNTRY_CONTINENT: dict[str, str] = {
    # ── Africa (all 54 UN-recognized states) ──
    "Algeria": "Africa", "Angola": "Africa", "Benin": "Africa",
    "Botswana": "Africa", "Burkina Faso": "Africa", "Burundi": "Africa",
    "Cabo Verde": "Africa", "Cameroon": "Africa",
    "Central African Republic": "Africa", "Chad": "Africa",
    "Comoros": "Africa", "Democratic Republic of the Congo": "Africa",
    "Republic of the Congo": "Africa", "Ivory Coast": "Africa",
    "Djibouti": "Africa", "Egypt": "Africa", "Equatorial Guinea": "Africa",
    "Eritrea": "Africa", "Eswatini": "Africa", "Ethiopia": "Africa",
    "Gabon": "Africa", "Gambia": "Africa", "Ghana": "Africa",
    "Guinea": "Africa", "Guinea-Bissau": "Africa", "Kenya": "Africa",
    "Lesotho": "Africa", "Liberia": "Africa", "Libya": "Africa",
    "Madagascar": "Africa", "Malawi": "Africa", "Mali": "Africa",
    "Mauritania": "Africa", "Mauritius": "Africa", "Morocco": "Africa",
    "Mozambique": "Africa", "Namibia": "Africa", "Niger": "Africa",
    "Nigeria": "Africa", "Rwanda": "Africa",
    "Sao Tome and Principe": "Africa", "Senegal": "Africa",
    "Seychelles": "Africa", "Sierra Leone": "Africa", "Somalia": "Africa",
    "South Africa": "Africa", "South Sudan": "Africa", "Sudan": "Africa",
    "Tanzania": "Africa", "Togo": "Africa", "Tunisia": "Africa",
    "Uganda": "Africa", "Zambia": "Africa", "Zimbabwe": "Africa",

    # ── North America ──
    "United States": "North America", "Canada": "North America",
    "Mexico": "North America",

    # ── Europe ──
    "United Kingdom": "Europe", "Ireland": "Europe", "Germany": "Europe",
    "France": "Europe", "Spain": "Europe", "Italy": "Europe",
    "Netherlands": "Europe", "Belgium": "Europe", "Switzerland": "Europe",
    "Austria": "Europe", "Portugal": "Europe", "Poland": "Europe",
    "Sweden": "Europe", "Norway": "Europe", "Denmark": "Europe",
    "Finland": "Europe", "Greece": "Europe", "Romania": "Europe",
    "Czech Republic": "Europe", "Hungary": "Europe", "Ukraine": "Europe",
    "Croatia": "Europe", "Bulgaria": "Europe", "Serbia": "Europe",
    "Slovakia": "Europe", "Slovenia": "Europe", "Estonia": "Europe",
    "Latvia": "Europe", "Lithuania": "Europe", "Luxembourg": "Europe",
    "Iceland": "Europe", "Cyprus": "Europe", "Malta": "Europe",

    # ── Asia ──
    "India": "Asia", "China": "Asia", "Japan": "Asia",
    "South Korea": "Asia", "Singapore": "Asia", "Philippines": "Asia",
    "Indonesia": "Asia", "Malaysia": "Asia", "Vietnam": "Asia",
    "Thailand": "Asia", "Pakistan": "Asia", "Bangladesh": "Asia",
    "Israel": "Asia", "United Arab Emirates": "Asia",
    "Saudi Arabia": "Asia", "Qatar": "Asia", "Turkey": "Asia",
    "Hong Kong": "Asia", "Taiwan": "Asia", "Sri Lanka": "Asia",

    # ── South America ──
    "Brazil": "South America", "Argentina": "South America",
    "Chile": "South America", "Colombia": "South America",
    "Peru": "South America", "Uruguay": "South America",
    "Ecuador": "South America", "Venezuela": "South America",
    "Bolivia": "South America", "Paraguay": "South America",

    # ── Oceania ──
    "Australia": "Oceania", "New Zealand": "Oceania",
}

AFRICAN_COUNTRIES = {c for c, cont in COUNTRY_CONTINENT.items() if cont == "Africa"}

# ══════════════════════════════════════════════════════════
# US states / Canadian provinces — NAMES and CODES kept in
# separate namespaces from country aliases (see module docstring).
# ══════════════════════════════════════════════════════════

_US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut",
    "DE": "Delaware", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan",
    "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}

_CA_PROVINCES = {
    "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba",
    "NB": "New Brunswick", "NL": "Newfoundland and Labrador",
    "NS": "Nova Scotia", "ON": "Ontario", "PE": "Prince Edward Island",
    "QC": "Quebec", "SK": "Saskatchewan", "NT": "Northwest Territories",
    "NU": "Nunavut", "YT": "Yukon",
}

# Full state/province NAMES, longest-first so "New Hampshire" matches
# before a shorter partial would. These get MASKED OUT of the text before
# country-name matching runs, specifically so a state name that ends in
# a country's name (e.g. "New Mexico") can never register as that country.
_US_STATE_NAMES_RE = re.compile(
    r"\b(" + "|".join(re.escape(n) for n in sorted(_US_STATES.values(), key=len, reverse=True)) + r")\b",
    re.I,
)
# "Ontario" and "Yukon" excluded from the free province-name mask —
# confirmed live: "Ontario, California" / "Ontario, NY" (both real US
# cities named Ontario) and "Yukon, OK" (a real Oklahoma town) were
# getting Canada credit purely from the word "Ontario"/"Yukon" itself,
# stacking with the correct US-state evidence to falsely register 2
# countries. Every other province full name (British Columbia, Alberta,
# Saskatchewan, Manitoba, Quebec, Nova Scotia, New Brunswick, Prince
# Edward Island, Newfoundland and Labrador, Northwest Territories,
# Nunavut) isn't a real US place name, so those stay trusted.
_AMBIGUOUS_CA_PROVINCE_NAMES = {"Ontario", "Yukon"}
_CA_PROVINCE_NAMES_RE = re.compile(
    r"\b(" + "|".join(
        re.escape(n) for n in sorted(_CA_PROVINCES.values(), key=len, reverse=True)
        if n not in _AMBIGUOUS_CA_PROVINCE_NAMES
    ) + r")\b",
    re.I,
)

# ── ISO2-country + region prefix shape, e.g. iCIMS's "CA-SK-Saskatoon" or
# "US-NY-Malta": the FIRST token here is a literal ISO2 country code, a
# different namespace from the US-state 2-letter codes above (both happen
# to include "CA", but they never mean the same thing in this shape).
_ISO2_COUNTRY_PREFIX = {
    "US": "United States", "CA": "Canada", "GB": "United Kingdom",
    "UK": "United Kingdom", "AU": "Australia", "NZ": "New Zealand",
    "IE": "Ireland", "DE": "Germany", "FR": "France", "ES": "Spain",
    "IT": "Italy", "NL": "Netherlands", "BE": "Belgium", "CH": "Switzerland",
    "IN": "India", "SG": "Singapore", "ZA": "South Africa", "NG": "Nigeria",
    "KE": "Kenya", "EG": "Egypt", "MA": "Morocco", "GH": "Ghana",
}
# Consume the trailing city-name segment too (through the next comma/
# semicolon/end-of-string), not just the "XX-YY-" prefix itself — the
# city name is never country evidence in this shape, and leaving it
# unmasked lets a city that happens to share a name with a real country
# (e.g. iCIMS's "US-NY-Malta" — Malta, New York — vs the country Malta)
# get double-counted as a second, bogus country.
_ISO2_PREFIX_RE = re.compile(
    r"(?:^|[;,]\s*)(" + "|".join(sorted(_ISO2_COUNTRY_PREFIX, key=len, reverse=True)) + r")-[A-Za-z]{2,3}-[^,;]*",
)

# ══════════════════════════════════════════════════════════
# Alias -> canonical country name (full names + safe long
# abbreviations only — no bare 2-letter state/province codes here)
# ══════════════════════════════════════════════════════════

COUNTRY_ALIASES: dict[str, str] = {}


def _alias(alias: str, canonical: str) -> None:
    COUNTRY_ALIASES[alias.lower()] = canonical


for _country in COUNTRY_CONTINENT:
    _alias(_country, _country)

# Common alternate names / abbreviations (length >= 2, matched via the
# short-code path below when <=3 chars so casing is enforced)
_alias("US", "United States")
_alias("USA", "United States")
_alias("U.S.", "United States")
_alias("U.S.A.", "United States")
_alias("United States of America", "United States")
_alias("UK", "United Kingdom")
_alias("U.K.", "United Kingdom")
_alias("Great Britain", "United Kingdom")
# England/Scotland/Wales/Northern Ireland deliberately NOT registered as
# free-matching aliases — confirmed live, this exact class of bug: "North
# Wales, PA" (a real Pennsylvania town) and "New South Wales" (an
# Australian state) both contain "Wales" as a stand-alone word, and
# similarly "New England" (the US region) contains "England". Real ATS
# postings for actual UK jobs consistently say "United Kingdom" or "UK",
# so this loses essentially nothing while closing off a whole class of
# false-positive collisions with real place names elsewhere.
_alias("UAE", "United Arab Emirates")
_alias("Ivory Coast", "Ivory Coast")
_alias("Cote d'Ivoire", "Ivory Coast")
_alias("Côte d'Ivoire", "Ivory Coast")
_alias("DRC", "Democratic Republic of the Congo")
_alias("DR Congo", "Democratic Republic of the Congo")
_alias("Congo-Kinshasa", "Democratic Republic of the Congo")
_alias("Congo-Brazzaville", "Republic of the Congo")
_alias("Cape Verde", "Cabo Verde")
_alias("Swaziland", "Eswatini")
_alias("South Korea", "South Korea")
_alias("Republic of Korea", "South Korea")

# Split aliases into "long" (matched case-insensitively, anywhere, safe —
# these are full names/phrases not easily confused with ordinary prose)
# and "short" (<=3 chars: US, UK, USA, UAE, DRC — matched case-SENSITIVELY
# so lowercase prose words like "us"/"uk" can never match).
_LONG_ALIASES = {a: c for a, c in COUNTRY_ALIASES.items() if len(a) > 3}
_SHORT_ALIASES = {a: c for a, c in COUNTRY_ALIASES.items() if len(a) <= 3}

_LONG_ALIAS_LIST = sorted(_LONG_ALIASES.keys(), key=len, reverse=True)
_LONG_ALIAS_RE = re.compile(
    r"\b(" + "|".join(re.escape(a) for a in _LONG_ALIAS_LIST) + r")\b",
    re.I,
)

# Case-sensitive: keys here are lowercase (from _alias()), so match against
# the ORIGINAL-CASE uppercase form only, e.g. "US" not "us".
_SHORT_ALIAS_ORIGCASE = {a.upper(): c for a, c in _SHORT_ALIASES.items()}
_SHORT_ALIAS_LIST = sorted(_SHORT_ALIAS_ORIGCASE.keys(), key=len, reverse=True)
_SHORT_ALIAS_RE = re.compile(
    r"\b(" + "|".join(re.escape(a) for a in _SHORT_ALIAS_LIST) + r")\b",
)

# 2-letter state/province codes are only trustworthy as country evidence
# when they appear in the classic "City, XX" pattern (comma then 2 letters,
# case-sensitive — real codes are uppercase in ATS location strings;
# lowercase would just be ordinary text) — bare "IN" or "OR" floating in
# prose text is far too likely to be the English word/preposition.
#
# A few US-state codes are ALSO real ISO2 country codes for a country
# already in COUNTRY_CONTINENT (DE=Delaware/Germany, IN=Indiana/India,
# MA=Massachusetts/Morocco) — genuinely ambiguous from a bare "City, XX"
# string alone. Confirmed live: "Berlin, DE | Germany (REMOTE) |
# Stuttgart, DE" was resolving "DE" to Delaware (United States) via this
# path, turning a single-country (Germany) posting into a false 2-country
# "Germany + United States" Global match. These three codes are excluded
# from the plain 2-part comma path entirely — they're still resolved
# correctly in the unambiguous 3-part "City, Region, cc" trailing-code
# shape (_TRAILING_CC_RE) and the "XX-YY-City" iCIMS shape (both apply
# BEFORE this path runs, so a real Delaware/Indiana/Massachusetts posting
# in either of those shapes is unaffected) — only the bare "City, DE" /
# "City, IN" / "City, MA" 2-part shape now falls through to no match
# instead of guessing, which is the safer failure direction here.
_AMBIGUOUS_STATE_CODES = {"DE", "IN", "MA"}
_STATE_CODE_MAP = {
    k: v for k, v in {**_US_STATES, **{k: v for k, v in _CA_PROVINCES.items() if k not in _US_STATES}}.items()
    if k not in _AMBIGUOUS_STATE_CODES
}
_STATE_CODE_RE = re.compile(
    r",\s*(" + "|".join(sorted(_STATE_CODE_MAP.keys(), key=len, reverse=True)) + r")\b",
)
_STATE_CODE_TO_COUNTRY = {
    **{k: "United States" for k in _US_STATES},
    **{k: "Canada" for k in _CA_PROVINCES},
}


# "City, Region, Country" — a very common ATS shape (Greenhouse/Lever/
# iCIMS/etc.), e.g. "Peru, IL, us", "Schiphol, NH, nl", "Perth, WA,
# Australia", "Sydney, New South Wales, Australia". Requires >= 2 commas
# (3+ parts): if the LAST part is itself a recognized country (either a
# full name or an ISO2 code, matched case-insensitively since this
# terminal position is a trusted, unambiguous slot), that's authoritative
# for the WHOLE segment — every earlier part is ignored. This one rule is
# what lets us safely skip:
#   - the CITY happening to be a real country name ("Peru", "Angola",
#     "Mexico", "Malta", "Brazil" are all real US town names)
#   - the REGION code/name colliding with something else ("WA" =
#     Washington state *or* Western Australia; "NH" = New Hampshire *or*
#     Noord-Holland; "New South Wales" contains the literal word "Wales",
#     which is itself a registered United Kingdom alias)
# All confirmed live in production data — e.g. "Sydney, New South Wales,
# Australia" was resolving to {Australia, United Kingdom} (2 countries,
# wrongly promoted to a Global match) purely because "Wales" is a
# stand-alone word inside "New South Wales".
_COMMA_SPLIT_RE = re.compile(r"\s*,\s*")
_TRAILING_PAREN_RE = re.compile(r"\(.*?\)", re.I)


def _extract_from_segment(seg: str) -> set[str]:
    found: set[str] = set()
    seg_stripped = seg.strip()

    # 0a. "PROVINCE, CA" 2-part shape, e.g. "ON, CA" / "BC, CA" — CA here
    # is the ISO2 country code Canada, not the US state California,
    # because the part before it is ITSELF a Canadian province code (no
    # real city is named "ON" or "BC" or "AB"). Narrow, deliberately
    # exact-match only.
    m0 = re.match(r"^([A-Za-z]{2})\s*,\s*CA$", seg_stripped, re.I)
    if m0 and m0.group(1).upper() in _CA_PROVINCES:
        return {"Canada"}

    # 0b. Trusted terminal country — see module-level comment above.
    parts = [p for p in _COMMA_SPLIT_RE.split(seg_stripped) if p]
    if len(parts) >= 3:
        last = _TRAILING_PAREN_RE.sub("", parts[-1]).strip()
        canon = COUNTRY_ALIASES.get(last.lower()) or _ISO2_COUNTRY_PREFIX.get(last.upper())
        if canon:
            return {canon}
        # last part isn't a recognized country — fall through to the
        # normal pipeline below instead of silently finding nothing.

    # 0c. "Country, ST" 2-part shape where the FIRST part is a real,
    # complete country name and the SECOND is a real US/CA state or
    # province code — confirmed live: "Poland, OH" (a real Ohio town) was
    # resolving to {Poland, United States}, 2 countries, wrongly promoted
    # to Global. With no 3rd part to settle it (rule 0b needs 3+ parts),
    # a bare 2-letter code straight after a full country name is much
    # more likely a coincidental US/CA town name than a real "Country,
    # State" pairing — trust the code, drop the literal country match.
    if len(parts) == 2:
        first_canon = COUNTRY_ALIASES.get(parts[0].lower())
        # Match a code at the START of the second part rather than
        # requiring an exact match — confirmed live, some ATS templates
        # leave unrendered placeholder junk trailing after the real code
        # (e.g. "Poland, OH %LABEL_POSITION_TYPE_REMOTE_WITHIN% ..."),
        # which an exact-match check would silently fail to catch.
        second_code_m = re.match(r"^([A-Za-z]{2})\b", parts[1])
        second_code = second_code_m.group(1).upper() if second_code_m else None
        if first_canon and second_code in _US_STATES:
            return {"United States"}
        if first_canon and second_code in _CA_PROVINCES:
            return {"Canada"}

    working = seg

    # 1. iCIMS-style ISO2 prefix, e.g. "CA-SK-Saskatoon", "US-NY-Malta"
    def _mask_iso2(mm):
        canon = _ISO2_COUNTRY_PREFIX.get(mm.group(1).upper())
        if canon:
            found.add(canon)
        return " " * len(mm.group(0))

    working = _ISO2_PREFIX_RE.sub(_mask_iso2, working)

    # 1b. Known phrases that legitimately CONTAIN a registered country
    # alias as a stand-alone word but mean something else entirely —
    # confirmed live: "Sydney, New South Wales, Australia" (< 3 comma
    # parts, so rule 0b above didn't apply) was resolving to {Australia,
    # United Kingdom} because "Wales" is a whole word inside "New South
    # Wales" (an Australian state) and is also a registered UK alias.
    # Same issue for "New England" (the US northeast region) containing
    # "England". Mask these out before the general alias scan runs.
    def _mask_nsw(mm):
        found.add("Australia")
        return " " * len(mm.group(0))

    def _mask_new_england(mm):
        found.add("United States")
        return " " * len(mm.group(0))

    working = re.sub(r"\bNew South Wales\b", _mask_nsw, working, flags=re.I)
    working = re.sub(r"\bNew England\b", _mask_new_england, working, flags=re.I)

    # 2. Full US-state / CA-province names — mask so they can't also
    # trigger a country-name match (e.g. "Mexico" inside "New Mexico").
    def _mask_us_state(mm):
        found.add("United States")
        return " " * len(mm.group(0))

    def _mask_ca_province(mm):
        found.add("Canada")
        return " " * len(mm.group(0))

    working = _US_STATE_NAMES_RE.sub(_mask_us_state, working)
    working = _CA_PROVINCE_NAMES_RE.sub(_mask_ca_province, working)

    # 3. Long/full country names + safe long abbreviations
    for mm in _LONG_ALIAS_RE.finditer(working):
        canon = _LONG_ALIASES.get(mm.group(1).lower())
        if canon:
            found.add(canon)

    # 4. Short country codes — case-sensitive, must be uppercase
    for mm in _SHORT_ALIAS_RE.finditer(working):
        canon = _SHORT_ALIAS_ORIGCASE.get(mm.group(1))
        if canon:
            found.add(canon)

    # 5. ", XX" state/province code — case-sensitive, comma-anchored
    for mm in _STATE_CODE_RE.finditer(working):
        canon = _STATE_CODE_TO_COUNTRY.get(mm.group(1))
        if canon:
            found.add(canon)

    return found


def extract_countries(text: str) -> set[str]:
    """Return the set of canonical countries mentioned in `text`.

    Splits on ';' / '|' (the delimiters this project's ATS location
    strings use between multiple distinct locations) and classifies each
    segment independently — this is what lets the trailing-country-code
    rule (see `_TRAILING_CC_RE`) safely settle a segment's country from
    its OWN trailing code without a neighboring segment's text leaking in.
    """
    if not text:
        return set()

    found: set[str] = set()
    for seg in re.split(r"[;|]", text):
        if seg.strip():
            found |= _extract_from_segment(seg)
    return found


def countries_to_continents(countries: set[str]) -> set[str]:
    return {COUNTRY_CONTINENT[c] for c in countries if c in COUNTRY_CONTINENT}
