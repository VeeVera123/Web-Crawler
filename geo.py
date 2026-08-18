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
# Alias -> canonical country name
# ══════════════════════════════════════════════════════════
# Covers: full names, common abbreviations, ISO2/ISO3 where distinctive
# enough not to collide with ordinary English words. Also maps US states
# and Canadian provinces back to their country, since ATS location strings
# are very often "City, ST" / "City, Province" with no literal country
# word at all (e.g. ADP's "Miami, FL, US" or bare "Tampa, FL").

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

COUNTRY_ALIASES: dict[str, str] = {}


def _alias(alias: str, canonical: str) -> None:
    COUNTRY_ALIASES[alias.lower()] = canonical


for _country in COUNTRY_CONTINENT:
    _alias(_country, _country)

# Common alternate names / abbreviations
_alias("US", "United States")
_alias("USA", "United States")
_alias("U.S.", "United States")
_alias("U.S.A.", "United States")
_alias("United States of America", "United States")
_alias("UK", "United Kingdom")
_alias("U.K.", "United Kingdom")
_alias("Great Britain", "United Kingdom")
_alias("England", "United Kingdom")
_alias("Scotland", "United Kingdom")
_alias("Wales", "United Kingdom")
_alias("Northern Ireland", "United Kingdom")
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

# US state 2-letter codes -> United States (only as a whole-word token, so
# "IN" doesn't match inside ordinary text elsewhere — callers must use the
# word-boundary regex below, not a naive substring check)
for _abbr in _US_STATES:
    _alias(_abbr, "United States")

# Canadian province codes -> Canada
for _abbr in _CA_PROVINCES:
    if _abbr not in COUNTRY_ALIASES:  # avoid clobbering e.g. "NS" collisions
        _alias(_abbr, "Canada")

# Build one regex alternation, longest alias first so e.g. "United States
# of America" matches before the shorter "United States" would truncate it.
_ALIAS_LIST = sorted(COUNTRY_ALIASES.keys(), key=len, reverse=True)
_ALIAS_RE = re.compile(
    r"\b(" + "|".join(re.escape(a) for a in _ALIAS_LIST) + r")\b",
    re.I,
)

# 2-letter state/province codes are only trustworthy as country evidence
# when they appear in the classic "City, XX" pattern (comma then 2 letters)
# — bare "IN" or "OR" floating in prose text is far too likely to be the
# English word, not the state. Everything else in COUNTRY_ALIASES (country
# names, US/UK/UAE etc.) is safe to match anywhere via _ALIAS_RE above.
_STATE_CODE_RE = re.compile(
    r",\s*(" + "|".join(sorted(set(_US_STATES) | set(_CA_PROVINCES))) + r")\b",
    re.I,
)


def extract_countries(text: str) -> set[str]:
    """Return the set of canonical countries mentioned in `text`. Combines
    a direct alias match (country names, US/UK/UAE, etc. — safe anywhere
    in the text) with a stricter ", XX" state/province-code match (only
    trusted right after a comma, to avoid false positives like the word
    "IN" or "OR" in ordinary prose)."""
    if not text:
        return set()
    found = set()
    for m in _ALIAS_RE.finditer(text):
        canon = COUNTRY_ALIASES.get(m.group(1).lower())
        if canon:
            found.add(canon)
    for m in _STATE_CODE_RE.finditer(text):
        canon = COUNTRY_ALIASES.get(m.group(1).lower())
        if canon:
            found.add(canon)
    return found


def countries_to_continents(countries: set[str]) -> set[str]:
    return {COUNTRY_CONTINENT[c] for c in countries if c in COUNTRY_CONTINENT}
