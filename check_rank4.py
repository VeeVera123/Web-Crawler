"""Offline stress test for Rank 4 (classifier.classify_rank4).

Policy under test (explicit user policy):
  * The LOCATION FIELD must itself be an allowed place: one of the curated
    countries, or a region NAME (APAC, LATAM, Europe...). Member countries and
    cities of a region (Thailand/Bangkok for APAC, Brazil for LATAM) are NOT
    allowed just because the region is, and neither are states/provinces.
  * "City, <allowed country>" is allowed; a bare city is allowed only when it is
    a known city of an allowed country AND the title/JD adds a regional or
    global signal (title "CSM, EMEA" + location "London").
  * Anything the location names that is not allowed rejects the job, whatever
    the title/JD say.

    python check_rank4.py            # summary, exit 1 on any failure
    python check_rank4.py -v         # also list every failure
"""
import os
import sys

for _k in ("SUPABASE_URL", "SUPABASE_KEY", "CEREBRAS_API_KEY", "GROQ_API_KEY", "GROQ_API_KEY_C",
           "OPENAI_API_KEY", "NVIDIA_API_KEY", "ANTHROPIC_API_KEY"):
    os.environ.setdefault(_k, "x")

import classifier  # noqa: E402
import geo  # noqa: E402

ALLOWED_COUNTRIES = ["United States", "USA", "U.S.", "US", "United Kingdom", "UK", "Canada", "Australia", "Germany",
                     "Ireland", "Singapore", "Luxembourg", "Norway", "Switzerland", "Denmark", "Netherlands",
                     "Iceland", "Sweden", "Italy", "Deutschland", "Great Britain", "England", "Scotland", "Wales"]
REGION_NAMES = ["APAC", "LATAM", "Europe", "North America", "South America", "Central America", "Asia",
                "Asia Pacific", "Oceania", "Middle East", "MENA", "Nordics", "Benelux", "DACH", "ANZ",
                "Western Europe", "Eastern Europe", "Southeast Asia", "European Union", "EU", "Caribbean",
                "Americas", "AMER", "CEE", "Latin America"]
# (city, allowed country) pairs; the city need not be one we know.
CITY_COUNTRY = [("Sydney", "Australia"), ("London", "United Kingdom"), ("Toronto", "Canada"), ("Berlin", "Germany"),
                ("Dublin", "Ireland"), ("Zurich", "Switzerland"), ("Amsterdam", "Netherlands"),
                ("Stockholm", "Sweden"), ("Milan", "Italy"), ("Oslo", "Norway"), ("Copenhagen", "Denmark"),
                ("Reykjavik", "Iceland"), ("Austin", "United States"), ("Reading", "United Kingdom"),
                ("Bendigo", "Australia"), ("Leiden", "Netherlands"), ("Manchester", "UK"), ("Munich", "Germany")]
# Bare cities of allowed countries (non-US): admitted only WITH a regional/global signal.
KNOWN_ALLOWED_CITIES = ["London", "Manchester", "Edinburgh", "Sydney", "Melbourne", "Toronto", "Vancouver",
                        "Berlin", "Munich", "Hamburg", "Dublin", "Zurich", "Geneva", "Amsterdam", "Stockholm",
                        "Milan", "Rome", "Oslo", "Copenhagen", "Reykjavik", "Montreal", "Brisbane", "Quebec City", "York",
                        "Perth", "The Hague", "Den Haag", "Zürich", "München", "Köln", "Göteborg"]

EXTRA_FOREIGN_COUNTRIES = [
    "Brasil", "México", "España", "Türkiye", "Polska", "Österreich", "Ελλάδα", "日本", "中国", "한국", "ประเทศไทย",
    "Россия", "भारत", "Côte d'Ivoire", "Czechia", "Taiwan", "Hong Kong", "Macau", "Puerto Rico",
    "Greenland", "Thailand", "Vietnam", "Indonesia", "Malaysia", "Philippines", "Pakistan", "Bangladesh",
    "Sri Lanka", "Saudi Arabia", "Qatar", "Kuwait", "Argentina", "Colombia", "Chile", "Peru", "Uruguay",
    "Costa Rica", "Panama", "Dominican Republic", "Israel", "Turkey", "Ukraine", "Romania", "Bulgaria", "Greece",
    "Portugal", "Belgium", "Austria", "Finland", "Poland", "Hungary", "France", "Spain", "New Zealand", "Japan",
    "China", "India", "South Korea", "South Africa", "Nigeria", "Kenya", "Ghana", "Egypt", "Morocco", "Brazil",
    "Mexico", "UAE", "United Arab Emirates", "Estonia", "Latvia", "Lithuania", "Croatia", "Serbia", "Cyprus", "Malta",
]
FOREIGN_CITIES = ("Bangkok,Chiang Mai,Phuket,Hanoi,Ho Chi Minh City,Jakarta,Kuala Lumpur,Manila,Cebu,Mumbai,Delhi,"
                  "New Delhi,Bengaluru,Bangalore,Hyderabad,Chennai,Pune,Gurgaon,Noida,Kolkata,Karachi,Lahore,Dhaka,"
                  "Colombo,Shanghai,Beijing,Shenzhen,Guangzhou,Hong Kong,Taipei,Seoul,Busan,Tokyo,Osaka,Dubai,Abu Dhabi,"
                  "Riyadh,Doha,Tel Aviv,Istanbul,Ankara,Cairo,Casablanca,Lagos,Abuja,Nairobi,Accra,Johannesburg,"
                  "Cape Town,Durban,Addis Ababa,Kampala,Dar es Salaam,Kigali,Mexico City,Guadalajara,Monterrey,Bogota,"
                  "Medellin,Lima,Santiago,Buenos Aires,Sao Paulo,São Paulo,Rio de Janeiro,Brasilia,Belo Horizonte,"
                  "Curitiba,Porto Alegre,Montevideo,San Jose,Panama City,Paris,Lyon,Madrid,Barcelona,Lisbon,Porto,"
                  "Brussels,Vienna,Warsaw,Krakow,Prague,Budapest,Bucharest,Sofia,Athens,Helsinki,Tallinn,Riga,Vilnius,"
                  "Kyiv,Moscow,Belgrade,Zagreb,Auckland,Wellington,Christchurch,Benin City,Port Harcourt,Kumasi,Lusaka,"
                  "Harare,Maputo,Luanda,Dakar,Abidjan,Tunis,Algiers,Amman,Beirut,Kuwait City,Manama,Muscat,Karachi,"
                  "Islamabad,Kathmandu,Yangon,Phnom Penh,Vientiane,Ulaanbaatar,Almaty,Tashkent,Baku,Tbilisi,Yerevan,"
                  "Minsk,Chisinau,Sarajevo,Skopje,Tirana,Valletta,Nicosia,Cluj,Wroclaw,Gdansk,Brno,Bratislava,"
                  "Ljubljana,Salvador,Recife,Fortaleza,Campinas,Florianopolis,Quito,Guayaquil,La Paz,Asuncion,"
                  "Caracas,San Salvador,Guatemala City,Tegucigalpa,Managua,Havana,Santo Domingo,Kingston,Port of Spain,"
                  "Nassau,San Juan,Valencia,Seville,Bilbao,Marseille,Toulouse,Nice,Lille,Bordeaux,Nantes,Ghent,Antwerp,"
                  "Nice").split(",")
STATES_AND_ADDRESSES = ["New York City", "Kansas City", "Newcastle", "Cambridge", "Hamilton", "Birmingham, AL",
                        "Portland", "Salem", "Richmond", "Jersey City", "Quebec City, QC","Texas", "California", "New York", "Florida", "Remote, New York", "Austin, TX", "Atlanta, GA",
                        "Denver, CO; New York City, NY; San Francisco, CA", "Boston, Massachusetts; Chicago, Illinois",
                        "AT002 Industriestraße 2, 5303 Thalgau", "Remote, New Jersey", "Washington", "Georgia",
                        "Ontario", "Quebec", "Bavaria", "Victoria", "Queensland", "Catalonia", "Sao Paulo State"]

BASE = {"title": "Customer Success Manager", "country": "", "workplace_type": "", "role_category": "CS",
        "source_ats": "Greenhouse"}
Q = "Application Question: How did you hear about this role?"
CONTEXTS = {
    "no signal": ("Customer Success Manager", "We help teams ship software faster."),
    "title APAC": ("Customer Success Manager - APAC", "We help teams ship software faster."),
    "title EMEA": ("Customer Success Manager, EMEA", "We help teams ship software faster."),
    "title Global": ("Global Customer Success Manager", "We help teams ship software faster."),
    "JD hiring APAC/EMEA": ("Customer Success Manager", "We are hiring candidates across APAC and EMEA."),
    "JD hiring LATAM": ("Customer Success Manager", "We are hiring customer success talent across LATAM."),
    "JD worldwide": ("Customer Success Manager", "This role is open to candidates worldwide."),
}
SIGNAL_CONTEXTS = [c for c in CONTEXTS if c != "no signal"]


def rank4(loc, ctx):
    title, jd = CONTEXTS[ctx]
    return classifier.classify_rank4(dict(BASE, title=title, location=loc, description_snippet=jd + "\n" + Q))[0]


def fuzz(n=8000, seed=7):
    """Random combinations: a foreign place anywhere => never admitted; only allowed
    places => admitted whenever the context carries a signal."""
    import random
    rnd = random.Random(seed)
    foreign_pool = sorted({c for c in list(geo.COUNTRY_CONTINENT) + EXTRA_FOREIGN_COUNTRIES
                           if c.lower() not in {a.lower() for a in ALLOWED_COUNTRIES}}
                          | {c.strip() for c in FOREIGN_CITIES if c.strip()} | set(STATES_AND_ADDRESSES[:12]))
    allowed_pool = ALLOWED_COUNTRIES + REGION_NAMES + KNOWN_ALLOWED_CITIES
    seps = [", ", " / ", " - ", " | ", "; ", " or ", " and ", ", Remote, ", " (", ") "]
    bad = []
    for _ in range(n):
        k = rnd.randint(1, 3)
        has_foreign = rnd.random() < 0.5
        parts = [rnd.choice(allowed_pool) for _ in range(k)]
        if has_foreign:
            parts.insert(rnd.randint(0, len(parts)), rnd.choice(foreign_pool))
        loc = parts[0]
        for p in parts[1:]:
            sep = rnd.choice(seps)
            loc += sep + p + (")" if sep == " (" else "")
        ctx = rnd.choice(SIGNAL_CONTEXTS)
        got = rank4(loc, ctx)
        if has_foreign and got is not None:
            bad.append(("foreign place admitted", loc, ctx, got))
        if not has_foreign and got is None:
            bad.append(("allowed-only rejected", loc, ctx, got))
    return n, bad


def main() -> int:
    verbose = "-v" in sys.argv
    failures = []
    totals = {}

    def expect(name, cases, want_admit):
        """cases: [(location, context)]; want_admit True/False."""
        bad = [(loc, ctx, rank4(loc, ctx)) for loc, ctx in cases
               if (rank4(loc, ctx) is not None) != want_admit]
        totals[name] = (len(cases) - len(bad), len(cases))
        failures.extend((name, loc, ctx, got) for loc, ctx, got in bad)

    every_ctx = list(CONTEXTS)
    allowed = []
    for c in ALLOWED_COUNTRIES:
        allowed += [(c, x) for x in every_ctx[:3]] + [(f"{c} Remote", "no signal"), (f"Remote - {c}", "no signal")]
    expect("ADMIT bare allowed country", allowed, True)
    expect("ADMIT region name", [(r, x) for r in REGION_NAMES for x in every_ctx[:3]], True)
    expect("ADMIT 'City, AllowedCountry'", [(f"{a}, {b}", x) for a, b in CITY_COUNTRY for x in every_ctx[:3]], True)
    expect("ADMIT two allowed countries", [("United States or Canada", "no signal"), ("UK, Ireland", "no signal"),
                                           ("Canada, Australia", "no signal")], True)
    expect("ADMIT known allowed city + signal", [(c, x) for c in KNOWN_ALLOWED_CITIES for x in SIGNAL_CONTEXTS], True)
    expect("REJECT known allowed city, no signal", [(c, "no signal") for c in KNOWN_ALLOWED_CITIES], False)

    foreign = geo.COUNTRY_CONTINENT.keys()
    allowed_l = {a.lower() for a in ALLOWED_COUNTRIES}
    countries = sorted({c for c in list(foreign) + EXTRA_FOREIGN_COUNTRIES
                        if c.lower() not in allowed_l})
    cases = []
    for c in countries:
        for ctx in every_ctx:
            cases += [(c, ctx), (f"Remote - {c}", ctx)]
        cases += [(f"{c} Remote", "no signal"), (f"{c}, Remote", "title APAC")]
    expect("REJECT foreign country (bare / Remote)", cases, False)
    cities = [c.strip() for c in FOREIGN_CITIES if c.strip()]
    expect("REJECT foreign city (bare, any signal)", [(c, x) for c in cities for x in every_ctx], False)
    expect("REJECT foreign city + its country", [(f"{c}, {k}", x) for c in cities[:60] for k in
                                                   ("Thailand", "Brazil", "India", "France", "Nigeria", "Mexico", "Japan")[:3]
                                                   for x in every_ctx[:4]], False)
    expect("REJECT state / province / address", [(s, x) for s in STATES_AND_ADDRESSES for x in every_ctx], False)
    expect("REJECT mixed list with a foreign place",
           [(f"{a}, {b}", x) for a in ("Singapore", "London", "Remote", "APAC", "Sydney", "Germany")
            for b in ("Bangkok", "Brasil", "Sao Paulo", "Mumbai", "Lagos", "Paris", "Manila") for x in every_ctx[:4]], False)
    workplace = ["Hybrid", "Hybrid - London", "London (Hybrid)", "London, United Kingdom (Hybrid)", "Germany - Hybrid",
                 "Sydney Office", "Berlin (Office)", "Office-based - Berlin", "On-site - London", "London - Onsite",
                 "In-office London", "United Kingdom (On-site)", "Australia - In Person", "Singapore Office",
                 "United States (Hybrid)", "Canada, On-site", "APAC (Hybrid)", "Europe - Office"]
    expect("REJECT workplace word in location (hybrid/office/on-site)",
           [(w, x) for w in workplace for x in every_ctx], False)
    expect("ADMIT remote alongside a place", [(f"Remote, {c}", x) for c in ("London", "Sydney", "Berlin")
                                              for x in SIGNAL_CONTEXTS] + [("London (Remote)", "title EMEA"),
                                                                           ("Remote - Australia", "no signal")], True)
    expect("REJECT region + foreign city", [(f"{r} ({c})", x) for r in ("APAC", "LATAM", "Europe", "Asia")
                                             for c in ("Bangkok", "Sao Paulo", "Paris", "Mumbai") for x in every_ctx[:4]], False)

    n, fz = fuzz()
    totals[f"FUZZ {n} random allowed/foreign combinations"] = (n - len(fz), n)
    failures.extend((kind, loc, ctx, got) for kind, loc, ctx, got in fz)

    ok = sum(a for a, _ in totals.values())
    tot = sum(b for _, b in totals.values())
    print(f"Rank 4 stress test: {ok}/{tot} cases as expected")
    for name, (a, b) in totals.items():
        print(f"  {'ok  ' if a == b else 'FAIL'} {name}: {a}/{b}")
    if failures:
        by = {}
        for name, loc, ctx, got in failures:
            by.setdefault(name, []).append((loc, ctx, got))
        for name, items in by.items():
            shown = items if verbose else items[:6]
            for loc, ctx, got in shown:
                print(f"    {name:42} loc={loc!r:36} ctx={ctx!r:22} -> {got}")
            if len(items) > len(shown):
                print(f"    ... {len(items) - len(shown)} more in this group (use -v)")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
