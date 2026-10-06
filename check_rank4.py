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


# ── Eligibility families (the "silent form" rule) ───────────────────────────
NARROW_LOCS = ["United States", "Australia", "United Kingdom", "Canada", "Germany", "Singapore", "Ireland",
               "Netherlands", "Remote - US", "Sydney, Australia", "London, United Kingdom", "Toronto, Canada",
               "United States - Remote"]
BROAD_LOCS = ["LATAM", "Europe", "APAC", "North America", "Asia Pacific", "Americas", "DACH", "Nordics"]
CO = ["OpenLoop", "Tenable", "Mollie", "Acme"]

# verbatim from the three reported jobs
REAL_QUESTIONS = [
    "Will you now or in the future require visa sponsorship? This includes initiating, continuing or transferring "
    "your visa to OpenLoop, now or at any time in the future.",
    "Do you have the legal right to work in the country within which you are applying?",
    "Do you now, or will you in the future, require sponsorship?",
    "Do you require visa sponsorship or a visa transfer?",
]
Q_AUTH_BARE = REAL_QUESTIONS + [
    "Will you now or in the future require visa sponsorship?", "Will you require sponsorship for employment visa status?",
    "Do you require sponsorship to work for {Co}?", "Will you need employer sponsorship now or in the future?",
    "Are you legally authorized to work for any employer?", "Are you authorized to work?",
    "Are you legally authorized to work in the country in which this role is located?",
    "Are you eligible to work without sponsorship?", "Do you have unrestricted work authorization?",
    "What is your work authorization status?", "What is your current immigration status?",
    "Do you currently hold a valid work permit?", "Please confirm that you have the right to work.",
    "Are you a citizen or permanent resident?", "What is your citizenship?", "What is your nationality?",
    "Please indicate your visa type.", "Do you need a visa to work?", "Do you require immigration support?",
    "Are you currently authorized to work on a full-time basis without restriction?",
    "Are you legally eligible for employment?", "I confirm that I am eligible to work.",
    "Will you now or at any time in the future require the company to sponsor an employment visa?",
    "Are you able to provide proof of eligibility to work upon hire?",
    "Is your work authorization dependent on an employer (e.g., H-1B, OPT)?", "Are you a green card holder?",
    "Do you hold a valid passport?", "Do you currently reside in the country where this role is located?",
    "Are you currently living in the same country as the role?", "Can you work in the country where the position is based?",
    "Do you now or in the future require work authorization sponsorship?", "Is sponsorship required for you?",
    "Are you permitted to work in the location of this role?", "Do you have the right to work where this job is based?",
    "Have you got the legal right to work?", "Are you lawfully able to work?",
    "Will you require a visa to work for {Co}?", "Is your visa transferable to {Co}?",
    "Do you need a work permit?", "Are you a national of the country where this role is located?",
    "Do you have permanent residency?", "Please state your employment eligibility.",
]
Q_PRESENCE = [  # tied to a place whatever the scope
    "Are you willing to relocate for this role?", "Are you able to commute to the office?",
    "Are you comfortable working on-site?", "Are you able to work from our office three days a week?",
    "Do you hold an active security clearance?", "Would you be open to relocating?",
]
Q_NAMED = ["Are you authorized to work in {C}?", "Do you have the right to work in {C}?",
           "Will you require visa sponsorship to work in {C}?", "Are you a citizen or permanent resident of {C}?",
           "Do you currently reside in {C}?", "Do you hold a valid work permit for {C}?"]
NAMED_COUNTRIES = ["the United States", "Canada", "Germany", "Brazil", "India", "the UK"]
Q_MULTILINGUAL = [
    "Benötigen Sie eine Arbeitserlaubnis oder ein Visum?", "Sind Sie berechtigt, in Deutschland zu arbeiten?",
    "Besitzen Sie die deutsche Staatsangehörigkeit?", "Sind Sie bereit umzuziehen?",
    "Avez-vous le droit de travailler au Canada ?", "Avez-vous besoin d'un parrainage de visa ?",
    "Êtes-vous citoyen canadien ?", "Hai bisogno di un permesso di lavoro?", "Sei autorizzato a lavorare in Italia?",
    "Heeft u een werkvergunning nodig?", "Bent u gerechtigd om in Nederland te werken?",
    "Behöver du arbetstillstånd?", "Är du medborgare i Sverige?", "Trenger du arbeidstillatelse?",
    "Har du brug for arbejdstilladelse?", "Hast du eine Aufenthaltserlaubnis?",
]
Q_BENIGN = [
    "Why are you interested in this role?", "How did you hear about this job opportunity?",
    "If you were referred by one of our employees, please specify the name of the person who referred you.",
    "Describe your customer success experience.", "What are your salary expectations? (Yearly gross)",
    "Do you have a non-compete, non-disclosure or non-solicitation agreement?",
    "Have you ever previously worked for {Co}?", "Link to your LinkedIn profile", "What is your notice period?",
    "Which CRM tools have you used?", "Are you comfortable with a fully remote role?",
    "Do you have experience with SaaS onboarding?", "Please share your pronouns.", "Country", "Address Line 1",
    "City", "Postal Code/Zip Code", "Region (State/County/Province)", "Location", "Phone type",
    "Are you at least 18 years of age?", "Do you have a valid driver's license?",
    "Do you agree to {Co}'s Background and Reference Check Disclosure?", "Have you ever been terminated from a job?",
    "Are you open to a 6-month contract?", "Describe your experience working with US-based customers.",
    "Do you have experience managing enterprise accounts?", "What is your preferred start date?",
    "Are you comfortable using Zoom and Slack?", "Tell us about a time you reduced churn.",
]
JD_REQUIRE = [
    "Candidates must be legally authorized to work.", "Applicants must have unrestricted work authorization.",
    "We are unable to provide visa sponsorship for this role.", "Visa sponsorship is not available.",
    "This position does not offer sponsorship.", "You must be eligible to work without sponsorship.",
    "Employment is contingent upon proof of eligibility to work.", "Must be able to work lawfully.",
    "Only candidates with existing work authorization will be considered.", "Must reside in the country.",
    "Candidates must have the right to work.", "We cannot sponsor visas.", "Sponsorship is not offered for this position.",
    "Must be authorized to work in {C}.", "{C} residents only.", "Applicants must hold a valid work permit.",
    "Must be a citizen or permanent resident.", "Applicants must be eligible to work in the country of the role.",
]
JD_BENEFIT = ["We offer relocation assistance and visa support for the right candidate.",
              "We help employees obtain work permits when needed.", "We can provide visa sponsorship for exceptional candidates."]
JD_BENIGN = ["We are an equal opportunity employer.", "We offer a competitive salary and equity.",
             "Our team is spread across the US, UK and Australia.", "You will own a portfolio of enterprise accounts.",
             "Our customers include Visa and Mastercard.", "We value diversity of backgrounds and experience.",
             "You will report to the VP of Customer Success.", "Benefits include health insurance and paid time off."]


def rank4_job(loc, qs=(), jd="", title="Customer Success Manager"):
    desc = (jd or "We help teams ship software faster.") + "\n" + "\n".join(
        "Application Question: " + q for q in (list(qs) or ["How did you hear about this role?"]))
    return classifier.classify_rank4(dict(BASE, title=title, location=loc, description_snippet=desc))[0]


def fill(template, c=None, co=None):
    return template.replace("{C}", c or "the United States").replace("{Co}", co or "Acme")


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


    def expect_jobs(name, cases, want_admit):
        """cases: [(label, loc, qs, jd, title)]"""
        bad = []
        for label, loc, qs, jd, title in cases:
            got = rank4_job(loc, qs, jd, title)
            if (got is not None) != want_admit:
                bad.append((label, loc, got))
        totals[name] = (len(cases) - len(bad), len(cases))
        failures.extend((name, label, loc, got) for label, loc, got in bad)

    def q_cases(locs, templates, **kw):
        return [(fill(t, **kw)[:70], loc, [fill(t, **kw)], "", "Customer Success Manager") for loc in locs for t in templates]

    expect_jobs("REJECT narrow scope + eligibility question (bare)", q_cases(NARROW_LOCS, Q_AUTH_BARE), False)
    expect_jobs("REJECT narrow scope + non-English eligibility question", q_cases(NARROW_LOCS, Q_MULTILINGUAL), False)
    expect_jobs("REJECT relocation/commute/on-site/clearance (any scope)", q_cases(NARROW_LOCS + BROAD_LOCS, Q_PRESENCE), False)
    expect_jobs("REJECT named-place question (any scope)",
                [(fill(t, c)[:70], loc, [fill(t, c)], "", "Customer Success Manager")
                 for loc in NARROW_LOCS + BROAD_LOCS for t in Q_NAMED for c in NAMED_COUNTRIES], False)
    expect_jobs("ADMIT narrow scope + ordinary questions", q_cases(NARROW_LOCS, Q_BENIGN), True)
    expect_jobs("ADMIT broad region + bare eligibility question (documented)",
                q_cases(BROAD_LOCS, ["Will you now or in the future require sponsorship for a work visa?",
                                     "Do you require visa sponsorship or a visa transfer?",
                                     "Are you legally authorized to work?"]), True)
    expect_jobs("REJECT US/UK/AU location + 'Global' title + bare sponsorship question (function name, not scope)",
                [("Global/" + loc, loc, ["Will you now or in the future require visa sponsorship for employment? (e.g., H1B, Blue Card, etc.)"],
                  "", "Director of Sales, Global Account Management")
                 for loc in ("Remote in the United States", "Remote in Australia", "London, United Kingdom", "Canada - Remote")], False)
    expect_jobs("ADMIT 'Remote in <country>' (4a) with ordinary questions",
                [("RemoteIn/" + c, f"Remote in {c}", [], "", "Customer Success Manager")
                 for c in ("the United States", "Canada", "Australia", "Germany", "the United Kingdom")], True)
    expect_jobs("ADMIT 4b broad (London + EMEA title) + bare sponsorship question",
                [("EMEA/London", "London", ["Will you now or in the future require sponsorship for a work visa?"], "",
                  "Customer Success Manager, EMEA")], True)
    expect_jobs("REJECT 4b narrow (London + UK-only signal) + bare sponsorship question",
                [("UK/London", "London", [q], "We are hiring customer success managers in the United Kingdom.",
                  "Customer Success Manager") for q in Q_AUTH_BARE[:12]], False)
    expect_jobs("REJECT narrow scope + JD requires eligibility/sponsorship/residency",
                [(fill(j, c)[:70], loc, [], fill(j, c), "Customer Success Manager")
                 for loc in NARROW_LOCS for j in JD_REQUIRE for c in ("the United States", "Canada")], False)
    expect_jobs("ADMIT narrow scope + JD only offers help / ordinary boilerplate",
                [(j[:70], loc, [], j, "Customer Success Manager") for loc in NARROW_LOCS for j in JD_BENEFIT + JD_BENIGN], True)

    # grammar fuzz: framing x predicate x tail, so wordings nobody listed are still exercised
    import random as _r
    rnd = _r.Random(11)
    frames = ["Do you", "Will you", "Would you", "Can you", "Are you", "Have you", "Please confirm that you",
              "Please indicate whether you", "I confirm that I", "Does the candidate"]
    preds = ["require sponsorship", "need a visa", "hold a work permit", "have the right to work",
             "are eligible to work", "are authorized to work", "are legally able to work", "are permitted to work",
             "need employer sponsorship", "require visa sponsorship", "have a valid work visa",
             "have work authorization", "are a citizen", "are a permanent resident", "have unrestricted work rights",
             "are legally entitled to work", "need immigration support", "require a work permit",
             "have the legal right to be employed", "are lawfully allowed to work"]
    tails = ["", "?", " now or in the future?", " without sponsorship?", " for {Co}?", " at any time in the future?",
             " (e.g., H-1B, OPT)?", ". This includes transferring your visa to {Co}.", " in order to be hired?"]
    gcases = []
    for _ in range(1500):
        q = rnd.choice(frames) + " " + rnd.choice(preds) + rnd.choice(tails).replace("{Co}", rnd.choice(CO))
        gcases.append((q[:70], rnd.choice(NARROW_LOCS), [q], "", "Customer Success Manager"))
    expect_jobs("FUZZ 1500 grammar-built eligibility questions at narrow scope", gcases, False)

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
