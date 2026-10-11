"""Classifier regression checks: false rejects found in real audits, the vocabulary pass, and the ~2,900-case generated corpus.

Each section below was its own script; they are one file now so a check lives with the thing it checks.
Run: python check_classifier.py
"""
import sys as _sys

_FAILED = 0

# ====================================================================================================
# SECTION: check_classifier_fp - Regression checks for classifier FALSE REJECTS found by auditing employer-declared-worldwide jobs (Himalayas).
# ====================================================================================================
import os, sys
for _k in ("SUPABASE_URL", "SUPABASE_KEY", "CEREBRAS_API_KEY", "GROQ_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
    os.environ.setdefault(_k, "x")
import logging
logging.disable(logging.CRITICAL)
import classifier as C

fails, n = [], 0
def check(c, m):
    global n
    n += 1
    if not c: fails.append(m)

def res(desc, loc="Remote - Worldwide", title="Customer Success Manager"):
    return C._keyword_classify_location_detail({"title": title, "location": loc, "country": "", "description_snippet": desc,
                                                "workplace_type": "Remote", "source_ats": "Himalayas", "role_category": "CS"})[0]

BASE = "You will own onboarding and renewals for our customers and partner with sales. " * 3
# 1. a pin emoji used as a bullet before OTHER labelled facts is not a place
check(res(BASE + " Location Symbol: Team: Sales") == "match", "pin + 'Team: Sales' is not a place")
check(res(BASE + " Location Symbol: Department: Customer Success") == "match", "pin + 'Department:' is not a place")
check(res(BASE + " Location Symbol: Lagos, Nigeria") == "no_match", "pin + a real city is still a named place")
check(res(BASE + " Location Symbol: Location: Berlin, Germany") == "no_match", "pin + 'Location: Berlin' is still a named place")
# 2. pay-band boilerplate is not a hiring restriction
PAY = BASE + " Compensation for US-based employees: the salary range listed above is for US-based employees. Our benefits include PTO."
check(res(PAY) == "match", "pay band sentence about US-based employees is not a restriction")
check(res(BASE + " The base salary range for candidates based in London is GBP 60,000 - 70,000 per year.") == "match", "salary for London-based candidates is not a restriction")
check(res(BASE + " Candidates must be US-based. Salary range $100,000 - $120,000.") == "no_match", "a real US-only requirement next to a salary still rejects")
check(res(BASE + " You must live in Germany to be eligible; salary is EUR 70,000 per year.") == "no_match", "must live in Germany + salary still rejects")
check(res(BASE + " This role is only open to US-based employees. The salary is $90,000 per year.") == "no_match", "'only open to US-based employees' still rejects")
check(len(C._split_into_sentences("Great team. The salary range is $90,000 per year for US-based employees. Apply now.")) == 2, "pay sentence dropped from sentence scan")
# 3. the plainest US-only phrasing must reject (was a false ACCEPT)
for t in ("Candidates must be US-based.", "You need to be UK based to apply.", "Applicants have to be Canada-based.", "You must be a US-based employee."):
    check(res(BASE + t) == "no_match", f"'{t}' rejects")
check(res(BASE + "This role is not restricted to US-based candidates; we hire globally.") == "match", "'not restricted to US-based' still passes")
check(res(BASE + "Our team must be remote-based and async.") == "match", "'must be remote-based' is not a place")

# 4. more false-positive classes found in the 1,298-job audit
for t, why in (("We not only function and support a fully remote setting but also offer the possibility to work from our office in Vienna.", "optional office"),
               ("These roles do not require a security clearance.", "no clearance required"),
               ("In certain cases, we may be able to provide visa sponsorship to the US or UK and help candidates relocate.", "sponsorship offer"),
               ("You can work in your own time zone.", "own time zone"),
               ("Prior experience managing teams in a remote/hybrid work environment.", "hybrid experience"),
               ("Ability to travel occasionally for in-person working sessions.", "in-person sessions"),
               ("We are a successful law firm based in California. Our platform is based in Amsterdam.", "company HQ"),
               ("For international employees we partner with an Employer of Record, Deel. B2B only - work through your own legal entity.", "EOR positive"),
               ("You are responsible not only for payroll but also for onboarding.", "not only payroll"),
               ("Location Symbol: About this role Location Symbol: Over 200,000 users worldwide.", "pin as bullet")):
    check(res(BASE + t) == "match", f"stays a match: {why}")
for t, why in (("This role requires candidates to be based in the APAC region.", "APAC only"),
               ("We have no legal entity in your country so we can only hire from the US.", "no entity"),
               ("You must work from our Berlin office three days a week.", "office days")):
    check(res(BASE + t) == "no_match", f"still rejects: {why}")

# 5. placeholder locations (v14) take the blank-location path, real places do not
for loc in ("Various", "Unknown", "Other", "Not applicable", "Location TBD", "To be confirmed", "Flexible", "TBC", "Varies", "Several locations", "N/A"):
    check(res("", loc=loc) == "unsure", f"placeholder {loc!r} is blank-like")
for loc in ("Berlin", "Other, Germany", "Open Space, Berlin", "Lagos, Nigeria"):
    check(res("", loc=loc) == "no_match" or C.PLACEHOLDER_LOC_RE.match(loc) is None, f"{loc!r} is not a placeholder")
check(C.PLACEHOLDER_LOC_RE.match("Other, Germany") is None and C.PLACEHOLDER_LOC_RE.match("Open Space, Berlin") is None, "placeholder regex only matches the whole value")

# 6. global-hiring language (v15)
for loc in ("🌍 Remote", "🌎 Worldwide", "Virtual - Worldwide", "Telecommute - Worldwide", "100% Remote - Worldwide", "Anywhere on Earth", "Remote - Open to all locations",
            "Remote - Not location specific", "Remote - World", "Home based - worldwide", "Remote – Everywhere", "Remote (Worldwide)"):
    check(res("", loc=loc) == "match", f"global location {loc!r}")
for loc in ("Remote - Germany", "🌍 Berlin", "Remote - Rest of World", "Virtual - Berlin"):
    check(res("", loc=loc) != "match", f"not global: {loc!r}")
for t in ("The position is not location-bound.", "We don't care where you work from.", "Location doesn't matter to us.", "We hire across the globe.", "You choose where you work.",
          "This role is location agnostic.", "Location: Wherever you are"):
    check(res(BASE + t, loc="Remote") == "match", f"global sentence {t!r}")
check(res(BASE + "Candidates must be US-based. Location doesn't matter within the US.", loc="Remote") == "no_match", "location-doesn't-matter inside a US-only role still rejects")

# 7. citizenship / residency "only" wording is a hard restriction even next to a global claim (v15), and a NEGATED requirement is not
GL = "We hire globally. "
for t in ("This role requires US residency.", "US residency is required.", "Must have US residency.", "This role is open to UK residents.", "Canadian citizens only.",
          "U.S. citizenship or permanent residency only.", "Limited to residents of Brazil.", "Only applicants within the US will be considered.",
          "Our team is US-based and so is this role.", "Limited to candidates in North America.", "We do not require a degree, but US citizenship is required.",
          "No degree needed, but US citizenship is required."):
    check(res(BASE + GL + t, loc="Remote") == "no_match", f"restriction next to a global claim rejects: {t!r}")
for t in ("US citizenship is not required.", "No US citizenship required.", "We do not require US citizenship.", "You do not need to be a US citizen.",
          "Authorization to work in the US is not required.", "We welcome candidates regardless of citizenship.", "No US residency required.",
          "We can sponsor US residency for the right candidate.", "We are open to candidates in EMEA or North America."):
    check(res(BASE + GL + t, loc="Remote") == "match", f"negated / offered / multi-region statement is not a restriction: {t!r}")

print(f"classifier false-reject checks: {n - len(fails)}/{n} passed")
for m in fails: print("FAIL", m)
_FAILED += len(fails)

# ====================================================================================================
# SECTION: check_classifier_vocab - Regression corpus for the 2026-10 classifier vocabulary pass: global-hiring wording that must NOT be rejected, and restriction
# ====================================================================================================
import os, sys
for _k in ("SUPABASE_URL", "SUPABASE_KEY", "CEREBRAS_API_KEY", "GROQ_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
    os.environ.setdefault(_k, "x")
import logging
logging.disable(logging.CRITICAL)
import classifier as C

BASE = "You will own onboarding, adoption and renewals for our customers and partner closely with sales and product. " * 3


def run(loc, desc, wt="Remote"):
    return C._keyword_classify_location_detail({"title": "Customer Success Manager", "location": loc, "country": "", "description_snippet": desc,
                                                "workplace_type": wt, "source_ats": "X", "role_category": "CS"})[0]


LOC_KEEP = [
    'Remote - Worldwide',
    'Worldwide',
    'Anywhere',
    'Remote (Global)',
    'Global',
    'Remote, Anywhere',
    'Work from anywhere',
    'Remote - Anywhere in the world',
    'Fully Remote - Global',
    'Remote | Worldwide',
    'Global Remote',
    'Remote (Worldwide)',
    'International (Remote)',
    'Distributed',
    'Remote – Any location',
    'Remote - All countries',
    'Remote, Earth',
    'Remote - Global (any timezone)',
    '🌍 Remote',
    '🌎 Anywhere',
    'Remote, EMEA',
    'EMEA',
    'Remote - Africa',
    'Africa',
    'Remote (EMEA)',
    'Europe/Middle East/Africa',
    'Remote Africa and Europe',
    'Remote, Worldwide (Any Timezone)',
    'Anywhere (Remote)',
    'Location independent',
    'Remote - Multiple Countries',
    'Remote - International',
    'Remote (Any Country)',
    '100% Remote - Worldwide',
    'Telecommute - Worldwide',
    'Virtual - Global',
    'Remote - Global (EOR)',
    'Remote – Worldwide (Contractor)',
    'Remote / Anywhere',
    'MEA',
    'Pan-Africa',
    'Pan-African',
    'Sub-Saharan Africa',
    'East Africa',
    'West Africa',
    'Southern Africa',
    'North Africa',
    'Northern Africa',
    'Central Africa',
    'Remote - Africa & Middle East',
    'Remote, Africa/Europe',
    'EMEA Remote',
    'Remote – EMEA',
    'Remote (EMEA/APAC)',
    'AMER, EMEA',
    'Remote - Nigeria, Ghana, Kenya',
    'Nigeria, Kenya, South Africa',
    'Remote - Anglophone Africa',
    'Africa (Remote)',
    'Remote Africa',
    'Remote - Middle East & Africa',
    'Remote - EMEA & APAC',
    'EMEA/LATAM',
    'Remote - Americas, EMEA',
    'Remote – Africa, Europe and Middle East',
    'Africa and the Middle East',
    'Remote: Anywhere',
    'Remote Anywhere',
    'Remote (Anywhere)',
    'Anywhere, Remote',
    'Work From Home - Worldwide',
    'WFH Global',
    'Remote-Global',
    'Remote_Worldwide',
    'Remote worldwide',
    'remote - worldwide',
    'GLOBAL',
    'WORLDWIDE',
    'Remote | Global',
    'International',
    'Globally',
    'Remote — Global',
    'Remote – International',
    'Remote (International)',
    'Télétravail - Monde entier',
    'Remoto - Todo el mundo',
    'Weltweit remote',
    'Remoto (Global)',
    'Remoto - Mundo',
    'Remote Worldwide (Contractor/EOR)',
    'Worldwide (Remote-first)',
    'Remote - Global Talent',
    'Open to remote worldwide',
    'Remote, Planet Earth',
    'Anywhere in the World',
    'Anywhere in the world (remote)',
    'Remote — anywhere in the world',
    'Remote – Worldwide (excluding US)',
    'Global (ex-US)',
]
LOC_REJECT = [
    'Remote - US',
    'Remote (United States)',
    'United States',
    'Remote, USA',
    'Remote - Canada',
    'London, United Kingdom',
    'Remote - UK',
    'Remote, Germany',
    'Berlin',
    'New York, NY',
    'San Francisco, CA',
    'Remote - California',
    'Remote (Texas)',
    'Austin, TX (Hybrid)',
    'Remote - India',
    'Bangalore, India',
    'Remote, Australia',
    'Sydney',
    'Toronto, ON',
    'Remote - France',
    'Paris',
    'Remote – Brazil',
    'São Paulo',
    'Remote, Mexico',
    'Singapore',
    'Tokyo, Japan',
    'Remote - LATAM',
    'Remote - APAC',
    'Remote - North America',
    'Remote (Americas)',
    'Remote - US/Canada',
    'Remote - Eastern Time',
    'Remote, EST',
    'Remote - US East Coast',
    'Remote - PST',
    'Remote (US only)',
    'Remote - Colorado',
    'Remote, Florida',
    'Dublin, Ireland',
    'Remote - Ireland',
    'Amsterdam',
    'Remote - Netherlands',
    'Remote - Spain',
    'Madrid',
    'Remote - Poland',
    'Warsaw',
    'Remote - Philippines',
    'Manila',
    'Remote - Argentina',
]
DESC_KEEP = [
    'We hire in over 50 countries and you can work from anywhere in the world.',
    'This position is open to candidates worldwide with no location restrictions.',
    "We are a remote-first company; you can be based anywhere as long as you overlap with our team's working hours.",
    'Work from anywhere. We have no offices and employees in 40 countries.',
    'We are location independent and hire globally through our employer of record partner.',
    'You can live anywhere in the world that has reliable internet.',
    'This role is fully remote and open to applicants in any country or timezone.',
    'Our headquarters are in San Francisco, but the team is distributed across the globe and this role can be done from anywhere.',
    'US citizenship is not required for this role.',
    'We do not require you to be located in any particular country.',
    'Candidates from all over the world are welcome to apply.',
    "We welcome applications from anywhere; we'll support you with an EOR or contractor agreement.",
    'Remote across EMEA with occasional team offsites a couple of times a year.',
    'Our team is 100% remote and spans 25 time zones.',
    'Fully distributed company. You choose where you work.',
    'Based anywhere in Africa or Europe, working a European time zone.',
    'Open to anyone, anywhere. We are timezone flexible.',
    'Travel to our annual company retreat is expected once a year.',
    'This is a global role; location is not a factor in hiring decisions.',
    'We hire talent wherever it lives. Fully remote, async-first.',
    'Applicants from any country can apply; relocation is not required.',
    'No visa sponsorship is necessary because you work remotely from your home country.',
    'Remote-first. Pick a coworking space or work from home, anywhere you like.',
    'Our company is registered in the UK but we hire remote employees across the world.',
    'Eligible candidates can be located in any country where we operate or via our EOR in 100+ countries.',
    'Fully remote position – work from your home country, we hire in Africa, Europe and the Middle East.',
    'Trabajo remoto desde cualquier lugar del mundo.',
    'Poste en télétravail complet, ouvert aux candidats du monde entier.',
    'Vollständig remote, Sie können von überall auf der Welt arbeiten.',
    'Trabalho 100% remoto, de qualquer lugar do mundo.',
    'We offer visa sponsorship for relocating candidates, but this particular role can be done remotely from anywhere.',
    'No relocation is required.',
    'We do not discriminate on the basis of nationality or citizenship status.',
    'You do not need to be a US citizen to apply.',
    'There is no requirement to be located in the United States.',
    "Our customers are mostly based in the US and Canada, so you'll support North American accounts, working from wherever you are.",
    "You'll work with customers across the UK, Germany and France from your home office anywhere in the world.",
    'Our offices are in London and New York, but this role is remote and open globally.',
    'Occasional travel (up to 10%) may be required.',
    'You will need a reliable internet connection and a quiet workspace at home.',
    'Hiring in: Africa, Europe, Middle East. Fully remote.',
    'We are able to employ people in most countries through Deel.',
    'The team is spread across Lagos, Nairobi, Cape Town, London and Berlin and works fully remotely.',
    'Remote role. Candidates in any location are welcome, subject to our ability to employ in your country.',
    'This is a fully remote role and can be performed from any country in which we have a legal entity or via an EOR.',
    'We are open to remote candidates in EMEA time zones.',
    'Looking for someone in EMEA to join our remote team.',
    'Work remotely from any African country.',
    'Contractor position: you can be based in any country.',
    'Remote (international candidates welcome).',
    'International applicants encouraged. English fluency required.',
    'You must be fluent in English and French.',
    'Must have a strong understanding of US tax law.',
    'Experience selling into the US market is a plus.',
    'You will be working US business hours (overlap with 4 hours of EST) from anywhere.',
    'Willing to work flexible hours to support customers in different time zones.',
    'Residents of any country may apply.',
    'Our compensation is location-agnostic: the same pay wherever you live.',
    'We pay a global salary band regardless of where you live.',
    'Citizens of any nationality may apply.',
    'Right to work: we will help you get set up as a contractor in your country of residence.',
    'Not limited to a specific country or region.',
    'Candidates from Africa are strongly encouraged to apply.',
    'We have no preference on location, as long as you can work during our core hours (10am-2pm UTC).',
    'Based anywhere between UTC-1 and UTC+4.',
    'We support customers who run hybrid and on-site operations.',
    'Our product helps teams manage in-person events and hybrid meetings.',
    "You'll host in-person customer workshops a few times a year.",
    'Experience with on-site implementation at customers is a plus.',
    'Trabajamos 100% remoto y puedes residir en cualquier país.',
    'No es necesario residir en España.',
    'Sin necesidad de vivir en Madrid; trabaja desde donde quieras.',
    'Não é necessário morar em Portugal, o trabalho é remoto.',
    'Ein Wohnsitz in Deutschland ist nicht erforderlich.',
    'Pas besoin de résider en France, télétravail complet.',
    'Non è necessario risiedere in Italia.',
    'Je hoeft niet in Nederland te wonen.',
    'You must hold a valid passport for occasional travel.',
    'The office in Madrid is optional; most of us work remotely.',
    'This is not a remote-only company culture problem; we are remote-first and fully remote.',
    'Our customers use hybrid work models and on-site kiosks.',
    'We are a fully remote company with no offices; in-person meetups are optional.',
    'Remote work is available and encouraged for this role.',
    'Attendance at our annual in-person summit is expected once a year.',
    'Los clientes se encuentran en España, pero el equipo trabaja de forma remota desde cualquier lugar.',
    'Hybrid cloud experience is required.',
    'You should be comfortable with on-site customer visits when needed.',
]
DESC_REJECT = [
    'Candidates must reside in the United States.',
    'This role requires you to be located in Germany.',
    'You must be authorized to work in the UK without sponsorship.',
    'US work authorization required; we are unable to sponsor visas.',
    'This position is hybrid, with three days a week in our London office.',
    'Onsite role. You will be expected in the office five days a week.',
    'Must be able to work from our Austin office.',
    'Only candidates based in Canada will be considered.',
    'Applicants must be eligible to work in Australia.',
    'This role is restricted to residents of the EU.',
    'You must have the right to work in Ireland.',
    'Active Secret security clearance required.',
    'U.S. citizens only due to government contract requirements.',
    'Must be a US citizen or permanent resident.',
    'Remote, but you must be located in the Eastern Time zone in the United States.',
    'Remote within the US only.',
    'We are only hiring in the following states: CA, NY, TX, WA.',
    'This is a remote position open to residents of Brazil only.',
    'Relocation to Singapore is required.',
    'Candidates must live within commuting distance of our Chicago office.',
    'Position based in Toronto; relocation assistance available. Must be in the office.',
    'Residency in India is mandatory for this role.',
    'Due to local regulations we can only hire in Poland.',
    'Must have a valid work permit for Canada.',
    'We can only consider applicants currently residing in Mexico.',
    'Eligibility to work in the United States is required; no sponsorship available.',
    'Los candidatos deben residir en España.',
    'Les candidats doivent résider en France.',
    'Bewerber müssen in Deutschland wohnhaft sein.',
    'Candidatos devem residir no Brasil.',
    'You must be based in the UK to be considered; this is not a remote-anywhere role.',
    'In-person presence at our Dubai headquarters is required.',
    'This role is only open to people living in Australia or New Zealand.',
    'Applicants must be legally authorised to work in the Netherlands.',
    'Must be able to commute to our Lagos office three days a week.',
    'You must be physically present in the office at least four days a week.',
    'In-office role: employees are required on site daily.',
    'In-person presence at our Dubai headquarters is required.',
    'You will be required to work from our Lagos office.',
    'This is not a remote position.',
    'Remote work is not available for this role.',
    'Position is 100% on-site at the customer location.',
    'Hybrid schedule: Tuesday-Thursday in the office.',
    'We require employees to be within 50 miles of our Denver office.',
    'Must live in the Bay Area.',
    'Applicants must hold a UK passport or settled status.',
    'Only EU citizens or those with EU work authorization may apply.',
    'Candidates must be eligible to work in the US on a full-time basis without sponsorship now or in the future.',
    'We cannot sponsor work visas; you must already have the right to work in Canada.',
    'Must be a resident of Texas, Florida, or Georgia.',
    'TS/SCI clearance required.',
    'Must be able to obtain and maintain a public trust clearance.',
    'Only considering candidates in Pacific Time.',
    'Candidates must be located within the Central European Time zone, in Germany.',
    'Open only to residents of South Africa.',
    'This role is for Nigerian residents only.',
    'Residency in the Philippines is required.',
    'Preference will be given to candidates in the United States who can work EST hours; candidates outside the US will not be considered.',
    'Los candidatos deben residir en España.',
    'Se requiere residir en México.',
    'Debes vivir en Colombia para aplicar a esta vacante.',
    'Les candidats doivent résider en France.',
    'Vous devez habiter en Belgique.',
    'Résidence en France obligatoire.',
    'Candidatos devem residir no Brasil.',
    'É necessário morar em Portugal.',
    'Bewerber müssen in Deutschland wohnhaft sein.',
    'Wohnsitz in Deutschland erforderlich.',
    'Sie müssen in Österreich leben.',
    'Devi risiedere in Italia.',
    'Residenza in Italia obbligatoria.',
    'Je moet in Nederland wonen.',
    'Autorisation de travailler en France requise.',
    'Se requiere autorización para trabajar en España.',
    'Arbeitserlaubnis für Deutschland erforderlich.',
    'Presencia en la oficina de Madrid obligatoria tres días por semana.',
    'Présence au bureau de Paris obligatoire deux jours par semaine.',
    'Präsenz im Büro in München erforderlich.',
    'Presença no escritório em São Paulo obrigatória.',
    'Applicants must have a US passport.',
    'Only considering candidates in Pacific Time.',
    'This role is on-site only.',
    'This is not a remote role.',
    'Je moet in Nederland wonen.',
    'Debes residir en Argentina.',
    'Müssen in der Schweiz leben.',
]

fails, n = [], 0
for l in LOC_KEEP:
    n += 1
    if run(l, BASE) == "no_match":
        fails.append(f"false reject, location {l!r}")
for l in LOC_REJECT:
    n += 1
    if run(l, BASE, wt="") != "no_match":
        fails.append(f"false accept, location {l!r}")
for d in DESC_KEEP:
    for loc in ("Remote - Worldwide", ""):
        n += 1
        if run(loc, BASE + d) == "no_match":
            fails.append(f"false reject ({loc or 'blank location'}): {d}")
for d in DESC_REJECT:
    for loc in ("Remote - Worldwide", ""):
        n += 1
        if run(loc, BASE + d) != "no_match":
            fails.append(f"false accept ({loc or 'blank location'}): {d}")
# a time-zone range that spans every inhabited zone is no restriction; a single zone is
for l, ok in (("Remote (time zones UTC-10 to UTC+14 only)", True), ("Remote (UTC-12 to UTC+14)", True), ("Remote (time zones UTC-4 to UTC-4 only)", False)):
    n += 1
    if (run(l, BASE) == "match") != ok:
        fails.append(f"time-zone range {l!r}")
# a privacy-notice sentence naming a state is not an eligibility rule; a real rule next to it still is
n += 2
if run("Remote - Worldwide", BASE + " Pursuant to the California Consumer Privacy Act, this notice explains our practices for job applicants residing in California.") == "no_match":
    fails.append("CCPA notice must not reject")
if run("Remote - Worldwide", BASE + " Privacy notice. You must be a California resident to apply.") != "no_match":
    fails.append("a real state-residency rule beside a privacy notice must reject")
print(f"classifier vocabulary checks: {n - len(fails)}/{n} passed")
for f in fails:
    print("  FAIL", f)
_FAILED += len(fails)

# ====================================================================================================
# SECTION: check_classifier_expanded - Expanded classifier regression corpus (2026-10): ~2,700 generated cases through the real classifier - time-zone hours / ranges,
# ====================================================================================================
import sys
import os,sys,itertools,collections,random
for k in ("SUPABASE_URL","SUPABASE_KEY","CEREBRAS_API_KEY","GROQ_API_KEY","OPENAI_API_KEY"): os.environ.setdefault(k,"x")
import logging; logging.disable(50)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import classifier as C
BASE="You will own onboarding, adoption and renewals for our customers and partner closely with sales and product. "*3
def run(loc,desc,wt="Remote"):
    return C._keyword_classify_location_detail({"title":"Customer Success Manager","location":loc,"country":"","description_snippet":desc,"workplace_type":wt,"source_ats":"Greenhouse","role_category":"CS"})[0]
TZ=["EST","PST","CET","GMT","UTC+2","EET","CAT","WAT","EAT","IST","JST","AEST","Eastern Time","Pacific Time","Central European Time","UTC","UTC+1","UTC-5","GMT+3","South Africa Standard Time"]
CTRY=["the United States","Canada","Germany","France","Nigeria","Kenya","South Africa","Egypt","Ghana","India","Brazil","Mexico","Australia","Spain","Italy","Poland","the Netherlands","the UK","Ireland","Singapore","Japan","the UAE","Morocco","Ethiopia","Tanzania","Uganda","Rwanda","Senegal","Argentina","Colombia"]
CITY=["London","Berlin","New York","Lagos","Nairobi","Cape Town","Cairo","Paris","Dublin","Amsterdam","Toronto","Sydney","Dubai","Johannesburg","Accra"]
REG=["Africa","EMEA","Europe and Africa","the Middle East and Africa","Sub-Saharan Africa","East Africa","West Africa","Southern Africa","North Africa","Africa and Europe"]
KEEP=[]  # (category, location, desc, expect) expect K = not no_match
REJ=[]
def K(cat,desc,loc="Remote - Worldwide"): KEEP.append((cat,loc,desc))
def R(cat,desc,loc="Remote - Worldwide"): REJ.append((cat,loc,desc))
# ---- time zones: hours/overlap/preference statements are NOT location restrictions
for tz in TZ:
    K("tz-keep",f"Overlap with {tz} working hours is required.")
    K("tz-keep",f"You should be available for meetings during {tz} hours.")
    K("tz-keep",f"Our core hours are 10:00-14:00 {tz}; you can work from anywhere.")
    K("tz-keep",f"Flexible schedule with a few hours of overlap in {tz}.")
    K("tz-keep",f"Preferably working in a time zone close to {tz}.")
    K("tz-keep",f"The team meets every morning at 9am {tz}.")
    K("tz-keep",f"This role supports customers in {tz} business hours.")
    K("tz-keep",f"Willing to work some evenings to cover {tz}.")
for a,b in itertools.permutations(["UTC-8","UTC-5","UTC","UTC+1","UTC+3","UTC+5:30","UTC+8"],2):
    K("tz-range-keep",f"We work across time zones from {a} to {b}.")
    K("tz-range-keep",f"Our team spans {a} to {b} and you can be anywhere in between.")
for tz in ["Pacific","Eastern","Central","Mountain"]:
    R("tz-reject",f"Only considering candidates in {tz} Time.")
    R("tz-reject",f"You must be located in the {tz} Time Zone.")
    R("tz-reject",f"Candidates must reside in the {tz} time zone.")
# ---- single-country restrictions (reject) incl. African, each phrasing
PH=["Candidates must reside in {c}.","You must be located in {c}.","This role is only open to candidates based in {c}.","Applicants must be legally authorized to work in {c}.",
"Must have the right to work in {c}.","We can only hire in {c} for this position.","Eligibility to work in {c} is required.","This position is restricted to residents of {c}.","Only candidates living in {c} will be considered.",
"Must be a citizen of {c}.","Citizenship of {c} is required.","You need a valid work permit for {c}.","Relocation to {c} is required.","This role is based in {c}; remote work from other countries is not possible."]
for c in CTRY:
    for p in PH: R("country-reject",p.format(c=c))
for c in CITY:
    R("city-reject",f"This role requires you to work from our {c} office.")
    R("city-reject",f"Hybrid: three days per week in our {c} office.")
    R("city-reject",f"Must live within commuting distance of {c}.")
    R("city-reject",f"On-site in {c}, five days a week.")
# ---- region / global keep
for r in REG:
    K("region-keep",f"Open to candidates anywhere in {r}.")
    K("region-keep",f"We are hiring remotely across {r}.")
    K("region-keep",f"Candidates based in {r} are welcome.")
    K("region-keep",f"Fully remote role for people living in {r}.")
GL=["We hire anywhere in the world.","This role is open worldwide.","Work from anywhere.","Location is not a factor.","We are a fully distributed team with no offices.","Apply from any country.","Remote-first, global team.","No location restrictions apply.","Any time zone is fine.","We welcome applicants from all over the globe.","You can work from wherever you choose.","Our employees live in 30+ countries.","We hire through an employer of record so you can live almost anywhere.","Candidates from all countries are eligible.","Open to international applicants.","No relocation needed.","You can live anywhere, as long as you have internet.","Digital nomads welcome.","This is a global remote position."]
for g in GL: K("global-keep",g)
# multi-country company description is not a restriction
for combo in [("the US","UK","Germany"),("Nigeria","Kenya","Ghana"),("Canada","France","Brazil","India"),("South Africa","Egypt","Morocco")]:
    K("company-countries-keep",f"We have employees in {', '.join(combo[:-1])} and {combo[-1]}.")
    K("company-countries-keep",f"Our customers are located in {', '.join(combo[:-1])} and {combo[-1]}.")
    K("company-countries-keep",f"You will work with partners in {', '.join(combo[:-1])} and {combo[-1]}.")
    K("company-countries-keep",f"We are headquartered in {combo[0]} with offices in {combo[-1]}, and this role is remote.")
# ---- benign statements that contain scary words
BEN=["No visa sponsorship is needed for this remote role.","We do not require you to relocate.","Relocation is not required.","Travel to our {c} office once a year for the team offsite.","Occasional travel to {c} is expected.",
"Experience with {c} regulations is a plus.","Knowledge of the {c} market is helpful.","You will support customers in {c}.","Our {c} team will onboard you remotely.","Salary is benchmarked to {c} but paid in your local currency.",
"We do background checks for all hires.","This role may involve occasional on-site client visits.","Our office in {c} is open if you want to use it.","Hybrid work is available for those near our {c} office, but this role is fully remote.","We offer relocation support if you wish to move.",
"No security clearance required.","You do not need a work permit for {c} as you work from your home country.","Fluency in English is required.","Candidates must have strong written and verbal English.","Must be comfortable working in a fast-paced startup environment.",
"Must be authorized to work in your country of residence.","You must have the legal right to work where you live.","You are responsible for your own taxes in your country of residence.","We pay in USD via Deel.","Contractors only; you invoice monthly."]
for b in BEN:
    if "{c}" in b:
        for c in CTRY[:8]: K("benign-scary-keep",b.format(c=c))
    else: K("benign-scary-keep",b)
# ---- application questions
QK=["How did you hear about this role?","What is your current country of residence?","Where are you currently located?","What is your expected salary?","Please share your LinkedIn profile.","What are your preferred pronouns?","Are you comfortable working fully remotely?","Do you have a reliable internet connection and a quiet workspace?","What time zone are you in?","Are you able to work during overlapping hours with our team?","Are you at least 18 years old?","Do you have experience with Salesforce or HubSpot?","Why do you want to work here?","Describe a time you handled a difficult customer.","What is your notice period?","Are you open to occasional travel?","Do you have your own laptop?","Are you able to start within 30 days?","Have you previously worked at our company?","Do you consent to us processing your data?","Which languages do you speak fluently?","Are you comfortable with async communication?","Will you require visa sponsorship to work from your country of residence?","Are you legally able to work in the country where you live?","Can you work as a contractor in your country?","Are you available for a call in the next week?","What is your github username?","Do you have experience in B2B SaaS customer success?","Are you willing to undergo a background check?","Can you commit to at least 40 hours per week?"]
QR=[f"Are you legally authorized to work in {c}?" for c in CTRY]+[f"Do you currently reside in {c}?" for c in CTRY]+[f"Are you willing to relocate to {c}?" for c in CTRY[:12]]+[f"Are you a citizen of {c}?" for c in CTRY[:10]]+[f"Will you now or in the future require visa sponsorship to work in {c}?" for c in CTRY[:12]]+[f"Are you able to commute to our {c} office?" for c in CITY]+[f"Do you hold an active security clearance in {c}?" for c in CTRY[:6]]
for q in QK: K("question-keep","\nApplication Question: "+q)
for q in QR: R("question-reject","\nApplication Question: "+q)
# ---- global vocabulary in location field
LOCK=[]
PRE=["","Remote - ","Remote, ","Remote (","Remote / ","Fully Remote - ","100% Remote - ","Work from home - ","Virtual - ","Telecommute - ","Home based - ","Distributed - ","Flexible - "]
WORDS=["Worldwide","Global","Anywhere","Anywhere in the world","International","Globally","Any location","All locations","Multiple countries","Various countries","Earth","World","Everywhere","Global (any country)","Africa","EMEA","Pan-Africa","Pan-African","Sub-Saharan Africa","East Africa","West Africa","Southern Africa","North Africa","Africa & Europe","Europe, Middle East & Africa","Europe/Africa","Africa/Middle East","Anywhere (WFA)","Open to all countries","Not location specific","Location agnostic","Any timezone","All time zones"]
for p in PRE:
    for w in WORDS:
        v=(p+w+(")" if p.endswith("(") else ""))
        LOCK.append(v)
LOCR=[]
for p in ["","Remote - ","Remote, ","Remote (","Hybrid - ","On-site - ",""]:
    for c in ["United States","USA","Canada","Germany","France","UK","United Kingdom","India","Brazil","Mexico","Australia","Nigeria","Kenya","South Africa","Egypt","Ghana","Spain","Italy","Poland","Netherlands","Singapore","Japan","UAE","Ireland","Argentina","London","Berlin","New York","Lagos","Nairobi","Cape Town","Cairo","Paris","Toronto","Sydney","Dubai","Bangalore","Austin, TX","Remote - LATAM","APAC","North America","Europe","LATAM","US/Canada"]:
        LOCR.append(p+c+(")" if p.endswith("(") else ""))
bad=collections.defaultdict(list); tot=collections.Counter()
for cat,loc,d in KEEP:
    for l in (loc,""):
        tot[cat]+=1
        if run(l,BASE+d)=="no_match": bad[cat].append((l,d))
for cat,loc,d in REJ:
    for l in (loc,""):
        tot[cat]+=1
        if run(l,BASE+d)!="no_match": bad[cat].append((l,d))
for v in LOCK:
    tot["loc-keep"]+=1
    if run(v,BASE)=="no_match": bad["loc-keep"].append((v,""))
for v in LOCR:
    tot["loc-reject"]+=1
    wt="" 
    if run(v,BASE,wt="")!="no_match": bad["loc-reject"].append((v,""))

# ---- found on real postings in the 2026-10 audit
for d in ["Please note that any offer of employment may be conditioned on your authorization to receive software or technology controlled under these U.S. export laws without sponsorship for an export license.",
          "Applicants who progress to the offer stage of the interview process may be asked to attend an in-person interview within one of our offices.",
          "You might be required to visit our office occasionally.","We sponsor the open source podcast.",
          "Pursuant to the California Consumer Privacy Act, this notice explains our practices for job applicants residing in California.",
          "Candidates based in East Africa are welcome.","Preferably based in Europe.","Ideally in a time zone close to CET."]:
    for l in ("Remote - Worldwide", ""):
        tot["real-keep"] += 1
        if run(l, BASE + d) == "no_match": bad["real-keep"].append((l, d))
for d in ["You must attend the office in London five days a week.","We cannot provide visa sponsorship.","We do not sponsor visas.","Must be able to work without sponsorship.","Eligibility to work in Rwanda is required.","Citizenship of Uganda is required."]:
    for l in ("Remote - Worldwide", ""):
        tot["real-reject"] += 1
        if run(l, BASE + d) != "no_match": bad["real-reject"].append((l, d))
n = sum(tot.values()); nbad = sum(len(v) for v in bad.values())
for cat in sorted(tot):
    if bad[cat]:
        print(f"{cat}: {len(bad[cat])}/{tot[cat]} wrong")
        for l, d in bad[cat][:15]:
            print("   ", repr(l), "|", d[:110])
print(f"expanded classifier checks: {n - nbad}/{n} passed")
_FAILED += nbad

_sys.exit(1 if _FAILED else 0)
