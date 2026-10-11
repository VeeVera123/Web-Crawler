"""Expanded classifier regression corpus (2026-10): ~2,700 generated cases through the real classifier - time-zone hours / ranges,
single-country and single-city restrictions (30 countries incl. 12 African, 15 cities), Africa / EMEA / global vocabulary in
location fields and prose, benign sentences that merely contain scary words, and application questions (benign vs restrictive).
Every KEEP case must NOT come back no_match (unsure = Rank 3 route); every REJECT case must."""
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
sys.exit(1 if nbad else 0)
