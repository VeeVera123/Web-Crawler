"""Regression checks for classifier FALSE REJECTS found by auditing employer-declared-worldwide jobs (Himalayas)."""
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
sys.exit(1 if fails else 0)
