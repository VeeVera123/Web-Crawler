"""Offline regression check: application questions that tie a job to a place
must reject it, and ordinary questions that merely mention a place must not.

Runs the real deterministic classifier (no network, no LLM) on a bare-"Remote"
Customer Success job carrying ONE application question, expanded from the
templates below over many countries / cities / states.

    python check_restrictive_questions.py            # summary, exit 1 on any miss
    python check_restrictive_questions.py -v         # also list every miss
"""
import itertools
import os
import sys

for _k in ("SUPABASE_URL", "SUPABASE_KEY", "CEREBRAS_API_KEY", "GROQ_API_KEY", "GROQ_API_KEY_C",
           "OPENAI_API_KEY", "NVIDIA_API_KEY", "ANTHROPIC_API_KEY"):
    os.environ.setdefault(_k, "x")

import classifier  # noqa: E402

COUNTRIES = ["the U.S.", "the US", "the United States", "USA", "U.S.A.", "Canada", "the UK", "the United Kingdom",
             "Germany", "France", "Australia", "India", "Brazil", "Mexico", "Ireland", "the Netherlands", "Spain",
             "Poland", "Singapore", "Japan", "South Africa", "Nigeria", "Kenya", "New Zealand", "the UAE",
             "the Philippines", "Colombia", "Argentina", "Israel", "the EU", "the EEA", "Europe", "North America"]
CITIES = ["NYC", "New York", "San Francisco", "SF", "Denver", "Austin", "Boston", "London", "Toronto", "Berlin",
          "Sydney", "Chicago", "Seattle", "the Bay Area", "Los Angeles", "Dublin", "Amsterdam", "Paris", "Lagos",
          "Nairobi", "Atlanta", "Miami", "Dallas", "Vancouver", "Singapore"]
CITY_LISTS = ["NYC, San Francisco or Denver", "New York, Austin, or Chicago", "London or Dublin",
              "Toronto or Vancouver", "Boston, MA", "our Seattle office", "the Bay Area"]
STATES = ["California", "Texas", "New York", "Florida", "Washington", "Colorado", "Illinois", "Massachusetts",
          "Georgia", "Virginia", "North Carolina", "Ohio"]
ZONES = ["US Eastern", "Pacific", "Eastern Time", "Central European", "GMT", "UK", "EST", "PST", "CET",
         "Australian Eastern", "IST"]

# {C} country, {X} city or city list, {S} US state, {Z} time zone.
RESTRICTIVE = [
    # work authorization / right to work
    "Are you authorized to work in {C}?",
    "Are you legally authorized to work in {C}?",
    "Are you authorized to work for any employer in {C}?",
    "Are you currently authorized to work in {C} on a full-time basis?",
    "Are you authorised to work in {C}?",
    "Do you have the legal right to work in {C}?",
    "Do you have the right to work in {C}?",
    "Do you have the unrestricted right to work in {C}?",
    "Are you legally eligible to work in {C}?",
    "Are you eligible to work in {C} without sponsorship?",
    "Are you legally permitted to work in {C}?",
    "Are you legally entitled to work in {C}?",
    "Can you legally work in {C} without employer sponsorship?",
    "Do you have valid work authorization for {C}?",
    "Do you have unrestricted work authorization in {C}?",
    "Do you hold a valid work permit for {C}?",
    "Do you hold a valid work visa for {C}?",
    "Do you possess a valid work permit in {C}?",
    "Do you have a valid visa that permits you to work in {C}?",
    "Please confirm you are legally permitted to work in {C}.",
    "I am legally authorized to work in {C}.",
    "I confirm that I have the right to work in {C}.",
    "Are you able to provide proof of your eligibility to work in {C}?",
    "Do you have the legal right to be employed in {C}?",
    "Are you a citizen or permanent resident of {C}?",
    "Are you a citizen of {C}?",
    "Are you a permanent resident of {C}?",
    "Do you hold citizenship or permanent residency in {C}?",
    "Do you currently hold a work permit or citizenship in {C}?",
    # sponsorship tied to a place
    "Will you now or in the future require sponsorship to work in {C}?",
    "Will you require visa sponsorship to work in {C}?",
    "Do you now or will you in the future require employer sponsorship in {C}?",
    "Will you need an employer to sponsor your work visa for {C}?",
    "Do you require work authorization sponsorship for {C}?",
    "Would you require immigration sponsorship in order to work in {C}?",
    # residence / location
    "Are you currently a resident of {C}?",
    "Do you currently reside in {C}?",
    "Are you currently living in {C}?",
    "Do you currently live in {C}?",
    "Are you currently located in {C}?",
    "Are you based in {C}?",
    "Are you physically located in {C}?",
    "Do you live in or are you willing to relocate to {C}?",
    "Is your permanent residence in {C}?",
    "Are you a resident of {C}?",
    "Will you be working from {C}?",
    "Will you be located in {C} for the duration of your employment?",
    "Can you work from {C} full time?",
    "Are you able to work from {C}?",
    "This role requires you to be based in {C}. Do you meet this requirement?",
    "Do you meet the requirement to reside in {C}?",
    # relocation / commute / on-site
    "Are you willing to relocate to {X}?",
    "Are you open to relocating to {X}?",
    "Would you be willing to relocate to {X}?",
    "Are you able to relocate to {X} within 3 months?",
    "Are you willing to move to {X}?",
    "Would you consider relocating to {X}?",
    "Are you comfortable relocating to {X}?",
    "Are you currently in {X} or willing to relocate?",
    "Are you commuting distance from {X}?",
    "Do you live within commuting distance of {X}?",
    "Do you live within 50 miles of {X}?",
    "Are you within a reasonable commute of {X}?",
    "Can you commute to {X}?",
    "Are you able to commute to our {X} office?",
    "Are you able to work from our {X} office 3 days a week?",
    "Are you able to work on-site in {X}?",
    "Are you comfortable working in the {X} office?",
    "Are you able to come into the office in {X} regularly?",
    "Are you able to attend in-person meetings in {X}?",
    "Do you live in the {X} area?",
    "Do you currently live in or near {X}?",
    "Are you local to {X}?",
    "Are you a {X} resident?",
    "Are you willing to work a hybrid schedule in {X}?",
    "Are you able to work in-person in {X}?",
    # states
    "Do you reside in {S}?",
    "Are you a resident of {S}?",
    "Do you currently live in {S}?",
    "Are you currently located in {S}?",
    "Are you licensed to work in {S}?",
    "Do you hold an active license in {S}?",
    "Are you authorized to work in {S}?",
    "Will you be working from {S}?",
    # time zone / hours
    "Are you able to work {Z} business hours?",
    "Are you able to work in the {Z} time zone?",
    "Are you located in the {Z} time zone?",
    "Can you overlap at least 4 hours with {Z} time?",
    "Are you available to work {Z} hours?",
    "Will you be working in the {Z} time zone?",
    # citizenship / clearance / background
    "Are you a U.S. citizen?",
    "Are you a US citizen or green card holder?",
    "Do you hold an active security clearance?",
    "Are you eligible to obtain a U.S. security clearance?",
    "Are you eligible for a government security clearance?",
    "Can you pass a background check in {C}?",
]

# Mention a place but are ordinary screening questions: must NOT reject the job.
BENIGN = [
    "Why do you want to work here?",
    "Describe a time you retained an at-risk customer.",
    "How many years of customer success experience do you have?",
    "Are you comfortable working in a remote-first, async environment?",
    "What are your salary expectations?",
    "Are you available to start within 30 days?",
    "Which CRM tools have you used (Salesforce, HubSpot, Gainsight)?",
    "Do you have experience selling to enterprise customers in {C}?",
    "Describe your experience working with customers in {C}.",
    "Have you managed accounts in the {C} market?",
    "How would you describe your knowledge of the {C} SaaS market?",
    "What is your notice period?",
    "Are you open to a contract-to-hire arrangement?",
    "Please share a link to your LinkedIn profile.",
    "Have you ever worked at a company headquartered in {C}?",
    "Do you speak English fluently?",
    "Are you comfortable with occasional travel?",
    "What interests you about this role?",
    "How did you hear about this role?",
    "Tell us about a project you are proud of.",
    "Do you have experience working in the {C} market?",
    "Do you have experience working with customers in {C}?",
    "How many years have you worked with clients in {C}?",
    "Have you worked in the SaaS industry in {C}?",
    "Are you familiar with the regulatory environment in {C}?",
    "Do you have experience supporting {C} customers in their own time zone?",
    "Are you comfortable working with customers in {C}?",
    "Why are you interested in working for a company based in {C}?",
    "Our customers are mostly in {C}. How would you build rapport with them?",
    "Our team is spread across {C}. How do you feel about async work?",
    "Tell us about a time you onboarded a customer in {C}.",
    "What do you know about our competitors in {C}?",
    "Where are you currently located?",
    "Where do you currently live?",
    "What city and country do you live in?",
    "What time zone are you in?",
    "Are you comfortable working across multiple time zones?",
    "Are you comfortable working across time zones such as US Eastern and CET?",
    "Describe how you would handle an escalation from a customer in a different time zone.",
    "Are you open to working with a globally distributed team?",
    "Are you comfortable working in a fast-paced environment?",
    "Are you willing to travel occasionally for customer visits?",
    "Do you require relocation assistance?",
    "Are you able to start on a Monday?",
    "Are you currently employed?",
    "Are you at least 18 years old?",
    "Do you require visa sponsorship?",
    "Are you legally able to sign contracts?",
    "Do you have a reliable internet connection?",
    "Do you have a home office setup suitable for remote work?",
    "Are you comfortable using Zoom and Slack for most communication?",
    "Do you have experience with GDPR or other privacy regulations?",
    "Please tell us about your experience with enterprise onboarding.",
    "Have you ever worked remotely full time?",
    "What would you do in your first 90 days?",
    "Which languages do you speak?",
    "Describe your ideal working environment.",
    "Are you comfortable with a role that is 100% remote?",
    "Is there anything else you'd like us to know?",
]

JOB = {"title": "Customer Success Manager", "location": "Remote", "country": "", "workplace_type": "",
       "role_category": "CS", "source_ats": "Ashby"}


def rejected(question: str) -> bool:
    job = dict(JOB, description_snippet="We help teams ship.\nApplication Question: " + question)
    return classifier._keyword_classify_location_detail(job)[0] == "no_match"


def expand(template: str):
    slots = {"{C}": COUNTRIES, "{X}": CITIES + CITY_LISTS, "{S}": STATES, "{Z}": ZONES}
    used = [k for k in slots if k in template]
    if not used:
        yield template
        return
    for combo in itertools.product(*(slots[k] for k in used)):
        q = template
        for k, v in zip(used, combo):
            q = q.replace(k, v)
        yield q


def main() -> int:
    verbose = "-v" in sys.argv
    misses, total = [], 0
    for t in RESTRICTIVE:
        cases = list(expand(t))
        step = max(1, len(cases) // 12)  # <=12 per template keeps the run fast
        cases = cases[::step]
        bad = [q for q in cases if not rejected(q)]
        total += len(cases)
        if bad:
            misses.append((t, len(bad), len(cases), bad[0]))
    fp = [q for t in BENIGN for q in list(expand(t))[::3] if rejected(q)]
    fp_total = sum(len(list(expand(t))[::3]) for t in BENIGN)
    print(f"restrictive: {total - sum(m[1] for m in misses)}/{total} rejected "
          f"({len(RESTRICTIVE)} templates, {len(misses)} templates with misses)")
    print(f"benign:      {fp_total - len(fp)}/{fp_total} correctly kept ({len(fp)} wrongly rejected)")
    if verbose or misses:
        for t, n, tot, ex in misses:
            print(f"  MISS {n}/{tot}  {t}   e.g. {ex!r}")
    for q in fp:
        print(f"  FALSE POSITIVE  {q!r}")
    return 1 if misses or fp else 0


if __name__ == "__main__":
    raise SystemExit(main())
