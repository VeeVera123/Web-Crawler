"""Offline checks for the form-field surface (no network):

  * answer OPTIONS: restrictive option sets / closed country lists reject, ordinary option sets don't
  * hard-wrapped questions and look-alike characters can't hide a restriction
  * transformation fuzz: every restrictive question from the corpus, re-written with fullwidth letters,
    zero-width / soft-hyphen characters, non-breaking spaces, upper-casing, decoration and extra spaces,
    must still be rejected for a one-country Rank 4 job
  * the fetcher helpers (one-line labels, options line)

    python check_form_fields.py [-v]
"""
import os
import re
import sys

for _k in ("SUPABASE_URL", "SUPABASE_KEY", "CEREBRAS_API_KEY", "GROQ_API_KEY", "GROQ_API_KEY_C",
           "OPENAI_API_KEY", "NVIDIA_API_KEY", "ANTHROPIC_API_KEY"):
    os.environ.setdefault(_k, "x")

import ats_scrapers  # noqa: E402
import classifier  # noqa: E402

BASE = {"title": "Customer Success Manager", "country": "", "workplace_type": "", "role_category": "CS",
        "source_ats": "Greenhouse"}
VERBOSE = "-v" in sys.argv
failures: list[str] = []
total = 0


def check(name, ok, detail=""):
    global total
    total += 1
    if not ok:
        failures.append(f"{name} {detail}")


def job(loc, qs=(), options=(), jd="We help teams ship software faster."):
    lines = [jd] + ["Application Question: " + q for q in (qs or ["How did you hear about this role?"])]
    lines += [f"Application Options: {lab} => " + " | ".join(opts) for lab, opts in options]
    return dict(BASE, location=loc, description_snippet="\n".join(lines))


def universal_rejected(j):
    return classifier._keyword_classify_location_detail(dict(j, location="Remote"))[0] == "no_match"


def r4_admitted(j):
    return classifier.classify_rank4(j)[0] is not None


# ── answer options ──────────────────────────────────────────────────────────
NAMED_OPTIONS = [  # (label, options): a place-bound eligibility option -> rejected on BOTH paths
    ("Please select your work authorization status.", ["I am authorized to work in the US without sponsorship",
                                                       "I require sponsorship", "I am not authorized to work in the US"]),
    ("Status", ["US citizen", "Permanent resident", "Other"]),
    ("Status", ["UK Skilled Worker", "British citizen", "Other"]),
    ("Right to work", ["I have the right to work in the United Kingdom", "I do not"]),
    ("Where do you live?", ["I currently reside in Australia", "Elsewhere"]),
    ("Relocation", ["Yes, I am willing to relocate to New York", "No"]),
    ("Work eligibility", ["I hold unrestricted Canadian work authorization", "I need sponsorship"]),
    ("Eligibility", ["I am eligible to work in Canada without sponsorship", "I am not"]),
]
BARE_OPTIONS = [  # no place named: rejected for a one-country Rank 4 job only
    ("What is your status?", ["Citizen", "PR", "Need Visa"]),
    ("Please select one", ["I require sponsorship", "I do not require sponsorship"]),
    ("Current status", ["H-1B", "OPT", "Green card", "Other"]),
    ("Please select one", ["I am authorized to work without sponsorship", "I am not authorized to work"]),
    ("Type", ["I have a valid work permit", "I require a work visa"]),
]
CLOSED_LISTS = [("Which country are you located in?", ["United States", "Canada", "United Kingdom"]),
                ("Country of residence", ["Germany", "France", "Netherlands", "Ireland"]),
                ("Where are you based?", ["USA", "UK"])]
OPEN_LISTS = [("Which country are you located in?", ["United States", "Canada", "Other"]),
              ("Which country are you located in?", ["Nigeria", "Kenya", "United States"]),
              ("Where are you based?", ["USA", "Rest of world"]),
              ("Preferred office", ["London", "New York", "Remote"])]
BENIGN_OPTIONS = [("How did you hear about this role?", ["LinkedIn", "Indeed", "Careers page", "Referral", "Other"]),
                  ("Which team interests you?", ["Customer Success", "Account Management", "Support"]),
                  ("Notice period", ["Immediately", "1 month", "2 months", "3 months"]),
                  ("Preferred payment network experience", ["Visa", "Mastercard", "Amex"]),
                  ("Expected start", ["ASAP", "Within a month", "Later"])]
for lab, opts in NAMED_OPTIONS:
    j = job("United States", options=[(lab, opts)])
    check("named option rejects (universal)", universal_rejected(j), f"{lab} {opts}")
    check("named option rejects (rank4)", not r4_admitted(j), f"{lab} {opts}")
for lab, opts in BARE_OPTIONS:
    check("bare option rejects (rank4 narrow)", not r4_admitted(job("United States", options=[(lab, opts)])), f"{lab} {opts}")
for lab, opts in CLOSED_LISTS:
    j = job("United States", options=[(lab, opts)])
    check("closed country list rejects (universal)", universal_rejected(j), f"{lab} {opts}")
    check("closed country list rejects (rank4 broad)", not r4_admitted(job("APAC", options=[(lab, opts)])), f"{lab} {opts}")
for lab, opts in OPEN_LISTS + BENIGN_OPTIONS:
    j = job("Remote", options=[(lab, opts)])
    check("open/benign options kept (universal)", not universal_rejected(j), f"{lab} {opts}")
    check("open/benign options kept (rank4 narrow)", r4_admitted(job("United States", options=[(lab, opts)])), f"{lab} {opts}")

# ── hard-wrapped questions (rows stored before the fetchers collapsed whitespace) ──
for q in ["Are you legally\nauthorized to work in the\nUnited States?", "Will you now or in the future\nrequire visa sponsorship\nfor employment?",
          "Are you willing to\nrelocate to New York?"]:
    desc = "We help teams ship.\nApplication Question: " + q + "\nApplication Question: How did you hear about us?"
    j = dict(BASE, location="United States", description_snippet=desc)
    check("wrapped question rejects (rank4)", not r4_admitted(j), repr(q))
# a wrapped JD sentence
j = dict(BASE, location="Remote", description_snippet="Candidates must reside\nin the United States.\nApplication Question: Why us?")
check("wrapped JD sentence rejects (universal)", universal_rejected(j))

# ── transformation fuzz over the Part 1 corpus ──────────────────────────────
def fullwidth(t):
    return "".join(chr(ord(c) + 0xFEE0) if re.match(r"[A-Za-z]", c) else c for c in t)


TRANSFORMS = {
    "upper": str.upper,
    "fullwidth": fullwidth,
    "nbsp": lambda t: t.replace(" ", "\u00a0"),
    "zero-width": lambda t: re.sub(r"(\w)(\w)", "\\1\u200b\\2", t, count=0),
    "soft-hyphen": lambda t: re.sub(r"(\w{4})(\w)", "\\1\u00ad\\2", t),
    "decorated": lambda t: "* Required: " + t + " (required) *",
    "spaced": lambda t: re.sub(r" ", "   ", t),
}
corpus = "corpus/restrictive_questions_openai_1.txt"
rows = [m.groups() for m in map(re.compile(r"^(\w+)\s+\[(NAMED|BARE)\]\s+(.*\S)\s*$").match,
                                open(corpus, encoding="utf-8").read().splitlines()) if m]
base_rejected = [(c, q) for c, _, q in rows if not r4_admitted(job("United States", [q]))]
for name, fn in TRANSFORMS.items():
    bad = [q for _, q in base_rejected if r4_admitted(job("United States", [fn(q)]))]
    check(f"transform '{name}' keeps {len(base_rejected)} rejections", not bad, f"{len(bad)} leaked, e.g. {bad[:2]}")

# ── fetcher helpers ─────────────────────────────────────────────────────────
out = ats_scrapers._format_screening_questions([
    {"label": "Work\n authorization\u00a0status", "options": ["I am authorized to work in the US", "I require sponsorship", "No"]},
    {"label": "Name"}, {"label": "Dept", "options": ["Yes", "No"]},
    {"label": "Country", "options": [f"C{i}" for i in range(60)]}])
check("fetcher: one-line label", "Application Question: Work authorization status" in out, out)
check("fetcher: options line", "Application Options: Work authorization status => I am authorized to work in the US | I require sponsorship" in out, out)
check("fetcher: yes/no and >40 option lists dropped", out.count("Application Options:") == 1, out)
check("fetcher: boilerplate dropped", "Name" not in out, out)

print(f"form-field checks: {total - len(failures)}/{total} passed")
for f in failures[: (None if VERBOSE else 15)]:
    print("  FAIL", f)
sys.exit(1 if failures else 0)
