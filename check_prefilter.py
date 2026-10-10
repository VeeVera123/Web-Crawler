"""Offline checks for classifier.location_prefilter_keep (the pre-enrichment cost gate)."""
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

def job(loc, title="Customer Success Manager", ats="Greenhouse", country="", cat=None):
    return {"title": title, "location": loc, "country": country, "source_ats": ats, "role_category": cat or C.classify_role_category(title),
            "description_snippet": "x", "workplace_type": ""}

K = lambda j, r4: C.location_prefilter_keep(j, rank4_enabled=r4)
# certain rejects: a named non-allowed place, no rank-4 chance
check(not K(job("Warsaw, Poland"), True), "Warsaw rejects (poland not on rank-4 list)")
check(not K(job("Bangalore, India"), True), "India rejects")
check(not K(job("Austin, TX"), True), "US state rejects")
check(not K(job("Warsaw, Poland", ats="Workday"), True), "non-eligible ATS rejects")
check(not K(job("Berlin, Germany", title="Operations Manager", cat="OM"), True), "non CS/AM role cannot use rank 4")
check(not K(job("Berlin, Germany"), False), "rank 4 disabled -> a named place rejects")
# must be kept: description/LLM/rank-4 can still decide
check(K(job("Remote"), False), "bare Remote kept")
check(K(job(""), False), "blank location kept")
check(K(job("Worldwide"), False), "global kept")
check(K(job("EMEA"), False), "EMEA kept")
check(K(job("Remote - Worldwide, except US"), False), "worldwide except kept")
check(K(job("Anywhere"), False), "anywhere kept")
check(K(job("Berlin, Germany"), True), "rank-4 candidate (CS, eligible ATS, allowed country) kept")
check(K(job("London"), True) in (True, False), "no crash")
# fails open
check(C.location_prefilter_keep({}, rank4_enabled=True) in (True, False), "empty job does not crash")
kept, dropped = C.prefilter_jobs_by_location([job("Remote"), job("Warsaw, Poland")])
check(len(kept) == 1 and dropped == 1, f"prefilter_jobs_by_location {len(kept)}/{dropped}")
# --- fuzz: the gate may NEVER drop a job the full information would keep -------------------------------------------
import random
rnd = random.Random(7)
PLACES = ["Germany", "United Kingdom", "UK", "Canada", "Australia", "Sydney", "London", "Berlin", "Dublin", "Ireland", "Singapore",
          "Netherlands", "Amsterdam", "Norway", "Oslo", "Switzerland", "Zurich", "Sweden", "Stockholm", "Italy", "Milan", "Iceland",
          "Luxembourg", "Europe", "EMEA", "APAC", "DACH", "LATAM", "North America", "Nordics", "Benelux", "India", "Kenya", "Mumbai",
          "Texas", "Warsaw", "Paris", "Remote", "Hybrid", "Worldwide", "Global", "Anywhere", "United States", "USA", "Africa", "Nigeria",
          "Lagos", "Brazil", "São Paulo", "Tokyo", "Berlin, Germany", "New York, NY", "Remote - EMEA", "Home based", "Flexible", "TBD", ""]
CONN = [", ", " - ", " / ", " | ", " and ", " or ", " (", "; ", " "]
DESCS = ["", "We hire globally across EMEA.", "Open to candidates in Europe, APAC and the Americas.", "You must reside in Germany.",
         "Work from anywhere in the world.", "Application Question: Are you authorized to work in the US?", "Fully remote, any time zone."]
ATS = ["Greenhouse", "Lever", "Ashby", "Workday", "Workable", "Teamtailor", "Personio", "BambooHR"]
TITLES = ["Customer Success Manager", "Account Manager", "Project Manager", "Operations Manager", "Senior CSM", "Client Support Manager"]
bad = 0
for _ in range(6000):
    k = rnd.choice([1, 1, 2, 3])
    parts = [rnd.choice(PLACES) for _ in range(k)]
    loc = parts[0]
    for q in parts[1:]:
        c = rnd.choice(CONN)
        loc += c + q + (")" if c == " (" else "")
    j = {"title": rnd.choice(TITLES), "location": loc, "country": rnd.choice(["", "", "Germany", "United States", "Kenya"]),
         "source_ats": rnd.choice(ATS), "description_snippet": rnd.choice(DESCS), "workplace_type": rnd.choice(["", "", "Remote", "Hybrid"])}
    j["role_category"] = C.classify_role_category(j["title"])
    r4_on = rnd.choice([True, False])
    full_kw = C._keyword_classify_location_detail(j)[0]
    full_r4 = bool(r4_on and j["role_category"] in ("CS", "AM") and j["source_ats"] in C.RANK4_ELIGIBLE_ATS and C.classify_rank4(j)[0])
    would_keep = full_kw != "no_match" or full_r4
    if would_keep and not C.location_prefilter_keep(j, rank4_enabled=r4_on):
        bad += 1
        if bad <= 5:
            print("DROPPED A KEEPER:", j, full_kw, full_r4)
check(bad == 0, f"fuzz: gate dropped {bad} jobs the full classifier would have kept")

print(f"prefilter checks: {n - len(fails)}/{n} passed")
for m in fails: print("FAIL", m)
sys.exit(1 if fails else 0)
