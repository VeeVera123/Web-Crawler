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
print(f"prefilter checks: {n - len(fails)}/{n} passed")
for m in fails: print("FAIL", m)
sys.exit(1 if fails else 0)
