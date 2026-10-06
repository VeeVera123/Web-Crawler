"""Run a tagged question corpus (corpus/*.txt, one `CATEGORY [TAG] question` per line)
through the real classifier and report what slips through.

    python check_corpus.py corpus/restrictive_questions_openai_1.txt [-v]

Tags: [NAMED]/[BARE]/[RESTRICTIVE] = restrictive (must reject), [BENIGN] = must be kept,
[AMBIGUOUS] = reported only. Lines whose category starts with JD are job-DESCRIPTION sentences, all
others are application questions. A restrictive question is checked at four points:
  rank4-narrow   job located in one allowed country (United States)   -> must NOT be admitted
  rank4-city     "City, Country" job                                  -> must NOT be admitted
  rank4-broad    job located in a region NAME (APAC)                  -> only NAMED must not be admitted
  universal      bare "Remote" job on the Rank 1/2/3 path             -> only NAMED must reject
"""
import os
import re
import sys
from collections import defaultdict

for _k in ("SUPABASE_URL", "SUPABASE_KEY", "CEREBRAS_API_KEY", "GROQ_API_KEY", "GROQ_API_KEY_C",
           "OPENAI_API_KEY", "NVIDIA_API_KEY", "ANTHROPIC_API_KEY"):
    os.environ.setdefault(_k, "x")

import classifier  # noqa: E402

BASE = {"title": "Customer Success Manager", "country": "", "workplace_type": "", "role_category": "CS",
        "source_ats": "Greenhouse"}
LINE = re.compile(r"^(\w+)\s+\[(\w+)\]\s+(.*\S)\s*$")


def _desc(q):
    return "We help teams ship software faster.\nApplication Question: " + q


def r4_admitted(loc, q):
    return classifier.classify_rank4(dict(BASE, location=loc, description_snippet=_desc(q)))[0] is not None


def r4_admitted_jd(loc, sentence):
    desc = sentence + "\nApplication Question: How did you hear about this role?"
    return classifier.classify_rank4(dict(BASE, location=loc, description_snippet=desc))[0] is not None


def universal_rejected_jd(sentence):
    job = dict(BASE, location="Remote", description_snippet=sentence)
    return classifier._keyword_classify_location_detail(job)[0] == "no_match"


def universal_rejected(q):
    job = dict(BASE, location="Remote", description_snippet=_desc(q))
    return classifier._keyword_classify_location_detail(job)[0] == "no_match"


def main():
    path = next(a for a in sys.argv[1:] if not a.startswith("-"))
    verbose = "-v" in sys.argv
    rows = [m.groups() for m in map(LINE.match, open(path, encoding="utf-8").read().splitlines()) if m]
    miss = defaultdict(list)
    n = 0
    for cat, tag, q in rows:
        jd = cat.startswith("JD")
        if tag in ("NAMED", "BARE", "RESTRICTIVE"):
            n += 1
            if jd:
                if r4_admitted_jd("United States", q):
                    miss["rank4-narrow"].append((cat, tag, q))
                if r4_admitted_jd("London, United Kingdom", q):
                    miss["rank4-city"].append((cat, tag, q))
                if tag in ("NAMED", "RESTRICTIVE") and not universal_rejected_jd(q):
                    miss["universal"].append((cat, tag, q))
                continue
            if r4_admitted("United States", q):
                miss["rank4-narrow"].append((cat, tag, q))
            if r4_admitted("London, United Kingdom", q):
                miss["rank4-city"].append((cat, tag, q))
            if tag in ("NAMED", "RESTRICTIVE") and r4_admitted("APAC", q):
                miss["rank4-broad"].append((cat, tag, q))
            if tag in ("NAMED", "RESTRICTIVE") and not universal_rejected(q):
                miss["universal"].append((cat, tag, q))
        elif tag == "BENIGN":
            n += 1
            if jd:
                if not r4_admitted_jd("United States", q):
                    miss["benign-rank4-narrow"].append((cat, tag, q))
                if universal_rejected_jd(q):
                    miss["benign-universal"].append((cat, tag, q))
                continue
            if not r4_admitted("United States", q):
                miss["benign-rank4-narrow"].append((cat, tag, q))
            if universal_rejected(q):
                miss["benign-universal"].append((cat, tag, q))
    print(f"{n} lines checked")
    for k in ("rank4-narrow", "rank4-city", "rank4-broad", "universal", "benign-rank4-narrow", "benign-universal"):
        v = miss.get(k, [])
        print(f"{k:22s} misses: {len(v)}")
        by_cat = defaultdict(int)
        for c, _, _ in v:
            by_cat[c] += 1
        if by_cat:
            print("    " + ", ".join(f"{c}={x}" for c, x in sorted(by_cat.items())))
        if verbose:
            for c, t, q in v:
                print(f"      {c} [{t}] {q}")
    return 1 if any(miss.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
