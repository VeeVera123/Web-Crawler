"""Regression: a country field that repeats what the location already says must not change the verdict
(Ashby sends location "Remote, Global" + country "Global"; that was rejected, then re-admitted by Rank 4 as 4b).

    python check_location_field.py
"""
import os
import sys

for _k in ("SUPABASE_URL", "SUPABASE_KEY", "CEREBRAS_API_KEY", "GROQ_API_KEY", "GROQ_API_KEY_C",
           "OPENAI_API_KEY", "NVIDIA_API_KEY", "ANTHROPIC_API_KEY"):
    os.environ.setdefault(_k, "x")
import classifier  # noqa: E402

BASE = {"title": "Account Manager", "workplace_type": "Remote", "role_category": "AM", "source_ats": "Ashby",
        "description_snippet": "We hire globally.\nApplication Question: Country of Residence"}
fails, n = [], 0


def verdict(loc, country):
    return classifier._keyword_classify_location_detail(dict(BASE, location=loc, country=country))[:2]


# duplicated country text -> same verdict as no country at all
for loc, country in [("Remote, Global", "Global"), ("Global", "Global"), ("Remote - Global", "Global"),
                     ("Worldwide", "Worldwide"), ("Remote, Worldwide", "Worldwide"), ("Anywhere", "Anywhere"),
                     ("Remote, EMEA", "EMEA"), ("EMEA", "EMEA"), ("Remote - Africa", "Africa"),
                     ("Remote, Global", "global"), ("Remote, Global", "Global, Global")]:
    n += 1
    got, want = verdict(loc, country), verdict(loc, "")
    if got != want:
        fails.append(f"{loc!r}+{country!r}: {got} != {want}")
n += 1
if verdict("Remote, Global", "Global") != ("match", "1"):
    fails.append(f"Remote, Global + Global should be Rank 1, got {verdict('Remote, Global', 'Global')}")
# a country field that adds NEW information still counts
for loc, country in [("Remote, Global", "United States"), ("Remote", "United States"), ("Berlin", "Germany")]:
    n += 1
    got = verdict(loc, country)
    if got[0] == "match" and country == "United States":
        fails.append(f"{loc!r}+{country!r} must not be a match, got {got}")
n += 1
if classifier.classify_rank4(dict(BASE, location="Remote, Global", country="Global"))[0] is not None and \
        verdict("Remote, Global", "Global")[0] != "match":
    fails.append("Rank 4 would still catch a job the keyword stage should have matched")
print(f"location-field checks: {n - len(fails)}/{n} passed")
for f in fails:
    print("  FAIL", f)
sys.exit(1 if fails else 0)
