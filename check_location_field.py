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
# A location FIELD that already states a broad scope is not narrowed by the separate country field: Ashby fills
# country from the employer's postal address (Pencil's EMEA role carries "European Union"), which is not the hiring
# scope. Same verdict as with no country at all.
for loc, country in [("EMEA", "European Union"), ("Remote - EMEA", "United Kingdom"), ("EMEA", "Germany"),
                     ("EMEA", "Kenya"), ("Remote, EMEA", "France"), ("APAC, EMEA", "Singapore"),
                     ("Remote, Global", "United States"), ("Worldwide", "Germany"), ("Africa", "European Union"),
                     ("Remote - Global", "United Kingdom"), ("Anywhere", "Canada")]:
    n += 1
    got, want = verdict(loc, country), verdict(loc, "")
    if got != want or got[0] != "match":
        fails.append(f"{loc!r}+{country!r} should equal the verdict without a country and match, got {got} (want {want})")
# a place written INSIDE the location field still narrows it
for loc in ["EMEA, Germany", "EMEA / European Union", "EMEA - UK only", "Global (US only)", "Remote, European Union",
            "Worldwide, United States only", "Europe"]:
    n += 1
    got = verdict(loc, "")
    if got[0] == "match":
        fails.append(f"{loc!r} names a place inside the location field and must not be a match, got {got}")
# a country field still counts when the location field is NOT a broad scope
for loc, country in [("Remote", "United States"), ("Berlin", "Germany"), ("Remote", "European Union"), ("Remote", "Kenya")]:
    n += 1
    got = verdict(loc, country)
    if got[0] == "match":
        fails.append(f"{loc!r}+{country!r} must not be a match, got {got}")
# ignoring the country field must not hide a real restriction: the description and the form still reject
for jd, label in [("This role is open to US residents only.", "description"),
                  ("Candidates must be located on the East Coast and within the Eastern Time Zone.", "description"),
                  ("We can only hire in the United States.\nApplication Question: Are you legally authorized to work in the United States?", "form")]:
    for loc, country in [("EMEA", "European Union"), ("Remote, Global", "United States")]:
        n += 1
        job = dict(BASE, location=loc, country=country, description_snippet=jd)
        got = classifier._keyword_classify_location_detail(job)[:2]
        if got[0] == "match":
            fails.append(f"{label} restriction {jd[:50]!r} must still reject {loc!r}+{country!r}, got {got}")
# the broad-scope test used for the country field never claims more than the classifier itself matches
for loc in ["EMEA", "Remote - EMEA", "Global", "Remote, Global", "Worldwide", "Anywhere", "Africa", "Remote - Africa",
            "APAC, EMEA", "MENA, AMER, EMEA, Latam", "EMEA-wide", "Global (Remote)", "International", "Work from anywhere",
            "EMEA, Germany", "Europe", "United States", "Remote", "Berlin", "South Africa", "Nigeria", ""]:
    n += 1
    if classifier._location_field_states_broad_scope(loc) and verdict(loc, "")[0] != "match":
        fails.append(f"_location_field_states_broad_scope({loc!r}) is True but the classifier does not match it: {verdict(loc, '')}")
n += 1
if classifier.classify_rank4(dict(BASE, location="Remote, Global", country="Global"))[0] is not None and \
        verdict("Remote, Global", "Global")[0] != "match":
    fails.append("Rank 4 would still catch a job the keyword stage should have matched")
print(f"location-field checks: {n - len(fails)}/{n} passed")
for f in fails:
    print("  FAIL", f)
sys.exit(1 if fails else 0)
