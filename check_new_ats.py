"""Offline checks for the 2026-10 platforms Emply, CATS, Elmo Talent and Easy Apply: URL -> slug extraction and the
parsing of each scraper against fixtures copied from real pages. No network.

    python check_new_ats.py
"""
import json
import os
import sys

for _k in ("SUPABASE_URL", "SUPABASE_KEY", "CEREBRAS_API_KEY", "GROQ_API_KEY", "GROQ_API_KEY_C",
           "OPENAI_API_KEY", "NVIDIA_API_KEY", "ANTHROPIC_API_KEY"):
    os.environ.setdefault(_k, "x")
import logging  # noqa: E402

logging.disable(logging.CRITICAL)
import ats_scrapers as A  # noqa: E402
import discovery as D  # noqa: E402

fails, n = [], 0


def check(cond, msg):
    global n
    n += 1
    if not cond:
        fails.append(msg)


# ── URL -> slug (URLs taken from archive_ii) ──
cases = [
    (D._url_to_slug_emply, "https://semcomaritime.career.emply.com/vacancies", "semcomaritime"),
    (D._url_to_slug_emply, "https://eltronicfueltech.career.emply.com/da/apply/unsolicited-applications-stores/35canc/en", "eltronicfueltech"),
    (D._url_to_slug_emply, "https://career.emply.com/", None), (D._url_to_slug_emply, "https://emply.com/", None),
    (D._url_to_slug_emply, "https://www.career.emply.com/x", None),
    (D._url_to_slug_cats, "https://theskagit.catsone.com/careers/22217/jobs/16831061-Facility-Maintenance-Mechanic/apply", "theskagit|22217"),
    (D._url_to_slug_cats, "https://willcor.catsone.com/careers/57887-willcor/jobs", "willcor|57887"),
    (D._url_to_slug_cats, "https://willcor.catsone.com/careers", None), (D._url_to_slug_cats, "https://www.catsone.com/careers/1/jobs", None),
    (D._url_to_slug_elmo, "https://steadfast.elmotalent.com.au/careers/default/job/view/297", "steadfast|default"),
    (D._url_to_slug_elmo, "https://covermore.elmotalent.com.au/careers/careers/jobs", "covermore|careers"),
    (D._url_to_slug_elmo, "https://greencollar.elmotalent.com.au/", None), (D._url_to_slug_elmo, "https://elmotalent.com.au/careers/x/jobs", None),
    (D._url_to_slug_easyapply, "https://upstreamdata.easyapply.co/", "upstreamdata"),
    (D._url_to_slug_easyapply, "https://kingsdiningentertainment.easyapply.co", "kingsdiningentertainment"),
    (D._url_to_slug_easyapply, "https://easyapply.co/job/draftsperson-5", None), (D._url_to_slug_easyapply, "https://www.easyapply.co/", None),
]
for fn, url, want in cases:
    check(fn(url) == want, f"{fn.__name__}({url}) = {fn(url)!r}, want {want!r}")
for ats in ("emply", "cats", "elmo", "easyapply"):
    check(ats in D.URL_TO_SLUG and ats in D.SUPPORTED_ATS and ats in A.SCRAPERS, f"{ats} registered")
check(A.DESCRIPTION_FETCHERS["CATS"] is A._fetch_cats_description and A.DESCRIPTION_FETCHERS["Elmo"] is A._fetch_elmo_description, "description fetchers")


class R:
    def __init__(self, text="", status=200, url="", data=None):
        self.text, self.status_code, self.url, self._d = text, status, url, data

    def json(self):
        return self._d


# ── CATS: one list page ──
CATS = ('<title>Careers | WILLCOR Inc</title><div class="header-row"><div class="header-cell">Job Title</div></div>'
        '<a class="table-row" href="/careers/57887/jobs/16850341-Lead-Engineering-Technician" data-discover="true">'
        '<div class="data-cell title-cell">Lead Engineering Technician (Machinery Control Systems)</div>'
        '<div class="data-cell" data-label="Category">Engineering</div><div class="data-cell" data-label="Location">Philadelphia, PA</div></a>'
        '<a class="table-row" href="/careers/57887/jobs/16850341-Lead-Engineering-Technician" data-discover="true"><div class="data-cell title-cell">dup</div></a>'
        '<a class="table-row" href="/careers/57887/jobs/16850338-Customer-Success-Manager"><div class="data-cell title-cell">Customer Success Manager &amp; Onboarding</div>'
        '<div class="data-cell" data-label="Category"></div><div class="data-cell" data-label="Location">Remote</div></a>')
A._get_requests_sync = lambda url, **kw: R(CATS) if "/careers/57887/jobs" in url else None
jobs = A.scrape_cats("WillCor|57887")
check(len(jobs) == 2 and jobs[0]["location"] == "Philadelphia, PA" and jobs[0]["department"] == "Engineering", f"cats rows: {jobs}")
check(jobs[0]["url"] == "https://willcor.catsone.com/careers/57887/jobs/16850341-Lead-Engineering-Technician" and jobs[0]["company"] == "WILLCOR Inc", f"cats url/company: {jobs[0]}")
check(jobs[1]["title"] == "Customer Success Manager & Onboarding" and jobs[1]["location"] == "Remote" and jobs[1]["source_ats"] == "CATS", f"cats row 2: {jobs[1]}")
check(A.scrape_cats("bad") == [] and A.scrape_cats("x|notanid") == [], "cats bad slugs")

# ── Elmo: paged list, location after the map-marker, employment type ──
def elmo_row(i, title, loc, emp):
    return (f'<div class="row"><p><a class="e-clickable redirect_elmo_link" data-url="/careers/careers/job/view/{i}" href="/careers/careers/job/view/{i}"> {title} </a>'
            f'<span class="hidden-xs">&nbsp;</span></p></div><div class="row"><div class="col-md-1"><strong><div class="glyphicon glyphicon-map-marker"></div></strong></div>'
            f'<div class="col-md-10"> {loc} </div></div><div class="row"><div class="col-md-10"> {emp} </div></div>')


PAGES = {1: "<title>COMPASS - Career - Browse Jobs</title>" + elmo_row(3398, "Salesforce Developer", "Uxbridge, Middlesex, United Kingdom", "Permanent - Full Time"),
         2: elmo_row(3443, "Account Manager", "Sydney, New South Wales", "Contract") + elmo_row(3398, "Salesforce Developer", "dup", "x"), 3: ""}
calls = []


def elmo_get(url, **kw):
    page = int(url.split("page=")[1]) if "page=" in url else 1
    calls.append(page)
    return R(PAGES[page]) if page in PAGES and PAGES[page] else R("", 404)


A._get_requests_sync = elmo_get
jobs = A.scrape_elmo("CoverMore|careers")
check([j["title"] for j in jobs] == ["Salesforce Developer", "Account Manager"], f"elmo titles/dedupe: {[j['title'] for j in jobs]}")
check(jobs[0]["location"] == "Uxbridge, Middlesex, United Kingdom" and jobs[0]["employment_type"] == "Permanent - Full Time", f"elmo loc/emp: {jobs[0]}")
check(jobs[1]["location"] == "Sydney, New South Wales" and jobs[1]["company"] == "Covermore" and jobs[0]["url"].endswith("/careers/careers/job/view/3398"), f"elmo row 2: {jobs[1]}")
check(calls == [1, 2, 3], f"elmo paging stops at the first empty page: {calls}")
check(A.scrape_elmo("bad") == [], "elmo bad slug")

# ── Easy Apply: one page, each job linked twice ──
def ea_row(slug, title, loc, emp):
    link = f'<a class="job_apply_link vega-default-link" target="_blank" href="https://easyapply.co/job/{slug}">{title}</a>'
    return (f'<div><h5 class="mb-3">{link}</h5><span class="tag"><i class="fa fa-map-marker"></i> {loc} </span>'
            f'<span class="tag"><i class="fa fa-clock-o"></i> {emp} </span><p>blurb</p></div><div>{link}</div>')


EA = "<title>View jobs at Upstream Data Inc</title><h1>Jobs at Upstream Data Inc</h1>" + ea_row("sheet-metal-fabricator-11", "Sheet Metal Fabricator", "Lloydminster, AB", "Full-time") + ea_row("account-manager-2", "Account &amp; Sales Manager", "Remote", "Part-time")
A._get_requests_sync = lambda url, **kw: R(EA) if "upstreamdata.easyapply.co" in url else R("", 302)
jobs = A.scrape_easyapply("UpstreamData")
check(len(jobs) == 2 and jobs[0]["location"] == "Lloydminster, AB" and jobs[0]["employment_type"] == "Full-time" and jobs[0]["company"] == "Upstream Data Inc", f"easyapply: {jobs}")
check(jobs[1]["title"] == "Account & Sales Manager" and jobs[1]["url"] == "https://easyapply.co/job/account-manager-2", f"easyapply row 2: {jobs[1]}")
check(A.scrape_easyapply("nobody") == [] and A.scrape_easyapply("www") == [] and A.scrape_easyapply("") == [], "easyapply unknown tenant / bad slug")

# ── Emply: sectionId page + vacancy API ──
PAGE = "<title>Career - BWS</title><script>var config = { count: 6, sectionId: '160c6408-cc84-4bea-ae7f-ab9b927020da', x: 1 };</script><div>© 2020 Semco Maritime A/S. All rights reserved. Powered by Emply</div>"
VAC = {"vacancies": [
    {"title": "Customer Success Manager", "titleAsUrl": "customer-success-manager", "shortId": "abc123", "location": "Copenhagen, Denmark", "department": "CS",
     "translations": [{"title": "x", "content": "<p>Own accounts. Salary $80,000 - $95,000.</p>"}]},
    {"title": "Unsolicited application", "titleAsUrl": "unsolicited", "shortId": "zzz999", "talentPool": True, "translations": []},
    {"title": "Customer Success Manager", "titleAsUrl": "customer-success-manager", "shortId": "abc123", "translations": []},
    {"title": "No id", "titleAsUrl": "x", "shortId": ""}]}


class Sess:
    def post(self, url, json=None, headers=None, timeout=None):
        assert url.endswith("/api/integration/vacancy/get-page") and json["sectionId"] == "160c6408-cc84-4bea-ae7f-ab9b927020da"
        return type("P", (), {"raise_for_status": lambda s: None, "json": lambda s: VAC})()


A._get_requests_sync = lambda url, **kw: R(PAGE, 200, url)
A._get_session = lambda: Sess()
jobs = A.scrape_emply("BWSGlobal")
check(len(jobs) == 1, f"emply: talent pool / duplicate / id-less dropped, got {len(jobs)}")
j = jobs[0]
check(j["url"] == "https://bwsglobal.career.emply.com/ad/customer-success-manager/abc123" and j["location"] == "Copenhagen, Denmark", f"emply url/loc: {j}")
check("Own accounts" in j["description_snippet"] and j["company"] == "Semco Maritime A/S" and j["source_ats"] == "Emply" and j["slug"] == "bwsglobal", f"emply fields: {j}")
A._get_requests_sync = lambda url, **kw: R("<html>", 200, "https://emply.com/")     # unknown tenant redirected to the marketing site
check(A.scrape_emply("nobody") == [], "emply: redirect to emply.com is an unknown tenant")
A._get_requests_sync = lambda url, **kw: R("<html>no section</html>", 200, "https://x.career.emply.com/vacancies")
check(A.scrape_emply("x") == [] and A.scrape_emply("Bad_Slug!") == [], "emply: no sectionId / bad slug")

print(f"new-platform checks: {n - len(fails)}/{n} passed")
for f in fails:
    print("  FAIL", f)
sys.exit(1 if fails else 0)
