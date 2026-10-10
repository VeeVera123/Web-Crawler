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

# ── HiBob / Deel (2026-10) ──
for fn, url, want in [
    (D._url_to_slug_hibob, "https://onwardmedical.careers.hibob.com/jobs/abc", "onwardmedical"),
    (D._url_to_slug_hibob, "https://swissto12-b67d359b42.careers.hibob.com", "swissto12-b67d359b42"),
    (D._url_to_slug_hibob, "https://careers.hibob.com/", None), (D._url_to_slug_hibob, "https://www.hibob.com/", None),
    (D._url_to_slug_deel, "https://jobs.deel.com/klarna", "klarna"),
    (D._url_to_slug_deel, "https://jobs.deel.com/dott/job-details/bb68d5b5-7e05/overview", "dott"),
    (D._url_to_slug_deel, "https://jobs.deel.com/job-boards/Domyn-SPA", "domyn-spa"),
    (D._url_to_slug_deel, "https://jobs.deel.com/", None), (D._url_to_slug_deel, "https://jobs.deel.com/login", None),
    (D._url_to_slug_deel, "https://www.deel.com/jobs/x", None),
]:
    check(fn(url) == want, f"{fn.__name__}({url}) = {fn(url)!r}, want {want!r}")
for ats in ("hibob", "deel"):
    check(ats in D.URL_TO_SLUG and ats in D.SUPPORTED_ATS and ats in A.SCRAPERS and ats in D._CC_LIVE_CHECK, f"{ats} registered")

import asyncio  # noqa: E402

HB = {"jobAdDetails": [
    {"id": "11", "title": "Customer Success Manager", "department": "CS", "employmentType": "Permanent", "site": "Remote", "country": "Portugal",
     "description": "<p>Own renewals.</p>", "responsibilities": "<ul><li>QBRs</li></ul>", "requirements": None, "benefits": "",
     "workspaceTypeId": "remote", "workspaceType": "Remote", "payTransparencyMinSalary": 50000, "payTransparencyMaxSalary": 70000,
     "payTransparencySalaryCurrency": "EUR"},
    {"id": "12", "title": "Office Manager", "site": "Lausanne", "country": "Switzerland", "description": "x", "workspaceTypeId": "on_site"},
    {"title": "no id"}]}
seen_headers = {}


async def fake_get(url, **kw):
    seen_headers.update(kw.get("headers") or {})
    return R("", 200, url, HB)


A._get = fake_get
hj = asyncio.run(A.scrape_hibob("Acme-Corp-0123456789"))
check(len(hj) == 2 and seen_headers.get("Referer") == "https://acme-corp-0123456789.careers.hibob.com/", f"hibob referer/count {seen_headers} {len(hj)}")
check(hj[0]["location"] == "Remote, Portugal" and hj[0]["workplace_type"] == "Remote" and "QBRs" in hj[0]["description_snippet"]
      and hj[0]["salary"].startswith("EUR 50000") and hj[0]["company"] == "Acme Corp", f"hibob fields {hj[0]}")
check(hj[1]["location"] == "Lausanne, Switzerland" and hj[1]["workplace_type"] == "On-site" and hj[1]["url"].endswith("/jobs/12"), f"hibob 2 {hj[1]}")
check(asyncio.run(A.scrape_hibob("Bad_Slug!")) == [], "hibob: bad slug")

DS = {"organizationId": "org1", "jobBoard": {"id": "board1"}, "preferredOrganizationName": "Klarna"}
DP = [{"id": "p1", "title": "Incident Manager", "richtextDescription": "<p>Run incidents.</p>", "isCompensationVisible": True,
       "job": {"workArrangementEnum": "HYBRID", "jobLocations": [{"location": {"name": "Stockholm"}}, {"location": {"name": "Berlin"}}],
               "jobEmploymentTypes": [{"employmentType": {"name": "Full-time"}}], "jobDepartments": [{"department": {"name": "Ops"}}],
               "currentCompensation": {"currencyIsoCode": "SEK", "minAmount": 1, "maxAmount": 2}}}, {"title": "no id"}]


async def fake_get2(url, **kw):
    if url.endswith("/career_page_settings"):
        return R("", 200, url, DS)
    assert "/org1/job_boards/board1/job_postings" in url
    return R("", 200, url, DP)


A._get = fake_get2
dj = asyncio.run(A.scrape_deel("Klarna"))
check(len(dj) == 1 and dj[0]["url"] == "https://jobs.deel.com/klarna/job-details/p1/overview" and dj[0]["location"] == "Stockholm; Berlin"
      and dj[0]["workplace_type"] == "Hybrid" and dj[0]["company"] == "Klarna" and dj[0]["department"] == "Ops" and "Run incidents" in dj[0]["description_snippet"], f"deel {dj}")


async def fake_404(url, **kw):
    return R("", 404, url, {})


A._get = fake_404
check(asyncio.run(A.scrape_deel("nobody")) == [] and asyncio.run(A.scrape_hibob("nobody")) == [], "unknown tenants -> []")

# ── ApplicantPro (2026-10) ──
for url, want in [("https://xbowsystems.applicantpro.com/jobs/4175504", "xbowsystems"), ("https://www.applicantpro.com/", None),
                  ("https://applicantpro.com/", None), ("https://app.applicantpro.com/x", None)]:
    check(D._url_to_slug_applicantpro(url) == want, f"applicantpro slug {url} -> {D._url_to_slug_applicantpro(url)!r}")
check("applicantpro" in D.URL_TO_SLUG and "applicantpro" in D.SUPPORTED_ATS and "applicantpro" in A.SCRAPERS
      and "applicantpro" in D._CC_LIVE_CHECK and "ApplicantPro" in A.DESCRIPTION_FETCHERS, "applicantpro registered")
APJ = {"success": True, "data": {"jobs": [
    {"id": 1, "title": "Customer Success Manager", "city": "Austin", "abbreviation": "TX", "iso3": "USA", "workplaceType": "Remote",
     "employmentType": "Full Time", "orgTitle": "Support", "parentTitle": "Acme Inc", "minSalary": "90,000", "maxSalary": "110,000",
     "payTypeFrame": "per year", "jobUrl": "https://acme.applicantpro.com/jobs/1"},
    {"id": 2, "title": "Welder", "city": "Mesa", "iso3": "USA", "workplaceType": "Onsite", "jobUrl": "https://acme.applicantpro.com/jobs/2"},
    {"id": 3, "title": "", "jobUrl": "https://acme.applicantpro.com/jobs/3"}]}}
ap_calls = []


async def fake_ap(url, **kw):
    ap_calls.append(url)
    if url.endswith("/jobs/"):
        return R('<script>x = {"domain_id":"3546","career_site_name":"Acme"}</script>', 200, url)
    assert "/core/jobs/3546" in url and "getParams" in (kw.get("params") or {})
    return R("", 200, url, APJ)


A._get = fake_ap
apj = asyncio.run(A.scrape_applicantpro("Acme"))
check(len(apj) == 2 and apj[0]["location"] == "Austin, TX, USA" and apj[0]["workplace_type"] == "Remote" and apj[0]["company"] == "Acme Inc"
      and apj[0]["salary"] == "90,000-110,000 per year" and apj[1]["workplace_type"] == "On-site" and apj[1]["location"] == "Mesa, USA", f"applicantpro {apj}")


async def fake_ap_none(url, **kw):
    return R("<html>no id</html>", 200, url)


A._get = fake_ap_none
check(asyncio.run(A.scrape_applicantpro("acme")) == [] and asyncio.run(A.scrape_applicantpro("Bad_Slug!")) == [], "applicantpro: no domain id / bad slug")

# ── Getro (2026-10) ──
GP = {"tenant_page": '<html><script id="__NEXT_DATA__" type="application/json">' + json.dumps({"props": {"pageProps": {"network": {"id": "36986"}}}}) + "</script></html>"}
GJOBS = {"results": {"count": 3, "jobs": [
    {"id": 1, "title": "Customer Success Manager", "url": "https://boards.greenhouse.io/pelago/jobs/1", "work_mode": "remote",
     "organization": {"name": "Pelago"}, "searchable_locations": ["Remote", "Europe"], "compensation_public": True,
     "compensation_amount_min_cents": 10000000, "compensation_amount_max_cents": 11500000, "compensation_currency": "USD", "compensation_period": "year"},
    {"id": 2, "title": "Account Manager", "url": "https://stripe.com/jobs/listing/2", "work_mode": "remote", "organization": {"name": "Stripe"}, "searchable_locations": ["United States"]},
    {"id": 1, "title": "Dup of 1", "url": "https://x.com/dup", "work_mode": "remote", "organization": {}},
    {"title": "no url"}]}}
posted = []


async def fake_getro_get(url, **kw):
    return R(GP["tenant_page"], 200, url) if url.startswith("https://mayfield.getro.com/") else R("", 404, url)


async def fake_getro_post(url, **kw):
    posted.append((url, kw["json"]))
    return R("", 200, url, GJOBS if kw["json"]["page"] == 0 else {"results": {"jobs": []}})


A._get, A._post = fake_getro_get, fake_getro_post
A._getro_network_ids.clear()
gj = asyncio.run(A.scrape_getro("Mayfield"))
check(all(u.endswith("/collections/36986/search/jobs") and b["filters"] == {"work_mode": ["remote"]} for u, b in posted) and len(posted) == len(A._GETRO_QUERIES),
      f"getro: tenant resolved to network id, one remote-filtered search per query term ({len(posted)} calls)")
check(len(gj) == 2 and gj[0]["company"] == "Pelago" and gj[0]["workplace_type"] == "Remote" and gj[0]["location"] == "Remote, Europe"
      and gj[0]["salary"] == "USD 100000-115000 per year" and gj[0]["slug"] == "Mayfield" and gj[0]["source_ats"] == "Getro", f"getro fields {gj}")
posted.clear()
check(len(asyncio.run(A.scrape_getro("36986"))) == 2 and "mayfield" in " ".join(A._getro_network_ids), "getro: numeric id used directly")
check(asyncio.run(A.scrape_getro("nobody-here")) == [] and asyncio.run(A.scrape_getro("")) == [], "getro: unknown tenant / empty -> []")
check("getro" in A.SCRAPERS and "getro" in D.SUPPORTED_ATS and D._url_to_slug_getro("https://mayfield.getro.com/jobs") == "mayfield", "getro registered")

print(f"new-platform checks: {n - len(fails)}/{n} passed")
for f in fails:
    print("  FAIL", f)
sys.exit(1 if fails else 0)
