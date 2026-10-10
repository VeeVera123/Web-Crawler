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
# EasyApply current markup (2026-10): row anchor wraps the row, title in span.font_18, icon-* classes
_EA_NEW = ('<h1>View jobs at ACME (X)</h1><div id="list"><a class="border_bottom font_6_grey job_row job_apply_link" target="_blank" href="https://easyapply.co/job/front-desk-1" style="display:block">'
           '<div class="no_word_break"><span class="font_18 vega-default-link" href="#">Front Desk</span></div>'
           '<p><i class="icon-map-marker"></i><span class="padding_left_5">New Orleans, LA</span></p><p>blurb</p></a></div>')
A._get_requests_sync = lambda url, **kw: R(_EA_NEW) if "newlayout.easyapply.co" in url else R("", 302)
_nj = A.scrape_easyapply("newlayout")
check(len(_nj) == 1 and _nj[0]["title"] == "Front Desk" and _nj[0]["location"] == "New Orleans, LA" and _nj[0]["url"].endswith("front-desk-1") and _nj[0]["company"] == "ACME (X)",
      f"easyapply: current wrapped-anchor markup {_nj}")
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
import re  # noqa: E402

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

# ── Traffit (2026-10) ──
for url, want in [("https://bat.traffit.com/career/", "bat"), ("https://www.traffit.com/", None), ("https://cdn3.traffit.com/x.js", None)]:
    check(D._url_to_slug_traffit(url) == want, f"traffit slug {url} -> {D._url_to_slug_traffit(url)!r}")
check("traffit" in D.URL_TO_SLUG and "traffit" in D.SUPPORTED_ATS and "traffit" in A.SCRAPERS and "traffit" in D._CC_LIVE_CHECK, "traffit registered")
TF = [{"url": "https://bat.traffit.com/public/an/abc?source=career_page", "advert": {"name": "Customer Success Manager",
      "values": [{"field_id": "description", "value": "<p>Own renewals.</p>"}, {"field_id": "requirements", "name": "Requirements:", "value": "<ul><li>QBRs</li></ul>"}],
      "locations": [{"locality": "Poznan", "country": "Polska"}]}, "options": {"_work_model": "Hybrid", "job_type": ["Full time"], "branches": ["Support"],
      "_Salary_MIN": "5000", "_Salary_MAX": "7000", "_Salary_Currency": "PLN", "_Salary_Rate": "Monthly"}},
      {"url": "https://bat.traffit.com/public/an/def", "advert": {"name": "Remote AM", "values": [], "locations": []}, "options": {"remote": "1"}}, {"advert": {"name": "no url"}}]
tf_pages = []


async def fake_tf(url, **kw):
    page = int((kw.get("headers") or {}).get("X-Request-Current-Page"))
    tf_pages.append(page)
    resp = R("", 200, url, TF[:2] if page == 1 else TF[1:2])
    resp.headers = {"x-result-total-pages": "2"}
    return resp


A._get = fake_tf
tfj = asyncio.run(A.scrape_traffit("BAT"))
check(tf_pages == [1, 2] and len(tfj) == 3 and tfj[0]["location"] == "Poznan, Polska" and tfj[0]["workplace_type"] == "Hybrid"
      and tfj[0]["department"] == "Support" and "QBRs" in tfj[0]["description_snippet"] and tfj[0]["salary"] == "PLN 5000-7000 Monthly"
      and tfj[1]["workplace_type"] == "Remote", f"traffit {tf_pages} {tfj}")
check(asyncio.run(A.scrape_traffit("Bad_Slug!")) == [], "traffit: bad slug")

# ── Freshteam / PeopleForce / Factorial / Loxo (HTML boards, 2026-10) ──
for fn, url, want in [
    (D._url_to_slug_freshteam, "https://ninjacart.freshteam.com/jobs/abc/x", "ninjacart"), (D._url_to_slug_freshteam, "https://support.freshteam.com/a", None),
    (D._url_to_slug_peopleforce, "https://takenos.peopleforce.io/careers/v/1-x", "takenos"), (D._url_to_slug_peopleforce, "https://peopleforce.io/", None),
    (D._url_to_slug_factorial, "https://digitail.factorial.com/job_posting/x-1", "digitail"), (D._url_to_slug_factorial, "https://www.factorial.com/", None),
    (D._url_to_slug_loxo, "https://app.loxo.co/mastec-purnell-canada-inc", "mastec-purnell-canada-inc"),
    (D._url_to_slug_loxo, "https://app.loxo.co/job/MzE0=", None), (D._url_to_slug_loxo, "https://www.loxo.co/", None),
]:
    check(fn(url) == want, f"{fn.__name__}({url}) = {fn(url)!r}, want {want!r}")
for ats in ("freshteam", "peopleforce", "factorial", "loxo"):
    check(ats in D.URL_TO_SLUG and ats in D.SUPPORTED_ATS and ats in A.SCRAPERS and ats in D._CC_LIVE_CHECK, f"{ats} registered")
FT = ('<div class="job-list"><a href="/jobs/-QkLte4ZHEi9/hands-on-frontend-tech-lead" data-portal-location="Tel Aviv, Israel" data-portal-remote-location="false">'
      '<div class="row"><div class="job-title">Hands-on Frontend Tech Lead</div><div class="job-location">Tel Aviv</div></div></a>'
      '<a href="/jobs/vSPE4N8_ul1R/xp-implementation-consultant" data-portal-location="Remote" data-portal-remote-location="true"><div class="job-title">Implementation Consultant</div></a>'
      '<a href="/jobs/x">bad</a></div>')
PF = ('<div class="tw-p-4"><h4><a href="/careers/v/241620-paid-media-analyst">Paid Media Analyst</a></h4><div class="small"><div><i class="fas fa-briefcase fa-fw"></i>Growth <span>·</span></div>'
      '<div><i class="fas fa-clock fa-fw"></i>Full-time <span>·</span></div><div><i class="fas fa-map-marker-alt fa-fw"></i>Buenos Aires </div></div></div>')
FA = ('<div class="row"><span class="md:w-3/6"><div class="font-bold">Platform Support Engineer </div></span><div class="flex-grow md:w-1/6"><div>Platform Engineering </div></div>'
      '<div class="flex-grow md:w-1/6"><div>Hybrid </div></div><div><a href="https://digitail.factorial.com/job_posting/platform-support-engineer-325008">Apply now </a></div></div>')
LX = '<div class="data-cell"><a class="job-title" href="/job/MzE0NzMtM3ppaWd0eGJvaHl4cDF5ag==">MasTec - Apprentice Pipefitter</a></div>'
HTMLS = {"freshteam": FT, "peopleforce": PF, "factorial": FA, "loxo": LX}


def make_html_get(kind):
    async def g(url, **kw):
        return R(HTMLS[kind], 200, url)
    return g


for kind in HTMLS:
    A._get = make_html_get(kind)
    hj = asyncio.run(getattr(A, "scrape_" + kind)("Acme"))
    if kind == "freshteam":
        check(len(hj) == 2 and hj[0]["location"] == "Tel Aviv, Israel" and hj[0]["title"] == "Hands-on Frontend Tech Lead"
              and hj[1]["workplace_type"] == "Remote" and hj[0]["url"] == "https://acme.freshteam.com/jobs/-QkLte4ZHEi9/hands-on-frontend-tech-lead", f"freshteam {hj}")
    elif kind == "peopleforce":
        check(len(hj) == 1 and hj[0]["location"] == "Buenos Aires" and hj[0]["department"] == "Growth" and hj[0]["employment_type"] == "Full-time"
              and hj[0]["title"] == "Paid Media Analyst", f"peopleforce {hj}")
    elif kind == "factorial":
        check(len(hj) == 1 and hj[0]["title"] == "Platform Support Engineer" and hj[0]["department"] == "Platform Engineering"
              and hj[0]["workplace_type"] == "Hybrid", f"factorial {hj}")
    else:
        check(len(hj) == 1 and hj[0]["url"] == "https://app.loxo.co/job/MzE0NzMtM3ppaWd0eGJvaHl4cDF5ag==" and hj[0]["title"].endswith("Pipefitter"), f"loxo {hj}")
    check(asyncio.run(getattr(A, "scrape_" + kind)("Bad Slug!")) == [], f"{kind}: bad slug")

# ── Recruiterflow / Homerun (2026-10) ──
for fn, url, want in [
    (D._url_to_slug_recruiterflow, "https://recruiterflow.com/acme-corp/jobs/12", "acme-corp"), (D._url_to_slug_recruiterflow, "https://recruiterflow.com/acme/jobs", "acme"),
    (D._url_to_slug_recruiterflow, "https://recruiterflow.com/blog/jobs", None), (D._url_to_slug_recruiterflow, "https://recruiterflow.com/pricing", None),
    (D._url_to_slug_recruiterflow, "https://example.com/acme/jobs", None),
    (D._url_to_slug_homerun, "https://chillhop-music.homerun.co/", "chillhop-music"), (D._url_to_slug_homerun, "https://jobs.homerun.co/sales-exec", None),
    (D._url_to_slug_homerun, "https://www.homerun.co/", None),
]:
    check(fn(url) == want, f"{fn.__name__}({url}) = {fn(url)!r}, want {want!r}")
for ats in ("recruiterflow", "homerun"):
    check(ats in D.URL_TO_SLUG and ats in D.SUPPORTED_ATS and ats in A.SCRAPERS and ats in D._CC_LIVE_CHECK, f"{ats} registered")
RFJ = {"department": [["Admin", [{"apply_link": "acme/jobs/1", "details": "Gotham", "employment_type": "Full time", "job_id": 1, "job_name": "Security Specialist", "remote_type": None}]],
                      ["Support", [{"apply_link": "acme/jobs/9", "details": "Remote - EMEA", "employment_type": "Full time", "job_id": 9, "job_name": "Customer Success Manager", "remote_type": "Remote"},
                                   {"job_name": "broken"}]]]}
RF = "<html><script>window.jobsList = " + json.dumps(RFJ) + ";\nvar other = {a: 1};</script></html>"


async def fake_rf(url, **kw):
    return R(RF, 200, url)


A._get = fake_rf
rfj = asyncio.run(A.scrape_recruiterflow("Acme"))
check(len(rfj) == 2 and rfj[1]["url"] == "https://recruiterflow.com/acme/jobs/9" and rfj[1]["location"] == "Remote - EMEA" and rfj[1]["workplace_type"] == "Remote"
      and rfj[1]["department"] == "Support" and rfj[0]["title"] == "Security Specialist", f"recruiterflow {rfj}")
A._get = fake_ap_none
check(asyncio.run(A.scrape_recruiterflow("acme")) == [] and asyncio.run(A.scrape_recruiterflow("Bad Slug!")) == [], "recruiterflow: no embed / bad slug")
HRP = {"content": {"vacancies": [{"id": 1, "title": "Open application", "location_id": 5, "department_id": None, "url": "https://jobs.homerun.co/open/en"},
                                 {"id": 2, "title": "Customer Success Specialist", "location_id": 5, "department_id": 7, "url": "https://jobs.homerun.co/customer-success-specialist"}],
                  "departments": [{"id": 7, "name": "Customer"}], "locations": [{"id": 5, "name": "Amsterdam"}], "job_types": []}}
import html as _html  # noqa: E402
HR = '<section id="job-list"><job-list v-bind="' + _html.escape(json.dumps(HRP), quote=True) + '"></job-list></section>'


async def fake_hr(url, **kw):
    return R(HR, 200, "https://acme.homerun.co/")


A._get = fake_hr
hrj = asyncio.run(A.scrape_homerun("Acme"))
check(len(hrj) == 1 and hrj[0]["title"] == "Customer Success Specialist" and hrj[0]["location"] == "Amsterdam" and hrj[0]["department"] == "Customer"
      and hrj[0]["url"] == "https://jobs.homerun.co/customer-success-specialist", f"homerun {hrj}")


async def fake_hr404(url, **kw):
    return R(HR, 200, "https://404.homerun.co/working_at/acme")


A._get = fake_hr404
check(asyncio.run(A.scrape_homerun("acme")) == [] and asyncio.run(A.scrape_homerun("Bad Slug!")) == [], "homerun: unknown tenant / bad slug")

# ── Keka Hire (2026-10) ──
for url, want in [("https://inc42.keka.com/careers", "inc42"), ("https://techdome.keka.com/careers/jobdetails/159380", "techdome"),
                  ("https://acme.keka.com/", None), ("https://www.keka.com/careers", None), ("https://help.keka.com/careers", None)]:
    check(D._url_to_slug_keka(url) == want, f"keka slug {url} -> {D._url_to_slug_keka(url)!r}")
check("keka" in D.URL_TO_SLUG and "keka" in D.SUPPORTED_ATS and "keka" in A.SCRAPERS and "keka" in D._CC_LIVE_CHECK, "keka registered")
KK = [{"id": 164639, "title": "Associate - Client Management", "jobLocations": [{"city": "New Delhi", "countryName": "India"}], "description": "<p>Run accounts.</p>",
       "departmentName": "BrandLabs", "jobType": 2, "salaryRange": {"minimum": 0, "maximum": 0}}, {"id": 2, "title": "", "description": "x"}, {"title": "no id"}]


async def fake_keka(url, **kw):
    if url.endswith("/careers/"):
        return R('<link href="/ats/documents/0a1b2c3d-1111-2222-3333-444455556666/logo.png">', 200, url)
    assert url.endswith("/api/embedjobs/default/active/0a1b2c3d-1111-2222-3333-444455556666")
    return R("", 200, url, KK)


A._get = fake_keka
kj = asyncio.run(A.scrape_keka("Inc42"))
check(len(kj) == 1 and kj[0]["url"] == "https://inc42.keka.com/careers/jobdetails/164639" and kj[0]["location"] == "New Delhi, India"
      and kj[0]["employment_type"] == "Full time" and kj[0]["department"] == "BrandLabs" and "Run accounts" in kj[0]["description_snippet"], f"keka {kj}")


async def fake_keka_nf(url, **kw):
    return R("", 200, "https://acme.keka.com/careers/Content/TenantNotFound.html")


A._get = fake_keka_nf
check(asyncio.run(A.scrape_keka("acme")) == [] and asyncio.run(A.scrape_keka("Bad Slug!")) == [], "keka: unknown tenant / bad slug")

# ── Jobsoid (2026-10) ──
for url, want in [("https://music-ministry.jobsoid.com/j/1/x", "music-ministry"), ("https://www.jobsoid.com/", None), ("https://resources.jobsoid.com/a", None), ("https://portal.jobsoid.com/", None)]:
    check(D._url_to_slug_jobsoid(url) == want, f"jobsoid slug {url} -> {D._url_to_slug_jobsoid(url)!r}")
check("jobsoid" in D.URL_TO_SLUG and "jobsoid" in D.SUPPORTED_ATS and "jobsoid" in A.SCRAPERS and "jobsoid" in D._CC_LIVE_CHECK, "jobsoid registered")
JS = [{"id": "86833", "title": "Protestant Coordinator", "description": "<p>Lead worship.</p>", "location": {"title": "NSA Naples", "city": "Gricignano", "state": "", "country": "Italy"},
       "department": {"title": "Religious Education"}, "type": "Contract", "salary": "", "hostedUrl": "https://mm.jobsoid.com/j/86833/x", "company": "Music Ministry"},
      {"id": "2", "title": "No url"}, {"id": "3", "title": "Remote Coordinator", "description": "x", "location": {"title": "Remote"}, "hostedUrl": "https://mm.jobsoid.com/j/3/y"}]


async def fake_js(url, **kw):
    return R("", 200, url, JS)


A._get = fake_js
jj = asyncio.run(A.scrape_jobsoid("MM"))
check(len(jj) == 2 and jj[0]["location"] == "Gricignano, Italy" and jj[0]["department"] == "Religious Education" and jj[0]["company"] == "Music Ministry"
      and "Lead worship" in jj[0]["description_snippet"] and jj[1]["location"] == "Remote", f"jobsoid {jj}")
check(asyncio.run(A.scrape_jobsoid("Bad Slug!")) == [], "jobsoid: bad slug")

# ── Gem: per-job URLs + application questions (2026-10) ──
GEM_LIST = [{"data": {"oatsExternalJobPostings": {"jobPostings": [
    {"id": "T2F0cw==", "extId": "ext_one", "title": "Customer Success Manager", "locations": [{"name": "US - Remote", "isRemote": True}], "job": {"department": {"name": "CS"}, "employmentType": "FULL_TIME"}},
    {"id": "T2F0cx==", "extId": "ext_two", "title": "Account Manager", "locations": [{"name": "London", "isRemote": False}], "job": {}}]}}}]
GEM_Q = [{"data": {"oatsJobPostFieldsAndQuestions": {"fields": [], "questions": [
    {"text": "Are you authorized to work in the US or Canada?", "isRequired": True, "options": [{"value": "Yes"}, {"value": "No"}]},
    {"text": "Email", "isRequired": True, "options": []}]}}}]
gem_calls = []


def fake_gem(ops):
    gem_calls.append(ops[0]["operationName"])
    if ops[0]["operationName"] == "JobBoardList":
        return GEM_LIST
    if ops[0]["operationName"] == "JobQuestions":
        assert ops[0]["variables"] == {"boardId": "acme", "extId": "ext_one"}, ops[0]["variables"]
        return GEM_Q
    return [{"data": {"oatsExternalJobPosting": {"descriptionHtml": "<p>Own renewals for customers.</p>"}}} for _ in ops]


A._gem_graphql_batch = fake_gem
gj = A.scrape_gem("acme")
check(len(gj) == 2 and gj[0]["url"] == "https://jobs.gem.com/acme/ext_one" and gj[1]["url"] == "https://jobs.gem.com/acme/ext_two"
      and len({j["url"] for j in gj}) == 2, f"gem: every job has its own deep link {[j['url'] for j in gj]}")
gq = A._fetch_gem_questions(gj[0])
check("Application Question: Are you authorized to work in the US or Canada?" in gq and "Application Question: Email" not in gq
      and gj[0].get("_form_status") == "ok" and "Gem" in A.QUESTION_FETCHERS, f"gem questions {gq!r}")
check(A._fetch_gem_questions({"url": "https://jobs.gem.com/acme"}) == "", "gem: board-level url -> no questions")

# ── HiBob / Deel application questions (2026-10) ──
HBF = {"data": {"jobAd": {"applicationForm": {"/applicationForm/questions": {"value": [
    {"/question/text": {"value": "Please confirm your eligibility to work in Switzerland"}, "/question/isMandatory": {"value": True},
     "/question/options": {"value": [{"/questionOption/text": {"value": "I am a Swiss citizen"}}, {"/questionOption/text": {"value": "I am not eligible"}}]}}]},
    "/applicationForm/fields": {"value": []}}}}}
hb_seen = {}


def fake_sync_hb(url, **kw):
    hb_seen["url"], hb_seen["ref"] = url, (kw.get("headers") or {}).get("Referer")
    return R("", 200, url, HBF)


_orig_sync = A._get_requests_sync
A._get_requests_sync = fake_sync_hb
hjob = {"url": "https://acme.careers.hibob.com/jobs/44a2ddf2-f5a4-42ba-a19b-45d5c9aed2f3"}
hq = A._fetch_hibob_questions(hjob)
check("Application Question: Please confirm your eligibility to work in Switzerland" in hq and "I am a Swiss citizen" in hq and hjob.get("_form_status") == "ok"
      and hb_seen["url"].endswith("/api/job-ad/44a2ddf2-f5a4-42ba-a19b-45d5c9aed2f3/application-form") and hb_seen["ref"] == "https://acme.careers.hibob.com/", f"hibob questions {hq!r} {hb_seen}")
HBF["data"]["jobAd"]["applicationForm"]["/applicationForm/questions"]["value"] = []
hjob2 = {"url": "https://acme.careers.hibob.com/jobs/44a2ddf2-f5a4-42ba-a19b-45d5c9aed2f3"}
check(A._fetch_hibob_questions(hjob2) == "" and hjob2.get("_form_status") == "ok", "hibob: read form with no custom questions is still a read form")
check(A._fetch_hibob_questions({"url": "https://example.com/x"}) == "", "hibob: non-hibob url")
DEEL_FORM = {"id": "f", "pages": [{"id": "p", "sections": [{"id": "s", "questions": [
    {"title": "Will you require visa sponsorship to work in the location where this job is based?", "type": "SingleSelection", "isRequired": True,
     "options": [{"title": "Yes"}, {"title": "No"}]}, {"title": "First name", "type": "Text", "isRequired": True}]}]}]}
DEEL_HTML = "<html><script>self.__next_f.push([1," + json.dumps(json.dumps(DEEL_FORM)) + "])</script></html>"
deel_seen = {}


def fake_sync_deel(url, **kw):
    deel_seen["url"] = url
    return R(DEEL_HTML, 200, url)


A._get_requests_sync = fake_sync_deel
djob = {"url": "https://jobs.deel.com/klarna/job-details/396853ef-cdd6/overview"}
dq = A._fetch_deel_questions(djob)
check("Application Question: Will you require visa sponsorship" in dq and "Application Question: First name" not in dq and djob.get("_form_status") == "ok"
      and deel_seen["url"] == "https://jobs.deel.com/klarna/job-details/396853ef-cdd6/application", f"deel questions {dq!r} {deel_seen}")
A._get_requests_sync = lambda url, **kw: R("<html>nothing</html>", 200, url)
check(A._fetch_deel_questions({"url": "https://jobs.deel.com/klarna/job-details/x/overview"}) == "", "deel: page without a form payload")
A._get_requests_sync = _orig_sync
check("HiBob" in A.QUESTION_FETCHERS and "Deel" in A.QUESTION_FETCHERS, "hibob/deel question fetchers registered")

# ── GitHub registry parsers: URL-record and txt slug lists (2026-10) ──
ur = D._parse_url_records(json.dumps([
    {"name": "Cusmat", "ats_links": ["https://boards.greenhouse.io/cusmat", "https://cusmat.com/careers/"], "website": "https://cusmat.com"},
    {"name": "Acme", "ats_links": ["https://jobs.lever.co/acme"]}, {"name": "Nope", "ats_links": ["https://example.com/jobs"]}]), "t/outscal")
check(ur.get("greenhouse", {}).get("cusmat") == "Cusmat" and ur.get("lever", {}).get("acme") == "Acme" and sum(len(v) for v in ur.values()) == 2, f"url_records json list {ur}")
ur2 = D._parse_url_records(json.dumps({"ethena": {"name": "ethena", "jobs_url": "https://jobs.lever.co/ethena", "company_url": "https://www.goethena.com"}}), "t/crypto")
check(ur2 == {"lever": {"ethena": "ethena"}}, f"url_records name-keyed dict {ur2}")
ur3 = D._parse_url_records("company_slug,board_url,monitor_type\nairbus,https://airbusspaceanddefense.applicantpro.com/jobs/,api_sniffer\nx,https://app.loxo.co/mastec,dom\n", "t/jobseek")
check(ur3.get("applicantpro", {}).get("airbusspaceanddefense") is not None and "mastec" in ur3.get("loxo", {}), f"url_records csv {ur3}")
check(D._parse_url_records("not json {", "t/bad") == {} or isinstance(D._parse_url_records("not json {", "t/bad"), dict), "url_records: bad input does not raise")
_orig_fetch = D._github_registry_fetch
D._github_registry_fetch = lambda reg: {"a.txt": "acme\n# c\n\nbeta-co\n"}.get(reg["path"])
tx = D._parse_txt_slug_files({"repo": "t/u", "branch": "main", "files": {"greenhouse": "a.txt", "lever": "missing.txt"}})
check(set(tx.get("greenhouse", {})) == {"acme", "beta-co"} and "lever" not in tx, f"txt_slugs {tx}")
D._github_registry_fetch = _orig_fetch
check(any(r.get("format") == "url_records" for r in D.GITHUB_REGISTRY_REPOS) and any(r.get("format") == "txt_slugs" for r in D.GITHUB_REGISTRY_REPOS), "new registries listed")

# ── Remote.com virtual board (2026-10) ──
RC = [
    {"status": "published", "title": "Customer Success Manager", "slug": "csm-j1", "department": {"name": "Support"}, "employment_type": "full_time",
     "company_profile": {"name": "Acme", "slug": "acme-c1"}, "workplace_location": {"type": "remote"},
     "hiring_location": {"type": "location", "included_locations": [{"type": "country", "value": {"name": "South Africa"}}, {"type": "country", "value": {"name": "Kenya"}}]},
     "compensation": {"minimum": 1100000, "maximum": 1600000, "frequency": "yearly", "currency": {"code": "USD"}}},
    {"status": "published", "title": "Account Manager", "slug": "am-j2", "company_profile": {"name": "Beta", "slug": "beta-c2"}, "workplace_location": {"type": "remote"},
     "hiring_location": {"type": "global"}},
    {"status": "published", "title": "Sales Ops", "slug": "so-j3", "company_profile": {"name": "Gamma", "slug": "gamma-c3"}, "workplace_location": {"type": "remote"},
     "hiring_location": {"type": "timezone", "timezone": {"offset": -6.0}, "timezone_range": 2}},
    {"status": "published", "title": "Engineer", "slug": "en-j4", "company_profile": {"name": "Delta", "slug": "delta-c4"}, "hiring_location": {},
     "workplace_location": {"type": "hybrid", "city": "Hanoi", "country": {"name": "Vietnam"}}},
    {"status": "draft", "title": "Hidden", "slug": "h-j5", "company_profile": {"name": "E", "slug": "e-c5"}}]
rc_calls = []


async def fake_rc(url, **kw):
    rc_calls.append((kw.get("params") or {}).get("page"))
    return R("", 200, url, {"data": {"jobs": RC, "total_pages": 1, "total_count": 5, "current_page": 1}})


A._get = fake_rc
rcj = {j["title"]: j for j in asyncio.run(A.scrape_remote("global"))}
check(len(rcj) == 4 and "Hidden" not in rcj, f"remote.com: drafts dropped {list(rcj)}")
check(rcj["Customer Success Manager"]["location"] == "Remote - South Africa, Kenya" and rcj["Customer Success Manager"]["salary"] == "USD 11,000-16,000 yearly"
      and rcj["Customer Success Manager"]["url"] == "https://remote.com/jobs/acme-c1/csm-j1" and rcj["Customer Success Manager"]["employment_type"] == "Full time", f"remote.com fields {rcj['Customer Success Manager']}")
check(rcj["Account Manager"]["location"] == "Remote - Worldwide", "remote.com: global -> Remote - Worldwide")
check(rcj["Sales Ops"]["location"] == "Remote (time zones UTC-8 to UTC-4 only)", f"remote.com tz {rcj['Sales Ops']['location']}")
check(rcj["Engineer"]["location"] == "Hanoi, Vietnam" and rcj["Engineer"]["workplace_type"] == "Hybrid", f"remote.com hybrid {rcj['Engineer']}")
check(asyncio.run(A.scrape_remote("other")) == [] and ("remote", "global") in A.VIRTUAL_BOARDS and "Remote.com" in A.DESCRIPTION_FETCHERS, "remote.com registered as a virtual board")


async def fake_rc_detail(url, **kw):
    assert url.endswith("/public/jobs/acme-c1/csm-j1"), url
    return R("", 200, url, {"data": {"description": "<p>Own renewals for customers.</p>"}})


A._get = fake_rc_detail
check("Own renewals" in asyncio.run(A._fetch_remote_com_description({"url": "https://remote.com/jobs/acme-c1/csm-j1"})), "remote.com description fetch")
# Rank 4 eligibility list (2026-10 probe)
import classifier as _C  # noqa: E402
check({"Gem", "HiBob", "Deel", "Paylocity", "Dayforce", "Cornerstone OnDemand", "CareerPlug"} <= _C.RANK4_ELIGIBLE_ATS
      and not ({"SmartRecruiters", "JOIN", "Workday", "iCIMS"} & _C.RANK4_ELIGIBLE_ATS), "rank 4: probed platforms eligible, blocked ones not")

# ── Paylocity application questions (2026-10) ──
PAYD = {"screener": {"title": "Director", "questions": [
    {"title": "Are you legally authorized to work in the United States?", "isRequired": True, "answers": [{"title": "Yes"}, {"title": "No"}], "data": "<data></data>"},
    {"title": "Do you have a Bachelor's degree?", "isRequired": True, "data": "<data><answers><title>Yes</title></answers><answers><title>No</title></answers></data>"}]}}
PAYH = "<html><script> window.pageData = " + json.dumps(PAYD) + "; var x = 1;</script></html>"
pay_seen = {}


def fake_sync_pay(url, **kw):
    pay_seen["url"] = url
    return R(PAYH, 200, url)


A._get_requests_sync = fake_sync_pay
pj = {"url": "https://recruiting.paylocity.com/recruiting/jobs/Details/4563618/Winthrop"}
pq = A._fetch_paylocity_questions(pj)
check("Application Question: Are you legally authorized to work in the United States?" in pq and "Bachelor" in pq and pj.get("_form_status") == "ok"
      and pay_seen["url"] == "https://recruiting.paylocity.com/Recruiting/jobs/Apply/4563618", f"paylocity {pq!r} {pay_seen}")
PAYD["screener"]["questions"] = []
pj2 = {"url": "https://recruiting.paylocity.com/recruiting/jobs/Details/1/x"}
PAYH = "<html><script> window.pageData = " + json.dumps(PAYD) + ";</script></html>"
check(A._fetch_paylocity_questions(pj2) == "" and pj2.get("_form_status") == "ok", "paylocity: read form with no screener")
A._get_requests_sync = _orig_sync

# ── Dayforce application questions (2026-10) ──
DFA = {"sections": [
    {"xRefCode": "PERSONALINFORMATION", "fields": [{"x": 1}], "questionnaire": None},
    {"xRefCode": None, "displayName": "References", "questionnaire": {"displayName": "References", "questions": [{"description": "References will be obtained at offer.", "options": []}]}},
    {"xRefCode": None, "displayName": "Additional Questions", "questionnaire": {"displayName": "Additional Questions", "questions": [
        {"displayName": "Right to Work", "description": "Are you authorised to work in the UK?", "isRequired": True, "options": [{"displayName": "Yes"}, {"displayName": "No"}]},
        {"displayName": "Passport", "description": "", "isRequired": False, "options": []}]}}]}
df_seen = {}


def fake_sync_df(url, **kw):
    if "/sitecontext/" in url:
        return R("", 200, url, {"jobBoardId": 1})
    df_seen["url"] = url
    return R("", 200, url, DFA)


A._get_requests_sync = fake_sync_df
dfj = {"url": "https://jobs.dayforcehcm.com/en-US/caciltd/CANDIDATEPORTAL/jobs/3295"}
dq = A._fetch_dayforce_questions(dfj)
check("Application Question: Are you authorised to work in the UK?" in dq and "References will be obtained" not in dq and "Application Question: Passport" in dq
      and dfj.get("_form_status") == "ok" and df_seen["url"] == "https://jobs.dayforcehcm.com/api/geo/caciltd/jobapplication/caciltd/en-GB/1/3295", f"dayforce {dq!r} {df_seen}")
check(A._fetch_dayforce_questions({"url": "https://example.com/x"}) == "" and "Dayforce" in A.QUESTION_FETCHERS, "dayforce: other url / registered")
A._dayforce_board_ids.clear()
A._get_requests_sync = lambda url, **kw: R("", 200, url, {"jobBoardId": 4}) if "/sitecontext/" in url else fake_sync_df(url, **kw)
dfj2 = {"url": "https://jobs.dayforcehcm.com/en-US/dcrusa/Join-us/jobs/2996"}
A._fetch_dayforce_questions(dfj2)
check(df_seen["url"] == "https://jobs.dayforcehcm.com/api/geo/dcrusa/jobapplication/dcrusa/en-GB/4/2996", f"dayforce custom board id {df_seen}")
A._get_requests_sync = _orig_sync

# ── CSOD application questions (2026-10) ──
CSW = {"data": [{"totalPages": 2, "applicationId": 0, "actions": [
    {"type": "contactInformation"},
    {"type": "prescreeningQuestions", "section": {"questions": [
        {"text": "Are you legally authorized to work in the United States?", "isRequired": True, "options": [{"text": "Yes"}, {"text": "No"}]}]}},
    {"type": "eeoQuestions", "eeoQuestions": [{"text": "Gender"}]}]}]}
cs_urls = []


def fake_sync_cs(url, **kw):
    cs_urls.append(url)
    if "/home?c=" in url:
        return R('<script>var b = {"token":"eyJabc.def.ghi","cloud":"https://us.api.csod.com/"}</script>', 200, url)
    assert (kw.get("headers") or {}).get("Authorization") == "Bearer eyJabc.def.ghi"
    return R("", 200, url, CSW)


A._get_requests_sync = fake_sync_cs
csj = {"url": "https://turner.csod.com/ux/ats/careersite/1/home/requisition/21960?c=turner"}
csq = A._fetch_csod_questions(csj)
check("Application Question: Are you legally authorized to work in the United States?" in csq and "Gender" not in csq and csj.get("_form_status") == "ok"
      and any("/jobrequisition/21960/page/2" in u for u in cs_urls), f"csod {csq!r} {cs_urls}")
check(A._fetch_csod_questions({"url": "https://example.com/x"}) == "" and "Cornerstone OnDemand" in A.QUESTION_FETCHERS, "csod: other url / registered")
A._get_requests_sync = _orig_sync

# ── CareerPlug (2026-10) ──
for url, want in [("https://iaqa-careers.careerplug.com/jobs/123", "iaqa-careers"), ("https://app.careerplug.com/", None), ("https://support.careerplug.com/x", None), ("https://www.careerplug.com/careers/", None)]:
    check(D._url_to_slug_careerplug(url) == want, f"careerplug slug {url} -> {D._url_to_slug_careerplug(url)!r}")
check("careerplug" in D.URL_TO_SLUG and "careerplug" in D.SUPPORTED_ATS and "careerplug" in A.SCRAPERS and "careerplug" in D._CC_LIVE_CHECK, "careerplug registered")
check(A._careerplug_place("SC-Columbia-29205") == "Columbia, SC" and A._careerplug_place("Remote") == "Remote" and A._careerplug_place("ON-Toronto-M5V 2T6") == "Toronto, ON", "careerplug place format")
CPH = ('<div id="job_table"><div><a aria-label="Pool Design Consultant" href="/jobs/3382940"><div class="row"><div class="job-title col-sm-7"><span class="name">Pool Design Consultant</span></div>'
       '<div class="job-location"><div><span class="job-row-title">Location:</span> SC-Columbia-29205 </div></div></div></a></div>'
       '<div class="row"><div class="job-title"><a href="/jobs/3633705"><span class="name">Account Manager</span></a></div><div class="job-location">TX-Austin-78701</div><div class="job-type">Full Time</div></div>'
       '<a href="/jobs/3382940/apps/new">Apply</a></div>')


async def fake_cp(url, **kw):
    return R(CPH, 200, url)


A._get = fake_cp
cpj = asyncio.run(A.scrape_careerplug("Acme"))
check(len(cpj) == 2 and cpj[0]["title"] == "Pool Design Consultant" and cpj[0]["location"] == "Columbia, SC" and cpj[0]["url"] == "https://acme.careerplug.com/jobs/3382940"
      and cpj[1]["location"] == "Austin, TX" and cpj[1]["employment_type"] == "Full Time", f"careerplug {cpj}")

# ── CareerPlug application questions (2026-10) ──
CPF = ('<form><input name="app[applicant_attributes][firstname]"/>'
       '<input type="hidden" name="app[answer_sets_attributes][0][question_id]" value="1"/>'
       '<div class="select input required form-group"><span class="form-label"><label for="app_answer_sets_attributes_0_answer_id">Do you hold a valid driver\u2019s license?<span title="required">*</span></label></span>'
       '<select name="app[answer_sets_attributes][0][answer_id]" id="app_answer_sets_attributes_0_answer_id" required="required"><option value="" label=" "></option><option value="1">Yes</option><option value="2">No</option></select></div>'
       '<div class="form-group"><label for="app_answer_sets_attributes_1_answer_id">Are you legally authorized to work in the United States?</label>'
       '<select name="app[answer_sets_attributes][1][answer_id]" id="app_answer_sets_attributes_1_answer_id"><option value="3">Yes</option><option value="4">No</option></select></div></form>')
cp_seen = {}


def fake_sync_cp(url, **kw):
    cp_seen["url"] = url
    return R(CPF, 200, url)


A._get_requests_sync = fake_sync_cp
cqj = {"url": "https://iaqa.careerplug.com/jobs/3540329"}
cq = A._fetch_careerplug_questions(cqj)
check("Application Question: Do you hold a valid driver\u2019s license?" in cq and "Are you legally authorized to work in the United States?" in cq
      and cqj.get("_form_status") == "ok" and cp_seen["url"] == "https://iaqa.careerplug.com/jobs/3540329/apps/new", f"careerplug questions {cq!r} {cp_seen}")
A._get_requests_sync = lambda url, **kw: R("<html>login</html>", 200, url)
cqj2 = {"url": "https://iaqa.careerplug.com/jobs/1"}
check(A._fetch_careerplug_questions(cqj2) == "" and cqj2.get("_form_status") is None and "CareerPlug" in A.QUESTION_FETCHERS, "careerplug: no form -> not read")
A._get_requests_sync = _orig_sync

# ── Workday: a retired saved site falls back to the live site named in robots.txt (2026-10) ──
WD_ROBOTS = "Sitemap: https://solera.wd5.myworkdayjobs.com/Global_Career_Site/siteMap.xml\n\nUser-agent: *\nAllow: /Global_Career_Site/\nDisallow: /refreshFacet/"
wd_posts = []


async def fake_wd_post(url, **kw):
    wd_posts.append(url)
    if "/international_career_site/" in url.lower():
        return R("permission denied", 403, url, {"errorCode": "S22"})
    return R("", 200, url, {"total": 1, "jobPostings": [{"title": "Principal Engineer", "externalPath": "/job/Bangalore/Principal-Engineer_JR-1", "locationsText": "Bangalore"}]})


async def fake_wd_get(url, **kw):
    return R(WD_ROBOTS, 200, url) if url.endswith("/robots.txt") else R("", 404, url)


_post0, _get0 = A._post, A._get
A._post, A._get = fake_wd_post, fake_wd_get
wdj = asyncio.run(A.scrape_workday("solera|wd5|international_career_site"))
check(len(wdj) == 1 and wdj[0]["url"] == "https://solera.wd5.myworkdayjobs.com/Global_Career_Site/job/Bangalore/Principal-Engineer_JR-1"
      and len(wd_posts) == 2, f"workday site fallback {wdj} {wd_posts}")
wd_posts.clear()
async def fake_wd_get_none(url, **kw):
    return R("", 422, url)


A._get = fake_wd_get_none
check(asyncio.run(A.scrape_workday("solera|wd5|international_career_site")) == [] and len(wd_posts) == 1, "workday: no robots.txt site -> no retry")
A._post, A._get = _post0, _get0

# ── Discovery coverage guard: every supported ATS reaches every discovery source that can see it (2026-10) ──
import node as _node  # noqa: E402
_vend = _node._ATS_VENDOR_DOMAINS
_no_url = {"successfactors"}  # recognised by page fingerprint, not URL
_no_cc = {"getro", "successfactors"}  # getro: own --source getro sweep
check(all(a in D.URL_TO_SLUG for a in D.SUPPORTED_ATS - _no_url), f"URL_TO_SLUG covers {sorted(D.SUPPORTED_ATS - _no_url - set(D.URL_TO_SLUG))}")
check(all(a in D.CC_PLATFORM_PATTERNS and a in D.CC_EXTRACTORS for a in D.SUPPORTED_ATS - _no_cc), "Common Crawl / Wayback patterns cover every platform")
_op_targets = set(D._OPENPOSTINGS_ATS_MAP_RAW.values())
check(not (D.SUPPORTED_ATS - _op_targets - {"successfactors"}), f"OpenPostings labels cover {sorted(D.SUPPORTED_ATS - _op_targets - {'successfactors'})}")
check({"applicantpro", "careerplug", "homerun", "factorial"} <= set(D._GITHUB_REGISTRY_ATS_MAP.values()), "openroles registry maps the four new platforms")
check({"freshteam", "comeet"} <= set(D.HTTPARCHIVE_ATS_TECH_NAMES), "HTTP Archive fingerprints for Freshteam / Comeet")
_missing_vendor = [(a, p) for a, ps in D.CC_PLATFORM_PATTERNS.items() for p in ps
                   if not any(re.sub(r"^\*\.", "", p.split("/")[0]).lower() in (v, ) or re.sub(r"^\*\.", "", p.split("/")[0]).lower().endswith("." + v) for v in _vend)]
check(not _missing_vendor, f"node vendor domains missing {_missing_vendor}")

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


# ── node: escaped / encoded / protocol-relative URL forms in pages ──
import node as N
_pg = ('<script>var a="https:\\/\\/boards.greenhouse.io\\/acme1\\/jobs\\/1";var b="https:\\u002F\\u002Fjobs.lever.co\\u002Facme2";</script>'
       '<a href="/r?url=https%3A%2F%2Fjobs.ashbyhq.com%2Facme3&x=1">x</a>'
       '<script src="//boards.greenhouse.io/embed/job_board/js?for=acme4"></script>'
       '<iframe src="https://acme5.bamboohr.com/jobs/embed2.php"></iframe>')
_hits = {(a, s) for a, s, _ in N._detect_ats_hits(N._extract_candidate_urls(_pg, "https://example.com/careers"))}
check({("greenhouse", "acme1"), ("lever", "acme2"), ("ashby", "acme3"), ("greenhouse", "acme4"), ("bamboohr", "acme5")} <= _hits,
      f"node: JSON-escaped, \\u002F, %3A%2F, protocol-relative script src and iframe src all yield tenants {sorted(_hits)}")
check(D._url_to_slug_rippling("https://ats.rippling.com/en-GB/acme/jobs") == "acme" and D._url_to_slug_rippling("https://ats.rippling.com/en-US/jobs") is None,
      "rippling: locale-prefixed board URL resolves to the company; bare locale/jobs does not")


# ── location regressions found in the 2026-10 live audit ──
check(A._flatchr_location({"address": {"locality": "Vincennes", "administrative_area_level_1": "Île-de-France", "country": "France"}}, {}) == "Vincennes, Île-de-France, France"
      and A._flatchr_location({"address": {}, "company": {"address": {"locality": "Lyon", "country": "France"}}}, {}) == "Lyon, France"
      and A._flatchr_location({}, {}) == "", "flatchr: location read from vacancy.address (was always blank), company address fallback")
from selectolax.lexbor import LexborHTMLParser as _LP
_t = _LP('<div class="jobs-listing-card"><div><a class="job-title" href="/job/abc=">T</a></div><div class="job-type"> Contract </div>'
         '<div class="job-location"><i class="material-icons">location_on</i> Grande Prairie, Alberta, Canada </div></div>')
check(A._card_loxo(_t.css_first("a.job-title")) == {"location": "Grande Prairie, Alberta, Canada", "employment_type": "Contract"}, "loxo: card reader returns place and type")

# ── Taleo Business Edition (tbe|instance|org|site|cws) ──
check(D._url_to_slug_taleo("https://phf.tbe.taleo.net/phf02/ats/careers/v2/jobSearch?cws=63&org=PERISHER") == "tbe|phf|PERISHER|phf02|63"
      and D._url_to_slug_taleo("https://phh.tbe.taleo.net/phh01/ats/careers/v2/viewRequisition?org=GRANTTHORNTON&cws=66&rid=11561") == "tbe|phh|GRANTTHORNTON|phh01|66"
      and D._url_to_slug_taleo("https://tre.tbe.taleo.net/tre01/ats/careers/v2/jobSearch?org=NVRINC") is None
      and D._url_to_slug_taleo("https://capps.taleo.net/careersection/ex/jobdetail.ftl?job=1") == "capps|ex",
      "taleo: TBE URL -> tbe|instance|org|site|cws (needs cws), classic careersection unchanged")
check([A._tbe_label_kind(x) for x in ("Office Location", "Alternate Location", "Employment Category", "Department", "Posted")]
      == ["location", "location", "employment_type", "department", ""], "taleo TBE: column labels map to fields")
_tj = {"url": "https://x.tbe.taleo.net/x01/ats/careers/v2/viewRequisition?org=O&cws=1&rid=2", "location": ""}
_ld = ('<script type="application/ld+json">{"@type":"JobPosting","title":"T","employmentType":"Full time","description":"<p>' + "Real description. " * 20 +
       '</p>","jobLocation":{"@type":"Place","address":{"addressLocality":"Toronto, ON","addressRegion":"Ontario","addressCountry":{"name":"CA"}}}}</script>')
_td = A._fetch_taleo_tbe_description(_tj, _ld)
check(_td.startswith("Real description") and _tj["location"] == "Toronto, ON, Ontario, CA" and _tj["country"] == "CA" and _tj["employment_type"] == "Full time",
      f"taleo TBE: JSON-LD gives description + location + country + type {_tj}")
check(A._fetch_taleo_questions(_tj) == "" and A._scrape_taleo_tbe_sync("tbe|bad") == [] and asyncio.run(A.scrape_taleo("tbe|a|b")) == [], "taleo TBE: no questions, bad slugs -> []")

# ── 2026-10 discovery sources: WDC domain seed + HF URL datasets ──
import importlib.util as _iu
_sp = _iu.spec_from_file_location("wdc_seed", "OpenData/wdc_seed.py"); _w = _iu.module_from_spec(_sp); _sp.loader.exec_module(_w)
_rows = _w.build_rows("Domain\t#Quads\t#Entities\tProps\nacme.co.uk\t9\t5\t{}\nsmall.com\t3\t1\t{}\nacme.greenhouse.io\t9\t9\t{}\nlinkedin.com\t9\t99\t{}\nbig.example.org\t50\t12\t{}\n", 3)
check(_rows == [("acme", "acme.co.uk", ""), ("big", "big.example.org", "")], f"wdc_seed: >=3 postings, vendors/boards dropped {_rows}")
check(callable(D.fetch_scholarweave_slugs) and callable(D.fetch_hireheat_slugs), "scholarweave + hireheat discovery sources defined")

print(f"new-platform checks: {n - len(fails)}/{n} passed")
for f in fails:
    print("  FAIL", f)
sys.exit(1 if fails else 0)
