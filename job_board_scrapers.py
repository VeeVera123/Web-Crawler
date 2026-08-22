"""
Job board scrapers — pull from free public APIs (JSON + RSS).

These are job AGGREGATORS (not ATS platforms). They list jobs from companies
using any ATS (including ones we can't scrape) or no ATS at all.

Supported boards:
  - RemoteOK         https://remoteok.com/api
  - Remotive         https://remotive.com/api/remote-jobs
  - Himalayas        https://himalayas.app/jobs/api
  - Arbeitnow        https://www.arbeitnow.com/api/job-board-api
  - Jobicy           https://jobicy.com/api/v2/remote-jobs
  - We Work Remotely https://weworkremotely.com/remote-jobs.rss (RSS)
  - Working Nomads   https://www.workingnomads.co/api/exposed_jobs/
  - FreeHire         https://www.freehire.me/api/v1/jobs

REMOVED 2026-08 — Jooble: only ever returned a short `snippet`, no way to
get the full JD, so real disqualifying language (e.g. a US-work-
authorization requirement) could slip past the classifier undetected.
Jobicy (already above) was widened to cover the same ground instead — see
scrape_jobicy docstring — since its API already returns the full,
untruncated job description at no extra cost.

All APIs are free, no auth required. Output is normalized to the same dict
format that ats_scrapers.py produces, so it feeds directly into the existing
classification pipeline (role filter → location filter → Supabase).
"""

import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from html import unescape
import requests

log = logging.getLogger(__name__)

_TIMEOUT = 20
_HEADERS = {"User-Agent": "ATS-Global-Scanner/1.0"}

# ═══════════════════════════════════════════════════════
# SLUG DISCOVERY — extract company slugs from aggregator job URLs
# ═══════════════════════════════════════════════════════

# URL patterns that map to our ATS scrapers. Each tuple is (regex, ats_name, slug_group_index).
_ATS_URL_PATTERNS = [
    (re.compile(r"boards\.greenhouse\.io/([^/?\s]+)", re.I), "greenhouse"),
    (re.compile(r"jobs\.lever\.co/([^/?\s]+)", re.I), "lever"),
    (re.compile(r"jobs\.ashbyhq\.com/([^/?\s]+)", re.I), "ashby"),
    (re.compile(r"([^/.\s]+)\.bamboohr\.com", re.I), "bamboohr"),
    (re.compile(r"icims\.com/.*?company=([^&/\s]+)", re.I), "icims"),
    (re.compile(r"([^/.\s]+)\.myworkdayjobs\.com", re.I), "workday"),
    (re.compile(r"([^/.\s]+)\.rippling\.com", re.I), "rippling"),
    (re.compile(r"apply\.workable\.com/([^/?\s]+)", re.I), "workable"),
    (re.compile(r"([^/.\s]+)\.recruitee\.com", re.I), "recruitee"),
    (re.compile(r"jobs\.smartrecruiters\.com/([^/?\s]+)", re.I), "smartrecruiters"),
    (re.compile(r"([^/.\s]+)\.teamtailor\.com", re.I), "teamtailor"),
    (re.compile(r"([^/.\s]+)\.breezy\.hr", re.I), "breezyhr"),
    # ApplyToJob pattern removed 2026-08 — ATS retired (see ats_scrapers.py),
    # discovering its slugs would be pointless since nothing scrapes them.
    (re.compile(r"([^/.\s]+)\.jobs\.personio\.com", re.I), "personio"),
    (re.compile(r"join\.com/companies/([^/?\s]+)", re.I), "joincom"),
    (re.compile(r"([^/.\s]+)\.taleo\.net", re.I), "taleo"),
    (re.compile(r"([^/.\s]+)\.fa\..*\.oraclecloud\.com", re.I), "oracle_cloud_hcm"),
    (re.compile(r"recruiting\.paylocity\.com/.*?/([^/?\s]+)", re.I), "paylocity"),
    (re.compile(r"([^/.\s]+)\.hrmdirect\.com", re.I), "hrmdirect"),
    (re.compile(r"([^/.\s]+)\.zohorecruit\.com", re.I), "zoho"),
]

# Collect discovered slugs across all aggregator scrapers
_discovered_slugs: list[tuple[str, str]] = []


def _try_extract_slug(url: str) -> tuple[str, str] | None:
    """Try to extract (ats, slug) from a job URL. Returns None if no match."""
    if not url:
        return None
    for pattern, ats_name in _ATS_URL_PATTERNS:
        m = pattern.search(url)
        if m:
            slug = m.group(1).strip().lower()
            if slug and len(slug) > 1 and slug not in ("www", "api", "app", "jobs"):
                return (ats_name, slug)
    return None


def get_discovered_slugs() -> list[tuple[str, str]]:
    """Return all slugs discovered from aggregator job URLs this run.
    Deduplicated. Call after scrape_all_job_boards()."""
    seen = set()
    unique = []
    for pair in _discovered_slugs:
        if pair not in seen:
            seen.add(pair)
            unique.append(pair)
    return unique


# ═══════════════════════════════════════════════════════
# REMOTEOK
# ═══════════════════════════════════════════════════════

def _remoteok_fetch_one(url: str) -> list[dict]:
    """Fetch a single RemoteOK endpoint (main feed or tag-filtered) and
    normalize its jobs. Shared by scrape_remoteok()."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.debug(f"RemoteOK fetch failed ({url}): {e}")
        return []

    jobs = []
    # First item is metadata/legal notice, skip it
    for item in data[1:] if isinstance(data, list) and len(data) > 1 else []:
        job_url = item.get("url", "")
        if not job_url:
            continue
        # Prefer the original apply URL if available
        apply_url = item.get("apply_url") or job_url
        jobs.append({
            "title": item.get("position", ""),
            "company": item.get("company", ""),
            "location": item.get("location", "Remote"),
            "url": apply_url,
            "source_ats": "remoteok",
            "source_type": "job_board",
            "description_snippet": (item.get("description", "") or "")[:2000],
            "salary": _format_salary(item.get("salary_min"), item.get("salary_max")),
            "tags": item.get("tags", []),
        })
    return jobs


# RemoteOK has no true pagination/offset param (verified — it's a fixed
# snapshot of recent jobs). Its only lever for surfacing more unique jobs
# is tag-filtered endpoints (remoteok.com/api?tag=...), which may include
# postings outside the main snapshot's recency window. We fetch the main
# feed plus a set of CSM/AM-relevant tags and dedupe by URL — cheap
# (9 requests total) and strictly additive, never fewer jobs than before.
_REMOTEOK_SUPPLEMENTAL_TAGS = [
    "customer-support", "customer-success", "sales",
    "account-manager", "marketing", "non-tech",
]


def scrape_remoteok() -> list[dict]:
    """Fetch jobs from RemoteOK: the main feed plus tag-filtered endpoints
    for CSM/AM-relevant tags, deduplicated by URL. Returns normalized job dicts."""
    seen_urls = set()
    jobs = []

    for job in _remoteok_fetch_one("https://remoteok.com/api"):
        if job["url"] not in seen_urls:
            seen_urls.add(job["url"])
            jobs.append(job)

    for tag in _REMOTEOK_SUPPLEMENTAL_TAGS:
        for job in _remoteok_fetch_one(f"https://remoteok.com/api?tag={tag}"):
            if job["url"] not in seen_urls:
                seen_urls.add(job["url"])
                jobs.append(job)
        time.sleep(0.3)  # be polite between tag calls

    log.info(f"  RemoteOK: {len(jobs)} jobs fetched (main feed + {len(_REMOTEOK_SUPPLEMENTAL_TAGS)} tag feeds)")
    return jobs


# ═══════════════════════════════════════════════════════
# REMOTIVE
# ═══════════════════════════════════════════════════════

def scrape_remotive() -> list[dict]:
    """Fetch all jobs from Remotive. Max 4 calls/day, 1 is enough."""
    url = "https://remotive.com/api/remote-jobs"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Remotive API error: {e}")
        return []

    jobs = []
    for item in data.get("jobs", []):
        job_url = item.get("url", "")
        if not job_url:
            continue
        jobs.append({
            "title": item.get("title", ""),
            "company": item.get("company_name", ""),
            "location": item.get("candidate_required_location", "Remote"),
            "url": job_url,
            "source_ats": "remotive",
            "source_type": "job_board",
            "description_snippet": (item.get("description", "") or "")[:2000],
            "salary": item.get("salary", ""),
            "employment_type": item.get("job_type", ""),
        })

    log.info(f"  Remotive: {len(jobs)} jobs fetched")
    return jobs


# ═══════════════════════════════════════════════════════
# HIMALAYAS
# ═══════════════════════════════════════════════════════

def scrape_himalayas() -> list[dict]:
    """Fetch jobs from Himalayas with pagination (max 20 per page)."""
    jobs = []
    offset = 0
    limit = 20
    max_pages = 50  # safety cap: 1000 jobs max

    for _ in range(max_pages):
        url = f"https://himalayas.app/jobs/api?limit={limit}&offset={offset}"
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"Himalayas API error at offset {offset}: {e}")
            break

        page_jobs = data.get("jobs", [])
        if not page_jobs:
            break

        for item in page_jobs:
            job_url = item.get("applicationLink") or item.get("url", "")
            if not job_url:
                continue
            salary = _format_salary(
                item.get("minSalary"), item.get("maxSalary")
            )
            jobs.append({
                "title": item.get("title", ""),
                "company": item.get("companyName", ""),
                "location": ", ".join(item.get("locationRestrictions") or []) or "Remote",
                "url": job_url,
                "source_ats": "himalayas",
                "source_type": "job_board",
                "description_snippet": (item.get("description", "") or "")[:2000],
                "salary": salary,
                "employment_type": item.get("type", ""),
            })

        offset += limit
        if len(page_jobs) < limit:
            break
        time.sleep(0.5)  # be polite

    log.info(f"  Himalayas: {len(jobs)} jobs fetched")
    return jobs


# ═══════════════════════════════════════════════════════
# ARBEITNOW
# ═══════════════════════════════════════════════════════

def scrape_arbeitnow() -> list[dict]:
    """Fetch jobs from Arbeitnow with pagination (100 per page)."""
    jobs = []
    page = 1
    max_pages = 20  # safety cap: 2000 jobs max

    for _ in range(max_pages):
        url = f"https://www.arbeitnow.com/api/job-board-api?page={page}"
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"Arbeitnow API error on page {page}: {e}")
            break

        page_jobs = data.get("data", [])
        if not page_jobs:
            break

        for item in page_jobs:
            job_url = item.get("url", "")
            if not job_url:
                continue
            jobs.append({
                "title": item.get("title", ""),
                "company": item.get("company_name", ""),
                "location": item.get("location", "Remote"),
                "url": job_url,
                "source_ats": "arbeitnow",
                "source_type": "job_board",
                "description_snippet": (item.get("description", "") or "")[:2000],
                "tags": item.get("tags", []),
                "employment_type": ", ".join(item.get("job_types", [])),
            })

        # Check if there are more pages
        meta = data.get("meta", {}) or data.get("links", {})
        next_url = meta.get("next")
        if not next_url or len(page_jobs) < 100:
            break
        page += 1
        time.sleep(0.5)

    log.info(f"  Arbeitnow: {len(jobs)} jobs fetched")
    return jobs


# ═══════════════════════════════════════════════════════
# JOBICY
# ═══════════════════════════════════════════════════════

def _jobicy_fetch_one(params: dict) -> list[dict]:
    """Fetch a single Jobicy query and normalize its jobs. Shared by scrape_jobicy().

    WIDENED 2026-08 (replacing Jooble + ApplyToJob, both removed — Jooble's
    `snippet` field is a short excerpt with no full-JD option, and
    ApplyToJob (JazzHR) generic-fetched descriptions weren't reliably
    catching real US-eligibility language, e.g. a live posting that said
    "Legal work authorization in the US" got through the classifier
    anyway; ApplyToJob is also a small long-tail ATS, not a major board,
    so it wasn't worth debugging further). Jobicy was already integrated
    here but under-used: its own API docs confirm `jobDescription` is the
    FULL untruncated HTML description (not a snippet), yet the old code
    truncated it to 2000 chars anyway for no reason — that cap is gone
    now, so the real JD reaches the classifier same as an ATS-scraped one.
    """
    url = "https://jobicy.com/api/v2/remote-jobs"
    try:
        resp = requests.get(url, params=params, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.debug(f"Jobicy fetch failed ({params}): {e}")
        return []

    jobs = []
    for item in data.get("jobs", []):
        job_url = item.get("url", "")
        if not job_url:
            continue
        jobs.append({
            "title": item.get("jobTitle", ""),
            "company": item.get("companyName", ""),
            "location": item.get("jobGeo", "Remote"),
            "url": job_url,
            "source_ats": "jobicy",
            "source_type": "job_board",
            # Full HTML description, no truncation — see docstring above.
            "description_snippet": item.get("jobDescription", "") or "",
            "salary": _format_salary(
                item.get("annualSalaryMin"), item.get("annualSalaryMax")
            ),
            "employment_type": item.get("jobType", ""),
        })
    return jobs


# Jobicy's `count` param actually goes up to 100 per call (confirmed live
# via its own docs 2026-08 — the old 50 here was an unverified guess).
# There's still no true pagination beyond that per query, so to surface
# more than one 100-job snapshot we fan out across `industry` filters
# relevant to CSM/AM roles (widened from 4 to 10 — Jobicy's docs list
# many more industry slugs than were being used) AND `geo` filters for
# our target hiring regions, then dedupe by URL. This, combined with the
# full-description fix above, is meant to cover what Jooble/ApplyToJob
# were doing (CSM/AM roles, full JD, location, salary) from one already-
# free, no-signup, no-rate-limit-key source instead of two weaker ones.
_JOBICY_COUNT = 100
_JOBICY_INDUSTRIES = [
    "customer-service", "business", "marketing", "sales",
    "management", "hr", "supporting", "admin",
    "project management", "business development",
]
_JOBICY_GEOS = ["usa", "canada", "uk", "europe", "emea", "apac", "australia"]


def scrape_jobicy() -> list[dict]:
    """Fetch remote jobs from Jobicy: an unfiltered call, several
    industry-filtered calls, and several geo-filtered calls (each capped
    at 100 by the API), deduplicated by URL. See _jobicy_fetch_one
    docstring for why this replaced Jooble/ApplyToJob 2026-08."""
    seen_urls = set()
    jobs = []

    for job in _jobicy_fetch_one({"count": _JOBICY_COUNT}):
        if job["url"] not in seen_urls:
            seen_urls.add(job["url"])
            jobs.append(job)

    for industry in _JOBICY_INDUSTRIES:
        for job in _jobicy_fetch_one({"count": _JOBICY_COUNT, "industry": industry}):
            if job["url"] not in seen_urls:
                seen_urls.add(job["url"])
                jobs.append(job)
        time.sleep(0.3)

    for geo in _JOBICY_GEOS:
        for job in _jobicy_fetch_one({"count": _JOBICY_COUNT, "geo": geo}):
            if job["url"] not in seen_urls:
                seen_urls.add(job["url"])
                jobs.append(job)
        time.sleep(0.3)

    log.info(f"  Jobicy: {len(jobs)} jobs fetched (unfiltered + "
             f"{len(_JOBICY_INDUSTRIES)} industry feeds + {len(_JOBICY_GEOS)} geo feeds)")
    return jobs


# ═══════════════════════════════════════════════════════
# WE WORK REMOTELY (RSS)
# ═══════════════════════════════════════════════════════

# We Work Remotely's main feed doesn't necessarily include every category
# (verified — "all-other-remote-jobs" is a separate catch-all). Combine the
# main feed with the category feeds most relevant to CSM/AM roles for a
# superset of jobs; scrape_weworkremotely() already dedupes by URL.
_WWR_FEEDS = [
    "https://weworkremotely.com/remote-jobs.rss",
    "https://weworkremotely.com/categories/remote-customer-support-jobs.rss",
    "https://weworkremotely.com/categories/remote-sales-and-marketing-jobs.rss",
    "https://weworkremotely.com/categories/all-other-remote-jobs.rss",
]

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    """Strip HTML tags from RSS description content."""
    return _HTML_TAG_RE.sub(" ", text).strip()


def scrape_weworkremotely() -> list[dict]:
    """Fetch jobs from We Work Remotely via RSS feed."""
    jobs = []
    seen_urls = set()

    for feed_url in _WWR_FEEDS:
        try:
            resp = requests.get(feed_url, headers=_HEADERS, timeout=_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            log.error(f"WWR RSS error ({feed_url}): {e}")
            continue

        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError as e:
            log.error(f"WWR RSS parse error: {e}")
            continue

        for item in root.findall(".//item"):
            link = (item.findtext("link") or "").strip()
            if not link or link in seen_urls:
                continue
            seen_urls.add(link)

            # Slug discovery from job URLs
            slug_pair = _try_extract_slug(link)
            if slug_pair:
                _discovered_slugs.append(slug_pair)

            title = (item.findtext("title") or "").strip()
            # WWR titles often include company: "Company: Job Title"
            company = ""
            if ": " in title:
                company, title = title.split(": ", 1)

            # Extract region from category tags
            region = "Remote"
            for cat in item.findall("category"):
                cat_text = (cat.text or "").strip()
                if cat_text and cat_text.lower() not in ("", "remote"):
                    region = cat_text

            desc_raw = item.findtext("description") or ""
            desc = _strip_html(desc_raw)

            jobs.append({
                "title": title,
                "company": company,
                "location": region,
                "url": link,
                "source_ats": "weworkremotely",
                "source_type": "job_board",
                "description_snippet": desc[:2000],
                "pubdate": (item.findtext("pubDate") or "").strip(),
            })

    log.info(f"  WeWorkRemotely: {len(jobs)} jobs fetched")
    return jobs


# ═══════════════════════════════════════════════════════
# WORKING NOMADS
# ═══════════════════════════════════════════════════════

def scrape_workingnomads() -> list[dict]:
    """Fetch all jobs from Working Nomads JSON API."""
    url = "https://www.workingnomads.co/api/exposed_jobs/"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Working Nomads API error: {e}")
        return []

    jobs = []
    for item in data:
        job_url = item.get("url", "")
        if not job_url:
            continue

        # Slug discovery
        slug_pair = _try_extract_slug(job_url)
        if slug_pair:
            _discovered_slugs.append(slug_pair)

        jobs.append({
            "title": item.get("title", ""),
            "company": item.get("company_name", ""),
            "location": item.get("location", "Remote"),
            "url": job_url,
            "source_ats": "workingnomads",
            "source_type": "job_board",
            "description_snippet": _strip_html(item.get("description", "") or "")[:2000],
            "category": item.get("category_name", ""),
            "pubdate": item.get("pub_date", ""),
        })

    log.info(f"  Working Nomads: {len(jobs)} jobs fetched")
    return jobs


# ═══════════════════════════════════════════════════════
# FREEHIRE
# ═══════════════════════════════════════════════════════

# Only pull from ATS sources we DON'T already scrape, to minimize duplicates
_FREEHIRE_EXCLUDE_SOURCES = {
    "greenhouse", "lever", "ashby", "bamboohr", "icims", "workday",
    "rippling", "workable", "recruitee", "smartrecruiters", "teamtailor",
    "breezyhr", "personio", "joincom", "taleo",
    "oracle_cloud_hcm", "paylocity", "hrmdirect", "zoho",
    # "applytojob" removed from this exclude set 2026-08 — we no longer
    # scrape ApplyToJob directly (see ats_scrapers.py), so its postings
    # are no longer duplicates; FreeHire can now surface them instead of
    # silently dropping them.
}


def scrape_freehire() -> list[dict]:
    """Fetch jobs from FreeHire API, filtering to ATS sources we don't cover.

    FreeHire aggregates 5.5M+ jobs from 80+ ATS platforms.
    We only pull jobs from sources NOT in our ATS scraper list
    (e.g. BrassRing, SuccessFactors, Jobvite, etc.) to avoid duplication.
    """
    jobs = []
    page = 1
    max_pages = 50  # safety cap

    for _ in range(max_pages):
        url = f"https://www.freehire.me/api/v1/jobs?page={page}&per_page=100&remote=true"
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"FreeHire API error on page {page}: {e}")
            break

        page_jobs = data.get("jobs", data.get("data", []))
        if not page_jobs:
            break

        for item in page_jobs:
            job_url = item.get("url", item.get("apply_url", ""))
            if not job_url:
                continue

            # Try to discover slugs from ALL job URLs (even ones we skip)
            slug_pair = _try_extract_slug(job_url)
            if slug_pair:
                _discovered_slugs.append(slug_pair)

            # Skip jobs from ATS sources we already scrape
            source = (item.get("source", "") or "").lower().strip()
            if source in _FREEHIRE_EXCLUDE_SOURCES:
                continue

            location = item.get("location", "")
            if not location:
                location = "Remote" if item.get("remote") else ""

            jobs.append({
                "title": item.get("title", ""),
                "company": item.get("company", item.get("company_name", "")),
                "location": location,
                "url": job_url,
                "source_ats": "freehire",
                "source_type": "job_board",
                "description_snippet": (item.get("description", "") or "")[:2000],
                "freehire_source": source,  # track which ATS it came from
            })

        # Check for more pages
        total = data.get("total", data.get("total_count", 0))
        if not total or page * 100 >= total or len(page_jobs) < 100:
            break
        page += 1
        time.sleep(0.5)

    log.info(f"  FreeHire: {len(jobs)} jobs fetched (filtered to non-overlapping sources)")
    return jobs


# ═══════════════════════════════════════════════════════
# YCOMBINATOR (Work at a Startup)
# ═══════════════════════════════════════════════════════
#
# Not a single-company ATS — workatastartup.com is a multi-company job
# aggregator across the whole YC portfolio, so (like RemoteOK/Jobicy/etc.)
# it belongs here rather than keyed by per-company slug in ats_scrapers.py.
#
# The site is Rails/Inertia-based (NOT Next.js): job data isn't in a
# __NEXT_DATA__ script tag, it's JSON in a `data-page` attribute on the
# React-mount <div>:
#   <div id="jobs/public/pages/JobsPage-react-component-..." data-page='{...}'>
# List pages:   /jobs  and  /jobs/l/{category}
# Detail pages: /jobs/{numeric_id}  (data-page id starts with JobDetailPage)
#
# Requires an explicit Accept: text/html header — the default requests
# Accept header gets a 406 from this site.

_YC_CATEGORIES = [
    "software-engineer", "designer", "recruiting", "science",
    "product-manager", "operations", "sales-manager", "marketing",
    "legal", "finance",
]

_YC_HEADERS = {
    **_HEADERS,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _yc_extract_data_page(html: str) -> dict | None:
    """Pull the `data-page='{...}'` JSON blob off the React-mount div."""
    match = re.search(
        r'id="jobs/public/pages/(?:JobsPage|JobDetailPage)-react-component-[^"]*"'
        r'[^>]*data-page=\'([^\']+)\'',
        html, re.I
    )
    if not match:
        # Some templates use double quotes for the attribute instead
        match = re.search(
            r'id="jobs/public/pages/(?:JobsPage|JobDetailPage)-react-component-[^"]*"'
            r'[^>]*data-page="([^"]+)"',
            html, re.I
        )
    if not match:
        return None
    try:
        return json.loads(unescape(match.group(1)))
    except Exception:
        return None


def _yc_job_from_item(item: dict) -> dict:
    company_name = item.get("companyName", "")
    job_id = item.get("id", "")
    apply_url = item.get("applyUrl") or (f"https://www.workatastartup.com/jobs/{job_id}" if job_id else "")

    salary = item.get("salaryRange") or ""
    if not salary and item.get("minSalary") and item.get("maxSalary"):
        salary = f"${item['minSalary']:,} - ${item['maxSalary']:,}"

    return {
        "title": item.get("title", ""),
        "company": company_name,
        "location": item.get("location") or ("Remote" if item.get("roleType") == "remote" else ""),
        "url": apply_url,
        "source_ats": "ycombinator",
        "source_type": "job_board",
        "description_snippet": (item.get("descriptionHtml") or item.get("description") or "")[:2000],
        "salary": salary,
        "employment_type": item.get("jobType", ""),
        "company_batch": item.get("companyBatch", ""),
    }


def scrape_ycombinator() -> list[dict]:
    """Fetch jobs from YC's Work at a Startup board: the main /jobs feed
    plus every verified category page, deduped by job id."""
    jobs = []
    seen_ids = set()

    urls = ["https://www.workatastartup.com/jobs"] + [
        f"https://www.workatastartup.com/jobs/l/{cat}" for cat in _YC_CATEGORIES
    ]

    for url in urls:
        try:
            resp = requests.get(url, headers=_YC_HEADERS, timeout=_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            log.debug(f"YCombinator fetch failed ({url}): {e}")
            continue

        data_page = _yc_extract_data_page(resp.text)
        if not data_page:
            continue

        props = data_page.get("props", {})
        items = props.get("jobs") or []
        for item in items:
            job_id = item.get("id")
            if job_id and job_id in seen_ids:
                continue
            if job_id:
                seen_ids.add(job_id)
            jobs.append(_yc_job_from_item(item))

        time.sleep(0.3)  # be polite between category pages

    log.info(f"  YCombinator: {len(jobs)} jobs fetched across /jobs + {len(_YC_CATEGORIES)} category pages")
    return jobs


# ═══════════════════════════════════════════════════════
# UNIFIED ENTRY POINT
# ═══════════════════════════════════════════════════════

BOARD_SCRAPERS = {
    "remoteok": scrape_remoteok,
    "remotive": scrape_remotive,
    "himalayas": scrape_himalayas,
    "arbeitnow": scrape_arbeitnow,
    "jobicy": scrape_jobicy,
    "weworkremotely": scrape_weworkremotely,
    "workingnomads": scrape_workingnomads,
    "freehire": scrape_freehire,
    "ycombinator": scrape_ycombinator,
}


def scrape_all_job_boards() -> list[dict]:
    """Pull from all 8 job boards. Returns normalized job dicts."""
    all_jobs = []
    for name, scraper in BOARD_SCRAPERS.items():
        try:
            jobs = scraper()
            all_jobs.extend(jobs)
        except Exception as e:
            log.error(f"Job board {name} failed: {e}")
    log.info(f"Total from job boards: {len(all_jobs)} jobs across {len(BOARD_SCRAPERS)} boards")
    return all_jobs


# ═══════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════

def _format_salary(min_val, max_val) -> str:
    """Format min/max salary into a readable string."""
    if not min_val and not max_val:
        return ""
    if min_val and max_val:
        return f"${int(min_val):,} - ${int(max_val):,}"
    if min_val:
        return f"${int(min_val):,}+"
    return f"Up to ${int(max_val):,}"
