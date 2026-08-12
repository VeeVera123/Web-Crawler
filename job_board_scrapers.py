"""
Job board scrapers — pull from free public JSON APIs.

These are job AGGREGATORS (not ATS platforms). They list jobs from companies
using any ATS (including ones we can't scrape) or no ATS at all.

Supported boards:
  - RemoteOK      https://remoteok.com/api
  - Remotive      https://remotive.com/api/remote-jobs
  - Himalayas     https://himalayas.app/jobs/api
  - Arbeitnow     https://www.arbeitnow.com/api/job-board-api
  - Jobicy        https://jobicy.com/api/v2/remote-jobs

All APIs are free, no auth required. Output is normalized to the same dict
format that ats_scrapers.py produces, so it feeds directly into the existing
classification pipeline (role filter → location filter → Supabase).
"""

import logging
import time
import requests

log = logging.getLogger(__name__)

_TIMEOUT = 20
_HEADERS = {"User-Agent": "ATS-Global-Scanner/1.0"}


# ═══════════════════════════════════════════════════════
# REMOTEOK
# ═══════════════════════════════════════════════════════

def scrape_remoteok() -> list[dict]:
    """Fetch all jobs from RemoteOK. Returns normalized job dicts."""
    url = "https://remoteok.com/api"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"RemoteOK API error: {e}")
        return []

    jobs = []
    # First item is metadata/legal notice, skip it
    for item in data[1:] if len(data) > 1 else []:
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

    log.info(f"  RemoteOK: {len(jobs)} jobs fetched")
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

def scrape_jobicy() -> list[dict]:
    """Fetch remote jobs from Jobicy (max 50 per request)."""
    url = "https://jobicy.com/api/v2/remote-jobs?count=50"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Jobicy API error: {e}")
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
            "description_snippet": (item.get("jobDescription", "") or "")[:2000],
            "salary": _format_salary(
                item.get("annualSalaryMin"), item.get("annualSalaryMax")
            ),
            "employment_type": item.get("jobType", ""),
        })

    log.info(f"  Jobicy: {len(jobs)} jobs fetched")
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
}


def scrape_all_job_boards() -> list[dict]:
    """Pull from all 5 job boards. Returns normalized job dicts."""
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
