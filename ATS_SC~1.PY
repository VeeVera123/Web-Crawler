"""
ATS scrapers — one function per platform.
Each returns a list of job dicts with standardised keys:
  { title, url, company, location, department, workplace_type,
    employment_type, salary, description_snippet, source_ats, slug }
"""

import re
import logging
import requests
from config import REQUEST_TIMEOUT, MAX_RETRIES

log = logging.getLogger(__name__)


def _get(url: str, **kwargs) -> requests.Response | None:
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            if r.status_code == 429:
                import time
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.debug(f"Failed {url}: {e}")
                return None
    return None


def _snippet(html_or_text: str, max_chars: int = 2000) -> str:
    """Strip HTML and truncate."""
    if not html_or_text:
        return ""
    text = re.sub(r"<[^>]+>", " ", html_or_text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


# ── Rippling ────────────────────────────────────────────

def scrape_rippling(slug: str) -> list[dict]:
    """Rippling public API — paginated."""
    base = f"https://ats.rippling.com/api/v2/board/{slug}/jobs"
    all_jobs = []
    page = 0

    while True:
        r = _get(base, params={
            "page": page, "pageSize": 50,
            "searchQuery": "", "city": "", "country": "",
            "state": "", "workplaceType": "",
            "groupJobsByLocation": "false",
        })
        if not r:
            break
        try:
            data = r.json()
        except Exception:
            break

        for item in data.get("items", []):
            locations = item.get("locations") or []
            loc_names = ", ".join(l.get("name", "") for l in locations)
            countries = ", ".join(sorted(set(
                l.get("country", "") for l in locations if l.get("country")
            )))
            wt = ", ".join(sorted(set(
                l.get("workplaceType", "") for l in locations if l.get("workplaceType")
            )))
            all_jobs.append({
                "title": (item.get("name") or "").strip(),
                "url": item.get("url", ""),
                "company": "",  # filled from detail or board info
                "location": loc_names,
                "country": countries,
                "department": (item.get("department") or {}).get("name", ""),
                "workplace_type": wt,
                "employment_type": "",
                "salary": "",
                "description_snippet": "",
                "source_ats": "Rippling",
                "slug": slug,
            })

        total_pages = data.get("totalPages", 0)
        if page + 1 >= total_pages or not data.get("items"):
            break
        page += 1

    # Try to get company name from board info
    if all_jobs:
        r = _get(f"https://ats.rippling.com/api/v2/board/{slug}/jobs",
                 params={"page": 0, "pageSize": 1})
        if r:
            try:
                # Get company name from first job detail
                first_id = all_jobs[0]["url"].split("/")[-1]
                detail_r = _get(f"https://ats.rippling.com/api/v2/board/{slug}/jobs/{first_id}")
                if detail_r:
                    detail = detail_r.json()
                    company_name = detail.get("companyName", "")
                    for j in all_jobs:
                        j["company"] = company_name
            except Exception:
                pass

    return all_jobs


# ── Greenhouse ──────────────────────────────────────────

def scrape_greenhouse(slug: str) -> list[dict]:
    """Greenhouse public Job Board API — no auth required."""
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    r = _get(url, params={"content": "true"})
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []

    jobs = []
    for post in data.get("jobs", []):
        loc = post.get("location", {}).get("name", "")
        depts = post.get("departments", [])
        dept = depts[0].get("name", "") if depts else ""

        # Extract country from metadata if available
        country = ""
        metadata = post.get("metadata", [])
        for m in metadata:
            if m.get("name", "").lower() in ("country", "location_country"):
                country = str(m.get("value", ""))

        jobs.append({
            "title": post.get("title", "").strip(),
            "url": post.get("absolute_url", ""),
            "company": "",  # Greenhouse doesn't include company in job list
            "location": loc,
            "country": country,
            "department": dept,
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": _snippet(post.get("content", "")),
            "source_ats": "Greenhouse",
            "slug": slug,
        })

    # Get company name from board info
    if jobs:
        board_r = _get(f"https://boards-api.greenhouse.io/v1/boards/{slug}")
        if board_r:
            try:
                board_data = board_r.json()
                company_name = board_data.get("name", "")
                for j in jobs:
                    j["company"] = company_name
            except Exception:
                pass

    return jobs


# ── Lever ───────────────────────────────────────────────

def scrape_lever(slug: str) -> list[dict]:
    """Lever public postings API — no auth required."""
    url = f"https://api.lever.co/v0/postings/{slug}"
    r = _get(url, params={"mode": "json"})
    if not r:
        # Try EU endpoint
        r = _get(f"https://api.eu.lever.co/v0/postings/{slug}", params={"mode": "json"})
        if not r:
            return []
    try:
        data = r.json()
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    jobs = []
    for post in data:
        categories = post.get("categories", {})
        loc = categories.get("location", "")
        all_locs = categories.get("allLocations", [])
        if all_locs and not loc:
            loc = ", ".join(all_locs)

        # Salary
        salary_str = ""
        salary = post.get("salaryRange") or {}
        if isinstance(salary, dict) and salary:
            min_s = salary.get("min", "")
            max_s = salary.get("max", "")
            currency = salary.get("currency", "USD")
            if min_s and max_s:
                salary_str = f"{currency} {min_s}-{max_s}"

        jobs.append({
            "title": post.get("text", "").strip(),
            "url": post.get("hostedUrl", ""),
            "company": "",
            "location": loc,
            "country": "",
            "department": categories.get("department", "") or categories.get("team", ""),
            "workplace_type": post.get("workplaceType", ""),
            "employment_type": categories.get("commitment", ""),
            "salary": salary_str,
            "description_snippet": _snippet(post.get("descriptionPlain", "") or post.get("description", "")),
            "source_ats": "Lever",
            "slug": slug,
        })

    # Company name from first post
    if jobs and data:
        company = data[0].get("categories", {}).get("team", "") or slug.replace("-", " ").title()
        for j in jobs:
            j["company"] = company

    return jobs


# ── Ashby ───────────────────────────────────────────────

def scrape_ashby(slug: str) -> list[dict]:
    """Ashby public job board API — no auth required."""
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    r = _get(url, params={"includeCompensation": "true"})
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []

    jobs_data = data.get("jobs", [])
    company_name = data.get("jobBoard", {}).get("organizationName", "") or slug.replace("-", " ").title()

    jobs = []
    for post in jobs_data:
        loc = post.get("location", "")
        dept = post.get("department", "")
        if isinstance(dept, dict):
            dept = dept.get("name", "")

        # Compensation
        salary_str = ""
        comp = post.get("compensation")
        if comp:
            parts = []
            for comp_item in (comp if isinstance(comp, list) else [comp]):
                if isinstance(comp_item, dict):
                    low = comp_item.get("low", "")
                    high = comp_item.get("high", "")
                    currency = comp_item.get("currency", "USD")
                    if low and high:
                        parts.append(f"{currency} {low}-{high}")
            salary_str = "; ".join(parts)

        # Country from location string
        country = ""
        if loc:
            # Common patterns: "City, Country" or "Remote - Country"
            parts = [p.strip() for p in loc.replace(" - ", ", ").split(",")]
            if len(parts) >= 2:
                country = parts[-1]

        jobs.append({
            "title": post.get("title", "").strip(),
            "url": post.get("jobUrl", "") or f"https://jobs.ashbyhq.com/{slug}/{post.get('id', '')}",
            "company": company_name,
            "location": loc,
            "country": country,
            "department": dept,
            "workplace_type": post.get("employmentType", ""),
            "employment_type": "",
            "salary": salary_str,
            "description_snippet": _snippet(post.get("descriptionHtml", "") or post.get("descriptionPlain", "")),
            "source_ats": "Ashby",
            "slug": slug,
        })

    return jobs


# ── Dispatcher ──────────────────────────────────────────

SCRAPERS = {
    "rippling": scrape_rippling,
    "greenhouse": scrape_greenhouse,
    "lever": scrape_lever,
    "ashby": scrape_ashby,
}


def scrape_board(ats: str, slug: str) -> list[dict]:
    """Dispatch to the correct scraper."""
    fn = SCRAPERS.get(ats.lower())
    if not fn:
        log.warning(f"Unknown ATS: {ats}")
        return []
    try:
        return fn(slug)
    except Exception as e:
        log.error(f"Error scraping {ats}/{slug}: {e}")
        return []
