"""
ATS scrapers — one function per platform.
Each returns a list of job dicts with standardised keys:
  { title, url, company, location, department, workplace_type,
    employment_type, salary, description_snippet, source_ats, slug }
"""

import re
import logging
import random
import time
import xml.etree.ElementTree as ET
from urllib.parse import unquote
import requests
from config import REQUEST_TIMEOUT, MAX_RETRIES

log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
]


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


def _extract_salary(text: str) -> str:
    """Try to extract salary/compensation from description text."""
    if not text:
        return ""
    # Common salary patterns
    patterns = [
        # $120,000 - $180,000 or $120K - $180K
        r"\$[\d,]+\.?\d*\s*[kK]?\s*[-–—to]+\s*\$[\d,]+\.?\d*\s*[kK]?(?:\s*(?:per\s+)?(?:year|annually|yr|pa|p\.a\.))?",
        # USD 120,000 - 180,000
        r"(?:USD|EUR|GBP|CAD|AUD)\s*[\d,]+\.?\d*\s*[-–—to]+\s*[\d,]+\.?\d*",
        # Salary range: $X - $Y
        r"(?:salary|compensation|pay)\s*(?:range)?[\s:]+\$[\d,]+\.?\d*\s*[kK]?\s*[-–—to]+\s*\$[\d,]+\.?\d*\s*[kK]?",
        # $120,000 USD/year
        r"\$[\d,]+\.?\d*\s*[kK]?\s*(?:USD|EUR|GBP)?\s*(?:/\s*(?:year|yr|annually))?",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(0).strip()
    return ""


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
            # Salary from compensation if available
            salary_str = ""
            comp = item.get("compensation") or item.get("salary") or {}
            if isinstance(comp, dict):
                min_s = comp.get("min", "") or comp.get("minimum", "")
                max_s = comp.get("max", "") or comp.get("maximum", "")
                currency = comp.get("currency", "USD")
                if min_s and max_s:
                    salary_str = f"{currency} {min_s}-{max_s}"

            desc = _snippet(item.get("description", "") or item.get("descriptionHtml", "") or "")
            if not salary_str:
                salary_str = _extract_salary(desc)

            all_jobs.append({
                "title": (item.get("name") or "").strip(),
                "url": item.get("url", ""),
                "company": "",
                "location": loc_names,
                "country": countries,
                "department": (item.get("department") or {}).get("name", ""),
                "workplace_type": wt,
                "employment_type": "",
                "salary": salary_str,
                "description_snippet": desc,
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

    jobs_list = data.get("jobs")
    if not jobs_list or not isinstance(jobs_list, list):
        return []

    jobs = []
    for post in jobs_list:
        loc_obj = post.get("location")
        loc = loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj or "")
        depts = post.get("departments") or []
        dept = depts[0].get("name", "") if depts else ""

        # Extract country from metadata if available
        country = ""
        metadata = post.get("metadata") or []
        if isinstance(metadata, list):
            for m in metadata:
                if isinstance(m, dict) and m.get("name", "").lower() in ("country", "location_country"):
                    country = str(m.get("value", ""))

        # Extract salary from description content
        content = post.get("content", "")
        description = _snippet(content)
        salary = _extract_salary(description)

        jobs.append({
            "title": post.get("title", "").strip(),
            "url": post.get("absolute_url", ""),
            "company": "",
            "location": loc,
            "country": country,
            "department": dept,
            "workplace_type": "",
            "employment_type": "",
            "salary": salary,
            "description_snippet": description,
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

    # Company name from slug (Lever API categories.team is the department, not company)
    if jobs:
        company = slug.replace("-", " ").title()
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


# ── BambooHR ───────────────────────────────────────────

def scrape_bamboohr(slug: str) -> list[dict]:
    """BambooHR careers list — JSON endpoint, no auth required."""
    url = f"https://{slug}.bamboohr.com/careers/list"
    headers = {
        "Accept": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
    }
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        if r.status_code != 200:
            return []
        if "application/json" not in r.headers.get("Content-Type", ""):
            return []
        data = r.json()
    except Exception:
        return []

    jobs_list = data.get("result")
    if not jobs_list or not isinstance(jobs_list, list):
        return []

    jobs = []
    for job in jobs_list:
        loc = job.get("location") or {}
        if isinstance(loc, dict):
            city = loc.get("city", "")
            state = loc.get("state", "")
            country = loc.get("country", "")
            location = ", ".join(filter(None, [city, state, country]))
        else:
            location = str(loc) if loc else ""
            country = ""

        dept = job.get("departmentLabel", "") or ""
        desc = _snippet(job.get("description", "") or "")
        salary = _extract_salary(desc)

        jobs.append({
            "title": (job.get("jobOpeningName") or "").strip(),
            "url": f"https://{slug}.bamboohr.com/careers/{job.get('id', '')}",
            "company": slug.replace("-", " ").title(),
            "location": location or "Not specified",
            "country": country,
            "department": dept,
            "workplace_type": "",
            "employment_type": job.get("employmentStatusLabel", ""),
            "salary": salary,
            "description_snippet": desc,
            "source_ats": "BambooHR",
            "slug": slug,
        })

    return jobs


# ── iCIMS ──────────────────────────────────────────────

def scrape_icims(slug: str) -> list[dict]:
    """iCIMS sitemap scraper — parses sitemap.xml for job URLs.
    Title is extracted from URL path. No description/location from sitemap."""
    sitemap_url = f"https://careers-{slug}.icims.com/sitemap.xml"
    headers = {
        "Accept": "application/xml",
        "User-Agent": random.choice(USER_AGENTS),
    }
    try:
        r = requests.get(sitemap_url, timeout=REQUEST_TIMEOUT, headers=headers)
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.content)
    except Exception:
        return []

    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    jobs = []

    for url_el in root.findall(".//s:url", ns):
        loc_el = url_el.find("s:loc", ns)
        if loc_el is None:
            continue
        job_url = (loc_el.text or "").strip()
        if not job_url or "/jobs/" not in job_url or job_url.endswith("/jobs/intro"):
            continue

        path = job_url.split("/jobs/")[-1]
        parts = path.split("/")
        if len(parts) >= 2:
            title = unquote(parts[1]).replace("-", " ").strip().title()
        else:
            continue

        jobs.append({
            "title": title,
            "url": job_url,
            "company": slug.replace("-", " ").title(),
            "location": "",  # iCIMS sitemap doesn't include location
            "country": "",
            "department": "",
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "iCIMS",
            "slug": slug,
        })

    return jobs


# ── Workday ────────────────────────────────────────────

def scrape_workday(slug: str) -> list[dict]:
    """Workday CXS JSON API. Slug format: 'company|wd#|site_id'.
    POST to /wday/cxs/{company}/{site_id}/jobs for paginated results."""
    parts = slug.split("|")
    if len(parts) != 3:
        log.debug(f"Invalid Workday slug format: {slug}")
        return []

    company, wd, site_id = parts
    wd_num = wd.replace("wd", "")
    base_url = f"https://{company}.wd{wd_num}.myworkdayjobs.com"
    api_url = f"{base_url}/wday/cxs/{company}/{site_id}/jobs"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
        "Origin": base_url,
        "Referer": f"{base_url}/{site_id}",
    }

    all_jobs = []
    offset = 0
    limit = 20

    while True:
        payload = {
            "appliedFacets": {},
            "limit": limit,
            "offset": offset,
            "searchText": "",
        }

        try:
            r = requests.post(
                api_url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
            )
            if r.status_code != 200:
                break
            data = r.json()
        except Exception:
            break

        postings = data.get("jobPostings", [])
        total = data.get("total", 0)

        if not postings:
            break

        for post in postings:
            job_path = post.get("externalPath", "")
            location = (post.get("locationsText") or "")[:200]

            all_jobs.append({
                "title": (post.get("title") or "").strip(),
                "url": f"{base_url}/{site_id}{job_path}",
                "company": company.replace("-", " ").title(),
                "location": location or "Not specified",
                "country": "",
                "department": "",
                "workplace_type": "",
                "employment_type": "",
                "salary": "",
                "description_snippet": "",  # Would need per-job fetch, too slow
                "source_ats": "Workday",
                "slug": slug,
            })

        offset += limit
        if offset >= total:
            break

        # Jitter to avoid bot detection
        time.sleep(random.uniform(0.3, 1.0))

    return all_jobs


# ── Workable ──────────────────────────────────────────

def scrape_workable(slug: str) -> list[dict]:
    """Workable public widget API — no auth, no pagination (returns all at once)."""
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    r = _get(url, params={"details": "true"}, headers=headers)
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []

    company_name = data.get("name", slug.replace("-", " ").title())
    jobs_list = data.get("jobs")
    if not jobs_list or not isinstance(jobs_list, list):
        return []

    jobs = []
    for post in jobs_list:
        city = post.get("city", "")
        country = post.get("country", "")
        state = post.get("state", "")
        location = ", ".join(filter(None, [city, state, country]))

        desc = _snippet(post.get("description", ""))
        salary = _extract_salary(desc)

        # Workplace type from telecommuting flag
        telecommuting = post.get("telecommuting", False)
        workplace = "Remote" if telecommuting else ""

        jobs.append({
            "title": (post.get("title") or "").strip(),
            "url": post.get("url") or post.get("shortlink") or "",
            "company": company_name,
            "location": location or "Not specified",
            "country": country,
            "department": post.get("department", ""),
            "workplace_type": workplace,
            "employment_type": post.get("employment_type", ""),
            "salary": salary,
            "description_snippet": desc,
            "source_ats": "Workable",
            "slug": slug,
        })

    return jobs


# ── Recruitee ─────────────────────────────────────────

def scrape_recruitee(slug: str) -> list[dict]:
    """Recruitee Careers Site API — no auth, returns all offers at once."""
    url = f"https://{slug}.recruitee.com/api/offers/"
    headers = {
        "Accept": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
    }
    r = _get(url, headers=headers)
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []

    offers = data.get("offers")
    if not offers or not isinstance(offers, list):
        return []

    jobs = []
    for offer in offers:
        city = offer.get("city", "")
        country = offer.get("country", "")
        location = offer.get("location", "") or ", ".join(filter(None, [city, country]))

        # Remote flag
        remote = offer.get("remote", False)
        workplace = "Remote" if remote else ""

        # Description — try translations first, then direct field
        translations = offer.get("translations") or {}
        en_trans = translations.get("en", {})
        desc_html = en_trans.get("description", "") or offer.get("description", "")
        desc = _snippet(desc_html)

        # Salary — structured object or fallback to text extraction
        salary_str = ""
        salary_obj = offer.get("salary")
        if isinstance(salary_obj, dict):
            min_sal = salary_obj.get("min", "")
            max_sal = salary_obj.get("max", "")
            currency = salary_obj.get("currency", "")
            period = salary_obj.get("period", "")
            if min_sal and max_sal:
                salary_str = f"{currency} {min_sal}-{max_sal}".strip()
                if period:
                    salary_str += f" per {period}"
        elif isinstance(salary_obj, str) and salary_obj:
            salary_str = salary_obj
        if not salary_str:
            salary_str = _extract_salary(desc)

        # Employment type
        emp_type = offer.get("employment_type_code", "")

        jobs.append({
            "title": (offer.get("title") or "").strip(),
            "url": offer.get("careers_url") or offer.get("url") or f"https://{slug}.recruitee.com/o/{offer.get('slug', '')}",
            "company": offer.get("company_name", slug.replace("-", " ").title()),
            "location": location or "Not specified",
            "country": country,
            "department": offer.get("department", ""),
            "workplace_type": workplace,
            "employment_type": emp_type,
            "salary": salary_str or "",
            "description_snippet": desc,
            "source_ats": "Recruitee",
            "slug": slug,
        })

    return jobs


# ── SmartRecruiters ───────────────────────────────────

def scrape_smartrecruiters(slug: str) -> list[dict]:
    """SmartRecruiters Posting API — no auth for public postings, paginated."""
    base_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    all_jobs = []
    offset = 0
    limit = 100

    while True:
        r = _get(base_url, params={"limit": limit, "offset": offset}, headers=headers)
        if not r:
            break
        try:
            data = r.json()
        except Exception:
            break

        content = data.get("content", [])
        total = data.get("totalFound", 0)

        if not content:
            break

        for post in content:
            # Location
            loc = post.get("location") or {}
            city = loc.get("city", "")
            region = loc.get("region", "")
            country = loc.get("country", "")
            remote = loc.get("remote", False)
            location = ", ".join(filter(None, [city, region, country]))
            if remote and not location:
                location = "Remote"
            elif remote:
                location += " (Remote)"

            # Company
            company_obj = post.get("company") or {}
            company_name = company_obj.get("name", slug.replace("-", " ").title())

            # Department
            dept_obj = post.get("department") or {}
            department = dept_obj.get("label", "")

            # Employment type
            toe_obj = post.get("typeOfEmployment") or {}
            employment_type = toe_obj.get("label", "")

            # Job URL — use ref for detail, or construct careers page URL
            ref_url = post.get("ref", "")
            posting_id = post.get("id", "")
            job_url = f"https://jobs.smartrecruiters.com/{slug}/{posting_id}" if posting_id else ref_url

            workplace = "Remote" if remote else ""

            all_jobs.append({
                "title": (post.get("name") or "").strip(),
                "url": job_url,
                "company": company_name,
                "location": location or "Not specified",
                "country": country,
                "department": department,
                "workplace_type": workplace,
                "employment_type": employment_type,
                "salary": "",  # Not available in list endpoint
                "description_snippet": "",  # Need per-posting fetch, too slow at scale
                "source_ats": "SmartRecruiters",
                "slug": slug,
            })

        offset += limit
        if offset >= total:
            break

        # Jitter to avoid rate limits
        time.sleep(random.uniform(0.2, 0.6))

    return all_jobs


# ── Dispatcher ──────────────────────────────────────────

SCRAPERS = {
    "rippling": scrape_rippling,
    "greenhouse": scrape_greenhouse,
    "lever": scrape_lever,
    "ashby": scrape_ashby,
    "bamboohr": scrape_bamboohr,
    "icims": scrape_icims,
    "workday": scrape_workday,
    "workable": scrape_workable,
    "recruitee": scrape_recruitee,
    "smartrecruiters": scrape_smartrecruiters,
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
