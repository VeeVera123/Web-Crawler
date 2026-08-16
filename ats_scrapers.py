"""
ATS scrapers — one function per platform.
Each returns a list of job dicts with standardised keys:
  { title, url, company, location, department, workplace_type,
    employment_type, salary, description_snippet, source_ats, slug }
"""

import re
import logging
import random
import threading
import time
import xml.etree.ElementTree as ET
from urllib.parse import unquote
import requests
from config import REQUEST_TIMEOUT, MAX_RETRIES

log = logging.getLogger(__name__)

# ── Connection pooling via thread-local sessions ──────────
# Each thread reuses a single requests.Session, avoiding TLS
# re-negotiation on every request. Huge win for paginated scrapers
# (Workday, Oracle, Taleo, SmartRecruiters, etc.).
_thread_local = threading.local()


def _get_session() -> requests.Session:
    """Return the current thread's reusable HTTP session."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        # Set default retry adapter with connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=0,  # We handle retries in _get()
        )
        _thread_local.session.mount("https://", adapter)
        _thread_local.session.mount("http://", adapter)
    return _thread_local.session

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
]


def _get(url: str, **kwargs) -> requests.Response | None:
    session = _get_session()
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.debug(f"Failed {url}: {e}")
                return None
    return None


def _snippet(html_or_text: str, max_chars: int = 8000) -> str:
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

        # Extract country and enriched location from metadata
        country = ""
        metadata_location = ""
        metadata = post.get("metadata") or []
        if isinstance(metadata, list):
            for m in metadata:
                if not isinstance(m, dict):
                    continue
                meta_name = m.get("name", "").lower()
                meta_val = str(m.get("value") or "")
                if meta_name in ("country", "location_country") and meta_val:
                    country = meta_val
                elif meta_name == "location" and meta_val:
                    metadata_location = meta_val

        # If location is bare "Remote" but metadata has a richer value
        # (e.g. "United States (Remote)"), use the metadata value instead
        if metadata_location and loc.strip().lower() in ("remote", ""):
            loc = metadata_location

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

        # Enrich location from address.postalAddress if location is bare
        # Ashby's `location` field is often just "Remote" or "Hybrid",
        # while `address.postalAddress` has the real geographic data
        address = post.get("address") or {}
        postal = address.get("postalAddress") or {}
        addr_country = postal.get("addressCountry", "") or ""
        addr_region = postal.get("addressRegion", "") or ""
        addr_city = postal.get("addressLocality", "") or ""

        if loc.strip().lower() in ("remote", "hybrid", "on-site", "onsite", ""):
            # Build location from postal address
            addr_parts = filter(None, [addr_city, addr_region, addr_country])
            addr_loc = ", ".join(addr_parts)
            if addr_loc:
                workplace_type = loc.strip() if loc.strip() else (post.get("workplaceType") or "")
                if workplace_type.lower() == "remote" and addr_loc:
                    loc = f"Remote, {addr_loc}"
                elif addr_loc:
                    loc = addr_loc

        # Country from address or location string
        country = addr_country
        if not country and loc:
            # Fallback: "City, Country" or "Remote - Country"
            parts = [p.strip() for p in loc.replace(" - ", ", ").split(",")]
            if len(parts) >= 2:
                country = parts[-1]

        # Workplace type from workplaceType field (more reliable than employmentType)
        wt = post.get("workplaceType", "") or post.get("employmentType", "")

        jobs.append({
            "title": post.get("title", "").strip(),
            "url": post.get("jobUrl", "") or f"https://jobs.ashbyhq.com/{slug}/{post.get('id', '')}",
            "company": company_name,
            "location": loc,
            "country": country,
            "department": dept,
            "workplace_type": wt,
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
        r = _get_session().get(url, timeout=REQUEST_TIMEOUT, headers=headers)
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
        # BambooHR has two location objects:
        #   "location": {"city": ..., "state": ...}
        #   "atsLocation": {"country": ..., "state": ..., "province": ..., "city": ...}
        loc = job.get("location") or {}
        ats_loc = job.get("atsLocation") or {}
        if isinstance(loc, dict):
            city = loc.get("city", "") or ""
            state = loc.get("state", "") or ""
            country = loc.get("country", "") or ""
        else:
            city, state, country = (str(loc) if loc else ""), "", ""

        # Fallback to atsLocation if primary location is empty
        if not city and not state and not country and isinstance(ats_loc, dict):
            city = ats_loc.get("city", "") or ats_loc.get("province", "") or ""
            state = ats_loc.get("state", "") or ""
            country = ats_loc.get("country", "") or ""

        location = ", ".join(filter(None, [city, state, country]))

        dept = job.get("departmentLabel", "") or ""
        desc = _snippet(job.get("description", "") or "")
        salary = _extract_salary(desc)

        jobs.append({
            "title": (job.get("jobOpeningName") or "").strip(),
            "url": f"https://{slug}.bamboohr.com/careers/{job.get('id', '')}",
            "company": slug.replace("-", " ").title(),
            "location": location,
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
    sitemap_url = f"https://{slug}.icims.com/sitemap.xml"
    headers = {
        "Accept": "application/xml",
        "User-Agent": random.choice(USER_AGENTS),
    }
    try:
        r = _get_session().get(sitemap_url, timeout=REQUEST_TIMEOUT, headers=headers)
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
            r = _get_session().post(
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

            # Skip stale postings (30+ days old)
            posted_on = post.get("postedOn", "") or ""
            if "30+" in posted_on:
                continue

            # Location: try locationsText first, then bulletFields
            location = (post.get("locationsText") or "").strip()
            if not location:
                # bulletFields = [cities, states/regions, jobID]
                bf = post.get("bulletFields") or []
                if len(bf) >= 2:
                    cities = bf[0] if bf[0] else ""
                    states = bf[1] if bf[1] else ""
                    location = f"{cities}, {states}" if cities and states else (cities or states)
            location = location[:200]

            # Remote type from API
            remote_type = post.get("remoteType", "") or ""

            all_jobs.append({
                "title": (post.get("title") or "").strip(),
                "url": f"{base_url}/{site_id}{job_path}",
                "company": company.replace("-", " ").title(),
                "location": location,
                "country": "",
                "department": "",
                "workplace_type": remote_type,
                "employment_type": "",
                "salary": "",
                "description_snippet": "",  # Would need per-job fetch, too slow
                "source_ats": "Workday",
                "slug": slug,
                "posted_on": posted_on,
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
            "location": location,
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
            "location": location,
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
                "location": location,
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


# ── Taleo (Oracle legacy) ────────────────────────────────

def scrape_taleo(slug: str) -> list[dict]:
    """Taleo REST API scraper — direct POST, no session/CSRF needed.
    Slug format: 'company|section|portal_id' or 'company|section' (portal auto-discovered)."""
    import json as _json
    parts = slug.split("|")
    if len(parts) == 3:
        company, section, portal_id = parts
    elif len(parts) == 2:
        company, section = parts
        # Auto-discover portal ID from career page
        career_url = f"https://{company}.taleo.net/careersection/{section}/jobsearch.ftl"
        r = _get(career_url, headers={"User-Agent": random.choice(USER_AGENTS)})
        if not r:
            log.debug(f"Taleo: could not fetch career page for {company}/{section}")
            return []
        portal_match = re.search(r'portal\s*=\s*["\']?(\d+)', r.text, re.I)
        if not portal_match:
            log.debug(f"Taleo: could not extract portal ID for {company}/{section}")
            return []
        portal_id = portal_match.group(1)
        log.debug(f"Taleo: auto-discovered portal={portal_id} for {company}/{section}")
    else:
        log.debug(f"Invalid Taleo slug format: {slug}")
        return []
    base_url = f"https://{company}.taleo.net/careersection"
    api_url = f"{base_url}/rest/jobboard/searchjobs"

    all_jobs = []
    page_no = 1

    while True:
        payload = {
            "multilineEnabled": False,
            "sortingSelection": {
                "sortBySelectionParam": "1",
                "ascendingSortingOrder": "false",
            },
            "fieldData": {
                "fields": {"KEYWORD": "", "LOCATION": ""},
                "valid": True,
            },
            "filterSelectionParam": {
                "searchFilterSelections": [
                    {"id": "POSTING_DATE", "selectedValues": []},
                    {"id": "LOCATION", "selectedValues": []},
                    {"id": "JOB_FIELD", "selectedValues": []},
                    {"id": "JOB_TYPE", "selectedValues": []},
                    {"id": "JOB_SCHEDULE", "selectedValues": []},
                ]
            },
            "advancedSearchFiltersSelectionParam": {
                "searchFilterSelections": [
                    {"id": "LOCATION", "selectedValues": []},
                    {"id": "JOB_FIELD", "selectedValues": []},
                    {"id": "JOB_NUMBER", "selectedValues": []},
                    {"id": "ORGANIZATION", "selectedValues": []},
                ]
            },
            "pageNo": page_no,
        }

        headers = {
            "Content-Type": "application/json",
            "tz": "GMT-05:00",
            "User-Agent": random.choice(USER_AGENTS),
        }

        try:
            resp = _get_session().post(
                api_url,
                params={"lang": "en", "portal": portal_id},
                headers=headers,
                data=_json.dumps(payload),
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                log.debug(f"Taleo: API returned {resp.status_code} for {company}")
                break
            data = resp.json()
        except Exception as e:
            log.debug(f"Taleo: API request failed for {company}: {e}")
            break

        requisitions = data.get("requisitionList", [])
        if not requisitions:
            break

        for req in requisitions:
            contest_no = req.get("contestNo", "")

            # Column array: [title, location_json, posted_date]
            columns = req.get("column", [])
            title = columns[0] if len(columns) > 0 else ""
            location_raw = columns[1] if len(columns) > 1 else ""

            # Location comes as JSON string: '["United States-Iowa-Des Moines"]'
            location = location_raw
            country = ""
            try:
                loc_list = _json.loads(location_raw) if location_raw.startswith("[") else []
                if loc_list:
                    location = "; ".join(loc_list[:3])
                    # Extract country from first entry: "Country-State-City"
                    first_loc = loc_list[0]
                    loc_parts = first_loc.split("-")
                    if loc_parts:
                        country = loc_parts[0].strip()
            except Exception:
                pass

            job_url = f"{base_url}/{section}/jobdetail.ftl?job={contest_no}"

            all_jobs.append({
                "title": str(title).strip(),
                "url": job_url,
                "company": company.replace("-", " ").replace("_", " ").title(),
                "location": location,
                "country": country,
                "department": "",
                "workplace_type": "",
                "employment_type": "",
                "salary": "",
                "description_snippet": "",
                "source_ats": "Taleo",
                "slug": slug,
            })

        # Pagination
        paging = data.get("pagingData", {})
        total_count = paging.get("totalCount", 0)
        if len(all_jobs) >= total_count or not requisitions:
            break

        page_no += 1
        time.sleep(random.uniform(0.3, 1.0))

    return all_jobs


# ── Oracle Cloud HCM ────────────────────────────────────

def scrape_oracle_cloud_hcm(slug: str) -> list[dict]:
    """Oracle Cloud HCM Recruiting REST API.
    Slug format: 'host_prefix|site_number' (e.g. 'eeho.fa.us2|CX_1')
    or legacy 'tenant|site_number' (e.g. 'eeho|CX_1') or tenant-only."""
    parts = slug.split("|")
    if len(parts) == 2:
        host_prefix, site_number = parts
    elif len(parts) == 1:
        host_prefix = parts[0]
        site_number = None
    else:
        log.debug(f"Invalid Oracle Cloud HCM slug format: {slug}")
        return []

    import uuid as _uuid
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "ora-irc-cx-userid": str(_uuid.uuid4()),
        "ora-irc-language": "en",
        "content-type": "application/vnd.oracle.adf.resourceitem+json;charset=utf-8",
    }

    # Build API URL — host_prefix can be 'eeho.fa.us2' (new) or 'eeho' (legacy)
    if ".fa." in host_prefix or "." in host_prefix:
        # Full host prefix like 'eeho.fa.us2' or 'idcs-xxx.identity'
        base_api = f"https://{host_prefix}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
        tenant = host_prefix.split(".")[0]
    else:
        # Legacy short tenant — discover full domain via career page redirect
        tenant = host_prefix
        base_api = None

        # Method 1: Hit career page, follow redirects, extract real domain
        for try_site in (site_number or "CX_1", "CX_1", "CX", "CX_2"):
            try:
                probe_url = f"https://{tenant}.oraclecloud.com/hcmUI/CandidateExperience/en/sites/{try_site}/requisitions"
                probe_r = _get_session().get(probe_url, headers={
                    "User-Agent": random.choice(USER_AGENTS)}, timeout=15, allow_redirects=True)
                # Check if we got redirected to a URL with .fa.{region}
                final_host = probe_r.url.split("/")[2] if probe_r.url else ""
                if ".fa." in final_host and "oraclecloud.com" in final_host:
                    real_prefix = final_host.replace(".oraclecloud.com", "")
                    base_api = f"https://{real_prefix}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
                    if not site_number:
                        site_number = try_site
                    log.debug(f"Oracle Cloud HCM: discovered domain={real_prefix} via redirect for {tenant}")
                    break
            except Exception:
                continue

        # Method 2: Brute-force common regions via API
        if not base_api:
            for region in ("fa.us2", "fa.us6", "fa.us1", "fa.em2", "fa.em3", "fa.em4",
                           "fa.ap1", "fa.ap2", "fa.ca1", "fa.sa1", "fa.me1"):
                test_url = f"https://{tenant}.{region}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
                try:
                    test_r = _get_session().get(test_url,
                        params={"onlyData": "true", "finder": f"findReqs;siteNumber={site_number or 'CX_1'},limit=1,offset=0"},
                        headers=headers, timeout=8)
                    if test_r.status_code == 200:
                        try:
                            data = test_r.json()
                            items = data.get("items", [])
                            if items and items[0].get("requisitionList"):
                                base_api = test_url
                                if not site_number:
                                    site_number = "CX_1"
                                log.debug(f"Oracle Cloud HCM: discovered region={region} for {tenant}")
                                break
                        except Exception:
                            continue
                except Exception:
                    continue

        if not base_api:
            log.debug(f"Oracle Cloud HCM: could not discover domain for {tenant}")
            return []

    # Auto-discover site number for tenant-only slugs
    if not site_number:
        for try_site in ("CX_1", "CX", "CX_2", "CX_3"):
            test_params = {
                "onlyData": "true",
                "finder": f"findReqs;siteNumber={try_site},limit=1,offset=0",
            }
            test_r = _get(base_api, params=test_params, headers=headers)
            if test_r and test_r.status_code == 200:
                try:
                    test_data = test_r.json()
                    test_items = test_data.get("items", [])
                    if test_items and test_items[0].get("requisitionList"):
                        site_number = try_site
                        log.debug(f"Oracle Cloud HCM: auto-discovered site={site_number} for {tenant}")
                        break
                except Exception:
                    continue
        if not site_number:
            log.debug(f"Oracle Cloud HCM: could not discover site number for {tenant}")
            return []

    all_jobs = []
    offset = 0
    limit = 25

    while True:
        params = {
            "onlyData": "true",
            "expand": "requisitionList.workLocation",
            "finder": f"findReqs;siteNumber={site_number},limit={limit},offset={offset}",
        }

        r = _get(base_api, params=params, headers=headers)
        if not r:
            log.debug(f"Oracle Cloud HCM: API request failed for {tenant}/{site_number} offset={offset}")
            break

        try:
            data = r.json()
        except Exception as e:
            log.debug(f"Oracle Cloud HCM: JSON parse failed for {tenant}/{site_number}: {e}")
            break

        items = data.get("items", [])
        if not items:
            break

        # The requisition list is nested inside the first item
        first_item = items[0] if items else {}
        req_list = first_item.get("requisitionList", [])

        if not req_list:
            break

        for req in req_list:
            title = req.get("Title", "")
            job_id = req.get("Id", "")
            primary_location = req.get("PrimaryLocation", "")
            categories = req.get("CategoriesDisplay", "")
            workplace_type = req.get("WorkplaceTypeDisplay", "")
            description_html = req.get("ExternalDescriptionStr", "")

            desc = _snippet(description_html)
            salary = _extract_salary(desc)

            # Build job URL — extract host from base_api
            api_host = base_api.split("/hcmRestApi")[0]
            job_url = (
                f"{api_host}/hcmUI/CandidateExperience"
                f"/en/sites/{site_number}/job/{job_id}"
            )

            # Try to extract country from location
            country = ""
            if primary_location:
                loc_parts = [p.strip() for p in primary_location.split(",")]
                if len(loc_parts) >= 2:
                    country = loc_parts[-1]

            all_jobs.append({
                "title": str(title).strip(),
                "url": job_url,
                "company": tenant.replace("-", " ").replace("_", " ").title(),
                "location": primary_location,
                "country": country,
                "department": categories,
                "workplace_type": workplace_type,
                "employment_type": "",
                "salary": salary,
                "description_snippet": desc,
                "source_ats": "Oracle Cloud HCM",
                "slug": slug,
            })

        # Check pagination
        has_more = first_item.get("hasMore", False)
        total_count = first_item.get("totalCount", 0) or first_item.get("count", 0)

        if not has_more and total_count and len(all_jobs) >= total_count:
            break
        if not has_more and not total_count:
            # If no hasMore flag and no total, check if we got fewer than limit
            if len(req_list) < limit:
                break

        offset += limit
        time.sleep(random.uniform(0.3, 1.0))

    return all_jobs


# ── BrassRing (IBM/Infinite) ─────────────────────────────

def scrape_brassring(slug: str) -> list[dict]:
    """BrassRing search API scraper. Slug format: 'partner_id|site_id'."""
    parts = slug.split("|")
    if len(parts) != 2:
        log.debug(f"Invalid BrassRing slug format: {slug} (expected 'partner_id|site_id')")
        return []

    partner_id, site_id = parts
    search_url = "https://sjobs.brassring.com/TgNewUI/Search/Ajax/MatchedJobs"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": random.choice(USER_AGENTS),
    }

    all_jobs = []
    page = 1

    while True:
        form_data = (
            f"partnerid={partner_id}&siteid={site_id}"
            f"&keyword=&location=&pagenum={page}"
            f"&sortBy=posteddate&SortType=desc"
        )

        try:
            r = _get_session().post(
                search_url,
                data=form_data,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if r.status_code != 200:
                log.debug(f"BrassRing: API returned {r.status_code} for {slug} page {page}")
                break
            data = r.json()
        except Exception as e:
            log.debug(f"BrassRing: request failed for {slug}: {e}")
            break

        jobs_array = data.get("Jobs", [])
        if not jobs_array:
            break

        for job in jobs_array:
            auto_req_id = job.get("AutoReqId", "")
            title = job.get("JobTitle", "")
            location = job.get("JobInfo1", "")
            department = job.get("JobInfo3", "")
            job_type = job.get("JobInfo2", "")
            short_desc = _snippet(job.get("formattedShortDescription", ""))
            salary = _extract_salary(short_desc)

            # Try to extract country from location
            country = ""
            if location:
                loc_parts = [p.strip() for p in location.split(",")]
                if len(loc_parts) >= 2:
                    country = loc_parts[-1]

            job_url = (
                f"https://sjobs.brassring.com/TgNewUI/Search/home/HomeWithPreLoad"
                f"?partnerid={partner_id}&siteid={site_id}&jobid={auto_req_id}"
            )

            all_jobs.append({
                "title": str(title).strip(),
                "url": job_url,
                "company": partner_id,
                "location": location,
                "country": country,
                "department": department,
                "workplace_type": "",
                "employment_type": job_type,
                "salary": salary,
                "description_snippet": short_desc,
                "source_ats": "BrassRing",
                "slug": slug,
            })

        total_hits = data.get("TotalHits", 0)
        fetched_so_far = page * 50
        if fetched_so_far >= total_hits:
            break

        page += 1
        time.sleep(random.uniform(0.3, 1.0))

    return all_jobs


# ── Teamtailor ───────────────────────────────────────────

def scrape_teamtailor(slug: str) -> list[dict]:
    """Teamtailor RSS feed scraper with HTML fallback.
    Slug is the company subdomain (e.g. 'spotify').
    RSS feed includes tt: namespace with structured location data."""
    company_name = slug.capitalize()
    rss_url = f"https://{slug}.teamtailor.com/jobs.rss"

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/rss+xml, application/xml, text/xml",
    }

    # Teamtailor XML namespace for structured location/department data
    TT_NS = {"tt": "https://teamtailor.com/locations"}

    # ── Primary: RSS feed ──
    r = _get(rss_url, headers=headers)
    if r and r.status_code == 200:
        try:
            root = ET.fromstring(r.content)
            jobs = []

            for item in root.iter("item"):
                title_el = item.find("title")
                link_el = item.find("link")
                desc_el = item.find("description")
                category_el = item.find("category")

                title = (title_el.text or "").strip() if title_el is not None else ""
                link = (link_el.text or "").strip() if link_el is not None else ""
                desc_html = (desc_el.text or "") if desc_el is not None else ""

                # ── Department: prefer tt:department, fallback to <category> ──
                tt_dept = item.findtext("tt:department", default=None, namespaces=TT_NS)
                department = (tt_dept or "").strip() if tt_dept else ""
                if not department:
                    department = (category_el.text or "").strip() if category_el is not None else ""

                # ── Location: parse tt:locations namespace ──
                location_parts = []
                country = ""
                remote_status = (item.findtext("remoteStatus") or "").strip()
                for loc_el in item.findall("tt:locations/tt:location", TT_NS):
                    # Prefer tt:name (pre-formatted), fallback to city+country
                    loc_name = (loc_el.findtext("tt:name", namespaces=TT_NS) or "").strip()
                    if loc_name:
                        location_parts.append(loc_name)
                    else:
                        city = (loc_el.findtext("tt:city", namespaces=TT_NS) or "").strip()
                        ctry = (loc_el.findtext("tt:country", namespaces=TT_NS) or "").strip()
                        combined = ", ".join(p for p in [city, ctry] if p)
                        if combined:
                            location_parts.append(combined)
                    # Capture country from first location
                    if not country:
                        country = (loc_el.findtext("tt:country", namespaces=TT_NS) or "").strip()

                location = "; ".join(location_parts[:3]) if location_parts else ""

                # If remote_status is set, append it
                if remote_status and remote_status.lower() != "none":
                    if location:
                        location = f"{location} ({remote_status})"
                    else:
                        location = remote_status.capitalize()

                desc = _snippet(desc_html)
                salary = _extract_salary(desc)

                jobs.append({
                    "title": title,
                    "url": link,
                    "company": company_name,
                    "location": location,
                    "country": country,
                    "department": department,
                    "workplace_type": remote_status if remote_status and remote_status.lower() != "none" else "",
                    "employment_type": "",
                    "salary": salary,
                    "description_snippet": desc,
                    "source_ats": "Teamtailor",
                    "slug": slug,
                })

            if jobs:
                return jobs
        except ET.ParseError:
            log.debug(f"Teamtailor: RSS XML parse failed for {slug}, trying HTML fallback")

    # ── Fallback: HTML scrape ──
    html_url = f"https://{slug}.teamtailor.com/jobs"
    r = _get(html_url, headers=headers)
    if not r:
        return []

    jobs = []
    # Job links follow pattern: /jobs/{id}-{slug-title}
    for match in re.finditer(r'href=["\'](/jobs/(\d+)-[^"\']+)["\']', r.text):
        path = match.group(1)
        job_url = f"https://{slug}.teamtailor.com{path}"

        # Derive title from the slug portion of the URL
        slug_part = path.split("/jobs/")[-1] if "/jobs/" in path else ""
        # Remove the numeric prefix: "12345-some-job-title" -> "some-job-title"
        title_slug = re.sub(r"^\d+-", "", slug_part)
        title = title_slug.replace("-", " ").strip().title()

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": "",
            "country": "",
            "department": "",
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "Teamtailor",
            "slug": slug,
        })

    # Deduplicate by URL (HTML may have repeated links)
    seen_urls = set()
    unique_jobs = []
    for j in jobs:
        if j["url"] not in seen_urls:
            seen_urls.add(j["url"])
            unique_jobs.append(j)

    return unique_jobs


# ── SAP SuccessFactors ─────────────────────────────────

def scrape_successfactors(slug: str) -> list[dict]:
    """SAP SuccessFactors career site scraper.
    Slug format: 'instance|company_key' (e.g. 'performancemanager5.successfactors.eu|companyKey').
    Uses the career site JSON API at /xi/ui/pages/careersite/api/v1/jobs."""
    parts = slug.split("|")
    if len(parts) != 2:
        log.debug(f"Invalid SuccessFactors slug format: {slug} (expected 'instance|company_key')")
        return []

    instance, company_key = parts
    base_url = f"https://{instance}"
    api_url = f"{base_url}/xi/ui/pages/careersite/api/v1/jobs"

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
    }

    all_jobs = []
    offset = 0
    limit = 20

    while True:
        params = {
            "company": company_key,
            "offset": offset,
            "limit": limit,
        }

        r = _get(api_url, params=params, headers=headers)
        if not r:
            log.debug(f"SuccessFactors: API request failed for {slug} offset={offset}")
            break

        try:
            data = r.json()
        except Exception as e:
            log.debug(f"SuccessFactors: JSON parse failed for {slug}: {e}")
            break

        results = data.get("results", data.get("jobRequisitions", []))
        if not results:
            # Try alternate response shape
            results = data.get("d", {}).get("results", [])
        if not results:
            break

        for req in results:
            title = req.get("jobTitle", req.get("externalTitle", ""))
            job_id = req.get("jobReqId", req.get("id", ""))
            location = req.get("location", req.get("primaryLocation", ""))
            department = req.get("department", req.get("division", ""))
            desc_html = req.get("jobDescription", req.get("externalDescription", ""))
            employment_type = req.get("employmentType", req.get("scheduleType", ""))

            desc = _snippet(desc_html) if desc_html else ""
            salary = _extract_salary(desc) if desc else ""

            # Build job URL
            job_url = f"{base_url}/career?company={company_key}&career_job_req_id={job_id}&career_ns=job_listing_summary"

            # Try to extract country from location
            country = ""
            if location:
                loc_parts = [p.strip() for p in location.split(",")]
                if len(loc_parts) >= 2:
                    country = loc_parts[-1]

            all_jobs.append({
                "title": str(title).strip(),
                "url": job_url,
                "company": company_key.replace("-", " ").replace("_", " ").title(),
                "location": location,
                "country": country,
                "department": department,
                "workplace_type": "",
                "employment_type": employment_type,
                "salary": salary,
                "description_snippet": desc,
                "source_ats": "SAP SuccessFactors",
                "slug": slug,
            })

        # Check pagination
        total = data.get("total", data.get("totalCount", 0))
        if total and len(all_jobs) >= total:
            break
        if len(results) < limit:
            break

        offset += limit
        time.sleep(random.uniform(0.3, 1.0))

    return all_jobs


# ── BreezyHR ────────────────────────────────────────────

def scrape_breezyhr(slug: str) -> list[dict]:
    """BreezyHR — HTML scrape, parses position list items.
    Slug is the company subdomain (e.g. 'acme').
    Extracts location from <li class="location"> and title from <h2>."""
    company_name = slug.replace("-", " ").title()
    base_url = f"https://{slug}.breezy.hr"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = _get(base_url, headers=headers)
    if not r:
        return []

    jobs = []
    seen_urls = set()

    # BreezyHR HTML structure:
    # <li class="position transition">
    #   <a href="/p/<id>-<slug>"><h2>Title</h2>
    #     <ul class="meta">
    #       <li class="location"><span class="polygot">Location</span></li>
    #       <li class="type"><span class="polygot">Full-Time</span></li>
    #     </ul>
    #   </a>
    # </li>
    # Match each position block
    for pos_match in re.finditer(
        r'<li[^>]*class="[^"]*position[^"]*"[^>]*>(.*?)</li>\s*(?=<li[^>]*class="[^"]*position|</ul>|$)',
        r.text, re.I | re.DOTALL
    ):
        block = pos_match.group(1)

        # Extract URL
        href_match = re.search(r'href=["\'](/p/[a-f0-9]+[-/][^"\']+)["\']', block)
        if not href_match:
            continue
        job_url = f"{base_url}{href_match.group(1)}"
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        # Extract title from <h2>
        title_match = re.search(r'<h2[^>]*>(.*?)</h2>', block, re.I | re.DOTALL)
        title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip() if title_match else ""

        # Extract location from <li class="location">
        loc_match = re.search(
            r'<li[^>]*class="[^"]*location[^"]*"[^>]*>(.*?)</li>',
            block, re.I | re.DOTALL
        )
        location = ""
        if loc_match:
            # Strip HTML tags, get text content
            loc_text = re.sub(r'<[^>]+>', ' ', loc_match.group(1)).strip()
            # Clean up multiple spaces
            location = re.sub(r'\s+', ' ', loc_text).strip()

        # Extract employment type
        type_match = re.search(
            r'<li[^>]*class="[^"]*type[^"]*"[^>]*>(.*?)</li>',
            block, re.I | re.DOTALL
        )
        emp_type = ""
        if type_match:
            emp_type = re.sub(r'<[^>]+>', ' ', type_match.group(1)).strip()
            emp_type = re.sub(r'\s+', ' ', emp_type).strip()

        # Try to extract country from location (e.g. "Berlin, Germany")
        country = ""
        if location:
            loc_parts = [p.strip() for p in location.split(",")]
            if len(loc_parts) >= 2:
                country = loc_parts[-1]

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": location,
            "country": country,
            "department": "",
            "workplace_type": "",
            "employment_type": emp_type,
            "salary": "",
            "description_snippet": "",
            "source_ats": "BreezyHR",
            "slug": slug,
        })

    # Fallback: if no position blocks found, try simple link extraction
    if not jobs:
        for match in re.finditer(
            r'href=["\'](/p/([a-f0-9]+)[-/]([^"\']+))["\']', r.text
        ):
            path = match.group(1)
            job_url = f"{base_url}{path}"
            if job_url in seen_urls:
                continue
            seen_urls.add(job_url)
            title_slug = match.group(3).rstrip("/")
            title = title_slug.replace("-", " ").strip().title()
            jobs.append({
                "title": title, "url": job_url, "company": company_name,
                "location": "", "country": "", "department": "",
                "workplace_type": "", "employment_type": "", "salary": "",
                "description_snippet": "", "source_ats": "BreezyHR", "slug": slug,
            })

    return jobs


# ── ApplyToJob ──────────────────────────────────────────

def scrape_applytojob(slug: str) -> list[dict]:
    """ApplyToJob (JazzHR) — HTML scrape, parses job listings.
    Slug is the company subdomain (e.g. 'acme').
    Extracts location from fa-map-marker icons. Deduplicates by title."""
    company_name = slug.replace("-", " ").title()
    base_url = f"https://{slug}.applytojob.com"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = _get(base_url, headers=headers)
    if not r:
        return []

    jobs = []
    seen_urls = set()
    seen_titles = set()  # deduplicate by title (companies post same job many times)

    # Pattern 1: Newer layout — list-group-item with heading + location icon
    # HTML: <li class="list-group-item">
    #         <h3 class="list-group-item-heading"><a href="...">Title</a></h3>
    #         <ul class="list-group-item-text">
    #           <li><i class="fa fa-map-marker"></i>Location</li>
    #         </ul>
    #       </li>
    for item_match in re.finditer(
        r'<li[^>]*class="list-group-item"[^>]*>(.*?)</li>\s*(?=<li[^>]*class="list-group-item"|</ul>|$)',
        r.text, re.I | re.DOTALL
    ):
        item_html = item_match.group(1)

        # Extract title + URL from heading
        link_match = re.search(
            r'class="list-group-item-heading"[^>]*>.*?'
            r'<a\s+href=["\']([^"\']+)["\'][^>]*>([^<]+)</a>',
            item_html, re.I | re.DOTALL
        )
        if not link_match:
            continue

        url = link_match.group(1).strip()
        title = link_match.group(2).strip()
        if not url.startswith("http"):
            url = base_url + url

        # Extract location from fa-map-marker icon
        loc_match = re.search(
            r'fa-map-marker["\'][^>]*></i>\s*([^<]+)',
            item_html, re.I
        )
        location = loc_match.group(1).strip() if loc_match else ""

        title_key = title.lower().strip()
        if url not in seen_urls and title_key not in seen_titles:
            seen_urls.add(url)
            seen_titles.add(title_key)
            jobs.append({
                "title": title,
                "url": url,
                "company": company_name,
                "location": location,
                "country": "",
                "department": "",
                "workplace_type": "",
                "employment_type": "",
                "salary": "",
                "description_snippet": "",
                "source_ats": "ApplyToJob",
                "slug": slug,
            })

    # Pattern 2: Legacy layout — resumator-job-title-link
    if not jobs:
        for match in re.finditer(
            r'class="resumator-job-title-link"[^>]*href=["\']([^"\']+)["\'][^>]*>([^<]+)</a>',
            r.text, re.I
        ):
            url = match.group(1).strip()
            title = match.group(2).strip()
            if not url.startswith("http"):
                url = base_url + url
            title_key = title.lower().strip()
            if url not in seen_urls and title_key not in seen_titles:
                seen_urls.add(url)
                seen_titles.add(title_key)
                jobs.append({
                    "title": title,
                    "url": url,
                    "company": company_name,
                    "location": "",
                    "country": "",
                    "department": "",
                    "workplace_type": "",
                    "employment_type": "",
                    "salary": "",
                    "description_snippet": "",
                    "source_ats": "ApplyToJob",
                    "slug": slug,
                })

    # Pattern 3: Generic fallback — any link to /apply/ pages
    if not jobs:
        for match in re.finditer(
            r'<a\s+[^>]*href=["\']([^"\']*(?:/apply/|/opening/)[^"\']*)["\'][^>]*>'
            r'([^<]+)</a>',
            r.text, re.I
        ):
            url = match.group(1).strip()
            title = match.group(2).strip()
            if not url.startswith("http"):
                url = base_url + url
            title_key = title.lower().strip()
            if url not in seen_urls and title_key not in seen_titles and len(title) > 3:
                seen_urls.add(url)
                seen_titles.add(title_key)
                jobs.append({
                    "title": title,
                    "url": url,
                    "company": company_name,
                    "location": "",
                    "country": "",
                    "department": "",
                    "workplace_type": "",
                    "employment_type": "",
                    "salary": "",
                    "description_snippet": "",
                    "source_ats": "ApplyToJob",
                    "slug": slug,
                })

    # Try to extract location from nearby elements
    for job in jobs:
        loc_match = re.search(
            re.escape(job["title"]) + r'</a>.*?class="[^"]*location[^"]*"[^>]*>([^<]+)',
            r.text, re.I | re.DOTALL
        )
        if loc_match:
            job["location"] = loc_match.group(1).strip()

    return jobs


# ── HRMDirect ───────────────────────────────────────────

def scrape_hrmdirect(slug: str) -> list[dict]:
    """HRMDirect / ClearCompany — HTML scrape of job openings table.
    Slug is the company subdomain (e.g. 'novabio').
    Uses ?search=true to force all jobs to display (not just filter dropdowns)."""
    company_name = slug.replace("-", " ").title()
    url = f"https://{slug}.hrmdirect.com/employment/openings.php?search=true"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = _get(url, headers=headers)
    if not r:
        return []

    jobs = []
    seen_urls = set()

    # Parse table rows — each <tr> contains <td> cells with job link, city, state, country
    # Split by <tr to process row by row
    rows = re.split(r'<tr[^>]*>', r.text, flags=re.I)
    for row_html in rows:
        # Find job link in this row
        link_match = re.search(
            r'<a\s+[^>]*href=["\']([^"\']*job-opening\.php\?req=\d+[^"\']*)["\'][^>]*>'
            r'\s*([^<]+)</a>',
            row_html, re.I
        )
        if not link_match:
            continue

        job_path = link_match.group(1).strip()
        title = link_match.group(2).strip()

        if not job_path.startswith("http"):
            job_url = f"https://{slug}.hrmdirect.com/employment/{job_path}"
        else:
            job_url = job_path

        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        # Extract ALL <td> cell contents from this row
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.I | re.DOTALL)
        # Strip HTML tags from cells
        clean_cells = []
        for cell in cells:
            text = re.sub(r'<[^>]+>', '', cell).strip()
            clean_cells.append(text)

        # HRMDirect tables vary but commonly:
        # [department?, title, city, state, country?] or [title, city, state]
        # Find the cell index that contains the title to know the layout
        title_idx = -1
        for i, c in enumerate(clean_cells):
            if title in c:
                title_idx = i
                break

        city = ""
        state = ""
        country = ""
        department = ""

        if title_idx >= 0:
            remaining = clean_cells[title_idx + 1:]
            if len(remaining) >= 1:
                city = remaining[0]
            if len(remaining) >= 2:
                state = remaining[1]
            if len(remaining) >= 3:
                country = remaining[2]
            # Department is usually before the title
            if title_idx >= 1:
                department = clean_cells[title_idx - 1]

        location = city
        if state and city:
            location = f"{city}, {state}"
        elif state:
            location = state
        if country and country not in location:
            location = f"{location}, {country}" if location else country

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": location,
            "country": country,
            "department": department,
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "HRMDirect",
            "slug": slug,
        })

    return jobs


# ── Softgarden ──────────────────────────────────────────

def scrape_softgarden(slug: str) -> list[dict]:
    """Softgarden — REST API scraper.
    Slug is the channel/board ID.
    API: GET https://api.softgarden.io/api/rest/v3/frontend/jobboards/{channelId}/jobs"""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
    }

    # Try API endpoint first
    api_url = f"https://api.softgarden.io/api/rest/v3/frontend/jobboards/{slug}/jobs"
    r = _get(api_url, headers=headers, params={"limit": 100, "offset": 0})

    if r:
        try:
            data = r.json()
            results = data.get("results") or data.get("jobs") or data.get("content") or []
            if isinstance(data, list):
                results = data

            jobs = []
            for item in results:
                loc = item.get("geo_city", "") or item.get("city", "") or ""
                country = item.get("geo_country", "") or item.get("country", "") or ""

                desc_html = item.get("jobDescription", "") or item.get("description", "") or ""
                desc = _snippet(desc_html)
                salary = _extract_salary(desc)

                job_id = item.get("jobDbId") or item.get("id") or ""
                job_url = item.get("jobUrl") or item.get("url") or ""
                if not job_url and job_id:
                    job_url = f"https://jobdb.softgarden.de/jobdb/public/jobposting/{job_id}/applicationForm"

                jobs.append({
                    "title": (item.get("jobName") or item.get("title") or "").strip(),
                    "url": job_url,
                    "company": (item.get("companyName") or item.get("company") or "").strip(),
                    "location": loc,
                    "country": country,
                    "department": (item.get("audience") or item.get("department") or "").strip(),
                    "workplace_type": (item.get("workplaceType") or "").strip(),
                    "employment_type": (item.get("employmentType") or item.get("projectNumber") or "").strip(),
                    "salary": salary,
                    "description_snippet": desc,
                    "source_ats": "Softgarden",
                    "slug": slug,
                })

            if jobs:
                return jobs
        except Exception:
            pass

    # Fallback: HTML scrape of the softgarden career page
    html_url = f"https://{slug}.softgarden.io/job/list"
    r = _get(html_url, headers={"User-Agent": random.choice(USER_AGENTS)})
    if not r:
        return []

    jobs = []
    seen = set()

    # Parse JSON-LD structured data
    for ld_match in re.finditer(
        r'<script[^>]*type="application/ld\+json"[^>]*>([^<]+)</script>',
        r.text, re.I
    ):
        try:
            import json
            ld_data = json.loads(ld_match.group(1))
            items = ld_data if isinstance(ld_data, list) else [ld_data]
            for item in items:
                if item.get("@type") != "JobPosting":
                    continue
                job_url = item.get("url", "")
                if job_url in seen:
                    continue
                seen.add(job_url)

                loc_obj = item.get("jobLocation", {})
                if isinstance(loc_obj, dict):
                    addr = loc_obj.get("address", {})
                    loc = addr.get("addressLocality", "")
                    country = addr.get("addressCountry", "")
                elif isinstance(loc_obj, list) and loc_obj:
                    addr = loc_obj[0].get("address", {})
                    loc = addr.get("addressLocality", "")
                    country = addr.get("addressCountry", "")
                else:
                    loc = ""
                    country = ""

                desc = _snippet(item.get("description", ""))
                salary = _extract_salary(desc)
                org = item.get("hiringOrganization", {})

                jobs.append({
                    "title": item.get("title", "").strip(),
                    "url": job_url,
                    "company": (org.get("name", "") if isinstance(org, dict) else "").strip(),
                    "location": loc,
                    "country": country,
                    "department": "",
                    "workplace_type": "",
                    "employment_type": item.get("employmentType", ""),
                    "salary": salary,
                    "description_snippet": desc,
                    "source_ats": "Softgarden",
                    "slug": slug,
                })
        except Exception:
            continue

    return jobs


# ── Zoho Recruit ────────────────────────────────────────

def scrape_zoho(slug: str) -> list[dict]:
    """Zoho Recruit — HTML scrape with embedded JSON.
    Slug is the company subdomain (e.g. 'acme').
    Parses hidden input#jobs JSON data."""
    company_name = slug.replace("-", " ").title()
    url = f"https://{slug}.zohorecruit.com/jobs/Careers"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = _get(url, headers=headers)
    if not r:
        return []

    jobs = []

    # Primary: Parse hidden input with jobs JSON
    # Try id="jobs" and name="jobs", both attribute orders
    jobs_input = None
    for attr in ('id', 'name'):
        if jobs_input:
            break
        # attr before value
        jobs_input = re.search(
            rf'<input[^>]*{attr}=["\']jobs["\'][^>]*value=["\']([^"\']+)["\']',
            r.text, re.I
        )
        if not jobs_input:
            # value before attr
            jobs_input = re.search(
                rf'<input[^>]*value=["\']([^"\']+)["\'][^>]*{attr}=["\']jobs["\']',
                r.text, re.I
            )
    if jobs_input:
        import json
        try:
            raw = jobs_input.group(1)
            # Unescape HTML entities
            raw = raw.replace("&quot;", '"').replace("&amp;", "&")
            raw = raw.replace("&lt;", "<").replace("&gt;", ">")
            raw = raw.replace("&#39;", "'")
            job_data = json.loads(raw)

            if isinstance(job_data, list):
                for item in job_data:
                    title = item.get("Posting_Title") or item.get("Job_Opening_Name") or ""
                    job_id = item.get("id") or item.get("Job Opening Id") or ""
                    job_url_val = item.get("$url") or ""
                    if not job_url_val and job_id:
                        job_url_val = f"https://{slug}.zohorecruit.com/jobs/Careers/{job_id}"

                    loc = item.get("City") or item.get("city") or ""
                    state = item.get("State") or ""
                    country = item.get("Country") or ""
                    if state and loc:
                        loc = f"{loc}, {state}"

                    salary = item.get("Salary") or ""
                    desc = _snippet(item.get("Job_Description") or item.get("description") or "")
                    if not salary:
                        salary = _extract_salary(desc)

                    jobs.append({
                        "title": title.strip(),
                        "url": job_url_val,
                        "company": company_name,
                        "location": loc.strip(),
                        "country": country.strip() if isinstance(country, str) else "",
                        "department": (item.get("Department") or "").strip(),
                        "workplace_type": (item.get("Remote_Job") or item.get("Work_Mode") or "").strip(),
                        "employment_type": (item.get("Job_Type") or item.get("jobtype") or "").strip(),
                        "salary": str(salary).strip() if salary else "",
                        "description_snippet": desc,
                        "source_ats": "Zoho",
                        "slug": slug,
                    })
        except (json.JSONDecodeError, Exception) as e:
            log.debug(f"Zoho: JSON parse failed for {slug}: {e}")

    # Fallback: Parse JSON-LD structured data
    if not jobs:
        for ld_match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>([^<]+)</script>',
            r.text, re.I
        ):
            try:
                import json
                ld_data = json.loads(ld_match.group(1))
                items = ld_data if isinstance(ld_data, list) else [ld_data]
                for item in items:
                    if item.get("@type") != "JobPosting":
                        continue
                    loc_obj = item.get("jobLocation", {})
                    addr = loc_obj.get("address", {}) if isinstance(loc_obj, dict) else {}
                    desc = _snippet(item.get("description", ""))
                    salary = _extract_salary(desc)
                    org = item.get("hiringOrganization", {})

                    jobs.append({
                        "title": item.get("title", "").strip(),
                        "url": item.get("url", ""),
                        "company": (org.get("name", "") if isinstance(org, dict) else company_name).strip(),
                        "location": addr.get("addressLocality", ""),
                        "country": addr.get("addressCountry", ""),
                        "department": "",
                        "workplace_type": "",
                        "employment_type": item.get("employmentType", ""),
                        "salary": salary,
                        "description_snippet": desc,
                        "source_ats": "Zoho",
                        "slug": slug,
                    })
            except Exception:
                continue

    # Fallback 2: Generic link scrape
    if not jobs:
        seen = set()
        for match in re.finditer(
            r'<a\s+[^>]*href=["\']([^"\']*(?:/jobs/|/careers/|/opening)[^"\']*)["\'][^>]*>([^<]+)</a>',
            r.text, re.I
        ):
            link = match.group(1).strip()
            title = match.group(2).strip()
            if not link.startswith("http"):
                link = f"https://{slug}.zohorecruit.com{link}"
            if link not in seen and len(title) > 3:
                seen.add(link)
                jobs.append({
                    "title": title,
                    "url": link,
                    "company": company_name,
                    "location": "",
                    "country": "",
                    "department": "",
                    "workplace_type": "",
                    "employment_type": "",
                    "salary": "",
                    "description_snippet": "",
                    "source_ats": "Zoho",
                    "slug": slug,
                })

    return jobs


# ── YCombinator (Work at a Startup) ─────────────────────

def scrape_ycombinator(slug: str) -> list[dict]:
    """YCombinator Work at a Startup — scrapes company pages on workatastartup.com.
    Slug is the company identifier on workatastartup.com."""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/json",
    }
    company_url = f"https://www.workatastartup.com/companies/{slug}"

    r = _get(company_url, headers=headers)
    if not r:
        return []

    jobs = []

    # Try parsing __NEXT_DATA__ or embedded JSON
    next_data_match = re.search(
        r'<script\s+id="__NEXT_DATA__"[^>]*>([^<]+)</script>',
        r.text, re.I
    )
    if next_data_match:
        import json
        try:
            nd = json.loads(next_data_match.group(1))
            # Navigate through Next.js data structure
            props = nd.get("props", {}).get("pageProps", {})
            company = props.get("company", {})
            company_name = company.get("name", slug.replace("-", " ").title())
            job_list = company.get("jobs", []) or props.get("jobs", [])

            for item in job_list:
                loc = item.get("pretty_location") or item.get("location") or ""
                desc = _snippet(item.get("description") or "")
                salary_min = item.get("salary_min")
                salary_max = item.get("salary_max")
                salary = ""
                if salary_min and salary_max:
                    salary = f"${salary_min:,}-${salary_max:,}"
                elif salary_min:
                    salary = f"${salary_min:,}+"
                if not salary:
                    salary = _extract_salary(desc)

                job_id = item.get("id") or ""
                job_url = item.get("url") or ""
                if not job_url and job_id:
                    job_url = f"https://www.workatastartup.com/jobs/{job_id}"
                if not job_url:
                    job_url = company_url

                jobs.append({
                    "title": (item.get("title") or "").strip(),
                    "url": job_url,
                    "company": company_name,
                    "location": loc,
                    "country": "",
                    "department": (item.get("role_type") or item.get("role") or "").strip(),
                    "workplace_type": (item.get("remote") or "").strip() if isinstance(item.get("remote"), str) else ("Remote" if item.get("remote") else ""),
                    "employment_type": (item.get("type") or "").strip(),
                    "salary": salary,
                    "description_snippet": desc,
                    "source_ats": "YCombinator",
                    "slug": slug,
                })

            if jobs:
                return jobs
        except Exception as e:
            log.debug(f"YCombinator: NEXT_DATA parse failed for {slug}: {e}")

    # Fallback: parse job links from HTML
    seen = set()
    for match in re.finditer(
        r'<a\s+[^>]*href=["\'](?:https://www\.workatastartup\.com)?(/jobs/\d+)["\'][^>]*>'
        r'\s*([^<]+)</a>',
        r.text, re.I
    ):
        path = match.group(1)
        title = match.group(2).strip()
        job_url = f"https://www.workatastartup.com{path}"
        if job_url not in seen and len(title) > 3:
            seen.add(job_url)
            jobs.append({
                "title": title,
                "url": job_url,
                "company": slug.replace("-", " ").title(),
                "location": "",
                "country": "",
                "department": "",
                "workplace_type": "",
                "employment_type": "",
                "salary": "",
                "description_snippet": "",
                "source_ats": "YCombinator",
                "slug": slug,
            })

    return jobs


def scrape_personio(slug: str) -> list[dict]:
    """Personio — public XML feed, no auth required.
    Slug is the company subdomain (e.g. 'acme').
    Tries both .de and .com domains."""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    company_name = slug.replace("-", " ").title()

    xml_text = None
    for domain in ["jobs.personio.de", "jobs.personio.com"]:
        url = f"https://{slug}.{domain}/xml?language=en"
        r = _get(url, headers=headers)
        if r and r.text.strip().startswith("<?xml"):
            xml_text = r.text
            break

    if not xml_text:
        return []

    jobs = []
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        log.debug(f"Personio: XML parse failed for {slug}: {e}")
        return []

    for pos in root.iter("position"):
        title = (pos.findtext("name") or "").strip()
        if not title:
            continue

        job_id = pos.findtext("id") or ""
        office = (pos.findtext("office") or "").strip()
        department = (pos.findtext("department") or "").strip()
        emp_type = (pos.findtext("employmentType") or "").strip()
        company = (pos.findtext("subcompany") or company_name).strip()
        schedule = (pos.findtext("schedule") or "").strip()

        # Description blocks
        desc_parts = []
        for desc_elem in pos.iter("jobDescription"):
            name = (desc_elem.findtext("name") or "").strip()
            value = (desc_elem.findtext("value") or "").strip()
            if value:
                desc_parts.append(_snippet(value, max_chars=2000))
        desc = " ".join(desc_parts)
        salary = _extract_salary(desc) if desc else ""

        # Build job URL
        job_url = f"https://{slug}.jobs.personio.de/job/{job_id}" if job_id else ""

        # Try to extract country from office field (e.g. "Munich, Germany")
        country = ""
        if office:
            office_parts = [p.strip() for p in office.split(",")]
            if len(office_parts) >= 2:
                country = office_parts[-1]

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company,
            "location": office,
            "country": country,
            "department": department,
            "workplace_type": schedule,
            "employment_type": emp_type,
            "salary": salary,
            "description_snippet": desc,
            "source_ats": "Personio",
            "slug": slug,
        })

    return jobs


# ── Dispatcher ──────────────────────────────────────────

def scrape_joincom(slug: str) -> list[dict]:
    """JOIN.com — public REST API, no auth required.
    Slug is the company slug (e.g. 'marswalk').
    Two-step: resolve slug → company_id via __NEXT_DATA__, then paginate the jobs API.
    pageSize max is 5 (server rejects >= 6 with HTTP 422)."""
    import json as _json
    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "text/html"}

    # Step 1: Resolve slug → numeric company_id
    page_r = _get(f"https://join.com/companies/{slug}", headers=headers)
    if not page_r:
        return []

    nd_match = re.search(
        r'<script\s+id="__NEXT_DATA__"[^>]*>([^<]+)</script>',
        page_r.text, re.I,
    )
    if not nd_match:
        log.debug(f"JOIN: no __NEXT_DATA__ for {slug}")
        return []

    try:
        nd = _json.loads(nd_match.group(1))
        company_id = nd["props"]["pageProps"]["initialState"]["company"]["id"]
        company_name = nd["props"]["pageProps"]["initialState"]["company"].get("name", slug.replace("-", " ").title())
    except (KeyError, _json.JSONDecodeError) as e:
        log.debug(f"JOIN: failed to extract company_id for {slug}: {e}")
        return []

    # Step 2: Paginate the public jobs API (pageSize max 5)
    api_base = f"https://join.com/api/public/companies/{company_id}/jobs"
    all_jobs = []
    page = 1

    while True:
        r = _get(api_base, params={"locale": "en-us", "page": page, "pageSize": 5},
                 headers={"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"})
        if not r:
            break
        try:
            data = r.json()
        except Exception:
            break

        items = data.get("items", [])
        pagination = data.get("pagination", {})
        if not items:
            break

        for item in items:
            city_obj = item.get("city") or {}
            city = city_obj.get("cityName", "") if isinstance(city_obj, dict) else ""
            region = city_obj.get("regionName", "") if isinstance(city_obj, dict) else ""
            country = city_obj.get("countryName", "") if isinstance(city_obj, dict) else ""
            location = ", ".join(filter(None, [city, region, country]))

            # Salary in cents → dollars/euros
            sal_from_obj = item.get("salaryAmountFrom") or {}
            sal_to_obj = item.get("salaryAmountTo") or {}
            salary_str = ""
            if isinstance(sal_from_obj, dict) and isinstance(sal_to_obj, dict):
                amt_from = sal_from_obj.get("amount", 0)
                amt_to = sal_to_obj.get("amount", 0)
                currency = sal_from_obj.get("currency", "EUR")
                if amt_from and amt_to:
                    salary_str = f"{currency} {amt_from / 100:,.0f}-{amt_to / 100:,.0f}"

            cat = item.get("category") or {}
            dept = cat.get("name", "") if isinstance(cat, dict) else ""
            emp_obj = item.get("employmentType") or {}
            emp_type = emp_obj.get("name", "") if isinstance(emp_obj, dict) else ""
            wt = item.get("workplaceType", "")  # ONSITE, REMOTE, HYBRID

            id_param = item.get("idParam", "")
            job_url = f"https://join.com/companies/{slug}/jobs/{id_param}" if id_param else ""

            all_jobs.append({
                "title": (item.get("title") or "").strip(),
                "url": job_url,
                "company": company_name,
                "location": location,
                "country": country,
                "department": dept,
                "workplace_type": wt,
                "employment_type": emp_type,
                "salary": salary_str,
                "description_snippet": "",  # Need per-job fetch for full description
                "source_ats": "JOIN",
                "slug": slug,
            })

        page_count = pagination.get("pageCount", 1)
        if page >= page_count:
            break
        page += 1
        time.sleep(random.uniform(0.2, 0.5))

    return all_jobs


# ── Paylocity ──────────────────────────────────────────

def scrape_paylocity(slug: str) -> list[dict]:
    """Paylocity — embedded window.pageData JSON in career page HTML.
    Slug format: 'company_id|company_name' (e.g. '9b6dbe18-.../The-Guidance-Center')."""
    import json as _json
    parts = slug.split("|", 1)
    if len(parts) != 2:
        log.debug(f"Invalid Paylocity slug format: {slug} (expected 'company_id|company_name')")
        return []

    company_id, company_name_slug = parts
    url = f"https://recruiting.paylocity.com/recruiting/jobs/All/{company_id}/{company_name_slug}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = _get(url, headers=headers)
    if not r:
        return []

    # Extract window.pageData JSON
    pd_match = re.search(r'window\.pageData\s*=\s*(\{.*?\});\s*</script>', r.text, re.DOTALL)
    if not pd_match:
        log.debug(f"Paylocity: no window.pageData found for {company_name_slug}")
        return []

    try:
        page_data = _json.loads(pd_match.group(1))
    except _json.JSONDecodeError as e:
        log.debug(f"Paylocity: JSON parse failed for {company_name_slug}: {e}")
        return []

    company_name = (page_data.get("companyName")
                    or page_data.get("ModuleTitle")
                    or company_name_slug.replace("-", " ").title())
    jobs_list = page_data.get("Jobs", page_data.get("jobs", []))
    if not isinstance(jobs_list, list):
        return []

    jobs = []
    seen_titles = set()
    for item in jobs_list:
        # Skip inactive / expired jobs
        status = str(item.get("Status", item.get("PostingStatus", ""))).lower()
        is_active = item.get("IsActive", item.get("isActive", None))
        if status in ("closed", "inactive", "expired", "draft", "archived"):
            continue
        if is_active is False or str(is_active).lower() == "false":
            continue

        title = item.get("JobTitle", item.get("Title", ""))
        job_id = item.get("JobId", item.get("Id", ""))
        location = item.get("LocationName", item.get("Location", ""))
        department = item.get("HiringDepartment", item.get("Department", ""))
        desc = _snippet(item.get("Description", item.get("JobDescription", "")))
        salary = _extract_salary(desc)

        # Deduplicate by title (same company may list same role multiple times)
        title_key = str(title).lower().strip()
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)

        job_url = f"https://recruiting.paylocity.com/recruiting/jobs/Details/{company_id}/{job_id}/{company_name_slug}"

        jobs.append({
            "title": str(title).strip(),
            "url": job_url,
            "company": company_name,
            "location": location or "",
            "country": "",
            "department": department,
            "workplace_type": "",
            "employment_type": item.get("EmploymentType", ""),
            "salary": salary,
            "description_snippet": desc,
            "source_ats": "Paylocity",
            "slug": slug,
        })

    return jobs


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
    "teamtailor": scrape_teamtailor,
    "breezyhr": scrape_breezyhr,
    "applytojob": scrape_applytojob,
    "personio": scrape_personio,
    "joincom": scrape_joincom,
    # ── Newly enabled (confirmed working) ──
    "taleo": scrape_taleo,
    "oracle_cloud_hcm": scrape_oracle_cloud_hcm,
    "paylocity": scrape_paylocity,
    "hrmdirect": scrape_hrmdirect,
    "zoho": scrape_zoho,
    # ── DISABLED (JS-rendered / auth-required / blocked) ──
    # "brassring": scrape_brassring,
    # "successfactors": scrape_successfactors,
    # "softgarden": scrape_softgarden,
    # "ycombinator": scrape_ycombinator,
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


# ── Second-pass: fetch individual job descriptions ─────
# These are called AFTER the role filter, so only a handful
# of jobs need enrichment (not thousands).

def _extract_location_from_html(html: str) -> str:
    """Universal location extractor — works for any ATS job page.
    Tries JSON-LD JobPosting schema first (most reliable), then
    common meta tags, then typical HTML patterns."""
    import json as _json

    # ── 1. JSON-LD structured data (most reliable) ─────────
    for ld_match in re.finditer(
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
        html, re.I | re.DOTALL
    ):
        try:
            ld = _json.loads(ld_match.group(1))
            # Handle @graph arrays
            if isinstance(ld, dict) and "@graph" in ld:
                ld = ld["@graph"]
            if isinstance(ld, list):
                for item in ld:
                    if isinstance(item, dict) and item.get("@type") in ("JobPosting", "jobPosting"):
                        ld = item
                        break
                else:
                    continue
            if not isinstance(ld, dict):
                continue
            if ld.get("@type") not in ("JobPosting", "jobPosting"):
                continue

            # Extract jobLocation
            job_loc = ld.get("jobLocation")
            if not job_loc:
                continue
            locs = job_loc if isinstance(job_loc, list) else [job_loc]
            parts = []
            for loc in locs:
                if isinstance(loc, str):
                    parts.append(loc)
                    continue
                addr = loc.get("address") or loc
                if isinstance(addr, str):
                    parts.append(addr)
                    continue
                city = addr.get("addressLocality", "")
                region = addr.get("addressRegion", "")
                country = addr.get("addressCountry", "")
                if isinstance(country, dict):
                    country = country.get("name", "") or country.get("@id", "")
                loc_str = ", ".join(p for p in [city, region, country] if p)
                if loc_str:
                    parts.append(loc_str)
            if parts:
                return "; ".join(parts[:3])  # cap at 3 locations
        except Exception:
            continue

    # ── 2. Open Graph / meta tags (handle both attribute orders) ──
    meta_loc_tags = [
        ("property", "og:locality"),
        ("name", "geo.placename"),
        ("name", "location"),
    ]
    for attr, val in meta_loc_tags:
        escaped_val = re.escape(val)
        for pat in [
            rf'<meta[^>]*{attr}=["\']{ escaped_val}["\'][^>]*content=["\']([^"\']+)["\']',
            rf'<meta[^>]*content=["\']([^"\']+)["\'][^>]*{attr}=["\']{ escaped_val}["\']',
        ]:
            m = re.search(pat, html, re.I)
            if m:
                loc = m.group(1).strip()
                if loc and len(loc) < 200:
                    return loc

    # ── 3. Common HTML patterns ────────────────────────────
    for pat in [
        r'class="[^"]*(?:job-location|jobLocation|location-name|posting-location)[^"]*"[^>]*>\s*([^<]+?)\s*<',
        r'data-automation=["\']job-location["\'][^>]*>\s*([^<]+?)\s*<',
        r'itemprop=["\']jobLocation["\'][^>]*>\s*([^<]+?)\s*<',
    ]:
        m = re.search(pat, html, re.I | re.DOTALL)
        if m:
            loc = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            if loc and len(loc) < 200:
                return loc

    return ""


def _extract_icims_location(html: str) -> str:
    """Extract location from iCIMS job page HTML.
    Tries multiple patterns since iCIMS templates vary."""
    # Pattern 1: iCIMS format "US-XX-CityName" or "XX-XX-City"
    m = re.search(r'\b([A-Z]{2}-[A-Z]{2}-[\w\s\-\.]+?)(?:<|"|\'|\s*\n|\s*<)', html)
    if m:
        return m.group(1).strip()

    # Pattern 2: Page title "Job Title in Location | Careers at Location"
    #   e.g. "Sr Consultant in Remote | Careers at US Nationwide Remote"
    #   Extract the "Careers at [Location]" part (more specific than the first part)
    m = re.search(r'Careers\s+at\s+([^|<"]+)', html, re.I)
    if m:
        loc = m.group(1).strip().rstrip(' .')
        if loc and len(loc) < 100:
            return loc

    # Pattern 2b: Fallback — "Job Title in [Location] |"
    m = re.search(r'<title>[^<]*?\bin\s+([^|<]+?)(?:\s*\|)', html, re.I)
    if m:
        loc = m.group(1).strip().rstrip(' .')
        if loc and len(loc) < 100:
            return loc

    # Pattern 3: og:title meta tag — "Job Title in Location | Careers at Location"
    m = re.search(r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)["\']', html, re.I)
    if m:
        og_title = m.group(1)
        # Try "Careers at [Location]" from og:title
        m2 = re.search(r'Careers\s+at\s+(.+)', og_title, re.I)
        if m2:
            loc = m2.group(1).strip()
            if loc and len(loc) < 100:
                return loc

    # Pattern 4: iCIMS-specific location CSS classes
    for pat in [
        r'class="[^"]*iCIMS_JobHeader(?:Location|Field)[^"]*"[^>]*>\s*(.*?)\s*<',
        r'class="[^"]*header-location[^"]*"[^>]*>\s*(.*?)\s*<',
        r'class="[^"]*location[^"]*"[^>]*>\s*([^<]+?)\s*<',
    ]:
        m = re.search(pat, html, re.I | re.DOTALL)
        if m:
            loc = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            if loc and len(loc) < 200:
                return loc

    # Pattern 5: Fall back to the universal extractor (JSON-LD, meta tags, etc.)
    return _extract_location_from_html(html)


def _fetch_icims_content(url: str) -> str:
    """Fetch iCIMS job page HTML, handling the iframe wrapper problem.
    Many iCIMS career sites wrap the actual job content in an iframe.
    The real content is at the same URL with ?in_iframe=1.
    Returns the HTML with actual job content, or empty string."""
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    # Strategy 1: Try ?in_iframe=1 first — this gets the ACTUAL content
    #   (bypasses the wrapper page that loads content via iframe)
    iframe_url = url + ("&" if "?" in url else "?") + "in_iframe=1"
    r = _get(iframe_url, headers=headers)
    if r and r.text:
        # Verify we got real iCIMS content (not a redirect/error page)
        text = r.text
        has_icims_content = any(marker in text for marker in [
            "iCIMS_", "icims", "job-description", "JobContent",
            "addressLocality", "JobPosting", "jobLocation",
        ])
        has_real_title = "<title>" in text and "in_iframe" not in text.lower()
        if has_icims_content or has_real_title:
            return text

    # Strategy 2: Try the original URL (some iCIMS sites don't use iframe)
    r = _get(url, headers=headers)
    if r and r.text:
        text = r.text
        # Check if it's a wrapper page (has iframe src pointing to itself)
        has_iframe = re.search(r'<iframe[^>]*src=["\'][^"\']*in_iframe', text, re.I)
        if has_iframe:
            # It's a wrapper — try extracting iframe src and fetch that
            iframe_match = re.search(r'<iframe[^>]*src=["\']([^"\']+)["\']', text, re.I)
            if iframe_match:
                iframe_src = iframe_match.group(1)
                if not iframe_src.startswith("http"):
                    from urllib.parse import urljoin
                    iframe_src = urljoin(url, iframe_src)
                r2 = _get(iframe_src, headers=headers)
                if r2 and r2.text:
                    return r2.text
        return text

    # Strategy 3: Try mobile version (cleaner, no iframe)
    mobile_url = url + ("&" if "?" in url else "?") + "mobile=true&needsRedirect=false"
    r = _get(mobile_url, headers=headers)
    if r and r.text:
        return r.text

    return ""


def _fetch_icims_description(job: dict) -> str:
    """Fetch full description and location from an individual iCIMS job page.
    Also extracts location as a side-effect (updates job dict in place).
    Handles iframe wrapper pages by trying multiple URL variants."""
    html = _fetch_icims_content(job["url"])
    if not html:
        return ""

    # ── Extract location if missing (side-effect) ──────────
    if not job.get("location"):
        loc = _extract_icims_location(html)
        if loc:
            job["location"] = loc

    # ── Extract description ────────────────────────────────
    # iCIMS embeds the FULL JD as JSON-LD in a <script> tag.
    # The meta tags (og:description) only have a ~400 char summary.
    # Try JSON-LD FIRST to get the complete description.

    import json as _json

    # 1. JSON-LD — has the FULL JD (thousands of chars)
    #    Match any script tag containing JSON-LD (some iCIMS sites
    #    omit the type attribute but still embed valid JSON-LD)
    for ld_match in re.finditer(
        r'<script[^>]*>(.*?)</script>',
        html, re.I | re.DOTALL
    ):
        content = ld_match.group(1).strip()
        if not content.startswith("{"):
            continue
        try:
            ld = _json.loads(content)
            if isinstance(ld, dict) and ld.get("@type") == "JobPosting" and ld.get("description"):
                return _snippet(ld["description"])
        except Exception:
            continue

    # 2. og:description — truncated (~400 chars) but better than nothing
    for meta_pat in [
        r'<meta[^>]*property=["\']og:description["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:description["\']',
    ]:
        meta_match = re.search(meta_pat, html, re.I)
        if meta_match:
            desc = meta_match.group(1).strip()
            desc = desc.replace("&nbsp;", " ").replace("&#160;", " ")
            if len(desc) > 100:
                return _snippet(desc)

    # 3. name="description" — sometimes the JD, sometimes a generic blurb
    for meta_pat in [
        r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*name=["\']description["\']',
    ]:
        meta_match = re.search(meta_pat, html, re.I)
        if meta_match:
            desc = meta_match.group(1).strip()
            desc = desc.replace("&nbsp;", " ").replace("&#160;", " ")
            if len(desc) > 100 and "review all of the job details" not in desc.lower():
                return _snippet(desc)

    # 4. iCIMS CSS containers (rare — most sites are JS-rendered)
    for pattern in [
        r'class="iCIMS_JobContent[^"]*"[^>]*>(.*?)</div>',
        r'class="iCIMS_InfoMsg_Job[^"]*"[^>]*>(.*?)</div>',
        r'<div\s+id="job-description"[^>]*>(.*?)</div>',
    ]:
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            text = _snippet(match.group(1))
            if len(text) > 100:
                return text

    # 5. Fallback: main element
    body_match = re.search(r'<main[^>]*>(.*?)</main>', html, re.DOTALL | re.IGNORECASE)
    if body_match:
        text = _snippet(body_match.group(1))
        if len(text) > 100:
            return text
    return ""


def _fetch_workday_description(job: dict) -> str:
    """Fetch full description from a Workday job detail API.
    Job URL format: https://{company}.wd{N}.myworkdayjobs.com/{site}{path}
    Detail API: POST to /wday/cxs/{company}/{site}{path}"""
    url = job.get("url", "")
    if not url:
        return ""
    # Parse the URL to build the API call
    from urllib.parse import urlparse as _urlparse
    parsed = _urlparse(url)
    hostname = parsed.hostname or ""
    if "myworkdayjobs.com" not in hostname:
        return ""
    company = hostname.split(".")[0]
    # Path is like /{site_id}/job/{path}
    path = parsed.path
    api_url = f"https://{hostname}/wday/cxs{path}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
    }
    try:
        r = _get_session().get(api_url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return ""
        data = r.json()
        posting = data.get("jobPostingInfo", {})
        desc = posting.get("jobDescription", "")
        if desc:
            return _snippet(desc)
    except Exception:
        pass
    return ""


def _fetch_smartrecruiters_description(job: dict) -> str:
    """Fetch full description from SmartRecruiters job detail API.
    Detail endpoint: GET /v1/companies/{slug}/postings/{posting_id}"""
    url = job.get("url", "")
    slug = job.get("slug", "")
    if not url or not slug:
        return ""
    # Extract posting ID from URL: /slug/posting_id
    parts = url.rstrip("/").split("/")
    if len(parts) < 2:
        return ""
    posting_id = parts[-1]
    api_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/{posting_id}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    r = _get(api_url, headers=headers)
    if not r:
        return ""
    try:
        data = r.json()
        # Description is in jobAd.sections.jobDescription.text
        job_ad = data.get("jobAd", {})
        sections = job_ad.get("sections", {})
        desc_section = sections.get("jobDescription", {})
        desc = desc_section.get("text", "")
        if desc:
            return _snippet(desc)
        # Fallback: companyDescription
        comp_desc = sections.get("companyDescription", {}).get("text", "")
        if comp_desc:
            return _snippet(comp_desc)
    except Exception:
        pass
    return ""


def _fetch_taleo_description(job: dict) -> str:
    """Fetch full description from a Taleo job detail page."""
    r = _get(job["url"], headers={"User-Agent": random.choice(USER_AGENTS)})
    if not r:
        return ""
    # Taleo pages have description in specific divs
    patterns = [
        r'class="[^"]*jobdescription[^"]*"[^>]*>(.*?)</div>',
        r'id="[^"]*jobdescription[^"]*"[^>]*>(.*?)</div>',
        r'class="[^"]*requisition[Dd]escription[^"]*"[^>]*>(.*?)</div>',
        r'<div\s+class="contentlinepanel"[^>]*>(.*?)</div>',
    ]
    for pattern in patterns:
        match = re.search(pattern, r.text, re.DOTALL | re.IGNORECASE)
        if match:
            return _snippet(match.group(1))
    return ""


def _fetch_generic_description(job: dict) -> str:
    """Generic description fetcher — loads the job URL and extracts
    text from common HTML patterns (JSON-LD, meta description, body text).
    Also extracts location as a side-effect if job has no location."""
    url = job.get("url", "")
    if not url:
        return ""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    r = _get(url, headers=headers)
    if not r:
        return ""

    html = r.text

    # ── Extract location if missing (side-effect) ──────────
    if not job.get("location"):
        loc = _extract_location_from_html(html)
        if loc:
            job["location"] = loc

    # Try JSON-LD first
    ld_match = re.search(
        r'<script[^>]*type="application/ld\+json"[^>]*>([^<]+)</script>',
        html, re.I
    )
    if ld_match:
        try:
            import json
            ld = json.loads(ld_match.group(1))
            if isinstance(ld, dict) and ld.get("description"):
                return _snippet(ld["description"])
        except Exception:
            pass

    # Try meta description (handle both attribute orders)
    for meta_pat in [
        r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*name=["\']description["\']',
        r'<meta[^>]*property=["\']og:description["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:description["\']',
    ]:
        meta_match = re.search(meta_pat, html, re.I)
        if meta_match:
            desc = meta_match.group(1).strip()
            desc = desc.replace("&nbsp;", " ").replace("&#160;", " ")
            if len(desc) > 50:
                return _snippet(desc)

    # Try common job description containers
    for pattern in [
        r'class="[^"]*(?:job-description|job_description|description|posting-content|job-details)[^"]*"[^>]*>(.*?)</(?:div|section)',
        r'<article[^>]*>(.*?)</article>',
    ]:
        match = re.search(pattern, html, re.DOTALL | re.I)
        if match:
            text = _snippet(match.group(1))
            if len(text) > 50:
                return text

    return ""


def _fetch_joincom_description(job: dict) -> str:
    """Fetch full description from JOIN.com job detail API."""
    url = job.get("url", "")
    if not url:
        return ""
    # Extract job ID from URL: /companies/{slug}/jobs/{idParam}
    # We need the numeric ID, which requires an extra lookup
    # Try the generic fetcher on the job page (has JSON-LD)
    return _fetch_generic_description(job)


def _fetch_teamtailor_location(job: dict) -> str:
    """Fetch location from a Teamtailor job page.
    The RSS feed has no location, but individual job pages have JSON-LD.
    Returns existing description if already set (we only need location)."""
    url = job.get("url", "")
    if not url:
        return job.get("description_snippet", "")
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    r = _get(url, headers=headers)
    if not r:
        return job.get("description_snippet", "")

    html = r.text

    # ── Extract location (primary purpose) ─────────────────
    if not job.get("location"):
        loc = _extract_location_from_html(html)
        if loc:
            job["location"] = loc

    # ── Also grab a better description if current one is weak ──
    existing_desc = job.get("description_snippet", "")
    if len(existing_desc) < 100:
        desc = ""
        # Try JSON-LD description
        import json as _json
        for ld_match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.I | re.DOTALL
        ):
            try:
                ld = _json.loads(ld_match.group(1))
                if isinstance(ld, dict) and ld.get("description"):
                    desc = _snippet(ld["description"])
                    break
            except Exception:
                continue
        if desc:
            return desc

    return existing_desc


# Platforms that need description enrichment
DESCRIPTION_FETCHERS = {
    "iCIMS": _fetch_icims_description,
    "Workday": _fetch_workday_description,
    "SmartRecruiters": _fetch_smartrecruiters_description,
    "Taleo": _fetch_taleo_description,
    "BreezyHR": _fetch_generic_description,
    "ApplyToJob": _fetch_generic_description,
    "HRMDirect": _fetch_generic_description,
    "Paylocity": _fetch_generic_description,
    "Oracle Cloud HCM": _fetch_generic_description,
    "JOIN": _fetch_joincom_description,
    "Teamtailor": _fetch_teamtailor_location,
}


def enrich_descriptions(jobs: list[dict], max_workers: int = 20) -> list[dict]:
    """Fetch individual job descriptions for platforms that don't
    include them in the list API. Call this AFTER the role filter
    so we only fetch details for the small subset of CSM/AM jobs.

    Modifies jobs in place and returns the same list."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    to_enrich = [j for j in jobs
                 if j.get("source_ats") in DESCRIPTION_FETCHERS
                 and (not j.get("description_snippet") or not j.get("location"))]

    if not to_enrich:
        return jobs

    log.info(f"Enriching {len(to_enrich)} jobs (missing description or location) "
             f"across {len(set(j['source_ats'] for j in to_enrich))} platforms...")

    def _fetch_one(job):
        fetcher = DESCRIPTION_FETCHERS[job["source_ats"]]
        try:
            desc = fetcher(job)
            if desc:
                job["description_snippet"] = desc
                salary = _extract_salary(desc)
                if salary and not job.get("salary"):
                    job["salary"] = salary
        except Exception as e:
            log.debug(f"Failed to enrich {job['url']}: {e}")
        time.sleep(random.uniform(0.2, 0.5))
        return job

    enriched = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_one, j): j for j in to_enrich}
        for future in as_completed(futures):
            try:
                job = future.result()
                if job.get("description_snippet"):
                    enriched += 1
            except Exception:
                pass

    log.info(f"Enriched {enriched}/{len(to_enrich)} jobs with descriptions")

    # ── Fallback: fetch job URL directly for ANY job still missing a JD ──
    # Some ATS APIs don't return descriptions, but the job page itself has one.
    # This catches Workday, iCIMS, SuccessFactors, etc. where the API fetch failed.
    still_missing = [j for j in jobs if not j.get("description_snippet")
                     and j.get("url")]
    if still_missing:
        log.info(f"Fallback: fetching {len(still_missing)} job URLs directly for missing JDs...")
        fallback_ok = 0

        def _fetch_fallback(job):
            try:
                desc = _fetch_generic_description(job)
                if desc:
                    job["description_snippet"] = desc
                    salary = _extract_salary(desc)
                    if salary and not job.get("salary"):
                        job["salary"] = salary
            except Exception as e:
                log.debug(f"Fallback fetch failed {job['url']}: {e}")
            time.sleep(random.uniform(0.3, 0.8))
            return job

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_fetch_fallback, j): j for j in still_missing}
            for future in as_completed(futures):
                try:
                    job = future.result()
                    if job.get("description_snippet"):
                        fallback_ok += 1
                except Exception:
                    pass

        log.info(f"Fallback enriched {fallback_ok}/{len(still_missing)} jobs from job URLs")

    # NOTE: Location-only pass removed — it was redundant.
    # _fetch_generic_description (used by both primary and fallback enrichment)
    # already extracts location as a side-effect via _extract_location_from_html.
    # The separate location pass re-fetched the same pages with the same method,
    # achieving only ~0.2% success rate (1/616). Jobs still missing location
    # simply don't have parseable location data on their pages.
    no_location = sum(1 for j in jobs if not j.get("location"))
    if no_location:
        log.info(f"Note: {no_location} jobs still have no location (pages lack structured location data)")

    return jobs


# ── Application Question Enrichment ─────────────────────
# Greenhouse and Ashby expose application questions via their APIs.
# Questions about work authorization / visa sponsorship are strong
# signals that a job is NOT globally open. We extract these and
# append them to description_snippet so the AI location classifier
# can see them.

_WORK_AUTH_RE = re.compile(
    r"(authorized?\s*to\s*work|work\s*authoriz|visa\s*sponsor|"
    r"immigration\s*sponsor|right\s*to\s*work|work\s*permit|"
    r"employment\s*eligib|legally\s*authorized|"
    r"require.*\bsponsorship\b|"
    r"do\s*you\s*now\s*or\s*in\s*the\s*future\s*require)",
    re.I,
)


def _fetch_greenhouse_questions(job: dict) -> str:
    """Fetch application questions from Greenhouse job API.
    Returns a string of work-authorization-related questions, or empty."""
    url = job.get("url", "")
    # Extract board slug and job ID from URL
    # https://job-boards.greenhouse.io/SLUG/jobs/JOBID
    m = re.search(r"greenhouse\.io/([^/]+)/jobs/(\d+)", url)
    if not m:
        return ""
    slug, job_id = m.group(1), m.group(2)
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}?questions=true"
    r = _get(api_url)
    if not r:
        return ""
    try:
        data = r.json()
    except Exception:
        return ""

    questions = data.get("questions") or []
    auth_questions = []
    for q in questions:
        label = q.get("label", "")
        if _WORK_AUTH_RE.search(label):
            auth_questions.append(f"Application Question: {label}")

    # Also check metadata for location hints (e.g. "United States (Remote)")
    metadata = data.get("metadata") or []
    for md in metadata:
        if isinstance(md, dict):
            name = md.get("name", "").lower()
            val = str(md.get("value", ""))
            if name in ("location", "location_country") and val:
                auth_questions.append(f"Metadata Location: {val}")

    return "\n".join(auth_questions)


def _fetch_ashby_questions(job: dict) -> str:
    """Fetch application form from Ashby posting API.
    Returns work-authorization-related form fields, or empty."""
    url = job.get("url", "")
    # https://jobs.ashbyhq.com/SLUG/JOBID
    m = re.search(r"ashbyhq\.com/([^/]+)/([a-f0-9-]+)", url)
    if not m:
        return ""
    slug, job_id = m.group(1), m.group(2)

    # Ashby's posting-api/posting endpoint returns form fields
    api_url = f"https://api.ashbyhq.com/posting-api/posting/{slug}/{job_id}"
    r = _get(api_url)
    if not r:
        return ""
    try:
        data = r.json()
    except Exception:
        return ""

    auth_questions = []
    # Check applicationFormDefinition for work auth questions
    form_def = data.get("applicationFormDefinition") or data.get("formDefinition") or {}
    sections = form_def.get("sections") or []
    for section in sections:
        fields = section.get("fields") or section.get("fieldEntries") or []
        for field in fields:
            # field might be nested: {field: {title: ...}} or {title: ...}
            f = field.get("field", field) if isinstance(field, dict) else field
            if not isinstance(f, dict):
                continue
            title = f.get("title", "") or f.get("label", "") or f.get("name", "")
            if _WORK_AUTH_RE.search(title):
                auth_questions.append(f"Application Question: {title}")

    # Also check surveyQuestions
    survey = data.get("surveyQuestions") or []
    for sq in survey:
        label = sq.get("label", "") or sq.get("title", "") or sq.get("question", "")
        if _WORK_AUTH_RE.search(label):
            auth_questions.append(f"Application Question: {label}")

    return "\n".join(auth_questions)


def enrich_application_questions(jobs: list[dict], max_workers: int = 15) -> list[dict]:
    """Fetch application questions for Greenhouse/Ashby jobs with bare 'Remote' location.

    Work authorization questions (visa sponsorship, authorized to work, etc.)
    are appended to description_snippet so the AI location classifier can use
    them as signals. Only fetches for jobs likely to be sent to AI (bare Remote).

    Call this AFTER enrich_descriptions and BEFORE filter_locations."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    bare_remote_re = re.compile(
        r"^\s*(remote|fully\s*remote|remote\s*worker|remote\s*job)?\s*$", re.I
    )

    to_enrich = [
        j for j in jobs
        if j.get("source_ats") in ("Greenhouse", "Ashby")
        and bare_remote_re.match(j.get("location", ""))
    ]

    if not to_enrich:
        return jobs

    log.info(f"Fetching application questions for {len(to_enrich)} "
             f"Greenhouse/Ashby jobs with bare 'Remote' location...")

    fetchers = {
        "Greenhouse": _fetch_greenhouse_questions,
        "Ashby": _fetch_ashby_questions,
    }

    def _fetch_one(job):
        fetcher = fetchers[job["source_ats"]]
        try:
            questions = fetcher(job)
            if questions:
                existing = job.get("description_snippet", "") or ""
                job["description_snippet"] = existing + "\n\n" + questions
        except Exception as e:
            log.debug(f"Failed to fetch questions for {job['url']}: {e}")
        time.sleep(random.uniform(0.2, 0.5))
        return job

    enriched = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_one, j): j for j in to_enrich}
        for future in as_completed(futures):
            try:
                job = future.result()
                if "Application Question:" in (job.get("description_snippet") or ""):
                    enriched += 1
            except Exception:
                pass

    log.info(f"Found work-auth questions for {enriched}/{len(to_enrich)} jobs")
    return jobs
