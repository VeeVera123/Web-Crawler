"""
ATS scrapers — one function per platform.
Each returns a list of job dicts with standardised keys:
  { title, url, company, location, department, workplace_type,
    employment_type, salary, description_snippet, source_ats, slug }
"""

import re
import json
import logging
import random
import threading
import time
import xml.etree.ElementTree as ET
from html import unescape
from urllib.parse import unquote
import requests
from bs4 import BeautifulSoup
from config import REQUEST_TIMEOUT, MAX_RETRIES
import geo

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
        # Set default retry adapter with connection pooling.
        # 20 (was 10) — matches the higher per-platform worker counts below;
        # a pool smaller than a platform's max_workers forces threads to
        # queue for a connection even though the remote side has capacity.
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
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
                # Respect the server's own Retry-After header when it gives
                # one — this is the single most ban-risk-reducing change
                # available: it means we back off exactly as long as the
                # platform asked, instead of guessing with blind exponential
                # backoff. Cap at 30s so one stubborn platform can't stall
                # an entire worker thread for minutes.
                retry_after = r.headers.get("Retry-After")
                if retry_after and retry_after.strip().isdigit():
                    wait = min(int(retry_after), 30)
                else:
                    wait = min(2 ** attempt + random.uniform(0, 1), 30)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.debug(f"Failed {url}: {e}")
                return None
            # Jitter on ordinary failures too — prevents a "thundering herd"
            # of many worker threads retrying a flaky endpoint in lockstep.
            time.sleep(min(2 ** attempt + random.uniform(0, 0.5), 15))
    return None


# ATS template placeholders that leak into raw description payloads when a
# templating variable fails to resolve — e.g. "%LABEL_POSITION_TYPE_REMOTE_WITHIN%"
# (confirmed live in an ADP/Workday-style feed, see geo.py history). These are
# never real content, always noise, and materially hurt AI classification when
# left in (garbled jargon around the real sentence). Matched before the
# generic-junk pass below so a legitimate reading of the surrounding text
# survives.
_TEMPLATE_TOKEN_RE = re.compile(r"%[A-Z][A-Z0-9_]{2,}%")

# Runs of 3+ non-alphanumeric/non-space symbols in a row are near-always
# rendering/encoding garbage (mismatched CMS tokens, stray markup fragments)
# rather than real punctuation — real prose never needs "@)%" or "##%%--".
# Threshold is 3 so ordinary punctuation ("...", "--", "!!") and single
# percent signs ("50% remote") are left untouched.
_SYMBOL_GARBAGE_RE = re.compile(r"[^\w\s]{3,}")


def _snippet(html_or_text: str, max_chars: int = 30_000) -> str:
    """Strip HTML, decode entities, drop ATS template/encoding junk, and cap length.

    max_chars defaults to 30,000 — large enough that virtually no genuine job
    description (even a long, multi-section one) is ever actually truncated;
    it exists purely as a safety ceiling against pathological outliers (e.g.
    an ATS dumping repeated legal boilerplate), not as a normal operating limit.
    Full, untruncated text matters here because this snippet is what gets sent
    to the AI classification stage — a JD cut off mid-sentence can hide the
    exact restriction/eligibility language the AI is being asked to find.
    """
    if not html_or_text:
        return ""
    text = re.sub(r"<[^>]+>", " ", html_or_text)
    text = unescape(text)
    text = _TEMPLATE_TOKEN_RE.sub(" ", text)
    text = _SYMBOL_GARBAGE_RE.sub(" ", text)
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

    # Legacy short-tenant slugs ('eeho' / 'eeho|CX_1') require brute-force
    # domain discovery below. Once resolved this run, we cache the resolved
    # slug back to slug_registry (see call near the end of discovery) so
    # future runs skip the discovery cost entirely.
    needs_resolve = not (".fa." in host_prefix or "." in host_prefix)

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

    # Cache the fully-resolved slug so future runs skip discovery entirely.
    # base_api looks like 'https://eeho.fa.us2.oraclecloud.com/hcmRestApi/...'
    # so the resolved host prefix is everything between 'https://' and
    # '.oraclecloud.com'.
    if needs_resolve and base_api:
        try:
            resolved_prefix = base_api.split("://", 1)[1].split(".oraclecloud.com")[0]
            new_slug = f"{resolved_prefix}|{site_number}"
            from supabase_handler import resolve_oracle_slug
            resolve_oracle_slug(slug, new_slug)
        except Exception as e:
            log.debug(f"Oracle Cloud HCM: slug caching skipped for {slug!r}: {e}")

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


# ── ApplyToJob (REMOVED 2026-08 — dead code, kept for reference only) ──
#
# No longer wired into SCRAPERS/DESCRIPTION_FETCHERS/QUESTION_FETCHERS
# below. Removed because a live posting requiring US work authorization
# ("Client Engagement Representative — Remote") got past the classifier
# despite ApplyToJob being registered for full-description enrichment via
# _fetch_generic_description — the generic JD fetch wasn't reliably
# catching real disqualifying language on this platform's pages, and
# ApplyToJob (JazzHR) is a small long-tail ATS, not worth debugging
# further. Its GitHub Actions matrix slot was reassigned; see
# job_board_scrapers.py's scrape_jobicy for the replacement approach
# (Jobicy widened to cover full-JD remote CSM/AM discovery instead).

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
    """Softgarden — HTML scraper of the public career microsite.
    Slug is the company's softgarden subdomain (e.g. 'acme' for
    acme.softgarden.io). There is NO public unauthenticated REST API —
    Softgarden's real API (dev.softgarden.de) requires an OAuth2 client
    credential grant issued per-customer, so we parse the static HTML
    vacancy listing instead (it is server-rendered, no JS required).

    List page:   https://{slug}.softgarden.io/en/vacancies  (falls back to /vacancies)
    Detail page: https://{slug}.softgarden.io/job/{jobId}/{title-slug}?jobDbPVId={dbId}&l=en
    """
    company_name = slug.replace("-", " ").title()
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = None
    for path in ("/en/vacancies", "/vacancies"):
        r = _get(f"https://{slug}.softgarden.io{path}", headers=headers)
        if r:
            break
    if not r:
        return []

    jobs = []
    seen = set()

    # ── Primary: JSON-LD JobPosting blocks, if the template includes them ──
    for ld_match in re.finditer(
        r'<script[^>]*type="application/ld\+json"[^>]*>([^<]+)</script>',
        r.text, re.I
    ):
        try:
            ld_data = json.loads(ld_match.group(1))
            items = ld_data if isinstance(ld_data, list) else [ld_data]
            for item in items:
                if not isinstance(item, dict) or item.get("@type") != "JobPosting":
                    continue
                job_url = item.get("url", "")
                if not job_url or job_url in seen:
                    continue
                seen.add(job_url)

                loc_obj = item.get("jobLocation", {})
                if isinstance(loc_obj, list) and loc_obj:
                    loc_obj = loc_obj[0]
                addr = loc_obj.get("address", {}) if isinstance(loc_obj, dict) else {}
                loc = addr.get("addressLocality", "") if isinstance(addr, dict) else ""
                country = addr.get("addressCountry", "") if isinstance(addr, dict) else ""
                if isinstance(country, dict):
                    country = country.get("name", "")

                desc = _snippet(item.get("description", ""))
                salary = _extract_salary(desc)
                org = item.get("hiringOrganization", {})

                jobs.append({
                    "title": (item.get("title") or "").strip(),
                    "url": job_url,
                    "company": (org.get("name", "") if isinstance(org, dict) else "").strip() or company_name,
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

    if jobs:
        return jobs

    # ── Fallback: plain HTML vacancy links ──
    # Detail links look like /job/{jobId}/{title-slug}?jobDbPVId={dbId}&l=en
    for match in re.finditer(
        r'href=["\'](/job/(\d+)/([^"\'?]+)[^"\']*)["\']',
        r.text, re.I
    ):
        path, job_id, title_slug = match.group(1), match.group(2), match.group(3)
        job_url = f"https://{slug}.softgarden.io{path}"
        if job_url in seen:
            continue
        seen.add(job_url)
        title = unescape(title_slug.replace("-", " ")).strip().title()

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
            "source_ats": "Softgarden",
            "slug": slug,
        })

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

        # Description blocks — clean each block (HTML strip/entity-decode/
        # junk-strip) WITHOUT per-block truncation, then cap the joined
        # whole once via _snippet's default. A JD is usually split across
        # several blocks (intro/requirements/benefits); truncating each
        # block individually (previously 2000 chars each) could still chop
        # a real block mid-sentence even though the full joined text was
        # well under the overall safety ceiling.
        desc_parts = []
        for desc_elem in pos.iter("jobDescription"):
            name = (desc_elem.findtext("name") or "").strip()
            value = (desc_elem.findtext("value") or "").strip()
            if value:
                desc_parts.append(_snippet(value, max_chars=30_000))
        desc = _snippet(" ".join(desc_parts))
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


# ── Eploy ───────────────────────────────────────────────

def scrape_eploy(slug: str) -> list[dict]:
    """Eploy — HTML scrape of the public vacancy search page.
    Slug is the customer's Eploy portal subdomain (e.g. 'acme' for
    acme.eploy.net). No public JSON API; the vacancy list and detail
    pages are plain server-rendered HTML.

    List page:   https://{slug}.eploy.net/candidate/jobboard/vacancysearchresults.aspx
    Detail page: https://{slug}.eploy.net/candidate/jobboard/vacancy/{id}/{title-slug}
    """
    company_name = slug.replace("-", " ").title()
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    base = f"https://{slug}.eploy.net"

    r = None
    for path in (
        "/candidate/jobboard/vacancysearchresults.aspx",
        "/candidate/JobBoard/VacancySearchResults.aspx",
        "/vacancies",
    ):
        r = _get(base + path, headers=headers)
        if r:
            break
    if not r:
        return []

    jobs = []
    seen = set()

    for match in re.finditer(
        r'href=["\']([^"\']*/vacancy/(\d+)/[^"\']*)["\'][^>]*>\s*([^<]+)</a>',
        r.text, re.I
    ):
        path, vac_id, title = match.group(1), match.group(2), unescape(match.group(3)).strip()
        job_url = path if path.startswith("http") else base + path
        if job_url in seen or not title:
            continue
        seen.add(job_url)

        # Location is frequently rendered as a sibling <span>/<div> right
        # after the link inside the same list item — best-effort grab.
        window = r.text[match.end():match.end() + 400]
        loc_match = re.search(r'class="[^"]*(?:location|vacancy-location)[^"]*"[^>]*>([^<]+)', window, re.I)
        location = unescape(loc_match.group(1)).strip() if loc_match else ""

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": location,
            "country": "",
            "department": "",
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "Eploy",
            "slug": slug,
        })

    return jobs


# ── Folks HR (Folks Applicant Tracking System) ──────────

def scrape_folkshr(slug: str) -> list[dict]:
    """Folks HR — HTML scrape of the public careers microsite.
    Slug is the company identifier on the shared board domain.
    No public API; listing and detail pages are server-rendered HTML.

    Two domains are live: jobs.folksats.app (post-2025-rebrand) and
    jobs.glowinthecloud.com (the older "Glow Talents" domain Folks HR
    acquired — most existing customers are still actually hosted there).
    A given company lives on one or the other, not both, so we try
    folksats.app first and fall back to glowinthecloud.com.

    List page:   https://{domain}/{company}
    Detail page: https://{domain}/{company}/{job-id}
    """
    company_name = slug.replace("-", " ").title()
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    domain = None
    r = None
    for candidate in ("jobs.folksats.app", "jobs.glowinthecloud.com"):
        r = _get(f"https://{candidate}/{slug}", headers=headers)
        if r:
            domain = candidate
            break
    if not r or not domain:
        return []

    jobs = []
    seen = set()

    for match in re.finditer(
        r'href=["\'](/' + re.escape(slug) + r'/([a-zA-Z0-9\-]+))["\'][^>]*>\s*([^<]+)</a>',
        r.text, re.I
    ):
        path, job_id, title = match.group(1), match.group(2), unescape(match.group(3)).strip()
        if job_id.lower() in ("apply", "about", "jobs", "") or len(title) < 3:
            continue
        job_url = f"https://{domain}{path}"
        if job_url in seen:
            continue
        seen.add(job_url)

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
            "source_ats": "FolksHR",
            "slug": slug,
        })

    return jobs


# ── JobAdder ────────────────────────────────────────────

def scrape_jobadder(slug: str) -> list[dict]:
    """JobAdder — HTML scrape of the hosted candidate job board.
    Slug encodes the JobAdder client-app id and board name as
    '{client_id}|{board_slug}' (both required to build the URL —
    JobAdder boards are namespaced per-client, not by company name alone).

    List page:   https://clientapps.jobadder.com/{client_id}/{board_slug}
    Detail page: https://clientapps.jobadder.com/{client_id}/{board_slug}/job/{job_id}
    """
    if "|" in slug:
        client_id, board_slug = slug.split("|", 1)
    else:
        client_id, board_slug = slug, ""

    company_name = board_slug.replace("-", " ").title() or client_id
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    base = f"https://clientapps.jobadder.com/{client_id}/{board_slug}".rstrip("/")

    r = _get(base, headers=headers)
    if not r:
        return []

    jobs = []
    seen = set()

    for match in re.finditer(
        r'href=["\']([^"\']*/job/(\d+)[^"\']*)["\'][^>]*>\s*(?:<[^>]+>\s*)*([^<]+)</a>',
        r.text, re.I
    ):
        path, job_id, title = match.group(1), match.group(2), unescape(match.group(3)).strip()
        job_url = path if path.startswith("http") else f"https://clientapps.jobadder.com{path}"
        if job_url in seen or not title:
            continue
        seen.add(job_url)

        window = r.text[match.end():match.end() + 400]
        loc_match = re.search(r'class="[^"]*location[^"]*"[^>]*>([^<]+)', window, re.I)
        location = unescape(loc_match.group(1)).strip() if loc_match else ""

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": location,
            "country": "",
            "department": "",
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "JobAdder",
            "slug": slug,
        })

    return jobs


# ── Jobvite ─────────────────────────────────────────────

def scrape_jobvite(slug: str) -> list[dict]:
    """Jobvite — HTML scrape of the hosted careers site.
    Slug is the company identifier on jobs.jobvite.com
    (e.g. 'acme' for jobs.jobvite.com/acme/jobs).

    List page:   https://jobs.jobvite.com/{company}/jobs
    Detail page: https://jobs.jobvite.com/{company}/job/{job_id}
    """
    company_name = slug.replace("-", " ").title()
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    base = f"https://jobs.jobvite.com/{slug}/jobs"

    r = _get(base, headers=headers)
    if not r:
        return []

    jobs = []
    seen = set()

    for match in re.finditer(
        r'href=["\']([^"\']*/' + re.escape(slug) + r'/job/([a-zA-Z0-9\-]+)[^"\']*)["\']'
        r'[^>]*>\s*(?:<[^>]+>\s*)*([^<]+)</a>',
        r.text, re.I
    ):
        path, job_id, title = match.group(1), match.group(2), unescape(match.group(3)).strip()
        job_url = path if path.startswith("http") else f"https://jobs.jobvite.com{path}"
        if job_url in seen or not title:
            continue
        seen.add(job_url)

        window = r.text[match.end():match.end() + 400]
        loc_match = re.search(r'class="[^"]*(?:location|jv-job-list__location)[^"]*"[^>]*>([^<]+)', window, re.I)
        location = unescape(loc_match.group(1)).strip() if loc_match else ""
        dept_match = re.search(r'class="[^"]*(?:department|jv-job-list__department)[^"]*"[^>]*>([^<]+)', window, re.I)
        department = unescape(dept_match.group(1)).strip() if dept_match else ""

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": location,
            "country": "",
            "department": department,
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "Jobvite",
            "slug": slug,
        })

    return jobs


# ── ADP Workforce Now (recruiting/staffing) ──────────────

def scrape_adp(slug: str) -> list[dict]:
    """ADP Workforce Now — public career-center JSON API (no auth).
    Slug encodes both required identifiers as '{cid}|{ccId}':
      cid  = the customer id (query param 'cid')
      ccId = the career-center id (query param 'ccId')
    Both are visible in any public ADP careers URL, e.g.
    workforcenow.adp.com/mascsr/default/careercenter/public/events/
    staffing/v1/job-requisitions?cid={cid}&ccId={ccId}.

    Real, verified field names (list endpoint) — the earlier version of
    this function guessed several field names that don't actually exist
    (requisitionId, hiringOrganizationName, primaryLocation,
    jobFamilyName, workerTypeCode, and a description on the list item
    itself) and silently produced empty/wrong data for all of them:
      itemID              — the requisition's real ID (used for the detail
                             URL and _fetch_adp_description below)
      requisitionTitle    — plain string, not a nested object
      requisitionLocations[] — list of {address, nameCode.shortName}; a
                             requisition can have MULTIPLE real locations
                             (confirmed live: e.g. one req posted in both
                             Miami, FL and St. Petersburg, FL)
    There is no company-name or department field in the payload at all —
    left as slug-derived / empty rather than guessed again. The full job
    description (requisitionDescription) only exists on the per-item
    DETAIL endpoint, not this list endpoint — see _fetch_adp_description,
    registered in DESCRIPTION_FETCHERS, which the existing enrichment
    pass calls after role filtering (so only the small role-relevant
    subset costs an extra HTTP call, not every listed job)."""
    if "|" not in slug:
        return []
    cid, cc_id = slug.split("|", 1)

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
    }
    api_url = (
        "https://workforcenow.adp.com/mascsr/default/careercenter/public/events/"
        "staffing/v1/job-requisitions"
    )
    # ADP's slug is '{cid}|{ccId}' (two opaque GUIDs/IDs), NOT a readable
    # company name — unlike every other ATS in this file. The old
    # `slug.replace("-", " ").title()` fallback therefore leaked raw GUID
    # text into company_name (confirmed live, e.g.
    # "F417713F 4524 4Ba7 B017 731934A3B31C|19000101_000001"). The list
    # payload has no company/org name field either (see docstring), so
    # leave it blank rather than emit garbage — enrichment/UI should treat
    # blank company_name as "unknown", not display a fake name.
    company_name = ""
    jobs = []
    limit = 50
    offset = 0

    while True:
        r = _get(api_url, headers=headers, params={
            "cid": cid, "ccId": cc_id, "$top": limit, "$skip": offset,
        })
        if not r:
            break
        try:
            data = r.json()
        except Exception:
            break

        items = data.get("jobRequisitions") or data.get("items") or []
        if not items:
            break

        for item in items:
            title = item.get("requisitionTitle", "")
            if isinstance(title, dict):  # defensive — seen as plain string in practice
                title = title.get("titleText", "")
            req_id = item.get("itemID") or item.get("requisitionId") or item.get("id") or ""

            # requisitionLocations is a LIST — a requisition can genuinely
            # have more than one real location. nameCode.shortName is
            # already a human-readable "City, ST, US"-style string.
            req_locs = item.get("requisitionLocations") or []
            loc_strings = []
            for rl in req_locs:
                if not isinstance(rl, dict):
                    continue
                name = (rl.get("nameCode") or {}).get("shortName", "")
                if name and name.strip():
                    loc_strings.append(name.strip())
                else:
                    addr = rl.get("address") or {}
                    city = addr.get("cityName", "")
                    state = (addr.get("countrySubdivisionLevel1") or {}).get("codeValue", "")
                    if city or state:
                        loc_strings.append(", ".join(p for p in [city, state] if p))
            location = "; ".join(loc_strings)

            countries = geo.extract_countries(location)
            country = ", ".join(sorted(countries))

            job_url = (
                f"https://workforcenow.adp.com/mascsr/default/careercenter/public/"
                f"events/staffing/v1/job-requisitions/{req_id}?cid={cid}&ccId={cc_id}"
            )

            jobs.append({
                "title": str(title).strip(),
                "url": job_url,
                "company": company_name,
                "location": location,
                "country": country,
                "department": "",
                "workplace_type": "",
                "employment_type": "",
                "salary": "",
                "description_snippet": "",  # filled by _fetch_adp_description
                "source_ats": "ADP",
                "slug": slug,
            })

        if len(items) < limit:
            break
        offset += limit
        if offset > 1000:  # safety cap
            break

    return jobs


# ── Avature (best-effort generic HTML parser) ────────────

def scrape_avature(slug: str) -> list[dict]:
    """Avature — best-effort HTML scrape of the public career portal.
    Avature is heavily white-labeled (each customer runs their own
    subdomain + skinned template + locale prefix), so there is no single
    reliable markup pattern across customers. This scraper is deliberately
    conservative: it looks for the most common SearchJobs/JobDetail
    markup and JSON-LD, and simply returns fewer/no results for customers
    whose template deviates. Treat Avature coverage as lower-confidence
    than the other platforms in this file.

    Slug is '{subdomain}' (e.g. 'acme' for acme.avature.net).
    List page:   https://{subdomain}.avature.net/careers/SearchJobs
    Detail page: https://{subdomain}.avature.net/careers/JobDetail/{id}
    """
    company_name = slug.replace("-", " ").title()
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    base = f"https://{slug}.avature.net"

    r = None
    for path in ("/careers/SearchJobs", "/careers/SearchJobs/", "/en_US/careers/SearchJobs"):
        r = _get(base + path, headers=headers)
        if r:
            break
    if not r:
        return []

    jobs = []
    seen = set()

    # JSON-LD first, if present (some Avature templates include it)
    for ld_match in re.finditer(
        r'<script[^>]*type="application/ld\+json"[^>]*>([^<]+)</script>',
        r.text, re.I
    ):
        try:
            ld_data = json.loads(ld_match.group(1))
            items = ld_data if isinstance(ld_data, list) else [ld_data]
            for item in items:
                if not isinstance(item, dict) or item.get("@type") != "JobPosting":
                    continue
                job_url = item.get("url", "")
                if not job_url or job_url in seen:
                    continue
                seen.add(job_url)
                loc_obj = item.get("jobLocation", {})
                if isinstance(loc_obj, list) and loc_obj:
                    loc_obj = loc_obj[0]
                addr = loc_obj.get("address", {}) if isinstance(loc_obj, dict) else {}
                loc = addr.get("addressLocality", "") if isinstance(addr, dict) else ""
                desc = _snippet(item.get("description", ""))
                jobs.append({
                    "title": (item.get("title") or "").strip(),
                    "url": job_url,
                    "company": company_name,
                    "location": loc,
                    "country": "",
                    "department": "",
                    "workplace_type": "",
                    "employment_type": item.get("employmentType", ""),
                    "salary": _extract_salary(desc),
                    "description_snippet": desc,
                    "source_ats": "Avature",
                    "slug": slug,
                })
        except Exception:
            continue

    if jobs:
        return jobs

    # Fallback: JobDetail links in raw HTML
    for match in re.finditer(
        r'href=["\']([^"\']*/careers/JobDetail/[^"\']+)["\'][^>]*>\s*(?:<[^>]+>\s*)*([^<]+)</a>',
        r.text, re.I
    ):
        path, title = match.group(1), unescape(match.group(2)).strip()
        job_url = path if path.startswith("http") else base + path
        if job_url in seen or len(title) < 3:
            continue
        seen.add(job_url)

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
            "source_ats": "Avature",
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
    # "applytojob": scrape_applytojob,  # REMOVED 2026-08 — see module notes below
    "personio": scrape_personio,
    "joincom": scrape_joincom,
    # ── Newly enabled (confirmed working) ──
    "taleo": scrape_taleo,
    "oracle_cloud_hcm": scrape_oracle_cloud_hcm,
    "paylocity": scrape_paylocity,
    "hrmdirect": scrape_hrmdirect,
    "zoho": scrape_zoho,
    "softgarden": scrape_softgarden,
    # ── New (2026-08): Eploy / Folks HR / JobAdder / Jobvite / ADP / Avature ──
    "eploy": scrape_eploy,
    "folkshr": scrape_folkshr,
    "jobadder": scrape_jobadder,
    "jobvite": scrape_jobvite,
    "adp": scrape_adp,
    "avature": scrape_avature,
    # ── DISABLED (JS-rendered / auth-required / blocked / robots.txt) ──
    # "brassring": scrape_brassring,
    # "successfactors": scrape_successfactors,
    # "ukg": — robots.txt disallow on recruiting.ultipro.com; every real
    #          URL we could verify also served an "unsupported browser"
    #          fallback page instead of real content. Excluded.
    # "phenom": — confirmed client-side-JS-only rendering for both listings
    #             and full descriptions. No plain-HTTP path exists, and
    #             adding a headless browser conflicts with this project's
    #             established architecture. Excluded.
    # YCombinator (Work at a Startup) moved to job_board_scrapers.py —
    # it's a multi-company job AGGREGATOR (like RemoteOK/Jobicy), not a
    # single-company ATS, so it belongs in the job-boards pipeline, not
    # keyed by per-company slug here. See scrape_ycombinator() there.
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


def _fetch_adp_description(job: dict) -> str:
    """Fetch full description from ADP's per-requisition DETAIL endpoint.
    scrape_adp() already builds job["url"] as this exact detail URL
    (.../job-requisitions/{itemID}?cid=...&ccId=...) — confirmed live to
    return the same fields as the list endpoint PLUS one extra field,
    requisitionDescription (raw HTML: intro, duties, requirements,
    benefits, etc.), which does NOT exist on the list endpoint at all."""
    url = job.get("url", "")
    if not url:
        return ""
    r = _get(url, headers={
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
    })
    if not r:
        return ""
    try:
        data = r.json()
    except Exception:
        return ""
    desc_html = data.get("requisitionDescription", "")
    return _snippet(desc_html) if desc_html else ""


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
    # "ApplyToJob": _fetch_generic_description,  # REMOVED 2026-08 — see module notes below
    "HRMDirect": _fetch_generic_description,
    "Paylocity": _fetch_generic_description,
    "Oracle Cloud HCM": _fetch_generic_description,
    "JOIN": _fetch_joincom_description,
    "Teamtailor": _fetch_teamtailor_location,
    # ── New (2026-08) — none of these expose full descriptions on their
    # list pages, so every job needs a detail-page fetch. The generic
    # fetcher (JSON-LD → meta description → common JD containers) covers
    # all of them since they're plain server-rendered HTML.
    "Softgarden": _fetch_generic_description,
    "Eploy": _fetch_generic_description,
    "FolksHR": _fetch_generic_description,
    "JobAdder": _fetch_generic_description,
    "Jobvite": _fetch_generic_description,
    "Avature": _fetch_generic_description,
    # ADP's list API does NOT include requisitionDescription — confirmed
    # live; only the per-item DETAIL endpoint does (see
    # _fetch_adp_description). An earlier version of this file assumed
    # the list endpoint had it and silently produced empty descriptions
    # for every ADP job.
    "ADP": _fetch_adp_description,
    # Zoho and BambooHR normally get a full description straight from
    # their LIST endpoint (see scrape_zoho / scrape_bamboohr) — no detail
    # fetch is architecturally needed in the common case. But both have a
    # fallback code path that can legitimately produce an EMPTY
    # description_snippet (Zoho's generic-link fallback when structured
    # JSON/JSON-LD parsing fails; BambooHR's undocumented public
    # `/careers/list` feed, which is a different endpoint from BambooHR's
    # official documented Applicant Tracking API — that documented one is
    # confirmed summary-only, so this is a defensive safety net in case
    # the public feed field is ever short/empty for a given posting too).
    # Previously NEITHER had any fallback registered here, so a job that
    # hit either gap silently kept an empty description forever with no
    # way to recover it.
    "Zoho": _fetch_generic_description,
    "BambooHR": _fetch_generic_description,
}


# Below this length, a description is treated as "missing" for enrichment
# purposes even if it's non-empty — a real job description is essentially
# never this short. Catches list-endpoint fields that turn out to be a
# short teaser/summary rather than the full JD (the documented risk for
# BambooHR's official Applicant Tracking API, and a plausible failure mode
# for any platform if a company's posting is unusually terse at the source)
# instead of silently accepting a truncated description as "done".
MIN_REAL_DESC_CHARS = 150


def enrich_descriptions(jobs: list[dict], max_workers: int = 20) -> list[dict]:
    """Fetch individual job descriptions for platforms that don't
    include them in the list API. Call this AFTER the role filter
    so we only fetch details for the small subset of CSM/AM jobs.

    Modifies jobs in place and returns the same list."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    to_enrich = [j for j in jobs
                 if j.get("source_ats") in DESCRIPTION_FETCHERS
                 and (len(j.get("description_snippet") or "") < MIN_REAL_DESC_CHARS
                      or not j.get("location"))]

    if not to_enrich:
        return jobs

    log.info(f"Enriching {len(to_enrich)} jobs (missing description or location) "
             f"across {len(set(j['source_ats'] for j in to_enrich))} platforms...")

    def _fetch_one(job):
        fetcher = DESCRIPTION_FETCHERS[job["source_ats"]]
        try:
            desc = fetcher(job)
            # Only replace the existing description if the fetch produced
            # something at least as long — a detail-page fetch can itself
            # fail partially (rate-limited, JS-rendered shell, changed DOM)
            # and return a short/empty result. Since this function can now
            # run on jobs that already have a short-but-real description
            # (see MIN_REAL_DESC_CHARS), never let a worse result clobber a
            # better one already in hand.
            if desc and len(desc) >= len(job.get("description_snippet") or ""):
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
# Custom application-form screening questions ("Are you authorized to work
# in X?", "Do you require visa sponsorship?") are strong signals that a job
# is NOT actually globally open, even when its location field just says
# "Remote". We extract those specific questions — across all 20 ATS
# platforms — and append them to description_snippet so the AI location
# classifier can see them.
#
# Multi-tier fallback per platform (verified per-platform via live research,
# not assumed — see the docstring on each _fetch_*_questions function):
#   Level 1 — public, unauthenticated API that returns question definitions
#             directly (Greenhouse, Ashby, Workable, Recruitee).
#   Level 2 — embedded JSON in the apply page's HTML (BreezyHR's hidden
#             input#questions field; a generic __NEXT_DATA__/JSON-script
#             scan used as a bonus pass inside the Level-3 fallback).
#   Level 3 — _fetch_generic_form_questions(): universal HTML form parser
#             (BeautifulSoup) that walks <input>/<textarea>/<select>
#             elements and resolves each one's <label>. Used as the primary
#             mechanism for platforms with predictable server-rendered
#             apply forms (Lever, Teamtailor, ApplyToJob/JazzHR), and as the
#             final fallback for every other platform if its dedicated
#             fetcher finds nothing.
#
# Honest limitation: Rippling, BambooHR, iCIMS, Workday, Personio, JOIN,
# Taleo, and Paylocity render their REAL application form client-side
# (React/Angular SPA) behind session state, or inside a cross-origin iframe
# (iCIMS), or behind partner-gated auth (Workday Staffing API, iCIMS
# iForms). None of that is reachable with plain HTTP requests — it would
# require a headless browser (Playwright/Selenium) driving the actual
# "Apply" click. We still run the Level-3 DOM parser against their best
# known URL as a best-effort attempt (a few tenants may have server-side-
# rendered fallback markup), but expect these to mostly return nothing.
# That's a real platform limitation, not a bug in this code — flagged
# explicitly here so nobody "fixes" it into a false success rate later.

_WORK_AUTH_RE = re.compile(
    r"(authorized?\s*to\s*work|work\s*authoriz|visa\s*sponsor|"
    r"immigration\s*sponsor|right\s*to\s*work|work\s*permit|"
    r"employment\s*eligib|legally\s*authorized|"
    r"require.*\bsponsorship\b|"
    r"do\s*you\s*now\s*or\s*in\s*the\s*future\s*require)",
    re.I,
)


def _clean_label(text: str) -> str:
    """Normalize a question label: unescape entities, strip tags/whitespace,
    strip trailing required-markers (*, ✱)."""
    if not text:
        return ""
    text = unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"[\*✱]+\s*$", "", text).strip()
    return text


def _format_auth_questions(questions: list[dict]) -> str:
    """Given [{label, required}, ...], keep only work-authorization-relevant
    ones and format them as 'Application Question: ...' lines."""
    lines = []
    for q in questions or []:
        label = (q.get("label") or "").strip()
        if label and _WORK_AUTH_RE.search(label):
            lines.append(f"Application Question: {label}")
    return "\n".join(lines)


# ── Level 3: universal fallback (embedded JSON + generic DOM form parse) ──

_QUESTION_JSON_SCRIPT_RE = re.compile(
    r'<script[^>]*(?:id=["\']__NEXT_DATA__["\']|type=["\']application/json["\'])[^>]*>(.*?)</script>',
    re.I | re.DOTALL,
)
_QUESTION_KEY_RE = re.compile(
    r"question|screening|prescreen|knockout|custom.?field", re.I
)


def _walk_for_questions(obj, out: list[dict], depth: int = 0):
    """Recursively search a parsed JSON blob for arrays that look like
    application-form question definitions."""
    if depth > 12 or len(out) > 100:
        return
    if isinstance(obj, dict):
        for key, val in obj.items():
            if isinstance(val, list) and _QUESTION_KEY_RE.search(str(key)):
                for item in val:
                    if not isinstance(item, dict):
                        continue
                    raw_label = (
                        item.get("label") or item.get("title") or item.get("text")
                        or item.get("question") or item.get("body") or item.get("prompt") or ""
                    )
                    label = _clean_label(str(raw_label))
                    if label:
                        required = bool(item.get("required") or item.get("isRequired"))
                        out.append({"label": label, "required": required})
            else:
                _walk_for_questions(val, out, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            _walk_for_questions(item, out, depth + 1)


def _find_embedded_questions(html_text: str) -> list[dict]:
    """Level 2 sub-fallback: scan __NEXT_DATA__ / application-json <script>
    blocks for embedded question definitions."""
    for m in _QUESTION_JSON_SCRIPT_RE.finditer(html_text):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        found: list[dict] = []
        _walk_for_questions(data, found)
        if found:
            return found
    return []


def _parse_form_elements(html_text: str) -> list[dict]:
    """Absolute Level-3 fallback: blindly parse <input>/<textarea>/<select>
    elements in the page and resolve each one's label."""
    soup = BeautifulSoup(html_text, "lxml")
    form = soup.find("form") or soup

    questions = []
    for el in form.find_all(["input", "textarea", "select"]):
        el_type = el.name if el.name != "input" else (el.get("type") or "text").lower()
        if el_type in ("hidden", "submit", "button", "image", "reset", "file"):
            continue

        label = ""
        el_id = el.get("id")
        if el_id:
            label_tag = soup.find("label", attrs={"for": el_id})
            if label_tag:
                label = label_tag.get_text(" ", strip=True)
        if not label:
            parent_label = el.find_parent("label")
            if parent_label:
                label = parent_label.get_text(" ", strip=True)
        label = _clean_label(label)
        if not label:
            continue

        required = el.has_attr("required") or (el.get("aria-required") == "true")
        questions.append({"label": label, "required": required})

    return questions


def _fetch_generic_form_questions(url: str) -> list[dict]:
    """Universal Level-3 fallback used by every platform: fetch a URL and
    try embedded JSON first, then raw form-element parsing. Returns []
    (not an exception) on any failure — callers treat that as 'no signal
    found', which is expected and fine for JS-rendered platforms."""
    if not url:
        return []
    r = _get(url, headers={"User-Agent": random.choice(USER_AGENTS)})
    if not r:
        return []
    found = _find_embedded_questions(r.text)
    if found:
        return found
    return _parse_form_elements(r.text)


# ── Level 1/2: Greenhouse (public API) ──

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


# ── Level 1/2: Ashby (public API) ──

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


# ── Level 3 (server-rendered, predictable DOM): Lever ──
# Verified live: /apply pages wrap each question in
# <li class="application-question">, with custom ones additionally tagged
# class="custom-question". Label lives in .application-label .text;
# required questions carry a <span class="required">✱</span>.

def _fetch_lever_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    apply_url = url if url.rstrip("/").endswith("/apply") else url.rstrip("/") + "/apply"
    r = _get(apply_url, headers={"User-Agent": random.choice(USER_AGENTS)})

    questions = []
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        for li in soup.select("li.application-question"):
            label_el = li.select_one(".application-label .text") or li.select_one(".application-label")
            label = _clean_label(label_el.get_text(" ", strip=True)) if label_el else ""
            if not label:
                continue
            required = bool(li.select_one("span.required")) or bool(li.find(attrs={"required": True}))
            questions.append({"label": label, "required": required})

    if not questions:
        questions = _fetch_generic_form_questions(apply_url)
    return _format_auth_questions(questions)


# ── Level 1: Workable (public API) ──
# Verified live: GET https://apply.workable.com/api/v1/jobs/{shortcode}/form
# returns field groups; custom questions live under fields[] with
# label/required/type. shortcode is the alphanumeric segment in the job's
# "/j/{shortcode}/" URL path (no account/auth needed for this endpoint).

_WORKABLE_SHORTCODE_RE = re.compile(r"/j/([A-Za-z0-9]+)")


def _fetch_workable_questions(job: dict) -> str:
    url = job.get("url", "")
    m = _WORKABLE_SHORTCODE_RE.search(url)
    questions = []
    if m:
        shortcode = m.group(1)
        api_url = f"https://apply.workable.com/api/v1/jobs/{shortcode}/form"
        r = _get(api_url, headers={"User-Agent": random.choice(USER_AGENTS)})
        if r:
            try:
                data = r.json()
            except Exception:
                data = None
            if isinstance(data, list):
                for group in data:
                    for field in (group.get("fields") or []) if isinstance(group, dict) else []:
                        label = _clean_label(field.get("label", ""))
                        if label:
                            questions.append({"label": label, "required": bool(field.get("required"))})

    if not questions:
        questions = _fetch_generic_form_questions(url)
    return _format_auth_questions(questions)


# ── Level 1: Recruitee (public API) ──
# Verified live: both the listing (/api/offers/) and detail
# (/api/offers/{offer_slug}) endpoints include open_questions[] inline —
# {body, required, kind, ...}. No extra auth needed.

_RECRUITEE_OFFER_RE = re.compile(r"/o/([^/?#]+)")


def _fetch_recruitee_questions(job: dict) -> str:
    url = job.get("url", "")
    slug = job.get("slug", "")
    m = _RECRUITEE_OFFER_RE.search(url)
    questions = []
    if m and slug:
        offer_slug = m.group(1)
        api_url = f"https://{slug}.recruitee.com/api/offers/{offer_slug}"
        r = _get(api_url, headers={"Accept": "application/json", "User-Agent": random.choice(USER_AGENTS)})
        if r:
            try:
                data = r.json()
            except Exception:
                data = None
            if isinstance(data, dict):
                offer = data.get("offer", data)
                for oq in (offer.get("open_questions") or []):
                    label = _clean_label(oq.get("body", ""))
                    if label:
                        questions.append({"label": label, "required": bool(oq.get("required"))})

    if not questions:
        questions = _fetch_generic_form_questions(url)
    return _format_auth_questions(questions)


# ── Level 3 (server-rendered, predictable DOM): Teamtailor ──
# Verified live: no __NEXT_DATA__ data island exists (despite Teamtailor
# being React-based, careers pages are server-rendered). The apply form is
# plain HTML at /jobs/{id}-{slug}/applications/new; each question is a
# <div class="question">.

def _fetch_teamtailor_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    apply_url = url.rstrip("/") + "/applications/new"
    r = _get(apply_url, headers={"User-Agent": random.choice(USER_AGENTS)})

    questions = []
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        for div in soup.select("div.question"):
            label = _clean_label(div.get_text(" ", strip=True))
            if not label:
                continue
            classes = " ".join(div.get("class") or [])
            required = ("required" in classes or bool(div.find(attrs={"required": True}))
                        or div.get_text().rstrip().endswith("*"))
            questions.append({"label": label, "required": required})

    if not questions:
        questions = _fetch_generic_form_questions(apply_url)
    return _format_auth_questions(questions)


# ── Level 2: BreezyHR (embedded JSON) ──
# Verified live: the /apply page embeds the full question list as JSON in
# <input id="questions" value="[...]">  — {text, type, required, _id}.
# No separate XHR call is made for it; it's server-rendered into the page.

_BREEZY_QUESTIONS_INPUT_RE = re.compile(r'id=["\']questions["\'][^>]*value=["\']([^"\']*)["\']', re.I)
_BREEZY_QUESTIONS_INPUT_RE_ALT = re.compile(r'value=["\']([^"\']*)["\'][^>]*id=["\']questions["\']', re.I)


def _fetch_breezyhr_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    apply_url = url.rstrip("/") + "/apply"
    r = _get(apply_url, headers={"User-Agent": random.choice(USER_AGENTS)})

    questions = []
    if r:
        m = _BREEZY_QUESTIONS_INPUT_RE.search(r.text) or _BREEZY_QUESTIONS_INPUT_RE_ALT.search(r.text)
        if m:
            raw = unescape(m.group(1))
            try:
                data = json.loads(raw)
            except Exception:
                data = None
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    label = _clean_label(item.get("text", ""))
                    if label:
                        questions.append({"label": label, "required": bool(item.get("required"))})

    if not questions:
        questions = _fetch_generic_form_questions(apply_url)
    return _format_auth_questions(questions)


# ── Level 3 (server-rendered, predictable DOM): ApplyToJob (JazzHR) ──
# Verified live: classic server-rendered form (legacy "TheResumator" DOM
# survives in JazzHR white-label pages). Custom questions sit in
# div.job-form-fields, each with a
# <label id="resumator-questionnaire-q{ID}-label"> and a matching
# #resumator-questionnaire-q{ID} input/select/textarea. Required questions
# have a trailing "*" in the label text. The scraped job listing URL is
# already the apply page itself — no URL transform needed.

def _fetch_applytojob_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    r = _get(url, headers={"User-Agent": random.choice(USER_AGENTS)})

    questions = []
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        container = soup.select_one("div.job-form-fields") or soup
        for label_el in container.select('label[id^="resumator-questionnaire-"]'):
            raw_label = label_el.get_text(" ", strip=True)
            required = raw_label.rstrip().endswith("*")
            label = _clean_label(raw_label)
            if not label:
                continue
            questions.append({"label": label, "required": required})

    if not questions:
        questions = _fetch_generic_form_questions(url)
    return _format_auth_questions(questions)


# ── Level 2 (best-effort, inconsistent): Zoho Recruit ──
# Research found Zoho sometimes embeds a candidate-module field-layout JSON
# blob (with a custom_field flag) in the listing page's HTML, but this was
# NOT confirmed present on every job-detail page or every org's template —
# treat as a bonus pass, not a guaranteed source. Falls through to the
# generic DOM parser either way.

def _fetch_zoho_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    questions = []
    r = _get(url, headers={"User-Agent": random.choice(USER_AGENTS)})
    if r:
        questions = _find_embedded_questions(r.text)
        if not questions:
            questions = _parse_form_elements(r.text)
    return _format_auth_questions(questions)


# ── Level 2 (unverified schema, best-effort): Oracle Cloud HCM ──
# Research found a real "CE" (Candidate Experience) REST namespace, and a
# recruitingCEJobRequisitionDetails resource that the public career site's
# own Angular/JET app calls client-side to render the requisition — but
# could not directly verify the exact JSON key for questionnaire data in
# this session (the career sites are JS-rendered, blocking static
# verification). We attempt the same authless CE endpoint pattern already
# used successfully for job listings (see scrape_oracle_cloud_hcm) with
# expand=all, then generically recurse the response for question-shaped
# data. Falls back to the generic DOM parser if that comes up empty.

_ORACLE_JOB_URL_RE = re.compile(r"^(https://[^/]+)/hcmUI/CandidateExperience/en/sites/([^/]+)/job/([^/?#]+)")


def _fetch_oracle_cloud_hcm_questions(job: dict) -> str:
    url = job.get("url", "")
    questions = []
    m = _ORACLE_JOB_URL_RE.match(url)
    if m:
        host, _site_number, job_id = m.groups()
        api_url = f"{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails/{job_id}"
        try:
            import uuid as _uuid
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json",
                "ora-irc-cx-userid": str(_uuid.uuid4()),
                "ora-irc-language": "en",
            }
            r = _get(api_url, params={"onlyData": "true", "expand": "all"}, headers=headers)
            if r:
                data = r.json()
                _walk_for_questions(data, questions)
        except Exception:
            pass

    if not questions:
        questions = _fetch_generic_form_questions(url)
    return _format_auth_questions(questions)


# ── Not reliably obtainable: HRMDirect ──
# Verified live: listing pages (*.hrmdirect.com) are plain static HTML with
# no form on them at all — the real apply form lives on a SEPARATE
# subdomain (apply.hrmdirect.com, a ClearCompany/"ResumeDirect ApplyOnline"
# ASP.NET app), and that subdomain's robots.txt disallows crawling
# entirely ("Disallow: /"). We respect that rather than silently bypass
# it — this fetcher intentionally does not request that subdomain.

def _fetch_hrmdirect_questions(job: dict) -> str:
    log.debug("HRMDirect: apply.hrmdirect.com disallows crawling via robots.txt; skipping")
    return ""


# ── Best-effort DOM-only platforms ──────────────────────
# Verified live (see research notes above): these platforms render their
# real application form client-side (React/Angular SPA) after JS
# execution, behind session/cookie state established by the "Apply" click,
# or (iCIMS) inside a cross-origin iframe. None of that is reachable with
# plain HTTP requests. We still run the Level-3 DOM parser against the
# best-known URL in case a given tenant happens to serve server-rendered
# fallback markup, but for most jobs on these platforms this will
# correctly return nothing — that's the platform's architecture, not a
# bug here. Upgrading these to real coverage would require adding a
# headless-browser step (Playwright) to drive the actual apply flow.

def _fetch_rippling_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    apply_url = url.rstrip("/") + "/apply"
    return _format_auth_questions(_fetch_generic_form_questions(apply_url))


def _fetch_bamboohr_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions(job.get("url", "")))


def _fetch_icims_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions(job.get("url", "")))


def _fetch_workday_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions(job.get("url", "")))


def _fetch_personio_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions(job.get("url", "")))


def _fetch_joincom_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions(job.get("url", "")))


def _fetch_taleo_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions(job.get("url", "")))


def _fetch_paylocity_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions(job.get("url", "")))


# ── SmartRecruiters ──
# Research found the documented screening-questions endpoint
# (GET /postings/{uuid}/configuration) requires an X-SmartToken auth
# header issued per-company — not usable for arbitrary postings. An
# unauthenticated "oneclick" widget config endpoint exists but could not
# be confirmed to expose custom questions (the one live posting tested had
# none configured). Best-effort DOM fallback only.

def _fetch_smartrecruiters_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions(job.get("url", "")))


# ── Dispatch table: source_ats (as stored on job dicts) → fetcher ──
QUESTION_FETCHERS = {
    "Greenhouse": _fetch_greenhouse_questions,
    "Ashby": _fetch_ashby_questions,
    "Lever": _fetch_lever_questions,
    "Workable": _fetch_workable_questions,
    "Recruitee": _fetch_recruitee_questions,
    "SmartRecruiters": _fetch_smartrecruiters_questions,
    "Teamtailor": _fetch_teamtailor_questions,
    "BreezyHR": _fetch_breezyhr_questions,
    # "ApplyToJob": _fetch_applytojob_questions,  # REMOVED 2026-08 — see module notes below
    "HRMDirect": _fetch_hrmdirect_questions,
    "Zoho": _fetch_zoho_questions,
    "Oracle Cloud HCM": _fetch_oracle_cloud_hcm_questions,
    "Rippling": _fetch_rippling_questions,
    "BambooHR": _fetch_bamboohr_questions,
    "iCIMS": _fetch_icims_questions,
    "Workday": _fetch_workday_questions,
    "Personio": _fetch_personio_questions,
    "JOIN": _fetch_joincom_questions,
    "Taleo": _fetch_taleo_questions,
    "Paylocity": _fetch_paylocity_questions,
}


def enrich_application_questions(jobs: list[dict], max_workers: int = 15) -> list[dict]:
    """Fetch application questions for location-'unsure' jobs, across ALL
    20 supported ATS platforms (see QUESTION_FETCHERS above).

    Work authorization / visa sponsorship questions are strong signals that
    a job is NOT globally open, even when its location field just says
    "Remote". We extract just those and append them to description_snippet
    so the AI location classifier can use them.

    "Unsure" is determined the same way classifier.keyword_classify_location()
    determines it — i.e. this targets exactly the subset of jobs that will
    actually be sent to the AI location step, not the full job list.

    Call this AFTER enrich_descriptions and BEFORE filter_locations."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from classifier import keyword_classify_location

    to_enrich = [
        j for j in jobs
        if j.get("source_ats") in QUESTION_FETCHERS
        and keyword_classify_location(j) == "unsure"
    ]

    if not to_enrich:
        return jobs

    by_platform: dict[str, int] = {}
    for j in to_enrich:
        by_platform[j["source_ats"]] = by_platform.get(j["source_ats"], 0) + 1
    platform_summary = ", ".join(f"{k}:{v}" for k, v in sorted(by_platform.items()))
    log.info(f"Fetching application questions for {len(to_enrich)} unsure-location jobs "
             f"across {len(by_platform)} ATS platforms ({platform_summary})...")

    def _fetch_one(job):
        fetcher = QUESTION_FETCHERS[job["source_ats"]]
        try:
            questions = fetcher(job)
            if questions:
                existing = job.get("description_snippet", "") or ""
                job["description_snippet"] = existing + "\n\n" + questions
        except Exception as e:
            log.debug(f"Failed to fetch questions for {job.get('url', '')}: {e}")
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
