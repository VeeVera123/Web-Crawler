"""
ATS scrapers — one async function per platform.
Each returns a list of job dicts with standardised keys:
{ title, url, company, location, department, workplace_type,
  employment_type, salary, description_snippet, source_ats, slug }

Async I/O (httpx.AsyncClient) with a single shared connection pool.
HTML scraping uses selectolax (C-based) instead of fragile regex.
"""
import re
import json
import logging
import random
import asyncio
import xml.etree.ElementTree as ET
from urllib.parse import unquote, urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

from config import (
    REQUEST_TIMEOUT, MAX_RETRIES,
    MAX_HTTP_CONNECTIONS, MAX_KEEPALIVE_CONNECTIONS,
    ENRICH_CONCURRENCY, QUESTION_ENRICH_CONCURRENCY,
)

log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
]


# ── Shared async HTTP client (connection pooling) ────────
# One client for the whole process = one pool of reused, keep-alive
# TLS connections. No per-thread sessions, no repeated handshakes.
_client: httpx.AsyncClient | None = None
_client_lock = asyncio.Lock()


async def get_client() -> httpx.AsyncClient:
    """Return (and lazily create) the shared async HTTP client."""
    global _client
    if _client is None:
        async with _client_lock:
            if _client is None:
                _client = httpx.AsyncClient(
                    timeout=httpx.Timeout(REQUEST_TIMEOUT),
                    limits=httpx.Limits(
                        max_connections=MAX_HTTP_CONNECTIONS,
                        max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS,
                    ),
                    follow_redirects=True,
                    http2=False,  # set True + `pip install httpx[http2]` for extra speed
                )
    return _client


async def close_client() -> None:
    """Close the shared client (call once at program exit)."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def _get(url: str, **kwargs) -> httpx.Response | None:
    client = await get_client()
    headers = kwargs.pop("headers", None) or {}
    headers.setdefault("User-Agent", random.choice(USER_AGENTS))
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = await client.get(url, headers=headers, **kwargs)
            if r.status_code == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.debug(f"Failed {url}: {e}")
                return None
            await asyncio.sleep(0.5 * (attempt + 1))
    return None


async def _post(url: str, **kwargs) -> httpx.Response | None:
    client = await get_client()
    headers = kwargs.pop("headers", None) or {}
    headers.setdefault("User-Agent", random.choice(USER_AGENTS))
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = await client.post(url, headers=headers, **kwargs)
            if r.status_code == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.debug(f"Failed POST {url}: {e}")
                return None
            await asyncio.sleep(0.5 * (attempt + 1))
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
    patterns = [
        r"\$[\d,]+\.?\d*\s*[kK]?\s*[-–—to]+\s*\$[\d,]+\.?\d*\s*[kK]?(?:\s*(?:per\s+)?(?:year|annually|yr|pa|p\.a\.))?",
        r"(?:USD|EUR|GBP|CAD|AUD)\s*[\d,]+\.?\d*\s*[-–—to]+\s*[\d,]+\.?\d*",
        r"(?:salary|compensation|pay)\s*(?:range)?[\s:]+\$[\d,]+\.?\d*\s*[kK]?\s*[-–—to]+\s*\$[\d,]+\.?\d*\s*[kK]?",
        r"\$[\d,]+\.?\d*\s*[kK]?\s*(?:USD|EUR|GBP)?\s*(?:/\s*(?:year|yr|annually))?",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(0).strip()
    return ""


# ── Rippling ────────────────────────────────────────────
async def scrape_rippling(slug: str) -> list[dict]:
    """Rippling public API — paginated."""
    base = f"https://ats.rippling.com/api/v2/board/{slug}/jobs"
    all_jobs = []
    page = 0
    while True:
        r = await _get(base, params={
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

    if all_jobs:
        try:
            first_id = all_jobs[0]["url"].split("/")[-1]
            detail_r = await _get(f"https://ats.rippling.com/api/v2/board/{slug}/jobs/{first_id}")
            if detail_r:
                detail = detail_r.json()
                company_name = detail.get("companyName", "")
                for j in all_jobs:
                    j["company"] = company_name
        except Exception:
            pass
    return all_jobs


# ── Greenhouse ──────────────────────────────────────────
async def scrape_greenhouse(slug: str) -> list[dict]:
    """Greenhouse public Job Board API — no auth required."""
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    r = await _get(url, params={"content": "true"})
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
        if metadata_location and loc.strip().lower() in ("remote", ""):
            loc = metadata_location
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
    if jobs:
        board_r = await _get(f"https://boards-api.greenhouse.io/v1/boards/{slug}")
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
async def scrape_lever(slug: str) -> list[dict]:
    """Lever public postings API — no auth required."""
    url = f"https://api.lever.co/v0/postings/{slug}"
    r = await _get(url, params={"mode": "json"})
    if not r:
        r = await _get(f"https://api.eu.lever.co/v0/postings/{slug}", params={"mode": "json"})
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
    if jobs:
        company = slug.replace("-", " ").title()
        for j in jobs:
            j["company"] = company
    return jobs


# ── Ashby ───────────────────────────────────────────────
async def scrape_ashby(slug: str) -> list[dict]:
    """Ashby public job board API — no auth required."""
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    r = await _get(url, params={"includeCompensation": "true"})
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
        address = post.get("address") or {}
        postal = address.get("postalAddress") or {}
        addr_country = postal.get("addressCountry", "") or ""
        addr_region = postal.get("addressRegion", "") or ""
        addr_city = postal.get("addressLocality", "") or ""
        if loc.strip().lower() in ("remote", "hybrid", "on-site", "onsite", ""):
            addr_parts = filter(None, [addr_city, addr_region, addr_country])
            addr_loc = ", ".join(addr_parts)
            if addr_loc:
                workplace_type = loc.strip() if loc.strip() else (post.get("workplaceType") or "")
                if workplace_type.lower() == "remote" and addr_loc:
                    loc = f"Remote, {addr_loc}"
                elif addr_loc:
                    loc = addr_loc
        country = addr_country
        if not country and loc:
            parts = [p.strip() for p in loc.replace(" - ", ", ").split(",")]
            if len(parts) >= 2:
                country = parts[-1]
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
async def scrape_bamboohr(slug: str) -> list[dict]:
    """BambooHR careers list — JSON endpoint, no auth required."""
    url = f"https://{slug}.bamboohr.com/careers/list"
    headers = {"Accept": "application/json"}
    r = await _get(url, headers=headers)
    if not r:
        return []
    if "application/json" not in r.headers.get("Content-Type", ""):
        return []
    try:
        data = r.json()
    except Exception:
        return []
    jobs_list = data.get("result")
    if not jobs_list or not isinstance(jobs_list, list):
        return []
    jobs = []
    for job in jobs_list:
        loc = job.get("location") or {}
        ats_loc = job.get("atsLocation") or {}
        if isinstance(loc, dict):
            city = loc.get("city", "") or ""
            state = loc.get("state", "") or ""
            country = loc.get("country", "") or ""
        else:
            city, state, country = (str(loc) if loc else ""), "", ""
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
async def scrape_icims(slug: str) -> list[dict]:
    """iCIMS sitemap scraper — parses sitemap.xml for job URLs."""
    sitemap_url = f"https://{slug}.icims.com/sitemap.xml"
    headers = {"Accept": "application/xml"}
    r = await _get(sitemap_url, headers=headers)
    if not r:
        return []
    try:
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
            "location": "",
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
async def scrape_workday(slug: str) -> list[dict]:
    """Workday CXS JSON API. Slug format: 'company|wd#|site_id'."""
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
        "Origin": base_url,
        "Referer": f"{base_url}/{site_id}",
    }
    all_jobs = []
    offset = 0
    limit = 20
    while True:
        payload = {"appliedFacets": {}, "limit": limit, "offset": offset, "searchText": ""}
        r = await _post(api_url, json=payload, headers=headers)
        if not r:
            break
        try:
            data = r.json()
        except Exception:
            break
        postings = data.get("jobPostings", [])
        total = data.get("total", 0)
        if not postings:
            break
        for post in postings:
            job_path = post.get("externalPath", "")
            posted_on = post.get("postedOn", "") or ""
            if "30+" in posted_on:
                continue
            location = (post.get("locationsText") or "").strip()
            if not location:
                bf = post.get("bulletFields") or []
                if len(bf) >= 2:
                    cities = bf[0] if bf[0] else ""
                    states = bf[1] if bf[1] else ""
                    location = f"{cities}, {states}" if cities and states else (cities or states)
            location = location[:200]
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
                "description_snippet": "",
                "source_ats": "Workday",
                "slug": slug,
                "posted_on": posted_on,
            })
        offset += limit
        if offset >= total:
            break
        await asyncio.sleep(random.uniform(0.3, 1.0))
    return all_jobs


# ── Workable ──────────────────────────────────────────
async def scrape_workable(slug: str) -> list[dict]:
    """Workable public widget API — no auth, no pagination."""
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
    r = await _get(url, params={"details": "true"})
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
async def scrape_recruitee(slug: str) -> list[dict]:
    """Recruitee Careers Site API — no auth, returns all offers at once."""
    url = f"https://{slug}.recruitee.com/api/offers/"
    headers = {"Accept": "application/json"}
    r = await _get(url, headers=headers)
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
        remote = offer.get("remote", False)
        workplace = "Remote" if remote else ""
        translations = offer.get("translations") or {}
        en_trans = translations.get("en", {})
        desc_html = en_trans.get("description", "") or offer.get("description", "")
        desc = _snippet(desc_html)
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
async def scrape_smartrecruiters(slug: str) -> list[dict]:
    """SmartRecruiters Posting API — paginated."""
    base_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    all_jobs = []
    offset = 0
    limit = 100
    while True:
        r = await _get(base_url, params={"limit": limit, "offset": offset})
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
            company_obj = post.get("company") or {}
            company_name = company_obj.get("name", slug.replace("-", " ").title())
            dept_obj = post.get("department") or {}
            department = dept_obj.get("label", "")
            toe_obj = post.get("typeOfEmployment") or {}
            employment_type = toe_obj.get("label", "")
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
                "salary": "",
                "description_snippet": "",
                "source_ats": "SmartRecruiters",
                "slug": slug,
            })
        offset += limit
        if offset >= total:
            break
        await asyncio.sleep(random.uniform(0.2, 0.6))
    return all_jobs


# ── Taleo (Oracle legacy) ────────────────────────────────
async def scrape_taleo(slug: str) -> list[dict]:
    """Taleo REST API scraper — direct POST. Slug: 'company|section|portal_id'."""
    parts = slug.split("|")
    if len(parts) == 3:
        company, section, portal_id = parts
    elif len(parts) == 2:
        company, section = parts
        career_url = f"https://{company}.taleo.net/careersection/{section}/jobsearch.ftl"
        r = await _get(career_url)
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
            "sortingSelection": {"sortBySelectionParam": "1", "ascendingSortingOrder": "false"},
            "fieldData": {"fields": {"KEYWORD": "", "LOCATION": ""}, "valid": True},
            "filterSelectionParam": {"searchFilterSelections": [
                {"id": "POSTING_DATE", "selectedValues": []},
                {"id": "LOCATION", "selectedValues": []},
                {"id": "JOB_FIELD", "selectedValues": []},
                {"id": "JOB_TYPE", "selectedValues": []},
                {"id": "JOB_SCHEDULE", "selectedValues": []},
            ]},
            "advancedSearchFiltersSelectionParam": {"searchFilterSelections": [
                {"id": "LOCATION", "selectedValues": []},
                {"id": "JOB_FIELD", "selectedValues": []},
                {"id": "JOB_NUMBER", "selectedValues": []},
                {"id": "ORGANIZATION", "selectedValues": []},
            ]},
            "pageNo": page_no,
        }
        headers = {"Content-Type": "application/json", "tz": "GMT-05:00"}
        resp = await _post(api_url, params={"lang": "en", "portal": portal_id},
                           headers=headers, data=json.dumps(payload))
        if not resp:
            log.debug(f"Taleo: API request failed for {company}")
            break
        try:
            data = resp.json()
        except Exception:
            break
        requisitions = data.get("requisitionList", [])
        if not requisitions:
            break
        for req in requisitions:
            contest_no = req.get("contestNo", "")
            columns = req.get("column", [])
            title = columns[0] if len(columns) > 0 else ""
            location_raw = columns[1] if len(columns) > 1 else ""
            location = location_raw
            country = ""
            try:
                loc_list = json.loads(location_raw) if location_raw.startswith("[") else []
                if loc_list:
                    location = "; ".join(loc_list[:3])
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
        paging = data.get("pagingData", {})
        total_count = paging.get("totalCount", 0)
        if len(all_jobs) >= total_count or not requisitions:
            break
        page_no += 1
        await asyncio.sleep(random.uniform(0.3, 1.0))
    return all_jobs


# ── Oracle Cloud HCM ────────────────────────────────────
async def scrape_oracle_cloud_hcm(slug: str) -> list[dict]:
    """Oracle Cloud HCM Recruiting REST API. Slug: 'host_prefix|site_number'."""
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
        "Accept": "application/json",
        "ora-irc-cx-userid": str(_uuid.uuid4()),
        "ora-irc-language": "en",
        "content-type": "application/vnd.oracle.adf.resourceitem+json;charset=utf-8",
    }

    if "." in host_prefix:
        base_api = f"https://{host_prefix}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
        tenant = host_prefix.split(".")[0]
    else:
        tenant = host_prefix
        base_api = None
        # Method 1: follow career-page redirects to discover real domain
        for try_site in (site_number or "CX_1", "CX_1", "CX", "CX_2"):
            probe_url = f"https://{tenant}.oraclecloud.com/hcmUI/CandidateExperience/en/sites/{try_site}/requisitions"
            probe_r = await _get(probe_url, follow_redirects=True, timeout=15)
            if not probe_r:
                continue
            final_host = str(probe_r.url).split("/")[2] if probe_r.url else ""
            if ".fa." in final_host and "oraclecloud.com" in final_host:
                real_prefix = final_host.replace(".oraclecloud.com", "")
                base_api = f"https://{real_prefix}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
                if not site_number:
                    site_number = try_site
                log.debug(f"Oracle Cloud HCM: discovered domain={real_prefix} via redirect for {tenant}")
                break
        # Method 2: brute-force common regions
        if not base_api:
            for region in ("fa.us2", "fa.us6", "fa.us1", "fa.em2", "fa.em3", "fa.em4",
                           "fa.ap1", "fa.ap2", "fa.ca1", "fa.sa1", "fa.me1"):
                test_url = f"https://{tenant}.{region}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
                test_r = await _get(test_url,
                                    params={"onlyData": "true",
                                            "finder": f"findReqs;siteNumber={site_number or 'CX_1'},limit=1,offset=0"},
                                    headers=headers, timeout=8)
                if test_r:
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
        if not base_api:
            log.debug(f"Oracle Cloud HCM: could not discover domain for {tenant}")
            return []

    # Auto-discover site number for tenant-only slugs
    if not site_number:
        for try_site in ("CX_1", "CX", "CX_2", "CX_3"):
            test_params = {"onlyData": "true",
                           "finder": f"findReqs;siteNumber={try_site},limit=1,offset=0"}
            test_r = await _get(base_api, params=test_params, headers=headers)
            if test_r:
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
        r = await _get(base_api, params=params, headers=headers)
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
            api_host = base_api.split("/hcmRestApi")[0]
            job_url = (f"{api_host}/hcmUI/CandidateExperience"
                       f"/en/sites/{site_number}/job/{job_id}")
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
        has_more = first_item.get("hasMore", False)
        total_count = first_item.get("totalCount", 0) or first_item.get("count", 0)
        if not has_more and total_count and len(all_jobs) >= total_count:
            break
        if not has_more and not total_count:
            if len(req_list) < limit:
                break
        offset += limit
        await asyncio.sleep(random.uniform(0.3, 1.0))
    return all_jobs


# ── BrassRing (IBM/Infinite) ─────────────────────────────
async def scrape_brassring(slug: str) -> list[dict]:
    """BrassRing search API scraper. Slug format: 'partner_id|site_id'."""
    parts = slug.split("|")
    if len(parts) != 2:
        log.debug(f"Invalid BrassRing slug format: {slug}")
        return []
    partner_id, site_id = parts
    search_url = "https://sjobs.brassring.com/TgNewUI/Search/Ajax/MatchedJobs"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    all_jobs = []
    page = 1
    while True:
        form_data = (
            f"partnerid={partner_id}&siteid={site_id}"
            f"&keyword=&location=&pagenum={page}"
            f"&sortBy=posteddate&SortType=desc"
        )
        r = await _post(search_url, data=form_data, headers=headers)
        if not r:
            break
        try:
            data = r.json()
        except Exception:
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
        if page * 50 >= total_hits:
            break
        page += 1
        await asyncio.sleep(random.uniform(0.3, 1.0))
    return all_jobs


# ── Teamtailor ───────────────────────────────────────────
async def scrape_teamtailor(slug: str) -> list[dict]:
    """Teamtailor RSS feed scraper with selectolax HTML fallback."""
    company_name = slug.capitalize()
    rss_url = f"https://{slug}.teamtailor.com/jobs.rss"
    headers = {"Accept": "application/rss+xml, application/xml, text/xml"}
    TT_NS = {"tt": "https://teamtailor.com/locations"}

    r = await _get(rss_url, headers=headers)
    if r:
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
                tt_dept = item.findtext("tt:department", default=None, namespaces=TT_NS)
                department = (tt_dept or "").strip() if tt_dept else ""
                if not department:
                    department = (category_el.text or "").strip() if category_el is not None else ""
                location_parts = []
                country = ""
                remote_status = (item.findtext("remoteStatus") or "").strip()
                for loc_el in item.findall("tt:locations/tt:location", TT_NS):
                    loc_name = (loc_el.findtext("tt:name", namespaces=TT_NS) or "").strip()
                    if loc_name:
                        location_parts.append(loc_name)
                    else:
                        city = (loc_el.findtext("tt:city", namespaces=TT_NS) or "").strip()
                        ctry = (loc_el.findtext("tt:country", namespaces=TT_NS) or "").strip()
                        combined = ", ".join(p for p in [city, ctry] if p)
                        if combined:
                            location_parts.append(combined)
                    if not country:
                        country = (loc_el.findtext("tt:country", namespaces=TT_NS) or "").strip()
                location = "; ".join(location_parts[:3]) if location_parts else ""
                if remote_status and remote_status.lower() != "none":
                    location = f"{location} ({remote_status})" if location else remote_status.capitalize()
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

    # ── Fallback: selectolax HTML scrape ──
    html_url = f"https://{slug}.teamtailor.com/jobs"
    r = await _get(html_url, headers=headers)
    if not r:
        return []
    tree = HTMLParser(r.text)
    jobs = []
    seen_urls = set()
    for a in tree.css("a[href*='/jobs/']"):
        path = a.attributes.get("href", "")
        m = re.match(r"/jobs/(\d+)-([^\"']+)", path)
        if not m:
            continue
        job_url = f"https://{slug}.teamtailor.com{path}"
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)
        title_slug = re.sub(r"^\d+-", "", path.split("/jobs/")[-1])
        title = title_slug.replace("-", " ").strip().title()
        jobs.append({
            "title": title, "url": job_url, "company": company_name,
            "location": "", "country": "", "department": "",
            "workplace_type": "", "employment_type": "", "salary": "",
            "description_snippet": "", "source_ats": "Teamtailor", "slug": slug,
        })
    return jobs


# ── SAP SuccessFactors ─────────────────────────────────
async def scrape_successfactors(slug: str) -> list[dict]:
    """SAP SuccessFactors career site scraper."""
    parts = slug.split("|")
    if len(parts) != 2:
        log.debug(f"Invalid SuccessFactors slug format: {slug}")
        return []
    instance, company_key = parts
    base_url = f"https://{instance}"
    api_url = f"{base_url}/xi/ui/pages/careersite/api/v1/jobs"
    headers = {"Accept": "application/json"}
    all_jobs = []
    offset = 0
    limit = 20
    while True:
        params = {"company": company_key, "offset": offset, "limit": limit}
        r = await _get(api_url, params=params, headers=headers)
        if not r:
            break
        try:
            data = r.json()
        except Exception:
            break
        results = data.get("results", data.get("jobRequisitions", []))
        if not results:
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
            job_url = f"{base_url}/career?company={company_key}&career_job_req_id={job_id}&career_ns=job_listing_summary"
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
        total = data.get("total", data.get("totalCount", 0))
        if total and len(all_jobs) >= total:
            break
        if len(results) < limit:
            break
        offset += limit
        await asyncio.sleep(random.uniform(0.3, 1.0))
    return all_jobs


# ── BreezyHR (selectolax) ───────────────────────────────
async def scrape_breezyhr(slug: str) -> list[dict]:
    """BreezyHR — selectolax CSS parsing (replaces fragile regex)."""
    company_name = slug.replace("-", " ").title()
    base_url = f"https://{slug}.breezy.hr"
    r = await _get(base_url)
    if not r:
        return []
    tree = HTMLParser(r.text)
    jobs = []
    seen_urls = set()

    for pos in tree.css("li.position"):
        a_tag = pos.css_first("a[href*='/p/']")
        if not a_tag:
            continue
        href = a_tag.attributes.get("href", "")
        if not href:
            continue
        job_url = f"{base_url}{href}"
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        h2_tag = pos.css_first("h2")
        title = h2_tag.text(strip=True) if h2_tag else ""

        loc_tag = pos.css_first("li.location")
        location = loc_tag.text(strip=True) if loc_tag else ""

        type_tag = pos.css_first("li.type")
        emp_type = type_tag.text(strip=True) if type_tag else ""

        country = ""
        if location:
            loc_parts = [p.strip() for p in location.split(",")]
            if len(loc_parts) >= 2:
                country = loc_parts[-1]

        jobs.append({
            "title": title, "url": job_url, "company": company_name,
            "location": location, "country": country, "department": "",
            "workplace_type": "", "employment_type": emp_type, "salary": "",
            "description_snippet": "", "source_ats": "BreezyHR", "slug": slug,
        })

    # Fallback: bare /p/ links
    if not jobs:
        for a in tree.css("a[href*='/p/']"):
            href = a.attributes.get("href", "")
            m = re.match(r"/p/([a-f0-9]+)[-/]([^\"']+)", href)
            if not m:
                continue
            job_url = f"{base_url}{href}"
            if job_url in seen_urls:
                continue
            seen_urls.add(job_url)
            title = m.group(2).rstrip("/").replace("-", " ").strip().title()
            jobs.append({
                "title": title, "url": job_url, "company": company_name,
                "location": "", "country": "", "department": "",
                "workplace_type": "", "employment_type": "", "salary": "",
                "description_snippet": "", "source_ats": "BreezyHR", "slug": slug,
            })
    return jobs


# ── ApplyToJob (selectolax) ─────────────────────────────
async def scrape_applytojob(slug: str) -> list[dict]:
    """ApplyToJob (JazzHR) — selectolax parsing (replaces fragile regex)."""
    company_name = slug.replace("-", " ").title()
    base_url = f"https://{slug}.applytojob.com"
    r = await _get(base_url)
    if not r:
        return []
    tree = HTMLParser(r.text)
    jobs = []
    seen_urls = set()
    seen_titles = set()

    def _add(title: str, url: str, location: str = ""):
        title = title.strip()
        url = url.strip()
        if not url or len(title) <= 3:
            return
        if not url.startswith("http"):
            url = base_url + url
        title_key = title.lower()
        if url in seen_urls or title_key in seen_titles:
            return
        seen_urls.add(url)
        seen_titles.add(title_key)
        jobs.append({
            "title": title, "url": url, "company": company_name,
            "location": location, "country": "", "department": "",
            "workplace_type": "", "employment_type": "", "salary": "",
            "description_snippet": "", "source_ats": "ApplyToJob", "slug": slug,
        })

    # Pattern 1: newer layout — list-group items
    for item in tree.css("li.list-group-item"):
        heading = item.css_first(".list-group-item-heading a")
        if not heading:
            continue
        url = heading.attributes.get("href", "")
        title = heading.text(strip=True)
        location = ""
        for li in item.css("ul.list-group-item-text li"):
            if li.css_first("i.fa-map-marker"):
                location = li.text(strip=True)
                break
        _add(title, url, location)

    # Pattern 2: legacy layout
    if not jobs:
        for a in tree.css("a.resumator-job-title-link"):
            _add(a.text(strip=True), a.attributes.get("href", ""))

    # Pattern 3: generic fallback
    if not jobs:
        for a in tree.css("a[href*='/apply/'], a[href*='/opening/']"):
            _add(a.text(strip=True), a.attributes.get("href", ""))

    return jobs


# ── HRMDirect (selectolax) ──────────────────────────────
async def scrape_hrmdirect(slug: str) -> list[dict]:
    """HRMDirect / ClearCompany — selectolax table parsing."""
    company_name = slug.replace("-", " ").title()
    url = f"https://{slug}.hrmdirect.com/employment/openings.php?search=true"
    r = await _get(url)
    if not r:
        return []
    tree = HTMLParser(r.text)
    jobs = []
    seen_urls = set()

    for tr in tree.css("tr"):
        link = tr.css_first("a[href*='job-opening.php']")
        if not link:
            continue
        job_path = link.attributes.get("href", "").strip()
        title = link.text(strip=True)
        if not job_path:
            continue
        if not job_path.startswith("http"):
            job_url = urljoin(f"https://{slug}.hrmdirect.com/employment/", job_path)
        else:
            job_url = job_path
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        cells = [td.text(strip=True) for td in tr.css("td")]
        title_idx = -1
        for i, c in enumerate(cells):
            if title and title in c:
                title_idx = i
                break
        city = state = country = department = ""
        if title_idx >= 0:
            remaining = cells[title_idx + 1:]
            if len(remaining) >= 1:
                city = remaining[0]
            if len(remaining) >= 2:
                state = remaining[1]
            if len(remaining) >= 3:
                country = remaining[2]
            if title_idx >= 1:
                department = cells[title_idx - 1]
        location = city
        if state and city:
            location = f"{city}, {state}"
        elif state:
            location = state
        if country and country not in location:
            location = f"{location}, {country}" if location else country

        jobs.append({
            "title": title, "url": job_url, "company": company_name,
            "location": location, "country": country, "department": department,
            "workplace_type": "", "employment_type": "", "salary": "",
            "description_snippet": "", "source_ats": "HRMDirect", "slug": slug,
        })
    return jobs


# ── Softgarden ──────────────────────────────────────────
async def scrape_softgarden(slug: str) -> list[dict]:
    """Softgarden — REST API scraper with JSON-LD fallback."""
    headers = {"Accept": "application/json"}
    api_url = f"https://api.softgarden.io/api/rest/v3/frontend/jobboards/{slug}/jobs"
    r = await _get(api_url, headers=headers, params={"limit": 100, "offset": 0})
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
                    "location": loc, "country": country,
                    "department": (item.get("audience") or item.get("department") or "").strip(),
                    "workplace_type": (item.get("workplaceType") or "").strip(),
                    "employment_type": (item.get("employmentType") or item.get("projectNumber") or "").strip(),
                    "salary": salary, "description_snippet": desc,
                    "source_ats": "Softgarden", "slug": slug,
                })
            if jobs:
                return jobs
        except Exception:
            pass

    html_url = f"https://{slug}.softgarden.io/job/list"
    r = await _get(html_url)
    if not r:
        return []
    jobs = []
    seen = set()
    for ld_match in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>([^<]+)</script>', r.text, re.I):
        try:
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
                    loc, country = "", ""
                desc = _snippet(item.get("description", ""))
                salary = _extract_salary(desc)
                org = item.get("hiringOrganization", {})
                jobs.append({
                    "title": item.get("title", "").strip(),
                    "url": job_url,
                    "company": (org.get("name", "") if isinstance(org, dict) else "").strip(),
                    "location": loc, "country": country, "department": "",
                    "workplace_type": "", "employment_type": item.get("employmentType", ""),
                    "salary": salary, "description_snippet": desc,
                    "source_ats": "Softgarden", "slug": slug,
                })
        except Exception:
            continue
    return jobs


# ── Zoho Recruit ────────────────────────────────────────
async def scrape_zoho(slug: str) -> list[dict]:
    """Zoho Recruit — HTML scrape with embedded JSON."""
    company_name = slug.replace("-", " ").title()
    url = f"https://{slug}.zohorecruit.com/jobs/Careers"
    r = await _get(url)
    if not r:
        return []
    jobs = []
    jobs_input = None
    for attr in ('id', 'name'):
        if jobs_input:
            break
        jobs_input = re.search(rf'<input[^>]*{attr}=["\']jobs["\'][^>]*value=["\']([^"\']+)["\']', r.text, re.I)
        if not jobs_input:
            jobs_input = re.search(rf'<input[^>]*value=["\']([^"\']+)["\'][^>]*{attr}=["\']jobs["\']', r.text, re.I)
    if jobs_input:
        try:
            raw = jobs_input.group(1)
            raw = raw.replace("&quot;", '"').replace("&amp;", "&")
            raw = raw.replace("&lt;", "<").replace("&gt;", ">").replace("&#39;", "'")
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
                        "title": title.strip(), "url": job_url_val, "company": company_name,
                        "location": loc.strip(),
                        "country": country.strip() if isinstance(country, str) else "",
                        "department": (item.get("Department") or "").strip(),
                        "workplace_type": (item.get("Remote_Job") or item.get("Work_Mode") or "").strip(),
                        "employment_type": (item.get("Job_Type") or item.get("jobtype") or "").strip(),
                        "salary": str(salary).strip() if salary else "",
                        "description_snippet": desc, "source_ats": "Zoho", "slug": slug,
                    })
        except Exception as e:
            log.debug(f"Zoho: JSON parse failed for {slug}: {e}")

    if not jobs:
        for ld_match in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>([^<]+)</script>', r.text, re.I):
            try:
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
                        "department": "", "workplace_type": "",
                        "employment_type": item.get("employmentType", ""),
                        "salary": salary, "description_snippet": desc,
                        "source_ats": "Zoho", "slug": slug,
                    })
            except Exception:
                continue

    if not jobs:
        seen = set()
        tree = HTMLParser(r.text)
        for a in tree.css("a[href*='/jobs/'], a[href*='/careers/'], a[href*='/opening']"):
            link = a.attributes.get("href", "").strip()
            title = a.text(strip=True)
            if not link.startswith("http"):
                link = f"https://{slug}.zohorecruit.com{link}"
            if link not in seen and len(title) > 3:
                seen.add(link)
                jobs.append({
                    "title": title, "url": link, "company": company_name,
                    "location": "", "country": "", "department": "",
                    "workplace_type": "", "employment_type": "", "salary": "",
                    "description_snippet": "", "source_ats": "Zoho", "slug": slug,
                })
    return jobs


# ── YCombinator (Work at a Startup) ─────────────────────
async def scrape_ycombinator(slug: str) -> list[dict]:
    """YCombinator Work at a Startup — scrapes company pages."""
    headers = {"Accept": "text/html,application/json"}
    company_url = f"https://www.workatastartup.com/companies/{slug}"
    r = await _get(company_url, headers=headers)
    if not r:
        return []
    jobs = []
    next_data_match = re.search(r'<script\s+id="__NEXT_DATA__"[^>]*>([^<]+)</script>', r.text, re.I)
    if next_data_match:
        try:
            nd = json.loads(next_data_match.group(1))
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
                    "url": job_url, "company": company_name,
                    "location": loc, "country": "",
                    "department": (item.get("role_type") or item.get("role") or "").strip(),
                    "workplace_type": (item.get("remote") or "").strip() if isinstance(item.get("remote"), str) else ("Remote" if item.get("remote") else ""),
                    "employment_type": (item.get("type") or "").strip(),
                    "salary": salary, "description_snippet": desc,
                    "source_ats": "YCombinator", "slug": slug,
                })
            if jobs:
                return jobs
        except Exception as e:
            log.debug(f"YCombinator: NEXT_DATA parse failed for {slug}: {e}")

    seen = set()
    tree = HTMLParser(r.text)
    for a in tree.css("a[href*='/jobs/']"):
        path = a.attributes.get("href", "")
        m = re.search(r"(/jobs/\d+)$", path)
        title = a.text(strip=True)
        if not m or len(title) <= 3:
            continue
        job_url = f"https://www.workatastartup.com{m.group(1)}"
        if job_url not in seen:
            seen.add(job_url)
            jobs.append({
                "title": title, "url": job_url,
                "company": slug.replace("-", " ").title(),
                "location": "", "country": "", "department": "",
                "workplace_type": "", "employment_type": "", "salary": "",
                "description_snippet": "", "source_ats": "YCombinator", "slug": slug,
            })
    return jobs


# ── Personio ────────────────────────────────────────────
async def scrape_personio(slug: str) -> list[dict]:
    """Personio — public XML feed, no auth required."""
    company_name = slug.replace("-", " ").title()
    xml_text = None
    for domain in ["jobs.personio.de", "jobs.personio.com"]:
        url = f"https://{slug}.{domain}/xml?language=en"
        r = await _get(url)
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
        desc_parts = []
        for desc_elem in pos.iter("jobDescription"):
            name = (desc_elem.findtext("name") or "").strip()
            value = (desc_elem.findtext("value") or "").strip()
            if value:
                desc_parts.append(_snippet(value, max_chars=2000))
        desc = " ".join(desc_parts)
        salary = _extract_salary(desc) if desc else ""
        job_url = f"https://{slug}.jobs.personio.de/job/{job_id}" if job_id else ""
        country = ""
        if office:
            office_parts = [p.strip() for p in office.split(",")]
            if len(office_parts) >= 2:
                country = office_parts[-1]
        jobs.append({
            "title": title, "url": job_url, "company": company,
            "location": office, "country": country, "department": department,
            "workplace_type": schedule, "employment_type": emp_type,
            "salary": salary, "description_snippet": desc,
            "source_ats": "Personio", "slug": slug,
        })
    return jobs


# ── JOIN.com ────────────────────────────────────────────
async def scrape_joincom(slug: str) -> list[dict]:
    """JOIN.com — public REST API. Two-step: slug → company_id → jobs."""
    headers = {"Accept": "text/html"}
    page_r = await _get(f"https://join.com/companies/{slug}", headers=headers)
    if not page_r:
        return []
    nd_match = re.search(r'<script\s+id="__NEXT_DATA__"[^>]*>([^<]+)</script>', page_r.text, re.I)
    if not nd_match:
        log.debug(f"JOIN: no __NEXT_DATA__ for {slug}")
        return []
    try:
        nd = json.loads(nd_match.group(1))
        company_id = nd["props"]["pageProps"]["initialState"]["company"]["id"]
        company_name = nd["props"]["pageProps"]["initialState"]["company"].get("name", slug.replace("-", " ").title())
    except (KeyError, json.JSONDecodeError) as e:
        log.debug(f"JOIN: failed to extract company_id for {slug}: {e}")
        return []

    api_base = f"https://join.com/api/public/companies/{company_id}/jobs"
    all_jobs = []
    page = 1
    while True:
        r = await _get(api_base, params={"locale": "en-us", "page": page, "pageSize": 5},
                       headers={"Accept": "application/json"})
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
            wt = item.get("workplaceType", "")
            id_param = item.get("idParam", "")
            job_url = f"https://join.com/companies/{slug}/jobs/{id_param}" if id_param else ""
            all_jobs.append({
                "title": (item.get("title") or "").strip(),
                "url": job_url, "company": company_name,
                "location": location, "country": country, "department": dept,
                "workplace_type": wt, "employment_type": emp_type,
                "salary": salary_str, "description_snippet": "",
                "source_ats": "JOIN", "slug": slug,
            })
        page_count = pagination.get("pageCount", 1)
        if page >= page_count:
            break
        page += 1
        await asyncio.sleep(random.uniform(0.2, 0.5))
    return all_jobs


# ── Paylocity ──────────────────────────────────────────
async def scrape_paylocity(slug: str) -> list[dict]:
    """Paylocity — embedded window.pageData JSON in career page HTML."""
    parts = slug.split("|", 1)
    if len(parts) != 2:
        log.debug(f"Invalid Paylocity slug format: {slug}")
        return []
    company_id, company_name_slug = parts
    url = f"https://recruiting.paylocity.com/recruiting/jobs/All/{company_id}/{company_name_slug}"
    r = await _get(url)
    if not r:
        return []
    pd_match = re.search(r'window\.pageData\s*=\s*(\{.*?\});\s*</script>', r.text, re.DOTALL)
    if not pd_match:
        log.debug(f"Paylocity: no window.pageData found for {company_name_slug}")
        return []
    try:
        page_data = json.loads(pd_match.group(1))
    except json.JSONDecodeError as e:
        log.debug(f"Paylocity: JSON parse failed for {company_name_slug}: {e}")
        return []
    company_name = (page_data.get("companyName") or page_data.get("ModuleTitle")
                    or company_name_slug.replace("-", " ").title())
    jobs_list = page_data.get("Jobs", page_data.get("jobs", []))
    if not isinstance(jobs_list, list):
        return []
    jobs = []
    seen_titles = set()
    for item in jobs_list:
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
        title_key = str(title).lower().strip()
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)
        job_url = f"https://recruiting.paylocity.com/recruiting/jobs/Details/{company_id}/{job_id}/{company_name_slug}"
        jobs.append({
            "title": str(title).strip(), "url": job_url, "company": company_name,
            "location": location or "", "country": "", "department": department,
            "workplace_type": "", "employment_type": item.get("EmploymentType", ""),
            "salary": salary, "description_snippet": desc,
            "source_ats": "Paylocity", "slug": slug,
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


async def scrape_board(ats: str, slug: str) -> list[dict]:
    """Dispatch to the correct scraper."""
    fn = SCRAPERS.get(ats.lower())
    if not fn:
        log.warning(f"Unknown ATS: {ats}")
        return []
    try:
        return await fn(slug)
    except Exception as e:
        log.error(f"Error scraping {ats}/{slug}: {e}")
        return []


# ── Second-pass: fetch individual job descriptions ─────
def _extract_location_from_html(html: str) -> str:
    """Universal location extractor — JSON-LD first, then meta tags, then HTML patterns."""
    for ld_match in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                                html, re.I | re.DOTALL):
        try:
            ld = json.loads(ld_match.group(1))
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
                return "; ".join(parts[:3])
        except Exception:
            continue

    meta_loc_tags = [
        ("property", "og:locality"),
        ("name", "geo.placename"),
        ("name", "location"),
    ]
    for attr, val in meta_loc_tags:
        escaped_val = re.escape(val)
        for pat in [
            rf'<meta[^>]*{attr}=["\']{escaped_val}["\'][^>]*content=["\']([^"\']+)["\']',
            rf'<meta[^>]*content=["\']([^"\']+)["\'][^>]*{attr}=["\']{escaped_val}["\']',
        ]:
            m = re.search(pat, html, re.I)
            if m:
                loc = m.group(1).strip()
                if loc and len(loc) < 200:
                    return loc

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
    """Extract location from iCIMS job page HTML."""
    m = re.search(r'\b([A-Z]{2}-[A-Z]{2}-[\w\s-.]+?)(?:<|"|\'|\s*\n|\s*<)', html)
    if m:
        return m.group(1).strip()
    m = re.search(r'Careers\s+at\s+([^|<"]+)', html, re.I)
    if m:
        loc = m.group(1).strip().rstrip(' .')
        if loc and len(loc) < 100:
            return loc
    m = re.search(r'<title>[^<]*?\bin\s+([^|<]+?)(?:\s*\|)', html, re.I)
    if m:
        loc = m.group(1).strip().rstrip(' .')
        if loc and len(loc) < 100:
            return loc
    m = re.search(r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)["\']', html, re.I)
    if m:
        og_title = m.group(1)
        m2 = re.search(r'Careers\s+at\s+(.+)', og_title, re.I)
        if m2:
            loc = m2.group(1).strip()
            if loc and len(loc) < 100:
                return loc
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
    return _extract_location_from_html(html)


async def _fetch_icims_content(url: str) -> str:
    """Fetch iCIMS job page HTML, handling the iframe wrapper problem."""
    iframe_url = url + ("&" if "?" in url else "?") + "in_iframe=1"
    r = await _get(iframe_url)
    if r and r.text:
        text = r.text
        has_icims_content = any(marker in text for marker in [
            "iCIMS_", "icims", "job-description", "JobContent",
            "addressLocality", "JobPosting", "jobLocation",
        ])
        has_real_title = "<title>" in text and "in_iframe" not in text.lower()
        if has_icims_content or has_real_title:
            return text

    r = await _get(url)
    if r and r.text:
        text = r.text
        has_iframe = re.search(r'<iframe[^>]*src=["\'][^"\']*in_iframe', text, re.I)
        if has_iframe:
            iframe_match = re.search(r'<iframe[^>]*src=["\']([^"\']+)["\']', text, re.I)
            if iframe_match:
                iframe_src = iframe_match.group(1)
                if not iframe_src.startswith("http"):
                    iframe_src = urljoin(url, iframe_src)
                r2 = await _get(iframe_src)
                if r2 and r2.text:
                    return r2.text
        return text

    mobile_url = url + ("&" if "?" in url else "?") + "mobile=true&needsRedirect=false"
    r = await _get(mobile_url)
    if r and r.text:
        return r.text
    return ""


async def _fetch_icims_description(job: dict) -> str:
    """Fetch full description and location from an individual iCIMS job page."""
    html = await _fetch_icims_content(job["url"])
    if not html:
        return ""
    if not job.get("location"):
        loc = _extract_icims_location(html)
        if loc:
            job["location"] = loc

    for ld_match in re.finditer(r'<script[^>]*>(.*?)</script>', html, re.I | re.DOTALL):
        content = ld_match.group(1).strip()
        if not content.startswith("{"):
            continue
        try:
            ld = json.loads(content)
            if isinstance(ld, dict) and ld.get("@type") == "JobPosting" and ld.get("description"):
                return _snippet(ld["description"])
        except Exception:
            continue

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

    body_match = re.search(r'<main[^>]*>(.*?)</main>', html, re.DOTALL | re.IGNORECASE)
    if body_match:
        text = _snippet(body_match.group(1))
        if len(text) > 100:
            return text
    return ""


async def _fetch_workday_description(job: dict) -> str:
    """Fetch full description from a Workday job detail API."""
    url = job.get("url", "")
    if not url:
        return ""
    parsed = urlparse(url)
    hostname = parsed.hostname or ""
    if "myworkdayjobs.com" not in hostname:
        return ""
    path = parsed.path
    api_url = f"https://{hostname}/wday/cxs{path}"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    r = await _get(api_url, headers=headers)
    if not r:
        return ""
    try:
        data = r.json()
        posting = data.get("jobPostingInfo", {})
        desc = posting.get("jobDescription", "")
        if desc:
            return _snippet(desc)
    except Exception:
        pass
    return ""


async def _fetch_smartrecruiters_description(job: dict) -> str:
    """Fetch full description from SmartRecruiters job detail API."""
    url = job.get("url", "")
    slug = job.get("slug", "")
    if not url or not slug:
        return ""
    parts = url.rstrip("/").split("/")
    if len(parts) < 2:
        return ""
    posting_id = parts[-1]
    api_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/{posting_id}"
    r = await _get(api_url)
    if not r:
        return ""
    try:
        data = r.json()
        job_ad = data.get("jobAd", {})
        sections = job_ad.get("sections", {})
        desc_section = sections.get("jobDescription", {})
        desc = desc_section.get("text", "")
        if desc:
            return _snippet(desc)
        comp_desc = sections.get("companyDescription", {}).get("text", "")
        if comp_desc:
            return _snippet(comp_desc)
    except Exception:
        pass
    return ""


async def _fetch_taleo_description(job: dict) -> str:
    """Fetch full description from a Taleo job detail page."""
    r = await _get(job["url"])
    if not r:
        return ""
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


async def _fetch_generic_description(job: dict) -> str:
    """Generic description fetcher — loads the job URL and extracts text."""
    url = job.get("url", "")
    if not url:
        return ""
    r = await _get(url)
    if not r:
        return ""
    html = r.text

    if not job.get("location"):
        loc = _extract_location_from_html(html)
        if loc:
            job["location"] = loc

    ld_match = re.search(r'<script[^>]*type="application/ld\+json"[^>]*>([^<]+)</script>', html, re.I)
    if ld_match:
        try:
            ld = json.loads(ld_match.group(1))
            if isinstance(ld, dict) and ld.get("description"):
                return _snippet(ld["description"])
        except Exception:
            pass

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


async def _fetch_joincom_description(job: dict) -> str:
    """Fetch full description from JOIN.com job detail (via generic fetcher)."""
    return await _fetch_generic_description(job)


async def _fetch_teamtailor_location(job: dict) -> str:
    """Fetch location from a Teamtailor job page (JSON-LD)."""
    url = job.get("url", "")
    if not url:
        return job.get("description_snippet", "")
    r = await _get(url)
    if not r:
        return job.get("description_snippet", "")
    html = r.text

    if not job.get("location"):
        loc = _extract_location_from_html(html)
        if loc:
            job["location"] = loc

    existing_desc = job.get("description_snippet", "")
    if len(existing_desc) < 100:
        desc = ""
        for ld_match in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                                    html, re.I | re.DOTALL):
            try:
                ld = json.loads(ld_match.group(1))
                if isinstance(ld, dict) and ld.get("description"):
                    desc = _snippet(ld["description"])
                    break
            except Exception:
                continue
        if desc:
            return desc
    return existing_desc


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


async def enrich_descriptions(jobs: list[dict], max_concurrent: int | None = None) -> list[dict]:
    """Fetch individual job descriptions concurrently (asyncio, bounded by semaphore)."""
    max_concurrent = max_concurrent or ENRICH_CONCURRENCY
    to_enrich = [j for j in jobs
                 if j.get("source_ats") in DESCRIPTION_FETCHERS
                 and (not j.get("description_snippet") or not j.get("location"))]
    if not to_enrich:
        return jobs

    log.info(f"Enriching {len(to_enrich)} jobs (missing description or location) "
             f"across {len(set(j['source_ats'] for j in to_enrich))} platforms...")

    sem = asyncio.Semaphore(max_concurrent)

    async def _fetch_one(job):
        async with sem:
            fetcher = DESCRIPTION_FETCHERS[job["source_ats"]]
            try:
                desc = await fetcher(job)
                if desc:
                    job["description_snippet"] = desc
                    salary = _extract_salary(desc)
                    if salary and not job.get("salary"):
                        job["salary"] = salary
            except Exception as e:
                log.debug(f"Failed to enrich {job['url']}: {e}")
            await asyncio.sleep(random.uniform(0.1, 0.3))
            return job

    await asyncio.gather(*[_fetch_one(j) for j in to_enrich], return_exceptions=True)
    enriched = sum(1 for j in to_enrich if j.get("description_snippet"))
    log.info(f"Enriched {enriched}/{len(to_enrich)} jobs with descriptions")

    # ── Fallback: fetch job URL directly for ANY job still missing a JD ──
    still_missing = [j for j in jobs if not j.get("description_snippet") and j.get("url")]
    if still_missing:
        log.info(f"Fallback: fetching {len(still_missing)} job URLs directly for missing JDs...")

        async def _fetch_fallback(job):
            async with sem:
                try:
                    desc = await _fetch_generic_description(job)
                    if desc:
                        job["description_snippet"] = desc
                        salary = _extract_salary(desc)
                        if salary and not job.get("salary"):
                            job["salary"] = salary
                except Exception as e:
                    log.debug(f"Fallback fetch failed {job['url']}: {e}")
                await asyncio.sleep(random.uniform(0.1, 0.4))
                return job

        await asyncio.gather(*[_fetch_fallback(j) for j in still_missing], return_exceptions=True)
        fallback_ok = sum(1 for j in still_missing if j.get("description_snippet"))
        log.info(f"Fallback enriched {fallback_ok}/{len(still_missing)} jobs from job URLs")

    no_location = sum(1 for j in jobs if not j.get("location"))
    if no_location:
        log.info(f"Note: {no_location} jobs still have no location (pages lack structured location data)")
    return jobs


# ── Application Question Enrichment ─────────────────────
_WORK_AUTH_RE = re.compile(
    r"(authorized?\s+to\s+work|work\s+authoriz|visa\s+sponsor|"
    r"immigration\s+sponsor|right\s+to\s+work|work\s+permit|"
    r"employment\s+eligib|legally\s+authorized|"
    r"require.*\bsponsorship\b|"
    r"do\s+you\s+now\s+or\s+in\s+the\s+future\s+require)",
    re.I,
)


async def _fetch_greenhouse_questions(job: dict) -> str:
    """Fetch application questions from Greenhouse job API."""
    url = job.get("url", "")
    m = re.search(r"greenhouse\.io/([^/]+)/jobs/(\d+)", url)
    if not m:
        return ""
    slug, job_id = m.group(1), m.group(2)
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}?questions=true"
    r = await _get(api_url)
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
    metadata = data.get("metadata") or []
    for md in metadata:
        if isinstance(md, dict):
            name = md.get("name", "").lower()
            val = str(md.get("value", ""))
            if name in ("location", "location_country") and val:
                auth_questions.append(f"Metadata Location: {val}")
    return "\n".join(auth_questions)


async def _fetch_ashby_questions(job: dict) -> str:
    """Fetch application form from Ashby posting API."""
    url = job.get("url", "")
    m = re.search(r"ashbyhq\.com/([^/]+)/([a-f0-9-]+)", url)
    if not m:
        return ""
    slug, job_id = m.group(1), m.group(2)
    api_url = f"https://api.ashbyhq.com/posting-api/posting/{slug}/{job_id}"
    r = await _get(api_url)
    if not r:
        return ""
    try:
        data = r.json()
    except Exception:
        return ""
    auth_questions = []
    form_def = data.get("applicationFormDefinition") or data.get("formDefinition") or {}
    sections = form_def.get("sections") or []
    for section in sections:
        fields = section.get("fields") or section.get("fieldEntries") or []
        for field in fields:
            f = field.get("field", field) if isinstance(field, dict) else field
            if not isinstance(f, dict):
                continue
            title = f.get("title", "") or f.get("label", "") or f.get("name", "")
            if _WORK_AUTH_RE.search(title):
                auth_questions.append(f"Application Question: {title}")
    survey = data.get("surveyQuestions") or []
    for sq in survey:
        label = sq.get("label", "") or sq.get("title", "") or sq.get("question", "")
        if _WORK_AUTH_RE.search(label):
            auth_questions.append(f"Application Question: {label}")
    return "\n".join(auth_questions)


async def enrich_application_questions(jobs: list[dict], max_concurrent: int | None = None) -> list[dict]:
    """Fetch application questions for Greenhouse/Ashby jobs with bare 'Remote' location."""
    max_concurrent = max_concurrent or QUESTION_ENRICH_CONCURRENCY
    bare_remote_re = re.compile(r"^\s*(remote|fully\s*remote|remote\s*worker|remote\s*job)?\s*$", re.I)
    to_enrich = [
        j for j in jobs
        if j.get("source_ats") in ("Greenhouse", "Ashby")
        and bare_remote_re.match(j.get("location", ""))
    ]
    if not to_enrich:
        return jobs

    log.info(f"Fetching application questions for {len(to_enrich)} "
             f"Greenhouse/Ashby jobs with bare 'Remote' location...")
    fetchers = {"Greenhouse": _fetch_greenhouse_questions, "Ashby": _fetch_ashby_questions}
    sem = asyncio.Semaphore(max_concurrent)

    async def _fetch_one(job):
        async with sem:
            fetcher = fetchers[job["source_ats"]]
            try:
                questions = await fetcher(job)
                if questions:
                    existing = job.get("description_snippet", "") or ""
                    job["description_snippet"] = existing + "\n\n" + questions
            except Exception as e:
                log.debug(f"Failed to fetch questions for {job['url']}: {e}")
            await asyncio.sleep(random.uniform(0.1, 0.3))
            return job

    await asyncio.gather(*[_fetch_one(j) for j in to_enrich], return_exceptions=True)
    enriched = sum(1 for j in to_enrich if "Application Question:" in (j.get("description_snippet") or ""))
    log.info(f"Found work-auth questions for {enriched}/{len(to_enrich)} jobs")
    return jobs
