"""
ATS scrapers — one function per platform.
Each returns a list of job dicts with standardised keys.
Converted to Async I/O for maximum throughput.
"""

import re
import logging
import random
import asyncio
import xml.etree.ElementTree as ET
from urllib.parse import unquote
import httpx
from config import REQUEST_TIMEOUT, MAX_RETRIES

log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
]

async def _get(client: httpx.AsyncClient, url: str, **kwargs) -> httpx.Response | None:
    """Reusable async HTTP GET with exponential backoff on 429s."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = await client.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            if r.status_code == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r
        except httpx.HTTPError as e:
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


# ── ATS Dispatcher & Enrichers ──────────────────────────

async def scrape_board(ats: str, slug: str, client: httpx.AsyncClient) -> list[dict]:
    """Routes the scrape request to the correct ATS handler."""
    func_name = f"scrape_{ats}"
    func = globals().get(func_name)
    if func:
        return await func(slug, client)
    log.debug(f"No scraper implemented yet for ATS: {ats}")
    return []

async def enrich_descriptions(jobs: list[dict], client: httpx.AsyncClient) -> list[dict]:
    """Placeholder for description enrichment API calls if required."""
    return jobs

async def enrich_application_questions(jobs: list[dict], client: httpx.AsyncClient) -> list[dict]:
    """Placeholder for app questions enrichment if required."""
    return jobs


# ── Rippling ────────────────────────────────────────────

async def scrape_rippling(slug: str, client: httpx.AsyncClient) -> list[dict]:
    base = f"https://ats.rippling.com/api/v2/board/{slug}/jobs"
    all_jobs = []
    page = 0

    while True:
        r = await _get(client, base, params={
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
            countries = ", ".join(sorted(set(l.get("country", "") for l in locations if l.get("country"))))
            wt = ", ".join(sorted(set(l.get("workplaceType", "") for l in locations if l.get("workplaceType"))))
            
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

        if page + 1 >= data.get("totalPages", 0) or not data.get("items"):
            break
        page += 1

    if all_jobs:
        r = await _get(client, f"https://ats.rippling.com/api/v2/board/{slug}/jobs", params={"page": 0, "pageSize": 1})
        if r:
            try:
                first_id = all_jobs[0]["url"].split("/")[-1]
                detail_r = await _get(client, f"https://ats.rippling.com/api/v2/board/{slug}/jobs/{first_id}")
                if detail_r:
                    company_name = detail_r.json().get("companyName", "")
                    for j in all_jobs:
                        j["company"] = company_name
            except Exception:
                pass

    return all_jobs


# ── Greenhouse ──────────────────────────────────────────

async def scrape_greenhouse(slug: str, client: httpx.AsyncClient) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    r = await _get(client, url, params={"content": "true"})
    if not r: return []
    try:
        data = r.json()
    except Exception:
        return []

    jobs_list = data.get("jobs")
    if not jobs_list or not isinstance(jobs_list, list): return []

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
                if not isinstance(m, dict): continue
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
        board_r = await _get(client, f"https://boards-api.greenhouse.io/v1/boards/{slug}")
        if board_r:
            try:
                company_name = board_r.json().get("name", "")
                for j in jobs:
                    j["company"] = company_name
            except Exception:
                pass

    return jobs


# ── Lever ───────────────────────────────────────────────

async def scrape_lever(slug: str, client: httpx.AsyncClient) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{slug}"
    r = await _get(client, url, params={"mode": "json"})
    if not r:
        r = await _get(client, f"https://api.eu.lever.co/v0/postings/{slug}", params={"mode": "json"})
        if not r: return []
    try:
        data = r.json()
    except Exception:
        return []

    if not isinstance(data, list): return []

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
            "company": slug.replace("-", " ").title(),
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

    return jobs


# ── Ashby ───────────────────────────────────────────────

async def scrape_ashby(slug: str, client: httpx.AsyncClient) -> list[dict]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    r = await _get(client, url, params={"includeCompensation": "true"})
    if not r: return []
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
            addr_loc = ", ".join(filter(None, [addr_city, addr_region, addr_country]))
            if addr_loc:
                workplace_type = loc.strip() if loc.strip() else (post.get("workplaceType") or "")
                if workplace_type.lower() == "remote":
                    loc = f"Remote, {addr_loc}"
                else:
                    loc = addr_loc

        country = addr_country
        if not country and loc:
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
            "workplace_type": post.get("workplaceType", "") or post.get("employmentType", ""),
            "employment_type": "",
            "salary": salary_str,
            "description_snippet": _snippet(post.get("descriptionHtml", "") or post.get("descriptionPlain", "")),
            "source_ats": "Ashby",
            "slug": slug,
        })

    return jobs


# ── BambooHR ───────────────────────────────────────────

async def scrape_bamboohr(slug: str, client: httpx.AsyncClient) -> list[dict]:
    url = f"https://{slug}.bamboohr.com/careers/list"
    headers = {"Accept": "application/json", "User-Agent": random.choice(USER_AGENTS)}
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        if r.status_code != 200 or "application/json" not in r.headers.get("Content-Type", ""):
            return []
        data = r.json()
    except Exception:
        return []

    jobs_list = data.get("result")
    if not jobs_list or not isinstance(jobs_list, list): return []

    jobs = []
    for job in jobs_list:
        loc = job.get("location") or {}
        ats_loc = job.get("atsLocation") or {}
        if isinstance(loc, dict):
            city, state, country = loc.get("city", ""), loc.get("state", ""), loc.get("country", "")
        else:
            city, state, country = (str(loc) if loc else ""), "", ""

        if not city and not state and not country and isinstance(ats_loc, dict):
            city = ats_loc.get("city", "") or ats_loc.get("province", "") or ""
            state = ats_loc.get("state", "") or ""
            country = ats_loc.get("country", "") or ""

        desc = _snippet(job.get("description", "") or "")
        jobs.append({
            "title": (job.get("jobOpeningName") or "").strip(),
            "url": f"https://{slug}.bamboohr.com/careers/{job.get('id', '')}",
            "company": slug.replace("-", " ").title(),
            "location": ", ".join(filter(None, [city, state, country])),
            "country": country,
            "department": job.get("departmentLabel", "") or "",
            "workplace_type": "",
            "employment_type": job.get("employmentStatusLabel", ""),
            "salary": _extract_salary(desc),
            "description_snippet": desc,
            "source_ats": "BambooHR",
            "slug": slug,
        })

    return jobs


# ── iCIMS ──────────────────────────────────────────────

async def scrape_icims(slug: str, client: httpx.AsyncClient) -> list[dict]:
    sitemap_url = f"https://{slug}.icims.com/sitemap.xml"
    headers = {"Accept": "application/xml", "User-Agent": random.choice(USER_AGENTS)}
    try:
        r = await client.get(sitemap_url, timeout=REQUEST_TIMEOUT, headers=headers)
        if r.status_code != 200: return []
        root = ET.fromstring(r.content)
    except Exception:
        return []

    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    jobs = []

    for url_el in root.findall(".//s:url", ns):
        loc_el = url_el.find("s:loc", ns)
        if loc_el is None: continue
        job_url = (loc_el.text or "").strip()
        if not job_url or "/jobs/" not in job_url or job_url.endswith("/jobs/intro"):
            continue

        parts = job_url.split("/jobs/")[-1].split("/")
        if len(parts) < 2: continue
        title = unquote(parts[1]).replace("-", " ").strip().title()

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

async def scrape_workday(slug: str, client: httpx.AsyncClient) -> list[dict]:
    parts = slug.split("|")
    if len(parts) != 3: return []

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
        payload = {"appliedFacets": {}, "limit": limit, "offset": offset, "searchText": ""}
        try:
            r = await client.post(api_url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200: break
            data = r.json()
        except Exception:
            break

        postings = data.get("jobPostings", [])
        if not postings: break

        for post in postings:
            posted_on = post.get("postedOn", "") or ""
            if "30+" in posted_on: continue

            location = (post.get("locationsText") or "").strip()
            if not location:
                bf = post.get("bulletFields") or []
                if len(bf) >= 2:
                    cities, states = bf[0] or "", bf[1] or ""
                    location = f"{cities}, {states}" if cities and states else (cities or states)

            all_jobs.append({
                "title": (post.get("title") or "").strip(),
                "url": f"{base_url}/{site_id}{post.get('externalPath', '')}",
                "company": company.replace("-", " ").title(),
                "location": location[:200],
                "country": "",
                "department": "",
                "workplace_type": post.get("remoteType", "") or "",
                "employment_type": "",
                "salary": "",
                "description_snippet": "",
                "source_ats": "Workday",
                "slug": slug,
            })

        offset += limit
        if offset >= data.get("total", 0): break
        
        await asyncio.sleep(random.uniform(0.3, 1.0))

    return all_jobs


# ── Workable ──────────────────────────────────────────

async def scrape_workable(slug: str, client: httpx.AsyncClient) -> list[dict]:
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    r = await _get(client, url, params={"details": "true"}, headers=headers)
    if not r: return []
    try:
        data = r.json()
    except Exception:
        return []

    company_name = data.get("name", slug.replace("-", " ").title())
    jobs_list = data.get("jobs")
    if not jobs_list or not isinstance(jobs_list, list): return []

    jobs = []
    for post in jobs_list:
        city, state, country = post.get("city", ""), post.get("state", ""), post.get("country", "")
        desc = _snippet(post.get("description", ""))

        jobs.append({
            "title": (post.get("title") or "").strip(),
            "url": post.get("url") or post.get("shortlink") or "",
            "company": company_name,
            "location": ", ".join(filter(None, [city, state, country])),
            "country": country,
            "department": post.get("department", ""),
            "workplace_type": "Remote" if post.get("telecommuting", False) else "",
            "employment_type": post.get("employment_type", ""),
            "salary": _extract_salary(desc),
            "description_snippet": desc,
            "source_ats": "Workable",
            "slug": slug,
        })

    return jobs


# ── Recruitee ─────────────────────────────────────────

async def scrape_recruitee(slug: str, client: httpx.AsyncClient) -> list[dict]:
    url = f"https://{slug}.recruitee.com/api/offers/"
    headers = {"Accept": "application/json", "User-Agent": random.choice(USER_AGENTS)}
    r = await _get(client, url, headers=headers)
    if not r: return []
    try:
        data = r.json()
    except Exception:
        return []

    offers = data.get("offers")
    if not offers or not isinstance(offers, list): return []

    jobs = []
    for offer in offers:
        city, country = offer.get("city", ""), offer.get("country", "")
        location = offer.get("location", "") or ", ".join(filter(None, [city, country]))
        
        en_trans = (offer.get("translations") or {}).get("en", {})
        desc_html = en_trans.get("description", "") or offer.get("description", "")
        desc = _snippet(desc_html)

        salary_str = ""
        salary_obj = offer.get("salary")
        if isinstance(salary_obj, dict):
            min_sal, max_sal = salary_obj.get("min", ""), salary_obj.get("max", "")
            if min_sal and max_sal:
                salary_str = f"{salary_obj.get('currency', '')} {min_sal}-{max_sal}".strip()
                if salary_obj.get("period"): salary_str += f" per {salary_obj.get('period')}"
        elif isinstance(salary_obj, str) and salary_obj:
            salary_str = salary_obj

        jobs.append({
            "title": (offer.get("title") or "").strip(),
            "url": offer.get("careers_url") or offer.get("url") or f"https://{slug}.recruitee.com/o/{offer.get('slug', '')}",
            "company": offer.get("company_name", slug.replace("-", " ").title()),
            "location": location,
            "country": country,
            "department": offer.get("department", ""),
            "workplace_type": "Remote" if offer.get("remote", False) else "",
            "employment_type": offer.get("employment_type_code", ""),
            "salary": salary_str or _extract_salary(desc),
            "description_snippet": desc,
            "source_ats": "Recruitee",
            "slug": slug,
        })

    return jobs


# ── SmartRecruiters ───────────────────────────────────

async def scrape_smartrecruiters(slug: str, client: httpx.AsyncClient) -> list[dict]:
    base_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    all_jobs = []
    offset = 0
    limit = 100

    while True:
        r = await _get(client, base_url, params={"limit": limit, "offset": offset}, headers=headers)
        if not r: break
        try:
            data = r.json()
        except Exception:
            break

        content = data.get("content", [])
        if not content: break

        for post in content:
            loc = post.get("location") or {}
            remote = loc.get("remote", False)
            location = ", ".join(filter(None, [loc.get("city", ""), loc.get("region", ""), loc.get("country", "")]))
            if remote and not location: location = "Remote"
            elif remote: location += " (Remote)"

            company_name = (post.get("company") or {}).get("name", slug.replace("-", " ").title())
            department = (post.get("department") or {}).get("label", "")
            emp_type = (post.get("typeOfEmployment") or {}).get("label", "")
            
            job_url = f"https://jobs.smartrecruiters.com/{slug}/{post.get('id', '')}" if post.get("id") else post.get("ref", "")

            all_jobs.append({
                "title": (post.get("name") or "").strip(),
                "url": job_url,
                "company": company_name,
                "location": location,
                "country": loc.get("country", ""),
                "department": department,
                "workplace_type": "Remote" if remote else "",
                "employment_type": emp_type,
                "salary": "",
                "description_snippet": "",
                "source_ats": "SmartRecruiters",
                "slug": slug,
            })

        offset += limit
        if offset >= data.get("totalFound", 0): break
        await asyncio.sleep(random.uniform(0.2, 0.6))

    return all_jobs


# ── Taleo (Oracle legacy) ────────────────────────────────

async def scrape_taleo(slug: str, client: httpx.AsyncClient) -> list[dict]:
    parts = slug.split("|")
    if len(parts) == 3:
        company, section, portal_id = parts
    elif len(parts) == 2:
        company, section = parts
        career_url = f"https://{company}.taleo.net/careersection/{section}/jobsearch.ftl"
        r = await _get(client, career_url, headers={"User-Agent": random.choice(USER_AGENTS)})
        if not r: return []
        portal_match = re.search(r'portal\s*=\s*["\']?(\d+)', r.text, re.I)
        if not portal_match: return []
        portal_id = portal_match.group(1)
    else:
        return []

    base_url = f"https://{company}.taleo.net/careersection"
    api_url = f"{base_url}/rest/jobboard/searchjobs"
    all_jobs = []
    page_no = 1

    headers = {
        "Content-Type": "application/json",
        "tz": "GMT-05:00",
        "User-Agent": random.choice(USER_AGENTS),
    }

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

        try:
            resp = await client.post(
                api_url,
                params={"lang": "en", "portal": portal_id},
                headers=headers,
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200: break
            data = resp.json()
        except Exception:
            break

        requisitions = data.get("requisitionList", [])
        if not requisitions: break
        
        for req in requisitions:
            all_jobs.append({
                "title": str(req.get("jobTitle", "Taleo Job")),
                "url": f"https://{company}.taleo.net/careersection/{section}/jobdetail.ftl?job={req.get('contestNumber', '')}",
                "company": company.title(),
                "location": "Remote",
                "country": "",
                "department": "",
                "workplace_type": "",
                "employment_type": "",
                "salary": "",
                "description_snippet": "",
                "source_ats": "Taleo",
                "slug": slug,
            })
        
        page_no += 1
        await asyncio.sleep(random.uniform(0.2, 0.6))

    return all_jobs
