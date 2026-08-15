


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
  - We Work Remotely https://weworkremotely.com/remote-jobs.rss + category RSS feeds
  - Working Nomads   https://www.workingnomads.co/api/exposed_jobs/
  - FreeHire         https://www.freehire.me/api/v1/jobs (paged; configurable depth)

All APIs are free, no auth required. Output is normalized to the same dict
format that ats_scrapers.py produces, so it feeds directly into the existing
classification pipeline (role filter → location filter → Supabase).
"""

import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlsplit, urlunsplit
import xml.etree.ElementTree as ET
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
    (re.compile(r"([^/.\s]+)\.applytojob\.com", re.I), "applytojob"),
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
    max_pages = int(os.environ.get("HIMALAYAS_MAX_PAGES", "500"))  # 10,000-job safety ceiling; API max is 20/page

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
        total_count = int(data.get("totalCount") or 0)
        if total_count and offset >= total_count:
            break
        if len(page_jobs) < limit:
            break
        # Public API is rate limited; keep a small delay between pages.
        time.sleep(float(os.environ.get("HIMALAYAS_PAGE_DELAY", "0.35")))

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
    """Fetch the maximum 100 remote jobs from Jobicy per request."""
    url = "https://jobicy.com/api/v2/remote-jobs?count=100"
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
# WE WORK REMOTELY (RSS)
# ═══════════════════════════════════════════════════════

# Category feeds for targeted scraping (plus main feed as fallback)
_WWR_FEEDS = [
    # Main feed + every public category feed. The same job can appear in more
    # than one feed; scrape all of them and deduplicate after normalization.
    "https://weworkremotely.com/remote-jobs.rss",
    "https://weworkremotely.com/categories/remote-customer-support-jobs.rss",
    "https://weworkremotely.com/categories/remote-product-jobs.rss",
    "https://weworkremotely.com/categories/remote-full-stack-programming-jobs.rss",
    "https://weworkremotely.com/categories/remote-back-end-programming-jobs.rss",
    "https://weworkremotely.com/categories/remote-front-end-programming-jobs.rss",
    "https://weworkremotely.com/categories/remote-programming-jobs.rss",
    "https://weworkremotely.com/categories/remote-sales-and-marketing-jobs.rss",
    "https://weworkremotely.com/categories/remote-management-and-finance-jobs.rss",
    "https://weworkremotely.com/categories/remote-design-jobs.rss",
    "https://weworkremotely.com/categories/remote-devops-sysadmin-jobs.rss",
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
_FREEHIRE_EXCLUDE_SOURCES: set[str] = set()

# We intentionally DO NOT exclude ATS sources already scraped directly.
# The board layer is a second discovery surface and the final pipeline
# deduplicates jobs. This preserves maximum coverage and also lets us discover
# ATS slugs from aggregator URLs even when a direct ATS board scrape fails.



def scrape_freehire() -> list[dict]:
    """Fetch as many FreeHire jobs as practical; do not discard overlapping ATS sources.

    FreeHire is treated as an independent discovery surface. Duplicates are
    removed later, after we have had a chance to discover direct ATS URLs.
    Set FREEHIRE_MAX_PAGES to control the scan depth (100 jobs/page).
    """
    jobs = []
    page = 1
    max_pages = int(os.environ.get("FREEHIRE_MAX_PAGES", "100"))  # 10,000 jobs by default

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

            source = (item.get("source", "") or "").lower().strip()

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
# JOOBLE
# ═══════════════════════════════════════════════════════

# Targeted queries: CSM/AM roles in key hiring regions.
# We search specific countries rather than all 60+ to stay under the 500 req limit.
# Each (keyword, location) pair = 1 page of results (~1 request). Budget:
#   4 keywords × 8 locations × ~2 pages avg = ~64 requests, well under 500.
_JOOBLE_KEYWORDS = [
    "customer success manager",
    "account manager",
    "client success manager",
    "customer experience manager",
]

_JOOBLE_LOCATIONS = [
    "Remote",
    "United States",
    "United Kingdom",
    "Canada",
    "Germany",
    "Netherlands",
    "Ireland",
    "Australia",
]

_JOOBLE_QUERIES = [
    {"keywords": kw, "location": loc}
    for kw in _JOOBLE_KEYWORDS
    for loc in _JOOBLE_LOCATIONS
]


def scrape_jooble() -> list[dict]:
    """Fetch remote CSM/AM jobs from Jooble in targeted countries.

    Requires JOOBLE_API_KEY env var. Skipped silently if not set.
    Uses targeted keyword + location searches (32 combos × ~2 pages = ~64 requests,
    well under the 500 request API limit).
    """
    api_key = os.environ.get("JOOBLE_API_KEY", "")
    if not api_key:
        log.info("  Jooble: skipped (JOOBLE_API_KEY not set)")
        return []

    endpoint = f"https://jooble.org/api/{api_key}"
    jobs = []
    seen_ids = set()

    total_requests = 0
    max_total_requests = 400  # hard cap to stay under 500 API limit

    for query in _JOOBLE_QUERIES:
        if total_requests >= max_total_requests:
            log.warning(f"  Jooble: hit {max_total_requests} request cap, stopping early")
            break

        page = 1
        max_pages = 3  # 3 pages per query × 32 queries = 96 max requests

        for _ in range(max_pages):
            if total_requests >= max_total_requests:
                break
            payload = {**query, "page": str(page)}
            try:
                resp = requests.post(
                    endpoint,
                    json=payload,
                    headers={**_HEADERS, "Content-Type": "application/json"},
                    timeout=_TIMEOUT,
                )
                total_requests += 1
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.error(f"Jooble API error (query={query.get('keywords', '')}, page={page}): {e}")
                break

            page_jobs = data.get("jobs", [])
            if not page_jobs:
                break

            for item in page_jobs:
                job_id = item.get("id")
                if job_id and job_id in seen_ids:
                    continue
                if job_id:
                    seen_ids.add(job_id)

                job_url = item.get("link", "")
                if not job_url:
                    continue

                # Slug discovery from job URLs
                slug_pair = _try_extract_slug(job_url)
                if slug_pair:
                    _discovered_slugs.append(slug_pair)

                jobs.append({
                    "title": item.get("title", ""),
                    "company": item.get("company", ""),
                    "location": item.get("location", "Remote"),
                    "url": job_url,
                    "source_ats": "jooble",
                    "source_type": "job_board",
                    "description_snippet": (item.get("snippet", "") or "")[:2000],
                    "salary": item.get("salary", ""),
                    "employment_type": item.get("type", ""),
                    "jooble_source": item.get("source", ""),
                })

            total_count = data.get("totalCount", 0)
            if not total_count or page * len(page_jobs) >= total_count or len(page_jobs) < 20:
                break
            page += 1
            time.sleep(1)  # be polite between pages

        time.sleep(1)  # pause between different queries

    log.info(f"  Jooble: {len(jobs)} jobs fetched across {len(_JOOBLE_QUERIES)} queries")
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
    "jooble": scrape_jooble,
}


def _canonical_job_url(url: str) -> str:
    """Remove common tracking parameters while preserving the actual job URL."""
    if not url:
        return ""
    try:
        parts = urlsplit(url.strip())
        if not parts.netloc:
            return url.strip().lower().rstrip("/")
        # Query parameters used for tracking / feed attribution.
        tracking = {
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "source", "src", "ref", "referrer", "feed", "gh_src", "lever-source",
        }
        from urllib.parse import parse_qsl, urlencode
        query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
                 if k.lower() not in tracking]
        return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), urlencode(query), ""))
    except Exception:
        return url.strip().lower().rstrip("/")


def dedupe_job_board_jobs(jobs: list[dict]) -> list[dict]:
    """Deduplicate aggregator results without throwing away source provenance."""
    out = []
    by_key = {}
    for job in jobs:
        url_key = _canonical_job_url(job.get("url", ""))
        title_key = re.sub(r"\W+", " ", str(job.get("title", "")).lower()).strip()
        company_key = re.sub(r"\W+", " ", str(job.get("company", "")).lower()).strip()
        key = url_key or f"{title_key}|{company_key}"
        if key in by_key:
            existing = by_key[key]
            sources = set(existing.get("source_boards", []))
            sources.add(existing.get("source_ats", ""))
            sources.add(job.get("source_ats", ""))
            existing["source_boards"] = sorted(x for x in sources if x)
            # Prefer the richer description/salary/location.
            for field in ("description_snippet", "salary", "location", "employment_type"):
                if len(str(job.get(field, "") or "")) > len(str(existing.get(field, "") or "")):
                    existing[field] = job.get(field, "")
            continue
        job = dict(job)
        job["source_boards"] = [job.get("source_ats", "")] if job.get("source_ats") else []
        by_key[key] = job
        out.append(job)
    return out


def scrape_all_job_boards(dedupe: bool = True, max_workers: int | None = None) -> list[dict]:
    """Pull all configured job boards concurrently, then optionally deduplicate."""
    all_jobs = []
    workers = max_workers or min(len(BOARD_SCRAPERS), 10)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {pool.submit(scraper): name for name, scraper in BOARD_SCRAPERS.items()}
        for future in as_completed(future_map):
            name = future_map[future]
            try:
                jobs = future.result() or []
                all_jobs.extend(jobs)
                log.info("Job board %s: %d jobs", name, len(jobs))
            except Exception as e:
                log.error(f"Job board {name} failed: {e}")

    if dedupe:
        all_jobs = dedupe_job_board_jobs(all_jobs)
    log.info(f"Total from job boards: {len(all_jobs)} unique jobs across {len(BOARD_SCRAPERS)} boards")
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
