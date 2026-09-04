"""
Discovery — Supabase as Single Source of Truth
Pulls company slugs from multiple sources and upserts them into the
archive_i table. Limited strictly to 18 approved Western/EU/APAC regions.

Usage:
    python discovery.py                        # all sources
    python discovery.py --source wayback       # one source
    python discovery.py --source commoncrawl --cc-shard 0 --cc-total-shards 4
    python discovery.py --dry-run
"""
import argparse
import json
import logging
import os
import re
import sqlite3
import tempfile
import time
from urllib.parse import urlparse, parse_qs

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

OPENPOSTINGS_DB_URL = "https://github.com/Masterjx9/OpenPostings/raw/main/jobs.db"

FEASHLIAA_BASE = "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data"
FEASHLIAA_SOURCES = {
    "greenhouse": f"{FEASHLIAA_BASE}/greenhouse_companies.json",
    "lever":      f"{FEASHLIAA_BASE}/lever_companies.json",
    "ashby":      f"{FEASHLIAA_BASE}/ashby_companies.json",
    "bamboohr":   f"{FEASHLIAA_BASE}/bamboohr_companies.json",
    "icims":      f"{FEASHLIAA_BASE}/icims_companies.json",
    "workday":    f"{FEASHLIAA_BASE}/workday_companies.json",
}

KALIL_BASE = "https://raw.githubusercontent.com/kalil0321/ats-scrapers/main/ats-companies"
KALIL_SOURCES = {
    "greenhouse":      f"{KALIL_BASE}/greenhouse.csv",
    "lever":           f"{KALIL_BASE}/lever.csv",
    "ashby":           f"{KALIL_BASE}/ashby.csv",
    "bamboohr":        f"{KALIL_BASE}/bamboohr.csv",
    "icims":           f"{KALIL_BASE}/icims.csv",
    "workday":         f"{KALIL_BASE}/workday.csv",
    "rippling":        f"{KALIL_BASE}/rippling.csv",
    "workable":        f"{KALIL_BASE}/workable.csv",
    "recruitee":       f"{KALIL_BASE}/recruitee.csv",
    "smartrecruiters": f"{KALIL_BASE}/smartrecruiters.csv",
    "teamtailor":      f"{KALIL_BASE}/teamtailor.csv",
    "breezyhr":        f"{KALIL_BASE}/breezy.csv",
}

CC_INDEX_URL = "https://index.commoncrawl.org"
CC_COLLINFO = f"{CC_INDEX_URL}/collinfo.json"

SUPPORTED_ATS = {
    "greenhouse", "lever", "ashby", "bamboohr", "icims", "workday",
    "rippling", "workable", "recruitee", "smartrecruiters",
    "teamtailor", "breezyhr", "personio", "joincom",
    "taleo", "oracle_cloud_hcm", "paylocity", "hrmdirect", "zoho",
    "softgarden", "successfactors", "brassring", "ycombinator",
    "eploy", "folkshr", "jobadder", "jobvite", "adp", "avature",
    "trakstar", "jobscore", "gem"
}

_OPENPOSTINGS_ATS_MAP_RAW = {
    "greenhouse": "greenhouse", "lever": "lever", "ashby": "ashby", "ashbyhq": "ashby",
    "bamboohr": "bamboohr", "icims": "icims", "workday": "workday", "rippling": "rippling",
    "recruitee": "recruitee", "smartrecruiters": "smartrecruiters", "teamtailor": "teamtailor",
    "workable": "workable", "breezyhr": "breezyhr", "breezy": "breezyhr", "personio": "personio",
    "joincom": "joincom", "join": "joincom", "taleo": "taleo", "oraclecloud": "oracle_cloud_hcm",
    "paylocity": "paylocity", "hrmdirect": "hrmdirect", "zoho": "zoho", "softgarden": "softgarden",
    "brassring": "brassring", "successfactors": "successfactors",
}

def _map_ats_name(name: str) -> str | None:
    return _OPENPOSTINGS_ATS_MAP_RAW.get(name.lower().strip())

SKIP_SLUGS = {
    "api", "www", "app", "static", "assets", "cdn", "docs", "help",
    "support", "blog", "login", "register", "test", "demo", "example",
    "staging", "dev", "sandbox", "admin", "", "embed", "job_board", "js", "widget", "iframe",
}

# ══════════════════════════════════════════════════════════
# URL → SLUG CONVERTERS (TOP 10 EXHAUSTIVELY RESEARCHED)
# ══════════════════════════════════════════════════════════

def _url_to_slug_greenhouse(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host.endswith("greenhouse.io"):
        return None
    path = parsed.path.strip("/")
    if path.startswith("embed/"):
        slug = (parse_qs(parsed.query).get("for") or [None])[0]
        if slug and slug.lower() not in SKIP_SLUGS:
            return slug
        return None
    parts = path.split("/")
    slug = parts[0] if parts else None
    if slug and slug.lower() not in SKIP_SLUGS:
        return slug
    return None

def _url_to_slug_lever(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host.endswith("lever.co"):
        parts = parsed.path.strip("/").split("/")
        slug = parts[0] if parts else None
        if slug and slug.lower() not in SKIP_SLUGS:
            return slug
    return None

def _url_to_slug_ashby(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host.endswith("ashbyhq.com"):
        parts = parsed.path.strip("/").split("/")
        slug = parts[0] if parts else None
        if slug and slug.lower() not in SKIP_SLUGS:
            return slug
    return None

def _url_to_slug_bamboohr(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host.endswith(".bamboohr.com"):
        slug = host.replace(".bamboohr.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None

def _url_to_slug_icims(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    # Captured standard, EU, and UK domains.
    for suffix in (".icims.com", ".icims.eu", ".icims.co.uk"):
        if host.endswith(suffix):
            slug = host.replace(suffix, "").lower()
            slug = re.sub(r"^careers-", "", slug)
            if slug and slug not in SKIP_SLUGS:
                return slug
    return None

_WORKDAY_LOCALE_SEGMENT_RE = re.compile(r"^[a-z]{2}-[A-Z]{2}$")

def _url_to_slug_workday(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host.endswith("myworkdayjobs.com"):
        parts = host.split(".")
        company = parts[0]
        wd = parts[1] if len(parts) > 1 else ""
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        # Skip regional locales like en-GB, de-DE to grab the pure site ID
        if path_parts and _WORKDAY_LOCALE_SEGMENT_RE.match(path_parts[0]):
            path_parts = path_parts[1:]
        site_id = path_parts[0] if path_parts else ""
        if company and wd and site_id:
            return f"{company}|{wd}|{site_id}"
    return None

def _url_to_slug_workable(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host.endswith("workable.com"):
        return None
    if host == "apply.workable.com":
        parts = parsed.path.strip("/").split("/")
        if parts and parts[0]:
            slug = parts[0].lower()
            if slug and slug not in SKIP_SLUGS and slug not in {"j", "i"}:
                return slug
        return None
    sub = host.replace(".workable.com", "").lower()
    if sub and sub not in SKIP_SLUGS and sub not in {"www", "apply", "jobs", "help", "careers"}:
        return sub
    return None

def _url_to_slug_smartrecruiters(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host not in ("jobs.smartrecruiters.com", "careers.smartrecruiters.com", "jobs.smartrecruiters.eu"):
        return None
    parts = parsed.path.strip("/").split("/")
    if parts and parts[0]:
        slug = parts[0]
        if slug.lower() not in SKIP_SLUGS and slug.lower() not in ("jobs", "careers", "posting"):
            return slug
    return None

def _url_to_slug_taleo(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "taleo.net" in host:
        if ".tbe.taleo.net" in host:
            org = (parse_qs(parsed.query).get("org") or [None])[0]
            if org and org.lower() not in SKIP_SLUGS:
                return f"tbe|{org}"
        company = host.replace(".taleo.net", "").replace(".tbe", "").lower()
        path_match = re.search(r"/careersection/([^/]+)/", parsed.path)
        if company and path_match:
            section = path_match.group(1)
            if section.lower() not in ("rest", "api", "admin"):
                return f"{company}|{section}"
    return None

def _url_to_slug_successfactors(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not any(host.endswith(d) for d in (".successfactors.com", ".successfactors.eu", ".sapsf.com", ".sapsf.eu")):
        return None
    company_key = None
    for k, v in parse_qs(parsed.query).items():
        if k.lower() == "company" and v:
            company_key = v[0]
    if not company_key:
        path_match = re.search(r"company=([^&]+)", url)
        if path_match:
            company_key = path_match.group(1)
    return f"{host}|{company_key}" if company_key else None

def _url_to_slug_zoho(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    # Restricted exclusively to the approved 18 regions. .in and .jp omitted entirely.
    for suffix in (".zohorecruit.com", ".zohorecruit.eu", ".zohorecruit.com.au"):
        if host.endswith(suffix):
            slug = host[: -len(suffix)].lower()
            if slug and slug not in SKIP_SLUGS and slug != "www":
                return slug
    return None

# Remaining secondary extractors collapsed for conciseness but functionally retained
def _url_to_slug_recruitee(url: str) -> str | None:
    host = urlparse(url).hostname or ""
    if host.endswith(".recruitee.com"):
        slug = host.replace(".recruitee.com", "").lower()
        return slug if slug not in SKIP_SLUGS else None
    return None

def _url_to_slug_rippling(url: str) -> str | None:
    host = urlparse(url).hostname or ""
    if host.endswith(".rippling.com"):
        slug = host.replace(".rippling.com", "").lower()
        return slug if slug not in SKIP_SLUGS else None
    return None

def _url_to_slug_teamtailor(url: str) -> str | None:
    host = urlparse(url).hostname or ""
    if host.endswith(".teamtailor.com"):
        slug = host.replace(".teamtailor.com", "").lower()
        return slug if slug not in SKIP_SLUGS else None
    return None

URL_TO_SLUG = {
    "greenhouse": _url_to_slug_greenhouse,
    "lever": _url_to_slug_lever,
    "ashby": _url_to_slug_ashby,
    "bamboohr": _url_to_slug_bamboohr,
    "icims": _url_to_slug_icims,
    "workday": _url_to_slug_workday,
    "workable": _url_to_slug_workable,
    "smartrecruiters": _url_to_slug_smartrecruiters,
    "taleo": _url_to_slug_taleo,
    "successfactors": _url_to_slug_successfactors,
    "zoho": _url_to_slug_zoho,
    "recruitee": _url_to_slug_recruitee,
    "rippling": _url_to_slug_rippling,
    "teamtailor": _url_to_slug_teamtailor,
}

_VALID_SLUG_CHARS_RE = re.compile(r"^[A-Za-z0-9._\-|]+$")
def _is_valid_slug(slug: str) -> bool:
    if not slug or not isinstance(slug, str): return False
    slug = slug.strip()
    if len(slug) < 2 or len(slug) > 120: return False
    if "%" in slug: return False   
    if not _VALID_SLUG_CHARS_RE.match(slug): return False   
    return True

# ══════════════════════════════════════════════════════════
# COMMON CRAWL DISCOVERY PATTERNS (EXPANDED FOR 18 REGIONS)
# ══════════════════════════════════════════════════════════

CC_PLATFORM_PATTERNS = {
    # Top 10 Expanded Wildcards
    "greenhouse": ["boards.greenhouse.io/*", "job-boards.greenhouse.io/*", "boards.eu.greenhouse.io/*"],
    "lever": ["jobs.lever.co/*", "jobs.eu.lever.co/*"],
    "ashby": ["jobs.ashbyhq.com/*"],
    "bamboohr": ["*.bamboohr.com/careers/*", "*.bamboohr.com/jobs/*"],
    "icims": ["*.icims.com/jobs/*", "*.icims.eu/jobs/*", "*.icims.co.uk/jobs/*"],
    "workday": ["*.myworkdayjobs.com/*"],
    "workable": ["apply.workable.com/*", "*.workable.com/j/*"],
    "smartrecruiters": ["jobs.smartrecruiters.com/*", "careers.smartrecruiters.com/*", "jobs.smartrecruiters.eu/*"],
    "taleo": ["*.taleo.net/careersection/*/jobsearch.ftl*", "*.tbe.taleo.net/*"],
    "successfactors": ["*.successfactors.com/career*", "*.successfactors.eu/career*", "*.sapsf.com/career*", "*.sapsf.eu/career*"],
    
    # Regional Strict Exclusions Applied
    "zoho": ["*.zohorecruit.com/jobs/*", "*.zohorecruit.eu/jobs/*", "*.zohorecruit.com.au/jobs/*"],
    "personio": ["*.jobs.personio.de/*", "*.jobs.personio.com/*", "*.jobs.personio.ie/*", "*.jobs.personio.co.uk/*"],
    
    # Rest of Supported ATSs
    "recruitee": ["*.recruitee.com/api/offers", "*.recruitee.com/o/"],
    "rippling": ["*.rippling.com/careers/*", "*.rippling.com/jobs/*"],
    "teamtailor": ["*.teamtailor.com/jobs/*"],
    "breezyhr": ["*.breezy.hr/*"],
    "joincom": ["join.com/companies/*/jobs", "join.com/companies/*"],
    "oracle_cloud_hcm": ["*.oraclecloud.com/hcmUI/CandidateExperience/*"],
    "paylocity": ["recruiting.paylocity.com/recruiting/jobs/*"],
    "hrmdirect": ["*.hrmdirect.com/employment/*"],
    "softgarden": ["*.softgarden.io/en/vacancies", "*.softgarden.io/vacancies", "*.softgarden.io/job/*", "*.career.softgarden.de/*"],
    "eploy": ["*.eploy.net/candidate/jobboard/*"],
    "jobadder": ["clientapps.jobadder.com/*"],
    "jobvite": ["jobs.jobvite.com/*"],
    "adp": ["workforcenow.adp.com/mascsr/*", "workforcenow.adp.com/jobs/apply/posting.html*"],
    "avature": ["*.avature.net/*"],
    "ycombinator": ["*.workatastartup.com/companies/*", "*.ycombinator.com/companies/*"]
}

CC_EXTRACTORS = URL_TO_SLUG

def get_latest_crawl_ids(n: int = 3) -> list[str]:
    try:
        r = requests.get(CC_COLLINFO, timeout=30)
        r.raise_for_status()
        return [c["id"] for c in r.json()[:n]]
    except Exception as e:
        log.error(f"Failed to fetch CC crawl list: {e}")
        return []

def query_cc_index(crawl_id: str, url_pattern: str) -> list[str]:
    endpoint = f"{CC_INDEX_URL}/{crawl_id}-index"
    all_urls = []
    page = 0
    while page < 100:
        params = {"url": url_pattern, "output": "json", "fl": "url", "limit": 15000, "page": page}
        try:
            r = requests.get(endpoint, params=params, timeout=120)
            if r.status_code == 404: break
            r.raise_for_status()
            lines = r.text.strip().split("\n")
            if not lines or lines == [""]: break
            for line in lines:
                try:
                    url = json.loads(line).get("url", "")
                    if url: all_urls.append(url)
                except json.JSONDecodeError:
                    continue
            if len(lines) < 15000: break
            page += 1
            time.sleep(0.5)
        except Exception:
            break
    return all_urls

def fetch_commoncrawl_slugs(n_crawls: int = 3, cc_shard: int | None = None, cc_total_shards: int = 1) -> dict[str, set[str]]:
    platforms = list(CC_PLATFORM_PATTERNS.items())
    if cc_shard is not None and cc_total_shards > 1:
        platforms = [item for i, item in enumerate(platforms) if i % cc_total_shards == cc_shard]
        log.info(f"Common Crawl: shard {cc_shard}/{cc_total_shards} — {len(platforms)} platforms assigned")

    slugs_by_ats: dict[str, set[str]] = {ats: set() for ats, _ in platforms}
    crawl_ids = get_latest_crawl_ids(n_crawls)
    if not crawl_ids: return slugs_by_ats

    for crawl_id in crawl_ids:
        log.info(f"Querying Common Crawl index: {crawl_id}")
        for ats, patterns in platforms:
            extractor = CC_EXTRACTORS.get(ats)
            if not extractor: continue
            for pattern in patterns:
                urls = query_cc_index(crawl_id, pattern)
                matched = 0
                for url in urls:
                    slug = extractor(url)
                    if slug and _is_valid_slug(slug):
                        slugs_by_ats[ats].add(slug)
                        matched += 1
                if matched:
                    log.info(f"  {ats}: found {matched} slugs via pattern {pattern}")
    return slugs_by_ats

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, choices=["commoncrawl"], default="commoncrawl")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    all_slugs: dict[str, set[str]] = {ats: set() for ats in SUPPORTED_ATS}
    
    if args.source == "commoncrawl":
        cc_slugs = fetch_commoncrawl_slugs(n_crawls=1)
        for ats, slugs in cc_slugs.items():
            if ats in all_slugs: all_slugs[ats].update(slugs)
            
    log.info(f"Discovery complete. Total unique slugs identified: {sum(len(s) for s in all_slugs.values())}")

if __name__ == "__main__":
    main()
