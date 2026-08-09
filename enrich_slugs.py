"""
Slug Enrichment — Supabase as Single Source of Truth
=====================================================
Pulls company slugs from multiple sources and upserts them
into the Supabase slug_registry table.

Sources:
  1. OpenPostings jobs.db (110k+ companies across 80+ ATSs)
  2. Common Crawl index (ongoing discovery for 9 platforms)

Runs weekly (Sunday) via GitHub Actions. The daily scanner
reads from Supabase slug_registry — no local .txt files needed.

Usage:
    python enrich_slugs.py                    # full enrichment
    python enrich_slugs.py --source openpostings  # OpenPostings only
    python enrich_slugs.py --source commoncrawl   # Common Crawl only
    python enrich_slugs.py --dry-run          # count without writing
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

# OpenPostings raw download (SQLite database)
OPENPOSTINGS_DB_URL = (
    "https://github.com/Masterjx9/OpenPostings/raw/main/jobs.db"
)

# Common Crawl
CC_INDEX_URL = "https://index.commoncrawl.org"
CC_COLLINFO = f"{CC_INDEX_URL}/collinfo.json"

# ATS platforms we have scrapers for — only import these
SUPPORTED_ATS = {
    "greenhouse", "lever", "ashby", "bamboohr", "icims", "workday",
    "rippling", "workable", "recruitee", "smartrecruiters",
    "taleo", "oracle_cloud_hcm", "brassring", "teamtailor",
    "successfactors",
}

# Map OpenPostings ATS names → our ATS keys
OPENPOSTINGS_ATS_MAP = {
    "Greenhouse": "greenhouse",
    "greenhouse": "greenhouse",
    "Lever": "lever",
    "lever": "lever",
    "Ashby": "ashby",
    "ashby": "ashby",
    "BambooHR": "bamboohr",
    "bamboohr": "bamboohr",
    "iCIMS": "icims",
    "icims": "icims",
    "Workday": "workday",
    "workday": "workday",
    "Rippling": "rippling",
    "rippling": "rippling",
    "Recruitee": "recruitee",
    "recruitee": "recruitee",
    "smartrecruiters": "smartrecruiters",
    "SmartRecruiters": "smartrecruiters",
    "Taleo": "taleo",
    "taleo": "taleo",
    "Oracle Cloud": "oracle_cloud_hcm",
    "oracle cloud": "oracle_cloud_hcm",
    "BrassRing": "brassring",
    "brassring": "brassring",
    "Teamtailor": "teamtailor",
    "teamtailor": "teamtailor",
    "SAP HR Cloud": "successfactors",
    "sap hr cloud": "successfactors",
}

# Slugs to skip
SKIP_SLUGS = {
    "api", "www", "app", "static", "assets", "cdn", "docs", "help",
    "support", "blog", "login", "register", "test", "demo", "example",
    "staging", "dev", "sandbox", "admin", "",
}


# ══════════════════════════════════════════════════════════
# URL → SLUG CONVERTERS (OpenPostings stores full URLs)
# ══════════════════════════════════════════════════════════

def _url_to_slug_greenhouse(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "greenhouse.io" in host:
        parts = parsed.path.strip("/").split("/")
        slug = parts[0] if parts else None
        if slug and slug.lower() not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_lever(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "lever.co" in host:
        parts = parsed.path.strip("/").split("/")
        slug = parts[0] if parts else None
        if slug and slug.lower() not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_ashby(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "ashbyhq.com" in host:
        parts = parsed.path.strip("/").split("/")
        slug = parts[0] if parts else None
        if slug and slug.lower() not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_bamboohr(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "bamboohr.com" in host:
        slug = host.replace(".bamboohr.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None


def _url_to_slug_icims(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "icims.com" in host:
        # Pattern: careers-{slug}.icims.com or {slug}.icims.com
        slug = host.replace(".icims.com", "").lower()
        slug = re.sub(r"^careers-", "", slug)
        if slug and slug not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_workday(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "myworkdayjobs.com" in host:
        # Pattern: {company}.wd{N}.myworkdayjobs.com/{site_id}
        parts = host.split(".")
        company = parts[0]
        wd = parts[1] if len(parts) > 1 else ""
        path_parts = parsed.path.strip("/").split("/")
        site_id = path_parts[0] if path_parts and path_parts[0] else ""
        if company and wd and site_id:
            return f"{company}|{wd}|{site_id}"
    return None


def _url_to_slug_rippling(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "rippling.com" in host:
        slug = host.replace(".rippling.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug not in ("www", "app", "ats"):
            return slug
    return None


def _url_to_slug_workable(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "workable.com" in host:
        parts = parsed.path.strip("/").split("/")
        # apply.workable.com/{slug}
        if host == "apply.workable.com" and parts:
            slug = parts[0].lower()
            if slug and slug not in SKIP_SLUGS:
                return slug
    return None


def _url_to_slug_recruitee(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "recruitee.com" in host:
        slug = host.replace(".recruitee.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None


def _url_to_slug_smartrecruiters(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "smartrecruiters.com" in host:
        parts = parsed.path.strip("/").split("/")
        if parts and parts[0]:
            slug = parts[0]
            if slug.lower() not in SKIP_SLUGS:
                return slug
    return None


def _url_to_slug_taleo(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "taleo.net" in host:
        company = host.replace(".taleo.net", "").lower()
        path_match = re.search(r"/careersection/([^/]+)/", parsed.path)
        if company and path_match:
            section = path_match.group(1)
            if section.lower() not in ("rest", "api", "admin"):
                return f"{company}|{section}"
    return None


def _url_to_slug_oracle_cloud(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "oraclecloud.com" in host:
        tenant = host.split(".")[0].lower()
        site_match = re.search(r"/sites/([^/]+)", parsed.path)
        if tenant and site_match:
            return f"{tenant}|{site_match.group(1)}"
    return None


def _url_to_slug_brassring(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "brassring.com" in host:
        qs = parse_qs(parsed.query)
        pid = None
        sid = None
        for k, v in qs.items():
            if k.lower() == "partnerid" and v:
                pid = v[0]
            elif k.lower() == "siteid" and v:
                sid = v[0]
        if pid and sid and pid.isdigit() and sid.isdigit():
            return f"{pid}|{sid}"
    return None


def _url_to_slug_teamtailor(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "teamtailor.com" in host:
        slug = host.replace(".teamtailor.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug not in ("www", "app"):
            return slug
    return None


def _url_to_slug_successfactors(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    sf_domains = (".successfactors.com", ".successfactors.eu", ".sapsf.com")
    if any(host.endswith(d) for d in sf_domains):
        qs = parse_qs(parsed.query)
        company_key = None
        for k, v in qs.items():
            if k.lower() == "company" and v:
                company_key = v[0]
        if company_key:
            return f"{host}|{company_key}"
    return None


URL_TO_SLUG = {
    "greenhouse": _url_to_slug_greenhouse,
    "lever": _url_to_slug_lever,
    "ashby": _url_to_slug_ashby,
    "bamboohr": _url_to_slug_bamboohr,
    "icims": _url_to_slug_icims,
    "workday": _url_to_slug_workday,
    "rippling": _url_to_slug_rippling,
    "workable": _url_to_slug_workable,
    "recruitee": _url_to_slug_recruitee,
    "smartrecruiters": _url_to_slug_smartrecruiters,
    "taleo": _url_to_slug_taleo,
    "oracle_cloud_hcm": _url_to_slug_oracle_cloud,
    "brassring": _url_to_slug_brassring,
    "teamtailor": _url_to_slug_teamtailor,
    "successfactors": _url_to_slug_successfactors,
}


# ══════════════════════════════════════════════════════════
# SOURCE 1: OpenPostings
# ══════════════════════════════════════════════════════════

def fetch_openpostings_slugs() -> dict[str, set[str]]:
    """Download OpenPostings jobs.db and extract company slugs
    for platforms we support."""
    log.info("Downloading OpenPostings jobs.db...")
    slugs_by_ats: dict[str, set[str]] = {ats: set() for ats in SUPPORTED_ATS}
    skipped_ats = {}

    try:
        r = requests.get(OPENPOSTINGS_DB_URL, timeout=120, stream=True)
        r.raise_for_status()
    except Exception as e:
        log.error(f"Failed to download OpenPostings DB: {e}")
        return slugs_by_ats

    # Write to temp file and open as SQLite
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name
        for chunk in r.iter_content(chunk_size=8192):
            tmp.write(chunk)

    try:
        conn = sqlite3.connect(tmp_path)
        cursor = conn.execute(
            "SELECT company_name, url_string, ATS_name FROM companies"
        )
        total = 0
        matched = 0

        for company_name, url_string, ats_name in cursor:
            total += 1
            our_ats = OPENPOSTINGS_ATS_MAP.get(ats_name)
            if not our_ats:
                # Track unmapped ATSs for logging
                skipped_ats[ats_name] = skipped_ats.get(ats_name, 0) + 1
                continue

            converter = URL_TO_SLUG.get(our_ats)
            if not converter:
                continue

            slug = converter(url_string)
            if slug:
                slugs_by_ats[our_ats].add(slug)
                matched += 1

        conn.close()
        log.info(f"OpenPostings: {total} total companies, "
                 f"{matched} matched to our {len(SUPPORTED_ATS)} platforms")

        # Log unmapped ATSs (for future expansion)
        if skipped_ats:
            top_skipped = sorted(skipped_ats.items(), key=lambda x: -x[1])[:10]
            log.info(f"Top unmapped ATSs: {', '.join(f'{k}({v})' for k, v in top_skipped)}")

        for ats in SUPPORTED_ATS:
            count = len(slugs_by_ats[ats])
            if count:
                log.info(f"  {ats}: {count} companies")

    except Exception as e:
        log.error(f"Failed to parse OpenPostings DB: {e}")
    finally:
        os.unlink(tmp_path)

    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 2: Common Crawl (reused from discover_slugs.py)
# ══════════════════════════════════════════════════════════

CC_PLATFORM_PATTERNS = {
    "workable": ["apply.workable.com/*"],
    "recruitee": ["*.recruitee.com/api/offers*", "*.recruitee.com/o/*"],
    "smartrecruiters": ["jobs.smartrecruiters.com/*", "careers.smartrecruiters.com/*"],
    "rippling": ["*.rippling.com/careers*", "*.rippling.com/jobs*"],
    "taleo": ["*.taleo.net/careersection/*/jobsearch*", "*.taleo.net/careersection/*/jobdetail*"],
    "oracle_cloud_hcm": ["*.oraclecloud.com/hcmUI/CandidateExperience/*/sites/*"],
    "brassring": ["sjobs.brassring.com/TgNewUI/Search/*", "sjobs.brassring.com/TGnewUI/Search/*"],
    "teamtailor": ["*.teamtailor.com/jobs*"],
    "successfactors": ["*.successfactors.com/career*", "*.successfactors.eu/career*", "*.sapsf.com/career*"],
}

# Reuse URL_TO_SLUG converters for Common Crawl extraction
CC_EXTRACTORS = {
    "workable": _url_to_slug_workable,
    "recruitee": _url_to_slug_recruitee,
    "smartrecruiters": _url_to_slug_smartrecruiters,
    "rippling": _url_to_slug_rippling,
    "taleo": _url_to_slug_taleo,
    "oracle_cloud_hcm": _url_to_slug_oracle_cloud,
    "brassring": _url_to_slug_brassring,
    "teamtailor": _url_to_slug_teamtailor,
    "successfactors": _url_to_slug_successfactors,
}


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
        params = {
            "url": url_pattern,
            "output": "json",
            "fl": "url",
            "limit": 15000,
            "page": page,
        }
        try:
            r = requests.get(endpoint, params=params, timeout=120)
            if r.status_code == 404:
                break
            r.raise_for_status()
            lines = r.text.strip().split("\n")
            if not lines or lines == [""]:
                break
            for line in lines:
                try:
                    record = json.loads(line)
                    url = record.get("url", "")
                    if url:
                        all_urls.append(url)
                except json.JSONDecodeError:
                    continue
            if len(lines) < 15000:
                break
            page += 1
            time.sleep(0.5)
        except Exception as e:
            log.warning(f"CC query error ({crawl_id}, {url_pattern}): {e}")
            break

    return all_urls


def fetch_commoncrawl_slugs(n_crawls: int = 3) -> dict[str, set[str]]:
    """Discover slugs from Common Crawl for platforms not well-covered
    by OpenPostings."""
    slugs_by_ats: dict[str, set[str]] = {ats: set() for ats in CC_PLATFORM_PATTERNS}

    crawl_ids = get_latest_crawl_ids(n_crawls)
    if not crawl_ids:
        return slugs_by_ats

    log.info(f"Common Crawl: querying {len(crawl_ids)} crawls")

    for ats, patterns in CC_PLATFORM_PATTERNS.items():
        extractor = CC_EXTRACTORS[ats]
        for crawl_id in crawl_ids:
            for pattern in patterns:
                log.info(f"  Querying {crawl_id} for {pattern}...")
                urls = query_cc_index(crawl_id, pattern)
                log.info(f"    Got {len(urls)} URLs")
                for url in urls:
                    slug = extractor(url)
                    if slug:
                        slugs_by_ats[ats].add(slug)
                time.sleep(1)

        count = len(slugs_by_ats[ats])
        if count:
            log.info(f"  {ats}: {count} companies from Common Crawl")

    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SUPABASE UPSERT
# ══════════════════════════════════════════════════════════

def upsert_to_supabase(slugs_by_ats: dict[str, set[str]], source: str,
                        dry_run: bool = False) -> int:
    """Upsert slugs to Supabase slug_registry. Returns total upserted."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL or SUPABASE_KEY not set")
        return 0

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }

    total = 0
    chunk_size = 500

    for ats, slugs in slugs_by_ats.items():
        if not slugs:
            continue

        slug_list = list(slugs)
        ats_total = 0

        for i in range(0, len(slug_list), chunk_size):
            chunk = slug_list[i:i + chunk_size]
            rows = [{"ats": ats, "slug": s, "source": source} for s in chunk]

            if dry_run:
                ats_total += len(chunk)
                continue

            try:
                r = requests.post(
                    f"{SUPABASE_URL}/rest/v1/slug_registry",
                    headers=headers,
                    json=rows,
                    timeout=60,
                    params={"on_conflict": "ats,slug"},
                )
                r.raise_for_status()
                ats_total += len(chunk)
            except Exception as e:
                log.error(f"Supabase upsert failed for {ats}: {e}")

        if ats_total:
            log.info(f"  {ats}: upserted {ats_total} slugs ({source})")
        total += ats_total

    return total


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Enrich Supabase slug_registry from OpenPostings + Common Crawl"
    )
    parser.add_argument(
        "--source",
        choices=["openpostings", "commoncrawl", "all"],
        default="all",
        help="Which source to pull from (default: all)",
    )
    parser.add_argument(
        "--crawls", type=int, default=3,
        help="Number of Common Crawl archives to query (default: 3)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count slugs without writing to Supabase",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SLUG ENRICHMENT — Supabase as single source of truth")
    log.info("=" * 60)

    grand_total = 0

    # Source 1: OpenPostings
    if args.source in ("openpostings", "all"):
        log.info("\n--- OPENPOSTINGS (110k+ companies) ---")
        op_slugs = fetch_openpostings_slugs()
        op_total = sum(len(s) for s in op_slugs.values())
        log.info(f"OpenPostings total: {op_total} slugs across "
                 f"{sum(1 for s in op_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(op_slugs, source="openpostings",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += op_total

    # Source 2: Common Crawl
    if args.source in ("commoncrawl", "all"):
        log.info("\n--- COMMON CRAWL (ongoing discovery) ---")
        cc_slugs = fetch_commoncrawl_slugs(args.crawls)
        cc_total = sum(len(s) for s in cc_slugs.values())
        log.info(f"Common Crawl total: {cc_total} slugs across "
                 f"{sum(1 for s in cc_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(cc_slugs, source="commoncrawl",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += cc_total

    action = "would upsert" if args.dry_run else "upserted"
    log.info(f"\nDone! {action} {grand_total} total slugs to Supabase.")


if __name__ == "__main__":
    main()
