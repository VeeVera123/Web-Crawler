"""
Slug Discovery via Common Crawl Index API
==========================================
Queries the free Common Crawl CDX index to find company slugs for
Workable, Recruitee, and SmartRecruiters by searching for crawled
career-page URLs. Merges results with existing local slug files.
Also backs up discovered slugs to Supabase slug_registry.

Usage:
    python discover_slugs.py                  # query latest 3 crawls
    python discover_slugs.py --crawls 5       # query latest 5 crawls
    python discover_slugs.py --dry-run        # print slugs without saving

Can also run as a weekly GitHub Action to keep slug lists fresh.
"""

import argparse
import json
import logging
import os
import re
import time
from urllib.parse import urlparse

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SLUGS_DIR = os.path.join(os.path.dirname(__file__), "slugs")
CC_INDEX_URL = "https://index.commoncrawl.org"
CC_COLLINFO = f"{CC_INDEX_URL}/collinfo.json"

# URL patterns → slug extraction rules
# Each entry: (CC query pattern, slug extractor function, output filename)
PLATFORM_QUERIES = {
    "workable": {
        "patterns": ["apply.workable.com/*"],
        "extract": "_extract_workable_slug",
        "file": "workable.txt",
    },
    "recruitee": {
        "patterns": ["*.recruitee.com/api/offers*", "*.recruitee.com/o/*"],
        "extract": "_extract_recruitee_slug",
        "file": "recruitee.txt",
    },
    "smartrecruiters": {
        "patterns": [
            "jobs.smartrecruiters.com/*",
            "careers.smartrecruiters.com/*",
        ],
        "extract": "_extract_smartrecruiters_slug",
        "file": "smartrecruiters.txt",
    },
}

# Slugs to skip (test/demo/invalid)
SKIP_SLUGS = {
    "api", "www", "app", "static", "assets", "cdn", "docs", "help",
    "support", "blog", "login", "register", "signup", "sign-up",
    "favicon.ico", "robots.txt", "sitemap.xml", "",
}


def get_latest_crawl_ids(n: int = 3) -> list[str]:
    """Fetch the N most recent Common Crawl index IDs."""
    try:
        r = requests.get(CC_COLLINFO, timeout=30)
        r.raise_for_status()
        collections = r.json()
        return [c["id"] for c in collections[:n]]
    except Exception as e:
        log.error(f"Failed to fetch CC collection info: {e}")
        return []


def query_cc_index(crawl_id: str, url_pattern: str, max_pages: int = 50) -> list[str]:
    """Query a single CC index for URLs matching the pattern.
    Uses pagination to get all results."""
    endpoint = f"{CC_INDEX_URL}/{crawl_id}-index"
    all_urls = []
    page = 0

    while page < max_pages:
        params = {
            "url": url_pattern,
            "output": "json",
            "fl": "url",
            "limit": 10000,
            "page": page,
        }

        try:
            r = requests.get(endpoint, params=params, timeout=60)
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

            # If we got fewer results than the limit, we've reached the end
            if len(lines) < 10000:
                break

            page += 1
            time.sleep(0.5)  # Be respectful to CC servers

        except requests.exceptions.Timeout:
            log.warning(f"Timeout querying {crawl_id} for {url_pattern}, page {page}")
            break
        except Exception as e:
            log.warning(f"Error querying {crawl_id} for {url_pattern}: {e}")
            break

    return all_urls


# ── Slug extraction functions ──────────────────────────

def _extract_workable_slug(url: str) -> str | None:
    """Extract slug from apply.workable.com/{slug}/... URLs."""
    parsed = urlparse(url)
    if parsed.hostname != "apply.workable.com":
        return None
    parts = parsed.path.strip("/").split("/")
    if not parts or not parts[0]:
        return None
    slug = parts[0].lower()
    # Skip API paths and static assets
    if slug in ("api", "j", "embed", "static", "assets", "widget"):
        return None
    if slug in SKIP_SLUGS:
        return None
    # Valid slugs are alphanumeric with hyphens
    if not re.match(r"^[a-z0-9][a-z0-9\-]*[a-z0-9]$", slug) and len(slug) > 1:
        return None
    return slug


def _extract_recruitee_slug(url: str) -> str | None:
    """Extract slug from {slug}.recruitee.com URLs."""
    parsed = urlparse(url)
    hostname = parsed.hostname or ""
    if not hostname.endswith(".recruitee.com"):
        return None
    slug = hostname.replace(".recruitee.com", "").lower()
    if slug in SKIP_SLUGS or slug in ("www", "app", "api", "support", "help"):
        return None
    if not slug or len(slug) < 2:
        return None
    return slug


def _extract_smartrecruiters_slug(url: str) -> str | None:
    """Extract company identifier from jobs/careers.smartrecruiters.com/{id}/... URLs."""
    parsed = urlparse(url)
    hostname = parsed.hostname or ""
    if hostname not in ("jobs.smartrecruiters.com", "careers.smartrecruiters.com"):
        return None
    parts = parsed.path.strip("/").split("/")
    if not parts or not parts[0]:
        return None
    slug = parts[0]
    # SmartRecruiters identifiers are case-sensitive, don't lowercase
    if slug.lower() in SKIP_SLUGS:
        return None
    if not re.match(r"^[a-zA-Z0-9][a-zA-Z0-9\-_.]*$", slug):
        return None
    return slug


EXTRACTORS = {
    "_extract_workable_slug": _extract_workable_slug,
    "_extract_recruitee_slug": _extract_recruitee_slug,
    "_extract_smartrecruiters_slug": _extract_smartrecruiters_slug,
}


def load_existing_slugs(filepath: str) -> set[str]:
    """Load existing slugs from a text file."""
    slugs = set()
    if not os.path.exists(filepath):
        return slugs
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                slugs.add(line)
    return slugs


def save_slugs(filepath: str, slugs: set[str], platform: str):
    """Save slugs to a text file, sorted, with header comment."""
    sorted_slugs = sorted(slugs, key=lambda s: s.lower())
    with open(filepath, "w") as f:
        f.write(f"# {platform} company slugs\n")
        f.write(f"# Auto-discovered via Common Crawl + manually curated\n")
        f.write(f"# Total: {len(sorted_slugs)} companies\n")
        for slug in sorted_slugs:
            f.write(f"{slug}\n")


def discover_platform(
    platform: str,
    config: dict,
    crawl_ids: list[str],
    dry_run: bool = False,
) -> set[str]:
    """Discover slugs for one platform across multiple CC crawls."""
    extractor = EXTRACTORS[config["extract"]]
    filepath = os.path.join(SLUGS_DIR, config["file"])

    # Start with existing slugs
    existing = load_existing_slugs(filepath)
    log.info(f"  {platform}: {len(existing)} existing slugs")

    all_new_slugs = set()

    for crawl_id in crawl_ids:
        for pattern in config["patterns"]:
            log.info(f"  Querying {crawl_id} for {pattern}...")
            urls = query_cc_index(crawl_id, pattern)
            log.info(f"    Got {len(urls)} URLs")

            for url in urls:
                slug = extractor(url)
                if slug and slug not in existing:
                    all_new_slugs.add(slug)

            # Rate limit between queries
            time.sleep(1)

    log.info(f"  {platform}: found {len(all_new_slugs)} NEW slugs")

    if all_new_slugs and not dry_run:
        merged = existing | all_new_slugs
        save_slugs(filepath, merged, platform)
        log.info(f"  {platform}: saved {len(merged)} total slugs to {config['file']}")

    return all_new_slugs


def backup_to_supabase(platform: str, slugs: set[str]):
    """Back up discovered slugs to Supabase slug_registry."""
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_KEY", "")
    if not supabase_url or not supabase_key:
        log.info("  Supabase credentials not set, skipping backup")
        return

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }

    slug_list = list(slugs)
    chunk_size = 500
    total = 0

    for i in range(0, len(slug_list), chunk_size):
        chunk = slug_list[i:i + chunk_size]
        rows = [{"ats": platform, "slug": s, "source": "commoncrawl"} for s in chunk]
        try:
            r = requests.post(
                f"{supabase_url}/rest/v1/slug_registry",
                headers=headers, json=rows, timeout=60,
                params={"on_conflict": "ats,slug"},
            )
            r.raise_for_status()
            total += len(chunk)
        except Exception as e:
            log.error(f"  Supabase backup failed: {e}")

    log.info(f"  {platform}: backed up {total} slugs to Supabase")


def main():
    parser = argparse.ArgumentParser(description="Discover ATS company slugs via Common Crawl")
    parser.add_argument("--crawls", type=int, default=3, help="Number of recent CC crawls to query (default: 3)")
    parser.add_argument("--dry-run", action="store_true", help="Print discovered slugs without saving")
    parser.add_argument("--platform", choices=list(PLATFORM_QUERIES.keys()), help="Only discover for one platform")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SLUG DISCOVERY — Common Crawl Index")
    log.info("=" * 60)

    # Get latest crawl IDs
    crawl_ids = get_latest_crawl_ids(args.crawls)
    if not crawl_ids:
        log.error("Could not fetch CC crawl list. Exiting.")
        return
    log.info(f"Querying {len(crawl_ids)} crawls: {', '.join(crawl_ids)}")

    os.makedirs(SLUGS_DIR, exist_ok=True)

    # Discover for each platform
    platforms = {args.platform: PLATFORM_QUERIES[args.platform]} if args.platform else PLATFORM_QUERIES
    total_new = 0

    for platform, config in platforms.items():
        log.info(f"\n--- {platform.upper()} ---")
        new_slugs = discover_platform(platform, config, crawl_ids, args.dry_run)
        total_new += len(new_slugs)

        if args.dry_run and new_slugs:
            for slug in sorted(new_slugs):
                print(f"  {slug}")
        elif new_slugs:
            # Back up all slugs (existing + new) to Supabase
            filepath = os.path.join(SLUGS_DIR, config["file"])
            all_slugs = load_existing_slugs(filepath)
            backup_to_supabase(platform, all_slugs)

    log.info(f"\nDone! Discovered {total_new} new slugs total.")


if __name__ == "__main__":
    main()
