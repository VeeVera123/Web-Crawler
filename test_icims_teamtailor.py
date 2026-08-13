"""
Test Runner — iCIMS + Teamtailor ONLY
======================================
Tests location extraction for these two platforms.
Scrapes a handful of companies, shows extracted locations,
and runs the location filter to verify correct filtering.

Usage:
    python test_icims_teamtailor.py
    python test_icims_teamtailor.py --dry-run    # don't write to DB
"""

import argparse
import logging
import os
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

from ats_scrapers import scrape_icims, scrape_teamtailor, enrich_descriptions
from classifier import keyword_classify_role, keyword_classify_location

# ── Test companies ──────────────────────────────────────
# Pick a mix of known iCIMS and Teamtailor companies

ICIMS_SLUGS = [
    "careers-cotiviti",          # US roles (US Nationwide Remote)
    "globalcareers-cotiviti",    # Global roles (India, etc.)
]

TEAMTAILOR_SLUGS = [
    "polestar",              # EV company (Sweden)
    "funnel",                # SaaS (Sweden)
    "truelayer",             # fintech (UK/EU)
    "oda",                   # grocery (Nordics)
    "fishbrain",             # app company
]


def test_platform(platform: str, slugs: list[str], scraper_fn) -> list[dict]:
    """Scrape a platform and show location results."""
    all_jobs = []
    for slug in slugs:
        log.info(f"\n{'='*60}")
        log.info(f"Scraping {platform}: {slug}")
        log.info(f"{'='*60}")
        try:
            jobs = scraper_fn(slug)
            log.info(f"  Found {len(jobs)} raw jobs")

            for j in jobs[:3]:  # Show first 3 per company
                log.info(f"    Title: {j['title'][:60]}")
                log.info(f"    Location: \"{j.get('location', '')}\"")
                log.info(f"    Country: \"{j.get('country', '')}\"")
                log.info(f"    URL: {j.get('url', '')[:80]}")
                log.info(f"    ---")

            all_jobs.extend(jobs)
        except Exception as e:
            log.error(f"  FAILED: {e}")

    return all_jobs


def main():
    parser = argparse.ArgumentParser(description="Test iCIMS + Teamtailor location extraction")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("TEST RUNNER: iCIMS + Teamtailor Location Extraction")
    log.info("=" * 60)

    # ── Phase 1: Scrape ──────────────────────────────────
    log.info("\n\n>>> PHASE 1: SCRAPING iCIMS")
    icims_jobs = test_platform("iCIMS", ICIMS_SLUGS, scrape_icims)

    log.info("\n\n>>> PHASE 1: SCRAPING TEAMTAILOR")
    tt_jobs = test_platform("Teamtailor", TEAMTAILOR_SLUGS, scrape_teamtailor)

    all_jobs = icims_jobs + tt_jobs
    log.info(f"\n\nTotal raw jobs: {len(all_jobs)} ({len(icims_jobs)} iCIMS, {len(tt_jobs)} Teamtailor)")

    # ── Phase 2: Role filter ─────────────────────────────
    log.info("\n\n>>> PHASE 2: ROLE FILTER (keyword only, no AI)")
    csm_jobs = []
    for j in all_jobs:
        result = keyword_classify_role(j["title"])
        if result == "include":
            csm_jobs.append(j)

    log.info(f"CSM/AM roles found: {len(csm_jobs)}")
    for j in csm_jobs:
        log.info(f"  [{j['source_ats']}] {j['title'][:50]} | loc=\"{j.get('location', '')}\"")

    # ── Phase 3: Enrich descriptions (this is where location extraction happens) ──
    log.info("\n\n>>> PHASE 3: ENRICHMENT (description + location extraction)")
    csm_jobs = enrich_descriptions(csm_jobs)

    log.info("\nAfter enrichment:")
    for j in csm_jobs:
        loc = j.get("location", "")
        desc_len = len(j.get("description_snippet", ""))
        log.info(f"  [{j['source_ats']}] {j['title'][:50]}")
        log.info(f"    Location: \"{loc}\"")
        log.info(f"    Description: {desc_len} chars")
        log.info(f"    URL: {j.get('url', '')[:80]}")

    # ── Phase 4: Location filter (keyword only, no AI) ──
    log.info("\n\n>>> PHASE 4: LOCATION FILTER (keyword only)")
    matched = []
    rejected = []
    unsure = []

    for j in csm_jobs:
        result = keyword_classify_location(j)
        loc = j.get("location", "")
        if result == "match":
            matched.append(j)
            log.info(f"  MATCH:    {j['title'][:40]} | \"{loc}\"")
        elif result == "no_match":
            rejected.append(j)
            log.info(f"  REJECTED: {j['title'][:40]} | \"{loc}\"")
        else:
            unsure.append(j)
            log.info(f"  UNSURE:   {j['title'][:40]} | \"{loc}\"")

    log.info(f"\n\nSUMMARY:")
    log.info(f"  Total scraped:     {len(all_jobs)}")
    log.info(f"  CSM/AM roles:      {len(csm_jobs)}")
    log.info(f"  Location MATCH:    {len(matched)} (would be added to DB)")
    log.info(f"  Location REJECTED: {len(rejected)} (filtered out — saved LLM cost!)")
    log.info(f"  Location UNSURE:   {len(unsure)} (would go to AI classifier)")

    if rejected:
        log.info(f"\n  Rejected jobs (correctly filtered out):")
        for j in rejected:
            log.info(f"    [{j['source_ats']}] {j['title'][:50]} → \"{j.get('location', '')}\"")

    if unsure:
        log.info(f"\n  Unsure jobs (would be sent to AI):")
        for j in unsure:
            log.info(f"    [{j['source_ats']}] {j['title'][:50]} → \"{j.get('location', '')}\"")


if __name__ == "__main__":
    main()
