"""
ATS Global Scanner — Main Orchestrator
=======================================
Scans 20,000+ company boards across 10 ATS platforms:
Greenhouse, Lever, Ashby, BambooHR, iCIMS, Workday, Rippling,
Workable, Recruitee, SmartRecruiters.
Filters for CSM/Account Management roles hiring globally or in Africa.
Pushes matches to Notion.
"""

import os
import json
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import CONCURRENT_WORKERS, SLUGS_DIR
from ats_scrapers import scrape_board
from classifier import (
    keyword_classify_role, ai_classify_roles,
    keyword_classify_location, ai_classify_locations,
)
from notion_handler import add_jobs_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# GitHub repo with 20k+ company slugs (updated regularly via Common Crawl)
SLUG_REPO_BASE = "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data"
SLUG_SOURCES = {
    "greenhouse": f"{SLUG_REPO_BASE}/greenhouse_companies.json",
    "lever":      f"{SLUG_REPO_BASE}/lever_companies.json",
    "ashby":      f"{SLUG_REPO_BASE}/ashby_companies.json",
    "bamboohr":   f"{SLUG_REPO_BASE}/bamboohr_companies.json",
    "icims":      f"{SLUG_REPO_BASE}/icims_companies.json",
    "workday":    f"{SLUG_REPO_BASE}/workday_companies.json",
}

# Per-platform concurrency limits (respect rate limits)
PLATFORM_WORKERS = {
    "greenhouse": 30,
    "lever": 30,
    "ashby": 5,
    "bamboohr": 10,
    "icims": 30,
    "workday": 15,
    "rippling": 8,
    "workable": 10,
    "recruitee": 10,
    "smartrecruiters": 10,
}


def _fetch_remote_slugs(ats: str, url: str) -> list[str]:
    """Download a company slug list from the GitHub repo."""
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        slugs = r.json()
        if isinstance(slugs, list):
            return slugs
    except Exception as e:
        log.warning(f"Failed to fetch {ats} slugs from GitHub: {e}")
    return []


def _load_local_slugs(ats: str) -> list[str]:
    """Fallback: load from local slugs/ text files."""
    filepath = os.path.join(SLUGS_DIR, f"{ats}.txt")
    if not os.path.exists(filepath):
        return []
    slugs = []
    with open(filepath, "r") as f:
        for line in f:
            slug = line.strip()
            if slug and not slug.startswith("#"):
                slugs.append(slug)
    return slugs


def load_slugs() -> list[tuple[str, str]]:
    """
    Load (ats, slug) pairs — fetches from GitHub repo for each ATS,
    falls back to local slugs/ text files if GitHub is unreachable.
    Rippling is local-only (not in the GitHub repo).
    """
    pairs = []

    # Fetch from GitHub repo
    for ats, url in SLUG_SOURCES.items():
        slugs = _fetch_remote_slugs(ats, url)
        if slugs:
            log.info(f"  {ats}: {len(slugs)} companies (GitHub)")
        else:
            slugs = _load_local_slugs(ats)
            if slugs:
                log.info(f"  {ats}: {len(slugs)} companies (local fallback)")
        for slug in slugs:
            pairs.append((ats, slug))

    # Local-only platforms (no GitHub repo source yet)
    for local_ats in ["rippling", "workable", "recruitee", "smartrecruiters"]:
        local_slugs = _load_local_slugs(local_ats)
        if local_slugs:
            log.info(f"  {local_ats}: {len(local_slugs)} companies (local)")
            for slug in local_slugs:
                pairs.append((local_ats, slug))

    ats_counts = {}
    for ats, _ in pairs:
        ats_counts[ats] = ats_counts.get(ats, 0) + 1
    log.info(f"Total: {len(pairs)} boards across {len(ats_counts)} ATS platforms")
    return pairs


def scrape_all(boards: list[tuple[str, str]]) -> list[dict]:
    """Scrape all boards in parallel, grouped by ATS platform."""
    all_jobs = []
    failed = 0

    # Group boards by ATS to apply per-platform concurrency
    by_ats = {}
    for ats, slug in boards:
        by_ats.setdefault(ats, []).append(slug)

    def _scrape_platform(ats: str, slugs: list[str]) -> tuple[int, int, list[dict]]:
        """Scrape one ATS platform with appropriate concurrency."""
        workers = PLATFORM_WORKERS.get(ats, 8)
        platform_jobs = []
        platform_failed = 0

        def _do_scrape(slug):
            return slug, scrape_board(ats, slug)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_do_scrape, s): s for s in slugs}
            for future in as_completed(futures):
                try:
                    slug, jobs = future.result()
                    if jobs:
                        platform_jobs.extend(jobs)
                except Exception:
                    platform_failed += 1

        return len(slugs) - platform_failed, platform_failed, platform_jobs

    # Run each platform concurrently (up to 4 platforms at once)
    with ThreadPoolExecutor(max_workers=4) as platform_pool:
        platform_futures = {}
        for ats, slugs in by_ats.items():
            f = platform_pool.submit(_scrape_platform, ats, slugs)
            platform_futures[f] = ats

        for future in as_completed(platform_futures):
            ats = platform_futures[future]
            try:
                ok, bad, jobs = future.result()
                all_jobs.extend(jobs)
                failed += bad
                log.info(f"  {ats}: {len(jobs)} jobs from {ok} active boards ({bad} failed)")
            except Exception as e:
                log.error(f"  {ats}: platform error: {e}")

    log.info(f"Total raw jobs scraped: {len(all_jobs)} ({failed} boards failed)")
    return all_jobs


def filter_roles(jobs: list[dict]) -> list[dict]:
    """Stage 1+2: Keep only CSM/AM roles."""
    included = []
    unsure = []

    for job in jobs:
        result = keyword_classify_role(job["title"])
        if result == "include":
            included.append(job)
        elif result == "unsure":
            unsure.append(job)
        # "exclude" → drop

    log.info(f"Role filter: {len(included)} keyword match, {len(unsure)} unsure → sending to AI")

    # AI classify the unsure batch
    if unsure:
        unsure_titles = [j["title"] for j in unsure]
        ai_results = ai_classify_roles(unsure_titles)
        for job in unsure:
            if ai_results.get(job["title"], False):
                included.append(job)

    log.info(f"After role filter: {len(included)} CSM/AM jobs")
    return included


def filter_locations(jobs: list[dict]) -> tuple[list[dict], list[str]]:
    """Stage 3+4: Keep only global/Africa-eligible jobs."""
    matched = []
    matched_confidences = []
    unsure_jobs = []

    for job in jobs:
        result = keyword_classify_location(job)
        if result == "match":
            matched.append(job)
            matched_confidences.append("match")
        elif result == "unsure":
            unsure_jobs.append(job)
        # "no_match" → drop

    log.info(f"Location filter: {len(matched)} keyword match, {len(unsure_jobs)} unsure → sending to AI")

    # AI classify the unsure batch
    if unsure_jobs:
        ai_results = ai_classify_locations(unsure_jobs)
        for job, label in zip(unsure_jobs, ai_results):
            if label == "match":
                matched.append(job)
                matched_confidences.append("match")
            elif label == "uncertain":
                matched.append(job)
                matched_confidences.append("uncertain")

    log.info(f"After location filter: {len(matched)} global/Africa jobs")
    return matched, matched_confidences


def main():
    log.info("=" * 60)
    log.info("ATS GLOBAL SCANNER — starting")
    log.info("=" * 60)

    # 1. Load slug lists
    log.info("Loading company slugs...")
    boards = load_slugs()
    if not boards:
        log.error("No boards to scrape.")
        return

    # 2. Scrape all boards
    log.info(f"\nScraping {len(boards)} boards across 10 ATS platforms...")
    all_jobs = scrape_all(boards)
    if not all_jobs:
        log.info("No jobs found across any board.")
        return

    # 3. Filter for CSM/AM roles
    log.info(f"\nFiltering for CSM/AM roles...")
    csm_jobs = filter_roles(all_jobs)
    if not csm_jobs:
        log.info("No CSM/AM roles found.")
        return

    # 4. Filter for Africa/Global locations
    log.info(f"\nFiltering for Africa/Global eligibility...")
    global_jobs, confidences = filter_locations(csm_jobs)
    if not global_jobs:
        log.info("No global/Africa-eligible CSM/AM roles found.")
        return

    # 5. Push to Notion
    log.info(f"\nPushing {len(global_jobs)} jobs to Notion...")
    added = add_jobs_batch(global_jobs, confidences)

    log.info(f"\nDone! {added} new jobs added to Notion.")
    log.info(f"   Pipeline: {len(all_jobs)} scraped -> {len(csm_jobs)} CSM/AM -> {len(global_jobs)} global/Africa -> {added} new")


if __name__ == "__main__":
    main()
