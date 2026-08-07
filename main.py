"""
ATS Global Scanner — Main Orchestrator
=======================================
Scans all known company boards across Rippling, Greenhouse, Lever, and Ashby.
Filters for CSM/Account Management roles hiring globally or in Africa.
Pushes matches to Notion.
"""

import os
import logging
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


def load_slugs() -> list[tuple[str, str]]:
    """
    Load (ats, slug) pairs from text files in slugs/ directory.
    Each file is named after the ATS (rippling.txt, greenhouse.txt, etc.)
    with one slug per line.
    """
    pairs = []
    for filename in os.listdir(SLUGS_DIR):
        if not filename.endswith(".txt"):
            continue
        ats = filename.replace(".txt", "")
        filepath = os.path.join(SLUGS_DIR, filename)
        with open(filepath, "r") as f:
            for line in f:
                slug = line.strip()
                if slug and not slug.startswith("#"):
                    pairs.append((ats, slug))
    log.info(f"Loaded {len(pairs)} boards across {len(set(a for a, _ in pairs))} ATS platforms")
    return pairs


def scrape_all(boards: list[tuple[str, str]]) -> list[dict]:
    """Scrape all boards in parallel. Returns flat list of jobs."""
    all_jobs = []
    failed = 0

    def _scrape(ats_slug):
        ats, slug = ats_slug
        return ats, slug, scrape_board(ats, slug)

    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as pool:
        futures = {pool.submit(_scrape, b): b for b in boards}
        for future in as_completed(futures):
            try:
                ats, slug, jobs = future.result()
                if jobs:
                    all_jobs.extend(jobs)
                    log.info(f"  {ats}/{slug}: {len(jobs)} jobs")
                else:
                    log.debug(f"  {ats}/{slug}: 0 jobs")
            except Exception as e:
                failed += 1
                log.debug(f"  Error: {e}")

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
            if label in ("match", "uncertain"):
                matched.append(job)
                matched_confidences.append(label if label == "match" else "uncertain")

    log.info(f"After location filter: {len(matched)} global/Africa jobs")
    return matched, matched_confidences


def main():
    log.info("=" * 60)
    log.info("ATS GLOBAL SCANNER — starting")
    log.info("=" * 60)

    # 1. Load slug lists
    boards = load_slugs()
    if not boards:
        log.error("No boards to scrape. Check slugs/ directory.")
        return

    # 2. Scrape all boards
    log.info(f"\n📡 Scraping {len(boards)} boards...")
    all_jobs = scrape_all(boards)
    if not all_jobs:
        log.info("No jobs found across any board.")
        return

    # 3. Filter for CSM/AM roles
    log.info(f"\n🎯 Filtering for CSM/AM roles...")
    csm_jobs = filter_roles(all_jobs)
    if not csm_jobs:
        log.info("No CSM/AM roles found.")
        return

    # 4. Filter for Africa/Global locations
    log.info(f"\n🌍 Filtering for Africa/Global eligibility...")
    global_jobs, confidences = filter_locations(csm_jobs)
    if not global_jobs:
        log.info("No global/Africa-eligible CSM/AM roles found.")
        return

    # 5. Push to Notion
    log.info(f"\n📝 Pushing {len(global_jobs)} jobs to Notion...")
    added = add_jobs_batch(global_jobs, confidences)

    log.info(f"\n✅ Done! {added} new jobs added to Notion.")
    log.info(f"   Pipeline: {len(all_jobs)} scraped → {len(csm_jobs)} CSM/AM → {len(global_jobs)} global/Africa → {added} new")


if __name__ == "__main__":
    main()
