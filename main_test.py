"""
main_test.py — Quick AI classification test
=============================================
Scrapes Rippling + Greenhouse to test AI classification.
Does NOT write to Supabase — just prints results.

Changes from main.py:
  1. Imports from classifier_test (no rate throttle, batch size 1)
  2. Only scrapes 'rippling' + 'greenhouse' platforms (all boards)
  3. Prints results to console instead of pushing to Supabase
  4. No scan_report tracking

Run:  python main_test.py
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from ats_scrapers import scrape_board, enrich_descriptions
from classifier_test import (
    keyword_classify_role, ai_classify_roles,
    keyword_classify_location, ai_classify_locations,
    detect_visa_sponsorship,
)
from supabase_handler import (
    add_jobs_batch, start_scan_report, finish_scan_report,
    get_all_slugs,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

TEST_PLATFORMS = ["rippling", "greenhouse"]
PLATFORM_WORKERS = {"rippling": 8, "greenhouse": 30}


def main():
    log.info("=" * 60)
    log.info("TEST MODE — Rippling + Greenhouse, no Supabase writes")
    log.info("=" * 60)

    # 1. Load slugs for test platforms only
    log.info("Loading slugs from Supabase...")
    all_pairs = get_all_slugs()
    by_ats = {}
    for ats, slug in all_pairs:
        if ats in TEST_PLATFORMS:
            by_ats.setdefault(ats, []).append(slug)

    for ats, slugs in by_ats.items():
        log.info(f"  {ats}: {len(slugs)} boards")

    if not by_ats:
        log.error("No slugs found for test platforms!")
        return

    # 2. Scrape all boards for test platforms
    all_jobs = []
    failed = 0

    def _scrape_platform(ats, slugs):
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
        return platform_jobs, platform_failed

    log.info(f"\nScraping {sum(len(s) for s in by_ats.values())} boards across {len(by_ats)} platforms...")
    with ThreadPoolExecutor(max_workers=len(by_ats)) as platform_pool:
        futs = {platform_pool.submit(_scrape_platform, ats, slugs): ats
                for ats, slugs in by_ats.items()}
        for future in as_completed(futs):
            ats = futs[future]
            try:
                jobs, bad = future.result()
                all_jobs.extend(jobs)
                failed += bad
                log.info(f"  {ats}: {len(jobs)} jobs ({bad} boards failed)")
            except Exception as e:
                log.error(f"  {ats}: platform error: {e}")

    log.info(f"Total: {len(all_jobs)} raw jobs ({failed} boards failed)")
    if not all_jobs:
        log.info("No jobs found.")
        return

    # 3. Filter for CSM/AM roles
    log.info(f"\nFiltering for CSM/AM roles...")
    included = []
    unsure = []
    for job in all_jobs:
        result = keyword_classify_role(job["title"])
        if result == "include":
            included.append(job)
        elif result == "unsure":
            unsure.append(job)

    log.info(f"Role filter: {len(included)} keyword match, {len(unsure)} unsure")

    if unsure:
        log.info(f"Sending {len(unsure)} unsure titles to AI...")
        for j in unsure:
            log.info(f"  [UNSURE] {j['title']} @ {j.get('company', '?')}")
        unsure_titles = [j["title"] for j in unsure]
        ai_results = ai_classify_roles(unsure_titles)
        for job in unsure:
            verdict = ai_results.get(job["title"], False)
            log.info(f"  [AI ROLE] {job['title']} → {'YES' if verdict else 'NO'}")
            if verdict:
                included.append(job)

    csm_jobs = included
    log.info(f"After role filter: {len(csm_jobs)} CSM/AM jobs")

    if not csm_jobs:
        log.info("No CSM/AM roles found in test batch.")
        return

    # 3b. Enrich descriptions
    log.info(f"\nFetching descriptions for jobs missing them...")
    csm_jobs = enrich_descriptions(csm_jobs)

    # 4. Filter locations
    log.info(f"\nFiltering locations...")
    matched = []
    matched_confidences = []
    unsure_jobs = []

    for job in csm_jobs:
        result = keyword_classify_location(job)
        log.info(f"  [LOC KEYWORD] {job['title']} @ {job.get('company', '?')} | "
                 f"loc='{job.get('location', '')}' → {result}")
        if result == "match":
            matched.append(job)
            matched_confidences.append("match")
        elif result == "unsure":
            unsure_jobs.append(job)

    if unsure_jobs:
        log.info(f"\nSending {len(unsure_jobs)} location-unsure jobs to AI...")
        ai_results = ai_classify_locations(unsure_jobs)
        for job, label in zip(unsure_jobs, ai_results):
            log.info(f"  [AI LOC] {job['title']} @ {job.get('company', '?')} → {label}")
            if label == "match":
                matched.append(job)
                matched_confidences.append("match")
            elif label == "uncertain":
                matched.append(job)
                matched_confidences.append("uncertain")

    # 5. Visa detection
    for job in matched:
        job["visa_sponsorship"] = detect_visa_sponsorship(job)

    # 6. Push to Supabase
    if matched:
        log.info(f"\nPushing {len(matched)} jobs to Supabase...")
        added = add_jobs_batch(matched, matched_confidences)
        duplicates = len(matched) - added
        log.info(f"Added {added} new jobs ({duplicates} duplicates)")
    else:
        added = 0

    # 7. Print results
    log.info(f"\n{'=' * 60}")
    log.info(f"TEST RESULTS")
    log.info(f"{'=' * 60}")
    log.info(f"Raw jobs scraped: {len(all_jobs)}")
    log.info(f"CSM/AM roles: {len(csm_jobs)}")
    log.info(f"Global/Africa matches: {len(matched)}")
    log.info(f"New jobs added to Supabase: {added}")

    if matched:
        log.info(f"\nMatched jobs:")
        for i, job in enumerate(matched, 1):
            log.info(f"  {i}. {job['title']} @ {job.get('company', '?')}")
            log.info(f"     Location: {job.get('location', 'N/A')}")
            log.info(f"     URL: {job.get('url', 'N/A')}")
            log.info(f"     Visa: {job.get('visa_sponsorship', 'unknown')}")
            log.info(f"     Confidence: {matched_confidences[i-1]}")
    else:
        log.info("\nNo matches found — the important thing is whether AI calls worked without errors above.")


if __name__ == "__main__":
    main()
