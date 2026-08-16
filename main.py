"""
ATS Global Scanner — Main Orchestrator
=======================================
Scans 87,000+ company boards across 20 ATS platforms,
plus 5 remote job boards (RemoteOK, Remotive, Himalayas, Arbeitnow, Jobicy).

Reads slugs from Supabase slug_registry (single source of truth,
enriched weekly by enrich_slugs.py from Feashliaa + kalil0321 + OpenPostings + Common Crawl).
Job boards are scraped directly via free public JSON APIs (no slugs needed).
Filters for CSM/Account Management roles hiring globally or in Africa.
Pushes matches to Supabase (PostgreSQL).

LLM provider is set via LLM_PROVIDER env var (see SWITCHING_GUIDE.md).
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import LLM_PROVIDER, LOCATION_PROVIDERS
from ats_scrapers import scrape_board, enrich_descriptions, enrich_application_questions
from job_board_scrapers import scrape_all_job_boards, get_discovered_slugs
from classifier import (
    keyword_classify_role, ai_classify_roles,
    keyword_classify_location, ai_classify_locations,
    detect_visa_sponsorship,
)
from supabase_handler import (
    add_jobs_batch, start_scan_report, finish_scan_report,
    get_all_slugs, cleanup_stale_jobs, populate_slug_registry,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

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
    "teamtailor": 10,
    "breezyhr": 10,
    "applytojob": 10,
    "personio": 10,
    "joincom": 5,  # JOIN.com pageSize max 5, needs slug→ID resolution
    # Newly enabled platforms:
    "taleo": 10,
    "oracle_cloud_hcm": 10,
    "paylocity": 15,
    "hrmdirect": 15,
    "zoho": 5,  # 1.7MB pages, keep low
}


def load_slugs() -> list[tuple[str, str]]:
    """
    Load (ats, slug) pairs from Supabase slug_registry.
    Supabase is the single source of truth — enriched weekly
    by enrich_slugs.py (Feashliaa + kalil0321 + OpenPostings + Common Crawl).
    """
    pairs = get_all_slugs()

    if not pairs:
        log.warning("No slugs found in Supabase slug_registry!")
        log.warning("Run enrich_slugs.py first to populate the registry.")
        return []

    ats_counts: dict[str, int] = {}
    for ats, _ in pairs:
        ats_counts[ats] = ats_counts.get(ats, 0) + 1

    for ats in sorted(ats_counts, key=lambda a: -ats_counts[a]):
        log.info(f"  {ats}: {ats_counts[ats]} companies")
    log.info(f"Total: {len(pairs)} boards across {len(ats_counts)} ATS platforms")

    return pairs


def scrape_all(boards: list[tuple[str, str]]) -> tuple[list[dict], int, int]:
    """Scrape all boards in parallel, grouped by ATS platform.
    Returns (jobs, boards_ok, boards_failed)."""
    all_jobs = []
    total_ok = 0
    total_failed = 0

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
                except Exception as e:
                    platform_failed += 1
                    if platform_failed <= 3:  # log first 3 errors per platform
                        log.error(f"  {ats} scrape error: {e}")

        return len(slugs) - platform_failed, platform_failed, platform_jobs

    # Run all platforms concurrently — each has its own per-platform worker limit
    with ThreadPoolExecutor(max_workers=len(by_ats)) as platform_pool:
        platform_futures = {}
        for ats, slugs in by_ats.items():
            f = platform_pool.submit(_scrape_platform, ats, slugs)
            platform_futures[f] = ats

        for future in as_completed(platform_futures):
            ats = platform_futures[future]
            try:
                ok, bad, jobs = future.result()
                all_jobs.extend(jobs)
                total_ok += ok
                total_failed += bad
                log.info(f"  {ats}: {len(jobs)} jobs from {ok} active boards ({bad} failed)")
            except Exception as e:
                log.error(f"  {ats}: platform error: {e}")

    log.info(f"Total raw jobs scraped: {len(all_jobs)} ({total_failed} boards failed)")
    return all_jobs, total_ok, total_failed


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

    log.info(f"Role filter: {len(included)} keyword match, {len(unsure)} unsure → sending to AI")

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
            job["clearance"] = "regex"
            matched.append(job)
            matched_confidences.append("match")
        elif result == "unsure":
            unsure_jobs.append(job)

    log.info(f"Location filter: {len(matched)} keyword match, {len(unsure_jobs)} unsure → sending to AI")

    if unsure_jobs:
        ai_results = ai_classify_locations(unsure_jobs)
        for i, (job, label) in enumerate(zip(unsure_jobs, ai_results)):
            # Determine which provider classified this job (round-robin assignment)
            provider_name = LOCATION_PROVIDERS[i % len(LOCATION_PROVIDERS)]["name"]
            if label == "match":
                job["clearance"] = provider_name
                matched.append(job)
                matched_confidences.append("match")
            elif label == "uncertain":
                job["clearance"] = provider_name
                matched.append(job)
                matched_confidences.append("uncertain")
            # "no_match" → drop; AI failure defaults to "uncertain" (included)

    log.info(f"After location filter: {len(matched)} global/Africa jobs")
    return matched, matched_confidences


def main():
    log.info("=" * 60)
    log.info("ATS GLOBAL SCANNER — starting")
    log.info("=" * 60)

    report_id = start_scan_report()

    try:
        # 1. Load slugs from Supabase
        log.info("Loading company slugs from Supabase...")
        boards = load_slugs()
        if not boards:
            log.error("No boards to scrape.")
            if report_id:
                finish_scan_report(report_id, status="failed")
            return

        # 2. Scrape all ATS boards
        log.info(f"\nScraping {len(boards)} boards across {len(set(a for a,_ in boards))} ATS platforms...")
        all_jobs, boards_ok, boards_failed = scrape_all(boards)

        # 2b. Scrape job boards (9 boards: RemoteOK, Remotive, Himalayas, Arbeitnow,
        #     Jobicy, WeWorkRemotely, Working Nomads, FreeHire, Jooble)
        log.info(f"\nScraping remote job boards...")
        board_jobs = scrape_all_job_boards()
        all_jobs.extend(board_jobs)

        # 2c. Save any new company slugs discovered from aggregator job URLs
        discovered = get_discovered_slugs()
        if discovered:
            log.info(f"\nDiscovered {len(discovered)} new slugs from job board URLs → saving to slug_registry")
            populate_slug_registry(discovered, source="job_board_discovery")

        if not all_jobs:
            log.info("No jobs found across any source.")
            if report_id:
                finish_scan_report(
                    report_id,
                    boards_scanned=boards_ok,
                    boards_failed=boards_failed,
                )
            return

        # 3. Filter for CSM/AM roles
        log.info(f"\nFiltering for CSM/AM roles...")
        csm_jobs = filter_roles(all_jobs)
        if not csm_jobs:
            log.info("No CSM/AM roles found.")
            if report_id:
                finish_scan_report(
                    report_id,
                    boards_scanned=boards_ok,
                    boards_failed=boards_failed,
                    total_jobs_raw=len(all_jobs),
                )
            return

        # 3b. Enrich descriptions for platforms that lack them
        log.info(f"\nFetching descriptions for jobs missing them...")
        csm_jobs = enrich_descriptions(csm_jobs)

        # 3c. Fetch application questions for location-"unsure" jobs, across
        # all 20 ATS platforms (multi-tier fallback — see ats_scrapers.py).
        # Work authorization questions help the AI detect country-restricted roles
        log.info(f"\nEnriching application questions across all ATS platforms...")
        csm_jobs = enrich_application_questions(csm_jobs)

        # 4. Filter for Africa/Global locations
        log.info(f"\nFiltering for Africa/Global eligibility...")
        global_jobs, confidences = filter_locations(csm_jobs)
        if not global_jobs:
            log.info("No global/Africa-eligible CSM/AM roles found.")
            if report_id:
                finish_scan_report(
                    report_id,
                    boards_scanned=boards_ok,
                    boards_failed=boards_failed,
                    total_jobs_raw=len(all_jobs),
                    csm_roles=len(csm_jobs),
                )
            return

        # 5. Detect visa sponsorship from descriptions (before discarding them)
        for job in global_jobs:
            job["visa_sponsorship"] = detect_visa_sponsorship(job)

        # 6. Push to Supabase
        log.info(f"\nPushing {len(global_jobs)} jobs to Supabase...")
        added = add_jobs_batch(global_jobs, confidences)

        # 7. Finalize report
        duplicates = len(global_jobs) - added
        if report_id:
            finish_scan_report(
                report_id,
                boards_scanned=boards_ok,
                boards_failed=boards_failed,
                total_jobs_raw=len(all_jobs),
                csm_roles=len(csm_jobs),
                global_jobs=len(global_jobs),
                new_jobs_added=added,
                duplicates=duplicates,
            )

        log.info(f"\nDone! {added} new jobs added to Supabase.")
        log.info(f"   Pipeline: {len(all_jobs)} scraped -> {len(csm_jobs)} CSM/AM -> {len(global_jobs)} global -> {added} new")

        # 8. Cleanup stale jobs (inactive after 30 days, delete after 60)
        log.info(f"\nCleaning up stale jobs...")
        cleanup_stale_jobs(inactive_days=30, delete_days=60)

    except Exception as e:
        log.error(f"Scanner failed: {e}")
        if report_id:
            finish_scan_report(report_id, status="failed")
        raise


if __name__ == "__main__":
    main()
