"""
ATS Global Scanner — Main Orchestrator (Async I/O Version)
Scans 87,000+ company boards at maximum speed using httpx and asyncio.
"""

import logging
import asyncio
import httpx

from config import LLM_PROVIDER, LOCATION_PROVIDER, REQUEST_TIMEOUT
from ats_scrapers import scrape_board, enrich_descriptions, enrich_application_questions
# Ensure job_board_scrapers module also supports async, or adjust accordingly.
from job_board_scrapers import scrape_all_job_boards 
from classifier import (
    keyword_classify_role, ai_classify_roles,
    keyword_classify_location, ai_classify_locations,
    detect_visa_sponsorship,
)
from supabase_handler import (
    add_jobs_batch, start_scan_report, finish_scan_report,
    get_all_slugs, cleanup_stale_jobs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

PLATFORM_WORKERS = {
    "greenhouse": 30, "lever": 30, "ashby": 5, "bamboohr": 10,
    "icims": 30, "workday": 15, "rippling": 8, "workable": 10,
    "recruitee": 10, "smartrecruiters": 10, "teamtailor": 10,
    "breezyhr": 10, "applytojob": 10, "personio": 10,
    "joincom": 5, "taleo": 10, "oracle_cloud_hcm": 10,
    "paylocity": 15, "hrmdirect": 15, "zoho": 5,
}

def load_slugs() -> list[tuple[str, str]]:
    pairs = get_all_slugs()
    if not pairs: return []
    return pairs

async def _scrape_platform(ats: str, slugs: list[str], client: httpx.AsyncClient) -> tuple[int, int, list[dict]]:
    workers = PLATFORM_WORKERS.get(ats, 8)
    sem = asyncio.Semaphore(workers)
    platform_failed = 0
    platform_jobs = []

    async def _do_scrape(slug):
        nonlocal platform_failed
        try:
            async with sem:
                jobs = await scrape_board(ats, slug, client)
                return slug, jobs
        except Exception as e:
            platform_failed += 1
            if platform_failed <= 3:
                log.error(f"  {ats} scrape error: {e}")
            return slug, None

    tasks = [_do_scrape(s) for s in slugs]
    results = await asyncio.gather(*tasks)

    for slug, jobs in results:
        if jobs:
            platform_jobs.extend(jobs)

    return len(slugs) - platform_failed, platform_failed, platform_jobs

async def scrape_all(boards: list[tuple[str, str]], client: httpx.AsyncClient) -> tuple[list[dict], int, int]:
    all_jobs, total_ok, total_failed = [], 0, 0
    by_ats = {}
    for ats, slug in boards:
        by_ats.setdefault(ats, []).append(slug)

    tasks = [_scrape_platform(ats, slugs, client) for ats, slugs in by_ats.items()]
    results = await asyncio.gather(*tasks)

    for (ok, bad, jobs), ats in zip(results, by_ats.keys()):
        all_jobs.extend(jobs)
        total_ok += ok
        total_failed += bad
        log.info(f"  {ats}: {len(jobs)} jobs from {ok} active boards ({bad} failed)")

    return all_jobs, total_ok, total_failed

async def filter_roles(jobs: list[dict]) -> list[dict]:
    included, unsure = [], []
    for job in jobs:
        result = keyword_classify_role(job["title"])
        if result == "include": included.append(job)
        elif result == "unsure": unsure.append(job)

    if unsure:
        unsure_titles = [j["title"] for j in unsure]
        ai_results = await ai_classify_roles(unsure_titles)
        for job in unsure:
            if ai_results.get(job["title"], False):
                included.append(job)
    return included

async def filter_locations(jobs: list[dict]) -> tuple[list[dict], list[str]]:
    matched, matched_confidences, unsure_jobs = [], [], []
    for job in jobs:
        result = keyword_classify_location(job)
        if result == "match":
            job["clearance"] = "regex"
            matched.append(job); matched_confidences.append("match")
        elif result == "unsure":
            unsure_jobs.append(job)

    if unsure_jobs:
        ai_results = await ai_classify_locations(unsure_jobs)
        for job, label in zip(unsure_jobs, ai_results):
            if label in ("match", "uncertain"):
                job["clearance"] = LOCATION_PROVIDER["name"]
                matched.append(job); matched_confidences.append(label)
    return matched, matched_confidences


async def main():
    log.info("=" * 60)
    log.info("ATS GLOBAL SCANNER — starting (Async)")
    log.info("=" * 60)

    report_id = await asyncio.to_thread(start_scan_report)

    try:
        boards = await asyncio.to_thread(load_slugs)
        if not boards:
            if report_id: await asyncio.to_thread(finish_scan_report, report_id, status="failed")
            return

        # ── Setup Connection Pooling & Scrape ──
        limits = httpx.Limits(max_connections=500, max_keepalive_connections=150)
        timeout = httpx.Timeout(REQUEST_TIMEOUT)
        
        async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
            all_jobs, boards_ok, boards_failed = await scrape_all(boards, client)
            
            try:
                board_jobs = await scrape_all_job_boards(client) 
                all_jobs.extend(board_jobs)
            except TypeError:
                board_jobs = await asyncio.to_thread(scrape_all_job_boards)
                all_jobs.extend(board_jobs)

            if not all_jobs:
                if report_id: await asyncio.to_thread(finish_scan_report, report_id, boards_scanned=boards_ok, boards_failed=boards_failed)
                return

            csm_jobs = await filter_roles(all_jobs)
            if not csm_jobs:
                if report_id: await asyncio.to_thread(finish_scan_report, report_id, boards_scanned=boards_ok, boards_failed=boards_failed, total_jobs_raw=len(all_jobs))
                return

            csm_jobs = await enrich_descriptions(csm_jobs, client)
            csm_jobs = await enrich_application_questions(csm_jobs, client)

        # ── Local processing / DB pushes ──
        global_jobs, confidences = await filter_locations(csm_jobs)
        if not global_jobs:
            if report_id: await asyncio.to_thread(finish_scan_report, report_id, boards_scanned=boards_ok, boards_failed=boards_failed, total_jobs_raw=len(all_jobs), csm_roles=len(csm_jobs))
            return

        for job in global_jobs:
            job["visa_sponsorship"] = detect_visa_sponsorship(job)

        added = await asyncio.to_thread(add_jobs_batch, global_jobs, confidences)
        
        if report_id:
            await asyncio.to_thread(
                finish_scan_report, report_id, boards_scanned=boards_ok, boards_failed=boards_failed,
                total_jobs_raw=len(all_jobs), csm_roles=len(csm_jobs), global_jobs=len(global_jobs),
                new_jobs_added=added, duplicates=len(global_jobs) - added
            )

        await asyncio.to_thread(cleanup_stale_jobs, inactive_days=30, delete_days=60)
        log.info(f"\nDone! Pipeline: {len(all_jobs)} scraped -> {len(csm_jobs)} CSM/AM -> {len(global_jobs)} global -> {added} new")

    except Exception as e:
        log.error(f"Scanner failed: {e}")
        if report_id: await asyncio.to_thread(finish_scan_report, report_id, status="failed")
        raise

if __name__ == "__main__":
    asyncio.run(main())
