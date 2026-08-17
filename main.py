"""
ATS Global Scanner — Main Orchestrator
=======================================
Scans 87,000+ company boards across 20 ATS platforms,
plus 9 remote job boards (RemoteOK, Remotive, Himalayas, Arbeitnow, Jobicy,
WeWorkRemotely, Working Nomads, FreeHire, Jooble).

Reads slugs from Supabase slug_registry (single source of truth,
enriched weekly by enrich_slugs.py from Feashliaa + kalil0321 + OpenPostings + Common Crawl).
Job boards are scraped directly via free public JSON APIs (no slugs needed).
Filters for CSM/Account Management roles hiring globally or in Africa.
Pushes matches to Supabase (PostgreSQL).

LLM provider is set via LLM_PROVIDER env var (see SWITCHING_GUIDE.md).

CLI modes (see .github/workflows/daily_scan.yml for how these compose):
  python main.py                                   Full run: all ATS boards + job boards +
                                                     cleanup, in one process. Default for
                                                     manual/local use — unchanged behavior.
  python main.py --shard 0 --total-shards 8         ATS boards only, this shard's 1/8 slice.
                                                     No job boards, no cleanup (see run_finalize()).
  python main.py --job-boards-only                  Job board aggregators only, no ATS boards,
                                                     no cleanup. Runs once per scan (not once per
                                                     shard), as its own CI job.
  python main.py --finalize                         Cleanup only (mark/delete stale jobs). Run
                                                     ONCE, after every shard + the job-boards job
                                                     have finished (gate with `needs:` in CI).
"""

import argparse
import hashlib
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import LLM_PROVIDER, LOCATION_PROVIDERS
from ats_scrapers import scrape_board, enrich_descriptions, enrich_application_questions
from job_board_scrapers import scrape_all_job_boards, get_discovered_slugs
from classifier import (
    keyword_classify_role, ai_classify_roles,
    keyword_classify_location, ai_classify_locations,
    detect_visa_sponsorship,
    _keyword_classify_location_detail,
    PRIORITY_GLOBAL, PRIORITY_AFRICA, PRIORITY_UNSURE,
)
from supabase_handler import (
    add_jobs_batch, start_scan_report, finish_scan_report,
    get_all_slugs, cleanup_stale_jobs, populate_slug_registry,
    SupabaseFetchError,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Per-platform concurrency limits ──────────────────────
# Two categories, tuned differently:
#
#  - SINGLE SHARED API DOMAIN platforms (every company's requests land on
#    the same origin, e.g. api.smartrecruiters.com): the risk is US
#    self-inflicting a rate limit by concentrating load on one endpoint.
#    Kept bounded.
#
#  - PER-COMPANY SUBDOMAIN platforms (e.g. {company}.bamboohr.com): each
#    worker's requests spread across different origins, so higher
#    concurrency is generally safer — but NOT maxed out. Many of these are
#    still multi-tenant SaaS behind a SHARED WAF/CDN (Cloudflare or the
#    vendor's own) that can fingerprint by source IP across an entire zone
#    regardless of subdomain, and a single IP requesting hundreds of
#    DIFFERENT companies' career pages back-to-back is itself a bot
#    signal — no real human does that. Values already proven safe in
#    production (Greenhouse/Lever/iCIMS at 30) are left unchanged; every
#    other value below is a first-time, moderate increase — watch actual
#    429/403 rates in the logs after deploying and raise further only
#    once a batch has run clean.
#
# Paylocity is intentionally in the bounded group despite superficially
# looking subdomain-like: it actually serves every company from a single
# shared domain (recruiting.paylocity.com), differentiated by URL path,
# not by subdomain.
PLATFORM_WORKERS = {
    # ── Single shared API domain: keep bounded ──
    "greenhouse": 30,         # proven in production, unchanged
    "lever": 30,               # proven in production, unchanged
    "ashby": 5,                 # already conservative; Ashby is known stricter
    "rippling": 8,
    "workable": 10,
    "smartrecruiters": 10,
    "joincom": 6,               # pageSize max 5, needs slug→ID resolution
    "paylocity": 15,            # shared domain — do not raise further

    # ── Per-company subdomain: moderate first-time increase ──
    "bamboohr": 18,
    "icims": 30,                # proven in production, unchanged
    "workday": 20,
    "recruitee": 18,
    "teamtailor": 18,
    "breezyhr": 18,
    "applytojob": 18,
    "personio": 18,
    "taleo": 16,                 # legacy platform, slightly more cautious
    "oracle_cloud_hcm": 16,
    "hrmdirect": 18,
    "zoho": 8,                   # raised from 5, but still capped low —
                                  # 1.7MB pages per request is a runner
                                  # memory/bandwidth constraint, not a
                                  # ban-risk one; concurrency here trades
                                  # against the runner's own resources.
    "softgarden": 18,            # per-company subdomain

    # ── New (2026-08) ──
    "eploy": 12,                  # per-company subdomain, unproven at scale — conservative
    "folkshr": 15,                # shared jobs.folksats.app domain, lightweight pages
    "jobadder": 10,                # shared clientapps.jobadder.com domain — be cautious
    "jobvite": 15,                 # shared jobs.jobvite.com domain
    "adp": 10,                     # shared workforcenow.adp.com domain, real JSON API
                                    # but unauthenticated public endpoint — stay modest
    "avature": 8,                  # per-customer subdomain, but templates vary wildly
                                    # and reliability is lower — keep it conservative
}


def _shard_of(ats: str, slug: str, total_shards: int) -> int:
    """Deterministic hash-based shard assignment. Using a stable hash
    (rather than a running index % N) means every platform gets spread
    evenly across all shards regardless of how slug_registry rows happen
    to be ordered/clustered by source — so no shard accidentally ends up
    as "all Workday" with a different completion profile than its peers."""
    h = hashlib.md5(f"{ats}|{slug}".encode()).hexdigest()
    return int(h, 16) % total_shards


def load_slugs(shard: int = 0, total_shards: int = 1) -> list[tuple[str, str]]:
    """
    Load (ats, slug) pairs from Supabase slug_registry.
    Supabase is the single source of truth — enriched weekly
    by enrich_slugs.py (Feashliaa + kalil0321 + OpenPostings + Common Crawl).

    When total_shards > 1, returns only this shard's slice (for GitHub
    Actions matrix parallelism — see module docstring).
    """
    pairs = get_all_slugs()

    if not pairs:
        log.warning("No slugs found in Supabase slug_registry!")
        log.warning("Run enrich_slugs.py first to populate the registry.")
        return []

    if total_shards > 1:
        pairs = [(a, s) for a, s in pairs if _shard_of(a, s, total_shards) == shard]
        log.info(f"Shard {shard}/{total_shards}: {len(pairs)} boards assigned")

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
    """
    Stage 3+4: Keep only global/Africa-eligible jobs.

    Also tags each matched job with job["location_priority"]:
      1 = Global   (explicit worldwide/anywhere/global-hiring signal)
      2 = Africa   (Africa continent, or bare EMEA)
      3 = Unsure   (kept as a plausible remote match, but AI/keyword
                    evidence didn't confirm which of the above it is)
    This is what jobs.location_priority (already in the schema) sorts on,
    so Global rows surface before Africa rows before "maybe" rows.
    """
    matched = []
    matched_confidences = []
    unsure_jobs = []

    for job in jobs:
        result, priority = _keyword_classify_location_detail(job)
        if result == "match":
            job["clearance"] = "regex"
            job["location_priority"] = priority  # PRIORITY_GLOBAL or PRIORITY_AFRICA
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
                # AI confirmed genuinely global hiring from the description
                # (LOCATION_SYSTEM_PROMPT only ever says MATCH for worldwide
                # evidence, never Africa-specifically — keyword step above
                # already catches literal "Africa" mentions), so this is a
                # Global-tier match, same as a keyword-level global match.
                job["clearance"] = provider_name
                job["location_priority"] = PRIORITY_GLOBAL
                matched.append(job)
                matched_confidences.append("match")
            elif label == "uncertain":
                job["clearance"] = provider_name
                job["location_priority"] = PRIORITY_UNSURE
                matched.append(job)
                matched_confidences.append("uncertain")
            # "no_match" → drop; AI failure defaults to "uncertain" (included)

    log.info(f"After location filter: {len(matched)} global/Africa jobs")
    return matched, matched_confidences


def _run_pipeline(boards: list[tuple[str, str]], include_job_boards: bool) -> None:
    """Shared core: scrape → filter → enrich → push, with its own
    scan_report row. Does NOT run cleanup_stale_jobs() — see
    run_finalize() for why that's split out."""
    report_id = start_scan_report()

    try:
        all_jobs: list[dict] = []
        boards_ok = boards_failed = 0

        if boards:
            log.info(f"\nScraping {len(boards)} boards across "
                     f"{len(set(a for a, _ in boards))} ATS platforms...")
            all_jobs, boards_ok, boards_failed = scrape_all(boards)

        if include_job_boards:
            log.info(f"\nScraping remote job boards...")
            board_jobs = scrape_all_job_boards()
            all_jobs.extend(board_jobs)

            # Save any new company slugs discovered from aggregator job URLs
            discovered = get_discovered_slugs()
            if discovered:
                log.info(f"\nDiscovered {len(discovered)} new slugs from job board URLs → saving to slug_registry")
                populate_slug_registry(discovered, source="job_board_discovery")

        if not all_jobs:
            log.info("No jobs found across any source.")
            if report_id:
                finish_scan_report(report_id, boards_scanned=boards_ok, boards_failed=boards_failed)
            return

        # Filter for CSM/AM roles
        log.info(f"\nFiltering for CSM/AM roles...")
        csm_jobs = filter_roles(all_jobs)
        if not csm_jobs:
            log.info("No CSM/AM roles found.")
            if report_id:
                finish_scan_report(
                    report_id, boards_scanned=boards_ok, boards_failed=boards_failed,
                    total_jobs_raw=len(all_jobs),
                )
            return

        # Enrich descriptions for platforms that lack them
        log.info(f"\nFetching descriptions for jobs missing them...")
        csm_jobs = enrich_descriptions(csm_jobs)

        # Fetch application questions for location-"unsure" jobs, across
        # all 20 ATS platforms (multi-tier fallback — see ats_scrapers.py).
        # Work authorization questions help the AI detect country-restricted roles
        log.info(f"\nEnriching application questions across all ATS platforms...")
        csm_jobs = enrich_application_questions(csm_jobs)

        # Filter for Africa/Global locations
        log.info(f"\nFiltering for Africa/Global eligibility...")
        global_jobs, confidences = filter_locations(csm_jobs)
        if not global_jobs:
            log.info("No global/Africa-eligible CSM/AM roles found.")
            if report_id:
                finish_scan_report(
                    report_id, boards_scanned=boards_ok, boards_failed=boards_failed,
                    total_jobs_raw=len(all_jobs), csm_roles=len(csm_jobs),
                )
            return

        # Detect visa sponsorship from descriptions (before discarding them)
        for job in global_jobs:
            job["visa_sponsorship"] = detect_visa_sponsorship(job)

        # Push to Supabase
        log.info(f"\nPushing {len(global_jobs)} jobs to Supabase...")
        added = add_jobs_batch(global_jobs, confidences)

        # Finalize this run's report
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
        log.info(f"   Pipeline: {len(all_jobs)} scraped -> {len(csm_jobs)} CSM/AM -> "
                 f"{len(global_jobs)} global -> {added} new")

    except Exception as e:
        log.error(f"Scanner failed: {e}")
        if report_id:
            finish_scan_report(report_id, status="failed")
        raise


def run_finalize() -> None:
    """Cleanup pass — call this ONCE, after every scraping shard and the
    job-boards job have all finished (gate with `needs:` in CI so this
    doesn't start until they're done).

    This is deliberately a separate step rather than the last line of each
    shard's own run. cleanup_stale_jobs() marks/deletes jobs across the
    WHOLE table based on how long ago they were last "seen" — if it ran
    inside an individual shard, whichever shard happened to finish first
    would run a global cleanup pass while slower shards were still
    mid-scan, and could hard-delete a job belonging to a slow shard's
    company moments before that shard was about to re-scrape and refresh
    its last_seen date. Splitting this out into its own `needs`-gated job
    removes that race entirely."""
    log.info("=" * 60)
    log.info("ATS GLOBAL SCANNER — finalize (cleanup stale jobs)")
    log.info("=" * 60)
    cleanup_stale_jobs(inactive_days=30, delete_days=60)


def main():
    parser = argparse.ArgumentParser(description="ATS Global Scanner")
    parser.add_argument("--shard", type=int, default=0,
                         help="This shard's index (0-based), for GitHub Actions matrix parallelism")
    parser.add_argument("--total-shards", type=int, default=1,
                         help="Total number of shards; each processes ~1/N of the ATS boards")
    parser.add_argument("--job-boards-only", action="store_true",
                         help="Only scrape job board aggregators (RemoteOK, Remotive, etc.) — no ATS boards")
    parser.add_argument("--finalize", action="store_true",
                         help="Only run cleanup (mark/delete stale jobs) — call once after all shards/jobs finish")
    args = parser.parse_args()

    mode_note = ""
    if args.total_shards > 1:
        mode_note = f" (shard {args.shard}/{args.total_shards})"
    elif args.job_boards_only:
        mode_note = " (job-boards-only)"
    elif args.finalize:
        mode_note = " (finalize)"

    log.info("=" * 60)
    log.info(f"ATS GLOBAL SCANNER — starting{mode_note}")
    log.info("=" * 60)

    if args.finalize:
        run_finalize()
        return

    boards: list[tuple[str, str]] = []
    if not args.job_boards_only:
        log.info("Loading company slugs from Supabase...")
        try:
            boards = load_slugs(shard=args.shard, total_shards=args.total_shards)
        except SupabaseFetchError as e:
            # A real fetch failure (e.g. the one-off 401 that made shard 2/8
            # silently scrape nothing) is NOT the same as "this shard
            # legitimately has zero boards" below — it must fail loudly
            # (non-zero exit) so the GitHub Actions job shows red instead of
            # a quiet, misleading "completed" with 0 jobs found.
            log.error(f"Failed to load slugs from Supabase after retries — aborting shard: {e}")
            sys.exit(1)
        if not boards:
            if args.total_shards == 1:
                log.error("No boards to scrape.")
                return
            # A single shard legitimately CAN come back empty (e.g. more
            # shards than boards on some platform) — not an error, just
            # nothing for this shard to do.
            log.warning(f"Shard {args.shard}/{args.total_shards}: no boards assigned, skipping ATS scrape.")

    # Standalone/manual full run (no sharding, not --job-boards-only) scrapes
    # job boards inline, same as before. Sharded ATS runs do NOT — job boards
    # are scraped once per scan via a separate `--job-boards-only` CI job,
    # not once per shard (the aggregator APIs aren't slug-based, so there's
    # nothing to shard, and hitting them 8x would just be wasted/duplicate
    # requests against those APIs' own rate limits).
    include_job_boards = args.job_boards_only or args.total_shards == 1

    _run_pipeline(boards, include_job_boards=include_job_boards)

    # Only the unsharded, manual/local full run does cleanup inline.
    # Sharded CI runs call `--finalize` as their own separate, `needs`-gated
    # step instead (see run_finalize() docstring for why that matters).
    if args.total_shards == 1 and not args.job_boards_only:
        run_finalize()


if __name__ == "__main__":
    main()
