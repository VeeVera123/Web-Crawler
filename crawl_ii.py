"""
CRAWL I — the trusted, actually-scraped-daily ATS scanner (formerly
main.py). Renamed 2026-08 as part of the Crawl I / Crawl II restructure —
"Crawl I" is this file, scraping known ATS boards from archive_i (formerly
slug_registry); "Crawl II" (crawl_ii.py) is the newer, separate heuristic
scraper for archive_ii's in-house/unsupported career pages. Both are
launched by the single unified crawl.yml workflow (formerly daily_scan.yml).
=======================================
Scans 87,000+ company boards across 20+ ATS platforms (ApplyToJob retired
2026-08 — see ats_scrapers.py's SCRAPERS dict for the current live list).

2026-08: job board aggregators (RemoteOK, Remotive, Himalayas, Arbeitnow,
Jobicy, WeWorkRemotely, Working Nomads, FreeHire) were disabled and
job_board_scrapers.py removed entirely — ATS boards are now the only
source. --job-boards-only mode is gone; discovery of new slugs from
aggregator job URLs (populate_slug_registry(source="job_board_discovery"))
is gone with it.

Reads slugs from Supabase archive_i (formerly slug_registry — single
source of truth, populated by node.py's crawl_batch() writing ATS-pattern
hits directly, no intermediate staging/verify step; see node.py's module
docstring). Filters for CSM/Account Management roles hiring globally or
in Africa. Pushes matches to Supabase (PostgreSQL), tagged
source_pipeline='crawl_i' (the jobs table column's default).

LLM provider is set via LLM_PROVIDER env var (see SWITCHING_GUIDE.md).

CLI modes (see .github/workflows/crawl.yml for how these compose):
  python crawl_i.py                                   Full run: all ATS boards + cleanup,
                                                        in one process. Default for manual/
                                                        local use — unchanged behavior.
  python crawl_i.py --shard 0 --total-shards 8         ATS boards only, this shard's 1/8 slice.
                                                        No cleanup (see run_finalize()).
  python crawl_i.py --finalize                         Cleanup only (mark/delete stale jobs, 31-day
                                                        hard-delete threshold — was 60). Run ONCE,
                                                        after every shard has finished (gate with
                                                        `needs:` in CI).
"""

import argparse
import hashlib
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from ats_scrapers import scrape_board, enrich_descriptions, enrich_application_questions, SCRAPERS
from classifier import (
    keyword_classify_role, ai_classify_roles,
    keyword_classify_location, ai_classify_locations,
    detect_visa_sponsorship,
    _keyword_classify_location_detail,
    PRIORITY_GLOBAL, PRIORITY_AFRICA, PRIORITY_UNSURE,
)
from supabase_handler import (
    add_jobs_batch, start_scan_report, finish_scan_report,
    get_all_slugs, cleanup_stale_jobs,
    get_existing_urls, touch_seen_jobs_raw,
    touch_archive_i_last_seen,
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
    # "applytojob": 18,  # REMOVED 2026-08 — ATS retired, see ats_scrapers.py
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
    evenly across all shards regardless of how archive_i rows happen
    to be ordered/clustered by source — so no shard accidentally ends up
    as "all Workday" with a different completion profile than its peers."""
    h = hashlib.md5(f"{ats}|{slug}".encode()).hexdigest()
    return int(h, 16) % total_shards


def load_slugs(shard: int = 0, total_shards: int = 1) -> list[tuple[str, str]]:
    """
    Load (ats, slug) pairs from Supabase archive_i (formerly slug_registry).
    Populated directly by node.py's crawl_batch() (ATS-pattern hits) — no
    intermediate staging/verify table anymore; Verification/verification.py
    is the only thing that ever removes a row, and only once confirmed dead.

    When total_shards > 1, returns only this shard's slice (for GitHub
    Actions matrix parallelism — see module docstring).
    """
    pairs = get_all_slugs()

    if not pairs:
        log.warning("No slugs found in Supabase archive_i!")
        log.warning("Run node.py (via a seed source) first to populate it.")
        return []

    # Drop rows for ATSs with no registered scraper BEFORE sharding/dispatch,
    # not one-by-one inside scrape_board() — a retired ATS (e.g. applytojob,
    # removed 2026-08) can leave thousands of stale rows in archive_i
    # from before its discovery.py sources were also updated, and dispatching
    # each one individually just to log "Unknown ATS" per row is wasted
    # per-row overhead across a whole scan, not just log noise. One summary
    # line here instead of one warning per stale row.
    supported = [(a, s) for a, s in pairs if a.lower() in SCRAPERS]
    unsupported_counts: dict[str, int] = {}
    for ats, _ in pairs:
        if ats.lower() not in SCRAPERS:
            unsupported_counts[ats] = unsupported_counts.get(ats, 0) + 1
    if unsupported_counts:
        for ats, count in sorted(unsupported_counts.items(), key=lambda kv: -kv[1]):
            log.warning(f"Skipping {count} archive_i rows for unsupported "
                        f"ATS '{ats}' (no scraper registered — stale rows from "
                        f"a retired/renamed ATS? consider deleting them from "
                        f"Supabase directly).")
    pairs = supported

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


def scrape_all(boards: list[tuple[str, str]]) -> tuple[list[dict], int, int, set[tuple[str, str]]]:
    """Scrape all boards in parallel, grouped by ATS platform.
    Returns (jobs, boards_ok, boards_failed, boards_with_roles).

    `boards_with_roles` (2026-09) is the set of (ats, slug) pairs that
    returned at least one RAW job posting this run — i.e. straight off
    scrape_board(), before filter_roles()'s CSM/AM-only filter and before
    filter_locations()'s Global/Africa filter. This is deliberate, per an
    explicit user instruction: archive_i's last_seen is repurposed (see
    supabase_handler.touch_archive_i_last_seen) to mean "this slug had ANY
    role at all," not "had a CSM/AM role," and specifically must NOT be
    scoped to customer-success-shaped titles — a CEO opening counts just
    as much as a CSM one. Computing this set here, from the same raw
    per-board result scrape_board() already returns, means the signal
    is correct regardless of what filter_roles()/filter_locations() later
    decide to keep for the `jobs` table."""
    all_jobs = []
    total_ok = 0
    total_failed = 0
    boards_with_roles: set[tuple[str, str]] = set()

    # Group boards by ATS to apply per-platform concurrency
    by_ats = {}
    for ats, slug in boards:
        by_ats.setdefault(ats, []).append(slug)

    def _scrape_platform(ats: str, slugs: list[str]) -> tuple[int, int, list[dict], set[tuple[str, str]]]:
        """Scrape one ATS platform with appropriate concurrency."""
        workers = PLATFORM_WORKERS.get(ats, 8)
        platform_jobs = []
        platform_failed = 0
        platform_boards_with_roles: set[tuple[str, str]] = set()

        def _do_scrape(slug):
            return slug, scrape_board(ats, slug)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_do_scrape, s): s for s in slugs}
            for future in as_completed(futures):
                try:
                    slug, jobs = future.result()
                    if jobs:
                        platform_jobs.extend(jobs)
                        platform_boards_with_roles.add((ats, slug))
                except Exception as e:
                    platform_failed += 1
                    if platform_failed <= 3:  # log first 3 errors per platform
                        log.error(f"  {ats} scrape error: {e}")

        return len(slugs) - platform_failed, platform_failed, platform_jobs, platform_boards_with_roles

    # Run all platforms concurrently — each has its own per-platform worker limit
    with ThreadPoolExecutor(max_workers=len(by_ats)) as platform_pool:
        platform_futures = {}
        for ats, slugs in by_ats.items():
            f = platform_pool.submit(_scrape_platform, ats, slugs)
            platform_futures[f] = ats

        for future in as_completed(platform_futures):
            ats = platform_futures[future]
            try:
                ok, bad, jobs, with_roles = future.result()
                all_jobs.extend(jobs)
                total_ok += ok
                total_failed += bad
                boards_with_roles |= with_roles
                log.info(f"  {ats}: {len(jobs)} jobs from {ok} active boards ({bad} failed)")
            except Exception as e:
                log.error(f"  {ats}: platform error: {e}")

    log.info(f"Total raw jobs scraped: {len(all_jobs)} ({total_failed} boards failed, "
             f"{len(boards_with_roles)} boards had >=1 role)")
    return all_jobs, total_ok, total_failed, boards_with_roles


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
        for job, (label, provider_name) in zip(unsure_jobs, ai_results):
            # provider_name is whichever of LOCATION_PROVIDERS actually
            # classified this job — returned directly by ai_classify_locations
            # (2026-09: was re-derived here via a separate i%len(LOCATION_PROVIDERS)
            # round-robin that could silently drift out of sync with the one
            # ai_classify_locations does internally; see that function's docstring).
            if label == "match_global":
                # AI found genuinely worldwide-hiring evidence in the title/
                # description that the location field itself never stated.
                job["clearance"] = provider_name or "ai"
                job["location_priority"] = PRIORITY_GLOBAL
                matched.append(job)
                matched_confidences.append("match")
            elif label == "match_africa":
                # AI found Africa-continent or bare-EMEA evidence in the
                # title/description — same tier as a keyword-level Africa
                # match, just discovered via the AI stage instead.
                job["clearance"] = provider_name or "ai"
                job["location_priority"] = PRIORITY_AFRICA
                matched.append(job)
                matched_confidences.append("match")
            elif label == "uncertain":
                job["clearance"] = provider_name or "ai"
                job["location_priority"] = PRIORITY_UNSURE
                matched.append(job)
                matched_confidences.append("uncertain")
            # "no_match" → drop; AI failure defaults to "uncertain" (included)

    log.info(f"After location filter: {len(matched)} global/Africa jobs")
    return matched, matched_confidences


def _run_pipeline(boards: list[tuple[str, str]]) -> None:
    """Shared core: scrape → filter → enrich → push, with its own
    scan_report row. Does NOT run cleanup_stale_jobs() — see
    run_finalize() for why that's split out.

    2026-08: job board aggregators (RemoteOK, Remotive, etc. — see
    job_board_scrapers.py) were disabled and the file removed entirely —
    ATS boards are now the only source Crawl I scrapes."""
    report_id = start_scan_report()

    try:
        all_jobs: list[dict] = []
        boards_ok = boards_failed = 0
        boards_with_roles: set[tuple[str, str]] = set()

        if boards:
            log.info(f"\nScraping {len(boards)} boards across "
                     f"{len(set(a for a, _ in boards))} ATS platforms...")
            all_jobs, boards_ok, boards_failed, boards_with_roles = scrape_all(boards)

        # 2026-09: repurpose archive_i.last_seen to mean "last time this
        # slug had ANY role at all" (per explicit user instruction) rather
        # than "last time discovery re-confirmed the ATS page exists" —
        # touch it here, from the RAW per-board scrape result, regardless
        # of whether any of these jobs go on to pass CSM/AM or Global/
        # Africa filtering below (a CEO opening counts the same as a CSM
        # one). No-op (0 touched) when boards_with_roles is empty, e.g. on
        # a totally job-less shard.
        if boards_with_roles:
            touch_archive_i_last_seen(boards_with_roles)

        if not all_jobs:
            log.info("No jobs found across any source.")
            if report_id:
                finish_scan_report(report_id, boards_scanned=boards_ok, boards_failed=boards_failed)
            return

        raw_scraped_count = len(all_jobs)

        # 2026-08: dedup against Supabase BEFORE classification — a job
        # whose URL is already in `jobs` is one we've already classified
        # in a prior run; sending it through keyword_classify_role/
        # ai_classify_roles/ai_classify_locations again just burns LLM
        # calls (and time) to re-derive an answer we already have. Only
        # genuinely new URLs go on to filter_roles() below; already-known
        # ones just get last_seen/is_active refreshed directly.
        log.info(f"\nChecking Supabase for already-known jobs (skip re-classification)...")
        existing_urls = get_existing_urls()
        new_jobs, already_seen = [], []
        for job in all_jobs:
            url = job.get("url", "")
            if url and url in existing_urls:
                already_seen.append(job)
            else:
                new_jobs.append(job)
        if already_seen:
            log.info(f"  {len(already_seen)}/{raw_scraped_count} jobs already known — "
                     f"skipping LLM classification, just refreshing last_seen")
            touch_seen_jobs_raw(already_seen)
        if not new_jobs:
            log.info("No new (previously unseen) jobs to classify.")
            if report_id:
                finish_scan_report(
                    report_id, boards_scanned=boards_ok, boards_failed=boards_failed,
                    total_jobs_raw=raw_scraped_count, duplicates=len(already_seen),
                )
            return
        all_jobs = new_jobs

        # Filter for CSM/AM roles
        log.info(f"\nFiltering for CSM/AM roles...")
        csm_jobs = filter_roles(all_jobs)
        if not csm_jobs:
            log.info("No CSM/AM roles found.")
            if report_id:
                finish_scan_report(
                    report_id, boards_scanned=boards_ok, boards_failed=boards_failed,
                    total_jobs_raw=raw_scraped_count,
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
                    total_jobs_raw=raw_scraped_count, csm_roles=len(csm_jobs),
                )
            return

        # Detect visa sponsorship from descriptions (before discarding them)
        for job in global_jobs:
            job["visa_sponsorship"] = detect_visa_sponsorship(job)

        # Push to Supabase — pass the already-fetched existing_urls through
        # so add_jobs_batch doesn't re-pull the whole `jobs` table again.
        log.info(f"\nPushing {len(global_jobs)} jobs to Supabase...")
        added = add_jobs_batch(global_jobs, confidences, existing_urls=existing_urls)

        # Finalize this run's report. `duplicates` now counts BOTH kinds:
        # pre-classification skips (already_seen) and any post-classification
        # re-matches (global_jobs that still weren't a true first-insert —
        # should be rare now, but not impossible with in-run URL reuse).
        duplicates = len(already_seen) + (len(global_jobs) - added)
        if report_id:
            finish_scan_report(
                report_id,
                boards_scanned=boards_ok,
                boards_failed=boards_failed,
                total_jobs_raw=raw_scraped_count,
                csm_roles=len(csm_jobs),
                global_jobs=len(global_jobs),
                new_jobs_added=added,
                duplicates=duplicates,
            )

        log.info(f"\nDone! {added} new jobs added to Supabase.")
        log.info(f"   Pipeline: {raw_scraped_count} scraped ({len(already_seen)} already known, "
                 f"skipped) -> {len(all_jobs)} new -> {len(csm_jobs)} CSM/AM -> "
                 f"{len(global_jobs)} global -> {added} new")

    except Exception as e:
        log.error(f"Scanner failed: {e}")
        if report_id:
            finish_scan_report(report_id, status="failed")
        raise


def run_finalize() -> None:
    """Cleanup pass — call this ONCE, after every scraping shard has
    finished (gate with `needs:` in CI so this doesn't start until
    they're done).

    This is deliberately a separate step rather than the last line of each
    shard's own run. cleanup_stale_jobs() marks/deletes jobs across the
    WHOLE table based on how long ago they were last "seen" — if it ran
    inside an individual shard, whichever shard happened to finish first
    would run a global cleanup pass while slower shards were still
    mid-scan, and could hard-delete a job belonging to a slow shard's
    company moments before that shard was about to re-scrape and refresh
    its last_seen date. Splitting this out into its own `needs`-gated job
    removes that race entirely.

    2026-08: delete_days dropped from 60 to 31 (per user instruction), and
    scoped to source_pipeline='crawl_i' only — Crawl II runs its own
    separate finalize (crawl_ii.py) with its own policy, and neither
    pipeline's cleanup should be able to touch the other's rows."""
    log.info("=" * 60)
    log.info("CRAWL I — finalize (cleanup stale jobs)")
    log.info("=" * 60)
    summary = cleanup_stale_jobs(inactive_days=30, delete_days=31, source_pipeline="crawl_i")
    log.info(f"Crawl I finalize summary: inactive cutoff {summary['inactive_cutoff']} "
             f"(ok={summary['mark_inactive_ok']}), delete cutoff {summary['delete_cutoff']} "
             f"(ok={summary['delete_ok']})")


def main():
    parser = argparse.ArgumentParser(description="Crawl I — ATS Global Scanner")
    parser.add_argument("--shard", type=int, default=0,
                         help="This shard's index (0-based), for GitHub Actions matrix parallelism")
    parser.add_argument("--total-shards", type=int, default=1,
                         help="Total number of shards; each processes ~1/N of the ATS boards")
    parser.add_argument("--finalize", action="store_true",
                         help="Only run cleanup (mark/delete stale jobs) — call once after all shards finish")
    args = parser.parse_args()

    mode_note = ""
    if args.total_shards > 1:
        mode_note = f" (shard {args.shard}/{args.total_shards})"
    elif args.finalize:
        mode_note = " (finalize)"

    log.info("=" * 60)
    log.info(f"CRAWL I — starting{mode_note}")
    log.info("=" * 60)

    if args.finalize:
        run_finalize()
        return

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

    _run_pipeline(boards)

    # Only the unsharded, manual/local full run does cleanup inline.
    # Sharded CI runs call `--finalize` as their own separate, `needs`-gated
    # step instead (see run_finalize() docstring for why that matters).
    if args.total_shards == 1:
        run_finalize()


if __name__ == "__main__":
    main()
