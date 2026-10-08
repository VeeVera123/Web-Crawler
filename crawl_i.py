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
import asyncio
import hashlib
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import config
import location_diagnostics
import excluded_cache
from ats_scrapers import (scrape_board, enrich_descriptions, enrich_application_questions,
                          SCRAPERS, log_scrape_failure_summary, aclose_http_client)
from classifier import (
    keyword_classify_role, ai_classify_roles,
    keyword_classify_location, ai_classify_locations,
    detect_visa_sponsorship,
    _keyword_classify_location_detail,
    classify_role_category,
    classify_rank4, RANK4_ELIGIBLE_ATS,
    PRIORITY_GLOBAL, PRIORITY_AFRICA,
    PRIORITY_UNSURE_BLANK, PRIORITY_UNSURE_SILENT,
)
from supabase_handler import (
    add_jobs_batch, bump_scan_report, finish_scan_report_for_pipeline,
    get_all_slugs, cleanup_stale_jobs,
    get_existing_urls, get_known_jobs_meta, touch_seen_jobs_raw, mark_jobs_vetoed,
    touch_archive_i_last_seen,
    SupabaseFetchError,
    log_egress_summary,
)
from supabase_handler import CLASSIFIER_VERSION  # noqa: E402
from job_url import UrlSet, split_known_jobs  # noqa: E402
import revalidate
# 2026-09 (second pass): Notion sync moved OUT of this file entirely, into
# prefix_supabase.py (before shards)/postfix_notion.py (after shards) —
# see notion_sync.py's module docstring for why. This file is back to
# being a pure Supabase writer.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# httpx logs one INFO-level "HTTP Request: ..." line per request by
# default, which propagates straight through basicConfig's root INFO
# level — at 19,000+ boards that's tens of thousands of lines drowning
# out the per-platform completion lines and heartbeat this file actually
# wants visible. Raised to WARNING so httpx/httpcore only speak up for
# their own internal problems, not routine successful requests.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Matches add_jobs_batch()'s default and _build_row()'s default —
# spelled out explicitly here (rather than relying on those defaults)
# purely so bump_scan_report()/finish_scan_report_for_pipeline() calls
# below have one obvious source of truth for the pipeline name, same
# style as crawl_ii.py's/crawl_iii.py's own SOURCE_PIPELINE constants.
SOURCE_PIPELINE = "crawl_i"

# 2026-09 (explicit user request — "excluded roles that did not make it
# stored as a json file on github ... so they dont send the same roles to
# the LLM over"): this crawl's own local paths for the excluded-jobs cache
# (see excluded_cache.py's module docstring for the full design). The
# canonical file is downloaded here by the CI workflow BEFORE this shard's
# classify step runs (a no-op/empty-cache if it doesn't exist yet — first
# run, or nothing cached), and this shard's own newly-excluded findings get
# written to its own uniquely-named partial file for the workflow to
# upload — never written to/read from the canonical path directly, to
# avoid the git/Release-asset write race ~10 concurrent shards would cause.
EXCLUDED_CACHE_PATH = "excluded_1.json"
EXCLUDED_CACHE_SHARD_GLOB = "excluded_1_shard_*.json"


def _excluded_cache_shard_path(shard: int) -> str:
    return f"excluded_1_shard_{shard}.json"


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

    # 2026-09 BUG FIX (explicit user report: "an ungodly amount of time
    # is being spent crawling SAP SuccessFactors"): successfactors had NO
    # entry here at all, silently falling back to the default of 8 —
    # same low cap as ashby (deliberately conservative for a stricter
    # platform), despite successfactors being a per-tenant-subdomain
    # platform like workday/icims/hrmdirect above, where different
    # boards never share a host and so don't compete for the same
    # per-host semaphore. Set to match icims's proven-in-production
    # ceiling: scrape_successfactors itself was ALSO cut from up to 11
    # sequential locale passes per board down to 1-2 (see that function's
    # BUG FIX comment — verified live that extra locales were the exact
    # same job postings, just retranslated chrome text, not extra
    # coverage), so each board is now a genuinely light 1-2-request job
    # rather than the potential hundreds it used to be, and can safely
    # support the same board-level concurrency as any other per-tenant
    # platform here.
    "successfactors": 30,

    # 2026-10: Dayforce — every tenant shares ONE host (jobs.dayforcehcm.com)
    # and each scrape does a CSRF handshake plus 25-posting pages, so keep
    # it bounded like the other shared-host platforms above. HireHive is
    # subdomain-per-tenant with a single light JSON call per ~30 jobs.
    "dayforce": 6,
    "hirehive": 12,
    # 2026-10: Manatal — shared host (careers-page.com), a scrape is one
    # HTML page per 10 postings (up to ~60 pages) so keep it modest;
    # JobScore / Crelate are one JSON / RSS call per tenant on shared hosts.
    "manatal": 8,
    "jobscore": 12,
    "crelate": 12,
    "comeet": 12,  # 2026-10: page + one API call per tenant
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

    2026-09: sharding now happens server-side (get_all_slugs() passes
    shard_index/shard_count straight through to the archive_i_shard
    Postgres RPC) so each shard's Supabase fetch only ever downloads its
    own ~1/total_shards slice of archive_i, instead of every shard
    downloading the full table and discarding (total_shards-1)/total_shards
    of it client-side. get_all_slugs() itself falls back to the old
    full-table-then-filter behavior (using this same _shard_of() hash) if
    the RPC is ever unavailable, so this still works if the migration
    hasn't been applied yet.
    """
    if total_shards > 1:
        pairs = get_all_slugs(shard_index=shard, shard_count=total_shards)
    else:
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
        log.info(f"Shard {shard}/{total_shards}: {len(pairs)} boards assigned")

    ats_counts: dict[str, int] = {}
    for ats, _ in pairs:
        ats_counts[ats] = ats_counts.get(ats, 0) + 1

    for ats in sorted(ats_counts, key=lambda a: -ats_counts[a]):
        log.info(f"  {ats}: {ats_counts[ats]} companies")
    log.info(f"Total: {len(pairs)} boards across {len(ats_counts)} ATS platforms")

    return pairs


async def _scrape_all_async(boards: list[tuple[str, str]]) -> tuple[list[dict], int, int, set[tuple[str, str]]]:
    """Async core of scrape_all — see that function's docstring for the
    full behavior contract (unchanged by this migration). Runs under
    asyncio.run() from the sync scrape_all() wrapper below, so the rest of
    _run_pipeline (classification, enrichment scheduling, Supabase writes)
    stays fully synchronous — only the ATS-scraping HTTP fan-out itself
    moves to asyncio.

    2026-09 ASYNC MIGRATION (batched — see ats_scrapers.py's module
    docstring): replaces the old two-level ThreadPoolExecutor nesting
    (one pool of platforms, each running its own pool of per-slug scrape
    calls) with two levels of asyncio.gather. PLATFORM_WORKERS' per-ATS
    worker cap becomes an asyncio.Semaphore of the same size — same
    "don't run more than N of this platform's boards at once" contract,
    just expressed as a concurrency gate a coroutine awaits on rather than
    a thread pool's queue depth. ats_scrapers.py's own per-HOST semaphore
    (_host_semaphore_for) still applies underneath this — a platform like
    Workday that spans many distinct tenant hosts gets its per-platform
    cap here AND a separate per-tenant-host cap there, same as before."""
    all_jobs = []
    total_ok = 0
    total_failed = 0
    boards_with_roles: set[tuple[str, str]] = set()

    # Group boards by ATS to apply per-platform concurrency
    by_ats = {}
    for ats, slug in boards:
        by_ats.setdefault(ats, []).append(slug)

    async def _scrape_platform(ats: str, slugs: list[str]) -> tuple[int, int, list[dict], set[tuple[str, str]]]:
        """Scrape one ATS platform with appropriate concurrency."""
        workers = PLATFORM_WORKERS.get(ats, 8)
        platform_jobs = []
        platform_failed = 0
        platform_boards_with_roles: set[tuple[str, str]] = set()
        sem = asyncio.Semaphore(workers)

        async def _do_scrape(slug):
            async with sem:
                return slug, await scrape_board(ats, slug)

        results = await asyncio.gather(
            *(_do_scrape(s) for s in slugs), return_exceptions=True
        )
        for res in results:
            if isinstance(res, Exception):
                # 2026-09: no longer logged inline here (real-time per-error
                # lines during the scrape were exactly the noise the user
                # asked to remove) — every failure is already recorded into
                # ats_scrapers.py's grouped-failure collector inside
                # scrape_board() itself, and reported ONCE, grouped, right
                # after scrape_all() returns (see scrape_all()'s own
                # log_scrape_failure_summary() call below).
                platform_failed += 1
                continue
            slug, jobs = res
            if jobs:
                platform_jobs.extend(jobs)
                platform_boards_with_roles.add((ats, slug))

        return len(slugs) - platform_failed, platform_failed, platform_jobs, platform_boards_with_roles

    # Run all platforms concurrently — each has its own per-platform worker
    # limit. 2026-09 BUG FIX (real production evidence: a 25,284-board/39-
    # platform run produced zero per-platform "N jobs from M active boards"
    # lines until the ENTIRE scrape finished): asyncio.gather() only
    # returns once every one of its awaitables has completed, so the old
    # `for ats, res in zip(...)` loop below it — the only place these
    # lines were logged — never ran until the slowest platform (csod: 308
    # boards, hrmdirect: 210, ...) finished, however long that took. The
    # pre-asyncio ThreadPoolExecutor version logged each platform as its
    # own pool drained; this restores that per-platform-as-it-finishes
    # visibility without changing the overall concurrency at all — each
    # platform is still a separately-scheduled task racing every other
    # one, only the LOGGING now happens per-completion instead of
    # batched after the last straggler.
    # 2026-09 BUG FIX (real production evidence, same incident as the fix
    # above): a platform's OWN inner asyncio.gather() over its boards has
    # the identical "wait for every single one, then report" problem one
    # level deeper — if even a single board hits a slow/unresponsive host
    # and burns through its full MAX_RETRIES+timeout cycle, THAT
    # platform's line stays silent for however long that takes, no matter
    # how few boards it has (confirmed live: eploy/avature/folkshr, 15-30
    # boards each, produced nothing for 2+ minutes). A 30s-heartbeat
    # "...still working — N/M platform(s) not yet finished: ..." line
    # (asyncio.wait(..., timeout=HEARTBEAT_SECONDS)) was added here to
    # make a real stall distinguishable from a live-but-slow run — it did
    # its job (surfaced the actual event-loop-blocking bug in scrape_board,
    # since fixed) and was removed once confirmed live that platforms
    # complete and log individually again; the same slow-stragglers-named
    # line just repeating unchanged every 30s for the run's last few
    # minutes was pure noise once there was nothing new to learn from it.
    tasks = {asyncio.create_task(_scrape_platform(ats, slugs)): ats
             for ats, slugs in by_ats.items()}
    pending = set(tasks.keys())
    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            ats = tasks[task]
            try:
                ok, bad, jobs, with_roles = task.result()
            except Exception as e:
                log.error(f"  {ats}: platform error: {e}")
                continue
            all_jobs.extend(jobs)
            total_ok += ok
            total_failed += bad
            boards_with_roles |= with_roles
            log.info(f"  {ats}: {len(jobs)} jobs from {ok} active boards ({bad} failed)")

    log.info(f"Total raw jobs scraped: {len(all_jobs)} ({total_failed} boards failed, "
             f"{len(boards_with_roles)} boards had >=1 role)")
    return all_jobs, total_ok, total_failed, boards_with_roles


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
    decide to keep for the `jobs` table.

    2026-09 ASYNC MIGRATION: this is now a thin sync wrapper around
    _scrape_all_async() (asyncio.run()) — every OTHER function in this
    file (_run_pipeline and everything it calls after this point:
    classification, enrichment, Supabase writes) stays synchronous and
    unaware that scraping itself now runs on an event loop internally.

    2026-09 BUG FIX (real production evidence: "RuntimeError: Event loop
    is closed" on GitHub Actions, confirmed live during async migration
    batch 1 testing): this used to call asyncio.run() TWICE in
    sequence — once for _scrape_all_async(boards), then a SEPARATE
    asyncio.run() for aclose_http_client(). Each asyncio.run() call
    creates a brand-new event loop and fully tears it down when it
    returns. The shared httpx.AsyncClient (and its underlying TCP/TLS
    connections) are created inside the FIRST loop — by the time the
    second, separate asyncio.run() spins up a NEW loop and tries to
    close that client, the connections are still bound to the first
    (now-closed) loop, and asyncio refuses to touch a closed loop's
    resources from a different one. Fixed by running both the scrape AND
    the client cleanup inside ONE asyncio.run() call, via a single inner
    async function — same event loop for creation, use, and teardown.

    2026-09 BUG FIX: sets a generously-sized default executor before
    scraping starts. ats_scrapers.py's scrape_board() now runs every
    not-yet-async-converted scrape_* function via asyncio.to_thread(),
    which schedules onto the running loop's DEFAULT executor if one was
    never set — a plain concurrent.futures.ThreadPoolExecutor() sized
    min(32, os.cpu_count() + 4) (often as few as 6-8 threads on a hosted
    CI runner). PLATFORM_WORKERS' caps for the still-sync platforms alone
    (rippling, bamboohr, icims, recruitee, teamtailor, breezyhr, personio,
    joincom, paylocity, hrmdirect, zoho, jobvite, avature, pageup,
    flatchr, paycom, hireology, gem, recruiterbox, jazzhr) sum to well
    over 200 desired concurrent workers, so leaving the default in place
    would silently re-impose a process-wide bottleneck one level down from
    the one asyncio.to_thread() was just introduced to fix. 300 gives
    headroom above that sum with no meaningful per-thread cost (idle
    threads waiting on a semaphore or socket read are cheap)."""
    async def _scrape_and_cleanup():
        asyncio.get_running_loop().set_default_executor(
            ThreadPoolExecutor(max_workers=300)
        )
        try:
            return await _scrape_all_async(boards)
        finally:
            await aclose_http_client()

    return asyncio.run(_scrape_and_cleanup())



def filter_roles(jobs: list[dict]) -> list[dict]:
    """Stage 1+2: Keep only CSM/AM roles.

    2026-09 (explicit user request): every included job is also tagged
    job["role_category"] — one of "CS"/"AM"/"PM"/"OM" (see classifier.py's
    classify_role_category()) — persisted as its own DB column by
    supabase_handler._build_row/_build_row_raw."""
    included = []
    unsure = []

    for job in jobs:
        result = keyword_classify_role(job["title"])
        if result == "include":
            job["role_category"] = classify_role_category(job["title"])
            included.append(job)
        elif result == "unsure":
            unsure.append(job)

    log.info(f"Role filter: {len(included)} keyword match, {len(unsure)} unsure → sending to AI")

    if unsure:
        unsure_titles = [j["title"] for j in unsure]
        ai_results = ai_classify_roles(unsure_titles)
        for job in unsure:
            if ai_results.get(job["title"], False):
                job["role_category"] = classify_role_category(job["title"])
                included.append(job)

    log.info(f"After role filter: {len(included)} CSM/AM jobs")
    return included


def filter_locations(jobs: list[dict], excluded_urls: dict | None = None,
                      new_exclusions: set | None = None) -> tuple[list[dict], list[str]]:
    """
    Stage 3+4: Keep only global/Africa-eligible jobs.

    Also tags each matched job with job["location_priority"]:
      1 = Global   (explicit worldwide/anywhere/global-hiring signal)
      2 = Africa   (Africa continent, or bare EMEA)
    This is what jobs.location_priority (already in the schema) sorts on,
    so Global rows surface before Africa rows.

    2026-09 policy change (explicit user request): there is no more
    "Unsure, kept anyway" tier. A job only survives this filter with
    AFFIRMATIVE evidence of global/Africa hiring — a keyword match, or an
    AI match_global/match_africa verdict. Anything the keyword+AI stages
    can't actually confirm (AI genuinely said UNCERTAIN, AI never got to
    look at all, or plain "no_match") is dropped, full stop. See the
    "uncertain"/"no_match" branch below for the real posting (GFL
    Environmental, a local Indianapolis, IN role with zero location text
    captured at all) that got kept under the old policy and shouldn't
    have been.

    2026-09 Phase 2 (explicit user request — classification revamp,
    verbatim: "only regex passes makes this go into rank 1 or 2. if it
    goes to the LLM then it must mean no known hiring language we know
    was used... So regex pass: rank 1 or 2. And LLM pass: Rank 3b"): the
    AI stage is no longer authoritative for PRIORITY_GLOBAL/PRIORITY_AFRICA
    — only a keyword (_keyword_classify_location_detail) match sets those
    now. An AI match_global/match_africa verdict on an unsure job is kept,
    but only at the Rank 3 tier: a blank-location job (unsure_reason ==
    "blank") the AI backs with real match_global/match_africa evidence
    lands at PRIORITY_UNSURE_BLANK ("3a" — "the unsure ones sent to the
    LLM... they don't have a location field"); a bare-Remote job
    (unsure_reason == "bare_remote") — whether AI-confirmed or genuinely
    AI-uncertain — lands at PRIORITY_UNSURE_SILENT ("3b" — "just dead
    location silence... bare remote in the location field"), same
    protective bar as before (a blank-location job the AI can't back with
    real evidence is still dropped, unchanged — see the "blank" docstring
    in classifier.py's _keyword_classify_location_detail for the real
    scraper bugs this guards against).

    2026-09 Phase 2, Rank 4 (explicit user request, opt-in via
    config.ENABLE_RANK4_COUNTRY_SPECIFIC / crawl.yml's
    enable_rank4_country_specific checkbox, Crawl I only): a CS/AM-only
    job from a RANK4_ELIGIBLE_ATS platform that this filter would
    otherwise drop outright (keyword-stage "no_match", or an AI-stage
    drop) gets one more look from classify_rank4() before being dropped —
    a bare country/region/continent location (or a title/JD naming one)
    admitted at 4a/4b as long as neither the description nor the
    application questions confirm an actual country-tied restriction. See
    _try_rank4() below and classify_rank4()'s own docstring.

    2026-09 (explicit user request — see excluded_cache.py's module
    docstring for the full design): `excluded_urls` is an optional
    {url: excluded_at_iso} mapping — already TTL-filtered by the caller via
    excluded_cache.load_excluded_cache() — of jobs a PAST run already sent
    to the LLM and confirmed excluded. A job whose keyword-stage result is
    "unsure" and whose URL is in this mapping skips the LLM call entirely.
    It still gets a fresh Rank 4 attempt regardless (that's cheap,
    regex-only, and Rank 4's own config/logic can change independently of
    this cache — only the expensive AI step is ever skipped).
    `new_exclusions` is an optional set this function ADDS TO (never
    replaces) with the URL of every job that ends up genuinely dropped
    this run — AI-excluded or keyword-stage no_match, in both cases only
    after Rank 4 also declined it — for the caller to persist as this
    shard's own newly-excluded partial file. Deliberately narrow: only
    jobs that actually reached (or would have reached, if not cache-
    skipped) the LLM are ever recorded here — a hard keyword-stage
    no_match was never going to the LLM either way, so caching it would
    save nothing and only bloat the file. Both params default to
    None/disabled for any caller (or test) that doesn't need the cache.
    """
    if excluded_urls is None:
        excluded_urls = {}
    if new_exclusions is None:
        new_exclusions = set()

    # A job whose application form could not be read (rate-limited/blocked/
    # errored) is NOT judged on a form nobody saw: it is neither admitted nor
    # remembered as excluded, so it comes back as new and is retried next run.
    # (Hightouch's "authorized to work in the U.S." form was admitted at Rank 1
    # because an unreadable form looked the same as a question-free one.)
    unreadable = [j for j in jobs if j.get("_form_status") == "failed"]
    if unreadable:
        log.warning(f"  {len(unreadable)} jobs deferred: application form unreadable "
                    f"({sum(1 for j in unreadable if j.get('source_ats') == 'Ashby')} Ashby) — retried next run")
        jobs = [j for j in jobs if j.get("_form_status") != "failed"]

    matched = []
    matched_confidences = []
    unsure_jobs = []
    # Parallel to unsure_jobs — 'blank' (location field was empty/
    # placeholder, so possibly a scraper extraction bug rather than a
    # genuinely unlisted location) or 'bare_remote' (location field said
    # "Remote" with nothing else — a real signal, just not region-
    # specific). See _keyword_classify_location_detail's docstring.
    unsure_reasons = []

    rank4_enabled = getattr(config, "ENABLE_RANK4_COUNTRY_SPECIFIC", False)

    def _try_rank4(job: dict) -> bool:
        """Last-chance Rank 4 look at a job this filter would otherwise
        drop. Returns True (and appends to matched) if classify_rank4()
        admits it. See the module docstring above for the eligibility
        gate this enforces before ever calling classify_rank4() — role,
        ATS platform, the config toggle, and a genuine "Application
        Question:" line actually present (Rank 4's admission logic
        depends on questions being confirmed ABSENT, not merely
        unfetched)."""
        if not rank4_enabled:
            return False
        if job.get("role_category") not in ("CS", "AM"):
            return False
        if job.get("source_ats") not in RANK4_ELIGIBLE_ATS:
            return False
        if "Application Question:" not in (job.get("description_snippet") or ""):
            return False
        priority, reason = classify_rank4(job)
        if not priority:
            return False
        job["clearance"] = "rank4"
        job["location_priority"] = priority
        matched.append(job)
        matched_confidences.append(f"rank4_{reason}")
        return True

    cache_skipped = 0
    for job in jobs:
        result, priority, unsure_reason = _keyword_classify_location_detail(job)
        if result == "match":
            job["clearance"] = "regex"
            job["location_priority"] = priority  # PRIORITY_GLOBAL or PRIORITY_AFRICA
            matched.append(job)
            matched_confidences.append("match")
        elif result == "unsure":
            url = job.get("url") or ""
            if url and url in excluded_urls:
                # A past run already sent this exact job to the LLM and
                # confirmed it excluded (still within the TTL window) — skip
                # the LLM call, but still give it a fresh, cheap Rank 4 look
                # (see docstring above for why that's always safe to do).
                cache_skipped += 1
                _try_rank4(job)
            else:
                unsure_jobs.append(job)
                unsure_reasons.append(unsure_reason)
        else:
            # Keyword-stage "no_match" — never reaches the AI stage at
            # all under the existing policy; Rank 4 gets one last look
            # before this job is dropped for good. Not recorded into
            # new_exclusions — it was never going to the LLM regardless of
            # caching, so there's nothing to save by caching it.
            _try_rank4(job)

    if cache_skipped:
        log.info(f"  {cache_skipped} unsure jobs already known-excluded (cached, not yet expired) — skipped LLM reclassification")

    log.info(f"Location filter: {len(matched)} keyword match, {len(unsure_jobs)} unsure → sending to AI")

    # 2026-09 (Phase 2, explicit user request — "Do all 3", following the
    # two real scraper bugs — JazzHR/inabia and SuccessFactors/sonepar —
    # that silently produced location="" for postings the live page showed
    # as ordinary, specific US roles): a blank location is now excluded
    # more strictly (see the "blank" branch below), but that only protects
    # THIS run — it doesn't tell anyone a given scraper's extraction just
    # broke. Diagnostics moved to a shared module (location_diagnostics.py,
    # used identically by crawl_ii.py) that compares each ATS's blank rate
    # against its own persisted historical baseline rather than one flat
    # threshold, and breaks results down by location_status where scrapers
    # populate it (marker_not_found vs. marker_found_empty vs. extracted).
    location_diagnostics.report_and_update("CRAWL I", jobs, unsure_jobs, unsure_reasons)

    if unsure_jobs:
        ai_results = ai_classify_locations(unsure_jobs)
        for job, (label, provider_name), unsure_reason in zip(unsure_jobs, ai_results, unsure_reasons):
            # 2026-09 (explicit user policy, verbatim: "a rank 4 addition
            # MUST have the applications questions field. That's its whole
            # deal. Same with rank 3b, application questions are a MUST
            # ... For rank 3a ... no application questions are seen [and
            # that's fine/expected]"): Rank 3b (bare-"Remote" location) now
            # requires real "Application Question:" text in
            # description_snippet, exactly like Rank 4 already required —
            # the real postings that motivated this (Kraft Heinz/Eightfold:
            # location "Remote", description said "Hybrid Working" in body
            # text; Ofload/Workable: location unclear, a screening question
            # revealed a country-tied work-rights requirement) both show
            # the SAME failure mode: a bare "Remote" field alone proves
            # nothing, and the only place a hidden Hybrid/On-site/country
            # restriction usually surfaces is the application questions —
            # so a bare-Remote job with NO application questions at all
            # gives this pipeline no way to rule that out and no longer
            # gets the benefit of the doubt. Deliberately NOT applied to
            # the "blank" (3a) branch just below — that tier explicitly
            # exists to give Crawl II/III entries (which routinely have no
            # application questions at all) a chance; see that branch's
            # own comment.
            has_app_questions = "Application Question:" in (job.get("description_snippet") or "")
            # provider_name is whichever of LOCATION_PROVIDERS actually
            # classified this job — returned directly by ai_classify_locations
            # (2026-09: was re-derived here via a separate i%len(LOCATION_PROVIDERS)
            # round-robin that could silently drift out of sync with the one
            # ai_classify_locations does internally; see that function's docstring).
            if label in ("match_global", "match_africa") and unsure_reason == "blank":
                # 2026-09 Phase 2 (explicit user request — see the module
                # docstring's "regex pass: rank 1 or 2, LLM pass: Rank 3b"
                # policy): the AI is no longer authoritative for
                # PRIORITY_GLOBAL/PRIORITY_AFRICA — only a keyword match
                # sets those. This job had NO location field at all, and
                # the AI found real global/Africa evidence elsewhere in
                # the title/description — exactly Rank 3a's own
                # definition ("the unsure ones sent to the LLM... they
                # don't have a location field"). Kept, not promoted.
                job["clearance"] = provider_name or "ai"
                job["location_priority"] = PRIORITY_UNSURE_BLANK
                matched.append(job)
                matched_confidences.append("uncertain")
            elif (label in ("match_global", "match_africa") and unsure_reason == "bare_remote"
                    and has_app_questions):
                # Same demotion, for a bare-"Remote" location the AI backed
                # with real evidence — Rank 3b's own worked example is
                # literally "bare remote in the location field", so this
                # lands there too, same tier as a genuinely AI-uncertain
                # bare-remote job just below. Requires has_app_questions —
                # see this loop's own comment above.
                job["clearance"] = provider_name or "ai"
                job["location_priority"] = PRIORITY_UNSURE_SILENT
                matched.append(job)
                matched_confidences.append("uncertain")
            elif (label in ("match_global", "match_africa") and unsure_reason == "global_plus_place"
                    and has_app_questions):
                # 2026-10: a Global/Worldwide keyword PLUS a place that neither narrows nor excludes cleanly
                # ("Worldwide - US"). Same tier and same application-question requirement as bare Remote, but
                # stricter: only a real match_global/match_africa verdict keeps it. "uncertain" does NOT (the
                # location text itself is contradictory), it falls to the drop below, where Rank 4 gets its look.
                job["clearance"] = provider_name or "ai"
                job["location_priority"] = PRIORITY_UNSURE_SILENT
                matched.append(job)
                matched_confidences.append("uncertain")
            elif (label == "uncertain" and unsure_reason == "bare_remote" and has_app_questions
                    and provider_name is not None):
                # 2026-09 policy change (refined per explicit user
                # follow-up): a GENUINE AI-reviewed uncertainty — the AI
                # actually read the title/description and still couldn't
                # tell — is kept at PRIORITY_UNSURE, same as before.
                #
                # 2026-09 (second refinement, explicit user request): that
                # benefit-of-the-doubt is now reserved for `unsure_reason
                # == "bare_remote"` — a job whose location field literally
                # said "Remote" with nothing else, a real signal the
                # company itself gave. A BLANK location field is a
                # different animal: it's indistinguishable from a scraper
                # extraction bug (confirmed live twice in the same week —
                # Inabia/JazzHR and Sonepar/SuccessFactors both stored
                # location="" for postings that were, on the live page,
                # unambiguously and explicitly US-only) rather than a
                # genuine "company just didn't say." A blank-location job
                # the AI can't back with real match_global/match_africa
                # evidence no longer gets the same pass — see the
                # `unsure_reason == "blank"` branch below, which drops it
                # instead.
                #
                # 2026-10 POLICY SUPERSESSION (explicit user instruction:
                # "why do some of the clearance notes say ai_unreviewed...
                # if an LLM did not see it, have it wait and then try one
                # more time after which you should discard it should it
                # fail"): the `provider_name is not None` requirement this
                # branch used to drop (2026-09 fix, full writeup below,
                # kept for history) is now back, on purpose. That 2026-09
                # fix existed because a job could reach this point having
                # genuinely NEVER gotten a real shot at review — every
                # provider exhausted/rate-limited with no retry left in
                # that same run — so treating "never reviewed" the same
                # as "no_match" meant silently losing real Remote signal
                # to upstream rate-limiting, not anything about the job.
                # That's no longer true: ai_classify_locations() now runs
                # a last-chance retry (wait, then one forced attempt
                # bypassing the circuit breaker) for exactly this
                # scenario before ever returning — see its own docstring.
                # A job that STILL comes back with provider_name=None
                # after that has had a genuine fair shot and failed it,
                # which is exactly the "discard it should it fail" case
                # the user asked for — it now falls through to the `else`
                # below like any other drop (Rank4 still gets one last
                # look there first, same as everything else).
                #
                # ORIGINAL 2026-09 WRITEUP (why the requirement was
                # removed then — superseded above, kept for context):
                # triggered by a live collapse from 1500+/day to <300/day
                # survivors with csm_roles in the 10,000-17,000 range per
                # shard but global_jobs down to 11-75 (scan_reports,
                # Supabase project mqkcmkwpfvpajzjrbdji). This project
                # runs one process per ATS shard (10+ concurrent GitHub
                # Actions jobs), several of LOCATION_PROVIDERS' free-tier
                # keys are SHARED across all of them (Groq's real pool is
                # only 8K TPM, shared with role classification too — see
                # config.py), and once one provider trips the circuit
                # breaker early in a run, it never serves another request
                # for the rest of that shard's ~3 hour run — with no
                # last-chance retry existing yet at the time, that was a
                # real, unrecoverable dead end for every later bare-remote
                # job in the shard.
                job["clearance"] = provider_name
                job["location_priority"] = PRIORITY_UNSURE_SILENT
                matched.append(job)
                matched_confidences.append("uncertain")
            else:
                # "no_match" → drop. "uncertain" with unsure_reason ==
                # "blank" (a blank location field the AI still couldn't
                # back with real evidence, REGARDLESS of whether a
                # provider actually reviewed it) → also drop — a blank
                # field only survives via the match_global/match_africa
                # branches above, i.e. the AI found real textual evidence
                # for it — never on "we looked and still can't tell" (or
                # "never got looked at") alone, since a blank field can't
                # be told apart from the location simply never having been
                # captured in the first place (see the bare_remote branch
                # above for why that reasoning does NOT extend to a
                # genuine "Remote" signal from the company). A bare_remote
                # job with no application questions at all also lands here
                # now (see has_app_questions above) — Rank 4 requires the
                # same "Application Question:" marker anyway, so this is
                # the correct final drop for it either way. Rank 4 gets
                # one last look before the drop is final.
                before = len(matched)
                _try_rank4(job)
                if len(matched) == before:
                    # Genuinely excluded this run (AI didn't confirm it,
                    # and Rank 4 didn't rescue it either) — record it so a
                    # future run with the same URL skips the LLM call.
                    #
                    # 2026-10 (explicit user request — see the bare_remote
                    # branch above's policy-supersession note): NEVER cache
                    # this as an exclusion when provider_name is None —
                    # that means no AI ever actually reviewed this job,
                    # even after the last-chance retry. Caching a "nobody
                    # looked" outcome identically to a genuine negative AI
                    # verdict would mean a future run (possibly with
                    # healthy providers again) silently skips the LLM call
                    # for this URL for the full 21-day TTL, compounding the
                    # exact provider-outage problem that caused this
                    # instead of giving it a fresh look once providers
                    # recover.
                    if provider_name is not None:
                        url = job.get("url")
                        if url:
                            new_exclusions.add(url)

    log.info(f"After location filter: {len(matched)} global/Africa/Rank3/Rank4 jobs")
    return matched, matched_confidences


def _run_pipeline(boards: list[tuple[str, str]], shard: int = 0) -> None:
    """Shared core: scrape → filter → enrich → push. Does NOT run
    cleanup_stale_jobs() — see run_finalize() for why that's split out.

    2026-08: job board aggregators (RemoteOK, Remotive, etc. — see
    job_board_scrapers.py) were disabled and the file removed entirely —
    ATS boards are now the only source Crawl I scrapes.

    2026-09: scan-report accounting switched from start_scan_report()/
    finish_scan_report(report_id, ...) (one brand-new Supabase row per
    shard — confirmed live as 10-70+ rows/day, useless as a daily
    summary) to bump_scan_report(SOURCE_PIPELINE, ...) — this shard
    reports only its OWN contribution, and Postgres atomically adds it
    into the single (run_date, source_pipeline) row every shard shares.
    See supabase_handler.bump_scan_report()'s docstring.

    2026-09 (explicit user request — see excluded_cache.py's module
    docstring): loads this crawl's canonical excluded-jobs cache (already
    downloaded by the CI workflow to EXCLUDED_CACHE_PATH, or simply absent)
    and passes it into filter_locations() so an already-known-excluded
    job's URL never triggers another LLM call. Wrapped in try/finally so
    this shard's own newly-excluded findings are always written out to its
    per-shard partial file — via save_new_exclusions(), itself a no-op on
    an empty set — on EVERY exit path (an early "nothing found" return, a
    genuine "no global jobs this run" return, or even an exception after
    filter_locations already ran), not just the happy path at the bottom."""
    excluded_urls = excluded_cache.load_excluded_cache(EXCLUDED_CACHE_PATH)
    new_exclusions: set = set()
    try:
        all_jobs: list[dict] = []
        boards_ok = boards_failed = 0
        boards_with_roles: set[tuple[str, str]] = set()

        if boards:
            log.info(f"── Crawling entries ({len(boards)} boards across "
                     f"{len(set(a for a, _ in boards))} ATS platforms) ──")
            all_jobs, boards_ok, boards_failed, boards_with_roles = scrape_all(boards)
            # 2026-09 BUG FIX: moved here, unconditional, from deep inside
            # the role-filter branch below (see that spot's own removed
            # comment) — this used to only fire when csm_jobs was
            # non-empty, so a shard with real scrape failures but zero
            # CSM/AM roles this run silently never printed it at all. Also
            # widened from "Workday only" to every platform: scrape_board()
            # (ats_scrapers.py) now records every platform's raised
            # failures into the same grouped collector, not just
            # Workday's, so this one call surfaces all of them.
            log_scrape_failure_summary()

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
            bump_scan_report(SOURCE_PIPELINE, boards_scanned=boards_ok, boards_failed=boards_failed)
            return

        raw_scraped_count = len(all_jobs)

        # 2026-08: dedup against Supabase BEFORE classification — a job
        # whose URL is already in `jobs` is one we've already classified
        # in a prior run; sending it through keyword_classify_role/
        # ai_classify_roles/ai_classify_locations again just burns LLM
        # calls (and time) to re-derive an answer we already have. Only
        # genuinely new URLs go on to filter_roles() below; already-known
        # ones just get last_seen/is_active refreshed directly.
        log.info("── Deduplication ──")
        # 2026-10 (pipeline audit): the per-URL metadata also tells us which
        # rules version classified each stored row, so rows written under
        # OLDER rules get one deterministic, veto-only re-check below — see
        # revalidate.py's module docstring for why (a classifier fix used to
        # never reach jobs that were already stored).
        known_meta = get_known_jobs_meta()
        existing_urls = UrlSet(known_meta) if known_meta else get_existing_urls()
        new_jobs, already_seen = split_known_jobs(all_jobs, existing_urls)
        if already_seen:
            log.info(f"  {len(already_seen)}/{raw_scraped_count} jobs already known — "
                     f"skipping LLM classification, just refreshing last_seen")
            stale, rest = revalidate.select_stale(already_seen, known_meta, CLASSIFIER_VERSION)
            if stale:
                log.info(f"── Re-validation ({len(stale)} stored jobs classified under older rules "
                         f"than v{CLASSIFIER_VERSION}) ──")
                stale = enrich_descriptions(stale)
                stale = enrich_application_questions(stale)
                results = revalidate.evaluate(stale, known_meta)
                revalidate.summarize(results, "Crawl I")
                veto_jobs, stamp_jobs, retry_jobs = revalidate.plan(results)
                mark_jobs_vetoed(veto_jobs)
                touch_seen_jobs_raw(stamp_jobs, classifier_version=CLASSIFIER_VERSION)
                touch_seen_jobs_raw(rest + retry_jobs)
            else:
                touch_seen_jobs_raw(already_seen)
        if not new_jobs:
            log.info("No new (previously unseen) jobs to classify.")
            bump_scan_report(
                SOURCE_PIPELINE, boards_scanned=boards_ok, boards_failed=boards_failed,
                total_jobs_raw=raw_scraped_count, duplicates=len(already_seen),
            )
            return
        all_jobs = new_jobs

        # Filter for CSM/AM roles
        log.info("── Role check (is this a CSM/AM role?) ──")
        csm_jobs = filter_roles(all_jobs)
        if not csm_jobs:
            log.info("No CSM/AM roles found.")
            bump_scan_report(
                SOURCE_PIPELINE, boards_scanned=boards_ok, boards_failed=boards_failed,
                total_jobs_raw=raw_scraped_count,
            )
            return

        log.info("── Location check (open to global/Africa hires?) ──")
        # Enrich descriptions for platforms that lack them
        log.info("  fetching descriptions for jobs missing them...")
        csm_jobs = enrich_descriptions(csm_jobs)

        # Fetch application questions for EVERY job (2026-09 ROUND 2,
        # explicit user instruction — previously only "unsure"-location jobs
        # got this), across all 20 ATS platforms (multi-tier fallback — see
        # ats_scrapers.py). Work authorization questions help both the
        # keyword classifier's hard overrides and the AI stage detect
        # country-restricted roles.
        log.info("  enriching application questions across all ATS platforms...")
        csm_jobs = enrich_application_questions(csm_jobs)

        global_jobs, confidences = filter_locations(csm_jobs, excluded_urls=excluded_urls,
                                                      new_exclusions=new_exclusions)
        if not global_jobs:
            log.info("No global/Africa-eligible CSM/AM roles found.")
            bump_scan_report(
                SOURCE_PIPELINE, boards_scanned=boards_ok, boards_failed=boards_failed,
                total_jobs_raw=raw_scraped_count, csm_roles=len(csm_jobs),
            )
            return

        # Detect visa sponsorship from descriptions (before discarding them)
        for job in global_jobs:
            job["visa_sponsorship"] = detect_visa_sponsorship(job)

        # Push to Supabase — pass the already-fetched existing_urls through
        # so add_jobs_batch doesn't re-pull the whole `jobs` table again.
        # Single write for the whole shard, same as it's always been here —
        # crawl_ii.py's 2026-09 restructure (see its crawl_batch_ii
        # docstring) brought IT in line with this, not the other way round.
        log.info("── Writing to Supabase ──")
        added, _inserted_rows = add_jobs_batch(global_jobs, confidences, existing_urls=existing_urls)
        log.info(f"  {added} new jobs written")

        # Finalize this run's report. `duplicates` now counts BOTH kinds:
        # pre-classification skips (already_seen) and any post-classification
        # re-matches (global_jobs that still weren't a true first-insert —
        # should be rare now, but not impossible with in-run URL reuse).
        duplicates = len(already_seen) + (len(global_jobs) - added)
        bump_scan_report(
            SOURCE_PIPELINE,
            boards_scanned=boards_ok,
            boards_failed=boards_failed,
            total_jobs_raw=raw_scraped_count,
            csm_roles=len(csm_jobs),
            global_jobs=len(global_jobs),
            new_jobs_added=added,
            duplicates=duplicates,
        )

        log.info("── Summary ──")
        log.info(f"  {added} new jobs added to Supabase.")
        log.info(f"  Pipeline: {raw_scraped_count} scraped ({len(already_seen)} already known, "
                 f"skipped) -> {len(all_jobs)} new -> {len(csm_jobs)} CSM/AM -> "
                 f"{len(global_jobs)} global -> {added} new")

    except Exception as e:
        log.error(f"Scanner failed: {e}")
        bump_scan_report(SOURCE_PIPELINE, status="failed")
        raise
    finally:
        excluded_cache.save_new_exclusions(_excluded_cache_shard_path(shard), new_exclusions)


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
    pipeline's cleanup should be able to touch the other's rows.

    2026-09: delete_days dropped again, from 31 to 3 (per explicit user
    instruction: "if a role is not found, after three runs, that's days 1
    through 3, it should be deleted, cause its likely closed"). inactive_days
    dropped to match (3) rather than left at 30 — with delete_days now
    also 3, a 30-day inactive_days would be dead code: cleanup_stale_jobs
    runs its mark-inactive pass and its hard-delete pass in the same call,
    so any row old enough to hard-delete at day 3 would never have lived
    long enough to hit a 30-day inactive_cutoff first. This assumes crawl_i
    runs roughly daily — three consecutive runs where a job's board no
    longer lists it (last_seen not refreshed) now means it's gone, not
    just "not re-confirmed in the last month"."""
    log.info("=" * 60)
    log.info("CRAWL I — finalize (cleanup stale jobs)")
    log.info("=" * 60)
    summary = cleanup_stale_jobs(inactive_days=3, delete_days=3, source_pipeline=SOURCE_PIPELINE)
    log.info(f"Crawl I finalize summary: inactive cutoff {summary['inactive_cutoff']} "
             f"(ok={summary['mark_inactive_ok']}), delete cutoff {summary['delete_cutoff']} "
             f"(ok={summary['delete_ok']})")
    # 2026-09: closes out today's single scan_reports row for crawl_i —
    # finished_at + status='completed' (unless a shard already marked it
    # 'failed' via bump_scan_report — see that function's docstring).
    finish_scan_report_for_pipeline(SOURCE_PIPELINE)


def merge_excluded_cache() -> None:
    """Excluded-jobs cache finalize pass — call this ONCE, after every
    Crawl I shard has finished (same `needs`-gated timing as run_finalize()
    above, called right alongside it from postfix_notion.py). By the time
    this runs, the CI workflow has already downloaded every shard's own
    newly-excluded partial file (EXCLUDED_CACHE_SHARD_GLOB) AND the
    existing canonical cache (EXCLUDED_CACHE_PATH, or nothing if this is
    the first run ever) into the current working directory — this function
    only does the actual Python merge, writing the result back to
    EXCLUDED_CACHE_PATH for the workflow to publish as the new canonical
    Release asset. See excluded_cache.py's module docstring for the full
    design and the merge semantics (TTL filtering, first-entry-wins on a
    same-run duplicate)."""
    count = excluded_cache.merge_excluded_caches(
        existing_path=EXCLUDED_CACHE_PATH,
        shard_glob=EXCLUDED_CACHE_SHARD_GLOB,
        output_path=EXCLUDED_CACHE_PATH,
    )
    log.info(f"Crawl I excluded-cache finalize: {count} URLs cached as known-excluded")


def main():
    parser = argparse.ArgumentParser(description="Crawl I — ATS Global Scanner")
    parser.add_argument("--shard", type=int, default=0,
                         help="This shard's index (0-based), for GitHub Actions matrix parallelism")
    parser.add_argument("--total-shards", type=int, default=1,
                         help="Total number of shards; each processes ~1/N of the ATS boards")
    parser.add_argument("--finalize", action="store_true",
                         help="Only run cleanup (mark/delete stale jobs) — call once after all shards finish")
    parser.add_argument("--ats-only", type=str, default="",
                         help="2026-09 TEMPORARY (async migration testing): comma-separated "
                              "ATS names (e.g. 'greenhouse,lever,ashby,workable') to restrict "
                              "this run to — every other platform's boards are skipped before "
                              "scraping starts. Lets a real end-to-end test (real network, real "
                              "Supabase writes, real LLM classification) exercise ONLY the "
                              "platforms whose scrape_* function has been converted to async so "
                              "far, without the cost/time of scraping the ~34 platforms not yet "
                              "converted. Remove this flag once the full async migration lands "
                              "and every platform is converted.")
    parser.add_argument("--limit", type=int, default=0,
                         help="2026-09 TEMPORARY (async migration testing): cap the number of "
                              "boards scraped this run to N (applied AFTER --ats-only, so "
                              "'--ats-only greenhouse,lever,ashby,workable --limit 40' scrapes "
                              "only ~10 real boards per platform) -- for a cheap smoke test "
                              "against real network/Supabase/LLM cost without a full run. "
                              "0 (default) = no limit. Remove this flag once the full async "
                              "migration lands.")
    args = parser.parse_args()
    ats_only = {a.strip().lower() for a in args.ats_only.split(",") if a.strip()}

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
        merge_excluded_cache()
        return

    log.info("── Getting entries ──")
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
    if ats_only:
        before = len(boards)
        boards = [(a, s) for a, s in boards if a.lower() in ats_only]
        log.info(f"  --ats-only filter ({', '.join(sorted(ats_only))}): "
                 f"{before} -> {len(boards)} boards")

    if args.limit and len(boards) > args.limit:
        by_ats = {}
        for pair in boards:
            by_ats.setdefault(pair[0], []).append(pair)
        per_ats_cap = max(1, args.limit // max(1, len(by_ats)))
        capped = []
        for ats, pairs in by_ats.items():
            capped.extend(pairs[:per_ats_cap])
        boards = capped[:args.limit]
        log.info(f"  --limit {args.limit}: capped to {len(boards)} boards "
                 f"(~{per_ats_cap}/platform across {len(by_ats)} platform(s))")

    log.info(f"  {len(boards)} boards assigned to this shard")
    if not boards:
        if args.total_shards == 1:
            log.error("No boards to scrape.")
            return
        # A single shard legitimately CAN come back empty (e.g. more
        # shards than boards on some platform) — not an error, just
        # nothing for this shard to do.
        log.warning(f"Shard {args.shard}/{args.total_shards}: no boards assigned, skipping ATS scrape.")

    _run_pipeline(boards, shard=args.shard)

    # Only the unsharded, manual/local full run does cleanup inline.
    # Sharded CI runs call `--finalize` as their own separate, `needs`-gated
    # step instead (see run_finalize() docstring for why that matters).
    if args.total_shards == 1:
        run_finalize()
        merge_excluded_cache()

    log_egress_summary(label=f"crawl_i shard {args.shard}/{args.total_shards}")


if __name__ == "__main__":
    main()
