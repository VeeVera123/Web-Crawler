"""
CRAWL III — direct job-board consumer for stapply.ai's bulk, pre-scraped
CSVs (Phenom, UKG, SAP/SuccessFactors, Dayforce, Eightfold, Recruitee,
MokaHR).

2026-09 (explicit user instruction): a third, independent crawl pipeline,
alongside Crawl I (crawl_i.py — known-ATS live scanner) and Crawl II
(crawl_ii.py — in-house/unsupported career-page heuristic scanner). All
three feed the SAME `jobs` table and the SAME classifier.py functions —
role classification (keyword_classify_role/ai_classify_roles +
classify_role_category), location classification
(_keyword_classify_location_detail/ai_classify_locations), and visa-
sponsorship detection (detect_visa_sponsorship) — tagged
jobs.source_pipeline='crawl_iii' so each pipeline's own finalize/cleanup
pass only ever touches its own rows (see run_finalize() below).

WHY THIS IS DIFFERENT FROM CRAWL I/II, AND WHY IT'S FASTER PER JOB:
Crawl I/II scrape live ATS boards directly and then have to separately
FETCH each job's full description/application questions (see
ats_scrapers.enrich_descriptions()/enrich_application_questions()) because
many ATS list endpoints return only a title + a location stub. stapply.ai
(https://data.stapply.ai — built on kalil0321/ats-scrapers) has already
done that scraping for us, once, for its entire dataset, and republishes
the result as one CSV per ATS platform at
https://storage.stapply.ai/jobhive/v1/{source}/jobs.csv — full title,
company, location, and DESCRIPTION already inline. So Crawl III's pipeline
is: fetch 7 CSVs (async, aiohttp) -> parse -> classify -> write. No
per-job HTTP round-trip at all, for either the initial scrape OR the
enrichment step Crawl I/II both need — literally "just regex [and AI], not
fetching," per the explicit design instruction this file was built from.

SEVEN SOURCES, WHY EACH IS GENUINE INCREMENTAL COVERAGE:
  - phenom          — no scraper/discovery support of our own at all.
  - ukg             — no scraper/discovery support of our own at all.
  - successfactors (the "SAP" ask — stapply has no separate "sap" key) —
                      our own coverage is 100% opportunistic (a content-
                      fingerprint match in node.py when it happens to crawl
                      a SuccessFactors company page for some other reason).
                      SuccessFactors Career Site Builder tenants live on
                      the customer's OWN branded domain (no shared vendor
                      suffix like *.myworkdayjobs.com), so there is no
                      possible bulk sweep on our end — this is genuinely
                      new coverage, not overlap.
  - dayforce        — we have slug-DISCOVERY only (_url_to_slug_dayforce
                      in discovery.py's URL_TO_SLUG); no scraper exists in
                      ats_scrapers.SCRAPERS today. stapply gives us actual
                      Dayforce job rows for the first time.
  - eightfold       — no scraper/discovery support of our own at all.
  - recruitee       — the one source here we ALREADY scrape live
                      (ats_scrapers.scrape_recruitee, one of the original
                      12 kalil0321-supported platforms) — included anyway
                      per explicit instruction, as a faster supplemental
                      source alongside our own scraper, not a replacement
                      for it.
  - moka            — MokaHR (app.mokahr.com/social-recruitment/...), a
                      Chinese ATS platform. No live scraper/discovery of
                      our own — MokaHR's own API (mokahr.com/docs/api) is
                      per-employer OAuth-authenticated, no anonymous
                      cross-tenant endpoint found. stapply's CSV (31,943
                      rows, confirmed live via manifest.json) is the only
                      practical way to get this coverage. NOTE this source
                      is the exception to the "English/US-market" language
                      assumption below — see LANGUAGE FILTERING.

LANGUAGE FILTERING (explicit, deliberate, and SCOPED TO THIS FILE ONLY):
no language-detection library is used here. classifier.py's role-keyword
regexes (CS_KEYWORDS/AM_KEYWORDS/etc. — see keyword_classify_role) are
English-only phrases ("customer success", "account manager", ...) — a
non-English title essentially never matches, so it's filtered out at
filter_roles() before it ever reaches location classification, same as
any other title that isn't CSM/AM/PM/OM. Six of these seven sources are
enterprise HR-suite platforms whose postings skew heavily English/US-
market, so the volume this misses is expected to be small. The seventh,
moka, is the opposite case: MokaHR's customer base (Trip.com, SHEIN,
Zhihu, BIGO, ...) skews heavily Chinese-market/Chinese-language, so this
source's postings will mostly self-filter out at filter_roles() as non-
English titles rather than genuinely lacking CSM/AM/PM/OM roles — this is
expected and fine (same self-filtering behavior, just a much higher miss
rate for this one source than the other six), not a bug to chase. Per
explicit instruction, none of this is a general policy. A future
EURES/Bundesagentur/jobs.ch/jobbank.gc.ca-style batch (non-English-market
job boards) remains a SEPARATE, not-yet-authorized effort that would need
a real language-ID step (langdetect/fastText) — do not extend this file's
English-only assumption to that batch.

AGGRESSIVE STALENESS POLICY (explicit instruction — "today or latest the
day before"): stapply's CSV schema has NO closed/status/is_active field —
a posting that's been filled or pulled just silently vanishes from the
next day's CSV with no signal at all (confirmed via manifest.json schema
research). So instead of Crawl I/II's 3-day inactive/delete cutoffs (which
assume live re-confirmation might occasionally miss a day), Crawl III uses
INACTIVE_DAYS = DELETE_DAYS = 1 — a job not re-seen in TODAY's run is
marked inactive AND hard-deleted in the very same finalize pass. This
guarantees any surviving crawl_iii row in Supabase was found today or, at
worst, yesterday — never a stale leftover from a stapply.ai posting that's
already gone. See run_finalize() for the matching Notion-page cleanup this
requires (stapply rows have no equivalent "closed" webhook, so we can only
find out a job disappeared by it not showing up in the next CSV pull).

SHARDING: unlike Crawl I (server-side, per-slug, via a Supabase RPC) there
is no per-row API to shard against here — stapply publishes one bulk CSV
file per platform, not a paginated/queryable endpoint. Each shard fetches
the SAME 7 CSVs in full (small relative to Crawl I's ~87K live per-company
HTTP round-trips — this is 6 bulk downloads, not thousands) and then keeps
only its own ~1/total_shards slice, selected by hashing each job's URL
(_shard_of()) — deterministic and stable across shards/runs, same
technique crawl_i.py's own _shard_of() uses for (ats, slug) pairs. This
distributes the (comparatively expensive) role/location AI-classification
work evenly across shards even though the CSV download itself is
duplicated per shard.

CLI modes (mirrors crawl_i.py exactly — see .github/workflows/crawl.yml):
  python crawl_iii.py                                  Full run: all 6 sources + cleanup,
                                                          one process. Manual/local default.
  python crawl_iii.py --shard 0 --total-shards 8        This shard's ~1/8 slice only.
                                                          No cleanup (see run_finalize()).
  python crawl_iii.py --finalize                        Cleanup only (mark inactive + hard-delete
                                                          any crawl_iii row not re-seen today,
                                                          archiving its Notion page first). Run
                                                          ONCE, after every shard has finished.
"""

import argparse
import asyncio
import csv
import hashlib
import io
import logging
import sys

import aiohttp

import notion_sync
from ats_scrapers import _snippet
from classifier import detect_visa_sponsorship
from crawl_i import filter_roles, filter_locations
from supabase_handler import (
    add_jobs_batch, bump_scan_report, finish_scan_report_for_pipeline,
    get_existing_urls, touch_seen_jobs_raw,
    cleanup_stale_jobs, get_stale_job_ids,
    log_egress_summary,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SOURCE_PIPELINE = "crawl_iii"

# Same-day staleness policy — see module docstring's "AGGRESSIVE STALENESS
# POLICY" section for why these are both 1, not staggered like Crawl I/II's
# 3/3.
INACTIVE_DAYS = 1
DELETE_DAYS = 1

# One bulk CSV per ATS platform. Confirmed live 2026-09 via stapply.ai's
# manifest.json (https://storage.stapply.ai/jobhive/v1/manifest.json) —
# all 6 follow the identical {source}/jobs.csv path pattern.
STAPPLY_BASE = "https://storage.stapply.ai/jobhive/v1"
STAPPLY_SOURCES = {
    "phenom": f"{STAPPLY_BASE}/phenom/jobs.csv",
    "ukg": f"{STAPPLY_BASE}/ukg/jobs.csv",
    "successfactors": f"{STAPPLY_BASE}/successfactors/jobs.csv",
    "dayforce": f"{STAPPLY_BASE}/dayforce/jobs.csv",
    "eightfold": f"{STAPPLY_BASE}/eightfold/jobs.csv",
    "recruitee": f"{STAPPLY_BASE}/recruitee/jobs.csv",
    # 2026-09 (explicit user request: "Add MokaHR ... if you can't add the
    # scraper directly, just add it to crawl 3 since scraply has it"):
    # MokaHR (app.mokahr.com/social-recruitment/{company}/{id}) has no
    # public/anonymous per-company API of our own to build a live scraper
    # from — a real search this session found only an OAuth-authenticated
    # employer API (mokahr.com/docs/api) and a third-party project
    # (gzchenhao/openhire) that has Moka on its roadmap but hasn't built it
    # yet. But stapply's own manifest.json (confirmed live) lists "moka" as
    # a real source: 31,943 rows, last updated 2026-09-21, same 25-column
    # schema as every other source here (url, title, company, location,
    # description, ...) — so it drops straight into the existing
    # source-agnostic _row_to_job()/parse_source_csv() with no special-
    # casing needed, same as the other 6.
    "moka": f"{STAPPLY_BASE}/moka/jobs.csv",
}

_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=180, connect=30)


# ── Fetch (async/aiohttp, per module design instruction) ────────────────

async def _fetch_one(session: aiohttp.ClientSession, source: str, url: str) -> tuple[str, str | None]:
    try:
        async with session.get(url, timeout=_FETCH_TIMEOUT) as resp:
            if resp.status != 200:
                log.warning(f"[{source}] stapply CSV fetch failed: HTTP {resp.status}")
                return source, None
            text = await resp.text()
            return source, text
    except Exception as e:
        log.warning(f"[{source}] stapply CSV fetch error: {e}")
        return source, None


async def _fetch_all_sources() -> dict[str, str]:
    """Fetches all 6 stapply CSVs concurrently. One shared session/
    connector — these are 6 large one-shot GETs to the same host, not a
    fan-out over many small requests, so a small connection limit is
    plenty (no per-platform worker tuning needed the way ats_scrapers.py's
    live per-company scraping requires)."""
    connector = aiohttp.TCPConnector(limit=6)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [asyncio.create_task(_fetch_one(session, source, url))
                 for source, url in STAPPLY_SOURCES.items()]
        results = await asyncio.gather(*tasks)
    return {source: text for source, text in results if text is not None}


# ── Parse + field-mapping (stapply schema -> this project's job dict) ───
# stapply's ~26-column schema (per manifest research): url, title, company,
# ats_type, ats_id, location, country_iso, region, language, lat, lon,
# is_remote, salary_min, salary_max, salary_currency, salary_period,
# employment_type, department, team, description, posted_at,
# requisition_id, apply_url, commitment, raw, fetched_at.
#
# Mapped onto this project's standard job dict shape (see ats_scrapers.py's
# module docstring: title, url, company, location, department,
# workplace_type, employment_type, salary, description_snippet, source_ats,
# slug) — only the fields classifier.py/supabase_handler.py actually read.

def _shard_of(url: str, total_shards: int) -> int:
    h = hashlib.md5(url.encode()).hexdigest()
    return int(h, 16) % total_shards


def _build_salary_str(row: dict) -> str:
    smin, smax = (row.get("salary_min") or "").strip(), (row.get("salary_max") or "").strip()
    if not smin and not smax:
        return ""
    currency = (row.get("salary_currency") or "").strip()
    period = (row.get("salary_period") or "").strip()
    span = f"{smin or '?'}-{smax or '?'}" if (smin or smax) else ""
    return " ".join(p for p in (span, currency, period) if p)


def _row_to_job(source: str, row: dict) -> dict | None:
    url = (row.get("url") or row.get("apply_url") or "").strip()
    title = (row.get("title") or "").strip()
    if not url or not title:
        return None

    location = (row.get("location") or "").strip()
    if not location and (row.get("is_remote") or "").strip().lower() in ("1", "true", "yes"):
        location = "Remote"
    country = (row.get("country_iso") or row.get("region") or "").strip()

    return {
        "title": title,
        "url": url,
        "company": (row.get("company") or "").strip(),
        "location": location,
        "country": country,
        "department": (row.get("department") or row.get("team") or "").strip(),
        "employment_type": (row.get("employment_type") or row.get("commitment") or "").strip(),
        "salary": _build_salary_str(row),
        # stapply's description field is typically HTML — reuse
        # ats_scrapers._snippet() for the same strip/decode/cap treatment
        # every live scraper already applies, so classifier.py sees the
        # same kind of text regardless of which pipeline found the job.
        "description_snippet": _snippet(row.get("description") or ""),
        "source_ats": source,
        "slug": (row.get("ats_id") or row.get("requisition_id") or "").strip(),
    }


def parse_source_csv(source: str, text: str, shard: int, total_shards: int) -> list[dict]:
    jobs: list[dict] = []
    malformed = 0
    reader = csv.DictReader(io.StringIO(text))
    for raw_row in reader:
        try:
            job = _row_to_job(source, raw_row)
        except Exception:
            malformed += 1
            continue
        if not job:
            continue
        if total_shards > 1 and _shard_of(job["url"], total_shards) != shard:
            continue
        jobs.append(job)
    if malformed:
        log.warning(f"[{source}] skipped {malformed} malformed CSV rows")
    return jobs


# ── Pipeline ──────────────────────────────────────────────

def _run_pipeline(shard: int, total_shards: int) -> None:
    """Shared core: fetch -> parse/shard -> dedup -> filter_roles (role
    classification, and — per the module docstring's "LANGUAGE FILTERING"
    section — the implicit English-only gate) -> filter_locations
    (location classification) -> visa detection -> push. Does NOT run
    cleanup_stale_jobs() — see run_finalize() for why that's split out,
    same reasoning as crawl_i.py's own run_finalize().

    2026-09: scan-report accounting uses bump_scan_report(SOURCE_PIPELINE,
    ...) rather than a per-shard start_scan_report()/finish_scan_report()
    row — see supabase_handler.bump_scan_report()'s docstring for why (one
    new Supabase row per shard made the table useless as a daily
    summary)."""
    try:
        mode_note = f" (shard {shard}/{total_shards})" if total_shards > 1 else ""
        log.info(f"── Fetching stapply.ai CSVs ({len(STAPPLY_SOURCES)} sources){mode_note} ──")
        raw_texts = asyncio.run(_fetch_all_sources())
        missing = [s for s in STAPPLY_SOURCES if s not in raw_texts]
        if missing:
            log.warning(f"  failed to fetch: {', '.join(missing)} — continuing with the rest")
        if not raw_texts:
            log.error("No stapply sources fetched — aborting this shard.")
            bump_scan_report(SOURCE_PIPELINE, status="failed")
            return

        all_jobs: list[dict] = []
        for source, text in raw_texts.items():
            source_jobs = parse_source_csv(source, text, shard, total_shards)
            log.info(f"  {source}: {len(source_jobs)} rows this shard")
            all_jobs.extend(source_jobs)

        if not all_jobs:
            log.info("No jobs parsed from any stapply source this shard.")
            bump_scan_report(SOURCE_PIPELINE)
            return

        raw_count = len(all_jobs)

        # Same pre-classification dedup pattern as crawl_i.py/crawl_ii.py —
        # a URL already in `jobs` was already classified by SOME pipeline
        # before; just refresh last_seen rather than re-spending LLM calls.
        log.info("── Deduplication ──")
        existing_urls = get_existing_urls()
        new_jobs, already_seen = [], []
        for job in all_jobs:
            if job["url"] in existing_urls:
                already_seen.append(job)
            else:
                new_jobs.append(job)
        if already_seen:
            log.info(f"  {len(already_seen)}/{raw_count} jobs already known — "
                     f"skipping LLM classification, just refreshing last_seen")
            touch_seen_jobs_raw(already_seen)
        if not new_jobs:
            log.info("No new (previously unseen) jobs to classify.")
            bump_scan_report(SOURCE_PIPELINE, total_jobs_raw=raw_count, duplicates=len(already_seen))
            return

        log.info("── Role check (is this a CSM/AM/PM/OM role?) ──")
        csm_jobs = filter_roles(new_jobs)
        if not csm_jobs:
            log.info("No CSM/AM/PM/OM roles found.")
            bump_scan_report(SOURCE_PIPELINE, total_jobs_raw=raw_count)
            return

        # No enrich_descriptions()/enrich_application_questions() step —
        # stapply's CSVs already carry the full description inline (see
        # module docstring). This is the entire reason Crawl III is faster
        # per job than Crawl I/II: zero extra network round-trips here.
        log.info("── Location check (open to global/Africa hires?) ──")
        global_jobs, confidences = filter_locations(csm_jobs)
        if not global_jobs:
            log.info("No global/Africa-eligible CSM/AM/PM/OM roles found.")
            bump_scan_report(SOURCE_PIPELINE, total_jobs_raw=raw_count, csm_roles=len(csm_jobs))
            return

        for job in global_jobs:
            job["visa_sponsorship"] = detect_visa_sponsorship(job)

        log.info("── Writing to Supabase ──")
        added, _inserted_rows = add_jobs_batch(
            global_jobs, confidences, source_pipeline=SOURCE_PIPELINE, existing_urls=existing_urls,
        )
        log.info(f"  {added} new jobs written (source_pipeline={SOURCE_PIPELINE!r})")

        duplicates = len(already_seen) + (len(global_jobs) - added)
        bump_scan_report(
            SOURCE_PIPELINE,
            total_jobs_raw=raw_count,
            csm_roles=len(csm_jobs),
            global_jobs=len(global_jobs),
            new_jobs_added=added,
            duplicates=duplicates,
        )

        log.info("── Summary ──")
        log.info(f"  {added} new jobs added to Supabase.")
        log.info(f"  Pipeline: {raw_count} scraped ({len(already_seen)} already known, "
                 f"skipped) -> {len(new_jobs)} new -> {len(csm_jobs)} CSM/AM/PM/OM -> "
                 f"{len(global_jobs)} global -> {added} new")

    except Exception as e:
        log.error(f"Crawl III failed: {e}")
        bump_scan_report(SOURCE_PIPELINE, status="failed")
        raise


def run_finalize() -> None:
    """Cleanup pass — call ONCE, after every Crawl III shard has finished
    (gate with `needs:` in CI, same reasoning as crawl_i.py's own
    run_finalize()).

    Two steps, in this order:
      1. Find every crawl_iii row not re-seen since DELETE_DAYS ago (i.e.
         about to be hard-deleted by step 2) and archive its Notion page
         FIRST — see supabase_handler.get_stale_job_ids()/notion_sync.
         archive_notion_pages_for_supabase_ids()'s docstrings for why the
         order matters: once the row is gone, its id can't be used to find
         the page anymore.
      2. The regular cleanup_stale_jobs() pass, scoped to source_pipeline=
         'crawl_iii' with INACTIVE_DAYS=DELETE_DAYS=1 (see module
         docstring's "AGGRESSIVE STALENESS POLICY" section for why this is
         far more aggressive than Crawl I/II's 3-day cutoffs)."""
    log.info("=" * 60)
    log.info("CRAWL III — finalize (archive stale Notion pages, then cleanup stale jobs)")
    log.info("=" * 60)

    stale_ids = get_stale_job_ids(cutoff_days=DELETE_DAYS, source_pipeline=SOURCE_PIPELINE)
    if stale_ids:
        log.info(f"  {len(stale_ids)} crawl_iii jobs not re-seen today — "
                 f"archiving their Notion pages before deleting from Supabase")
        notion_sync.archive_notion_pages_for_supabase_ids(stale_ids)
    else:
        log.info("  no stale crawl_iii jobs to archive in Notion this run")

    summary = cleanup_stale_jobs(inactive_days=INACTIVE_DAYS, delete_days=DELETE_DAYS,
                                  source_pipeline=SOURCE_PIPELINE)
    log.info(f"Crawl III finalize summary: inactive cutoff {summary['inactive_cutoff']} "
             f"(ok={summary['mark_inactive_ok']}), delete cutoff {summary['delete_cutoff']} "
             f"(ok={summary['delete_ok']})")
    # 2026-09: closes out today's single scan_reports row for crawl_iii —
    # finished_at + status='completed' (unless a shard already marked it
    # 'failed' via bump_scan_report).
    finish_scan_report_for_pipeline(SOURCE_PIPELINE)


def main():
    parser = argparse.ArgumentParser(description="Crawl III — stapply.ai direct job-board consumer")
    parser.add_argument("--shard", type=int, default=0,
                         help="This shard's index (0-based), for GitHub Actions matrix parallelism")
    parser.add_argument("--total-shards", type=int, default=1,
                         help="Total number of shards; each processes ~1/N of every source's rows")
    parser.add_argument("--finalize", action="store_true",
                         help="Only run cleanup (archive stale Notion pages + mark/delete stale "
                              "jobs) — call once after all shards finish")
    args = parser.parse_args()

    mode_note = ""
    if args.total_shards > 1:
        mode_note = f" (shard {args.shard}/{args.total_shards})"
    elif args.finalize:
        mode_note = " (finalize)"

    log.info("=" * 60)
    log.info(f"CRAWL III — starting{mode_note}")
    log.info("=" * 60)

    if args.finalize:
        run_finalize()
        return

    _run_pipeline(args.shard, args.total_shards)

    # Only the unsharded, manual/local full run does cleanup inline —
    # sharded CI runs call --finalize as their own separate, needs-gated
    # step instead (see run_finalize() docstring).
    if args.total_shards == 1:
        run_finalize()

    log_egress_summary(label=f"crawl_iii shard {args.shard}/{args.total_shards}")


if __name__ == "__main__":
    main()
