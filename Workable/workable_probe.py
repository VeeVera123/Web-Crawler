"""
WORKABLE PROBE — a thin, disposable probe source on top of node.py (the
permanent engine). Reads workable_seed.py's output CSV ({name, domain}),
shards the crawl across N jobs, hands domains to node.crawl_batch(). All
fetch/parse/detect/write logic lives in node.py — fix a bug there once,
every probe/seed source gets the fix.

SEED (2026-09): workable_seed.py pages through jobs.workable.com's public,
unauthenticated jobs API (no query = the whole 170k+-job board) and writes
out each posting's already-embedded company website — see that file's
module docstring for the full research behind why that API (not the
visible search page, not a keyword-rotation workaround) is the right
source. workable.yml's `seed` job runs it and uploads the CSV as a single
GitHub Release asset (small enough here to need no chunking the way PDL's
22M-row file does); the `crawl` job downloads it and points
WORKABLE_DATASET_PATH here at that file.

SHARD-AWARE STREAMING: same approach as people_data_labs_probe.py — the
modulo shard filter is applied INLINE during a single streaming pass over
the CSV (on a running counter of rows that already passed the domain
filter), so each shard only builds the companies list it's actually going
to crawl, without every shard paying the full parse cost of the whole
file. This file's seed CSV is far smaller than PDL's (thousands, not tens
of millions, of rows), so this matters less here in absolute terms, but
keeping the same pattern costs nothing and keeps the probes consistent.

Usage:
    pip install aiohttp aiodns selectolax python-dotenv requests
    python workable_probe.py
    python workable_probe.py --shard-index 0 --shard-count 10
"""
import argparse
import asyncio
import csv
import logging
import os
import sys
import time
from collections import Counter

import aiohttp
from dotenv import load_dotenv

load_dotenv()
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)                       # for node.py
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # for discovery.py
from discovery import SKIP_SLUGS  # noqa: E402
import node  # noqa: E402

log = logging.getLogger("workable_probe")

SOURCE_LABEL = "workable_probe"
WORKABLE_DATASET_PATH = os.environ.get("WORKABLE_DATASET_PATH", "workable_companies.csv")

# 'domain' is this project's own workable_seed.py output; 'website' covers
# the column name if a raw/unfiltered export were ever used instead —
# same defensive-alias pattern as people_data_labs_probe.py.
NAME_COLS = ("name", "company_name")
DOMAIN_COLS = ("domain", "website")

PROGRESS_EVERY = 50_000  # raw rows scanned between progress log lines


def _col_index(header_lower: list[str], aliases: tuple[str, ...]) -> int | None:
    for alias in aliases:
        if alias in header_lower:
            return header_lower.index(alias)
    return None


def read_seed_csv(shard_index: int | None = None, shard_count: int | None = None) -> list[dict]:
    """Single streaming pass over the seed CSV. Applies domain-validity
    filtering, THEN (only if sharding) a modulo filter on a running
    counter of rows that passed that check, so each shard keeps roughly
    1/shard_count of the usable rows regardless of shard_count. Missing
    file logs once and returns empty rather than crashing."""
    if not os.path.exists(WORKABLE_DATASET_PATH):
        log.warning(f"Seed dataset not found at '{WORKABLE_DATASET_PATH}' — nothing to crawl.")
        return []

    out = []
    total_rows = 0
    kept_before_shard = 0
    start = time.monotonic()

    try:
        with open(WORKABLE_DATASET_PATH, newline="", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                log.error("Seed dataset is empty (no header row).")
                return []
            header_lower = [h.strip().lower() for h in header]
            name_i = _col_index(header_lower, NAME_COLS)
            domain_i = _col_index(header_lower, DOMAIN_COLS)
            if name_i is None or domain_i is None:
                log.error(f"Couldn't find a name/domain column in the header: {header}")
                return []
            log.info(f"  columns: name='{header[name_i]}' domain='{header[domain_i]}'")

            for i, row in enumerate(reader):
                total_rows += 1
                if total_rows % PROGRESS_EVERY == 0:
                    elapsed = time.monotonic() - start
                    log.info(f"  ...scanned {total_rows:,} rows ({total_rows / max(elapsed, 0.001):,.0f} rows/sec), "
                             f"{kept_before_shard:,} usable so far")
                if len(row) <= domain_i or len(row) <= name_i:
                    continue

                name = row[name_i].strip()
                domain = row[domain_i].strip().lower()
                if not (name and domain and "." in domain and domain not in SKIP_SLUGS):
                    continue

                keep_this_shard = (shard_index is None or shard_count is None
                                    or kept_before_shard % shard_count == shard_index)
                kept_before_shard += 1
                if keep_this_shard:
                    out.append({"name": name, "domain": domain})
    except Exception as e:
        log.error(f"Failed to read seed dataset: {e}")
        return []

    elapsed = time.monotonic() - start
    shard_note = f", {len(out):,} in this shard" if shard_index is not None else ""
    log.info(f"Seed dataset: {total_rows:,} total rows, {kept_before_shard:,} usable "
             f"({kept_before_shard / max(total_rows, 1) * 100:.1f}%){shard_note}, parsed in {elapsed:.1f}s")
    return out


async def run_crawl(shard_index: int | None = None, shard_count: int | None = None,
                     concurrency: int = node.CRAWL_CONCURRENCY,
                     time_budget_minutes: int = node.TIME_BUDGET_MINUTES,
                     restart_index: int | None = None) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(f"── Workable probe{label} ──")
    log.info(f"  concurrency={concurrency}  parse_workers={node.PARSE_WORKERS}  "
             f"time_budget={time_budget_minutes}min  source={SOURCE_LABEL}")
    time_budget_seconds = time_budget_minutes * 60

    companies = read_seed_csv(shard_index=shard_index, shard_count=shard_count)
    if not companies:
        log.error("  No seed companies with a usable domain — aborting.")
        return

    domains = [c["domain"] for c in companies]

    connector = node.new_connector()
    sem = asyncio.Semaphore(concurrency)
    stats = Counter()
    found_rows: list[dict] = []
    parse_pool = node.new_parse_pool()
    crawl_start = time.monotonic()

    start_at = 0
    try:
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
            if shard_index is not None and shard_count is not None:
                if restart_index == 0:
                    log.info("  restart_index=0 — forcing a full restart of this shard, ignoring any checkpoint")
                    await node.clear_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count)
                elif restart_index:
                    start_at = restart_index
                    log.info(f"  restart_index={start_at:,} (manual override) — skipping ahead in this shard")
                else:
                    start_at = await node.load_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count)
                    if start_at:
                        log.info(f"  resuming from checkpoint: {start_at:,}/{len(domains):,} companies in this "
                                 f"shard already done — skipping straight past them")
                if start_at >= len(domains):
                    log.info("  checkpoint shows this shard is already fully done — nothing left to crawl.")
                    return
                domains = domains[start_at:]
            # capture_inhouse=True — same reasoning as every other probe
            # with no size signal of its own (OpenData/Common Crawl): no
            # employee-count floor, so archive_ii eligibility goes through
            # node.py's Quality Index gate instead.
            _, elapsed, rate, time_budget_hit = await node.crawl_batch(
                domains, session, sem, stats, parse_pool, node.ACCEPT_ANY_COUNTRY,
                SOURCE_LABEL, found_rows, crawl_start, time_budget_seconds,
                time_budget_minutes, batch_size=1000, unit_label="companies",
                capture_inhouse=True, shard_index=shard_index, shard_count=shard_count, start_at=start_at)
    finally:
        parse_pool.shutdown(wait=True)

    hit_n = stats["hits_from_homepage"] + stats["hits_from_career_path"] + stats["hits_from_sitemap"]
    companies_n = max(stats["companies_attempted"], 1)
    status = "STOPPED EARLY (time budget)" if time_budget_hit else "complete"
    log.info(f"── shard{label} {status}: {stats['companies_attempted']}/{len(domains)} companies this run "
             f"({start_at:,} skipped from a prior checkpoint, {len(companies):,} total in shard), "
             f"{elapsed:.0f}s, {rate:.1f}/sec, hit={hit_n / companies_n * 100:.1f}% ({hit_n}) ──")
    ats_breakdown = Counter(r["ats"] for r in found_rows)
    if ats_breakdown:
        log.info(f"  by platform: {dict(ats_breakdown.most_common())}")
    node.log_quality_index_summary(stats)


def main():
    parser = argparse.ArgumentParser(description="Workable seed-CSV domain-crawl discovery (async)")
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=node.CRAWL_CONCURRENCY)
    parser.add_argument("--time-budget-minutes", type=int, default=node.TIME_BUDGET_MINUTES)
    parser.add_argument("--restart-index", type=int, default=None,
                         help="Per-shard resume. Omit (default) to auto-resume from this shard's own "
                              "Supabase checkpoint. 0 forces a full restart, ignoring any checkpoint. "
                              "A positive value manually overrides the checkpoint for this run.")
    args = parser.parse_args()
    asyncio.run(run_crawl(args.shard_index, args.shard_count, args.concurrency,
                           args.time_budget_minutes, args.restart_index))


if __name__ == "__main__":
    main()
