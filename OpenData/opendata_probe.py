"""
OPENDATA PROBE — a thin, disposable probe source on top of node.py (the
permanent engine), same role people_data_labs_probe.py plays for PDL's dataset.
Copied directly from people_data_labs_probe.py (2026-08) since the shape is
identical: read a {name, domain, country} CSV, hand domains to
node.crawl_batch(). All fetch/parse/detect/write logic lives in node.py —
fix a bug there once, every probe source gets the fix.

Reads the FILTERED, already-small CSV opendata_seed.py produced (and
opendata.yml's workflow downloaded/joined from GitHub Release assets, if
it was split across multiple <2GB chunks — see opendata_seed.py's module
docstring for the full download->filter->upload->rejoin pipeline). Column
names are fixed ("name","domain","country") since opendata_seed.py always
writes that exact header regardless of what the raw upstream source called
its own columns — no alias-detection needed here, unlike people_data_labs_probe.py
reading PDL's file directly.

Usage:
    pip install aiohttp aiodns selectolax python-dotenv
    python opendata_probe.py
    python opendata_probe.py --shard-index 0 --shard-count 15
"""
import argparse
import asyncio
import csv
import logging
import os
import re
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

log = logging.getLogger("opendata_probe")

OPENDATA_FILTERED_PATH = os.environ.get("OPENDATA_FILTERED_PATH", "opendata_organizations_filtered.csv")
OPENDATA_ROW_LIMIT = int(os.environ.get("OPENDATA_ROW_LIMIT", "0"))  # 0 = no cap

# Same {name,domain,country} shape people_data_labs_probe.py/github_org_seed.py
# produce — this env var is the only thing that changes between seed
# files, so rows are correctly attributed regardless of which one found
# them (see archive_i's `source` column / archive_i_source_check).
SEED_SOURCE_LABEL = os.environ.get("SEED_SOURCE_LABEL", "opendata_probe")

# opendata_seed.py always writes exactly this header — no alias detection
# needed (contrast people_data_labs_probe.py, which reads PDL's own raw file and has
# to tolerate 'domain' vs 'website' etc.).
NAME_COL, DOMAIN_COL, COUNTRY_COL = "name", "domain", "country"

# Kept identical to people_data_labs_probe.py's/opendata_seed.py's DEFAULT_COUNTRIES
# deliberately — opendata_seed.py already filtered to this set upstream,
# so this second pass is normally a no-op; it stays here as a safety net
# in case OPENDATA_FILTERED_PATH ever points at an unfiltered/differently
# filtered file, and so --country/--all-countries work the same way
# people_data_labs_probe.py's do if someone wants a narrower crawl than the seed.
DEFAULT_COUNTRIES = {
    "united states", "united kingdom", "canada", "australia",
    "ireland", "new zealand", "singapore",
    "netherlands", "norway", "sweden", "denmark", "finland", "austria", "belgium",
    "iceland", "luxembourg",
    "france", "germany",
}

PROGRESS_EVERY = 2_000_000  # raw rows scanned between progress log lines


def read_seed_csv(limit: int = OPENDATA_ROW_LIMIT, countries: set[str] | None = None,
                   shard_index: int | None = None, shard_count: int | None = None) -> list[dict]:
    """Single streaming pass over opendata_seed.py's filtered CSV — same
    shard-aware inline modulo filter as people_data_labs_probe.py's read_seed_csv
    (see that function's docstring for why: each shard only builds the
    companies list it's actually going to crawl, no full-list materialize-
    then-slice). Missing file logs once and returns empty rather than
    crashing."""
    if not os.path.exists(OPENDATA_FILTERED_PATH):
        log.warning(f"Filtered seed CSV not found at '{OPENDATA_FILTERED_PATH}' — nothing to crawl. "
                    f"Run opendata_seed.py (and download/join its Release asset chunks) first.")
        return []

    countries_lower = {c.strip().lower() for c in countries} if countries else None
    if countries_lower:
        log.info(f"  filtering to {len(countries_lower)} countries")

    out = []
    total_rows = 0
    kept_before_shard = 0
    skipped_wrong_country = 0
    start = time.monotonic()

    try:
        with open(OPENDATA_FILTERED_PATH, newline="", encoding="utf-8", errors="ignore",
                  buffering=1024 * 1024) as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                log.error("Filtered seed CSV is empty (no header row).")
                return []
            header_lower = [h.strip().lower() for h in header]
            try:
                name_i = header_lower.index(NAME_COL)
                domain_i = header_lower.index(DOMAIN_COL)
            except ValueError:
                log.error(f"Expected columns '{NAME_COL}'/'{DOMAIN_COL}' not found in header: {header}")
                return []
            country_i = header_lower.index(COUNTRY_COL) if COUNTRY_COL in header_lower else None

            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break
                total_rows += 1
                if total_rows % PROGRESS_EVERY == 0:
                    elapsed = time.monotonic() - start
                    log.info(f"  ...scanned {total_rows:,} rows ({total_rows / max(elapsed, 0.001):,.0f} rows/sec), "
                              f"{kept_before_shard:,} usable so far")
                if len(row) <= domain_i or len(row) <= name_i:
                    continue

                if country_i is not None and countries_lower:
                    row_country = row[country_i].strip().lower() if len(row) > country_i else ""
                    if row_country not in countries_lower:
                        skipped_wrong_country += 1
                        continue

                name = row[name_i].strip()
                domain = row[domain_i].strip().lower()
                domain = re.sub(r"^https?://", "", domain).split("/")[0].strip()
                if not (name and domain and "." in domain and domain not in SKIP_SLUGS):
                    continue

                keep_this_shard = (shard_index is None or shard_count is None
                                    or kept_before_shard % shard_count == shard_index)
                kept_before_shard += 1
                if keep_this_shard:
                    out.append({"name": name, "domain": domain})
    except Exception as e:
        log.error(f"Failed to read filtered seed CSV: {e}")
        return []

    elapsed = time.monotonic() - start
    filter_note = f", {skipped_wrong_country:,} skipped by country filter" if countries_lower else ""
    shard_note = f", {len(out):,} in this shard" if shard_index is not None else ""
    log.info(f"Filtered seed CSV: {total_rows:,} total rows{filter_note}, {kept_before_shard:,} with a "
             f"usable domain ({kept_before_shard / max(total_rows, 1) * 100:.1f}%){shard_note}, "
             f"parsed in {elapsed:.1f}s")
    return out


async def run_crawl(shard_index: int | None = None, shard_count: int | None = None,
                     concurrency: int = node.CRAWL_CONCURRENCY,
                     time_budget_minutes: int = node.TIME_BUDGET_MINUTES,
                     countries: set[str] | None = None,
                     restart_index: int | None = None) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(f"── OpenData probe{label} ──")
    log.info(f"  concurrency={concurrency}  parse_workers={node.PARSE_WORKERS}  "
             f"time_budget={time_budget_minutes}min  source={SEED_SOURCE_LABEL}")
    time_budget_seconds = time_budget_minutes * 60

    companies = read_seed_csv(countries=countries, shard_index=shard_index, shard_count=shard_count)
    if not companies:
        log.error("  No seed companies with a usable domain — aborting.")
        return

    domains = [c["domain"] for c in companies]
    target_geo_countries = (node.target_countries_geo_form(countries) if countries
                             else node.ACCEPT_ANY_COUNTRY)

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
                    await node.clear_crawl_checkpoint(session, SEED_SOURCE_LABEL, shard_index, shard_count)
                elif restart_index:
                    start_at = restart_index
                    log.info(f"  restart_index={start_at:,} (manual override) — skipping ahead in this shard")
                else:
                    start_at = await node.load_crawl_checkpoint(session, SEED_SOURCE_LABEL, shard_index, shard_count)
                    if start_at:
                        log.info(f"  resuming from checkpoint: {start_at:,}/{len(domains):,} companies in this "
                                 f"shard already done — skipping straight past them")
                if start_at >= len(domains):
                    log.info("  checkpoint shows this shard is already fully done — nothing left to crawl.")
                    return
                domains = domains[start_at:]
            # 2026-09, REVISED: OpenData carries no employee-count/size
            # column at all (never has — this dataset's schema is just
            # name/domain/country), so a no-ATS hit here used to be
            # indistinguishable from a genuine small business and this
            # source fed archive_i only. Now that node.py's company-
            # maturity heuristic exists (HTML signals + a DNS MX check,
            # not employee count), that's a real substitute quality bar —
            # capture_inhouse=True here, with no capture_inhouse_domains
            # set, is exactly what turns the maturity gate ON for this
            # source (see crawl_batch's docstring).
            _, elapsed, rate, time_budget_hit = await node.crawl_batch(
                domains, session, sem, stats, parse_pool, target_geo_countries,
                SEED_SOURCE_LABEL, found_rows, crawl_start, time_budget_seconds,
                time_budget_minutes, batch_size=3000, unit_label="companies",
                capture_inhouse=True,
                shard_index=shard_index, shard_count=shard_count, start_at=start_at)
    finally:
        parse_pool.shutdown(wait=True)

    hit_n = stats['hits_from_homepage'] + stats['hits_from_career_path'] + stats['hits_from_sitemap']
    companies_n = max(stats["companies_attempted"], 1)
    status = "STOPPED EARLY (time budget)" if time_budget_hit else "complete"
    log.info(f"── shard{label} {status}: {stats['companies_attempted']}/{len(domains)} companies this run "
             f"({start_at:,} skipped from a prior checkpoint, {len(companies):,} total in shard), "
             f"{elapsed:.0f}s, {rate:.1f}/sec, hit={hit_n / companies_n * 100:.1f}% "
             f"({hit_n}), unreachable={stats['homepage_unreachable'] / companies_n * 100:.1f}% ──")
    if hit_n:
        log.info(f"  hit source: homepage={stats['hits_from_homepage'] / hit_n * 100:.1f}% "
                 f"career_path={stats['hits_from_career_path'] / hit_n * 100:.1f}% "
                 f"sitemap={stats['hits_from_sitemap'] / hit_n * 100:.1f}%")
    ats_breakdown = Counter(r["ats"] for r in found_rows)
    if ats_breakdown:
        log.info(f"  by platform: {dict(ats_breakdown.most_common())}")
    known_ats_found = stats["known_ats_found"]
    inhouse_captured = stats["inhouse_career_page_captured"]
    career_pages_total = known_ats_found + inhouse_captured
    if career_pages_total:
        log.info(f"  career pages found: {career_pages_total} total — {known_ats_found} known-ats "
                 f"(→ archive_i, as usual) + {inhouse_captured} in-house/unsupported "
                 f"(→ archive_ii, {inhouse_captured / career_pages_total * 100:.1f}% of all career pages found)")


def main():
    parser = argparse.ArgumentParser(description="OpenData.org domain-crawl discovery (async)")
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=node.CRAWL_CONCURRENCY)
    parser.add_argument("--time-budget-minutes", type=int, default=node.TIME_BUDGET_MINUTES)
    parser.add_argument("--country", action="append", default=None,
                         help="Only crawl this country value (repeatable). Default: DEFAULT_COUNTRIES.")
    parser.add_argument("--all-countries", action="store_true",
                         help="Disable the country filter — crawl every country, including blank ones.")
    parser.add_argument("--restart-index", type=int, default=None,
                         help="Per-shard resume. Omit (default) to auto-resume from this shard's own "
                              "Supabase checkpoint. 0 forces a full restart, ignoring any checkpoint. "
                              "A positive value manually overrides the checkpoint for this run.")
    args = parser.parse_args()
    if args.country:
        countries = set(args.country)
    elif args.all_countries:
        countries = None
    else:
        countries = DEFAULT_COUNTRIES
    asyncio.run(run_crawl(args.shard_index, args.shard_count, args.concurrency,
                           args.time_budget_minutes, countries, args.restart_index))


if __name__ == "__main__":
    main()
