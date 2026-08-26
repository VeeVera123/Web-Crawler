"""
CLASS A DOMAIN-CRAWL DISCOVERY — a thin, disposable seed source on top of
node.py (the permanent engine). This file's only job: read PDL's Free
Company Dataset, extract {name, domain, country}, hand domains to
node.crawl_batch(). All fetch/parse/detect/write logic lives in node.py —
fix a bug there once, this and every other seed source gets the fix.

Usage:
    pip install aiohttp aiodns selectolax python-dotenv requests
    python class_a_probe.py
    python class_a_probe.py --shard-index 0 --shard-count 20
"""
import argparse
import asyncio
import logging
import os
import re
import sys
import time
from collections import Counter

import aiohttp
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from discovery import SKIP_SLUGS  # noqa: E402
import node  # noqa: E402

log = logging.getLogger("class_a_probe")

PDL_DATASET_PATH = os.environ.get("PDL_DATASET_PATH", "people_data_labs_companies.csv")
PDL_ROW_LIMIT = int(os.environ.get("PDL_ROW_LIMIT", "0"))  # 0 = no cap

# Same {name,domain,country} CSV shape is also produced by
# github_org_seed.py — this env var is the only thing that changes
# between seed files, so rows are correctly attributed regardless of
# which one actually found them.
SEED_SOURCE_LABEL = os.environ.get("SEED_SOURCE_LABEL", "pdl_domain_crawl")

# Hand-picked, narrowed 2026-08 (was a 31-country US/UK/Canada/Australia+EU-27
# set). 'singapore'/'malta'/'new zealand'/'bahamas'/'guyana'/'barbados' aren't
# confirmed present in PDL's real data but cost nothing to list if absent.
DEFAULT_COUNTRIES = {
    "united states", "united kingdom", "canada", "australia",
    "ireland", "new zealand", "singapore", "malta",
    "bahamas", "guyana", "barbados",
    "france", "germany",
}


def fetch_pdl_companies_with_domain(limit: int = PDL_ROW_LIMIT,
                                     countries: set[str] | None = None) -> list[dict]:
    """Reads the seed CSV (PDL's own dataset, or any other source that
    matches its {name,domain,country} shape). Missing file logs once and
    returns empty rather than crashing. countries: case-insensitive exact
    match against the 'country' column; None = no filter (includes every
    blank-country row too)."""
    import csv

    if not os.path.exists(PDL_DATASET_PATH):
        log.warning(f"Seed dataset not found at '{PDL_DATASET_PATH}' — nothing to crawl.")
        return []

    countries_lower = {c.strip().lower() for c in countries} if countries else None
    if countries_lower:
        log.info(f"  filtering to {len(countries_lower)} countries")

    out = []
    total_rows = 0
    skipped_wrong_country = 0
    try:
        with open(PDL_DATASET_PATH, newline="", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break
                total_rows += 1
                if countries_lower:
                    row_country = (row.get("country") or "").strip().lower()
                    if row_country not in countries_lower:
                        skipped_wrong_country += 1
                        continue
                name = (row.get("name") or "").strip()
                domain = (row.get("domain") or "").strip().lower()
                domain = re.sub(r"^https?://", "", domain).split("/")[0].strip()
                if name and domain and "." in domain and domain not in SKIP_SLUGS:
                    out.append({"name": name, "domain": domain})
    except Exception as e:
        log.error(f"Failed to read seed dataset: {e}")
        return []
    filter_note = f", {skipped_wrong_country} skipped by country filter" if countries_lower else ""
    log.info(f"Seed dataset: {total_rows} total rows{filter_note}, {len(out)} with a usable domain "
             f"({len(out) / max(total_rows, 1) * 100:.1f}%)")
    return out


async def run_crawl(shard_index: int | None = None, shard_count: int | None = None,
                     concurrency: int = node.CRAWL_CONCURRENCY,
                     time_budget_minutes: int = node.TIME_BUDGET_MINUTES,
                     countries: set[str] | None = None) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(f"── Class A domain-crawl discovery{label} ──")
    log.info(f"  concurrency={concurrency}  parse_workers={node.PARSE_WORKERS}  "
             f"time_budget={time_budget_minutes}min  source={SEED_SOURCE_LABEL}")
    time_budget_seconds = time_budget_minutes * 60

    companies = fetch_pdl_companies_with_domain(countries=countries)
    if not companies:
        log.error("  No seed companies with a usable domain — aborting.")
        return

    if shard_index is not None and shard_count is not None:
        # MODULO sharding — PDL's rows are sorted largest-company-first,
        # so a contiguous chunk would skew which shard gets the signal.
        companies = companies[shard_index::shard_count]
        log.info(f"  {len(companies)} companies in this shard's slice")

    domains = [c["domain"] for c in companies]
    target_geo_countries = (node.target_countries_geo_form(countries) if countries
                             else node.ACCEPT_ANY_COUNTRY)

    connector = node.new_connector()
    sem = asyncio.Semaphore(concurrency)
    stats = Counter()
    found_rows: list[dict] = []
    parse_pool = node.new_parse_pool()
    crawl_start = time.monotonic()

    try:
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
            _, elapsed, rate, time_budget_hit = await node.crawl_batch(
                domains, session, sem, stats, parse_pool, target_geo_countries,
                SEED_SOURCE_LABEL, found_rows, crawl_start, time_budget_seconds,
                time_budget_minutes, batch_size=3000, unit_label="companies")
    finally:
        parse_pool.shutdown(wait=True)

    hit_n = stats['hits_from_homepage'] + stats['hits_from_career_path'] + stats['hits_from_sitemap']
    companies_n = max(stats["companies_attempted"], 1)
    status = "STOPPED EARLY (time budget)" if time_budget_hit else "complete"
    log.info(f"── shard{label} {status}: {stats['companies_attempted']}/{len(companies)} companies, "
             f"{elapsed:.0f}s, {rate:.1f}/sec, hit={hit_n / companies_n * 100:.1f}% "
             f"({hit_n}), unreachable={stats['homepage_unreachable'] / companies_n * 100:.1f}% ──")
    if hit_n:
        log.info(f"  hit source: homepage={stats['hits_from_homepage'] / hit_n * 100:.1f}% "
                 f"career_path={stats['hits_from_career_path'] / hit_n * 100:.1f}% "
                 f"sitemap={stats['hits_from_sitemap'] / hit_n * 100:.1f}%")
    ats_breakdown = Counter(r["ats"] for r in found_rows)
    if ats_breakdown:
        log.info(f"  by platform: {dict(ats_breakdown.most_common())}")


def main():
    parser = argparse.ArgumentParser(description="Class A domain-crawl discovery (async)")
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=node.CRAWL_CONCURRENCY)
    parser.add_argument("--time-budget-minutes", type=int, default=node.TIME_BUDGET_MINUTES)
    parser.add_argument("--country", action="append", default=None,
                         help="Only crawl this PDL 'country' value (repeatable). Default: DEFAULT_COUNTRIES.")
    parser.add_argument("--all-countries", action="store_true",
                         help="Disable the country filter — crawl every country, including blank ones.")
    args = parser.parse_args()
    if args.country:
        countries = set(args.country)
    elif args.all_countries:
        countries = None
    else:
        countries = DEFAULT_COUNTRIES
    asyncio.run(run_crawl(args.shard_index, args.shard_count, args.concurrency,
                           args.time_budget_minutes, countries))


if __name__ == "__main__":
    main()
