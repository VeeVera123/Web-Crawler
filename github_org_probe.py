"""
GITHUB ORG PROBE — thin seed source on top of node.py (the permanent
engine). Reads github_org_seed.py's output CSV ({name,domain,country},
country always blank here), hands domains to node.crawl_batch(). Depends
on node.py only — not on class_a_probe.py.

Usage:
    python github_org_probe.py --shard-index 0 --shard-count 10
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
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)                       # for node.py
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # for discovery.py
from discovery import SKIP_SLUGS  # noqa: E402
import node  # noqa: E402

log = logging.getLogger("github_org_probe")

CSV_PATH = os.environ.get("GITHUB_ORG_CSV_PATH", "github_org_companies.csv")
SEED_SOURCE_LABEL = "github_org_domain_crawl"


def fetch_github_org_companies() -> list[dict]:
    import csv

    if not os.path.exists(CSV_PATH):
        log.warning(f"Seed dataset not found at '{CSV_PATH}' — nothing to crawl.")
        return []

    out = []
    with open(CSV_PATH, newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("name") or "").strip()
            domain = (row.get("domain") or "").strip().lower()
            domain = re.sub(r"^https?://", "", domain).split("/")[0].strip()
            if name and domain and "." in domain and domain not in SKIP_SLUGS:
                out.append({"name": name, "domain": domain})
    log.info(f"Seed dataset: {len(out)} companies with a usable domain")
    return out


async def run_crawl(shard_index: int | None, shard_count: int | None,
                     concurrency: int, time_budget_minutes: int) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(f"── GitHub org probe{label} ──")
    time_budget_seconds = time_budget_minutes * 60

    companies = fetch_github_org_companies()
    if not companies:
        log.error("  No seed companies with a usable domain — aborting.")
        return

    if shard_index is not None and shard_count is not None:
        companies = companies[shard_index::shard_count]
        log.info(f"  {len(companies)} companies in this shard's slice")

    domains = [c["domain"] for c in companies]
    connector = node.new_connector()
    sem = asyncio.Semaphore(concurrency)
    stats = Counter()
    found_rows: list[dict] = []
    parse_pool = node.new_parse_pool()
    crawl_start = time.monotonic()

    try:
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
            _, elapsed, rate, time_budget_hit = await node.crawl_batch(
                domains, session, sem, stats, parse_pool, node.ACCEPT_ANY_COUNTRY,
                SEED_SOURCE_LABEL, found_rows, crawl_start, time_budget_seconds,
                time_budget_minutes, batch_size=3000, unit_label="companies")
    finally:
        parse_pool.shutdown(wait=True)

    hit_n = stats['hits_from_homepage'] + stats['hits_from_career_path'] + stats['hits_from_sitemap']
    status = "STOPPED EARLY (time budget)" if time_budget_hit else "complete"
    log.info(f"── shard{label} {status}: {stats['companies_attempted']}/{len(companies)} companies, "
             f"{elapsed:.0f}s, {rate:.1f}/sec, {hit_n} hits ──")
    ats_breakdown = Counter(r["ats"] for r in found_rows)
    if ats_breakdown:
        log.info(f"  by platform: {dict(ats_breakdown.most_common())}")


def main():
    parser = argparse.ArgumentParser(description="GitHub-org-seeded domain-crawl discovery (async)")
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=node.CRAWL_CONCURRENCY)
    parser.add_argument("--time-budget-minutes", type=int, default=node.TIME_BUDGET_MINUTES)
    args = parser.parse_args()
    asyncio.run(run_crawl(args.shard_index, args.shard_count, args.concurrency, args.time_budget_minutes))


if __name__ == "__main__":
    main()
