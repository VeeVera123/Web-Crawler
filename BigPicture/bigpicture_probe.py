"""
BIGPICTURE PROBE — a thin, disposable probe source on top of node.py (the
permanent engine), same role kaggle_probe.py/opendata_probe.py play for
their own seeds. Copied directly from opendata_probe.py (2026-08) since
the shape is identical: read a {name, domain, country} CSV, hand domains
to node.crawl_batch(). All fetch/parse/detect/write logic lives in
node.py — fix a bug there once, every probe source gets the fix.

Reads the FILTERED, already-small CSV bigpicture_seed.py produced (and
bigpicture.yml's workflow downloaded/joined from GitHub Release assets, if
split across multiple <2GB chunks — see bigpicture_seed.py's module
docstring). Column names are fixed ("name","domain","country","size")
since bigpicture_seed.py always writes that exact header — no
alias-detection needed here.

EVERY domain in the seed gets crawled/ATS-checked regardless of size —
archive_i is never size-gated (see node.py's crawl_batch docstring). The
'size' column (a raw BigPicture range string like "501-1K", carried
through unfiltered by bigpicture_seed.py) is only used here to build
capture_inhouse_domains: the per-domain allowlist of which no-ATS-match
companies are allowed to become archive_ii candidates. A company below
MIN_COMPANY_SIZE that DOES match a known ATS still lands in archive_i as
usual — the size filter only ever blocks the archive_ii/no-ATS path.

Usage:
    pip install aiohttp aiodns selectolax python-dotenv
    python bigpicture_probe.py
    python bigpicture_probe.py --shard-index 0 --shard-count 15
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

_SIZE_NUM_RE = re.compile(r"[\d,]+")


def _size_floor(size_str: str) -> int | None:
    """Extract the LOWER bound of a BigPicture-style size range string
    ("501-1K", "10K+", "1-10") as an int, or None if blank/unparseable.
    Handles the 'K'/'M' suffix BigPicture uses that PDL's raw numeric
    ranges don't (kaggle_probe.py's own _size_floor has no K/M case) —
    same helper that used to live in bigpicture_seed.py before size
    filtering moved from seed-time to probe-time."""
    if not size_str:
        return None
    s = size_str.strip().upper()
    m = re.match(r"([\d,.]+)\s*(K|M)?", s)
    if not m or not m.group(1):
        return None
    try:
        n = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    if m.group(2) == "K":
        n *= 1_000
    elif m.group(2) == "M":
        n *= 1_000_000
    return int(n)

import aiohttp
from dotenv import load_dotenv

load_dotenv()
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)                       # for node.py
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # for discovery.py
from discovery import SKIP_SLUGS  # noqa: E402
import node  # noqa: E402

log = logging.getLogger("bigpicture_probe")

BIGPICTURE_FILTERED_PATH = os.environ.get("BIGPICTURE_FILTERED_PATH", "bigpicture_companies_filtered.csv")
BIGPICTURE_ROW_LIMIT = int(os.environ.get("BIGPICTURE_ROW_LIMIT", "0"))  # 0 = no cap

SEED_SOURCE_LABEL = os.environ.get("SEED_SOURCE_LABEL", "bigpicture_probe")

# Same reasoning/default as kaggle_probe.py's MIN_COMPANY_SIZE — see that
# file's comment for the full "why 100" writeup (Kumospace, 11-50
# employees, hired globally — 501 was too strict). Only gates archive_ii
# eligibility, never crawl-eligibility — see module docstring.
MIN_COMPANY_SIZE = int(os.environ.get("MIN_COMPANY_SIZE", "100"))

# bigpicture_seed.py always writes exactly this header.
NAME_COL, DOMAIN_COL, COUNTRY_COL, SIZE_COL = "name", "domain", "country", "size"

# Kept as full lowercase country NAMES (not the 2-letter codes
# bigpicture_seed.py filters on) — this second pass runs against the
# ALREADY-filtered CSV, whose country column bigpicture_seed.py writes in
# lowercase-code form (e.g. "us", "gb") same as it writes everything else
# lowercased; --country here lets you narrow further using those same codes.
DEFAULT_COUNTRIES = {
    "us", "gb", "ca", "au", "ie", "nz", "sg",
    "nl", "no", "se", "dk", "fi", "at", "be",
    "is", "lu", "fr", "de",
}

PROGRESS_EVERY = 2_000_000


def read_seed_csv(limit: int = BIGPICTURE_ROW_LIMIT, countries: set[str] | None = None,
                   shard_index: int | None = None, shard_count: int | None = None) -> list[dict]:
    """Single streaming pass over bigpicture_seed.py's filtered CSV — same
    shard-aware inline modulo filter as kaggle_probe.py's/opendata_probe.py's
    read_seed_csv. Missing file logs once and returns empty rather than
    crashing."""
    if not os.path.exists(BIGPICTURE_FILTERED_PATH):
        log.warning(f"Filtered seed CSV not found at '{BIGPICTURE_FILTERED_PATH}' — nothing to crawl. "
                    f"Run bigpicture_seed.py (and download/join its Release asset chunks) first.")
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
        with open(BIGPICTURE_FILTERED_PATH, newline="", encoding="utf-8", errors="ignore",
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
            size_i = header_lower.index(SIZE_COL) if SIZE_COL in header_lower else None
            if size_i is None:
                log.warning(f"No '{SIZE_COL}' column found in the header ({header}) — every no-ATS "
                            f"match this run will be treated as below MIN_COMPANY_SIZE (size unknown).")

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

                size_floor = None
                if size_i is not None and len(row) > size_i:
                    size_floor = _size_floor(row[size_i].strip())

                keep_this_shard = (shard_index is None or shard_count is None
                                    or kept_before_shard % shard_count == shard_index)
                kept_before_shard += 1
                if keep_this_shard:
                    out.append({"name": name, "domain": domain, "size_floor": size_floor})
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
                     min_size: int = MIN_COMPANY_SIZE) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(f"── BigPicture probe{label} ──")
    log.info(f"  concurrency={concurrency}  parse_workers={node.PARSE_WORKERS}  "
             f"time_budget={time_budget_minutes}min  min_size={min_size}  source={SEED_SOURCE_LABEL}")
    time_budget_seconds = time_budget_minutes * 60

    companies = read_seed_csv(countries=countries, shard_index=shard_index, shard_count=shard_count)
    if not companies:
        log.error("  No seed companies with a usable domain — aborting.")
        return

    domains = [c["domain"] for c in companies]
    target_geo_countries = (node.target_countries_geo_form(countries) if countries
                             else node.ACCEPT_ANY_COUNTRY)

    # min_size does NOT gate crawl-eligibility (every domain above is
    # crawled/ATS-checked regardless of size). It only decides which
    # no-ATS-match domains are allowed to become archive_ii candidates —
    # see node.py's crawl_batch docstring for the capture_inhouse_domains
    # mechanism this feeds.
    capture_inhouse_domains = {c["domain"] for c in companies
                                if (c.get("size_floor") or -1) >= min_size}
    log.info(f"  archive_ii eligible (size >= {min_size}): {len(capture_inhouse_domains):,}/{len(companies):,} companies")

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
                time_budget_minutes, batch_size=3000, unit_label="companies",
                capture_inhouse_domains=capture_inhouse_domains)
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
    known_ats_found = stats["known_ats_found"]
    inhouse_captured = stats["inhouse_career_page_captured"]
    career_pages_total = known_ats_found + inhouse_captured
    if career_pages_total:
        log.info(f"  career pages found: {career_pages_total} total — {known_ats_found} known-ats "
                 f"(→ archive_i, as usual) + {inhouse_captured} in-house/unsupported "
                 f"(→ archive_ii, {inhouse_captured / career_pages_total * 100:.1f}% of all career pages found)")


def main():
    parser = argparse.ArgumentParser(description="BigPicture.io domain-crawl discovery (async)")
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=node.CRAWL_CONCURRENCY)
    parser.add_argument("--time-budget-minutes", type=int, default=node.TIME_BUDGET_MINUTES)
    parser.add_argument("--country", action="append", default=None,
                         help="Only crawl this country-code value (repeatable). Default: DEFAULT_COUNTRIES.")
    parser.add_argument("--all-countries", action="store_true",
                         help="Disable the country filter — crawl every country, including blank ones.")
    parser.add_argument("--min-size", type=int, default=MIN_COMPANY_SIZE,
                         help="Does NOT affect which companies get crawled — every company in the "
                              "target countries is crawled/ATS-checked regardless of size. Only "
                              "controls which no-ATS-match companies are allowed into archive_ii "
                              "(default 100 employees). 0 lets every no-ATS-match company through.")
    args = parser.parse_args()
    if args.country:
        countries = set(args.country)
    elif args.all_countries:
        countries = None
    else:
        countries = DEFAULT_COUNTRIES
    asyncio.run(run_crawl(args.shard_index, args.shard_count, args.concurrency,
                           args.time_budget_minutes, countries, args.min_size))


if __name__ == "__main__":
    main()
