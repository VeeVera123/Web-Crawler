"""
PEOPLE DATA LABS PROBE — a thin, disposable probe source on top of node.py
(the permanent engine). Reads PDL's Free Company Dataset, extracts
{name, domain, country, size}, hands domains to node.crawl_batch(). All
fetch/parse/detect/write logic lives in node.py — fix a bug there once,
every probe/seed source gets the fix.

SEED (2026-08): people_data_labs_seed.py downloads + filters PDL's Free
Company Dataset from its direct no-signup CSV link (http://pdl.ai/company-
dataset-csv), bypassing peopledatalabs.com/company-dataset's gated
landing-page form entirely — see people_data_labs_seed.py's module
docstring. people_data_labs.yml's `seed` job runs it and uploads the
filtered CSV as GitHub Release asset chunks; the `crawl` job downloads
them and points PDL_DATASET_PATH here at the joined file. read_seed_csv()
below still
auto-detects column names generically ('domain' vs 'website', etc.) so
an older raw/unfiltered PDL export works too if one is ever used instead.
The 'size' column IS used (unlike some other extra columns PDL provides)
— see run_crawl()'s capture_inhouse_domains construction below.

SHARD-AWARE STREAMING (2026-08, for the 3x larger file): the old version
read the ENTIRE csv into one Python list of dicts, then sliced
list[shard_index::shard_count] — meaning every one of 20 shards paid the
full 22M-row parse+dict-build cost just to keep 1/20th of it. Now the
modulo filter is applied INLINE during the single streaming pass (on a
running counter of rows that already passed the domain/country filters —
same effective boundaries as the old post-filter slice), so each shard
only builds the companies list it's actually going to crawl. Also
switched csv.DictReader -> csv.reader with a header-index map (no per-row
dict allocation) — meaningfully cheaper across 22M rows.

Usage:
    pip install aiohttp aiodns selectolax python-dotenv requests
    python people_data_labs_probe.py
    python people_data_labs_probe.py --shard-index 0 --shard-count 20
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

log = logging.getLogger("people_data_labs_probe")

PDL_DATASET_PATH = os.environ.get("PDL_DATASET_PATH", "pdl_companies_filtered.csv")
PDL_ROW_LIMIT = int(os.environ.get("PDL_ROW_LIMIT", "0"))  # 0 = no cap, counts raw rows scanned

# Same {name,domain,country} shape is also produced by github_org_seed.py
# — this env var is the only thing that changes between seed files, so
# rows are correctly attributed regardless of which one found them.
SEED_SOURCE_LABEL = os.environ.get("SEED_SOURCE_LABEL", "people_data_labs_probe")

# Column aliases: 'domain' covers an older raw/unfiltered PDL export; PDL's
# own current free-dataset download uses 'website'. Whichever is present
# wins — first match in each tuple, case-insensitive header match.
NAME_COLS = ("name", "company_name")
DOMAIN_COLS = ("domain", "website")
COUNTRY_COLS = ("country",)
# 2026-08: PDL's free file also carries a 'size' column (an employee-count
# RANGE string like "501-1000", "10001+") — see MIN_COMPANY_SIZE below.
SIZE_COLS = ("size", "size_range", "employee_count", "employees")

# 2026-08, REVISED: archive_ii was getting flooded with small-business
# noise from no-size-signal sources (opendata_probe.py/common_crawl_probe.py
# set capture_inhouse=False for exactly that — see node.py's crawl_batch
# docstring). PDL DOES carry a size signal, but an earlier version
# of this file used it to drop small companies from the CRAWL LIST at seed
# time — wrong, because that also threw away their perfectly good
# archive_i-eligible ATS hits, for almost nothing gained (archive_i was
# never the problem). Fixed: EVERY country-matched company gets crawled
# now regardless of size (archive_i is never size-gated), and
# MIN_COMPANY_SIZE is applied ONLY in run_crawl(), to decide which
# no-ATS-match companies are allowed to land in archive_ii — see
# node.py's crawl_batch docstring for the capture_inhouse_domains
# mechanism this feeds. Lowered from 501 to 100 (2026-08): 501 was too
# strict — plenty of real global-hiring companies (e.g. Kumospace, an
# 11-50-employee company that has hired globally) sit well under it. 100
# still filters out the smallest/most local-only end while catching a lot
# more of the genuinely-hiring-globally range. Override via
# MIN_COMPANY_SIZE env var or --min-size at any time.
MIN_COMPANY_SIZE = int(os.environ.get("MIN_COMPANY_SIZE", "100"))
_SIZE_NUM_RE = re.compile(r"[\d,]+")


def _size_floor(size_str: str) -> int | None:
    """Extract the LOWER bound of a PDL-style size range string
    ("501-1000", "10,001+", "1-10") as an int, or None if blank/unparseable.
    Filtering on the range's own lower bound (not its midpoint or upper
    bound) is the conservative reading — a "201-500" row only clears a
    MIN_COMPANY_SIZE of 201 or less, never anything higher, even though
    some fraction of real companies in that bucket are closer to 500."""
    if not size_str:
        return None
    nums = _SIZE_NUM_RE.findall(size_str)
    if not nums:
        return None
    try:
        return int(nums[0].replace(",", ""))
    except ValueError:
        return None

# Hand-picked, narrowed 2026-08 (was a 31-country US/UK/Canada/Australia+EU-27
# set). Malta/Bahamas/Guyana/Barbados dropped (2026-08, too small a
# population to be worth it) in favor of Netherlands/Norway/Sweden/
# Denmark/Finland/Austria/Belgium — all countries with a real base of
# English-language-friendly companies actually present in PDL's data.
DEFAULT_COUNTRIES = {
    "united states", "united kingdom", "canada", "australia",
    "ireland", "new zealand", "singapore",
    "netherlands", "norway", "sweden", "denmark", "finland", "austria", "belgium",
    "iceland", "luxembourg",
    "france", "germany",
}

PROGRESS_EVERY = 2_000_000  # raw rows scanned between progress log lines


def _col_index(header_lower: list[str], aliases: tuple[str, ...]) -> int | None:
    for alias in aliases:
        if alias in header_lower:
            return header_lower.index(alias)
    return None


def read_seed_csv(limit: int = PDL_ROW_LIMIT, countries: set[str] | None = None,
                   shard_index: int | None = None, shard_count: int | None = None) -> list[dict]:
    """Single streaming pass over the seed CSV — no full-file list-of-dicts
    build. Applies country + domain-validity filtering ONLY, THEN (only if
    sharding) a modulo filter on a running counter of rows that passed
    those checks, so each shard keeps roughly 1/shard_count of the usable
    rows regardless of shard_count. Missing file logs once and returns
    empty rather than crashing.

    2026-08, REVISED: no size filter here anymore — every company in the
    target countries gets crawled and checked for a known-ATS match
    regardless of employee count (archive_i is never size-gated; see
    MIN_COMPANY_SIZE's comment above for why the old seed-time size skip
    was wrong). Each row's size STRING is still carried through (as
    "size_floor", pre-parsed via _size_floor()) so run_crawl() can use it
    later to decide which no-ATS-match companies are large enough to
    become an archive_ii candidate — see node.py's crawl_batch docstring
    for the capture_inhouse_domains mechanism this feeds."""
    if not os.path.exists(PDL_DATASET_PATH):
        log.warning(f"Seed dataset not found at '{PDL_DATASET_PATH}' — nothing to crawl.")
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
        # 1MB read buffer — real win on a file this size vs the default.
        with open(PDL_DATASET_PATH, newline="", encoding="utf-8", errors="ignore", buffering=1024 * 1024) as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                log.error("Seed dataset is empty (no header row).")
                return []
            header_lower = [h.strip().lower() for h in header]
            name_i = _col_index(header_lower, NAME_COLS)
            domain_i = _col_index(header_lower, DOMAIN_COLS)
            country_i = _col_index(header_lower, COUNTRY_COLS)
            size_i = _col_index(header_lower, SIZE_COLS)
            if name_i is None or domain_i is None:
                log.error(f"Couldn't find a name/domain column in the header: {header}")
                return []
            if size_i is None:
                log.warning(f"No size/employee-count column found in the header ({header}) — "
                             f"every company will be treated as size-unknown (never archive_ii-eligible).")
            log.info(f"  columns: name='{header[name_i]}' domain='{header[domain_i]}'"
                      + (f" country='{header[country_i]}'" if country_i is not None else " (no country column)")
                      + (f" size='{header[size_i]}'" if size_i is not None else ""))

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

                # Modulo filter applied HERE (on kept_before_shard, not the
                # raw row index) — same effective shard boundaries as
                # slicing the old fully-filtered list, but without ever
                # materializing the full list.
                keep_this_shard = (shard_index is None or shard_count is None
                                    or kept_before_shard % shard_count == shard_index)
                kept_before_shard += 1
                if keep_this_shard:
                    out.append({"name": name, "domain": domain, "size_floor": size_floor})
    except Exception as e:
        log.error(f"Failed to read seed dataset: {e}")
        return []

    elapsed = time.monotonic() - start
    filter_note = f", {skipped_wrong_country:,} skipped by country filter" if countries_lower else ""
    shard_note = f", {len(out):,} in this shard" if shard_index is not None else ""
    log.info(f"Seed dataset: {total_rows:,} total rows{filter_note}, "
             f"{kept_before_shard:,} usable "
             f"({kept_before_shard / max(total_rows, 1) * 100:.1f}%){shard_note}, parsed in {elapsed:.1f}s")
    return out


async def run_crawl(shard_index: int | None = None, shard_count: int | None = None,
                     concurrency: int = node.CRAWL_CONCURRENCY,
                     time_budget_minutes: int = node.TIME_BUDGET_MINUTES,
                     countries: set[str] | None = None,
                     min_size: int = MIN_COMPANY_SIZE,
                     restart_index: int | None = None) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(f"── People Data Labs probe{label} ──")
    log.info(f"  concurrency={concurrency}  parse_workers={node.PARSE_WORKERS}  "
             f"time_budget={time_budget_minutes}min  min_size={min_size}  source={SEED_SOURCE_LABEL}")
    time_budget_seconds = time_budget_minutes * 60

    companies = read_seed_csv(countries=countries, shard_index=shard_index, shard_count=shard_count)
    if not companies:
        log.error("  No seed companies with a usable domain — aborting.")
        return

    # "name" itself is no longer carried past this point (2026-08: archive_ii
    # dropped its company_name column — stored no real identifying info beyond
    # the domain/website_url it already keys on, just cost extra space) — it's
    # still required as a data-quality gate above (rows with no company name at
    # all are skipped as likely-junk records), just never collected into a
    # domain->name lookup anymore.
    domains = [c["domain"] for c in companies]
    target_geo_countries = (node.target_countries_geo_form(countries) if countries
                             else node.ACCEPT_ANY_COUNTRY)

    # min_size no longer gates crawl-eligibility (ALL 18-country domains get
    # crawled/ATS-checked regardless of size — archive_i is never size-gated).
    # It only decides which no-ATS-match domains are allowed to become
    # archive_ii candidates — see node.py's crawl_batch docstring for the
    # capture_inhouse_domains mechanism this feeds.
    capture_inhouse_domains = {c["domain"] for c in companies
                                if (c.get("size_floor") or -1) >= min_size}
    log.info(f"  every one of these {len(companies):,} companies gets crawled regardless of size — "
             f"{len(capture_inhouse_domains):,}/{len(companies):,} are size >= {min_size} and will be "
             f"allowed into archive_ii later, IF they turn out to have no known ATS but a real career page")

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
            _, elapsed, rate, time_budget_hit = await node.crawl_batch(
                domains, session, sem, stats, parse_pool, target_geo_countries,
                SEED_SOURCE_LABEL, found_rows, crawl_start, time_budget_seconds,
                time_budget_minutes, batch_size=3000, unit_label="companies",
                capture_inhouse_domains=capture_inhouse_domains,
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
    # logging config comes from node.py's basicConfig (set at import time above).
    parser = argparse.ArgumentParser(description="People Data Labs domain-crawl discovery (async)")
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=node.CRAWL_CONCURRENCY)
    parser.add_argument("--time-budget-minutes", type=int, default=node.TIME_BUDGET_MINUTES)
    parser.add_argument("--country", action="append", default=None,
                         help="Only crawl this PDL 'country' value (repeatable). Default: DEFAULT_COUNTRIES.")
    parser.add_argument("--all-countries", action="store_true",
                         help="Disable the country filter — crawl every country, including blank ones.")
    parser.add_argument("--min-size", type=int, default=MIN_COMPANY_SIZE,
                         help="Does NOT affect which companies get crawled — every company in the "
                              "target countries is crawled/ATS-checked regardless of size. Only "
                              "controls which no-ATS-match companies are allowed into archive_ii "
                              "(default 100 employees). 0 lets every no-ATS-match company through.")
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
                           args.time_budget_minutes, countries, args.min_size, args.restart_index))


if __name__ == "__main__":
    main()
