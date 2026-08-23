"""
Tranco — Seed Step
====================
Populates t_seeding with domains from the Tranco list
(tranco-list.eu) — a free, research-grade ranking of the top ~1 million
domains on the internet, combining multiple underlying popularity signals
(historically Cisco Umbrella, Majestic, Farsight, etc.) specifically to
resist the kind of gaming any single ranking source is vulnerable to.

WHY TRANCO INSTEAD OF THE (retired) Common Crawl Host Index approach:
the host-crawl pipeline (see ./host crawl/ — retired, kept for reference)
seeded from Common Crawl's Host Index, which is EVERY host Common Crawl's
own crawler ever fetched — the vast majority of which are parked domains,
dead redirects, or otherwise irrelevant, and the index itself is split
across ~30 huge (200-340MB) Parquet files per crawl partition needing a
whole per-file draining/dedup-tracking system (h_total_seeded) just to
seed it in Supabase-sized batches. That dedup-tracking table alone grew
past 499,000 rows partway through seeding a SINGLE Parquet file, and the
pipeline was retired before finishing even that one file's ~30-file
partition — of 26 total partitions.

Tranco fixes the structural problem, not just the immediate one: it's
ALREADY a fixed-size (~1M row), pre-ranked list of domains known to
receive real traffic — a plain CSV, one file, no per-partition draining
needed. Seeding it is a single pass: download once, upsert once, done.
No table is needed to track "which hosts have already been seeded from
this file" the way h_total_seeded was, because there's no multi-file
partition to converge across — this script either seeds a domain or it
doesn't, in one shot.

Usage:
    python tranco_seed.py                    # seed top TRANCO_TOP_N domains
    python tranco_seed.py --dry-run          # count without writing
    python tranco_seed.py --top-n 500000     # override how many ranks to seed
    python tranco_seed.py --list-date 2026-08-20   # pin a specific daily list
    python tranco_seed.py --from-csv tranco_1m.csv  # seed from an already-
                                              # downloaded Tranco CSV instead
                                              # of hitting tranco-list.eu at
                                              # all (see _load_from_csv's
                                              # docstring — same "rank,domain"
                                              # no-header shape as the ZIP's
                                              # contents, so a manually
                                              # downloaded list drops in as-is)
"""

import argparse
import csv
import io
import logging
import os
import sys
import time
import zipfile

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# Tranco's own public, unauthenticated endpoints (see
# https://github.com/DistriNet/tranco-python-package for the reference
# implementation this mirrors — no API key needed for the daily list).
# IF THESE EVER 404/CHANGE: check tranco-list.eu's own site or that
# package's current source for the up-to-date URL pattern before assuming
# the whole approach is broken — Tranco has changed its endpoint shape
# before.
TRANCO_DAILY_ID_URL = "https://tranco-list.eu/daily_list_id"
TRANCO_DOWNLOAD_URL = "https://tranco-list.eu/download_daily/{list_id}"

# Tranco's OWN daily-list generation has a documented history of
# intermittent failures — see github.com/DistriNet/tranco-list issues
# #34/#36/#39/#40 (outages, generation failures, and one severely
# truncated list). A single 503/timeout here isn't evidence our request
# is malformed; it's their service being transiently unavailable. Retry
# with a short backoff before giving up, instead of failing the whole
# seed step (and the CI workflow) on one blip.
_NETWORK_RETRIES = 3
_NETWORK_BACKOFF_SECONDS = 10  # doubles each retry: 10s, 20s, 40s


def _get_with_retry(url, **kwargs):
    last_err = None
    for attempt in range(1, _NETWORK_RETRIES + 1):
        try:
            r = requests.get(url, **kwargs)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            if attempt < _NETWORK_RETRIES:
                wait = _NETWORK_BACKOFF_SECONDS * (2 ** (attempt - 1))
                log.warning(f"Attempt {attempt}/{_NETWORK_RETRIES} failed for "
                            f"{url}: {e} — retrying in {wait}s.")
                time.sleep(wait)
    raise last_err

# How many top-ranked domains to seed by default. Tranco's daily list is
# ~1M rows; capping well under Supabase's free tier (500MB) — at a rough
# ~40 bytes/row for t_seeding, 1M rows is only ~40MB, so this is a
# generous default with a lot of headroom, not a tight safety cap like
# host_crawl_seed.py's _DEFAULT_SAFE_LIMIT had to be.
_DEFAULT_TOP_N = 1_000_000


def _download_tranco_csv(list_date: str | None) -> list[tuple[int, str]] | None:
    """Fetch the Tranco list (by explicit date, or today's daily list) and
    return [(rank, domain), ...]. The daily list ships as a ZIP containing
    one CSV with no header row: "rank,domain" per line."""
    import datetime

    date_str = list_date or datetime.date.today().isoformat()

    try:
        r = _get_with_retry(
            TRANCO_DAILY_ID_URL,
            params={"date": date_str, "subdomains": "false"},
            timeout=30,
        )
        list_id = r.text.strip()
        if not list_id:
            log.error(f"Tranco returned an empty list ID for date={date_str}.")
            return None
        log.info(f"Tranco list ID for {date_str}: {list_id}")
    except Exception as e:
        log.error(f"Failed to fetch Tranco's daily list ID for {date_str} "
                   f"after {_NETWORK_RETRIES} attempts: {e}")
        return None

    try:
        r = _get_with_retry(
            TRANCO_DOWNLOAD_URL.format(list_id=list_id),
            timeout=120,
        )
    except Exception as e:
        log.error(f"Failed to download Tranco list {list_id} after "
                   f"{_NETWORK_RETRIES} attempts: {e}")
        return None

    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            # The zip contains exactly one CSV named after the list ID.
            names = zf.namelist()
            if not names:
                log.error(f"Tranco list {list_id} zip was empty.")
                return None
            with zf.open(names[0]) as f:
                text = io.TextIOWrapper(f, encoding="utf-8")
                rows = []
                for row in csv.reader(text):
                    if len(row) < 2:
                        continue
                    try:
                        rank = int(row[0])
                    except ValueError:
                        continue  # skip a stray header row if present
                    domain = row[1].strip().lower()
                    if domain:
                        rows.append((rank, domain))
        log.info(f"Tranco list {list_id}: {len(rows)} domains downloaded.")
        return rows
    except Exception as e:
        log.error(f"Failed to parse Tranco list {list_id} zip/CSV: {e}")
        return None


def _load_from_csv(path: str) -> list[tuple[int, str]] | None:
    """Load [(rank, domain), ...] from an already-downloaded Tranco CSV
    instead of hitting tranco-list.eu at all — useful when their daily-
    list-generation service is down (a documented recurring issue, see
    _NETWORK_RETRIES's comment) or when you already have a list saved
    locally. Expects the SAME shape as the CSV inside Tranco's own daily-
    list ZIP: no header row, "rank,domain" per line — exactly what
    tranco-list.eu/download_daily and /download both produce, so a
    manually downloaded list (e.g. the top-1M CSV from Tranco's site)
    drops in unmodified, no reformatting needed."""
    rows = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                if len(row) < 2:
                    continue
                try:
                    rank = int(row[0])
                except ValueError:
                    continue  # skip a stray header row if present
                domain = row[1].strip().lower()
                if domain:
                    rows.append((rank, domain))
    except Exception as e:
        log.error(f"Failed to read/parse {path}: {e}")
        return None

    log.info(f"Loaded {len(rows)} domains from {path}.")
    return rows


def seed(top_n: int | None = None, dry_run: bool = False,
          list_date: str | None = None, from_csv: str | None = None) -> int:
    rows = _load_from_csv(from_csv) if from_csv else _download_tranco_csv(list_date)
    if rows is None:
        return 0

    if top_n:
        rows = [r for r in rows if r[0] <= top_n]
        log.info(f"Trimmed to top {top_n:,} ranks — {len(rows)} domains.")

    if dry_run:
        log.info(f"Dry run — {len(rows)} domains would be written to "
                 f"t_seeding. Not writing to Supabase.")
        return len(rows)

    if not (SUPABASE_URL and SUPABASE_KEY):
        log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot write t_seeding.")
        return 0

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates",
    }

    total_written = 0
    batch_size = 5000
    payload = [{"host": domain, "rank": rank} for rank, domain in rows]
    for i in range(0, len(payload), batch_size):
        batch = payload[i:i + batch_size]
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/t_seeding",
                headers=headers, json=batch, timeout=60,
                params={"on_conflict": "host"},
            )
            r.raise_for_status()
            total_written += len(batch)
        except Exception as e:
            log.error(f"Failed to write batch starting at row {i}: {e}")

        if (i // batch_size) % 20 == 0:
            log.info(f"  ...{total_written}/{len(payload)} written so far")

    log.info(f"Seed complete: {total_written} domains written to t_seeding.")
    return total_written


def main():
    parser = argparse.ArgumentParser(
        description="Seed t_seeding from the Tranco top-sites list "
                    "(tranco-list.eu, free, no API key needed)"
    )
    parser.add_argument("--top-n", type=int, default=_DEFAULT_TOP_N,
                         help=f"Only seed domains ranked <= this (default: "
                              f"{_DEFAULT_TOP_N:,} — Tranco's daily list is "
                              f"~1M rows total). Pass 0 to seed the entire "
                              f"list with no rank cutoff.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Count without writing to Supabase")
    parser.add_argument("--list-date", type=str, default=None,
                         help="Pin a specific daily list by date "
                              "(YYYY-MM-DD). Default: today's daily list.")
    parser.add_argument("--from-csv", type=str, default=None,
                         help="Seed from an already-downloaded Tranco CSV "
                              "(no header, 'rank,domain' per line) instead "
                              "of fetching from tranco-list.eu. Bypasses "
                              "the network entirely — useful if Tranco's "
                              "daily-list service is down.")
    args = parser.parse_args()

    top_n = args.top_n if args.top_n and args.top_n > 0 else None

    written = seed(top_n=top_n, dry_run=args.dry_run,
                    list_date=args.list_date, from_csv=args.from_csv)

    if written == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
