"""
Host Crawl — Seed Step
=======================
Populates host_crawl_queue with candidate hostnames from Common Crawl's
Host Index (free, no signup, no AWS account — read directly over HTTPS
via DuckDB's Parquet reader, no full download needed).

This is a SEPARATE step from the actual crawl (host_crawl.py) because
seeding is a one-time-ish bulk operation (read millions of index rows,
filter, write ~however many survive to Supabase), while crawling is the
repeated, resumable, checkpointed part that runs on a schedule. Re-run
this seed step occasionally (e.g. when Common Crawl publishes a new host
index release) rather than every crawl run — host_crawl.py just drains
whatever's already queued.

Why DuckDB: it can query a remote Parquet file over plain HTTPS with
column projection and filter pushdown — i.e. it downloads only the
columns and row-groups it actually needs, not the whole ~7GB/crawl file.
We only need 4 columns (surt_host_name, url_host_tld, hcrank, and enough
fetch-status counts to judge liveness), so this is cheap even though the
source file is huge.

Filtering applied here (see TARGET_TLDS / _looks_dead below) is exactly
why this is a separate step from the crawl — every host that survives
this filter is one we're confident is BOTH plausibly in a target country
AND plausibly alive, so the crawl step never wastes a live fetch on a
host that could have been ruled out for free from data Common Crawl
already collected.

Usage:
    python host_crawl_seed.py                  # seed from latest release
    python host_crawl_seed.py --dry-run        # count without writing
    python host_crawl_seed.py --limit 500000   # cap rows written (testing)
"""

import argparse
import logging
import os
import sys

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

# Common Crawl Host Index — public, free, no AWS account needed (this is
# the same host that returned a 403 to earlier dev-sandbox attempts in
# this project; that was confirmed to be a sandbox-local proxy block, not
# a real restriction — see discovery.py's Web Data Commons history for
# the same false alarm). GitHub Actions runners are the real test.
HOST_INDEX_BASE = "https://data.commoncrawl.org/projects/host-index-testing"
HOST_INDEX_MANIFEST = f"{HOST_INDEX_BASE}/v2.paths.gz"

# ── TLD allowlist ────────────────────────────────────────
# Per-country ccTLDs for the target markets, PLUS the generic/startup
# TLDs (.com, .io, .co, .app, .dev) that skew heavily toward exactly
# these markets but aren't attributable to any single country. This is a
# SOFT geography proxy, not a hard filter on company location — .com in
# particular is the default for US companies and plenty of others, and
# .io/.co/.app/.dev are globally used by tech startups regardless of
# country. Deliberately inclusive: better to crawl some non-target-market
# .com sites than to miss real target-market companies that happen to use
# a generic TLD. ccTLDs for uninvolved countries (.ng, .in, .br, etc.) are
# excluded — those are the ones actually worth skipping, since a company
# using a country-specific ccTLD for a country we're not targeting is a
# genuinely strong signal it's not relevant here.
TARGET_TLDS = {
    # Country-specific
    "us", "uk", "co.uk", "ca", "de", "au", "com.au", "ie", "mt",
    # Generic/global — not attributable to one country, kept for recall
    "com", "io", "co", "app", "dev",
}

# ── Liveness filter ──────────────────────────────────────
# A host is treated as likely-dead if Common Crawl's own crawl attempts
# never got a single 2xx response for it. This costs nothing extra (the
# data's already in the index) and only excludes hosts CC itself could
# never successfully fetch — a real small/quiet company that returned
# 200 even once still passes, regardless of how low its rank is. This is
# deliberately conservative (biased toward keeping a host if in doubt)
# per the instruction to stay inclusive rather than exclusive.
def _looks_dead(fetch_200: int, fetch_4xx: int, fetch_5xx: int,
                 fetch_gone: int, nutch_gone: int) -> bool:
    if fetch_200 and fetch_200 > 0:
        return False
    # No successful fetch ever recorded, AND at least one explicit
    # failure/gone signal — this is the "confidently dead" case, not
    # "we just don't have much data on it".
    return (fetch_4xx or 0) + (fetch_5xx or 0) + (fetch_gone or 0) + (nutch_gone or 0) > 0


def _get_latest_host_index_prefix() -> str | None:
    """Fetch the manifest of part-file paths and derive the release
    prefix (e.g. 'host-index-testing/v2/...'). Returns None on failure —
    caller should abort cleanly rather than guess a path."""
    try:
        r = requests.get(HOST_INDEX_MANIFEST, timeout=60)
        r.raise_for_status()
    except Exception as e:
        log.error(f"Failed to fetch host index manifest: {e}")
        return None

    import gzip
    import io
    try:
        with gzip.open(io.BytesIO(r.content), "rt") as f:
            paths = [line.strip() for line in f if line.strip()]
    except Exception as e:
        log.error(f"Failed to parse host index manifest: {e}")
        return None

    if not paths:
        log.error("Host index manifest was empty.")
        return None

    log.info(f"Host index manifest: {len(paths)} part files listed.")
    return paths


def _robots_allows_host_index() -> bool:
    """Live robots.txt check against data.commoncrawl.org before doing
    anything else — same non-negotiable policy as every other source in
    this project (see discovery.py's _robots_allows)."""
    try:
        r = requests.get("https://data.commoncrawl.org/robots.txt", timeout=30)
        if r.status_code >= 400:
            # No robots.txt / unreachable — fail CLOSED per project policy.
            log.warning("Could not fetch data.commoncrawl.org/robots.txt "
                        f"(HTTP {r.status_code}) — treating as disallowed.")
            return False
        import urllib.robotparser
        rp = urllib.robotparser.RobotFileParser()
        rp.parse(r.text.splitlines())
        allowed = rp.can_fetch("ATS-Global-Scanner/1.0", "/projects/host-index-testing/")
        if not allowed:
            log.warning("data.commoncrawl.org/robots.txt disallows this path.")
        return allowed
    except Exception as e:
        log.warning(f"robots.txt check failed for data.commoncrawl.org: {e} "
                    f"— treating as disallowed.")
        return False


def seed(limit: int | None = None, dry_run: bool = False) -> int:
    if not _robots_allows_host_index():
        log.error("robots.txt disallows crawling the host index — aborting seed.")
        return 0

    try:
        import duckdb
    except ImportError:
        log.error("duckdb not installed — pip install duckdb to run this step.")
        return 0

    paths = _get_latest_host_index_prefix()
    if not paths:
        return 0

    # Use the most recent release's part files (manifest is ordered
    # oldest->newest per Common Crawl's convention; take the ones sharing
    # the newest path prefix). Paths in the manifest are relative to the
    # HTTPS root, not HOST_INDEX_BASE — normalize accordingly.
    newest_prefix = paths[-1].rsplit("/", 1)[0]
    part_urls = [f"https://data.commoncrawl.org/{p}" for p in paths
                 if p.rsplit("/", 1)[0] == newest_prefix]

    if not part_urls:
        log.error("Could not determine current release's part files from manifest.")
        return 0

    log.info(f"Reading {len(part_urls)} part file(s) from release "
             f"'{newest_prefix}' via DuckDB (HTTPS, column-pruned)...")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    tld_list = ",".join(f"'{t}'" for t in TARGET_TLDS)
    parquet_glob = "[" + ",".join(f"'{u}'" for u in part_urls) + "]"

    query = f"""
        SELECT
            surt_host_name,
            url_host_tld,
            hcrank,
            fetch_200, fetch_4xx, fetch_5xx, fetch_gone, nutch_gone
        FROM read_parquet({parquet_glob})
        WHERE url_host_tld IN ({tld_list})
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    try:
        result = con.execute(query).fetchall()
    except Exception as e:
        log.error(f"DuckDB query failed: {e}")
        return 0

    log.info(f"Host index: {len(result)} rows matched target TLDs, "
             f"filtering dead hosts...")

    rows_to_insert = []
    dead_skipped = 0
    for surt_host, tld, hcrank, f200, f4xx, f5xx, fgone, ngone in result:
        if _looks_dead(f200, f4xx, f5xx, fgone, ngone):
            dead_skipped += 1
            continue
        # surt_host_name is reversed (org,commoncrawl,www) — un-reverse to
        # a normal hostname for the crawl step.
        host = ".".join(reversed(surt_host.split(",")))
        rows_to_insert.append({
            "host": host,
            "tld": tld,
            "hcrank": float(hcrank) if hcrank is not None else None,
        })

    log.info(f"Seed candidates: {len(rows_to_insert)} hosts "
             f"({dead_skipped} skipped as likely-dead)")

    if dry_run:
        log.info("Dry run — not writing to Supabase.")
        return len(rows_to_insert)

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot write queue.")
        return 0

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates",
    }
    batch_size = 5000
    written = 0
    for i in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[i:i + batch_size]
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/host_crawl_queue",
                headers=headers, json=batch, timeout=60,
                # on_conflict required alongside the ignore-duplicates
                # Prefer header — PostgREST needs to know which column
                # defines a duplicate (host_crawl_queue's PK is `host`).
                params={"on_conflict": "host"},
            )
            r.raise_for_status()
            written += len(batch)
        except Exception as e:
            log.error(f"Failed to write batch {i}-{i+len(batch)}: {e}")
        if (i // batch_size) % 20 == 0:
            log.info(f"  ...{written}/{len(rows_to_insert)} written so far")

    log.info(f"Seed complete: {written} hosts written to host_crawl_queue.")
    return written


def main():
    parser = argparse.ArgumentParser(
        description="Seed host_crawl_queue from Common Crawl's Host Index"
    )
    parser.add_argument("--limit", type=int, default=None,
                         help="Cap total rows read from the index (testing)")
    parser.add_argument("--dry-run", action="store_true",
                         help="Count without writing to Supabase")
    args = parser.parse_args()

    written = seed(limit=args.limit, dry_run=args.dry_run)
    if written == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
