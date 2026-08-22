"""
Host Crawl — Seed Step
=======================
Populates host_crawl_queue with candidate hostnames from Common Crawl's
HOST INDEX, re-hosted by Common Crawl's own official org on Hugging Face
(huggingface.co/datasets/commoncrawl/host-index-testing-v2), queried
directly via DuckDB's native `hf://` support — no AWS account, no
billing, no robots.txt wall, genuinely free.

FULL HISTORY (why this is v5 — see BULK_DOMAIN_DISCOVERY_NOTES.md for the
complete write-up of every path tried, with live error messages):
  v1 — Host Index via data.commoncrawl.org (HTTPS mirror). DEAD: that
       host's robots.txt blanket-disallows "/" for every crawler
       (confirmed live).
  v2 — Columnar index via DuckDB's httpfs reading s3://commoncrawl
       directly, assuming no-credentials meant unsigned-like-the-AWS-CLI.
       DEAD: DuckDB's S3 client always signs requests; no anonymous
       provider exists. Real 403 AccessDenied on a live GitHub Actions run.
  v3 — Columnar index via the actual AWS CLI's --no-sign-request
       (genuinely unsigned). DEAD: anonymous GetObject on a KNOWN key
       works, but anonymous ListObjectsV2 (browsing/discovering what
       files exist) is denied, and Parquet part-file names include a
       random Spark-generated UUID with no published manifest — so there
       was no way to discover what to fetch. Real AccessDenied on a live
       GitHub Actions run.
  v4 — Columnar index via real AWS Athena (a genuine, working fix, since
       Athena's own Glue/metastore does file discovery server-side,
       sidestepping the ListObjects restriction entirely). WORKS, but
       requires a real, billed AWS account (~$1-1.50/query-run) — user
       decided not to open one for this right now. Code kept, dormant,
       for if that changes (see git history / the workflow's comments).
  v5 (THIS VERSION) — a second opinion (Gemini) surfaced that Common
       Crawl's own org re-hosts the HOST INDEX (the same dataset v1
       tried and failed to reach) on Hugging Face, where anonymous file
       LISTING genuinely works (unlike S3's ListObjectsV2) — confirmed
       live via a direct, unauthenticated fetch of
       https://huggingface.co/api/datasets/commoncrawl/host-index-testing-v2/tree/main/data,
       which returned a real directory listing of 26 crawl= partitions,
       no token, no account. DuckDB has NATIVE, documented `hf://` read
       support (shipped in httpfs since v0.10.3, confirmed via DuckDB's
       own docs and Hugging Face's own docs) — no separate extension, no
       auth for public datasets, glob patterns work. This is free, no
       AWS account, no billing, no robots.txt issue (this access path
       never touches data.commoncrawl.org or S3 at all), and it's the
       HOST-level aggregated index (fetch-status counts, rank scores)
       rather than the raw per-URL columnar index — meaning the
       dead-domain filtering logic this project designed back in v1
       (see _looks_dead below) is directly usable again, unmodified.

Trade-off versus the abandoned columnar/Athena approach: this is
per-HOST (one row per known host, aggregated), not per-URL — slightly
less granular, but genuinely sufficient for seeding a "visit this host
and look for an ATS link" crawl queue, which is exactly what this is for.

Usage:
    python host_crawl_seed.py                       # seed from latest crawl
    python host_crawl_seed.py --dry-run             # count without writing
    python host_crawl_seed.py --limit 500000        # cap rows written (testing)
    python host_crawl_seed.py --crawl CC-MAIN-2025-18   # pin a specific crawl
    python host_crawl_seed.py --months 3            # scan the 3 most recent crawls
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

# Optional: a Hugging Face token, purely to get the higher authenticated
# rate-limit tier (HF's docs note anonymous CI traffic from shared IP
# pools like GitHub Actions runners is more exposed to rate-limiting than
# a token'd request) — NOT required for functionality, this dataset is
# fully public. Left empty, DuckDB just queries anonymously.
HF_TOKEN = os.environ.get("HF_TOKEN", "")

HF_DATASET = "commoncrawl/host-index-testing-v2"
HF_BASE = f"hf://datasets/{HF_DATASET}/data"

# Used only if live discovery of available crawl= partitions fails.
_FALLBACK_CRAWL = "CC-MAIN-2025-18"

# ── TLD allowlist ────────────────────────────────────────
# Per-country ccTLDs for the target markets, PLUS the generic/startup
# TLDs (.com, .io, .co, .app, .dev) that skew heavily toward exactly
# these markets but aren't attributable to any single country. This is a
# SOFT geography proxy, not a hard filter on company location. ccTLDs for
# uninvolved countries (.ng, .in, .br, etc.) are excluded on purpose.
TARGET_TLDS = {
    "us", "uk", "ca", "de", "au", "ie", "mt",
    "com", "net", "io", "co", "app", "dev",
}
TARGET_SUFFIXES_EXTRA = {"co.uk", "com.au"}


def _build_tld_filter() -> str:
    parts = [f"'{t}'" for t in TARGET_TLDS] + [f"'{t}'" for t in TARGET_SUFFIXES_EXTRA]
    return ",".join(parts)


# ── Liveness filter ──────────────────────────────────────
# A host is treated as likely-dead if Common Crawl's own crawl attempts
# never got a single 2xx response for it. This costs nothing extra (the
# data's already in the index) and only excludes hosts CC itself could
# never successfully fetch — a real small/quiet company that returned
# 200 even once still passes, regardless of how low its rank is.
# Deliberately conservative (biased toward keeping a host if in doubt),
# matching this project's "inclusive not exclusive" instruction.
def _looks_dead(fetch_200: int, fetch_4xx: int, fetch_5xx: int,
                 fetch_gone: int, nutch_gone: int) -> bool:
    if fetch_200 and fetch_200 > 0:
        return False
    return (fetch_4xx or 0) + (fetch_5xx or 0) + (fetch_gone or 0) + (nutch_gone or 0) > 0


def _get_duckdb_connection():
    try:
        import duckdb
    except ImportError:
        log.error("duckdb not installed — pip install duckdb to run this step.")
        return None

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    except Exception as e:
        log.error(f"Failed to load DuckDB's httpfs extension (needed for hf:// "
                  f"reads): {e}")
        return None

    if HF_TOKEN:
        try:
            con.execute(f"CREATE SECRET hf_token (TYPE huggingface, TOKEN '{HF_TOKEN}');")
            log.info("Using HF_TOKEN for authenticated (higher rate-limit) access.")
        except Exception as e:
            log.warning(f"Failed to configure HF_TOKEN secret (continuing "
                        f"anonymously): {e}")
    return con


def _list_available_crawls(con) -> list[str]:
    """Ask Hugging Face's (genuinely anonymous, unlike S3) directory
    listing API which crawl= partitions exist, via DuckDB's glob()."""
    try:
        rows = con.execute(
            f"SELECT DISTINCT regexp_extract(file, 'crawl=([^/]+)', 1) AS crawl "
            f"FROM glob('{HF_BASE}/crawl=*/') "
            f"ORDER BY crawl DESC"
        ).fetchall()
        names = [r[0] for r in rows if r[0]]
        if names:
            return names
    except Exception as e:
        log.warning(f"Could not list crawl partitions via hf:// glob: {e}")
    return []


def seed(limit: int | None = None, dry_run: bool = False,
         crawl: str | None = None, months: int = 1) -> int:
    con = _get_duckdb_connection()
    if con is None:
        return 0

    if crawl:
        crawls = [crawl]
    else:
        available = _list_available_crawls(con)
        if not available:
            log.warning(f"Could not list crawl partitions live — falling back to "
                        f"hardcoded {_FALLBACK_CRAWL!r} (may be stale).")
            available = [_FALLBACK_CRAWL]
        else:
            log.info(f"Available crawl partitions (most recent 5): {available[:5]}")
        crawls = available[:max(1, months)]

    log.info(f"Seeding from crawl partition(s): {crawls}")

    tld_filter = _build_tld_filter()
    crawl_filter = ",".join(f"'{c}'" for c in crawls)

    # hive_partitioning explicitly forced true rather than relying on
    # auto-detection over a remote hf:// path (unconfirmed in DuckDB's
    # own docs for this specific backend — safer to be explicit).
    query = f"""
        SELECT
            surt_host_name,
            url_host_tld,
            hcrank,
            fetch_200, fetch_4xx, fetch_5xx, fetch_gone, nutch_gone
        FROM read_parquet(
            '{HF_BASE}/crawl=*/*.parquet',
            hive_partitioning=true
        )
        WHERE crawl IN ({crawl_filter})
          AND url_host_tld IN ({tld_filter})
    """
    if limit:
        # NOTE: LIMIT here caps ROWS READ BEFORE the dead-domain filter
        # below, not final written rows — kept generous by the caller
        # for that reason (see main()'s --limit help text).
        query += f" LIMIT {int(limit) * 2}"

    log.info("Querying Common Crawl's Host Index via Hugging Face (hf://) — "
             "free, no AWS account, no billing...")
    try:
        result = con.execute(query).fetchall()
    except Exception as e:
        log.error(f"DuckDB query against {HF_BASE} failed: {e}")
        return 0

    log.info(f"Host Index: {len(result)} rows matched target TLDs, "
             f"filtering dead hosts...")

    rows_to_insert = []
    seen = set()
    dead_skipped = 0
    for surt_host, tld, hcrank, f200, f4xx, f5xx, fgone, ngone in result:
        if _looks_dead(f200, f4xx, f5xx, fgone, ngone):
            dead_skipped += 1
            continue
        # surt_host_name is reversed (org,commoncrawl,www) — un-reverse to
        # a normal hostname for the crawl step.
        host = ".".join(reversed(surt_host.split(",")))
        if not host or host in seen:
            continue
        seen.add(host)
        rows_to_insert.append({
            "host": host,
            "tld": tld,
            "hcrank": float(hcrank) if hcrank is not None else None,
        })
        if limit and len(rows_to_insert) >= limit:
            break

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
        description="Seed host_crawl_queue from Common Crawl's Host Index "
                    "(via Hugging Face + DuckDB, free)"
    )
    parser.add_argument("--limit", type=int, default=None,
                         help="Cap total rows written (testing) — the underlying "
                              "query reads more rows than this to account for "
                              "dead-host filtering, then stops once this many "
                              "survive")
    parser.add_argument("--dry-run", action="store_true",
                         help="Count without writing to Supabase")
    parser.add_argument("--crawl", type=str, default=None,
                         help="Pin a specific crawl partition, e.g. CC-MAIN-2025-18 "
                              "(default: auto-detect latest available)")
    parser.add_argument("--months", type=int, default=1,
                         help="Number of recent crawl partitions to scan "
                              "(default: 1 — more = broader coverage, longer runtime, "
                              "still free)")
    args = parser.parse_args()

    written = seed(limit=args.limit, dry_run=args.dry_run,
                   crawl=args.crawl, months=args.months)
    if written == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
