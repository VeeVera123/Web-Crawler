"""
Host Crawl — Seed Step
=======================
Populates host_crawl_queue with candidate hostnames from Common Crawl's
COLUMNAR INDEX (the URL-level Parquet index of every page CC has crawled).

RETHOUGHT 2026-08 (v1): switched from the Host Index to the columnar
index, since the Host Index has NO free access path at all (its only
free HTTPS mirror, data.commoncrawl.org, blanket-disallows every
crawler in robots.txt — confirmed live).

RETHOUGHT 2026-08 (v2): the first columnar-index attempt tried to have
DuckDB's httpfs extension read s3://commoncrawl directly, assuming "no
CREATE SECRET configured" would mean "send an unsigned request",
matching `aws s3 --no-sign-request`. A live GitHub Actions run proved
that assumption wrong: DuckDB's S3 client always attempts to sign
requests once httpfs is loaded (confirmed against DuckDB's own docs —
there is no documented anonymous/unsigned secret provider, only `config`
with explicit keys or `credential_chain`), so it hit a real 403
AccessDenied even though the bucket itself is genuinely open to
anonymous reads.

Fix: stop asking DuckDB to talk to S3 at all. Use the AWS CLI instead
(`aws s3 ... --no-sign-request`), which sends a genuinely unsigned
request and is Common Crawl's own documented anonymous-access method.

RETHOUGHT 2026-08 (v3 — THIS VERSION): v2's design downloaded every
Parquet part file for a crawl BEFORE querying any of them. Each monthly
crawl's subset=warc/ folder is ~300 files averaging ~700MB each — a full
crawl is ~200-300GB. GitHub Actions' standard runners only have ~14GB of
disk, so a real (non-testing, no --max-files cap) run would have failed
outright by filling the disk, not just been slow. Fixed by processing
ONE FILE AT A TIME: download a part file, query+filter it locally with
DuckDB, accumulate the (small) list of matching domains, delete the
Parquet file, move to the next. Disk usage stays bounded to ~1 file
(under ~1GB) at any moment regardless of how many files or crawls are
scanned — this is the only change from v2; the access method (AWS CLI
--no-sign-request) is unchanged and was already correct.

TLDs cover .com, .net, .us, .uk, .co.uk, .ca, .de, .au, .com.au, .ie,
.mt, plus generic .io/.co/.app/.dev in ONE pass — see TARGET_TLDS.

Usage:
    python host_crawl_seed.py                       # seed from latest crawl
    python host_crawl_seed.py --dry-run             # count without writing
    python host_crawl_seed.py --limit 500000        # cap rows written (testing)
    python host_crawl_seed.py --crawl CC-MAIN-2026-30   # pin a specific crawl
    python host_crawl_seed.py --max-files 5          # cap Parquet files downloaded (testing)
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tempfile

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

CC_S3_BUCKET = "commoncrawl"
CC_INDEX_BASE = "cc-index/table/cc-main/warc"

# Used only if live discovery via `aws s3 ls` fails — logged loudly, so a
# stale value here never silently queries a partition that no longer
# exists without the operator knowing why.
_FALLBACK_CRAWL = "CC-MAIN-2026-30"

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


def _check_aws_cli() -> bool:
    if shutil.which("aws") is None:
        log.error("aws CLI not found on PATH — install it (e.g. `pip install awscli` "
                   "or apt/brew) to run this step. DuckDB's own S3 client can't do "
                   "genuinely anonymous/unsigned reads (confirmed via a live 403 "
                   "AccessDenied against s3://commoncrawl even with no credentials "
                   "configured) — only the AWS CLI's --no-sign-request flag sends a "
                   "truly unsigned request, which is what this public bucket needs.")
        return False
    return True


def _list_crawl_partitions() -> list[str]:
    """List available crawl= partitions via anonymous `aws s3 ls`. Returns
    names sorted newest-first (crawl names sort correctly as strings:
    CC-MAIN-2026-30 > CC-MAIN-2026-25 lexicographically, since year/week
    are both fixed-width zero-padded)."""
    try:
        result = subprocess.run(
            ["aws", "s3", "ls", f"s3://{CC_S3_BUCKET}/{CC_INDEX_BASE}/",
             "--no-sign-request"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            log.warning(f"`aws s3 ls` failed (exit {result.returncode}): "
                        f"{result.stderr.strip()[:300]}")
            return []
        names = []
        for line in result.stdout.splitlines():
            # Lines look like "                           PRE crawl=CC-MAIN-2026-30/"
            line = line.strip()
            if line.startswith("PRE ") and "crawl=" in line:
                name = line.split("crawl=", 1)[1].rstrip("/")
                if name:
                    names.append(name)
        names.sort(reverse=True)
        return names
    except Exception as e:
        log.warning(f"Failed to list crawl partitions via aws s3 ls: {e}")
        return []


def _list_parquet_files(crawl: str) -> list[str]:
    """List the actual .parquet object keys under one crawl partition's
    subset=warc/ folder, via anonymous `aws s3 ls`."""
    prefix = f"{CC_INDEX_BASE}/crawl={crawl}/subset=warc/"
    try:
        result = subprocess.run(
            ["aws", "s3", "ls", f"s3://{CC_S3_BUCKET}/{prefix}",
             "--no-sign-request"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            log.warning(f"`aws s3 ls` failed for {crawl}: {result.stderr.strip()[:300]}")
            return []
        files = []
        for line in result.stdout.splitlines():
            parts = line.split()
            if parts and parts[-1].endswith(".parquet"):
                files.append(prefix + parts[-1])
        return files
    except Exception as e:
        log.warning(f"Failed to list Parquet files for {crawl}: {e}")
        return []


def _download_one_file(key: str, local_path: str) -> bool:
    """Anonymously download one S3 object key to local disk via the AWS
    CLI. Returns True on success — failures are logged and the caller
    skips this file rather than aborting the whole run."""
    try:
        result = subprocess.run(
            ["aws", "s3", "cp", f"s3://{CC_S3_BUCKET}/{key}", local_path,
             "--no-sign-request"],
            capture_output=True, text=True, timeout=600,
        )
        if result.returncode != 0:
            log.warning(f"Failed to download {key}: {result.stderr.strip()[:300]}")
            return False
        return True
    except Exception as e:
        log.warning(f"Failed to download {key}: {e}")
        return False


def seed(limit: int | None = None, dry_run: bool = False,
         crawl: str | None = None, months: int = 1,
         max_files: int | None = None) -> int:
    if not _check_aws_cli():
        return 0

    try:
        import duckdb
    except ImportError:
        log.error("duckdb not installed — pip install duckdb to run this step.")
        return 0

    crawls: list[str] = []
    if crawl:
        crawls = [crawl]
    else:
        available = _list_crawl_partitions()
        if not available:
            log.warning(f"Could not list crawl partitions live — falling back to "
                        f"hardcoded {_FALLBACK_CRAWL!r} (may be stale).")
            available = [_FALLBACK_CRAWL]
        else:
            log.info(f"Available crawl partitions (most recent 5): {available[:5]}")
        crawls = available[:max(1, months)]

    log.info(f"Seeding from crawl partition(s): {crawls}")

    # ── Discover + download the actual Parquet files ────
    all_keys: list[str] = []
    for c in crawls:
        keys = _list_parquet_files(c)
        if not keys:
            log.warning(f"No Parquet files found for crawl {c} — skipping it.")
            continue
        log.info(f"  {c}: {len(keys)} Parquet part files available")
        all_keys.extend(keys)

    if not all_keys:
        log.error("No Parquet files found across any crawl partition — aborting seed.")
        return 0

    if max_files:
        all_keys = all_keys[:max_files]
        log.info(f"Capped to {len(all_keys)} files via --max-files (testing mode).")

    log.info(f"Processing {len(all_keys)} Parquet file(s) ONE AT A TIME "
             f"(download -> filter -> delete) to keep disk usage bounded to "
             f"a single file (~700MB) regardless of total file/crawl count.")

    tld_filter = _build_tld_filter()
    con = duckdb.connect()
    seen: set[str] = set()
    rows_to_insert: list[dict] = []
    files_ok = 0

    tmp_dir = tempfile.mkdtemp(prefix="cc_columnar_")
    try:
        for i, key in enumerate(all_keys):
            local_path = os.path.join(tmp_dir, "part.parquet")
            if os.path.exists(local_path):
                os.remove(local_path)  # belt-and-suspenders before each download

            if not _download_one_file(key, local_path):
                continue

            query = f"""
                SELECT DISTINCT
                    url_host_registered_domain,
                    url_host_tld
                FROM read_parquet('{local_path}')
                WHERE url_host_tld IN ({tld_filter})
                  AND fetch_status = 200
                  AND url_host_registered_domain IS NOT NULL
            """
            try:
                for domain, tld in con.execute(query).fetchall():
                    if domain and domain not in seen:
                        seen.add(domain)
                        rows_to_insert.append({"host": domain, "tld": tld, "hcrank": None})
            except Exception as e:
                log.warning(f"Query failed for {key}: {e}")
            finally:
                # Delete immediately — this is the whole point: never hold
                # more than one part file on disk at once.
                if os.path.exists(local_path):
                    os.remove(local_path)

            files_ok += 1
            if (i + 1) % 5 == 0 or i == len(all_keys) - 1:
                log.info(f"  processed {i + 1}/{len(all_keys)} files "
                         f"({files_ok} downloaded OK) — "
                         f"{len(rows_to_insert)} distinct domains so far")

            if limit and len(rows_to_insert) >= limit:
                log.info(f"Hit --limit {limit} — stopping early "
                         f"({i + 1}/{len(all_keys)} files processed).")
                break
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    if limit:
        rows_to_insert = rows_to_insert[:limit]

    log.info(f"Columnar index: {len(rows_to_insert)} distinct registered domains "
             f"matched target TLDs with a successful fetch "
             f"(from {files_ok}/{len(all_keys)} files processed successfully).")

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
        description="Seed host_crawl_queue from Common Crawl's columnar index "
                    "(anonymous AWS CLI download + local DuckDB query)"
    )
    parser.add_argument("--limit", type=int, default=None,
                         help="Cap total rows written (testing)")
    parser.add_argument("--dry-run", action="store_true",
                         help="Count without writing to Supabase")
    parser.add_argument("--crawl", type=str, default=None,
                         help="Pin a specific crawl partition, e.g. CC-MAIN-2026-30 "
                              "(default: auto-detect latest available)")
    parser.add_argument("--months", type=int, default=1,
                         help="Number of recent monthly crawl partitions to scan "
                              "(default: 1 — more months = more files to download)")
    parser.add_argument("--max-files", type=int, default=None,
                         help="Cap the number of Parquet part files downloaded "
                              "(testing — a full crawl partition can be dozens of "
                              "files, each potentially GBs)")
    args = parser.parse_args()

    written = seed(limit=args.limit, dry_run=args.dry_run,
                   crawl=args.crawl, months=args.months,
                   max_files=args.max_files)
    if written == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
