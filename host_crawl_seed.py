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
import concurrent.futures
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

# Belt-and-suspenders wall-clock cap for any single DuckDB/hf:// call, on
# top of the http_timeout/http_retries settings in _get_duckdb_connection.
# DuckDB's own C++ core can, in rare cases, still block past its httpfs
# settings (e.g. during DNS resolution or TLS handshake before the HTTP
# layer's timeout logic ever engages). Running each call in a worker
# thread with .result(timeout=...) means a stall raises a clean Python
# TimeoutError this script can log and act on, instead of ever needing an
# external process (GitHub's runner) to be the one that kills it.
_DUCKDB_CALL_TIMEOUT_SECONDS = 180


def _run_with_timeout(fn, *args, timeout=_DUCKDB_CALL_TIMEOUT_SECONDS, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fn, *args, **kwargs)
        return future.result(timeout=timeout)
        # NOTE: on TimeoutError, the DuckDB call keeps running in its
        # thread in the background (Python can't forcibly kill a thread),
        # but the pool is a context manager for exactly one call and this
        # process exits/moves on right after — it doesn't leak across runs.

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

    # IMPORTANT: without HF_TOKEN, every request against huggingface.co is
    # fully anonymous and shares a rate-limit/throttling bucket with every
    # other anonymous request from the SAME source IP range — and GitHub
    # Actions runners come from a small, well-known, heavily-shared IP
    # pool. HF's own docs note anonymous traffic from shared-IP CI runners
    # is more exposed to throttling than token'd traffic. Critically, this
    # can manifest as a SILENT STALL (the connection hangs, never a clean
    # 429) rather than an error DuckDB can retry around — which matches
    # the observed symptom exactly: the log stops right after "Querying
    # Common Crawl's Host Index..." with no Python-level error at all,
    # then GitHub's own runner reports "The operation was canceled" —
    # i.e. something upstream of this process is what ended it, not a
    # try/except inside seed(). Setting HF_TOKEN (free, just needs a HF
    # account) moves this run onto the authenticated tier and is the
    # single highest-leverage fix available. See BULK_DOMAIN_DISCOVERY_NOTES.md.
    if not HF_TOKEN:
        log.warning("HF_TOKEN is not set — running fully anonymous against "
                     "huggingface.co from a shared GitHub Actions IP pool. "
                     "This is the most likely cause of silent hangs/timeouts "
                     "('Error: The operation was canceled.' with no Python-level "
                     "error). Strongly recommend setting the HF_TOKEN secret "
                     "(free — generate at huggingface.co/settings/tokens with "
                     "'read' scope) to move onto the authenticated rate-limit tier.")

    if HF_TOKEN:
        try:
            con.execute(f"CREATE SECRET hf_token (TYPE huggingface, TOKEN '{HF_TOKEN}');")
            log.info("Using HF_TOKEN for authenticated (higher rate-limit) access.")
        except Exception as e:
            log.warning(f"Failed to configure HF_TOKEN secret (continuing "
                        f"anonymously): {e}")

    # DuckDB's httpfs has NO default timeout on hf:// (HTTP) requests — if
    # the remote end stalls (as anonymous/throttled requests can), the
    # query blocks forever with nothing for Python to catch or log. These
    # settings give it explicit, finite bounds so a stall becomes a loud
    # DuckDB IOException (caught below) instead of a silent hang that only
    # GitHub's own infrastructure eventually kills from the outside.
    try:
        con.execute("SET http_timeout = 30000;")     # 30s per HTTP request
        con.execute("SET http_retries = 3;")
        con.execute("SET http_retry_wait_ms = 2000;")
        con.execute("SET http_retry_backoff = 2;")
    except Exception as e:
        log.warning(f"Could not set httpfs timeout/retry options (continuing "
                    f"with DuckDB defaults, which may hang indefinitely on a "
                    f"stalled connection): {e}")

    # A single Host Index crawl partition is ~7GB (confirmed via Common
    # Crawl's own blog post announcing the Host Index) — reading that over
    # a remote hf:// HTTP filesystem can make DuckDB buffer far more in
    # memory than a local-disk read would, especially before predicate
    # pushdown narrows anything down. GitHub-hosted runners' free tier
    # defaults to 7GB RAM total. An uncapped DuckDB can get OOM-killed by
    # the OS — which the Actions runner then reports as the generic
    # "Error: The operation was canceled." with NO Python-level exception
    # ever raised, because SIGKILL from the OOM killer can't be caught,
    # unlike a DuckDB-level error. Explicitly capping DuckDB's memory
    # forces it to spill to disk instead of growing unbounded, trading
    # some speed for actually finishing instead of being killed.
    try:
        con.execute("SET memory_limit = '3GB';")
        con.execute("PRAGMA threads=2;")  # fewer parallel readers = lower peak memory
    except Exception as e:
        log.warning(f"Could not set DuckDB memory_limit (continuing with "
                    f"DuckDB's default, unbounded-by-us memory usage): {e}")

    return con


def _list_available_crawls(con) -> list[str]:
    """Ask Hugging Face's (genuinely anonymous, unlike S3) directory
    listing API which crawl= partitions exist, via DuckDB's glob()."""
    def _do_glob():
        return con.execute(
            f"SELECT DISTINCT regexp_extract(file, 'crawl=([^/]+)', 1) AS crawl "
            f"FROM glob('{HF_BASE}/crawl=*/') "
            f"ORDER BY crawl DESC"
        ).fetchall()

    try:
        rows = _run_with_timeout(_do_glob, timeout=60)
        names = [r[0] for r in rows if r[0]]
        if names:
            return names
    except concurrent.futures.TimeoutError:
        log.warning("Listing crawl partitions via hf:// glob timed out after 60s "
                    "(likely a stalled/throttled anonymous connection to "
                    "huggingface.co — see the HF_TOKEN warning above).")
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

    # IMPORTANT: glob ONLY the specific crawl=X folders actually requested
    # — NOT 'crawl=*/*.parquet' filtered afterward with WHERE crawl IN
    # (...). An earlier run timed out doing exactly that: globbing all 26
    # crawl partitions over a remote hf:// connection before the WHERE
    # clause ever got a chance to prune anything.
    #
    # SECOND, MORE SERIOUS issue found after that fix still didn't resolve
    # a live "Error: The operation was canceled." (even with HF_TOKEN set
    # and authenticating correctly, ruling out anonymous throttling as the
    # cause): a single Host Index crawl partition is ~7GB (Common Crawl's
    # own blog post). The prior version of this function ran ONE big
    # UNION ALL query over read_parquet('crawl={c}/*.parquet') — i.e. every
    # file in the partition at once — then called .fetchall(), which
    # materializes the ENTIRE filtered result as Python objects in memory
    # on top of whatever DuckDB itself buffers while streaming/decoding
    # Parquet over hf://'s HTTP layer. On a GitHub-hosted runner (7GB RAM
    # on the free tier), this is a real, plausible way to get OOM-killed by
    # the OS — which the Actions runner reports as the generic "Error: The
    # operation was canceled." with NO Python-level exception ever raised
    # (a SIGKILL from the OOM killer can't be caught), exactly matching the
    # observed symptom (log stops mid-query, no logged error before the
    # runner's own cancellation message).
    #
    # Fix: enumerate the individual Parquet files inside each crawl
    # partition first (a cheap glob, not a full read), then process ONE
    # FILE AT A TIME — query it, write its matching rows to Supabase, and
    # let DuckDB/Python release that file's memory before moving to the
    # next. Peak memory now scales with ONE file's size, not the whole
    # ~7GB partition. Combined with the memory_limit set in
    # _get_duckdb_connection, this should keep peak RSS well under the
    # runner's ceiling regardless of how large a partition is.
    def _list_partition_files(crawl_name: str) -> list[str]:
        try:
            rows = _run_with_timeout(
                lambda: con.execute(
                    f"SELECT file FROM glob('{HF_BASE}/crawl={crawl_name}/*.parquet')"
                ).fetchall(),
                timeout=60,
            )
            return [r[0] for r in rows]
        except concurrent.futures.TimeoutError:
            log.warning(f"Listing files in crawl={crawl_name} timed out after 60s.")
            return []
        except Exception as e:
            log.warning(f"Could not list files in crawl={crawl_name}: {e}")
            return []

    files_by_crawl = {}
    for c in crawls:
        files = _list_partition_files(c)
        if not files:
            # Fall back to the old single-glob-string approach for this
            # crawl if per-file listing failed for some reason — still
            # scoped to one crawl, so not the original all-26-partitions
            # bug, just less memory-safe.
            files = [f"{HF_BASE}/crawl={c}/*.parquet"]
        log.info(f"crawl={c}: {len(files)} Parquet file(s) to scan")
        files_by_crawl[c] = files

    total_files = sum(len(f) for f in files_by_crawl.values())
    log.info(f"Querying Common Crawl's Host Index via Hugging Face (hf://) — "
             f"free, no AWS account, no billing — processing {total_files} "
             f"file(s) one at a time to bound memory use...")

    rows_to_insert = []
    seen = set()
    dead_skipped = 0
    total_matched = 0
    query_timeout = 300  # per FILE now, not per whole partition — 5 min is generous
    file_num = 0

    for c, files in files_by_crawl.items():
        for fpath in files:
            file_num += 1
            per_file_query = f"""
                SELECT surt_host_name, url_host_tld, hcrank,
                       fetch_200, fetch_4xx, fetch_5xx, fetch_gone, nutch_gone
                FROM read_parquet('{fpath}')
                WHERE url_host_tld IN ({tld_filter})
            """

            def _do_query(q=per_file_query):
                return con.execute(q).fetchall()

            try:
                file_result = _run_with_timeout(_do_query, timeout=query_timeout)
            except concurrent.futures.TimeoutError:
                log.warning(f"  [{file_num}/{total_files}] query timed out after "
                            f"{query_timeout}s on {fpath} — skipping this file, "
                            f"continuing with the rest.")
                continue
            except Exception as e:
                log.warning(f"  [{file_num}/{total_files}] query failed on "
                            f"{fpath}: {e} — skipping this file, continuing.")
                continue

            total_matched += len(file_result)
            log.info(f"  [{file_num}/{total_files}] {len(file_result)} rows matched "
                     f"in this file ({total_matched} total so far)")

            for surt_host, tld, hcrank, f200, f4xx, f5xx, fgone, ngone in file_result:
                if _looks_dead(f200, f4xx, f5xx, fgone, ngone):
                    dead_skipped += 1
                    continue
                # surt_host_name is reversed (org,commoncrawl,www) — un-reverse
                # to a normal hostname for the crawl step.
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
            # free this file's raw result before moving to the next
            del file_result
            if limit and len(rows_to_insert) >= limit:
                break
        if limit and len(rows_to_insert) >= limit:
            break

    log.info(f"Host Index: {total_matched} rows matched target TLDs across "
             f"{file_num} file(s), filtering dead hosts...")

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
