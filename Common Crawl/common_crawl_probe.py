"""
COMMON CRAWL PROBE (renamed from host_crawl_v2.py, 2026-08) — a thin,
disposable probe source on top of node.py (the permanent engine). This
file's only job: stream candidate hostnames out of Common Crawl's Host
Index (via DuckDB, one Parquet file at a time — see STREAMING below) and
hand them to node.crawl_batch(). All fetch/parse/detect/write logic lives
in node.py. No separate common_crawl_seed.py: seeding and crawling are
interleaved on purpose (see STREAMING below) — a separate seed step would
mean writing the ENTIRE host list to disk first, reintroducing the exact
memory problem this design avoids.

STREAMING, ONE FILE AT A TIME: a real incident — an earlier version
accumulated 126M hostnames into one Python list and got OOM-killed by
GitHub Actions' runner. Fixed by (1) sharding pushed into the SQL query
itself (hash(surt_host_name) % shard_count = shard_index), and (2)
seeding+crawling interleaved per Parquet file — each file's hosts are
crawled and dropped before the next file is even queried, so memory never
scales with partition count or host count. TIME_BUDGET_MINUTES is the
only thing bounding a run's length.

"LATEST" PARTITION CAVEAT: this dataset (commoncrawl/host-index-testing-v2
on Hugging Face) has its own, much slower update cadence than Common
Crawl's raw monthly crawl archives — confirmed live, it hasn't been
refreshed since Nov 2025 (stuck at CC-MAIN-2025-18). Not a bug here; there
is simply nothing newer in THIS dataset yet. --crawl/no-argument always
queries the live listing fresh, so it'll pick up a refresh automatically
if/when one happens.

Usage:
    python common_crawl_probe.py                                        # 1 partition, unsharded (dev only)
    python common_crawl_probe.py --crawl CC-MAIN-2025-18 --partitions 3  # 3 contiguous partitions
    python common_crawl_probe.py --crawl-list CC-MAIN-2025-18,CC-MAIN-2024-42
    python common_crawl_probe.py --shard-index 0 --shard-count 10        # production shape
"""
import argparse
import asyncio
import concurrent.futures
import logging
import os
import sys
import time
from collections import Counter

import aiohttp
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # Crawler/, for node.py
import node  # noqa: E402

log = logging.getLogger("common_crawl_probe")

# Optional — HF gives a higher rate-limit tier for anonymous CI traffic;
# not required, the dataset is fully public.
HF_TOKEN = os.environ.get("HF_TOKEN", "")
HF_DATASET = "commoncrawl/host-index-testing-v2"
HF_BASE = f"hf://datasets/{HF_DATASET}/data"
_FALLBACK_CRAWL = "CC-MAIN-2025-18"

# Candidate pre-filter (never the real country decision — see node.detect_country).
TARGET_TLDS = {
    "us", "uk", "ca", "de", "au", "ie", "mt",
    "com", "net", "io", "co", "app", "dev",
}
TARGET_SUFFIXES_EXTRA = {"co.uk", "com.au"}

_DUCKDB_CALL_TIMEOUT_SECONDS = 180


def _run_with_timeout(fn, *args, timeout=_DUCKDB_CALL_TIMEOUT_SECONDS, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fn, *args, **kwargs)
        return future.result(timeout=timeout)


def _build_tld_filter() -> str:
    parts = [f"'{t}'" for t in TARGET_TLDS] + [f"'{t}'" for t in TARGET_SUFFIXES_EXTRA]
    return ",".join(parts)


def _looks_dead(fetch_200, fetch_4xx, fetch_5xx, fetch_gone, nutch_gone) -> bool:
    """Only treated as dead if Common Crawl's own attempts NEVER got a
    single 2xx — biased toward keeping a host if in doubt."""
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
        log.error(f"Failed to load DuckDB's httpfs extension: {e}")
        return None
    if not HF_TOKEN:
        log.warning("HF_TOKEN not set — anonymous access can silently stall. Recommend setting it "
                     "(free — huggingface.co/settings/tokens, 'read' scope).")
    else:
        try:
            con.execute(f"CREATE SECRET hf_token (TYPE huggingface, TOKEN '{HF_TOKEN}');")
        except Exception as e:
            log.warning(f"Failed to configure HF_TOKEN (continuing anonymously): {e}")
    try:
        con.execute("SET http_timeout = 30000;")
        con.execute("SET http_retries = 3;")
        con.execute("SET http_retry_wait_ms = 2000;")
        con.execute("SET http_retry_backoff = 2;")
        con.execute("SET memory_limit = '3GB';")  # one partition is ~7GB per CC's own blog post
        con.execute("PRAGMA threads=2;")
    except Exception as e:
        log.warning(f"Could not set DuckDB options (continuing with defaults): {e}")
    return con


def _list_all_crawl_names(con) -> list[str]:
    """Live-lists every partition Hugging Face has, most recent first.
    Empty list if the listing itself fails — callers fall back to a
    single hardcoded partition in that case."""
    try:
        rows = _run_with_timeout(
            lambda: con.execute(
                f"SELECT DISTINCT regexp_extract(file, 'crawl=([^/]+)', 1) AS crawl "
                f"FROM glob('{HF_BASE}/crawl=*/') ORDER BY crawl DESC"
            ).fetchall(), timeout=60,
        )
        return [r[0] for r in rows if r[0]]
    except concurrent.futures.TimeoutError:
        log.warning("Listing crawl partitions timed out after 60s.")
    except Exception as e:
        log.warning(f"Could not list crawl partitions live: {e}")
    return []


def _resolve_crawl_names(con, pinned: str | None, count: int) -> list[str]:
    """Resolves `count` partitions, most-recent-first, starting at
    `pinned` (or the true latest) and walking backward. Falls back to a
    single partition if the live listing fails."""
    names = _list_all_crawl_names(con)
    if names:
        log.info(f"Available crawl partitions (most recent 5 of {len(names)}): {names[:5]}")
        start = names.index(pinned) if pinned in names else 0
        selected = names[start:start + count]
        if len(selected) < count:
            log.warning(f"Only {len(selected)}/{count} partition(s) available at/older than the "
                        f"start point.")
        return selected
    single = pinned or _FALLBACK_CRAWL
    log.warning(f"Falling back to a single hardcoded partition {single!r} (may be stale) — live "
                f"listing failed.")
    return [single]


def _resolve_explicit_crawl_names(con, requested: list[str]) -> list[str]:
    """--crawl-list: a specific, possibly non-contiguous set of partitions
    named directly (e.g. spread across years to sample a more diverse
    company population than a contiguous walk gives). Validates against
    the live listing when available; trusts the list as-is otherwise."""
    names = _list_all_crawl_names(con)
    if not names:
        log.warning("Live partition listing failed — trusting --crawl-list as given, unvalidated.")
        return requested
    missing = [n for n in requested if n not in names]
    if missing:
        log.warning(f"{len(missing)}/{len(requested)} requested partition(s) not found: {missing}")
    return [n for n in requested if n in names]


def _list_partition_files(con, crawl_name: str) -> list[str]:
    try:
        files = _run_with_timeout(
            lambda: con.execute(
                f"SELECT file FROM glob('{HF_BASE}/crawl={crawl_name}/*.parquet')"
            ).fetchall(), timeout=60,
        )
        files = [r[0] for r in files]
    except concurrent.futures.TimeoutError:
        log.warning(f"Listing files in crawl={crawl_name} timed out after 60s.")
        files = []
    except Exception as e:
        log.warning(f"Could not list files in crawl={crawl_name}: {e}")
        files = []
    if not files:
        files = [f"{HF_BASE}/crawl={crawl_name}/*.parquet"]
    return files


def iter_seed_hosts_by_file(partitions: list[str], shard_index: int | None, shard_count: int | None,
                             start_file_index: int = 0):
    """Streams hostnames across one or more partitions, ONE FILE AT A
    TIME — yields (crawl_name, partition_num, total_partitions, file_num,
    total_files, hosts). Sharding happens IN THE SQL query (hash() %
    shard_count), not by slicing a Python list — see module docstring."""
    con = _get_duckdb_connection()
    if con is None:
        return

    sharded = shard_index is not None and shard_count is not None
    if not sharded:
        log.warning("Running UNSHARDED — a file's full candidate set (10M+ rows) loads into memory "
                    "at once. Use --shard-index/--shard-count for production runs.")

    tld_filter = _build_tld_filter()
    shard_clause = f"AND (hash(surt_host_name) % {shard_count}) = {shard_index}" if sharded else ""
    query_timeout = 300

    for partition_num, crawl_name in enumerate(partitions, start=1):
        log.info(f"── Partition {partition_num}/{len(partitions)}: {crawl_name} ──")
        files = _list_partition_files(con, crawl_name)
        if partition_num == 1 and start_file_index:
            skipped = files[:start_file_index]
            files = files[start_file_index:]
            log.info(f"  --start-file-index {start_file_index}: skipping {len(skipped)} file(s)")
        log.info(f"  {len(files)} file(s) to scan" + (f" — shard {shard_index}/{shard_count}" if sharded else ""))

        total_dead_skipped = 0
        total_hosts = 0
        for file_num, fpath in enumerate(files, start=1):
            query = f"""
                SELECT surt_host_name, fetch_200, fetch_4xx, fetch_5xx, fetch_gone, nutch_gone
                FROM read_parquet('{fpath}')
                WHERE url_host_tld IN ({tld_filter})
                {shard_clause}
            """
            try:
                rows = _run_with_timeout(lambda q=query: con.execute(q).fetchall(), timeout=query_timeout)
            except concurrent.futures.TimeoutError:
                log.warning(f"  file {file_num}/{len(files)}: query timed out — skipping.")
                continue
            except Exception as e:
                log.warning(f"  file {file_num}/{len(files)}: query failed — skipping: {e}")
                continue

            # Dedup is PER-FILE only, not cross-run — a persistent `seen`
            # set would regrow to the same OOM-risk size the streaming fix
            # eliminated. A host appearing in >1 file just gets crawled
            # twice (harmless — (ats,slug) dedup at the write path still
            # prevents any duplicate row).
            file_hosts: list[str] = []
            seen_this_file: set[str] = set()
            dead_skipped = 0
            for surt_host, f200, f4xx, f5xx, fgone, ngone in rows:
                if not surt_host:
                    continue
                if _looks_dead(f200, f4xx, f5xx, fgone, ngone):
                    dead_skipped += 1
                    continue
                domain = ".".join(reversed(surt_host.split(",")))
                if domain in seen_this_file:
                    continue
                seen_this_file.add(domain)
                file_hosts.append(domain)
            total_dead_skipped += dead_skipped
            total_hosts += len(file_hosts)
            log.info(f"  file {file_num}/{len(files)}: {len(file_hosts)} live hosts (of {len(rows)} "
                     f"candidates, {dead_skipped} dead-skipped) — {total_hosts} seeded so far")
            yield crawl_name, partition_num, len(partitions), file_num, len(files), file_hosts


async def run_host_crawl(crawl: str | None, partitions_count: int, shard_index: int | None,
                          shard_count: int | None, concurrency: int,
                          time_budget_minutes: int, crawl_list: list[str] | None = None,
                          start_file_index: int = 0) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(f"── Host Crawl v2{label} ──")
    log.info(f"  concurrency={concurrency}  parse_workers={node.PARSE_WORKERS}  "
             f"time_budget={time_budget_minutes}min (shared across all requested partitions)")

    con = _get_duckdb_connection()
    if con is None:
        return
    if crawl_list:
        log.info(f"  explicit --crawl-list given ({len(crawl_list)} requested) — ignoring --crawl/--partitions")
        partitions = _resolve_explicit_crawl_names(con, crawl_list)
    else:
        partitions = _resolve_crawl_names(con, crawl, partitions_count)
    if not partitions:
        log.error("No partition(s) resolved — nothing to crawl.")
        return
    log.info(f"  partitions ({len(partitions)}): {partitions}")

    connector = node.new_connector()
    sem = asyncio.Semaphore(concurrency)
    stats = Counter()
    found_rows: list[dict] = []
    parse_pool = node.new_parse_pool()
    time_budget_seconds = time_budget_minutes * 60
    crawl_start = time.monotonic()
    elapsed, rate = 0.0, 0.0
    time_budget_hit = False
    total_hosts_seen = 0
    partitions_completed = 0
    last_partition_name = partitions[0]

    try:
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
            for partition_name, partition_num, total_partitions, file_num, total_files, file_hosts \
                    in iter_seed_hosts_by_file(partitions, shard_index, shard_count, start_file_index):
                last_partition_name = partition_name
                if time.monotonic() - crawl_start >= time_budget_seconds:
                    time_budget_hit = True
                    log.warning(f"  time budget reached before {partition_name} file "
                                f"{file_num}/{total_files} — stopping, remaining seeding skipped too.")
                    break
                partitions_completed = partition_num - 1
                if not file_hosts:
                    continue
                total_hosts_seen += len(file_hosts)
                _, elapsed, rate, file_time_hit = await node.crawl_batch(
                    file_hosts, session, sem, stats, parse_pool, node.ACCEPT_ANY_COUNTRY,
                    "common_crawl_probe", found_rows, crawl_start, time_budget_seconds,
                    time_budget_minutes, batch_size=2000, unit_label="hosts")
                if file_time_hit:
                    time_budget_hit = True
                    break
                if file_num == total_files:
                    partitions_completed = partition_num
    finally:
        parse_pool.shutdown(wait=True)

    total_hits = stats["hits_from_homepage"] + stats["hits_from_career_path"] + stats["hits_from_sitemap"]
    hosts_n = max(stats["companies_attempted"], 1)

    log.info("")
    log.info(f"── Host Crawl v2{label} summary ──")
    log.info(f"  status:      {'STOPPED EARLY — time budget reached mid-' + last_partition_name if time_budget_hit else 'complete — all requested partitions covered'}")
    log.info(f"  partitions:  {partitions_completed}/{len(partitions)} fully covered ({partitions})")
    log.info(f"  hosts:       {stats['companies_attempted']} attempted, {total_hosts_seen} seeded")
    log.info(f"  time:        {elapsed:.0f}s of {time_budget_seconds:.0f}s budget, {rate:.1f} hosts/sec avg")
    log.info("")
    log.info("  accuracy:")
    log.info(f"    ATS hits found:  {total_hits} ({total_hits / hosts_n * 100:.2f}% of hosts attempted)")
    log.info(f"    with country:    {total_hits - stats['written_without_country']} "
             f"({(1 - stats['written_without_country'] / max(total_hits, 1)) * 100:.1f}% of hits)")
    log.info(f"    no ATS found:    {stats['dropped_no_ats']} ({stats['dropped_no_ats'] / hosts_n * 100:.1f}%)")
    log.info(f"    unreachable:     {stats['homepage_unreachable']} ({stats['homepage_unreachable'] / hosts_n * 100:.1f}%)")
    if total_hits:
        log.info("")
        log.info("  hits by tier:")
        log.info(f"    homepage:     {stats['hits_from_homepage']} ({stats['hits_from_homepage'] / total_hits * 100:.1f}%)")
        log.info(f"    career_path:  {stats['hits_from_career_path']} ({stats['hits_from_career_path'] / total_hits * 100:.1f}%)")
        log.info(f"    sitemap:      {stats['hits_from_sitemap']} ({stats['hits_from_sitemap'] / total_hits * 100:.1f}%)")
    ats_breakdown = Counter(r["ats"] for r in found_rows)
    if ats_breakdown:
        log.info("")
        log.info("  hits by platform:")
        for ats, n in ats_breakdown.most_common():
            log.info(f"    {ats}: {n}")
    country_breakdown = Counter(r["country"] or "unknown" for r in found_rows)
    if country_breakdown:
        log.info("")
        log.info("  hits by country:")
        for country, n in country_breakdown.most_common():
            log.info(f"    {country}: {n}")


def main():
    parser = argparse.ArgumentParser(
        description="Host Crawl v2 — follow-links ATS discovery across Common Crawl partitions.")
    parser.add_argument("--crawl", type=str, default=None,
                         help="Pin the starting partition. Blank = auto-detect latest EVERY run. "
                              "Ignored if --crawl-list is given.")
    parser.add_argument("--partitions", type=int, default=1,
                         help="How many CONTIGUOUS partitions, walking backward from --crawl. "
                              "Ignored if --crawl-list is given.")
    parser.add_argument("--crawl-list", type=str, default=None,
                         help="Comma-separated exact partition names, e.g. "
                              "'CC-MAIN-2025-18,CC-MAIN-2024-42'. Overrides --crawl/--partitions.")
    parser.add_argument("--start-file-index", type=int, default=0,
                         help="Skip this many files in the FIRST partition before starting.")
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=node.CRAWL_CONCURRENCY)
    parser.add_argument("--time-budget-minutes", type=int, default=node.TIME_BUDGET_MINUTES)
    args = parser.parse_args()
    crawl_list = [c.strip() for c in args.crawl_list.split(",") if c.strip()] if args.crawl_list else None

    asyncio.run(run_host_crawl(args.crawl, args.partitions, args.shard_index, args.shard_count,
                                args.concurrency, args.time_budget_minutes, crawl_list=crawl_list,
                                start_file_index=args.start_file_index))


if __name__ == "__main__":
    main()
