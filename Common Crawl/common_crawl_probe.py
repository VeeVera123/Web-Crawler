"""
COMMON CRAWL PROBE (renamed from host_crawl_v2.py, 2026-08) — a thin,
disposable probe source on top of node.py (the permanent engine). This
file's only job: stream candidate hostnames out of Common Crawl's OWN
official columnar index ("CC-Index Table", via DuckDB, one Parquet file
at a time — see STREAMING below) and hand them to node.crawl_batch(). All
fetch/parse/detect/write logic lives in node.py. No separate
common_crawl_seed.py: seeding and crawling are interleaved on purpose (see
STREAMING below) — a separate seed step would mean writing the ENTIRE host
list to disk first, reintroducing the exact memory problem this design
solves.

DATA SOURCE: Switched to querying Common Crawl's own official CC-Index Table
directly — published by Common Crawl themselves, in lockstep with every
monthly crawl release, no third party, no auth, no token:
partition list:  https://index.commoncrawl.org/collinfo.json
parquet files:   https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl={crawl}/subset=warc/*.parquet

SPEED OPTIMIZATIONS:
1. Query Rewrite: Replaced the heavy GROUP BY aggregation with a simple 
   SELECT DISTINCT filtered on 2xx responses. DuckDB now handles liveness 
   and deduplication natively in SQL, turning a complex CPU-bound aggregation 
   into a highly optimized, push-down filtered scan.
2. True Parallel Prefetching: Upgraded to a rolling window of 4 parallel 
   DuckDB queries. Each worker gets its own connection to query multiple 
   Parquet files in the background simultaneously, overlapping I/O with crawling.
3. HTTPS CDN: Uses the official https://data.commoncrawl.org/ endpoint, which 
   is backed by a fast CDN and avoids DuckDB S3 anonymous access authentication 
   quirks (403 Forbidden) entirely.

STREAMING, ONE FILE AT A TIME: Fixed by (1) sharding pushed into the SQL query
itself (hash(url_host_name) % shard_count = shard_index), and (2)
seeding+crawling interleaved per Parquet file — each file's hosts are
crawled and dropped before the next file is even queried, so memory never
scales with partition count or host count. TIME_BUDGET_MINUTES is the
only thing bounding a run's length.

RESUME (2026-09): each (shard_index, shard_count) checkpoints which file
number it last fully finished to the SAME Supabase checkpoint table
opendata_probe.py/people_data_labs_probe.py use (node.save_crawl_checkpoint,
keyed by source="common_c") — a re-run of the same shard picks
up right after the last completed file automatically.
"""
import argparse
import asyncio
import concurrent.futures
import gzip
import logging
import os
import sys
import time
from collections import Counter
from datetime import datetime
import aiohttp
import requests
from dotenv import load_dotenv

load_dotenv()

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # for node.py/discovery.py
import node  # noqa: E402

log = logging.getLogger("common_crawl_probe")

CC_DATA_BASE = "https://data.commoncrawl.org"
CC_COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
_FALLBACK_CRAWL = "CC-MAIN-2025-18"

TARGET_TLDS = {
    "us", "uk", "ca", "de", "au", "ie", "mt",
    "com", "net", "io", "co", "app", "dev",
}
TARGET_SUFFIXES_EXTRA = {"co.uk", "com.au"}

_DUCKDB_CALL_TIMEOUT_SECONDS = 180
_HTTP_TIMEOUT_SECONDS = 30
PREFETCH_WORKERS = 4  # Query 4 files in the background simultaneously

def _build_tld_filter() -> str:
    parts = [f"'{t}'" for t in TARGET_TLDS] + [f"'{t}'" for t in TARGET_SUFFIXES_EXTRA]
    return ",".join(parts)

def _get_duckdb_connection():
    try:
        import duckdb
    except ImportError:
        log.error("duckdb not installed — pip install duckdb to run this step.")
        return None
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        # Optimize HTTP range requests for the Common Crawl CDN
        con.execute("SET http_timeout = 30000;")
        con.execute("SET http_retries = 3;")
        con.execute("SET http_retry_wait_ms = 2000;")
        con.execute("SET http_retry_backoff = 2;")
        con.execute("SET memory_limit = '3GB';")
        # Cap at 2 threads per connection to avoid memory spikes when 
        # running multiple parallel queries in the thread pool
        con.execute("PRAGMA threads=2;")
    except Exception as e:
        log.warning(f"Could not set DuckDB options (continuing with defaults): {e}")
    return con

def _list_all_crawl_names() -> list[str]:
    try:
        resp = requests.get(CC_COLLINFO_URL, timeout=_HTTP_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()
        return [row["id"] for row in data if row.get("id", "").startswith("CC-MAIN-")]
    except Exception as e:
        log.warning(f"Could not fetch {CC_COLLINFO_URL}: {e}")
        return []

def _resolve_crawl_names(pinned: str | None, count: int) -> list[str]:
    names = _list_all_crawl_names()
    if names:
        log.info(f"Available crawl partitions (most recent 5 of {len(names)}): {names[:5]}")
        start = names.index(pinned) if pinned in names else 0
        selected = names[start:start + count]
        if len(selected) < count:
            log.warning(f"Only {len(selected)}/{count} partition(s) available at/older than the start point.")
        return selected
    single = pinned or _FALLBACK_CRAWL
    log.warning(f"Falling back to a single hardcoded partition {single!r} — live listing failed.")
    return [single]

def _resolve_explicit_crawl_names(requested: list[str]) -> list[str]:
    names = _list_all_crawl_names()
    if not names:
        log.warning("Live partition listing failed — trusting --crawl-list as given, unvalidated.")
        return requested
    missing = [n for n in requested if n not in names]
    if missing:
        log.warning(f"{len(missing)}/{len(requested)} requested partition(s) not found: {missing}")
    return [n for n in requested if n in names]

def _list_partition_files(crawl_name: str) -> list[str]:
    paths_url = f"{CC_DATA_BASE}/crawl-data/{crawl_name}/cc-index-table.paths.gz"
    try:
        resp = requests.get(paths_url, timeout=_HTTP_TIMEOUT_SECONDS)
        resp.raise_for_status()
        lines = gzip.decompress(resp.content).decode("utf-8").splitlines()
        # Use the standard HTTPS endpoint, which is backed by a fast CDN 
        # and avoids DuckDB S3 anonymous access authentication quirks (403 Forbidden).
        files = [f"{CC_DATA_BASE}/{line.strip()}" for line in lines
                 if line.strip().endswith(".parquet")]
    except Exception as e:
        log.warning(f"Could not fetch/parse {paths_url}: {e}")
        files = []
    if not files:
        log.warning(f"No parquet files resolved for crawl={crawl_name} — nothing to scan.")
    return files

def _query_file_rows(fpath: str, tld_filter: str, shard_clause: str) -> list[str]:
    """One file's live hosts. Each thread gets its own connection for true parallelism.
    Uses a simple filtered DISTINCT scan instead of heavy GROUP BY aggregation."""
    con = _get_duckdb_connection()
    if not con:
        return []
    try:
        query = f"""
        SELECT DISTINCT url_host_name
        FROM read_parquet('{fpath}')
        WHERE url_host_tld IN ({tld_filter})
          AND fetch_status BETWEEN 200 AND 299
          {shard_clause}
        """
        rows = con.execute(query).fetchall()
        return [row[0] for row in rows if row[0]]
    except Exception as e:
        log.warning(f"Query failed for {fpath}: {e}")
        return []
    finally:
        con.close()

def iter_seed_hosts_by_file(partitions: list[str], shard_index: int | None, shard_count: int | None,
                            resume_partition: str | None = None, resume_file_index: int = 0):
    """Streams hostnames across one or more partitions, ONE FILE AT A TIME.
    Uses a rolling window of parallel DuckDB queries to overlap I/O with crawling."""
    sharded = shard_index is not None and shard_count is not None
    if not sharded:
        log.warning("Running UNSHARDED — a file's full candidate set (10M+ rows) loads into memory "
                    "at once. Use --shard-index/--shard-count for production runs.")
    
    tld_filter = _build_tld_filter()
    shard_clause = f"AND (hash(url_host_name) % {shard_count}) = {shard_index}" if sharded else ""
    query_timeout = 300

    skip_prefix = True if resume_partition else False
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=PREFETCH_WORKERS) as prefetch_pool:
        for partition_num, crawl_name in enumerate(partitions, start=1):
            if skip_prefix:
                if crawl_name == resume_partition:
                    skip_prefix = False
                else:
                    log.info(f"── Partition {partition_num}/{len(partitions)}: {crawl_name} "
                             f"— already fully done (checkpoint resumes at {resume_partition!r}), skipping ──")
                    continue
            
            log.info(f"── Partition {partition_num}/{len(partitions)}: {crawl_name} ──")
            files = _list_partition_files(crawl_name)
            this_partition_start = resume_file_index if crawl_name == resume_partition else 0
            total_files = len(files)
            
            if this_partition_start:
                skipped = files[:this_partition_start]
                files = files[this_partition_start:]
                log.info(f"  resuming at file {this_partition_start}: skipping {len(skipped)} already-done file(s)")
            
            log.info(f"  {len(files)} file(s) to scan" + (f" — shard {shard_index}/{shard_count}" if sharded else ""))
            
            total_hosts = 0
            futures = {}
            
            # Submit initial batch of queries
            for i in range(min(PREFETCH_WORKERS, len(files))):
                futures[prefetch_pool.submit(_query_file_rows, files[i], tld_filter, shard_clause)] = i
                
            while futures:
                done, _ = concurrent.futures.wait(futures.keys(), return_when=concurrent.futures.FIRST_COMPLETED)
                
                for future in done:
                    i = futures.pop(future)
                    fpath = files[i]
                    file_num = i + this_partition_start + 1
                    
                    try:
                        # DuckDB already deduplicated and filtered for 2xx responses
                        hosts = future.result(timeout=query_timeout)
                    except concurrent.futures.TimeoutError:
                        log.warning(f"  file {file_num}/{total_files}: query timed out — skipping.")
                        hosts = []
                    except Exception as e:
                        log.warning(f"  file {file_num}/{total_files}: query failed — skipping: {e}")
                        hosts = []
                    
                    # Submit the NEXT file in the queue
                    next_i = i + PREFETCH_WORKERS
                    if next_i < len(files):
                        futures[prefetch_pool.submit(_query_file_rows, files[next_i], tld_filter, shard_clause)] = next_i
                    
                    total_hosts += len(hosts)
                    log.info(f"  file {file_num}/{total_files}: {len(hosts)} live hosts — {total_hosts} seeded so far")
                    
                    yield crawl_name, partition_num, len(partitions), file_num, total_files, hosts

SOURCE_LABEL = "common_crawl_probe"
_BANNER = "=" * 60
_DONE_SENTINEL = "DONE"

async def run_host_crawl(crawl: str | None, partitions_count: int, shard_index: int | None,
                         shard_count: int | None, concurrency: int,
                         time_budget_minutes: int, crawl_list: list[str] | None = None,
                         start_file_index: int | None = None, campaign: str | None = None,
                         reset_stats: bool = False) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(_BANNER)
    log.info(f"COMMON CRAWL — starting{label}")
    log.info(_BANNER)
    log.info(f"  concurrency={concurrency}  parse_workers={node.PARSE_WORKERS}   "
             f"time_budget={time_budget_minutes}min (shared across all requested partitions)")
    
    if crawl_list:
        log.info(f"  explicit --crawl-list given ({len(crawl_list)} requested) — ignoring --crawl/--partitions")
        partitions = _resolve_explicit_crawl_names(crawl_list)
    else:
        partitions = _resolve_crawl_names(crawl, partitions_count)
        
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
    elapsed, rate = 0.0, 0.
    time_budget_hit = False
    total_hosts_seen = 0
    partitions_completed = 0
    last_partition_name = partitions[0]
    
    resume_partition: str | None = None
    resume_file_index = 0
    resumable = shard_index is not None and shard_count is not None
    
    try:
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
            if reset_stats and campaign:
                log.info(f"  --reset-stats given — wiping cumulative tallies for campaign {campaign!r}")
                await node.reset_stat_tallies(session, SOURCE_LABEL, campaign)
                
            if resumable:
                if start_file_index == 0:
                    log.info("  --start-file-index 0 — forcing a full restart of this shard, clearing any checkpoint")
                    await node.clear_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count)
                elif start_file_index:
                    resume_partition, resume_file_index = partitions[0], start_file_index
                    log.info(f"  --start-file-index {resume_file_index} (manual override) — skipping ahead in {resume_partition}")
                else:
                    resume_partition, resume_file_index = await node.load_crawl_checkpoint_with_partition(
                        session, SOURCE_LABEL, shard_index, shard_count)
                    if resume_partition == _DONE_SENTINEL:
                        log.info(f"  shard{label} already fully completed a prior run (checkpoint "
                                 f"marked done) — nothing to do, not re-crawling. Pass "
                                 f"--start-file-index 0 to force a genuine restart.")
                        return
                    if resume_partition:
                        log.info(f"  resuming from checkpoint: {resume_file_index} file(s) already completed "
                                 f"in {resume_partition} on a prior run — skipping straight past them")
            elif start_file_index:
                resume_partition, resume_file_index = partitions[0], start_file_index
                
            for partition_name, partition_num, total_partitions, file_num, total_files, file_hosts \
                    in iter_seed_hosts_by_file(partitions, shard_index, shard_count,
                                               resume_partition, resume_file_index):
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
                stats_before = dict(stats)
                found_rows_len_before = len(found_rows)
                
                _, elapsed, rate, file_time_hit = await node.crawl_batch(
                    file_hosts, session, sem, stats, parse_pool, node.ACCEPT_ANY_COUNTRY,
                    SOURCE_LABEL, found_rows, crawl_start, time_budget_seconds,
                    time_budget_minutes, batch_size=2000, unit_label="hosts",
                    capture_inhouse=True)
                    
                if campaign:
                    deltas = {k: v - stats_before.get(k, 0) for k, v in stats.items()
                              if v != stats_before.get(k, 0)}
                    new_rows = found_rows[found_rows_len_before:]
                    for ats_hit in (r["ats"] for r in new_rows):
                        deltas[f"platform__{ats_hit}"] = deltas.get(f"platform__{ats_hit}", 0) + 1
                    for country_hit in (r["country"] or "unknown" for r in new_rows):
                        deltas[f"country__{country_hit}"] = deltas.get(f"country__{country_hit}", 0) + 1
                    deltas["hosts_seeded"] = deltas.get("hosts_seeded", 0) + len(file_hosts)
                    await node.flush_stat_tallies(session, SOURCE_LABEL, campaign, deltas)
                    
                if file_time_hit:
                    time_budget_hit = True
                    break
                    
                if resumable:
                    await node.save_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count,
                                                     file_num, partition=partition_name)
                if file_num == total_files:
                    partitions_completed = partition_num
                    
            if not time_budget_hit and resumable:
                await node.save_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count,
                                                 total_hosts_seen, partition=_DONE_SENTINEL)
    finally:
        parse_pool.shutdown(wait=True)
        
    ats_breakdown = Counter(r["ats"] for r in found_rows)
    country_breakdown = Counter(r["country"] or "unknown" for r in found_rows)
    status_line = ("STOPPED EARLY — time budget reached mid-" + last_partition_name if time_budget_hit
                   else "complete — all requested partitions covered")
                   
    node.log_crawl_summary(f"summary{label}", stats, ats_breakdown, country_breakdown, status_line,
                           hosts_attempted=stats["companies_attempted"], hosts_seeded=total_hosts_seen,
                           elapsed_seconds=elapsed, rate_per_sec=rate, time_budget_seconds=time_budget_seconds)

async def summarize_campaign(campaign: str, status_line: str) -> None:
    connector = node.new_connector()
    async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
        stats, platform_counts, country_counts, first_seen, last_seen = await node.fetch_cumulative_stats(
            session, SOURCE_LABEL, campaign)
        if not stats and not platform_counts and not country_counts:
            log.warning(f"No cumulative stats found for campaign {campaign!r} — nothing to summarize.")
            return
            
        elapsed_seconds = None
        if first_seen and last_seen:
            try:
                span = (datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
                        - datetime.fromisoformat(first_seen.replace("Z", "+00:00")))
                elapsed_seconds = span.total_seconds()
            except ValueError:
                pass
                
        node.log_crawl_summary(f"CUMULATIVE — campaign {campaign!r}", stats, platform_counts, country_counts,
                               status_line, hosts_attempted=stats.get("companies_attempted", 0),
                               hosts_seeded=stats.get("hosts_seeded"), elapsed_seconds=elapsed_seconds)

def main():
    parser = argparse.ArgumentParser(
        description="Host Crawl v2 — follow-links ATS discovery across Common Crawl partitions.")
    parser.add_argument("--crawl", type=str, default=None)
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--crawl-list", type=str, default=None)
    parser.add_argument("--start-file-index", type=int, default=None)
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=node.CRAWL_CONCURRENCY)
    parser.add_argument("--time-budget-minutes", type=int, default=node.TIME_BUDGET_MINUTES)
    parser.add_argument("--campaign", type=str, default=None)
    parser.add_argument("--reset-stats", action="store_true")
    parser.add_argument("--summarize-campaign", type=str, default=None, metavar="CAMPAIGN")
    parser.add_argument("--summary-status-line", type=str,
                        default="progress so far — campaign may still be running")
    args = parser.parse_args()
    
    crawl_list = [c.strip() for c in args.crawl_list.split(",") if c.strip()] if args.crawl_list else None
    
    if args.summarize_campaign:
        asyncio.run(summarize_campaign(args.summarize_campaign, args.summary_status_line))
        return
        
    if args.reset_stats and not args.campaign:
        parser.error("--reset-stats requires --campaign")
        
    asyncio.run(run_host_crawl(args.crawl, args.partitions, args.shard_index, args.shard_count,
                               args.concurrency, args.time_budget_minutes, crawl_list=crawl_list,
                               start_file_index=args.start_file_index, campaign=args.campaign,
                               reset_stats=args.reset_stats))

if __name__ == "__main__":
    main()
