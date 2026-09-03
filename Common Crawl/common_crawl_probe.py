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
avoids.

DATA SOURCE (2026-09 change): this used to go through a third-party
Hugging Face mirror (commoncrawl/host-index-testing-v2) that pre-aggregated
per-host fetch stats but lagged Common Crawl's real monthly releases by
over a year (stuck at CC-MAIN-2025-18 while CC-MAIN-2026-34 was already
out). Switched to querying Common Crawl's own official CC-Index Table
directly — published by Common Crawl themselves, in lockstep with every
monthly crawl release, no third party, no auth, no token:
  - partition list:  https://index.commoncrawl.org/collinfo.json  (always
    current — this IS Common Crawl's own release list, not a mirror)
  - parquet files:    https://data.commoncrawl.org/cc-index/table/cc-main/
    warc/crawl={crawl}/subset=warc/*.parquet
This index is per-URL, not pre-aggregated per-host like the old HF
dataset, so the host-liveness check is now done with a GROUP BY in the SQL
query itself (has_2xx / has_4xx_5xx per url_host_name) instead of reading
precomputed fetch_200/fetch_4xx/... columns — same "keep the host if any
2xx ever showed up, drop it only if Common Crawl's own attempts were only
4xx/5xx" logic as before, just computed live from always-current data.

STREAMING, ONE FILE AT A TIME: a real incident — an earlier version
accumulated 126M hostnames into one Python list and got OOM-killed by
GitHub Actions' runner. Fixed by (1) sharding pushed into the SQL query
itself (hash(url_host_name) % shard_count = shard_index), and (2)
seeding+crawling interleaved per Parquet file — each file's hosts are
crawled and dropped before the next file is even queried, so memory never
scales with partition count or host count. TIME_BUDGET_MINUTES is the
only thing bounding a run's length. The NEXT file's query is kicked off
in the background as soon as the current file's rows land, so DuckDB
querying and crawling overlap instead of the crawl stalling on every
file's query in serial — see _FilePrefetcher below.

RESUME (2026-09): each (shard_index, shard_count) checkpoints which file
number it last fully finished to the SAME Supabase checkpoint table
opendata_probe.py/people_data_labs_probe.py use (node.save_crawl_checkpoint,
keyed by source="common_crawl_probe") — a re-run of the same shard picks
up right after the last completed file automatically, no manual
--start-file-index bookkeeping from the logs required. --start-file-index
still works as an explicit override when given (0 forces a full restart
and clears the checkpoint; a positive value manually skips ahead without
touching the stored checkpoint) — same three-way convention
opendata_probe.py's --restart-index uses.

Usage:
    python common_crawl_probe.py                                        # 1 partition, unsharded (dev only)
    python common_crawl_probe.py --crawl CC-MAIN-2026-34 --partitions 3  # 3 contiguous partitions
    python common_crawl_probe.py --crawl-list CC-MAIN-2026-34,CC-MAIN-2025-18
    python common_crawl_probe.py --shard-index 0 --shard-count 10        # production shape
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

import aiohttp
import requests
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # Crawler/, for node.py
import node  # noqa: E402

log = logging.getLogger("common_crawl_probe")

CC_DATA_BASE = "https://data.commoncrawl.org"
CC_COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
# Absolute last resort only — hit if BOTH collinfo.json and a pinned
# --crawl are unavailable. collinfo.json is Common Crawl's own current
# release list, so this should essentially never actually get used; it's
# not a "may be stale" caveat like the old HF fallback was.
_FALLBACK_CRAWL = "CC-MAIN-2025-18"

# Candidate pre-filter (never the real country decision — see node.detect_country).
TARGET_TLDS = {
    "us", "uk", "ca", "de", "au", "ie", "mt",
    "com", "net", "io", "co", "app", "dev",
}
TARGET_SUFFIXES_EXTRA = {"co.uk", "com.au"}

_DUCKDB_CALL_TIMEOUT_SECONDS = 180
_HTTP_TIMEOUT_SECONDS = 30


def _run_with_timeout(fn, *args, timeout=_DUCKDB_CALL_TIMEOUT_SECONDS, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fn, *args, **kwargs)
        return future.result(timeout=timeout)


def _build_tld_filter() -> str:
    parts = [f"'{t}'" for t in TARGET_TLDS] + [f"'{t}'" for t in TARGET_SUFFIXES_EXTRA]
    return ",".join(parts)


def _looks_dead(has_2xx, has_4xx_5xx) -> bool:
    """Only treated as dead if Common Crawl's own attempts NEVER got a
    single 2xx — biased toward keeping a host if in doubt."""
    return not has_2xx and bool(has_4xx_5xx)


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
    try:
        con.execute("SET http_timeout = 30000;")
        con.execute("SET http_retries = 3;")
        con.execute("SET http_retry_wait_ms = 2000;")
        con.execute("SET http_retry_backoff = 2;")
        con.execute("SET memory_limit = '3GB';")
        con.execute("PRAGMA threads=2;")
    except Exception as e:
        log.warning(f"Could not set DuckDB options (continuing with defaults): {e}")
    return con


def _list_all_crawl_names() -> list[str]:
    """Fetches Common Crawl's own current release list directly — no
    lag, no third party. Empty list if the fetch itself fails — callers
    fall back to a single hardcoded partition in that case."""
    try:
        resp = requests.get(CC_COLLINFO_URL, timeout=_HTTP_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()
        # collinfo.json is already newest-first.
        return [row["id"] for row in data if row.get("id", "").startswith("CC-MAIN-")]
    except Exception as e:
        log.warning(f"Could not fetch {CC_COLLINFO_URL}: {e}")
        return []


def _resolve_crawl_names(pinned: str | None, count: int) -> list[str]:
    """Resolves `count` partitions, most-recent-first, starting at
    `pinned` (or the true latest) and walking backward. Falls back to a
    single partition if the live listing fails."""
    names = _list_all_crawl_names()
    if names:
        log.info(f"Available crawl partitions (most recent 5 of {len(names)}): {names[:5]}")
        start = names.index(pinned) if pinned in names else 0
        selected = names[start:start + count]
        if len(selected) < count:
            log.warning(f"Only {len(selected)}/{count} partition(s) available at/older than the "
                        f"start point.")
        return selected
    single = pinned or _FALLBACK_CRAWL
    log.warning(f"Falling back to a single hardcoded partition {single!r} — live listing failed.")
    return [single]


def _resolve_explicit_crawl_names(requested: list[str]) -> list[str]:
    """--crawl-list: a specific, possibly non-contiguous set of partitions
    named directly (e.g. spread across years to sample a more diverse
    company population than a contiguous walk gives). Validates against
    the live listing when available; trusts the list as-is otherwise."""
    names = _list_all_crawl_names()
    if not names:
        log.warning("Live partition listing failed — trusting --crawl-list as given, unvalidated.")
        return requested
    missing = [n for n in requested if n not in names]
    if missing:
        log.warning(f"{len(missing)}/{len(requested)} requested partition(s) not found: {missing}")
    return [n for n in requested if n in names]


def _list_partition_files(crawl_name: str) -> list[str]:
    """Common Crawl publishes the exact parquet file list for a crawl as
    a gzipped paths file — plain HTTPS directory listing/wildcards aren't
    supported against data.commoncrawl.org, so this is the documented way
    to enumerate a partition's CC-Index Table files."""
    paths_url = f"{CC_DATA_BASE}/crawl-data/{crawl_name}/cc-index-table.paths.gz"
    try:
        resp = requests.get(paths_url, timeout=_HTTP_TIMEOUT_SECONDS)
        resp.raise_for_status()
        lines = gzip.decompress(resp.content).decode("utf-8").splitlines()
        files = [f"{CC_DATA_BASE}/{line.strip()}" for line in lines
                 if line.strip().endswith(".parquet")]
    except Exception as e:
        log.warning(f"Could not fetch/parse {paths_url}: {e}")
        files = []
    if not files:
        log.warning(f"No parquet files resolved for crawl={crawl_name} — nothing to scan.")
    return files


def _query_file_rows(con, fpath: str, tld_filter: str, shard_clause: str) -> list[tuple]:
    """One file's per-host liveness rows. Per-URL rows aggregated to
    per-host liveness right here in SQL (has_2xx / has_4xx_5xx) — CC's own
    index has no precomputed per-host columns like the old HF mirror did."""
    query = f"""
        SELECT url_host_name,
               MAX(CASE WHEN fetch_status BETWEEN 200 AND 299 THEN 1 ELSE 0 END) AS has_2xx,
               MAX(CASE WHEN fetch_status >= 400 THEN 1 ELSE 0 END) AS has_4xx_5xx
        FROM read_parquet('{fpath}')
        WHERE url_host_tld IN ({tld_filter})
        {shard_clause}
        GROUP BY url_host_name
    """
    return con.execute(query).fetchall()


def iter_seed_hosts_by_file(partitions: list[str], shard_index: int | None, shard_count: int | None,
                             start_file_index: int = 0):
    """Streams hostnames across one or more partitions, ONE FILE AT A
    TIME — yields (crawl_name, partition_num, total_partitions, file_num,
    total_files, hosts). Sharding happens IN THE SQL query (hash() %
    shard_count), not by slicing a Python list — see module docstring.

    SPEED (2026-09): the next file's DuckDB query is submitted to a
    background thread as soon as the current file's rows are in hand —
    the query for file N+1 runs WHILE the caller is off crawling file N's
    hosts (real network I/O, seconds), instead of the two happening one
    after the other. `.result()` on an already-finished future returns
    instantly, so this only ever helps and never adds latency."""
    con = _get_duckdb_connection()
    if con is None:
        return

    sharded = shard_index is not None and shard_count is not None
    if not sharded:
        log.warning("Running UNSHARDED — a file's full candidate set (10M+ rows) loads into memory "
                    "at once. Use --shard-index/--shard-count for production runs.")

    tld_filter = _build_tld_filter()
    shard_clause = f"AND (hash(url_host_name) % {shard_count}) = {shard_index}" if sharded else ""
    query_timeout = 300

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as prefetch_pool:
        for partition_num, crawl_name in enumerate(partitions, start=1):
            log.info(f"── Partition {partition_num}/{len(partitions)}: {crawl_name} ──")
            files = _list_partition_files(crawl_name)
            if partition_num == 1 and start_file_index:
                skipped = files[:start_file_index]
                files = files[start_file_index:]
                log.info(f"  starting at file {start_file_index}: skipping {len(skipped)} already-done file(s)")
            log.info(f"  {len(files)} file(s) to scan" + (f" — shard {shard_index}/{shard_count}" if sharded else ""))

            total_dead_skipped = 0
            total_hosts = 0
            next_future = prefetch_pool.submit(_query_file_rows, con, files[0], tld_filter, shard_clause) if files else None
            for file_num, fpath in enumerate(files, start=1):
                try:
                    rows = next_future.result(timeout=query_timeout)
                except concurrent.futures.TimeoutError:
                    log.warning(f"  file {file_num}/{len(files)}: query timed out — skipping.")
                    rows = []
                except Exception as e:
                    log.warning(f"  file {file_num}/{len(files)}: query failed — skipping: {e}")
                    rows = []
                # Kick the NEXT file's query off immediately — it runs in
                # the background while this file's hosts get crawled below.
                next_future = (prefetch_pool.submit(_query_file_rows, con, files[file_num], tld_filter, shard_clause)
                               if file_num < len(files) else None)

                # Dedup is PER-FILE only, not cross-run — a persistent `seen`
                # set would regrow to the same OOM-risk size the streaming fix
                # eliminated. A host appearing in >1 file just gets crawled
                # twice (harmless — (ats,slug) dedup at the write path still
                # prevents any duplicate row).
                file_hosts: list[str] = []
                seen_this_file: set[str] = set()
                dead_skipped = 0
                for host, has_2xx, has_4xx_5xx in rows:
                    if not host:
                        continue
                    if _looks_dead(has_2xx, has_4xx_5xx):
                        dead_skipped += 1
                        continue
                    if host in seen_this_file:
                        continue
                    seen_this_file.add(host)
                    file_hosts.append(host)
                total_dead_skipped += dead_skipped
                total_hosts += len(file_hosts)
                log.info(f"  file {file_num}/{len(files)}: {len(file_hosts)} live hosts (of {len(rows)} "
                         f"candidates, {dead_skipped} dead-skipped) — {total_hosts} seeded so far")
                yield crawl_name, partition_num, len(partitions), file_num, len(files), file_hosts


SOURCE_LABEL = "common_crawl_probe"
_BANNER = "=" * 60


async def run_host_crawl(crawl: str | None, partitions_count: int, shard_index: int | None,
                          shard_count: int | None, concurrency: int,
                          time_budget_minutes: int, crawl_list: list[str] | None = None,
                          start_file_index: int | None = None) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(_BANNER)
    log.info(f"COMMON CRAWL — starting{label}")
    log.info(_BANNER)
    log.info(f"  concurrency={concurrency}  parse_workers={node.PARSE_WORKERS}  "
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
    elapsed, rate = 0.0, 0.0
    time_budget_hit = False
    total_hosts_seen = 0
    partitions_completed = 0
    last_partition_name = partitions[0]
    files_done_before = 0
    resumable = shard_index is not None and shard_count is not None

    try:
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
            # RESUME: same checkpoint table opendata_probe.py/
            # people_data_labs_probe.py use, keyed by (source, shard_index,
            # shard_count) — see module docstring's RESUME section. Only
            # tracks position within the FIRST requested partition, same
            # limitation --start-file-index always had.
            if resumable:
                if start_file_index == 0:
                    log.info("  --start-file-index 0 — forcing a full restart of this shard, clearing any checkpoint")
                    await node.clear_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count)
                elif start_file_index:
                    files_done_before = start_file_index
                    log.info(f"  --start-file-index {files_done_before} (manual override) — skipping ahead")
                else:
                    files_done_before = await node.load_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count)
                    if files_done_before:
                        log.info(f"  resuming from checkpoint: {files_done_before} file(s) already completed "
                                 f"in this shard on a prior run — skipping straight past them")
            elif start_file_index:
                files_done_before = start_file_index

            for partition_name, partition_num, total_partitions, file_num, total_files, file_hosts \
                    in iter_seed_hosts_by_file(partitions, shard_index, shard_count, files_done_before):
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
                # capture_inhouse=False (2026-08): Common Crawl carries no
                # employee-count/size signal — see opendata_probe.py's
                # identical comment. archive_i still gets every ATS hit.
                _, elapsed, rate, file_time_hit = await node.crawl_batch(
                    file_hosts, session, sem, stats, parse_pool, node.ACCEPT_ANY_COUNTRY,
                    SOURCE_LABEL, found_rows, crawl_start, time_budget_seconds,
                    time_budget_minutes, batch_size=2000, unit_label="hosts",
                    capture_inhouse=False)
                if file_time_hit:
                    time_budget_hit = True
                    break
                if partition_num == 1 and resumable:
                    await node.save_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count,
                                                      files_done_before + file_num)
                if file_num == total_files:
                    partitions_completed = partition_num
            if not time_budget_hit and resumable:
                # Ran clean to the end — clear the checkpoint so a later,
                # differently-shaped run doesn't wrongly skip ahead.
                await node.clear_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count)
    finally:
        parse_pool.shutdown(wait=True)

    total_hits = stats["hits_from_homepage"] + stats["hits_from_career_path"] + stats["hits_from_sitemap"]
    hosts_n = max(stats["companies_attempted"], 1)

    log.info("")
    log.info(_BANNER)
    log.info(f"COMMON CRAWL — summary{label}")
    log.info(_BANNER)
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
        description="Host Crawl v2 — follow-links ATS discovery across Common Crawl partitions "
                    "(queried directly from Common Crawl's own CC-Index Table).")
    parser.add_argument("--crawl", type=str, default=None,
                         help="Pin the starting partition. Blank = auto-detect latest EVERY run. "
                              "Ignored if --crawl-list is given.")
    parser.add_argument("--partitions", type=int, default=1,
                         help="How many CONTIGUOUS partitions, walking backward from --crawl. "
                              "Ignored if --crawl-list is given.")
    parser.add_argument("--crawl-list", type=str, default=None,
                         help="Comma-separated exact partition names, e.g. "
                              "'CC-MAIN-2026-34,CC-MAIN-2025-18'. Overrides --crawl/--partitions.")
    parser.add_argument("--start-file-index", type=int, default=None,
                         help="Per-shard resume, FIRST partition only. Omit (default) to auto-resume "
                              "from this shard's own Supabase checkpoint. 0 forces a full restart, "
                              "clearing any checkpoint. A positive value manually skips ahead this many "
                              "files without touching the stored checkpoint.")
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
