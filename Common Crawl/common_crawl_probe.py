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
from datetime import datetime

import aiohttp
import requests
from dotenv import load_dotenv

load_dotenv()
# 2026-09 bug fix: this used to insert ONLY _ROOT (Crawler/) itself, which
# does not contain node.py — node.py lives in Crawler/Main/. Verified by
# reproducing the exact real repo layout (Crawler/Main/node.py,
# Crawler/Common Crawl/common_crawl_probe.py) and running this file from
# Crawler/ the same way common_crawl.yml does: `import node` failed with
# ModuleNotFoundError every time. Every sibling probe (opendata_probe.py,
# people_data_labs_probe.py, bigpicture_probe.py, github_org_probe.py)
# already inserts BOTH _ROOT and os.path.join(_ROOT, "Main") for exactly
# this reason — this file was just missing the second one.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # for node.py/discovery.py
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
                             resume_partition: str | None = None, resume_file_index: int = 0):
    """Streams hostnames across one or more partitions, ONE FILE AT A
    TIME — yields (crawl_name, partition_num, total_partitions, file_num,
    total_files, hosts). Sharding happens IN THE SQL query (hash() %
    shard_count), not by slicing a Python list — see module docstring.

    SPEED (2026-09): the next file's DuckDB query is submitted to a
    background thread as soon as the current file's rows are in hand —
    the query for file N+1 runs WHILE the caller is off crawling file N's
    hosts (real network I/O, seconds), instead of the two happening one
    after the other. `.result()` on an already-finished future returns
    instantly, so this only ever helps and never adds latency.

    2026-09: `resume_partition`/`resume_file_index` replace the old
    'always skip start_file_index files into partitions[0]' assumption —
    see run_host_crawl's RESUME comment for why that was wrong for any
    --partitions > 1 run. Every partition BEFORE resume_partition in the
    list is skipped entirely (already fully done on a prior run); the
    resume_partition itself skips resume_file_index files; every
    partition AFTER it runs from file 0 as normal. resume_partition=None
    means no skip at all (a genuinely fresh run)."""
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

    # Skip any partition strictly before the checkpointed one outright —
    # they were already fully crawled on a prior run in this chain.
    skip_prefix = True if resume_partition else False

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as prefetch_pool:
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
            total_files = len(files)  # ORIGINAL count, before slicing — see file_num note below
            if this_partition_start:
                skipped = files[:this_partition_start]
                files = files[this_partition_start:]
                log.info(f"  resuming at file {this_partition_start}: skipping {len(skipped)} already-done file(s)")
            log.info(f"  {len(files)} file(s) to scan" + (f" — shard {shard_index}/{shard_count}" if sharded else ""))

            total_dead_skipped = 0
            total_hosts = 0
            next_future = prefetch_pool.submit(_query_file_rows, con, files[0], tld_filter, shard_clause) if files else None
            # start=this_partition_start+1, NOT 1 — file_num must stay the
            # ABSOLUTE position within crawl_name's full file list so a
            # checkpoint saved as (crawl_name, file_num) means exactly
            # "file_num files of THIS partition done," with no adjustment
            # needed by the caller. Yielding a post-slice-relative 1-based
            # index here (the pre-2026-09 behavior) was fine when only
            # partition 1 was ever resumable, but silently wrong for any
            # later partition once resume applies to any partition in the
            # list, not just the first.
            for file_num, fpath in enumerate(files, start=this_partition_start + 1):
                try:
                    rows = next_future.result(timeout=query_timeout)
                except concurrent.futures.TimeoutError:
                    log.warning(f"  file {file_num}/{total_files}: query timed out — skipping.")
                    rows = []
                except Exception as e:
                    log.warning(f"  file {file_num}/{total_files}: query failed — skipping: {e}")
                    rows = []
                # Kick the NEXT file's query off immediately — it runs in
                # the background while this file's hosts get crawled below.
                next_i = file_num - this_partition_start  # index into the (sliced) `files` list
                next_future = (prefetch_pool.submit(_query_file_rows, con, files[next_i], tld_filter, shard_clause)
                               if next_i < len(files) else None)

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
                log.info(f"  file {file_num}/{total_files}: {len(file_hosts)} live hosts (of {len(rows)} "
                         f"candidates, {dead_skipped} dead-skipped) — {total_hosts} seeded so far")
                yield crawl_name, partition_num, len(partitions), file_num, total_files, file_hosts


SOURCE_LABEL = "common_crawl_probe"
_BANNER = "=" * 60


async def run_host_crawl(crawl: str | None, partitions_count: int, shard_index: int | None,
                          shard_count: int | None, concurrency: int,
                          time_budget_minutes: int, crawl_list: list[str] | None = None,
                          start_file_index: int | None = None, campaign: str | None = None,
                          reset_stats: bool = False) -> None:
    """campaign (2026-09): identifies this whole crawl request (one
    starting partition + --partitions count + --crawl-list, computed once
    by common_crawl.yml's prepare-matrix job and passed through unchanged
    to every shard and every loop redispatch of it) for the cumulative
    cross-run stats accumulator — see node.flush_stat_tallies/
    fetch_cumulative_stats. None (the default) means "don't bother" — a
    manual/local run without --campaign just skips the flush, same as
    before this existed, no behavior change for that case."""
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
    resume_partition: str | None = None
    resume_file_index = 0
    resumable = shard_index is not None and shard_count is not None

    try:
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
            if reset_stats and campaign:
                log.info(f"  --reset-stats given — wiping cumulative tallies for campaign {campaign!r}")
                await node.reset_stat_tallies(session, SOURCE_LABEL, campaign)
            # RESUME: same checkpoint table opendata_probe.py/
            # people_data_labs_probe.py use, keyed by (source, shard_index,
            # shard_count) — see module docstring's RESUME section.
            #
            # 2026-09: now tracks (partition, file index WITHIN that
            # partition), not just a bare file count assumed to be inside
            # partitions[0] — a --partitions > 1 (or --crawl-list) run that
            # stopped mid-partition-2+ used to leave a stale partition-1
            # checkpoint behind, so a resumed run skipped the WRONG number
            # of files into whatever partition it started at. This is also
            # what makes the reloop (common_crawl.yml's `loop_runs`) safe
            # to chain across an arbitrary number of partitions, not just
            # a single one.
            if resumable:
                if start_file_index == 0:
                    log.info("  --start-file-index 0 — forcing a full restart of this shard, clearing any checkpoint")
                    await node.clear_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count)
                elif start_file_index:
                    # Manual override stays single-partition, same as
                    # before — an explicit --start-file-index is a human
                    # saying "start THIS run at file N of the first
                    # requested partition," not a multi-partition-aware
                    # resume (that's what the checkpoint-driven path below
                    # is for).
                    resume_partition, resume_file_index = partitions[0], start_file_index
                    log.info(f"  --start-file-index {resume_file_index} (manual override) — "
                             f"skipping ahead in {resume_partition}")
                else:
                    resume_partition, resume_file_index = await node.load_crawl_checkpoint_with_partition(
                        session, SOURCE_LABEL, shard_index, shard_count)
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
                # Snapshot BEFORE this file's crawl — see the flush just
                # below. Copying `stats` (a Counter, cheap: a few dozen
                # keys) rather than mutating a second running total avoids
                # ANY risk of this new bookkeeping perturbing crawl_batch's
                # own use of the SAME `stats` object (rate calc, hit-rate
                # logging) — it's purely read-only here.
                stats_before = dict(stats)
                found_rows_len_before = len(found_rows)
                # 2026-09, REVISED: Common Crawl carries no employee-count/
                # size signal — see opendata_probe.py's identical comment/
                # fix. capture_inhouse=True now, so this source feeds
                # archive_ii too, gated by node.py's company-maturity check
                # (HTML signals + DNS MX) instead of an employee-count floor
                # it never had. archive_i still gets every ATS hit either way.
                _, elapsed, rate, file_time_hit = await node.crawl_batch(
                    file_hosts, session, sem, stats, parse_pool, node.ACCEPT_ANY_COUNTRY,
                    SOURCE_LABEL, found_rows, crawl_start, time_budget_seconds,
                    time_budget_minutes, batch_size=2000, unit_label="hosts",
                    capture_inhouse=True)
                if campaign:
                    # This file's OWN contribution only — every scalar key
                    # `stats` gained/changed since stats_before, plus the
                    # per-hit platform/country breakdown for just the rows
                    # THIS file added (found_rows[found_rows_len_before:]),
                    # plus how many live hosts this file seeded. Flushed
                    # once per file (same cadence as the checkpoint save
                    # below) — cheap, and means a mid-file crash (like the
                    # native segfault this was built to survive) only ever
                    # loses that one file's numbers, never anything already
                    # completed.
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
                    # Checkpoint after EVERY partition now, not just the
                    # first — file_num is already absolute within
                    # partition_name (see iter_seed_hosts_by_file), so no
                    # offset arithmetic is needed here.
                    await node.save_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count,
                                                      file_num, partition=partition_name)
                if file_num == total_files:
                    partitions_completed = partition_num
            if not time_budget_hit and resumable:
                # Ran clean to the end — clear the checkpoint so a later,
                # differently-shaped run doesn't wrongly skip ahead.
                await node.clear_crawl_checkpoint(session, SOURCE_LABEL, shard_index, shard_count)
    finally:
        parse_pool.shutdown(wait=True)

    ats_breakdown = Counter(r["ats"] for r in found_rows)
    country_breakdown = Counter(r["country"] or "unknown" for r in found_rows)
    status_line = ("STOPPED EARLY — time budget reached mid-" + last_partition_name if time_budget_hit
                   else "complete — all requested partitions covered")
    log.info(f"  partitions:  {partitions_completed}/{len(partitions)} fully covered ({partitions})")
    # 2026-09: Common Crawl has no size signal of its own — every
    # archive_ii acceptance here went through the Quality Index gate
    # (capture_inhouse=True, no capture_inhouse_domains set), same as
    # OpenData — see node.log_quality_index_summary's docstring (called
    # inside log_crawl_summary below).
    node.log_crawl_summary(f"summary{label}", stats, ats_breakdown, country_breakdown, status_line,
                            hosts_attempted=stats["companies_attempted"], hosts_seeded=total_hosts_seen,
                            elapsed_seconds=elapsed, rate_per_sec=rate, time_budget_seconds=time_budget_seconds)


async def summarize_campaign(campaign: str, status_line: str) -> None:
    """Reads back every tally this campaign has accumulated across every
    shard and every loop redispatch (see node.fetch_cumulative_stats) and
    prints it with the exact same formatter run_host_crawl's own per-run
    summary uses (node.log_crawl_summary) — the true end-to-end picture,
    from the very first file of the very first partition to whatever's
    been flushed so far. Deliberately takes NO position on whether the
    campaign is actually finished — that's a checkpoint-table question
    (does ANY shard still have a remaining checkpoint?), which
    common_crawl.yml's finalize job already answers for its own looping
    decision; it passes the resulting status_line straight through here
    rather than this function re-deriving it a second, possibly
    inconsistent way."""
    connector = node.new_connector()
    async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
        stats, platform_counts, country_counts, first_seen, last_seen = await node.fetch_cumulative_stats(
            session, SOURCE_LABEL, campaign)
    if not stats and not platform_counts and not country_counts:
        log.warning(f"No cumulative stats found for campaign {campaign!r} — nothing to summarize "
                    f"(never run, or --reset-stats wiped it and nothing has flushed since).")
        return
    elapsed_seconds = None
    if first_seen and last_seen:
        try:
            span = (datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
                    - datetime.fromisoformat(first_seen.replace("Z", "+00:00")))
            elapsed_seconds = span.total_seconds()
        except ValueError:
            pass  # cosmetic only — a bad timestamp format just omits the time line, nothing else depends on it
    node.log_crawl_summary(f"CUMULATIVE — campaign {campaign!r}", stats, platform_counts, country_counts,
                            status_line, hosts_attempted=stats.get("companies_attempted", 0),
                            hosts_seeded=stats.get("hosts_seeded"), elapsed_seconds=elapsed_seconds)


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
    parser.add_argument("--campaign", type=str, default=None,
                         help="Identifies this whole crawl request for the cumulative cross-run stats "
                              "accumulator (see node.flush_stat_tallies). common_crawl.yml computes this "
                              "once and passes it to every shard/loop redispatch unchanged. Omit for a "
                              "manual/local run — cumulative tallying is simply skipped, same as before "
                              "this existed.")
    parser.add_argument("--reset-stats", action="store_true",
                         help="Wipe this --campaign's cumulative tallies before starting (requires "
                              "--campaign). Explicit, opt-in only — never set by a loop redispatch, same "
                              "as --start-file-index 0 for checkpoints. A fresh top-level dispatch that "
                              "wants a clean count (not mixed with an older run of the same partitions) "
                              "should pass this once, on that first dispatch only.")
    parser.add_argument("--summarize-campaign", type=str, default=None, metavar="CAMPAIGN",
                         help="Skip crawling entirely — just read back and print CAMPAIGN's cumulative "
                              "stats so far (see node.fetch_cumulative_stats), then exit. Used by "
                              "common_crawl.yml's finalize job once a campaign has no shards left with a "
                              "remaining checkpoint, but also safe to run any time for a progress check.")
    parser.add_argument("--summary-status-line", type=str,
                         default="progress so far — campaign may still be running",
                         help="Only used with --summarize-campaign: the status line to print (the caller "
                              "— e.g. finalize's own checkpoint check — knows whether the campaign is "
                              "actually complete; this function doesn't re-derive that itself).")
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
