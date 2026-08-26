"""
Host Crawl v2 — single-pass, multi-partition, country-aware
==============================================================
Replaces the old two-stage host_crawl.py/host_crawl_seed.py design
(seed into a Supabase queue table, then a SEPARATE crawl job drains that
queue) with ONE pass: seed hostnames from one or more Common Crawl Host
Index partitions, then immediately crawl each one the exact same way
class_a_probe.py crawls a PDL company (visit the real site, follow links,
look for a real ATS URL — NOT guessing slugs against any platform's API),
using class_a_probe.py's already-proven fetch/parse/detect core directly
(imported, not re-implemented).

WHY NOT THE OLD QUEUE DESIGN: that design existed to let seeding and
crawling happen as separate, independently-resumable GitHub Actions jobs,
converging on FULL coverage across many runs (h_seeding / h_file_count /
h_total_seeded checkpointing). This build doesn't need that convergence
machinery because it doesn't hold whole-partition state between runs —
it trades "eventually see everything via checkpointed resumption" for
"crawl N partitions per run, done." If that old resumption model is
wanted later, host_crawl_seed.py's checkpointing infrastructure is still
there and can be adapted; nothing here depends on it existing or not.

HOW MANY PARTITIONS: originally scoped to exactly 1 (a live check of
Common Crawl's own schedule found one partition, e.g. CC-MAIN-2025-18,
spans roughly 1-1.5 months, not a year — they publish ~6-12 a year, so
~26 available on Hugging Face spans 2-4 years of history). Widened
2026-08 to accept --partitions N: the per-file streaming fix below (see
STREAMING SEED+CRAWL) already bounds memory to one file at a time
regardless of partition count, so walking N partitions sequentially in
one run carries the same memory profile as walking N files within one
partition — no new risk. The --time-budget-minutes budget is shared
across however many partitions are requested, not multiplied per
partition, so this trades depth (more historical coverage) for breadth
per partition within the same wall-clock run, not "more total runtime."

EXPLICIT PARTITION LIST (2026-08, new): --crawl-list takes a comma-
separated set of EXACT partition names instead of --crawl/--partitions'
contiguous most-recent-first walk — e.g. picking one partition from each
of several different years to sample a more diverse company population,
since adjacent months mostly re-cover largely-the-same host population
(the flatlining problem this was added for). See _resolve_explicit_crawl_names.

"LATEST" IS NOT WHAT IT SOUNDS LIKE — READ THIS BEFORE ASSUMING A BUG:
this dataset's own partition list (commoncrawl/host-index-testing-v2 on
Hugging Face) stopped at CC-MAIN-2025-18 in Nov 2025 and, confirmed live
against the dataset itself, has not been updated since — that is a real,
current fact about this specific derived dataset, not a bug in
_resolve_crawl_names/_list_all_crawl_names here. It is a COMPLETELY
SEPARATE data product from Common Crawl's raw monthly crawl archives
(CC-MAIN-2026-34 etc., published on commoncrawl.org itself on a much
faster cadence) — this project only ever reads the derived, pre-
aggregated per-host Parquet index (fetch_200/4xx/5xx counts per host),
which Common Crawl computes and publishes separately from and less often
than the raw crawls, and apparently hasn't refreshed in nearly a year as
of this writing. If Common Crawl ever publishes a newer Host Index
partition, --crawl/no-argument will pick it up automatically (the live
listing is queried fresh every run) — there is nothing to fix in this
code for that to happen; there's simply nothing newer to find yet.

COUNTRY FILTER — WHY NOT COMMON CRAWL'S OWN DATA: live research into the
Host Index's actual schema (fields: surt_host_name, url_host_tld, crawl,
fetch_200/3xx/4xx/5xx, hcrank/prank, fetch_200_lote/_pct) confirmed
Common Crawl does NOT publish any geography/country field at all — not
even in the raw WARC captures' WARC-IP-Address field (which lives in a
much heavier, separate dataset this project isn't querying here, and
which would only ever proxy CDN/hosting location, not company HQ, anyway
— see below). The Host Index's only geography-adjacent field is
url_host_tld (ccTLD), which Common Crawl's own docs explicitly warn is
NOT a geography signal (.com dominates globally). So url_host_tld is used
here ONLY the same way host_crawl_seed.py already used it — a cheap
CANDIDATE pre-filter (TARGET_TLDS) to avoid downloading/crawling hosts
that are almost certainly irrelevant — never as the actual country
decision.

THE REAL COUNTRY DECISION happens per-host, during the crawl, using
class_a_probe.py's detect_country() — a 2-tier waterfall (JSON-LD
addressCountry, then footer/<address> tag text via geo.py's
extract_countries()) built and unit-tested specifically for this task.
Deliberately excludes ccTLD and IP geolocation as DECIDING signals (see
detect_country's own docstring for the full research-backed reasoning —
short version: IP geolocation reflects the CDN edge node, not the
company, and MaxMind's own docs warn against using it for business-
identity purposes).

COUNTRY IS METADATA, NOT A GATE (changed 2026-08 — was originally a hard
non-negotiable requirement alongside the ATS hit; reversed per explicit
instruction: "not having location should not be a filter... if it's
found, the location, it should be added, but it should not be a
filter."). A row is written whenever a real ATS link is found —
full stop. detect_country() still runs on every fetched page and, when it
confidently resolves a country, that country is attached to the row;
when it can't (the common case — see below), the row is still written,
just with country left NULL, exactly like every other discovery method
already writes rows with country=NULL. Dropping the gate matters because
the two checks aren't actually related in quality: the ATS hit is already
a real, verified match (the same detection logic class_a_probe.py's
proven core uses) regardless of whether the page happens to also carry
parseable address data, so gating on the weaker signal was discarding
correct ATS discoveries for no quality benefit — see the coverage
estimate below for how large that discard rate would have been.

HONEST LIMITATION (stated up front, not discovered after the fact): the
country-detection waterfall's own research found real-world coverage of
maybe 15-30% of sites getting a confident, correct country label at all
(JSON-LD PostalAddress markup is rare — Web Data Commons found it on only
a low single-digit percentage of sites; footer/address text isn't always
present or parseable either). Expect most rows this script writes to have
country=NULL — that's the honest ceiling of what static HTML alone can
tell you, not a bug, and exactly why it's no longer a gate.

STREAMING SEED+CRAWL, ONE FILE AT A TIME (2026-08 — real incident, not a
theoretical concern): the first version of this script seeded the ENTIRE
partition into one Python list before crawling anything. A live run with
a high --host-limit accumulated 126 MILLION domain strings in memory by
file 17/30 and got OOM-killed by the runner's OS (reported as the generic
"Error: The operation was canceled." — a SIGKILL can't be caught, so
there's no Python-level traceback, exactly matching an OOM incident
host_crawl_seed.py's own history had already run into once before with
the same underlying cause: too much held in memory at once). DuckDB's own
memory_limit setting (3GB) never protected against this — it only bounds
DuckDB's internal query engine, not the plain Python list this script was
building FROM the query results.

Fixed two ways, together: (1) sharding is now pushed INTO the SQL query
itself via DuckDB's hash() function (hash(surt_host_name) % shard_count =
shard_index), so each shard's query only ever pulls its own ~1/N slice of
matching rows over the wire — cheaper AND safer, instead of pulling
everything and discarding 9/10 of it in Python afterward. (2) seeding and
crawling are now interleaved per Parquet file — each file's shard-
filtered hosts are crawled (and written) immediately, then that file's
host list is dropped, before the next file is even queried, and this
holds true walking across multiple partitions too (see HOW MANY
PARTITIONS above) — memory never scales with anything bigger than one
file's shard-filtered slice. TIME_BUDGET_MINUTES is the sole thing
governing how long a run lasts — there's no host-count cap.

ACCURACY / COVERAGE (2026-08): three changes to _crawl_one_v2's fallback
chain, all zero-risk of introducing false positives since every one just
changes what gets FETCHED or MERGED, never how a fetched page's own URLs
get validated (still discovery.py's anchored URL_TO_SLUG — see that
file's _host_matches_domain history for why that anchoring itself
matters). (1) Tiers no longer stop at the first page with a hit — every
page fetched at a tier (all CAREER_PATHS, all sitemap-matched URLs) is
checked and merged (see class_a_probe.py's _collapse_hits), so a company
running two ATS platforms at once (old + new mid-migration) now gets
both instead of whichever happened to be checked first. (2) The sitemap
fallback's "does this URL look like a careers page" filter used to only
recognize career/jobs/join/work-with-us — now built directly from
CAREER_PATHS (CAREER_LIKE_RE) so it can't silently drift out of sync
with the direct-path list again. (3) SITEMAP_MAX_FOLLOW raised 3 -> 8,
and a site publishing its sitemap at /sitemap_index.xml instead of
/sitemap.xml (a common WordPress-family convention) is now reached too
(SITEMAP_INDEX_PATHS) — previously never tried at all.

Usage:
    python host_crawl_v2.py                                       # 1 partition, unsharded (small/dev runs only — see SHARDING note)
    python host_crawl_v2.py --crawl CC-MAIN-2025-18                # pin the starting partition
    python host_crawl_v2.py --partitions 3                        # 3 most recent partitions, one run, shared time budget
    python host_crawl_v2.py --shard-index 0 --shard-count 10 --partitions 3   # production shape
"""

import argparse
import asyncio
import concurrent.futures
import logging
import os
import sys
import time
from collections import Counter
from urllib.parse import urljoin, urlparse

import aiohttp
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# Reusing class_a_probe.py's already-proven fetch/parse/detect/write core
# directly, rather than re-implementing any of it — see module docstring.
from class_a_probe import (  # noqa: E402
    CAREER_LIKE_RE,
    CAREER_PATHS,
    CONNECTOR_LIMIT,
    CRAWL_CONCURRENCY,
    PARSE_WORKERS,
    SITEMAP_MAX_FOLLOW,
    SUPABASE_URL,
    SUPABASE_KEY,
    TIME_BUDGET_MINUTES,
    _collapse_hits,
    _fetch_page,
    _fetch_sitemap,
    _parse_detect_ats_and_country,
    write_rows_to_staging_table,
)
import re  # noqa: E402 — used for sitemap <loc> extraction, same pattern as class_a_probe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("host_crawl_v2")

# Optional: a Hugging Face token, purely for the higher authenticated
# rate-limit tier — NOT required, the Host Index dataset is fully public.
# See host_crawl_seed.py's HF_TOKEN comment for the full "why" (anonymous
# CI traffic from shared GitHub Actions IPs is more exposed to silent
# throttling/stalls).
HF_TOKEN = os.environ.get("HF_TOKEN", "")

HF_DATASET = "commoncrawl/host-index-testing-v2"
HF_BASE = f"hf://datasets/{HF_DATASET}/data"
_FALLBACK_CRAWL = "CC-MAIN-2025-18"

# Same candidate pre-filter host_crawl_seed.py already built and this
# project already trusts — per-country ccTLDs for the target markets plus
# generic/startup TLDs that skew heavily toward them (.com, .io, .co,
# .app, .dev). A SOFT proxy only, never the actual country decision (see
# module docstring) — the real decision is detect_country() during crawl.
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
    """Same conservative liveness filter host_crawl_seed.py uses — a host
    is only treated as dead if Common Crawl's own crawl attempts NEVER got
    a single 2xx for it. Biased toward keeping a host if in doubt."""
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
        log.error(f"Failed to load DuckDB's httpfs extension (needed for hf:// reads): {e}")
        return None
    if not HF_TOKEN:
        log.warning("HF_TOKEN is not set — running fully anonymous against huggingface.co "
                    "from a shared GitHub Actions IP pool. This can manifest as a SILENT "
                    "STALL rather than a clean error. Strongly recommend setting the "
                    "HF_TOKEN secret (free — generate at huggingface.co/settings/tokens "
                    "with 'read' scope).")
    else:
        try:
            con.execute(f"CREATE SECRET hf_token (TYPE huggingface, TOKEN '{HF_TOKEN}');")
            log.info("Using HF_TOKEN for authenticated (higher rate-limit) access.")
        except Exception as e:
            log.warning(f"Failed to configure HF_TOKEN secret (continuing anonymously): {e}")
    try:
        con.execute("SET http_timeout = 30000;")
        con.execute("SET http_retries = 3;")
        con.execute("SET http_retry_wait_ms = 2000;")
        con.execute("SET http_retry_backoff = 2;")
        # One partition is ~7GB (Common Crawl's own blog post) — capped
        # well under GitHub-hosted runners' 7GB free-tier RAM, same
        # reasoning as host_crawl_seed.py's identical setting.
        con.execute("SET memory_limit = '3GB';")
        con.execute("PRAGMA threads=2;")
    except Exception as e:
        log.warning(f"Could not set DuckDB timeout/memory options (continuing with "
                    f"DuckDB defaults): {e}")
    return con


def _list_all_crawl_names(con) -> list[str]:
    """Live-lists every partition Hugging Face has, most recent first.
    Empty list if the listing itself fails (network hiccup, HF down,
    etc) — callers fall back to a single hardcoded partition in that case."""
    try:
        rows = _run_with_timeout(
            lambda: con.execute(
                f"SELECT DISTINCT regexp_extract(file, 'crawl=([^/]+)', 1) AS crawl "
                f"FROM glob('{HF_BASE}/crawl=*/') ORDER BY crawl DESC"
            ).fetchall(),
            timeout=60,
        )
        return [r[0] for r in rows if r[0]]
    except concurrent.futures.TimeoutError:
        log.warning("Listing crawl partitions timed out after 60s.")
    except Exception as e:
        log.warning(f"Could not list crawl partitions live: {e}")
    return []


def _resolve_explicit_crawl_names(con, requested: list[str]) -> list[str]:
    """EXPLICIT-LIST seeding (2026-08, new): crawl a specific, possibly
    non-contiguous set of partitions the caller names directly — e.g.
    spread across different years to sample a more diverse company
    population instead of --partitions N's contiguous most-recent-first
    walk, which mostly re-covers largely-the-same host population month
    to month (see the flatlining discussion this was born from).

    Validates each requested name against the live listing when that
    listing succeeds — logging (not silently dropping) any name that
    doesn't actually exist in the dataset, since a typo'd partition name
    here would otherwise just silently crawl 0 hosts for it. If the live
    listing itself fails, there's nothing to validate against, so the
    requested names are trusted as-is (same "can't verify, so don't
    block" fallback philosophy _resolve_crawl_names already uses)."""
    names = _list_all_crawl_names(con)
    if not names:
        log.warning("Live partition listing failed — trusting the --crawl-list names as given, "
                     "unvalidated.")
        return requested
    missing = [n for n in requested if n not in names]
    if missing:
        log.warning(f"{len(missing)}/{len(requested)} requested partition(s) not found in the "
                    f"live listing (typo, or genuinely not in this dataset — see module "
                    f"docstring's note on the Host Index's real update cadence): {missing}")
    resolved = [n for n in requested if n in names]
    return resolved


def _resolve_crawl_names(con, pinned: str | None, count: int) -> list[str]:
    """Resolves `count` partitions to crawl this run, in most-recent-first
    order, starting from `pinned` if given (or the true latest otherwise)
    and walking backward through older partitions to fill out the count.
    Falls back to a single partition — `pinned` or the hardcoded default —
    if the live listing fails, since without it there's no way to know
    which partitions are older than a given one."""
    names = _list_all_crawl_names(con)
    if names:
        log.info(f"Available crawl partitions (most recent 5 of {len(names)}): {names[:5]}")
        start = names.index(pinned) if pinned in names else 0
        selected = names[start:start + count]
        if len(selected) < count:
            log.warning(f"Only {len(selected)}/{count} partition(s) available at/older than "
                        f"the start point — crawling all {len(selected)} of them.")
        return selected
    single = pinned or _FALLBACK_CRAWL
    log.warning(f"Falling back to a single hardcoded partition {single!r} (may be stale) — "
                f"live listing failed, so {count} partition(s) were requested but only this "
                f"one can be resolved without it.")
    return [single]


def _list_partition_files(con, crawl_name: str) -> list[str]:
    try:
        files = _run_with_timeout(
            lambda: con.execute(
                f"SELECT file FROM glob('{HF_BASE}/crawl={crawl_name}/*.parquet')"
            ).fetchall(),
            timeout=60,
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
    """Streams candidate hostnames across one or more Common Crawl Host
    Index partitions, ONE PARQUET FILE AT A TIME — yields (partition_name,
    partition_num, total_partitions, file_num, total_files_in_partition,
    hosts_for_this_file) and never holds more than one file's worth of
    hosts in memory at once, no matter how many partitions are requested
    (see module docstring's STREAMING SEED+CRAWL note for why: the
    previous whole-partition-at-once version OOM-killed a real run at
    126M accumulated hosts — walking additional partitions is safe here
    for the same reason a single partition's files are: each one is still
    processed and dropped before the next is even queried).

    SHARDING HAPPENS IN SQL, not in Python (2026-08 — see module
    docstring): when shard_index/shard_count are given, the query itself
    filters to hash(surt_host_name) % shard_count = shard_index, so each
    shard's DuckDB query only ever pulls its own ~1/N slice of matching
    rows over the wire — both cheaper (less data transferred per shard)
    and safer (each file's per-shard result set is proportionally
    smaller) than pulling every row and discarding 9/10 of it in Python
    afterward, which is what the old hosts[shard::shard_count] slicing
    did. hash() is deterministic for a given string, so independent shard
    jobs re-running this same query always get the identical, non-
    overlapping partitioning — no coordination between shards needed."""
    con = _get_duckdb_connection()
    if con is None:
        return

    sharded = shard_index is not None and shard_count is not None
    if not sharded:
        log.warning("Running UNSHARDED — each file's full TLD-matched candidate set (can be "
                    "10M+ rows) will be pulled into memory at once. Fine for a small/dev run; "
                    "use --shard-index/--shard-count for a production-scale run.")

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
        log.info(f"  {len(files)} file(s) to scan"
                 + (f" — shard {shard_index}/{shard_count}" if sharded else ""))

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
                log.warning(f"  file {file_num}/{len(files)}: query timed out after "
                            f"{query_timeout}s — skipping this file.")
                continue
            except Exception as e:
                log.warning(f"  file {file_num}/{len(files)}: query failed — skipping: {e}")
                continue

            # Dedup is PER-FILE only, not across the whole run (2026-08 — a
            # deliberate trade after the OOM incident): a persistent
            # cross-file `seen` set would itself grow to the same size as
            # the old whole-partition host list and reintroduce the same
            # memory risk. If the same host happens to appear in more than
            # one file (unlikely — the Host Index partitions by hash
            # range) it just gets crawled again — a handful of duplicate
            # HTTP requests, not a correctness problem: the (ats,slug)
            # dedup in the write path already prevents any duplicate row
            # from reaching Supabase either way.
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
            log.info(f"  file {file_num}/{len(files)}: {len(file_hosts)} live hosts to crawl "
                     f"(of {len(rows)} candidates, {dead_skipped} dead-skipped) — "
                     f"{total_hosts} seeded so far this partition")
            yield crawl_name, partition_num, len(partitions), file_num, len(files), file_hosts


# ── crawl (reuses class_a_probe.py's fetch/parse core) ─────────────────

async def _crawl_one_v2(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                         domain: str, stats: Counter,
                         parse_pool: concurrent.futures.ProcessPoolExecutor,
                         target_geo_countries: set[str]
                         ) -> list[tuple[str, str, str, str, str, str, str]]:
    """Same homepage -> career-path -> sitemap fallback chain as
    class_a_probe.py's _crawl_one (including the 2026-08 all-pages-in-a-
    tier merge — see _collapse_hits there), but ALSO resolves country per
    page via _parse_detect_ats_and_country. COUNTRY IS NOT A GATE (2026-08
    — see module docstring): rows are returned as soon as a tier produces
    ANY hits. Country is opportunistic metadata — whatever's confidently
    resolved from the pages already fetched in that tier is attached to
    every hit found there; if nothing confidently resolved, rows are still
    returned, just with country=None, same as every other discovery method
    already writes when it doesn't know. Returns (ats, slug, matched_url,
    domain, tier, country, country_method) tuples."""
    loop = asyncio.get_running_loop()
    async with sem:
        stats["companies_attempted"] += 1
        candidates = [f"https://{domain}"]
        if not domain.startswith("www."):
            candidates.append(f"https://www.{domain}")
        candidates.append(f"http://{domain}")

        page = None
        for base_url in candidates:
            page = await _fetch_page(session, base_url, stats)
            if page:
                break
        if not page:
            stats["homepage_unreachable"] += 1
            return []

        final_url, html = page
        stats["homepage_fetched"] += 1

        best_country, best_method = None, None

        async def _detect(html_, url_):
            nonlocal best_country, best_method
            hits, country, method = await loop.run_in_executor(
                parse_pool, _parse_detect_ats_and_country, html_, url_, target_geo_countries)
            if country and best_country is None:
                best_country, best_method = country, method
            return hits

        hits = await _detect(html, final_url)
        if hits:
            stats["hits_from_homepage"] += 1
            if not best_country:
                stats["written_without_country"] += 1
            return [(ats, slug, url, domain, "homepage", best_country, best_method)
                    for ats, slug, url in hits]

        # Fallback tier 1: common career-page paths, fetched concurrently,
        # every hit-bearing page merged (see class_a_probe.py's _collapse_hits).
        origin_parts = urlparse(final_url)
        origin = f"{origin_parts.scheme}://{origin_parts.netloc}"
        career_pages = await asyncio.gather(
            *[_fetch_page(session, urljoin(origin, p), stats) for p in CAREER_PATHS]
        )
        career_hit_lists = []
        for cp in career_pages:
            if not cp:
                continue
            cp_url, cp_html = cp
            career_hit_lists.append(await _detect(cp_html, cp_url))
        merged = _collapse_hits(career_hit_lists)
        if merged:
            stats["hits_from_career_path"] += 1
            if not best_country:
                stats["written_without_country"] += 1
            return [(ats, slug, url, domain, "career_path", best_country, best_method)
                    for ats, slug, url in merged]

        # Fallback tier 2: sitemap, only reached if everything above found nothing.
        sitemap = await _fetch_sitemap(session, origin, stats)
        if sitemap:
            sm_url, sm_xml = sitemap
            loc_urls = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", sm_xml, re.I)
            career_like = [u for u in loc_urls if CAREER_LIKE_RE.search(u)]
            sm_pages = await asyncio.gather(
                *[_fetch_page(session, u, stats) for u in career_like[:SITEMAP_MAX_FOLLOW]]
            )
            sitemap_hit_lists = []
            for sp in sm_pages:
                if not sp:
                    continue
                sp_url, sp_html = sp
                sitemap_hit_lists.append(await _detect(sp_html, sp_url))
            merged = _collapse_hits(sitemap_hit_lists)
            if merged:
                stats["hits_from_sitemap"] += 1
                if not best_country:
                    stats["written_without_country"] += 1
                return [(ats, slug, url, domain, "sitemap", best_country, best_method)
                        for ats, slug, url in merged]

        stats["dropped_no_ats"] += 1
        return []


async def _crawl_and_write_hosts(hosts: list[str], session, sem, stats, parse_pool,
                                  accept_any_country, found_rows: list[dict],
                                  crawl_start: float, time_budget_seconds: float,
                                  time_budget_minutes: int) -> tuple[int, float, float, bool]:
    """Crawls one already-seeded batch of hosts (typically one file's
    worth) in sub-batches of 2000, writing each sub-batch to Supabase as
    it completes — same batching/dedup/logging shape run_host_crawl used
    when it crawled the whole partition at once, just now called per file
    from the outer streaming loop. Returns (hosts_done, elapsed, rate,
    time_budget_hit) so the caller can decide whether to continue to the
    next file."""
    tasks = [_crawl_one_v2(session, sem, h, stats, parse_pool, accept_any_country) for h in hosts]
    BATCH = 2000
    elapsed = time.monotonic() - crawl_start
    rate = 0.0
    time_budget_hit = False
    for i in range(0, len(tasks), BATCH):
        if time.monotonic() - crawl_start >= time_budget_seconds:
            for t in tasks[i:]:
                t.close()
            time_budget_hit = True
            log.warning(f"  time budget ({time_budget_minutes}min) reached mid-file at "
                        f"{i}/{len(tasks)} hosts in this file — stopping here, everything "
                        f"found so far is written.")
            break
        batch = tasks[i:i + BATCH]
        results = await asyncio.gather(*batch)
        batch_rows = []
        seen_keys = set()
        duplicates_collapsed = 0
        for host_hits in results:
            for ats, slug, matched_url, domain, tier, country, method in host_hits:
                key = (ats, slug)
                if key in seen_keys:
                    duplicates_collapsed += 1
                    continue
                seen_keys.add(key)
                batch_rows.append({
                    "ats": ats,
                    "slug": slug,
                    "source_hostname": matched_url[:250],
                    "root_domain": domain,
                    "country": country,
                    "discovery_method": "host_crawl_v2",
                })
        written = 0
        if batch_rows:
            written = await write_rows_to_staging_table(session, batch_rows)
            found_rows.extend(batch_rows)

        done = min(i + BATCH, len(tasks))
        elapsed = time.monotonic() - crawl_start
        rate = stats["companies_attempted"] / elapsed if elapsed > 0 else 0
        hosts_n = max(stats["companies_attempted"], 1)
        hit_n = stats["hits_from_homepage"] + stats["hits_from_career_path"] + stats["hits_from_sitemap"]

        log.info(f"  {done}/{len(tasks)} hosts — {rate:.1f}/sec — {elapsed:.0f}s elapsed")
        dup_note = f", {duplicates_collapsed} dup collapsed" if duplicates_collapsed else ""
        log.info(f"    → {written} written{dup_note} — {len(found_rows)} hits total "
                 f"({hit_n / hosts_n * 100:.2f}% hit rate)")
    return len(tasks), elapsed, rate, time_budget_hit


async def run_host_crawl(crawl: str | None, partitions_count: int, shard_index: int | None,
                          shard_count: int | None, concurrency: int,
                          time_budget_minutes: int, crawl_list: list[str] | None = None,
                          start_file_index: int = 0) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(f"── Host Crawl v2{label} ──")
    log.info(f"  concurrency={concurrency} (network fetches in parallel — no rate limiting)")
    log.info(f"  parse_workers={PARSE_WORKERS} (separate process pool for HTML/JSON-LD parsing)")
    log.info(f"  time_budget={time_budget_minutes}min (graceful cutoff, shared across all "
             f"requested partitions)")

    con = _get_duckdb_connection()
    if con is None:
        return
    if crawl_list:
        log.info(f"  explicit --crawl-list given ({len(crawl_list)} requested) — ignoring "
                 f"--crawl/--partitions")
        partitions = _resolve_explicit_crawl_names(con, crawl_list)
    else:
        partitions = _resolve_crawl_names(con, crawl, partitions_count)
    if not partitions:
        log.error("No partition(s) resolved — nothing to crawl.")
        return
    log.info(f"  partitions ({len(partitions)}): {partitions}")

    # No target-country restriction at all (2026-08 — see module
    # docstring): country is never a gate here, so there's no "target
    # set" to restrict detect_country() to — pass a sentinel that accepts
    # ANY country detect_country() can ever resolve (geo.py's full known-
    # country set), keeping its single-match precision logic unchanged
    # while removing the "must be one of N countries" restriction
    # entirely.
    import geo
    accept_any_country = set(geo.COUNTRY_ALIASES.values()) | set(geo.COUNTRY_CONTINENT.keys())

    resolver = None
    try:
        import aiodns  # noqa: F401
        resolver = aiohttp.AsyncResolver()
    except ImportError:
        log.warning("  aiodns not installed — DNS resolution will use the slower default resolver.")
    connector = aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, ttl_dns_cache=300, resolver=resolver)
    sem = asyncio.Semaphore(concurrency)
    stats = Counter()
    found_rows: list[dict] = []

    parse_pool = concurrent.futures.ProcessPoolExecutor(max_workers=PARSE_WORKERS)
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
                    log.warning(f"  time budget ({time_budget_minutes}min) reached before "
                                f"{partition_name} file {file_num}/{total_files} — stopping "
                                f"here, everything found so far is written. Remaining seeding "
                                f"was skipped entirely (not just the crawl).")
                    break
                partitions_completed = partition_num - 1
                if not file_hosts:
                    continue
                total_hosts_seen += len(file_hosts)
                _, elapsed, rate, file_time_hit = await _crawl_and_write_hosts(
                    file_hosts, session, sem, stats, parse_pool, accept_any_country,
                    found_rows, crawl_start, time_budget_seconds, time_budget_minutes)
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
    if time_budget_hit:
        log.info(f"  status:      STOPPED EARLY — time budget reached mid-{last_partition_name}")
    else:
        log.info("  status:      complete — all requested partitions covered")
    log.info(f"  partitions:  {partitions_completed}/{len(partitions)} fully covered "
             f"({partitions})")
    log.info(f"  hosts:       {stats['companies_attempted']} attempted, {total_hosts_seen} seeded")
    log.info(f"  time:        {elapsed:.0f}s of {time_budget_seconds:.0f}s budget, "
             f"{rate:.1f} hosts/sec avg")
    log.info("")
    log.info("  accuracy:")
    log.info(f"    ATS hits found:     {total_hits} ({total_hits / hosts_n * 100:.2f}% of hosts attempted)")
    log.info(f"    with country:       {total_hits - stats['written_without_country']} "
             f"({(1 - stats['written_without_country'] / max(total_hits, 1)) * 100:.1f}% of hits)")
    log.info(f"    no ATS found:       {stats['dropped_no_ats']} "
             f"({stats['dropped_no_ats'] / hosts_n * 100:.1f}% of hosts attempted)")
    log.info(f"    unreachable:        {stats['homepage_unreachable']} "
             f"({stats['homepage_unreachable'] / hosts_n * 100:.1f}% of hosts attempted)")
    if total_hits:
        log.info("")
        log.info("  hits by tier:")
        log.info(f"    homepage:     {stats['hits_from_homepage']} "
                 f"({stats['hits_from_homepage'] / total_hits * 100:.1f}%)")
        log.info(f"    career_path:  {stats['hits_from_career_path']} "
                 f"({stats['hits_from_career_path'] / total_hits * 100:.1f}%)")
        log.info(f"    sitemap:      {stats['hits_from_sitemap']} "
                 f"({stats['hits_from_sitemap'] / total_hits * 100:.1f}%)")
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
        description="Host Crawl v2 — follow-links ATS discovery across one or more Common "
                     "Crawl partitions. Country is detected and recorded when found, but is "
                     "NOT a filter — see module docstring.")
    parser.add_argument("--crawl", type=str, default=None,
                         help="Pin the starting Common Crawl partition (e.g. CC-MAIN-2025-18). "
                              "Default: the most recent available in this dataset (see module "
                              "docstring — that is NOT necessarily Common Crawl's most recent "
                              "raw crawl release; this dataset has its own, slower update cadence). "
                              "Ignored if --crawl-list is given.")
    parser.add_argument("--partitions", type=int, default=1,
                         help="How many partitions to crawl this run, starting at --crawl (or "
                              "the latest) and walking backward through CONTIGUOUS older ones. "
                              "Safe to raise — memory only ever holds one file's worth of hosts "
                              "regardless of how many partitions are requested (see module "
                              "docstring). The --time-budget-minutes budget is shared across "
                              "all of them, not per-partition, so more partitions means less "
                              "time per partition, not more total runtime. Ignored if "
                              "--crawl-list is given.")
    parser.add_argument("--crawl-list", type=str, default=None,
                         help="Comma-separated exact partition names, e.g. "
                              "'CC-MAIN-2025-18,CC-MAIN-2024-42'. Overrides --crawl/--partitions.")
    parser.add_argument("--start-file-index", type=int, default=0,
                         help="Skip this many files in the FIRST partition before starting "
                              "(0-based). Later partitions always start at file 0.")
    parser.add_argument("--shard-index", type=int, default=None,
                         help="This job's shard index (0-based) — filtering happens IN THE SQL "
                              "QUERY itself (hash(surt_host_name) %% shard_count), not by "
                              "slicing a Python list, so pass this together with --shard-count "
                              "for any production-scale run (see module docstring's STREAMING "
                              "note for why an unsharded run can pull 10M+ rows per file into "
                              "memory at once).")
    parser.add_argument("--shard-count", type=int, default=None,
                         help="Total number of shards (must be passed together with --shard-index)")
    parser.add_argument("--concurrency", type=int, default=CRAWL_CONCURRENCY,
                         help=f"Hosts crawled in parallel (default {CRAWL_CONCURRENCY})")
    parser.add_argument("--time-budget-minutes", type=int, default=TIME_BUDGET_MINUTES,
                         help=f"Stop gracefully after this many minutes (default {TIME_BUDGET_MINUTES}) "
                              f"— the ONLY thing bounding how much gets covered; there's no "
                              f"separate host-count cap.")
    args = parser.parse_args()
    crawl_list = [c.strip() for c in args.crawl_list.split(",") if c.strip()] if args.crawl_list else None

    asyncio.run(run_host_crawl(args.crawl, args.partitions, args.shard_index, args.shard_count,
                                args.concurrency, args.time_budget_minutes, crawl_list=crawl_list,
                                start_file_index=args.start_file_index))


if __name__ == "__main__":
    main()
