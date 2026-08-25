"""
Host Crawl v2 — single-pass, single-partition, country-aware
==============================================================
Replaces the old two-stage host_crawl.py/host_crawl_seed.py design
(seed into a Supabase queue table, then a SEPARATE crawl job drains that
queue) with ONE pass: seed hostnames from exactly ONE Common Crawl Host
Index partition, then immediately crawl each one the exact same way
class_a_probe.py crawls a PDL company (visit the real site, follow links,
look for a real ATS URL — NOT guessing slugs against any platform's API),
using class_a_probe.py's already-proven fetch/parse/detect core directly
(imported, not re-implemented).

WHY NOT THE OLD QUEUE DESIGN: that design existed to let seeding and
crawling happen as separate, independently-resumable GitHub Actions jobs,
converging on FULL coverage of a partition across many runs (h_seeding /
h_file_count / h_total_seeded checkpointing). This build is deliberately
scoped to exactly 1 partition, one run, and doesn't need that convergence
machinery — it trades "eventually see everything" for "simple, single
job, done." If broader multi-partition coverage is wanted later, that
checkpointing infrastructure in host_crawl_seed.py is still there and can
be adapted; nothing here depends on it existing or not.

WHY EXACTLY 1 PARTITION: a live check of Common Crawl's own crawl
schedule confirmed one partition (e.g. CC-MAIN-2025-18) spans roughly
1-1.5 months, not a year — Common Crawl publishes ~6-12 of these a year.
The ~26 partitions Hugging Face lists span roughly 2-4 years of real
crawl history, not 26 years. Given that, and that host TURNOVER within a
couple months is low, one partition is a reasonable, deliberately modest
first pass — this is what was explicitly asked for, not a compromise.

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

Usage:
    python host_crawl_v2.py                              # 1 partition, safe host limit, country recorded when found
    python host_crawl_v2.py --crawl CC-MAIN-2025-18       # pin a specific partition
    python host_crawl_v2.py --host-limit 200000           # cap how many candidate hosts are seeded
    python host_crawl_v2.py --shard-index 0 --shard-count 10   # split the seeded host list across jobs
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
    CAREER_PATHS,
    CONNECTOR_LIMIT,
    CRAWL_CONCURRENCY,
    PARSE_WORKERS,
    SITEMAP_MAX_FOLLOW,
    SUPABASE_URL,
    SUPABASE_KEY,
    TIME_BUDGET_MINUTES,
    _fetch_page,
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

# SEEDING-ONLY CAP (2026-08 — explicit "max hosts" instruction): this
# does NOT bound how long a run takes — TIME_BUDGET_MINUTES already does
# that, by stopping the CRAWL gracefully and writing everything found so
# far, no matter how large the seeded host list is. This constant only
# stops the SEED SCAN (reading Parquet files from the partition) once
# it's collected this many candidate hosts, so it exists purely to avoid
# an unbounded Python list if the true partition total turns out to be
# absurd. Set deliberately far above any realistic partition size — a
# live host_crawl_seed.py run found ~1.66M matching hosts in JUST ONE of
# ~30 files in a single partition, so the true full-partition total is
# almost certainly well under this number, meaning in practice the seed
# scan runs to completion (every file) rather than stopping early.
_DEFAULT_HOST_LIMIT = 50_000_000

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


def _resolve_crawl_name(con, pinned: str | None) -> str:
    if pinned:
        return pinned
    try:
        rows = _run_with_timeout(
            lambda: con.execute(
                f"SELECT DISTINCT regexp_extract(file, 'crawl=([^/]+)', 1) AS crawl "
                f"FROM glob('{HF_BASE}/crawl=*/') ORDER BY crawl DESC"
            ).fetchall(),
            timeout=60,
        )
        names = [r[0] for r in rows if r[0]]
        if names:
            log.info(f"Available crawl partitions (most recent 5): {names[:5]}")
            return names[0]
    except concurrent.futures.TimeoutError:
        log.warning("Listing crawl partitions timed out after 60s.")
    except Exception as e:
        log.warning(f"Could not list crawl partitions live: {e}")
    log.warning(f"Falling back to hardcoded {_FALLBACK_CRAWL!r} (may be stale).")
    return _FALLBACK_CRAWL


def seed_hosts(crawl: str | None = None, host_limit: int = _DEFAULT_HOST_LIMIT) -> list[str]:
    """Seeds candidate hostnames from exactly ONE Common Crawl Host Index
    partition — one Parquet file at a time (memory-bounded, same proven
    pattern host_crawl_seed.py uses — a whole partition read at once can
    OOM-kill a GitHub-hosted runner). Returns a deduped list of real
    domain names (surt_host_name un-reversed), capped at host_limit."""
    con = _get_duckdb_connection()
    if con is None:
        return []

    crawl_name = _resolve_crawl_name(con, crawl)
    log.info(f"Seeding from crawl partition: {crawl_name} (exactly 1 partition, as instructed)")

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
    log.info(f"crawl={crawl_name}: {len(files)} Parquet file(s) to scan (stopping early once "
             f"host_limit={host_limit} is reached)")

    tld_filter = _build_tld_filter()
    hosts: list[str] = []
    seen: set[str] = set()
    dead_skipped = 0
    query_timeout = 300

    for file_num, fpath in enumerate(files, start=1):
        if len(hosts) >= host_limit:
            log.info(f"  host_limit reached ({len(hosts)}) — stopping seed scan "
                     f"({file_num - 1}/{len(files)} files scanned).")
            break
        query = f"""
            SELECT surt_host_name, fetch_200, fetch_4xx, fetch_5xx, fetch_gone, nutch_gone
            FROM read_parquet('{fpath}')
            WHERE url_host_tld IN ({tld_filter})
        """
        try:
            rows = _run_with_timeout(lambda q=query: con.execute(q).fetchall(), timeout=query_timeout)
        except concurrent.futures.TimeoutError:
            log.warning(f"  [{file_num}/{len(files)}] query timed out after {query_timeout}s — skipping.")
            continue
        except Exception as e:
            log.warning(f"  [{file_num}/{len(files)}] query failed — skipping: {e}")
            continue

        file_new = 0
        for surt_host, f200, f4xx, f5xx, fgone, ngone in rows:
            if not surt_host:
                continue
            if _looks_dead(f200, f4xx, f5xx, fgone, ngone):
                dead_skipped += 1
                continue
            domain = ".".join(reversed(surt_host.split(",")))
            if domain in seen:
                continue
            seen.add(domain)
            hosts.append(domain)
            file_new += 1
            if len(hosts) >= host_limit:
                break
        log.info(f"  [{file_num}/{len(files)}] {len(rows)} candidate rows, +{file_new} new hosts "
                 f"({len(hosts)}/{host_limit} total so far, {dead_skipped} dead-skipped)")

    log.info(f"Seed complete: {len(hosts)} candidate hosts from crawl={crawl_name} "
             f"(TLD-prefiltered only — NOT yet country-checked; see module docstring)")
    return hosts


# ── crawl (reuses class_a_probe.py's fetch/parse core) ─────────────────

async def _crawl_one_v2(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                         domain: str, stats: Counter,
                         parse_pool: concurrent.futures.ProcessPoolExecutor,
                         target_geo_countries: set[str]
                         ) -> list[tuple[str, str, str, str, str, str, str]]:
    """Same homepage -> career-path -> sitemap fallback chain as
    class_a_probe.py's _crawl_one, but ALSO resolves country per page via
    _parse_detect_ats_and_country. COUNTRY IS NOT A GATE (2026-08 — see
    module docstring): a row is returned as soon as a real ATS hit is
    found, at whichever tier finds it first, exactly like
    class_a_probe.py's _crawl_one. Country is opportunistic metadata —
    whatever's confidently resolved from the pages already fetched by the
    point the ATS hit is found (most often just the homepage) is attached;
    if nothing confidently resolved yet, the row is still returned, just
    with country=None, same as every other discovery method already
    writes when it doesn't know. Returns (ats, slug, matched_url, domain,
    tier, country, country_method) tuples."""
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

        origin_parts = urlparse(final_url)
        origin = f"{origin_parts.scheme}://{origin_parts.netloc}"
        career_pages = await asyncio.gather(
            *[_fetch_page(session, urljoin(origin, p), stats) for p in CAREER_PATHS]
        )
        for cp in career_pages:
            if not cp:
                continue
            cp_url, cp_html = cp
            hits = await _detect(cp_html, cp_url)
            if hits:
                stats["hits_from_career_path"] += 1
                if not best_country:
                    stats["written_without_country"] += 1
                return [(ats, slug, url, domain, "career_path", best_country, best_method)
                        for ats, slug, url in hits]

        sitemap = await _fetch_page(session, urljoin(origin, "/sitemap.xml"), stats)
        if sitemap:
            sm_url, sm_xml = sitemap
            loc_urls = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", sm_xml, re.I)
            career_like = [u for u in loc_urls if re.search(r"career|jobs?|join|work-with-us", u, re.I)]
            sm_pages = await asyncio.gather(
                *[_fetch_page(session, u, stats) for u in career_like[:SITEMAP_MAX_FOLLOW]]
            )
            for sp in sm_pages:
                if not sp:
                    continue
                sp_url, sp_html = sp
                hits = await _detect(sp_html, sp_url)
                if hits:
                    stats["hits_from_sitemap"] += 1
                    if not best_country:
                        stats["written_without_country"] += 1
                    return [(ats, slug, url, domain, "sitemap", best_country, best_method)
                            for ats, slug, url in hits]

        stats["dropped_no_ats"] += 1
        return []


async def run_host_crawl(crawl: str | None, host_limit: int,
                          shard_index: int | None, shard_count: int | None,
                          concurrency: int, time_budget_minutes: int) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(f"── Host Crawl v2{label} — 1 partition, follow-links ATS discovery, "
             f"country recorded when found (not a filter) ──")

    hosts = seed_hosts(crawl=crawl, host_limit=host_limit)
    if not hosts:
        log.error("  No seed hosts found — aborting.")
        return

    if shard_index is not None and shard_count is not None:
        hosts = hosts[shard_index::shard_count]
        log.info(f"  {len(hosts)} hosts in this shard's slice")

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

    try:
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
            tasks = [_crawl_one_v2(session, sem, h, stats, parse_pool, accept_any_country) for h in hosts]

            BATCH = 2000
            total_distinct_hits = 0
            crawl_start = time.monotonic()
            elapsed, rate = 0.0, 0.0
            time_budget_hit = False
            for i in range(0, len(tasks), BATCH):
                if time.monotonic() - crawl_start >= time_budget_seconds:
                    for t in tasks[i:]:
                        t.close()
                    time_budget_hit = True
                    log.warning(f"  time budget ({time_budget_minutes}min) reached at "
                                f"{i}/{len(tasks)} hosts — stopping here, everything found "
                                f"so far is written.")
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
                if duplicates_collapsed:
                    log.info(f"    ({duplicates_collapsed} duplicate (ats,slug) hits collapsed before writing)")
                written = 0
                if batch_rows:
                    total_distinct_hits += len(batch_rows)
                    written = await write_rows_to_staging_table(session, batch_rows)
                    found_rows.extend(batch_rows)

                done = min(i + BATCH, len(tasks))
                elapsed = time.monotonic() - crawl_start
                rate = done / elapsed if elapsed > 0 else 0

                hosts_n = max(stats["companies_attempted"], 1)
                hit_n = stats["hits_from_homepage"] + stats["hits_from_career_path"] + stats["hits_from_sitemap"]

                log.info(f"  batch {done}/{len(tasks)} hosts — {rate:.1f} hosts/sec — {elapsed:.0f}s elapsed")
                log.info(f"    → supabase: {written}/{len(batch_rows)} rows written this batch "
                         f"({total_distinct_hits} hits total so far)")
                log.info(f"    hosts so far: hit(ats)={hit_n / hosts_n * 100:.1f}% "
                         f"(of which no_country={stats['written_without_country'] / max(hit_n, 1) * 100:.1f}%) "
                         f"dropped_no_ats={stats['dropped_no_ats'] / hosts_n * 100:.1f}% "
                         f"unreachable={stats['homepage_unreachable'] / hosts_n * 100:.1f}%")
    finally:
        parse_pool.shutdown(wait=True)

    total_hits = stats["hits_from_homepage"] + stats["hits_from_career_path"] + stats["hits_from_sitemap"]
    hosts_n = max(stats["companies_attempted"], 1)
    if time_budget_hit:
        log.info(f"── host_crawl_v2{label} STOPPED EARLY (time budget): "
                 f"{stats['companies_attempted']}/{len(hosts)} hosts attempted, "
                 f"{elapsed:.0f}s, {rate:.1f} hosts/sec avg ──")
    else:
        log.info(f"── host_crawl_v2{label} complete: {len(hosts)} hosts, {elapsed:.0f}s, "
                 f"{rate:.1f} hosts/sec avg ──")
    log.info(f"  hits(ats)={total_hits / hosts_n * 100:.1f}% ({total_hits}) | "
             f"of which no_country={stats['written_without_country'] / max(total_hits, 1) * 100:.1f}% "
             f"({stats['written_without_country']}) | "
             f"dropped_no_ats={stats['dropped_no_ats'] / hosts_n * 100:.1f}% ({stats['dropped_no_ats']}) | "
             f"unreachable={stats['homepage_unreachable'] / hosts_n * 100:.1f}% ({stats['homepage_unreachable']})")
    if total_hits:
        log.info(f"  hit source: homepage={stats['hits_from_homepage'] / total_hits * 100:.1f}% "
                 f"career_path={stats['hits_from_career_path'] / total_hits * 100:.1f}% "
                 f"sitemap={stats['hits_from_sitemap'] / total_hits * 100:.1f}%")
    ats_breakdown = Counter(r["ats"] for r in found_rows)
    if ats_breakdown:
        log.info(f"  by platform: {dict(ats_breakdown.most_common())}")
    country_breakdown = Counter(r["country"] or "unknown" for r in found_rows)
    if country_breakdown:
        log.info(f"  by country (incl. unknown): {dict(country_breakdown.most_common())}")


def main():
    parser = argparse.ArgumentParser(
        description="Host Crawl v2 — 1-partition, follow-links ATS discovery. Country is "
                     "detected and recorded when found, but is NOT a filter — see module docstring.")
    parser.add_argument("--crawl", type=str, default=None,
                         help="Pin a specific Common Crawl partition (e.g. CC-MAIN-2025-18). "
                              "Default: the most recent available.")
    parser.add_argument("--host-limit", type=int, default=_DEFAULT_HOST_LIMIT,
                         help=f"Cap on candidate hosts seeded from the partition (default "
                              f"{_DEFAULT_HOST_LIMIT} — see module docstring for why this is "
                              f"much lower than host_crawl_seed.py's 900K queue-insert cap: "
                              f"real HTTP crawling per host is far more expensive than a queue "
                              f"insert).")
    parser.add_argument("--shard-index", type=int, default=None,
                         help="This job's shard index (0-based) when splitting the seeded host "
                              "list across multiple parallel jobs — modulo sharding.")
    parser.add_argument("--shard-count", type=int, default=None,
                         help="Total number of shards (must be passed together with --shard-index)")
    parser.add_argument("--concurrency", type=int, default=CRAWL_CONCURRENCY,
                         help=f"Hosts crawled in parallel (default {CRAWL_CONCURRENCY})")
    parser.add_argument("--time-budget-minutes", type=int, default=TIME_BUDGET_MINUTES,
                         help=f"Stop gracefully after this many minutes (default {TIME_BUDGET_MINUTES})")
    args = parser.parse_args()

    asyncio.run(run_host_crawl(args.crawl, args.host_limit, args.shard_index, args.shard_count,
                                args.concurrency, args.time_budget_minutes))


if __name__ == "__main__":
    main()
