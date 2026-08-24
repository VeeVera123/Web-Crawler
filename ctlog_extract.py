"""
CT-LOGS EXTRACTION — GitHub Actions production version (2026-08), replacing
the old standalone ctlogs_probe.py / ctlogs_icims_diagnostic.py (both
deleted) and ctlogs_probe_direct.py's manual/local BambooHR-only run.

PHASE 1 of the CT-logs pipeline: sweep crt.sh for one ATS platform's
candidate hostnames, resolve them to slugs, and write NET-NEW candidates
into the ctlog_probe_results staging table. Phase 2 (verification — hit
each candidate's live board and drop dead/redirected ones) is a SEPARATE
workflow (ctlog_verify.yml / ctlog_verify_async.py) per explicit
direction, so a slow or flaky verification pass never blocks or reruns
extraction, and vice versa.

PLATFORMS THIS RUN (chosen deliberately — see track_list.md and the
in-chat platform classification worked out before this script existed):
  bamboohr, icims, rippling, teamtailor
All four are genuinely SUBDOMAIN-per-tenant, which is the one shape CT
logs can extract directly (a certificate covers a hostname, never a URL
path or query string). Greenhouse/Lever/Ashby were deliberately EXCLUDED
from this batch: they're path-based (boards.greenhouse.io/{slug}), so a
%.root_domain CT sweep only ever returns the platform's own shared
hostnames, zero tenant signal — a custom-domain-CNAME angle for those
platforms is a real possibility but a fundamentally different
(live-fetch-first, needs a company-name-to-domain mapping this project
doesn't have yet) technique — separate follow-up, not part of this
pipeline.

WORKDAY — TRIED, ABANDONED (2026-08): this project's Workday slug format
is "{company}|{wd_number}|{site_id}" (see discovery.py's
_url_to_slug_workday), and a cert only ever proves
"{company}.{wd_number}.myworkdayjobs.com" exists — site_id lives in the
URL path, which no certificate carries. A live-resolve step (fetching
each confirmed host to recover site_id) was built and tried, but two live
runs confirmed Workday just isn't a viable CT-logs source at all: crt.sh
only ever surfaced ~115-123 hosts total for myworkdayjobs.com, and over
90% of THOSE were Workday's own internal infrastructure (staging/DR/
implementation-ops subdomains — wd117, dr-cp2-wd12, stgprod-wd500, etc,
confirmed non-public via DNS failure), not customer tenants. What was
left after filtering infra and resolving site_id was single-digit slug
counts, all already known. Removed from this pipeline entirely rather
than kept as permanent near-zero-yield dead weight.

WHY ASYNC/AIOHTTP: crt.sh's own guest Postgres connection is NOT
parallelized here — it's a single psycopg2 connection, one page at a
time, same keyset-pagination approach proven in ctlogs_probe_direct.py.
That part is a hard constraint of the data source (crt.sh's guest role
has shown real pool-exhaustion/session-limit behavior under concurrent
connections in this project's own history), not a design choice, and
matrix-sharding by PLATFORM (see ctlog_extract.yml) is what actually
makes multiple platforms fast — they run as fully independent GitHub
Actions jobs at once, not serialized. What genuinely benefits from
asyncio WITHIN one platform's shard is everything after the SQL page
comes back — resolving hostnames to slugs and writing to Supabase are
both handled per-page (see run_platform), so a platform's results land
in ctlog_probe_results incrementally as crt.sh pages complete, not only
once the entire sweep finishes.

Usage:
    pip install aiohttp psycopg2-binary python-dotenv
    python ctlog_extract.py --platform bamboohr
    python ctlog_extract.py --platform icims
    python ctlog_extract.py --platform rippling
    python ctlog_extract.py --platform teamtailor
"""
import argparse
import asyncio
import logging
import os
import re
import sys
import time

import aiohttp
import psycopg2
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from discovery import URL_TO_SLUG, SKIP_SLUGS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("ctlog_extract")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

CRT_SH_DSN = "postgresql://guest@crt.sh:5432/certwatch"

PLATFORMS = {
    "bamboohr": "bamboohr.com",
    "icims": "icims.com",
    "rippling": "rippling.com",
    "teamtailor": "teamtailor.com",
}

_HOSTNAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9\-\.]*[a-zA-Z0-9]$")
PAGE_SIZE = 5000
_MAX_PAGES = 500  # backstop, not the normal stopping condition — see
                    # query_certwatch's docstring, same reasoning as
                    # ctlogs_probe_direct.py's identical constant

# Shortened from 6/15s (worst case ~8min single sleep, ~15min total) —
# per explicit direction that a stuck/cancelled run needs to actually
# resolve quickly, not just "eventually". A long exponential backoff
# made sense when this was a one-shot local script you'd just wait out;
# in a GitHub Actions matrix job you want to CANCEL, a multi-minute
# blocking time.sleep() means Cancel workflow can look hung even though
# it's technically going to stop once the sleep finishes — same actual
# stopping behavior, just capped so the wait is never more than ~40s on
# a single retry and ~2min total worst case.
_CONNECT_RETRIES = 4
_CONNECT_BACKOFF_SECONDS = 5     # 5s, 10s, 20s, 40s ≈ 75s total worst case
_QUERY_TIMEOUT_RETRIES = 3
_QUERY_TIMEOUT_BACKOFF_SECONDS = 8   # 8s, 16s, 24s ≈ 48s total worst case

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


# ── infra-noise filter — carried over verbatim from ctlogs_probe_direct.py ──
_INFRA_KEYWORDS = {
    "status", "api", "cdn", "s3", "blog", "staging", "stage", "jenkins",
    "argocd", "internal", "test", "dev", "admin", "vpn", "mail", "docs",
    "help", "support", "app", "apps", "static", "assets", "images", "img",
    "cache", "edge", "ci", "build", "deploy", "monitor", "grafana",
    "prometheus", "sandbox", "demo", "beta", "alpha", "preview",
    "onboarding", "integration", "perform",
}

def _looks_like_infra(slug: str) -> bool:
    if "." in slug:
        return True
    lowered = slug.lower()
    tokens = re.split(r"[-_]", lowered)
    if lowered in _INFRA_KEYWORDS:
        return True
    return any(tok in _INFRA_KEYWORDS for tok in tokens)


# ── crt.sh sweep (sync — see module docstring for why) ──────────────

def connect_with_retry():
    last_err = None
    for attempt in range(1, _CONNECT_RETRIES + 1):
        try:
            conn = psycopg2.connect(CRT_SH_DSN, connect_timeout=30)
            conn.set_session(readonly=True, autocommit=True)
            return conn
        except Exception as e:
            last_err = e
            if attempt < _CONNECT_RETRIES:
                wait = _CONNECT_BACKOFF_SECONDS * (2 ** (attempt - 1))
                log.warning(f"  Connect attempt {attempt}/{_CONNECT_RETRIES} failed ({e}) — retrying in {wait}s.")
                time.sleep(wait)
    log.error(f"  Could not (re)connect after {_CONNECT_RETRIES} attempts: {last_err}")
    return None


def query_certwatch_pages(root_domain: str, shard_start: str = "", shard_end: str | None = None):
    """Fresh connection per page + keyset pagination + retry-on-timeout —
    same proven approach as ctlogs_probe_direct.py's query_certwatch_direct.
    Kept synchronous deliberately (see module docstring).

    A GENERATOR yielding each page's host set as soon as it's fetched,
    rather than returning one combined set at the very end — this is what
    lets run_platform() resolve+write each page to Supabase as it comes
    in, instead of waiting for the whole platform's crt.sh sweep (which
    can be many pages) to finish first. Per explicit direction: a run
    that gets interrupted partway through a big platform should still
    have written everything found so far, not lost it all waiting on a
    final write that never happens.

    shard_start/shard_end bound the keyset scan to a slice of the
    alphabetically-sorted NAME_VALUE range — e.g. shard_start="",
    shard_end="f" covers everything before "f", letting SEVERAL GitHub
    Actions jobs page through ONE platform's crt.sh results in parallel
    instead of one sequential job per platform (see ctlog_extract.yml's
    HOSTNAME_SHARDS and the --shard-start/--shard-end CLI flags below).
    This is genuinely parallelizable because the query is already
    ORDER BY NAME_VALUE — each shard just keyset-paginates its own slice
    of that same sorted order, with no coordination needed between
    shards (unlike, say, OFFSET-based paging, which can't be sliced this
    way without re-scanning). shard_end=None means "no upper bound",
    i.e. this shard runs to the real end of the platform's data."""
    suffix = "." + root_domain
    last_seen = shard_start
    page = 1
    timeout_retries_left = _QUERY_TIMEOUT_RETRIES

    while page <= _MAX_PAGES:
        conn = connect_with_retry()
        if conn is None:
            log.error(f"  Could not get a connection for page {page} — stopping this shard.")
            break
        try:
            with conn.cursor() as cur:
                if shard_end is not None:
                    cur.execute(
                        """
                        SELECT DISTINCT NAME_VALUE
                        FROM certificate_and_identities cai
                        WHERE plainto_tsquery('certwatch', %s) @@ identities(cai.CERTIFICATE)
                          AND cai.NAME_VALUE ILIKE %s
                          AND cai.NAME_VALUE > %s
                          AND cai.NAME_VALUE < %s
                        ORDER BY NAME_VALUE
                        LIMIT %s
                        """,
                        (root_domain, f"%{suffix}", last_seen, shard_end, PAGE_SIZE),
                    )
                else:
                    cur.execute(
                        """
                        SELECT DISTINCT NAME_VALUE
                        FROM certificate_and_identities cai
                        WHERE plainto_tsquery('certwatch', %s) @@ identities(cai.CERTIFICATE)
                          AND cai.NAME_VALUE ILIKE %s
                          AND cai.NAME_VALUE > %s
                        ORDER BY NAME_VALUE
                        LIMIT %s
                        """,
                        (root_domain, f"%{suffix}", last_seen, PAGE_SIZE),
                    )
                rows = cur.fetchall()
        except psycopg2.errors.QueryCanceled:
            conn.rollback()
            if timeout_retries_left > 0:
                timeout_retries_left -= 1
                wait = _QUERY_TIMEOUT_BACKOFF_SECONDS * (_QUERY_TIMEOUT_RETRIES - timeout_retries_left)
                log.warning(f"  Page {page} timed out ({timeout_retries_left} retries left) — retrying in {wait}s.")
                conn.close()
                time.sleep(wait)
                continue
            log.error(f"  Page {page} timed out after {_QUERY_TIMEOUT_RETRIES} retries — stopping this shard.")
            conn.close()
            break
        except psycopg2.OperationalError as e:
            log.warning(f"  Page {page} connection error ({e}) — retrying.")
            conn.close()
            continue

        conn.close()
        timeout_retries_left = _QUERY_TIMEOUT_RETRIES

        if not rows:
            log.info(f"  Page {page}: 0 rows — reached the end of this shard's range.")
            break

        page_hosts = set()
        for (name_value,) in rows:
            if not name_value:
                continue
            name = name_value.strip().lower()
            if name.startswith("*."):
                continue
            if not (name.endswith(suffix) or name == root_domain):
                continue
            if not _HOSTNAME_RE.match(name):
                continue
            page_hosts.add(name)

        log.info(f"  Page {page}: {len(rows)} rows -> {len(page_hosts)} hosts this page")
        yield page_hosts

        last_seen = rows[-1][0]
        if len(rows) < PAGE_SIZE:
            break
        page += 1
        time.sleep(1)


# ── slug resolution ──────────────────────────────────────────

def resolve_slugs(hosts: set[str], ats: str) -> dict[str, str]:
    resolver = URL_TO_SLUG.get(ats)
    if not resolver:
        log.error(f"No URL_TO_SLUG resolver registered for '{ats}'")
        return {}
    out = {}
    rejected_infra = 0
    for host in hosts:
        slug = resolver(f"https://{host}/")
        if not slug:
            continue
        if _looks_like_infra(slug):
            rejected_infra += 1
            continue
        if slug not in out:
            out[slug] = host
    if rejected_infra:
        log.info(f"  filtered out {rejected_infra} likely-infra non-tenant hostnames")
    return out



# ── Supabase I/O (async) ──────────────────────────────────────

async def fetch_existing_slug_registry_slugs(session: aiohttp.ClientSession, ats: str) -> set[str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("  SUPABASE_URL/SUPABASE_KEY not set — skipping net-new check.")
        return set()
    all_slugs = set()
    page_size = 1000
    offset = 0
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    while True:
        headers["Range"] = f"{offset}-{offset + page_size - 1}"
        try:
            async with session.get(
                f"{SUPABASE_URL}/rest/v1/slug_registry",
                headers=headers,
                params={"ats": f"eq.{ats}", "select": "slug"},
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                r.raise_for_status()
                batch = await r.json()
        except Exception as e:
            log.warning(f"  slug_registry lookup failed at offset {offset}: {e}")
            break
        if not batch:
            break
        all_slugs.update(row["slug"] for row in batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_slugs


async def write_to_staging_table(session: aiohttp.ClientSession, ats: str, root_domain: str,
                                  slugs: dict[str, str]) -> int:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("  SUPABASE_URL/SUPABASE_KEY not set — cannot write to staging table.")
        return 0
    rows = [
        {"ats": ats, "slug": slug, "source_hostname": host, "root_domain": root_domain}
        for slug, host in slugs.items()
    ]
    written = 0
    chunk_size = 1000
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "resolution=merge-duplicates",
    }
    # Chunks written CONCURRENTLY (asyncio.gather), not one-at-a-time —
    # this is the other real async win: tens of thousands of rows as N
    # simultaneous POSTs instead of a serial loop.
    async def _write_chunk(chunk):
        try:
            async with session.post(
                f"{SUPABASE_URL}/rest/v1/ctlog_probe_results",
                headers=headers,
                params={"on_conflict": "ats,slug"},
                json=chunk,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                r.raise_for_status()
                return len(chunk)
        except Exception as e:
            log.error(f"  Failed to write a chunk to staging table: {e}")
            return 0

    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]
    results = await asyncio.gather(*(_write_chunk(c) for c in chunks))
    written = sum(results)
    log.info(f"  Wrote {written}/{len(rows)} rows to ctlog_probe_results")
    return written


# ── orchestration ──────────────────────────────────────────

async def run_platform(ats: str, root_domain: str, shard_start: str = "", shard_end: str | None = None,
                        shard_label: str = "") -> None:
    """Resolves + writes EACH crt.sh page to Supabase as soon as that
    page comes back, instead of collecting every page for the whole
    platform first and writing once at the very end — a run that gets
    interrupted partway through (or just takes a long time on a big
    platform) still has everything found so far safely written, per
    explicit direction. query_certwatch_pages is a sync generator (see
    its docstring for why crt.sh querying itself stays sync); this
    function drives it from the async side, awaiting a write after each
    page before pulling the next one.

    shard_start/shard_end/shard_label: this platform's slice of the
    alphabetically-sorted hostname range, when run as one of several
    parallel GitHub Actions jobs covering the SAME platform (see
    ctlog_extract.yml's hostname-sharding and query_certwatch_pages'
    docstring for why this is safe to parallelize). Defaults cover the
    platform's FULL range (no sharding) for local/manual runs."""
    label = f" [shard {shard_label}]" if shard_label else ""
    log.info(f"── {ats} ({root_domain}){label} ──")

    total_hosts = 0
    total_slugs = 0
    total_net_new = 0
    saw_any_page = False

    async with aiohttp.ClientSession() as session:
        existing = await fetch_existing_slug_registry_slugs(session, ats)
        log.info(f"  slug_registry already has {len(existing)} {ats} slugs")

        for page_hosts in query_certwatch_pages(root_domain, shard_start, shard_end):
            saw_any_page = True
            if not page_hosts:
                continue
            total_hosts += len(page_hosts)

            slugs = resolve_slugs(page_hosts, ats)
            if not slugs:
                continue
            total_slugs += len(slugs)

            net_new = set(slugs) - existing
            total_net_new += len(net_new)
            existing |= set(slugs)  # so a slug seen on page 2 that also
                                     # appeared on page 1 isn't double-
                                     # counted as net-new twice

            await write_to_staging_table(session, ats, root_domain, slugs)

    if not saw_any_page:
        log.warning(f"  0 hosts for {ats}{label} — either genuinely nothing found in this range, or every page failed.")
        return

    log.info(f"  TOTAL{label}: {total_hosts} hostnames -> {total_slugs} slugs "
             f"({total_net_new} net-new vs slug_registry) written across all pages")


# Default alphabet split for --shard-count N (over the platform's raw
# hostname range, which always starts with a lowercase letter or digit —
# see _HOSTNAME_RE). Evenly divides a-z0-9 into N contiguous ranges so
# each shard's slice is roughly comparable in size (crt.sh hostnames
# aren't perfectly uniform across the alphabet, but this is a reasonable
# default absent real per-letter distribution data).
_SHARD_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"


def _shard_bounds(shard_index: int, shard_count: int) -> tuple[str, str | None]:
    """Returns (start, end) NAME_VALUE bounds for shard_index of
    shard_count total shards, splitting _SHARD_ALPHABET's first-character
    range evenly. The last shard's end is None (no upper bound — covers
    everything through the real end of data), so shard_count doesn't need
    to evenly divide len(_SHARD_ALPHABET)."""
    n = len(_SHARD_ALPHABET)
    chunk = max(1, n // shard_count)
    start_idx = shard_index * chunk
    end_idx = start_idx + chunk
    start = _SHARD_ALPHABET[start_idx] if start_idx < n else _SHARD_ALPHABET[-1]
    if shard_index == shard_count - 1 or end_idx >= n:
        end = None
    else:
        end = _SHARD_ALPHABET[end_idx]
    # shard 0 starts at "" (covers everything before/including the first
    # letter), not at _SHARD_ALPHABET[0] — NAME_VALUE > "" matches
    # anything, same as the unsharded default.
    return ("" if shard_index == 0 else start), end


async def main_async(platform: str, shard_index: int | None, shard_count: int | None) -> None:
    root_domain = PLATFORMS[platform]
    if shard_index is not None and shard_count is not None:
        start, end = _shard_bounds(shard_index, shard_count)
        label = f"{shard_index}/{shard_count} ({start or '(start)'}–{end or '(end)'})"
        await run_platform(platform, root_domain, shard_start=start, shard_end=end, shard_label=label)
    else:
        await run_platform(platform, root_domain)


def main():
    parser = argparse.ArgumentParser(description="CT-logs extraction (async) — one ATS platform per run")
    parser.add_argument("--platform", choices=list(PLATFORMS.keys()), required=True)
    parser.add_argument("--shard-index", type=int, default=None,
                         help="This job's shard index (0-based) when splitting one platform's "
                              "hostname range across multiple parallel GitHub Actions jobs")
    parser.add_argument("--shard-count", type=int, default=None,
                         help="Total number of shards splitting this platform's hostname range "
                              "(must be passed together with --shard-index)")
    args = parser.parse_args()
    if (args.shard_index is None) != (args.shard_count is None):
        parser.error("--shard-index and --shard-count must be passed together")
    asyncio.run(main_async(args.platform, args.shard_index, args.shard_count))


if __name__ == "__main__":
    main()
