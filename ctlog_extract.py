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
  bamboohr, workday, icims, rippling, teamtailor
All five are genuinely SUBDOMAIN-per-tenant, which is the one shape CT
logs can extract directly (a certificate covers a hostname, never a URL
path or query string — see WORKDAY_CAVEAT below for the one platform in
this set that still needs an extra step). Greenhouse/Lever/Ashby were
deliberately EXCLUDED from this batch: they're path-based
(boards.greenhouse.io/{slug}), so a %.root_domain CT sweep only ever
returns the platform's own shared hostnames, zero tenant signal — see the
module-level discussion in this project's own history for why a
custom-domain-CNAME angle for those platforms is a fundamentally
different (live-fetch-first, not CT-log-first) technique, better served
by piggybacking on the existing Common Crawl/HTTP Archive sources than by
this pipeline.

WORKDAY_CAVEAT — SOLVED IN THIS VERSION: this project's Workday slug
format is "{company}|{wd_number}|{site_id}" (see discovery.py's
_url_to_slug_workday). A cert only ever proves "{company}.{wd_number}
.myworkdayjobs.com" exists — site_id lives in the URL PATH, which no
certificate carries. Earlier probes (ctlogs_probe.py, ctlogs_probe_direct.py)
reported "119 Workday hosts, 0 usable slugs" and stopped there. THIS
script closes that gap: for every CT-log-confirmed Workday host, it does
one live async HTTP fetch of the bare root
(https://{company}.{wd_number}.myworkdayjobs.com/) and extracts site_id
from the redirect target or the page's own careers-site links (Workday
always redirects a bare tenant root to its default/first career site) —
see resolve_workday_site_id(). Run INLINE in the same async pass, per
explicit direction, not deferred to a later step.

WHY ASYNC/AIOHTTP: crt.sh's own guest Postgres connection is NOT
parallelized here — it's a single psycopg2 connection, one page at a
time, same keyset-pagination approach proven in ctlogs_probe_direct.py.
That part is a hard constraint of the data source (crt.sh's guest role
has shown real pool-exhaustion/session-limit behavior under concurrent
connections in this project's own history), not a design choice, and
matrix-sharding by PLATFORM (see ctlog_extract.yml) is what actually
makes 5 platforms fast — they run as 5 fully independent GitHub Actions
jobs at once, not serialized. What genuinely benefits from asyncio
WITHIN one platform's shard is everything after the SQL page comes back:
resolving thousands of hostnames to slugs is pure CPU (cheap either way),
but Workday's live site_id fetch is exactly the I/O-bound fan-out
aiohttp is built for — hundreds of concurrent HTTPS requests instead of
one at a time. So: sync psycopg2 for the crt.sh sweep, async aiohttp for
resolution/live-fetch and for the Supabase writes themselves (upserting
thousands of rows over HTTP is also I/O-bound).

Usage:
    pip install aiohttp psycopg2-binary python-dotenv
    python ctlog_extract.py --platform bamboohr
    python ctlog_extract.py --platform workday
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
    "workday": "myworkdayjobs.com",
    "icims": "icims.com",
    "rippling": "rippling.com",
    "teamtailor": "teamtailor.com",
}

_HOSTNAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9\-\.]*[a-zA-Z0-9]$")
PAGE_SIZE = 5000
_MAX_PAGES = 500  # backstop, not the normal stopping condition — see
                    # query_certwatch's docstring, same reasoning as
                    # ctlogs_probe_direct.py's identical constant

_CONNECT_RETRIES = 6
_CONNECT_BACKOFF_SECONDS = 15
_QUERY_TIMEOUT_RETRIES = 4
_QUERY_TIMEOUT_BACKOFF_SECONDS = 20

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
WORKDAY_CONCURRENCY = 40  # concurrent live site_id fetches — polite but fast;
                            # confirmed-Workday-host counts are in the low
                            # hundreds at most (per earlier probes), so this
                            # finishes in seconds, not minutes


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


def query_certwatch(root_domain: str) -> set[str]:
    """Fresh connection per page + keyset pagination + retry-on-timeout —
    same proven approach as ctlogs_probe_direct.py's query_certwatch_direct.
    Kept synchronous deliberately (see module docstring)."""
    hosts = set()
    suffix = "." + root_domain
    last_seen = ""
    page = 1
    timeout_retries_left = _QUERY_TIMEOUT_RETRIES

    while page <= _MAX_PAGES:
        conn = connect_with_retry()
        if conn is None:
            log.error(f"  Could not get a connection for page {page} — stopping with {len(hosts)} hosts so far.")
            break
        try:
            with conn.cursor() as cur:
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
            log.error(f"  Page {page} timed out after {_QUERY_TIMEOUT_RETRIES} retries — stopping with {len(hosts)} hosts.")
            conn.close()
            break
        except psycopg2.OperationalError as e:
            log.warning(f"  Page {page} connection error ({e}) — retrying.")
            conn.close()
            continue

        conn.close()
        timeout_retries_left = _QUERY_TIMEOUT_RETRIES

        if not rows:
            log.info(f"  Page {page}: 0 rows — reached the end.")
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
        hosts |= page_hosts

        log.info(f"  Page {page}: {len(rows)} rows -> {len(page_hosts)} hosts ({len(hosts)} total so far)")

        last_seen = rows[-1][0]
        if len(rows) < PAGE_SIZE:
            break
        page += 1
        time.sleep(1)

    return hosts


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
        if ats != "workday" and _looks_like_infra(slug):
            # Workday's slug is "company|wd|site_id" (pipe-delimited, not a
            # bare hostname fragment) — the infra-keyword filter doesn't
            # apply to it the same way and would misfire on legitimate
            # site_ids that happen to contain a filtered token.
            rejected_infra += 1
            continue
        if slug not in out:
            out[slug] = host
    if rejected_infra:
        log.info(f"  filtered out {rejected_infra} likely-infra non-tenant hostnames")
    return out


# ── Workday site_id live-resolve (async — see module docstring) ──────

_WORKDAY_SITE_RE = re.compile(r"/(?:wday/cxs/[^/]+/)?([A-Za-z0-9_\-]+)(?:/job|/?$)")


async def resolve_workday_site_id(session: aiohttp.ClientSession, host: str, sem: asyncio.Semaphore) -> str | None:
    """One host is 'company.wdN.myworkdayjobs.com' — CT logs can prove
    that much exists, but not which career SITE (site_id) it serves.
    Workday redirects a bare tenant root to its default career site
    (e.g. https://acme.wd12.myworkdayjobs.com/ -> .../CentreF), so a
    single GET with redirects followed recovers site_id from the final
    URL's path in the overwhelming majority of cases. Falls back to
    scanning the landing page's own links for a /{site_id}/ pattern if
    the redirect alone doesn't resolve it (some tenants serve content
    directly at the root without redirecting)."""
    url = f"https://{host}/"
    async with sem:
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                                    headers={"User-Agent": USER_AGENT}) as resp:
                final_path = resp.url.path.strip("/")
                if final_path:
                    site_id = final_path.split("/")[0]
                    if site_id:
                        return site_id
                # Fallback: scan the page body for a career-site path.
                try:
                    text = await resp.text(errors="ignore")
                except Exception:
                    return None
                m = _WORKDAY_SITE_RE.search(text)
                if m:
                    return m.group(1)
        except Exception as e:
            log.debug(f"    workday site_id fetch failed for {host}: {e}")
    return None


async def resolve_workday_slugs(hosts: set[str]) -> dict[str, str]:
    """Combines the CT-confirmed host with a live-fetched site_id into
    this project's real 'company|wd|site_id' slug format — the actual
    gap-closing step described in WORKDAY_CAVEAT above."""
    sem = asyncio.Semaphore(WORKDAY_CONCURRENCY)
    out: dict[str, str] = {}

    async with aiohttp.ClientSession() as session:
        tasks = {host: asyncio.create_task(resolve_workday_site_id(session, host, sem)) for host in hosts}
        done = 0
        for host, task in tasks.items():
            site_id = await task
            done += 1
            if done % 50 == 0 or done == len(tasks):
                log.info(f"    workday site_id resolution: {done}/{len(tasks)}")
            if not site_id:
                continue
            parts = host.split(".")
            if len(parts) < 2:
                continue
            company, wd = parts[0], parts[1]
            if not company or not wd:
                continue
            slug = f"{company}|{wd}|{site_id}"
            if slug not in out:
                out[slug] = host

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

async def run_platform(ats: str, root_domain: str) -> None:
    log.info(f"── {ats} ({root_domain}) ──")

    # crt.sh sweep — sync (see module docstring)
    hosts = query_certwatch(root_domain)
    if not hosts:
        log.warning(f"  0 hosts for {ats} — either genuinely nothing found, or every page failed.")
        return
    log.info(f"  {len(hosts)} distinct hostnames extracted")

    if ats == "workday":
        # Inline resolution — per explicit direction, not deferred to a
        # separate step. See WORKDAY_CAVEAT above.
        slugs = await resolve_workday_slugs(hosts)
        log.info(f"  {len(slugs)} slugs resolved with live site_id lookup "
                 f"({len(hosts) - len(slugs)} hosts had no resolvable site_id)")
    else:
        slugs = resolve_slugs(hosts, ats)
        skip_hits = sum(1 for s in slugs if s in SKIP_SLUGS)
        log.info(f"  {len(slugs)} distinct slugs resolved ({skip_hits} would've hit SKIP_SLUGS)")

    if not slugs:
        return

    async with aiohttp.ClientSession() as session:
        existing = await fetch_existing_slug_registry_slugs(session, ats)
        net_new = set(slugs) - existing
        log.info(f"  slug_registry already has {len(existing)} {ats} slugs — {len(net_new)} of this probe's {len(slugs)} are NET-NEW")

        await write_to_staging_table(session, ats, root_domain, slugs)


async def main_async(platform: str) -> None:
    root_domain = PLATFORMS[platform]
    await run_platform(platform, root_domain)


def main():
    parser = argparse.ArgumentParser(description="CT-logs extraction (async) — one ATS platform per run")
    parser.add_argument("--platform", choices=list(PLATFORMS.keys()), required=True)
    args = parser.parse_args()
    asyncio.run(main_async(args.platform))


if __name__ == "__main__":
    main()
