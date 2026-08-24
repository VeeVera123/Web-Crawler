"""
Tranco Crawl — Bulk Async Discovery
=====================================
Standalone, from-scratch web crawl: visit candidate hostnames seeded from
the Tranco top-sites list (via tranco_seed.py) and look for a link to a
known ATS platform, using the same detection logic every other source in
this project already uses (URL_TO_SLUG from discovery.py).

This is a direct port of the (retired) host_crawl.py — see ./host crawl/
— onto the new Tranco-sourced tables (t_seeding / t_slugs /
tranco_claim_batch) instead of the old Common Crawl Host Index ones
(h_seeding / h_slugs / host_crawl_claim_batch). The crawl mechanics
(async fetch, robots.txt handling, ATS link scan, checkpointing) are
carried over unchanged — what changed is only the source of hosts. See
tranco_seed.py's module docstring for why Tranco replaced the Common
Crawl Host Index as the seed source.

REACH STRATEGY (2026-08 update — see _crawl_one's docstring for the full
per-host decision tree): the original one-hop "find a careers-looking
link on the homepage" heuristic missed a large share of real ATS-using
companies, because most sites don't put a raw careers link on the
homepage itself, and a growing number inject their nav/footer (including
the careers link) via client-side JavaScript that a plain HTTP GET never
sees — aiohttp+selectolax fetch and parse raw HTML only, no JS execution,
so a link that only exists after the page's own JS runs is invisible to
this crawler. Rather than reach for a full headless browser (Playwright/
Puppeteer/Selenium) — researched and deliberately rejected as the
default path here: every credible real-world project that does this at
scale (Apify's Crawlee AdaptivePlaywrightCrawler, scrapy-playwright)
treats a real browser as an expensive escalation tier, not a default,
specifically because of the CPU/memory cost and context-leak fragility
of running thousands of browser contexts unattended in CI — this crawler
instead adds three cheap, no-JS-required techniques that catch most of
what the one-hop heuristic was missing:
  1. Direct URL guessing (_CAREER_PATH_GUESSES) — probe a fixed list of
     high-probability career-page paths (/careers, /jobs, /about/careers,
     etc.) directly, instead of only relying on finding a link. Catches
     sites where the careers page exists at a normal, static, guessable
     URL even though its nav link is JS-injected.
  2. sitemap.xml, discovered via robots.txt's `Sitemap:` directive (or
     the conventional /sitemap.xml fallback) — sitemaps are frequently
     generated server-side independent of the client-rendered nav chrome,
     so a careers URL often shows up there even when it's nowhere in the
     rendered homepage HTML at all.
  3. Two hops instead of one — the homepage, PLUS every candidate career
     URL found via (1)/(2)/the original link-heuristic, not just a single
     followed link.
A genuine headless-browser fallback tier (for the residual hosts that
still come back no_match after all of the above) is a reasonable future
addition but deliberately NOT implemented here — see the research notes
in this project's conversation history for the cost/complexity tradeoff
that motivated deferring it.

WHY ASYNC, NOT THREADS: every other live-fetch source in this project
(YC, HTTP Archive, WDC's live-resolve fallback) uses requests +
ThreadPoolExecutor, capping out at a few dozen concurrent connections
before thread-switching overhead dominates. This module is built around
aiohttp + asyncio instead specifically because the target scale here
(hundreds of thousands of hosts per run) is bottlenecked entirely on
"waiting on network," not CPU — a single process holding thousands of
in-flight connections is the only way to make a meaningful dent in a
6-hour window. selectolax (Lexbor/Modest C engine) is used for link
extraction instead of BeautifulSoup for the same reason at a smaller
scale: less CPU spent per page means less time stealing from the event
loop between awaits.

CHECKPOINTING: this is what makes repeated runs valuable instead of
wasteful. Every host this script attempts has its outcome (found /
no_match / unreachable / disallowed / http_error) written back onto its
existing t_seeding row immediately — a future run's queue query
(_claim_batch(), via outcome IS NULL) excludes anything already
resolved, so re-running this on a schedule keeps expanding coverage
instead of re-crawling the same slice every time. See _claim_batch() /
_flush_results().

TIME BUDGET: GitHub Actions' free-tier hard cap is 6 hours per job. This
script tracks its own elapsed time and stops claiming new batches with
enough headroom left to flush whatever it's found to Supabase before the
job gets killed — see MAX_RUNTIME_SECONDS / FLUSH_BUFFER_SECONDS. Results
are also flushed incrementally (not just at the end) so a hard timeout or
crash mid-run still keeps whatever was found up to that point.

Usage:
    python tranco_crawl.py                          # single shard, all queue
    python tranco_crawl.py --shard 0 --total-shards 8
    python tranco_crawl.py --max-runtime-seconds 300  # quick test run
    python tranco_crawl.py --dry-run
"""

import argparse
import asyncio
import logging
import os
import sys
import time
from urllib.parse import urljoin, urlparse

import aiohttp
from selectolax.lexbor import LexborHTMLParser
from dotenv import load_dotenv

# Reuse the SAME slug-detection logic every other source in this project
# uses — no reason to duplicate 30-platform URL parsing here.
from discovery import URL_TO_SLUG, SKIP_SLUGS

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

USER_AGENT = "ATS-Global-Scanner-TrancoCrawl/1.0"

# GitHub Actions free-tier hard cap is 6 hours (21600s). We stop claiming
# new batches at MAX_RUNTIME_SECONDS, leaving FLUSH_BUFFER_SECONDS of
# headroom to write out in-flight results before the job would be killed
# — a hard kill mid-batch loses whatever wasn't flushed yet, so this
# buffer is the difference between "graceful stop" and "silent data loss
# on the last batch."
MAX_RUNTIME_SECONDS = 5.5 * 3600
FLUSH_BUFFER_SECONDS = 5 * 60

# How many hosts to claim from the queue per batch. Each is one
# concurrent connection slot; batch size also bounds how much can be lost
# if the process dies mid-batch before the next checkpoint flush.
#
# REQUEST-COUNT TRADEOFF (2026-08, from adding the reach techniques —
# see module docstring): a host that has NO ATS match anywhere now costs
# up to ~14 requests (robots.txt + homepage + sitemap.xml + 11 guessed
# paths) instead of the original ~2-3 (robots.txt + homepage + maybe one
# link-hop). A host WITH a match usually short-circuits much earlier
# (first hit wins, see _crawl_one), so this worst case only applies to
# genuine no_match hosts — but since most Tranco-ranked domains ARE
# no_match (see the ROI discussion in this project's history — most
# top-ranked domains are infra/CDN/ad-tech with no careers page at all),
# expect overall request volume and wall-clock per batch to land roughly
# 5-7x higher than before this change. If a run's throughput becomes a
# real constraint, the first things to reconsider are trimming
# _CAREER_PATH_GUESSES to the highest-yield paths, or dropping the
# sitemap fetch — both are cheap, isolated changes.
BATCH_SIZE = 2000
MAX_CONCURRENCY = 500  # concurrent in-flight requests within a batch

FETCH_TIMEOUT = 10  # seconds — generous but bounded; a hung connection
                     # at MAX_CONCURRENCY scale can't be allowed to block
                     # the whole batch

# Career-page link heuristic, same pattern as discovery.py's YC resolver.
import re
_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.I)
_CAREER_LINK_RE = re.compile(
    r"\b(careers?|jobs?|join[\s\-]?us|we[\s\-]?re[\s\-]?hiring|work[\s\-]?with[\s\-]?us)\b",
    re.I,
)

# Direct path-guessing (reach technique #1 — see module docstring). Tried
# concurrently against every host regardless of whether a link was found
# on the homepage, since a JS-injected nav can hide an otherwise perfectly
# normal, static, server-rendered careers page. Ordered roughly by
# observed real-world frequency (plain /careers and /jobs cover the large
# majority of sites that have one at all) so _crawl_one's short-circuit
# (stop at the first 2xx hit) tends to spend the fewest extra requests.
_CAREER_PATH_GUESSES = [
    "/careers", "/jobs", "/careers/", "/jobs/",
    "/about/careers", "/company/careers", "/about-us/careers",
    "/join-us", "/work-with-us", "/en/careers", "/company/jobs",
]

# Sitemap discovery (reach technique #2). robots.txt's Sitemap: directive
# is the authoritative pointer when present; /sitemap.xml is the
# conventional fallback location most sites use even without declaring it
# in robots.txt. Only the FIRST sitemap is fetched per host (a sitemap
# index can point to many child sitemaps — chasing all of them would blow
# the per-host request budget for a technique that's already a secondary
# signal, not the primary detection path).
_SITEMAP_DIRECTIVE_RE = re.compile(r"^\s*sitemap:\s*(\S+)", re.I | re.M)
_CAREER_URL_IN_TEXT_RE = re.compile(
    r"https?://[^\s<>\"']*(?:careers?|jobs?)[^\s<>\"']*", re.I
)
_SITEMAP_FETCH_TIMEOUT = 8  # keep this tighter than FETCH_TIMEOUT — a
                             # slow/huge sitemap.xml is a secondary signal
                             # and shouldn't be allowed to dominate a
                             # host's total time budget

# ── Per-run stats (detailed but summarized once, not per-host noise) ──
class Stats:
    def __init__(self):
        self.attempted = 0
        self.found = 0
        self.no_match = 0
        self.unreachable = 0
        self.disallowed = 0
        self.http_error = 0
        self.by_ats: dict[str, int] = {}
        self.robots_cache_hits = 0
        self.robots_fetches = 0
        self.flush_failures = 0  # batches whose flush failed even after
                                  # _FLUSH_RETRIES retries — those hosts'
                                  # results were NOT recorded this run;
                                  # they'll auto-release (30min staleness)
                                  # and get re-claimed later, but this
                                  # counter makes the loss visible in the
                                  # run summary instead of silent.

    def summary(self) -> str:
        top_ats = sorted(self.by_ats.items(), key=lambda kv: -kv[1])[:10]
        ats_line = ", ".join(f"{a}:{c}" for a, c in top_ats) or "none"
        return (
            f"attempted={self.attempted} found={self.found} "
            f"no_match={self.no_match} unreachable={self.unreachable} "
            f"disallowed={self.disallowed} http_error={self.http_error} | "
            f"top ATS hits: {ats_line} | "
            f"robots.txt cache hit rate: "
            f"{self.robots_cache_hits}/{self.robots_cache_hits + self.robots_fetches}"
            f"{' | flush_failures=' + str(self.flush_failures) if self.flush_failures else ''}"
        )


stats = Stats()

# robots.txt results cached per-domain for the lifetime of the process —
# at this scale, re-fetching robots.txt once per host on the SAME domain
# (rare, but happens with subdomains) would double request volume for no
# benefit. Bounded by BATCH_SIZE turnover; not persisted across runs
# since robots.txt can legitimately change. Cache now stores the raw text
# too (was just (allowed, reason) before) so callers can pull the
# Sitemap: directive out of it without a second fetch.
_robots_cache: dict[str, tuple[bool, str, str]] = {}


async def _robots_allows(session: aiohttp.ClientSession, base_url: str) -> tuple[bool, str, str]:
    """Live robots.txt check, cached per-domain within this process.
    Returns (allowed, reason, raw_text) — reason is "ok", "unreachable"
    (DNS/timeout/connection/SSL failure — NOT a real robots.txt rule), or
    "disallowed" (an actual robots.txt rule blocked us). raw_text is the
    robots.txt body if one was fetched (empty string otherwise) — used by
    _extract_sitemap_url for reach technique #2, see module docstring.
    Only a real "disallowed" rule should ever be recorded as
    outcome='disallowed' in t_seeding — collapsing "the site was
    unreachable" into the same bucket as "the site explicitly disallows
    crawling" made the retired host-crawl pipeline's results table
    useless for telling those two very different situations apart (see
    host_crawl.py's original version of this docstring for the full
    history). Matches discovery.py's _robots_allows behavior: no
    robots.txt (4xx/5xx) means allowed, not disallowed; only an explicit
    Disallow rule blocks."""
    if base_url in _robots_cache:
        stats.robots_cache_hits += 1
        return _robots_cache[base_url]

    stats.robots_fetches += 1
    result = (False, "unreachable", "")
    try:
        async with session.get(f"{base_url}/robots.txt",
                                timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT)) as r:
            if r.status >= 400:
                # No robots.txt at all is conventionally "allow everything"
                # — matches discovery.py's _robots_allows, and the actual
                # robots.txt spec. This is NOT a failure of any kind.
                result = (True, "ok", "")
            else:
                text = await r.text(errors="replace")
                import urllib.robotparser
                rp = urllib.robotparser.RobotFileParser()
                rp.parse(text.splitlines())
                if rp.can_fetch(USER_AGENT, "/"):
                    result = (True, "ok", text)
                else:
                    result = (False, "disallowed", text)
    except Exception:
        # DNS failure, timeout, connection reset, SSL error, etc. — the
        # site itself is unreachable, this has nothing to do with
        # robots.txt rules and must not be recorded as "disallowed".
        result = (False, "unreachable", "")

    _robots_cache[base_url] = result
    return result


def _extract_sitemap_url(robots_text: str, base_url: str) -> str:
    """Pull the first Sitemap: directive out of a robots.txt body, or
    fall back to the conventional /sitemap.xml location most sites use
    even without declaring it (see module docstring, reach technique
    #2)."""
    if robots_text:
        m = _SITEMAP_DIRECTIVE_RE.search(robots_text)
        if m:
            try:
                return urljoin(base_url, m.group(1).strip())
            except Exception:
                pass
    return f"{base_url}/sitemap.xml"


async def _fetch_sitemap_career_urls(session: aiohttp.ClientSession,
                                      sitemap_url: str) -> list[str]:
    """Fetch one sitemap (XML or plain text — some sites serve a bare
    URL list) and pull out any entries that look like a careers/jobs
    page. Deliberately shallow: does NOT recurse into a sitemap INDEX's
    child sitemaps (a sitemap index can point to dozens of large child
    files — chasing all of them would blow the per-host request budget
    for what's already a secondary signal, not the primary detection
    path). Returns [] on any failure — this is a best-effort bonus
    signal, never a reason to fail the whole host."""
    try:
        async with session.get(
            sitemap_url, timeout=aiohttp.ClientTimeout(total=_SITEMAP_FETCH_TIMEOUT),
            headers={"User-Agent": USER_AGENT},
        ) as r:
            if r.status >= 400:
                return []
            text = await r.text(errors="replace")
    except Exception:
        return []

    urls = set(_CAREER_URL_IN_TEXT_RE.findall(text))
    return list(urls)[:5]  # cap — a sitemap matching many "jobs"-looking
                            # URLs (e.g. a job board's OWN sitemap) could
                            # otherwise blow up this host's request count


def _guess_career_urls(base_url: str) -> list[str]:
    """Reach technique #1 (see module docstring) — fixed list of
    high-probability career-page paths, tried directly rather than
    depending on finding a link on the homepage."""
    return [urljoin(base_url, p) for p in _CAREER_PATH_GUESSES]


def _scan_html_for_ats_slug(html: str, base_url: str) -> tuple[str, str] | None:
    """Parse with selectolax (fast C parser) and check every href against
    every known ATS URL pattern — same detection logic as
    discovery.py's YC/HTTP Archive resolvers, just a faster parser since
    this runs at much higher volume."""
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return None
    for a in tree.css("a[href]"):
        href = a.attributes.get("href")
        if not href:
            continue
        try:
            absolute = urljoin(base_url, href)
        except Exception:
            continue
        for ats, resolver in URL_TO_SLUG.items():
            slug = resolver(absolute)
            if slug:
                return ats, slug
    return None


def _find_career_page_link(html: str, base_url: str) -> str | None:
    for match in _HREF_RE.finditer(html):
        href = match.group(1)
        if _CAREER_LINK_RE.search(href):
            try:
                return urljoin(base_url, href)
            except Exception:
                continue
    return None


async def _try_career_url(session: aiohttp.ClientSession, url: str,
                           home_base: str) -> tuple[str, str] | None:
    """Fetch one candidate career-page URL and scan it for an ATS link.
    Returns (ats, slug) on a hit, None otherwise (covers robots-disallow,
    unreachable, non-2xx, and no-match alike — this is a best-effort
    probe among several candidates, not the primary per-host outcome, so
    it never raises and never distinguishes failure reasons the way
    _crawl_one's own top-level fetch does)."""
    url_host = urlparse(url).hostname or ""
    url_base = f"https://{url_host}"
    if url_base != home_base:
        # Different host than the one we're crawling (e.g. a sitemap
        # entry or guessed path resolved to an external domain) — still
        # respect ITS robots.txt before fetching.
        allowed, _, _ = await _robots_allows(session, url_base)
        if not allowed:
            return None
    try:
        async with session.get(
            url, timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
            headers={"User-Agent": USER_AGENT},
        ) as r:
            if r.status >= 400:
                return None
            html = await r.text(errors="replace")
    except Exception:
        return None
    return _scan_html_for_ats_slug(html, url)


async def _crawl_one(session: aiohttp.ClientSession, host: str) -> dict:
    """Visit one host, return a result dict to be written back onto
    t_seeding (+ t_slugs if a slug was found). Never raises — every
    failure mode is captured as an outcome, not an exception.

    Per-host decision tree (see module docstring for why — the short
    version: most real ATS-using companies don't have a homepage link
    the original one-hop heuristic could find, either because it's
    genuinely deeper in the site or because it's injected by JS this
    crawler can't execute):
      1. robots.txt check on the homepage — unchanged from before.
      2. Fetch the homepage itself, scan its raw HTML for a direct ATS
         link (the original, still-cheapest path — most matches will
         still be found right here).
      3. If no hit, gather EVERY candidate career-page URL from three
         sources at once: (a) the original href-text heuristic, (b) a
         fixed list of guessed common paths, (c) sitemap.xml (via
         robots.txt's Sitemap: directive or the /sitemap.xml fallback).
         All candidates are then fetched CONCURRENTLY and scanned —
         first hit wins, remaining in-flight fetches are simply not
         awaited further (their tasks are still let to finish so the
         event loop doesn't warn about unawaited coroutines, but their
         results are ignored once a hit is found)."""
    base = f"https://{host}"

    allowed, reason, robots_text = await _robots_allows(session, base)
    if not allowed:
        # reason is either "disallowed" (a real robots.txt rule) or
        # "unreachable" (DNS/timeout/connection/SSL failure) — record
        # whichever actually happened instead of conflating both as
        # "disallowed".
        return {"host": host, "outcome": reason}

    try:
        async with session.get(
            base, timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
            headers={"User-Agent": USER_AGENT},
        ) as r:
            if r.status >= 400:
                return {"host": host, "outcome": "http_error"}
            html = await r.text(errors="replace")
    except Exception:
        return {"host": host, "outcome": "unreachable"}

    hit = _scan_html_for_ats_slug(html, base)
    if hit:
        ats, slug = hit
        return {"host": host, "outcome": "found", "ats": ats, "slug": slug}

    # No direct hit on the homepage — gather every candidate career URL
    # from all three reach techniques and check them concurrently.
    candidates: list[str] = []

    link_hit = _find_career_page_link(html, base)
    if link_hit:
        candidates.append(link_hit)

    candidates.extend(_guess_career_urls(base))

    sitemap_url = _extract_sitemap_url(robots_text, base)
    sitemap_urls = await _fetch_sitemap_career_urls(session, sitemap_url)
    candidates.extend(sitemap_urls)

    # De-dupe while preserving order (link heuristic first, since it's
    # the highest-confidence signal — an explicit "careers" link beats a
    # guessed path).
    seen = set()
    ordered_candidates = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            ordered_candidates.append(c)

    if not ordered_candidates:
        return {"host": host, "outcome": "no_match"}

    tasks = [asyncio.ensure_future(_try_career_url(session, c, base))
             for c in ordered_candidates]
    try:
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result:
                ats, slug = result
                # First hit wins — cancel whatever else is still
                # in-flight (the finally block below) so a host with
                # many candidates doesn't hold open extra connections
                # once we already have an answer for it.
                return {"host": host, "outcome": "found", "ats": ats, "slug": slug}
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()

    return {"host": host, "outcome": "no_match"}


# ── Supabase I/O (sync requests, kept simple — this is low-volume
# compared to the crawl itself: one claim + one flush per batch, not
# per-host) ──────────────────────────────────────────────

def _sb_headers(prefer: str | None = None) -> dict:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h


# How many times to retry a FAILED claim call (network error, transient
# 500, statement timeout under concurrent load, etc.) before treating it
# as a real stop condition. This distinction matters a lot: _claim_batch
# returning [] used to mean ONLY ONE thing to run()'s loop — "queue
# exhausted, stop this shard for good" — but a transient failure (a
# dropped connection, a brief Supabase blip, one-off lock contention from
# 16 shards claiming at once) ALSO used to come back as [] after being
# swallowed here, and run() couldn't tell the two apart. A live run
# confirmed this really happened: scattered single 500s throughout an
# otherwise-healthy run (nowhere near the "all 16 shards at once"
# pileup the earlier statement-timeout bug caused) were each enough to
# permanently end whichever shard hit one — most shards showed as
# "completed" within ~4 minutes in the Actions UI while their real
# t_seeding slice was still 95%+ untouched. Retrying a genuine failure a
# few times with backoff, and raising instead of silently returning [],
# means run()'s loop only ever sees an empty result when the RPC itself
# says there's nothing left to claim — a real, meaningful signal again.
_CLAIM_RETRIES = 5
_CLAIM_BACKOFF_SECONDS = 5  # doubles each retry: 5s, 10s, 20s, 40s, 80s


class _ClaimBatchError(Exception):
    """Raised by _claim_batch after exhausting retries on a genuine
    failure — distinct from a normal empty result, which means the queue
    really is exhausted for this shard. run() catches this separately so
    a shard logs and stops on a REAL persistent failure, rather than
    silently mislabeling it as "queue exhausted" the way a bare []
    return used to."""


def _claim_batch(shard: int, total_shards: int, batch_size: int) -> list[dict]:
    """Pull the next batch of not-yet-visited hosts (outcome IS NULL)
    assigned to this shard, from t_seeding — the single queue+outcome
    table (same merged-table design as the retired host-crawl pipeline's
    h_seeding). Sharding is done in SQL by hashing the host, so this
    needs no coordination between concurrently-running shards — each
    shard only ever sees its own slice, so two shards claiming batches at
    the same moment can't double-claim the same host.

    Retries a failed call up to _CLAIM_RETRIES times with backoff before
    raising _ClaimBatchError — see that class's docstring and
    _CLAIM_RETRIES' comment for why this distinction matters. An empty
    list returned normally (no exception) means the RPC call itself
    SUCCEEDED and genuinely found nothing left to claim for this shard —
    the only case that should ever stop run()'s loop early."""
    # Using a Postgres function (tranco_claim_batch) keeps the "next
    # unvisited hosts for this shard" logic atomic and server-side rather
    # than racy client-side pagination — two shards claiming batches at
    # the same instant can't double-claim the same host, since the
    # function's own UPDATE...RETURNING marks rows claimed in the same
    # statement that selects them. Unlike the retired pipeline's
    # host_crawl_claim_batch, this orders by rank ASC (Tranco rank 1 =
    # most popular) instead of a Common-Crawl-derived score DESC.
    import requests
    last_err = None
    for attempt in range(1, _CLAIM_RETRIES + 1):
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/rpc/tranco_claim_batch",
                headers=_sb_headers(),
                json={"p_shard": shard, "p_total_shards": total_shards,
                      "p_batch_size": batch_size},
                timeout=60,
            )
            r.raise_for_status()
            return r.json() or []
        except Exception as e:
            last_err = e
            if attempt < _CLAIM_RETRIES:
                wait = _CLAIM_BACKOFF_SECONDS * (2 ** (attempt - 1))
                log.warning(f"Claim batch attempt {attempt}/{_CLAIM_RETRIES} "
                            f"failed: {e} — retrying in {wait}s.")
                time.sleep(wait)
            else:
                log.error(f"Claim batch failed after {_CLAIM_RETRIES} "
                          f"attempts: {e}")
    raise _ClaimBatchError(str(last_err))


class _FlushError(Exception):
    """Raised by _flush_results after exhausting retries on a genuine
    flush failure. This used to be silently swallowed (log.error and move
    on) — the actual root cause of "not all of them were checked": a
    failed flush POST left that whole batch's hosts stuck at
    claimed_at=<today>/outcome=NULL with NO retry and NO record that
    anything went wrong beyond a log line, while the crawler just claimed
    the next batch and kept going. Confirmed live via t_seeding: every one
    of the 16 shards showed almost exactly half its claimed rows resolved
    and half stuck, in batch-sized (2000-row) chunks, throughout the
    entire run — not a runtime-limit tail cutoff, but a roughly-constant
    flush failure rate compounding batch after batch. Paired with the
    2026-08 DB fix (claimed_at is now a real timestamp and
    tranco_claim_batch releases anything >30min stale regardless of
    calendar day), a batch that fails to flush even after retries here
    will still get automatically released and re-claimed later in the
    SAME run, instead of being an invisible, same-day-unrecoverable
    permanent loss."""


_FLUSH_RETRIES = 5
_FLUSH_BACKOFF_SECONDS = 5  # doubles each retry: 5s, 10s, 20s, 40s, 80s


def _flush_results(visited_rows: list[dict], found_rows: list[dict]):
    """Write this batch's outcomes. visited_rows are UPSERTED into
    t_seeding — this fills in outcome/ats/slug on the SAME row that was
    already there from seeding, rather than inserting into a separate
    "visited" table (matches the retired host-crawl pipeline's
    merged-table design). found_rows go to t_slugs (only actual ATS
    matches, a genuinely different, permanent dataset). on_conflict must
    be passed as a query param alongside the merge-duplicates Prefer
    header — PostgREST silently no-ops the upsert semantics without it
    (see discovery.py's upsert_to_supabase for the same pattern).

    NOTE (2026-08): t_seeding used to also have added_at/checked_at date
    columns — pure informational timestamps nothing in the pipeline read
    back. Dropped to reduce dead weight; claimed_at stays, since
    tranco_claim_batch's queue logic genuinely depends on it.

    Retries each POST up to _FLUSH_RETRIES times with backoff (same
    pattern as _claim_batch) before raising _FlushError — see that
    class's docstring for why a bare log-and-continue was actually the
    root cause of incomplete runs. Raises on either visited_rows or
    found_rows failing after exhausting retries; caller (run()) decides
    what to do about it."""
    import requests
    errors = []

    if visited_rows:
        last_err = None
        for attempt in range(1, _FLUSH_RETRIES + 1):
            try:
                r = requests.post(
                    f"{SUPABASE_URL}/rest/v1/t_seeding",
                    headers=_sb_headers("resolution=merge-duplicates"),
                    json=visited_rows, timeout=60,
                    params={"on_conflict": "host"},
                )
                r.raise_for_status()
                last_err = None
                break
            except Exception as e:
                last_err = e
                if attempt < _FLUSH_RETRIES:
                    wait = _FLUSH_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    log.warning(f"Flush of {len(visited_rows)} visited rows "
                                f"attempt {attempt}/{_FLUSH_RETRIES} failed: "
                                f"{e} — retrying in {wait}s.")
                    time.sleep(wait)
        if last_err is not None:
            log.error(f"Failed to flush {len(visited_rows)} visited rows "
                      f"after {_FLUSH_RETRIES} attempts: {last_err}")
            errors.append(f"visited_rows: {last_err}")

    if found_rows:
        last_err = None
        for attempt in range(1, _FLUSH_RETRIES + 1):
            try:
                r = requests.post(
                    f"{SUPABASE_URL}/rest/v1/t_slugs",
                    headers=_sb_headers("resolution=merge-duplicates"),
                    json=found_rows, timeout=60,
                    params={"on_conflict": "ats,slug"},
                )
                r.raise_for_status()
                last_err = None
                break
            except Exception as e:
                last_err = e
                if attempt < _FLUSH_RETRIES:
                    wait = _FLUSH_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    log.warning(f"Flush of {len(found_rows)} found rows "
                                f"attempt {attempt}/{_FLUSH_RETRIES} failed: "
                                f"{e} — retrying in {wait}s.")
                    time.sleep(wait)
        if last_err is not None:
            log.error(f"Failed to flush {len(found_rows)} found rows "
                      f"after {_FLUSH_RETRIES} attempts: {last_err}")
            errors.append(f"found_rows: {last_err}")

    if errors:
        raise _FlushError("; ".join(errors))


async def _run_batch(hosts: list[str]) -> list[dict]:
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(MAX_CONCURRENCY)

        async def _bounded(host):
            async with sem:
                return await _crawl_one(session, host)

        return await asyncio.gather(*[_bounded(h) for h in hosts])


def run(shard: int, total_shards: int, max_runtime: float, dry_run: bool):
    start = time.monotonic()
    deadline = start + max_runtime - FLUSH_BUFFER_SECONDS

    log.info(f"Tranco crawl starting — shard {shard}/{total_shards}, "
             f"max_runtime={max_runtime:.0f}s, batch_size={BATCH_SIZE}, "
             f"max_concurrency={MAX_CONCURRENCY}")

    batch_num = 0
    consecutive_claim_failures = 0
    # Cap on CONSECUTIVE genuine claim failures (each already having
    # survived _CLAIM_RETRIES internal retries) before this shard truly
    # gives up. Distinct from _CLAIM_RETRIES: that's short-backoff retry
    # WITHIN one claim attempt (network blip, one-off lock contention);
    # this is a much rarer outer safety net for "the failures keep
    # happening even across multiple fully-retried attempts," which
    # would suggest something more persistent (e.g. Supabase itself
    # down) rather than ordinary transient noise. A successful claim
    # anywhere resets this counter back to 0.
    _MAX_CONSECUTIVE_CLAIM_FAILURES = 5

    consecutive_flush_failures = 0
    # Same idea as _MAX_CONSECUTIVE_CLAIM_FAILURES but for flushes: each
    # failure here has already survived _FLUSH_RETRIES internal retries,
    # so several of THOSE in a row means something persistent (Supabase
    # down/degraded), not ordinary transient noise. Unlike a claim
    # failure, a flush failure doesn't lose data permanently on its own —
    # the DB-side 30-minute staleness release means that batch's hosts
    # get automatically un-stuck and re-claimed later (this run or the
    # next) — but there's no point letting a shard burn through its whole
    # runtime crawling batches it can't ever record, so this still caps
    # out and stops the shard early, same as the claim-failure cap.
    _MAX_CONSECUTIVE_FLUSH_FAILURES = 5

    while time.monotonic() < deadline:
        try:
            hosts = _claim_batch(shard, total_shards, BATCH_SIZE)
        except _ClaimBatchError as e:
            consecutive_claim_failures += 1
            log.error(f"Claim batch persistently failing "
                      f"({consecutive_claim_failures}/"
                      f"{_MAX_CONSECUTIVE_CLAIM_FAILURES} consecutive "
                      f"failures, each already retried "
                      f"{_CLAIM_RETRIES}x internally): {e}")
            if consecutive_claim_failures >= _MAX_CONSECUTIVE_CLAIM_FAILURES:
                log.error("Giving up after too many consecutive claim "
                          "failures — this is NOT the same as the queue "
                          "being exhausted; whatever's left in this "
                          "shard's slice will be picked up by the next "
                          "run.")
                break
            time.sleep(30)  # extra pause beyond _claim_batch's own
                             # internal backoff before trying the whole
                             # claim again
            continue

        consecutive_claim_failures = 0  # a successful call (even one
                                         # that legitimately found 0
                                         # hosts) resets this — only
                                         # actual exceptions count

        if not hosts:
            log.info("Queue exhausted for this shard — nothing left to claim.")
            break

        batch_num += 1
        batch_start = time.monotonic()
        results = asyncio.run(_run_batch([h["host"] for h in hosts]))
        batch_elapsed = time.monotonic() - batch_start

        visited_rows, found_rows = [], []
        for res in results:
            stats.attempted += 1
            outcome = res["outcome"]
            if outcome == "found":
                stats.found += 1
                stats.by_ats[res["ats"]] = stats.by_ats.get(res["ats"], 0) + 1
                found_rows.append({
                    "ats": res["ats"], "slug": res["slug"],
                    "source_host": res["host"],
                })
            elif outcome == "no_match":
                stats.no_match += 1
            elif outcome == "unreachable":
                stats.unreachable += 1
            elif outcome == "disallowed":
                stats.disallowed += 1
            elif outcome == "http_error":
                stats.http_error += 1
            visited_rows.append({"host": res["host"], "outcome": outcome,
                                  "ats": res.get("ats"), "slug": res.get("slug")})

        if not dry_run:
            try:
                _flush_results(visited_rows, found_rows)
                consecutive_flush_failures = 0
            except _FlushError as e:
                consecutive_flush_failures += 1
                stats.flush_failures += 1
                log.error(f"Flush persistently failing "
                          f"({consecutive_flush_failures}/"
                          f"{_MAX_CONSECUTIVE_FLUSH_FAILURES} consecutive "
                          f"failures, each already retried "
                          f"{_FLUSH_RETRIES}x internally): {e} — this "
                          f"batch's {len(visited_rows)} results were NOT "
                          f"recorded; those hosts will auto-release after "
                          f"30min staleness and get re-claimed later.")
                if consecutive_flush_failures >= _MAX_CONSECUTIVE_FLUSH_FAILURES:
                    log.error("Giving up after too many consecutive flush "
                              "failures — Supabase looks persistently "
                              "unreachable/degraded rather than "
                              "transiently blipping. Whatever's left in "
                              "this shard's slice (including the batches "
                              "that just failed to flush, once they "
                              "auto-release) will be picked up by a "
                              "future run.")
                    break

        elapsed_total = time.monotonic() - start
        log.info(f"Batch {batch_num}: {len(hosts)} hosts in {batch_elapsed:.1f}s "
                 f"({len(hosts) / batch_elapsed:.0f} hosts/sec) | "
                 f"total elapsed {elapsed_total / 60:.1f}min | {stats.summary()}")

    total_elapsed = time.monotonic() - start
    log.info(f"Tranco crawl finished — {stats.summary()} | "
             f"total runtime {total_elapsed / 60:.1f}min across {batch_num} batches")


def main():
    parser = argparse.ArgumentParser(
        description="Bulk async host crawl for ATS discovery, seeded from Tranco"
    )
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--total-shards", type=int, default=1)
    parser.add_argument("--max-runtime-seconds", type=float, default=MAX_RUNTIME_SECONDS)
    parser.add_argument("--dry-run", action="store_true",
                         help="Crawl but don't write results (still consumes "
                              "queue claims — use a small --max-runtime-seconds "
                              "for real testing)")
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL/SUPABASE_KEY not set.")
        sys.exit(1)

    run(args.shard, args.total_shards, args.max_runtime_seconds, args.dry_run)


if __name__ == "__main__":
    main()
