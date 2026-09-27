"""
ATS scrapers — one function per platform.
Each returns a list of job dicts with standardised keys:
  { title, url, company, location, department, workplace_type,
    employment_type, salary, description_snippet, source_ats, slug }
"""

import asyncio
import re
import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import xml.etree.ElementTree as ET
import urllib.robotparser
from html import unescape
from urllib.parse import unquote, urljoin, urlparse
import httpx
import requests
from bs4 import BeautifulSoup
from config import REQUEST_TIMEOUT, MAX_RETRIES
import geo
from discovery import _GH_JID_RE, extract_greenhouse_embed_token

log = logging.getLogger(__name__)

# ── Async HTTP transport (2026-09: requests -> httpx.AsyncClient) ──────
# Migration plan (externally reviewed, explicit user requirement: "async
# is allowed to change throughput, but it is absolutely not allowed to
# change the bytes/content that ultimately reach the extractor"):
#   Phase 0: differential test harness (httpx_migration_diff_harness.py)
#     proved byte-for-byte equivalence between requests and httpx across
#     20 real ATS URLs (4 platforms) before any production code changed —
#     see that file and .github/workflows/httpx-migration-test.yml.
#   Phase 1 (this change): swap the transport AND the concurrency model —
#     every scrape_* function becomes `async def`, awaiting this async
#     _get()/_get_client(), and crawl_i.py/crawl_ii.py's dispatch loops
#     move from ThreadPoolExecutor to asyncio.gather with a per-host
#     semaphore scheduler (see _host_semaphore below) instead of a flat
#     worker-count cap — this is what actually unlocks the throughput win,
#     since a sync requests.Session().get() call blocks its whole thread
#     for the full round-trip, while an awaited httpx call frees the event
#     loop to run other hosts' requests in the meantime.
#   BeautifulSoup itself stays fully synchronous (it isn't async-aware and
#     never will be) — CPU-bound HTML parsing is offloaded to a thread via
#     asyncio.to_thread at each scraper's own parsing step, so a large/
#     complex page's parse time doesn't block the event loop from
#     servicing other in-flight requests. See each converted scrape_*
#     function for its own `await asyncio.to_thread(BeautifulSoup, ...)`
#     call (added only where a function actually uses BeautifulSoup).
#
# One shared httpx.AsyncClient per event loop (there's exactly one event
# loop per crawl process, so effectively one client per process) replaces
# the old thread-local requests.Session pattern — httpx.AsyncClient is
# itself connection-pooled and safe to share across concurrent coroutines
# on the same loop (unlike requests.Session, it's explicitly documented as
# fine for concurrent use from async code).
#
# 2026-09 BUG FIX (real production evidence: a full 39-platform run with
# ~19,586 boards on one shard sat with ZERO platform completions —
# including tiny platforms like eploy (24 boards) and avature (30
# boards) that normally finish in well under a minute — for 12+ minutes
# straight): this limit used to be a flat 50/50, reasoned as "still far
# below any single ATS HOST's real capacity" — but max_connections is
# the ENTIRE PROCESS's connection budget shared across every platform and
# every host they touch AT ONCE, not a per-host figure. The old
# thread-local requests.Session pattern this replaced gave each worker
# THREAD its own independent pool_maxsize=20 pool, so the real aggregate
# capacity across a run scaled with however many threads were active
# (e.g. greenhouse/lever/icims alone could each have 30 threads, each
# with its own 20-connection pool) — nobody had to size one shared pool
# for the sum of every platform's concurrency at once, because there
# wasn't one shared pool. crawl_i.py's PLATFORM_WORKERS alone sums to
# ~400 across its explicitly configured platforms, before counting the
# dozen-plus unlisted platforms that fall back to a default of 8 each
# (~500+ total desired concurrency) — against a 50-connection shared
# pool, every platform's own semaphore says "go ahead, 8-30 of you can
# run" and then virtually all of them queue behind each other for one of
# only 50 real slots, process-wide, silently (httpx's connection-pool
# wait produces no log line of its own). Raised to comfortably cover
# that real configured demand with headroom for future platforms, rather
# than a number sized by comparison to a single host's tolerance.
_ASYNC_CLIENT_MAX_CONNECTIONS = 600
_ASYNC_CLIENT_MAX_KEEPALIVE = 300
_async_client: httpx.AsyncClient | None = None
_async_client_lock = asyncio.Lock()


async def _get_async_client() -> httpx.AsyncClient:
    """Return the process-wide shared AsyncClient, creating it on first
    use. Must be called from within a running event loop (asyncio.Lock()
    at module scope is safe here only because it's never awaited outside
    an event loop context — the crawl entrypoints always run under
    asyncio.run()/asyncio.gather())."""
    global _async_client
    if _async_client is not None:
        return _async_client
    async with _async_client_lock:
        if _async_client is None:
            # Explicit timeout categories (2026-09 external review: "read
            # timeout means the maximum time allowed between chunks of
            # response data, not a maximum total response size" — httpx's
            # strict defaults could otherwise cut off a slow-but-healthy
            # ATS host mid-response on a large job description, exactly
            # the "incomplete HTML -> partial description" failure mode
            # the migration explicitly must not introduce). Derived from
            # this project's existing REQUEST_TIMEOUT (config.py) for the
            # read leg — connect/write/pool kept short since those legs
            # failing slowly is pure wasted time, never a source of
            # truncated content.
            timeout = httpx.Timeout(
                connect=20.0,
                read=max(60.0, float(REQUEST_TIMEOUT)),
                write=30.0,
                pool=30.0,
            )
            limits = httpx.Limits(max_connections=_ASYNC_CLIENT_MAX_CONNECTIONS,
                                   max_keepalive_connections=_ASYNC_CLIENT_MAX_KEEPALIVE)
            _async_client = httpx.AsyncClient(
                timeout=timeout,
                limits=limits,
                http2=True,           # HTTP/2 preferred; httpx falls back to 1.1 automatically on incompatibility
                follow_redirects=True,
            )
        return _async_client


async def aclose_http_client() -> None:
    """Close the shared AsyncClient. Call once at process shutdown (after
    the crawl's asyncio.run() returns) to release pooled connections
    cleanly rather than relying on garbage collection."""
    global _async_client
    if _async_client is not None:
        await _async_client.aclose()
        _async_client = None


# ── Legacy thread-local requests.Session (2026-09: kept temporarily for
# any as-yet-unconverted call site during the batched migration — see the
# module docstring's Phase 1 note. Once every scrape_*/_fetch_* function
# in this file has moved to _get()/_get_async_client(), this and its
# _get_session()-based callers are removed entirely.) ──────────────────
_thread_local = threading.local()


def _get_session() -> requests.Session:
    """Return the current thread's reusable HTTP session."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        # Set default retry adapter with connection pooling.
        # 20 (was 10) — matches the higher per-platform worker counts below;
        # a pool smaller than a platform's max_workers forces threads to
        # queue for a connection even though the remote side has capacity.
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=0,  # We handle retries in _get_requests_sync()
        )
        _thread_local.session.mount("https://", adapter)
        _thread_local.session.mount("http://", adapter)
    return _thread_local.session


# ── Per-run scrape failure aggregation (2026-09, explicit user request) ──
# Individual company/tenant scrape failures (a bad Workday slug returning
# 403/422, a dead tenant, etc.) used to each get their own WARNING-level
# log line with the full URL and response body — real-world runs against
# tens of thousands of slugs can produce hundreds of these back-to-back,
# drowning out everything else in the log (confirmed live: a user
# complaint about exactly this wall of individual Workday warnings).
# Instead of removing the diagnostic detail entirely, each failure is
# recorded here (thread-safe — scrapers run under a ThreadPoolExecutor,
# see crawl_i.py's scrape_all) and the full body/URL still goes out at
# DEBUG level for anyone actually debugging a specific tenant. The
# grown-up-facing message is a single grouped one-liner per (platform,
# reason) — e.g. "breakthrought1d and 22 others: HTTP 422 (unrecognized
# site)" — printed once via log_scrape_failure_summary(), called from
# crawl_i.py right before the location-classification stage starts (the
# "errors section" the run's own log structure already has a natural
# place for).
_scrape_failures: dict[tuple[str, str], list[str]] = {}
_scrape_failures_lock = threading.Lock()


def _record_scrape_failure(ats: str, identifier: str, reason: str) -> None:
    """Record one company/tenant's scrape failure for later grouped
    summary instead of an immediate per-item WARNING log line. `reason`
    should already be a short, human-readable, GROUPABLE string (e.g.
    "HTTP 422 (unrecognized site)", "HTTP 403 (bot-blocked)", "connection
    error") — every failure with the same (ats, reason) pair gets grouped
    into one summary line together, so callers should normalize away
    anything unique-per-company (like error case IDs) before calling this."""
    with _scrape_failures_lock:
        _scrape_failures.setdefault((ats, reason), []).append(identifier)


def get_scrape_failure_summary(clear: bool = True) -> str | None:
    """Build one grouped, human-readable summary line per (ats, reason)
    from every failure recorded via _record_scrape_failure since the last
    call (or since process start). Returns None if nothing failed.

    Format per group: "{first identifier} and {N} others: {reason}" (just
    "{identifier}: {reason}" when there's only one) — matching the exact
    shape asked for ("company abc and 23 others 404'd, company efg and 4
    others 503'd"). Groups are sorted largest-first so the biggest,
    most-worth-investigating failure pattern reads first."""
    with _scrape_failures_lock:
        if not _scrape_failures:
            return None
        groups = list(_scrape_failures.items())
        if clear:
            _scrape_failures.clear()

    # Group further by ats so multi-platform runs read as one line per
    # platform rather than an unlabeled flat list.
    by_ats: dict[str, list[tuple[str, list[str]]]] = {}
    for (ats, reason), identifiers in groups:
        by_ats.setdefault(ats, []).append((reason, identifiers))

    lines = []
    for ats in sorted(by_ats):
        reason_groups = sorted(by_ats[ats], key=lambda rg: -len(rg[1]))
        parts = []
        total = 0
        for reason, identifiers in reason_groups:
            total += len(identifiers)
            first = identifiers[0]
            if len(identifiers) == 1:
                parts.append(f"{first}: {reason}")
            else:
                parts.append(f"{first} and {len(identifiers) - 1} other"
                             f"{'s' if len(identifiers) > 2 else ''}: {reason}")
        lines.append(f"[{ats}] {total} failed — " + "; ".join(parts))

    return "\n".join(lines)


def log_scrape_failure_summary() -> None:
    """Convenience wrapper: log the grouped summary (if any) at WARNING
    level and clear the collector for the next run. No-op, no log line at
    all, when nothing failed this run."""
    summary = get_scrape_failure_summary(clear=True)
    if summary:
        log.warning("── Scrape failures (grouped) ──\n" + summary)


def _categorize_scrape_error(err: Exception) -> str:
    """A short, GROUPABLE reason string for _record_scrape_failure — same
    shape scrape_workday's own hand-written reasons already use (see its
    "invalid JSON response"/"connection error"/f"HTTP {code}" calls
    above), generalized here so every platform's failures group the same
    way, not just Workday's.

    2026-09: this is the other half of the scrape_board BUG FIX below —
    _record_scrape_failure existed since 2026-09 but had exactly ONE
    caller (scrape_workday) in a 39-platform file, so the grouped
    end-of-run summary was silently empty for every other platform no
    matter how many boards failed. Called from scrape_board's except
    clause, this makes every platform's raised failures land in the same
    summary Workday's already did, with no per-scraper changes needed."""
    if isinstance(err, httpx.HTTPStatusError):
        return f"HTTP {err.response.status_code}"
    if isinstance(err, requests.exceptions.HTTPError) and err.response is not None:
        return f"HTTP {err.response.status_code}"
    if isinstance(err, (httpx.TimeoutException, requests.exceptions.Timeout)):
        return "timeout"
    if isinstance(err, (httpx.ConnectError, httpx.NetworkError, requests.exceptions.ConnectionError)):
        return "connection error"
    if isinstance(err, json.JSONDecodeError):
        return "invalid JSON response"
    return type(err).__name__


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
]


# ── Per-host adaptive rate limiter (2026-09, explicit user request,
# quoting an external architecture review verbatim: "No delay by default →
# Request → 200 → immediately continue → 429 → obey Retry-After → 5xx →
# exponential retry ... turn that into a proper host-aware rate limiter
# rather than adding random sleeps everywhere") ────────────────────────
# Every pagination/enrichment loop in this file used to call
# `time.sleep(random.uniform(0.2-0.3, 0.5-1.0))` UNCONDITIONALLY after
# every successful page/job fetch — real cost estimated by the user's own
# source at ~9.7 hours of pure artificial waiting per 100,000 jobs at a
# 0.35s average, confirmed as the direct cause of at least one real run
# blowing past its allotted GitHub Actions job timeout. `_get()` above
# already does the important, correctness-relevant half of "polite HTTP"
# (honors Retry-After on 429, exponential backoff on failure) — what was
# missing was ONLY the "no delay by default on a healthy host" half.
#
# This is deliberately NOT a full token-bucket/async rate limiter (that's
# a much bigger, separate change the user explicitly deferred — see this
# session's own scoping discussion). It's the smallest change that
# actually removes the unconditional-sleep waste while keeping every
# existing scraper's call shape (one `_get()` per page, called from a
# synchronous loop) unchanged: a per-host record of "how much this host
# currently seems to need to be paced," starting at ZERO wait for a host
# that's never given any sign of trouble, escalated only when `_get`
# itself observes a 429/repeated failure against that host, and decayed
# back toward zero after a run of clean responses. Callers that used to
# call `time.sleep(random.uniform(a, b))` unconditionally now call
# `_pace_host(url)` instead, which sleeps 0s the overwhelming majority of
# the time (a healthy ATS host) and only actually waits when this
# specific host has recently shown a reason to slow down.
_host_pace_lock = threading.Lock()
_host_pace_state: dict[str, dict] = {}  # host -> {"gap": float, "clean_streak": int}

_HOST_PACE_ESCALATE_TO = 1.0    # seconds — applied the moment a host 429s/repeatedly fails
_HOST_PACE_DECAY_AFTER = 5      # consecutive clean (non-429, non-exception) responses
_HOST_PACE_DECAY_FACTOR = 0.5   # halve the gap after each decay step
_HOST_PACE_FLOOR = 0.05         # below this, just snap to 0 — not worth timer overhead


def _host_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return url


def _pace_host(url: str) -> None:
    """Sleep only as long as this SPECIFIC host currently warrants —
    zero, for a host that's never shown trouble. Replaces the old
    unconditional `time.sleep(random.uniform(...))` calls scattered after
    every successful page/job fetch throughout this file."""
    host = _host_of(url)
    with _host_pace_lock:
        state = _host_pace_state.get(host)
        gap = state["gap"] if state else 0.0
    if gap > 0:
        # Small jitter on top of the current gap — same thundering-herd
        # reasoning _get()'s own backoff already uses, kept here too since
        # multiple worker threads can be pacing the same host at once.
        time.sleep(gap + random.uniform(0, gap * 0.25))


async def _pace_host_async(url: str) -> None:
    """Same contract as _pace_host, for callers running as a genuine
    coroutine on the shared event loop rather than a worker thread (2026-09
    description-enrichment async migration) — a blocking time.sleep() here
    would freeze every OTHER concurrent enrichment fetch for the pace
    duration, exactly the bug scrape_board's own migration just fixed one
    layer up. Reads the same _host_pace_state a still-sync caller's
    _pace_host would, just sleeps with asyncio.sleep() instead."""
    host = _host_of(url)
    with _host_pace_lock:
        state = _host_pace_state.get(host)
        gap = state["gap"] if state else 0.0
    if gap > 0:
        await asyncio.sleep(gap + random.uniform(0, gap * 0.25))


def _note_host_response(url: str, *, was_rate_limited: bool, was_error: bool) -> None:
    """Called from `_get_requests_sync()` after every real HTTP attempt against `url` to
    keep that host's pacing state current. A 429 or repeated failure
    escalates the gap immediately (a host that's already complaining
    doesn't need N more strikes before this project starts being more
    careful with it); a run of clean responses decays it back down, so a
    host that had one bad moment doesn't stay artificially slowed for the
    rest of a multi-thousand-job run."""
    host = _host_of(url)
    with _host_pace_lock:
        state = _host_pace_state.setdefault(host, {"gap": 0.0, "clean_streak": 0})
        if was_rate_limited or was_error:
            state["gap"] = max(state["gap"], _HOST_PACE_ESCALATE_TO)
            state["clean_streak"] = 0
        else:
            state["clean_streak"] += 1
            if state["clean_streak"] >= _HOST_PACE_DECAY_AFTER and state["gap"] > 0:
                state["gap"] = state["gap"] * _HOST_PACE_DECAY_FACTOR
                if state["gap"] < _HOST_PACE_FLOOR:
                    state["gap"] = 0.0
                state["clean_streak"] = 0


def _get_requests_sync(url: str, **kwargs) -> requests.Response | None:
    """LEGACY sync transport (requests) — 2026-09: kept only for
    not-yet-migrated scrape_*/_fetch_* functions during the batched async
    migration (see the module docstring's Phase 1 note). Every function
    still calling this will be converted to `await _get_requests_sync(...)` in a later
    batch; once none remain, this and _get_session()/_thread_local above
    are deleted entirely. Behavior is intentionally UNCHANGED from before
    the migration — this is the exact pre-migration implementation, not a
    new one — so a not-yet-converted scraper's behavior stays identical
    until its own batch converts it."""
    session = _get_session()
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            if r.status_code == 429:
                _note_host_response(url, was_rate_limited=True, was_error=False)
                retry_after = r.headers.get("Retry-After")
                if retry_after and retry_after.strip().isdigit():
                    wait = min(int(retry_after), 30)
                else:
                    wait = min(2 ** attempt + random.uniform(0, 1), 30)
                time.sleep(wait)
                continue
            r.raise_for_status()
            _note_host_response(url, was_rate_limited=False, was_error=False)
            return r
        except Exception as e:
            if attempt == MAX_RETRIES:
                _note_host_response(url, was_rate_limited=False, was_error=True)
                log.debug(f"Failed {url}: {e}")
                return None
            time.sleep(min(2 ** attempt + random.uniform(0, 0.5), 15))
    return None


# ── Per-host concurrency scheduler (async) ──────────────────────────────
# 2026-09 (external review, explicit user requirement): "Suppose you
# currently have 20 workers -> 20 requests -> ATS responds, and migrate to
# 500 async tasks -> 500 requests -> ATS says NO -> 429 429 429 ... You
# have technically increased throughput capacity while decreasing useful
# throughput... turn Retry-After handling into a real per-host scheduler:
# concurrency limit, requests/sec limit, backoff state, circuit breaker."
#
# This is the async-native replacement for the old thread-pool's implicit
# per-platform concurrency cap (crawl_i.py's ThreadPoolExecutor(max_workers=N)
# per ATS) — a per-HOST (not per-platform; some platforms span many
# distinct tenant hosts, e.g. Workday's *.myworkdayjobs.com subdomains)
# asyncio.Semaphore caps how many requests can be in flight against the
# SAME host at once, regardless of how many total coroutines the crawl has
# scheduled overall. _pace_host's existing gap-based pacing (escalate on
# 429/error, decay on a clean streak) still runs on top of this — the
# semaphore bounds CONCURRENT in-flight requests per host, while
# _pace_host bounds the RATE of new requests per host; together they cover
# both halves of "don't hammer one host" the external review called for.
_host_semaphore_lock = threading.Lock()
_host_semaphores: dict[str, asyncio.Semaphore] = {}

# Per-host concurrency ceiling. Deliberately conservative and uniform
# rather than a hand-tuned per-platform table (the external review's
# suggested "Greenhouse high, Workday moderate, Taleo/BrassRing
# conservative" tiers need real measured data this project doesn't have
# yet) — _pace_host's existing escalate-on-trouble/decay-on-health pacing
# already adapts per-host at the RATE level, so a uniform concurrency
# ceiling here is a safe starting point: no host can ever have more than
# this many requests in flight at once, healthy or not, and a host that
# starts showing 429s/errors gets slowed further by _pace_host's gap on
# top of this ceiling, not instead of it.
_HOST_CONCURRENCY_LIMIT = 8


def _host_semaphore_for(url: str) -> asyncio.Semaphore:
    host = _host_of(url)
    with _host_semaphore_lock:
        sem = _host_semaphores.get(host)
        if sem is None:
            sem = asyncio.Semaphore(_HOST_CONCURRENCY_LIMIT)
            _host_semaphores[host] = sem
        return sem


async def _get(url: str, **kwargs) -> httpx.Response | None:
    """Async transport (httpx.AsyncClient) — the replacement for the old
    sync `_get()` (requests), proven byte-for-byte equivalent by
    httpx_migration_diff_harness.py before this migration started. Same
    retry/backoff/Retry-After/pacing contract as the legacy version
    (_get_requests_sync above): returns the response on success, None
    after MAX_RETRIES exhausted failures — callers that already handle
    "_get() returned None" correctly need NO changes to that handling,
    only `await` added at the call site.

    kwargs are passed straight to httpx.AsyncClient.get() — `params` and
    `headers` (the two ever used by callers in this file) have identical
    names/semantics in httpx as in requests. `allow_redirects` (requests'
    name) is NOT accepted here — httpx's AsyncClient is constructed with
    follow_redirects=True globally (see _get_async_client), so passing
    allow_redirects would raise; any call site still passing it needs that
    kwarg removed as part of its own migration (see each converted
    scrape_*'s diff).
    """
    client = await _get_async_client()
    host_sem = _host_semaphore_for(url)
    async with host_sem:
        for attempt in range(MAX_RETRIES + 1):
            try:
                r = await client.get(url, **kwargs)
                if r.status_code == 429:
                    _note_host_response(url, was_rate_limited=True, was_error=False)
                    retry_after = r.headers.get("Retry-After")
                    if retry_after and retry_after.strip().isdigit():
                        wait = min(int(retry_after), 30)
                    else:
                        wait = min(2 ** attempt + random.uniform(0, 1), 30)
                    await asyncio.sleep(wait)
                    continue
                r.raise_for_status()
                _note_host_response(url, was_rate_limited=False, was_error=False)
                return r
            except Exception as e:
                if attempt == MAX_RETRIES:
                    _note_host_response(url, was_rate_limited=False, was_error=True)
                    log.debug(f"Failed {url}: {e}")
                    return None
                # Jitter on ordinary failures too — prevents a "thundering
                # herd" of many coroutines retrying a flaky endpoint in
                # lockstep.
                await asyncio.sleep(min(2 ** attempt + random.uniform(0, 0.5), 15))
    return None


async def _post(url: str, **kwargs) -> httpx.Response | None:
    """Async POST counterpart to _get, for the handful of scrapers that
    hit a search/query endpoint via POST with a JSON body (Workday,
    Taleo, Cornerstone OnDemand/csod, and any future platform's own
    _*_search-style helper) instead of a GET with query params. Same
    retry/backoff/Retry-After/pacing contract as _get, and same
    exception-swallowing-into-None return convention every existing
    caller already expects from `requests.Session().post(...)` wrapped
    in try/except — callers that already handle "returned None" need
    only `await` added at the call site.

    kwargs are passed straight to httpx.AsyncClient.post() — `json`,
    `data`, `params`, and `headers` (the ones used by callers in this
    file) have identical names/semantics in httpx as in requests.
    `allow_redirects`/`timeout` are NOT accepted here for the same
    reason as _get (see its docstring) — the client is constructed once
    with follow_redirects=True and a fixed httpx.Timeout.
    """
    client = await _get_async_client()
    host_sem = _host_semaphore_for(url)
    async with host_sem:
        for attempt in range(MAX_RETRIES + 1):
            try:
                r = await client.post(url, **kwargs)
                if r.status_code == 429:
                    _note_host_response(url, was_rate_limited=True, was_error=False)
                    retry_after = r.headers.get("Retry-After")
                    if retry_after and retry_after.strip().isdigit():
                        wait = min(int(retry_after), 30)
                    else:
                        wait = min(2 ** attempt + random.uniform(0, 1), 30)
                    await asyncio.sleep(wait)
                    continue
                return r
            except Exception as e:
                if attempt == MAX_RETRIES:
                    _note_host_response(url, was_rate_limited=False, was_error=True)
                    log.debug(f"POST failed {url}: {e}")
                    return None
                await asyncio.sleep(min(2 ** attempt + random.uniform(0, 0.5), 15))
    return None


async def _pace_host_async(url: str) -> None:
    """Async counterpart to _pace_host — same gap-based per-host pacing,
    but sleeps the coroutine (asyncio.sleep) instead of blocking the
    thread (time.sleep), so a paced host doesn't stall the whole event
    loop from servicing other hosts' in-flight requests while it waits."""
    host = _host_of(url)
    with _host_pace_lock:
        state = _host_pace_state.get(host)
        gap = state["gap"] if state else 0.0
    if gap > 0:
        await asyncio.sleep(gap + random.uniform(0, gap * 0.25))


# ── Shared markup-tolerant location extraction ────────────
# 2026-09: several scrapers below extracted a location by regex-matching a
# class/icon marker and then capturing "everything up to the next `<`"
# immediately after it — e.g. `class="[^"]*location[^"]*"[^>]*>([^<]+)`.
# That works only when the location text sits BARE right after the
# opening tag; if a board wraps it in one more tag (a `<span>`, a nested
# `<div>`, an icon wrapper), the very next character is `<`, the capture
# group requires 1+ non-`<` characters, and the whole match silently
# fails — producing location="" for a posting whose actual page clearly
# shows a real, often country-specific, location. Confirmed live on two
# independent platforms this session (JazzHR's inabia.applytojob.com and
# SuccessFactors' career.sonepar.com) and, on audit, present in the same
# shape across four more (Eploy, JobAdder, PageUp, Jobvite) and two shared
# HTML-fallback helpers used by several other platforms. This helper
# replaces the bare-capture pattern everywhere it was found: it takes the
# END of an already-matched "marker" (an icon tag or a class-attribute
# opening tag), grabs a bounded window of raw HTML after it, stops at the
# next block-level closing tag (so it can't run into an unrelated
# sibling's content), strips any tags inside that window instead of
# requiring their absence, and returns the resulting plain text.
def _extract_tolerant_text(html: str, after_pos: int, window: int = 400) -> str:
    """Best-effort plain-text extraction starting at `after_pos` in `html`,
    tolerant of any tags (spans, nested wrappers, icons) between the marker
    and the real text. Stops at the next li/ul/div/td/tr/h1-6 closing tag
    within `window` chars, so it can't bleed into an unrelated sibling
    field. Returns "" if nothing usable is found."""
    tail = html[after_pos:after_pos + window]
    tail = re.split(r'</(?:li|ul|div|td|tr|h\d)>', tail, maxsplit=1, flags=re.I)[0]
    text = unescape(re.sub(r'<[^>]+>', ' ', tail))
    return re.sub(r'\s+', ' ', text).strip()


def _bs4_find_location_near(anchor, class_substrings=("location",), max_levels=3) -> tuple[str, str]:
    """DOM-anchored replacement for the fixed-char-window fallback above.
    Given a BeautifulSoup <a> tag for a job's link, walks up to
    `max_levels` row/card-like ancestors (li/tr/div/article/section) and,
    at each level, looks for a descendant whose class contains one of
    `class_substrings` (e.g. "location", "vacancy-location"). Stops at
    the FIRST ancestor level where a match is found, so it stays anchored
    to the job's own semantic container rather than drifting into
    unrelated page chrome the way an unbounded whole-page search could.

    Returns (location_text, location_status), where status is one of
    "extracted" / "marker_found_empty" / "marker_not_found" — the same
    three-way distinction used elsewhere in this file so a downstream
    canary can tell "found the field but it was blank" apart from "never
    found a location field at all" (see the module-level comment on
    per-ATS extraction-status logging)."""
    selector = ", ".join(f'[class*="{c}"]' for c in class_substrings)
    node = anchor
    for _ in range(max_levels):
        node = node.find_parent(["li", "tr", "div", "article", "section"])
        if node is None:
            break
        loc_el = node.select_one(selector)
        if loc_el is not None and loc_el is not anchor:
            text = loc_el.get_text(" ", strip=True)
            return (text, "extracted") if text else ("", "marker_found_empty")
    return "", "marker_not_found"


# ATS template placeholders that leak into raw description payloads when a
# templating variable fails to resolve — e.g. "%LABEL_POSITION_TYPE_REMOTE_WITHIN%"
# (confirmed live in an ADP/Workday-style feed, see geo.py history). These are
# never real content, always noise, and materially hurt AI classification when
# left in (garbled jargon around the real sentence). Matched before the
# generic-junk pass below so a legitimate reading of the surrounding text
# survives.
_TEMPLATE_TOKEN_RE = re.compile(r"%[A-Z][A-Z0-9_]{2,}%")

# Runs of 3+ non-alphanumeric/non-space symbols in a row are near-always
# rendering/encoding garbage (mismatched CMS tokens, stray markup fragments)
# rather than real punctuation — real prose never needs "@)%" or "##%%--".
# Threshold is 3 so ordinary punctuation ("...", "--", "!!") and single
# percent signs ("50% remote") are left untouched.
_SYMBOL_GARBAGE_RE = re.compile(r"[^\w\s]{3,}")

# <script>/<style>/<noscript> TAG CONTENTS, not just the tags — the plain
# "<[^>]+>" strip below only removes the tags themselves, so without this
# a page's minified JS/CSS body text used to leak straight into the
# "cleaned" description as noise (real complaint: garbled symbol-heavy
# junk showing up in what's sent to the AI/regex classifiers).
_SCRIPT_STYLE_RE = re.compile(r"<(script|style|noscript)\b[^>]*>.*?</\1>", re.I | re.DOTALL)

# Invisible Unicode characters (zero-width space/non-joiner/joiner, LTR/
# RTL marks, BOM, soft hyphen) some sites use for copy-protection or that
# leak in from bad encoding. These render as nothing but still sit inside
# words — "spon​sor" with a hidden ZWSP mid-word — which can silently
# break keyword/regex matching (visa-sponsorship detection included)
# without being visible in any log or manual spot-check. Stripped
# entirely (not replaced with a space) since they're truly zero-width.
_ZERO_WIDTH_RE = re.compile(r"[​‌‍‎‏﻿­]")


# 2026-09 NEW (explicit user request: "track the location symbol and what
# location sits by it. Most companies seem to do that too. this would
# particularly be useful where the API call does not reveal it, like in
# the case of in-house ATSs"): many job postings render their location as
# a small icon/glyph immediately followed by a place name — a map-pin
# emoji, a house/office emoji for onsite-vs-remote, or a plain bullet
# character used the same way — rather than (or in addition to) a
# structured location FIELD the scraper can read directly. This is
# exactly the same signal-not-carried-by-the-API problem the existing
# "Metadata Location: ..." convention (see _fetch_greenhouse_questions,
# has_hard_metadata_location_signal in classifier.py) already solves for
# Greenhouse's own metadata — this generalizes the same idea to the raw
# HTML/text of ANY posting, before HTML tags are stripped (a glyph often
# sits in its own <span>/<i> tag right next to a text node, so this must
# run on the pre-strip markup, not the final plain-text snippet, or the
# adjacency between icon and place name is lost once tags collapse to
# whitespace).
#
# Deliberately narrow on what counts as a "location symbol": real map-pin/
# location glyphs (📍🌍🌎🌏🏢🏠🏙), never a bare generic bullet (•/·/-),
# since a generic bullet is used for countless other list items on a
# careers page (benefits, requirements, responsibilities) and would flood
# description_snippet with false "Location Symbol:" lines that have
# nothing to do with location. The captured "nearby text" is the run of
# words immediately following the glyph on the same line, up to the next
# glyph/punctuation boundary that plausibly ends a short location phrase
# (a following pipe/bullet/newline/another emoji, or a hard sentence stop) —
# this is intentionally a SHORT capture (a place name / short region
# phrase, not a whole sentence) since that's what actually sits next to
# these glyphs on real postings ("📍 Lagos, Nigeria | Full-time",
# "🌍 Remote - Africa only", "🏢 Onsite: Austin, TX").
_LOCATION_SYMBOL_GLYPHS = "📍🌍🌎🌏🏢🏠🏙"
_LOCATION_SYMBOL_RE = re.compile(
    r"[" + _LOCATION_SYMBOL_GLYPHS + r"]"
    r"\s*([^\n\r|•·<>]{2,80}?)"
    r"(?=\s*(?:[|•·\n\r<]|$|[" + _LOCATION_SYMBOL_GLYPHS + r"]))"
)

# Caps how many distinct "Location Symbol:" lines get appended per posting —
# a page that uses these glyphs decoratively throughout (e.g. a benefits
# list with a house emoji per bullet) should not balloon description_snippet
# with dozens of near-duplicate lines. 5 is generous for the real pattern
# (one location line, occasionally two for a hybrid "onsite + remote"
# posting) while still bounding the pathological case.
_MAX_LOCATION_SYMBOL_LINES = 5

# 2026-09 ROUND 2 (explicit user follow-up, with a real example: a red
# map-pin rendered as an actual IMAGE/icon, not a Unicode emoji character
# — "some location symbols look like the one attached"): plenty of career
# pages render their pin as a raster/SVG icon (a font-icon <i>/<span> with
# a "pin"/"location"/"map-marker"/"geo" class, an <img> whose src or alt
# names the same, or an inline <svg> with such a class/title/aria-label) —
# none of THOSE ever contain one of the emoji characters
# _LOCATION_SYMBOL_GLYPHS matches, so the check above silently misses
# this entire category. This is a SEPARATE, marker-element-based pass
# (rather than trying to shoehorn image detection into the character-glyph
# regex above): it looks for the icon ELEMENT itself via its class/src/alt/
# aria-label/title carrying a location-flavored keyword, then captures the
# plain text immediately following that element's closing tag — same
# "marker, then adjacent text" contract as the glyph-based check, and
# feeds into the exact same "Location Symbol: ..." line/dedup/cap so
# downstream (has_hard_location_symbol_signal, the AI prompt) doesn't need
# to know or care which of the two ways the marker was rendered.
#
# Deliberately keyword-scoped to the same narrow "clearly means location"
# set the whole file already trusts for icon-adjacent extraction (see
# _bs4_find_location_near's class_substrings=("location",) above) plus
# the few extra terms real icon libraries actually use for a pin (FontAwesome's
# "fa-map-marker(-alt)", Material Icons' "place"/"room", generic "pin"/"geo"/
# "map-pin") — NOT bare "map" alone (a "site map" link, an actual embedded
# map widget class, or "roadmap" would all false-positive on that).
_LOCATION_ICON_KEYWORDS = (
    r"pin|location|map-marker|map-pin|marker-icon|\bgeo\b|"
    r"\bplace\b|\broom\b"  # Material Icons' names for a pin glyph
)
_LOCATION_ICON_ELEMENT_RE = re.compile(
    r"<(img|svg|i|span)\b([^>]*(?:class|src|alt|aria-label|title)\s*=\s*"
    r'["\'][^"\']*(?:' + _LOCATION_ICON_KEYWORDS + r')[^"\']*["\'][^>]*)'
    r"(?:/>|>.*?</\1>|>)",
    re.I | re.DOTALL,
)
# Text immediately after the icon element, up to the same kind of boundary
# _LOCATION_SYMBOL_RE stops at (an intervening tag is fine here — unlike
# the glyph case, an <img>/<svg> icon is ALREADY the element being matched,
# so the text after it commonly sits in its own sibling <span> one level
# out; this is matched against the ORIGINAL html, not the tag-stripped
# copy, specifically so it can look past exactly one more wrapper tag).
_LOCATION_ICON_TRAILING_TEXT_RE = re.compile(
    r"^\s*(?:<[^>]+>\s*)?([^\n\r|•·<" + _LOCATION_SYMBOL_GLYPHS + r"]{2,80}?)"
    r"(?=\s*(?:[|•·\n\r<]|$|[" + _LOCATION_SYMBOL_GLYPHS + r"]))"
)


def _extract_location_icon_lines(html_or_text: str) -> list[str]:
    """Companion to _extract_location_symbol_lines for a location marker
    rendered as an IMAGE/SVG/font-icon element rather than a Unicode
    emoji glyph (see the module comment above _LOCATION_ICON_KEYWORDS for
    the real-world trigger). Returns "Location Symbol: <text>" lines,
    same format/convention as the glyph-based function, so both feed the
    same downstream dedup/cap/hard-filter/AI-prompt path without either
    one needing special-case handling."""
    if not html_or_text or "<" not in html_or_text:
        return []
    lines = []
    seen = set()
    for m in _LOCATION_ICON_ELEMENT_RE.finditer(html_or_text):
        tail = html_or_text[m.end():m.end() + 200]
        tm = _LOCATION_ICON_TRAILING_TEXT_RE.match(tail)
        if not tm:
            continue
        candidate = unescape(re.sub(r"<[^>]+>", " ", tm.group(1)))
        candidate = re.sub(r"\s+", " ", candidate).strip(" -:—–|")
        if not candidate or len(candidate) < 2:
            continue
        if not re.search(r"[A-Za-z]{2,}", candidate):
            continue
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        lines.append(f"Location Symbol: {candidate}")
        if len(lines) >= _MAX_LOCATION_SYMBOL_LINES:
            break
    return lines


def _extract_location_symbol_lines(html_or_text: str) -> list[str]:
    """Find map-pin/location-glyph + adjacent text pairs in raw (pre-strip)
    HTML/text and return them as "Location Symbol: <text>" lines, ready to
    append to a cleaned description_snippet — same convention as
    ats_scrapers.py's existing "Metadata Location: ..." line for
    Greenhouse. Returns [] when no such glyph is present (the overwhelming
    majority of postings), so this costs nothing when unused.

    2026-09 FIX: real markup very often wraps the glyph in its own
    element separate from the text node right after it (e.g.
    "<span>📍</span> Lagos, Nigeria") — confirmed by a failing test using
    exactly that shape. Matching straight against the raw HTML meant the
    very next character after the glyph was "<" (the closing tag), which
    the capture group's character class deliberately excludes (so a
    capture never accidentally swallows a whole tag), so the match failed
    outright even though a human reading the rendered page sees the pin
    sitting directly next to "Lagos, Nigeria". Tags are stripped (to
    nothing, not to a space — a space would itself break the adjacency
    this function exists to detect) BEFORE the glyph search runs, so the
    glyph and its neighboring text collapse onto the same run of
    characters exactly as they'd visually appear to a reader."""
    if not html_or_text or not any(g in html_or_text for g in _LOCATION_SYMBOL_GLYPHS):
        return []
    tagless = re.sub(r"<[^>]+>", "", html_or_text)
    lines = []
    seen = set()
    for m in _LOCATION_SYMBOL_RE.finditer(tagless):
        candidate = unescape(m.group(1))
        candidate = re.sub(r"\s+", " ", candidate).strip(" -:—–|")
        if not candidate or len(candidate) < 2:
            continue
        # Skip anything that's clearly not a place (pure digits/punctuation,
        # or a single common word too generic to be a location on its own —
        # "Full-time"/"Remote" alone are handled fine elsewhere and aren't
        # wrong to skip here since they add no NEW signal over what the
        # location field/other checks already see).
        if not re.search(r"[A-Za-z]{2,}", candidate):
            continue
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        lines.append(f"Location Symbol: {candidate}")
        if len(lines) >= _MAX_LOCATION_SYMBOL_LINES:
            break
    return lines


def _snippet(html_or_text: str, max_chars: int = 500_000) -> str:
    """Strip HTML, decode entities, drop ATS template/encoding junk, and cap length.

    2026-09 ROUND 7 (explicit user instruction: "never ever truncate a
    job"): raised from 30,000 to 500,000 — a real job description never
    comes anywhere close to this; it exists purely as a defensive ceiling
    against a genuinely pathological outlier (an ATS bug dumping megabytes
    of repeated boilerplate, or a scrape landing on the wrong page element
    entirely), not as a normal operating limit. Downstream,
    classifier.py's _assign_jobs_by_desc_length already routes a job to
    whichever provider has enough per-request budget to hold it whole
    (Groq's ~6,000-char budget, NVIDIA/OpenAI's 3.2-3.3M), so a genuinely
    long real-world JD still reaches the classifier untouched rather than
    being cut off mid-sentence — which can hide the exact
    restriction/eligibility language the AI is being asked to find.

    2026-09 NEW: also extracts any "location symbol + nearby text" pairs
    (map-pin/location emoji immediately followed by a place name — see
    _extract_location_symbol_lines) from the RAW html_or_text before tags
    are stripped, and appends them as "Location Symbol: ..." lines at the
    end of the cleaned text, same convention as the existing "Metadata
    Location: ..." line. This must be extracted here, pre-strip, since
    stripping HTML tags collapses the glyph and its neighboring text node
    onto the same whitespace-joined line as everything else, losing the
    adjacency that makes the signal meaningful in the first place.
    """
    if not html_or_text:
        return ""
    # Both the emoji-glyph marker and the image/SVG/font-icon marker feed
    # the same "Location Symbol: ..." line format — dedup across BOTH
    # sources together (not just within each one) so a posting using an
    # icon library that also happens to include an emoji fallback doesn't
    # get the same place name written out twice.
    location_symbol_lines = []
    _seen_combined = set()
    for line in _extract_location_symbol_lines(html_or_text) + _extract_location_icon_lines(html_or_text):
        key = line.lower()
        if key in _seen_combined:
            continue
        _seen_combined.add(key)
        location_symbol_lines.append(line)
        if len(location_symbol_lines) >= _MAX_LOCATION_SYMBOL_LINES:
            break
    text = _SCRIPT_STYLE_RE.sub(" ", html_or_text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    # Run again post-unescape — an entity can decode INTO a zero-width
    # char (e.g. &#8203; = zero-width space) that wasn't there pre-decode.
    text = _ZERO_WIDTH_RE.sub("", text)
    text = _TEMPLATE_TOKEN_RE.sub(" ", text)
    text = _SYMBOL_GARBAGE_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if location_symbol_lines:
        text = (text + " " + " | ".join(location_symbol_lines)).strip() if text else " | ".join(location_symbol_lines)
    return text[:max_chars]


# ── Compensation detection (2026-09: vocabulary expansion + false-
# positive guards) ───────────────────────────────────────────────────
# The original version only matched a bare "$X - $Y" shape (plus a
# handful of currency-code variants) and its last, most-permissive
# pattern matched almost ANY bare dollar figure at all — real postings
# describe pay in far more ways than that (hourly rates, OTE/on-target
# earnings for commission-heavy CSM/AM roles, "up to $X" framing, ranges
# written as "120K-160K" with no currency SYMBOL at all, non-USD symbols
# like £/€), while CSM/AM/PM job descriptions are also FULL of other
# dollar figures that are NOT compensation at all — a rep's quota, their
# book of business, the ARR/pipeline/deal size/territory they'll manage,
# a company's funding round or valuation. The old bare-figure pattern
# would have matched any of those exactly as happily as a real salary
# line. Two fixes applied together below:
#   1. Vocabulary: many more real compensation phrasings recognized —
#      base salary, total comp, OTE, pay/salary band, hourly rate, "up
#      to $X", non-$ currencies, K-ranges with no symbol at all.
#   2. A disqualifying-context check (both just before AND just after
#      the match, since real postings write both "$2M territory" and
#      "managing a $2M book of business" — the disqualifying word can
#      land on either side of the number): a match sitting next to a
#      quota/ARR/funding/valuation/territory-style phrase is skipped
#      even though it fits a compensation-shaped pattern.
_COMP_CURRENCY = r"(?:US\$|CA\$|C\$|A\$|NZ\$|\$|£|€|₹|USD|GBP|EUR|CAD|AUD|NZD|CHF|SGD|INR)"
_COMP_NUMBER = r"[\d][\d,]*(?:\.\d+)?\s*[kK]?"
_COMP_RANGE_SEP = r"(?:\s*(?:[-–—]|to|through)\s*)"
_COMP_PERIOD = (
    r"(?:\s*(?:/\s*(?:hr|hour|yr|year)|per\s+(?:hour|annum|year)|"
    r"annually|yearly|p\.a\.|\bpa\b))?"
)
_COMP_KEYWORDS = (
    r"(?:base\s+)?salary|compensation|\bcomp\b|total\s+comp(?:ensation)?|"
    r"\bpay\b|wages?|remuneration|stipend|\bOTE\b|on[- ]target\s+earnings|"
    r"target\s+compensation|expected\s+compensation|annual\s+compensation|"
    r"pay\s+range|salary\s+range|compensation\s+range|pay\s+scale|"
    r"salary\s+band|pay\s+band|hourly\s+rate|hourly\s+wage|base\s+pay|"
    r"starting\s+salary|annual\s+salary"
)
# Phrases that mean a nearby dollar figure is almost certainly NOT
# compensation — a sales quota/territory, revenue/ARR the rep manages,
# or the company's own funding/valuation, all of which read exactly like
# a salary range in isolation ("$1M-$5M") but never are one.
_COMP_DISQUALIFY_RE = re.compile(
    r"quota|book\s+of\s+business|territory|portfolio|\bARR\b|"
    r"annual\s+recurring\s+revenue|pipeline|revenue|\bACV\b|\bTCV\b|"
    r"contract\s+value|deal\s+size|funding|raised|raising|"
    r"series\s+[a-z]\b|valuation|valued\s+at|market\s+cap|"
    r"ad\s+spend|media\s+spend|budget|customers?\s+(?:with|worth)|"
    r"managing\s+(?:a|an)|\bAUM\b|assets\s+under\s+management",
    re.I,
)
_COMP_CONTEXT_BEFORE = 60  # chars scanned before a match for disqualifying context
_COMP_CONTEXT_AFTER = 40   # chars scanned after a match — "$2M territory" lands here

_COMP_PATTERNS = [
    # "up to $150,000" / "up to £80K"
    re.compile(rf"up\s+to\s+{_COMP_CURRENCY}\s*{_COMP_NUMBER}{_COMP_PERIOD}", re.I),
    # "$120,000 - $180,000" / "£45K-£60K" / "USD 120,000-180,000"
    re.compile(rf"{_COMP_CURRENCY}\s*{_COMP_NUMBER}{_COMP_RANGE_SEP}{_COMP_CURRENCY}?\s*"
               rf"{_COMP_NUMBER}{_COMP_PERIOD}", re.I),
    # "120,000 - 160,000 USD" / "120K-160K annually" — currency AFTER the
    # range, or no currency symbol at all but an explicit yearly period.
    re.compile(rf"{_COMP_NUMBER}{_COMP_RANGE_SEP}{_COMP_NUMBER}\s*"
               rf"(?:{_COMP_CURRENCY}|annually|per\s+year|/\s*year|/\s*yr)", re.I),
    # keyword-anchored: "salary range: $X - $Y", "OTE of $150K", "base
    # salary $95,000", "hourly rate: $22/hr" — the keyword itself is the
    # evidence, so a single bare figure (not necessarily a range) is
    # enough here, unlike the currency-only patterns above.
    re.compile(rf"(?:{_COMP_KEYWORDS})[\s:\-]{{1,15}}(?:of\s+)?{_COMP_CURRENCY}\s*"
               rf"{_COMP_NUMBER}(?:{_COMP_RANGE_SEP}{_COMP_CURRENCY}?\s*{_COMP_NUMBER})?"
               rf"{_COMP_PERIOD}", re.I),
    # bare hourly rate: "$22/hr", "$22.50 per hour"
    re.compile(rf"{_COMP_CURRENCY}\s*{_COMP_NUMBER}\s*(?:/\s*(?:hr|hour)|per\s+hour)", re.I),
]


def _extract_salary(text: str) -> str:
    """Try to extract salary/compensation from description text — see
    the block comment above for the vocabulary this recognizes and the
    false-positive guard against quota/ARR/funding/territory figures
    that look identical in shape to a real compensation range. Patterns
    are tried in order (most specific/least ambiguous first); within a
    pattern every match is scanned in order and the first one that
    clears the disqualifying-context check wins — a rejected match never
    blocks a real salary line elsewhere in the same description."""
    if not text:
        return ""
    for pattern in _COMP_PATTERNS:
        for m in pattern.finditer(text):
            before = text[max(0, m.start() - _COMP_CONTEXT_BEFORE):m.start()]
            after = text[m.end():m.end() + _COMP_CONTEXT_AFTER]
            if _COMP_DISQUALIFY_RE.search(before) or _COMP_DISQUALIFY_RE.search(after):
                continue  # e.g. "managing a $2M-$5M territory" — not pay
            return m.group(0).strip()
    return ""


def _text(value) -> str:
    """Coerce a possibly-None/non-string JSON field (title, url, id, …)
    into a clean, single-line string: collapse internal whitespace and
    unescape HTML entities. Shared by every scraper that pulls fields
    out of an embedded JSON blob or JSON-LD block."""
    return unescape(re.sub(r"\s+", " ", str(value or "").strip()))


# ── Rippling ────────────────────────────────────────────

def scrape_rippling(slug: str) -> list[dict]:
    """Rippling public API — paginated."""
    base = f"https://ats.rippling.com/api/v2/board/{slug}/jobs"
    all_jobs = []
    page = 0

    while True:
        r = _get_requests_sync(base, params={
            "page": page, "pageSize": 50,
            "searchQuery": "", "city": "", "country": "",
            "state": "", "workplaceType": "",
            "groupJobsByLocation": "false",
        })
        if not r:
            break
        try:
            data = r.json()
        except Exception:
            break

        for item in data.get("items", []):
            locations = item.get("locations") or []
            loc_names = ", ".join(l.get("name", "") for l in locations)
            countries = ", ".join(sorted(set(
                l.get("country", "") for l in locations if l.get("country")
            )))
            wt = ", ".join(sorted(set(
                l.get("workplaceType", "") for l in locations if l.get("workplaceType")
            )))
            # Salary from compensation if available
            salary_str = ""
            comp = item.get("compensation") or item.get("salary") or {}
            if isinstance(comp, dict):
                min_s = comp.get("min", "") or comp.get("minimum", "")
                max_s = comp.get("max", "") or comp.get("maximum", "")
                currency = comp.get("currency", "USD")
                if min_s and max_s:
                    salary_str = f"{currency} {min_s}-{max_s}"

            desc = _snippet(item.get("description", "") or item.get("descriptionHtml", "") or "")
            if not salary_str:
                salary_str = _extract_salary(desc)

            all_jobs.append({
                "title": (item.get("name") or "").strip(),
                "url": item.get("url", ""),
                "company": "",
                "location": loc_names,
                "country": countries,
                "department": (item.get("department") or {}).get("name", ""),
                "workplace_type": wt,
                "employment_type": "",
                "salary": salary_str,
                "description_snippet": desc,
                "source_ats": "Rippling",
                "slug": slug,
            })

        total_pages = data.get("totalPages", 0)
        if page + 1 >= total_pages or not data.get("items"):
            break
        page += 1

    # Try to get company name from board info
    if all_jobs:
        r = _get_requests_sync(f"https://ats.rippling.com/api/v2/board/{slug}/jobs",
                 params={"page": 0, "pageSize": 1})
        if r:
            try:
                # Get company name from first job detail
                first_id = all_jobs[0]["url"].split("/")[-1]
                detail_r = _get_requests_sync(f"https://ats.rippling.com/api/v2/board/{slug}/jobs/{first_id}")
                if detail_r:
                    detail = detail_r.json()
                    company_name = detail.get("companyName", "")
                    for j in all_jobs:
                        j["company"] = company_name
            except Exception:
                pass

    return all_jobs


# ── Greenhouse ──────────────────────────────────────────

async def scrape_greenhouse(slug: str) -> list[dict]:
    """Greenhouse public Job Board API — no auth required."""
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    r = await _get(url, params={"content": "true"})
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []

    jobs_list = data.get("jobs")
    if not jobs_list or not isinstance(jobs_list, list):
        return []

    jobs = []
    for post in jobs_list:
        loc_obj = post.get("location")
        loc = loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj or "")
        depts = post.get("departments") or []
        dept = depts[0].get("name", "") if depts else ""

        # Extract country and enriched location from metadata
        country = ""
        metadata_location = ""
        metadata = post.get("metadata") or []
        if isinstance(metadata, list):
            for m in metadata:
                if not isinstance(m, dict):
                    continue
                meta_name = m.get("name", "").lower()
                meta_val = str(m.get("value") or "")
                if meta_name in ("country", "location_country") and meta_val:
                    country = meta_val
                elif meta_name == "location" and meta_val:
                    metadata_location = meta_val

        # If location is bare "Remote" but metadata has a richer value
        # (e.g. "United States (Remote)"), use the metadata value instead
        if metadata_location and loc.strip().lower() in ("remote", ""):
            loc = metadata_location

        # Extract salary from description content
        content = post.get("content", "")
        description = _snippet(content)
        salary = _extract_salary(description)

        jobs.append({
            "title": post.get("title", "").strip(),
            "url": post.get("absolute_url", ""),
            "company": "",
            "location": loc,
            "country": country,
            "department": dept,
            "workplace_type": "",
            "employment_type": "",
            "salary": salary,
            "description_snippet": description,
            "source_ats": "Greenhouse",
            "slug": slug,
        })

    # Get company name from board info
    if jobs:
        board_r = await _get(f"https://boards-api.greenhouse.io/v1/boards/{slug}")
        if board_r:
            try:
                board_data = board_r.json()
                company_name = board_data.get("name", "")
                for j in jobs:
                    j["company"] = company_name
            except Exception:
                pass

    return jobs


# ── Lever ───────────────────────────────────────────────

async def scrape_lever(slug: str) -> list[dict]:
    """Lever public postings API — no auth required."""
    url = f"https://api.lever.co/v0/postings/{slug}"
    r = await _get(url, params={"mode": "json"})
    if not r:
        # Try EU endpoint
        r = await _get(f"https://api.eu.lever.co/v0/postings/{slug}", params={"mode": "json"})
        if not r:
            return []
    try:
        data = r.json()
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    jobs = []
    for post in data:
        categories = post.get("categories", {})
        loc = categories.get("location", "")
        all_locs = categories.get("allLocations", [])
        if all_locs and not loc:
            loc = ", ".join(all_locs)

        # Salary
        salary_str = ""
        salary = post.get("salaryRange") or {}
        if isinstance(salary, dict) and salary:
            min_s = salary.get("min", "")
            max_s = salary.get("max", "")
            currency = salary.get("currency", "USD")
            if min_s and max_s:
                salary_str = f"{currency} {min_s}-{max_s}"

        jobs.append({
            "title": post.get("text", "").strip(),
            "url": post.get("hostedUrl", ""),
            "company": "",
            "location": loc,
            "country": "",
            "department": categories.get("department", "") or categories.get("team", ""),
            "workplace_type": post.get("workplaceType", ""),
            "employment_type": categories.get("commitment", ""),
            "salary": salary_str,
            "description_snippet": _snippet(post.get("descriptionPlain", "") or post.get("description", "")),
            "source_ats": "Lever",
            "slug": slug,
        })

    # Company name from slug (Lever API categories.team is the department, not company)
    if jobs:
        company = slug.replace("-", " ").title()
        for j in jobs:
            j["company"] = company

    return jobs


# ── Ashby ───────────────────────────────────────────────

async def scrape_ashby(slug: str) -> list[dict]:
    """Ashby public job board API — no auth required."""
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    r = await _get(url, params={"includeCompensation": "true"})
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []

    jobs_data = data.get("jobs", [])
    company_name = data.get("jobBoard", {}).get("organizationName", "") or slug.replace("-", " ").title()

    jobs = []
    for post in jobs_data:
        loc = post.get("location", "")
        dept = post.get("department", "")
        if isinstance(dept, dict):
            dept = dept.get("name", "")

        # Compensation
        # 2026-09 fix: this used to check comp_item["low"]/["high"]/["currency"]
        # on `compensation` treated as a dict-or-list of ranges — those keys
        # don't exist anywhere in Ashby's real response, confirmed against
        # both developers.ashbyhq.com's docs and a live board fetch (Replit's).
        # The real shape (only present when ?includeCompensation=true, which
        # this scraper already passes) is:
        #   compensation: {
        #     scrapeableCompensationSalarySummary: "<plain-text summary>",
        #     compensationTierSummary: "<plain-text summary>",
        #     summaryComponents: [{compensationType, interval, currencyCode,
        #                          minValue, maxValue, summary}, ...],
        #     compensationTiers: [{components: [<same shape>], ...}, ...],
        #   }
        # so the old code's key lookups always missed and salary_str was
        # silently "" for every Ashby job regardless of what the API
        # actually returned.
        salary_str = ""
        comp = post.get("compensation")
        if isinstance(comp, dict):
            # Prefer Ashby's own ready-made human-readable summary — it's
            # exactly what the job board itself displays, so it already
            # handles multi-tier/multi-currency cases correctly.
            salary_str = (comp.get("scrapeableCompensationSalarySummary")
                          or comp.get("compensationTierSummary") or "")
            if not salary_str:
                # Fall back to building one from the structured components
                # (summaryComponents first, else every tier's components).
                components = comp.get("summaryComponents") or []
                if not components:
                    for tier in (comp.get("compensationTiers") or []):
                        if isinstance(tier, dict):
                            components.extend(tier.get("components") or [])
                parts = []
                for c in components:
                    if not isinstance(c, dict):
                        continue
                    low = c.get("minValue")
                    high = c.get("maxValue")
                    currency = c.get("currencyCode") or "USD"
                    if low is not None and high is not None:
                        parts.append(f"{currency} {low}-{high}")
                salary_str = "; ".join(parts)

        # Enrich location from address.postalAddress if location is bare
        # Ashby's `location` field is often just "Remote" or "Hybrid",
        # while `address.postalAddress` has the real geographic data
        address = post.get("address") or {}
        postal = address.get("postalAddress") or {}
        addr_country = postal.get("addressCountry", "") or ""
        addr_region = postal.get("addressRegion", "") or ""
        addr_city = postal.get("addressLocality", "") or ""

        if loc.strip().lower() in ("remote", "hybrid", "on-site", "onsite", ""):
            # Build location from postal address
            addr_parts = filter(None, [addr_city, addr_region, addr_country])
            addr_loc = ", ".join(addr_parts)
            if addr_loc:
                workplace_type = loc.strip() if loc.strip() else (post.get("workplaceType") or "")
                if workplace_type.lower() == "remote" and addr_loc:
                    loc = f"Remote, {addr_loc}"
                elif addr_loc:
                    loc = addr_loc

        # Country from address or location string
        country = addr_country
        if not country and loc:
            # Fallback: "City, Country" or "Remote - Country"
            parts = [p.strip() for p in loc.replace(" - ", ", ").split(",")]
            if len(parts) >= 2:
                country = parts[-1]

        # Workplace type from workplaceType field (more reliable than employmentType)
        wt = post.get("workplaceType", "") or post.get("employmentType", "")

        jobs.append({
            "title": post.get("title", "").strip(),
            "url": post.get("jobUrl", "") or f"https://jobs.ashbyhq.com/{slug}/{post.get('id', '')}",
            "company": company_name,
            "location": loc,
            "country": country,
            "department": dept,
            "workplace_type": wt,
            "employment_type": "",
            "salary": salary_str,
            "description_snippet": _snippet(post.get("descriptionHtml", "") or post.get("descriptionPlain", "")),
            "source_ats": "Ashby",
            "slug": slug,
        })

    return jobs


# ── BambooHR ───────────────────────────────────────────

def scrape_bamboohr(slug: str) -> list[dict]:
    """BambooHR careers list — JSON endpoint, no auth required."""
    url = f"https://{slug}.bamboohr.com/careers/list"
    headers = {
        "Accept": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
    }
    try:
        r = _get_session().get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        if r.status_code != 200:
            return []
        if "application/json" not in r.headers.get("Content-Type", ""):
            return []
        data = r.json()
    except Exception:
        return []

    jobs_list = data.get("result")
    if not jobs_list or not isinstance(jobs_list, list):
        return []

    jobs = []
    for job in jobs_list:
        # BambooHR has two location objects:
        #   "location": {"city": ..., "state": ...}
        #   "atsLocation": {"country": ..., "state": ..., "province": ..., "city": ...}
        loc = job.get("location") or {}
        ats_loc = job.get("atsLocation") or {}
        if isinstance(loc, dict):
            city = loc.get("city", "") or ""
            state = loc.get("state", "") or ""
            country = loc.get("country", "") or ""
        else:
            city, state, country = (str(loc) if loc else ""), "", ""

        # Fallback to atsLocation if primary location is empty
        if not city and not state and not country and isinstance(ats_loc, dict):
            city = ats_loc.get("city", "") or ats_loc.get("province", "") or ""
            state = ats_loc.get("state", "") or ""
            country = ats_loc.get("country", "") or ""

        location = ", ".join(filter(None, [city, state, country]))

        dept = job.get("departmentLabel", "") or ""
        desc = _snippet(job.get("description", "") or "")
        salary = _extract_salary(desc)

        jobs.append({
            "title": (job.get("jobOpeningName") or "").strip(),
            "url": f"https://{slug}.bamboohr.com/careers/{job.get('id', '')}",
            "company": slug.replace("-", " ").title(),
            "location": location,
            "country": country,
            "department": dept,
            "workplace_type": "",
            "employment_type": job.get("employmentStatusLabel", ""),
            "salary": salary,
            "description_snippet": desc,
            "source_ats": "BambooHR",
            "slug": slug,
        })

    return jobs


# ── iCIMS ──────────────────────────────────────────────

def scrape_icims(slug: str) -> list[dict]:
    """iCIMS sitemap scraper — parses sitemap.xml for job URLs.
    Title is extracted from URL path. No description/location from sitemap."""
    sitemap_url = f"https://{slug}.icims.com/sitemap.xml"
    headers = {
        "Accept": "application/xml",
        "User-Agent": random.choice(USER_AGENTS),
    }
    try:
        r = _get_session().get(sitemap_url, timeout=REQUEST_TIMEOUT, headers=headers)
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.content)
    except Exception:
        return []

    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    jobs = []

    for url_el in root.findall(".//s:url", ns):
        loc_el = url_el.find("s:loc", ns)
        if loc_el is None:
            continue
        job_url = (loc_el.text or "").strip()
        if not job_url or "/jobs/" not in job_url or job_url.endswith("/jobs/intro"):
            continue

        path = job_url.split("/jobs/")[-1]
        parts = path.split("/")
        if len(parts) >= 2:
            title = unquote(parts[1]).replace("-", " ").strip().title()
        else:
            continue

        jobs.append({
            "title": title,
            "url": job_url,
            "company": slug.replace("-", " ").title(),
            "location": "",  # iCIMS sitemap doesn't include location
            "country": "",
            "department": "",
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "iCIMS",
            "slug": slug,
        })

    return jobs


# ── Workday ────────────────────────────────────────────

async def scrape_workday(slug: str) -> list[dict]:
    """Workday CXS JSON API. Slug format: 'company|wd#|site_id'.
    POST to /wday/cxs/{company}/{site_id}/jobs for paginated results.

    2026-09 ASYNC MIGRATION (batch 2): converted from _get_session().post()
    to await _post(...) — same httpx.AsyncClient transport as _get,
    proven byte-for-byte equivalent to requests by
    httpx_migration_diff_harness.py. Behavior otherwise unchanged: same
    payload shape, same pagination loop, same failure handling."""
    parts = slug.split("|")
    if len(parts) != 3:
        log.warning(f"[workday] Invalid slug format (expected 'company|wd#|site_id'): {slug!r}")
        return []

    company, wd, site_id = parts
    wd_num = wd.replace("wd", "")
    base_url = f"https://{company}.wd{wd_num}.myworkdayjobs.com"
    api_url = f"{base_url}/wday/cxs/{company}/{site_id}/jobs"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
        "Origin": base_url,
        "Referer": f"{base_url}/{site_id}",
    }

    all_jobs = []
    offset = 0
    limit = 20

    while True:
        payload = {
            "appliedFacets": {},
            "limit": limit,
            "offset": offset,
            "searchText": "",
        }

        r = await _post(api_url, json=payload, headers=headers)
        if r is None:
            log.debug(
                f"[workday] Request failed for slug={slug!r} offset={offset} "
                f"url={api_url}"
            )
            _record_scrape_failure("workday", company, "connection error")
            break

        if r.status_code != 200:
            log.debug(
                f"[workday] Non-200 status for slug={slug!r} offset={offset} "
                f"url={api_url}: status={r.status_code} "
                f"body={r.text[:200]!r}"
            )
            # 2026-09: group by (status, errorCode) when Workday's own JSON
            # body has one (e.g. S21 "site not found", S22 "permission
            # denied"/bot-blocked) — that's the actually-groupable, stable
            # signal; the errorCaseId in the same body is unique PER
            # REQUEST and would defeat grouping entirely if included.
            error_code = None
            try:
                error_code = r.json().get("errorCode")
            except Exception:
                pass
            reason = f"HTTP {r.status_code}" + (f" ({error_code})" if error_code else "")
            _record_scrape_failure("workday", company, reason)
            break

        try:
            data = r.json()
        except Exception as e:
            log.debug(
                f"[workday] Failed to parse JSON for slug={slug!r} offset={offset} "
                f"url={api_url}: {type(e).__name__}: {e} "
                f"body={r.text[:200]!r}"
            )
            _record_scrape_failure("workday", company, "invalid JSON response")
            break

        postings = data.get("jobPostings", [])
        total = data.get("total", 0)

        if not postings:
            if offset == 0:
                log.debug(
                    f"[workday] slug={slug!r} returned 0 postings on first page "
                    f"(reported total={total}) — tenant likely has no open jobs"
                )
            break

        for post in postings:
            job_path = post.get("externalPath", "")

            # Skip stale postings (30+ days old)
            posted_on = post.get("postedOn", "") or ""
            if "30+" in posted_on:
                continue

            # Location: try locationsText first, then bulletFields
            location = (post.get("locationsText") or "").strip()
            if not location:
                # bulletFields = [cities, states/regions, jobID]
                bf = post.get("bulletFields") or []
                if len(bf) >= 2:
                    cities = bf[0] if bf[0] else ""
                    states = bf[1] if bf[1] else ""
                    location = f"{cities}, {states}" if cities and states else (cities or states)
            location = location[:200]

            # Remote type from API
            remote_type = post.get("remoteType", "") or ""

            all_jobs.append({
                "title": (post.get("title") or "").strip(),
                "url": f"{base_url}/{site_id}{job_path}",
                "company": company.replace("-", " ").title(),
                "location": location,
                "country": "",
                "department": "",
                "workplace_type": remote_type,
                "employment_type": "",
                "salary": "",
                "description_snippet": "",  # Would need per-job fetch, too slow
                "source_ats": "Workday",
                "slug": slug,
                "posted_on": posted_on,
            })

        offset += limit
        if offset >= total:
            break

        # 2026-09: was an unconditional time.sleep(random.uniform(0.3, 1.0))
        # on every page regardless of how the last request went — see the
        # module-level comment above _pace_host for the full reasoning.
        # 2026-09 ASYNC MIGRATION: now uses _pace_host_async (asyncio.sleep)
        # since Workday's own request above is now the async _post().
        await _pace_host_async(api_url)

    return all_jobs


# ── Workable ──────────────────────────────────────────

async def scrape_workable(slug: str) -> list[dict]:
    """Workable public widget API — no auth, no pagination (returns all at once)."""
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    r = await _get(url, params={"details": "true"}, headers=headers)
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []

    company_name = data.get("name", slug.replace("-", " ").title())
    jobs_list = data.get("jobs")
    if not jobs_list or not isinstance(jobs_list, list):
        return []

    jobs = []
    for post in jobs_list:
        city = post.get("city", "")
        country = post.get("country", "")
        state = post.get("state", "")
        location = ", ".join(filter(None, [city, state, country]))

        desc = _snippet(post.get("description", ""))
        salary = _extract_salary(desc)

        # Workplace type from telecommuting flag
        telecommuting = post.get("telecommuting", False)
        workplace = "Remote" if telecommuting else ""

        jobs.append({
            "title": (post.get("title") or "").strip(),
            "url": post.get("url") or post.get("shortlink") or "",
            "company": company_name,
            "location": location,
            "country": country,
            "department": post.get("department", ""),
            "workplace_type": workplace,
            "employment_type": post.get("employment_type", ""),
            "salary": salary,
            "description_snippet": desc,
            "source_ats": "Workable",
            "slug": slug,
        })

    return jobs


# ── Recruitee ─────────────────────────────────────────

def scrape_recruitee(slug: str) -> list[dict]:
    """Recruitee Careers Site API — no auth, returns all offers at once."""
    url = f"https://{slug}.recruitee.com/api/offers/"
    headers = {
        "Accept": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
    }
    r = _get_requests_sync(url, headers=headers)
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []

    offers = data.get("offers")
    if not offers or not isinstance(offers, list):
        return []

    jobs = []
    for offer in offers:
        city = offer.get("city", "")
        country = offer.get("country", "")
        location = offer.get("location", "") or ", ".join(filter(None, [city, country]))

        # Remote flag
        remote = offer.get("remote", False)
        workplace = "Remote" if remote else ""

        # Description — try translations first, then direct field
        translations = offer.get("translations") or {}
        en_trans = translations.get("en", {})
        desc_html = en_trans.get("description", "") or offer.get("description", "")
        desc = _snippet(desc_html)

        # Salary — structured object or fallback to text extraction
        salary_str = ""
        salary_obj = offer.get("salary")
        if isinstance(salary_obj, dict):
            min_sal = salary_obj.get("min", "")
            max_sal = salary_obj.get("max", "")
            currency = salary_obj.get("currency", "")
            period = salary_obj.get("period", "")
            if min_sal and max_sal:
                salary_str = f"{currency} {min_sal}-{max_sal}".strip()
                if period:
                    salary_str += f" per {period}"
        elif isinstance(salary_obj, str) and salary_obj:
            salary_str = salary_obj
        if not salary_str:
            salary_str = _extract_salary(desc)

        # Employment type
        emp_type = offer.get("employment_type_code", "")

        jobs.append({
            "title": (offer.get("title") or "").strip(),
            "url": offer.get("careers_url") or offer.get("url") or f"https://{slug}.recruitee.com/o/{offer.get('slug', '')}",
            "company": offer.get("company_name", slug.replace("-", " ").title()),
            "location": location,
            "country": country,
            "department": offer.get("department", ""),
            "workplace_type": workplace,
            "employment_type": emp_type,
            "salary": salary_str or "",
            "description_snippet": desc,
            "source_ats": "Recruitee",
            "slug": slug,
        })

    return jobs


# ── SmartRecruiters ───────────────────────────────────

async def scrape_smartrecruiters(slug: str) -> list[dict]:
    """SmartRecruiters Posting API — no auth for public postings, paginated.

    2026-09 ASYNC MIGRATION (batch 2): converted _get_requests_sync to
    await _get(...). Behavior otherwise unchanged."""
    base_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    all_jobs = []
    offset = 0
    limit = 100

    while True:
        r = await _get(base_url, params={"limit": limit, "offset": offset}, headers=headers)
        if not r:
            break
        try:
            data = r.json()
        except Exception:
            break

        content = data.get("content", [])
        total = data.get("totalFound", 0)

        if not content:
            break

        for post in content:
            # Location
            loc = post.get("location") or {}
            city = loc.get("city", "")
            region = loc.get("region", "")
            country = loc.get("country", "")
            remote = loc.get("remote", False)
            location = ", ".join(filter(None, [city, region, country]))
            if remote and not location:
                location = "Remote"
            elif remote:
                location += " (Remote)"

            # Company
            company_obj = post.get("company") or {}
            company_name = company_obj.get("name", slug.replace("-", " ").title())

            # Department
            dept_obj = post.get("department") or {}
            department = dept_obj.get("label", "")

            # Employment type
            toe_obj = post.get("typeOfEmployment") or {}
            employment_type = toe_obj.get("label", "")

            # Job URL — use ref for detail, or construct careers page URL
            ref_url = post.get("ref", "")
            posting_id = post.get("id", "")
            job_url = f"https://jobs.smartrecruiters.com/{slug}/{posting_id}" if posting_id else ref_url

            workplace = "Remote" if remote else ""

            all_jobs.append({
                "title": (post.get("name") or "").strip(),
                "url": job_url,
                "company": company_name,
                "location": location,
                "country": country,
                "department": department,
                "workplace_type": workplace,
                "employment_type": employment_type,
                "salary": "",  # Not available in list endpoint
                "description_snippet": "",  # Need per-posting fetch, too slow at scale
                "source_ats": "SmartRecruiters",
                "slug": slug,
            })

        offset += limit
        if offset >= total:
            break

        # 2026-09: see module-level comment above _pace_host.
        await _pace_host_async(base_url)

    return all_jobs


# ── Taleo (Oracle legacy) ────────────────────────────────

async def scrape_taleo(slug: str) -> list[dict]:
    """Taleo REST API scraper — direct POST, no session/CSRF needed.
    Slug format: 'company|section|portal_id' or 'company|section' (portal auto-discovered)."""
    import json as _json
    parts = slug.split("|")
    if len(parts) == 3:
        company, section, portal_id = parts
    elif len(parts) == 2:
        company, section = parts
        # Auto-discover portal ID from career page
        career_url = f"https://{company}.taleo.net/careersection/{section}/jobsearch.ftl"
        r = await _get(career_url, headers={"User-Agent": random.choice(USER_AGENTS)})
        if not r:
            log.debug(f"Taleo: could not fetch career page for {company}/{section}")
            return []
        portal_match = re.search(r'portal\s*=\s*["\']?(\d+)', r.text, re.I)
        if not portal_match:
            log.debug(f"Taleo: could not extract portal ID for {company}/{section}")
            return []
        portal_id = portal_match.group(1)
        log.debug(f"Taleo: auto-discovered portal={portal_id} for {company}/{section}")
    else:
        log.debug(f"Invalid Taleo slug format: {slug}")
        return []
    base_url = f"https://{company}.taleo.net/careersection"
    api_url = f"{base_url}/rest/jobboard/searchjobs"

    all_jobs = []
    page_no = 1

    while True:
        payload = {
            "multilineEnabled": False,
            "sortingSelection": {
                "sortBySelectionParam": "1",
                "ascendingSortingOrder": "false",
            },
            "fieldData": {
                "fields": {"KEYWORD": "", "LOCATION": ""},
                "valid": True,
            },
            "filterSelectionParam": {
                "searchFilterSelections": [
                    {"id": "POSTING_DATE", "selectedValues": []},
                    {"id": "LOCATION", "selectedValues": []},
                    {"id": "JOB_FIELD", "selectedValues": []},
                    {"id": "JOB_TYPE", "selectedValues": []},
                    {"id": "JOB_SCHEDULE", "selectedValues": []},
                ]
            },
            "advancedSearchFiltersSelectionParam": {
                "searchFilterSelections": [
                    {"id": "LOCATION", "selectedValues": []},
                    {"id": "JOB_FIELD", "selectedValues": []},
                    {"id": "JOB_NUMBER", "selectedValues": []},
                    {"id": "ORGANIZATION", "selectedValues": []},
                ]
            },
            "pageNo": page_no,
        }

        headers = {
            "Content-Type": "application/json",
            "tz": "GMT-05:00",
            "User-Agent": random.choice(USER_AGENTS),
        }

        try:
            resp = await _post(
                api_url,
                params={"lang": "en", "portal": portal_id},
                headers=headers,
                data=_json.dumps(payload),
            )
            if resp is None:
                log.debug(f"Taleo: API request failed for {company}")
                break
            if resp.status_code != 200:
                log.debug(f"Taleo: API returned {resp.status_code} for {company}")
                break
            data = resp.json()
        except Exception as e:
            log.debug(f"Taleo: API request failed for {company}: {e}")
            break

        requisitions = data.get("requisitionList", [])
        if not requisitions:
            break

        for req in requisitions:
            contest_no = req.get("contestNo", "")

            # Column array: [title, location_json, posted_date]
            columns = req.get("column", [])
            title = columns[0] if len(columns) > 0 else ""
            location_raw = columns[1] if len(columns) > 1 else ""

            # Location comes as JSON string: '["United States-Iowa-Des Moines"]'
            location = location_raw
            country = ""
            try:
                loc_list = _json.loads(location_raw) if location_raw.startswith("[") else []
                if loc_list:
                    location = "; ".join(loc_list[:3])
                    # Extract country from first entry: "Country-State-City"
                    first_loc = loc_list[0]
                    loc_parts = first_loc.split("-")
                    if loc_parts:
                        country = loc_parts[0].strip()
            except Exception:
                pass

            job_url = f"{base_url}/{section}/jobdetail.ftl?job={contest_no}"

            all_jobs.append({
                "title": str(title).strip(),
                "url": job_url,
                "company": company.replace("-", " ").replace("_", " ").title(),
                "location": location,
                "country": country,
                "department": "",
                "workplace_type": "",
                "employment_type": "",
                "salary": "",
                "description_snippet": "",
                "source_ats": "Taleo",
                "slug": slug,
            })

        # Pagination
        paging = data.get("pagingData", {})
        total_count = paging.get("totalCount", 0)
        if len(all_jobs) >= total_count or not requisitions:
            break

        page_no += 1
        # 2026-09: see module-level comment above _pace_host.
        await _pace_host_async(api_url)

    return all_jobs


# ── Oracle Cloud HCM ────────────────────────────────────

_ORACLE_DISCOVERY_BUDGET_SECONDS = 90.0


async def scrape_oracle_cloud_hcm(slug: str) -> list[dict]:
    """Oracle Cloud HCM Recruiting REST API.
    Slug format: 'host_prefix|site_number' (e.g. 'eeho.fa.us2|CX_1')
    or legacy 'tenant|site_number' (e.g. 'eeho|CX_1') or tenant-only.

    2026-09 ASYNC MIGRATION (batch 2): every _get_session()/_get_requests_sync
    call converted to await _get(...) (httpx's AsyncClient is already
    constructed with follow_redirects=True globally, so the explicit
    allow_redirects=True on the domain-discovery probe below is dropped —
    same effective behavior, just the httpx-native way to express it; the
    per-call timeout= kwargs are likewise dropped since the async client's
    Timeout is already fixed at construction, same as every other
    converted scraper). Multi-strategy domain-discovery logic, brute-force
    region loop, and pagination all otherwise unchanged.

    2026-09 BUG FIX (real production evidence: a batch-2 test run's
    per-shard log froze entirely mid-Oracle-Cloud-HCM-discovery, with no
    further output for many minutes): the legacy-short-tenant discovery
    path below (Method 1: up to 4 sequential probes; Method 2: up to 11
    more) was NEVER time-bounded, even before this migration — each probe
    is a full _get() call, and _get() itself can take up to
    MAX_RETRIES+1 attempts with exponential backoff between them (worst
    case, several minutes for a single probe against a genuinely
    unresponsive/nonexistent subdomain). Chained across up to 15
    sequential probes for one tenant that never resolves, worst-case
    discovery time for a SINGLE board could run into tens of minutes —
    previously masked by lower real-world concurrency exposure, now
    fully exposed once oracle_cloud_hcm boards actually ran concurrently
    (PLATFORM_WORKERS["oracle_cloud_hcm"] = 16) against real, at-scale
    traffic for the first time. Fixed by wrapping the whole discovery
    phase (both methods) in asyncio.wait_for() with a hard wall-clock
    budget — a tenant that can't be resolved within the budget is
    treated exactly like "domain discovery failed" (returns [] the same
    way an exhausted brute-force loop already did), it just can no
    longer consume unbounded time doing it. Already-resolved slugs
    (host_prefix containing '.fa.' or a dot) skip discovery entirely and
    are completely unaffected by this change."""
    parts = slug.split("|")
    if len(parts) == 2:
        host_prefix, site_number = parts
    elif len(parts) == 1:
        host_prefix = parts[0]
        site_number = None
    else:
        log.debug(f"Invalid Oracle Cloud HCM slug format: {slug}")
        return []

    # Legacy short-tenant slugs ('eeho' / 'eeho|CX_1') require brute-force
    # domain discovery below. Once resolved this run, we cache the resolved
    # slug back to slug_registry (see call near the end of discovery) so
    # future runs skip the discovery cost entirely.
    needs_resolve = not (".fa." in host_prefix or "." in host_prefix)

    import uuid as _uuid
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "ora-irc-cx-userid": str(_uuid.uuid4()),
        "ora-irc-language": "en",
        "content-type": "application/vnd.oracle.adf.resourceitem+json;charset=utf-8",
    }

    # Build API URL — host_prefix can be 'eeho.fa.us2' (new) or 'eeho' (legacy)
    if ".fa." in host_prefix or "." in host_prefix:
        # Full host prefix like 'eeho.fa.us2' or 'idcs-xxx.identity'
        base_api = f"https://{host_prefix}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
        tenant = host_prefix.split(".")[0]
    else:
        # Legacy short tenant — discover full domain via career page redirect
        tenant = host_prefix
        base_api = None
        discovered_site_number = site_number

        async def _discover_domain() -> tuple[str | None, str | None]:
            """Method 1 + Method 2 combined, run under the wall-clock
            budget below. Returns (base_api, site_number), either of
            which may still be None if nothing was found before the
            budget or the candidate lists were exhausted."""
            nonlocal_base_api = None
            nonlocal_site_number = discovered_site_number

            # Method 1: Hit career page, follow redirects, extract real domain
            for try_site in (nonlocal_site_number or "CX_1", "CX_1", "CX", "CX_2"):
                try:
                    probe_url = f"https://{tenant}.oraclecloud.com/hcmUI/CandidateExperience/en/sites/{try_site}/requisitions"
                    probe_r = await _get(probe_url, headers={
                        "User-Agent": random.choice(USER_AGENTS)})
                    if probe_r is None:
                        continue
                    # Check if we got redirected to a URL with .fa.{region}
                    final_host = str(probe_r.url).split("/")[2] if probe_r.url else ""
                    if ".fa." in final_host and "oraclecloud.com" in final_host:
                        real_prefix = final_host.replace(".oraclecloud.com", "")
                        nonlocal_base_api = f"https://{real_prefix}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
                        if not nonlocal_site_number:
                            nonlocal_site_number = try_site
                        log.debug(f"Oracle Cloud HCM: discovered domain={real_prefix} via redirect for {tenant}")
                        break
                except Exception:
                    continue

            # Method 2: Brute-force common regions via API
            if not nonlocal_base_api:
                for region in ("fa.us2", "fa.us6", "fa.us1", "fa.em2", "fa.em3", "fa.em4",
                               "fa.ap1", "fa.ap2", "fa.ca1", "fa.sa1", "fa.me1"):
                    test_url = f"https://{tenant}.{region}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
                    try:
                        test_r = await _get(test_url,
                            params={"onlyData": "true", "finder": f"findReqs;siteNumber={nonlocal_site_number or 'CX_1'},limit=1,offset=0"},
                            headers=headers)
                        if test_r is not None and test_r.status_code == 200:
                            try:
                                data = test_r.json()
                                items = data.get("items", [])
                                if items and items[0].get("requisitionList"):
                                    nonlocal_base_api = test_url
                                    if not nonlocal_site_number:
                                        nonlocal_site_number = "CX_1"
                                    log.debug(f"Oracle Cloud HCM: discovered region={region} for {tenant}")
                                    break
                            except Exception:
                                continue
                    except Exception:
                        continue

            return nonlocal_base_api, nonlocal_site_number

        try:
            base_api, discovered_site_number = await asyncio.wait_for(
                _discover_domain(), timeout=_ORACLE_DISCOVERY_BUDGET_SECONDS
            )
        except asyncio.TimeoutError:
            log.debug(
                f"Oracle Cloud HCM: domain discovery for {tenant} exceeded "
                f"{_ORACLE_DISCOVERY_BUDGET_SECONDS:.0f}s budget — giving up "
                f"(this tenant's subdomains are likely unresponsive, not "
                f"genuinely nonexistent; a future run will retry from scratch "
                f"since nothing gets cached on a timeout)"
            )
            base_api = None
        site_number = discovered_site_number

        if not base_api:
            log.debug(f"Oracle Cloud HCM: could not discover domain for {tenant}")
            return []

    # Auto-discover site number for tenant-only slugs
    if not site_number:
        for try_site in ("CX_1", "CX", "CX_2", "CX_3"):
            test_params = {
                "onlyData": "true",
                "finder": f"findReqs;siteNumber={try_site},limit=1,offset=0",
            }
            test_r = await _get(base_api, params=test_params, headers=headers)
            if test_r and test_r.status_code == 200:
                try:
                    test_data = test_r.json()
                    test_items = test_data.get("items", [])
                    if test_items and test_items[0].get("requisitionList"):
                        site_number = try_site
                        log.debug(f"Oracle Cloud HCM: auto-discovered site={site_number} for {tenant}")
                        break
                except Exception:
                    continue
        if not site_number:
            log.debug(f"Oracle Cloud HCM: could not discover site number for {tenant}")
            return []

    # Cache the fully-resolved slug so future runs skip discovery entirely.
    # base_api looks like 'https://eeho.fa.us2.oraclecloud.com/hcmRestApi/...'
    # so the resolved host prefix is everything between 'https://' and
    # '.oraclecloud.com'.
    if needs_resolve and base_api:
        try:
            resolved_prefix = base_api.split("://", 1)[1].split(".oraclecloud.com")[0]
            new_slug = f"{resolved_prefix}|{site_number}"
            from supabase_handler import resolve_oracle_slug
            resolve_oracle_slug(slug, new_slug)
        except Exception as e:
            log.debug(f"Oracle Cloud HCM: slug caching skipped for {slug!r}: {e}")

    all_jobs = []
    offset = 0
    limit = 25

    while True:
        params = {
            "onlyData": "true",
            "expand": "requisitionList.workLocation",
            "finder": f"findReqs;siteNumber={site_number},limit={limit},offset={offset}",
        }

        r = await _get(base_api, params=params, headers=headers)
        if not r:
            log.debug(f"Oracle Cloud HCM: API request failed for {tenant}/{site_number} offset={offset}")
            break

        try:
            data = r.json()
        except Exception as e:
            log.debug(f"Oracle Cloud HCM: JSON parse failed for {tenant}/{site_number}: {e}")
            break

        items = data.get("items", [])
        if not items:
            break

        # The requisition list is nested inside the first item
        first_item = items[0] if items else {}
        req_list = first_item.get("requisitionList", [])

        if not req_list:
            break

        for req in req_list:
            title = req.get("Title", "")
            job_id = req.get("Id", "")
            primary_location = req.get("PrimaryLocation", "")
            categories = req.get("CategoriesDisplay", "")
            workplace_type = req.get("WorkplaceTypeDisplay", "")
            description_html = req.get("ExternalDescriptionStr", "")

            desc = _snippet(description_html)
            salary = _extract_salary(desc)

            # Build job URL — extract host from base_api
            api_host = base_api.split("/hcmRestApi")[0]
            job_url = (
                f"{api_host}/hcmUI/CandidateExperience"
                f"/en/sites/{site_number}/job/{job_id}"
            )

            # Try to extract country from location
            country = ""
            if primary_location:
                loc_parts = [p.strip() for p in primary_location.split(",")]
                if len(loc_parts) >= 2:
                    country = loc_parts[-1]

            all_jobs.append({
                "title": str(title).strip(),
                "url": job_url,
                "company": tenant.replace("-", " ").replace("_", " ").title(),
                "location": primary_location,
                "country": country,
                "department": categories,
                "workplace_type": workplace_type,
                "employment_type": "",
                "salary": salary,
                "description_snippet": desc,
                "source_ats": "Oracle Cloud HCM",
                "slug": slug,
            })

        # Check pagination
        has_more = first_item.get("hasMore", False)
        total_count = first_item.get("totalCount", 0) or first_item.get("count", 0)

        if not has_more and total_count and len(all_jobs) >= total_count:
            break
        if not has_more and not total_count:
            # If no hasMore flag and no total, check if we got fewer than limit
            if len(req_list) < limit:
                break

        offset += limit
        # 2026-09: see module-level comment above _pace_host.
        await _pace_host_async(base_api)

    return all_jobs


# ── BrassRing (IBM/Infinite) ─────────────────────────────
# 2026-09: re-enabled after live investigation. This was previously
# disabled under a generic "JS-rendered / auth-required / blocked /
# robots.txt" comment shared with several other platforms, but none of
# those specific reasons actually held up for BrassRing: sjobs.brassring.com
# has no robots.txt at all (confirmed 404), the target endpoint is a real
# JSON API (not a client-side-JS-only page), and there's no login/auth
# wall in front of it. The real reason it was returning nothing: the code
# below POSTed straight to the AJAX search endpoint without ever loading
# the Home page first, so it carried no session cookies — BrassRing's
# search endpoint 500s on a cookie-less request. Fixed by priming the
# session (one GET to Search/Home/Home) before the POST, same as a real
# browser session would do.
async def scrape_brassring(slug: str) -> list[dict]:
    """BrassRing public Talent Gateway scraper.

    Accepts the historical ``partner_id|site_id`` identifier and, when
    discovery preserves it, ``host|partner_id|site_id``.  BrassRing has
    multiple production front doors (US/EU/China and Akamai/non-Akamai), so
    a bare partner/site pair cannot uniquely identify the correct host.

    Strategy, in order:
      1. Try the host preserved by discovery, if present.
      2. Otherwise probe the documented production BrassRing front doors.
      3. Prime the Talent Gateway session.
      4. Try current TGnewUI search HTML and embedded job records.
      5. Try the older MatchedJobs JSON endpoint as a compatibility path.

    We never accept a 200 response by itself as success: the response must
    contain identifiable job records.
    """
    parts = slug.split("|")
    if len(parts) == 3:
        supplied_host, partner_id, site_id = parts
        supplied_host = supplied_host.strip()
        if supplied_host.startswith("http"):
            supplied_host = urlparse(supplied_host).netloc
        supplied_host = supplied_host.strip().lower()
    elif len(parts) == 2:
        supplied_host, partner_id, site_id = "", parts[0].strip(), parts[1].strip()
    else:
        raise RuntimeError(f"BrassRing: invalid board slug {slug!r}; expected partner_id|site_id or host|partner_id|site_id")
    if not partner_id or not site_id:
        raise RuntimeError(f"BrassRing: invalid partner/site IDs in {slug!r}")

    production_hosts = [
        "sjobs.brassring.com",      # US production, non-Akamai
        "xjobs.brassring.com",      # US production, Akamai
        "krb-sjobs.brassring.com",  # EU production, non-Akamai
        "krb-xjobs.brassring.com",  # EU production, Akamai
        "krbcn-sjobs.brassring.com",# China production
    ]
    hosts = []
    if supplied_host:
        hosts.append(supplied_host)
    for h in production_hosts:
        if h not in hosts:
            hosts.append(h)

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    }

    def _job_dict(host: str, jid: str, title: str = "", location: str = "", desc: str = "", department: str = "", employment: str = "") -> dict:
        job_url = (f"https://{host}/TGnewUI/Search/home/HomeWithPreLoad"
                   f"?PageType=JobDetails&partnerid={partner_id}&siteid={site_id}&jobid={jid}")
        return {
            "title": title or f"BrassRing job {jid}",
            "url": job_url,
            "company": partner_id,
            "location": location,
            "country": location.split(",")[-1].strip() if "," in location else "",
            "department": department,
            "workplace_type": "",
            "employment_type": employment,
            "salary": _extract_salary(desc),
            "description_snippet": desc,
            "source_ats": "BrassRing",
            "slug": slug,
        }

    def _parse_html(html: str, base_url: str, host: str) -> list[dict]:
        found, seen = [], set()
        # Search both visible anchors and embedded JSON/template state.  The
        # current TGnewUI often renders the card client-side, so anchors are
        # not guaranteed to exist in the raw response.
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = str(a.get("href") or "")
            title = _text(a.get_text(" ", strip=True))
            if not title or len(title) < 2:
                continue
            m = re.search(r"(?:[?&](?:jobid|jobId|Areq|reqid)=)([^&#]+)", href, re.I)
            if not m:
                m = re.search(r"#jobDetails=([^_&#]+)", href, re.I)
            if not m:
                m = re.search(r"/(?:job|jobs)/(\d+[A-Za-z0-9_-]*)", href, re.I)
            if not m:
                continue
            jid = _text(m.group(1))
            if not jid or jid.lower() in {"home", "search"}:
                continue
            container = a
            blob = title
            for _ in range(6):
                container = container.parent
                if container is None:
                    break
                candidate = _text(container.get_text(" ", strip=True))
                if len(candidate) >= len(title) + 5 and len(candidate) < 5000:
                    blob = candidate
                    break
            loc = ""
            lm = re.search(r"(?:Location|Job Location)\s*[:|-]\s*([^|]{2,160})", blob, re.I)
            if lm:
                loc = _text(lm.group(1))
            key = jid
            if key not in seen:
                seen.add(key)
                found.append(_job_dict(host, jid, title, loc))

        # Embedded JSON / JS state.  Do not require a specific schema because
        # BrassRing has changed the Angular payload shape between deployments.
        if not found:
            id_patterns = (
                r'"(?:AutoReqId|JobId|jobId|jobid|Areq|requisitionId)"\s*:\s*"?([A-Za-z0-9_-]+)',
                r"(?:jobid|jobId|Areq)[=:'\"]+([A-Za-z0-9_-]+)",
            )
            for pat in id_patterns:
                for m in re.finditer(pat, html, re.I):
                    jid = _text(m.group(1))
                    if not jid or jid in seen:
                        continue
                    seen.add(jid)
                    found.append(_job_dict(host, jid))
        return found

    timeout = httpx.Timeout(connect=15, read=max(45.0, float(REQUEST_TIMEOUT)), write=20, pool=20)
    last_reason = "no candidate host produced job records"
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, http2=True) as client:
        for host in hosts:
            base = f"https://{host}"
            home_url = f"{base}/TGnewUI/Search/home/Home"
            search_url = f"{base}/TGnewUI/Search/home/HomeWithPreLoad"
            try:
                prime = await client.get(home_url, params={"partnerid": partner_id, "siteid": site_id}, headers=headers)
                if prime.status_code >= 400:
                    last_reason = f"{host}: home HTTP {prime.status_code}"
                    continue

                # Current public search-page forms. Try both query spellings
                # seen in indexed/live TGnewUI URLs.
                candidates = [
                    {"partnerid": partner_id, "siteid": site_id, "PageType": "searchResults", "SearchType": "linkquery"},
                    {"partnerid": partner_id, "siteid": site_id, "PageType": "searchResults", "SearchType": "linkquery", "keyWordSearch": "", "locationSearch": ""},
                ]
                for params in candidates:
                    r = await client.get(search_url, params=params, headers={**headers, "Referer": str(prime.url)})
                    if r.status_code >= 400:
                        last_reason = f"{host}: search HTTP {r.status_code}"
                        continue
                    jobs = await asyncio.to_thread(_parse_html, r.text, str(r.url), host)
                    if jobs:
                        return jobs

                # Legacy JSON endpoint. It is not assumed current, but it is
                # still worth probing after the correct host/session is known.
                for endpoint in ("/TGnewUI/Search/Ajax/MatchedJobs", "/TgNewUI/Search/Ajax/MatchedJobs"):
                    ajax = await client.post(
                        base + endpoint,
                        data={"partnerid": partner_id, "siteid": site_id, "keyword": "", "location": "", "pagenum": "1", "sortBy": "posteddate", "SortType": "desc"},
                        headers={**headers, "Accept": "application/json, text/javascript, */*; q=0.01", "X-Requested-With": "XMLHttpRequest", "Origin": base, "Referer": str(prime.url)},
                    )
                    if ajax.status_code != 200:
                        last_reason = f"{host}: {endpoint} HTTP {ajax.status_code}"
                        continue
                    try:
                        data = ajax.json()
                    except Exception:
                        continue
                    arr = data.get("Jobs") if isinstance(data, dict) else None
                    if not isinstance(arr, list) or not arr:
                        continue
                    out = []
                    for j in arr:
                        if not isinstance(j, dict):
                            continue
                        jid = _text(j.get("AutoReqId") or j.get("JobId") or j.get("Areq"))
                        title = _text(j.get("JobTitle") or j.get("Title"))
                        if not jid or not title:
                            continue
                        desc = _snippet(j.get("formattedShortDescription") or j.get("Description") or "")
                        out.append(_job_dict(host, jid, title, _text(j.get("JobInfo1") or j.get("Location")), desc,
                                             _text(j.get("JobInfo3") or j.get("Department")), _text(j.get("JobInfo2") or j.get("EmploymentType"))))
                    if out:
                        return out
            except Exception as e:
                last_reason = f"{host}: {type(e).__name__}: {e}"
                log.debug("BrassRing host %s failed for %s: %s", host, slug, e)
                continue

    raise RuntimeError(f"BrassRing: no job records found for {slug}; {last_reason}")


# ── Teamtailor ───────────────────────────────────────────

def scrape_teamtailor(slug: str) -> list[dict]:
    """Teamtailor RSS feed scraper with HTML fallback.
    Slug is the company subdomain (e.g. 'spotify').
    RSS feed includes tt: namespace with structured location data."""
    company_name = slug.capitalize()
    rss_url = f"https://{slug}.teamtailor.com/jobs.rss"

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/rss+xml, application/xml, text/xml",
    }

    # Teamtailor XML namespace for structured location/department data
    TT_NS = {"tt": "https://teamtailor.com/locations"}

    # ── Primary: RSS feed ──
    r = _get_requests_sync(rss_url, headers=headers)
    if r and r.status_code == 200:
        try:
            root = ET.fromstring(r.content)
            jobs = []

            for item in root.iter("item"):
                title_el = item.find("title")
                link_el = item.find("link")
                desc_el = item.find("description")
                category_el = item.find("category")

                title = (title_el.text or "").strip() if title_el is not None else ""
                link = (link_el.text or "").strip() if link_el is not None else ""
                desc_html = (desc_el.text or "") if desc_el is not None else ""

                # ── Department: prefer tt:department, fallback to <category> ──
                tt_dept = item.findtext("tt:department", default=None, namespaces=TT_NS)
                department = (tt_dept or "").strip() if tt_dept else ""
                if not department:
                    department = (category_el.text or "").strip() if category_el is not None else ""

                # ── Location: parse tt:locations namespace ──
                location_parts = []
                country = ""
                remote_status = (item.findtext("remoteStatus") or "").strip()
                for loc_el in item.findall("tt:locations/tt:location", TT_NS):
                    # Prefer tt:name (pre-formatted), fallback to city+country
                    loc_name = (loc_el.findtext("tt:name", namespaces=TT_NS) or "").strip()
                    if loc_name:
                        location_parts.append(loc_name)
                    else:
                        city = (loc_el.findtext("tt:city", namespaces=TT_NS) or "").strip()
                        ctry = (loc_el.findtext("tt:country", namespaces=TT_NS) or "").strip()
                        combined = ", ".join(p for p in [city, ctry] if p)
                        if combined:
                            location_parts.append(combined)
                    # Capture country from first location
                    if not country:
                        country = (loc_el.findtext("tt:country", namespaces=TT_NS) or "").strip()

                location = "; ".join(location_parts[:3]) if location_parts else ""

                # If remote_status is set, append it
                if remote_status and remote_status.lower() != "none":
                    if location:
                        location = f"{location} ({remote_status})"
                    else:
                        location = remote_status.capitalize()

                desc = _snippet(desc_html)
                salary = _extract_salary(desc)

                jobs.append({
                    "title": title,
                    "url": link,
                    "company": company_name,
                    "location": location,
                    "country": country,
                    "department": department,
                    "workplace_type": remote_status if remote_status and remote_status.lower() != "none" else "",
                    "employment_type": "",
                    "salary": salary,
                    "description_snippet": desc,
                    "source_ats": "Teamtailor",
                    "slug": slug,
                })

            if jobs:
                return jobs
        except ET.ParseError:
            log.debug(f"Teamtailor: RSS XML parse failed for {slug}, trying HTML fallback")

    # ── Fallback: HTML scrape ──
    html_url = f"https://{slug}.teamtailor.com/jobs"
    r = _get_requests_sync(html_url, headers=headers)
    if not r:
        return []

    jobs = []
    # Job links follow pattern: /jobs/{id}-{slug-title}
    for match in re.finditer(r'href=["\'](/jobs/(\d+)-[^"\']+)["\']', r.text):
        path = match.group(1)
        job_url = f"https://{slug}.teamtailor.com{path}"

        # Derive title from the slug portion of the URL
        slug_part = path.split("/jobs/")[-1] if "/jobs/" in path else ""
        # Remove the numeric prefix: "12345-some-job-title" -> "some-job-title"
        title_slug = re.sub(r"^\d+-", "", slug_part)
        title = title_slug.replace("-", " ").strip().title()

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": "",
            "country": "",
            "department": "",
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "Teamtailor",
            "slug": slug,
        })

    # Deduplicate by URL (HTML may have repeated links)
    seen_urls = set()
    unique_jobs = []
    for j in jobs:
        if j["url"] not in seen_urls:
            seen_urls.add(j["url"])
            unique_jobs.append(j)

    return unique_jobs


# ── SAP SuccessFactors (successfactors) ──────────────────────────────
# 2026-09: REVERSED out of "genuinely blocked" — see discovery.py's
# SUPPORTED_ATS comment and node.py's _detect_successfactors_hit for the
# full live-verified evidence trail, and GREYLIST_ATS.md for the writeup.
# The old verdict (robots.txt disallow on every checked live host) was
# actually correct for what it tested: the legacy SHARED-host tenants
# (career{N}.successfactors.com/.eu, sapsf.com/.eu, jobs2web.com) — still
# confirmed live this session that those genuinely disallow the whole
# site (e.g. career2.successfactors.eu). Those stay unscraped, and stay
# out of SUPPORTED_ATS/SCRAPERS.
#
# What the old verdict never anticipated: a modern Career Site Builder
# (CSB) tenant runs on the CUSTOMER's OWN branded domain (careers.swissre.com,
# jobs.sap.com, ...) with its own separate robots.txt — confirmed live on
# two independent such tenants that:
#   - /search/?page=N is plain paginated, server-rendered HTML (a results
#     count string like "Results 1-25 of 288", "Page 1 of 36"), giving
#     each job's title, URL (/job/{slug}/{numeric id}/), location, and
#     posted date — but NO description snippet, so every job needs the
#     second-pass enrichment fetch below.
#   - /job/{slug}/{id}/ has the FULL, untruncated description
#     server-rendered — no JS, no API call, no auth needed at all.
#   - Neither path appeared in either tenant's robots.txt disallow list.
#     The one real hidden API (`POST {origin}/services/recruiting/v1/jobs`,
#     which does exist and does work) IS robots.txt-disallowed on both
#     tenants tested (`Disallow: /services/`) — deliberately NOT used
#     here, per this project's non-negotiable robots.txt rule.
#   - A multi-locale tenant can report wildly different job counts per
#     locale (confirmed live on jobs.sap.com: de_DE/en_US/fr_FR/ja_JP/
#     zh_CN all present, each a completely different job set) — so this
#     queries every locale /search/ itself advertises via its own
#     language-switcher links, deduplicating by job ID.
#   - Since each tenant is the customer's own domain (not a shared vendor
#     host verified once for everyone), robots.txt is checked LIVE per
#     tenant here — unlike every other scraper in this file.
#
# Its ~866 already-discovered (and unscrapeable at the time) archive_i
# rows were deleted 2026-09 — those were all legacy-host discoveries from
# before this reversal, so no migration/backfill applies to them.

_SF_JOB_ROW_RE = re.compile(
    r'<a[^>]+href="(/job/[^"?#]+?/(\d{5,})/?)"[^>]*>(.*?)</a>(.{0,400}?)'
    r'(?=<a[^>]+href="/job/|\Z)',
    re.I | re.S,
)
_SF_LOCALE_RE = re.compile(r'[?&]locale=([a-z]{2}_[A-Z]{2})\b')
_SF_TRAILING_DATE_RE = re.compile(r'\s*\d{1,2}\s+[A-Za-z]{3,9}\.?\s+\d{4}\s*$')
# Some CSB tenants (confirmed live 2026-09 on career.sonepar.com) render
# each search-result card as ONE <a> wrapping title+location+date together,
# instead of title-inside-the-anchor/location-after-it as the row regex
# above assumes. When that happens, title_html swallows the whole card and
# trailer_html is empty, so the plain "strip tags" fallback below produces
# a garbled concatenated title and a blank location — exactly the Sonepar
# DB row this was diagnosed from. SAP's default Career Site Builder
# widgets commonly tag individual fields with a
# data-careersite-propertyid="..." attribute (title / city / postingDate
# are the common values); when present, this lets a tenant's field be
# extracted precisely regardless of how the fields are nested/ordered.
# This is a best-effort improvement based on that common CSB pattern, NOT
# confirmed against this specific tenant's raw markup (this environment
# can't fetch raw third-party HTML) — it only changes behavior when the
# attribute is actually found, so a tenant without it keeps the exact
# prior (already-working) behavior unchanged.
_SF_PROPID_RE = re.compile(
    r'data-careersite-propertyid=["\'](\w+)["\'][^>]*>(.*?)<', re.I | re.S
)
_SF_MAX_LOCALES = 10
_SF_MAX_PAGES_PER_LOCALE = 200
# SAP's own standard CSB template ends every job's real content with this
# kind of boilerplate before unrelated site chrome/footer — confirmed
# live verbatim on a real posting ("Reference Code: 138425", "Job
# Segment: ...", "Apply now »", "Find similar jobs:"). Used to trim the
# full page down to just the actual description in the enrichment fetch
# below; a tenant that customizes this wording just falls back to the
# untrimmed (but still real, still complete) page text.
_SF_DESC_END_MARKERS = ("Apply now", "Reference Code:", "Find similar jobs:")

_sf_robots_cache: dict[str, "urllib.robotparser.RobotFileParser | None"] = {}
_sf_robots_lock = threading.Lock()


async def _sf_robots_parser(origin: str) -> "urllib.robotparser.RobotFileParser | None":
    """Per-tenant robots.txt, cached per origin. Returns None (treated as
    allow-all below, same convention every other scraper here uses when a
    platform has no robots.txt at all) if it can't be fetched/parsed."""
    with _sf_robots_lock:
        if origin in _sf_robots_cache:
            return _sf_robots_cache[origin]
    rp = None
    r = await _get(f"{origin}/robots.txt")
    if r is not None:
        rp = urllib.robotparser.RobotFileParser()
        try:
            rp.parse(r.text.splitlines())
        except Exception:
            rp = None
    with _sf_robots_lock:
        _sf_robots_cache[origin] = rp
    return rp


async def _sf_robots_allows(origin: str, path: str) -> bool:
    rp = await _sf_robots_parser(origin)
    if rp is None:
        return True
    try:
        return rp.can_fetch("*", f"{origin}{path}")
    except Exception:
        return True


async def scrape_successfactors(slug: str) -> list[dict]:
    """SAP SuccessFactors — Career Site Builder tenants only (legacy
    shared-host tenants stay unscraped, see block comment above). Slug is
    the tenant's own branded host (e.g. 'careers.swissre.com') — there's
    no vendor domain suffix or per-tenant ID to extract the way every
    other platform here has; discovery happens via node.py's
    _detect_successfactors_hit content-fingerprint check instead of a
    URL_TO_SLUG converter.

    2026-09 ASYNC MIGRATION (batch 2): _sf_robots_parser/_sf_robots_allows
    and the nested _scrape_locale helper all converted to async def,
    awaiting _get(...) instead of calling _get_requests_sync. Behavior
    otherwise unchanged."""
    host = (slug or "").strip().lower()
    if not host or "/" in host or " " in host:
        log.debug(f"Invalid SuccessFactors (successfactors) slug format: {slug}")
        return []
    origin = f"https://{host}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    if not await _sf_robots_allows(origin, "/search/"):
        log.debug(f"SuccessFactors: {host} disallows /search/ via robots.txt; skipping")
        return []

    r = await _get(f"{origin}/search/", headers=headers)
    if r is None:
        return []
    locales = list(dict.fromkeys(_SF_LOCALE_RE.findall(r.text)))[:_SF_MAX_LOCALES]

    seen_ids: set[str] = set()
    jobs: list[dict] = []

    async def _scrape_locale(locale: str | None):
        page = 1
        while page <= _SF_MAX_PAGES_PER_LOCALE:
            params = {"page": page}
            if locale:
                params["locale"] = locale
            resp = await _get(f"{origin}/search/", headers=headers, params=params)
            if resp is None:
                break
            matches = list(_SF_JOB_ROW_RE.finditer(resp.text))
            if not matches:
                break
            for m in matches:
                job_path, job_id, title_html, trailer_html = m.groups()
                if job_id in seen_ids:
                    continue
                seen_ids.add(job_id)
                title = unescape(re.sub(r"<[^>]+>", "", title_html)).strip()
                trailer = unescape(re.sub(r"<[^>]+>", " ", trailer_html))
                trailer = re.sub(r"\s+", " ", trailer).strip()
                location = _SF_TRAILING_DATE_RE.sub("", trailer).strip()

                # Whole-card-in-one-<a> tenants (see _SF_PROPID_RE comment):
                # title_html then holds title+location+date glued together
                # and trailer_html is empty, which the block above turns
                # into a garbled title and a blank location. If this card
                # carries SAP's data-careersite-propertyid tags anywhere in
                # it, prefer those for an exact split instead.
                full_card_html = title_html + trailer_html
                prop_fields = {
                    key.lower(): unescape(re.sub(r"<[^>]+>", "", val)).strip()
                    for key, val in _SF_PROPID_RE.findall(full_card_html)
                }
                if prop_fields.get("title"):
                    title = prop_fields["title"]
                prop_location = (
                    prop_fields.get("city")
                    or prop_fields.get("location")
                    or prop_fields.get("joblocation")
                )
                if prop_location:
                    location = prop_location
                jobs.append({
                    "title": title,
                    "url": f"{origin}{job_path}",
                    "company": "",
                    "location": location,
                    "country": "",
                    "department": "",
                    "workplace_type": "",
                    "employment_type": "",
                    "salary": "",
                    # search page never shows a snippet — see block
                    # comment above; always enriched via
                    # _fetch_successfactors_description.
                    "description_snippet": "",
                    "source_ats": "SuccessFactors",
                    "slug": slug,
                })
            page += 1
            # 2026-09: see module-level comment above _pace_host.
            await _pace_host_async(origin)

    await _scrape_locale(None)
    for loc in locales:
        await _scrape_locale(loc)

    return jobs


def _fetch_successfactors_description(job: dict) -> str:
    """Full description is server-rendered directly on the /job/ page
    (see scrape_successfactors's block comment) — fetches it and trims
    from the job title down to just before SAP's own standard
    post-content boilerplate (see _SF_DESC_END_MARKERS).

    2026-09: also backfills job["location"] as a side-effect when it's
    still blank after the listing-page parse (mirrors what
    _fetch_generic_description already does for every OTHER platform
    registered in DESCRIPTION_FETCHERS — this scraper's own fetcher never
    had that side-effect, so a blank listing-page location for a
    SuccessFactors job had no second chance to be recovered, unlike every
    other platform's enrichment path). Tries the detail page's own
    JSON-LD JobPosting data first via _extract_location_from_html — SAP's
    Career Site Builder job pages often carry this even when the search
    results page's HTML doesn't expose location cleanly — before falling
    back to whatever the listing page already produced."""
    url = job.get("url", "")
    if not url:
        return job.get("description_snippet", "")
    r = _get_requests_sync(url, headers={"User-Agent": random.choice(USER_AGENTS)})
    if r is None:
        return job.get("description_snippet", "")
    if not job.get("location"):
        loc = _extract_location_from_html(r.text)
        if loc:
            job["location"] = loc
            job["location_status"] = "extracted_from_detail_page"
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        h1 = soup.find("h1")
        title_text = h1.get_text(strip=True) if h1 else (job.get("title") or "")
        text = soup.get_text("\n", strip=True)
        start = text.find(title_text) if title_text else -1
        body = text[start:] if start != -1 else text
        end = len(body)
        for marker in _SF_DESC_END_MARKERS:
            idx = body.find(marker)
            if idx != -1:
                end = min(end, idx)
        description = body[:end].strip()
        return _snippet(description) if description else job.get("description_snippet", "")
    except Exception:
        return job.get("description_snippet", "")

# ── BreezyHR ────────────────────────────────────────────

def scrape_breezyhr(slug: str) -> list[dict]:
    """BreezyHR — HTML scrape, parses position list items.
    Slug is the company subdomain (e.g. 'acme').
    Extracts location from <li class="location"> and title from <h2>."""
    company_name = slug.replace("-", " ").title()
    base_url = f"https://{slug}.breezy.hr"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = _get_requests_sync(base_url, headers=headers)
    if not r:
        return []

    jobs = []
    seen_urls = set()

    # BreezyHR HTML structure:
    # <li class="position transition">
    #   <a href="/p/<id>-<slug>"><h2>Title</h2>
    #     <ul class="meta">
    #       <li class="location"><span class="polygot">Location</span></li>
    #       <li class="type"><span class="polygot">Full-Time</span></li>
    #     </ul>
    #   </a>
    # </li>
    # Match each position block
    for pos_match in re.finditer(
        r'<li[^>]*class="[^"]*position[^"]*"[^>]*>(.*?)</li>\s*(?=<li[^>]*class="[^"]*position|</ul>|$)',
        r.text, re.I | re.DOTALL
    ):
        block = pos_match.group(1)

        # Extract URL
        href_match = re.search(r'href=["\'](/p/[a-f0-9]+[-/][^"\']+)["\']', block)
        if not href_match:
            continue
        job_url = f"{base_url}{href_match.group(1)}"
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        # Extract title from <h2>
        title_match = re.search(r'<h2[^>]*>(.*?)</h2>', block, re.I | re.DOTALL)
        title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip() if title_match else ""

        # Extract location from <li class="location">
        loc_match = re.search(
            r'<li[^>]*class="[^"]*location[^"]*"[^>]*>(.*?)</li>',
            block, re.I | re.DOTALL
        )
        location = ""
        if loc_match:
            # Strip HTML tags, get text content
            loc_text = re.sub(r'<[^>]+>', ' ', loc_match.group(1)).strip()
            # Clean up multiple spaces
            location = re.sub(r'\s+', ' ', loc_text).strip()

        # Extract employment type
        type_match = re.search(
            r'<li[^>]*class="[^"]*type[^"]*"[^>]*>(.*?)</li>',
            block, re.I | re.DOTALL
        )
        emp_type = ""
        if type_match:
            emp_type = re.sub(r'<[^>]+>', ' ', type_match.group(1)).strip()
            emp_type = re.sub(r'\s+', ' ', emp_type).strip()

        # Try to extract country from location (e.g. "Berlin, Germany")
        country = ""
        if location:
            loc_parts = [p.strip() for p in location.split(",")]
            if len(loc_parts) >= 2:
                country = loc_parts[-1]

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": location,
            "country": country,
            "department": "",
            "workplace_type": "",
            "employment_type": emp_type,
            "salary": "",
            "description_snippet": "",
            "source_ats": "BreezyHR",
            "slug": slug,
        })

    # Fallback: if no position blocks found, try simple link extraction
    if not jobs:
        for match in re.finditer(
            r'href=["\'](/p/([a-f0-9]+)[-/]([^"\']+))["\']', r.text
        ):
            path = match.group(1)
            job_url = f"{base_url}{path}"
            if job_url in seen_urls:
                continue
            seen_urls.add(job_url)
            title_slug = match.group(3).rstrip("/")
            title = title_slug.replace("-", " ").strip().title()
            jobs.append({
                "title": title, "url": job_url, "company": company_name,
                "location": "", "country": "", "department": "",
                "workplace_type": "", "employment_type": "", "salary": "",
                "description_snippet": "", "source_ats": "BreezyHR", "slug": slug,
            })

    return jobs


# ── JazzHR (formerly "ApplyToJob"; REMOVED 2026-08, REVIVED 2026-09) ──
#
# 2026-08 removal reason: a live posting requiring US work authorization
# ("Client Engagement Representative — Remote") got past the classifier
# despite this platform being registered for full-description enrichment
# via _fetch_generic_description — the generic JD fetch wasn't reliably
# catching real disqualifying language on this platform's pages.
#
# 2026-09 revival reasoning: the listing scraper below was never the
# problem (it's unchanged from 2026-08) — the failure was downstream, in
# JD enrichment + eligibility detection. Both of those have since been
# rewritten for unrelated reasons: _fetch_generic_description gained
# JSON-LD/embedded-JSON/itemprop/container fallbacks it didn't have
# before (see its own docstring), and classifier.py's
# detect_visa_sponsorship was rewritten 2026-09 specifically citing the
# same shape of miss ("must be authorized to work in the US; not able to
# sponsor visas" reading as globally-open) as the bug it fixed. Reviving
# on the strength of those two independently-documented fixes — could NOT
# live-verify this exact combination on a real JazzHR posting this session
# (no browser tool connected, direct fetch to applytojob.com blocked from
# this sandbox). Spot-check early output for eligibility-language leaks.
# See discovery.py's SUPPORTED_ATS comment for the same history.

def scrape_jazzhr(slug: str) -> list[dict]:
    """JazzHR (formerly branded ApplyToJob) — HTML scrape, parses job
    listings. Slug is the company subdomain (e.g. 'acme').
    Extracts location from fa-map-marker icons. Deduplicates by title."""
    company_name = slug.replace("-", " ").title()
    base_url = f"https://{slug}.applytojob.com"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = _get_requests_sync(base_url, headers=headers)
    if not r:
        return []

    jobs = []
    seen_urls = set()
    seen_titles = set()  # deduplicate by title (companies post same job many times)

    # Pattern 1: Newer layout — list-group-item with heading + location icon
    # HTML: <li class="list-group-item">
    #         <h3 class="list-group-item-heading"><a href="...">Title</a></h3>
    #         <ul class="list-group-item-text">
    #           <li><i class="fa fa-map-marker"></i>Location</li>
    #         </ul>
    #       </li>
    #
    # 2026-09: migrated from regex item-splitting + a bare-adjacency
    # location capture to BeautifulSoup. The regex version's location
    # capture (`></i>\s*([^<]+)`) required the location text to be plain
    # text immediately after the closing </i> with zero intervening
    # markup — confirmed live broken on inabia.applytojob.com (Senior
    # Project Manager – ITSM/ServiceNow) where the actual board wraps the
    # text in extra markup, silently producing location="". A DOM parser
    # doesn't have this problem: once the map-marker icon element is
    # located, `.parent.get_text()` correctly picks up its sibling text
    # regardless of how deeply it's nested or wrapped, with no adjacency
    # assumption and no arbitrary window-size limit.
    soup = BeautifulSoup(r.text, "html.parser")
    for item in soup.select("li.list-group-item"):
        heading = item.select_one(".list-group-item-heading a[href]")
        if not heading:
            continue
        url = (heading.get("href") or "").strip()
        title = heading.get_text(strip=True)
        if not url or not title:
            continue
        if not url.startswith("http"):
            url = base_url + url

        location = ""
        location_status = "marker_not_found"
        icon = item.select_one("i.fa-map-marker, i[class*='fa-map-marker']")
        if icon is not None:
            # The location text is the icon's own tail text plus any
            # sibling elements' text within its immediate container —
            # .parent.get_text() naturally covers both "bare text right
            # after the icon" and "text wrapped in a further <span>".
            container = icon.parent or icon
            text = container.get_text(" ", strip=True)
            location = text.strip()
            location_status = "extracted" if location else "marker_found_empty"

        title_key = title.lower().strip()
        if url not in seen_urls and title_key not in seen_titles:
            seen_urls.add(url)
            seen_titles.add(title_key)
            jobs.append({
                "title": title,
                "url": url,
                "company": company_name,
                "location": location,
                "location_status": location_status,
                "country": "",
                "department": "",
                "workplace_type": "",
                "employment_type": "",
                "salary": "",
                "description_snippet": "",
                "source_ats": "JazzHR",
                "slug": slug,
            })

    # Pattern 2: Legacy layout — resumator-job-title-link
    if not jobs:
        for match in re.finditer(
            r'class="resumator-job-title-link"[^>]*href=["\']([^"\']+)["\'][^>]*>([^<]+)</a>',
            r.text, re.I
        ):
            url = match.group(1).strip()
            title = match.group(2).strip()
            if not url.startswith("http"):
                url = base_url + url
            title_key = title.lower().strip()
            if url not in seen_urls and title_key not in seen_titles:
                seen_urls.add(url)
                seen_titles.add(title_key)
                jobs.append({
                    "title": title,
                    "url": url,
                    "company": company_name,
                    "location": "",
                    "country": "",
                    "department": "",
                    "workplace_type": "",
                    "employment_type": "",
                    "salary": "",
                    "description_snippet": "",
                    "source_ats": "JazzHR",
                    "slug": slug,
                })

    # Pattern 3: Generic fallback — any link to /apply/ pages
    if not jobs:
        for match in re.finditer(
            r'<a\s+[^>]*href=["\']([^"\']*(?:/apply/|/opening/)[^"\']*)["\'][^>]*>'
            r'([^<]+)</a>',
            r.text, re.I
        ):
            url = match.group(1).strip()
            title = match.group(2).strip()
            if not url.startswith("http"):
                url = base_url + url
            title_key = title.lower().strip()
            if url not in seen_urls and title_key not in seen_titles and len(title) > 3:
                seen_urls.add(url)
                seen_titles.add(title_key)
                jobs.append({
                    "title": title,
                    "url": url,
                    "company": company_name,
                    "location": "",
                    "country": "",
                    "department": "",
                    "workplace_type": "",
                    "employment_type": "",
                    "salary": "",
                    "description_snippet": "",
                    "source_ats": "JazzHR",
                    "slug": slug,
                })

    # Try to extract location from nearby elements
    for job in jobs:
        loc_match = re.search(
            re.escape(job["title"]) + r'</a>.*?class="[^"]*location[^"]*"[^>]*>([^<]+)',
            r.text, re.I | re.DOTALL
        )
        if loc_match:
            job["location"] = loc_match.group(1).strip()

    return jobs


# ── HRMDirect ───────────────────────────────────────────

# 2026-09: HRMDirect used to assume the cells AFTER the title cell are
# always [city, state, country] in that fixed order — a positional
# assumption that silently shifts every field one column over if a tenant's
# table has an extra inserted column (e.g. a "posted date" cell before
# city), with no error raised anywhere. Two defenses added below, per
# real-world scraping guidance from an external review of this exact
# failure class:
#   1. If the table has a header row (<th> cells, or a first row that
#      looks like one), map columns by their HEADER TEXT instead of
#      position — immune to column reordering/insertion as long as the
#      header itself is present.
#   2. Even without a header, sanity-check each candidate cell's CONTENT
#      SHAPE before trusting it as a city/state — a date-looking or
#      salary-looking string should never be silently accepted as a
#      location field just because it landed in the "expected" column.
_HRMD_HEADER_SYNONYMS = {
    "city": {"city", "location", "job location", "office location", "work location"},
    "state": {"state", "state/province", "province", "region"},
    "country": {"country"},
    "department": {"department", "dept", "team", "division"},
}
_HRMD_DATE_LIKE_RE = re.compile(
    r'^\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}'
    r'|[A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{4}'
    r'|\d{1,2}\s+[A-Za-z]{3,9}\.?\s+\d{4})\s*$'
)
_HRMD_SALARY_LIKE_RE = re.compile(r'^\s*\$[\d,]+(\.\d+)?(\s*[-–—]\s*\$?[\d,]+(\.\d+)?)?\s*$')


def _hrmd_looks_like_place(text: str) -> bool:
    """Best-effort content-shape check: rejects an obviously-wrong value
    (a date, a salary figure) that a positional guess might otherwise
    assign to a city/state field. Not a positive proof the text IS a
    place — just a guard against the clearest wrong-column cases."""
    if not text:
        return False
    if _HRMD_DATE_LIKE_RE.match(text) or _HRMD_SALARY_LIKE_RE.match(text):
        return False
    return True


def _hrmd_parse_header_row(html: str) -> dict[str, int] | None:
    """Looks for a header row (<th> cells, or an early row that is ALL
    short label-like text) and returns {canonical_field: cell_index}, or
    None if no usable header is found. A tenant that renders headers as
    an image, or omits them, falls back to the positional path below."""
    thead_match = re.search(r'<thead[^>]*>(.*?)</thead>', html, re.I | re.DOTALL)
    header_html = thead_match.group(1) if thead_match else html[:2000]
    header_row_match = re.search(r'<tr[^>]*>(.*?)</tr>', header_html, re.I | re.DOTALL)
    if not header_row_match:
        return None
    header_cells_html = re.findall(r'<t[hd][^>]*>(.*?)</t[hd]>', header_row_match.group(1), re.I | re.DOTALL)
    if len(header_cells_html) < 2:
        return None
    header_cells = [re.sub(r'<[^>]+>', '', c).strip().lower() for c in header_cells_html]

    mapping: dict[str, int] = {}
    for idx, label in enumerate(header_cells):
        for canonical, synonyms in _HRMD_HEADER_SYNONYMS.items():
            if label in synonyms and canonical not in mapping:
                mapping[canonical] = idx
    # Require at least a city/location column to trust this as a real
    # header row — otherwise this was probably just a normal data row
    # that happened to be short text, not an actual header.
    return mapping if "city" in mapping else None


def scrape_hrmdirect(slug: str) -> list[dict]:
    """HRMDirect / ClearCompany — HTML scrape of job openings table.
    Slug is the company subdomain (e.g. 'novabio').
    Uses ?search=true to force all jobs to display (not just filter dropdowns).
    Prefers header-based column mapping when a header row is present (see
    _hrmd_parse_header_row); falls back to positional guessing (with
    content-shape sanity checks) only when no header can be found."""
    company_name = slug.replace("-", " ").title()
    url = f"https://{slug}.hrmdirect.com/employment/openings.php?search=true"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = _get_requests_sync(url, headers=headers)
    if not r:
        return []

    header_map = _hrmd_parse_header_row(r.text)

    jobs = []
    seen_urls = set()

    # Parse table rows — each <tr> contains <td> cells with job link, city, state, country
    # Split by <tr to process row by row
    rows = re.split(r'<tr[^>]*>', r.text, flags=re.I)
    for row_html in rows:
        # Find job link in this row
        link_match = re.search(
            r'<a\s+[^>]*href=["\']([^"\']*job-opening\.php\?req=\d+[^"\']*)["\'][^>]*>'
            r'\s*([^<]+)</a>',
            row_html, re.I
        )
        if not link_match:
            continue

        job_path = link_match.group(1).strip()
        title = link_match.group(2).strip()

        if not job_path.startswith("http"):
            job_url = f"https://{slug}.hrmdirect.com/employment/{job_path}"
        else:
            job_url = job_path

        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        # Extract ALL <td> cell contents from this row
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.I | re.DOTALL)
        # Strip HTML tags from cells
        clean_cells = []
        for cell in cells:
            text = re.sub(r'<[^>]+>', '', cell).strip()
            clean_cells.append(text)

        city = ""
        state = ""
        country = ""
        department = ""
        location_status = "no_cells_found"

        if header_map:
            # Header-based mapping — immune to an inserted/reordered
            # column, as long as the header row itself matches.
            def _cell(idx):
                return clean_cells[idx] if 0 <= idx < len(clean_cells) else ""
            city = _cell(header_map.get("city", -1))
            state = _cell(header_map.get("state", -1))
            country = _cell(header_map.get("country", -1))
            department = _cell(header_map.get("department", -1))
            location_status = "extracted_by_header" if (city or state or country) else "marker_found_empty"
        else:
            # No header found — fall back to the old positional guess
            # (city/state/country are the cells right after the title),
            # but sanity-check each value's shape first so an inserted
            # date/salary column doesn't get silently accepted as a place.
            # HRMDirect tables vary but commonly:
            # [department?, title, city, state, country?] or [title, city, state]
            title_idx = -1
            for i, c in enumerate(clean_cells):
                if title in c:
                    title_idx = i
                    break

            if title_idx >= 0:
                remaining = clean_cells[title_idx + 1:]
                remaining = [c for c in remaining if _hrmd_looks_like_place(c)]
                if len(remaining) >= 1:
                    city = remaining[0]
                if len(remaining) >= 2:
                    state = remaining[1]
                if len(remaining) >= 3:
                    country = remaining[2]
                # Department is usually before the title
                if title_idx >= 1:
                    department = clean_cells[title_idx - 1]
                location_status = "extracted_positional" if (city or state or country) else "marker_found_empty"

        location = city
        if state and city:
            location = f"{city}, {state}"
        elif state:
            location = state
        if country and country not in location:
            location = f"{location}, {country}" if location else country

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": location,
            "location_status": location_status,
            "country": country,
            "department": department,
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "HRMDirect",
            "slug": slug,
        })

    return jobs


# ── Softgarden ──────────────────────────────────────────

async def scrape_softgarden(slug: str) -> list[dict]:
    """Softgarden scraper using the current public jobs.feed.json first.

    Modern Career Page / PCW tenants expose a public schema.org DataFeed at
    /jobs.feed.json (also /jobs.json and /.well-known/jobs.json). The feed is
    preferred because it contains complete structured JobPosting records and
    avoids the Next.js/PCW browser layer. Classic HTML/JSON-LD pages remain a
    fallback for older tenants.
    """
    company_name = slug.replace("-", " ").title()
    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json, text/html;q=0.9,*/*;q=0.8"}

    domains = (
        f"https://{slug}.career.softgarden.de",
        f"https://{slug}.softgarden.de",
        f"https://{slug}.softgarden.io",
    )

    def _link_feed(link_header: str | None, base: str) -> str | None:
        if not link_header:
            return None
        for part in re.split(r"\s*,\s*(?=<)", link_header):
            m = re.search(r'<([^>]+)>\s*;[^,]*rel=["\']alternate["\'][^,]*type=["\']application/json["\']', part, re.I)
            if m:
                return urljoin(base, m.group(1))
            m = re.search(r"<([^>]*jobs(?:\.feed)?\.json[^>]*)>", part, re.I)
            if m:
                return urljoin(base, m.group(1))
        return None

    def _make_job(item: dict) -> dict | None:
        if not isinstance(item, dict):
            return None
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        if not title or not url:
            return None
        org = item.get("hiringOrganization") or {}
        loc_obj = item.get("jobLocation") or {}
        if isinstance(loc_obj, list):
            loc_obj = loc_obj[0] if loc_obj else {}
        addr = loc_obj.get("address") if isinstance(loc_obj, dict) else {}
        addr = addr if isinstance(addr, dict) else {}
        locality = str(addr.get("addressLocality") or "").strip()
        region = str(addr.get("addressRegion") or "").strip()
        country = str(addr.get("addressCountry") or "").strip()
        if isinstance(item.get("jobLocationType"), str) and item.get("jobLocationType"):
            workplace = str(item.get("jobLocationType"))
        else:
            workplace = "Remote" if "TELECOMMUTE" in str(item.get("jobLocationType") or "").upper() else ""
        location = ", ".join(x for x in (locality, region) if x)
        desc = _snippet(item.get("description") or "")
        return {
            "title": title, "url": url,
            "company": str(org.get("name") or company_name).strip(),
            "location": location, "country": country,
            "department": "", "workplace_type": workplace,
            "employment_type": item.get("employmentType") or "",
            "salary": _extract_salary(desc), "description_snippet": desc,
            "source_ats": "Softgarden", "slug": slug,
        }

    for domain in domains:
        try:
            home = await _get(domain + "/", headers=headers)
            if not home:
                continue

            feed_candidates = []
            advertised = _link_feed(home.headers.get("Link"), domain + "/")
            if advertised:
                feed_candidates.append(advertised)
            for path in ("/jobs.feed.json", "/jobs.json", "/.well-known/jobs.json"):
                u = domain + path
                if u not in feed_candidates:
                    feed_candidates.append(u)

            for feed_url in feed_candidates:
                feed = await _get(feed_url, headers={**headers, "Accept": "application/json,*/*;q=0.8"})
                if not feed:
                    continue
                try:
                    data = feed.json()
                except Exception:
                    continue
                elements = data.get("dataFeedElement", []) if isinstance(data, dict) else []
                if not isinstance(elements, list):
                    elements = []
                jobs, seen = [], set()
                for element in elements:
                    item = element.get("item") if isinstance(element, dict) else element
                    job = _make_job(item)
                    if job and job["url"] not in seen:
                        seen.add(job["url"])
                        jobs.append(job)
                if jobs:
                    return jobs

            # Modern HTML fallback: /jobs/ is the documented public index.
            for path in ("/jobs/", "/jobs"):
                page = await _get(domain + path, headers=headers)
                if not page:
                    continue
                soup = await asyncio.to_thread(BeautifulSoup, page.text, "html.parser")
                jobs, seen = [], set()
                for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
                    try:
                        data = json.loads(script.string or script.get_text())
                    except Exception:
                        continue
                    values = data if isinstance(data, list) else [data]
                    for item in values:
                        if isinstance(item, dict) and item.get("@type") == "JobPosting":
                            job = _make_job(item)
                            if job and job["url"] not in seen:
                                seen.add(job["url"]); jobs.append(job)
                if jobs:
                    return jobs

            # Classic Softgarden paths, only after modern feed/index attempts.
            for path in ("/en/vacancies", "/en/vacancies/", "/vacancies", "/vacancies/", "/en/jobs", "/en/jobs/"):
                page = await _get(domain + path, headers=headers)
                if not page:
                    continue
                jobs, seen = [], set()
                for m in re.finditer(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', page.text, re.I | re.S):
                    try:
                        data = json.loads(m.group(1))
                    except Exception:
                        continue
                    values = data if isinstance(data, list) else [data]
                    for item in values:
                        if isinstance(item, dict) and item.get("@type") == "JobPosting":
                            job = _make_job(item)
                            if job and job["url"] not in seen:
                                seen.add(job["url"]); jobs.append(job)
                if jobs:
                    return jobs
        except Exception as e:
            log.debug("Softgarden domain %s failed for %s: %s", domain, slug, e)
            continue

    raise RuntimeError(f"Softgarden: no jobs/feed found for {slug} across known domain generations")


# ── Zoho Recruit ────────────────────────────────────────

def scrape_zoho(slug: str) -> list[dict]:
    """Zoho Recruit — HTML scrape with embedded JSON.
    Slug is the company subdomain (e.g. 'acme').
    Parses hidden input#jobs JSON data."""
    company_name = slug.replace("-", " ").title()
    url = f"https://{slug}.zohorecruit.com/jobs/Careers"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = _get_requests_sync(url, headers=headers)
    if not r:
        return []

    jobs = []

    # Primary: Parse hidden input with jobs JSON
    # Try id="jobs" and name="jobs", both attribute orders
    jobs_input = None
    for attr in ('id', 'name'):
        if jobs_input:
            break
        # attr before value
        jobs_input = re.search(
            rf'<input[^>]*{attr}=["\']jobs["\'][^>]*value=["\']([^"\']+)["\']',
            r.text, re.I
        )
        if not jobs_input:
            # value before attr
            jobs_input = re.search(
                rf'<input[^>]*value=["\']([^"\']+)["\'][^>]*{attr}=["\']jobs["\']',
                r.text, re.I
            )
    if jobs_input:
        import json
        try:
            raw = jobs_input.group(1)
            # Unescape HTML entities
            raw = raw.replace("&quot;", '"').replace("&amp;", "&")
            raw = raw.replace("&lt;", "<").replace("&gt;", ">")
            raw = raw.replace("&#39;", "'")
            job_data = json.loads(raw)

            if isinstance(job_data, list):
                for item in job_data:
                    title = item.get("Posting_Title") or item.get("Job_Opening_Name") or ""
                    job_id = item.get("id") or item.get("Job Opening Id") or ""
                    job_url_val = item.get("$url") or ""
                    if not job_url_val and job_id:
                        job_url_val = f"https://{slug}.zohorecruit.com/jobs/Careers/{job_id}"

                    loc = item.get("City") or item.get("city") or ""
                    state = item.get("State") or ""
                    country = item.get("Country") or ""
                    if state and loc:
                        loc = f"{loc}, {state}"

                    salary = item.get("Salary") or ""
                    desc = _snippet(item.get("Job_Description") or item.get("description") or "")
                    if not salary:
                        salary = _extract_salary(desc)

                    jobs.append({
                        "title": title.strip(),
                        "url": job_url_val,
                        "company": company_name,
                        "location": loc.strip(),
                        "country": country.strip() if isinstance(country, str) else "",
                        # 2026-09: live-verified against a real Zoho Recruit
                        # career site (ziplyfiber.zohorecruit.com, 74 real
                        # openings) that the embedded input#jobs JSON uses
                        # "Department_Name", not "Department" — the field
                        # previously looked up doesn't exist in current
                        # payloads, so department was always blank.
                        "department": (item.get("Department_Name") or item.get("Department") or "").strip(),
                        "workplace_type": (item.get("Remote_Job") or item.get("Work_Mode") or "").strip(),
                        "employment_type": (item.get("Job_Type") or item.get("jobtype") or "").strip(),
                        "salary": str(salary).strip() if salary else "",
                        "description_snippet": desc,
                        "source_ats": "Zoho",
                        "slug": slug,
                    })
        except (json.JSONDecodeError, Exception) as e:
            log.debug(f"Zoho: JSON parse failed for {slug}: {e}")

    # Fallback: Parse JSON-LD structured data
    if not jobs:
        for ld_match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>([^<]+)</script>',
            r.text, re.I
        ):
            try:
                import json
                ld_data = json.loads(ld_match.group(1))
                items = ld_data if isinstance(ld_data, list) else [ld_data]
                for item in items:
                    if item.get("@type") != "JobPosting":
                        continue
                    loc_obj = item.get("jobLocation", {})
                    addr = loc_obj.get("address", {}) if isinstance(loc_obj, dict) else {}
                    desc = _snippet(item.get("description", ""))
                    salary = _extract_salary(desc)
                    org = item.get("hiringOrganization", {})

                    jobs.append({
                        "title": item.get("title", "").strip(),
                        "url": item.get("url", ""),
                        "company": (org.get("name", "") if isinstance(org, dict) else company_name).strip(),
                        "location": addr.get("addressLocality", ""),
                        "country": addr.get("addressCountry", ""),
                        "department": "",
                        "workplace_type": "",
                        "employment_type": item.get("employmentType", ""),
                        "salary": salary,
                        "description_snippet": desc,
                        "source_ats": "Zoho",
                        "slug": slug,
                    })
            except Exception:
                continue

    # Fallback 2: Generic link scrape
    if not jobs:
        seen = set()
        for match in re.finditer(
            r'<a\s+[^>]*href=["\']([^"\']*(?:/jobs/|/careers/|/opening)[^"\']*)["\'][^>]*>([^<]+)</a>',
            r.text, re.I
        ):
            link = match.group(1).strip()
            title = match.group(2).strip()
            if not link.startswith("http"):
                link = f"https://{slug}.zohorecruit.com{link}"
            if link not in seen and len(title) > 3:
                seen.add(link)
                jobs.append({
                    "title": title,
                    "url": link,
                    "company": company_name,
                    "location": "",
                    "country": "",
                    "department": "",
                    "workplace_type": "",
                    "employment_type": "",
                    "salary": "",
                    "description_snippet": "",
                    "source_ats": "Zoho",
                    "slug": slug,
                })

    return jobs


def scrape_personio(slug: str) -> list[dict]:
    """Personio — public XML feed, no auth required.
    Slug is the company subdomain (e.g. 'acme').
    Tries both .de and .com domains."""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    company_name = slug.replace("-", " ").title()

    xml_text = None
    for domain in ["jobs.personio.de", "jobs.personio.com"]:
        url = f"https://{slug}.{domain}/xml?language=en"
        r = _get_requests_sync(url, headers=headers)
        if r and r.text.strip().startswith("<?xml"):
            xml_text = r.text
            break

    if not xml_text:
        return []

    jobs = []
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        log.debug(f"Personio: XML parse failed for {slug}: {e}")
        return []

    for pos in root.iter("position"):
        title = (pos.findtext("name") or "").strip()
        if not title:
            continue

        job_id = pos.findtext("id") or ""
        office = (pos.findtext("office") or "").strip()
        department = (pos.findtext("department") or "").strip()
        emp_type = (pos.findtext("employmentType") or "").strip()
        company = (pos.findtext("subcompany") or company_name).strip()
        schedule = (pos.findtext("schedule") or "").strip()

        # Description blocks — clean each block (HTML strip/entity-decode/
        # junk-strip) WITHOUT per-block truncation, then cap the joined
        # whole once via _snippet's default. A JD is usually split across
        # several blocks (intro/requirements/benefits); truncating each
        # block individually (previously 2000 chars each) could still chop
        # a real block mid-sentence even though the full joined text was
        # well under the overall safety ceiling.
        desc_parts = []
        for desc_elem in pos.iter("jobDescription"):
            name = (desc_elem.findtext("name") or "").strip()
            value = (desc_elem.findtext("value") or "").strip()
            if value:
                desc_parts.append(_snippet(value))
        desc = _snippet(" ".join(desc_parts))
        salary = _extract_salary(desc) if desc else ""

        # Build job URL
        job_url = f"https://{slug}.jobs.personio.de/job/{job_id}" if job_id else ""

        # Try to extract country from office field (e.g. "Munich, Germany")
        country = ""
        if office:
            office_parts = [p.strip() for p in office.split(",")]
            if len(office_parts) >= 2:
                country = office_parts[-1]

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company,
            "location": office,
            "country": country,
            "department": department,
            "workplace_type": schedule,
            "employment_type": emp_type,
            "salary": salary,
            "description_snippet": desc,
            "source_ats": "Personio",
            "slug": slug,
        })

    return jobs


# ── Dispatcher ──────────────────────────────────────────

def scrape_joincom(slug: str) -> list[dict]:
    """JOIN.com — public REST API, no auth required.
    Slug is the company slug (e.g. 'marswalk').
    Two-step: resolve slug → company_id via __NEXT_DATA__, then paginate the jobs API.
    pageSize max is 5 (server rejects >= 6 with HTTP 422)."""
    import json as _json
    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "text/html"}

    # Step 1: Resolve slug → numeric company_id
    page_r = _get_requests_sync(f"https://join.com/companies/{slug}", headers=headers)
    if not page_r:
        return []

    nd_match = re.search(
        r'<script\s+id="__NEXT_DATA__"[^>]*>([^<]+)</script>',
        page_r.text, re.I,
    )
    if not nd_match:
        log.debug(f"JOIN: no __NEXT_DATA__ for {slug}")
        return []

    try:
        nd = _json.loads(nd_match.group(1))
        company_id = nd["props"]["pageProps"]["initialState"]["company"]["id"]
        company_name = nd["props"]["pageProps"]["initialState"]["company"].get("name", slug.replace("-", " ").title())
    except (KeyError, _json.JSONDecodeError) as e:
        log.debug(f"JOIN: failed to extract company_id for {slug}: {e}")
        return []

    # Step 2: Paginate the public jobs API (pageSize max 5)
    api_base = f"https://join.com/api/public/companies/{company_id}/jobs"
    all_jobs = []
    page = 1

    while True:
        r = _get_requests_sync(api_base, params={"locale": "en-us", "page": page, "pageSize": 5},
                 headers={"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"})
        if not r:
            break
        try:
            data = r.json()
        except Exception:
            break

        items = data.get("items", [])
        pagination = data.get("pagination", {})
        if not items:
            break

        for item in items:
            city_obj = item.get("city") or {}
            city = city_obj.get("cityName", "") if isinstance(city_obj, dict) else ""
            region = city_obj.get("regionName", "") if isinstance(city_obj, dict) else ""
            country = city_obj.get("countryName", "") if isinstance(city_obj, dict) else ""
            location = ", ".join(filter(None, [city, region, country]))

            # Salary in cents → dollars/euros
            sal_from_obj = item.get("salaryAmountFrom") or {}
            sal_to_obj = item.get("salaryAmountTo") or {}
            salary_str = ""
            if isinstance(sal_from_obj, dict) and isinstance(sal_to_obj, dict):
                amt_from = sal_from_obj.get("amount", 0)
                amt_to = sal_to_obj.get("amount", 0)
                currency = sal_from_obj.get("currency", "EUR")
                if amt_from and amt_to:
                    salary_str = f"{currency} {amt_from / 100:,.0f}-{amt_to / 100:,.0f}"

            cat = item.get("category") or {}
            dept = cat.get("name", "") if isinstance(cat, dict) else ""
            emp_obj = item.get("employmentType") or {}
            emp_type = emp_obj.get("name", "") if isinstance(emp_obj, dict) else ""
            wt = item.get("workplaceType", "")  # ONSITE, REMOTE, HYBRID

            id_param = item.get("idParam", "")
            job_url = f"https://join.com/companies/{slug}/jobs/{id_param}" if id_param else ""

            all_jobs.append({
                "title": (item.get("title") or "").strip(),
                "url": job_url,
                "company": company_name,
                "location": location,
                "country": country,
                "department": dept,
                "workplace_type": wt,
                "employment_type": emp_type,
                "salary": salary_str,
                "description_snippet": "",  # Need per-job fetch for full description
                "source_ats": "JOIN",
                "slug": slug,
            })

        page_count = pagination.get("pageCount", 1)
        if page >= page_count:
            break
        page += 1
        # 2026-09: see module-level comment above _pace_host.
        _pace_host(api_base)

    return all_jobs


# ── Paylocity ──────────────────────────────────────────

def scrape_paylocity(slug: str) -> list[dict]:
    """Paylocity — embedded window.pageData JSON in career page HTML.
    Slug format: 'company_id|company_name' (e.g. '9b6dbe18-.../The-Guidance-Center')."""
    import json as _json
    parts = slug.split("|", 1)
    if len(parts) != 2:
        log.debug(f"Invalid Paylocity slug format: {slug} (expected 'company_id|company_name')")
        return []

    company_id, company_name_slug = parts
    url = f"https://recruiting.paylocity.com/recruiting/jobs/All/{company_id}/{company_name_slug}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = _get_requests_sync(url, headers=headers)
    if not r:
        return []

    # Extract window.pageData JSON
    pd_match = re.search(r'window\.pageData\s*=\s*(\{.*?\});\s*</script>', r.text, re.DOTALL)
    if not pd_match:
        log.debug(f"Paylocity: no window.pageData found for {company_name_slug}")
        return []

    try:
        page_data = _json.loads(pd_match.group(1))
    except _json.JSONDecodeError as e:
        log.debug(f"Paylocity: JSON parse failed for {company_name_slug}: {e}")
        return []

    company_name = (page_data.get("companyName")
                    or page_data.get("ModuleTitle")
                    or company_name_slug.replace("-", " ").title())
    jobs_list = page_data.get("Jobs", page_data.get("jobs", []))
    if not isinstance(jobs_list, list):
        return []

    jobs = []
    seen_titles = set()
    for item in jobs_list:
        # Skip inactive / expired jobs
        status = str(item.get("Status", item.get("PostingStatus", ""))).lower()
        is_active = item.get("IsActive", item.get("isActive", None))
        if status in ("closed", "inactive", "expired", "draft", "archived"):
            continue
        if is_active is False or str(is_active).lower() == "false":
            continue

        title = item.get("JobTitle", item.get("Title", ""))
        job_id = item.get("JobId", item.get("Id", ""))
        location = item.get("LocationName", item.get("Location", ""))
        department = item.get("HiringDepartment", item.get("Department", ""))
        desc = _snippet(item.get("Description", item.get("JobDescription", "")))
        salary = _extract_salary(desc)

        # Deduplicate by title (same company may list same role multiple times)
        title_key = str(title).lower().strip()
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)

        # 2026-09 ROUND 8 FIX (real production evidence — every recorded
        # Paylocity job_url 404'd): the path was built as
        # /Details/{company_id GUID}/{job_id}/{company_name_slug}, but
        # Paylocity's real URL shape is /Details/{job_id}/{company_name_slug}
        # (optionally plus a third, purely cosmetic title slug — confirmed
        # live that omitting it still resolves correctly). company_id is
        # the slug's OWN "which company" identifier (used to build the
        # listing URL above), not a path segment Paylocity's job-detail
        # route expects at all — every posting from every Paylocity tenant
        # was carrying this same wrong shape.
        job_url = f"https://recruiting.paylocity.com/recruiting/jobs/Details/{job_id}/{company_name_slug}"

        jobs.append({
            "title": str(title).strip(),
            "url": job_url,
            "company": company_name,
            "location": location or "",
            "country": "",
            "department": department,
            "workplace_type": "",
            "employment_type": item.get("EmploymentType", ""),
            "salary": salary,
            "description_snippet": desc,
            "source_ats": "Paylocity",
            "slug": slug,
        })

    return jobs


# ── Eploy ───────────────────────────────────────────────

async def scrape_eploy(slug: str) -> list[dict]:
    """Eploy public job-board scraper with host discovery and pagination.

    Eploy's public pages are genuinely server-rendered HTML. The important
    traps are that tenant hostnames vary (plain/web/web2), the list endpoint
    is commonly ``/vacancies/vacancy-search-results.aspx``, and a page can be
    HTTP 200 while containing zero roles. We therefore validate by vacancy
    links and follow the board's own pagination instead of guessing a fixed
    number of pages.
    """
    company_name = slug.replace("-", " ").title()
    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "text/html,application/xhtml+xml,*/*;q=0.8"}
    hosts = []
    for h in (slug, f"{slug}web", f"{slug}web2"):
        if h not in hosts:
            hosts.append(h)
    paths = (
        "/vacancies/vacancy-search-results.aspx",
        "/candidate/JobBoard/VacancySearchResults.aspx",
        "/candidate/jobboard/vacancysearchresults.aspx",
        "/vacancies",
        "/vacancies/",
    )
    # Current live Eploy detail URLs are /vacancies/<numeric-id>/<slug>.html;
    # retain a broad fallback for older/custom boards.
    vacancy_re = re.compile(r"(?:^|/)vacancies/(?:vacancies-amp/)?(\d+)(?:/|-)?[^/?#\"']*(?:\.html)?(?:[?#][^\"']*)?$", re.I)

    async def parse_page(r: httpx.Response, base_url: str) -> tuple[list[dict], list[str]]:
        soup = await asyncio.to_thread(BeautifulSoup, r.text, "html.parser")
        jobs, seen = [], set()
        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href") or "").strip()
            title = unescape(re.sub(r"\s+", " ", anchor.get_text(" ", strip=True))).strip()
            if not href or len(title) < 3:
                continue
            abs_url = urljoin(base_url, href)
            path = urlparse(abs_url).path
            if not (vacancy_re.search(path) or re.search(r"/vacancies/(?:[^/]+/)?\d+(?:/[^/]+)?", path, re.I)):
                continue
            if abs_url in seen:
                continue
            seen.add(abs_url)
            location, location_status = _bs4_find_location_near(anchor, class_substrings=("location", "vacancy-location"))
            jobs.append({
                "title": title, "url": abs_url, "company": company_name,
                "location": location, "location_status": location_status,
                "country": "", "department": "", "workplace_type": "",
                "employment_type": "", "salary": "", "description_snippet": "",
                "source_ats": "Eploy", "slug": slug,
            })

        next_urls = []
        for a in soup.find_all("a", href=True):
            text = _text(a.get_text(" ", strip=True))
            href = str(a.get("href") or "").strip()
            if not href:
                continue
            if re.search(r"\b(?:next|last)\b|›|»", text, re.I):
                next_urls.append(urljoin(base_url, href))
        return jobs, list(dict.fromkeys(next_urls))

    for host in hosts:
        base = f"https://{host}.eploy.net"
        for path in paths:
            first_url = base + path
            r = await _get(first_url, headers=headers)
            if not r:
                continue
            jobs, next_urls = await parse_page(r, str(r.url) if r.url else first_url)
            if not jobs:
                continue
            seen_jobs = {j["url"] for j in jobs}
            seen_pages = {str(r.url)}
            queue = next_urls[:]
            # Follow only same-host pagination URLs and stop on repeats.
            while queue:
                nxt = queue.pop(0)
                if urlparse(nxt).netloc.lower() != urlparse(str(r.url)).netloc.lower() or nxt in seen_pages:
                    continue
                seen_pages.add(nxt)
                nr = await _get(nxt, headers=headers)
                if not nr:
                    break
                more, more_next = await parse_page(nr, str(nr.url) if nr.url else nxt)
                for job in more:
                    if job["url"] not in seen_jobs:
                        seen_jobs.add(job["url"]); jobs.append(job)
                for u in more_next:
                    if u not in seen_pages:
                        queue.append(u)
                if not more:
                    break
            return jobs

    raise RuntimeError(f"Eploy: no vacancy records found for {slug} across host/path variants")


# ── Folks HR (Folks Applicant Tracking System) ──────────

async def scrape_folkshr(slug: str) -> list[dict]:
    """Folks HR — HTML scrape of the public careers microsite.
    Slug is the company identifier on the shared board domain.
    No public API; listing and detail pages are server-rendered HTML.

    Two domains are live: jobs.folksats.app (post-2025-rebrand) and
    jobs.glowinthecloud.com (the older "Glow Talents" domain Folks HR
    acquired — most existing customers are still actually hosted there).
    A given company lives on one or the other, not both, so we try
    folksats.app first and fall back to glowinthecloud.com.

    List page:   https://{domain}/{company}
    Detail page: https://{domain}/{company}/{job-id}
    """
    company_name = slug.replace("-", " ").title()
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    domain = None
    r = None
    for candidate in ("jobs.folksats.app", "jobs.glowinthecloud.com"):
        r = await _get(f"https://{candidate}/{slug}", headers=headers)
        if r:
            domain = candidate
            break
    if not r or not domain:
        return []

    jobs = []
    seen = set()

    for match in re.finditer(
        r'href=["\'](/' + re.escape(slug) + r'/([a-zA-Z0-9\-]+))["\'][^>]*>\s*([^<]+)</a>',
        r.text, re.I
    ):
        path, job_id, title = match.group(1), match.group(2), unescape(match.group(3)).strip()
        if job_id.lower() in ("apply", "about", "jobs", "") or len(title) < 3:
            continue
        job_url = f"https://{domain}{path}"
        if job_url in seen:
            continue
        seen.add(job_url)

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": "",
            "country": "",
            "department": "",
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "FolksHR",
            "slug": slug,
        })

    return jobs


# ── JobAdder ────────────────────────────────────────────

async def scrape_jobadder(slug: str) -> list[dict]:
    """JobAdder hosted CareersPage scraper.

    Current live CareersPages use URLs such as
    ``/66341/nexia-australia/1113369/intermediate-accountant``; older boards
    may expose ``/job/<id>`` links. Both forms are accepted. The official
    CareersPage is a public, server-rendered feed updated by JobAdder, so no
    authenticated API is required for this path.
    """
    if "|" in slug:
        client_id, board_slug = slug.split("|", 1)
    else:
        client_id, board_slug = slug, ""
    company_name = board_slug.replace("-", " ").title() or client_id
    if not client_id.isdigit() or not board_slug:
        raise RuntimeError(f"JobAdder: invalid discovered board slug {slug!r}; expected numeric_client_id|board_slug")
    if board_slug.lower() in {"flexslider", "animate-css", "bootstrap", "jquery", "jquery-ui", "fontawesome", "slick", "owl-carousel", "swiper", "vendors"}:
        raise RuntimeError(f"JobAdder: invalid discovered board slug {slug!r} — frontend/vendor asset name")

    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "text/html,application/xhtml+xml,*/*;q=0.8"}
    base = f"https://clientapps.jobadder.com/{client_id}/{board_slug}".rstrip("/")
    candidates = [base, base + "/"]
    jobs, seen = [], set()

    for list_url in candidates:
        r = await _get(list_url, headers=headers)
        if not r:
            continue
        soup = await asyncio.to_thread(BeautifulSoup, r.text, "html.parser")

        # Canonical current form: /<client>/<board>/<job-id>/<title-slug>[/apply]
        patterns = [
            re.compile(rf"^/{re.escape(client_id)}/{re.escape(board_slug)}/(\d+)(?:/[^?#]*)?(?:[?#].*)?$", re.I),
            re.compile(r"/(?:job|jobs)/(\d+)(?:/[^?#]*)?(?:[?#].*)?$", re.I),
        ]
        for a in soup.find_all("a", href=True):
            href = str(a.get("href") or "").strip()
            title = unescape(re.sub(r"\s+", " ", a.get_text(" ", strip=True))).strip()
            if not href or len(title) < 3:
                continue
            abs_url = urljoin(str(r.url), href)
            path = urlparse(abs_url).path
            jid = None
            for pat in patterns:
                m = pat.search(path)
                if m:
                    jid = m.group(1); break
            if not jid:
                # Also accept the canonical numeric path when the board slug
                # has been normalized by a redirect.
                m = re.search(rf"/{re.escape(client_id)}/[^/]+/(\d+)(?:/|$)", path, re.I)
                if m:
                    jid = m.group(1)
            if not jid or abs_url in seen:
                continue
            seen.add(abs_url)
            location, location_status = _bs4_find_location_near(a, class_substrings=("location", "job-location", "location-name"))
            jobs.append({
                "title": title, "url": abs_url, "company": company_name,
                "location": location, "location_status": location_status,
                "country": "", "department": "", "workplace_type": "",
                "employment_type": "", "salary": "", "description_snippet": "",
                "source_ats": "JobAdder", "slug": slug,
            })

        # JSON-LD fallback for boards whose visible anchors are injected or
        # represented through application/json state.
        if not jobs:
            for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
                try:
                    data = json.loads(script.string or script.get_text())
                except Exception:
                    continue
                values = data if isinstance(data, list) else [data]
                for item in values:
                    if not isinstance(item, dict) or item.get("@type") != "JobPosting":
                        continue
                    title = _text(item.get("title"))
                    url = _text(item.get("url"))
                    if not title or not url or url in seen:
                        continue
                    seen.add(url)
                    loc = item.get("jobLocation") or {}
                    if isinstance(loc, list): loc = loc[0] if loc else {}
                    addr = loc.get("address") if isinstance(loc, dict) else {}
                    addr = addr if isinstance(addr, dict) else {}
                    location = ", ".join(str(x).strip() for x in (addr.get("addressLocality"), addr.get("addressRegion"), addr.get("addressCountry")) if x)
                    desc = _snippet(item.get("description") or "")
                    jobs.append({
                        "title": title, "url": url, "company": company_name,
                        "location": location, "location_status": "extracted" if location else "marker_not_found",
                        "country": str(addr.get("addressCountry") or ""), "department": "",
                        "workplace_type": "", "employment_type": item.get("employmentType") or "",
                        "salary": _extract_salary(desc), "description_snippet": desc,
                        "source_ats": "JobAdder", "slug": slug,
                    })
        if jobs:
            return jobs

    raise RuntimeError(f"JobAdder: no job records found for {slug}; board URL was reachable but no current job-link pattern matched")


# ── Jobvite ─────────────────────────────────────────────

def scrape_jobvite(slug: str) -> list[dict]:
    """Jobvite — HTML scrape of the hosted careers site.
    Slug is the company identifier on jobs.jobvite.com
    (e.g. 'acme' for jobs.jobvite.com/acme/jobs).

    List page:   https://jobs.jobvite.com/{company}/jobs
    Detail page: https://jobs.jobvite.com/{company}/job/{job_id}
    """
    company_name = slug.replace("-", " ").title()
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    base = f"https://jobs.jobvite.com/{slug}/jobs"

    r = _get_requests_sync(base, headers=headers)
    if not r:
        return []

    jobs = []
    seen = set()

    # 2026-09: migrated location AND department extraction to the
    # DOM-anchored lookup — see scrape_eploy's comment above /
    # _bs4_find_location_near's docstring. Same helper reused for
    # "department" by passing different class substrings, since the
    # fragility (bare-adjacency capture) was identical for both fields.
    soup = BeautifulSoup(r.text, "html.parser")
    job_href_re = re.compile(r'/' + re.escape(slug) + r'/job/[a-zA-Z0-9\-]+')
    for anchor in soup.find_all("a", href=job_href_re):
        path = (anchor.get("href") or "").strip()
        title = anchor.get_text(strip=True)
        if not path or not title:
            continue
        job_url = path if path.startswith("http") else f"https://jobs.jobvite.com{path}"
        if job_url in seen:
            continue
        seen.add(job_url)

        location, location_status = _bs4_find_location_near(
            anchor, class_substrings=("location", "jv-job-list__location")
        )
        department, _ = _bs4_find_location_near(
            anchor, class_substrings=("department", "jv-job-list__department")
        )

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": location,
            "location_status": location_status,
            "country": "",
            "department": department,
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "Jobvite",
            "slug": slug,
        })

    return jobs


# ── ADP Workforce Now (recruiting/staffing) ──────────────

async def scrape_adp(slug: str) -> list[dict]:
    """ADP Workforce Now — public career-center JSON API (no auth).
    Slug encodes both required identifiers as '{cid}|{ccId}':
      cid  = the customer id (query param 'cid')
      ccId = the career-center id (query param 'ccId')
    Both are visible in any public ADP careers URL, e.g.
    workforcenow.adp.com/mascsr/default/careercenter/public/events/
    staffing/v1/job-requisitions?cid={cid}&ccId={ccId}.

    Real, verified field names (list endpoint) — the earlier version of
    this function guessed several field names that don't actually exist
    (requisitionId, hiringOrganizationName, primaryLocation,
    jobFamilyName, workerTypeCode, and a description on the list item
    itself) and silently produced empty/wrong data for all of them:
      itemID              — the requisition's real ID (used for the detail
                             URL and _fetch_adp_description below)
      requisitionTitle    — plain string, not a nested object
      requisitionLocations[] — list of {address, nameCode.shortName}; a
                             requisition can have MULTIPLE real locations
                             (confirmed live: e.g. one req posted in both
                             Miami, FL and St. Petersburg, FL)
    There is no company-name or department field in the payload at all —
    left as slug-derived / empty rather than guessed again. The full job
    description (requisitionDescription) only exists on the per-item
    DETAIL endpoint, not this list endpoint — see _fetch_adp_description,
    registered in DESCRIPTION_FETCHERS, which the existing enrichment
    pass calls after role filtering (so only the small role-relevant
    subset costs an extra HTTP call, not every listed job)."""
    if "|" not in slug:
        return []
    cid, cc_id = slug.split("|", 1)

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
    }
    api_url = (
        "https://workforcenow.adp.com/mascsr/default/careercenter/public/events/"
        "staffing/v1/job-requisitions"
    )
    # ADP's slug is '{cid}|{ccId}' (two opaque GUIDs/IDs), NOT a readable
    # company name — unlike every other ATS in this file. The old
    # `slug.replace("-", " ").title()` fallback therefore leaked raw GUID
    # text into company_name (confirmed live, e.g.
    # "F417713F 4524 4Ba7 B017 731934A3B31C|19000101_000001"). The list
    # payload has no company/org name field either (see docstring), so
    # leave it blank rather than emit garbage — enrichment/UI should treat
    # blank company_name as "unknown", not display a fake name.
    company_name = ""
    jobs = []
    limit = 50
    offset = 0

    while True:
        r = await _get(api_url, headers=headers, params={
            "cid": cid, "ccId": cc_id, "$top": limit, "$skip": offset,
        })
        if not r:
            break
        try:
            data = r.json()
        except Exception:
            break

        items = data.get("jobRequisitions") or data.get("items") or []
        if not items:
            break

        for item in items:
            title = item.get("requisitionTitle", "")
            if isinstance(title, dict):  # defensive — seen as plain string in practice
                title = title.get("titleText", "")
            req_id = item.get("itemID") or item.get("requisitionId") or item.get("id") or ""

            # requisitionLocations is a LIST — a requisition can genuinely
            # have more than one real location. nameCode.shortName is
            # already a human-readable "City, ST, US"-style string.
            req_locs = item.get("requisitionLocations") or []
            loc_strings = []
            for rl in req_locs:
                if not isinstance(rl, dict):
                    continue
                name = (rl.get("nameCode") or {}).get("shortName", "")
                if name and name.strip():
                    loc_strings.append(name.strip())
                else:
                    addr = rl.get("address") or {}
                    city = addr.get("cityName", "")
                    state = (addr.get("countrySubdivisionLevel1") or {}).get("codeValue", "")
                    if city or state:
                        loc_strings.append(", ".join(p for p in [city, state] if p))
            location = "; ".join(loc_strings)

            countries = geo.extract_countries(location)
            country = ", ".join(sorted(countries))

            # 2026-09: job["url"] used to just BE this raw JSON API endpoint
            # — confirmed live (reported by the user, then reproduced) that
            # opening it in a browser dumps the raw JSON response, not a
            # usable job page. The real human-facing career-center page is
            # a different path entirely (recruitment.html, an SPA), and it
            # deep-links to one job via a `jobId` query param — confirmed
            # live that `jobId` is the requisition's ExternalJobID (a small
            # numeric string in customFieldGroup.stringFields), NOT itemID
            # (the opaque GUID-shaped ID this API otherwise keys on): e.g.
            # itemID '9201144516913_1' had ExternalJobID '567183', and
            # .../recruitment.html?...&jobId=567183 opened that exact job.
            api_detail_url = (
                f"https://workforcenow.adp.com/mascsr/default/careercenter/public/"
                f"events/staffing/v1/job-requisitions/{req_id}?cid={cid}&ccId={cc_id}"
            )
            external_job_id = ""
            for sf in (item.get("customFieldGroup") or {}).get("stringFields", []):
                if isinstance(sf, dict) and (sf.get("nameCode") or {}).get("codeValue") == "ExternalJobID":
                    external_job_id = (sf.get("stringValue") or "").strip()
                    break
            if external_job_id:
                job_url = (
                    "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/"
                    f"recruitment.html?cid={cid}&ccId={cc_id}&lang=en_US"
                    f"&selectedMenuKey=CareerCenter&jobId={external_job_id}"
                )
            else:
                # No ExternalJobID found (not confirmed to ever actually
                # happen live, but the field is populated by ADP itself,
                # not guaranteed) — fall back to the raw API URL rather
                # than emit a jobId=-less link that can't possibly resolve
                # to the right posting.
                job_url = api_detail_url

            jobs.append({
                "title": str(title).strip(),
                "url": job_url,
                "company": company_name,
                "location": location,
                "country": country,
                "department": "",
                "workplace_type": "",
                "employment_type": "",
                "salary": "",
                "description_snippet": "",  # filled by _fetch_adp_description
                "source_ats": "ADP",
                "slug": slug,
                # Internal only — NOT a real job-schema field, dropped by
                # supabase_handler._build_row's explicit whitelist before
                # any DB write. _fetch_adp_description needs the raw API
                # endpoint (which has requisitionDescription); job["url"]
                # is now the human-facing page instead, which isn't JSON.
                "_adp_api_detail_url": api_detail_url,
            })

        if len(items) < limit:
            break
        offset += limit
        if offset > 1000:  # safety cap
            break

    return jobs


# ── Avature (best-effort generic HTML parser) ────────────

def scrape_avature(slug: str) -> list[dict]:
    """Avature — best-effort HTML scrape of the public career portal.
    Avature is heavily white-labeled (each customer runs their own
    subdomain + skinned template + locale prefix), so there is no single
    reliable markup pattern across customers. This scraper is deliberately
    conservative: it looks for the most common SearchJobs/JobDetail
    markup and JSON-LD, and simply returns fewer/no results for customers
    whose template deviates. Treat Avature coverage as lower-confidence
    than the other platforms in this file.

    Slug is '{subdomain}' (e.g. 'acme' for acme.avature.net).
    List page:   https://{subdomain}.avature.net/careers/SearchJobs
    Detail page: https://{subdomain}.avature.net/careers/JobDetail/{id}
    """
    company_name = slug.replace("-", " ").title()
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    base = f"https://{slug}.avature.net"

    r = None
    for path in ("/careers/SearchJobs", "/careers/SearchJobs/", "/en_US/careers/SearchJobs"):
        r = _get_requests_sync(base + path, headers=headers)
        if r:
            break
    if not r:
        return []

    jobs = []
    seen = set()

    # JSON-LD first, if present (some Avature templates include it)
    for ld_match in re.finditer(
        r'<script[^>]*type="application/ld\+json"[^>]*>([^<]+)</script>',
        r.text, re.I
    ):
        try:
            ld_data = json.loads(ld_match.group(1))
            items = ld_data if isinstance(ld_data, list) else [ld_data]
            for item in items:
                if not isinstance(item, dict) or item.get("@type") != "JobPosting":
                    continue
                job_url = item.get("url", "")
                if not job_url or job_url in seen:
                    continue
                seen.add(job_url)
                loc_obj = item.get("jobLocation", {})
                if isinstance(loc_obj, list) and loc_obj:
                    loc_obj = loc_obj[0]
                addr = loc_obj.get("address", {}) if isinstance(loc_obj, dict) else {}
                loc = addr.get("addressLocality", "") if isinstance(addr, dict) else ""
                desc = _snippet(item.get("description", ""))
                jobs.append({
                    "title": (item.get("title") or "").strip(),
                    "url": job_url,
                    "company": company_name,
                    "location": loc,
                    "country": "",
                    "department": "",
                    "workplace_type": "",
                    "employment_type": item.get("employmentType", ""),
                    "salary": _extract_salary(desc),
                    "description_snippet": desc,
                    "source_ats": "Avature",
                    "slug": slug,
                })
        except Exception:
            continue

    if jobs:
        return jobs

    # Fallback: JobDetail links in raw HTML
    for match in re.finditer(
        r'href=["\']([^"\']*/careers/JobDetail/[^"\']+)["\'][^>]*>\s*(?:<[^>]+>\s*)*([^<]+)</a>',
        r.text, re.I
    ):
        path, title = match.group(1), unescape(match.group(2)).strip()
        job_url = path if path.startswith("http") else base + path
        if job_url in seen or len(title) < 3:
            continue
        seen.add(job_url)

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": "",
            "country": "",
            "department": "",
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "Avature",
            "slug": slug,
        })

    return jobs


# ── PageUp ──────────────────────────────────────────────

def scrape_pageup(slug: str) -> list[dict]:
    """PageUp — HTML scrape of the public job search page.
    Dominant AU/NZ enterprise ATS (Telstra, Commonwealth Bank, Coles,
    etc.). Job data is server-rendered — no JS needed.

    Slug format: 'portalId|source' (e.g. '507|fb').
    List page:   https://careers.pageuppeople.com/{portalId}/{source}/en/
    Detail page: https://careers.pageuppeople.com/{portalId}/{source}/en/job/{jobId}/{title-slug}

    2026-09: the legacy 'ci' source shape is blocked by PageUp's own
    robots.txt (matches a '/ci' disallow rule) — this scraper doesn't
    special-case that (the slug already carries whatever source was
    actually discovered), but a blocked fetch here just returns []
    rather than raising, same as any other robots-disallowed request.
    """
    parts = slug.split("|", 1)
    if len(parts) != 2:
        log.debug(f"Invalid PageUp slug format: {slug} (expected 'portalId|source')")
        return []

    portal_id, source = parts
    base = f"https://careers.pageuppeople.com/{portal_id}/{source}/en"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    r = _get_requests_sync(f"{base}/", headers=headers)
    if not r:
        return []

    jobs = []
    seen = set()

    # 2026-09: migrated location extraction to the DOM-anchored lookup —
    # see scrape_eploy's comment above / _bs4_find_location_near's
    # docstring.
    soup = BeautifulSoup(r.text, "html.parser")
    job_href_re = re.compile(r'/job/(\d+)/([^"\'?#]+)')
    for anchor in soup.find_all("a", href=job_href_re):
        path = (anchor.get("href") or "").strip()
        if not path:
            continue
        href_match = job_href_re.search(path)
        title_slug = href_match.group(2) if href_match else ""
        job_url = path if path.startswith("http") else "https://careers.pageuppeople.com" + path
        if job_url in seen:
            continue
        seen.add(job_url)
        link_text = anchor.get_text(strip=True)
        title = link_text or unquote(title_slug).replace("-", " ").title()

        location, location_status = _bs4_find_location_near(anchor, class_substrings=("location",))

        jobs.append({
            "title": title,
            "url": job_url,
            "company": source.replace("-", " ").title(),
            "location": location,
            "location_status": location_status,
            "country": "",
            "department": "",
            "workplace_type": "",
            "employment_type": "",
            "salary": "",
            "description_snippet": "",
            "source_ats": "PageUp",
            "slug": slug,
        })

    return jobs


# ── Pinpoint ────────────────────────────────────────────

async def scrape_pinpoint(slug: str) -> list[dict]:
    """Pinpoint (UK) — public unauthenticated JSON API.
    Slug is the customer subdomain (e.g. 'acme' for acme.pinpointhq.com).
    Confirmed live: GET https://{slug}.pinpointhq.com/postings.json
    (documented at developers.pinpointhq.com/docs/jobs-json-endpoint)."""
    url = f"https://{slug}.pinpointhq.com/postings.json"
    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"}

    r = await _get(url, headers=headers)
    if not r:
        return []

    try:
        data = r.json()
    except Exception as e:
        log.debug(f"Pinpoint: JSON parse failed for {slug}: {e}")
        return []

    items = data.get("data", [])
    if not isinstance(items, list):
        return []

    company_name = slug.replace("-", " ").title()
    jobs = []
    for item in items:
        if not isinstance(item, dict):
            continue
        job = item.get("job") or {}
        department = (job.get("department") or {}).get("name", "")
        location = (item.get("location") or {}).get("name", "")
        desc = _snippet(item.get("description", ""))
        comp_min = item.get("compensation_minimum")
        comp_max = item.get("compensation_maximum")
        comp_currency = item.get("compensation_currency", "")
        salary = ""
        if comp_min and comp_max:
            salary = f"{comp_currency} {comp_min}-{comp_max}".strip()
        elif not salary:
            salary = _extract_salary(desc)

        job_url = item.get("url") or (
            f"https://{slug}.pinpointhq.com{item.get('path', '')}" if item.get("path") else ""
        )

        jobs.append({
            "title": (item.get("title") or "").strip(),
            "url": job_url,
            "company": company_name,
            "location": location,
            "country": "",
            "department": department,
            "workplace_type": item.get("workplace_type_text", item.get("workplace_type", "")),
            "employment_type": item.get("employment_type_text", item.get("employment_type", "")),
            "salary": salary,
            "description_snippet": desc,
            "source_ats": "Pinpoint",
            "slug": slug,
        })

    return jobs


# ── Flatchr ─────────────────────────────────────────────

def scrape_flatchr(slug: str) -> list[dict]:
    """Flatchr (France) — public unauthenticated JSON API.
    Slug is the company identifier used on careers.flatchr.io.
    Confirmed live: GET https://careers.flatchr.io/company/{slug}.json"""
    url = f"https://careers.flatchr.io/company/{slug}.json"
    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"}

    r = _get_requests_sync(url, headers=headers)
    if not r:
        return []

    try:
        data = r.json()
    except Exception as e:
        log.debug(f"Flatchr: JSON parse failed for {slug}: {e}")
        return []

    items = data.get("items", [])
    if not isinstance(items, list):
        return []

    company_name = slug.replace("-", " ").title()
    jobs = []
    for item in items:
        if not isinstance(item, dict):
            continue
        vacancy = item.get("vacancy") or {}
        title = vacancy.get("title", "")
        vacancy_id = vacancy.get("vacancy_id", vacancy.get("id", ""))
        desc = _snippet(vacancy.get("description", ""))
        salary = vacancy.get("salary", "") or _extract_salary(desc)

        job_url = f"https://careers.flatchr.io/vacancy/{slug}/{vacancy_id}" if vacancy_id else ""

        jobs.append({
            "title": str(title).strip(),
            "url": job_url,
            "company": company_name,
            "location": item.get("locality", "") or item.get("administrative_area_level_1", ""),
            "country": "",
            "department": item.get("metier", ""),
            "workplace_type": "",
            "employment_type": vacancy.get("contract_type", ""),
            "salary": salary,
            "description_snippet": desc,
            "source_ats": "Flatchr",
            "slug": slug,
        })

    return jobs


# ── Jobylon ─────────────────────────────────────────────

_jobylon_sitemap_cache: dict[str, tuple[float, list[str]]] = {}
_JOBYLON_SITEMAP_TTL = 3600  # seconds
# 2026-09: under real async concurrency (PLATFORM_WORKERS lets several
# Jobylon boards' coroutines run at once via asyncio.gather), multiple
# boards could previously race to populate this shared, site-wide cache
# on first access simultaneously — no correctness bug (dict assignment
# is atomic), but wasteful, redundant duplicate sitemap fetches.
# This lock makes every board after the first simply await the
# in-flight fetch's result instead of starting its own.
_jobylon_sitemap_lock = asyncio.Lock()


async def _jobylon_all_job_urls() -> list[str]:
    """Fetch (and cache) every job URL on Jobylon, across every customer.

    2026-09 BUG FIX: emp.jobylon.com/sitemap.xml is a sitemap INDEX
    (confirmed live) — a tiny (~237 byte) <sitemapindex> pointing at one
    child sitemap, currently sitemap-jobs.xml (confirmed live: ~9,100 job
    URLs). The old code parsed /sitemap.xml itself for <loc> entries
    containing "/jobs/" and found none, ever — every scrape_jobylon call
    then had nothing to search and failed 100% of the time (0 jobs / 40
    boards in the last real crawl run). This follows the index to its
    child sitemap(s) instead of assuming /sitemap.xml is the job list.

    Cached for _JOBYLON_SITEMAP_TTL seconds since this is the same
    site-wide resource for every company scraped in a run — refetching
    it per-company would be pure waste."""
    cached = _jobylon_sitemap_cache.get("urls")
    if cached and time.time() - cached[0] < _JOBYLON_SITEMAP_TTL:
        return cached[1]

    async with _jobylon_sitemap_lock:
        # Re-check inside the lock — another board's coroutine may have
        # already populated (or failed-cached) the list while this one
        # was waiting to acquire it.
        cached = _jobylon_sitemap_cache.get("urls")
        if cached and time.time() - cached[0] < _JOBYLON_SITEMAP_TTL:
            return cached[1]

        # A short NEGATIVE cache (distinct from the long positive
        # _JOBYLON_SITEMAP_TTL) stops a real outage from triggering a
        # fresh failing fetch for every single company in the same run —
        # one real request's worth of retrying, not ~40.
        failed_cached = _jobylon_sitemap_cache.get("failed_at")
        if failed_cached and time.time() - failed_cached < 60:
            raise RuntimeError("Jobylon: sitemap fetch failed recently, not retrying yet this run")

        headers = {"User-Agent": random.choice(USER_AGENTS)}
        r = await _get("https://emp.jobylon.com/sitemap.xml", headers=headers)
        if not r:
            _jobylon_sitemap_cache["failed_at"] = time.time()
            raise RuntimeError("Jobylon: sitemap.xml fetch failed")
        try:
            index_root = ET.fromstring(r.content)
        except Exception as e:
            _jobylon_sitemap_cache["failed_at"] = time.time()
            raise RuntimeError(f"Jobylon: sitemap index XML parse failed: {e}") from e

        child_sitemap_urls = [loc.text.strip() for loc in index_root.iter()
                               if loc.tag.endswith("loc") and loc.text]
        if not child_sitemap_urls:
            _jobylon_sitemap_cache["failed_at"] = time.time()
            raise RuntimeError("Jobylon: sitemap index had no child <loc> entries")

        urls: list[str] = []
        for sm_url in child_sitemap_urls:
            sr = await _get(sm_url, headers=headers)
            if not sr:
                continue
            try:
                sm_root = ET.fromstring(sr.content)
            except Exception:
                continue
            for loc in sm_root.iter():
                if loc.tag.endswith("loc") and loc.text and "/jobs/" in loc.text:
                    urls.append(loc.text.strip())

        if not urls:
            _jobylon_sitemap_cache["failed_at"] = time.time()
            raise RuntimeError("Jobylon: sitemap index's child sitemap(s) yielded no job URLs")

        _jobylon_sitemap_cache["urls"] = (time.time(), urls)
        return urls


async def scrape_jobylon(slug: str) -> list[dict]:
    """Jobylon (Nordics) scraper.

    Confirmed live (2026-09): company pages (emp.jobylon.com/companies/
    {id}-{slug}/) are old jQuery pages whose job-listing widget
    (jbl-offer-module.js, keyed by an embedded jbl_company_id JS var)
    loads via a separate client-side call — the static HTML never
    contains real job links, only an unrelated example URL pulled from
    an embedded API-schema blob (a prior attempt at "primary: scrape the
    company page" found 0 real job links across every real tenant
    tried). Job DETAIL pages, however, ARE fully server-rendered (real
    JSON-LD JobPosting, og:title, etc.) and every one of them is listed
    in the site-wide sitemap (see _jobylon_all_job_urls).

    A detail page's HTML does NOT link back to /companies/{id}-{slug}/
    at all (confirmed live: 0/60 sampled pages did — a company-marker
    fallback based on that never matches, regardless of how many pages
    it checks). It CAN be recovered for free from the sitemap URL
    itself, though: every job slug is generated as
    '{job_id}-{company_slug}-{job_title_slug}' (confirmed live, e.g.
    'https://emp.jobylon.com/jobs/384887-varner-creative-studio-assistant/'
    for real tenant slug '2160-varner') — so filtering the site-wide URL
    list for slugs starting with this company's own company_slug finds
    its postings with zero extra requests; only the (few) matches are
    then fetched for their real JobPosting data.

    Known limitation: a company whose real Jobylon slug transliterates
    non-ASCII characters differently than this project's own stored
    slug (e.g. an umlaut spelled out one way vs. another) won't prefix-
    match and will read as zero postings — same class of limitation the
    old sitemap-scan approach had, just via a different mechanism."""
    company_id, sep, company_slug = slug.partition("-")
    if not company_id.isdigit() or not company_slug:
        raise RuntimeError(f"Jobylon: invalid slug {slug!r}; expected '<numeric_id>-<company-slug>'")
    company_name = company_slug.replace("-", " ").title()
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    job_urls = await _jobylon_all_job_urls()
    slug_pat = re.compile(rf"/jobs/\d+-{re.escape(company_slug)}(?:-|/)", re.I)
    matches = [u for u in job_urls if slug_pat.search(u)]
    if not matches:
        # A complete, successful site-wide sitemap fetch with no slug
        # match is allowed to represent a genuinely empty/closed board.
        return []

    def parse_one(html: str, job_url: str) -> dict | None:
        title = desc = location = country = ""
        for script in BeautifulSoup(html, "html.parser").find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or script.get_text())
            except Exception:
                continue
            vals = data if isinstance(data, list) else [data]
            for item in vals:
                if not isinstance(item, dict) or item.get("@type") != "JobPosting":
                    continue
                title = _text(item.get("title"))
                desc = _snippet(item.get("description") or "")
                loc = item.get("jobLocation") or {}
                if isinstance(loc, list): loc = loc[0] if loc else {}
                addr = loc.get("address") if isinstance(loc, dict) else {}
                addr = addr if isinstance(addr, dict) else {}
                location = ", ".join(str(x).strip() for x in (addr.get("addressLocality"), addr.get("addressRegion")) if x)
                country = str(addr.get("addressCountry") or "")
                break
        if not title:
            m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
            title = _text(m.group(1)) if m else ""
        if not title:
            return None
        return {
            "title": title, "url": job_url, "company": company_name,
            "location": location, "country": country, "department": "",
            "workplace_type": "", "employment_type": "", "salary": _extract_salary(desc),
            "description_snippet": desc, "source_ats": "Jobylon", "slug": slug,
        }

    jobs = []
    for job_url in matches:
        r = await _get(job_url, headers=headers)
        if not r:
            continue
        job = await asyncio.to_thread(parse_one, r.text, job_url)
        if job:
            jobs.append(job)

    if not jobs:
        # Real slug matches existed but every fetch/parse failed — a
        # transient network/parse problem, not a genuinely empty board.
        raise RuntimeError(
            f"Jobylon: {len(matches)} sitemap slug match(es) for {slug} but none "
            f"could be fetched/parsed into a job"
        )
    return jobs


# REMOVED 2026-09: scrape_homerun. Its extractor in discovery.py
# (_url_to_slug_homerun) matched ANY "jobs.*" subdomain as a Homerun
# customer, but Homerun customers actually run on their own domain with
# no shared vendor suffix — live verification (WebFetch on 4 sampled
# archive_i "homerun" rows: jobs.hireart.com, jobs.cambly.com,
# jobs.wrkhq.com, jobs.we-mng.com) found 0 of 4 were actually Homerun
# installations (an Ashby customer, a WordPress site, a login portal, and
# a redirect to an unrelated company). 11,892 archive_i rows and 0 real
# jobs ever scraped. See discovery.py's SUPPORTED_ATS removal comment and
# Main/BLACKLISTED_ATS.md for the full writeup.

# Occupop (Ireland): confirmed JS-rendered SPA shell with zero job data in
# raw HTML; the only known API requires a Bearer token (live 403). No
# scraper here anymore — see Main/BLACKLISTED_ATS.md for the full
# evidence. Its ~34 already-discovered archive_i rows were deleted
# 2026-09 (dead weight — could never be scraped into real job data).

# ── Cornerstone OnDemand (csod) ─────────────────────────
# 2026-09: REVERSED out of discovery-only (see discovery.py's
# _url_to_slug_csod docstring and GREYLIST_ATS.md for the full evidence
# trail). Confirmed live, twice, on two independent real tenants (CN
# Rail/cn360.csod.com and Survitec/survitec.csod.com):
#   1. GET https://{tenant}.csod.com/ux/ats/careersite/{siteId}/home?c={tenant}
#      returns a small (~5KB) HTML document whose RAW response body (not
#      something client-JS synthesizes — confirmed via fetch(...,
#      {cache:'no-store'}) before any JS ran) embeds a bootstrap JS blob
#      containing an anonymous bearer JWT ("token":"eyJ...") and the
#      tenant's regional API host ("cloud":"https://{region}.api.csod.com/").
#      Decoding that JWT's own payload shows an "rurls" claim that
#      explicitly whitelists "rec-job-search/external" — this is a
#      deliberately anonymous-accessible route, not an accident/leak.
#   2. POST {cloud}rec-job-search/external/jobs with that bearer token
#      (no cookies/session needed) returns real job data:
#      {"status":"Success","data":{"totalCount":N,"requisitions":[...]}}.
#      Confirmed via two independent externally-sourced research reports
#      (Qwen, DeepSeek) plus this project's own live testing that
#      requests failing with totalCount:0 despite plausible-looking
#      bodies are missing "careerSitePageId" — required, and genuinely
#      NOT derivable from the JWT, the careersites config endpoint, or
#      any page attribute found (confirmed live: it does not reliably
#      equal the URL's careerSiteId — CN Rail's site 3 needed pageId 1,
#      Survitec's site 4 needed pageId 4, no pattern connects the two).
#      The open-source career-ops project's own CSOD provider (GitHub,
#      commit ffbbf41) uses "pageId == careerSiteId" as its only
#      heuristic — matches Survitec but not CN Rail — so that's tried
#      first here (cheapest, matches at least one real case), falling
#      back to brute-forcing a small range exactly as that project does
#      when its own heuristic misses.
_CSOD_SEARCH_PATH = "rec-job-search/external/jobs"
_CSOD_TOKEN_RE = re.compile(r'"token"\s*:\s*"(eyJ[A-Za-z0-9_\-\.]+)"')
_CSOD_CLOUD_RE = re.compile(r'"cloud"\s*:\s*"(https://[a-z0-9.\-]*api\.csod\.com/)"', re.I)
_CSOD_PAGE_ID_BRUTE_FORCE_MAX = 20  # small, cheap range — matches career-ops' own fallback scope


async def _csod_search(search_url: str, headers: dict, site_id: int, page_id: int,
                        page_number: int, page_size: int = 25) -> dict | None:
    """One POST to the anonymous job-search API. Returns the 'data' object
    on a real Success response, None on any failure/unexpected shape."""
    payload = {
        "careerSiteId": site_id, "careerSitePageId": page_id,
        "pageNumber": page_number, "pageSize": page_size, "cultureId": 1,
        "searchText": "", "cultureName": "en-US",
        "states": [], "countryCodes": [], "cities": [], "placeID": "",
        "radius": None, "postingsWithinDays": None,
        "customFieldCheckboxKeys": [], "customFieldDropdowns": [], "customFieldRadios": [],
    }
    resp = await _post(search_url, json=payload, headers=headers)
    if resp is None or resp.status_code != 200:
        return None
    try:
        data = resp.json()
    except Exception:
        return None
    if data.get("status") != "Success":
        return None
    result = data.get("data")
    return result if isinstance(result, dict) else None


async def scrape_csod(slug: str) -> list[dict]:
    """Cornerstone OnDemand — anonymous-bearer-JWT public JSON API.
    Slug format: 'tenant|careerSiteId' (careerSiteId from the URL path,
    e.g. 'cn360|3' — see discovery.py's _url_to_slug_csod). See the block
    comment above for the full live-verified evidence trail.

    2026-09 ASYNC MIGRATION (batch 2): _csod_search converted from
    _get_session().post() to await _post(...); scrape_csod's own
    bootstrap GET converted to await _get(...). Behavior unchanged."""
    parts = slug.split("|")
    if len(parts) != 2:
        log.debug(f"Invalid Cornerstone (csod) slug format: {slug}")
        return []
    tenant, site_id_str = parts
    if not site_id_str.isdigit():
        log.debug(f"Invalid Cornerstone (csod) careerSiteId in slug: {slug}")
        return []
    site_id = int(site_id_str)

    boot_url = f"https://{tenant}.csod.com/ux/ats/careersite/{site_id}/home?c={tenant}"
    r = await _get(boot_url, headers={"User-Agent": random.choice(USER_AGENTS)})
    if not r:
        return []

    token_match = _CSOD_TOKEN_RE.search(r.text)
    cloud_match = _CSOD_CLOUD_RE.search(r.text)
    if not token_match or not cloud_match:
        log.debug(f"Cornerstone: no bootstrap token/cloud endpoint found for {tenant}")
        return []
    token = token_match.group(1)
    cloud = cloud_match.group(1)
    search_url = f"{cloud}{_CSOD_SEARCH_PATH}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": random.choice(USER_AGENTS),
    }

    # Find a working careerSitePageId: try "same as careerSiteId" first
    # (career-ops' heuristic), then brute-force the rest of a small range.
    page_id_candidates = [site_id] + [
        i for i in range(1, _CSOD_PAGE_ID_BRUTE_FORCE_MAX + 1) if i != site_id
    ]
    working_page_id = None
    first_page = None
    for candidate in page_id_candidates:
        result = await _csod_search(search_url, headers, site_id, candidate, page_number=1)
        if result and result.get("totalCount", 0) > 0:
            working_page_id = candidate
            first_page = result
            break
        # 2026-09: see module-level comment above _pace_host.
        await _pace_host_async(search_url)

    if working_page_id is None or first_page is None:
        log.debug(f"Cornerstone: no working careerSitePageId found for {tenant} "
                  f"(tried 1-{_CSOD_PAGE_ID_BRUTE_FORCE_MAX}) — likely zero open postings")
        return []

    company_name = tenant.replace("-", " ").title()
    total = first_page.get("totalCount", 0)
    page_size = 25
    jobs = []
    seen = set()
    page_number = 1
    page_data = first_page

    while page_data:
        reqs = page_data.get("requisitions", [])
        if not reqs:
            break
        for req in reqs:
            req_id = req.get("requisitionId")
            if req_id is None or req_id in seen:
                continue
            seen.add(req_id)

            locs = req.get("locations") or []
            loc0 = locs[0] if locs and isinstance(locs[0], dict) else {}
            location = ", ".join(x for x in (loc0.get("city"), loc0.get("state")) if x)
            country = loc0.get("country", "")
            desc = _snippet(req.get("externalDescription", ""))

            jobs.append({
                "title": (req.get("displayJobTitle") or "").strip(),
                "url": f"https://{tenant}.csod.com/ux/ats/careersite/{site_id}/home/requisition/{req_id}?c={tenant}",
                "company": company_name,
                "location": location,
                "country": country,
                "department": "",
                "workplace_type": "",
                "employment_type": "",
                "salary": _extract_salary(desc),
                "description_snippet": desc,
                "source_ats": "Cornerstone OnDemand",
                "slug": slug,
            })

        if len(seen) >= total:
            break
        page_number += 1
        # 2026-09: see module-level comment above _pace_host.
        await _pace_host_async(search_url)
        page_data = await _csod_search(search_url, headers, site_id, working_page_id, page_number, page_size)

    return jobs


# ── Paycom ───────────────────────────────────────────────
# 2026-09 STATUS: RE-REGISTERED — a real, verified, plain-HTTP fix, not a
# headless-browser workaround. This went through three states this
# session, in order: (1) "confirmed live" (the ORIGINAL writeup below),
# (2) briefly pulled to discovery-only after shipping 0 jobs in
# production and an initial (incorrect) diagnosis that the token could
# only ever exist in JS runtime memory, (3) THIS state — the real bug
# found and fixed.
#
# The actual root cause: the original implementation searched for a bare
# `eyJ...`-shaped JWT string anywhere in the bootstrap page's HTML. That
# is NOT how Paycom's own career-page bootstraps itself. Found via
# external LLM consultation and independently CONFIRMED by fetching the
# real, current source of elliottdehn/open-jobs' Paycom fetcher — a
# working, maintained, plain-HTTP (no browser) scraper for this exact
# platform: the session token lives inside a `configsFromHost = {...}`
# JS assignment (a JSON object literal) under the key "sessionJWT", with
# the API base nested inside a "libConfig" sub-object (itself a JSON
# STRING needing its own parse) under "atsPortalMantleServiceUrl". A bare
# eyJ-regex either never matched anything real or matched an unrelated
# eyJ-shaped substring elsewhere on the page — either fully explains why
# every single tenant, every run, got 0 jobs despite the search/detail
# API steps below being completely real. Separately (also confirmed
# against that same reference implementation): the token must be sent as
# a lowercase `authorization: <token>` header with the raw JWT value —
# NOT `Authorization: Bearer <token>`. See _paycom_bootstrap and
# scrape_paycom's docstrings for the corrected implementation, and
# GREYLIST_ATS.md for the full three-state writeup.
#
# ORIGINAL (2026-09) verification claim, preserved for the record — the
# API shape it describes (search/detail endpoints, body schema, field
# names) is still believed accurate; only the token-EXTRACTION step was
# wrong, per the fix above. Confirmed live, twice, on two independent
# real Paycom tenants (FUTEK — clientkey 5AA9970AFB7E7320DA597F2CF00E6958
# — and a second unrelated tenant on clientkey
# 74B8425BF3D1B3ACB19CC1353DC5FA0E):
#   1. GET https://www.paycomonline.net/v4/ats/web.php/portal/{clientkey}/career-page
#      returns a real HTML document whose inline bootstrap script embeds
#      (a) a genuine bearer JWT (confirmed working — a separate endpoint,
#      GET .../api/ats/job-titles, returns real data with just this
#      token) and (b) "atsPortalMantleServiceUrl", the tenant's own
#      regional API base (e.g. "https://portal-applicant-tracking.
#      us-cent.paycomonline.net/") — same per-tenant-region pattern as
#      Cornerstone's "cloud" field, extracted dynamically here rather
#      than hardcoding one region.
#   2. POST {base}api/ats/job-posting-previews/search with that bearer
#      token returns real job data — but ONLY with the exact body shape
#      below; skip/take alone (confirmed required — an empty body gets a
#      real 422 validation error) silently return totalCount 0 without
#      the "filtersForQuery" wrapper object with every filter category
#      explicitly present as an empty array/string. This exact shape was
#      obtained from external research (matching the real, live-verified
#      minified-JS identifier names: keywordSearchText, workEnvironments,
#      positionTypes, educationLevels, categories, travelTypes,
#      shiftTypes, otherFilters, sortOption) and confirmed live to return
#      real, complete job listings (titles, locations, truncated
#      descriptions) on both test tenants.
#   3. GET {base}api/ats/job-postings/{jobId} (also just the bearer
#      token) returns the FULL job description, salary, and category —
#      the search endpoint's own description field is truncated.
_PAYCOM_JOB_URL_RE = re.compile(
    r"paycomonline\.net/v4/ats/web\.php/portal/([0-9A-Fa-f]{32})/jobs/(\d+)", re.I
)
_PAYCOM_BASE_URL_RE = re.compile(r'"atsPortalMantleServiceUrl"\s*:\s*"([^"]+)"')
_PAYCOM_TOKEN_RE = re.compile(r"eyJ[A-Za-z0-9_\-]+\.eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+")
_PAYCOM_CONFIGS_MARKER = "configsFromHost = "
_PAYCOM_DEFAULT_BASE = "https://portal-applicant-tracking.us-cent.paycomonline.net/"
_PAYCOM_EMPTY_FILTERS = {
    "distanceFrom": 0, "workEnvironments": [], "positionTypes": [],
    "educationLevels": [], "categories": [], "travelTypes": [], "shiftTypes": [],
    "otherFilters": [], "keywordSearchText": "", "location": "", "sortOption": "",
}
# Short-lived cache so scrape_paycom's pagination AND enrich_descriptions'
# later per-job detail fetches don't each refetch the same tenant's
# bootstrap page — real cost at scale (a tenant with 100 jobs would
# otherwise trigger 100 extra bootstrap fetches during enrichment).
_paycom_bootstrap_cache: dict[str, tuple[str, str]] = {}
_paycom_bootstrap_lock = threading.Lock()


def _paycom_bootstrap(clientkey: str, force: bool = False) -> tuple[str, str] | None:
    """Fetch the tenant's career-page bootstrap HTML and extract (token,
    api_base_url). Cached per clientkey for this process's lifetime — the
    token is scoped to the tenant, not to a single request, and a fresh
    one is cheap to re-derive next run. force=True bypasses the cache —
    used after a 401/403 (see scrape_paycom/_fetch_paycom_description's
    refresh-and-retry-once logic) so a cached-but-expired token isn't
    reused forever.

    2026-09 BUG FIX #2 (this scraper shipped ZERO real jobs from every
    tenant since being added — see GREYLIST_ATS.md's Paycom section for
    the full story). Root cause, found via external LLM consultation and
    independently verified by fetching the real, current
    elliottdehn/open-jobs Paycom fetcher source (a working, maintained,
    plain-HTTP scraper for this same platform — no headless browser
    involved): the bootstrap page's session token does NOT sit loose in
    the HTML as a bare `eyJ...` JWT-shaped string the way the original
    implementation here assumed. It lives inside a
    `configsFromHost = {...}` JS assignment — a JSON object literal —
    under the key "sessionJWT", with the API base nested one level
    deeper inside a "libConfig" sub-object (itself a JSON STRING that
    needs its own json.loads call, not a plain nested object) under
    "atsPortalMantleServiceUrl". A bare regex hunting for anything
    eyJ-shaped in the page is exactly the kind of "grab whatever text
    happens to look right nearby" fragility this project has already
    hit on several OTHER platforms' LOCATION extraction (see the
    _extract_tolerant_text/_bs4_find_location_near helpers above) — same
    failure class, just applied to auth instead of location. It plausibly
    either never matched anything real, or matched an unrelated
    eyJ-shaped substring elsewhere on the page, either of which fully
    explains a 100% failure rate. The OLD regex path is kept below as a
    SECONDARY fallback, tried only when the configsFromHost marker itself
    is missing, in case some tenant's page genuinely differs from the
    confirmed-live example.

    Separately (also confirmed against the same reference implementation,
    field-for-field): the token must be sent as a lowercase
    `authorization: <token>` header with the raw JWT as the value — NOT
    `Authorization: Bearer <token>`. See scrape_paycom/
    _fetch_paycom_description for that half of the fix."""
    with _paycom_bootstrap_lock:
        cached = _paycom_bootstrap_cache.get(clientkey)
    if cached and not force:
        return cached

    # 2026-09 BUG FIX: this returning None on ANY failure (fetch, or
    # token/base regex miss) is what let scrape_paycom silently report
    # "0 jobs" for every single tenant — see scrape_brassring's note
    # above for the general problem. Callers (scrape_paycom,
    # _fetch_paycom_description) now distinguish "no bootstrap" from
    # "genuinely 0 jobs" by raising instead of quietly returning [].
    r = _get_requests_sync(
        f"https://www.paycomonline.net/v4/ats/web.php/portal/{clientkey}/career-page",
        headers={"User-Agent": random.choice(USER_AGENTS)},
    )
    if not r:
        log.debug(f"Paycom: bootstrap page fetch failed for {clientkey}")
        return None

    token, base = None, None

    marker_idx = r.text.find(_PAYCOM_CONFIGS_MARKER)
    if marker_idx != -1:
        start = marker_idx + len(_PAYCOM_CONFIGS_MARKER)
        end = r.text.find(";\n", start)
        blob = r.text[start:end] if end != -1 else r.text[start:start + 20000]
        try:
            cfg = json.loads(blob)
            token = cfg.get("sessionJWT") or None
            lib_config = cfg.get("libConfig")
            if isinstance(lib_config, str):
                try:
                    lib_config = json.loads(lib_config)
                except Exception:
                    lib_config = {}
            if isinstance(lib_config, dict):
                base = lib_config.get("atsPortalMantleServiceUrl") or None
        except Exception as e:
            log.debug(f"Paycom: found configsFromHost but couldn't parse it for {clientkey}: {e}")

    if not token or not base:
        # Fallback — old bare-JWT-regex / raw-string search, only used
        # when the confirmed-live configsFromHost shape isn't present.
        token_match = _PAYCOM_TOKEN_RE.search(r.text)
        base_match = _PAYCOM_BASE_URL_RE.search(r.text)
        token = token or (token_match.group(0) if token_match else None)
        base = base or (base_match.group(1).replace("\\/", "/") if base_match else None)

    if not token:
        log.debug(f"Paycom: no sessionJWT found (configsFromHost or fallback) for {clientkey}")
        return None
    if not base:
        base = _PAYCOM_DEFAULT_BASE
    if not base.endswith("/"):
        base += "/"

    result = (token, base)
    with _paycom_bootstrap_lock:
        _paycom_bootstrap_cache[clientkey] = result
    return result


def _paycom_normalize_location(value) -> str:
    """Defensive normalizer for Paycom's location-ish fields (preview
    items' "locations", detail items' "location"/"secondaryLocations").
    The confirmed field list (from the same reference implementation
    _paycom_bootstrap's docstring cites) names these fields but doesn't
    pin down whether a given tenant returns a bare string, a list of
    strings, or a list of {city, state, ...}-shaped objects — so this
    never lets an unexpected shape reach job["location"] as a raw Python
    repr (e.g. "[{'city': 'Tulsa'}]"). Falls back to "" rather than
    guessing at something worse than blank, same policy as every other
    ATS scraper's location extraction in this file."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        parts = [str(value[k]) for k in
                 ("city", "state", "stateProvince", "province", "country", "countryName")
                 if value.get(k)]
        return ", ".join(parts)
    if isinstance(value, list):
        pieces = []
        for v in value:
            piece = _paycom_normalize_location(v)
            if piece and piece not in pieces:
                pieces.append(piece)
        return "; ".join(pieces)
    return ""


def scrape_paycom(slug: str) -> list[dict]:
    """Paycom — anonymous-bearer-JWT public JSON API.
    Slug is the 32-hex clientkey (see discovery.py's _url_to_slug_paycom —
    slug format unchanged by this reversal, unlike Cornerstone's). See the
    block comment above for the full live-verified evidence trail."""
    clientkey = slug.upper()
    if not re.match(r"^[0-9A-F]{32}$", clientkey):
        log.debug(f"Invalid Paycom (paycom) slug format: {slug}")
        return []

    bootstrap = _paycom_bootstrap(clientkey)
    if not bootstrap:
        raise RuntimeError(f"Paycom: bootstrap (sessionJWT/API base) failed for {clientkey}")
    token, base = bootstrap
    search_url = f"{base}api/ats/job-posting-previews/search"

    def _headers(tok: str) -> dict:
        # 2026-09 BUG FIX: "authorization: <token>" (lowercase key, raw
        # JWT value) — NOT "Authorization: Bearer <token>". See
        # _paycom_bootstrap's docstring for the reference implementation
        # this was verified against.
        return {
            "Content-Type": "application/json",
            "authorization": tok,
            "Locale": "en-US",
            "User-Agent": random.choice(USER_AGENTS),
        }

    jobs = []
    seen = set()
    skip = 0
    take = 25
    total = None
    refreshed_once = False

    while total is None or skip < total:
        payload = {"skip": skip, "take": take, "filtersForQuery": dict(_PAYCOM_EMPTY_FILTERS)}
        try:
            resp = _get_session().post(search_url, json=payload, headers=_headers(token), timeout=REQUEST_TIMEOUT)
            if resp.status_code in (401, 403) and not refreshed_once:
                # Cached token likely expired (or was wrong to begin with)
                # — force one fresh bootstrap and retry this same page
                # once, mirroring the reference implementation's own
                # refresh-and-retry-once pattern (see _paycom_bootstrap's
                # docstring).
                refreshed_once = True
                bootstrap = _paycom_bootstrap(clientkey, force=True)
                if not bootstrap:
                    raise RuntimeError(f"Paycom: token refresh failed for {clientkey} after HTTP {resp.status_code}")
                token, base = bootstrap
                search_url = f"{base}api/ats/job-posting-previews/search"
                resp = _get_session().post(search_url, json=payload, headers=_headers(token), timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                if skip == 0:
                    raise RuntimeError(f"Paycom: search API returned {resp.status_code} for {clientkey}")
                break
            data = resp.json()
        except RuntimeError:
            raise
        except Exception as e:
            if skip == 0:
                raise RuntimeError(f"Paycom: search API request failed for {clientkey}: {e}") from e
            break

        total = data.get("jobPostingPreviewsCount", 0)
        previews = data.get("jobPostingPreviews", [])
        if not previews:
            break

        for item in previews:
            job_id = item.get("jobId")
            if job_id is None or job_id in seen:
                continue
            seen.add(job_id)
            jobs.append({
                "title": (item.get("jobTitle") or "").strip(),
                "url": f"https://www.paycomonline.net/v4/ats/web.php/portal/{clientkey}/jobs/{job_id}",
                "company": "",  # filled in from the detail endpoint during enrichment
                "location": _paycom_normalize_location(item.get("locations")),
                "country": "",
                "department": "",
                "workplace_type": item.get("remoteType") or "",
                "employment_type": item.get("positionType") or "",
                "salary": "",
                "description_snippet": _snippet(item.get("description", "")),
                "source_ats": "Paycom",
                "slug": slug,
            })

        skip += take
        # 2026-09: see module-level comment above _pace_host.
        _pace_host(search_url)

    return jobs


def _fetch_paycom_description(job: dict) -> str:
    """Fetch the FULL description (plus salary/category) from Paycom's
    per-job detail endpoint — the search endpoint's own description field
    is truncated. Re-derives the tenant's bootstrap token/API base from
    job['url'] rather than requiring scrape_paycom to stash extra state
    (matches _fetch_workday_description's convention); _paycom_bootstrap's
    cache means this is a real network fetch only once per tenant."""
    m = _PAYCOM_JOB_URL_RE.search(job.get("url", ""))
    if not m:
        return job.get("description_snippet", "")
    clientkey, job_id = m.group(1).upper(), m.group(2)

    bootstrap = _paycom_bootstrap(clientkey)
    if not bootstrap:
        return job.get("description_snippet", "")
    token, base = bootstrap

    def _detail_headers(tok: str) -> dict:
        # See _paycom_bootstrap's docstring — raw JWT, lowercase header
        # key, not "Authorization: Bearer <tok>".
        return {"authorization": tok, "Locale": "en-US",
                "User-Agent": random.choice(USER_AGENTS)}

    r = _get_requests_sync(f"{base}api/ats/job-postings/{job_id}", headers=_detail_headers(token))
    if not r:
        # Could be a genuinely dead job, or an expired/wrong cached token
        # — _get() doesn't surface the status code, so cheaply try once
        # more with a forced-fresh bootstrap rather than giving up.
        bootstrap = _paycom_bootstrap(clientkey, force=True)
        if not bootstrap:
            return job.get("description_snippet", "")
        token, base = bootstrap
        r = _get_requests_sync(f"{base}api/ats/job-postings/{job_id}", headers=_detail_headers(token))
        if not r:
            return job.get("description_snippet", "")
    try:
        posting = r.json().get("jobPosting", {})
    except Exception:
        return job.get("description_snippet", "")

    desc = _snippet(posting.get("description", ""))
    salary = posting.get("salaryRange", "")
    if salary and not job.get("salary"):
        job["salary"] = salary
    department = posting.get("jobCategory", "")
    if department and not job.get("department"):
        job["department"] = department
    # 2026-09: the detail endpoint's own "location"/"secondaryLocations"
    # fields are a second, often more complete source than the search
    # endpoint's "locations" preview field — use them to backfill (never
    # overwrite) a still-blank location, and fold in any secondary
    # offices for a genuinely multi-location posting.
    if not job.get("location"):
        primary = _paycom_normalize_location(posting.get("location"))
        secondary = _paycom_normalize_location(posting.get("secondaryLocations"))
        combined = "; ".join(p for p in (primary, secondary) if p)
        if combined:
            job["location"] = combined
            job["location_status"] = "extracted_from_detail_page"
    return desc or job.get("description_snippet", "")


# ── Hireology ───────────────────────────────────────────

def scrape_hireology(slug: str) -> list[dict]:
    """Hireology — public unauthenticated JSON API (2026-09, new platform).
    Slug is the tenant identifier used on careers.hireology.com
    (e.g. '1sthonda' for careers.hireology.com/1sthonda/{job_id}/description).

    Confirmed live: GET https://api.hireology.com/v2/public/careers/{slug}
    ?page={page}&page_size={page_size} — no auth/cookies/JS needed, and
    api.hireology.com has no robots.txt at all (404 on /robots.txt, so no
    disallow rule of any kind — confirmed live 2026-09).

    Response shape: {"data": [...], "count", "page", "page_size"}, one
    real full job object per entry (confirmed live against a real tenant:
    id, name, status ("Open"/etc — case as-is from the API, matched
    case-insensitively below), job_description (full HTML, no truncation),
    locations (array of {city, state, zip_code}), remote (bool),
    job_family.name (department), organization.name (company),
    career_site_url (canonical apply-page URL), compensation
    ({comp_range_min, comp_range_max, comp_period, ...} — only present on
    some postings). Paginated; openroles' own scraper caps at 40 pages
    (4,000 postings/tenant) — mirrored here as a defensive ceiling, not
    because any real tenant has been seen hitting it."""
    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"}
    page_size = 100
    max_pages = 40
    company_name = ""
    jobs = []

    for page in range(1, max_pages + 1):
        r = _get_requests_sync(f"https://api.hireology.com/v2/public/careers/{slug}",
                  headers=headers, params={"page": page, "page_size": page_size})
        if not r:
            break
        try:
            payload = r.json()
        except Exception as e:
            log.debug(f"Hireology: JSON parse failed for {slug} page {page}: {e}")
            break

        items = payload.get("data", [])
        if not isinstance(items, list) or not items:
            break

        for j in items:
            if not isinstance(j, dict):
                continue
            if str(j.get("status", "")).strip().lower() != "open":
                continue
            title = (j.get("name") or "").strip()
            if not title:
                continue

            org_name = (j.get("organization") or {}).get("name", "")
            if org_name and not company_name:
                company_name = org_name

            locs = j.get("locations") or []
            if isinstance(locs, list) and locs and isinstance(locs[0], dict):
                loc0 = locs[0]
                location = ", ".join(p for p in (loc0.get("city", ""), loc0.get("state", "")) if p)
            else:
                location = ""
            if not location and j.get("remote"):
                location = "Remote"

            department = (j.get("job_family") or {}).get("name", "")
            desc = _snippet(j.get("job_description", ""))

            comp = j.get("compensation") or {}
            salary = ""
            if comp.get("comp_range_min") and comp.get("comp_range_max"):
                period = f"/{comp['comp_period']}" if comp.get("comp_period") else ""
                salary = f"${comp['comp_range_min']}-${comp['comp_range_max']}{period}"
            elif not salary:
                salary = _extract_salary(desc)

            job_url = j.get("career_site_url") or (
                f"https://careers.hireology.com/{slug}/{j.get('id')}/description" if j.get("id") else ""
            )

            jobs.append({
                "title": title,
                "url": job_url,
                "company": org_name or company_name or slug.replace("-", " ").title(),
                "location": location,
                "country": "",
                "department": department,
                "workplace_type": "Remote" if j.get("remote") else "",
                "employment_type": (j.get("employment_status") or "").strip(),
                "salary": salary,
                "description_snippet": desc,
                "source_ats": "Hireology",
                "slug": slug,
            })

        if len(items) < page_size:
            break

    return jobs


# ── RecruiterBox / Trakstar Hire ──────────────────────────
# 2026-09: added at explicit user request. RecruiterBox rebranded to
# "Trakstar Hire" some years ago but the public API host and the legacy
# {slug}.recruiterbox.com hosted-site domain are both still live — a
# tenant's canonical job URL can come back on EITHER domain (confirmed
# live: a real tenant found via a public web search on the legacy
# `mobilenations.recruiterbox.com/jobs/...` URL returns
# `hosted_url: https://mobilenations.hire.trakstar.com/jobs/...` from the
# API itself) — discovery.py registers both domain suffixes, extracting
# the same `client_name` slug from either one.
#
# Confirmed live (2026-09), two independent sources plus a direct fetch:
# the official API docs (apiv1.recruiterbox.com/frontend_api.html) and a
# real open-source scraper (github.com/sarthakjain004/headstart issue
# #540) both describe, and a live GET against a real tenant
# (client_name=mobilenations) confirmed, a fully public, keyless JSON API:
#   GET https://jsapi.recruiterbox.com/v1/openings/?client_name={slug}
#     &offset={n}&limit=100
# Response: {"meta": {"offset", "limit", "total"}, "objects": [...]}, each
# object carrying title, hosted_url (canonical apply-page URL — on
# whichever of the two domains that tenant currently uses), position_type,
# allows_remote (bool), a structured location (city/state/country/
# zipcode), team, and the FULL HTML description INLINE — no per-job
# detail-page fetch needed, same "no second pass required" shape as
# Hireology above. api.recruiterbox.com/jsapi.recruiterbox.com carries no
# separate robots.txt disallow for this path (the DataDome protection
# real open-source scrapers work around only guards the HOSTED HTML career
# page, not this JSON API host — confirmed by the headstart project
# switching its own scraper to this API specifically to avoid that wall).
def scrape_recruiterbox(slug: str) -> list[dict]:
    """RecruiterBox / Trakstar Hire — public unauthenticated JSON API.
    Slug is the tenant's client_name (e.g. 'mobilenations')."""
    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"}
    limit = 100
    max_pages = 40  # defensive ceiling, same convention as scrape_hireology
    jobs = []

    for page in range(max_pages):
        offset = page * limit
        r = _get_requests_sync("https://jsapi.recruiterbox.com/v1/openings/",
                  headers=headers, params={"client_name": slug, "offset": offset, "limit": limit})
        if not r:
            break
        try:
            payload = r.json()
        except Exception as e:
            log.debug(f"RecruiterBox: JSON parse failed for {slug} offset {offset}: {e}")
            break

        items = payload.get("objects") or []
        if not isinstance(items, list) or not items:
            break

        for j in items:
            if not isinstance(j, dict):
                continue
            title = (j.get("title") or "").strip()
            job_url = (j.get("hosted_url") or "").strip()
            if not title or not job_url:
                continue

            loc = j.get("location") or {}
            if isinstance(loc, dict):
                location = ", ".join(p for p in (loc.get("city", ""), loc.get("state", "")) if p)
                country = (loc.get("country") or "").strip()
            else:
                location = ""
                country = ""
            if not location and j.get("allows_remote"):
                location = "Remote"

            team = j.get("team")
            if isinstance(team, dict):
                team = team.get("name", "")

            desc = _snippet(j.get("description") or "")

            jobs.append({
                "title": title,
                "url": job_url,
                "company": slug.replace("-", " ").replace("_", " ").title(),
                "location": location,
                "country": country,
                "department": team or "",
                "workplace_type": "Remote" if j.get("allows_remote") else "",
                "employment_type": (j.get("position_type") or "").strip(),
                "salary": _extract_salary(desc),
                "description_snippet": desc,
                "source_ats": "RecruiterBox",
                "slug": slug,
            })

        meta = payload.get("meta") or {}
        total = meta.get("total")
        if total is not None and offset + limit >= total:
            break
        if len(items) < limit:
            break

    return jobs


# ── Gem ─────────────────────────────────────────

# Gem's public job board (jobs.gem.com/{slug}) is a client-rendered
# React/Relay app — job data is never present in the raw HTML (confirmed
# live via WebFetch failing to find it, then confirmed via real Chrome
# browser network inspection). It's fetched from a single public, keyless
# GraphQL endpoint: POST https://jobs.gem.com/api/public/graphql/batch,
# body is a JSON array of {operationName, query, variables} objects,
# response is a JSON array of {data: {...}} in the same order — confirmed
# live 2026-09 against a real tenant (jobs.gem.com/dragonfly-careers),
# 200 with real job data, no auth header/cookie of any kind. No
# robots.txt exists on jobs.gem.com at all (404 on /robots.txt), so
# nothing here is disallowed.
_GEM_GRAPHQL_URL = "https://jobs.gem.com/api/public/graphql/batch"

# List query — confirmed live: returns every open posting for a board in
# one call, no pagination params/cursor seen or needed (small per-company
# boards, same assumption this project already makes for isolvedhire's
# single-call list endpoint).
_GEM_LIST_QUERY = """
query JobBoardList($boardId: String!) {
  oatsExternalJobPostings(boardId: $boardId) {
    jobPostings {
      id
      extId
      title
      locations { id name city isoCountry isRemote extId }
      job { id department { id name extId } locationType employmentType }
    }
  }
}
"""

# Per-job description query — confirmed live (200 response) against the
# same real tenant/job used to confirm the list query above.
_GEM_DETAIL_QUERY = """
query ExternalJobPosting($boardId: String!, $extId: String!) {
  oatsExternalJobPosting(boardId: $boardId, extId: $extId) {
    descriptionHtml
  }
}
"""


def _gem_graphql_batch(operations: list[dict]) -> list[dict] | None:
    """POST a batch of GraphQL operations to Gem's public endpoint.
    Returns the parsed JSON array (one entry per operation, in order) or
    None on any failure — callers degrade gracefully rather than raising."""
    headers = {"User-Agent": random.choice(USER_AGENTS), "Content-Type": "application/json",
               "Accept": "application/json"}
    try:
        resp = _get_session().post(_GEM_GRAPHQL_URL, json=operations, headers=headers,
                                     timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return None
        data = resp.json()
    except Exception as e:
        log.debug(f"Gem: GraphQL batch request failed: {e}")
        return None
    return data if isinstance(data, list) else None


def scrape_gem(slug: str) -> list[dict]:
    """Gem — public keyless GraphQL job board (2026-09, new platform).
    Slug is the job-board vanity path used on jobs.gem.com
    (e.g. 'dragonfly-careers' for jobs.gem.com/dragonfly-careers), which
    is exactly the GraphQL $boardId variable.

    Two-step, both confirmed live 2026-09 via real browser network
    inspection (this platform is JS-rendered end to end — no data is
    ever present in the raw HTML):
      1. One "JobBoardList" batch call — every open posting for the
         board (title, locations, department, employment/location type).
      2. One further batch call packing an "ExternalJobPosting" operation
         per job (all in a single POST, matching the "batch" shape of
         the endpoint itself) to pull each job's full descriptionHtml —
         avoids an N-request fan-out for an N-job board.

    No Wappalyzer fingerprint exists for Gem (checked live, absent) and
    it isn't in openroles' tenant registry either (checked live, absent)
    — this is purely a URL_TO_SLUG/CC_PLATFORM_PATTERNS/OpenPostings-map
    discovery target, no bonus source to lean on.

    URL caveat: the per-job page path on jobs.gem.com uses a THIRD opaque
    ID encoding, different from both the GraphQL "id" and "extId" fields
    (confirmed live — navigating to jobs.gem.com/{slug}/{id} using the
    GraphQL "id" value 404'd as "Job not found"; no field in this query
    set decodes to the real page-path token). Rather than construct and
    ship a link confirmed to be wrong, `url` below points at the board's
    listing page instead of a per-job deep link."""
    list_resp = _gem_graphql_batch([
        {"operationName": "JobBoardList", "query": _GEM_LIST_QUERY, "variables": {"boardId": slug}}
    ])
    if not list_resp:
        return []
    try:
        postings = list_resp[0]["data"]["oatsExternalJobPostings"]["jobPostings"]
    except Exception as e:
        log.debug(f"Gem: unexpected list response shape for {slug}: {e}")
        return []
    if not isinstance(postings, list) or not postings:
        return []

    # One batch call for every job's description, matched back up by
    # array position (same order in, same order out — confirmed by the
    # endpoint's own "batch" contract).
    desc_ops = [
        {"operationName": "ExternalJobPosting", "query": _GEM_DETAIL_QUERY,
         "variables": {"boardId": slug, "extId": p.get("extId")}}
        for p in postings if p.get("extId")
    ]
    desc_by_ext_id = {}
    if desc_ops:
        desc_resp = _gem_graphql_batch(desc_ops) or []
        for op, result in zip(desc_ops, desc_resp):
            try:
                html = result["data"]["oatsExternalJobPosting"]["descriptionHtml"]
            except Exception:
                continue
            if html:
                desc_by_ext_id[op["variables"]["extId"]] = html

    company_name = slug.replace("-", " ").title()
    jobs = []
    for p in postings:
        if not isinstance(p, dict):
            continue
        title = (p.get("title") or "").strip()
        ext_id = p.get("extId")
        if not title or not ext_id:
            continue

        locs = p.get("locations") or []
        remote_loc = next((l for l in locs if isinstance(l, dict) and l.get("isRemote")), None)
        if locs and isinstance(locs[0], dict):
            loc0 = remote_loc or locs[0]
            location = loc0.get("name") or loc0.get("city", "")
        else:
            location = ""
        workplace_type = "Remote" if remote_loc else ""

        job_info = p.get("job") or {}
        department = (job_info.get("department") or {}).get("name", "")

        desc_html = desc_by_ext_id.get(ext_id, "")
        desc = _snippet(desc_html) if desc_html else ""

        jobs.append({
            "title": title,
            "url": f"https://jobs.gem.com/{slug}",  # see docstring URL caveat
            "company": company_name,
            "location": location,
            "country": "",
            "department": department,
            "workplace_type": workplace_type,
            "employment_type": (job_info.get("employmentType") or "").replace("_", " ").title(),
            "salary": _extract_salary(desc) if desc else "",
            "description_snippet": desc,
            "source_ats": "Gem",
            "slug": slug,
        })

    return jobs


# ── isolvedhire ─────────────────────────────────────────

# Matches BOTH the raw-JSON form ("domain_id":4412 — if a bootstrap blob
# is ever present verbatim in the static HTML) and the URL-encoded form
# actually confirmed live in the wild (widget hrefs/JS carry it as
# jsParamsJson=%7B%22domain_id%22:4412,... — a URL-encoded JSON blob, not
# double-encoded, so the colon itself is NOT %3A in real examples seen).
# %22 is a literal double-quote; matching both %22domain_id%22 and
# "domain_id" covers every form seen so far without needing a full
# urllib.parse.unquote() pass over the whole page.
# 2026-09 BUG FIX: widened to also accept camelCase "domainId" and a
# JS-assignment form ("domain_id = 123" / "domainId=123"), and the caller
# now also retries against a URL-decoded copy of the page — some tenants'
# bootstrap/config blob lives inside an encoded query string rather than
# the %22-escaped JSON form this regex originally targeted alone. (An
# externally-drafted version of this fix, reviewed before merging, used
# DOUBLE backslashes inside the raw-string literals — r'\\s', r'\\d',
# r'\\b' — which in a Python raw string produces a LITERAL backslash
# character in the compiled pattern, not a whitespace/digit/word-boundary
# metacharacter. Verified live in a Python shell: that version failed to
# match even the original, previously-working "domain_id": 12345 case,
# let alone the new ones — it would have been a silent regression to 0
# jobs for every isolvedhire tenant, not a fix. Single backslashes below,
# confirmed against all 5 real-shape test cases before merging.)
_ISOLVEDHIRE_DOMAIN_ID_RE = re.compile(
    r'(?:'
    r'(?:"|%22)domain_id(?:"|%22)\s*(?::|%3A)\s*(?:["\']?)(\d+)'
    r'|'
    r'(?:"|%22)domainId(?:"|%22)\s*(?::|%3A)\s*(?:["\']?)(\d+)'
    r'|'
    r'\bdomain_id\b\s*[:=]\s*(?:["\']?)(\d+)'
    r'|'
    r'\bdomainId\b\s*[:=]\s*(?:["\']?)(\d+)'
    r')',
    re.I,
)


async def scrape_isolvedhire(slug: str) -> list[dict]:
    """isolvedhire (iSolved Hire) — public unauthenticated JSON API
    (2026-09, new platform). Slug is the customer subdomain
    (e.g. '1stccu' for 1stccu.isolvedhire.com).

    Two-step, confirmed live end-to-end via real browser network
    inspection 2026-09 (this platform is a Vue SPA — its own bootstrap
    JSON isn't reliably present in a plain static-HTML fetch, so the
    domain_id is instead pulled from the FIRST live occurrence of
    "domain_id":N anywhere on the rendered page's own outgoing widget
    calls, e.g. /core/widget/{domain_id}/follow-us — the same numeric ID
    the page's own JobListings component uses to call /core/jobs/):

      1. GET https://{slug}.isolvedhire.com/jobs/ (plain HTML fetch is
         enough to find the embedded domain_id via regex below in the
         common case; confirmed the id is a small stable per-tenant
         integer, not session-specific).
      2. GET https://{slug}.isolvedhire.com/core/jobs/{domain_id}
         ?getParams=%7B%22isInternal%22%3A0%7D — confirmed live 200,
         returns {"success", "data": {"jobs": [...]}}, no pagination.

    robots.txt confirmed live 2026-09: disallows only /admin/, /stats/,
    and the /internaljobs* family — neither /jobs/ nor /core/jobs/ is
    blocked.

    Per openroles' own scraper: list-endpoint job objects carry NO
    description field at all (title/location/comp/category only) — every
    job here gets description_snippet="" and relies on DESCRIPTION_FETCHERS
    (_fetch_generic_description against item['jobUrl']) for enrichment,
    same convention as ADP/Jobvite/etc above."""
    # 2026-09 BUG FIX: three failure points below — board-page fetch,
    # domain_id extraction, and the JSON API call — all used to
    # `return []`/log.debug and swallow the failure, identical to "this
    # employer genuinely has zero open jobs" from crawl_i.py's per-
    # platform aggregator's point of view (see scrape_brassring's note
    # above for the full explanation). Given this platform is a Vue SPA
    # (per this function's own docstring — the domain_id "is enough to
    # find... in the common case", i.e. not guaranteed), a domain_id
    # extraction miss is a real and likely candidate for why every board
    # came back empty at once, and needs to be visible, not silent.
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    board_url = f"https://{slug}.isolvedhire.com/jobs/"
    r = await _get(board_url, headers=headers)
    if not r:
        raise RuntimeError(f"isolvedhire: board page fetch failed for {slug}")

    m = _ISOLVEDHIRE_DOMAIN_ID_RE.search(r.text)
    if not m:
        # Some tenants' bootstrap/config blob lives inside a URL-encoded
        # query string rather than the %22-escaped JSON form matched
        # above — try again against a decoded copy before giving up.
        try:
            decoded_html = unquote(r.text)
        except Exception:
            decoded_html = r.text
        m = _ISOLVEDHIRE_DOMAIN_ID_RE.search(decoded_html)
    if not m:
        raise RuntimeError(f"isolvedhire: couldn't find domain_id for {slug} — "
                            f"page markup may have changed (SPA bootstrap not in static HTML)")
    # The pattern has several alternative groups (domain_id/domainId,
    # JSON-style/JS-assignment-style) — use whichever one actually matched.
    domain_id = next((g for g in m.groups() if g), None)
    if not domain_id:
        raise RuntimeError(f"isolvedhire: domain_id match was empty for {slug}")

    r2 = await _get(f"https://{slug}.isolvedhire.com/core/jobs/{domain_id}",
               headers={**headers, "Accept": "application/json"},
               params={"getParams": '{"isInternal":0}'})
    if not r2:
        raise RuntimeError(f"isolvedhire: jobs API fetch failed for {slug} (domain_id={domain_id})")
    try:
        payload = r2.json()
    except Exception as e:
        raise RuntimeError(f"isolvedhire: JSON parse failed for {slug}: {e}") from e

    items = (payload.get("data") or {}).get("jobs", [])
    if not isinstance(items, list):
        return []

    company_name = slug.replace("-", " ").title()
    jobs = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = (item.get("title") or "").strip()
        if not title:
            continue

        location = item.get("jobLocation") or ", ".join(
            p for p in (item.get("city", ""), item.get("abbreviation", "")) if p)
        salary = ""
        if item.get("minSalary") and item.get("maxSalary"):
            salary = f"${item['minSalary']}-${item['maxSalary']}"

        job_url = item.get("jobUrl") or f"https://{slug}.isolvedhire.com/jobs/{item.get('id', '')}"

        jobs.append({
            "title": title,
            "url": job_url,
            "company": company_name,
            "location": location,
            "country": item.get("iso3", ""),
            "department": item.get("classification") or item.get("jobCategory", ""),
            "workplace_type": item.get("workplaceType", ""),
            "employment_type": item.get("employmentType", ""),
            "salary": salary,
            "description_snippet": "",  # see docstring — list endpoint has none
            "source_ats": "isolvedhire",
            "slug": slug,
        })

    return jobs


SCRAPERS = {
    "rippling": scrape_rippling,
    "greenhouse": scrape_greenhouse,
    "lever": scrape_lever,
    "ashby": scrape_ashby,
    "bamboohr": scrape_bamboohr,
    "icims": scrape_icims,
    "workday": scrape_workday,
    "workable": scrape_workable,
    "recruitee": scrape_recruitee,
    "smartrecruiters": scrape_smartrecruiters,
    "teamtailor": scrape_teamtailor,
    "breezyhr": scrape_breezyhr,
    "jazzhr": scrape_jazzhr,  # REVIVED 2026-09 — see module notes above scrape_jazzhr
    "personio": scrape_personio,
    "joincom": scrape_joincom,
    # ── Newly enabled (confirmed working) ──
    "taleo": scrape_taleo,
    "oracle_cloud_hcm": scrape_oracle_cloud_hcm,
    "paylocity": scrape_paylocity,
    "hrmdirect": scrape_hrmdirect,
    "zoho": scrape_zoho,
    "softgarden": scrape_softgarden,
    # ── New (2026-08): Eploy / Folks HR / JobAdder / Jobvite / ADP / Avature ──
    "eploy": scrape_eploy,
    "folkshr": scrape_folkshr,
    "jobadder": scrape_jobadder,
    "jobvite": scrape_jobvite,
    "adp": scrape_adp,
    "avature": scrape_avature,
    # 2026-09: re-enabled — see scrape_brassring's docstring for the real
    # root cause (missing session priming, not JS-rendering/auth/robots).
    "brassring": scrape_brassring,
    # ── New (2026-09): PageUp / Pinpoint / Flatchr / Jobylon ──
    # (Homerun removed 2026-09 — see the removal comment above scrape_homerun's
    # former location for the verified evidence.)
    "pageup": scrape_pageup,
    "pinpoint": scrape_pinpoint,
    "flatchr": scrape_flatchr,
    "jobylon": scrape_jobylon,
    # 2026-09: Cornerstone OnDemand — REVERSED out of discovery-only, see
    # scrape_csod's block comment above for the full live-verified evidence.
    "csod": scrape_csod,
    # 2026-09: Paycom — RE-REGISTERED. The earlier "requires a headless
    # browser" diagnosis was wrong — see scrape_paycom's block comment
    # above for the real bug (bootstrap token extraction, not a JS-only
    # token) and the verified fix.
    "paycom": scrape_paycom,
    # 2026-09: SAP SuccessFactors (Career Site Builder tenants) — REVERSED
    # out of "genuinely blocked", see scrape_successfactors's block
    # comment above for the full live-verified evidence. Legacy
    # shared-host successfactors.com/.eu tenants are NOT included — those
    # stay confirmed robots.txt-blocked.
    "successfactors": scrape_successfactors,
    # 2026-09: Hireology / isolvedhire — new platforms found via the
    # datascry/openroles GitHub-registry discovery source (discovery.py's
    # fetch_github_registries_slugs); both confirmed live, real public
    # JSON APIs, no robots.txt block, no auth/JS needed. See
    # scrape_hireology/scrape_isolvedhire's own docstrings above for the
    # full live-verified evidence trail.
    "hireology": scrape_hireology,
    "isolvedhire": scrape_isolvedhire,
    # 2026-09: Gem — scrape_gem's own docstring above has the full
    # confirmed-live GraphQL evidence trail (list + batched detail calls,
    # no auth, no robots.txt on jobs.gem.com at all).
    "gem": scrape_gem,
    "recruiterbox": scrape_recruiterbox,
    # No scraper exists for occupop, ukg, or phenom — all 3 confirmed
    # genuinely unscrapeable (robots.txt disallow, JS-only rendering, or
    # an auth-gated API with no public alternative). Full evidence for
    # each: Main/BLACKLISTED_ATS.md. That doc is the single place this
    # list lives now — don't re-add per-platform detail here.
    # YCombinator (Work at a Startup) — NOT an ATS. It's a multi-company
    # job AGGREGATOR (like RemoteOK/Jobicy were), not a single-company
    # ATS, so it was never keyed by per-company slug here. 2026-09: the
    # job_board_scrapers.py file this used to point readers to (and the
    # scrape_ycombinator() it mentioned) was disabled and removed
    # entirely in an earlier cleanup — there is no YC scraper anywhere in
    # this project anymore, and discovery.py's URL_TO_SLUG no longer
    # resolves YC/workatastartup.com URLs to a fake "ycombinator" ATS
    # either (that was producing permanently-unscrapable archive_i rows).
}


async def scrape_board(ats: str, slug: str) -> list[dict]:
    """Dispatch to the correct scraper.

    2026-09 BUG FIX: this used to catch every exception a scraper raised,
    log it, and return [] — which meant NO individual scraper's failure
    could ever reach crawl_i.py's per-platform aggregator (_do_scrape's
    `except Exception: platform_failed += 1`), no matter what that
    scraper itself raised. That's the actual root cause of whole
    platforms (brassring, paycom, jobylon, eploy, jobadder, softgarden,
    isolvedhire) reporting "(0 failed)" across every single board even
    once those scrapers were fixed to raise on a real request/parse
    failure instead of silently returning [] (see each one's own BUG FIX
    comment) — this dispatcher was swallowing that signal right back into
    an empty list one level up, before crawl_i.py ever saw it.
    Re-raising here (instead of catching) is what finally lets a genuine
    failure register as failed, and lets crawl_i.py's own "log first 3
    errors per platform" line show the real reason — a handful of clean
    log lines, not one per board.

    2026-09 ASYNC MIGRATION (batched — see module docstring): this
    dispatcher is `async def` and awaits SCRAPERS' entries. Every scrape_*
    function is being converted to `async def` in ordered batches; until a
    given platform's batch lands, its SCRAPERS entry is still a plain
    (non-async) function that makes its own blocking `requests`/
    `_get_requests_sync` calls.

    2026-09 BUG FIX (real production evidence: full-scale runs producing
    NO per-platform completion logs and no heartbeat "still working" lines
    either, for minutes at a time, across every platform including tiny
    ones): the previous version called `fn(slug)` unconditionally BEFORE
    checking whether it was a coroutine. For an async scrape_* that's
    fine (calling an `async def` just constructs a coroutine object — no
    scraper code runs until it's awaited), but for a still-sync scrape_*
    that line runs the ENTIRE blocking scrape — every retry, every
    backoff `time.sleep()` — directly on THIS coroutine, which asyncio is
    running on the one and only event-loop thread. Since asyncio is
    single-threaded and cooperative, that blocking call doesn't just stall
    its own board: it freezes the entire process. No other platform's
    coroutine can run, and — critically — neither can the heartbeat's own
    `asyncio.wait(timeout=...)` in crawl_i.py, since even a timeout needs
    the loop's scheduler to get a turn. With 20 of 39 platforms (roughly
    half the total board volume) still on plain `def` scrapers, this
    reduced the "concurrent" crawl to one blocking HTTP call at a time,
    process-wide, with silence guaranteed for however long that call's
    retries/backoff took — exactly the symptom reported.
    Fixed by checking asyncio.iscoroutinefunction(fn) up front: an async
    scraper is awaited directly as before; a still-sync one is handed to
    asyncio.to_thread(), which runs it on a real OS thread from the
    event loop's executor (see crawl_i.py's scrape_all() for that
    executor's sizing) — the same model the old ThreadPoolExecutor-based
    version used, so it can never block the loop itself. Once every
    scrape_* is converted, the sync branch is dead code and can be
    removed."""
    fn = SCRAPERS.get(ats.lower())
    if not fn:
        log.warning(f"Unknown ATS: {ats}")
        return []
    try:
        if asyncio.iscoroutinefunction(fn):
            return await fn(slug)
        return await asyncio.to_thread(fn, slug)
    except Exception as e:
        # Record into the same grouped end-of-run summary scrape_workday's
        # own hand-written call sites already use (see _categorize_scrape_error's
        # BUG FIX note) — every platform's raised failures now group the
        # same way, then re-raise unchanged so crawl_i.py's per-platform
        # ok/failed counting keeps working exactly as before.
        _record_scrape_failure(ats, slug, _categorize_scrape_error(e))
        raise


# ── Second-pass: fetch individual job descriptions ─────
# These are called AFTER the role filter, so only a handful
# of jobs need enrichment (not thousands).

def _extract_location_from_html(html: str) -> str:
    """Universal location extractor — works for any ATS job page.
    Tries JSON-LD JobPosting schema first (most reliable), then
    common meta tags, then typical HTML patterns."""
    import json as _json

    # ── 1. JSON-LD structured data (most reliable) ─────────
    for ld_match in re.finditer(
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
        html, re.I | re.DOTALL
    ):
        try:
            ld = _json.loads(ld_match.group(1))
            # Handle @graph arrays
            if isinstance(ld, dict) and "@graph" in ld:
                ld = ld["@graph"]
            if isinstance(ld, list):
                for item in ld:
                    if isinstance(item, dict) and item.get("@type") in ("JobPosting", "jobPosting"):
                        ld = item
                        break
                else:
                    continue
            if not isinstance(ld, dict):
                continue
            if ld.get("@type") not in ("JobPosting", "jobPosting"):
                continue

            # Extract jobLocation
            job_loc = ld.get("jobLocation")
            if not job_loc:
                continue
            locs = job_loc if isinstance(job_loc, list) else [job_loc]
            parts = []
            for loc in locs:
                if isinstance(loc, str):
                    parts.append(loc)
                    continue
                addr = loc.get("address") or loc
                if isinstance(addr, str):
                    parts.append(addr)
                    continue
                city = addr.get("addressLocality", "")
                region = addr.get("addressRegion", "")
                country = addr.get("addressCountry", "")
                if isinstance(country, dict):
                    country = country.get("name", "") or country.get("@id", "")
                loc_str = ", ".join(p for p in [city, region, country] if p)
                if loc_str:
                    parts.append(loc_str)
            if parts:
                return "; ".join(parts[:3])  # cap at 3 locations
        except Exception:
            continue

    # ── 2. Open Graph / meta tags (handle both attribute orders) ──
    meta_loc_tags = [
        ("property", "og:locality"),
        ("name", "geo.placename"),
        ("name", "location"),
    ]
    for attr, val in meta_loc_tags:
        escaped_val = re.escape(val)
        for pat in [
            rf'<meta[^>]*{attr}=["\']{ escaped_val}["\'][^>]*content=["\']([^"\']+)["\']',
            rf'<meta[^>]*content=["\']([^"\']+)["\'][^>]*{attr}=["\']{ escaped_val}["\']',
        ]:
            m = re.search(pat, html, re.I)
            if m:
                loc = m.group(1).strip()
                if loc and len(loc) < 200:
                    return loc

    # ── 3. Common HTML patterns ────────────────────────────
    # 2026-09: these used to capture directly with `([^<]+?)\s*<`, which
    # (even under DOTALL, since `.*?`/`[^<]+?` is lazy) stops at the FIRST
    # `<` it meets — so a location wrapped in one more tag right after the
    # matched class/attribute (e.g. `class="job-location"><span>Berlin
    # </span>`) captured an empty string instead of failing over to a
    # later pattern. Reworked to find just the opening tag, then run it
    # through _extract_tolerant_text so nested wrapper tags get stripped
    # rather than treated as a stop signal. See that helper's block
    # comment for the two live scraper bugs (JazzHR, SuccessFactors) this
    # same fragility caused.
    for pat in [
        r'class="[^"]*(?:job-location|jobLocation|location-name|posting-location)[^"]*"[^>]*>',
        r'data-automation=["\']job-location["\'][^>]*>',
        r'itemprop=["\']jobLocation["\'][^>]*>',
    ]:
        m = re.search(pat, html, re.I)
        if m:
            loc = _extract_tolerant_text(html, m.end(), window=300)
            if loc and len(loc) < 200:
                return loc

    return ""


def _extract_icims_location(html: str) -> str:
    """Extract location from iCIMS job page HTML.
    Tries multiple patterns since iCIMS templates vary."""
    # Pattern 1: iCIMS format "US-XX-CityName" or "XX-XX-City"
    m = re.search(r'\b([A-Z]{2}-[A-Z]{2}-[\w\s\-\.]+?)(?:<|"|\'|\s*\n|\s*<)', html)
    if m:
        return m.group(1).strip()

    # Pattern 2: Page title "Job Title in Location | Careers at Location"
    #   e.g. "Sr Consultant in Remote | Careers at US Nationwide Remote"
    #   Extract the "Careers at [Location]" part (more specific than the first part)
    m = re.search(r'Careers\s+at\s+([^|<"]+)', html, re.I)
    if m:
        loc = m.group(1).strip().rstrip(' .')
        if loc and len(loc) < 100:
            return loc

    # Pattern 2b: Fallback — "Job Title in [Location] |"
    m = re.search(r'<title>[^<]*?\bin\s+([^|<]+?)(?:\s*\|)', html, re.I)
    if m:
        loc = m.group(1).strip().rstrip(' .')
        if loc and len(loc) < 100:
            return loc

    # Pattern 3: og:title meta tag — "Job Title in Location | Careers at Location"
    m = re.search(r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)["\']', html, re.I)
    if m:
        og_title = m.group(1)
        # Try "Careers at [Location]" from og:title
        m2 = re.search(r'Careers\s+at\s+(.+)', og_title, re.I)
        if m2:
            loc = m2.group(1).strip()
            if loc and len(loc) < 100:
                return loc

    # Pattern 4: iCIMS-specific location CSS classes.
    # 2026-09: reworked to use _extract_tolerant_text for the same reason
    # as _extract_location_from_html's pattern 3 — a lazy `(.*?)`/`[^<]+?`
    # capture stops at the FIRST `<` it meets, silently returning "" when
    # the real text is one wrapper tag deeper than the matched class.
    for pat in [
        r'class="[^"]*iCIMS_JobHeader(?:Location|Field)[^"]*"[^>]*>',
        r'class="[^"]*header-location[^"]*"[^>]*>',
        r'class="[^"]*location[^"]*"[^>]*>',
    ]:
        m = re.search(pat, html, re.I)
        if m:
            loc = _extract_tolerant_text(html, m.end(), window=300)
            if loc and len(loc) < 200:
                return loc

    # Pattern 5: Fall back to the universal extractor (JSON-LD, meta tags, etc.)
    return _extract_location_from_html(html)


def _fetch_icims_content(url: str) -> str:
    """Fetch iCIMS job page HTML, handling the iframe wrapper problem.
    Many iCIMS career sites wrap the actual job content in an iframe.
    The real content is at the same URL with ?in_iframe=1.
    Returns the HTML with actual job content, or empty string."""
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    # Strategy 1: Try ?in_iframe=1 first — this gets the ACTUAL content
    #   (bypasses the wrapper page that loads content via iframe)
    iframe_url = url + ("&" if "?" in url else "?") + "in_iframe=1"
    r = _get_requests_sync(iframe_url, headers=headers)
    if r and r.text:
        # Verify we got real iCIMS content (not a redirect/error page)
        text = r.text
        has_icims_content = any(marker in text for marker in [
            "iCIMS_", "icims", "job-description", "JobContent",
            "addressLocality", "JobPosting", "jobLocation",
        ])
        has_real_title = "<title>" in text and "in_iframe" not in text.lower()
        if has_icims_content or has_real_title:
            return text

    # Strategy 2: Try the original URL (some iCIMS sites don't use iframe)
    r = _get_requests_sync(url, headers=headers)
    if r and r.text:
        text = r.text
        # Check if it's a wrapper page (has iframe src pointing to itself)
        has_iframe = re.search(r'<iframe[^>]*src=["\'][^"\']*in_iframe', text, re.I)
        if has_iframe:
            # It's a wrapper — try extracting iframe src and fetch that
            iframe_match = re.search(r'<iframe[^>]*src=["\']([^"\']+)["\']', text, re.I)
            if iframe_match:
                iframe_src = iframe_match.group(1)
                if not iframe_src.startswith("http"):
                    from urllib.parse import urljoin
                    iframe_src = urljoin(url, iframe_src)
                r2 = _get_requests_sync(iframe_src, headers=headers)
                if r2 and r2.text:
                    return r2.text
        return text

    # Strategy 3: Try mobile version (cleaner, no iframe)
    mobile_url = url + ("&" if "?" in url else "?") + "mobile=true&needsRedirect=false"
    r = _get_requests_sync(mobile_url, headers=headers)
    if r and r.text:
        return r.text

    return ""


def _fetch_icims_description(job: dict) -> str:
    """Fetch full description and location from an individual iCIMS job page.
    Also extracts location as a side-effect (updates job dict in place).
    Handles iframe wrapper pages by trying multiple URL variants."""
    html = _fetch_icims_content(job["url"])
    if not html:
        return ""

    # ── Extract location if missing (side-effect) ──────────
    if not job.get("location"):
        loc = _extract_icims_location(html)
        if loc:
            job["location"] = loc

    # ── Extract description ────────────────────────────────
    # iCIMS embeds the FULL JD as JSON-LD in a <script> tag.
    # The meta tags (og:description) only have a ~400 char summary.
    # Try JSON-LD FIRST to get the complete description.

    import json as _json

    # 1. JSON-LD — has the FULL JD (thousands of chars)
    #    Match any script tag containing JSON-LD (some iCIMS sites
    #    omit the type attribute but still embed valid JSON-LD)
    for ld_match in re.finditer(
        r'<script[^>]*>(.*?)</script>',
        html, re.I | re.DOTALL
    ):
        content = ld_match.group(1).strip()
        if not content.startswith("{"):
            continue
        try:
            ld = _json.loads(content)
            if isinstance(ld, dict) and ld.get("@type") == "JobPosting" and ld.get("description"):
                return _snippet(ld["description"])
        except Exception:
            continue

    # 2. og:description — truncated (~400 chars) but better than nothing
    for meta_pat in [
        r'<meta[^>]*property=["\']og:description["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:description["\']',
    ]:
        meta_match = re.search(meta_pat, html, re.I)
        if meta_match:
            desc = meta_match.group(1).strip()
            desc = desc.replace("&nbsp;", " ").replace("&#160;", " ")
            if len(desc) > 100:
                return _snippet(desc)

    # 3. name="description" — sometimes the JD, sometimes a generic blurb
    for meta_pat in [
        r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*name=["\']description["\']',
    ]:
        meta_match = re.search(meta_pat, html, re.I)
        if meta_match:
            desc = meta_match.group(1).strip()
            desc = desc.replace("&nbsp;", " ").replace("&#160;", " ")
            if len(desc) > 100 and "review all of the job details" not in desc.lower():
                return _snippet(desc)

    # 4. iCIMS CSS containers (rare — most sites are JS-rendered)
    for pattern in [
        r'class="iCIMS_JobContent[^"]*"[^>]*>(.*?)</div>',
        r'class="iCIMS_InfoMsg_Job[^"]*"[^>]*>(.*?)</div>',
        r'<div\s+id="job-description"[^>]*>(.*?)</div>',
    ]:
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            text = _snippet(match.group(1))
            if len(text) > 100:
                return text

    # 5. Fallback: main element
    body_match = re.search(r'<main[^>]*>(.*?)</main>', html, re.DOTALL | re.IGNORECASE)
    if body_match:
        text = _snippet(body_match.group(1))
        if len(text) > 100:
            return text
    return ""


_BRASSRING_QA_FIELD_RE = re.compile(
    r'"AnswerValue"\s*:\s*"(?P<value>(?:[^"\\]|\\.)*)"\s*,\s*'
    r'"VerityZone"\s*:\s*"(?P<zone>(?:[^"\\]|\\.)*)"\s*,\s*'
    r'"QuestionType"\s*:\s*"(?P<qtype>(?:[^"\\]|\\.)*)"'
)
_BRASSRING_FIELDS_TO_DISPLAY_RE = re.compile(
    r'"JobDetailFieldsToDisplay"\s*:\s*\{(?P<block>.*?)\}', re.DOTALL
)


def _decode_json_string_fragment(raw: str) -> str:
    """Decode a JSON string literal's escapes (\\", \\\\, \\/, \\uXXXX, ...)
    from a regex-captured fragment that was never parsed as part of a full
    JSON document (see _fetch_brassring_description — the surrounding
    object isn't valid JSON in isolation, so json.loads can't run on the
    whole blob, only on this one string re-wrapped in quotes)."""
    try:
        return json.loads('"' + raw + '"')
    except Exception:
        return raw


def _fetch_brassring_description(job: dict) -> str:
    """Fetch the full description from a BrassRing (TGnewUI) JobDetails page.

    2026-09 BUG FIX: scrape_brassring's working path (HTML search-results
    parsing) never captured a description at all, and the generic fetcher
    (_fetch_generic_description) finds nothing here either — confirmed
    live: BrassRing's rendered page reuses class="description" 30-42
    times for unrelated UI text (tooltips, category labels), so there's
    no unique container to match. The legacy AJAX endpoint
    (/TGnewUI/Search/Ajax/MatchedJobs) that WOULD carry a clean JSON
    description field is also confirmed live to return HTTP 500 on every
    request — that endpoint is dead server-side, not just unreachable.

    The real signal (confirmed live, cross-checked against an external
    LLM's lead): every question/answer field on the page is embedded as
    an HTML-attribute-encoded JSON fragment shaped like
    '"AnswerValue":"<value>","VerityZone":"<zone>","QuestionType":"<type>",...'
    (the outer HTML entity-encodes the JSON's own quotes as &quot;).
    Critically, WHICH VerityZone key holds the actual job-description
    prose is tenant-configurable, not a fixed literal string — confirmed
    live against two real tenants: one's own "JobDetailFieldsToDisplay"
    config named its Summary field literally "jobdescription", while
    another's Summary field was "formtext12", with "jobdescription"
    appearing only inside that tenant's own Section2Fields list instead.
    So this reads the field name(s) to use from the page's own
    JobDetailFieldsToDisplay.Summary/.Section2Fields, rather than
    assuming a literal field name, and only trusts fields whose
    QuestionType is "textarea" (real prose) — not "text"/"select"/"date"
    (short metadata like autoreq/department/hotjob), which is what tells
    apart a real JD section from an unrelated short field regardless of
    what that tenant happens to have named it."""
    url = job.get("url", "")
    if not url:
        return job.get("description_snippet", "")
    r = _get_requests_sync(url, headers={"User-Agent": random.choice(USER_AGENTS)})
    if not r:
        return job.get("description_snippet", "")

    html = unescape(r.text)
    fields = [
        {"value": _decode_json_string_fragment(m.group("value")),
         "zone": m.group("zone"), "qtype": m.group("qtype")}
        for m in _BRASSRING_QA_FIELD_RE.finditer(html)
    ]
    if not fields:
        return job.get("description_snippet", "")
    by_zone: dict[str, dict] = {}
    for f in fields:
        # A zone can repeat (e.g. across "show more" duplicate blocks) —
        # keep the longest value seen for it.
        existing = by_zone.get(f["zone"])
        if not existing or len(f["value"]) > len(existing["value"]):
            by_zone[f["zone"]] = f

    summary_zone = None
    section2: list[str] = []
    disp_match = _BRASSRING_FIELDS_TO_DISPLAY_RE.search(html)
    if disp_match:
        block = disp_match.group("block")
        sm = re.search(r'"Summary"\s*:\s*"([^"]*)"', block)
        if sm:
            summary_zone = sm.group(1)
        s2m = re.search(r'"Section2Fields"\s*:\s*\[(.*?)\]', block, re.DOTALL)
        if s2m:
            section2 = re.findall(r'"([^"]*)"', s2m.group(1))

    candidate_zones = ([summary_zone] if summary_zone else []) + section2
    parts = []
    seen_zones = set()
    for zone in candidate_zones:
        if zone in seen_zones:
            continue
        seen_zones.add(zone)
        f = by_zone.get(zone)
        if f and f["qtype"].lower() == "textarea" and f["value"]:
            parts.append(f["value"])

    if not parts:
        # This tenant's config didn't name the JD field(s) where expected
        # — fall back to any real-prose textarea field on the page.
        parts = [f["value"] for f in fields
                 if f["qtype"].lower() == "textarea" and len(f["value"]) > 100]

    if not parts:
        return job.get("description_snippet", "")
    return _snippet(" ".join(parts))


def _fetch_workday_description(job: dict) -> str:
    """Fetch full description from a Workday job detail API.
    Job URL format: https://{company}.wd{N}.myworkdayjobs.com/{site}{path}
    Detail API: POST to /wday/cxs/{company}/{site}{path}"""
    url = job.get("url", "")
    if not url:
        return ""
    # Parse the URL to build the API call
    from urllib.parse import urlparse as _urlparse
    parsed = _urlparse(url)
    hostname = parsed.hostname or ""
    if "myworkdayjobs.com" not in hostname:
        return ""
    company = hostname.split(".")[0]
    # Path is like /{site_id}/job/{path}
    path = parsed.path
    api_url = f"https://{hostname}/wday/cxs{path}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
    }
    try:
        r = _get_session().get(api_url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return ""
        data = r.json()
        posting = data.get("jobPostingInfo", {})
        desc = posting.get("jobDescription", "")
        if desc:
            return _snippet(desc)
    except Exception:
        pass
    return ""


def _fetch_smartrecruiters_description(job: dict) -> str:
    """Fetch full description from SmartRecruiters job detail API.
    Detail endpoint: GET /v1/companies/{slug}/postings/{posting_id}"""
    url = job.get("url", "")
    slug = job.get("slug", "")
    if not url or not slug:
        return ""
    # Extract posting ID from URL: /slug/posting_id
    parts = url.rstrip("/").split("/")
    if len(parts) < 2:
        return ""
    posting_id = parts[-1]
    api_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/{posting_id}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    r = _get_requests_sync(api_url, headers=headers)
    if not r:
        return ""
    try:
        data = r.json()
        # Description is in jobAd.sections.jobDescription.text
        job_ad = data.get("jobAd", {})
        sections = job_ad.get("sections", {})
        desc_section = sections.get("jobDescription", {})
        desc = desc_section.get("text", "")
        if desc:
            return _snippet(desc)
        # Fallback: companyDescription
        comp_desc = sections.get("companyDescription", {}).get("text", "")
        if comp_desc:
            return _snippet(comp_desc)
    except Exception:
        pass
    return ""


def _fetch_adp_description(job: dict) -> str:
    """Fetch full description from ADP's per-requisition DETAIL endpoint.
    scrape_adp() stashes this exact detail URL
    (.../job-requisitions/{itemID}?cid=...&ccId=...) in the internal
    '_adp_api_detail_url' key — confirmed live to return the same fields
    as the list endpoint PLUS one extra field, requisitionDescription (raw
    HTML: intro, duties, requirements, benefits, etc.), which does NOT
    exist on the list endpoint at all.

    2026-09: job["url"] itself is no longer this API endpoint — it's now
    the human-facing recruitment.html career-center page (see scrape_adp),
    which isn't JSON at all, so this function reads the stashed internal
    URL instead of job["url"]."""
    url = job.get("_adp_api_detail_url", "")
    if not url:
        return ""
    r = _get_requests_sync(url, headers={
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
    })
    if not r:
        return ""
    try:
        data = r.json()
    except Exception:
        return ""
    desc_html = data.get("requisitionDescription", "")
    return _snippet(desc_html) if desc_html else ""


def _fetch_taleo_description(job: dict) -> str:
    """Fetch full description from a Taleo job detail page."""
    r = _get_requests_sync(job["url"], headers={"User-Agent": random.choice(USER_AGENTS)})
    if not r:
        return ""
    # Taleo pages have description in specific divs
    patterns = [
        r'class="[^"]*jobdescription[^"]*"[^>]*>(.*?)</div>',
        r'id="[^"]*jobdescription[^"]*"[^>]*>(.*?)</div>',
        r'class="[^"]*requisition[Dd]escription[^"]*"[^>]*>(.*?)</div>',
        r'<div\s+class="contentlinepanel"[^>]*>(.*?)</div>',
    ]
    for pattern in patterns:
        match = re.search(pattern, r.text, re.DOTALL | re.IGNORECASE)
        if match:
            return _snippet(match.group(1))
    return ""


# ── Generic description extraction — multi-method fallback chain ────────
# 2026-09: rewritten after a real complaint that regex extraction wasn't
# reliably getting the ENTIRE job description. Two separate problems
# were found and fixed here:
#   1. Priority order was backwards — the (short, SEO-blurb) meta
#      description was tried BEFORE the full job-description container
#      scan, so any page with both a meta description AND a much fuller
#      real JD a few lines below it would silently short-circuit on the
#      ~150-300 char blurb and never see the real content. Meta
#      description is now tried dead last, only if every real-content
#      method below finds nothing.
#   2. Too few extraction methods, and the FIRST one to clear a 50-char
#      floor won even if a much longer/better one was available further
#      down the page. Now every method is tried, and the LONGEST
#      resulting candidate is kept (still each individually gated at
#      >50 chars, so a stray short div can't win by accident).
# New methods added: JSON-LD now scans EVERY <script type="application/
# ld+json"> block (a page often has several — breadcrumbs/org/website —
# with the JobPosting one anywhere among them, not necessarily first)
# and unwraps an "@graph" wrapper; embedded hydration JSON (__NEXT_DATA__/
# __NUXT_DATA__/generic application/json script) is walked for a
# description-shaped field, covering JS-rendered pages that still ship
# their data server-side; itemprop="description" microdata; a broadened
# container class/id list (many more real-world template names); and a
# <main> fallback before giving up.

_JSONLD_SCRIPT_RE = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.I | re.DOTALL,
)


def _extract_jsonld_description(html: str) -> str:
    """Scan EVERY ld+json block (not just the first) for a JobPosting-
    shaped object's description field, unwrapping one level of an
    "@graph" wrapper (common in real-world schema.org markup)."""
    for m in _JSONLD_SCRIPT_RE.finditer(html):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        expanded = []
        for item in items:
            if isinstance(item, dict) and isinstance(item.get("@graph"), list):
                expanded.extend(item["@graph"])
            else:
                expanded.append(item)
        for item in expanded:
            if isinstance(item, dict) and isinstance(item.get("description"), str) \
                    and len(item["description"]) > 50:
                return _snippet(item["description"])
    return ""


_DESC_JSON_SCRIPT_RE = re.compile(
    r'<script[^>]*(?:id=["\']__NEXT_DATA__["\']|id=["\']__NUXT_DATA__["\']|'
    r'type=["\']application/json["\'])[^>]*>(.*?)</script>',
    re.I | re.DOTALL,
)
_DESC_KEY_RE = re.compile(
    r"^(job)?description(html|text)?$|^jobdescription$|^body(html)?$", re.I
)


def _walk_for_description(obj, best: list, depth: int = 0) -> None:
    """Recursively hunt an embedded hydration JSON blob for a
    description-shaped string field, keeping the LONGEST one found —
    some apps nest the real JD several levels deep under a
    'job'/'posting'/'data' wrapper key. `best` is a 1-element list used
    as a mutable accumulator across the recursion."""
    if depth > 14:
        return
    if isinstance(obj, dict):
        for key, val in obj.items():
            if isinstance(val, str) and len(val) > 80 and _DESC_KEY_RE.match(str(key)):
                if len(val) > len(best[0]):
                    best[0] = val
            else:
                _walk_for_description(val, best, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            _walk_for_description(item, best, depth + 1)


def _extract_embedded_json_description(html: str) -> str:
    """Level-2 fallback for JS-rendered pages: many React/Next/Nuxt career
    pages still ship the real JD server-side inside a hydration JSON
    blob even though the visible DOM is client-rendered."""
    best = [""]
    for m in _DESC_JSON_SCRIPT_RE.finditer(html):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        _walk_for_description(data, best)
    return _snippet(best[0]) if best[0] else ""


_ITEMPROP_DESC_RE = re.compile(
    r'itemprop=["\']description["\'][^>]*>(.*?)</(?:div|section|span|p)>',
    re.I | re.DOTALL,
)

# Broadened 2026-09: class OR id, many more real-world container naming
# variants seen across ATS/careers-page templates (previously just
# "job-description|job_description|description|posting-content|
# job-details", which missed a lot of real templates).
_CONTAINER_RE = re.compile(
    r'(?:class|id)="[^"]*(?:job-description|job_description|jobdescription|'
    r'job-details|job_details|jobdetails|posting-content|posting-description|'
    r'careers?-detail|career-content|vacancy-description|job-body|job-content|'
    r'description-content|content-description|opening-description|role-description)'
    r'[^"]*"[^>]*>(.*?)</(?:div|section)',
    re.I | re.DOTALL,
)


async def _fetch_generic_description(job: dict) -> str:
    """Generic description fetcher — loads the job URL and tries, IN
    ORDER, every extraction method that's useful across real career-page
    templates, then keeps the LONGEST usable result rather than stopping
    at the first one that merely clears a length floor. See the module
    comment above for the two real bugs this fixed (meta-description
    tried too early, too few fallback methods).
    Also extracts location as a side-effect if job has no location.

    2026-09 ASYNC MIGRATION (description-enrichment speed pass, explicit
    user request — "fetching descriptions was what really took the most
    time in the old code"): converted to async/httpx (await _get(...))
    ahead of every other description fetcher, since this one function is
    shared by 15 of DESCRIPTION_FETCHERS' 25 platform entries (BreezyHR,
    JazzHR, HRMDirect, Paylocity, Softgarden, Eploy, FolksHR, JobAdder,
    Jobvite, Avature, Zoho, BambooHR, Hireology, isolvedhire, PageUp) plus
    the Stage-2 fallback path every OTHER platform can also hit — the
    single highest-leverage function in the whole enrichment stage.
    Everything below this point is pure CPU (regex/string parsing over
    already-fetched HTML) and was already fine either way."""
    url = job.get("url", "")
    if not url:
        return ""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    r = await _get(url, headers=headers)
    if not r:
        return ""

    html = r.text

    # ── Extract location if missing (side-effect) ──────────
    if not job.get("location"):
        loc = _extract_location_from_html(html)
        if loc:
            job["location"] = loc

    candidates = []

    ld_desc = _extract_jsonld_description(html)
    if ld_desc:
        candidates.append(ld_desc)

    embedded_desc = _extract_embedded_json_description(html)
    if embedded_desc:
        candidates.append(embedded_desc)

    itemprop_match = _ITEMPROP_DESC_RE.search(html)
    if itemprop_match:
        text = _snippet(itemprop_match.group(1))
        if len(text) > 50:
            candidates.append(text)

    container_match = _CONTAINER_RE.search(html)
    if container_match:
        text = _snippet(container_match.group(1))
        if len(text) > 50:
            candidates.append(text)

    article_match = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL | re.I)
    if article_match:
        text = _snippet(article_match.group(1))
        if len(text) > 50:
            candidates.append(text)

    if not candidates:
        main_match = re.search(r'<main[^>]*>(.*?)</main>', html, re.DOTALL | re.I)
        if main_match:
            text = _snippet(main_match.group(1))
            if len(text) > 50:
                candidates.append(text)

    if candidates:
        return max(candidates, key=len)

    # Last resort ONLY: short SEO meta description. Never the real JD
    # (usually ~150-300 chars) — reached only when every method above
    # found nothing at all.
    for meta_pat in [
        r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*name=["\']description["\']',
        r'<meta[^>]*property=["\']og:description["\'][^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:description["\']',
    ]:
        meta_match = re.search(meta_pat, html, re.I)
        if meta_match:
            desc = meta_match.group(1).strip()
            desc = desc.replace("&nbsp;", " ").replace("&#160;", " ")
            if len(desc) > 50:
                return _snippet(desc)

    return ""


async def _fetch_joincom_description(job: dict) -> str:
    """Fetch full description from JOIN.com job detail API."""
    url = job.get("url", "")
    if not url:
        return ""
    # Extract job ID from URL: /companies/{slug}/jobs/{idParam}
    # We need the numeric ID, which requires an extra lookup
    # Try the generic fetcher on the job page (has JSON-LD)
    return await _fetch_generic_description(job)


def _fetch_teamtailor_location(job: dict) -> str:
    """Fetch location from a Teamtailor job page.
    The RSS feed has no location, but individual job pages have JSON-LD.
    Returns existing description if already set (we only need location)."""
    url = job.get("url", "")
    if not url:
        return job.get("description_snippet", "")
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    r = _get_requests_sync(url, headers=headers)
    if not r:
        return job.get("description_snippet", "")

    html = r.text

    # ── Extract location (primary purpose) ─────────────────
    if not job.get("location"):
        loc = _extract_location_from_html(html)
        if loc:
            job["location"] = loc

    # ── Also grab a better description if current one is weak ──
    existing_desc = job.get("description_snippet", "")
    if len(existing_desc) < 100:
        desc = ""
        # Try JSON-LD description
        import json as _json
        for ld_match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.I | re.DOTALL
        ):
            try:
                ld = _json.loads(ld_match.group(1))
                if isinstance(ld, dict) and ld.get("description"):
                    desc = _snippet(ld["description"])
                    break
            except Exception:
                continue
        if desc:
            return desc

    return existing_desc


# Platforms that need description enrichment
DESCRIPTION_FETCHERS = {
    "iCIMS": _fetch_icims_description,
    "Workday": _fetch_workday_description,
    "SmartRecruiters": _fetch_smartrecruiters_description,
    "Taleo": _fetch_taleo_description,
    "BreezyHR": _fetch_generic_description,
    "JazzHR": _fetch_generic_description,  # REVIVED 2026-09 — see scrape_jazzhr's module notes
    "HRMDirect": _fetch_generic_description,
    "Paylocity": _fetch_generic_description,
    "Oracle Cloud HCM": _fetch_generic_description,
    "JOIN": _fetch_joincom_description,
    "Teamtailor": _fetch_teamtailor_location,
    # ── New (2026-08) — none of these expose full descriptions on their
    # list pages, so every job needs a detail-page fetch. The generic
    # fetcher (JSON-LD → meta description → common JD containers) covers
    # all of them since they're plain server-rendered HTML.
    "Softgarden": _fetch_generic_description,
    "Eploy": _fetch_generic_description,
    "FolksHR": _fetch_generic_description,
    "JobAdder": _fetch_generic_description,
    "Jobvite": _fetch_generic_description,
    "Avature": _fetch_generic_description,
    # ADP's list API does NOT include requisitionDescription — confirmed
    # live; only the per-item DETAIL endpoint does (see
    # _fetch_adp_description). An earlier version of this file assumed
    # the list endpoint had it and silently produced empty descriptions
    # for every ADP job.
    "ADP": _fetch_adp_description,
    # Zoho and BambooHR normally get a full description straight from
    # their LIST endpoint (see scrape_zoho / scrape_bamboohr) — no detail
    # fetch is architecturally needed in the common case. But both have a
    # fallback code path that can legitimately produce an EMPTY
    # description_snippet (Zoho's generic-link fallback when structured
    # JSON/JSON-LD parsing fails; BambooHR's undocumented public
    # `/careers/list` feed, which is a different endpoint from BambooHR's
    # official documented Applicant Tracking API — that documented one is
    # confirmed summary-only, so this is a defensive safety net in case
    # the public feed field is ever short/empty for a given posting too).
    # Previously NEITHER had any fallback registered here, so a job that
    # hit either gap silently kept an empty description forever with no
    # way to recover it.
    "Zoho": _fetch_generic_description,
    "BambooHR": _fetch_generic_description,
    # 2026-09: Paycom — the search endpoint's description field is
    # truncated; the real full text (plus salary/category) only comes
    # from the per-job detail endpoint. See _fetch_paycom_description.
    "Paycom": _fetch_paycom_description,
    # 2026-09: SuccessFactors — the /search/ listing page never shows a
    # description snippet at all (title/location/date only), so every
    # job needs this fetch. See _fetch_successfactors_description.
    "SuccessFactors": _fetch_successfactors_description,
    # 2026-09: Hireology's list endpoint already returns the full
    # job_description HTML — no enrichment fetch needed in the common
    # case (same as Zoho/BambooHR above), but registered with the generic
    # fetcher as a defensive fallback for the rare short/empty case.
    "Hireology": _fetch_generic_description,
    # RecruiterBox / Trakstar Hire deliberately NOT registered here (same
    # reasoning as Gem below): scrape_recruiterbox's jsapi.recruiterbox.com
    # listing call already returns the full HTML description inline, and
    # job["url"] (hosted_url) points at the tenant's HOSTED career-page
    # HTML, which real third-party scrapers confirm is DataDome-protected
    # (see scrape_recruiterbox's module comment) — a generic plain-request
    # fallback fetch against that URL would hit a bot-detection wall, not
    # recover a real description, so registering it would only waste a
    # request on the already-rare empty case rather than actually helping.
    # Gem deliberately NOT registered here: scrape_gem already fetches
    # each job's real descriptionHtml via GraphQL in one batched call, and
    # job["url"] points at the board's JS-rendered listing page (see
    # scrape_gem's URL caveat) — a generic HTML fetch against that URL
    # would return the same content-free shell for every job, not a
    # per-job description, so it would only waste requests on the rare
    # already-failed case rather than actually recovering anything.
    # isolvedhire's list endpoint has NO description field at all (see
    # scrape_isolvedhire's docstring) — every job needs this fetch.
    "isolvedhire": _fetch_generic_description,
    # 2026-09 BUG FIX: PageUp — scrape_pageup's search-results page never
    # carried a description at all (hardcoded "" with no enrichment
    # registered anywhere), the only platform in this whole file with that
    # combination. Its own docstring already documents that job detail
    # pages ARE plain server-rendered HTML ("no JS needed"), so the
    # generic fetcher (JSON-LD -> meta description -> common JD
    # containers) applies exactly as it does for Softgarden/Eploy/etc.
    # PageUp is a dominant AU/NZ enterprise ATS (Telstra, Commonwealth
    # Bank, Coles, ...), so this was a real, high-volume gap: every one of
    # its jobs reached location/visa classification with zero description
    # text to find eligibility language in.
    "PageUp": _fetch_generic_description,
    # 2026-09 BUG FIX: BrassRing — scrape_brassring's working scrape path
    # (HTML search-results parsing) never captured a description either,
    # and the generic fetcher can't be reused here (its page reuses
    # class="description" 30-42x for unrelated UI text; see
    # _fetch_brassring_description's docstring for the full evidence
    # trail, including why the legacy AJAX endpoint that WOULD have had a
    # clean description field is confirmed dead server-side).
    "BrassRing": _fetch_brassring_description,
}


# Below this length, a description is treated as "missing" for enrichment
# purposes even if it's non-empty — a real job description is essentially
# never this short. Catches list-endpoint fields that turn out to be a
# short teaser/summary rather than the full JD (the documented risk for
# BambooHR's official Applicant Tracking API, and a plausible failure mode
# for any platform if a company's posting is unusually terse at the source)
# instead of silently accepting a truncated description as "done".
MIN_REAL_DESC_CHARS = 150


def enrich_descriptions(jobs: list[dict], max_workers: int = 150) -> list[dict]:
    """Fetch individual job descriptions for platforms that don't
    include them in the list API. Call this AFTER the role filter
    so we only fetch details for the small subset of CSM/AM jobs.

    Modifies jobs in place and returns the same list.

    2026-09 ASYNC MIGRATION (explicit user request — "fetching
    descriptions was what really took the most time in the old code...
    more important than the [scrape_board fix] we just did"): this used
    to be a flat ThreadPoolExecutor(max_workers=20) regardless of how
    many jobs needed enrichment, capping this whole stage's concurrency
    an order of magnitude below the board-scrape phase's ~250-900. Now
    runs on its own asyncio.run() (same one-loop-for-everything pattern
    as scrape_all()/_scrape_and_cleanup — a shared httpx.AsyncClient
    can't cross event loops, so creation, use, and aclose_http_client()
    all happen inside this one call), dispatching each job's fetcher via
    asyncio.iscoroutinefunction(): the now-async _fetch_generic_description
    (15 of DESCRIPTION_FETCHERS' 25 platform entries, plus every Stage-2
    fallback below) is awaited directly on the shared async client; every
    still-sync specialized fetcher (iCIMS, Workday, SmartRecruiters,
    Taleo, Teamtailor, ADP, Paycom, SuccessFactors, BrassRing) runs via
    asyncio.to_thread() exactly like scrape_board's own still-sync
    scrapers — never called inline, so a slow one can't block any other
    job's fetch. max_workers raised 20 -> 150: this stage only runs on
    the much smaller "jobs missing a description/location after role
    filtering" subset, not the full board count, so a wide semaphore
    here doesn't risk the same host-level overload the scrape phase's
    per-host semaphore already guards against independently."""
    to_enrich = [j for j in jobs
                 if j.get("source_ats") in DESCRIPTION_FETCHERS
                 and (len(j.get("description_snippet") or "") < MIN_REAL_DESC_CHARS
                      or not j.get("location"))]

    if not to_enrich:
        return jobs

    to_enrich_platforms = len(set(j["source_ats"] for j in to_enrich))
    log.info(f"Enriching {len(to_enrich)} jobs (missing description or location) "
             f"across {to_enrich_platforms} platforms...")

    async def _call_fetcher(fetcher, job, sem):
        """Dispatch one job's fetch + host-pacing without ever blocking the
        loop, whichever kind of fetcher this platform still has (mirrors
        scrape_board's own dispatch — see that function's BUG FIX note)."""
        is_async = asyncio.iscoroutinefunction(fetcher)
        async with sem:
            desc = await fetcher(job) if is_async else await asyncio.to_thread(fetcher, job)
        if job.get("url"):
            if is_async:
                await _pace_host_async(job["url"])
            else:
                await asyncio.to_thread(_pace_host, job["url"])
        return desc

    async def _run():
        asyncio.get_running_loop().set_default_executor(
            ThreadPoolExecutor(max_workers=max_workers)
        )
        sem = asyncio.Semaphore(max_workers)
        try:
            async def _fetch_one(job):
                # 2026-09: track whether THIS fetch actually improved the
                # job, not just whether the job has any description
                # afterward — a job that qualified for to_enrich because
                # it had a short-but-real description (< MIN_REAL_DESC_CHARS,
                # still non-empty) already had a truthy description_snippet
                # BEFORE this fetch ran, so checking the after-state alone
                # counted it as "enriched" even when the fetch found nothing
                # new. That inflated the "Enriched X/Y" count above what
                # this pass actually accomplished.
                before_len = len(job.get("description_snippet") or "")
                fetcher = DESCRIPTION_FETCHERS[job["source_ats"]]
                try:
                    desc = await _call_fetcher(fetcher, job, sem)
                    # Only replace the existing description if the fetch
                    # produced something at least as long — a detail-page
                    # fetch can itself fail partially (rate-limited,
                    # JS-rendered shell, changed DOM) and return a
                    # short/empty result. Since this function can now run
                    # on jobs that already have a short-but-real
                    # description (see MIN_REAL_DESC_CHARS), never let a
                    # worse result clobber a better one already in hand.
                    if desc and len(desc) >= before_len:
                        job["description_snippet"] = desc
                        salary = _extract_salary(desc)
                        if salary and not job.get("salary"):
                            job["salary"] = salary
                except Exception as e:
                    log.debug(f"Failed to enrich {job.get('url', '')}: {e}")
                return len(job.get("description_snippet") or "") > before_len

            improved_flags = await asyncio.gather(
                *(_fetch_one(j) for j in to_enrich), return_exceptions=True
            )
            enriched = sum(1 for f in improved_flags if f is True)

            # ── Fallback population: fetch job URL directly for ANY job
            # still missing a JD ── Some ATS APIs don't return
            # descriptions, but the job page itself has one. This catches
            # Workday, iCIMS, SuccessFactors, etc. where the API fetch
            # failed.
            #
            # NOTE: this scans ALL of `jobs`, not just `to_enrich` above,
            # and uses a STRICTER definition of "missing" (completely
            # empty description_snippet) than to_enrich's (short OR
            # missing-location). So a job can be in to_enrich, fail to
            # improve there, and still NOT show up here — e.g. it was
            # only missing LOCATION (already had a full description), or
            # it had a short-but-real description the fetch just
            # couldn't beat. The reconciliation numbers in the summary
            # below make that explicit.
            #
            # 2026-09 BUG FIX: this MUST be computed here, after Stage 1's
            # gather above has actually run and mutated jobs in place —
            # computing it earlier (before Stage 1 started) meant it read
            # every to_enrich job's PRE-fetch empty description_snippet,
            # not just the ones Stage 1 genuinely failed to fill in. That
            # put every to_enrich job through a WASTED second fetch, and
            # — since _fetch_fallback below has no length comparison
            # (unlike Stage 1's), unlike a job that already had a real
            # Stage 1 result — a worse Stage 2 result could silently
            # clobber a perfectly good Stage 1 one. Caught locally via a
            # deliberately flaky-fetcher test before this ever shipped.
            still_missing = [j for j in jobs if not j.get("description_snippet")
                             and j.get("url")]
            from_enrich_pass = 0
            other_platforms = 0
            if still_missing:
                still_missing_urls = {j["url"] for j in still_missing}
                from_enrich_pass = sum(1 for j in to_enrich if j.get("url") in still_missing_urls)
                other_platforms = len(still_missing) - from_enrich_pass

            fallback_ok = 0
            if still_missing:
                async def _fetch_fallback(job):
                    try:
                        async with sem:
                            desc = await _fetch_generic_description(job)
                        if desc:
                            job["description_snippet"] = desc
                            salary = _extract_salary(desc)
                            if salary and not job.get("salary"):
                                job["salary"] = salary
                    except Exception as e:
                        log.debug(f"Fallback fetch failed {job.get('url', '')}: {e}")
                    if job.get("url"):
                        await _pace_host_async(job["url"])
                    return bool(job.get("description_snippet"))

                fallback_flags = await asyncio.gather(
                    *(_fetch_fallback(j) for j in still_missing), return_exceptions=True
                )
                fallback_ok = sum(1 for f in fallback_flags if f is True)

            return enriched, fallback_ok, len(still_missing), from_enrich_pass, other_platforms
        finally:
            await aclose_http_client()

    enriched, fallback_ok, still_missing_count, from_enrich_pass, other_platforms = asyncio.run(_run())
    not_improved = len(to_enrich) - enriched

    # NOTE: Location-only pass removed — it was redundant.
    # _fetch_generic_description (used by both primary and fallback enrichment)
    # already extracts location as a side-effect via _extract_location_from_html.
    # The separate location pass re-fetched the same pages with the same method,
    # achieving only ~0.2% success rate (1/616). Jobs still missing location
    # simply don't have parseable location data on their pages.
    no_location = sum(1 for j in jobs if not j.get("location"))

    # ── One consolidated, self-reconciling summary (2026-09) ──
    # Replaces 4 separate log.info() calls that each reported a number
    # without saying which population it was drawn from — readable only by
    # tracing through this function's code. Every number below either sums
    # to the line above it or explicitly says why it doesn't, so the whole
    # picture is visible from the log alone.
    partial_not_improved = not_improved - from_enrich_pass
    still_empty_after_fallback = still_missing_count - fallback_ok
    summary_lines = [
        "── Description/location enrichment summary ──",
        f"  Stage 1 (API re-fetch): {len(to_enrich)} jobs needed enrichment "
        f"(short/missing description OR missing location) across {to_enrich_platforms} platforms",
        f"    -> {enriched} improved (got a new/longer description)",
        f"    -> {not_improved} did not improve, of which:",
        f"         {from_enrich_pass} were left with a COMPLETELY EMPTY description -> passed to Stage 2 below",
        f"         {partial_not_improved} already had a short/partial description, or were only "
        f"missing location -> not eligible for Stage 2 (which only targets completely-empty descriptions)",
    ]
    if still_missing_count:
        summary_lines += [
            f"  Stage 2 (direct job-page fetch): {still_missing_count} jobs with a completely empty "
            f"description ({from_enrich_pass} carried over from Stage 1 + {other_platforms} from OTHER "
            f"platforms whose own list API returned a blank description, never part of Stage 1)",
            f"    -> {fallback_ok} recovered a description directly from the job page",
            f"    -> {still_empty_after_fallback} still completely empty after Stage 2 (dead link, JS-only "
            f"page, or the page genuinely has no JD text)",
        ]
    else:
        summary_lines.append("  Stage 2 (direct job-page fetch): skipped -- nothing was completely empty")
    summary_lines.append(
        f"  {no_location} jobs (out of all {len(jobs)} scraped, not just the {len(to_enrich)} above) "
        f"still have NO location at all -- their pages have no parseable location data"
    )
    log.info("\n".join(summary_lines))

    return jobs


# ── Application Question Enrichment ─────────────────────
# Custom application-form screening questions ("Are you authorized to work
# in X?", "Do you require visa sponsorship?") are strong signals that a job
# is NOT actually globally open, even when its location field just says
# "Remote". We extract those specific questions — across all 20 ATS
# platforms — and append them to description_snippet so the AI location
# classifier can see them.
#
# Multi-tier fallback per platform (verified per-platform via live research,
# not assumed — see the docstring on each _fetch_*_questions function):
#   Level 1 — public, unauthenticated API that returns question definitions
#             directly (Greenhouse, Ashby, Workable, Recruitee).
#   Level 2 — embedded JSON in the apply page's HTML (BreezyHR's hidden
#             input#questions field; a generic __NEXT_DATA__/JSON-script
#             scan used as a bonus pass inside the Level-3 fallback).
#   Level 3 — _fetch_generic_form_questions(): universal HTML form parser
#             (BeautifulSoup) that walks <input>/<textarea>/<select>
#             elements and resolves each one's <label>. Used as the primary
#             mechanism for platforms with predictable server-rendered
#             apply forms (Lever, Teamtailor, ApplyToJob/JazzHR), and as the
#             final fallback for every other platform if its dedicated
#             fetcher finds nothing.
#
# Honest limitation: Rippling, BambooHR, iCIMS, Workday, JOIN, and
# Paylocity render their REAL application form client-side (React/Angular
# SPA) behind session state, or inside a cross-origin iframe (iCIMS), or
# behind partner-gated auth (Workday Staffing API, iCIMS iForms). None of
# that is reachable with plain HTTP requests — it would require a headless
# browser (Playwright/Selenium) driving the actual "Apply" click. We still
# run the Level-3 DOM parser against their best known URL as a best-effort
# attempt (a few tenants may have server-side-rendered fallback markup),
# but expect these to mostly return nothing. That's a real platform
# limitation, not a bug in this code — flagged explicitly here so nobody
# "fixes" it into a false success rate later.
#
# Personio and Taleo were in this list too until 2026-09, when live
# research confirmed each actually has a distinct, real apply-page URL
# (Personio: posting URL + "/apply"; Taleo: swap jobdetail.ftl for
# jobapply.ftl, per Oracle's own docs) — their fetchers now try that URL
# first. Still best-effort (the form itself may still be JS-rendered
# underneath), but it's a real, confirmed-live URL rather than just
# re-fetching the listing page.

_WORK_AUTH_RE = re.compile(
    # 2026-09 FIX (real production evidence: Federato's "Senior Customer
    # Success Manager" on Greenhouse, job id 5391941008) — the screening
    # question label was "Are you eligible to work in the United States or
    # Canada?", which this regex previously never matched at all: the only
    # "...to work" alternative required the word "authorized"/"authorised"
    # specifically, so an "eligible to work in <country>" phrasing (just as
    # common in the wild as "authorized to work") was silently invisible to
    # enrich_application_questions() — the question never even became an
    # "Application Question:" line for classifier.py's
    # has_hard_country_specific_auth_signal to see, regardless of how good
    # that function's own country-matching got. Fixed by folding
    # "authorized/eligible/entitled/permitted to work" into one alternative
    # (mirrors the phrasing set classifier.py's _COUNTRY_AUTH_RE already
    # expects on the other end of this pipeline).
    r"((?:authorized?|authorised?|eligible|entitled|permitted)\s*to\s*work|"
    r"eligib\w*\s*to\s*work|work\s*authoriz|visa\s*sponsor|"
    r"immigration\s*sponsor|right\s*to\s*work|work\s*permit|"
    r"employment\s*eligib|legally\s*authorized|"
    r"require.*\bsponsorship\b|"
    r"do\s*you\s*now\s*or\s*in\s*the\s*future\s*require)",
    re.I,
)


def _clean_label(text: str) -> str:
    """Normalize a question label: unescape entities, strip tags/whitespace,
    strip trailing required-markers (*, ✱)."""
    if not text:
        return ""
    text = unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"[\*✱]+\s*$", "", text).strip()
    return text


# 2026-09 CRITICAL FIX (explicit user instruction, real production
# evidence): this function used to keep ONLY questions matching
# _WORK_AUTH_RE before ever appending them to description_snippet — every
# other question was silently dropped and NEVER reached classifier.py, no
# matter how relevant. Two real live postings proved this was actively
# hiding hard eligibility restrictions:
#   Sleep Doctor (Greenhouse, People Operations Manager): "Do you
#   currently reside in one of the following: AR, AZ, CA, CO, GA, FL, IL,
#   IA, KY, MD, MN, NC, NV, NY, OH, PA, TX, WI, WA?" — a hard US-state-list
#   residency restriction with NO "authorized"/"eligible"/"sponsor"/"work
#   permit" wording at all, so _WORK_AUTH_RE never matched it and the
#   question was dropped before classifier.py's already-existing
#   has_state_list_restriction_signal() ever got a chance to see it.
#   Together AI (Greenhouse, role redacted): "Are you willing to work four
#   days per week in our San Francisco office?" — a hard onsite-attendance
#   requirement, not a work-authorization question in any sense, so it was
#   dropped the same way.
# Fix: stop pre-filtering by TOPIC (work-auth-shaped wording) and instead
# filter by NOISE (universal PII/identity/EEO boilerplate that's on nearly
# every application form and never carries eligibility signal) — see
# _BOILERPLATE_QUESTION_RE below. Every other screening question, whatever
# it's about, is now appended as an "Application Question: ..." line and
# reaches BOTH classifier.py's deterministic regexes (has_state_list_
# restriction_signal, has_hard_country_specific_auth_signal, the new
# has_office_attendance_signal) and the AI classification stage for
# anything those regexes don't recognize. This does NOT reintroduce the
# earlier "any Application Question line = auto-reject" bug (see
# has_hard_country_specific_auth_signal's own docstring for that history)
# — the downstream functions still only fire on a SPECIFIC, named
# restriction (a country, an enumerated state list, a named office/city),
# never on the mere presence of a screening question. A country-agnostic
# "Are you legally authorized to work in the country where this job is
# located?" (Together AI's own second question, right below the office one
# in the same posting) still correctly passes through untouched.
_BOILERPLATE_QUESTION_RE = re.compile(
    r"^(?:"
    r"first\s*name|last\s*name|full\s*name|preferred\s*name|"
    r"e-?mail(?:\s*address)?|phone(?:\s*number)?|"
    r"r[ée]sum[ée]\s*/?\s*cv|r[ée]sum[ée]|cv|"
    r"cover\s*letter|"
    r"linked\s*in(?:\s*(?:profile|url))?|"
    r"website|portfolio|github|personal\s*website|"
    r"how\s+did\s+you\s+hear\s+about\s+(?:this|us)|referral|referred\s+by|"
    r"pronouns?|"
    r"race(?:\s*/\s*ethnicit\w*)?|ethnicit\w*|gender(?:\s*identity)?|\bsex\b|"
    r"veteran\s*status|disabilit\w*(?:\s*status)?|"
    r"sexual\s*orientation|"
    # 2026-09 (explicit user instruction): age and date-of-birth are
    # universal PII fields with zero location-classification signal,
    # same reasoning as name/email/phone above — added alongside the
    # existing EEO exclusions rather than as a separate category, since
    # the effect (never appended to description_snippet) is identical.
    r"\bage\b|date\s*of\s*birth|birth\s*date|\bdob\b"
    r")\s*[:\?]?\s*$",
    re.I,
)


def _format_screening_questions(questions: list[dict]) -> str:
    """Given [{label, required}, ...], keep every substantive screening
    question — excluding only universal PII/identity fields (name, email,
    phone, resume, cover letter, LinkedIn, website/portfolio) and EEO
    self-identification questions (race, gender, veteran status,
    disability, sexual orientation), which never carry job-eligibility
    signal and would otherwise be pure noise (or, for EEO fields,
    inappropriate to feed into any downstream classification at all).
    Formats survivors as 'Application Question: ...' lines. See the
    comment block above _BOILERPLATE_QUESTION_RE for why this replaced
    the old work-authorization-only pre-filter."""
    lines = []
    for q in questions or []:
        label = (q.get("label") or "").strip()
        if label and not _BOILERPLATE_QUESTION_RE.match(label):
            lines.append(f"Application Question: {label}")
    return "\n".join(lines)


# Backward-compat alias — every existing call site (and any future one)
# gets the broadened behavior automatically. Kept under the old name too
# since "auth questions" is still a reasonable mental model for most of
# what survives the boilerplate filter, even though it's no longer
# filtered BY that topic.
_format_auth_questions = _format_screening_questions


# ── Level 3: universal fallback (embedded JSON + generic DOM form parse) ──

_QUESTION_JSON_SCRIPT_RE = re.compile(
    r'<script[^>]*(?:id=["\']__NEXT_DATA__["\']|type=["\']application/json["\'])[^>]*>(.*?)</script>',
    re.I | re.DOTALL,
)
_QUESTION_KEY_RE = re.compile(
    r"question|screening|prescreen|knockout|custom.?field", re.I
)


def _walk_for_questions(obj, out: list[dict], depth: int = 0):
    """Recursively search a parsed JSON blob for arrays that look like
    application-form question definitions."""
    if depth > 12 or len(out) > 100:
        return
    if isinstance(obj, dict):
        for key, val in obj.items():
            if isinstance(val, list) and _QUESTION_KEY_RE.search(str(key)):
                for item in val:
                    if not isinstance(item, dict):
                        continue
                    raw_label = (
                        item.get("label") or item.get("title") or item.get("text")
                        or item.get("question") or item.get("body") or item.get("prompt") or ""
                    )
                    label = _clean_label(str(raw_label))
                    if label:
                        required = bool(item.get("required") or item.get("isRequired"))
                        out.append({"label": label, "required": required})
            else:
                _walk_for_questions(val, out, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            _walk_for_questions(item, out, depth + 1)


def _find_embedded_questions(html_text: str) -> list[dict]:
    """Level 2 sub-fallback: scan __NEXT_DATA__ / application-json <script>
    blocks for embedded question definitions."""
    for m in _QUESTION_JSON_SCRIPT_RE.finditer(html_text):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        found: list[dict] = []
        _walk_for_questions(data, found)
        if found:
            return found
    return []


def _parse_form_elements(html_text: str) -> list[dict]:
    """Absolute Level-3 fallback: blindly parse <input>/<textarea>/<select>
    elements in the page and resolve each one's label."""
    soup = BeautifulSoup(html_text, "lxml")
    form = soup.find("form") or soup

    questions = []
    for el in form.find_all(["input", "textarea", "select"]):
        el_type = el.name if el.name != "input" else (el.get("type") or "text").lower()
        if el_type in ("hidden", "submit", "button", "image", "reset", "file"):
            continue

        label = ""
        el_id = el.get("id")
        if el_id:
            label_tag = soup.find("label", attrs={"for": el_id})
            if label_tag:
                label = label_tag.get_text(" ", strip=True)
        if not label:
            parent_label = el.find_parent("label")
            if parent_label:
                label = parent_label.get_text(" ", strip=True)
        label = _clean_label(label)
        if not label:
            continue

        required = el.has_attr("required") or (el.get("aria-required") == "true")
        questions.append({"label": label, "required": required})

    return questions


def _fetch_generic_form_questions(url: str) -> list[dict]:
    """Universal Level-3 fallback used by every platform: fetch a URL and
    try embedded JSON first, then raw form-element parsing. Returns []
    (not an exception) on any failure — callers treat that as 'no signal
    found', which is expected and fine for JS-rendered platforms."""
    if not url:
        return []
    r = _get_requests_sync(url, headers={"User-Agent": random.choice(USER_AGENTS)})
    if not r:
        return []
    found = _find_embedded_questions(r.text)
    if found:
        return found
    return _parse_form_elements(r.text)


def _generic_form_url_candidates(url: str) -> list[str]:
    """2026-09: several ATS platforms (and plenty of individual white-
    label tenants on ones we DO have a dedicated fetcher for) simply
    serve their real application form at the plain job-posting URL with
    a suffix like "/apply" or "/application" appended — no documented
    API, no special convention, just that. A handful of platforms below
    only ever tried the bare listing URL and nothing else, so a tenant
    using this common pattern was silently missed even though the actual
    form was one cheap extra request away. Returns the bare URL first
    (still worth trying — some platforms DO render the form on the
    listing page itself), then every common suffix VARIATION seen across
    real ATS tenants (2026-09: widened from just "/apply"/"/application"
    — "apply now"/"apply-now" and "application form"/"application-form"
    are both common real button-label-shaped path variants too), de-
    duplicated and order-preserving. This is the LAST-resort guessing
    layer — _fetch_generic_form_questions_multi tries a real, DISCOVERED
    apply link from the page's own content before ever falling back to
    these blind guesses; see that function's docstring."""
    if not url:
        return []
    base = url.rstrip("/")
    candidates = [url]
    if not base.endswith(("/apply", "/application", "/applications/new", "/apply/",
                           "/apply-now", "/application-form")):
        candidates.append(base + "/apply")
        candidates.append(base + "/application")
        candidates.append(base + "/apply-now")
        candidates.append(base + "/application-form")
    seen = set()
    out = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


# ── 2026-09 (explicit user instruction: "find the application links and
# hit it and add that to what regex scans through and what is sent to the
# AI"): rather than only ever GUESSING a suffix on the listing URL, first
# look at the listing page's OWN content for a real "Apply" link/button —
# the exact destination this platform/tenant actually sends a real
# applicant to. A discovered real link beats a guess: it works even when
# a tenant's apply flow lives on a completely different domain/path shape
# than any suffix convention could predict (e.g. a customized in-house
# board whose "Apply Now" button points at a vendor-hosted form entirely
# off the listing page's own host), and it's tried BEFORE the blind
# suffix guesses below so a real link is always preferred over a guess
# when both are available.
_APPLY_LINK_TEXT_RE = re.compile(
    r"apply\s*(?:now|here|today|online)?(?:\s+for\s+this\s+(?:job|role|position))?|"
    r"apply\s+to\s+this\s+(?:job|role|position)|"
    r"submit\s+(?:your\s+)?application|start\s+(?:your\s+)?application|"
    r"begin\s+(?:your\s+)?application|complete\s+(?:the\s+|your\s+)?application|"
    r"application\s+form|start\s+applying|get\s+started",
    re.I,
)
_APPLY_LINK_HREF_RE = re.compile(
    r"/appl(?:y|ication)(?:[-_/]|$)|application[-_]?form|apply[-_]?now",
    re.I,
)


def _discover_real_apply_link(html_text: str, base_url: str) -> str | None:
    """Scans an already-fetched job-posting page for its OWN real 'Apply'
    link/button instead of blindly guessing a URL suffix. Two independent
    signals, either sufficient (mirrors node.py's own
    _extract_job_listing_link_candidates approach for career-listing
    links): (1) the link's visible text/aria-label/title reads like an
    apply CTA (_APPLY_LINK_TEXT_RE), or (2) the link's own URL path/query
    looks apply-shaped (_APPLY_LINK_HREF_RE) — catches a distinct-domain
    redirect to a vendor-hosted apply form that plain suffix-guessing on
    the listing URL would never construct. A link matching both signals
    outranks one matching only one; returns the single highest-scoring
    absolute URL, or None if nothing on the page looks like a real apply
    link at all. Never raises — a malformed page just yields no
    candidate, same as every other best-effort parse in this project."""
    try:
        soup = BeautifulSoup(html_text, "lxml")
    except Exception:
        return None
    best_url = None
    best_score = 0
    try:
        anchors = soup.find_all("a", href=True)
    except Exception:
        return None
    for a in anchors:
        href = a.get("href") or ""
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        text_sources = " ".join([
            a.get_text(" ", strip=True) or "",
            a.get("aria-label") or "",
            a.get("title") or "",
        ])
        score = 0
        if _APPLY_LINK_TEXT_RE.search(text_sources):
            score += 1
        if _APPLY_LINK_HREF_RE.search(href):
            score += 1
        if score == 0 or score <= best_score:
            continue
        try:
            resolved = urljoin(base_url, href)
        except Exception:
            continue
        best_score = score
        best_url = resolved
    return best_url


def _fetch_generic_form_questions_multi(url: str) -> list[dict]:
    """The full Level-3 chase, in priority order: (1) the bare listing
    URL itself — some platforms render the form right there; (2) a REAL
    apply link discovered directly on that same already-fetched page
    (_discover_real_apply_link) — preferred over any guess since it's
    the platform's own actual destination, not an assumption about its
    URL shape; (3) the common suffix-guessing conventions
    (_generic_form_url_candidates) as the last resort, for platforms
    whose apply link isn't a plain <a href> this parser can see (e.g. a
    JS-driven button with no real href at all). Stops at the first
    candidate that yields ANY signal — a fetch that fails outright is
    silently treated the same as one that fetched fine but found
    nothing, matching every other best-effort step in this file."""
    if not url:
        return []

    r = _get_requests_sync(url, headers={"User-Agent": random.choice(USER_AGENTS)})
    if r:
        found = _find_embedded_questions(r.text) or _parse_form_elements(r.text)
        if found:
            return found
        apply_link = _discover_real_apply_link(r.text, url)
        if apply_link and apply_link != url:
            found = _fetch_generic_form_questions(apply_link)
            if found:
                return found

    for candidate in _generic_form_url_candidates(url):
        if candidate == url:
            continue  # already tried above, whether or not the fetch itself succeeded
        found = _fetch_generic_form_questions(candidate)
        if found:
            return found
    return []


# ── Level 1/2: Greenhouse (public API) ──

def _fetch_greenhouse_questions(job: dict) -> str:
    """Fetch application questions from Greenhouse job API.
    Returns a string of work-authorization-related questions, or empty.

    2026-09 ROUND 5 (explicit user request: "make sure application
    questions are being fetched too ... use multiple methods and
    fallbacks"): every early-exit path below (no slug/job-id could be
    resolved, the boards-api call itself failed, or the API returned no
    matching job) now falls back to the same generic DOM/embedded-JSON
    parser (_fetch_generic_form_questions_multi) already used as the
    documented fallback for Lever/Workable/Recruitee/Teamtailor/BreezyHR/
    JazzHR/Zoho above/below, instead of silently giving up with "". This
    is a second, independent extraction method (real HTML on the actual
    apply page) for the same rare case where the otherwise-reliable public
    API path can't resolve a job (e.g. a not-yet-reindexed board, or a
    gh_jid embed whose token couldn't be recovered) — it does not change
    behavior for the normal case where the API succeeds."""
    url = job.get("url", "")

    def _fallback() -> str:
        return _format_auth_questions(_fetch_generic_form_questions_multi(url)) if url else ""

    # Extract board slug and job ID from URL
    # https://job-boards.greenhouse.io/SLUG/jobs/JOBID
    m = re.search(r"greenhouse\.io/([^/]+)/jobs/(\d+)", url)
    if m:
        slug, job_id = m.group(1), m.group(2)
    else:
        # 2026-09: Greenhouse's customer-domain "Job Board" embed —
        # ?gh_jid=<id> on the company's OWN domain, no greenhouse.io host
        # or path shape for the regex above to match at all. The real
        # slug isn't in this URL — it has to be recovered from the job
        # page's own HTML (the embed script/iframe/data-attr Greenhouse's
        # widget leaves behind — see discovery.extract_greenhouse_embed_token),
        # so this branch does its own fetch of the job page first. See
        # discovery.py's _GH_JID_RE/extract_gh_jid_ids/
        # extract_greenhouse_embed_token comments for the full background
        # (confirmed live false positives on spins.com/zesty.ai postings
        # that were classified from a thin fallback snippet because this
        # embed shape was never recognized anywhere in the pipeline).
        jid_match = _GH_JID_RE.search(url)
        if not jid_match:
            return _fallback()
        job_id = jid_match.group(1)
        page = _get_requests_sync(url)
        if not page:
            return _fallback()
        slug = extract_greenhouse_embed_token(page.text)
        if not slug:
            return _fallback()
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}?questions=true"
    r = _get_requests_sync(api_url)
    if not r:
        return _fallback()
    try:
        data = r.json()
    except Exception:
        return _fallback()
    # A recovered gh_jid-embed token is only a candidate (see
    # extract_greenhouse_embed_token's docstring) — reject unless the API
    # actually returned THIS job. Guards against a site reusing the
    # `gh_jid` query-param name for its own unrelated id (confirmed
    # real-world: a HubSpot careers page does exactly this) getting
    # mis-attributed to whatever Greenhouse token happens to also be on
    # the page. Applied even on the standard-URL path above — cheap and
    # correct either way.
    if str(data.get("id", "")) != str(job_id):
        return _fallback()

    # 2026-09: was `if _WORK_AUTH_RE.search(label)` — dropped every
    # non-auth-shaped screening question before it ever reached
    # classifier.py. See _format_screening_questions's docstring (the
    # Sleep Doctor/Together AI real-posting evidence) for why this now
    # keeps every substantive question, filtering only universal PII/EEO
    # boilerplate.
    questions = data.get("questions") or []
    auth_questions = []
    for q in questions:
        label = (q.get("label") or "").strip()
        if label and not _BOILERPLATE_QUESTION_RE.match(label):
            auth_questions.append(f"Application Question: {label}")

    # Also check metadata for location hints (e.g. "United States (Remote)")
    metadata = data.get("metadata") or []
    for md in metadata:
        if isinstance(md, dict):
            name = md.get("name", "").lower()
            val = str(md.get("value", ""))
            if name in ("location", "location_country") and val:
                auth_questions.append(f"Metadata Location: {val}")

    if not auth_questions:
        # API call succeeded but returned zero questions — could genuinely
        # mean this board has none configured, but could also mean the
        # board's actual apply form carries questions the API's own
        # `questions` array doesn't expose for this tenant. Give the
        # generic DOM parser one shot at the real apply page before
        # concluding there's truly nothing (see 2026-09 ROUND 5 note above
        # the function).
        return _fallback()
    return "\n".join(auth_questions)


# ── Level 1/2: Ashby (public API) ──

def _fetch_ashby_questions(job: dict) -> str:
    """Fetch application form from Ashby posting API.
    Returns work-authorization-related form fields, or empty.

    2026-09 ROUND 5 (explicit user request: "use multiple methods and
    fallbacks"): mirrors the same fallback added to
    _fetch_greenhouse_questions above — if the URL doesn't match Ashby's
    known shape, the posting-api call fails, or the API returns no
    substantive fields, fall back to the generic DOM/embedded-JSON parser
    against the real job page rather than giving up with ""."""
    url = job.get("url", "")

    def _fallback() -> str:
        return _format_auth_questions(_fetch_generic_form_questions_multi(url)) if url else ""

    # https://jobs.ashbyhq.com/SLUG/JOBID
    m = re.search(r"ashbyhq\.com/([^/]+)/([a-f0-9-]+)", url)
    if not m:
        return _fallback()
    slug, job_id = m.group(1), m.group(2)

    # Ashby's posting-api/posting endpoint returns form fields
    api_url = f"https://api.ashbyhq.com/posting-api/posting/{slug}/{job_id}"
    r = _get_requests_sync(api_url)
    if not r:
        return _fallback()
    try:
        data = r.json()
    except Exception:
        return _fallback()

    # 2026-09: was `if _WORK_AUTH_RE.search(title)` — see
    # _format_screening_questions's docstring for why every substantive
    # question is now kept, filtering only universal PII/EEO boilerplate.
    auth_questions = []
    # Check applicationFormDefinition for work auth questions
    form_def = data.get("applicationFormDefinition") or data.get("formDefinition") or {}
    sections = form_def.get("sections") or []
    for section in sections:
        fields = section.get("fields") or section.get("fieldEntries") or []
        for field in fields:
            # field might be nested: {field: {title: ...}} or {title: ...}
            f = field.get("field", field) if isinstance(field, dict) else field
            if not isinstance(f, dict):
                continue
            title = (f.get("title", "") or f.get("label", "") or f.get("name", "")).strip()
            if title and not _BOILERPLATE_QUESTION_RE.match(title):
                auth_questions.append(f"Application Question: {title}")

    # Also check surveyQuestions
    survey = data.get("surveyQuestions") or []
    for sq in survey:
        label = (sq.get("label", "") or sq.get("title", "") or sq.get("question", "")).strip()
        if label and not _BOILERPLATE_QUESTION_RE.match(label):
            auth_questions.append(f"Application Question: {label}")

    if not auth_questions:
        # Same reasoning as Greenhouse above: a successful API call with
        # zero fields could be a genuinely question-free posting, or could
        # mean this tenant's form isn't shaped the way applicationForm
        # Definition/surveyQuestions above expect — give the generic DOM
        # parser a shot at the real page before giving up entirely.
        return _fallback()
    return "\n".join(auth_questions)


# ── Level 3 (server-rendered, predictable DOM): Lever ──
# Verified live: /apply pages wrap each question in
# <li class="application-question">, with custom ones additionally tagged
# class="custom-question". Label lives in .application-label .text;
# required questions carry a <span class="required">✱</span>.

def _fetch_lever_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    apply_url = url if url.rstrip("/").endswith("/apply") else url.rstrip("/") + "/apply"
    r = _get_requests_sync(apply_url, headers={"User-Agent": random.choice(USER_AGENTS)})

    questions = []
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        for li in soup.select("li.application-question"):
            label_el = li.select_one(".application-label .text") or li.select_one(".application-label")
            label = _clean_label(label_el.get_text(" ", strip=True)) if label_el else ""
            if not label:
                continue
            required = bool(li.select_one("span.required")) or bool(li.find(attrs={"required": True}))
            questions.append({"label": label, "required": required})

    if not questions:
        questions = _fetch_generic_form_questions(apply_url)
    return _format_auth_questions(questions)


# ── Level 1: Workable (public API) ──
# Verified live: GET https://apply.workable.com/api/v1/jobs/{shortcode}/form
# returns field groups; custom questions live under fields[] with
# label/required/type. shortcode is the alphanumeric segment in the job's
# "/j/{shortcode}/" URL path (no account/auth needed for this endpoint).

_WORKABLE_SHORTCODE_RE = re.compile(r"/j/([A-Za-z0-9]+)")


def _fetch_workable_questions(job: dict) -> str:
    url = job.get("url", "")
    m = _WORKABLE_SHORTCODE_RE.search(url)
    questions = []
    if m:
        shortcode = m.group(1)
        api_url = f"https://apply.workable.com/api/v1/jobs/{shortcode}/form"
        r = _get_requests_sync(api_url, headers={"User-Agent": random.choice(USER_AGENTS)})
        if r:
            try:
                data = r.json()
            except Exception:
                data = None
            if isinstance(data, list):
                for group in data:
                    for field in (group.get("fields") or []) if isinstance(group, dict) else []:
                        label = _clean_label(field.get("label", ""))
                        if label:
                            questions.append({"label": label, "required": bool(field.get("required"))})

    if not questions:
        questions = _fetch_generic_form_questions(url)
    return _format_auth_questions(questions)


# ── Level 1: Recruitee (public API) ──
# Verified live: both the listing (/api/offers/) and detail
# (/api/offers/{offer_slug}) endpoints include open_questions[] inline —
# {body, required, kind, ...}. No extra auth needed.

_RECRUITEE_OFFER_RE = re.compile(r"/o/([^/?#]+)")


def _fetch_recruitee_questions(job: dict) -> str:
    url = job.get("url", "")
    slug = job.get("slug", "")
    m = _RECRUITEE_OFFER_RE.search(url)
    questions = []
    if m and slug:
        offer_slug = m.group(1)
        api_url = f"https://{slug}.recruitee.com/api/offers/{offer_slug}"
        r = _get_requests_sync(api_url, headers={"Accept": "application/json", "User-Agent": random.choice(USER_AGENTS)})
        if r:
            try:
                data = r.json()
            except Exception:
                data = None
            if isinstance(data, dict):
                offer = data.get("offer", data)
                for oq in (offer.get("open_questions") or []):
                    label = _clean_label(oq.get("body", ""))
                    if label:
                        questions.append({"label": label, "required": bool(oq.get("required"))})

    if not questions:
        questions = _fetch_generic_form_questions(url)
    return _format_auth_questions(questions)


# ── Level 3 (server-rendered, predictable DOM): Teamtailor ──
# Verified live: no __NEXT_DATA__ data island exists (despite Teamtailor
# being React-based, careers pages are server-rendered). The apply form is
# plain HTML at /jobs/{id}-{slug}/applications/new; each question is a
# <div class="question">.

def _fetch_teamtailor_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    apply_url = url.rstrip("/") + "/applications/new"
    r = _get_requests_sync(apply_url, headers={"User-Agent": random.choice(USER_AGENTS)})

    questions = []
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        for div in soup.select("div.question"):
            label = _clean_label(div.get_text(" ", strip=True))
            if not label:
                continue
            classes = " ".join(div.get("class") or [])
            required = ("required" in classes or bool(div.find(attrs={"required": True}))
                        or div.get_text().rstrip().endswith("*"))
            questions.append({"label": label, "required": required})

    if not questions:
        questions = _fetch_generic_form_questions(apply_url)
    return _format_auth_questions(questions)


# ── Level 2: BreezyHR (embedded JSON) ──
# Verified live: the /apply page embeds the full question list as JSON in
# <input id="questions" value="[...]">  — {text, type, required, _id}.
# No separate XHR call is made for it; it's server-rendered into the page.

_BREEZY_QUESTIONS_INPUT_RE = re.compile(r'id=["\']questions["\'][^>]*value=["\']([^"\']*)["\']', re.I)
_BREEZY_QUESTIONS_INPUT_RE_ALT = re.compile(r'value=["\']([^"\']*)["\'][^>]*id=["\']questions["\']', re.I)


def _fetch_breezyhr_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    apply_url = url.rstrip("/") + "/apply"
    r = _get_requests_sync(apply_url, headers={"User-Agent": random.choice(USER_AGENTS)})

    questions = []
    if r:
        m = _BREEZY_QUESTIONS_INPUT_RE.search(r.text) or _BREEZY_QUESTIONS_INPUT_RE_ALT.search(r.text)
        if m:
            raw = unescape(m.group(1))
            try:
                data = json.loads(raw)
            except Exception:
                data = None
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    label = _clean_label(item.get("text", ""))
                    if label:
                        questions.append({"label": label, "required": bool(item.get("required"))})

    if not questions:
        questions = _fetch_generic_form_questions(apply_url)
    return _format_auth_questions(questions)


# ── Level 3 (server-rendered, predictable DOM): JazzHR (formerly branded
# ApplyToJob) — REVIVED 2026-09, see scrape_jazzhr's module notes ──
# Verified live (prior to the 2026-08 removal): classic server-rendered
# form (legacy "TheResumator" DOM survives in JazzHR white-label pages).
# Custom questions sit in div.job-form-fields, each with a
# <label id="resumator-questionnaire-q{ID}-label"> and a matching
# #resumator-questionnaire-q{ID} input/select/textarea. Required questions
# have a trailing "*" in the label text. The scraped job listing URL is
# already the apply page itself — no URL transform needed.

def _fetch_jazzhr_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    r = _get_requests_sync(url, headers={"User-Agent": random.choice(USER_AGENTS)})

    questions = []
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        container = soup.select_one("div.job-form-fields") or soup
        for label_el in container.select('label[id^="resumator-questionnaire-"]'):
            raw_label = label_el.get_text(" ", strip=True)
            required = raw_label.rstrip().endswith("*")
            label = _clean_label(raw_label)
            if not label:
                continue
            questions.append({"label": label, "required": required})

    if not questions:
        questions = _fetch_generic_form_questions(url)
    return _format_auth_questions(questions)


# ── Level 2 (best-effort, inconsistent): Zoho Recruit ──
# Research found Zoho sometimes embeds a candidate-module field-layout JSON
# blob (with a custom_field flag) in the listing page's HTML, but this was
# NOT confirmed present on every job-detail page or every org's template —
# treat as a bonus pass, not a guaranteed source. Falls through to the
# generic DOM parser either way.

def _fetch_zoho_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    questions = []
    r = _get_requests_sync(url, headers={"User-Agent": random.choice(USER_AGENTS)})
    if r:
        questions = _find_embedded_questions(r.text)
        if not questions:
            questions = _parse_form_elements(r.text)
    if not questions:
        # 2026-09: fall back to the "/apply"/"/application" convention —
        # the bonus embedded-JSON pass above was only ever confirmed on
        # the listing page itself, not every org's apply flow.
        questions = _fetch_generic_form_questions_multi(url)
    return _format_auth_questions(questions)


# ── Level 2 (unverified schema, best-effort): Oracle Cloud HCM ──
# Research found a real "CE" (Candidate Experience) REST namespace, and a
# recruitingCEJobRequisitionDetails resource that the public career site's
# own Angular/JET app calls client-side to render the requisition — but
# could not directly verify the exact JSON key for questionnaire data in
# this session (the career sites are JS-rendered, blocking static
# verification). We attempt the same authless CE endpoint pattern already
# used successfully for job listings (see scrape_oracle_cloud_hcm) with
# expand=all, then generically recurse the response for question-shaped
# data. Falls back to the generic DOM parser if that comes up empty.

_ORACLE_JOB_URL_RE = re.compile(r"^(https://[^/]+)/hcmUI/CandidateExperience/en/sites/([^/]+)/job/([^/?#]+)")


def _fetch_oracle_cloud_hcm_questions(job: dict) -> str:
    url = job.get("url", "")
    questions = []
    m = _ORACLE_JOB_URL_RE.match(url)
    if m:
        host, _site_number, job_id = m.groups()
        api_url = f"{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails/{job_id}"
        try:
            import uuid as _uuid
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json",
                "ora-irc-cx-userid": str(_uuid.uuid4()),
                "ora-irc-language": "en",
            }
            r = _get_requests_sync(api_url, params={"onlyData": "true", "expand": "all"}, headers=headers)
            if r:
                data = r.json()
                _walk_for_questions(data, questions)
        except Exception:
            pass

    if not questions:
        questions = _fetch_generic_form_questions_multi(url)
    return _format_auth_questions(questions)


# ── Not reliably obtainable: HRMDirect ──
# Verified live: listing pages (*.hrmdirect.com) are plain static HTML with
# no form on them at all — the real apply form lives on a SEPARATE
# subdomain (apply.hrmdirect.com, a ClearCompany/"ResumeDirect ApplyOnline"
# ASP.NET app), and that subdomain's robots.txt disallows crawling
# entirely ("Disallow: /"). We respect that rather than silently bypass
# it — this fetcher intentionally does not request that subdomain.

def _fetch_hrmdirect_questions(job: dict) -> str:
    log.debug("HRMDirect: apply.hrmdirect.com disallows crawling via robots.txt; skipping")
    return ""


# ── ADP Workforce Now (public JSON — no browser, no click) ──
# 2026-09: RESEARCHED LIVE. job["url"] (set by scrape_adp) is already the
# per-requisition DETAIL API endpoint (.../job-requisitions/{itemID}?cid=
# ...&ccId=...) that _fetch_adp_description also fetches — a public,
# unauthenticated JSON API. Confirmed live against a real ADP client's
# requisitions that the detail payload carries a top-level
# "screeningRequirements" array (empty on every requisition checked in
# this pass, since not every ADP client configures screening questions —
# same as Greenhouse boards without custom questions — but the field
# itself, and the API shape, are real and live-verified, not guessed).
# ADP's own key name "screeningRequirements" already matches
# _QUESTION_KEY_RE ("screening"), so the existing generic
# _walk_for_questions() (used for Oracle Cloud HCM below) finds it with no
# ADP-specific parsing needed — it recurses the whole payload for any
# question-shaped array under a matching key, so it's robust even if a
# populated requisition nests things slightly differently than the empty
# ones checked here.
def _fetch_adp_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    r = _get_requests_sync(url, headers={
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
    })
    if not r:
        return ""
    try:
        data = r.json()
    except Exception:
        return ""
    questions: list[dict] = []
    _walk_for_questions(data, questions)
    return _format_auth_questions(questions)


# ── Rippling (public Next.js data route — no browser needed) ──
# 2026-09: RESEARCHED LIVE before writing this (a real Skillable posting on
# Rippling, ats.rippling.com/skillable-careers/jobs/{jobId}) — the previous
# version of this function assumed Rippling's apply form needed a headless
# browser to render (same assumption still true for BambooHR/iCIMS/Workday/
# JOIN/Paylocity below). That assumption was WRONG for Rippling specifically:
# clicking "Apply" just navigates to a plain Next.js page
# (.../jobs/{jobId}/apply?jobBoardSlug={slug}&jobId={jobId}&step=application)
# whose content comes from Next.js's own server-rendered-data route:
#   https://ats.rippling.com/_next/data/{buildId}/en-US/{slug}/jobs/{jobId}/apply.json
#     ?jobBoardSlug={slug}&jobId={jobId}&step=application
# Confirmed live: a plain server-side GET of that URL (no JS execution, no
# session/cookies, no click) returns the FULL question set as structured
# JSON at pageProps.apiData.jobPost.activeJobApplication.additionalQuestions
# -> [...].form.questions -> [{title, questionType, isRequired, ...}], where
# questionType "KNOCKOUT" is Rippling's OWN flag for a disqualifying
# question — better signal than regex-matching the title text, since a
# real knockout question isn't always about sponsorship (the same live
# posting also had a KNOCKOUT residency-restriction question with no
# "sponsor"/"authorized" wording at all, which _WORK_AUTH_RE alone would
# have missed). buildId isn't guessable but doesn't need to be: it's
# embedded in the job posting page's own __NEXT_DATA__ script tag, which
# is a single extra plain-HTTP GET of a page this codebase already fetches
# the URL for (job["url"], Rippling's listing API's own posting link).
_RIPPLING_JOB_URL_RE = re.compile(r"ats\.rippling\.com/([^/]+)/jobs/([a-zA-Z0-9-]+)")
_NEXT_BUILD_ID_RE = re.compile(r'"buildId"\s*:\s*"([^"]+)"')


def _fetch_rippling_questions(job: dict) -> str:
    url = job.get("url", "")
    m = _RIPPLING_JOB_URL_RE.search(url)
    if not m:
        return _format_auth_questions(_fetch_generic_form_questions(url)) if url else ""
    slug, job_id = m.group(1), m.group(2)

    questions: list[dict] = []
    r = _get_requests_sync(url, headers={"User-Agent": random.choice(USER_AGENTS)})
    build_id_match = _NEXT_BUILD_ID_RE.search(r.text) if r else None
    if build_id_match:
        build_id = build_id_match.group(1)
        data_url = (
            f"https://ats.rippling.com/_next/data/{build_id}/en-US/{slug}/jobs/{job_id}/apply.json"
        )
        r2 = _get_requests_sync(
            data_url,
            params={"jobBoardSlug": slug, "jobId": job_id, "step": "application"},
            headers={"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"},
        )
        if r2:
            try:
                data = r2.json()
            except Exception:
                data = None
            if data:
                job_post = (
                    (((data.get("pageProps") or {}).get("apiData") or {}).get("jobPost")) or {}
                )
                additional = (job_post.get("activeJobApplication") or {}).get(
                    "additionalQuestions"
                ) or []
                for group in additional:
                    for q in (group.get("form") or {}).get("questions") or []:
                        title = _clean_label(str(q.get("title") or ""))
                        if not title:
                            continue
                        required = bool(q.get("isRequired"))
                        is_knockout = str(q.get("questionType") or "").upper() == "KNOCKOUT"
                        # Keep every knockout question (Rippling's own
                        # disqualifying-question flag, not just ones whose
                        # wording happens to match _WORK_AUTH_RE — see
                        # module note above for the real residency-question
                        # example this closes) PLUS anything else that
                        # matches the usual work-authorization wording.
                        if is_knockout or _WORK_AUTH_RE.search(title):
                            questions.append({"label": title, "required": required})

    if questions:
        # NOTE: deliberately NOT calling _format_auth_questions() here — it
        # re-filters by _WORK_AUTH_RE internally, which would silently drop
        # the non-worded KNOCKOUT questions (e.g. the residency-restriction
        # example above) that were already deliberately kept above. Format
        # directly instead so that filtering decision actually sticks.
        return "\n".join(f"Application Question: {q['label']}" for q in questions)

    # Fallback: the Next.js data-route trick can fail if Rippling changes
    # its build layout — fall back to the old best-effort DOM guesses
    # rather than returning nothing.
    apply_url = url.rstrip("/") + "/apply"
    found = _fetch_generic_form_questions(apply_url)
    if not found:
        found = _fetch_generic_form_questions(url.rstrip("/") + "/application")
    return _format_auth_questions(found)


# ── Best-effort DOM-only platforms (BambooHR / iCIMS / Workday / JOIN /
# Paylocity) ──
# 2026-09: RESEARCHED LIVE, not assumed. Workday (a real Lennar posting)
# and JOIN (a real ShippyPro posting) both put the actual questions behind
# an account-creation/sign-in wall BEFORE any question ever renders — this
# is a real authentication gate, not a JS-rendering problem, so a headless
# browser would hit the exact same wall a plain HTTP request does. Getting
# past it would mean creating and signing in with fake applicant accounts
# at scale, which is a different and far riskier undertaking than "add a
# browser step" and is treated as out of scope here. iCIMS additionally
# renders inside a cross-origin iframe. BambooHR and Paylocity weren't
# reachable with a real live example in this research pass; left in this
# bucket rather than guessed at. We still run the Level-3 DOM parser
# against the best-known URL in case a tenant happens to serve
# server-rendered fallback markup, but for most jobs on these platforms
# this will correctly return nothing — that's the platform's
# architecture, not a bug here.

def _fetch_bamboohr_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions_multi(job.get("url", "")))


def _fetch_icims_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions_multi(job.get("url", "")))


def _fetch_workday_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions_multi(job.get("url", "")))


def _fetch_personio_questions(job: dict) -> str:
    """2026-09: try the dedicated /apply URL first — confirmed live
    (index-soft.jobs.personio.com/job/{id}/apply, linked from the posting's
    own "Apply for this job" button) rather than only re-fetching the
    posting page. Still best-effort/Level-3 only: the real form itself is
    client-side-rendered, so this just gives the DOM parser a shot at a
    page more likely to carry the actual form markup than the listing
    page. Falls back to the plain posting URL if that fails."""
    url = job.get("url", "")
    if not url:
        return ""
    apply_url = url.rstrip("/") + "/apply"
    found = _fetch_generic_form_questions(apply_url)
    if found:
        return _format_auth_questions(found)
    return _format_auth_questions(_fetch_generic_form_questions(url))


def _fetch_joincom_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions_multi(job.get("url", "")))


def _fetch_taleo_questions(job: dict) -> str:
    """2026-09: Oracle's own Taleo career-section docs confirm the apply
    page is a DISTINCT .ftl file from the posting page — jobapply.ftl vs
    jobdetail.ftl, same job= query param — not a guess, this is Oracle's
    documented URL convention. Swap to it before running the DOM parser;
    falls back to the plain posting URL if the swap doesn't apply (no
    jobdetail.ftl in the URL) or the apply page yields nothing."""
    url = job.get("url", "")
    if not url:
        return ""
    if "jobdetail.ftl" in url:
        apply_url = url.replace("jobdetail.ftl", "jobapply.ftl")
        found = _fetch_generic_form_questions(apply_url)
        if found:
            return _format_auth_questions(found)
    return _format_auth_questions(_fetch_generic_form_questions(url))


def _fetch_paylocity_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions_multi(job.get("url", "")))


# ── SmartRecruiters ──
# Research found the documented screening-questions endpoint
# (GET /postings/{uuid}/configuration) requires an X-SmartToken auth
# header issued per-company — not usable for arbitrary postings. An
# unauthenticated "oneclick" widget config endpoint exists but could not
# be confirmed to expose custom questions (the one live posting tested had
# none configured). Best-effort DOM fallback only.

def _fetch_smartrecruiters_questions(job: dict) -> str:
    return _format_auth_questions(_fetch_generic_form_questions_multi(job.get("url", "")))


# ── Jobvite ──
# 2026-09: added — confirmed live (jobs.jobvite.com/{company}/job/{id} →
# "Apply" button href is the same URL + "/apply"). Best-effort/Level-3
# only: no public question-definition API found for Jobvite, so this
# relies on the generic DOM form parser same as the other Level-3
# platforms above.

def _fetch_jobvite_questions(job: dict) -> str:
    url = job.get("url", "")
    if not url:
        return ""
    apply_url = url.rstrip("/") + "/apply"
    found = _fetch_generic_form_questions(apply_url)
    if not found:
        found = _fetch_generic_form_questions(url.rstrip("/") + "/application")
    return _format_auth_questions(found)


# ── Dispatch table: source_ats (as stored on job dicts) → fetcher ──
QUESTION_FETCHERS = {
    "Greenhouse": _fetch_greenhouse_questions,
    "Ashby": _fetch_ashby_questions,
    "Lever": _fetch_lever_questions,
    "Workable": _fetch_workable_questions,
    "Recruitee": _fetch_recruitee_questions,
    "SmartRecruiters": _fetch_smartrecruiters_questions,
    "Teamtailor": _fetch_teamtailor_questions,
    "BreezyHR": _fetch_breezyhr_questions,
    "JazzHR": _fetch_jazzhr_questions,  # REVIVED 2026-09 — see module notes above the function
    "HRMDirect": _fetch_hrmdirect_questions,
    "ADP": _fetch_adp_questions,
    "Zoho": _fetch_zoho_questions,
    "Oracle Cloud HCM": _fetch_oracle_cloud_hcm_questions,
    "Rippling": _fetch_rippling_questions,
    "BambooHR": _fetch_bamboohr_questions,
    "iCIMS": _fetch_icims_questions,
    "Workday": _fetch_workday_questions,
    "Personio": _fetch_personio_questions,
    "JOIN": _fetch_joincom_questions,
    "Taleo": _fetch_taleo_questions,
    "Paylocity": _fetch_paylocity_questions,
    "Jobvite": _fetch_jobvite_questions,
}


def _fetch_wild_questions(job: dict) -> str:
    """2026-09: fallback for jobs on unsupported/"wild" ATS platforms
    (archive_ii's target — no entry in QUESTION_FETCHERS at all, so they
    previously got ZERO application-question enrichment no matter how
    strong a signal the real form had). Reuses the same universal
    Level-3 parser + /apply, /application URL-guessing already proven out
    for supported platforms (_fetch_generic_form_questions_multi) — it
    doesn't know this platform's API, but the plain-URL-guess convention
    it tries is platform-agnostic by design, so it's exactly as applicable
    to an unknown wild site as to a named ATS with no dedicated fetcher."""
    return _format_auth_questions(_fetch_generic_form_questions_multi(job.get("url", "")))


def enrich_application_questions(jobs: list[dict], max_workers: int = 150) -> list[dict]:
    """Fetch application questions for EVERY job that has a URL.

    2026-09 ROUND 2 (explicit user instruction: "Make sure that all jobs
    have their application questions fetched. All of them. ... everything
    under our control should have application questions in it.
    Everything!"): previously this only fetched for jobs where
    classifier.keyword_classify_location(job) == "unsure" — i.e. only the
    subset that would actually be sent to the AI location-classification
    step. That left the majority of jobs (anything already keyword-
    classified as a hard 'match' or 'no_match') with NO application-
    question enrichment at all, even though a job already keyword-matched
    as globally open could still carry a hard-restriction application
    question (a work-authorization/visa screening question) that the
    keyword classifier's own description-text checks never got a chance to
    see, because it lives in the ATS's separate screening-questions data,
    not the description body. Fetching for every job closes that gap;
    crawl_iii.py's scrapply.ai-sourced jobs are the one explicit carve-out
    the user named (no application-question fetcher for that source), and
    that carve-out already existed and is unaffected by this change since
    crawl_iii.py doesn't call this function at all.

    Jobs on one of the ATS platforms listed in QUESTION_FETCHERS above use
    that platform's dedicated fetcher. Everything else — including
    archive_ii's unsupported-ATS/"wild" company career sites, and any
    platform IN QUESTION_FETCHERS whose dedicated fetcher comes back
    empty — falls back to _fetch_wild_questions: the same universal
    HTML-form parser that now (2026-09) also looks for a REAL discovered
    "Apply" link on the page itself before ever falling back to blind
    "/apply","/application"-style URL guessing (see
    _fetch_generic_form_questions_multi/_discover_real_apply_link).

    Work authorization / visa sponsorship questions are strong signals that
    a job is NOT globally open, even when its location field just says
    "Remote". We extract those (and every other substantive screening
    question — see _format_screening_questions) and append them to
    description_snippet so both the keyword classifier's hard overrides
    and the AI location classifier can use them, while universal PII/EEO
    boilerplate (name, email, phone, age, sex/gender, race, veteran
    status, etc. — see _BOILERPLATE_QUESTION_RE) is never appended at all,
    since none of it carries any location-classification signal.

    Call this AFTER enrich_descriptions and BEFORE filter_locations.

    2026-09 ASYNC MIGRATION (same speed pass as enrich_descriptions):
    every QUESTION_FETCHERS entry (and _fetch_wild_questions) is still a
    plain sync function -- none of the 21 have been converted to httpx
    yet, so this doesn't get the "await the fetcher directly on the
    shared client" win enrich_descriptions' generic fetcher now does.
    What it DOES get: its own asyncio.run() (same one-loop pattern as
    scrape_all()/enrich_descriptions -- httpx.AsyncClient can't cross
    event loops, so creation/use/aclose_http_client() all stay inside
    this one call) dispatching every fetcher through asyncio.to_thread()
    instead of a flat ThreadPoolExecutor(max_workers=15), with
    max_workers raised 15 -> 150 to match. A real concurrency win even
    with zero fetcher-body changes: 10x the jobs can be mid-fetch at
    once, each still safely isolated to its own OS thread exactly like
    the old ThreadPoolExecutor model, just drawing from a bigger pool."""
    to_enrich = [j for j in jobs if j.get("url")]

    if not to_enrich:
        return jobs

    by_platform: dict[str, int] = {}
    wild_count = 0
    for j in to_enrich:
        ats = j.get("source_ats") or "unknown"
        if ats not in QUESTION_FETCHERS:
            wild_count += 1
        by_platform[ats] = by_platform.get(ats, 0) + 1
    platform_summary = ", ".join(f"{k}:{v}" for k, v in sorted(by_platform.items()))
    log.info(f"Fetching application questions for {len(to_enrich)} jobs "
             f"across {len(by_platform)} ATS platforms ({platform_summary})"
             + (f" — {wild_count} on unsupported/wild sites, generic fallback" if wild_count else "")
             + "...")

    async def _fetch_one(job, sem):
        ats = job.get("source_ats") or "unknown"
        fetcher = QUESTION_FETCHERS.get(ats, _fetch_wild_questions)
        questions = ""
        outcome = "none"
        async with sem:
            try:
                questions = await asyncio.to_thread(fetcher, job)
            except Exception as e:
                log.debug(f"Failed to fetch questions for {job.get('url', '')} via "
                          f"{ats or 'wild'} fetcher: {e}")
            if questions:
                outcome = "wild" if fetcher is _fetch_wild_questions else "dedicated"
            # 2026-09 (explicit user request: "use multiple methods and
            # fallbacks if you have to" so application questions don't
            # silently come back empty): a DEDICATED per-platform fetcher
            # returning nothing doesn't necessarily mean the posting has no
            # real screening form — it can just as easily mean this one
            # tenant customized their form, or the platform's API/HTML shape
            # drifted since that fetcher was written, which is a real,
            # confirmed failure mode elsewhere in this file (see e.g.
            # scrape_brassring's "missing session priming" history and the
            # JazzHR "REVIVED" note above). Rather than accept a silent
            # empty result from a single extraction method, give every job
            # that went through a DEDICATED fetcher (not already the wild
            # one) a second try via the universal multi-method fallback
            # (_fetch_wild_questions: real-apply-link discovery + embedded-
            # JSON parse + raw form-element parse, across the bare/apply/
            # application URL conventions) — cheap (one more request, same
            # politeness sleep already below), only runs when the first
            # method found nothing, and never replaces a real result the
            # dedicated fetcher DID find.
            if not questions and fetcher is not _fetch_wild_questions:
                try:
                    questions = await asyncio.to_thread(_fetch_wild_questions, job)
                except Exception as e:
                    log.debug(f"Generic fallback also failed for {job.get('url', '')}: {e}")
                if questions:
                    outcome = "wild_fallback"
        if questions:
            existing = job.get("description_snippet", "") or ""
            job["description_snippet"] = existing + "\n\n" + questions
        # 2026-09: see module-level comment above _pace_host — was an
        # unconditional politeness sleep on every job regardless of host
        # health; now only actually waits for a host that's shown a
        # reason to.
        if job.get("url"):
            await asyncio.to_thread(_pace_host, job["url"])
        return ats, outcome

    # 2026-09 (explicit user instruction: "logs should be clean AF and
    # numbers add up"): every submitted job lands in exactly ONE outcome
    # bucket below, and the buckets are checked to sum back to
    # len(to_enrich) rather than just trusted to — previously a thread
    # that raised was silently `except: pass`-ed, quietly shrinking the
    # total with no trace in the log at all. A per-platform hit
    # breakdown is logged too, so "did platform X actually return
    # anything" is answerable straight from the log instead of inferred
    # from one bare aggregate number.
    outcome_counts = {"dedicated": 0, "wild": 0, "wild_fallback": 0, "none": 0, "crashed": 0}
    per_platform_hits: dict[str, int] = {}
    crashed_platforms: dict[str, int] = {}

    async def _run():
        asyncio.get_running_loop().set_default_executor(
            ThreadPoolExecutor(max_workers=max_workers)
        )
        sem = asyncio.Semaphore(max_workers)
        try:
            return await asyncio.gather(
                *(_fetch_one(j, sem) for j in to_enrich), return_exceptions=True
            )
        finally:
            await aclose_http_client()

    results = asyncio.run(_run())
    for job, res in zip(to_enrich, results):
        if isinstance(res, Exception):
            # _fetch_one already catches every expected failure mode
            # (a fetcher raising, the wild-fallback raising) — landing
            # here means something outside those try/excepts broke,
            # a real bug rather than a normal per-job scrape failure.
            # Counted explicitly instead of silently swallowed.
            ats = job.get("source_ats") or "unknown"
            outcome = "crashed"
            crashed_platforms[ats] = crashed_platforms.get(ats, 0) + 1
            log.debug(f"enrich_application_questions: unexpected failure for "
                      f"{job.get('url', '')} ({ats}): {res}")
        else:
            ats, outcome = res
        outcome_counts[outcome] += 1
        if outcome in ("dedicated", "wild", "wild_fallback"):
            per_platform_hits[ats] = per_platform_hits.get(ats, 0) + 1

    total_hit = outcome_counts["dedicated"] + outcome_counts["wild"] + outcome_counts["wild_fallback"]
    accounted_for = sum(outcome_counts.values())
    hit_summary = ", ".join(f"{k}:{v}" for k, v in sorted(per_platform_hits.items())) or "none"
    log.info(
        "── Application question enrichment summary ──\n"
        f"  {len(to_enrich)} jobs submitted -> {accounted_for} accounted for "
        f"(dedicated fetcher: {outcome_counts['dedicated']}, wild/unsupported-platform "
        f"fallback: {outcome_counts['wild']}, dedicated-then-wild rescue: "
        f"{outcome_counts['wild_fallback']}, nothing found: {outcome_counts['none']}, "
        f"unexpected failure: {outcome_counts['crashed']})\n"
        f"  {total_hit}/{len(to_enrich)} jobs got at least one real Application Question "
        f"line appended, by platform: {hit_summary}")
    if accounted_for != len(to_enrich):
        log.warning(f"enrich_application_questions: {len(to_enrich)} jobs submitted but only "
                    f"{accounted_for} accounted for above — a job went missing inside the "
                    f"thread pool, not just failed to find questions. This should never happen.")
    if crashed_platforms:
        log.warning(f"enrich_application_questions: unexpected (non-fetch) failures on "
                    f"{outcome_counts['crashed']} job(s): "
                    f"{', '.join(f'{k}:{v}' for k, v in sorted(crashed_platforms.items()))}")

    return jobs
