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
(async fetch, robots.txt handling, ATS link scan, one-hop career-page
follow, checkpointing) are unchanged — what changed is only the source
of hosts, so the working code for actually crawling them didn't need to
be rewritten, just re-pointed. See tranco_seed.py's module docstring for
why Tranco replaced the Common Crawl Host Index as the seed source.

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
import datetime
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
        )


stats = Stats()

# robots.txt results cached per-domain for the lifetime of the process —
# at this scale, re-fetching robots.txt once per host on the SAME domain
# (rare, but happens with subdomains) would double request volume for no
# benefit. Bounded by BATCH_SIZE turnover; not persisted across runs
# since robots.txt can legitimately change.
_robots_cache: dict[str, bool] = {}


async def _robots_allows(session: aiohttp.ClientSession, base_url: str) -> tuple[bool, str]:
    """Live robots.txt check, cached per-domain within this process.
    Returns (allowed, reason) — reason is "ok", "unreachable" (DNS/timeout/
    connection/SSL failure — NOT a real robots.txt rule), or "disallowed"
    (an actual robots.txt rule blocked us). Only a real "disallowed" rule
    should ever be recorded as outcome='disallowed' in t_seeding
    — collapsing "the site was unreachable" into the same bucket as "the
    site explicitly disallows crawling" made the retired host-crawl
    pipeline's results table useless for telling those two very different
    situations apart (see host_crawl.py's original version of this
    docstring for the full history). Matches discovery.py's
    _robots_allows behavior: no robots.txt (4xx/5xx) means allowed, not
    disallowed; only an explicit Disallow rule blocks."""
    if base_url in _robots_cache:
        stats.robots_cache_hits += 1
        return _robots_cache[base_url]

    stats.robots_fetches += 1
    result = (False, "unreachable")
    try:
        async with session.get(f"{base_url}/robots.txt",
                                timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT)) as r:
            if r.status >= 400:
                # No robots.txt at all is conventionally "allow everything"
                # — matches discovery.py's _robots_allows, and the actual
                # robots.txt spec. This is NOT a failure of any kind.
                result = (True, "ok")
            else:
                text = await r.text(errors="replace")
                import urllib.robotparser
                rp = urllib.robotparser.RobotFileParser()
                rp.parse(text.splitlines())
                if rp.can_fetch(USER_AGENT, "/"):
                    result = (True, "ok")
                else:
                    result = (False, "disallowed")
    except Exception:
        # DNS failure, timeout, connection reset, SSL error, etc. — the
        # site itself is unreachable, this has nothing to do with
        # robots.txt rules and must not be recorded as "disallowed".
        result = (False, "unreachable")

    _robots_cache[base_url] = result
    return result


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


async def _crawl_one(session: aiohttp.ClientSession, host: str) -> dict:
    """Visit one host, return a result dict to be written back onto
    t_seeding (+ t_slugs if a slug was found). Never raises — every
    failure mode is captured as an outcome, not an exception."""
    base = f"https://{host}"

    allowed, reason = await _robots_allows(session, base)
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

    # One hop to a careers page, same as discovery.py's YC resolver.
    career_url = _find_career_page_link(html, base)
    if career_url:
        career_host = urlparse(career_url).hostname or ""
        career_base = f"https://{career_host}"
        if career_base != base:
            career_allowed, _ = await _robots_allows(session, career_base)
            if not career_allowed:
                return {"host": host, "outcome": "no_match"}
        try:
            async with session.get(
                career_url, timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
                headers={"User-Agent": USER_AGENT},
            ) as r2:
                if r2.status < 400:
                    html2 = await r2.text(errors="replace")
                    hit2 = _scan_html_for_ats_slug(html2, career_url)
                    if hit2:
                        ats, slug = hit2
                        return {"host": host, "outcome": "found", "ats": ats, "slug": slug}
        except Exception:
            pass

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


def _today_str() -> str:
    # ISO date string for t_seeding.checked_at (a `date` column) — set the
    # moment a host's crawl outcome is recorded, distinct from added_at
    # (set when the host was first seeded by tranco_seed.py).
    return datetime.date.today().isoformat()


def _claim_batch(shard: int, total_shards: int, batch_size: int) -> list[dict]:
    """Pull the next batch of not-yet-visited hosts (outcome IS NULL)
    assigned to this shard, from t_seeding — the single queue+outcome
    table (same merged-table design as the retired host-crawl pipeline's
    h_seeding). Sharding is done in SQL by hashing the host, so this
    needs no coordination between concurrently-running shards — each
    shard only ever sees its own slice, so two shards claiming batches at
    the same moment can't double-claim the same host."""
    # Using a Postgres function (tranco_claim_batch) keeps the "next
    # unvisited hosts for this shard" logic atomic and server-side rather
    # than racy client-side pagination — two shards claiming batches at
    # the same instant can't double-claim the same host, since the
    # function's own UPDATE...RETURNING marks rows claimed in the same
    # statement that selects them. Unlike the retired pipeline's
    # host_crawl_claim_batch, this orders by rank ASC (Tranco rank 1 =
    # most popular) instead of a Common-Crawl-derived score DESC.
    import requests
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
        log.error(f"Failed to claim batch: {e}")
        return []


def _flush_results(visited_rows: list[dict], found_rows: list[dict]):
    """Write this batch's outcomes. visited_rows are UPSERTED into
    t_seeding — this fills in outcome/ats/slug/checked_at on the SAME
    row that was already there from seeding, rather than inserting into
    a separate "visited" table (matches the retired host-crawl pipeline's
    merged-table design). found_rows go to t_slugs (only actual ATS
    matches, a genuinely different, permanent dataset). on_conflict must
    be passed as a query param alongside the merge-duplicates Prefer
    header — PostgREST silently no-ops the upsert semantics without it
    (see discovery.py's upsert_to_supabase for the same pattern)."""
    import requests
    if visited_rows:
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/t_seeding",
                headers=_sb_headers("resolution=merge-duplicates"),
                json=visited_rows, timeout=60,
                params={"on_conflict": "host"},
            )
            r.raise_for_status()
        except Exception as e:
            log.error(f"Failed to flush {len(visited_rows)} visited rows: {e}")

    if found_rows:
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/t_slugs",
                headers=_sb_headers("resolution=merge-duplicates"),
                json=found_rows, timeout=60,
                params={"on_conflict": "ats,slug"},
            )
            r.raise_for_status()
        except Exception as e:
            log.error(f"Failed to flush {len(found_rows)} found rows: {e}")


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
    while time.monotonic() < deadline:
        hosts = _claim_batch(shard, total_shards, BATCH_SIZE)
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
                                  "ats": res.get("ats"), "slug": res.get("slug"),
                                  "checked_at": _today_str()})

        if not dry_run:
            _flush_results(visited_rows, found_rows)

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
