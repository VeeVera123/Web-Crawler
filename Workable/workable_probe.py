"""
WORKABLE PROBE — a thin, disposable probe source on top of node.py (the
permanent engine), same role common_crawl_probe.py/opendata_probe.py/
people_data_labs_probe.py/bigpicture_probe.py play for their own seed
sources. All fetch/parse/detect/write logic lives in node.py — this file's
only job is producing a list of CANDIDATE COMPANY WEBSITES for node.py to
crawl, the same way every other probe does.

WHY THIS EXISTS, AND WHY IT'S SHAPED THE WAY IT IS (2026-09, real research,
not guessed — see this session's own live Chrome network-capture/fetch
verification):

jobs.workable.com/search (170k+ live postings, confirmed) initially looked
unusable as a slug source: its visible search page never paginates for a
plain fetch (page=1/page=2 returned byte-identical company sets, live-
verified), and every link on it is an OPAQUE company ID
(jobs.workable.com/company/{opaqueId}/jobs-at-{name}), not the readable
{slug}.workable.com / apply.workable.com/{slug} form ats_scrapers.py's
scrape_workable already knows how to scrape. An earlier version of this
file worked around that with a keyword-rotation hack (~120 generic job-
title keywords, each surfacing a small first-page slice of companies).

That hack is GONE. A genuine, undocumented, public JSON API sits behind
the visible search page, found by watching real network traffic during a
real (non-scripted) scroll gesture in a live browser — NOT the `page=N`
param, which is a red herring:

    GET https://jobs.workable.com/api/v1/jobs?query=...&pageToken=...

Verified live, repeatedly, this session:
  - No query param at all returns the WHOLE board: totalSize 170,032 —
    matching the "170k+ jobs" figure this project already knew about.
  - Real cursor pagination: the response's `nextPageToken` (an opaque,
    server-issued string) is passed back as `pageToken` to get the next
    20 jobs — confirmed to return a genuinely different, non-overlapping
    set of jobs/companies, not a repeat of page 1.
  - Each job object already embeds `company.website` — the company's real
    external site — directly in this one response, with NO per-company
    page fetch and NO JSON-LD scraping needed at all (a big simplification
    over the abandoned keyword-hack version, which had to visit every
    company's jobs.workable.com page separately to extract this).
  - Genuinely public: fetched successfully with `credentials: 'omit'`
    (zero cookies sent) and a plain non-browser User-Agent, and separately
    confirmed this browser session had NO valid Workable session at the
    time (a sibling `/api/v2/user` call returned 401) — ruling out the
    same "riding an authenticated session" mistake made earlier this
    project with my.greenhouse.io. This is a real anonymous public API,
    not a lucky authenticated fetch.

So: page straight through this API in cursor order (no query — the full,
unfiltered board), dedupe companies as they're seen, pull the domain
straight out of `company.website`, and hand those domains to node.py the
same way every other probe does. Checkpointed by PAGE TOKEN (an opaque
string) rather than a numeric offset — see run_probe's use of node.py's
`partition` checkpoint column (generic nullable text, not literally
restricted to Common Crawl's use of it) to hold it, since
`crawl_checkpoints.resume_offset` is a plain SQL integer and can't hold an
opaque token. `resume_offset` itself is repurposed here as a running
"jobs paged through so far" counter — informational only, not used to
resume (the page token is what actually resumes).

At 20 jobs/page and 170k+ total jobs, one full pass over the whole feed is
thousands of pages — expected to take multiple runs, same "partial
progress every run, more coverage over time" shape as every other
checkpointed probe here. When the feed is exhausted (`nextPageToken`
absent), the checkpoint clears and the next run starts over from the top
of the feed — a reasonable behavior given Workable's own feed keeps
changing (new postings appear, old ones close) between full passes anyway.

Usage:
    pip install aiohttp aiodns selectolax requests python-dotenv
    python workable_probe.py
    python workable_probe.py --pages-per-run 300
"""
import argparse
import asyncio
import logging
import os
import sys
import time
from collections import Counter
from urllib.parse import urlparse

import aiohttp
import requests
from dotenv import load_dotenv

load_dotenv()
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)                       # for node.py
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # for discovery.py
from discovery import SKIP_SLUGS  # noqa: E402
import node  # noqa: E402

log = logging.getLogger("workable_probe")

SOURCE_LABEL = "workable_probe"
_JOBS_API = "https://jobs.workable.com/api/v1/jobs"
_HTTP_TIMEOUT = 20
_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0 (compatible; job-scanner-probe/1.0)"}


def _fetch_jobs_page(page_token: str | None) -> dict | None:
    """One page (20 jobs) of the public, unauthenticated Workable jobs
    feed — no query param, so this is the WHOLE board in cursor order, not
    a keyword-filtered slice. None on any failure (network error, bad
    JSON, non-200) so the caller can just stop this run's paging cleanly."""
    params = {"pageToken": page_token} if page_token else {}
    try:
        r = requests.get(_JOBS_API, params=params, headers=_HEADERS, timeout=_HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"  jobs API page fetch failed (token={'<start>' if not page_token else page_token[:12] + '...'}): {e}")
        return None


def seed_workable_domains(pages_per_run: int, start_page_token: str | None,
                           seed_time_budget_seconds: float | None
                           ) -> tuple[list[dict], str | None, int, bool]:
    """Pages through the public jobs feed starting at start_page_token
    (None = the very top of the feed), collecting unique companies as
    {name, domain} dicts — the same shape every other probe hands to
    node.py. Returns (domains, next_page_token, jobs_seen, reached_end):
    next_page_token is None when the feed ran out (caller should treat
    that as "start over from the top next run"), otherwise the token to
    resume from."""
    seed_start = time.monotonic()
    seen_company_ids: set[str] = set()
    domains: list[dict] = []
    token = start_page_token
    jobs_seen = 0
    reached_end = False

    for page_i in range(pages_per_run):
        if seed_time_budget_seconds and (time.monotonic() - seed_start) >= seed_time_budget_seconds:
            log.info(f"  seed time budget reached after {page_i} page(s) — stopping, keeping "
                     f"{len(domains)} companies resolved so far this run.")
            break

        data = _fetch_jobs_page(token)
        if data is None:
            break  # a page fetch failure just ends this run's paging early — next run resumes from `token`

        jobs = data.get("jobs") or []
        jobs_seen += len(jobs)
        for job in jobs:
            company = job.get("company") or {}
            cid = company.get("id")
            website = company.get("website")
            if not cid or cid in seen_company_ids or not website:
                continue
            seen_company_ids.add(cid)
            host = (urlparse(website).hostname or "").lower()
            if host and host not in SKIP_SLUGS and "." in host:
                domains.append({"name": company.get("title") or "", "domain": host})

        token = data.get("nextPageToken")
        if not token:
            log.info(f"  reached the end of the jobs feed after {page_i + 1} page(s) this run — "
                     f"next run will start over from the top.")
            reached_end = True
            break

        if (page_i + 1) % 50 == 0:
            log.info(f"  ...{page_i + 1}/{pages_per_run} pages, {jobs_seen} jobs, "
                     f"{len(domains)} unique companies resolved so far")

    log.info(f"  {jobs_seen} jobs scanned across this run -> {len(domains)} unique, usable companies")
    return domains, (None if reached_end else token), jobs_seen, reached_end


async def run_probe(pages_per_run: int, concurrency: int, time_budget_minutes: int,
                     seed_time_budget_minutes: int, reset: bool = False) -> None:
    log.info("── Workable probe ──")
    log.info(f"  concurrency={concurrency}  crawl_time_budget={time_budget_minutes}min  "
             f"seed_time_budget={seed_time_budget_minutes}min  source={SOURCE_LABEL}")

    connector = node.new_connector()
    async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
        # Checkpointed by PAGE TOKEN (an opaque string), held in the
        # generic nullable `partition` column since `resume_offset` is a
        # plain SQL integer — see module docstring. `resume_offset` itself
        # holds a running jobs-scanned count here: informational only, not
        # used to resume (the token is what actually resumes).
        start_token: str | None = None
        if reset:
            log.info("  --reset — forcing a full restart from the top of the feed, ignoring any checkpoint")
            await node.clear_crawl_checkpoint(session, SOURCE_LABEL, 0, 1)
        else:
            start_token, _prior_jobs_seen = await node.load_crawl_checkpoint_with_partition(
                session, SOURCE_LABEL, 0, 1)
            if start_token:
                log.info(f"  resuming from checkpoint (page token {start_token[:12]}...)")
            else:
                log.info("  no checkpoint found — starting from the top of the feed")

        companies, next_token, jobs_seen, reached_end = seed_workable_domains(
            pages_per_run, start_token, seed_time_budget_minutes * 60 if seed_time_budget_minutes else None)

        if reached_end:
            await node.clear_crawl_checkpoint(session, SOURCE_LABEL, 0, 1)
        else:
            await node.save_crawl_checkpoint(session, SOURCE_LABEL, 0, 1, jobs_seen, partition=next_token)

        if not companies:
            log.warning("  No companies resolved this run — nothing to crawl.")
            return

        domains = [c["domain"] for c in companies]
        sem = asyncio.Semaphore(concurrency)
        stats = Counter()
        found_rows: list[dict] = []
        parse_pool = node.new_parse_pool()
        crawl_start = time.monotonic()
        try:
            # capture_inhouse=True — same reasoning as every other probe
            # (OpenData/PDL/BigPicture/Common Crawl): no size signal of
            # its own, so archive_ii eligibility goes through node.py's
            # Quality Index gate instead of an employee-count floor.
            _, elapsed, rate, time_budget_hit = await node.crawl_batch(
                domains, session, sem, stats, parse_pool, node.ACCEPT_ANY_COUNTRY,
                SOURCE_LABEL, found_rows, crawl_start, time_budget_minutes * 60,
                time_budget_minutes, batch_size=1000, unit_label="companies",
                capture_inhouse=True)
        finally:
            parse_pool.shutdown(wait=True)

    hit_n = stats["hits_from_homepage"] + stats["hits_from_career_path"] + stats["hits_from_sitemap"]
    companies_n = max(stats["companies_attempted"], 1)
    status = "STOPPED EARLY (time budget)" if time_budget_hit else "complete"
    log.info(f"── Workable probe {status}: {stats['companies_attempted']}/{len(domains)} companies, "
             f"{elapsed:.0f}s, {rate:.1f}/sec, hit={hit_n / companies_n * 100:.1f}% ({hit_n}) ──")
    ats_breakdown = Counter(r["ats"] for r in found_rows)
    if ats_breakdown:
        log.info(f"  by platform: {dict(ats_breakdown.most_common())}")
    node.log_quality_index_summary(stats)


def main():
    parser = argparse.ArgumentParser(
        description="Workable probe — pages through jobs.workable.com's public jobs API "
                    "(no query = the whole board) and hands each job's already-embedded "
                    "company website to node.py to find their real ATS link.")
    parser.add_argument("--pages-per-run", type=int, default=200,
                         help="How many pages (20 jobs each) to scan this run, resuming from the "
                              "checkpointed page token (default: 200 = ~4,000 jobs).")
    parser.add_argument("--concurrency", type=int, default=node.CRAWL_CONCURRENCY)
    parser.add_argument("--time-budget-minutes", type=int, default=node.TIME_BUDGET_MINUTES,
                         help="Time budget for the node.py crawl phase (resolving each company's "
                              "own homepage to a real ATS link).")
    parser.add_argument("--seed-time-budget-minutes", type=int, default=20,
                         help="Separate time budget for the jobs-API paging phase, so a slow run "
                              "of that phase can't eat into the crawl phase's budget.")
    parser.add_argument("--reset", action="store_true",
                         help="Force a full restart from the top of the feed, ignoring any checkpoint.")
    args = parser.parse_args()
    asyncio.run(run_probe(args.pages_per_run, args.concurrency, args.time_budget_minutes,
                           args.seed_time_budget_minutes, args.reset))


if __name__ == "__main__":
    main()
