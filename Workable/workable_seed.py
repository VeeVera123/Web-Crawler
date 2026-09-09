"""
WORKABLE SEED — one-time (or periodically re-run) pass over
jobs.workable.com's public, unauthenticated jobs API, paging through the
WHOLE board (no query = every posting — confirmed totalSize 170,032+ at
verification time) and writing out each posting's already-embedded
company website. Same two-stage shape as people_data_labs_seed.py/
opendata_seed.py/bigpicture_seed.py: this script only downloads + filters,
it does NOT crawl (see node.py / workable_probe.py for that) and it does
not touch GitHub Releases itself — workable.yml's `seed` job uploads this
script's one output CSV as a Release asset (small enough here to need no
chunking/splitting the way PDL's 22M-row file does).

WHY THIS EXISTS, AND WHY IT'S SHAPED THE WAY IT IS (2026-09, real
research, not guessed — see this session's own live Chrome network-
capture/fetch verification):

jobs.workable.com's visible search page never paginates for a plain fetch
(page=1/page=2 returned byte-identical company sets, live-verified) and
every link on it is an opaque company ID, not a scrapable Workable slug.
But a real, undocumented, public JSON API sits behind it, found by
watching genuine network traffic during a REAL (non-scripted) scroll
gesture in a live browser:

    GET https://jobs.workable.com/api/v1/jobs?pageToken=...

  - No query param = the WHOLE board. `nextPageToken` in the response is
    real cursor pagination (verified: page 2 returns a genuinely different,
    non-overlapping job set, not a repeat).
  - Each job object already embeds `company.website` — the real external
    site — directly in the response. No per-company page visit, no
    JSON-LD scraping needed (an earlier, abandoned version of this project
    had to do exactly that; this API makes it unnecessary).
  - Genuinely public: fetched successfully with zero cookies sent
    (`credentials: 'omit'`) and confirmed the browser session used to find
    it had NO valid Workable session at the time (a sibling `/api/v2/user`
    call returned 401) and, separately, a real logged-out browser visit to
    the human-facing search page hard-redirects to a sign-in wall while
    this JSON API does not — ruling out the same "riding an authenticated
    session" mistake made once already on my.greenhouse.io.

2026-09 SPLIT: an earlier single-file version of this probe combined
paging and crawling in one unsharded job, which meant the (inherently
sequential, un-shardable — each page needs the previous page's cursor
token) paging step ate into the same time budget as the (easily sharded)
company-crawl step, and the crawl step could never be sharded across
multiple parallel jobs the way every other probe here is. Split, PDL-
style: this file just pages + writes a CSV; workable_probe.py downloads
it and shards the CRAWL across N jobs like people_data_labs_probe.py
already does.

RESUME MODEL (mirrors people_data_labs_seed.py's Restart ID, just with an
opaque page-token string instead of a row number, since this source has
no fixed row order to count against): a normal run starts a FRESH lap
from the top of the feed (overwrites the output file). If the time budget
runs out before finishing that lap, this script logs the exact page token
to resume from — pass it back in via --start-token on the next run to
CONTINUE that same lap (appends to the existing output rather than
overwriting). Once a lap actually finishes (the feed runs out of
`nextPageToken`), that's a complete pass — the next normal run (no
--start-token) starts an entirely fresh lap from the top, picking up
newly-posted jobs the way people_data_labs_seed.py's plain re-run refreshes
its own dataset from scratch.

Usage:
    python workable_seed.py --output workable_companies.csv
    python workable_seed.py --start-token <token> --time-budget-minutes 330
"""
import argparse
import csv
import logging
import os
import sys
import time
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

load_dotenv()
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)                       # discovery.py lives at repo root on GitHub
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # fallback, for a local Main/ layout
from discovery import SKIP_SLUGS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("workable_seed")

_JOBS_API = os.environ.get("WORKABLE_JOBS_API", "https://jobs.workable.com/api/v1/jobs")
_HTTP_TIMEOUT = 20
_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0 (compatible; job-scanner-probe/1.0)"}
PROGRESS_EVERY_PAGES = 250  # pages between progress log lines


def _err(e: Exception) -> str:
    """Same fix as opendata_seed.py's/bigpicture_seed.py's/
    people_data_labs_seed.py's _err() — str(e) can be empty for some
    exceptions, and an exception object is always truthy, so a naive
    `e or repr(e)` never actually falls through."""
    return str(e) or repr(e) or type(e).__name__


def _fetch_jobs_page(page_token: str | None) -> dict | None:
    """One page (20 jobs) of the public jobs feed. None on any failure
    (network error, bad JSON, non-200) so the caller can stop this run's
    paging cleanly and log a resume token."""
    params = {"pageToken": page_token} if page_token else {}
    try:
        r = requests.get(_JOBS_API, params=params, headers=_HEADERS, timeout=_HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"  page fetch failed (token={'<start>' if not page_token else page_token[:12] + '...'}): {_err(e)}")
        return None


def page_and_filter(output_path: str, start_token: str | None = None,
                     time_budget_minutes: int = 0) -> tuple[int, bool, str | None]:
    """Pages the public jobs feed from start_token (None = top of feed),
    writing unique {name, domain} rows to output_path. Returns (kept,
    stopped_early, resume_token): resume_token is what to pass back in as
    --start-token to continue this SAME lap (None means either the lap
    finished, or nothing was paged yet — both cases correctly resume from
    the top on the next run)."""
    resuming = bool(start_token) and os.path.exists(output_path)
    file_mode = "a" if resuming else "w"
    log.info(f"Paging jobs.workable.com's public jobs API — "
             f"{'resuming an in-progress lap' if resuming else 'starting a fresh lap from the top of the feed'}")

    seen_domains: set[str] = set()
    if resuming:
        with open(output_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # header
            for row in reader:
                if len(row) > 1 and row[1]:
                    seen_domains.add(row[1])
        log.info(f"  {len(seen_domains):,} companies already in {output_path} from this lap so far")

    kept = 0
    pages = 0
    token = start_token
    stopped_early = False
    time_budget_seconds = time_budget_minutes * 60 if time_budget_minutes else None
    start = time.monotonic()

    try:
        with open(output_path, file_mode, newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            if not resuming:
                writer.writerow(["name", "domain"])

            while True:
                if time_budget_seconds and (time.monotonic() - start) >= time_budget_seconds:
                    stopped_early = True
                    log.warning(f"Time budget ({time_budget_minutes}min) reached after {pages:,} page(s) "
                                f"this run — stopping here. Restart Token for next run: "
                                f"{token or '(blank — start of feed)'}")
                    break

                data = _fetch_jobs_page(token)
                if data is None:
                    stopped_early = True
                    log.error(f"Page fetch failed after {pages:,} page(s) this run — stopping here. "
                              f"Restart Token for next run: {token or '(blank — start of feed)'}")
                    break

                pages += 1
                for job in data.get("jobs") or []:
                    company = job.get("company") or {}
                    website = company.get("website")
                    if not website:
                        continue
                    host = (urlparse(website).hostname or "").lower()
                    if not host or host in SKIP_SLUGS or "." not in host or host in seen_domains:
                        continue
                    seen_domains.add(host)
                    writer.writerow([company.get("title") or "", host])
                    kept += 1

                if pages % PROGRESS_EVERY_PAGES == 0:
                    elapsed = time.monotonic() - start
                    log.info(f"  ...{pages:,} pages ({pages / max(elapsed, 0.001):.1f}/sec), "
                             f"{kept:,} new companies written so far")

                token = data.get("nextPageToken")
                if not token:
                    log.info(f"Reached the end of the jobs feed after {pages:,} page(s) this run — "
                             f"full lap complete. Leave Restart Token blank on the next seed run to "
                             f"start a fresh lap (picks up newly-posted jobs).")
                    break
    except Exception as e:
        log.error(f"Failed mid-scan after {pages:,} page(s), {kept:,} written this run: {_err(e)}. "
                  f"Restart Token for next run: {token or '(blank — start of feed)'}")
        return kept, True, token

    elapsed = time.monotonic() - start
    log.info(f"Done: {pages:,} page(s) scanned this run, {kept:,} new companies written to {output_path}, "
             f"{elapsed:.0f}s ({pages / max(elapsed, 0.001):.1f} pages/sec).")
    return kept, stopped_early, (token if stopped_early else None)


def main():
    parser = argparse.ArgumentParser(
        description="Workable jobs API — page through the whole public jobs feed to a company seed CSV")
    parser.add_argument("--output", default="workable_companies.csv")
    parser.add_argument("--start-token", default=None,
                         help="Restart Token — resume an in-progress lap from here. Blank (default) "
                              "starts a fresh lap from the top of the feed.")
    parser.add_argument("--time-budget-minutes", type=int, default=0,
                         help="Self-stop gracefully after this many minutes and log a Restart Token. "
                              "0 = no internal budget (run until the whole feed is scanned once).")
    args = parser.parse_args()

    kept, stopped_early, resume_token = page_and_filter(args.output, args.start_token, args.time_budget_minutes)

    if kept == 0 and not stopped_early:
        log.error("No rows written — aborting with a non-zero exit so the CI job shows red "
                  "instead of silently uploading an empty/missing Release asset.")
        sys.exit(1)
    if stopped_early:
        log.warning(f"Run stopped before finishing this lap — Restart Token for next run: "
                    f"{resume_token or '(blank — start of feed)'}")


if __name__ == "__main__":
    main()
