"""
WORKABLE SEED — pages jobs.workable.com's public, unauthenticated jobs API
and writes out each posting's already-embedded company website. Same
two-stage shape as people_data_labs_seed.py/opendata_seed.py/
bigpicture_seed.py: this script only downloads + filters, it does NOT crawl
(see node.py / workable_probe.py for that). workable.yml's `seed` stage
runs this (sharded — see below) and merges every shard's output into one
Release asset that the `crawl` stage downloads.

WHY THE UNDERLYING API IS THE RIGHT SOURCE (2026-09, real research, not
guessed — see this session's own live Chrome network-capture/fetch
verification):

jobs.workable.com's visible search page never paginates for a plain fetch
(page=1/page=2 returned byte-identical company sets, live-verified) and
every link on it is an opaque company ID, not a scrapable Workable slug.
But a real, undocumented, public JSON API sits behind it, found by
watching genuine network traffic during a REAL (non-scripted) scroll
gesture in a live browser:

    GET https://jobs.workable.com/api/v1/jobs?pageToken=...

  - No query/location param = the WHOLE board (170,032 jobs at
    verification time). `nextPageToken` is real cursor pagination
    (verified: page 2 returns a genuinely different, non-overlapping job
    set, not a repeat).
  - Each job object already embeds `company.website` — the real external
    site — directly in the response. No per-company page visit, no
    JSON-LD scraping needed.
  - Genuinely public: fetched successfully with zero cookies sent
    (`credentials: 'omit'`), and a real logged-out browser visit to the
    human-facing search page hard-redirects to a sign-in wall while this
    JSON API does not — ruling out an "authenticated session" false
    positive (the mistake made once already on my.greenhouse.io).

2026-09 SHARDING (this rewrite): a real live run hit a 429 after ~400
pages TWICE, at very different paging speeds (73s and 293s to get there)
— strong evidence this is a per-runner page/quota wall, not a burst-rate
limit, and NOT something automatic retry alone fixes (a live 429 also
carried `Retry-After: 86400` — 24 HOURS — so waiting it out isn't
practical either). The fix: spread the work across multiple GitHub Actions
runners (each gets its own IP) instead of one long sequential job.

The page cursor ITSELF can't be sharded — decoding a real token shows it's
an Elasticsearch-style "search-after" cursor (the previous page's exact
score/timestamp/ID), which by design can't jump to an arbitrary page
without having walked every page before it. So sharding here means
splitting the QUERY SPACE instead of the page range. Verified live: the
API's `location` param is a REAL, honored filter (`location=Germany`
returns 3,809 vs 169,919 unfiltered — a genuinely different, non-ignored
number, unlike `country` or `page` which are silently no-ops). Workable's
own sitemap.xml lists 122 real countries it segments jobs by (see
LOCATIONS below, live-extracted from https://jobs.workable.com/sitemap.xml
— not guessed), plus "Remote" as its own real, working value.

Country sizes are wildly uneven — a live sample found the United States
alone at 69,816 of ~170k total (~41%), vs Canada at 7,025 and Remote at
512 — so United States gets its OWN dedicated shard (shard 0) rather than
swamping whatever shard it landed in; every other shard round-robins the
remaining 121 countries + Remote. See bucket_locations().

HONEST COVERAGE CAVEAT (sharded mode only): the plain no-filter mode
(shard_count <= 1) is the only one confirmed to see literally the whole
board — every job, regardless of whether it has a recognizable single-
country location tag. Location-bucketed sharding (shard_count > 1) is a
disclosed tradeoff: a job posting with no clear single-country tag (fully
remote-and-unspecified, multi-country, etc.) might not surface under ANY
location bucket. This hasn't been exhaustively measured (that would need
summing every one of the 123 buckets' totals against the unfiltered total
— expensive to verify live without risking another rate-limit wall) — real
parallelism to dodge a confirmed 24-hour throttle is traded for a small,
unquantified completeness gap. Not every one of the 122 country names
below has been individually round-tripped through the live API either
(that alone would be ~122 extra requests); a shard whose assigned location
returns a suspicious totalSize of 0 logs a warning rather than silently
treating "0" as "this country really has no postings," since a spelling
mismatch would look identical.

RESUME MODEL (2026-09, changed from a manual Restart Token to real
Supabase checkpointing): each shard is its own stable (source="workable_seed",
shard_index, shard_count) identity — the SAME checkpoint mechanism/table
every other probe/crawl-stage shard in this project already uses (see
node.py's save_crawl_checkpoint/load_crawl_checkpoint_with_partition/
clear_crawl_checkpoint). This file doesn't import node.py itself (it's a
synchronous script, not async, and doesn't need node.py's crawling
machinery) — it talks to the same `crawl_checkpoints` Supabase table
directly via a few small sync helpers below. Checkpoint state is
{"loc_index": int, "token": str|None} — which of this shard's assigned
locations is in progress, and where in that location's own page cursor —
stored as JSON text in the table's `partition` column (a generic nullable
text column, not literally Common-Crawl-specific despite the name).
resume_offset holds a running "companies written so far in this shard's
current pass" count, informational only. Auto-resumes by default; --reset
clears the checkpoint and starts this shard's bucket over from the top.

Usage:
    python workable_seed.py --output workable_companies.csv
    python workable_seed.py --shard-index 1 --shard-count 12 --output shard1.csv
"""
import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
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

# 2026-09: a real run hit a 429 (Too Many Requests) after 400 pages, twice,
# at very different paging speeds — see module docstring. Retries with
# backoff (honoring a Retry-After header if the API sends one) instead of
# treating a single 429 as a hard stop. Only a 429 retries; every other
# failure (network error, other HTTP status, bad JSON) still fails the page
# immediately, since those aren't a "slow down" signal.
_MAX_429_RETRIES = 6
_BASE_BACKOFF_SECONDS = 5     # backs off 5s, 10s, 15s, 20s, 25s, 30s absent a Retry-After header
# 2026-09: a real run's Retry-After header came back as 86400 (24 HOURS) —
# confirmed live, not guessed. A GitHub Actions job gets killed at
# 350-360min regardless, so obeying that literally just wastes the whole
# run doing nothing. Anything bigger than this cap gives up this page (and
# location) now instead of actually sleeping that long — the checkpoint
# lets a LATER run pick back up.
_MAX_BACKOFF_SECONDS = 60

# ── SOURCE_LABEL / Supabase checkpoint plumbing (sync, no node.py import —
# see module docstring's RESUME MODEL section) ──
SOURCE_LABEL = "workable_seed"
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
CHECKPOINT_TABLE = "crawl_checkpoints"

# United States gets its own dedicated shard — by far the single biggest
# location (see module docstring). Every other shard round-robins the rest.
_US_LOCATION = "United States"

# Real, live-extracted (2026-09) from https://jobs.workable.com/sitemap.xml's
# /search/{country-slug}/... URLs — 122 countries. Display names are the
# plain English form the API's `location` filter expects (spot-verified for
# several, e.g. Germany/United States/Canada/India/United Kingdom — see
# module docstring's coverage caveat for the ones not individually
# round-tripped).
LOCATIONS = [
    "Afghanistan", "Albania", "Algeria", "Argentina", "Armenia", "Australia",
    "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Belarus",
    "Belgium", "Bolivia", "Bosnia and Herzegovina", "Brazil", "Bulgaria",
    "Burkina Faso", "Cameroon", "Canada", "Chile", "Colombia", "Costa Rica",
    "Ivory Coast", "Croatia", "Cyprus", "Czech Republic", "DR Congo",
    "Denmark", "Dominican Republic", "Ecuador", "Egypt", "El Salvador",
    "Estonia", "Fiji", "Finland", "France", "Gabon", "Georgia", "Germany",
    "Ghana", "Greece", "Hong Kong", "Hungary", "India", "Indonesia", "Iraq",
    "Ireland", "Iran", "Israel", "Italy", "Jamaica", "Japan", "Jordan",
    "Kazakhstan", "Kenya", "Kuwait", "Latvia", "Lebanon", "Lithuania",
    "Luxembourg", "Madagascar", "Malaysia", "Maldives", "Malta", "Mauritius",
    "Mexico", "Moldova", "Morocco", "Mozambique", "Namibia", "Nepal",
    "Netherlands", "New Zealand", "Nicaragua", "Nigeria", "Norway", "Oman",
    "Pakistan", "Panama", "Papua New Guinea", "China", "Peru", "Philippines",
    "Poland", "Portugal", "Puerto Rico", "Qatar", "Congo", "Romania",
    "Russia", "Rwanda", "Saudi Arabia", "Senegal", "Serbia", "Singapore",
    "Slovakia", "Slovenia", "Somalia", "South Africa", "South Korea",
    "Spain", "Sri Lanka", "Sweden", "Switzerland", "Taiwan", "Thailand",
    "North Macedonia", "Trinidad and Tobago", "Tunisia", "Turkey", "Uganda",
    "Ukraine", "United Arab Emirates", "United Kingdom", "Tanzania",
    _US_LOCATION, "Uruguay", "Uzbekistan", "Venezuela", "Vietnam", "Zambia",
]


def bucket_locations(shard_index: int, shard_count: int) -> list[str | None]:
    """This shard's slice of the query space to page through, each entry
    independently — see module docstring for the full reasoning.

    shard_count <= 1: [None] — the original, fully-verified-complete mode:
    one sequential pass with NO location filter at all (the whole board).

    shard_count > 1: shard 0 = ["United States"] alone. Every other shard
    round-robins the remaining 121 countries + "Remote" across
    (shard_count - 1) buckets."""
    if shard_count <= 1:
        return [None]
    if shard_index == 0:
        return [_US_LOCATION]
    others = [loc for loc in LOCATIONS if loc != _US_LOCATION] + ["Remote"]
    bucket_count = shard_count - 1
    bucket_i = shard_index - 1
    return [loc for i, loc in enumerate(others) if i % bucket_count == bucket_i]


def _err(e: Exception) -> str:
    """Same fix as opendata_seed.py's/bigpicture_seed.py's/
    people_data_labs_seed.py's _err() — str(e) can be empty for some
    exceptions, and an exception object is always truthy, so a naive
    `e or repr(e)` never actually falls through."""
    return str(e) or repr(e) or type(e).__name__


def _load_seed_checkpoint(shard_index: int, shard_count: int) -> dict | None:
    """{"loc_index": int, "token": str|None}, or None if never checkpointed
    (start this shard's bucket from location 0, top of feed)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    params = {"source": f"eq.{SOURCE_LABEL}", "shard_index": f"eq.{shard_index}",
              "shard_count": f"eq.{shard_count}", "select": "partition"}
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{CHECKPOINT_TABLE}", headers=headers,
                          params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data or not data[0].get("partition"):
            return None
        return json.loads(data[0]["partition"])
    except Exception as e:
        log.warning(f"  couldn't load seed checkpoint (starting this shard's bucket from the top): {_err(e)}")
        return None


def _save_seed_checkpoint(shard_index: int, shard_count: int, state: dict, resume_offset: int) -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
               "Prefer": "resolution=merge-duplicates"}
    row = {"source": SOURCE_LABEL, "shard_index": shard_index, "shard_count": shard_count,
           "resume_offset": resume_offset, "partition": json.dumps(state),
           "updated_at": datetime.now(timezone.utc).isoformat()}
    try:
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{CHECKPOINT_TABLE}", headers=headers,
                           params={"on_conflict": "source,shard_index,shard_count"},
                           json=[row], timeout=30)
        r.raise_for_status()
    except Exception as e:
        log.warning(f"  couldn't save seed checkpoint (non-fatal — a future resume may redo a bit): {_err(e)}")


def _clear_seed_checkpoint(shard_index: int, shard_count: int) -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    params = {"source": f"eq.{SOURCE_LABEL}", "shard_index": f"eq.{shard_index}", "shard_count": f"eq.{shard_count}"}
    try:
        r = requests.delete(f"{SUPABASE_URL}/rest/v1/{CHECKPOINT_TABLE}", headers=headers,
                             params=params, timeout=30)
        r.raise_for_status()
    except Exception as e:
        log.warning(f"  couldn't clear seed checkpoint (non-fatal): {_err(e)}")


def _fetch_jobs_page(page_token: str | None, location: str | None = None) -> dict | None:
    """One page (20 jobs) of the public jobs feed, optionally filtered to
    one location= value. Retries a 429 with backoff (see module docstring)
    up to _MAX_429_RETRIES times, capped at _MAX_BACKOFF_SECONDS. Returns
    None on any failure that isn't a retryable-and-recoverable 429, so the
    caller can move on (stop this location, or this run) cleanly."""
    token_note = '<start>' if not page_token else page_token[:12] + '...'
    loc_note = location or '<no filter>'
    params = {}
    if location:
        params["location"] = location
    if page_token:
        params["pageToken"] = page_token
    for attempt in range(_MAX_429_RETRIES + 1):
        try:
            r = requests.get(_JOBS_API, params=params, headers=_HEADERS, timeout=_HTTP_TIMEOUT)
            if r.status_code == 429:
                if attempt >= _MAX_429_RETRIES:
                    log.warning(f"  still rate-limited (429) after {_MAX_429_RETRIES} retries "
                                f"(location={loc_note}, token={token_note}) — giving up on this page")
                    return None
                retry_after = r.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after else _BASE_BACKOFF_SECONDS * (attempt + 1)
                except ValueError:
                    wait = _BASE_BACKOFF_SECONDS * (attempt + 1)
                if wait > _MAX_BACKOFF_SECONDS:
                    log.warning(f"  rate-limited (429, location={loc_note}, token={token_note}) — server "
                                f"asked for a {wait:.0f}s wait, longer than this script will ever sleep "
                                f"for ({_MAX_BACKOFF_SECONDS}s cap) — giving up on this page now instead.")
                    return None
                log.warning(f"  rate-limited (429, location={loc_note}, token={token_note}) — waiting "
                            f"{wait:.0f}s before retry {attempt + 1}/{_MAX_429_RETRIES}")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"  page fetch failed (location={loc_note}, token={token_note}): {_err(e)}")
            return None
    return None


def page_one_location(writer: "csv._writer", seen_domains: set[str], location: str | None,
                       start_token: str | None, deadline: float | None,
                       request_delay_seconds: float) -> tuple[int, str | None, bool]:
    """Pages ONE location bucket from start_token (None = top of that
    bucket's own feed) until it's exhausted, the deadline (a
    time.monotonic() value, or None) passes, or a page fetch fails.
    Returns (kept, next_token, location_done): next_token is None either
    because the location finished (location_done=True) or nothing was
    paged yet; a shard moves on to its next location once location_done."""
    kept = 0
    pages = 0
    token = start_token
    location_done = False
    total_for_location = None
    while True:
        if deadline and time.monotonic() >= deadline:
            break
        data = _fetch_jobs_page(token, location)
        if data is None:
            break
        pages += 1
        if total_for_location is None:
            total_for_location = data.get("totalSize")
            if location and total_for_location == 0:
                log.warning(f"  location={location!r} returned totalSize=0 — this might be a spelling "
                            f"mismatch against Workable's own naming rather than a real empty country; "
                            f"see module docstring's coverage caveat.")
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
            log.info(f"  ...[{location or 'no filter'}] {pages:,} pages, {kept:,} new companies so far")

        if request_delay_seconds:
            time.sleep(request_delay_seconds)

        token = data.get("nextPageToken")
        if not token:
            location_done = True
            break
    return kept, (None if location_done else token), location_done


def run_seed_shard(output_path: str, shard_index: int, shard_count: int,
                    time_budget_minutes: int = 0, request_delay_seconds: float = 0.0,
                    reset: bool = False) -> tuple[int, bool]:
    """Works through this shard's assigned locations (bucket_locations),
    resuming from this shard's own Supabase checkpoint unless reset=True.
    Returns (kept, stopped_early). stopped_early=False means every location
    in this shard's bucket was fully paged this run (the checkpoint is
    cleared); True means the time budget or a page failure stopped it
    mid-bucket (the checkpoint is left in place for the next run)."""
    bucket = bucket_locations(shard_index, shard_count)
    label = f"[shard {shard_index}/{shard_count}]" if shard_count > 1 else ""
    log.info(f"── Workable seed {label} — {len(bucket)} location(s) this bucket ──")

    if reset:
        log.info("  --reset — forcing a full restart of this shard's bucket, ignoring any checkpoint")
        _clear_seed_checkpoint(shard_index, shard_count)
        state = None
    else:
        state = _load_seed_checkpoint(shard_index, shard_count)

    loc_index = state["loc_index"] if state else 0
    start_token = state.get("token") if state else None
    resuming = state is not None
    if resuming:
        log.info(f"  resuming: location {loc_index + 1}/{len(bucket)} ({bucket[loc_index]!r}), "
                 f"token={'<start>' if not start_token else start_token[:12] + '...'}")

    file_mode = "a" if (resuming and os.path.exists(output_path)) else "w"
    seen_domains: set[str] = set()
    if file_mode == "a":
        with open(output_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) > 1 and row[1]:
                    seen_domains.add(row[1])
        log.info(f"  {len(seen_domains):,} companies already in {output_path} from this bucket's pass so far")

    time_budget_seconds = time_budget_minutes * 60 if time_budget_minutes else None
    deadline = (time.monotonic() + time_budget_seconds) if time_budget_seconds else None
    total_kept = 0
    stopped_early = False

    with open(output_path, file_mode, newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        if file_mode == "w":
            writer.writerow(["name", "domain"])

        i = loc_index
        tok = start_token
        while i < len(bucket):
            if deadline and time.monotonic() >= deadline:
                stopped_early = True
                break
            location = bucket[i]
            kept, next_token, location_done = page_one_location(
                writer, seen_domains, location, tok, deadline, request_delay_seconds)
            total_kept += kept
            if location_done:
                log.info(f"  [{location or 'no filter'}] done — {kept:,} new companies this pass")
                i += 1
                tok = None
                _save_seed_checkpoint(shard_index, shard_count, {"loc_index": i, "token": None}, total_kept)
            else:
                stopped_early = True
                log.warning(f"  [{location or 'no filter'}] stopped mid-lap after {kept:,} new companies "
                            f"this run — resuming here next time.")
                _save_seed_checkpoint(shard_index, shard_count, {"loc_index": i, "token": next_token}, total_kept)
                break

    if not stopped_early:
        log.info(f"── shard {label} bucket fully done — {total_kept:,} new companies this run, "
                 f"{len(seen_domains):,} total in {output_path} ──")
        _clear_seed_checkpoint(shard_index, shard_count)
    else:
        log.warning(f"── shard {label} stopped early — {total_kept:,} new companies this run, "
                    f"checkpoint saved for next run ──")
    return total_kept, stopped_early


def main():
    parser = argparse.ArgumentParser(
        description="Workable jobs API — page through the public jobs feed to a company seed CSV")
    parser.add_argument("--output", default="workable_companies.csv")
    parser.add_argument("--shard-index", type=int, default=0,
                         help="Which shard this run is (0-based). Default 0.")
    parser.add_argument("--shard-count", type=int, default=1,
                         help="Total shards. 1 (default) = no location sharding — one sequential pass "
                              "over the whole board, the fully-verified-complete mode. >1 splits by "
                              "location (see module docstring's coverage caveat).")
    parser.add_argument("--time-budget-minutes", type=int, default=0,
                         help="Self-stop gracefully after this many minutes. 0 = no internal budget "
                              "(run until this shard's whole bucket is scanned once).")
    parser.add_argument("--request-delay-seconds", type=float, default=0.0,
                         help="Fixed pause after each page, on top of the automatic 429 backoff/retry — "
                              "a proactive throttle to make hitting the rate limit less likely in the "
                              "first place. 0 (default) = no extra pause.")
    parser.add_argument("--reset", action="store_true",
                         help="Force a full restart of this shard's bucket, ignoring any checkpoint.")
    args = parser.parse_args()

    kept, stopped_early = run_seed_shard(args.output, args.shard_index, args.shard_count,
                                          args.time_budget_minutes, args.request_delay_seconds, args.reset)

    if kept == 0 and not stopped_early:
        log.error("No rows written — aborting with a non-zero exit so the CI job shows red "
                  "instead of silently uploading an empty/missing Release asset.")
        sys.exit(1)


if __name__ == "__main__":
    main()
