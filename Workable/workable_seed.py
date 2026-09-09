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

  - No query/location param = the WHOLE board (~170k jobs at verification
    time). `nextPageToken` is real cursor pagination (verified: page 2
    returns a genuinely different, non-overlapping job set, not a repeat).
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
splitting the QUERY SPACE instead of the page range.

SCOPE NARROWED TO THE 18 PDL-SUPPORTED COUNTRIES (2026-09, second
rewrite): the first sharded version bucketed all 122 real countries from
Workable's sitemap.xml. That's more than this project actually needs —
People Data Labs/people_data_labs_probe.py's DEFAULT_COUNTRIES already
defines the 18 countries this project actually targets (hand-picked,
English-language-friendly markets with a real base of companies). Reusing
that same 18-country scope here means Workable's seed only ever pages
countries this project will actually crawl companies in.

TWO REAL, LIVE-VERIFIED FILTERS (both confirmed additive, not fuzzy —
see below for one that ISN'T safe):
  - `location=<Country Name>` — confirmed safe for the 18 target
    countries: fetched each of their real totalSize values live
    (2026-09) and their sum (105,454) is comfortably less than the
    unfiltered board total (~169,955), consistent with 18 of 122
    countries and no overlap between them.
  - `workplace=remote|hybrid|on_site` — confirmed genuinely exhaustive
    and non-overlapping for United States specifically: live totals were
    remote=13,546, hybrid=10,567, on_site=45,732, and those three sum to
    EXACTLY 69,845 — the plain `location=United States` total at the same
    moment. This is a real second axis, used ONLY to split United States
    (by far the largest of the 18 — 66% of their combined total) into 3
    independently-pageable sub-buckets, each with its own cursor.

ONE THING TESTED AND FOUND *NOT* SAFE, WORTH RECORDING SO IT ISN'T
RE-TRIED: US STATE NAMES (e.g. `location=California`) are NOT a safe
splitting axis, despite returning distinct-looking non-zero numbers.
Live-verified two red flags: (1) a completely made-up location string
(`location=NotARealState12345`) returned ~169,948 — almost the ENTIRE
unfiltered board, not 0 — meaning an unrecognized `location` value
silently falls back to "no filter" rather than "no match", so a wrong
state name would silently double-count instead of failing loudly; (2)
summing just 10 major US states' totals came to ~78,470, MORE than the
entire United States total (~69,845) at the same moment — real states
overlap/fuzzy-match rather than partition cleanly (Workable's own
sitemap.xml has no state-level search URLs either — only country-level
and a handful of SEO category pages — confirming states aren't a real
taxonomy dimension on this site, just a text field that happens to
substring-match). `workplace` was checked the same way and passed both
tests (unknown values return 0 exactly, not a fallback; the 3 real values
sum to exactly the baseline) — the difference is why one is used here and
the other isn't.

Country sizes among the 18 are wildly uneven (United States alone is
69,845 of their 105,454 combined total — 66%), so instead of naive
round-robin (which ignores size and would badly overload whatever shard
drew the US), shards are built with a real weighted greedy bin-pack
(largest-first, each unit assigned to the currently-lightest shard) using
these live-sampled totals — see WORK_UNITS and _bin_pack_units(). United
States is represented as 3 separate weighted units (remote/hybrid/
on_site, see above) rather than 1, so the bin-packer can actually spread
its load across multiple shards instead of being forced to dedicate one
whole shard to it regardless of shard_count.

HONEST CAVEATS:
  - The weights baked into WORK_UNITS are a real live sample taken during
    this rewrite (2026-09), not a live lookup on every run — Workable's
    board changes day to day, so the actual split will drift slightly
    from these weights over time. This only affects how evenly shards
    are BALANCED (a stale weight might load one shard a bit more than
    another) — it does NOT affect correctness/completeness, since every
    unit is still paged to full exhaustion regardless of its assumed
    weight.
  - This intentionally covers ONLY the 18 PDL-matched countries, not the
    whole ~170k-job board. That's a deliberate scope match to this
    project's existing target-country list, not an accidental gap — see
    the module's SCOPE NARROWED section above. Pass --shard-count 1 (or
    leave it at the default) for the old, still-available whole-board
    mode with no location/workplace filtering at all.
  - A shard whose assigned bucket's totalSize comes back 0 logs a
    warning rather than silently treating that as "really empty", since
    (per the state-name lesson above) a wrong/renamed value could look
    identical to a real zero.

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
units is in progress, and where in that unit's own page cursor — stored
as JSON text in the table's `partition` column (a generic nullable text
column, not literally Common-Crawl-specific despite the name).
resume_offset holds a running "companies written so far in this shard's
current pass" count, informational only. Auto-resumes by default; --reset
clears the checkpoint and starts this shard's bucket over from the top.

Usage:
    python workable_seed.py --output workable_companies.csv
    python workable_seed.py --shard-index 1 --shard-count 8 --output shard1.csv
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
# unit) now instead of actually sleeping that long — the checkpoint lets a
# LATER run pick back up.
_MAX_BACKOFF_SECONDS = 60

# ── SOURCE_LABEL / Supabase checkpoint plumbing (sync, no node.py import —
# see module docstring's RESUME MODEL section) ──
SOURCE_LABEL = "workable_seed"
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
CHECKPOINT_TABLE = "crawl_checkpoints"

_US_LOCATION = "United States"

# Real, live-sampled (2026-09) totalSize per unit, from location=<country>
# and location=United States&workplace=<value> — see module docstring for
# how each of these was verified additive/non-overlapping. Used ONLY to
# balance the bin-pack (_bin_pack_units) — never to decide whether to page
# a unit, only how shards are grouped. A unit is (location, workplace);
# workplace is None for a whole-country unit.
#
# Country list matches People Data Labs/people_data_labs_probe.py's
# DEFAULT_COUNTRIES exactly (this project's existing 18-country target
# scope), display-named the way Workable's own `location` filter expects.
WORK_UNITS: list[tuple[tuple[str, str | None], int]] = [
    ((_US_LOCATION, "on_site"), 45732),
    ((_US_LOCATION, "remote"), 13546),
    ((_US_LOCATION, "hybrid"), 10567),
    (("United Kingdom", None), 10151),
    (("Canada", None), 7025),
    (("Germany", None), 3809),
    (("Australia", None), 3287),
    (("Singapore", None), 2859),
    (("France", None), 1637),
    (("Netherlands", None), 1451),
    (("Denmark", None), 1023),
    (("Ireland", None), 975),
    (("Belgium", None), 895),
    (("Sweden", None), 597),
    (("Norway", None), 492),
    (("New Zealand", None), 420),
    (("Austria", None), 420),
    (("Finland", None), 357),
    (("Luxembourg", None), 167),
    (("Iceland", None), 48),
]


def _bin_pack_units(units: list[tuple[tuple[str, str | None], int]],
                     shard_count: int) -> list[list[tuple[str, str | None]]]:
    """Greedy largest-first bin-pack: sort units by weight descending, each
    goes to whichever shard currently has the smallest running total. With
    real weights this balances actual paging work per shard far better than
    round-robin-by-count, without needing every shard to get an equal
    NUMBER of units. Deterministic — every shard computes the same full
    partition independently (no coordination needed) and just reads out
    its own index. shard_count > len(units) is handled by simply leaving
    the extra shards with an empty bucket (nothing to do, not an error)."""
    buckets: list[list[tuple[str, str | None]]] = [[] for _ in range(shard_count)]
    loads = [0] * shard_count
    for unit, weight in sorted(units, key=lambda u: -u[1]):
        i = loads.index(min(loads))
        buckets[i].append(unit)
        loads[i] += weight
    return buckets


def bucket_locations(shard_index: int, shard_count: int) -> list[tuple[str | None, str | None]]:
    """This shard's slice of the query space to page through, each entry
    independently — a list of (location, workplace) tuples; either half
    can be None. shard_count <= 1 returns [(None, None)] — the original,
    fully-verified-complete mode: one sequential pass with no filters at
    all (the whole board, not scoped to the 18 target countries). For
    shard_count > 1, see _bin_pack_units()/WORK_UNITS above."""
    if shard_count <= 1:
        return [(None, None)]
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError(f"shard_index {shard_index} out of range for shard_count {shard_count}")
    return _bin_pack_units(WORK_UNITS, shard_count)[shard_index]


def _err(e: Exception) -> str:
    """Same fix as opendata_seed.py's/bigpicture_seed.py's/
    people_data_labs_seed.py's _err() — str(e) can be empty for some
    exceptions, and an exception object is always truthy, so a naive
    `e or repr(e)` never actually falls through."""
    return str(e) or repr(e) or type(e).__name__


def _load_seed_checkpoint(shard_index: int, shard_count: int) -> dict | None:
    """{"loc_index": int, "token": str|None}, or None if never checkpointed
    (start this shard's bucket from unit 0, top of feed)."""
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


def _unit_note(location: str | None, workplace: str | None) -> str:
    if not location:
        return "<no filter>"
    return f"{location} [{workplace}]" if workplace else location


def _fetch_jobs_page(page_token: str | None, location: str | None = None,
                      workplace: str | None = None) -> dict | None:
    """One page (20 jobs) of the public jobs feed, optionally filtered to
    one location= and/or workplace= value (see module docstring for why
    each is safe). Retries a 429 with backoff up to _MAX_429_RETRIES times,
    capped at _MAX_BACKOFF_SECONDS. Returns None on any failure that isn't
    a retryable-and-recoverable 429, so the caller can move on (stop this
    unit, or this run) cleanly."""
    token_note = '<start>' if not page_token else page_token[:12] + '...'
    unit_note = _unit_note(location, workplace)
    params = {}
    if location:
        params["location"] = location
    if workplace:
        params["workplace"] = workplace
    if page_token:
        params["pageToken"] = page_token
    for attempt in range(_MAX_429_RETRIES + 1):
        try:
            r = requests.get(_JOBS_API, params=params, headers=_HEADERS, timeout=_HTTP_TIMEOUT)
            if r.status_code == 429:
                if attempt >= _MAX_429_RETRIES:
                    log.warning(f"  still rate-limited (429) after {_MAX_429_RETRIES} retries "
                                f"(unit={unit_note}, token={token_note}) — giving up on this page")
                    return None
                retry_after = r.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after else _BASE_BACKOFF_SECONDS * (attempt + 1)
                except ValueError:
                    wait = _BASE_BACKOFF_SECONDS * (attempt + 1)
                if wait > _MAX_BACKOFF_SECONDS:
                    log.warning(f"  rate-limited (429, unit={unit_note}, token={token_note}) — server "
                                f"asked for a {wait:.0f}s wait, longer than this script will ever sleep "
                                f"for ({_MAX_BACKOFF_SECONDS}s cap) — giving up on this page now instead.")
                    return None
                log.warning(f"  rate-limited (429, unit={unit_note}, token={token_note}) — waiting "
                            f"{wait:.0f}s before retry {attempt + 1}/{_MAX_429_RETRIES}")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"  page fetch failed (unit={unit_note}, token={token_note}): {_err(e)}")
            return None
    return None


def page_one_location(writer: "csv._writer", seen_domains: set[str],
                       location: str | None, workplace: str | None,
                       start_token: str | None, deadline: float | None,
                       request_delay_seconds: float) -> tuple[int, str | None, bool]:
    """Pages ONE (location, workplace) unit from start_token (None = top of
    that unit's own feed) until it's exhausted, the deadline (a
    time.monotonic() value, or None) passes, or a page fetch fails.
    Returns (kept, next_token, unit_done): next_token is None either
    because the unit finished (unit_done=True) or nothing was paged yet; a
    shard moves on to its next unit once unit_done."""
    kept = 0
    pages = 0
    token = start_token
    unit_done = False
    total_for_unit = None
    unit_note = _unit_note(location, workplace)
    while True:
        if deadline and time.monotonic() >= deadline:
            break
        data = _fetch_jobs_page(token, location, workplace)
        if data is None:
            break
        pages += 1
        if total_for_unit is None:
            total_for_unit = data.get("totalSize")
            if (location or workplace) and total_for_unit == 0:
                log.warning(f"  unit={unit_note} returned totalSize=0 — this might be a naming "
                            f"mismatch against Workable's own filter values rather than a real empty "
                            f"unit; see module docstring's caveats.")
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
            log.info(f"  ...[{unit_note}] {pages:,} pages, {kept:,} new companies so far")

        if request_delay_seconds:
            time.sleep(request_delay_seconds)

        token = data.get("nextPageToken")
        if not token:
            unit_done = True
            break
    return kept, (None if unit_done else token), unit_done


def run_seed_shard(output_path: str, shard_index: int, shard_count: int,
                    time_budget_minutes: int = 0, request_delay_seconds: float = 0.0,
                    reset: bool = False) -> tuple[int, bool]:
    """Works through this shard's assigned (location, workplace) units
    (bucket_locations), resuming from this shard's own Supabase checkpoint
    unless reset=True. Returns (kept, stopped_early). stopped_early=False
    means every unit in this shard's bucket was fully paged this run (the
    checkpoint is cleared) OR this shard's bucket was empty to begin with
    (shard_count exceeded the number of real work units — nothing to do,
    not an error); True means the time budget or a page failure stopped it
    mid-bucket (the checkpoint is left in place for the next run)."""
    bucket = bucket_locations(shard_index, shard_count)
    label = f"[shard {shard_index}/{shard_count}]" if shard_count > 1 else ""

    if not bucket:
        log.info(f"── Workable seed {label} — no units assigned (shard_count exceeds the real "
                 f"work-unit count) — nothing to do this shard ──")
        return 0, False

    log.info(f"── Workable seed {label} — {len(bucket)} unit(s) this bucket ──")

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
        resume_loc, resume_wp = bucket[loc_index]
        log.info(f"  resuming: unit {loc_index + 1}/{len(bucket)} ({_unit_note(resume_loc, resume_wp)}), "
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
            location, workplace = bucket[i]
            kept, next_token, unit_done = page_one_location(
                writer, seen_domains, location, workplace, tok, deadline, request_delay_seconds)
            total_kept += kept
            if unit_done:
                log.info(f"  [{_unit_note(location, workplace)}] done — {kept:,} new companies this pass")
                i += 1
                tok = None
                _save_seed_checkpoint(shard_index, shard_count, {"loc_index": i, "token": None}, total_kept)
            else:
                stopped_early = True
                log.warning(f"  [{_unit_note(location, workplace)}] stopped mid-lap after {kept:,} new "
                            f"companies this run — resuming here next time.")
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
                         help="Total shards. 1 (default) = no filtering — one sequential pass over the "
                              "WHOLE global board, the original fully-verified-complete mode. >1 splits "
                              "by (location, workplace) unit across the 18 PDL-target countries, weighted "
                              "by real sampled size (see WORK_UNITS/_bin_pack_units in the module docstring).")
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

    bucket_was_empty = len(bucket_locations(args.shard_index, args.shard_count)) == 0
    if kept == 0 and not stopped_early and not bucket_was_empty:
        log.error("No rows written — aborting with a non-zero exit so the CI job shows red "
                  "instead of silently uploading an empty/missing Release asset.")
        sys.exit(1)


if __name__ == "__main__":
    main()
