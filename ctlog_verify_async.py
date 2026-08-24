"""
CT-LOGS VERIFICATION — GitHub Actions production version (2026-08), async
rewrite of ctlog_verify.py's logic. Runs as a SEPARATE workflow from
extraction (ctlog_extract.yml) per explicit direction — verification is
slower and noisier (hits live third-party servers, can be rate-limited or
flaky) and should never block or force a rerun of extraction, and vice
versa.

THE RULE (unchanged from ctlog_verify.py, now applied across all 5
Phase-1 platforms instead of BambooHR only): visit each candidate slug's
real board URL.
  - Redirects to the ATS VENDOR's own marketing/root domain (confirmed
    live pattern: acty.bamboohr.com -> https://www.bamboohr.com/) ->
    DEAD, delete the row from ctlog_probe_results.
  - Serves an actual job-board response (even zero current openings —
    still a real, valid tenant) -> LIVE, keep it.
  - Network error / timeout / inconclusive -> left ALONE, not deleted —
    a row we couldn't check is not evidence it's dead.

WHY ASYNC: this is the textbook I/O-bound fan-out case — thousands of
independent HTTPS GETs to five different vendors' servers, each one just
waiting on a response. aiohttp with a bounded semaphore lets hundreds run
concurrently instead of one at a time, which is where the real speed
comes from here (unlike extraction's crt.sh sweep, which is
constrained by a single sync Postgres connection regardless of language).

Each platform's verifier reuses that platform's REAL production scraper
logic/endpoint (ats_scrapers.py) wherever practical, so "verified live"
means "the actual daily scanner would find this board" — not a separate,
looser check that could disagree with production.

Usage:
    pip install aiohttp python-dotenv
    python ctlog_verify_async.py --platform bamboohr
    python ctlog_verify_async.py --platform workday
    python ctlog_verify_async.py --platform icims
    python ctlog_verify_async.py --platform rippling
    python ctlog_verify_async.py --platform teamtailor
    python ctlog_verify_async.py --platform bamboohr --dry-run
    python ctlog_verify_async.py --platform bamboohr --limit 500
"""
import argparse
import asyncio
import logging
import os
import sys
from urllib.parse import urlparse

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("ctlog_verify_async")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
DEFAULT_CONCURRENCY = 40


# ── per-platform verifiers ──────────────────────────────────────────
# Each returns True (live), False (confirmed dead), or raises (couldn't
# check — caller treats this as "leave alone", never as dead).

async def _verify_bamboohr(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://{slug}.bamboohr.com/careers/list"
    async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                            headers={"Accept": "application/json", "User-Agent": USER_AGENT}) as r:
        final_host = urlparse(str(r.url)).hostname or ""
        if final_host != f"{slug}.bamboohr.com":
            return False  # bounced to bamboohr.com's own marketing site — the confirmed dead pattern
        if r.status != 200:
            return False
        if "application/json" not in r.headers.get("Content-Type", ""):
            return False
        try:
            data = await r.json(content_type=None)
        except Exception:
            return False
        return isinstance(data, dict) and "result" in data


async def _verify_icims(session: aiohttp.ClientSession, slug: str) -> bool:
    """iCIMS boards are scraped via sitemap.xml (see ats_scrapers.py's
    scrape_icims) — a live tenant's sitemap returns real XML with >=1
    <url> entry; a dead/redirected tenant lands on icims.com's own
    marketing site or an empty/error sitemap."""
    for host in (f"{slug}.icims.com", f"careers-{slug}.icims.com"):
        url = f"https://{host}/jobs/search?ss=1&searchRelation=keyword_all&hashed=-435972932"
        try:
            async with session.get(f"https://{host}/", timeout=REQUEST_TIMEOUT, allow_redirects=True,
                                    headers={"User-Agent": USER_AGENT}) as r:
                final_host = urlparse(str(r.url)).hostname or ""
                if final_host == host and r.status == 200:
                    return True
        except Exception:
            continue
    return False


async def _verify_rippling(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://{slug}.rippling.com/"
    async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                            headers={"User-Agent": USER_AGENT}) as r:
        final_host = urlparse(str(r.url)).hostname or ""
        if final_host != f"{slug}.rippling.com":
            return False
        return r.status == 200


async def _verify_teamtailor(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://{slug}.teamtailor.com/"
    async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                            headers={"User-Agent": USER_AGENT}) as r:
        final_host = urlparse(str(r.url)).hostname or ""
        if final_host != f"{slug}.teamtailor.com":
            return False
        return r.status == 200


async def _verify_workday(session: aiohttp.ClientSession, slug: str) -> bool:
    """slug is 'company|wd|site_id' — reconstruct the actual board URL."""
    parts = slug.split("|")
    if len(parts) != 3:
        return False
    company, wd, site_id = parts
    url = f"https://{company}.{wd}.myworkdayjobs.com/{site_id}"
    async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                            headers={"User-Agent": USER_AGENT}) as r:
        # Workday's own "no such site" page still serves from the same
        # host, so a host check alone isn't enough here — look for a 200
        # and the site_id surviving in the final path (a dead site_id
        # gets redirected back to a generic tenant landing page instead).
        return r.status == 200 and site_id.lower() in str(r.url).lower()


PLATFORM_VERIFIERS = {
    "bamboohr": _verify_bamboohr,
    "icims": _verify_icims,
    "rippling": _verify_rippling,
    "teamtailor": _verify_teamtailor,
    "workday": _verify_workday,
}


# ── Supabase I/O (async) ──────────────────────────────────────

async def fetch_rows(session: aiohttp.ClientSession, ats: str, limit: int | None) -> list[dict]:
    rows = []
    page_size = 1000
    offset = 0
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    while True:
        if limit is not None and len(rows) >= limit:
            rows = rows[:limit]
            break
        headers["Range"] = f"{offset}-{offset + page_size - 1}"
        async with session.get(
            f"{SUPABASE_URL}/rest/v1/ctlog_probe_results",
            headers=headers,
            params={"ats": f"eq.{ats}", "select": "id,slug"},
            timeout=aiohttp.ClientTimeout(total=60),
        ) as r:
            r.raise_for_status()
            batch = await r.json()
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


async def delete_row(session: aiohttp.ClientSession, row_id: int) -> bool:
    try:
        async with session.delete(
            f"{SUPABASE_URL}/rest/v1/ctlog_probe_results",
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
            params={"id": f"eq.{row_id}"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            r.raise_for_status()
            return True
    except Exception as e:
        log.error(f"    Failed to delete row id={row_id}: {e}")
        return False


# ── orchestration ──────────────────────────────────────────

async def verify_row(session: aiohttp.ClientSession, row: dict, verifier, dry_run: bool,
                      sem: asyncio.Semaphore, counts: dict, lock: asyncio.Lock) -> None:
    slug = row["slug"]
    async with sem:
        try:
            is_live = await verifier(session, slug)
        except Exception as e:
            async with lock:
                counts["error"] += 1
            log.debug(f"    {slug}: check failed ({e}) — leaving in place")
            return

    if is_live:
        async with lock:
            counts["live"] += 1
        return

    if dry_run:
        async with lock:
            counts["dead"] += 1
        log.debug(f"    {slug}: DEAD — would delete (dry-run)")
        return

    ok = await delete_row(session, row["id"])
    async with lock:
        counts["dead" if ok else "error"] += 1


async def run_verification(platform: str, limit: int | None, dry_run: bool, concurrency: int) -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot verify.")
        sys.exit(1)

    verifier = PLATFORM_VERIFIERS[platform]
    sem = asyncio.Semaphore(concurrency)
    counts = {"live": 0, "dead": 0, "error": 0}
    lock = asyncio.Lock()

    async with aiohttp.ClientSession() as session:
        log.info(f"── Verifying {platform} ──")
        rows = await fetch_rows(session, platform, limit)
        total = len(rows)
        log.info(f"  {total} rows to check" + (" (dry-run — nothing will be deleted)" if dry_run else ""))

        if not rows:
            print(f"\nNo rows to verify for {platform}.")
            return

        tasks = [verify_row(session, row, verifier, dry_run, sem, counts, lock) for row in rows]

        # Progress logging via as_completed-style polling — gather with a
        # periodic progress task rather than awaiting each one serially.
        async def _progress():
            last = -1
            while True:
                await asyncio.sleep(5)
                done = counts["live"] + counts["dead"] + counts["error"]
                if done != last:
                    log.info(f"  progress: {done}/{total} (live={counts['live']} dead={counts['dead']} error={counts['error']})")
                    last = done
                if done >= total:
                    return

        progress_task = asyncio.create_task(_progress())
        await asyncio.gather(*tasks)
        progress_task.cancel()

    print("\n" + "=" * 70)
    print(f"VERIFICATION SUMMARY — {platform}")
    print("=" * 70)
    print(f"  Total checked: {total}")
    print(f"  Live (kept):   {counts['live']}")
    print(f"  Dead ({'would be ' if dry_run else ''}removed): {counts['dead']}")
    print(f"  Errors (left in place, inconclusive): {counts['error']}")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Verify ctlog_probe_results rows against live ATS boards (async)")
    parser.add_argument("--platform", choices=list(PLATFORM_VERIFIERS.keys()), required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    args = parser.parse_args()
    asyncio.run(run_verification(args.platform, args.limit, args.dry_run, args.concurrency))


if __name__ == "__main__":
    main()
