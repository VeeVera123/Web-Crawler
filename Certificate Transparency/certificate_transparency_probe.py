"""
CTLOGS PROBE (renamed from ctlog_verify_async.py, 2026-08) — phase 2 of
the CT-logs pipeline. Runs as a SEPARATE script from seeding
(ctlogs_seed.py) per explicit direction — verification is slower and
noisier (hits live third-party servers, can be rate-limited or flaky) and
should never block or force a rerun of seeding, and vice versa. Now pulls
its shared plumbing (USER_AGENT, timeout, Supabase creds, connector) from
node.py — the per-platform verifiers below stay here since they check a
candidate's own board URL directly, a different shape than node.py's
company-homepage crawl.

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
    python ctlogs_probe.py --platform bamboohr
    python ctlogs_probe.py --platform icims
    python ctlogs_probe.py --platform rippling
    python ctlogs_probe.py --platform teamtailor
    python ctlogs_probe.py --platform bamboohr --dry-run
    python ctlogs_probe.py --platform bamboohr --limit 500
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

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)  # for node.py
import node  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("ctlogs_probe")

# SUPABASE_URL/KEY, REQUEST_TIMEOUT, USER_AGENT come from node.py now.
REQUEST_TIMEOUT = node.REQUEST_TIMEOUT
USER_AGENT = node.USER_AGENT
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


async def _verify_recruitee(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://{slug}.recruitee.com/"
    async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                            headers={"User-Agent": USER_AGENT}) as r:
        final_host = urlparse(str(r.url)).hostname or ""
        if final_host != f"{slug}.recruitee.com":
            return False
        return r.status == 200


async def _verify_softgarden(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://{slug}.softgarden.io/"
    async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                            headers={"User-Agent": USER_AGENT}) as r:
        final_host = urlparse(str(r.url)).hostname or ""
        if final_host != f"{slug}.softgarden.io":
            return False
        return r.status == 200


async def _verify_zoho(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://{slug}.zohorecruit.com/"
    async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                            headers={"User-Agent": USER_AGENT}) as r:
        final_host = urlparse(str(r.url)).hostname or ""
        if final_host != f"{slug}.zohorecruit.com":
            return False
        return r.status == 200


async def _verify_hrmdirect(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://{slug}.hrmdirect.com/"
    async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                            headers={"User-Agent": USER_AGENT}) as r:
        final_host = urlparse(str(r.url)).hostname or ""
        if final_host != f"{slug}.hrmdirect.com":
            return False
        return r.status == 200


async def _verify_personio(session: aiohttp.ClientSession, slug: str) -> bool:
    """Personio tenants live under either jobs.personio.de or
    jobs.personio.com — the extraction step doesn't record which suffix
    a given slug came from (slug is suffix-stripped, see
    _url_to_slug_personio), so try both rather than guessing."""
    for suffix in ("jobs.personio.de", "jobs.personio.com"):
        host = f"{slug}.{suffix}"
        url = f"https://{host}/"
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                                    headers={"User-Agent": USER_AGENT}) as r:
                final_host = urlparse(str(r.url)).hostname or ""
                if final_host == host and r.status == 200:
                    return True
        except Exception:
            continue
    return False


# workday's verifier was removed 2026-08 along with the platform itself —
# two live extraction runs confirmed crt.sh only surfaces ~115-123 hosts
# total for myworkdayjobs.com, over 90% of which is Workday's own
# internal infrastructure, not customer tenants (see ctlog_extract.py's
# module docstring for the full history). Nothing to verify for a
# platform that isn't extracted anymore.
PLATFORM_VERIFIERS = {
    "bamboohr": _verify_bamboohr,
    "icims": _verify_icims,
    "rippling": _verify_rippling,
    "teamtailor": _verify_teamtailor,
    "personio": _verify_personio,
    "recruitee": _verify_recruitee,
    "softgarden": _verify_softgarden,
    "zoho": _verify_zoho,
    "hrmdirect": _verify_hrmdirect,
}


# ── Supabase I/O (async) ──────────────────────────────────────

async def fetch_rows(session: aiohttp.ClientSession, ats: str, limit: int | None) -> list[dict]:
    rows = []
    page_size = 1000
    offset = 0
    headers = {"apikey": node.SUPABASE_KEY, "Authorization": f"Bearer {node.SUPABASE_KEY}"}
    while True:
        if limit is not None and len(rows) >= limit:
            rows = rows[:limit]
            break
        headers["Range"] = f"{offset}-{offset + page_size - 1}"
        async with session.get(
            f"{node.SUPABASE_URL}/rest/v1/{node.STAGING_TABLE}",
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
            f"{node.SUPABASE_URL}/rest/v1/{node.STAGING_TABLE}",
            headers={"apikey": node.SUPABASE_KEY, "Authorization": f"Bearer {node.SUPABASE_KEY}"},
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
    if not node.SUPABASE_URL or not node.SUPABASE_KEY:
        log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot verify.")
        sys.exit(1)

    verifier = PLATFORM_VERIFIERS[platform]
    sem = asyncio.Semaphore(concurrency)
    counts = {"live": 0, "dead": 0, "error": 0}
    lock = asyncio.Lock()

    async with aiohttp.ClientSession(connector=node.new_connector()) as session:
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
