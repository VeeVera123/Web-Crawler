"""
VERIFICATION ENGINE (2026-08) — scans archive_i (ats + slug, formerly
quarantine) and archive_ii (career pages, formerly scrape_test) and
removes ONLY rows that are CONFIRMED DEAD: the board/page genuinely no
longer exists.[cite: 1]

THE RULE:
  - A clear, structural "this slug/company does not exist here" signal
    (a definite 404, a marketing redirect, a DNS/SSL resolution failure,
    or a parked domain) -> DEAD, delete the row.[cite: 1]
  - Anything else — a real response, rate-limit, 5xx, or timeout -> LEFT ALONE.[cite: 1]
"""
import argparse
import asyncio
import concurrent.futures
import glob
import json
import logging
import os
import random
import socket
import sys
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import aiohttp
from dotenv import load_dotenv

load_dotenv()

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "Main"))

import node
from ats_scrapers import scrape_board

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("verification")

REQUEST_TIMEOUT = node.REQUEST_TIMEOUT
USER_AGENT = node.USER_AGENT
DEFAULT_CONCURRENCY = 2

def _new_connector(concurrency: int) -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(limit=concurrency + 10, ttl_dns_cache=300)

_UNVERIFIABLE_ATS = {
    "workday", "smartrecruiters", "breezyhr", "taleo", 
    "oracle_cloud_hcm", "jobadder", "folkshr", "adp", 
    "ycombinator", "brassring", "successfactors"
}

# ── archive_i: ATS Verifiers ────────────────────────────────────────────

async def _verify_bamboohr(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://{slug}.bamboohr.com/careers/list"
    async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                            headers={"Accept": "application/json", "User-Agent": USER_AGENT}) as r:
        final_host = urlparse(str(r.url)).hostname or ""
        if final_host != f"{slug}.bamboohr.com":
            return False 
        if r.status != 200 or "application/json" not in r.headers.get("Content-Type", ""):
            return False
        try:
            data = await r.json(content_type=None)
            return isinstance(data, dict) and "result" in data
        except Exception:
            return False

async def _verify_icims(session: aiohttp.ClientSession, slug: str) -> bool:
    for host in (f"{slug}.icims.com", f"careers-{slug}.icims.com"):
        try:
            async with session.get(f"https://{host}/", timeout=REQUEST_TIMEOUT, allow_redirects=True,
                                    headers={"User-Agent": USER_AGENT}) as r:
                final_host = urlparse(str(r.url)).hostname or ""
                if final_host == host and r.status == 200:
                    return True
        except Exception:
            continue
    return False

async def _verify_generic_200(session: aiohttp.ClientSession, url: str, expected_host: str) -> bool:
    """Helper for platforms where domain match + 200 OK is sufficient."""
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                               headers={"User-Agent": USER_AGENT}) as r:
            final_host = urlparse(str(r.url)).hostname or ""
            return final_host == expected_host and r.status == 200
    except Exception:
        return False

async def _verify_teamtailor(session: aiohttp.ClientSession, slug: str) -> bool:
    return await _verify_generic_200(session, f"https://{slug}.teamtailor.com/", f"{slug}.teamtailor.com")

async def _verify_recruitee(session: aiohttp.ClientSession, slug: str) -> bool:
    return await _verify_generic_200(session, f"https://{slug}.recruitee.com/", f"{slug}.recruitee.com")

async def _verify_softgarden(session: aiohttp.ClientSession, slug: str) -> bool:
    return await _verify_generic_200(session, f"https://{slug}.softgarden.io/", f"{slug}.softgarden.io")

async def _verify_zoho(session: aiohttp.ClientSession, slug: str) -> bool:
    return await _verify_generic_200(session, f"https://{slug}.zohorecruit.com/", f"{slug}.zohorecruit.com")

async def _verify_hrmdirect(session: aiohttp.ClientSession, slug: str) -> bool:
    return await _verify_generic_200(session, f"https://{slug}.hrmdirect.com/", f"{slug}.hrmdirect.com")

async def _verify_personio(session: aiohttp.ClientSession, slug: str) -> bool:
    for suffix in ("jobs.personio.de", "jobs.personio.com"):
        if await _verify_generic_200(session, f"https://{slug}.{suffix}/", f"{slug}.{suffix}"):
            return True
    return False

async def _verify_rippling(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://ats.rippling.com/api/v2/board/{slug}/jobs"
    async with session.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")

async def _verify_greenhouse(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    async with session.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            try:
                data = await r.json()
                if isinstance(data, dict) and data.get("error") == "Not Found":
                    return False
            except Exception:
                pass
            return True
        raise RuntimeError(f"ambiguous status {r.status}")

async def _verify_lever(session: aiohttp.ClientSession, slug: str) -> bool:
    for url in (f"https://api.lever.co/v0/postings/{slug}?mode=json",
                f"https://api.eu.lever.co/v0/postings/{slug}?mode=json"):
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}) as r:
                if r.status == 200:
                    return True
        except Exception:
            continue
    
    for url in (f"https://api.lever.co/v0/postings/{slug}?mode=json",
                f"https://api.eu.lever.co/v0/postings/{slug}?mode=json"):
        async with session.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}) as r:
            if r.status != 404:
                raise RuntimeError(f"ambiguous status {r.status} on {url}")
    return False

async def _verify_ashby(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")

async def _verify_workable(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            try:
                data = await r.json()
                if "error" in data and "not found" in str(data.get("error")).lower():
                    return False
            except Exception:
                pass
            return True
        raise RuntimeError(f"ambiguous status {r.status}")

async def _verify_joincom(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://join.com/companies/{slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")

async def _verify_paylocity(session: aiohttp.ClientSession, slug: str) -> bool:
    parts = slug.split("|", 1)
    if len(parts) != 2:
        raise RuntimeError(f"unexpected paylocity slug format: {slug!r}")
    company_id, company_name_slug = parts
    url = f"https://recruiting.paylocity.com/recruiting/jobs/All/{company_id}/{company_name_slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}) as r:
        if r.status != 200:
            raise RuntimeError(f"ambiguous status {r.status}")
        text = await r.text()
    if "window.pageData" in text:
        return True
    lowered = text.lower()
    if "does not exist" in lowered or "job not found" in lowered:
        return False
    raise RuntimeError("neither pageData nor a recognized not-found page — ambiguous")

async def _verify_jobvite(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://jobs.jobvite.com/{slug}/jobs"
    async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=False,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 200:
            return True
        if r.status in (301, 302, 303, 307, 308):
            location = r.headers.get("Location", "")
            if "invalid=1" in location:
                return False
            raise RuntimeError(f"redirected somewhere unrecognized: {location!r}")
        raise RuntimeError(f"ambiguous status {r.status}")

async def _dns_dead_check(session: aiohttp.ClientSession, url: str) -> bool:
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                                headers={"User-Agent": USER_AGENT}):
            return True
    except aiohttp.ClientConnectorError as e:
        os_err = getattr(e, "os_error", None)
        if isinstance(os_err, socket.gaierror) or isinstance(e, aiohttp.ClientConnectorCertificateError):
            return False 
        if "certificate verify failed" in str(e).lower():
            return False
        raise

async def _verify_avature(session: aiohttp.ClientSession, slug: str) -> bool:
    return await _dns_dead_check(session, f"https://{slug}.avature.net/careers/SearchJobs")

async def _verify_eploy(session: aiohttp.ClientSession, slug: str) -> bool:
    return await _dns_dead_check(session, f"https://{slug}.eploy.net/candidate/jobboard/vacancysearchresults.aspx")

ARCHIVE_I_VERIFIERS = {
    "bamboohr": _verify_bamboohr, "icims": _verify_icims, "teamtailor": _verify_teamtailor,
    "recruitee": _verify_recruitee, "softgarden": _verify_softgarden, "zoho": _verify_zoho,
    "hrmdirect": _verify_hrmdirect, "personio": _verify_personio, "rippling": _verify_rippling,
    "greenhouse": _verify_greenhouse, "lever": _verify_lever, "ashby": _verify_ashby,
    "workable": _verify_workable, "joincom": _verify_joincom, "paylocity": _verify_paylocity,
    "jobvite": _verify_jobvite, "avature": _verify_avature, "eploy": _verify_eploy,
}

# ── archive_ii: Career Page Verification ─────────────────────────────

def _is_soft_404(text: str, url: str, final_host: str) -> bool:
    """Catches parked domains and 200 OKs that render 'Not Found' interfaces."""
    parked_domains = {"godaddy.com", "hugedomains.com", "dan.com", "sedo.com", "namecheap.com"}
    if any(pd in final_host.lower() for pd in parked_domains):
        return True
    
    title_match = re.search(r'<title[^>]*>(.*?)</title>', text, re.IGNORECASE)
    if title_match:
        title = title_match.group(1).lower()
        dead_signatures = ["not found", "404", "page unavailable", "does not exist", "domain for sale", "account suspended"]
        if any(sig in title for sig in dead_signatures):
            return True
    return False

async def _url_confirmed_dead(session: aiohttp.ClientSession, url: str) -> bool | None:
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                                headers={"User-Agent": USER_AGENT}) as r:
            if r.status in (404, 410):
                return True
            if r.status == 200:
                final_host = urlparse(str(r.url)).hostname or ""
                text = await r.text()
                if _is_soft_404(text, url, final_host):
                    return True
            return False
    except aiohttp.ClientConnectorError as e:
        os_err = getattr(e, "os_error", None)
        if isinstance(os_err, socket.gaierror) or isinstance(e, aiohttp.ClientConnectorCertificateError):
            return True
        if "certificate verify failed" in str(e).lower():
            return True
        return None
    except Exception:
        return None

async def verify_archive_ii_row(session: aiohttp.ClientSession, row: dict) -> bool:
    career_dead = await _url_confirmed_dead(session, row["career_page_url"])
    if career_dead is not True:
        return False
    website_dead = await _url_confirmed_dead(session, row["website_url"])
    return website_dead is True

# ── Supabase I/O (async) ──────────────────────────────────────────────

_RETRYABLE_STATUSES = {401, 403, 429, 500, 502, 503, 504}
MAX_HTTP_RETRIES = 6
_RETRY_BASE_DELAY = 2.0  
_MAX_RETRY_WAIT = 20.0   

def _backoff_wait(attempt: int) -> float:
    return min(_RETRY_BASE_DELAY * (2 ** attempt), _MAX_RETRY_WAIT) + random.uniform(0, 1)

class VerificationFetchError(Exception):
    pass

_GET_TIMEOUT = aiohttp.ClientTimeout(total=45, connect=20, sock_connect=20, sock_read=30)

async def _get_with_retries(session: aiohttp.ClientSession, url: str, headers: dict, params: dict | None = None) -> list:
    last_error = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            async with session.get(url, headers=headers, params=params, timeout=_GET_TIMEOUT) as r:
                if r.status in _RETRYABLE_STATUSES and attempt < MAX_HTTP_RETRIES - 1:
                    wait = _backoff_wait(attempt)
                    log.warning(f"  GET {url} got HTTP {r.status}, retrying in {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                r.raise_for_status()
                return await r.json()
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            if attempt < MAX_HTTP_RETRIES - 1:
                wait = _backoff_wait(attempt)
                await asyncio.sleep(wait)
    raise VerificationFetchError(f"GET {url} failed after {MAX_HTTP_RETRIES} attempts: {last_error}")

async def _get_count(session: aiohttp.ClientSession, table: str, headers: dict, params: dict) -> int:
    url = f"{node.SUPABASE_URL}/rest/v1/{table}"
    count_headers = {**headers, "Range": "0-0", "Prefer": "count=exact"}
    last_error = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            async with session.get(url, headers=count_headers, params=params, timeout=_GET_TIMEOUT) as r:
                if r.status in _RETRYABLE_STATUSES and attempt < MAX_HTTP_RETRIES - 1:
                    wait = _backoff_wait(attempt)
                    await asyncio.sleep(wait)
                    continue
                r.raise_for_status()
                content_range = r.headers.get("Content-Range", "")
                return int(content_range.split("/")[-1])
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            if attempt < MAX_HTTP_RETRIES - 1:
                wait = _backoff_wait(attempt)
                await asyncio.sleep(wait)
    raise VerificationFetchError(f"COUNT {url} failed after {MAX_HTTP_RETRIES} attempts: {last_error}")

async def fetch_rows(session: aiohttp.ClientSession, table: str, select: str,
                      ats_filter: str | None, limit: int | None,
                      shard_index: int | None = None, shard_count: int | None = None) -> list[dict]:
    rows = []
    page_size = 1000
    headers = {"apikey": node.SUPABASE_KEY, "Authorization": f"Bearer {node.SUPABASE_KEY}"}
    params = {"select": select, "order": "id.asc"}
    if ats_filter:
        params["ats"] = f"eq.{ats_filter}"

    start_offset = 0
    end_offset = None
    if shard_count:
        total = await _get_count(session, table, headers, params)
        shard_size = -(-total // shard_count)
        start_offset = shard_index * shard_size
        end_offset = min(start_offset + shard_size, total)
        if start_offset >= total:
            return []

    offset = start_offset
    while True:
        if limit is not None and len(rows) >= limit:
            rows = rows[:limit]
            break
        page_end = offset + page_size - 1
        if end_offset is not None:
            page_end = min(page_end, end_offset - 1)
            if offset > page_end:
                break
        page_headers = {**headers, "Range": f"{offset}-{page_end}"}
        batch = await _get_with_retries(session, f"{node.SUPABASE_URL}/rest/v1/{table}", page_headers, params)
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < (page_end - offset + 1):
            break
        offset = page_end + 1
        if end_offset is not None and offset >= end_offset:
            break
    return rows

_DELETE_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=15, sock_connect=15, sock_read=20)

async def delete_row(session: aiohttp.ClientSession, table: str, row_id: int) -> bool:
    headers = {"apikey": node.SUPABASE_KEY, "Authorization": f"Bearer {node.SUPABASE_KEY}"}
    url = f"{node.SUPABASE_URL}/rest/v1/{table}"
    last_error = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            async with session.delete(url, headers=headers, params={"id": f"eq.{row_id}"}, timeout=_DELETE_TIMEOUT) as r:
                if r.status in _RETRYABLE_STATUSES and attempt < MAX_HTTP_RETRIES - 1:
                    wait = _backoff_wait(attempt)
                    await asyncio.sleep(wait)
                    continue
                r.raise_for_status()
                return True
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            if attempt < MAX_HTTP_RETRIES - 1:
                wait = _backoff_wait(attempt)
                await asyncio.sleep(wait)
    log.error(f"    Failed to delete row id={row_id} from {table} after {MAX_HTTP_RETRIES} attempts: {last_error}")
    return False

# ── orchestration ───────────────────────────────────────────────────

async def verify_archive_i_row(session: aiohttp.ClientSession, row: dict, dry_run: bool,
                                 sem: asyncio.Semaphore, executor: concurrent.futures.ThreadPoolExecutor,
                                 counts: dict, lock: asyncio.Lock) -> None:
    ats, slug = row["ats"], row["slug"]
    verifier = ARCHIVE_I_VERIFIERS[ats]
    async with sem:
        try:
            is_live = await verifier(session, slug)
        except Exception as e:
            async with lock:
                counts["unverified"] += 1
            log.debug(f"    {ats}/{slug}: check failed ({e}) — leaving in place")
            return

        if not is_live:
            if dry_run:
                async with lock:
                    counts["dead"] += 1
                log.info(f"    {ats}/{slug}: DEAD — would delete (report-only)")
                return
            ok = await delete_row(session, node.ARCHIVE_I_TABLE, row["id"])
            async with lock:
                counts["dead" if ok else "unverified"] += 1
            if ok:
                log.info(f"    {ats}/{slug}: DEAD — deleted")
            return

        loop = asyncio.get_running_loop()
        try:
            jobs = await loop.run_in_executor(executor, scrape_board, ats, slug)
        except Exception:
            jobs = []
        async with lock:
            counts["active" if jobs else "empty"] += 1

async def verify_archive_ii_row_task(session: aiohttp.ClientSession, row: dict, dry_run: bool,
                                       sem: asyncio.Semaphore, counts: dict, lock: asyncio.Lock) -> None:
    async with sem:
        try:
            is_dead = await verify_archive_ii_row(session, row)
        except Exception as e:
            async with lock:
                counts["unverified"] += 1
            log.debug(f"    {row['website_url']}: check failed ({e}) — leaving in place")
            return

        if not is_dead:
            async with lock:
                counts["live"] += 1
            return

        if dry_run:
            async with lock:
                counts["dead"] += 1
            log.info(f"    {row['website_url']}: DEAD — would delete (report-only)")
            return

        ok = await delete_row(session, node.ARCHIVE_II_TABLE, row["id"])
        async with lock:
            counts["dead" if ok else "unverified"] += 1
        if ok:
            log.info(f"    {row['website_url']}: DEAD — deleted")

async def _run_progress(counts: dict, total: int, label: str):
    last = -1
    while True:
        await asyncio.sleep(5)
        done = sum(counts.values())
        if done != last:
            log.info(f"  [{label}] progress: {done}/{total} ({counts})")
            last = done
        if done >= total:
            return

def _print_summary(label: str, checked: int, counts: dict, dry_run: bool):
    total_rows = sum(counts.values())
    retained = total_rows - counts.get("dead", 0)
    print("\n" + "=" * 70)
    print(f"VERIFICATION SUMMARY — {label}")
    print("=" * 70)
    print(f"  Total rows:                 {total_rows}")
    print(f"  Checked (live network call):{checked}")
    for key, display in (("active", "Active (>=1 open job)"),
                          ("empty", "Empty (real board, 0 jobs)"),
                          ("live", "Live (kept)"),
                          ("dead", f"Dead ({'would be ' if dry_run else ''}removed)"),
                          ("unverified", "Unverified (kept — incl. unverifiable platforms)")):
        if key in counts:
            print(f"  {display + ':':<45} {counts[key]}")
    print(f"  Total retained (everything NOT deleted):    {retained}")
    print("=" * 70)

def _empty_counts_i() -> dict:
    return {"active": 0, "empty": 0, "dead": 0, "unverified": 0}

def _empty_counts_ii() -> dict:
    return {"live": 0, "dead": 0, "unverified": 0}

async def _stagger_shard_start(shard_index: int | None, shard_count: int | None) -> None:
    if not shard_count or shard_index is None:
        return
    delay = shard_index * 1.5 + random.uniform(0, 1)
    if delay > 0:
        await asyncio.sleep(delay)

async def run_archive_i(ats_filter: str | None, limit: int | None, dry_run: bool,
                          concurrency: int, shard_index: int | None, shard_count: int | None) -> dict:
    sem = asyncio.Semaphore(concurrency)
    counts = _empty_counts_i()
    lock = asyncio.Lock()

    if ats_filter and ats_filter in _UNVERIFIABLE_ATS:
        log.warning(f"  '{ats_filter}' has no confirmed-safe not-found signal — nothing to verify.")
        return counts

    async with aiohttp.ClientSession(connector=_new_connector(concurrency)) as session:
        shard_note = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
        log.info(f"── Verifying archive_i{f' ({ats_filter})' if ats_filter else ' (all verifiable platforms)'}{shard_note} ──")
        await _stagger_shard_start(shard_index, shard_count)
        all_rows = await fetch_rows(session, node.ARCHIVE_I_TABLE, "id,ats,slug", ats_filter, None, shard_index, shard_count)

        verifiable_rows = [r for r in all_rows if r["ats"] in ARCHIVE_I_VERIFIERS]
        skipped_rows = [r for r in all_rows if r["ats"] not in ARCHIVE_I_VERIFIERS]
        counts["unverified"] += len(skipped_rows)

        if limit is not None:
            verifiable_rows = verifiable_rows[:limit]
        checked = len(verifiable_rows)

        if skipped_rows and not ats_filter:
            skipped_by_ats = {}
            for r in skipped_rows:
                skipped_by_ats[r["ats"]] = skipped_by_ats.get(r["ats"], 0) + 1
            log.info(f"  {len(skipped_rows)} rows on unverifiable platforms (kept, counted as unverified): "
                     f"{dict(sorted(skipped_by_ats.items(), key=lambda kv: -kv[1]))}")

        log.info(f"  {checked} rows to check" + (" (report-only)" if dry_run else " (EXECUTE MODE)"))
        if not checked:
            _print_summary("archive_i" + shard_note, checked, counts, dry_run)
            return counts

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            tasks = [verify_archive_i_row(session, row, dry_run, sem, executor, counts, lock)
                     for row in verifiable_rows]
            progress_task = asyncio.create_task(_run_progress(counts, checked + len(skipped_rows), "archive_i"))
            await asyncio.gather(*tasks)
            progress_task.cancel()

    _print_summary("archive_i" + shard_note, checked, counts, dry_run)
    return counts

async def run_archive_ii(limit: int | None, dry_run: bool, concurrency: int,
                           shard_index: int | None, shard_count: int | None) -> dict:
    sem = asyncio.Semaphore(concurrency)
    counts = _empty_counts_ii()
    lock = asyncio.Lock()

    async with aiohttp.ClientSession(connector=_new_connector(concurrency)) as session:
        shard_note = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
        log.info(f"── Verifying archive_ii (career pages){shard_note} ──")
        await _stagger_shard_start(shard_index, shard_count)
        rows = await fetch_rows(session, node.ARCHIVE_II_TABLE, "id,career_page_url,website_url", None, limit, shard_index, shard_count)
        total = len(rows)

        log.info(f"  {total} rows to check" + (" (report-only)" if dry_run else " (EXECUTE MODE)"))
        if not total:
            _print_summary("archive_ii" + shard_note, total, counts, dry_run)
            return counts

        tasks = [verify_archive_ii_row_task(session, row, dry_run, sem, counts, lock) for row in rows]
        progress_task = asyncio.create_task(_run_progress(counts, total, "archive_ii"))
        await asyncio.gather(*tasks)
        progress_task.cancel()

    _print_summary("archive_ii" + shard_note, total, counts, dry_run)
    return counts

def _summarize_dir(directory: str) -> None:
    paths = sorted(glob.glob(os.path.join(directory, "*.json")))
    if not paths:
        log.error(f"No *.json shard summaries found in {directory!r} — nothing to combine.")
        sys.exit(1)

    combined_ii = _empty_counts_i()
    combined_iii = _empty_counts_ii()
    dry_run = None
    shards_seen = 0

    for p in paths:
        with open(p) as f:
            data = json.load(f)
        shards_seen += 1
        if dry_run is None:
            dry_run = data.get("dry_run", True)
        for key, val in (data.get("archive_i") or {}).items():
            combined_ii[key] = combined_ii.get(key, 0) + val
        for key, val in (data.get("archive_ii") or {}).items():
            combined_iii[key] = combined_iii.get(key, 0) + val

    log.info(f"Combined {shards_seen} shard summaries from {directory}")
    if any(combined_ii.values()):
        checked_ii = combined_ii["active"] + combined_ii["empty"] + combined_ii["dead"]
        _print_summary("archive_i — ALL SHARDS COMBINED", checked_ii, combined_ii, bool(dry_run))
    if any(combined_iii.values()):
        checked_iii = combined_iii["live"] + combined_iii["dead"]
        _print_summary("archive_ii — ALL SHARDS COMBINED", checked_iii, combined_iii, bool(dry_run))

async def run_stale_role_cleanup(table: str, dry_run: bool, stale_days: int = 183) -> dict:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=stale_days)).isoformat()
    headers = {"apikey": node.SUPABASE_KEY, "Authorization": f"Bearer {node.SUPABASE_KEY}"}
    count_headers = {**headers, "Range": "0-0", "Prefer": "count=exact"}
    params = {"last_seen": f"lt.{cutoff}"}
    url = f"{node.SUPABASE_URL}/rest/v1/{table}"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=count_headers, params=params, timeout=_GET_TIMEOUT) as r:
                r.raise_for_status()
                content_range = r.headers.get("Content-Range", "")
                stale_count = int(content_range.split("/")[-1])
        except Exception as e:
            log.error(f"{table}: stale-role-cleanup COUNT failed: {e}")
            return {"table": table, "cutoff": cutoff, "stale_count": None, "deleted": False}

        log.info(f"{table}: {stale_count} rows with last_seen older than {stale_days} days "
                 f"(cutoff {cutoff})" + (" — DRY RUN, not deleting" if dry_run else " — DELETING"))

        if dry_run or stale_count == 0:
            return {"table": table, "cutoff": cutoff, "stale_count": stale_count, "deleted": False}

        try:
            async with session.delete(url, headers=headers, params=params, timeout=_DELETE_TIMEOUT) as r:
                r.raise_for_status()
        except Exception as e:
            log.error(f"{table}: stale-role-cleanup DELETE failed: {e}")
            return {"table": table, "cutoff": cutoff, "stale_count": stale_count, "deleted": False}

    log.info(f"{table}: deleted {stale_count} rows with no role found in the last {stale_days} days.")
    return {"table": table, "cutoff": cutoff, "stale_count": stale_count, "deleted": True}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=["archive_i", "archive_ii", "both"], default="both")
    parser.add_argument("--ats", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=None)
    parser.add_argument("--summary-out", default=None)
    parser.add_argument("--summarize", default=None)
    parser.add_argument("--stale-role-cleanup", action="store_true")
    parser.add_argument("--stale-days", type=int, default=183)
    args = parser.parse_args()

    if args.summarize:
        _summarize_dir(args.summarize)
        return

    if args.stale_role_cleanup:
        if not node.SUPABASE_URL or not node.SUPABASE_KEY:
            log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot run stale-role cleanup.")
            sys.exit(1)
        dry_run = not args.execute

        async def _run_stale_cleanup():
            summary = {"dry_run": dry_run, "stale_days": args.stale_days}
            if args.table in ("archive_i", "both"):
                summary["archive_i_stale"] = await run_stale_role_cleanup(node.ARCHIVE_I_TABLE, dry_run, args.stale_days)
            if args.table in ("archive_ii", "both"):
                summary["archive_ii_stale"] = await run_stale_role_cleanup(node.ARCHIVE_II_TABLE, dry_run, args.stale_days)
            if args.summary_out:
                os.makedirs(os.path.dirname(args.summary_out) or ".", exist_ok=True)
                with open(args.summary_out, "w") as f:
                    json.dump(summary, f)
        asyncio.run(_run_stale_cleanup())
        return

    if (args.shard_index is None) != (args.shard_count is None):
        parser.error("--shard-index and --shard-count must be given together")

    if not node.SUPABASE_URL or not node.SUPABASE_KEY:
        log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot verify.")
        sys.exit(1)

    dry_run = not args.execute

    async def _run_all():
        summary = {"dry_run": dry_run}
        if args.table in ("archive_i", "both"):
            summary["archive_i"] = await run_archive_i(args.ats, args.limit, dry_run, args.concurrency, args.shard_index, args.shard_count)
        if args.table in ("archive_ii", "both"):
            summary["archive_ii"] = await run_archive_ii(args.limit, dry_run, args.concurrency, args.shard_index, args.shard_count)
        if args.summary_out:
            os.makedirs(os.path.dirname(args.summary_out) or ".", exist_ok=True)
            with open(args.summary_out, "w") as f:
                json.dump(summary, f)

    asyncio.run(_run_all())

if __name__ == "__main__":
    main()
