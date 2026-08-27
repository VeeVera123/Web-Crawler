"""
VERIFICATION ENGINE (2026-08) — scans archive_ii (ats + slug, formerly
quarantine) and archive_iii (career pages, formerly scrape_test) and
removes ONLY rows that are CONFIRMED DEAD: the board/page genuinely no
longer exists. It does NOT remove rows just because a real board
currently has zero open postings, or a career page's content has
changed — see THE RULE below for why, and the 2026-08 research summary
for how each platform's rule was derived.

THE RULE, same spirit as certificate_transparency_probe.py's Phase-2
verifier (this file follows and extends that one's design, and directly
reuses several of its already-proven per-platform checks):
  - A clear, structural "this slug/company does not exist here" signal
    (a definite 404 from the platform's OWN API, a redirect that bounces
    away to the ATS VENDOR's marketing/root domain instead of staying on
    the tenant's own subdomain, a DNS resolution failure for a
    subdomain-per-tenant platform, or an explicit "does not exist" page
    the platform itself serves) -> DEAD, delete the row.
  - Anything else — a real response, even an empty one (zero current
    jobs), a redirect that lands somewhere else entirely (ambiguous, not
    the vendor's own marketing bounce), a rate-limit, a 5xx, a timeout,
    an unexpected status code -> LEFT ALONE, never deleted. A row we
    couldn't cleanly confirm dead is not evidence it's dead.
  - Per explicit instruction (2026-08): a real, valid board that simply
    has zero open postings right now is KEPT, not deleted — that's what
    ats_scrapers.py's own future 183-day-since-last-hit cleanup (a
    separate, later piece of work) is for, not this file. This file's
    only job is pruning slugs/pages that never existed, were mistyped,
    or the company has genuinely torn down — not "currently quiet".

WHY 18 OF 26 ATS PLATFORMS, NOT ALL: 2026-08, four parallel research
passes empirically tested real vs fake slugs against every SCRAPERS-
registered platform's actual endpoint (WebFetch against live real and
obviously-fake slugs, cross-checked against each platform's own docs).
18 platforms have a confirmed-safe, structurally distinct "does not
exist" signal. 8 do not (see _UNVERIFIABLE_ATS below) — either the
platform returns an identical-looking response for "doesn't exist" and
"real board, 0 jobs" (oracle_cloud_hcm, confirmed empirically: a real
empty tenant returns the exact same 200+empty-array shape a nonexistent
one would), the check requires JS/POST semantics no lightweight HTTP
probe can safely replicate (workday, smartrecruiters, taleo), or no
live example could be found/reached to confirm a rule at all (breezyhr,
jobadder — jobadder's "Nothing here I'm afraid..." page was found to be
plausibly the SAME message a real empty board shows, an explicitly
UNSAFE signal — folkshr, adp). Rows on unverifiable platforms, plus
brassring/successfactors (JS-rendered, no scraper at all — see
ats_scrapers.py's SCRAPERS dict) and ycombinator (a job-board aggregator,
not a per-company ATS — see discovery.py's URL_TO_SLUG comment), are
left completely untouched by this engine and only counted/logged.

SAFETY MODEL (a wrong delete here is real, silent, permanent data loss):
  - dry-run is the DEFAULT and the only way anything is ever deleted is
    the explicit --execute flag — a normal run only logs what it WOULD
    delete. Read that log before ever passing --execute.
  - every per-row check is wrapped so ANY exception (timeout, connection
    reset, unexpected error) is treated as inconclusive/leave-alone —
    only an explicit, structural "confirmed dead" return value can lead
    to a delete, mirroring certificate_transparency_probe.py's own
    verify_row() pattern exactly.
  - archive_iii verification requires BOTH the career_page_url AND the
    company's own website_url (root domain) to independently fail before
    a row is treated as dead — a career page alone returning 404 usually
    just means the page moved (common on a real, live site), which is
    not the same as the company disappearing.

Usage:
    pip install aiohttp python-dotenv
    python verification.py --table archive_ii                 # dry-run, all verifiable platforms
    python verification.py --table archive_ii --ats greenhouse
    python verification.py --table archive_iii
    python verification.py --table both --execute              # ACTUALLY deletes confirmed-dead rows
    python verification.py --table archive_ii --limit 500 --concurrency 40
"""
import argparse
import asyncio
import logging
import os
import socket
import sys
from urllib.parse import urlparse

import aiohttp
from dotenv import load_dotenv

load_dotenv()

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)  # for node.py
sys.path.insert(0, os.path.join(_ROOT, "Certificate Transparency"))  # for proven verifiers below
import node  # noqa: E402
# Reused as-is — these 8 already check the exact host/path
# ats_scrapers.py's real production scraper uses (or, for icims/teamtailor/
# recruitee/softgarden/zoho/hrmdirect, the tenant's own subdomain root,
# which is a safe, path-independent existence proxy for a per-tenant-
# subdomain platform), and already treat any connection failure as
# inconclusive rather than dead — exactly this file's own safety model,
# just proven in production first. See that file's own module docstring
# for the full "final redirect host must match, else dead" rationale.
from certificate_transparency_probe import (  # noqa: E402
    _verify_bamboohr, _verify_icims, _verify_teamtailor, _verify_recruitee,
    _verify_softgarden, _verify_zoho, _verify_hrmdirect, _verify_personio,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("verification")

REQUEST_TIMEOUT = node.REQUEST_TIMEOUT
USER_AGENT = node.USER_AGENT
DEFAULT_CONCURRENCY = 30


# ── archive_ii: platforms with NO safe "does not exist" signal ────────────
# Left completely untouched — never checked, never deleted, only counted.
# See the module docstring above for why each one is here.
_UNVERIFIABLE_ATS = {
    "workday",           # SPA + POST-based API; no reliable GET-based signal found
    "smartrecruiters",   # docs suggest a 404, but shared-domain API is robots-gated
                          # and couldn't be empirically re-confirmed live
    "breezyhr",           # no live customer example could be found/reached to confirm any rule
    "taleo",              # host unreachable from this research pass (env/network limitation)
    "oracle_cloud_hcm",   # CONFIRMED UNSAFE: a real, empty tenant returns the exact same
                          # 200 + {"items":[],"count":0} shape a nonexistent one plausibly would
    "jobadder",           # CONFIRMED UNSAFE: "Nothing here I'm afraid..." could be the same
                          # message a real, empty board shows — no way to distinguish
    "folkshr",            # no live customer example could be found/reached to confirm any rule
    "adp",                 # no live customer example could be found/reached to confirm any rule
    "brassring",          # JS-rendered, no HTTP scraper at all (see ats_scrapers.py SCRAPERS)
    "successfactors",     # JS-rendered, no HTTP scraper at all (see ats_scrapers.py SCRAPERS)
    "ycombinator",        # job-board aggregator, not a per-company ATS slug (see discovery.py)
}


# ── archive_ii: NEW verifiers (platforms certificate_transparency_probe.py
# never covered) ────────────────────────────────────────────────────────

async def _verify_rippling(session: aiohttp.ClientSession, slug: str) -> bool:
    """Rippling's real production scraper (ats_scrapers.scrape_rippling)
    hits the shared ats.rippling.com API, NOT a {slug}.rippling.com
    subdomain — checking the actual API endpoint here (unlike a bare
    subdomain check) is what keeps this consistent with what the daily
    scanner would actually find."""
    url = f"https://ats.rippling.com/api/v2/board/{slug}/jobs"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")


async def _verify_greenhouse(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")


async def _verify_lever(session: aiohttp.ClientSession, slug: str) -> bool:
    """Only DEAD if BOTH the main and EU endpoints 404 — a real board
    could legitimately live on either one (see ats_scrapers.scrape_lever's
    own EU fallback)."""
    for url in (f"https://api.lever.co/v0/postings/{slug}?mode=json",
                f"https://api.eu.lever.co/v0/postings/{slug}?mode=json"):
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT,
                                    headers={"User-Agent": USER_AGENT}) as r:
                if r.status == 200:
                    return True
        except Exception:
            continue
    # Neither endpoint returned 200 — confirm both actually 404'd (not just
    # errored) before calling it dead.
    for url in (f"https://api.lever.co/v0/postings/{slug}?mode=json",
                f"https://api.eu.lever.co/v0/postings/{slug}?mode=json"):
        async with session.get(url, timeout=REQUEST_TIMEOUT,
                                headers={"User-Agent": USER_AGENT}) as r:
            if r.status != 404:
                raise RuntimeError(f"ambiguous status {r.status} on {url}")
    return False


async def _verify_ashby(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")


async def _verify_workable(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")


async def _verify_joincom(session: aiohttp.ClientSession, slug: str) -> bool:
    """Page-level check only (join.com/companies/{slug}) — the real jobs
    API needs a company_id resolved from this same page first, so a clean
    404 on the page itself is already a safe existence proxy."""
    url = f"https://join.com/companies/{slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")


async def _verify_paylocity(session: aiohttp.ClientSession, slug: str) -> bool:
    """slug is 'company_id|company_name' (see discovery._url_to_slug_paylocity
    and ats_scrapers.scrape_paylocity). A real board's page embeds a
    window.pageData JSON blob (checked here) regardless of whether it
    currently has any open jobs; a fake company_id instead serves a static
    'Job Not Found'/'does not exist' page with no such blob."""
    parts = slug.split("|", 1)
    if len(parts) != 2:
        raise RuntimeError(f"unexpected paylocity slug format: {slug!r}")
    company_id, company_name_slug = parts
    url = f"https://recruiting.paylocity.com/recruiting/jobs/All/{company_id}/{company_name_slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
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
    """A nonexistent Jobvite company 302s to jobvite.com's own support page
    with a distinctive '?invalid=1' query param — a real board (even an
    empty one) serves its own jobs page directly, no such redirect."""
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
    """Shared helper for subdomain-per-tenant platforms whose ONLY safe
    'does not exist' signal is the subdomain simply failing to resolve at
    all (confirmed for avature/eploy: a nonexistent tenant subdomain isn't
    provisioned in DNS at all, vs a real tenant which resolves and serves
    something — even an error page — every time). Any other failure mode
    (connection refused post-DNS, timeout, non-2xx after resolving) is
    left ambiguous, not dead — only a genuine name-resolution failure
    counts."""
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                                headers={"User-Agent": USER_AGENT}):
            return True  # resolved and got SOME response — tenant exists
    except aiohttp.ClientConnectorError as e:
        os_err = getattr(e, "os_error", None)
        if isinstance(os_err, socket.gaierror):
            return False  # genuine DNS resolution failure — confirmed dead
        raise  # some other connection failure (refused, unreachable) — ambiguous


async def _verify_avature(session: aiohttp.ClientSession, slug: str) -> bool:
    return await _dns_dead_check(session, f"https://{slug}.avature.net/careers/SearchJobs")


async def _verify_eploy(session: aiohttp.ClientSession, slug: str) -> bool:
    return await _dns_dead_check(
        session, f"https://{slug}.eploy.net/candidate/jobboard/vacancysearchresults.aspx")


ARCHIVE_II_VERIFIERS = {
    "bamboohr": _verify_bamboohr,
    "icims": _verify_icims,
    "teamtailor": _verify_teamtailor,
    "recruitee": _verify_recruitee,
    "softgarden": _verify_softgarden,
    "zoho": _verify_zoho,
    "hrmdirect": _verify_hrmdirect,
    "personio": _verify_personio,
    "rippling": _verify_rippling,
    "greenhouse": _verify_greenhouse,
    "lever": _verify_lever,
    "ashby": _verify_ashby,
    "workable": _verify_workable,
    "joincom": _verify_joincom,
    "paylocity": _verify_paylocity,
    "jobvite": _verify_jobvite,
    "avature": _verify_avature,
    "eploy": _verify_eploy,
}


# ── archive_iii: career-page verification ─────────────────────────────

async def _url_confirmed_dead(session: aiohttp.ClientSession, url: str) -> bool | None:
    """Returns True if this specific URL is confirmed dead (404, or the
    domain fails to resolve at all), False if it responded with anything
    else usable, or None if the check was inconclusive (timeout, 5xx,
    other connection error — never treated as dead)."""
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                                headers={"User-Agent": USER_AGENT}) as r:
            if r.status == 404:
                return True
            return False
    except aiohttp.ClientConnectorError as e:
        os_err = getattr(e, "os_error", None)
        if isinstance(os_err, socket.gaierror):
            return True  # domain doesn't resolve at all
        return None
    except Exception:
        return None


async def verify_archive_iii_row(session: aiohttp.ClientSession, row: dict) -> bool:
    """DEAD only if BOTH the specific career_page_url AND the company's
    root website_url independently confirm dead — a career page 404 alone
    usually just means the page moved on an otherwise-live site, which is
    not the same as the company disappearing (see module docstring)."""
    career_dead = await _url_confirmed_dead(session, row["career_page_url"])
    if career_dead is not True:
        return False  # career page still resolves (or check was inconclusive) — keep
    website_dead = await _url_confirmed_dead(session, row["website_url"])
    return website_dead is True


# ── Supabase I/O (async) ──────────────────────────────────────────────

async def fetch_rows(session: aiohttp.ClientSession, table: str, select: str,
                      ats_filter: str | None, limit: int | None) -> list[dict]:
    rows = []
    page_size = 1000
    offset = 0
    headers = {"apikey": node.SUPABASE_KEY, "Authorization": f"Bearer {node.SUPABASE_KEY}"}
    params = {"select": select}
    if ats_filter:
        params["ats"] = f"eq.{ats_filter}"
    while True:
        if limit is not None and len(rows) >= limit:
            rows = rows[:limit]
            break
        headers["Range"] = f"{offset}-{offset + page_size - 1}"
        async with session.get(
            f"{node.SUPABASE_URL}/rest/v1/{table}",
            headers=headers, params=params,
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


async def delete_row(session: aiohttp.ClientSession, table: str, row_id: int) -> bool:
    try:
        async with session.delete(
            f"{node.SUPABASE_URL}/rest/v1/{table}",
            headers={"apikey": node.SUPABASE_KEY, "Authorization": f"Bearer {node.SUPABASE_KEY}"},
            params={"id": f"eq.{row_id}"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            r.raise_for_status()
            return True
    except Exception as e:
        log.error(f"    Failed to delete row id={row_id} from {table}: {e}")
        return False


# ── orchestration ───────────────────────────────────────────────────

async def verify_archive_ii_row(session: aiohttp.ClientSession, row: dict, dry_run: bool,
                                 sem: asyncio.Semaphore, counts: dict, lock: asyncio.Lock) -> None:
    ats, slug = row["ats"], row["slug"]
    verifier = ARCHIVE_II_VERIFIERS[ats]
    async with sem:
        try:
            is_live = await verifier(session, slug)
        except Exception as e:
            async with lock:
                counts["error"] += 1
            log.debug(f"    {ats}/{slug}: check failed ({e}) — leaving in place")
            return

    if is_live:
        async with lock:
            counts["live"] += 1
        return

    if dry_run:
        async with lock:
            counts["dead"] += 1
        log.info(f"    {ats}/{slug}: DEAD — would delete (dry-run)")
        return

    ok = await delete_row(session, node.STAGING_TABLE, row["id"])
    async with lock:
        counts["dead" if ok else "error"] += 1
    if ok:
        log.info(f"    {ats}/{slug}: DEAD — deleted")


async def verify_archive_iii_row_task(session: aiohttp.ClientSession, row: dict, dry_run: bool,
                                       sem: asyncio.Semaphore, counts: dict, lock: asyncio.Lock) -> None:
    async with sem:
        try:
            is_dead = await verify_archive_iii_row(session, row)
        except Exception as e:
            async with lock:
                counts["error"] += 1
            log.debug(f"    {row['website_url']}: check failed ({e}) — leaving in place")
            return

    if not is_dead:
        async with lock:
            counts["live"] += 1
        return

    if dry_run:
        async with lock:
            counts["dead"] += 1
        log.info(f"    {row['website_url']}: DEAD — would delete (dry-run)")
        return

    ok = await delete_row(session, node.ARCHIVE_III_TABLE, row["id"])
    async with lock:
        counts["dead" if ok else "error"] += 1
    if ok:
        log.info(f"    {row['website_url']}: DEAD — deleted")


async def _run_progress(counts: dict, total: int, label: str):
    last = -1
    while True:
        await asyncio.sleep(5)
        done = sum(counts.values())
        if done != last:
            log.info(f"  [{label}] progress: {done}/{total} "
                     f"(live={counts['live']} dead={counts['dead']} error={counts['error']})")
            last = done
        if done >= total:
            return


def _print_summary(label: str, total: int, skipped: int, counts: dict, dry_run: bool):
    print("\n" + "=" * 70)
    print(f"VERIFICATION SUMMARY — {label}")
    print("=" * 70)
    print(f"  Total rows in table:        {total + skipped}")
    print(f"  Skipped (unverifiable):     {skipped}")
    print(f"  Checked:                    {total}")
    print(f"  Live (kept):                {counts['live']}")
    print(f"  Dead ({'would be ' if dry_run else ''}removed):        {counts['dead']}")
    print(f"  Errors (left in place):     {counts['error']}")
    print("=" * 70)


async def run_archive_ii(ats_filter: str | None, limit: int | None, dry_run: bool,
                          concurrency: int) -> None:
    sem = asyncio.Semaphore(concurrency)
    counts = {"live": 0, "dead": 0, "error": 0}
    lock = asyncio.Lock()

    async with aiohttp.ClientSession(connector=node.new_connector()) as session:
        log.info(f"── Verifying archive_ii{f' ({ats_filter})' if ats_filter else ' (all verifiable platforms)'} ──")
        all_rows = await fetch_rows(session, node.STAGING_TABLE, "id,ats,slug", ats_filter, None)

        if ats_filter and ats_filter in _UNVERIFIABLE_ATS:
            log.warning(f"  '{ats_filter}' has no confirmed-safe not-found signal — nothing to verify, "
                        f"see _UNVERIFIABLE_ATS in this file's module docstring for why.")
            return

        verifiable_rows = [r for r in all_rows if r["ats"] in ARCHIVE_II_VERIFIERS]
        skipped = len(all_rows) - len(verifiable_rows)
        if limit is not None:
            verifiable_rows = verifiable_rows[:limit]
        total = len(verifiable_rows)

        skipped_by_ats = {}
        for r in all_rows:
            if r["ats"] not in ARCHIVE_II_VERIFIERS:
                skipped_by_ats[r["ats"]] = skipped_by_ats.get(r["ats"], 0) + 1
        if skipped_by_ats and not ats_filter:
            log.info(f"  skipping {skipped} rows on unverifiable platforms: "
                     f"{dict(sorted(skipped_by_ats.items(), key=lambda kv: -kv[1]))}")

        log.info(f"  {total} rows to check" + (" (dry-run — nothing will be deleted)" if dry_run else " (EXECUTE MODE — confirmed-dead rows WILL be deleted)"))
        if not total:
            print("\nNo verifiable rows to check.")
            return

        tasks = [verify_archive_ii_row(session, row, dry_run, sem, counts, lock) for row in verifiable_rows]
        progress_task = asyncio.create_task(_run_progress(counts, total, "archive_ii"))
        await asyncio.gather(*tasks)
        progress_task.cancel()

    _print_summary("archive_ii", total, skipped, counts, dry_run)


async def run_archive_iii(limit: int | None, dry_run: bool, concurrency: int) -> None:
    sem = asyncio.Semaphore(concurrency)
    counts = {"live": 0, "dead": 0, "error": 0}
    lock = asyncio.Lock()

    async with aiohttp.ClientSession(connector=node.new_connector()) as session:
        log.info("── Verifying archive_iii (career pages) ──")
        rows = await fetch_rows(session, node.ARCHIVE_III_TABLE, "id,career_page_url,website_url", None, limit)
        total = len(rows)
        log.info(f"  {total} rows to check" + (" (dry-run — nothing will be deleted)" if dry_run else " (EXECUTE MODE — confirmed-dead rows WILL be deleted)"))
        if not total:
            print("\nNo rows to verify for archive_iii.")
            return

        tasks = [verify_archive_iii_row_task(session, row, dry_run, sem, counts, lock) for row in rows]
        progress_task = asyncio.create_task(_run_progress(counts, total, "archive_iii"))
        await asyncio.gather(*tasks)
        progress_task.cancel()

    _print_summary("archive_iii", total, 0, counts, dry_run)


def main():
    parser = argparse.ArgumentParser(
        description="Verify archive_ii/archive_iii rows against live ATS boards/career pages (async). "
                     "Dry-run by default — pass --execute to actually delete confirmed-dead rows.")
    parser.add_argument("--table", choices=["archive_ii", "archive_iii", "both"], default="both")
    parser.add_argument("--ats", default=None,
                         help="archive_ii only — restrict to one ATS platform (e.g. greenhouse)")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--execute", action="store_true",
                         help="Actually delete confirmed-dead rows. Without this, always dry-run.")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    args = parser.parse_args()

    if not node.SUPABASE_URL or not node.SUPABASE_KEY:
        log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot verify.")
        sys.exit(1)

    dry_run = not args.execute

    async def _run_all():
        if args.table in ("archive_ii", "both"):
            await run_archive_ii(args.ats, args.limit, dry_run, args.concurrency)
        if args.table in ("archive_iii", "both"):
            await run_archive_iii(args.limit, dry_run, args.concurrency)

    asyncio.run(_run_all())


if __name__ == "__main__":
    main()
