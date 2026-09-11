"""Reclassify archive_ii — 2026-09.

archive_ii holds career pages that, at the time they were captured, had NO
known ATS anywhere on them (see node.py's crawl_one docstring). Two things
can make that stale without the row ever being re-crawled from scratch:

  1. node.py's own detection logic gets a bug fix (a new ATS URL pattern
     recognized, a slug-extraction bug fixed) AFTER a row was captured —
     the row's actual page may have always belonged to a known ATS, node.py
     just couldn't see it yet.
  2. The "follow a career page's own click-through link" feature (2026-09)
     did not exist when older archive_ii rows were captured — a page that
     looked like a genuine in-house dead end back then may in fact link
     straight to a real ATS one hop away (the exact Selective/iCIMS case
     that motivated this script).

This script re-visits every archive_ii row's career_page_url through the
EXACT SAME trusted detection path crawl_one uses (node.detect_page_hits +
node._follow_career_listing_links — no vendored/duplicated logic) and does
exactly one of three things per row:

  - PROMOTE: a known ATS is found (directly on the page, or one hop away
    via the follow-step) -> write the hit(s) to archive_i
    (discovery_method="archive_ii", already a permitted value in
    archive_i's source CHECK constraint — confirmed live, no migration
    needed) and delete the now-redundant row from archive_ii.
  - UPDATE: still no known ATS, but the follow-step turns up a longer/
    better-qualified in-house page than the one currently on file -> the
    archive_ii row's career_page_url is updated in place (same upsert-on-
    website_url path write_career_pages_to_archive_ii already uses).
  - LEAVE ALONE: page unreachable, or nothing better found than what's
    already on file -> no write at all.

Sharded exactly like crawl_ii.py (server-side archive_ii_shard RPC via
get_archive_ii_pages, same --shard/--total-shards flags) since it reads
from the same table crawl_ii.py does. This is a standalone, manually-run
cleanup pass, not wired into any GitHub Actions workflow yet — the
question of whether it should become one is a separate decision from
building it.
"""
import argparse
import asyncio
import concurrent.futures
import logging
import os
import sys
import time
from urllib.parse import urlparse

import aiohttp
from dotenv import load_dotenv

load_dotenv()
_MAIN_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_MAIN_DIR)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _MAIN_DIR)

import node  # noqa: E402 — reuse detect_page_hits, _follow_career_listing_links,
             # _best_inhouse_candidate, _fetch_page, _collapse_hits, new_connector,
             # new_parse_pool, write_ats_hits_to_archive_i, write_career_pages_to_archive_ii
from supabase_handler import (  # noqa: E402
    get_archive_ii_pages, delete_archive_ii_rows, SupabaseFetchError,
    log_egress_summary,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("reclassify_archive_ii")

CONCURRENCY = int(os.environ.get("RECLASSIFY_CONCURRENCY", "60"))
TIME_BUDGET_MINUTES = int(os.environ.get("RECLASSIFY_TIME_BUDGET_MINUTES", "300"))


async def reclassify_row(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                          parse_pool: concurrent.futures.Executor, row: dict,
                          stats: dict) -> dict | None:
    """Visits one archive_ii row's career_page_url and decides its fate.
    Returns None (leave alone) or a dict describing the write to make:
    {"action": "promote", "website_url", "hit_rows": [(ats, slug, matched_url), ...]}
    {"action": "update", "website_url", "career_page_url"}
    Never raises — a dead page, timeout, or parse failure is just another
    "leave alone", same as every other tier in node.py's crawl_one."""
    async with sem:
        stats["rows_attempted"] += 1
        career_url = row.get("career_page_url")
        website_url = row.get("website_url")
        if not career_url or not website_url:
            stats["rows_skipped_bad_data"] += 1
            return None

        page = await node._fetch_page(session, career_url, stats)
        if not page:
            stats["page_unreachable"] += 1
            return None
        final_url, html = page

        async def _detect(html_, url_):
            hits, _country, _method, text_len, has_hiring_vocab = await node.detect_page_hits(
                session, parse_pool, html_, url_, node.ACCEPT_ANY_COUNTRY, stats)
            return hits, text_len, has_hiring_vocab

        hits, text_len, has_hiring_vocab = await _detect(html, final_url)
        if hits:
            stats["promoted_direct"] += 1
            return {"action": "promote", "website_url": website_url,
                    "hit_rows": list(hits)}

        already_fetched = {final_url}
        origin_parts = urlparse(final_url)
        origin = f"{origin_parts.scheme}://{origin_parts.netloc}"
        candidate = {"url": final_url, "html": html, "hits": [],
                     "text_len": text_len, "has_hiring_vocab": has_hiring_vocab}
        follow_results = await node._follow_career_listing_links(
            _detect, session, [candidate], already_fetched, stats)
        merged = node._collapse_hits([c["hits"] for c in follow_results])
        if merged:
            stats["promoted_via_follow"] += 1
            return {"action": "promote", "website_url": website_url,
                    "hit_rows": list(merged)}

        best_inhouse = node._best_inhouse_candidate([candidate] + follow_results, origin)
        if best_inhouse and best_inhouse["url"] not in (final_url, career_url):
            stats["updated_inhouse"] += 1
            return {"action": "update", "website_url": website_url,
                    "career_page_url": best_inhouse["url"]}

        stats["kept_unchanged"] += 1
        return None


async def _run_shard(shard: int, total_shards: int) -> None:
    log.info("=" * 60)
    log.info(f"RECLASSIFY ARCHIVE_II — starting (shard {shard}/{total_shards})")
    log.info("=" * 60)

    try:
        rows = get_archive_ii_pages(shard_index=shard, shard_count=total_shards)
    except SupabaseFetchError as e:
        log.error(f"Failed to load archive_ii pages from Supabase after retries — aborting shard: {e}")
        sys.exit(1)
    log.info(f"  {len(rows)} archive_ii pages assigned to this shard")

    if not rows:
        log.warning(f"Shard {shard}/{total_shards}: no pages assigned, nothing to do.")
        return

    stats = node.Counter()
    sem = asyncio.Semaphore(CONCURRENCY)
    connector = node.new_connector()
    parse_pool = node.new_parse_pool()
    start = time.monotonic()
    time_budget_seconds = TIME_BUDGET_MINUTES * 60

    promote_hit_rows = []       # flattened (ats, slug, matched_url, website_url) for logging
    promote_website_urls = set()
    update_rows = []            # {"career_page_url","website_url","discovery_method"}

    try:
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
            # Same time-budget pattern crawl_ii.py uses: process in
            # batches so a long shard can still stop early and keep
            # whatever it already decided, rather than losing everything
            # to a hard timeout mid-gather.
            BATCH = 200
            for i in range(0, len(rows), BATCH):
                if time.monotonic() - start > time_budget_seconds:
                    log.warning(f"Time budget ({TIME_BUDGET_MINUTES} min) reached — "
                                f"stopping early at {i}/{len(rows)} rows.")
                    break
                batch = rows[i:i + BATCH]
                results = await asyncio.gather(
                    *(reclassify_row(session, sem, parse_pool, r, stats) for r in batch))
                for decision in results:
                    if not decision:
                        continue
                    if decision["action"] == "promote":
                        promote_website_urls.add(decision["website_url"])
                        for ats, slug, matched_url in decision["hit_rows"]:
                            promote_hit_rows.append({
                                "ats": ats, "slug": slug,
                                "discovery_method": "archive_ii",
                            })
                    elif decision["action"] == "update":
                        update_rows.append({
                            "career_page_url": decision["career_page_url"],
                            "website_url": decision["website_url"],
                        })
                log.info(f"  ...{min(i + BATCH, len(rows))}/{len(rows)} rows checked "
                         f"({len(promote_website_urls)} to promote, {len(update_rows)} to update so far)")

            # 2026-09 fix: dedupe by each write's own on_conflict key before
            # sending. A single upsert command touching the same conflict
            # key twice is a hard Postgres error ("ON CONFLICT DO UPDATE
            # command cannot affect row a second time", 21000), not a
            # per-row failure — it kills the WHOLE chunk. Unlike a normal
            # crawl_one domain crawl (one company at a time, already
            # internally deduped via _collapse_hits), this script processes
            # thousands of INDEPENDENT archive_ii rows in one shard, so two
            # different rows landing on the same (ats, slug) — e.g. two
            # stray archive_ii entries for the same company under slightly
            # different URLs — or, in principle, the same website_url
            # appearing twice, is a real possibility here that the shared
            # write helpers were never built to defend against. Confirmed
            # live: this exact class of error is what caused shard 12/20's
            # entire archive_i AND archive_ii write to fail after a
            # otherwise-successful 6,031-row run.
            if len(promote_hit_rows) != len({(r["ats"], r["slug"]) for r in promote_hit_rows}):
                before = len(promote_hit_rows)
                promote_hit_rows = list({(r["ats"], r["slug"]): r for r in promote_hit_rows}.values())
                log.warning(f"  {before - len(promote_hit_rows)} duplicate (ats, slug) hit(s) collapsed "
                            f"before writing to archive_i (same slug found via >1 archive_ii row)")
            if len(update_rows) != len({r["website_url"] for r in update_rows}):
                before = len(update_rows)
                update_rows = list({r["website_url"]: r for r in update_rows}.values())
                log.warning(f"  {before - len(update_rows)} duplicate website_url update(s) collapsed "
                            f"before writing to archive_ii")

            written_archive_i = 0
            if promote_hit_rows:
                written_archive_i = await node.write_ats_hits_to_archive_i(session, promote_hit_rows)
                log.info(f"  → {written_archive_i}/{len(promote_hit_rows)} hit rows written to archive_i")
                # Conservative on purpose: only delete an archive_ii row
                # once we're confident its promotion actually landed.
                # write_ats_hits_to_archive_i's return value is a
                # chunk-level success COUNT, not a per-row report, so a
                # partial failure here can't be mapped back to which
                # specific website_urls are actually safe to delete —
                # skipping the whole deletion pass on any shortfall trades
                # a little archive_ii cleanup for never dropping a genuine
                # archive_i candidate whose write may not have landed.
                if written_archive_i >= len(promote_hit_rows):
                    deleted = delete_archive_ii_rows(promote_website_urls)
                    log.info(f"  → {deleted}/{len(promote_website_urls)} promoted rows removed from archive_ii")
                else:
                    log.warning(
                        f"  archive_i write short by {len(promote_hit_rows) - written_archive_i} row(s) — "
                        f"skipping archive_ii deletion this run so nothing promoted is lost; "
                        f"these rows will simply be re-checked next run.")

            written_archive_ii = 0
            if update_rows:
                written_archive_ii = await node.write_career_pages_to_archive_ii(session, update_rows)
                log.info(f"  → {written_archive_ii}/{len(update_rows)} archive_ii rows updated with a better in-house page")
    finally:
        parse_pool.shutdown(wait=False)

    log.info("── Summary ──")
    log.info(f"  Rows checked: {stats['rows_attempted']} attempted, {stats['page_unreachable']} unreachable")
    log.info(f"  Promoted to archive_i: {stats['promoted_direct']} direct hits, "
             f"{stats['promoted_via_follow']} via follow-link")
    log.info(f"  Updated in archive_ii: {stats['updated_inhouse']}")
    log.info(f"  Left unchanged: {stats['kept_unchanged']}")
    log.info(f"  Career-link follow: {stats['career_link_follow_attempted']} links followed, "
             f"{stats['career_link_follow_ats_hit']} of those hit a known ATS")

    log_egress_summary(label=f"reclassify_archive_ii shard {shard}/{total_shards}")


def main():
    parser = argparse.ArgumentParser(
        description="Reclassify archive_ii — re-run node.py's detection on already-captured "
                     "in-house career pages and promote/update/leave each one alone")
    parser.add_argument("--shard", type=int, default=0,
                         help="This shard's index (0-based), for GitHub Actions matrix parallelism")
    parser.add_argument("--total-shards", type=int, default=1,
                         help="Total number of shards; each processes ~1/N of archive_ii")
    args = parser.parse_args()

    asyncio.run(_run_shard(args.shard, args.total_shards))


if __name__ == "__main__":
    main()
