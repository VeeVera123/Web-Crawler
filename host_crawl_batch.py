"""
Host Crawl — Batch Mode (seed one file -> crawl it fully -> clear -> repeat)
==============================================================================
Runs the whole host-crawl pipeline in file-sized batches instead of one
huge seed + one huge crawl. Each batch:
  1. Seeds host_seed from exactly ONE unprocessed Common Crawl
     Parquet file (host_crawl_seed.py's --one-file mode).
  2. Crawls that queue to exhaustion (host_crawl.py's run(), single
     shard = 100% of the queue since a batch's queue is only one file's
     worth of hosts, not the millions a full unsharded run would see).
  3. Wipes host_seed and host_visited back to empty —
     ONLY host_slug (the actual ats/slug hits) survives between
     batches. host_file also survives, so a re-run of this
     script (or a plain host_crawl_seed.py run later) still knows which
     files are already done.
  4. Repeats for --batches files total (default 30 — a full crawl
     partition's worth), or until no unprocessed file remains.

WHY: a single Common Crawl Parquet file can contain 1.6M+ matching hosts
(confirmed on a live run) — seeding/crawling/storing all 30 files' worth
at once would blow past Supabase's free-tier storage many times over (see
BULK_DOMAIN_DISCOVERY_NOTES.md's storage math). Since host_seed and
host_visited are pure scratch state (nothing worth keeping — the
actual product is host_slug), there's no reason to keep them
around after a batch is fully crawled. This keeps Supabase usage bounded
to "one file's worth" at any given moment, indefinitely, regardless of
how many batches you run in total.

This is meant to be run manually, one invocation at a time or with
--batches N for N in a row — NOT wired into a GitHub Actions schedule,
since each batch's crawl step can take a while (however long it takes to
visit ~1-2M hosts) and this script's whole design assumes someone is
available to watch/restart it if needed, same as how seeding/crawling
were being run manually before this.

Usage:
    python host_crawl_batch.py                  # do ONE batch (one file), then stop
    python host_crawl_batch.py --batches 30      # do up to 30 batches in a row
    python host_crawl_batch.py --batches 5 --max-runtime-per-batch 1800
    python host_crawl_batch.py --dry-run         # seed+crawl in dry-run, no writes/clears
"""

import argparse
import logging
import os
import sys

import requests
from dotenv import load_dotenv

import host_crawl_seed
import host_crawl

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# Generous default per-batch crawl budget — one file's hosts (hundreds of
# thousands to low millions) at host_crawl.py's own concurrency/batch
# settings. Override with --max-runtime-per-batch for a quicker manual
# test of the whole batch cycle before committing to a long run.
DEFAULT_MAX_RUNTIME_PER_BATCH = 5.5 * 3600

# How many hosts to seed PER BATCH (i.e. per file). A live run showed a
# SINGLE Common Crawl file can contain 1.66M+ matching hosts — writing all
# of that to host_seed in one shot is ~370MB, which alone nearly
# filled Supabase's 500MB free tier and made further writes start failing
# with 500 errors mid-batch. This cap keeps each batch's footprint small
# and predictable regardless of how large the underlying file is — the
# leftover, untrimmed part of an oversized file is automatically picked
# up by a LATER batch (host_crawl_seed.py's --one-file mode leaves a
# trimmed file unmarked in host_file specifically so this
# works). Override with --seed-limit-per-batch if you want a different
# size (e.g. larger once you've moved to a bigger-capacity DB like Turso).
DEFAULT_SEED_LIMIT_PER_BATCH = 100_000


def _clear_queue_and_visited(dry_run: bool) -> tuple[int, int]:
    """Wipe host_seed and host_visited back to empty.
    host_slug and host_file are NEVER touched —
    those are the two tables meant to survive across batches."""
    if dry_run:
        log.info("Dry run — not clearing host_seed/host_visited.")
        return (0, 0)
    if not (SUPABASE_URL and SUPABASE_KEY):
        log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot clear tables.")
        return (0, 0)

    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    cleared = {}
    for table, pk_col in [("host_seed", "host"), ("host_visited", "host")]:
        try:
            # PostgREST requires a filter on DELETE — "not equal to
            # impossible value" deletes everything without needing to
            # know the real key values up front.
            r = requests.delete(
                f"{SUPABASE_URL}/rest/v1/{table}",
                headers=headers,
                params={pk_col: "neq.__never_matches__"},
                timeout=60,
            )
            r.raise_for_status()
            cleared[table] = True
        except Exception as e:
            log.error(f"Failed to clear {table}: {e}")
            cleared[table] = False

    return cleared.get("host_seed", False), cleared.get("host_visited", False)


def _get_results_count() -> int:
    if not (SUPABASE_URL and SUPABASE_KEY):
        return -1
    try:
        r = requests.head(
            f"{SUPABASE_URL}/rest/v1/host_slug",
            headers={
                "apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                "Prefer": "count=exact",
            },
            params={"select": "id"},
            timeout=30,
        )
        content_range = r.headers.get("content-range", "")
        # format: "0-24/137" -> total is after the slash
        if "/" in content_range:
            return int(content_range.split("/")[-1])
    except Exception as e:
        log.warning(f"Could not fetch host_slug count: {e}")
    return -1


def run_batches(num_batches: int, max_runtime_per_batch: float,
                 crawl: str | None, dry_run: bool,
                 seed_limit_per_batch: int = DEFAULT_SEED_LIMIT_PER_BATCH) -> int:
    total_found_this_session = 0
    results_before = _get_results_count()

    for batch_num in range(1, num_batches + 1):
        log.info(f"\n{'='*70}\nBATCH {batch_num}/{num_batches}\n{'='*70}")

        # 1. Seed from exactly one unprocessed file, capped at
        #    seed_limit_per_batch (a single file can contain 1M+ matches
        #    on its own — see DEFAULT_SEED_LIMIT_PER_BATCH's comment for
        #    why this cap exists).
        log.info(f"Step 1/3: seeding up to {seed_limit_per_batch:,} hosts "
                 f"from one file...")
        seeded = host_crawl_seed.seed(dry_run=dry_run, crawl=crawl,
                                      one_file=True, limit=seed_limit_per_batch)
        if seeded == 0:
            log.info("No hosts seeded — either every file is already "
                     "processed, or seeding failed. Stopping.")
            break
        log.info(f"Seeded {seeded} hosts for this batch.")

        # 2. Crawl the whole queue (single shard = 100%, since this
        #    batch's queue is only one file's worth, not the full
        #    unsharded dataset host_crawl.yml's 16-shard matrix is for).
        log.info("Step 2/3: crawling this batch's queue to exhaustion...")
        host_crawl.run(shard=0, total_shards=1,
                       max_runtime=max_runtime_per_batch, dry_run=dry_run)

        # 3. Clear queue + visited — only host_slug survives.
        log.info("Step 3/3: clearing host_seed/host_visited "
                 "(host_slug is kept)...")
        q_ok, v_ok = _clear_queue_and_visited(dry_run)
        if not dry_run and not (q_ok and v_ok):
            log.error("Clearing failed for at least one table — stopping "
                      "here rather than seeding a NEW file on top of "
                      "leftover rows from this one.")
            break

        results_now = _get_results_count()
        if results_before >= 0 and results_now >= 0:
            this_batch_found = results_now - results_before - total_found_this_session
            total_found_this_session = results_now - results_before
            log.info(f"Batch {batch_num} complete — {this_batch_found} new ATS "
                     f"hits this batch, {total_found_this_session} total this "
                     f"session, {results_now} all-time in host_slug.")

    log.info(f"\nAll requested batches done — {total_found_this_session} new "
             f"ATS hits found this session.")
    return total_found_this_session


def main():
    parser = argparse.ArgumentParser(
        description="Seed-crawl-clear one Common Crawl Parquet file at a "
                    "time, keeping only host_slug between batches."
    )
    parser.add_argument("--batches", type=int, default=1,
                         help="How many files to process in a row (default: 1 "
                              "— just the next unprocessed file). A full crawl "
                              "partition is 30 files, so --batches 30 processes "
                              "all of them if none are done yet.")
    parser.add_argument("--max-runtime-per-batch", type=float,
                         default=DEFAULT_MAX_RUNTIME_PER_BATCH,
                         help=f"Max seconds host_crawl.py's crawl step runs per "
                              f"batch before moving on regardless (default: "
                              f"{DEFAULT_MAX_RUNTIME_PER_BATCH:.0f}s / "
                              f"{DEFAULT_MAX_RUNTIME_PER_BATCH/3600:.1f}h). Lower "
                              f"this for a quick test of the whole batch cycle.")
    parser.add_argument("--crawl", type=str, default=None,
                         help="Pin a specific Common Crawl partition to seed "
                              "from (default: auto-detect latest).")
    parser.add_argument("--dry-run", action="store_true",
                         help="Seed and crawl in dry-run (no Supabase writes), "
                              "and skip clearing — for testing the cycle logic "
                              "without touching real data.")
    parser.add_argument("--seed-limit-per-batch", type=int,
                         default=DEFAULT_SEED_LIMIT_PER_BATCH,
                         help=f"Max hosts seeded PER BATCH/file (default: "
                              f"{DEFAULT_SEED_LIMIT_PER_BATCH:,}). A single Common "
                              f"Crawl file can contain 1M+ matches on its own — "
                              f"this cap keeps each batch's Supabase footprint "
                              f"small and predictable regardless of file size. "
                              f"The untrimmed remainder of an oversized file is "
                              f"automatically picked up by a later batch.")
    args = parser.parse_args()

    found = run_batches(args.batches, args.max_runtime_per_batch,
                        args.crawl, args.dry_run,
                        seed_limit_per_batch=args.seed_limit_per_batch)
    if found == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
