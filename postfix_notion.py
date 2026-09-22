"""
Postfix — Notion.

Runs ONCE, after every crawl-i AND crawl-ii shard has finished (gated with
`needs: [crawl-i, crawl-ii]` + `if: always()` in CI, same reasoning as the
old crawl_i.py/crawl_ii.py `--finalize` split this replaces). Named for
what it writes TO: it takes whatever's new in Supabase and pushes it to
Notion, then runs the regular stale-job cleanup.

Two steps, done in this order, each its own clearly-marked section in the
log:

  1. Push every Supabase job that's never been mirrored to Notion yet
     (jobs.notion_synced_at IS NULL — see supabase_handler.
     get_jobs_pending_notion_sync()) to the Notion working-set database.
     Covers new rows from BOTH pipelines in one pass — no need to know
     which pipeline inserted which row, and safe to run any number of
     times a day (a row is only ever offered here once).

  2. The regular stale-job cleanup — previously `crawl_i.py --finalize`
     and `crawl_ii.py --finalize`, run as two separate CI jobs. Merged
     here since this step already runs after both pipelines are done.
     Crawl II's cleanup only runs when Crawl II itself ran this cycle
     (--run-crawl-ii-cleanup), preserving the exact same "Crawl II never
     runs unattended" behavior the old crawl-ii-finalize job's `if:`
     condition enforced — merging the two jobs must not, by itself,
     start cleaning up Crawl II's rows on days Crawl II never scanned
     them.

  3. Crawl III's cleanup (2026-09) — same conditional-on-having-run
     pattern as Crawl II, via --run-crawl-iii-cleanup. crawl_iii.
     run_finalize() does its own two-step job internally (archive stale
     Notion pages FIRST, then hard-delete the same-day-stale Supabase
     rows — see that function's docstring), so this file just calls it;
     no separate Notion-archiving step is needed here.

Usage: python postfix_notion.py [--run-crawl-ii-cleanup] [--run-crawl-iii-cleanup]
"""

import argparse
import logging

import notion_sync
import crawl_i
import crawl_ii
import crawl_iii

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("postfix_notion")


def main() -> None:
    parser = argparse.ArgumentParser(description="Postfix — Notion")
    parser.add_argument("--run-crawl-ii-cleanup", action="store_true",
                         help="Also run Crawl II's stale-job cleanup — pass this only on "
                              "runs where Crawl II's shards actually ran this cycle")
    parser.add_argument("--run-crawl-iii-cleanup", action="store_true",
                         help="Also run Crawl III's stale-job cleanup (archives stale Notion "
                              "pages, then hard-deletes same-day-stale rows) — pass this only "
                              "on runs where Crawl III's shards actually ran this cycle")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("POSTFIX — NOTION")
    log.info("=" * 60)

    log.info("── Step 1: pushing new Supabase jobs to Notion ──")
    push_summary = notion_sync.push_pending_jobs_to_notion()
    log.info(f"  {push_summary['created']}/{push_summary['attempted']} "
              f"new jobs created in Notion")

    log.info("── Step 2: Crawl I cleanup ──")
    crawl_i.run_finalize()

    if args.run_crawl_ii_cleanup:
        log.info("── Step 3: Crawl II cleanup ──")
        crawl_ii.run_finalize()
    else:
        log.info("── Step 3: Crawl II cleanup — skipped (Crawl II did not run this cycle) ──")

    if args.run_crawl_iii_cleanup:
        log.info("── Step 4: Crawl III cleanup ──")
        crawl_iii.run_finalize()
    else:
        log.info("── Step 4: Crawl III cleanup — skipped (Crawl III did not run this cycle) ──")

    log.info("=" * 60)
    log.info("POSTFIX — NOTION complete")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
