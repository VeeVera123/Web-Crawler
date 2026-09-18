"""
Prefix — Supabase.

Runs ONCE, before any crawl-i/crawl-ii shard starts (gated with `needs:`
in CI). Named for what it writes TO: it reads the Notion working-set
database's current Status values and writes them into Supabase's
jobs.application_status — see notion_sync.py for the actual logic, this
is just the CI entrypoint.

Running this before the shards start (rather than, say, at the very end)
means any status you set in Notion is recorded in Supabase before this
run's own dedup/classification pass looks at the table — the crawl and
the status reconciliation never race each other.

Usage: python prefix_supabase.py
"""

import logging

import notion_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("prefix_supabase")


def main() -> None:
    log.info("=" * 60)
    log.info("PREFIX — SUPABASE  (Notion statuses -> Supabase)")
    log.info("=" * 60)

    summary = notion_sync.sync_notion_statuses_to_supabase()

    log.info("── Summary ──")
    log.info(f"  {summary['pages_read']} Notion pages read, "
              f"{summary['statuses_synced']} statuses written to Supabase")


if __name__ == "__main__":
    main()
