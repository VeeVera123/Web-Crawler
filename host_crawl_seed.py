"""
Host Crawl — Seed Step
=======================
Populates host_crawl_queue with candidate hostnames from Common Crawl's
COLUMNAR INDEX (the URL-level Parquet index of every page CC has crawled —
a different dataset from the Host Index this file used before).

RETHOUGHT 2026-08: the Host Index approach was abandoned because
data.commoncrawl.org (the only free, no-AWS-account HTTPS distribution
point) blanket-disallows every crawler in its robots.txt
("User-Agent: * / Disallow: /" — confirmed live, not a sandbox artifact),
and the Host Index has no other free access path (S3 access is documented
by Common Crawl as free "from inside AWS", not from an arbitrary CI
runner, and needs real AWS credentials either way).

The columnar index is different: Common Crawl's own docs state it plainly
— "Crawl data is free to access by anyone from anywhere... There is no
need to create an AWS account... The argument --no-sign-request allows
for anonymous access." That's the actual S3 API (a different protocol
from HTTP), which robots.txt has no jurisdiction over in the first place
— robots.txt only governs HTTP crawling of data.commoncrawl.org's web
paths, never AWS S3 API calls against s3://commoncrawl. So this reads
Parquet directly from s3://commoncrawl via DuckDB's anonymous/unsigned S3
support — no account, no login, no cost, no robots.txt question at all.

Trade-off versus the old Host Index approach: this is a SAMPLE of the web
(whatever Common Crawl's crawler actually fetched and linked-to), not an
exhaustive domain registry — but for finding companies with a live
careers page and ATS integration, that's arguably the right bias: a
site that's actually linked-to and gets crawled is far more likely to be
a real, active company site than a random registered-but-parked domain,
which is exactly the kind of noise an exhaustive zone-file approach would
force the crawl step to wade through for free.

TLDs widened 2026-08 per explicit instruction to not stop at .com: now
covers every ccTLD/generic TLD in TARGET_TLDS (.com, .net, .us, .uk,
.ca, .de, .au, .ie, .mt, plus generic .io/.co/.app/.dev) in ONE pass,
since the columnar index's WHERE clause can filter on url_host_tld
directly — no need for a separate run per TLD.

Usage:
    python host_crawl_seed.py                       # seed from latest crawl
    python host_crawl_seed.py --dry-run             # count without writing
    python host_crawl_seed.py --limit 500000        # cap rows written (testing)
    python host_crawl_seed.py --crawl CC-MAIN-2026-30   # pin a specific crawl
"""

import argparse
import logging
import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# Common Crawl's columnar index — S3 API access only (never the HTTPS
# mirror at data.commoncrawl.org, which robots.txt blanket-disallows).
# Public, anonymous, no AWS account — see module docstring.
CC_S3_BUCKET = "commoncrawl"
CC_S3_REGION = "us-east-1"
CC_INDEX_BASE = "cc-index/table/cc-main/warc"

# Crawls are named CC-MAIN-YYYY-WW (year + ISO week of the crawl). Common
# Crawl publishes roughly one per month. This is used only as a fallback
# if we can't confirm the latest crawl name live (see
# _get_latest_crawl_name) — kept current-ish but NOT trusted blindly, so
# a stale hardcoded value here never silently queries a nonexistent or
# very old partition.
_FALLBACK_CRAWL = "CC-MAIN-2026-30"

# ── TLD allowlist ────────────────────────────────────────
# Per-country ccTLDs for the target markets, PLUS the generic/startup
# TLDs (.com, .io, .co, .app, .dev) that skew heavily toward exactly
# these markets but aren't attributable to any single country. This is a
# SOFT geography proxy, not a hard filter on company location — .com in
# particular is the default for US companies and plenty of others, and
# .io/.co/.app/.dev are globally used by tech startups regardless of
# country. Deliberately inclusive: better to crawl some non-target-market
# .com sites than to miss real target-market companies that happen to use
# a generic TLD. ccTLDs for uninvolved countries (.ng, .in, .br, etc.) are
# excluded — those are the ones actually worth skipping, since a company
# using a country-specific ccTLD for a country we're not targeting is a
# genuinely strong signal it's not relevant here.
#
# "net" added explicitly 2026-08 (previously only implied by "generic
# TLDs" prose, never actually in the set) — plenty of real companies,
# especially older ones, run their main site or careers subdomain off
# .net rather than .com.
TARGET_TLDS = {
    # Country-specific
    "us", "uk", "ca", "de", "au", "ie", "mt",
    # Generic/global — not attributable to one country, kept for recall
    "com", "net", "io", "co", "app", "dev",
}
# NOTE: the columnar index's url_host_tld field is the PUBLIC SUFFIX
# (e.g. "co.uk", "com.au"), not always the bare final label — unlike the
# old Host Index's url_host_tld, which used the same convention, this is
# unchanged. Both "uk" (bare) and the registrable-domain suffix matter;
# see _build_tld_filter() below for how both are matched safely.
TARGET_SUFFIXES_EXTRA = {"co.uk", "com.au"}

# ── Liveness filter ──────────────────────────────────────
# The columnar index only carries ONE fetch_status per row (this row IS
# one successful or failed fetch of one URL — unlike the Host Index's
# aggregated per-host counts), so "dead" here is judged per-domain by
# whether the domain has ANY row with fetch_status == 200 in the crawl(s)
# queried. A domain with zero 200 rows in a given monthly crawl might
# still be alive (CC just didn't refetch it that month) — this filter is
# applied SQL-side by simply requiring fetch_status = 200, which is a
# strictly more conservative/inclusive stance than trying to prove a
# domain dead: we only ever ADD domains that were successfully fetched,
# never explicitly exclude ones seen failing, so there's no dead-domain
# false-negative risk introduced here at all (contrast the old Host
# Index's _looks_dead(), which actively excluded some hosts — this
# version doesn't need that logic since we only harvest positive hits).


def _build_tld_filter() -> str:
    parts = [f"'{t}'" for t in TARGET_TLDS] + [f"'{t}'" for t in TARGET_SUFFIXES_EXTRA]
    return ",".join(parts)


def _robots_allows_https_mirror() -> bool:
    """Kept ONLY as a guardrail: if anything in this module is ever
    changed to also read from https://data.commoncrawl.org (the HTTPS
    mirror), it must check this first. Currently unused by the S3-only
    seed() path below — data.commoncrawl.org/robots.txt disallows "/"
    entirely, so this will always return False, which is correct."""
    try:
        r = requests.get("https://data.commoncrawl.org/robots.txt", timeout=30)
        if r.status_code >= 400:
            return False
        import urllib.robotparser
        rp = urllib.robotparser.RobotFileParser()
        rp.parse(r.text.splitlines())
        return rp.can_fetch("ATS-Global-Scanner/1.0", "/cc-index/table/cc-main/warc/")
    except Exception:
        return False


def _get_latest_crawl_name(con) -> str | None:
    """Ask S3 itself (via DuckDB's glob) which crawl= partitions actually
    exist, rather than trust a hardcoded crawl name that goes stale the
    moment Common Crawl publishes a new monthly crawl. Falls back to
    _FALLBACK_CRAWL (logged loudly) only if the listing call itself
    fails — never silently guesses past that."""
    try:
        rows = con.execute(
            f"SELECT DISTINCT regexp_extract(file, 'crawl=([^/]+)', 1) AS crawl "
            f"FROM glob('s3://{CC_S3_BUCKET}/{CC_INDEX_BASE}/crawl=*/subset=warc/') "
            f"ORDER BY crawl DESC LIMIT 5"
        ).fetchall()
        names = [r[0] for r in rows if r[0]]
        if names:
            log.info(f"Available crawl partitions (most recent 5): {names}")
            return names[0]
    except Exception as e:
        log.warning(f"Could not list crawl partitions from S3: {e} — "
                    f"falling back to hardcoded {_FALLBACK_CRAWL!r} (may be stale).")
    return _FALLBACK_CRAWL


def seed(limit: int | None = None, dry_run: bool = False,
         crawl: str | None = None, months: int = 1) -> int:
    try:
        import duckdb
    except ImportError:
        log.error("duckdb not installed — pip install duckdb to run this step.")
        return 0

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # Anonymous/unsigned S3 access: no CREATE SECRET call at all — DuckDB
    # sends unsigned requests when no credentials are configured, which
    # is exactly what a public bucket like commoncrawl needs. Region must
    # still be set explicitly since it isn't auto-discoverable without a
    # credential chain.
    con.execute(f"SET s3_region='{CC_S3_REGION}';")

    crawls: list[str] = []
    if crawl:
        crawls = [crawl]
    else:
        latest = _get_latest_crawl_name(con)
        if not latest:
            log.error("Could not determine any crawl partition — aborting seed.")
            return 0
        if months <= 1:
            crawls = [latest]
        else:
            # Best-effort: derive prior crawl names by walking the actual
            # partition listing again (already fetched above) rather than
            # guessing month arithmetic on the YYYY-WW crawl-name format,
            # which doesn't decrement predictably.
            try:
                rows = con.execute(
                    f"SELECT DISTINCT regexp_extract(file, 'crawl=([^/]+)', 1) AS crawl "
                    f"FROM glob('s3://{CC_S3_BUCKET}/{CC_INDEX_BASE}/crawl=*/subset=warc/') "
                    f"ORDER BY crawl DESC LIMIT {int(months)}"
                ).fetchall()
                crawls = [r[0] for r in rows if r[0]] or [latest]
            except Exception:
                crawls = [latest]

    log.info(f"Seeding from crawl partition(s): {crawls}")

    tld_filter = _build_tld_filter()
    crawl_filter = ",".join(f"'{c}'" for c in crawls)

    # Column-pruned, partition-pruned query: only the columns we need,
    # only the crawl partitions we asked for, only rows that both match
    # our TLD allowlist AND were successfully fetched (fetch_status=200)
    # — see the liveness-filter note above for why 200-only is safe here.
    query = f"""
        SELECT DISTINCT
            url_host_registered_domain,
            url_host_tld
        FROM read_parquet(
            's3://{CC_S3_BUCKET}/{CC_INDEX_BASE}/crawl=*/subset=warc/*.parquet',
            hive_partitioning=1
        )
        WHERE crawl IN ({crawl_filter})
          AND url_host_tld IN ({tld_filter})
          AND fetch_status = 200
          AND url_host_registered_domain IS NOT NULL
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    log.info("Querying Common Crawl's columnar index via anonymous S3 "
             "(this can take a while — scanning real Parquet partitions)...")
    try:
        result = con.execute(query).fetchall()
    except Exception as e:
        log.error(f"DuckDB query against s3://{CC_S3_BUCKET} failed: {e} — "
                  f"if this is an auth/permission error, anonymous S3 access "
                  f"may need an explicit unsigned-request DuckDB secret for "
                  f"the DuckDB version in use; see module docstring.")
        return 0

    log.info(f"Columnar index: {len(result)} distinct registered domains "
             f"matched target TLDs with a successful fetch.")

    rows_to_insert = []
    seen = set()
    for domain, tld in result:
        if not domain or domain in seen:
            continue
        seen.add(domain)
        rows_to_insert.append({
            "host": domain,
            "tld": tld,
            "hcrank": None,  # columnar index has no ranking field (that
                              # was Host-Index-only) — left null, queue
                              # ordering falls back to insertion order.
        })

    log.info(f"Seed candidates: {len(rows_to_insert)} distinct domains")

    if dry_run:
        log.info("Dry run — not writing to Supabase.")
        return len(rows_to_insert)

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot write queue.")
        return 0

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates",
    }
    batch_size = 5000
    written = 0
    for i in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[i:i + batch_size]
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/host_crawl_queue",
                headers=headers, json=batch, timeout=60,
                # on_conflict required alongside the ignore-duplicates
                # Prefer header — PostgREST needs to know which column
                # defines a duplicate (host_crawl_queue's PK is `host`).
                params={"on_conflict": "host"},
            )
            r.raise_for_status()
            written += len(batch)
        except Exception as e:
            log.error(f"Failed to write batch {i}-{i+len(batch)}: {e}")
        if (i // batch_size) % 20 == 0:
            log.info(f"  ...{written}/{len(rows_to_insert)} written so far")

    log.info(f"Seed complete: {written} hosts written to host_crawl_queue.")
    return written


def main():
    parser = argparse.ArgumentParser(
        description="Seed host_crawl_queue from Common Crawl's columnar index (anonymous S3)"
    )
    parser.add_argument("--limit", type=int, default=None,
                         help="Cap total rows read from the index (testing)")
    parser.add_argument("--dry-run", action="store_true",
                         help="Count without writing to Supabase")
    parser.add_argument("--crawl", type=str, default=None,
                         help="Pin a specific crawl partition, e.g. CC-MAIN-2026-30 "
                              "(default: auto-detect latest available)")
    parser.add_argument("--months", type=int, default=1,
                         help="Number of recent monthly crawl partitions to scan "
                              "(default: 1 — more months = more coverage, more scan cost/time)")
    args = parser.parse_args()

    written = seed(limit=args.limit, dry_run=args.dry_run,
                   crawl=args.crawl, months=args.months)
    if written == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
