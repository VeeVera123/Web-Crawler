"""
Host Crawl — Seed Step
=======================
Populates h_seeding with candidate hostnames from Common Crawl's
HOST INDEX, re-hosted by Common Crawl's own official org on Hugging Face
(huggingface.co/datasets/commoncrawl/host-index-testing-v2), queried
directly via DuckDB's native `hf://` support — no AWS account, no
billing, no robots.txt wall, genuinely free.

NOTE (2026-08 merge): h_seeding used to be two separate tables (one for
the not-yet-visited queue, one for crawl outcomes) — since by the end of
any batch every row is in exactly one of those two states, keeping them
apart was pure duplication of the same hosts. They're now one table: a
row starts with outcome IS NULL (queued, written by THIS script) and
host_crawl.py's _flush_results() later fills in outcome/ats/slug/
checked_at in place on the same row once it's actually been visited.

FULL HISTORY (why this is v5 — see BULK_DOMAIN_DISCOVERY_NOTES.md for the
complete write-up of every path tried, with live error messages):
  v1 — Host Index via data.commoncrawl.org (HTTPS mirror). DEAD: that
       host's robots.txt blanket-disallows "/" for every crawler
       (confirmed live).
  v2 — Columnar index via DuckDB's httpfs reading s3://commoncrawl
       directly, assuming no-credentials meant unsigned-like-the-AWS-CLI.
       DEAD: DuckDB's S3 client always signs requests; no anonymous
       provider exists. Real 403 AccessDenied on a live GitHub Actions run.
  v3 — Columnar index via the actual AWS CLI's --no-sign-request
       (genuinely unsigned). DEAD: anonymous GetObject on a KNOWN key
       works, but anonymous ListObjectsV2 (browsing/discovering what
       files exist) is denied, and Parquet part-file names include a
       random Spark-generated UUID with no published manifest — so there
       was no way to discover what to fetch. Real AccessDenied on a live
       GitHub Actions run.
  v4 — Columnar index via real AWS Athena (a genuine, working fix, since
       Athena's own Glue/metastore does file discovery server-side,
       sidestepping the ListObjects restriction entirely). WORKS, but
       requires a real, billed AWS account (~$1-1.50/query-run) — user
       decided not to open one for this right now. Code kept, dormant,
       for if that changes (see git history / the workflow's comments).
  v5 (THIS VERSION) — a second opinion (Gemini) surfaced that Common
       Crawl's own org re-hosts the HOST INDEX (the same dataset v1
       tried and failed to reach) on Hugging Face, where anonymous file
       LISTING genuinely works (unlike S3's ListObjectsV2) — confirmed
       live via a direct, unauthenticated fetch of
       https://huggingface.co/api/datasets/commoncrawl/host-index-testing-v2/tree/main/data,
       which returned a real directory listing of 26 crawl= partitions,
       no token, no account. DuckDB has NATIVE, documented `hf://` read
       support (shipped in httpfs since v0.10.3, confirmed via DuckDB's
       own docs and Hugging Face's own docs) — no separate extension, no
       auth for public datasets, glob patterns work. This is free, no
       AWS account, no billing, no robots.txt issue (this access path
       never touches data.commoncrawl.org or S3 at all), and it's the
       HOST-level aggregated index (fetch-status counts, rank scores)
       rather than the raw per-URL columnar index — meaning the
       dead-domain filtering logic this project designed back in v1
       (see _looks_dead below) is directly usable again, unmodified.

Trade-off versus the abandoned columnar/Athena approach: this is
per-HOST (one row per known host, aggregated), not per-URL — slightly
less granular, but genuinely sufficient for seeding a "visit this host
and look for an ATS link" crawl queue, which is exactly what this is for.

PER-BATCH DRAINING (2026-08 update, h_file_count + h_total_seeded —
renamed from host_crawl_seed_progress / host_crawl_seeded_hosts, via an
intermediate file_count / total_seeded naming, to their final h_-
prefixed names so all four of this pipeline's tables group together
alphabetically in the table viewer): a single Common
Crawl Parquet file can hold 1.6M+ matching hosts — the batch pipeline
(host_crawl_batch.py / host_crawl.yml's seed/crawl_batch/finalize jobs)
caps each --one-file seed at a small --limit (default 100K) so a batch's
Supabase footprint stays bounded. Since that means one file often needs
SEVERAL batches to fully seed, h_total_seeded tracks every host already
seeded from each file (separately from h_file_count's file-level
done/not-done), so a re-run of a not-yet-drained file skips hosts already
written and picks genuinely NEW ones each time — converging on full
coverage of the file after enough batches, rather than repeatedly
re-picking the same random slice. A file is only marked fully done in
h_file_count once a batch finds strictly fewer new hosts left than the cap
(proof nothing remains), not merely "this batch wrote something." The
moment a file IS marked fully done, its rows in h_total_seeded are
deleted (see _clear_seeded_hosts_for_file) — that table only ever needs
to hold rows for files still IN PROGRESS, so clearing a finished file's
rows keeps it from growing forever.

Usage:
    python host_crawl_seed.py                       # seed from latest crawl
    python host_crawl_seed.py --dry-run             # count without writing
    python host_crawl_seed.py --limit 500000        # cap rows written (testing)
    python host_crawl_seed.py --crawl CC-MAIN-2025-18   # pin a specific crawl
    python host_crawl_seed.py --months 3            # scan the 3 most recent crawls
"""

import argparse
import concurrent.futures
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

# Optional: a Hugging Face token, purely to get the higher authenticated
# rate-limit tier (HF's docs note anonymous CI traffic from shared IP
# pools like GitHub Actions runners is more exposed to rate-limiting than
# a token'd request) — NOT required for functionality, this dataset is
# fully public. Left empty, DuckDB just queries anonymously.
HF_TOKEN = os.environ.get("HF_TOKEN", "")

HF_DATASET = "commoncrawl/host-index-testing-v2"
HF_BASE = f"hf://datasets/{HF_DATASET}/data"

# Used only if live discovery of available crawl= partitions fails.
_FALLBACK_CRAWL = "CC-MAIN-2025-18"

# SAFE DEFAULT CAP — applies whenever --limit isn't passed explicitly.
# A live run showed ~1.66M live hosts matched from JUST ONE of 30 files in
# a single crawl partition — the full partition could plausibly yield
# tens of millions, which would blow well past Supabase's 500MB free tier
# (measured: ≈167 bytes/row queued, ≈175 bytes/row once an outcome is
# recorded, both now on the same h_seeding row — see
# BULK_DOMAIN_DISCOVERY_NOTES.md's 2026-08-22 update for
# the full math). ~900K total hosts keeps steady-state usage comfortably
# under budget. This is a DEFAULT, not a hard ceiling — pass
# --limit 0 (or any explicit --limit) to override it.
_DEFAULT_SAFE_LIMIT = 900_000

# Set by seed() when one_file=True, so main() can report whether the file
# it just processed is now FULLY drained (no new hosts left at all) or
# only partially drained (this batch hit --limit, more remain). The
# batch-pipeline workflow (host_crawl.yml's finalize job) greps this line
# from the step's log to decide whether to keep re-triggering on the SAME
# file (not yet drained) or move on to the next one (file_done=True) —
# see main()'s final print of "ONE_FILE_RESULT: file_done=<bool>".
_LAST_ONE_FILE_RESULT = {"file_done": None}

# Belt-and-suspenders wall-clock cap for any single DuckDB/hf:// call, on
# top of the http_timeout/http_retries settings in _get_duckdb_connection.
# DuckDB's own C++ core can, in rare cases, still block past its httpfs
# settings (e.g. during DNS resolution or TLS handshake before the HTTP
# layer's timeout logic ever engages). Running each call in a worker
# thread with .result(timeout=...) means a stall raises a clean Python
# TimeoutError this script can log and act on, instead of ever needing an
# external process (GitHub's runner) to be the one that kills it.
_DUCKDB_CALL_TIMEOUT_SECONDS = 180


def _run_with_timeout(fn, *args, timeout=_DUCKDB_CALL_TIMEOUT_SECONDS, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fn, *args, **kwargs)
        return future.result(timeout=timeout)
        # NOTE: on TimeoutError, the DuckDB call keeps running in its
        # thread in the background (Python can't forcibly kill a thread),
        # but the pool is a context manager for exactly one call and this
        # process exits/moves on right after — it doesn't leak across runs.

# ── TLD allowlist ────────────────────────────────────────
# Per-country ccTLDs for the target markets, PLUS the generic/startup
# TLDs (.com, .io, .co, .app, .dev) that skew heavily toward exactly
# these markets but aren't attributable to any single country. This is a
# SOFT geography proxy, not a hard filter on company location. ccTLDs for
# uninvolved countries (.ng, .in, .br, etc.) are excluded on purpose.
TARGET_TLDS = {
    "us", "uk", "ca", "de", "au", "ie", "mt",
    "com", "net", "io", "co", "app", "dev",
}
TARGET_SUFFIXES_EXTRA = {"co.uk", "com.au"}


def _build_tld_filter() -> str:
    parts = [f"'{t}'" for t in TARGET_TLDS] + [f"'{t}'" for t in TARGET_SUFFIXES_EXTRA]
    return ",".join(parts)


# ── Liveness filter ──────────────────────────────────────
# A host is treated as likely-dead if Common Crawl's own crawl attempts
# never got a single 2xx response for it. This costs nothing extra (the
# data's already in the index) and only excludes hosts CC itself could
# never successfully fetch — a real small/quiet company that returned
# 200 even once still passes, regardless of how low its rank is.
# Deliberately conservative (biased toward keeping a host if in doubt),
# matching this project's "inclusive not exclusive" instruction.
def _looks_dead(fetch_200: int, fetch_4xx: int, fetch_5xx: int,
                 fetch_gone: int, nutch_gone: int) -> bool:
    if fetch_200 and fetch_200 > 0:
        return False
    return (fetch_4xx or 0) + (fetch_5xx or 0) + (fetch_gone or 0) + (nutch_gone or 0) > 0


def _get_duckdb_connection():
    try:
        import duckdb
    except ImportError:
        log.error("duckdb not installed — pip install duckdb to run this step.")
        return None

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    except Exception as e:
        log.error(f"Failed to load DuckDB's httpfs extension (needed for hf:// "
                  f"reads): {e}")
        return None

    # IMPORTANT: without HF_TOKEN, every request against huggingface.co is
    # fully anonymous and shares a rate-limit/throttling bucket with every
    # other anonymous request from the SAME source IP range — and GitHub
    # Actions runners come from a small, well-known, heavily-shared IP
    # pool. HF's own docs note anonymous traffic from shared-IP CI runners
    # is more exposed to throttling than token'd traffic. Critically, this
    # can manifest as a SILENT STALL (the connection hangs, never a clean
    # 429) rather than an error DuckDB can retry around — which matches
    # the observed symptom exactly: the log stops right after "Querying
    # Common Crawl's Host Index..." with no Python-level error at all,
    # then GitHub's own runner reports "The operation was canceled" —
    # i.e. something upstream of this process is what ended it, not a
    # try/except inside seed(). Setting HF_TOKEN (free, just needs a HF
    # account) moves this run onto the authenticated tier and is the
    # single highest-leverage fix available. See BULK_DOMAIN_DISCOVERY_NOTES.md.
    if not HF_TOKEN:
        log.warning("HF_TOKEN is not set — running fully anonymous against "
                     "huggingface.co from a shared GitHub Actions IP pool. "
                     "This is the most likely cause of silent hangs/timeouts "
                     "('Error: The operation was canceled.' with no Python-level "
                     "error). Strongly recommend setting the HF_TOKEN secret "
                     "(free — generate at huggingface.co/settings/tokens with "
                     "'read' scope) to move onto the authenticated rate-limit tier.")

    if HF_TOKEN:
        try:
            con.execute(f"CREATE SECRET hf_token (TYPE huggingface, TOKEN '{HF_TOKEN}');")
            log.info("Using HF_TOKEN for authenticated (higher rate-limit) access.")
        except Exception as e:
            log.warning(f"Failed to configure HF_TOKEN secret (continuing "
                        f"anonymously): {e}")

    # DuckDB's httpfs has NO default timeout on hf:// (HTTP) requests — if
    # the remote end stalls (as anonymous/throttled requests can), the
    # query blocks forever with nothing for Python to catch or log. These
    # settings give it explicit, finite bounds so a stall becomes a loud
    # DuckDB IOException (caught below) instead of a silent hang that only
    # GitHub's own infrastructure eventually kills from the outside.
    try:
        con.execute("SET http_timeout = 30000;")     # 30s per HTTP request
        con.execute("SET http_retries = 3;")
        con.execute("SET http_retry_wait_ms = 2000;")
        con.execute("SET http_retry_backoff = 2;")
    except Exception as e:
        log.warning(f"Could not set httpfs timeout/retry options (continuing "
                    f"with DuckDB defaults, which may hang indefinitely on a "
                    f"stalled connection): {e}")

    # A single Host Index crawl partition is ~7GB (confirmed via Common
    # Crawl's own blog post announcing the Host Index) — reading that over
    # a remote hf:// HTTP filesystem can make DuckDB buffer far more in
    # memory than a local-disk read would, especially before predicate
    # pushdown narrows anything down. GitHub-hosted runners' free tier
    # defaults to 7GB RAM total. An uncapped DuckDB can get OOM-killed by
    # the OS — which the Actions runner then reports as the generic
    # "Error: The operation was canceled." with NO Python-level exception
    # ever raised, because SIGKILL from the OOM killer can't be caught,
    # unlike a DuckDB-level error. Explicitly capping DuckDB's memory
    # forces it to spill to disk instead of growing unbounded, trading
    # some speed for actually finishing instead of being killed.
    try:
        con.execute("SET memory_limit = '3GB';")
        con.execute("PRAGMA threads=2;")  # fewer parallel readers = lower peak memory
    except Exception as e:
        log.warning(f"Could not set DuckDB memory_limit (continuing with "
                    f"DuckDB's default, unbounded-by-us memory usage): {e}")

    return con


def _get_processed_files() -> set[str]:
    """Which Parquet files have already been fully processed (queried AND
    successfully written to Supabase) in a prior run — lets a re-run skip
    straight past them instead of re-downloading/re-querying files that
    already made it into h_seeding. Stored in Supabase (not a local
    file) because GitHub Actions runners are ephemeral — nothing written
    to local disk survives between separate workflow runs.

    PAGINATED, same reason as _get_seeded_hosts_for_file(): an unpaginated
    GET silently caps out at PostgREST's default max-rows (1000 on
    Supabase). h_file_count only holds ~30 rows per crawl partition today
    so this hasn't bitten in practice yet, but it's the identical bug and
    would silently start skipping "already done" files the moment this
    table crosses 1000 rows (e.g. once seeding runs across several crawl
    partitions/months) — fixed the same way, by paging through Range."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return set()
    files: set[str] = set()
    page_size = 1000
    offset = 0
    while True:
        try:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/h_file_count",
                headers={
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {SUPABASE_KEY}",
                    "Range-Unit": "items",
                    "Range": f"{offset}-{offset + page_size - 1}",
                },
                params={"select": "file", "order": "file"},
                timeout=30,
            )
            r.raise_for_status()
            page = r.json()
        except Exception as e:
            log.warning(f"Could not fetch already-processed file list "
                        f"(page starting at offset {offset} — will "
                        f"re-process everything not already collected "
                        f"this call): {e}")
            break
        if not page:
            break
        files.update(row["file"] for row in page)
        if len(page) < page_size:
            break
        offset += page_size
    return files


def _mark_file_processed(fpath: str, crawl_name: str, rows_matched: int):
    """Record a file as done immediately after ITS rows are successfully
    written to Supabase — not merely queried — so a Supabase write
    failure doesn't falsely mark progress that didn't actually land."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/h_file_count",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=ignore-duplicates",
            },
            json={"file": fpath, "crawl": crawl_name, "rows_matched": rows_matched},
            params={"on_conflict": "file"},
            timeout=30,
        )
        r.raise_for_status()
    except Exception as e:
        log.warning(f"Could not record {fpath} as processed (a future re-run "
                    f"may redo this file): {e}")


def _get_seeded_hosts_for_file(fpath: str) -> set[str]:
    """Which hosts have ALREADY been seeded from this specific file in an
    earlier batch. Used so a re-run of a file that got trimmed by
    --limit (not yet fully drained) picks up NEW hosts instead of
    re-matching and potentially re-picking the same 100K again — this is
    what makes the per-batch cap converge on full coverage of a file
    instead of gambling on the same random slice every time. Stored
    separately from h_file_count (file-level done/not-done) and cleared for
    a given file the moment that file is marked done (see
    _clear_seeded_hosts_for_file) — it only needs to hold rows for files
    still in progress.

    PAGINATED — CRITICAL BUG FIXED HERE: PostgREST caps any single
    request at its configured max-rows (Supabase's default is 1000) when
    no explicit Range is given. An earlier version of this function did
    one unpaginated GET, so once a file had more than 1000 already-seeded
    hosts recorded (any file needing more than one 100K batch reaches
    this immediately), every batch after the first only ever saw the
    FIRST 1000 of them — the other 99,000+ looked "not yet seeded" and
    got re-matched and re-queued as if new. Confirmed live: after two
    100K-capped batches on one file, h_total_seeded correctly held
    163,939 real rows, but the file was never marked done in h_file_count
    even though the second batch's new-host count (63,939) was under the
    100K cap and should have triggered that — because the "already
    seeded" set the second batch actually filtered against was built
    from only 1000 rows, not the true ~100,000, so the numbers never
    lined up the way the drain-detection logic expects. Fixed by paging
    through the full result set with Range headers instead of a single
    unbounded GET."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return set()
    hosts: set[str] = set()
    page_size = 1000
    offset = 0
    while True:
        try:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/h_total_seeded",
                headers={
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {SUPABASE_KEY}",
                    "Range-Unit": "items",
                    "Range": f"{offset}-{offset + page_size - 1}",
                },
                params={"select": "host", "file": f"eq.{fpath}", "order": "host"},
                timeout=30,
            )
            r.raise_for_status()
            page = r.json()
        except Exception as e:
            log.warning(f"Could not fetch already-seeded hosts for {fpath} "
                        f"(page starting at offset {offset} — a re-run of "
                        f"this file may re-pick some hosts already seeded, "
                        f"not harmful since h_seeding's upsert "
                        f"on_conflict=host just no-ops on a duplicate, but "
                        f"wastes some of this batch's cap on hosts already "
                        f"written): {e}")
            break
        if not page:
            break
        hosts.update(row["host"] for row in page)
        if len(page) < page_size:
            break
        offset += page_size
    return hosts


def _record_seeded_hosts(fpath: str, hosts: list[str]):
    """Record hosts as seeded from this file, right after they're
    successfully written to h_seeding — so the NEXT batch (if this
    file isn't fully drained yet) knows to skip them and pick genuinely
    new ones instead."""
    if not SUPABASE_URL or not SUPABASE_KEY or not hosts:
        return
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates",
    }
    batch_size = 5000
    for i in range(0, len(hosts), batch_size):
        batch = [{"host": h, "file": fpath} for h in hosts[i:i + batch_size]]
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/h_total_seeded",
                headers=headers, json=batch, timeout=60,
                params={"on_conflict": "host"},
            )
            r.raise_for_status()
        except Exception as e:
            log.warning(f"Could not record {len(batch)} seeded hosts for "
                        f"{fpath} (a future re-run of this file may re-pick "
                        f"some of these — not harmful, just some wasted cap "
                        f"budget): {e}")


def _clear_seeded_hosts_for_file(fpath: str):
    """Delete this file's rows from h_total_seeded now that it's been
    marked fully done in h_file_count. h_total_seeded's whole purpose is
    tracking per-host progress WITHIN an in-progress file — once a file is
    done, h_file_count's own record of that (file -> done) is sufficient and
    permanent; keeping the (potentially 1M+ row) per-host detail around
    forever would make h_total_seeded grow without bound across many
    files. h_file_count itself (the small, permanent completion tracker) is
    NEVER cleared — only this per-file detail table."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    try:
        r = requests.delete(
            f"{SUPABASE_URL}/rest/v1/h_total_seeded",
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
            params={"file": f"eq.{fpath}"},
            timeout=60,
        )
        r.raise_for_status()
        log.info(f"Cleared h_total_seeded rows for completed file {fpath}.")
    except Exception as e:
        log.warning(f"Could not clear h_total_seeded rows for completed "
                    f"file {fpath} (not harmful — just leaves some now-unneeded "
                    f"rows behind until a future cleanup): {e}")


def _list_available_crawls(con) -> list[str]:
    """Ask Hugging Face's (genuinely anonymous, unlike S3) directory
    listing API which crawl= partitions exist, via DuckDB's glob()."""
    def _do_glob():
        return con.execute(
            f"SELECT DISTINCT regexp_extract(file, 'crawl=([^/]+)', 1) AS crawl "
            f"FROM glob('{HF_BASE}/crawl=*/') "
            f"ORDER BY crawl DESC"
        ).fetchall()

    try:
        rows = _run_with_timeout(_do_glob, timeout=60)
        names = [r[0] for r in rows if r[0]]
        if names:
            return names
    except concurrent.futures.TimeoutError:
        log.warning("Listing crawl partitions via hf:// glob timed out after 60s "
                    "(likely a stalled/throttled anonymous connection to "
                    "huggingface.co — see the HF_TOKEN warning above).")
    except Exception as e:
        log.warning(f"Could not list crawl partitions via hf:// glob: {e}")
    return []


def seed(limit: int | None = None, dry_run: bool = False,
         crawl: str | None = None, months: int = 1,
         one_file: bool = False) -> int:
    if one_file:
        # Reset for this call — set to True below only if a file was
        # actually processed and confirmed drained; True here at the
        # start (before any file is touched) covers the "no unprocessed
        # file left at all" case, which counts as "done" for the
        # pipeline's purposes (nothing left to seed = move on).
        _LAST_ONE_FILE_RESULT["file_done"] = True

    con = _get_duckdb_connection()
    if con is None:
        return 0

    if crawl:
        crawls = [crawl]
    else:
        available = _list_available_crawls(con)
        if not available:
            log.warning(f"Could not list crawl partitions live — falling back to "
                        f"hardcoded {_FALLBACK_CRAWL!r} (may be stale).")
            available = [_FALLBACK_CRAWL]
        else:
            log.info(f"Available crawl partitions (most recent 5): {available[:5]}")
        crawls = available[:max(1, months)]

    log.info(f"Seeding from crawl partition(s): {crawls}")

    tld_filter = _build_tld_filter()

    # IMPORTANT: glob ONLY the specific crawl=X folders actually requested
    # — NOT 'crawl=*/*.parquet' filtered afterward with WHERE crawl IN
    # (...). An earlier run timed out doing exactly that: globbing all 26
    # crawl partitions over a remote hf:// connection before the WHERE
    # clause ever got a chance to prune anything.
    #
    # SECOND, MORE SERIOUS issue found after that fix still didn't resolve
    # a live "Error: The operation was canceled." (even with HF_TOKEN set
    # and authenticating correctly, ruling out anonymous throttling as the
    # cause): a single Host Index crawl partition is ~7GB (Common Crawl's
    # own blog post). The prior version of this function ran ONE big
    # UNION ALL query over read_parquet('crawl={c}/*.parquet') — i.e. every
    # file in the partition at once — then called .fetchall(), which
    # materializes the ENTIRE filtered result as Python objects in memory
    # on top of whatever DuckDB itself buffers while streaming/decoding
    # Parquet over hf://'s HTTP layer. On a GitHub-hosted runner (7GB RAM
    # on the free tier), this is a real, plausible way to get OOM-killed by
    # the OS — which the Actions runner reports as the generic "Error: The
    # operation was canceled." with NO Python-level exception ever raised
    # (a SIGKILL from the OOM killer can't be caught), exactly matching the
    # observed symptom (log stops mid-query, no logged error before the
    # runner's own cancellation message).
    #
    # Fix: enumerate the individual Parquet files inside each crawl
    # partition first (a cheap glob, not a full read), then process ONE
    # FILE AT A TIME — query it, write its matching rows to Supabase, and
    # let DuckDB/Python release that file's memory before moving to the
    # next. Peak memory now scales with ONE file's size, not the whole
    # ~7GB partition. Combined with the memory_limit set in
    # _get_duckdb_connection, this should keep peak RSS well under the
    # runner's ceiling regardless of how large a partition is.
    def _list_partition_files(crawl_name: str) -> list[str]:
        try:
            rows = _run_with_timeout(
                lambda: con.execute(
                    f"SELECT file FROM glob('{HF_BASE}/crawl={crawl_name}/*.parquet')"
                ).fetchall(),
                timeout=60,
            )
            return [r[0] for r in rows]
        except concurrent.futures.TimeoutError:
            log.warning(f"Listing files in crawl={crawl_name} timed out after 60s.")
            return []
        except Exception as e:
            log.warning(f"Could not list files in crawl={crawl_name}: {e}")
            return []

    files_by_crawl = {}
    for c in crawls:
        files = _list_partition_files(c)
        if not files:
            # Fall back to the old single-glob-string approach for this
            # crawl if per-file listing failed for some reason — still
            # scoped to one crawl, so not the original all-26-partitions
            # bug, just less memory-safe.
            files = [f"{HF_BASE}/crawl={c}/*.parquet"]
        log.info(f"crawl={c}: {len(files)} Parquet file(s) to scan")
        files_by_crawl[c] = files

    # Skip files a PRIOR run already fully processed (queried AND written
    # to Supabase) — recorded in h_file_count. This is what
    # makes a re-run resume instead of re-doing the whole partition from
    # scratch: Common Crawl's Parquet files aren't row-numbered in any way
    # that supports a "resume from row N" checkpoint (rows within a file
    # aren't in a stable/meaningful order), but tracking completed FILES
    # is simple and works with how the data is actually laid out.
    already_done = _get_processed_files()
    if already_done:
        log.info(f"{len(already_done)} file(s) already processed in a prior "
                 f"run — will skip those and resume with the rest.")

    total_files = sum(len(f) for f in files_by_crawl.values())
    skipped_done = sum(1 for files in files_by_crawl.values()
                        for f in files if f in already_done)
    log.info(f"Querying Common Crawl's Host Index via Hugging Face (hf://) — "
             f"free, no AWS account, no billing — {total_files} file(s) total, "
             f"{skipped_done} already done, processing the rest one at a "
             f"time to bound memory use...")

    if not dry_run and not (SUPABASE_URL and SUPABASE_KEY):
        log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot write queue.")
        return 0

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates",
    }

    seen = set()
    dead_skipped = 0
    total_matched = 0
    total_written = 0
    query_timeout = 300  # per FILE now, not per whole partition — 5 min is generous
    file_num = 0
    dry_run_rows = []  # only accumulated in --dry-run mode, for a final count

    one_file_done = False  # set once, when one_file=True, to stop after 1 real file

    for c, files in files_by_crawl.items():
        for fpath in files:
            if one_file_done:
                break
            file_num += 1
            if fpath in already_done:
                log.info(f"  [{file_num}/{total_files}] already processed — skipping")
                continue

            per_file_query = f"""
                SELECT surt_host_name, url_host_tld, hcrank,
                       fetch_200, fetch_4xx, fetch_5xx, fetch_gone, nutch_gone
                FROM read_parquet('{fpath}')
                WHERE url_host_tld IN ({tld_filter})
            """

            def _do_query(q=per_file_query):
                return con.execute(q).fetchall()

            try:
                file_result = _run_with_timeout(_do_query, timeout=query_timeout)
            except concurrent.futures.TimeoutError:
                log.warning(f"  [{file_num}/{total_files}] query timed out after "
                            f"{query_timeout}s on {fpath} — skipping this file "
                            f"(NOT marked done, will retry next run), continuing.")
                continue
            except Exception as e:
                log.warning(f"  [{file_num}/{total_files}] query failed on "
                            f"{fpath}: {e} — skipping (NOT marked done), continuing.")
                continue

            total_matched += len(file_result)

            # Hosts already seeded from THIS file in an earlier batch (only
            # relevant/populated when one_file mode previously trimmed this
            # same file) — skipping them here is what makes repeated
            # --one-file batches converge on covering the WHOLE file
            # instead of gambling on the same random slice each time.
            already_seeded_this_file = (
                _get_seeded_hosts_for_file(fpath) if one_file else set()
            )
            skipped_already_seeded = 0

            file_rows = []
            for surt_host, tld, hcrank, f200, f4xx, f5xx, fgone, ngone in file_result:
                if _looks_dead(f200, f4xx, f5xx, fgone, ngone):
                    dead_skipped += 1
                    continue
                # surt_host_name is reversed (org,commoncrawl,www) — un-reverse
                # to a normal hostname for the crawl step.
                host = ".".join(reversed(surt_host.split(",")))
                if not host or host in seen:
                    continue
                if host in already_seeded_this_file:
                    skipped_already_seeded += 1
                    continue
                seen.add(host)
                file_rows.append({
                    "host": host,
                    "tld": tld,
                    "hcrank": float(hcrank) if hcrank is not None else None,
                })
            del file_result  # free this file's raw result before moving on

            if skipped_already_seeded:
                log.info(f"  [{file_num}/{total_files}] {skipped_already_seeded} "
                         f"hosts already seeded from this file in an earlier "
                         f"batch — skipped, only NEW hosts count toward this "
                         f"batch's cap")

            # Trim to whatever's left of --limit BEFORE writing — a single
            # file can contain far more matches than the whole limit (a
            # live run saw 1.66M live hosts from just ONE of 30 files —
            # ~370MB written in one shot, which alone nearly filled
            # Supabase's 500MB free tier and started throwing 500 errors
            # mid-write). This ALSO applies in one_file mode — an earlier
            # version of this comment argued one_file should skip trimming
            # since "the whole point is this exact file, complete, no
            # truncation," but that reasoning was wrong: it didn't account
            # for a SINGLE file being large enough to blow the storage
            # budget on its own. one_file mode (used by
            # host_crawl_batch.py) now always passes a --limit (default
            # 100K, see host_crawl_batch.py's DEFAULT_SEED_LIMIT_PER_BATCH)
            # specifically so a batch's seed step can't do this again.
            #
            # Whether this file counts as FULLY drained after this pass:
            # true only if what's left (post already-seeded-hosts filter,
            # pre-trim) fits inside the remaining cap with room to spare —
            # i.e. this batch saw every remaining new host in the file and
            # still didn't need to trim. If it's an exact/over fit, treat
            # it as NOT yet confirmed drained (a later batch will re-check,
            # find zero new hosts left, and mark it done then — one extra
            # cheap query, never a risk of leaving hosts unseeded).
            if limit:
                already_have = total_written if not dry_run else len(dry_run_rows)
                remaining = max(0, limit - already_have)
                file_was_trimmed = len(file_rows) > remaining
                file_fully_drained_this_pass = not file_was_trimmed
                if file_was_trimmed:
                    log.info(f"  [{file_num}/{total_files}] trimming this file's "
                             f"{len(file_rows)} new matches down to {remaining} to "
                             f"respect --limit={limit} — this file is NOT yet "
                             f"fully drained, a later batch will pick up the rest")
                    file_rows = file_rows[:remaining]
                elif len(file_rows) == 0 and skipped_already_seeded > 0:
                    log.info(f"  [{file_num}/{total_files}] no NEW hosts left in "
                             f"this file (all {skipped_already_seeded} remaining "
                             f"matches were already seeded in earlier batches) — "
                             f"file is now fully drained")
            else:
                file_fully_drained_this_pass = True

            log.info(f"  [{file_num}/{total_files}] {len(file_rows)} live hosts "
                     f"from this file ({total_matched} rows matched so far)")

            if dry_run:
                dry_run_rows.extend(file_rows)
                # dry runs don't write, so there's nothing to mark "done" —
                # a real run still needs to process this file for real later
            else:
                # Write THIS FILE's rows now (not batched at the very end)
                # so a crash partway through a long seed run still leaves
                # already-completed files written and marked, instead of
                # losing everything back to the start.
                file_written = 0
                written_hosts = []
                batch_size = 5000
                write_failed = False
                for i in range(0, len(file_rows), batch_size):
                    batch = file_rows[i:i + batch_size]
                    try:
                        r = requests.post(
                            f"{SUPABASE_URL}/rest/v1/h_seeding",
                            headers=headers, json=batch, timeout=60,
                            params={"on_conflict": "host"},
                        )
                        r.raise_for_status()
                        file_written += len(batch)
                        written_hosts.extend(row["host"] for row in batch)
                    except Exception as e:
                        log.error(f"  Failed to write batch from {fpath}: {e}")
                        write_failed = True
                total_written += file_written

                # Record exactly the hosts that were actually written (not
                # the whole file_rows list — if a later batch write
                # failed partway, only what genuinely landed should count
                # as "already seeded" for next time) so a future re-run of
                # this same file, if it's not yet fully drained, skips
                # these and picks new ones instead of re-matching them.
                if one_file and written_hosts:
                    _record_seeded_hosts(fpath, written_hosts)

                if not write_failed and file_fully_drained_this_pass:
                    _mark_file_processed(fpath, c, len(file_rows))
                    # File is now permanently recorded as done in h_file_count
                    # — h_total_seeded's per-host detail for this file has
                    # served its purpose (letting THIS file's batches
                    # converge without duplicates) and would otherwise sit
                    # around forever. Clear it now so the table only ever
                    # holds rows for files still in progress.
                    _clear_seeded_hosts_for_file(fpath)
                    _LAST_ONE_FILE_RESULT["file_done"] = True
                elif not file_fully_drained_this_pass:
                    pass  # already logged above — deliberately left unmarked,
                          # a later batch will pick up the remaining hosts
                    _LAST_ONE_FILE_RESULT["file_done"] = False
                else:
                    log.warning(f"  {fpath} had a write failure — NOT marked "
                                f"done, will be retried on the next run.")
                    _LAST_ONE_FILE_RESULT["file_done"] = False

            if one_file:
                one_file_done = True
                log.info(f"--one-file: {fpath} fully processed, stopping "
                         f"(this is the batch-workflow mode).")
                break
            if limit and (total_written if not dry_run else len(dry_run_rows)) >= limit:
                log.info(f"Reached --limit={limit}, stopping early.")
                break
        if one_file_done:
            break
        if limit and (total_written if not dry_run else len(dry_run_rows)) >= limit:
            break

    dead_summary = f" ({dead_skipped} skipped as likely-dead)"
    if dry_run:
        log.info(f"Dry run — {len(dry_run_rows)} hosts would be written"
                 f"{dead_summary}. Not writing to Supabase.")
        return len(dry_run_rows)

    log.info(f"Seed complete: {total_written} hosts written to "
             f"h_seeding{dead_summary}.")
    return total_written


def main():
    parser = argparse.ArgumentParser(
        description="Seed h_seeding from Common Crawl's Host Index "
                    "(via Hugging Face + DuckDB, free)"
    )
    parser.add_argument("--limit", type=int, default=None,
                         help=f"Cap total rows written. Defaults to "
                              f"{_DEFAULT_SAFE_LIMIT:,} if not passed — a single "
                              f"crawl partition can contain tens of millions of "
                              f"matching hosts, well past Supabase's free-tier "
                              f"storage budget, so this script refuses to run "
                              f"fully unbounded by accident. Pass --limit 0 to "
                              f"disable the cap and seed everything.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Count without writing to Supabase")
    parser.add_argument("--crawl", type=str, default=None,
                         help="Pin a specific crawl partition, e.g. CC-MAIN-2025-18 "
                              "(default: auto-detect latest available)")
    parser.add_argument("--months", type=int, default=1,
                         help="Number of recent crawl partitions to scan "
                              "(default: 1 — more = broader coverage, longer runtime, "
                              "still free)")
    parser.add_argument("--one-file", action="store_true",
                         help="Seed from exactly ONE unprocessed Parquet file "
                              "and stop, still respecting --limit (a single file "
                              "can contain 1M+ matches on its own — trimmed if "
                              "needed, and left unmarked so a later run can "
                              "finish it). Used by host_crawl_batch.py's "
                              "seed-crawl-clear cycle.")
    args = parser.parse_args()

    if args.limit is None:
        effective_limit = _DEFAULT_SAFE_LIMIT
        log.info(f"No --limit passed — defaulting to the safe cap of "
                 f"{_DEFAULT_SAFE_LIMIT:,} hosts (pass --limit 0 to seed "
                 f"everything instead).")
    elif args.limit == 0:
        effective_limit = None
        log.warning("--limit 0 — cap disabled, this run will seed EVERY "
                    "matching host with no ceiling. Make sure that's really "
                    "what you want (see BULK_DOMAIN_DISCOVERY_NOTES.md's "
                    "storage math before doing this on the free Supabase tier).")
    else:
        effective_limit = args.limit

    written = seed(limit=effective_limit, dry_run=args.dry_run,
                   crawl=args.crawl, months=args.months, one_file=args.one_file)

    if args.one_file:
        # Greppable line for host_crawl.yml's finalize job to read out of
        # this step's log — decides whether to re-trigger on the SAME file
        # (not yet drained: more batches needed) or move on to the next
        # one (file_done=true). Printed via plain print(), not log, so
        # it's not prefixed with a timestamp/level that would complicate
        # a simple grep in the workflow's shell step.
        done = _LAST_ONE_FILE_RESULT["file_done"]
        print(f"ONE_FILE_RESULT: file_done={'true' if done else 'false'}")

    if written == 0 and not args.one_file:
        # A one_file run legitimately can write 0 NEW hosts (e.g. this
        # batch found only already-seeded hosts left, confirming the file
        # is drained) without that being a failure — exit 0 either way and
        # let the ONE_FILE_RESULT line communicate the real outcome.
        sys.exit(1)


if __name__ == "__main__":
    main()
