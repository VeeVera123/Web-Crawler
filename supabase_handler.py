"""
Supabase handler — writes filtered jobs to PostgreSQL via REST API.
Handles deduplication by job URL, populates slug_registry, and logs scan reports.
"""

import logging
import random
import time
from datetime import date, datetime, timedelta, timezone

import requests as http_requests

import os
from dotenv import load_dotenv
load_dotenv()
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

log = logging.getLogger(__name__)


class SupabaseFetchError(Exception):
    """
    Raised when a Supabase GET fails after all retries are exhausted —
    deliberately distinct from a query that legitimately returns zero rows.

    Without this, a single transient blip (Supabase cold-start, a brief
    gateway 401/5xx, a network timeout) looks IDENTICAL to "the table is
    genuinely empty" to any caller checking `if not rows`. For
    get_all_slugs() that's dangerous: main.py's load_slugs() treats an
    empty result as "nothing assigned to this shard" and quietly skips
    scraping — no error, no non-zero exit, just a shard that silently did
    nothing (this is exactly what happened to shard 2/8 on a one-off 401).
    """
    pass


# Retryable: connection blips, and HTTP statuses that can be transient on
# Supabase's side (401/403 can happen on project cold-start/gateway hiccups,
# not just a genuinely bad key — a bad key will just keep failing and we'll
# find out after MAX_HTTP_RETRIES anyway).
_RETRYABLE_STATUSES = {401, 403, 429, 500, 502, 503, 504}
MAX_HTTP_RETRIES = 4
_RETRY_BASE_DELAY = 2  # seconds

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

REST = f"{SUPABASE_URL}/rest/v1"


# ── Helpers ──────────────────────────────────────────────

def _post(table: str, data: dict | list[dict], upsert: bool = False) -> dict | list | None:
    """POST to Supabase REST API. Returns parsed JSON or None on error."""
    headers = {**HEADERS}
    if upsert:
        headers["Prefer"] = "return=representation,resolution=merge-duplicates"
    try:
        r = http_requests.post(f"{REST}/{table}", headers=headers, json=data, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"Supabase POST {table} failed: {e}")
        return None


def _patch(table: str, filters: str, data: dict) -> bool:
    """PATCH (update) rows matching filters."""
    try:
        r = http_requests.patch(
            f"{REST}/{table}?{filters}",
            headers=HEADERS, json=data, timeout=30,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        detail = ""
        resp = getattr(e, "response", None)
        if resp is not None:
            detail = f" | body: {resp.text[:500]}"
        log.error(f"Supabase PATCH {table} failed: {e}{detail}")
        return False


def _get(table: str, params: str = "", limit: int = 10000) -> list[dict]:
    """
    GET rows from a table. Retries transient failures (network errors and
    401/403/429/5xx responses) with exponential backoff + jitter before
    giving up. Raises SupabaseFetchError if every attempt fails — callers
    that need to tell "really empty" apart from "the fetch broke" (like
    get_all_slugs(), see above) let that propagate; callers where a
    best-effort empty result is an acceptable fallback should catch it.
    """
    url = f"{REST}/{table}?{params}&limit={limit}" if params else f"{REST}/{table}?limit={limit}"
    last_error = None

    for attempt in range(MAX_HTTP_RETRIES):
        try:
            r = http_requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code in _RETRYABLE_STATUSES and attempt < MAX_HTTP_RETRIES - 1:
                wait = _RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
                log.warning(
                    f"Supabase GET {table} got HTTP {r.status_code}, "
                    f"retrying in {wait:.1f}s (attempt {attempt + 1}/{MAX_HTTP_RETRIES})"
                )
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_error = e
            if attempt < MAX_HTTP_RETRIES - 1:
                wait = _RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
                log.warning(
                    f"Supabase GET {table} failed ({e}), "
                    f"retrying in {wait:.1f}s (attempt {attempt + 1}/{MAX_HTTP_RETRIES})"
                )
                time.sleep(wait)

    log.error(f"Supabase GET {table} failed after {MAX_HTTP_RETRIES} attempts: {last_error}")
    raise SupabaseFetchError(f"GET {table} failed after {MAX_HTTP_RETRIES} attempts: {last_error}")


# ── Slug Registry ────────────────────────────────────────

def populate_slug_registry(slugs: list[tuple[str, str]], source: str = "seed") -> int:
    """
    Upsert slugs into slug_registry. Each slug is (ats, slug_value).
    Returns count of rows upserted.
    """
    if not slugs:
        return 0

    # Batch upsert in chunks of 500
    total = 0
    chunk_size = 500

    for i in range(0, len(slugs), chunk_size):
        chunk = slugs[i:i + chunk_size]
        rows = [
            {
                "ats": ats,
                "slug": slug,
                "source": source,
                "last_seen": datetime.now(timezone.utc).isoformat(),
            }
            for ats, slug in chunk
        ]

        headers = {
            **HEADERS,
            "Prefer": "return=minimal,resolution=merge-duplicates",
        }
        r = None
        try:
            r = http_requests.post(
                f"{REST}/archive_i",
                headers=headers, json=rows, timeout=60,
                params={"on_conflict": "ats,slug"},
            )
            r.raise_for_status()
            total += len(chunk)
        except Exception as e:
            # Without the response body, a CHECK-constraint rejection (e.g.
            # an unrecognized `source` value — this is exactly what silently
            # dropped every job_board_discovery slug before the
            # slug_registry_source_check migration) looks identical to a
            # transient network error in the logs. Always surface it.
            body = f" — response: {r.text[:500]}" if r is not None else ""
            log.error(f"Failed to upsert slug batch (source={source}): {e}{body}")

    log.info(f"Slug registry: upserted {total} slugs (source={source})")
    return total


def resolve_oracle_slug(old_slug: str, new_slug: str) -> bool:
    """
    Cache a discovered Oracle Cloud HCM domain/site by replacing a legacy
    short-tenant slug (e.g. 'eeho' or 'eeho|CX_1') with its resolved form
    (e.g. 'eeho.fa.us2|CX_1') in slug_registry.

    scrape_oracle_cloud_hcm() has to brute-force up to 11 regions to find a
    legacy tenant's real domain — expensive and slow. Once discovered, the
    result is stable (companies don't move Oracle regions), so we persist it
    here and every future run hits the direct API instead of re-discovering.

    Best-effort: any failure is logged and swallowed so a Supabase hiccup
    never breaks the actual scrape that's already in progress.
    """
    if not old_slug or not new_slug or old_slug == new_slug:
        return False
    try:
        # Upsert the resolved slug first...
        r = http_requests.post(
            f"{REST}/archive_i",
            headers={**HEADERS, "Prefer": "return=minimal,resolution=merge-duplicates"},
            json=[{
                "ats": "oracle_cloud_hcm",
                "slug": new_slug,
                "source": "resolved",
                "last_seen": datetime.now(timezone.utc).isoformat(),
            }],
            timeout=30,
            params={"on_conflict": "ats,slug"},
        )
        r.raise_for_status()

        # ...then delete the legacy slug so it isn't scraped (and
        # re-discovered) again on the next run.
        r2 = http_requests.delete(
            f"{REST}/archive_i",
            headers=HEADERS,
            timeout=30,
            params={"ats": "eq.oracle_cloud_hcm", "slug": f"eq.{old_slug}"},
        )
        r2.raise_for_status()
        log.info(f"Oracle Cloud HCM: cached resolved slug {old_slug!r} -> {new_slug!r}")
        return True
    except Exception as e:
        log.debug(f"Oracle Cloud HCM: failed to cache resolved slug {old_slug!r} -> {new_slug!r}: {e}")
        return False


def get_all_slugs() -> list[tuple[str, str]]:
    """
    Fetch all (ats, slug) pairs from slug_registry.
    Supabase caps responses at 1000 rows, so we paginate with that limit.

    Raises SupabaseFetchError if any page fetch fails after retries — this
    is intentionally NOT caught here. main.py's load_slugs() needs to be
    able to tell "the registry is genuinely empty" apart from "the fetch to
    Supabase broke", so it can fail the shard loudly instead of silently
    scraping nothing.
    """
    pairs = []
    offset = 0
    batch_size = 1000  # Supabase default max per response

    while True:
        rows = _get(
            "archive_i",
            f"select=ats,slug&offset={offset}",
            limit=batch_size,
        )
        if not rows:
            break
        for row in rows:
            pairs.append((row["ats"], row["slug"]))
        if len(rows) < batch_size:
            break
        offset += batch_size

    log.info(f"Loaded {len(pairs)} slugs from Supabase slug_registry")
    return pairs


def get_archive_ii_pages() -> list[dict]:
    """Fetch every {career_page_url, website_url} pair from archive_ii
    (formerly archive_iii — in-house/unsupported career pages captured by
    node.py's crawl_batch()). This is Crawl II's (crawl_ii.py) input list,
    the same role get_all_slugs() plays for Crawl I.

    Raises SupabaseFetchError if any page fetch fails after retries — same
    reasoning as get_all_slugs(): crawl_ii.py needs to tell "archive_ii is
    genuinely empty" apart from "the fetch to Supabase broke" so it can
    fail the shard loudly instead of silently scraping nothing."""
    rows = []
    offset = 0
    batch_size = 1000

    while True:
        page = _get(
            "archive_ii",
            f"select=career_page_url,website_url&offset={offset}",
            limit=batch_size,
        )
        if not page:
            break
        rows.extend(page)
        if len(page) < batch_size:
            break
        offset += batch_size

    log.info(f"Loaded {len(rows)} career pages from Supabase archive_ii")
    return rows


# ── Deduplication ────────────────────────────────────────

def get_existing_urls() -> set[str]:
    """
    Pull all job URLs already in the database to avoid duplicates.

    Unlike get_all_slugs(), a fetch failure here is tolerated: worst case
    we treat a few already-known jobs as "new" and re-upsert them, which is
    harmless (upserts are idempotent). Failing the whole shard over a
    dedup-list fetch would be a much worse trade than that.
    """
    urls = set()
    offset = 0
    batch_size = 1000  # Supabase default max per response

    try:
        while True:
            rows = _get("jobs", f"select=job_url&offset={offset}", limit=batch_size)
            if not rows:
                break
            for row in rows:
                url = row.get("job_url", "")
                if url:
                    urls.add(url)
            if len(rows) < batch_size:
                break
            offset += batch_size
    except SupabaseFetchError as e:
        log.warning(f"get_existing_urls: fetch failed after retries, proceeding with {len(urls)} URLs known so far: {e}")

    log.info(f"Found {len(urls)} existing jobs in Supabase")
    return urls


# ── Helpers for safe string extraction ──────────────────

def _safe_str(val, max_len: int = 500) -> str:
    """Coerce value to string, join lists, truncate."""
    if val is None:
        return ""
    if isinstance(val, list):
        val = ", ".join(str(x) for x in val)
    return str(val)[:max_len]


def _build_row(job: dict, location_confidence: str, source_pipeline: str = "crawl_i") -> dict:
    """Build a Supabase row dict from a job dict.

    NOTE 2026-08: country/department/workplace_type/employment_type/
    source_board/location_confidence were dropped from the `jobs` table
    (space-saving — see migration `drop_unused_jobs_columns`). The upstream
    scraper dicts in ats_scrapers.py still populate these keys on `job`
    (classifier.py and geo.py still use job.get("country") etc. as
    in-memory signals for Global/Africa/EMEA classification) — only the
    Supabase persistence step here was trimmed, so no scraper changes were
    needed. `location_confidence` (the function's own parameter) is now
    unused for the same reason — kept as a parameter rather than removed
    from every call site, since add_jobs_batch()/_touch_last_seen() zip it
    in from filter_locations() output.

    `source_pipeline` (2026-08, for the Crawl I/Crawl II restructure) tags
    which pipeline is doing a true first-insert of this row — 'crawl_i'
    (default, matches the column's own DB default) for the known-ATS
    scanner, 'crawl_ii' for the in-house/unsupported career-page heuristic
    scraper. This lets each pipeline's finalize pass hard-delete only its
    own stale rows (see cleanup_stale_jobs's source_pipeline filter)
    without touching the other's. Only meaningful on INSERT — see
    _touch_last_seen, which deliberately strips this back out before an
    UPDATE so re-seeing an existing job never reassigns its pipeline
    ownership (relevant if the same job_url is ever found by both
    pipelines — whichever inserted it first keeps ownership).
    """
    today = date.today().isoformat()
    return {
        "title": _safe_str(job.get("title"), 500),
        "job_url": job.get("url", ""),
        "company_name": _safe_str(job.get("company"), 300),
        "ats": job.get("source_ats", "unknown"),
        "location": _safe_str(job.get("location"), 500),
        "salary": _safe_str(job.get("salary"), 200),
        "visa_sponsorship": _safe_str(job.get("visa_sponsorship") or "unknown", 50),
        "clearance": job.get("clearance", ""),
        "date_added": today,
        "last_seen": today,
        "is_active": True,
        # 1=Global, 2=Africa, 3=Unsure — set by crawl_i.py's filter_locations().
        # Falls back to 3 (Unsure) for any job that somehow reaches here
        # without it set, rather than silently sorting as if it were tier 0.
        "location_priority": job.get("location_priority", 3),
        "source_pipeline": source_pipeline,
    }


# ── Pre-classification dedup (2026-08) ──────────────────
# Both crawl_i.py and crawl_ii.py now call get_existing_urls() BEFORE
# role/location classification, not just at write time — a job whose URL
# is already in the table is a job we've already classified before;
# sending its title/description through keyword_classify_role/
# ai_classify_roles/ai_classify_locations again is a pure waste of LLM
# calls (and time) for a result we already know. touch_seen_jobs_raw()
# below is what refreshes last_seen/is_active for those skipped jobs
# instead — the cheap, always-available raw-scrape fields (title/company/
# ats/location/salary) are refreshed too, but visa_sponsorship/clearance/
# location_priority are deliberately left untouched (see _build_row_raw)
# since this job was never reclassified this run and we must not
# overwrite its real, previously-computed values with defaults.

def _build_row_raw(job: dict) -> dict:
    """Minimal touch-only row for a job whose URL is ALREADY in the DB and
    is being skipped past classification entirely this run. Omits
    visa_sponsorship/clearance/location_priority/date_added/source_pipeline
    — not even as null — so PostgREST's on_conflict=job_url upsert leaves
    those classification-owned columns exactly as they already are (same
    trick _touch_last_seen uses for date_added/source_pipeline).
    title/job_url/ats are still required even for a touch: they're NOT
    NULL with no default, and Postgres validates NOT NULL against the
    INSERT's full target column list before it ever reaches the ON
    CONFLICT branch — see _touch_last_seen's 2026-08 bug note for the
    full story on why omitting them would fail even on a guaranteed
    update-only upsert."""
    today = date.today().isoformat()
    return {
        "title": _safe_str(job.get("title"), 500),
        "job_url": job.get("url", ""),
        "company_name": _safe_str(job.get("company"), 300),
        "ats": job.get("source_ats", "unknown"),
        "location": _safe_str(job.get("location"), 500),
        "salary": _safe_str(job.get("salary"), 200),
        "last_seen": today,
        "is_active": True,
    }


def touch_seen_jobs_raw(jobs: list[dict]) -> int:
    """Bulk-refresh last_seen/is_active for jobs skipped BEFORE
    classification because their job_url is already known (see
    crawl_i.py's/crawl_ii.py's pre-filter step). Unlike _touch_last_seen
    there's no (job, confidence) pair with real classification output to
    send — these jobs never ran through filter_roles/filter_locations/
    detect_visa_sponsorship this run — so _build_row_raw() is used
    instead of _build_row(), omitting the classification-owned columns
    entirely rather than overwriting them with defaults."""
    if not jobs:
        return 0
    CHUNK = 500
    headers = {**HEADERS, "Prefer": "return=minimal,resolution=merge-duplicates"}
    touched = 0
    for i in range(0, len(jobs), CHUNK):
        chunk = jobs[i:i + CHUNK]
        rows = [_build_row_raw(j) for j in chunk if j.get("url")]
        if not rows:
            continue
        try:
            r = http_requests.post(
                f"{REST}/jobs", headers=headers, json=rows, timeout=60,
                params={"on_conflict": "job_url"},
            )
            r.raise_for_status()
            touched += len(rows)
        except Exception as e:
            detail = ""
            resp = getattr(e, "response", None)
            if resp is not None:
                detail = f" | body: {resp.text[:500]}"
            log.error(f"Supabase touch-upsert (jobs, pre-classification skip) "
                      f"failed for chunk of {len(rows)}: {e}{detail}")
    log.info(f"Touched last_seen for {touched}/{len(jobs)} already-known jobs "
             f"(skipped role/location classification — saved the LLM calls)")
    return touched


# ── archive_i.last_seen repurposing (2026-09) ────────────────────────────
# At the user's explicit instruction, archive_i.last_seen now means "the
# last time this (ats, slug) had ANY role at all found on its board" —
# not "the last time discovery re-confirmed the board/page exists" (the
# old meaning, previously set by node.py's write_ats_hits_to_archive_i on
# every discovery hit, empty board or not). Crawl I (crawl_i.py) is now
# the only thing that touches this column, and only for slugs where
# scrape_board() returned >=1 raw job this run — see crawl_i.py's
# scrape_all()/boards_with_roles for why that's computed from the RAW
# per-board result, before any CSM/AM or Global/Africa filtering (any
# role counts, explicitly including non-CSM roles like a CEO opening).
# Crawl II's equivalent for archive_ii is touch_archive_ii_last_seen()
# below, keyed on website_url instead of (ats, slug).

def touch_archive_i_last_seen(pairs: set[tuple[str, str]]) -> int:
    """Bulk-refresh archive_i.last_seen for every (ats, slug) pair that
    had >=1 role found this run. Upsert payload deliberately omits
    `source` (archive_i's CHECK-constrained column) — same "omit on
    conflict, let Postgres's merge-duplicates leave it untouched" trick
    already used for `jobs.date_added` (see _build_row's docstring) and
    archive_ii.date_added (see node.py's write_career_pages_to_archive_ii)
    — a touch here should never overwrite which discovery source
    originally found this slug. On the (should-never-happen) case this
    on_conflict=ats,slug upsert instead hits a genuinely new row, `source`
    falls back to the column's own DEFAULT ('seed') rather than failing."""
    if not pairs:
        return 0
    CHUNK = 500
    headers = {**HEADERS, "Prefer": "return=minimal,resolution=merge-duplicates"}
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = [{"ats": ats, "slug": slug, "last_seen": now_iso} for ats, slug in pairs]
    touched = 0
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i + CHUNK]
        try:
            r = http_requests.post(
                f"{REST}/archive_i", headers=headers, json=chunk, timeout=60,
                params={"on_conflict": "ats,slug"},
            )
            r.raise_for_status()
            touched += len(chunk)
        except Exception as e:
            detail = ""
            resp = getattr(e, "response", None)
            if resp is not None:
                detail = f" | body: {resp.text[:500]}"
            log.error(f"Supabase touch-upsert (archive_i.last_seen) failed for "
                      f"chunk of {len(chunk)}: {e}{detail}")
    log.info(f"Touched archive_i.last_seen for {touched}/{len(pairs)} slugs with >=1 role found this run")
    return touched


def touch_archive_ii_last_seen(website_urls: set[str]) -> int:
    """archive_ii equivalent of touch_archive_i_last_seen — same
    repurposed meaning ("last time a role was actually found"), same
    omit-on-conflict trick (leaves discovery_method/career_page_url
    untouched), keyed on website_url (archive_ii's identity key, see
    node.py's write_career_pages_to_archive_ii). Called by crawl_ii.py
    for every page whose heuristic extractor found >=1 job posting this
    run."""
    if not website_urls:
        return 0
    CHUNK = 500
    headers = {**HEADERS, "Prefer": "return=minimal,resolution=merge-duplicates"}
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = [{"website_url": url, "last_seen": now_iso} for url in website_urls]
    touched = 0
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i + CHUNK]
        try:
            r = http_requests.post(
                f"{REST}/archive_ii", headers=headers, json=chunk, timeout=60,
                params={"on_conflict": "website_url"},
            )
            r.raise_for_status()
            touched += len(chunk)
        except Exception as e:
            detail = ""
            resp = getattr(e, "response", None)
            if resp is not None:
                detail = f" | body: {resp.text[:500]}"
            log.error(f"Supabase touch-upsert (archive_ii.last_seen) failed for "
                      f"chunk of {len(chunk)}: {e}{detail}")
    log.info(f"Touched archive_ii.last_seen for {touched}/{len(website_urls)} pages with >=1 role found this run")
    return touched


# ── Job insertion ────────────────────────────────────────

def add_jobs_batch(jobs: list[dict], location_confidences: list[str],
                    source_pipeline: str = "crawl_i",
                    existing_urls: set[str] | None = None) -> int:
    """
    Upsert jobs in bulk. New jobs are inserted; existing jobs get
    last_seen and is_active updated.  Returns count of new jobs added.

    `source_pipeline` tags true first-inserts only — see _build_row's
    docstring. Crawl II (crawl_ii.py) calls this with source_pipeline=
    'crawl_ii'; Crawl I leaves the default.

    `existing_urls` (2026-08): pass in a set already fetched by the
    caller's own pre-classification dedup pass to skip ANOTHER full
    get_existing_urls() table scan here — every job reaching this
    function already cleared that pre-filter, so re-fetching would just
    be a second redundant full `jobs` table pull. Still falls back to
    fetching it here (backward compatible) when the caller doesn't have
    one — e.g. any direct/manual call to this function.
    """
    existing = existing_urls if existing_urls is not None else get_existing_urls()
    today = date.today().isoformat()

    new_rows = []
    seen_jobs = []  # (job, confidence) pairs for existing jobs — see _touch_last_seen

    for job, confidence in zip(jobs, location_confidences):
        url = job.get("url", "")
        if not url:
            continue
        if url in existing:
            seen_jobs.append((job, confidence))
        else:
            new_rows.append(_build_row(job, confidence, source_pipeline))
            existing.add(url)  # prevent dupes within this batch

    # ── Bulk insert new jobs (chunks of 100) ──────────────
    added = 0
    for i in range(0, len(new_rows), 100):
        chunk = new_rows[i:i + 100]
        result = _post("jobs", chunk)
        if result is not None:
            added += len(chunk)

    # ── Touch last_seen for existing jobs still active ────
    if seen_jobs:
        _touch_last_seen(seen_jobs, today)

    log.info(f"Added {added} new jobs to Supabase "
             f"({len(seen_jobs)} existing touched, "
             f"{len(jobs) - len(new_rows) - len(seen_jobs)} no-url skipped)")
    return added


def _touch_last_seen(seen_jobs: list[tuple[dict, str]], today: str):
    """Update last_seen and is_active for jobs we saw again this scan.

    Uses a bulk upsert (POST + Prefer: resolution=merge-duplicates, keyed on
    job_url's unique constraint) instead of a PATCH with a job_url IN(...)
    filter. Building that filter by hand embeds raw URLs into the request's
    query string — which is fragile two ways: it can exceed the ~8KB URL
    length Supabase/Kong allows once enough URLs are chunked together, and
    any URL containing a percent-encoded reserved character (e.g. "%22" for
    a literal quote, "%2C" for a comma) gets decoded by the HTTP layer and
    can corrupt the `in.(...)` filter syntax outright (PGRST100 parse
    errors). Sending URLs as normal JSON body values sidesteps both — JSON
    handles escaping correctly by construction.

    CRITICAL BUG FIXED 2026-08: this previously sent upsert rows with ONLY
    job_url/last_seen/is_active. Postgres validates NOT NULL constraints
    against an INSERT...ON CONFLICT statement's target column list before
    it even checks whether a conflict exists — so a row missing `title` or
    `ats` (both NOT NULL, no default) throws a 23502 not-null-violation
    (PostgREST 400) for EVERY row, EVERY time, even though the row always
    already existed and only an UPDATE was ever intended. This was 100%
    silently failing (confirmed live via Supabase edge logs: every
    `on_conflict=job_url` POST returned 400) which meant `last_seen` was
    NEVER being refreshed for previously-seen jobs — so once the
    60-day-stale hard-delete in cleanup_stale_jobs() finally ran, it wiped
    out the entire accumulated backlog in one pass (this is what caused
    the jobs table to go to 0 rows in production). Fix: build full rows
    (same shape as a fresh insert via _build_row) so the upsert is a valid
    standalone row no matter which branch Postgres takes.
    """
    CHUNK = 500
    headers = {
        **HEADERS,
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }

    touched = 0
    for i in range(0, len(seen_jobs), CHUNK):
        chunk = seen_jobs[i:i + CHUNK]
        rows = []
        for job, confidence in chunk:
            row = _build_row(job, confidence)
            row["last_seen"] = today
            row["is_active"] = True
            # merge-duplicates upserts UPDATE every column present in the
            # payload — date_added must NOT be included here, or every
            # daily touch would overwrite a job's true first-seen date
            # with today's date. _build_row() always sets it to today
            # (correct for a genuine new insert); strip it back out here
            # so the UPDATE branch leaves the existing date_added alone.
            del row["date_added"]
            # Same reasoning for source_pipeline (2026-08): _build_row()
            # defaults it to 'crawl_i', but a touch here just means "seen
            # again," not "newly discovered by this pipeline" — stripping
            # it out means the UPDATE branch leaves whichever pipeline
            # actually inserted the row as its owner, untouched.
            del row["source_pipeline"]
            rows.append(row)
        try:
            r = http_requests.post(
                f"{REST}/jobs",
                headers=headers, json=rows, timeout=60,
                params={"on_conflict": "job_url"},
            )
            r.raise_for_status()
            touched += len(chunk)
        except Exception as e:
            detail = ""
            resp = getattr(e, "response", None)
            if resp is not None:
                detail = f" | body: {resp.text[:500]}"
            log.error(f"Supabase touch-upsert (jobs) failed for chunk of {len(chunk)}: {e}{detail}")

    log.info(f"Updated last_seen for {touched}/{len(seen_jobs)} existing jobs")


# ── Stale job cleanup ───────────────────────────────────

def cleanup_stale_jobs(inactive_days: int = 30, delete_days: int = 60,
                        source_pipeline: str | None = None) -> dict:
    """
    Mark jobs inactive if not seen in `inactive_days`.
    Hard-delete jobs not seen in `delete_days`.
    Call AFTER new jobs are inserted so last_seen is current.

    2026-08: `source_pipeline` optionally scopes every step (backfill,
    mark-inactive, hard-delete) to just 'crawl_i' or 'crawl_ii' rows —
    jobs.source_pipeline tags which pipeline wrote each row specifically so
    Crawl I's and Crawl II's finalize passes can run independent cleanup
    policies without one's deletes touching the other's rows. None (the
    default) means no filter — every row, matching the pre-restructure
    behavior for any caller that doesn't pass it.

    Returns a small summary dict (marked_inactive/deleted counts aren't
    exact — PostgREST PATCH/DELETE don't return counts without an extra
    round trip — so this reports the cutoffs used and whether each step's
    request succeeded, which is what the finalize summary actually needs).
    """
    today = date.today()
    pipeline_filter = f"&source_pipeline=eq.{source_pipeline}" if source_pipeline else ""
    summary = {"inactive_cutoff": None, "delete_cutoff": None,
               "mark_inactive_ok": False, "delete_ok": False}

    # 1. Backfill: set last_seen = date_added for old rows that have no last_seen
    _patch(
        "jobs",
        f"last_seen=is.null{pipeline_filter}",
        {"last_seen": today.isoformat()},
    )

    # 2. Mark inactive: last_seen older than inactive_days
    inactive_cutoff = (today - timedelta(days=inactive_days)).isoformat()
    summary["inactive_cutoff"] = inactive_cutoff
    ok = _patch(
        "jobs",
        f"last_seen=lt.{inactive_cutoff}&is_active=eq.true{pipeline_filter}",
        {"is_active": False},
    )
    summary["mark_inactive_ok"] = bool(ok)
    if ok:
        log.info(f"Marked jobs not seen since {inactive_cutoff} as inactive"
                 f"{f' (source_pipeline={source_pipeline})' if source_pipeline else ''}")

    # 3. Hard-delete: last_seen older than delete_days
    delete_cutoff = (today - timedelta(days=delete_days)).isoformat()
    summary["delete_cutoff"] = delete_cutoff
    try:
        r = http_requests.delete(
            f"{REST}/jobs?last_seen=lt.{delete_cutoff}{pipeline_filter}",
            headers=HEADERS, timeout=30,
        )
        r.raise_for_status()
        summary["delete_ok"] = True
        log.info(f"Deleted jobs not seen since {delete_cutoff}"
                 f"{f' (source_pipeline={source_pipeline})' if source_pipeline else ''}")
    except Exception as e:
        log.error(f"Failed to delete stale jobs: {e}")

    return summary


# ── Scan reports ─────────────────────────────────────────

def start_scan_report() -> int | None:
    """Create a new scan report row. Returns the report ID."""
    result = _post("scan_reports", {
        "status": "running",
        "run_date": date.today().isoformat(),
        "started_at": datetime.now(timezone.utc).isoformat(),
    })
    if result and len(result) > 0:
        report_id = result[0]["id"]
        log.info(f"Scan report #{report_id} started")
        return report_id
    return None


def finish_scan_report(
    report_id: int,
    boards_scanned: int = 0,
    boards_failed: int = 0,
    total_jobs_raw: int = 0,
    csm_roles: int = 0,
    global_jobs: int = 0,
    new_jobs_added: int = 0,
    duplicates: int = 0,
    status: str = "completed",
):
    """Update the scan report with final stats."""
    _patch(
        "scan_reports",
        f"id=eq.{report_id}",
        {
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "boards_scanned": boards_scanned,
            "boards_failed": boards_failed,
            "total_jobs_raw": total_jobs_raw,
            "csm_roles": csm_roles,
            "global_jobs": global_jobs,
            "new_jobs_added": new_jobs_added,
            "duplicates": duplicates,
            "status": status,
        },
    )
    log.info(f"Scan report #{report_id} → {status}")
