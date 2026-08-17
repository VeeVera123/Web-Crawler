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
        try:
            r = http_requests.post(
                f"{REST}/slug_registry",
                headers=headers, json=rows, timeout=60,
                params={"on_conflict": "ats,slug"},
            )
            r.raise_for_status()
            total += len(chunk)
        except Exception as e:
            log.error(f"Failed to upsert slug batch: {e}")

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
            f"{REST}/slug_registry",
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
            f"{REST}/slug_registry",
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
            "slug_registry",
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


def _build_row(job: dict, location_confidence: str) -> dict:
    """Build a Supabase row dict from a job dict."""
    today = date.today().isoformat()
    return {
        "title": _safe_str(job.get("title"), 500),
        "job_url": job.get("url", ""),
        "company_name": _safe_str(job.get("company"), 300),
        "ats": job.get("source_ats", "unknown"),
        "location": _safe_str(job.get("location"), 500),
        "country": _safe_str(job.get("country"), 100),
        "department": _safe_str(job.get("department"), 200),
        "workplace_type": _safe_str(job.get("workplace_type"), 100),
        "employment_type": _safe_str(job.get("employment_type"), 100),
        "salary": _safe_str(job.get("salary"), 200),
        "visa_sponsorship": _safe_str(job.get("visa_sponsorship") or "unknown", 50),
        "location_confidence": location_confidence.capitalize(),
        "clearance": job.get("clearance", ""),
        "source_board": _safe_str(job.get("source_type"), 50),
        "date_added": today,
        "last_seen": today,
        "is_active": True,
        # 1=Global, 2=Africa, 3=Unsure — set by main.py's filter_locations().
        # Falls back to 3 (Unsure) for any job that somehow reaches here
        # without it set, rather than silently sorting as if it were tier 0.
        "location_priority": job.get("location_priority", 3),
    }


# ── Job insertion ────────────────────────────────────────

def add_jobs_batch(jobs: list[dict], location_confidences: list[str]) -> int:
    """
    Upsert jobs in bulk. New jobs are inserted; existing jobs get
    last_seen and is_active updated.  Returns count of new jobs added.
    """
    existing = get_existing_urls()
    today = date.today().isoformat()

    new_rows = []
    seen_urls = []

    for job, confidence in zip(jobs, location_confidences):
        url = job.get("url", "")
        if not url:
            continue
        if url in existing:
            seen_urls.append(url)
        else:
            new_rows.append(_build_row(job, confidence))
            existing.add(url)  # prevent dupes within this batch

    # ── Bulk insert new jobs (chunks of 100) ──────────────
    added = 0
    for i in range(0, len(new_rows), 100):
        chunk = new_rows[i:i + 100]
        result = _post("jobs", chunk)
        if result is not None:
            added += len(chunk)

    # ── Touch last_seen for existing jobs still active ────
    if seen_urls:
        _touch_last_seen(seen_urls, today)

    log.info(f"Added {added} new jobs to Supabase "
             f"({len(seen_urls)} existing touched, "
             f"{len(jobs) - len(new_rows) - len(seen_urls)} no-url skipped)")
    return added


def _touch_last_seen(urls: list[str], today: str):
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
    """
    CHUNK = 500
    headers = {
        **HEADERS,
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }

    touched = 0
    for i in range(0, len(urls), CHUNK):
        chunk = urls[i:i + CHUNK]
        rows = [{"job_url": u, "last_seen": today, "is_active": True} for u in chunk]
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

    log.info(f"Updated last_seen for {touched}/{len(urls)} existing jobs")


# ── Stale job cleanup ───────────────────────────────────

def cleanup_stale_jobs(inactive_days: int = 30, delete_days: int = 60):
    """
    Mark jobs inactive if not seen in `inactive_days`.
    Hard-delete jobs not seen in `delete_days`.
    Call AFTER new jobs are inserted so last_seen is current.
    """
    today = date.today()

    # 1. Backfill: set last_seen = date_added for old rows that have no last_seen
    _patch(
        "jobs",
        "last_seen=is.null",
        {"last_seen": today.isoformat()},
    )

    # 2. Mark inactive: last_seen older than inactive_days
    inactive_cutoff = (today - timedelta(days=inactive_days)).isoformat()
    ok = _patch(
        "jobs",
        f"last_seen=lt.{inactive_cutoff}&is_active=eq.true",
        {"is_active": False},
    )
    if ok:
        log.info(f"Marked jobs not seen since {inactive_cutoff} as inactive")

    # 3. Hard-delete: last_seen older than delete_days
    delete_cutoff = (today - timedelta(days=delete_days)).isoformat()
    try:
        r = http_requests.delete(
            f"{REST}/jobs?last_seen=lt.{delete_cutoff}",
            headers=HEADERS, timeout=30,
        )
        r.raise_for_status()
        log.info(f"Deleted jobs not seen since {delete_cutoff}")
    except Exception as e:
        log.error(f"Failed to delete stale jobs: {e}")


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
