"""
Supabase handler — writes filtered jobs to PostgreSQL via REST API.
Handles deduplication by job URL, upserts companies, and logs scan reports.
"""

import logging
from datetime import date, datetime, timezone

import requests as http_requests

from config import SUPABASE_URL, SUPABASE_KEY

log = logging.getLogger(__name__)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

REST = f"{SUPABASE_URL}/rest/v1"


# ── Helpers ──────────────────────────────────────────────

def _post(table: str, data: dict | list[dict], upsert_cols: str = "") -> dict | list | None:
    """POST to Supabase REST API. Returns parsed JSON or None on error."""
    headers = {**HEADERS}
    if upsert_cols:
        headers["Prefer"] = f"return=representation,resolution=merge-duplicates"
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
        log.error(f"Supabase PATCH {table} failed: {e}")
        return False


def _get(table: str, params: str = "", limit: int = 10000) -> list[dict]:
    """GET rows from a table."""
    url = f"{REST}/{table}?{params}&limit={limit}" if params else f"{REST}/{table}?limit={limit}"
    try:
        r = http_requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"Supabase GET {table} failed: {e}")
        return []


# ── Companies ────────────────────────────────────────────

def upsert_company(ats: str, slug: str, name: str = "") -> int | None:
    """Insert or update a company. Returns company ID."""
    data = {
        "ats": ats,
        "slug": slug,
        "name": name or slug,
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }
    headers = {
        **HEADERS,
        "Prefer": "return=representation,resolution=merge-duplicates",
    }
    try:
        r = http_requests.post(
            f"{REST}/companies",
            headers=headers, json=data, timeout=30,
            params={"on_conflict": "ats,slug"},
        )
        r.raise_for_status()
        rows = r.json()
        if rows and len(rows) > 0:
            return rows[0]["id"]
    except Exception as e:
        log.error(f"Failed to upsert company {ats}/{slug}: {e}")
    return None


# ── Deduplication ────────────────────────────────────────

def get_existing_urls() -> set[str]:
    """Pull all job URLs already in the database to avoid duplicates."""
    urls = set()
    offset = 0
    batch_size = 10000

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

    log.info(f"Found {len(urls)} existing jobs in Supabase")
    return urls


# ── Job insertion ────────────────────────────────────────

def add_job(job: dict, location_confidence: str = "Match", company_id: int | None = None) -> bool:
    """Insert a single job. Returns True on success."""
    row = {
        "title": (job.get("title") or "")[:500],
        "job_url": job.get("url", ""),
        "company_name": (job.get("company") or "")[:300],
        "ats": job.get("source_ats", "unknown"),
        "location": (job.get("location") or "")[:500],
        "country": (job.get("country") or "")[:100],
        "department": (job.get("department") or "")[:200],
        "workplace_type": (job.get("workplace_type") or "")[:100],
        "employment_type": (job.get("employment_type") or "")[:100],
        "salary": (job.get("salary") or "")[:200],
        "description": (job.get("description") or "")[:50000],
        "location_confidence": location_confidence.capitalize(),
        "date_added": date.today().isoformat(),
    }
    if company_id:
        row["company_id"] = company_id

    result = _post("jobs", row)
    return result is not None


def add_jobs_batch(jobs: list[dict], location_confidences: list[str]) -> int:
    """
    Add multiple jobs with deduplication.
    Returns count of successfully added jobs.
    """
    existing = get_existing_urls()
    added = 0
    skipped = 0

    # Cache company IDs to avoid repeated upserts
    company_cache: dict[tuple[str, str], int | None] = {}

    for job, confidence in zip(jobs, location_confidences):
        url = job.get("url", "")
        if not url or url in existing:
            skipped += 1
            continue

        # Upsert company
        ats = job.get("source_ats", "unknown")
        slug = job.get("slug", "")
        cache_key = (ats, slug)
        if cache_key not in company_cache:
            company_cache[cache_key] = upsert_company(ats, slug, job.get("company", ""))
        company_id = company_cache[cache_key]

        if add_job(job, confidence, company_id):
            added += 1
            existing.add(url)

    log.info(f"Added {added} new jobs to Supabase ({skipped} duplicates skipped)")
    return added


# ── Scan reports ─────────────────────────────────────────

def start_scan_report() -> int | None:
    """Create a new scan report row. Returns the report ID."""
    result = _post("scan_reports", {"status": "running"})
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
