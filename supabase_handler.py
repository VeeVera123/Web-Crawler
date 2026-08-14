"""
Supabase handler — writes filtered jobs to PostgreSQL via REST API.
Async (httpx.AsyncClient) with a shared connection pool.

Note: populate_slug_registry stays SYNCHRONOUS on purpose so the weekly
enrich_slugs.py script keeps working without modification.
"""
import logging
import asyncio
from datetime import date, datetime, timedelta, timezone
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
log = logging.getLogger(__name__)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}
REST = f"{SUPABASE_URL}/rest/v1"


# ── Shared async client (connection pooling) ───────────
_client: httpx.AsyncClient | None = None
_client_lock = asyncio.Lock()


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        async with _client_lock:
            if _client is None:
                _client = httpx.AsyncClient(
                    timeout=30,
                    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
                )
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


# ── Async REST helpers ─────────────────────────────────
async def _post(table: str, data: dict | list[dict], upsert: bool = False):
    client = await _get_client()
    headers = {**HEADERS}
    if upsert:
        headers["Prefer"] = "return=representation,resolution=merge-duplicates"
    try:
        r = await client.post(f"{REST}/{table}", headers=headers, json=data)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"Supabase POST {table} failed: {e}")
        return None


async def _patch(table: str, filters: str, data: dict) -> bool:
    client = await _get_client()
    try:
        r = await client.patch(f"{REST}/{table}?{filters}", headers=HEADERS, json=data)
        r.raise_for_status()
        return True
    except Exception as e:
        log.error(f"Supabase PATCH {table} failed: {e}")
        return False


async def _get(table: str, params: str = "", limit: int = 10000) -> list[dict]:
    client = await _get_client()
    url = f"{REST}/{table}?{params}&limit={limit}" if params else f"{REST}/{table}?limit={limit}"
    try:
        r = await client.get(url, headers=HEADERS)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"Supabase GET {table} failed: {e}")
        return []


async def _delete(table: str, filters: str) -> bool:
    client = await _get_client()
    try:
        r = await client.delete(f"{REST}/{table}?{filters}", headers=HEADERS)
        r.raise_for_status()
        return True
    except Exception as e:
        log.error(f"Supabase DELETE {table} failed: {e}")
        return False


# ── Slug Registry (SYNC — used by enrich_slugs.py) ─────
def populate_slug_registry(slugs: list[tuple[str, str]], source: str = "seed") -> int:
    """Upsert slugs into slug_registry. Synchronous (weekly offline script)."""
    if not slugs:
        return 0
    total = 0
    chunk_size = 500
    headers = {**HEADERS, "Prefer": "return=minimal,resolution=merge-duplicates"}
    with httpx.Client(timeout=60) as client:
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
            try:
                r = client.post(
                    f"{REST}/slug_registry",
                    headers=headers, json=rows,
                    params={"on_conflict": "ats,slug"},
                )
                r.raise_for_status()
                total += len(chunk)
            except Exception as e:
                log.error(f"Failed to upsert slug batch: {e}")
    log.info(f"Slug registry: upserted {total} slugs (source={source})")
    return total


async def get_all_slugs() -> list[tuple[str, str]]:
    """Fetch all (ats, slug) pairs from slug_registry (paginated)."""
    pairs = []
    offset = 0
    batch_size = 1000
    while True:
        rows = await _get("slug_registry", f"select=ats,slug&offset={offset}", limit=batch_size)
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
async def get_existing_urls() -> set[str]:
    """Pull all job URLs already in the database to avoid duplicates."""
    urls = set()
    offset = 0
    batch_size = 1000
    while True:
        rows = await _get("jobs", f"select=job_url&offset={offset}", limit=batch_size)
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


# ── Safe string helpers ─────────────────────────────────
def _safe_str(val, max_len: int = 500) -> str:
    if val is None:
        return ""
    if isinstance(val, list):
        val = ", ".join(str(x) for x in val)
    return str(val)[:max_len]


def _build_row(job: dict, location_confidence: str) -> dict:
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
        "date_added": today,
        "last_seen": today,
        "is_active": True,
    }


# ── Job insertion ────────────────────────────────────────
async def add_jobs_batch(jobs: list[dict], location_confidences: list[str]) -> int:
    """Upsert jobs in bulk. Returns count of new jobs added."""
    existing = await get_existing_urls()
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
            existing.add(url)

    added = 0
    for i in range(0, len(new_rows), 100):
        chunk = new_rows[i:i + 100]
        result = await _post("jobs", chunk)
        if result is not None:
            added += len(chunk)

    if seen_urls:
        await _touch_last_seen(seen_urls, today)

    log.info(f"Added {added} new jobs to Supabase "
             f"({len(seen_urls)} existing touched, "
             f"{len(jobs) - len(new_rows) - len(seen_urls)} no-url skipped)")
    return added


async def _touch_last_seen(urls: list[str], today: str):
    """Update last_seen and is_active for jobs we saw again this scan."""
    for i in range(0, len(urls), 200):
        chunk = urls[i:i + 200]
        encoded = ",".join(f'"{u}"' for u in chunk)
        filters = f"job_url=in.({encoded})"
        await _patch("jobs", filters, {"last_seen": today, "is_active": True})
    log.info(f"Updated last_seen for {len(urls)} existing jobs")


# ── Stale job cleanup ───────────────────────────────────
async def cleanup_stale_jobs(inactive_days: int = 30, delete_days: int = 60):
    """Mark inactive + hard-delete stale jobs."""
    today = date.today()

    await _patch("jobs", "last_seen=is.null", {"last_seen": today.isoformat()})

    inactive_cutoff = (today - timedelta(days=inactive_days)).isoformat()
    ok = await _patch("jobs", f"last_seen=lt.{inactive_cutoff}&is_active=eq.true",
                      {"is_active": False})
    if ok:
        log.info(f"Marked jobs not seen since {inactive_cutoff} as inactive")

    delete_cutoff = (today - timedelta(days=delete_days)).isoformat()
    if await _delete("jobs", f"last_seen=lt.{delete_cutoff}"):
        log.info(f"Deleted jobs not seen since {delete_cutoff}")


# ── Scan reports ─────────────────────────────────────────
async def start_scan_report() -> int | None:
    result = await _post("scan_reports", {
        "status": "running",
        "run_date": date.today().isoformat(),
        "started_at": datetime.now(timezone.utc).isoformat(),
    })
    if result and len(result) > 0:
        report_id = result[0]["id"]
        log.info(f"Scan report #{report_id} started")
        return report_id
    return None


async def finish_scan_report(
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
    await _patch("scan_reports", f"id=eq.{report_id}", {
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "boards_scanned": boards_scanned,
        "boards_failed": boards_failed,
        "total_jobs_raw": total_jobs_raw,
        "csm_roles": csm_roles,
        "global_jobs": global_jobs,
        "new_jobs_added": new_jobs_added,
        "duplicates": duplicates,
        "status": status,
    })
    log.info(f"Scan report #{report_id} → {status}")
