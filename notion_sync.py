"""
Notion sync — the working-set mirror described in the 2026-09 architecture
discussion. Two one-way passes, both server-side (GitHub Actions), both
calling Notion's REST API directly — no Edge Function/relay needed for
either, since neither side of this is ever a browser (that's the only
reason a CORS relay was ever on the table).

  1. sync_notion_statuses_to_supabase() — read every page's Status
     property out of the Notion working-set database, write whatever
     isn't "Not Applied" back into Supabase's jobs.application_status.
     Applied/Rejected/etc. pages are left in Notion (not archived) per
     explicit instruction — this only ever reads Notion and writes
     Supabase, never touches the Notion page itself.

  2. push_new_jobs_to_notion(new_rows) — for jobs that were JUST inserted
     into Supabase this run (see supabase_handler.add_jobs_batch, which
     now hands back each new row's id), create one Notion page per job.
     ONLY six fields are ever written here, by explicit instruction:
     title, job_url, date_added, salary, role_category, and the
     Supabase id (the join key step 1 reads back). Company/ATS/location/
     etc. are deliberately left alone.

Both passes are best-effort and self-disabling: if NOTION_TOKEN or
NOTION_DATABASE_ID isn't set, each logs one clear line and returns
immediately rather than raising — so a crawl run works exactly as before
until the Notion side of this is actually configured, and a Notion outage
never takes the crawl down with it.

Volume here is tiny (the whole point of "Supabase already dedups, only
genuinely-new jobs ever reach Notion" — see the architecture discussion):
a handful to a few dozen pages per run, not thousands. So this stays
deliberately simple — one request at a time, no concurrency, a plain
sleep to stay well under Notion's ~3 req/sec limit, with backoff only on
an actual 429. If daily volume ever grows enough for that to matter,
that's the point to revisit, not before.
"""

import logging
import os
import time

import requests

log = logging.getLogger("notion_sync")

NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "")
NOTION_VERSION = "2022-06-28"
NOTION_API = "https://api.notion.com/v1"

# Property names as they must exist on the Notion database — create these
# once by hand in Notion's UI (Status/Role Category as "Select", Supabase
# ID as "Number", the rest as their obvious types). Nothing here creates
# or alters Notion's schema; a missing property just makes that one
# page's create/read a no-op for that field, logged, not fatal.
PROP_TITLE = "Job Title"
PROP_URL = "Job URL"
PROP_DATE_ADDED = "Date Added"
PROP_SALARY = "Salary"
PROP_ROLE_CATEGORY = "Role Category"
PROP_SUPABASE_ID = "Supabase ID"
PROP_STATUS = "Status"

STATUS_NOT_APPLIED = "Not Applied"

# Notion Status select label -> Supabase jobs.application_status value
# (the CHECK constraint on that column only allows these five).
_STATUS_TO_SUPABASE = {
    "Not Applied": "not_applied",
    "Applied": "applied",
    "Interviewing": "under_review",
    "Offer": "accepted",
    "Rejected": "rejected",
}

_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

_MIN_REQUEST_GAP = 0.35  # seconds — keeps sequential calls comfortably under 3/sec
_last_request_at = 0.0


def _configured() -> bool:
    if NOTION_TOKEN and NOTION_DATABASE_ID:
        return True
    log.info("Notion sync skipped — NOTION_TOKEN/NOTION_DATABASE_ID not set")
    return False


def _throttle():
    global _last_request_at
    wait = _MIN_REQUEST_GAP - (time.monotonic() - _last_request_at)
    if wait > 0:
        time.sleep(wait)
    _last_request_at = time.monotonic()


def _request(method: str, path: str, json_body: dict | None = None, max_retries: int = 3):
    """One Notion API call with pacing + 429 backoff. Returns parsed JSON
    on success, None on a failure that isn't worth retrying further
    (logged either way) — callers treat None as "skip this item."""
    url = f"{NOTION_API}{path}"
    for attempt in range(max_retries):
        _throttle()
        try:
            r = requests.request(method, url, headers=_HEADERS, json=json_body, timeout=30)
        except requests.RequestException as e:
            log.warning(f"Notion {method} {path} network error: {e}")
            return None
        if r.status_code == 429 and attempt < max_retries - 1:
            wait = float(r.headers.get("Retry-After", "1")) + 0.5
            log.warning(f"Notion rate-limited, retrying in {wait:.1f}s")
            time.sleep(wait)
            continue
        if not r.ok:
            log.warning(f"Notion {method} {path} failed: HTTP {r.status_code} — {r.text[:300]}")
            return None
        return r.json()
    return None


# ── Step 1: Notion statuses → Supabase ──────────────────────────────────

def sync_notion_statuses_to_supabase() -> dict:
    """Reads every page currently in the Notion working-set database,
    writes any non-"Not Applied" status back into Supabase, keyed by the
    Supabase ID stamped on each page at creation. Returns a small summary
    dict for the caller's own end-of-run log line."""
    summary = {"pages_read": 0, "statuses_synced": 0}
    if not _configured():
        return summary

    from supabase_handler import update_application_statuses_bulk

    log.info("── Notion: reconciling statuses back to Supabase ──")
    updates = []
    cursor = None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        data = _request("POST", f"/databases/{NOTION_DATABASE_ID}/query", body)
        if data is None:
            break
        results = data.get("results", [])
        summary["pages_read"] += len(results)
        for page in results:
            props = page.get("properties", {})
            supabase_id = (props.get(PROP_SUPABASE_ID) or {}).get("number")
            status_obj = (props.get(PROP_STATUS) or {}).get("select") or {}
            status_label = status_obj.get("name", STATUS_NOT_APPLIED)
            if supabase_id is None or status_label == STATUS_NOT_APPLIED:
                continue
            mapped = _STATUS_TO_SUPABASE.get(status_label)
            if not mapped:
                log.warning(f"Notion page has unrecognized Status {status_label!r} — skipping")
                continue
            updates.append({"id": supabase_id, "application_status": mapped})
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    if updates:
        summary["statuses_synced"] = update_application_statuses_bulk(updates)
    log.info(f"  {summary['pages_read']} Notion pages read, "
             f"{summary['statuses_synced']} statuses synced to Supabase")
    return summary


# ── Step 2: new Supabase rows → Notion ──────────────────────────────────

def _build_page_properties(row: dict) -> dict:
    props = {
        PROP_TITLE: {"title": [{"text": {"content": (row.get("title") or "")[:2000]}}]},
        PROP_URL: {"url": row.get("job_url") or None},
        PROP_SUPABASE_ID: {"number": row.get("id")},
        PROP_STATUS: {"select": {"name": STATUS_NOT_APPLIED}},
    }
    date_added = row.get("date_added")
    if date_added:
        props[PROP_DATE_ADDED] = {"date": {"start": date_added}}
    salary = row.get("salary")
    if salary:
        props[PROP_SALARY] = {"rich_text": [{"text": {"content": salary[:2000]}}]}
    role_category = row.get("role_category")
    if role_category:
        props[PROP_ROLE_CATEGORY] = {"select": {"name": role_category}}
    return props


def push_new_jobs_to_notion(new_rows: list[dict]) -> dict:
    """new_rows: the actual Supabase rows just inserted this run (from
    supabase_handler.add_jobs_batch's second return value) — each already
    has its Supabase `id`. Creates one Notion page per row. Only the six
    fields named in this module's docstring are ever written."""
    summary = {"attempted": len(new_rows), "created": 0}
    if not new_rows or not _configured():
        return summary

    log.info(f"── Notion: pushing {len(new_rows)} new job(s) ──")
    for row in new_rows:
        body = {
            "parent": {"database_id": NOTION_DATABASE_ID},
            "properties": _build_page_properties(row),
        }
        result = _request("POST", "/pages", body)
        if result is not None:
            summary["created"] += 1
    log.info(f"  {summary['created']}/{summary['attempted']} new jobs created in Notion")
    return summary
