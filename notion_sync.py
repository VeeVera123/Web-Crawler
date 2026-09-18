"""
Notion sync — the working-set mirror described in the 2026-09 architecture
discussion. Two one-way passes, both server-side (GitHub Actions), both
calling Notion's REST API directly — no Edge Function/relay needed for
either, since neither side of this is ever a browser (that's the only
reason a CORS relay was ever on the table).

2026-09 (second pass, at explicit user instruction): split OUT of
crawl_i.py/crawl_ii.py entirely and into two standalone CI steps that
bookend the actual crawl shards — see prefix_supabase.py and
postfix_notion.py. crawl_i.py/crawl_ii.py no longer import this module at
all; they're back to being pure Supabase writers. Reasoning: each shard
runs as its own GitHub Actions job with no shared memory, so "push
whatever THIS shard just inserted" doesn't compose cleanly across N
parallel shards — a single step that runs once, after every shard is
done, is simpler and matches the mental model ("Prefix - Supabase" reads
Notion writes Supabase; crawl-i/crawl-ii only ever touch Supabase;
"Postfix - Notion" reads Supabase writes Notion, then does the regular
cleanup).

  1. sync_notion_statuses_to_supabase() — called once by prefix_supabase.py
     BEFORE any shard starts. Reads every page's Status property out of
     the Notion working-set database, writes whatever isn't "Not Applied"
     back into Supabase's jobs.application_status. Applied/Rejected/etc.
     pages are left in Notion (not archived) per explicit instruction —
     this only ever reads Notion and writes Supabase, never touches the
     Notion page itself.

  2. push_pending_jobs_to_notion() — called once by postfix_notion.py
     AFTER every crawl-i/crawl-ii shard has finished. Reads every
     Supabase row that's never been pushed to Notion (jobs.notion_synced_at
     IS NULL — see supabase_handler.get_jobs_pending_notion_sync()),
     creates one Notion page per row, then stamps notion_synced_at on the
     ones that succeeded. Using that marker instead of "added today"
     means this is correct no matter how many times a day the whole
     pipeline runs — a row is only ever offered here once, however many
     runs it takes postfix to actually catch it (see that function's
     docstring). Seven fields are ever written to a page, by explicit
     instruction (Company Name added 2026-09 at explicit request): title,
     company_name, job_url, date_added, salary, role_category, and the
     Supabase id (the join key step 1 reads back). ATS/location/etc. are
     deliberately left alone.

     NOTE ON NOTION PROPERTY NAMES: Notion treats property names as exact,
     case-sensitive strings — "Status" and "status" are two different
     properties as far as the API is concerned, and a page create/query
     against a name that doesn't match EXACTLY what's in the database
     either silently no-ops that field or gets rejected outright. The
     PROP_* constants below are only a best-guess starting point — 2026-09
     real-world testing hit live 400s ("Job Title is expected to be
     rich_text", "Job URL is not a property that exists", "Status is not
     a property that exists") proving a hardcoded name/type assumption is
     too fragile to rely on. So as of that fix, this module fetches the
     database's LIVE schema once per process (_get_schema()) and checks
     every property against it before ever including it in a request:
       - the title-type property (every Notion database has exactly one,
         it can be named anything) is auto-detected by type rather than
         assumed to be named "Job Title" — see _title_property_name().
       - every other PROP_* constant is looked up by name in the live
         schema; if it's missing, or present under a different type than
         expected, that single field is skipped with a warning instead of
         either corrupting the request or taking the whole page-create
         down with a 400. Get the name/type mismatch logged, fix the
         PROP_* constant (or the Notion column) to match, re-run — no
         data is lost in the meantime since the row simply isn't marked
         notion_synced_at until its page is actually created.

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
PROP_TITLE = "Job Title"  # only used as a fallback label in logs — the
                          # real title property is auto-detected by TYPE
                          # at runtime, see _title_property_name()
PROP_URL = "Job URL"
PROP_COMPANY_NAME = "Company Name"
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


_schema_cache: dict | None = None


def _get_schema(force: bool = False) -> dict:
    """Fetches the Notion database's live property schema
    (name -> {"type": ..., ...}) and caches it for the rest of this
    process — one extra API call per run, not per page. This is what lets
    every other function here check a property's real name/type before
    using it instead of trusting the PROP_* constants blindly (see the
    module docstring's "NOTE ON NOTION PROPERTY NAMES")."""
    global _schema_cache
    if _schema_cache is not None and not force:
        return _schema_cache
    data = _request("GET", f"/databases/{NOTION_DATABASE_ID}")
    if data is None:
        log.warning("Could not fetch Notion database schema — "
                     "property name/type checks will be skipped this run")
        _schema_cache = {}
    else:
        _schema_cache = data.get("properties", {}) or {}
    return _schema_cache


def _title_property_name(schema: dict) -> str | None:
    """Every Notion database has exactly one property of type 'title',
    and it can be named anything — this finds it by type instead of
    assuming it's named PROP_TITLE. Falls back to PROP_TITLE only if the
    schema fetch itself failed (empty schema), so a page-create attempt
    still goes out rather than silently doing nothing."""
    for name, meta in schema.items():
        if meta.get("type") == "title":
            return name
    return PROP_TITLE if not schema else None


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

    schema = _get_schema()
    if schema:
        for prop_name, expected_type in ((PROP_SUPABASE_ID, "number"), (PROP_STATUS, "select")):
            meta = schema.get(prop_name)
            if meta is None:
                log.warning(f"Notion property {prop_name!r} not found in database schema — "
                             f"status reconciliation will not see it on any page")
            elif meta.get("type") != expected_type:
                log.warning(f"Notion property {prop_name!r} is type {meta.get('type')!r}, "
                             f"expected {expected_type!r} — status reconciliation will not see it")

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

def _build_page_properties(row: dict, schema: dict) -> dict:
    """Builds the page-create payload's "properties" object, checking
    every field against the database's live schema first (see
    _get_schema()) instead of trusting the PROP_* constants blindly. A
    field whose name isn't in the schema, or whose type doesn't match
    what's expected, is skipped with a warning rather than sent anyway —
    that's what turns a single wrong property name into "one field
    missing from this page, logged" instead of "this whole page create
    400s and the row never syncs at all"."""
    props: dict = {}

    def _add(prop_name: str, expected_type: str, value):
        meta = schema.get(prop_name)
        if meta is None:
            log.warning(f"Notion property {prop_name!r} not found in database schema — "
                         f"skipping this field (row id {row.get('id')})")
            return
        if meta.get("type") != expected_type:
            log.warning(f"Notion property {prop_name!r} is type {meta.get('type')!r}, "
                         f"expected {expected_type!r} — skipping this field (row id {row.get('id')})")
            return
        props[prop_name] = value

    title_prop = _title_property_name(schema)
    if title_prop:
        props[title_prop] = {"title": [{"text": {"content": (row.get("title") or "")[:2000]}}]}
    else:
        log.warning("Notion database has no title-type property — page create will likely fail")

    _add(PROP_URL, "url", {"url": row.get("job_url") or None})
    _add(PROP_SUPABASE_ID, "number", {"number": row.get("id")})
    _add(PROP_STATUS, "select", {"select": {"name": STATUS_NOT_APPLIED}})

    company_name = row.get("company_name")
    if company_name:
        _add(PROP_COMPANY_NAME, "rich_text", {"rich_text": [{"text": {"content": company_name[:2000]}}]})

    date_added = row.get("date_added")
    if date_added:
        _add(PROP_DATE_ADDED, "date", {"date": {"start": date_added}})

    salary = row.get("salary")
    if salary:
        _add(PROP_SALARY, "rich_text", {"rich_text": [{"text": {"content": salary[:2000]}}]})

    role_category = row.get("role_category")
    if role_category:
        _add(PROP_ROLE_CATEGORY, "select", {"select": {"name": role_category}})

    return props


def push_pending_jobs_to_notion() -> dict:
    """Called once by postfix_notion.py, after every crawl-i/crawl-ii
    shard has finished. Fetches every Supabase row with no Notion page
    yet (see supabase_handler.get_jobs_pending_notion_sync()), creates a
    page for each, and marks the successful ones synced so they're never
    offered again — see this module's docstring for why that marker
    (rather than "added today") is what makes this safe to run any
    number of times a day."""
    from supabase_handler import get_jobs_pending_notion_sync, mark_notion_synced

    summary = {"attempted": 0, "created": 0}
    if not _configured():
        return summary

    pending = get_jobs_pending_notion_sync()
    summary["attempted"] = len(pending)
    if not pending:
        return summary

    schema = _get_schema()

    created_ids = []
    for row in pending:
        body = {
            "parent": {"database_id": NOTION_DATABASE_ID},
            "properties": _build_page_properties(row, schema),
        }
        result = _request("POST", "/pages", body)
        if result is not None:
            summary["created"] += 1
            created_ids.append(row["id"])

    if created_ids:
        mark_notion_synced(created_ids)
    return summary
