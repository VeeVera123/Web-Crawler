"""
Notion API handler — writes filtered jobs, deduplicates by URL.
"""

import re
import logging
import requests as http_requests
from datetime import date
from notion_client import Client
import os
from dotenv import load_dotenv
load_dotenv()
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
JOBS_DB_ID = os.environ.get("JOBS_DB_ID", "")

log = logging.getLogger(__name__)
notion = Client(auth=NOTION_TOKEN)


def _format_db_id(raw_id: str) -> str:
    """Ensure database ID is in UUID format with hyphens."""
    clean = raw_id.replace("-", "")
    if len(clean) == 32:
        return f"{clean[:8]}-{clean[8:12]}-{clean[12:16]}-{clean[16:20]}-{clean[20:]}"
    return raw_id


def get_existing_urls() -> set[str]:
    """Pull all job URLs already in the database to avoid duplicates."""
    urls = set()
    db_id = _format_db_id(JOBS_DB_ID)
    has_more = True
    start_cursor = None

    while has_more:
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor

        try:
            # Use raw HTTP to avoid notion-client URL formatting issues
            resp = http_requests.post(
                f"https://api.notion.com/v1/databases/{db_id}/query",
                headers={
                    "Authorization": f"Bearer {NOTION_TOKEN}",
                    "Notion-Version": "2022-06-28",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=30,
            )
            resp.raise_for_status()
            response = resp.json()
        except Exception as e:
            log.error(f"Failed to query Notion: {e}")
            break

        for page in response.get("results", []):
            props = page.get("properties", {})
            url_prop = props.get("Job URL", {})
            url_val = url_prop.get("url", "")
            if url_val:
                urls.add(url_val)

        has_more = response.get("has_more", False)
        start_cursor = response.get("next_cursor")

    log.info(f"Found {len(urls)} existing jobs in Notion")
    return urls


def add_job(
    job: dict,
    location_confidence: str = "match",
) -> bool:
    """
    Add a single job to the Notion jobs database.
    Returns True on success.
    """
    properties = {
        "Job Title": {"title": [{"text": {"content": job.get("title", "")[:200]}}]},
        "Company": {"rich_text": [{"text": {"content": job.get("company", "")[:200]}}]},
        "Job URL": {"url": job.get("url", "") or None},
        "Location": {"rich_text": [{"text": {"content": job.get("location", "")[:200]}}]},
        "Department": {"rich_text": [{"text": {"content": job.get("department", "")[:200]}}]},
        "ATS": {"select": {"name": job.get("source_ats", "Unknown")}},
        "Date Added": {"date": {"start": date.today().isoformat()}},
        "Location Confidence": {"select": {"name": location_confidence.capitalize()}},
    }

    # Optional fields
    if job.get("salary"):
        properties["Salary"] = {"rich_text": [{"text": {"content": job["salary"][:200]}}]}
    if job.get("workplace_type"):
        properties["Workplace Type"] = {"rich_text": [{"text": {"content": job["workplace_type"][:100]}}]}
    if job.get("employment_type"):
        properties["Employment Type"] = {"rich_text": [{"text": {"content": job["employment_type"][:100]}}]}
    if job.get("country"):
        properties["Country"] = {"rich_text": [{"text": {"content": job["country"][:100]}}]}

    try:
        notion.pages.create(parent={"database_id": JOBS_DB_ID}, properties=properties)
        return True
    except Exception as e:
        log.error(f"Failed to add job '{job.get('title', '')}': {e}")
        return False


def add_jobs_batch(jobs: list[dict], location_confidences: list[str]) -> int:
    """Add multiple jobs. Returns count of successfully added."""
    existing = get_existing_urls()
    added = 0

    for job, confidence in zip(jobs, location_confidences):
        url = job.get("url", "")
        if url in existing:
            continue
        if add_job(job, confidence):
            added += 1
            existing.add(url)

    log.info(f"Added {added} new jobs to Notion")
    return added
