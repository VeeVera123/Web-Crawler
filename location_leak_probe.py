"""Standalone, one-off diagnostic -- NOT part of the production pipeline.

Explicit user report: a real crawl run just wrote rows with locations
like "New York, NY", "Bangalore, India", a raw Austrian street address,
"Kenya", "Brazil", "South Africa" -- pulls the REAL rows for these from
Supabase (title, role_category, source_ats, source_pipeline, rank/
location_priority, clearance, and description_snippet) so the actual
cause (which rank let each one in, and why) can be diagnosed from real
production data instead of guessed.
"""
import os
import sys
import json

import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

NEEDLES = [
    "New York",
    "Bangalore",
    "Thalgau",
    "Kenya",
    "Brazil",
    "South Africa",
]

COLUMNS = (
    "id,title,location,role_category,ats,source_pipeline,"
    "clearance,location_priority,job_url,date_added,last_seen,is_active"
)


def query(needle: str):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/jobs",
        params={
            "select": COLUMNS,
            "location": f"ilike.*{needle}*",
            "order": "date_added.desc",
            "limit": 10,
        },
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def main():
    for needle in NEEDLES:
        rows = query(needle)
        print(f"\n{'=' * 100}\n{needle} -- {len(rows)} row(s)\n{'=' * 100}")
        for row in rows:
            print(f"  id={row.get('id')} title={row.get('title')!r}")
            print(f"    location={row.get('location')!r}")
            print(f"    role_category={row.get('role_category')!r} ats={row.get('ats')!r} "
                  f"source_pipeline={row.get('source_pipeline')!r}")
            print(f"    clearance={row.get('clearance')!r} location_priority={row.get('location_priority')!r}")
            print(f"    job_url={row.get('job_url')!r}")
            print(f"    date_added={row.get('date_added')!r} last_seen={row.get('last_seen')!r} "
                  f"is_active={row.get('is_active')!r}")
            print()


if __name__ == "__main__":
    main()
