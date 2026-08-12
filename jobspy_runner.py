"""
JobSpy Test Runner — Standalone
================================
Scrapes Indeed + Google Jobs for CSM/Account Management roles
using python-jobspy. Stores results in the jobs_jobspy table
(separate from the main scanner's jobs table).

Usage:
    pip install python-jobspy
    python jobspy_runner.py                    # default search
    python jobspy_runner.py --sites indeed     # indeed only
    python jobspy_runner.py --sites google     # google only
    python jobspy_runner.py --dry-run          # print results, don't write to DB
"""

import argparse
import logging
import os
import re
from datetime import date

import requests as http_requests
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

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}
REST = f"{SUPABASE_URL}/rest/v1"

# ── Search queries ──────────────────────────────────────
# Multiple search terms to cast a wide net for CSM/AM roles

SEARCH_TERMS = [
    "customer success manager remote",
    "account manager remote",
    "client success manager remote",
    "customer success lead remote",
    "head of customer success remote",
    "VP customer success remote",
    "director customer success remote",
    "strategic account manager remote",
    "enterprise customer success remote",
    "customer success partner remote",
]

# Google needs its own format
GOOGLE_SEARCH_TERMS = [
    "customer success manager remote jobs worldwide",
    "account manager remote jobs international",
    "client success manager remote jobs global",
    "customer success lead remote jobs",
    "head of customer success remote jobs",
    "director customer success remote jobs",
]

# ── Role filter (reuse logic from classifier) ──────────
CSM_KEYWORDS = [
    r"\bcustomer\s+success",
    r"\bclient\s+success",
    r"\baccount\s+manag",
    r"\baccount\s+executive",
    r"\bkey\s+account",
    r"\bstrategic\s+account",
    r"\benterprise\s+account",
    r"\bcustomer\s+experience\s+manager",
    r"\bcustomer\s+relationship\s+manager",
    r"\bcustomer\s+engagement\s+manager",
    r"\bpartner\s+success",
    r"\bpartner\s+manager",
    r"\bcustomer\s+advocate",
    r"\brenewals?\s+manager",
    r"\bretention\s+manager",
    r"\bonboarding\s+manager",
    r"\bpost[\-\s]?sales?",
    r"\bcsm\b",
    r"\bvp.{0,10}customer\s+success",
    r"\bdirector.{0,10}customer\s+success",
    r"\bhead\s+of\s+customer\s+success",
]

CSM_RE = re.compile("|".join(CSM_KEYWORDS), re.IGNORECASE)

EXCLUDE_RE = re.compile(
    r"\b(cashier|janitor|warehouse|driver|nurse|mechanic|cook"
    r"|plumber|electrician|hvac|dental|medical\s+assist"
    r"|security\s+guard|housekeeper|bartend|waitress|waiter"
    r"|dishwasher|custodian|landscap)\b",
    re.IGNORECASE,
)


def is_csm_role(title: str) -> bool:
    """Quick keyword check — is this a CSM/AM-adjacent role?"""
    if not title:
        return False
    if EXCLUDE_RE.search(title):
        return False
    return bool(CSM_RE.search(title))


# ── Supabase helpers ────────────────────────────────────

def get_existing_urls() -> set[str]:
    """Pull all job URLs already in jobs_jobspy."""
    urls = set()
    offset = 0
    while True:
        try:
            r = http_requests.get(
                f"{REST}/jobs_jobspy?select=job_url&offset={offset}&limit=1000",
                headers=HEADERS, timeout=30,
            )
            r.raise_for_status()
            rows = r.json()
            if not rows:
                break
            for row in rows:
                urls.add(row.get("job_url", ""))
            if len(rows) < 1000:
                break
            offset += 1000
        except Exception as e:
            log.error(f"Failed to fetch existing URLs: {e}")
            break
    return urls


def insert_jobs(rows: list[dict]) -> int:
    """Bulk insert to jobs_jobspy in chunks of 100. Returns count inserted."""
    added = 0
    for i in range(0, len(rows), 100):
        chunk = rows[i:i + 100]
        try:
            r = http_requests.post(
                f"{REST}/jobs_jobspy",
                headers=HEADERS, json=chunk, timeout=30,
            )
            r.raise_for_status()
            added += len(chunk)
        except Exception as e:
            log.error(f"Insert failed: {e}")
            # Try to see what went wrong
            if hasattr(e, "response") and e.response is not None:
                log.error(f"Response: {e.response.text[:500]}")
    return added


# ── Main scraping logic ────────────────────────────────

def run_jobspy(sites: list[str], results_per_query: int = 100,
               hours_old: int = 72, dry_run: bool = False):
    """Run JobSpy searches and store results."""

    try:
        from jobspy import scrape_jobs
    except ImportError:
        log.error("python-jobspy not installed. Run: pip install python-jobspy")
        return

    today = date.today().isoformat()
    existing = set() if dry_run else get_existing_urls()
    all_rows = []
    seen_urls = set()

    for i, search_term in enumerate(SEARCH_TERMS):
        google_term = GOOGLE_SEARCH_TERMS[i] if i < len(GOOGLE_SEARCH_TERMS) else None

        log.info(f"[{i+1}/{len(SEARCH_TERMS)}] Searching: {search_term}")

        try:
            kwargs = {
                "site_name": sites,
                "search_term": search_term,
                "results_wanted": results_per_query,
                "hours_old": hours_old,
                "is_remote": True,
                "verbose": 0,
            }
            if "google" in sites and google_term:
                kwargs["google_search_term"] = google_term

            jobs_df = scrape_jobs(**kwargs)

            if jobs_df is None or jobs_df.empty:
                log.info(f"  No results")
                continue

            log.info(f"  Got {len(jobs_df)} raw results")

            # Filter for CSM/AM roles
            for _, row in jobs_df.iterrows():
                title = str(row.get("title", ""))
                if not is_csm_role(title):
                    continue

                job_url = str(row.get("job_url", ""))
                if not job_url or job_url in seen_urls or job_url in existing:
                    continue
                seen_urls.add(job_url)

                # Parse salary
                salary_min = row.get("min_amount")
                salary_max = row.get("max_amount")
                salary_interval = str(row.get("interval", "")) or None
                salary_currency = str(row.get("currency", "")) or None

                # Clean NaN values
                if salary_min is not None:
                    try:
                        salary_min = float(salary_min)
                        if salary_min != salary_min:  # NaN check
                            salary_min = None
                    except (ValueError, TypeError):
                        salary_min = None

                if salary_max is not None:
                    try:
                        salary_max = float(salary_max)
                        if salary_max != salary_max:
                            salary_max = None
                    except (ValueError, TypeError):
                        salary_max = None

                # Parse location
                location_parts = []
                city = str(row.get("city", "")) if row.get("city") else ""
                state = str(row.get("state", "")) if row.get("state") else ""
                country = str(row.get("country", "")) if row.get("country") else ""
                if city:
                    location_parts.append(city)
                if state:
                    location_parts.append(state)
                location = ", ".join(location_parts) if location_parts else "Remote"

                # Parse date_posted
                date_posted = None
                dp = row.get("date_posted")
                if dp is not None:
                    try:
                        date_posted = str(dp)[:10]  # YYYY-MM-DD
                        if date_posted == "NaT" or date_posted == "nan":
                            date_posted = None
                    except Exception:
                        date_posted = None

                # Get description (truncate for storage)
                description = str(row.get("description", ""))[:5000] if row.get("description") else None

                db_row = {
                    "title": title[:500],
                    "job_url": job_url,
                    "company_name": str(row.get("company", ""))[:300] or None,
                    "source_site": str(row.get("site", "unknown")),
                    "location": location[:500],
                    "country": country[:100] if country else None,
                    "is_remote": bool(row.get("is_remote", False)),
                    "job_type": str(row.get("job_type", ""))[:50] or None,
                    "salary_min": salary_min,
                    "salary_max": salary_max,
                    "salary_interval": salary_interval,
                    "salary_currency": salary_currency,
                    "description": description,
                    "date_posted": date_posted,
                    "date_added": today,
                    "last_seen": today,
                    "is_active": True,
                }
                all_rows.append(db_row)

        except Exception as e:
            log.error(f"  Search failed: {e}")
            continue

    log.info(f"\nTotal CSM/AM roles found: {len(all_rows)}")

    if dry_run:
        log.info("DRY RUN — not writing to database")
        for row in all_rows[:20]:
            log.info(f"  [{row['source_site']}] {row['company_name']} — {row['title']} — {row['location']}")
        if len(all_rows) > 20:
            log.info(f"  ... and {len(all_rows) - 20} more")
        return

    if not all_rows:
        log.info("No new jobs to insert.")
        return

    added = insert_jobs(all_rows)
    log.info(f"Inserted {added} new jobs into jobs_jobspy")
    log.info(f"  ({len(all_rows) - added} failed)")


def main():
    parser = argparse.ArgumentParser(description="JobSpy test runner")
    parser.add_argument(
        "--sites", nargs="+",
        default=["indeed", "google"],
        choices=["indeed", "google", "linkedin", "glassdoor", "zip_recruiter"],
        help="Sites to scrape (default: indeed google)",
    )
    parser.add_argument(
        "--results", type=int, default=100,
        help="Results per search query (default: 100)",
    )
    parser.add_argument(
        "--hours", type=int, default=72,
        help="Only jobs posted in last N hours (default: 72)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print results without writing to DB",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("JOBSPY TEST RUNNER")
    log.info(f"  Sites: {args.sites}")
    log.info(f"  Results per query: {args.results}")
    log.info(f"  Hours old: {args.hours}")
    log.info("=" * 60)

    run_jobspy(
        sites=args.sites,
        results_per_query=args.results,
        hours_old=args.hours,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
