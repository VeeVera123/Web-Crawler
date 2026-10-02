"""Standalone, one-off diagnostic — NOT part of the production pipeline,
never imported by crawl_i.py/crawl_ii.py/crawl_iii.py.

Purpose (explicit user request): decide whether any ATS platform outside
classifier.RANK4_ELIGIBLE_ATS's current 12 platforms should be added —
"maybe other ones like Ashby... definitely ashby... maybe just the top
5" — by actually measuring, against real companies already sitting in
Supabase's archive_i table, whether that platform's scraper + its
QUESTION_FETCHERS entry reliably return BOTH a location and a real
application-question value on the same job. That's Rank 4's entire
eligibility premise (see classifier.py's RANKING_REFERENCE.md-documented
gate) — a platform that can't clear this bar can't safely host Rank 4
admissions no matter how it's wired in.

Run via .github/workflows/ats_probe.yml (workflow_dispatch only, never
on a schedule) — prints a per-company and per-platform summary to the
Actions log for a human/Claude to read afterward. Does not write
anything to Supabase or Notion.
"""
import asyncio
import os
import sys

import requests

import ats_scrapers as S

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# ats_scrapers.SCRAPERS key -> the source_ats display string its scrape_*
# function stamps on each job dict, which is also the QUESTION_FETCHERS key.
CANDIDATES = {
    "ashby": "Ashby",
    "workday": "Workday",
    "icims": "iCIMS",
    "bamboohr": "BambooHR",
    "smartrecruiters": "SmartRecruiters",
    "adp": "ADP",
}

SAMPLE_COMPANIES = 12
JOBS_PER_COMPANY = 3


def fetch_slugs(ats: str, limit: int) -> list[str]:
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/archive_i",
        params={"select": "slug", "ats": f"eq.{ats}", "limit": limit},
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        timeout=30,
    )
    r.raise_for_status()
    return [row["slug"] for row in r.json()]


async def probe_platform(ats: str, display: str) -> dict:
    fetcher = S.QUESTION_FETCHERS.get(display)
    slugs = fetch_slugs(ats, SAMPLE_COMPANIES)
    print(f"\n{'=' * 90}\n{display} ({ats}) — {len(slugs)} companies sampled from archive_i\n{'=' * 90}")

    stats = {
        "companies_sampled": len(slugs),
        "companies_with_jobs": 0,
        "companies_with_location": 0,
        "companies_with_description": 0,
        "jobs_checked": 0,
        "jobs_with_questions": 0,
        "sample_questions": [],
    }

    if not slugs:
        print("  NO SLUGS FOUND in archive_i for this platform.")
        return stats

    for slug in slugs:
        try:
            jobs = await S.scrape_board(ats, slug)
        except Exception as e:
            print(f"  {slug}: scrape_board raised {type(e).__name__}: {e}")
            continue

        if not jobs:
            print(f"  {slug}: 0 jobs returned")
            continue

        stats["companies_with_jobs"] += 1
        sample = jobs[:JOBS_PER_COMPANY]
        has_loc = any((j.get("location") or "").strip() for j in sample)
        has_desc = any(
            (j.get("description_snippet") or j.get("description") or "").strip()
            for j in sample
        )
        stats["companies_with_location"] += int(has_loc)
        stats["companies_with_description"] += int(has_desc)

        q_hits = 0
        for job in sample:
            stats["jobs_checked"] += 1
            if fetcher is None:
                continue
            try:
                qtext = fetcher(job)
            except Exception as e:
                print(f"    {slug} / {job.get('url', '?')}: fetcher raised "
                      f"{type(e).__name__}: {e}")
                continue
            if qtext and qtext.strip():
                q_hits += 1
                stats["jobs_with_questions"] += 1
                if len(stats["sample_questions"]) < 5:
                    first_line = qtext.strip().splitlines()[0]
                    stats["sample_questions"].append(f"{slug}: {first_line}")

        print(f"  {slug}: {len(jobs)} jobs total, checked {len(sample)} | "
              f"location={'yes' if has_loc else 'NO'} | "
              f"description={'yes' if has_desc else 'NO'} | "
              f"questions found on {q_hits}/{len(sample)} checked jobs")

    return stats


async def main() -> None:
    platforms = sys.argv[1:] or list(CANDIDATES.keys())
    results = {}
    for ats in platforms:
        display = CANDIDATES.get(ats)
        if display is None:
            print(f"Unknown platform {ats!r} — skipping. Known: {sorted(CANDIDATES)}")
            continue
        results[display] = await probe_platform(ats, display)

    print(f"\n{'=' * 90}\nSUMMARY\n{'=' * 90}")
    header = f"{'Platform':<18}{'Companies w/ jobs':<20}{'Location %':<13}{'Description %':<16}{'Questions %':<13}"
    print(header)
    print("-" * len(header))
    for display, s in results.items():
        n = s["companies_with_jobs"] or 1
        loc_pct = 100 * s["companies_with_location"] / n
        desc_pct = 100 * s["companies_with_description"] / n
        jobs_n = s["jobs_checked"] or 1
        q_pct = 100 * s["jobs_with_questions"] / jobs_n
        print(f"{display:<18}{s['companies_with_jobs']}/{s['companies_sampled']:<16}"
              f"{loc_pct:<13.0f}{desc_pct:<16.0f}{q_pct:<13.0f}")
        if s["sample_questions"]:
            print(f"    sample questions seen: {s['sample_questions']}")


if __name__ == "__main__":
    asyncio.run(main())
