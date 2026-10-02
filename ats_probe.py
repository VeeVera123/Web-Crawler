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

Concurrent by design: every scrape_board() call and every QUESTION_FETCHERS
call is independent I/O against a different company/job, so all of them
run in parallel (bounded by a per-platform semaphore) rather than one at a
time — sequential execution of ~72 companies, several of them retrying
against dead/JS-rendered endpoints at REQUEST_TIMEOUT=15s x MAX_RETRIES=2
each, is what made the first version of this script slow. The
QUESTION_FETCHERS functions are synchronous (blocking `requests` calls), so
they're run via asyncio.to_thread to actually get concurrency out of them.

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

SAMPLE_COMPANIES = 40
JOBS_PER_COMPANY = 3
PER_PLATFORM_CONCURRENCY = 10  # concurrent companies in flight, per platform


def fetch_slugs(ats: str, limit: int) -> list[str]:
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/archive_i",
        params={"select": "slug", "ats": f"eq.{ats}", "limit": limit},
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        timeout=30,
    )
    r.raise_for_status()
    return [row["slug"] for row in r.json()]


async def probe_company(ats: str, display: str, slug: str, fetcher, sem: asyncio.Semaphore) -> dict | None:
    async with sem:
        try:
            jobs = await S.scrape_board(ats, slug)
        except Exception as e:
            print(f"  [{display}] {slug}: scrape_board raised {type(e).__name__}: {e}")
            return None

        if not jobs:
            print(f"  [{display}] {slug}: 0 jobs returned")
            return None

        sample = jobs[:JOBS_PER_COMPANY]
        has_loc = any((j.get("location") or "").strip() for j in sample)
        has_desc = any(
            (j.get("description_snippet") or j.get("description") or "").strip()
            for j in sample
        )

        q_hits = 0
        sample_question = None
        if fetcher is not None:
            # Each fetcher call is a blocking `requests` call — run them
            # concurrently via a thread pool instead of awaiting one at a time.
            results = await asyncio.gather(
                *(asyncio.to_thread(fetcher, job) for job in sample),
                return_exceptions=True,
            )
            for job, qtext in zip(sample, results):
                if isinstance(qtext, Exception):
                    print(f"    [{display}] {slug} / {job.get('url', '?')}: fetcher raised "
                          f"{type(qtext).__name__}: {qtext}")
                    continue
                if qtext and qtext.strip():
                    q_hits += 1
                    if sample_question is None:
                        sample_question = qtext.strip().splitlines()[0]

        print(f"  [{display}] {slug}: {len(jobs)} jobs total, checked {len(sample)} | "
              f"location={'yes' if has_loc else 'NO'} | "
              f"description={'yes' if has_desc else 'NO'} | "
              f"questions found on {q_hits}/{len(sample)} checked jobs")

        return {
            "has_loc": has_loc,
            "has_desc": has_desc,
            "jobs_checked": len(sample),
            "jobs_with_questions": q_hits,
            "sample_question": f"{slug}: {sample_question}" if sample_question else None,
        }


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

    sem = asyncio.Semaphore(PER_PLATFORM_CONCURRENCY)
    company_results = await asyncio.gather(
        *(probe_company(ats, display, slug, fetcher, sem) for slug in slugs)
    )

    for result in company_results:
        if result is None:
            continue
        stats["companies_with_jobs"] += 1
        stats["companies_with_location"] += int(result["has_loc"])
        stats["companies_with_description"] += int(result["has_desc"])
        stats["jobs_checked"] += result["jobs_checked"]
        stats["jobs_with_questions"] += result["jobs_with_questions"]
        if result["sample_question"] and len(stats["sample_questions"]) < 5:
            stats["sample_questions"].append(result["sample_question"])

    return stats


async def main() -> None:
    platforms = sys.argv[1:] or list(CANDIDATES.keys())
    valid = []
    for ats in platforms:
        if ats not in CANDIDATES:
            print(f"Unknown platform {ats!r} — skipping. Known: {sorted(CANDIDATES)}")
            continue
        valid.append(ats)

    # Platforms hit entirely different hosts, so run them concurrently too.
    platform_results = await asyncio.gather(
        *(probe_platform(ats, CANDIDATES[ats]) for ats in valid)
    )
    results = dict(zip((CANDIDATES[ats] for ats in valid), platform_results))

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
