"""TEMPORARY verification (2026-09) — re-run the full real pipeline
(scrape_board -> enrich_descriptions_async -> enrich_application_questions_async)
against fresh live samples for every platform touched by this round of
ATS-audit bug fixes, to confirm the fixes actually improved outcomes.
Deleted after use."""
import asyncio
import logging
import random
import sys

logging.basicConfig(level=logging.WARNING)

import ats_scrapers as A
import supabase_handler

SAMPLE_SIZE = 12
MAX_ATTEMPTS = 40
SEM = asyncio.Semaphore(6)


def sample_slugs(ats: str, n: int):
    rows = supabase_handler._get("archive_i", f"select=slug&ats=eq.{ats}", limit=5000)
    slugs = list({r["slug"] for r in (rows or []) if r.get("slug")})
    random.shuffle(slugs)
    return slugs[:n]


async def probe_one(ats: str, slug: str):
    try:
        jobs = await A.scrape_board(ats, slug)
    except Exception as e:
        return {"slug": slug, "error": f"scrape: {type(e).__name__}: {e}"}
    if not jobs:
        return None
    job = jobs[0]
    try:
        job = (await A.enrich_descriptions_async([job]))[0]
        job = (await A.enrich_application_questions_async([job]))[0]
    except Exception as e:
        return {"slug": slug, "error": f"enrich: {type(e).__name__}: {e}"}
    return {
        "slug": slug, "title": job.get("title", ""),
        "location": job.get("location") or "",
        "has_jd": len((job.get("description_snippet") or "").split("Application Question:")[0]) > 50,
        "has_questions": "Application Question:" in (job.get("description_snippet") or ""),
    }


async def run_ats(ats: str):
    slugs = sample_slugs(ats, 200)
    if not slugs:
        print(f"[{ats}] no slugs found in archive_i")
        return
    results = []
    attempted = 0
    idx = 0

    async def _guarded(s):
        async with SEM:
            return await probe_one(ats, s)

    while len(results) < SAMPLE_SIZE and idx < len(slugs) and attempted < MAX_ATTEMPTS:
        batch = slugs[idx: idx + 8]
        idx += len(batch)
        attempted += len(batch)
        outs = await asyncio.gather(*(_guarded(s) for s in batch), return_exceptions=True)
        for o in outs:
            if isinstance(o, dict):
                results.append(o)

    samples = [r for r in results if "error" not in r]
    errors = [r for r in results if "error" in r]
    no_loc = sum(1 for r in samples if not r["location"])
    no_jd = sum(1 for r in samples if not r["has_jd"])
    no_q = sum(1 for r in samples if not r["has_questions"])
    print(f"\n[{ats}] attempted={attempted} samples={len(samples)} errors={len(errors)} "
          f"| no_location={no_loc}/{len(samples)} no_jd={no_jd}/{len(samples)} no_questions={no_q}/{len(samples)}")
    for r in samples[:5]:
        print(f"    slug={r['slug']!r} title={r['title'][:50]!r} location={r['location'][:40]!r} "
              f"has_jd={r['has_jd']} has_questions={r['has_questions']}")
    for r in errors[:5]:
        print(f"    ERROR slug={r['slug']!r}: {r['error']}")


async def main():
    platforms = sys.argv[1:] or [
        "adp", "brassring", "jobylon", "rippling", "oracle_cloud_hcm", "personio", "avature",
    ]
    for ats in platforms:
        try:
            await run_ats(ats)
        except Exception as e:
            print(f"\n!!! {ats} verification CRASHED: {type(e).__name__}: {e}")
    await A.aclose_http_client()


if __name__ == "__main__":
    asyncio.run(main())
