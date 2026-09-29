"""TEMPORARY (2026-09) — determines which ATS platforms reliably return
BOTH location AND application-question data through the real production
pipeline, to gate Rank 4 eligibility in the classifier revamp. One
platform per invocation (matrix job). Deleted after use."""
import asyncio
import logging
import random
import sys

logging.basicConfig(level=logging.WARNING)

import ats_scrapers as A
import supabase_handler

SAMPLE_SIZE = 25
MAX_ATTEMPTS = 90
SEM = asyncio.Semaphore(8)


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
    desc = job.get("description_snippet") or ""
    return {
        "slug": slug, "title": job.get("title", ""),
        "location": job.get("location") or "",
        "has_questions": "Application Question:" in desc,
        "questions_preview": desc.split("Application Question:")[-1][:120] if "Application Question:" in desc else "",
    }


async def main():
    ats = sys.argv[1]
    slugs = sample_slugs(ats, 400)
    if not slugs:
        print(f"[{ats}] SUMMARY: no slugs found in archive_i")
        await A.aclose_http_client()
        return

    results = []
    attempted = 0
    idx = 0

    async def _guarded(s):
        async with SEM:
            return await probe_one(ats, s)

    while len(results) < SAMPLE_SIZE and idx < len(slugs) and attempted < MAX_ATTEMPTS:
        batch = slugs[idx: idx + 10]
        idx += len(batch)
        attempted += len(batch)
        outs = await asyncio.gather(*(_guarded(s) for s in batch), return_exceptions=True)
        for o in outs:
            if isinstance(o, dict):
                results.append(o)

    samples = [r for r in results if "error" not in r]
    errors = [r for r in results if "error" in r]
    no_loc = sum(1 for r in samples if not r["location"])
    no_q = sum(1 for r in samples if not r["has_questions"])
    n = len(samples) or 1
    loc_pct = 100 * no_loc / n
    q_pct = 100 * no_q / n
    eligible = len(samples) >= 10 and loc_pct <= 15 and q_pct <= 20
    print(f"\n[{ats}] SUMMARY: attempted={attempted} samples={len(samples)} errors={len(errors)} "
          f"| no_location={no_loc}/{len(samples)} ({loc_pct:.0f}%) "
          f"no_questions={no_q}/{len(samples)} ({q_pct:.0f}%) "
          f"| RANK4_ELIGIBLE={eligible}")
    for r in samples[:8]:
        print(f"    slug={r['slug']!r} title={r['title'][:40]!r} location={r['location'][:35]!r} "
              f"has_questions={r['has_questions']} q_preview={r['questions_preview'][:60]!r}")
    for r in errors[:5]:
        print(f"    ERROR slug={r['slug']!r}: {r['error']}")
    await A.aclose_http_client()


if __name__ == "__main__":
    asyncio.run(main())
