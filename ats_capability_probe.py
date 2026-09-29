"""
TEMPORARY - ATS capability probe (2026-09, explicit user request).

For ONE ATS platform (--ats), samples real company slugs from Supabase
archive_i and runs each through the SAME live pipeline the real crawls
use — ats_scrapers.scrape_board() -> enrich_descriptions_async() ->
enrich_application_questions_async() — then logs exactly what comes back:
the location field, a JD snippet, and the application questions (already
PII/EEO-filtered by ats_scrapers._BOILERPLATE_QUESTION_RE, the same
filter the real pipeline applies before anything reaches classifier.py).

NOT for gating which ATSs qualify for a future ranking tier — the user
explicitly said that's not needed. This is a scraper-FIDELITY AUDIT: "I
want to be sure that the AI and classifier are really seeing all they
should be." No logic is reimplemented here; every field logged is
whatever the real production functions actually returned.

Slugs are sampled directly from archive_i (populated by discovery.py)
rather than hand-picked, so whatever slug-format variants a platform
actually has on file (a company might be discovered via more than one
naming convention) show up naturally in a random sample, no special-
casing needed.

Delete this file (and .github/workflows/tmp-ats-capability-probe.yml)
once the results have been reviewed.

Usage:
    python ats_capability_probe.py --ats greenhouse --sample-size 30
"""
import argparse
import asyncio
import random

import ats_scrapers
from supabase_handler import _get


async def probe_one_slug(ats: str, slug: str) -> dict | None:
    """Returns None for a company with no open roles right now (not a
    failure — just nothing to sample), a dict with an "error" key if the
    scraper/enrichment itself raised, or a full sample dict otherwise."""
    try:
        jobs = await ats_scrapers.scrape_board(ats, slug)
    except Exception as e:
        return {"slug": slug, "error": f"scrape_board: {type(e).__name__}: {e}"}
    if not jobs:
        return None

    job = jobs[0]
    try:
        job = (await ats_scrapers.enrich_descriptions_async([job]))[0]
        job = (await ats_scrapers.enrich_application_questions_async([job]))[0]
    except Exception as e:
        return {
            "slug": slug,
            "error": f"enrichment: {type(e).__name__}: {e}",
            "location": job.get("location"),
        }

    desc = job.get("description_snippet") or ""
    marker = "Application Question:"
    idx = desc.find(marker)
    if idx == -1:
        jd_snippet, questions_block = desc, ""
    else:
        jd_snippet, questions_block = desc[:idx].rstrip(), desc[idx:].strip()

    return {
        "slug": slug,
        "company": job.get("company") or "",
        "title": job.get("title") or "",
        "url": job.get("url") or "",
        "location": job.get("location") or "",
        "jd_snippet": jd_snippet[:400],
        "questions": questions_block or "(none survived PII/EEO filtering, or none found)",
    }


async def main_async(ats: str, sample_size: int, max_attempts: int) -> None:
    rows = _get("archive_i", f"select=slug&ats=eq.{ats}", limit=5000)
    slugs = [r["slug"] for r in rows if r.get("slug")]
    if not slugs:
        print(f"[{ats}] NO SLUGS FOUND IN archive_i — cannot probe this platform.")
        return

    random.shuffle(slugs)
    attempt_slugs = slugs[:max_attempts]
    print(f"[{ats}] {len(slugs)} slugs on file in archive_i; attempting up to "
          f"{len(attempt_slugs)} to gather {sample_size} live samples")

    sem = asyncio.Semaphore(8)

    async def worker(slug: str):
        async with sem:
            return await probe_one_slug(ats, slug)

    raw = await asyncio.gather(*(worker(s) for s in attempt_slugs), return_exceptions=True)

    results, errors, empties = [], [], 0
    for r in raw:
        if isinstance(r, Exception):
            errors.append({"slug": "?", "error": f"{type(r).__name__}: {r}"})
        elif r is None:
            empties += 1
        elif "error" in r:
            errors.append(r)
        else:
            results.append(r)
    results = results[:sample_size]

    no_location = sum(1 for r in results if not r["location"])
    no_jd = sum(1 for r in results if not r["jd_snippet"])
    no_questions = sum(1 for r in results if r["questions"].startswith("(none"))

    print(f"[{ats}] SUMMARY: attempted={len(attempt_slugs)} samples={len(results)} "
          f"empty_boards={empties} errors={len(errors)} | "
          f"of {len(results)} samples: no_location={no_location} no_jd_snippet={no_jd} "
          f"no_questions_survived={no_questions}")
    print("=" * 70)

    for r in results:
        print(f"--- {ats} / {r['slug']} ({r['company']}) ---")
        print(f"URL: {r['url']}")
        print(f"TITLE: {r['title']}")
        print(f"LOCATION FIELD: {r['location']!r}")
        print(f"JD SNIPPET: {r['jd_snippet']!r}")
        print(f"APPLICATION QUESTIONS:\n{r['questions']}")
        print()

    if errors:
        print(f"--- {ats}: {len(errors)} ERRORS (scrape or enrichment threw) ---")
        for e in errors[:15]:
            print(f"  {e['slug']}: {e['error']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="ATS capability/fidelity probe")
    parser.add_argument("--ats", required=True)
    parser.add_argument("--sample-size", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=90,
                         help="Slugs to try before giving up on reaching --sample-size "
                              "(covers ATSs where many sampled companies currently have "
                              "zero open roles)")
    args = parser.parse_args()
    asyncio.run(main_async(args.ats, args.sample_size, args.max_attempts))


if __name__ == "__main__":
    main()
