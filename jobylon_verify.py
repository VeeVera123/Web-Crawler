"""One-off, temporary live verification of the fixed scrape_jobylon
against real Jobylon tenant slugs. Removed again after confirming the
fix works end-to-end against the real, unmodified production code path
(not a mock) -- see jobylon-verify.yml.
"""
import asyncio
import sys

import ats_scrapers as m

REAL_SLUGS = ["2160-varner", "9-meltwater-group", "2-truecaller", "20-beemobile",
              "1364-dermicus", "2959"]


async def main() -> int:
    for slug in REAL_SLUGS:
        try:
            jobs = await m.scrape_jobylon(slug)
            print(f"{slug!r}: OK, {len(jobs)} job(s)")
            for j in jobs[:2]:
                print(f"    {j['title']!r} @ {j['company']!r} ({j['location']!r}) -> {j['url']}")
        except Exception as e:
            print(f"{slug!r}: RAISED {type(e).__name__}: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
