"""Round 4: verify the real, newly-added _fetch_brassring_description
against real, currently-live BrassRing job detail pages. Removed once
confirmed.
"""
import sys
import asyncio

import ats_scrapers as m

BOARD_SLUGS = ["16030|6100", "16030|6086", "25008|5131"]


async def main() -> int:
    for slug in BOARD_SLUGS:
        print("=" * 70)
        print(f"board {slug!r}")
        print("=" * 70)
        jobs = await m.scrape_brassring(slug)
        print(f"{len(jobs)} job(s) scraped")
        for job in jobs[:3]:
            desc = m._fetch_brassring_description(job)
            print(f"  job {job['url'].split('jobid=')[-1]!r}: "
                  f"description length = {len(desc)}")
            if desc:
                print(f"    first 300 chars: {desc[:300]!r}")
            else:
                print("    (empty)")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
