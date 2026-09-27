"""One-off, temporary live verification of OpenAI's BrassRing lead:
does the real TGnewUI JobDetails page actually contain a
"VerityZone:jobdescription" / "AnswerValue" field marker, or a
".jobdescriptionInJobDetails" DOM element? Checked against real,
currently-live job detail pages before writing any extraction code.
Uses the actual, already-working scrape_brassring() to get real job
URLs (a simplified reimplementation missed matches in round 1).
Removed once confirmed either way.
"""
import re
import sys
import asyncio
import requests
from bs4 import BeautifulSoup

import ats_scrapers as m

BOARD_SLUGS = ["16030|6100", "16030|6086", "25008|5131"]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
}


async def main() -> int:
    session = requests.Session()
    for slug in BOARD_SLUGS:
        print("=" * 70)
        print(f"board {slug!r}")
        print("=" * 70)
        try:
            jobs = await m.scrape_brassring(slug)
        except Exception as e:
            print(f"scrape_brassring raised: {type(e).__name__}: {e}")
            continue
        print(f"{len(jobs)} job(s) scraped")
        if not jobs:
            continue
        job = jobs[0]
        detail_url = job["url"]
        print(f"sample job: title={job['title']!r} url={detail_url}")

        r = session.get(detail_url, headers=HEADERS, timeout=30)
        print(f"detail page: status={r.status_code}, {len(r.text)} chars")
        html = r.text

        for needle in ("VerityZone", "jobdescription", "AnswerValue",
                       "jobdescriptionInJobDetails", "JobDetailFieldsToDisplay",
                       "ActualValueFromSolar"):
            count = html.count(needle)
            print(f"  {needle!r}: {count} occurrence(s)")

        vz_match = re.search(r"VerityZone\s*[:=]\s*[\"']?jobdescription", html, re.I)
        if vz_match:
            print(f"  VerityZone:jobdescription found at char {vz_match.start()}")
            context = html[max(0, vz_match.start() - 500):vz_match.start() + 100]
            print(f"  context before marker: {context!r}")
        else:
            print("  NO VerityZone:jobdescription marker found in raw HTML")

        soup2 = BeautifulSoup(html, "html.parser")
        dom_hits = soup2.select(".jobdescriptionInJobDetails")
        print(f"  .jobdescriptionInJobDetails DOM elements found: {len(dom_hits)}")
        if dom_hits:
            print(f"    first element text (first 300 chars): {dom_hits[0].get_text(' ', strip=True)[:300]!r}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
