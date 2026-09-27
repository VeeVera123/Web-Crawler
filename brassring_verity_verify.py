"""Round 3: VerityZone/AnswerValue/JobDetailFieldsToDisplay all really
exist on live pages (confirmed round 2), but the exact
"VerityZone:jobdescription" adjacency regex an external LLM proposed
found zero matches. This dumps the raw context around every VerityZone
occurrence and around JobDetailFieldsToDisplay to find the REAL field
structure before writing an extraction regex based on it.
"""
import re
import sys
import asyncio
import requests

import ats_scrapers as m

BOARD_SLUGS = ["16030|6086", "25008|5131"]
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
        jobs = await m.scrape_brassring(slug)
        if not jobs:
            continue
        detail_url = jobs[0]["url"]
        r = session.get(detail_url, headers=HEADERS, timeout=30)
        html = r.text
        print(f"detail page: {len(html)} chars")

        print("\n-- context around each 'VerityZone' occurrence (first 6) --")
        for i, m_ in enumerate(re.finditer("VerityZone", html)):
            if i >= 6:
                print(f"  ... ({len(re.findall('VerityZone', html))} total)")
                break
            start = max(0, m_.start() - 60)
            end = min(len(html), m_.end() + 120)
            print(f"  [{m_.start()}] ...{html[start:end]!r}...")

        print("\n-- context around 'JobDetailFieldsToDisplay' --")
        m2 = re.search("JobDetailFieldsToDisplay", html)
        if m2:
            start = max(0, m2.start() - 50)
            end = min(len(html), m2.end() + 600)
            print(f"  [{m2.start()}] ...{html[start:end]!r}...")

        print("\n-- context around first 'jobdescription' occurrence that is NOT inside a CSS class attribute --")
        count = 0
        for m3 in re.finditer("jobdescription", html, re.I):
            start = max(0, m3.start() - 80)
            end = min(len(html), m3.end() + 80)
            snippet = html[start:end]
            if 'class=' in snippet.lower() and 'jobdescriptionInJobDetails' in snippet:
                continue  # skip the obvious CSS-class-attribute hits
            print(f"  [{m3.start()}] ...{snippet!r}...")
            count += 1
            if count >= 6:
                break

        print("\n-- context around first 3 'AnswerValue' occurrences --")
        for i, m4 in enumerate(re.finditer("AnswerValue", html)):
            if i >= 3:
                break
            start = max(0, m4.start() - 30)
            end = min(len(html), m4.end() + 250)
            print(f"  [{m4.start()}] ...{html[start:end]!r}...")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
