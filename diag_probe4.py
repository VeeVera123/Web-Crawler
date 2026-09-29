"""TEMPORARY diagnostic round 3 (2026-09) — Ashby embedded-state search +
JobAdder body scan. Deleted after use."""
import asyncio
import logging
import random
import re

logging.basicConfig(level=logging.WARNING)

import ats_scrapers as A


def hr(title):
    print(f"\n{'='*90}\n{title}\n{'='*90}")


async def diag_ashby_page():
    hr("ASHBY — real job page HTML search for embedded state")
    urls = [
        "https://jobs.ashbyhq.com/abby-care/9a213b20-035d-458c-921e-01c80bc2f3df",
        "https://jobs.ashbyhq.com/alchemi/f1254076-a795-4417-8793-764cdb33b7a2",
    ]
    for url in urls:
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"\nURL={url} STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
        if not r:
            continue
        text = r.text
        for kw in ("__NEXT_DATA__", "application/json", "window.__", "__appData", "ApolloState",
                   "questionType", "applicationFormDefinition", "<form", "<input", "jobPostingId",
                   "script id=", "type=\"application/ld+json\""):
            idx = text.find(kw)
            print(f"  contains {kw!r}: {idx != -1} (idx={idx})")
        # dump all <script> tag opening attrs to see what's actually embedded
        script_tags = re.findall(r"<script[^>]*>", text)
        print(f"  {len(script_tags)} <script> tags; first 15 openings:")
        for s in script_tags[:15]:
            print("   ", s[:200])
        found = A._find_embedded_questions(text)
        print("  embedded questions found:", found)


async def diag_jobadder_body():
    hr("JOBADDER — full body scan for location/description markup")
    urls = [
        "https://clientapps.jobadder.com/57292/the-north-australian-pastoral-company/909564/2027-head-stockman-kynuna-station-qld",
        "https://clientapps.jobadder.com/21713/lkm-recruitment/1118044/bookkeeper-accounts-administrator-asap-start-gladesville",
    ]
    for url in urls:
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"\nURL={url} STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
        if not r:
            continue
        text = r.text
        for kw in ("job-location", "jobLocation", "location-name", "posting-location",
                   "application/ld+json", "job_snippet", "class=\"location", "location:",
                   "job-description", "job_details", "<article"):
            idx = text.find(kw)
            print(f"  contains {kw!r}: {idx != -1} (idx={idx})")
        # print a window around 'location' (case-insensitive) if found anywhere
        m = re.search(r"location", text, re.I)
        if m:
            start = max(0, m.start() - 300)
            print("  --- context around first 'location' match ---")
            print(text[start:m.start() + 500])
        loc = A._extract_location_from_html(text)
        print("  extract_location_from_html:", repr(loc))
        desc = await A._fetch_generic_description({"url": url, "location": ""})
        print("  generic_description len:", len(desc or ""))


async def main():
    await diag_ashby_page()
    await diag_jobadder_body()
    await A.aclose_http_client()


if __name__ == "__main__":
    asyncio.run(main())
