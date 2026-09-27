"""One-off, temporary live verification for two description-capture gaps
found while auditing ats_scrapers.py:

1. PageUp — confirms DESCRIPTION_FETCHERS["PageUp"] = _fetch_generic_description
   (just added) actually recovers real description text.
2. BrassRing — scrape_brassring's currently-working path (HTML search
   results) never captures a description and BrassRing isn't registered
   in DESCRIPTION_FETCHERS at all. This checks whether its own
   JobDetails page (the URL scrape_brassring already builds into
   job["url"]) is real server-rendered HTML with a real description, or
   a client-side-rendered shell (TGnewUI is Angular-based) before
   deciding whether a fetcher is even feasible.

Removed again once confirmed. See jobylon_verify.py for the same pattern.
"""
import re
import sys

import ats_scrapers as m

PAGEUP_SLUGS = ["1106|cw", "873|po", "1023|ClientPublicFile"]
BRASSRING_SLUGS = ["16030|6100", "16030|6086", "25008|5131"]


def check_pageup() -> None:
    print("=" * 70)
    print("PAGEUP")
    print("=" * 70)
    for slug in PAGEUP_SLUGS:
        jobs = m.scrape_pageup(slug)
        print(f"{slug!r}: {len(jobs)} job(s) scraped")
        if not jobs:
            continue
        job = jobs[0]
        print(f"  sample before enrichment: title={job['title']!r} "
              f"desc_len={len(job.get('description_snippet') or '')} url={job['url']}")
        desc = m._fetch_generic_description(job)
        print(f"  _fetch_generic_description -> {len(desc)} chars")
        if desc:
            print(f"  first 300 chars: {desc[:300]!r}")


async def check_brassring() -> None:
    print()
    print("=" * 70)
    print("BRASSRING")
    print("=" * 70)
    for slug in BRASSRING_SLUGS:
        try:
            jobs = await m.scrape_brassring(slug)
        except Exception as e:
            print(f"{slug!r}: RAISED {type(e).__name__}: {e}")
            continue
        print(f"{slug!r}: {len(jobs)} job(s) scraped")
        if not jobs:
            continue
        job = jobs[0]
        print(f"  sample: title={job['title']!r} desc_len={len(job.get('description_snippet') or '')} "
              f"url={job['url']}")
        r = await m._get(job["url"], headers={"User-Agent": "Mozilla/5.0"})
        if not r:
            print("  detail-page fetch FAILED")
            continue
        html = r.text
        print(f"  detail page: status ok, {len(html)} bytes")
        # Look for common JD signal text vs. an Angular/JS-shell page
        # (script-heavy, almost no real text once tags are stripped).
        text_only = re.sub(r"<script.*?</script>", " ", html, flags=re.S | re.I)
        text_only = re.sub(r"<[^>]+>", " ", text_only)
        text_only = re.sub(r"\s+", " ", text_only).strip()
        print(f"  visible text after stripping tags/scripts: {len(text_only)} chars")
        print(f"  sample: {text_only[:300]!r}")
        has_jd_words = bool(re.search(r"\b(responsibilit|qualificat|requirement|job description|about the role)\b", text_only, re.I))
        print(f"  looks like real JD text present: {has_jd_words}")
        # What would the existing generic fetcher actually extract from
        # this exact page? (uses the same job dict, real fetched URL)
        generic_desc = m._fetch_generic_description(job)
        print(f"  _fetch_generic_description on this BrassRing page -> {len(generic_desc)} chars")
        if generic_desc:
            print(f"    first 400 chars: {generic_desc[:400]!r}")
        # Structural probe: find recognizable description container
        # class/id names in the RAW html (before any tag-stripping), and
        # show where the job title's own text re-appears in the visible
        # text (real body content usually starts there, after nav/cookie
        # banner boilerplate).
        for pat in (r'class="[^"]*(?:job-?description|jobDetail|description)[^"]*"',
                    r'id="[^"]*(?:job-?description|jobDetail|description)[^"]*"',
                    r'"description"\s*:\s*"', r'"JobDescription"\s*:\s*"',
                    r'ng-bind[^=]*="[^"]*[Dd]esc'):
            hits = re.findall(pat, html)
            if hits:
                print(f"    raw-html marker {pat!r}: {len(hits)} hit(s), e.g. {hits[0]!r}")
        title_pos = text_only.find(job["title"].split(" - ")[0][:20]) if job["title"] else -1
        second_title_pos = text_only.find(job["title"].split(" - ")[0][:20], title_pos + 1) if title_pos != -1 else -1
        print(f"  title first appears at char {title_pos}, "
              f"re-appears at {second_title_pos} (out of {len(text_only)} total)")
        if second_title_pos != -1:
            print(f"    text around 2nd occurrence: {text_only[second_title_pos:second_title_pos+400]!r}")


def main() -> int:
    check_pageup()
    import asyncio
    asyncio.run(check_brassring())
    return 0


if __name__ == "__main__":
    sys.exit(main())
