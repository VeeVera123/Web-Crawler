"""Standalone, read-only diagnostic for the Jobylon scraper's 100%
failure rate (0 jobs / 40 boards failed in the last real crawl run).
Does not touch ats_scrapers.py, discovery.py, Supabase, or any
production crawl path -- only fetches public Jobylon URLs and prints
what it finds, so the real fix can be based on live evidence instead
of another guess.

Manual-dispatch only, via jobylon-diag.yml.

Round 2: round 1 found sitemap.xml is only 237 bytes (effectively
empty -- 0 /jobs/ <loc> entries) and that real company pages return 200
with "/jobs/" appearing SOMEWHERE in ~90-110KB of HTML, but no anchor
tag matches href=".../jobs/<digits>...". This round inspects what that
"/jobs/" text actually is, and looks for any embedded JSON state or API
endpoint the real job data might come from instead.
"""
import re
import sys
import httpx

REAL_SLUGS = ["2-truecaller", "9-meltwater-group", "20-beemobile", "2160-varner"]

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


def show_context(html: str, needle: str, n: int = 3, width: int = 100) -> None:
    count = 0
    for m in re.finditer(re.escape(needle), html, re.I):
        if count >= n:
            print(f"    ... ({len(re.findall(re.escape(needle), html, re.I))} total occurrences)")
            break
        start = max(0, m.start() - width)
        end = min(len(html), m.end() + width)
        snippet = html[start:end].replace("\n", "\\n")
        print(f"    [{m.start()}] ...{snippet}...")
        count += 1
    if count == 0:
        print(f"    (0 occurrences of {needle!r})")


def main() -> int:
    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30, http2=True)

    print("=" * 70)
    print("1. FULL sitemap.xml content (it was only 237 bytes)")
    print("=" * 70)
    r = client.get("https://emp.jobylon.com/sitemap.xml")
    print(f"status={r.status_code}")
    print(repr(r.text))

    print()
    print("=" * 70)
    print("2. Truecaller company page -- structural investigation")
    print("=" * 70)
    cr = client.get("https://emp.jobylon.com/companies/2-truecaller/")
    html = cr.text
    print(f"status={cr.status_code} bytes={len(cr.content)}")

    print("\n-- context around '/jobs/' occurrences --")
    show_context(html, "/jobs/", n=5)

    print("\n-- <script src=...> tags (first 15) --")
    for m in re.findall(r'<script[^>]*\bsrc=["\']([^"\']+)["\']', html, re.I)[:15]:
        print(f"    {m}")

    print("\n-- <link> tags with rel= (first 15) --")
    for m in re.findall(r'<link\b[^>]*>', html, re.I)[:15]:
        print(f"    {m}")

    print("\n-- any inline <script> WITHOUT src (first 5, first 200 chars each) --")
    inline_scripts = re.findall(r'<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>', html, re.I | re.S)
    for s in inline_scripts[:5]:
        s = s.strip()
        print(f"    [{len(s)} chars] {s[:200]!r}")

    print("\n-- searching for API/state hints (graphql, __INITIAL, api., hydrat, apollo, application/json) --")
    for needle in ("graphql", "__INITIAL", "api.jobylon", "hydrat", "apollo", "application/json",
                   "window.__", "data-testid", "vacan", "career"):
        found = len(re.findall(re.escape(needle), html, re.I))
        print(f"    {needle!r}: {found} occurrences")

    print("\n-- <title> and first 500 chars of <body> text (rough) --")
    tm = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    print(f"    title: {tm.group(1) if tm else None!r}")
    bm = re.search(r"<body[^>]*>(.*)", html, re.I | re.S)
    body_text = re.sub(r"<[^>]+>", " ", bm.group(1)[:3000]) if bm else ""
    body_text = re.sub(r"\s+", " ", body_text).strip()
    print(f"    body text sample: {body_text[:500]!r}")

    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
