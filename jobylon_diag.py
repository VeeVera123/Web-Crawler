"""Round 5: confirmed the URL-slug company match works (varner matched
11 real postings by "/jobs/<id>-varner-..." prefix, zero false positives
expected since it's the full company slug). jbl_company_id isn't
embedded on job detail pages though (only on company pages), so this
inspects one matched detail page's actual structure -- JSON-LD, title,
hiringOrganization -- to build the real parser and confirm the company
match is genuine (not just a coincidental slug prefix)."""
import re
import sys
import json
import httpx

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}

URL = "https://emp.jobylon.com/jobs/384887-varner-creative-studio-assistant/"


def main() -> int:
    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30, http2=True)
    r = client.get(URL)
    print(f"status={r.status_code} bytes={len(r.content)}")

    ld_blocks = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', r.text, re.I | re.S)
    print(f"ld+json script blocks: {len(ld_blocks)}")
    for b in ld_blocks:
        try:
            data = json.loads(b)
        except Exception as e:
            print(f"  PARSE FAILED: {e}")
            print(f"  raw[:300]={b[:300]!r}")
            continue
        print(f"  parsed OK, top-level type: {data.get('@type') if isinstance(data, dict) else type(data)}")
        print(json.dumps(data, indent=2)[:2000])

    print("\n-- title --")
    tm = re.search(r"<title[^>]*>(.*?)</title>", r.text, re.I | re.S)
    print(tm.group(1) if tm else None)

    print("\n-- jbl_company_id / jbl_company anywhere? --")
    for pat in (r"jbl_company_id\s*=\s*\d+", r"company_id[\"']?\s*[:=]\s*\d+", r'"company"\s*:\s*\{[^}]{0,200}'):
        found = re.findall(pat, r.text, re.I)
        print(f"  {pat!r}: {found[:3]}")

    print("\n-- meta og: tags --")
    for m in re.findall(r'<meta[^>]*property=["\']og:[^"\']+["\'][^>]*>', r.text, re.I):
        print(f"  {m}")

    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
