"""Round 3: sitemap.xml is a sitemap INDEX pointing at sitemap-jobs.xml
(not the job list itself -- that's the real bug behind Jobylon's 0/40).
Company pages are old-school jQuery pages that load their job widget via
AJAX (jbl_company_id + jbl-offer-module.js), so they never contain real
job links in the static HTML -- only an unrelated example URL from an
embedded API-schema blob. This round measures sitemap-jobs.xml's real
size and checks whether job detail pages really do link back to
/companies/<id>-, to size the real fix correctly.
"""
import re
import sys
import httpx

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


def main() -> int:
    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=60, http2=True)

    print("=" * 70)
    print("1. sitemap-jobs.xml size")
    print("=" * 70)
    r = client.get("https://emp.jobylon.com/sitemap-jobs.xml")
    print(f"status={r.status_code} bytes={len(r.content)} content-type={r.headers.get('content-type')}")
    job_urls = re.findall(r"<loc>([^<]+)</loc>", r.text)
    print(f"total <loc> entries: {len(job_urls)}")
    for u in job_urls[:5]:
        print(f"  sample: {u}")
    print(f"  ...")
    for u in job_urls[-5:]:
        print(f"  sample (end): {u}")

    print()
    print("=" * 70)
    print("2. Do job detail pages link back to /companies/<id>-?")
    print("=" * 70)
    known_company_ids = {"2": "truecaller", "9": "meltwater-group", "20": "beemobile", "2160": "varner"}
    checked = 0
    matches = {}
    for u in job_urls:
        if checked >= 60:
            break
        try:
            jr = client.get(u)
        except Exception as e:
            print(f"{u} -> EXCEPTION {e}")
            checked += 1
            continue
        checked += 1
        company_links = sorted(set(re.findall(r'/companies/(\d+)-', jr.text)))
        if company_links:
            matches[u] = company_links
        for cid in company_links:
            if cid in known_company_ids:
                print(f"MATCH at position {job_urls.index(u)}: {u} -> company {cid} ({known_company_ids[cid]})")

    print(f"\nChecked {checked} of {len(job_urls)} job detail pages.")
    print(f"Pages with a /companies/<id>- backlink: {len(matches)}/{checked}")
    for u, cids in list(matches.items())[:5]:
        print(f"  {u} -> {cids}")

    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
