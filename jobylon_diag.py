"""Standalone, read-only diagnostic for the Jobylon scraper's 100%
failure rate (0 jobs / 40 boards failed in the last real crawl run).
Does not touch ats_scrapers.py, discovery.py, Supabase, or any
production crawl path -- only fetches public Jobylon URLs and prints
what it finds, so the real fix can be based on live evidence instead
of another guess.

Manual-dispatch only, via jobylon-diag.yml.
"""
import re
import sys
import time
import httpx

REAL_SLUGS = [
    "2-truecaller",
    "9-meltwater-group",
    "20-beemobile",
    "1364-dermicus",
    "2160-varner",
]

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


def main() -> int:
    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30, http2=True)

    print("=" * 70)
    print("1. Site-wide sitemap.xml")
    print("=" * 70)
    r = client.get("https://emp.jobylon.com/sitemap.xml")
    print(f"status={r.status_code} bytes={len(r.content)} content-type={r.headers.get('content-type')}")
    job_urls = re.findall(r"<loc>([^<]*/jobs/[^<]*)</loc>", r.text)
    print(f"total <loc> entries containing /jobs/: {len(job_urls)}")
    for u in job_urls[:5]:
        print(f"  sample: {u}")

    print()
    print("=" * 70)
    print("2. Real company pages")
    print("=" * 70)
    for slug in REAL_SLUGS:
        for path in (f"/companies/{slug}/", f"/companies/{slug.split('-')[0]}/"):
            url = f"https://emp.jobylon.com{path}"
            try:
                cr = client.get(url)
            except Exception as e:
                print(f"{url} -> EXCEPTION {e}")
                continue
            has_jobs_link = "/jobs/" in cr.text
            has_ld_json = "application/ld+json" in cr.text
            has_next_data = "__NEXT_DATA__" in cr.text or "_next/static" in cr.text
            print(f"{url} -> status={cr.status_code} final_url={cr.url} bytes={len(cr.content)} "
                  f"has_/jobs/_link={has_jobs_link} has_ld+json={has_ld_json} looks_like_js_app_shell={has_next_data}")
            if has_jobs_link:
                found = re.findall(r'href=["\']([^"\']*?/jobs/\d+[^"\']*)["\']', cr.text)
                print(f"    /jobs/ hrefs found: {found[:5]}")
        time.sleep(0.5)

    print()
    print("=" * 70)
    print("3. Where does each company's own postings land in the shared sitemap list?")
    print("=" * 70)
    if job_urls:
        for slug in REAL_SLUGS:
            company_id = slug.split("-")[0]
            marker = f"/companies/{company_id}-"
            # Check the first 400 (today's hardcoded cap) job detail pages
            # for a link back to this company -- but that's 400 * 5 = 2000
            # requests, too expensive for a diagnostic run. Instead just
            # report the sitemap's total size, which alone proves/disproves
            # whether a fixed 400-entry prefix scan could ever be enough.
            print(f"company {slug}: sitemap has {len(job_urls)} total job URLs site-wide "
                  f"(current scraper caps its per-company scan at 400)")

    print()
    print("=" * 70)
    print("4. Does a job detail page's HTML actually link back to /companies/{id}-?")
    print("=" * 70)
    for u in job_urls[:3]:
        try:
            jr = client.get(u)
        except Exception as e:
            print(f"{u} -> EXCEPTION {e}")
            continue
        company_links = re.findall(r'/companies/(\d+)-[^"\'<>]*', jr.text)
        print(f"{u} -> status={jr.status_code} bytes={len(jr.content)} "
              f"company_links_found={sorted(set(company_links))}")

    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
