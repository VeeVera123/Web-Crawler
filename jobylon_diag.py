"""Round 4: job detail pages have 0/60 backlinks to /companies/<id>- (the
marker the current scraper's fallback searches for), so that whole
matching strategy is dead regardless of cap size. But sitemap-jobs.xml's
9133 URL slugs visibly encode "<job_id>-<company-name-slug>-<job-title-
slug>" (e.g. "385238-hema-teamleider-winkel", "356-adnoesis-java-
utvecklare"). This checks whether the URL slug itself can identify a
job's company (prefix match against the known company_slug, zero extra
requests beyond the one sitemap fetch), and confirms a match is real by
checking the fetched detail page's embedded jbl_company_id JS variable
(the same one seen on the company page) against the expected numeric id.
"""
import re
import sys
import httpx

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}

KNOWN = [("2", "truecaller"), ("9", "meltwater-group"), ("20", "beemobile"), ("2160", "varner")]


def main() -> int:
    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=60, http2=True)

    r = client.get("https://emp.jobylon.com/sitemap-jobs.xml")
    job_urls = re.findall(r"<loc>([^<]+)</loc>", r.text)
    print(f"sitemap-jobs.xml: {len(job_urls)} URLs\n")

    for company_id, company_slug in KNOWN:
        print("=" * 70)
        print(f"company {company_id}-{company_slug}")
        print("=" * 70)
        pat = re.compile(rf"/jobs/\d+-{re.escape(company_slug)}(-|/)", re.I)
        matched = [u for u in job_urls if pat.search(u)]
        print(f"  slug-prefix matches in sitemap: {len(matched)}")
        for u in matched[:3]:
            print(f"    {u}")
        if matched:
            jr = client.get(matched[0])
            m = re.search(r"jbl_company_id\s*=\s*(\d+)", jr.text)
            print(f"  first match's embedded jbl_company_id: {m.group(1) if m else None} "
                  f"(expected {company_id}) -> {'MATCH' if m and m.group(1) == company_id else 'MISMATCH/MISSING'}")
        print()

    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
