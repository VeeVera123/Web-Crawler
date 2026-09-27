"""One-off, temporary live verification of OpenAI's BrassRing lead:
does the real TGnewUI JobDetails page actually contain a
"VerityZone:jobdescription" / "AnswerValue" field marker, or a
".jobdescriptionInJobDetails" DOM element? Checked against real,
currently-live job detail pages before writing any extraction code.
Removed once confirmed either way.
"""
import re
import sys
import requests
from bs4 import BeautifulSoup

BOARDS = [("sjobs.brassring.com", "16030", "6100"), ("sjobs.brassring.com", "16030", "6086"),
          ("krb-sjobs.brassring.com", "25008", "5131")]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
}
JOB_ID_RE = re.compile(r"(?:[?&](?:jobid|jobId|Areq|reqid)=)([^&#]+)", re.I)


def main() -> int:
    session = requests.Session()
    for host, partner_id, site_id in BOARDS:
        print("=" * 70)
        print(f"{host} partnerid={partner_id} siteid={site_id}")
        print("=" * 70)
        base = f"https://{host}"
        prime = session.get(f"{base}/TGnewUI/Search/home/Home",
                             params={"partnerid": partner_id, "siteid": site_id},
                             headers=HEADERS, timeout=30)
        search = session.get(
            f"{base}/TGnewUI/Search/home/HomeWithPreLoad",
            params={"partnerid": partner_id, "siteid": site_id,
                    "PageType": "searchResults", "SearchType": "linkquery"},
            headers={**HEADERS, "Referer": str(prime.url)}, timeout=30,
        )
        soup = BeautifulSoup(search.text, "html.parser")
        job_id = None
        for a in soup.find_all("a", href=True):
            m = JOB_ID_RE.search(str(a.get("href") or ""))
            if m:
                job_id = m.group(1)
                break
        if not job_id:
            print("could not find a real job id on the search results page, skipping")
            continue

        detail_url = (f"{base}/TGnewUI/Search/home/HomeWithPreLoad"
                      f"?PageType=JobDetails&partnerid={partner_id}&siteid={site_id}&jobid={job_id}")
        r = session.get(detail_url, headers={**HEADERS, "Referer": str(search.url)}, timeout=30)
        print(f"detail page for jobid={job_id}: status={r.status_code}, {len(r.text)} chars")
        html = r.text

        for needle in ("VerityZone", "jobdescription", "AnswerValue",
                       "jobdescriptionInJobDetails", "JobDetailFieldsToDisplay",
                       "ActualValueFromSolar"):
            count = html.count(needle)
            print(f"  {needle!r}: {count} occurrence(s)")

        m = re.search(r"VerityZone\s*[:=]\s*[\"']?jobdescription", html, re.I)
        if m:
            print(f"  VerityZone:jobdescription found at char {m.start()}")
            context = html[max(0, m.start() - 500):m.start() + 100]
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
    sys.exit(main())
