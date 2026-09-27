"""One-off, temporary live verification of the BrassRing legacy AJAX
MatchedJobs endpoint's pagination/response shape, before building a
bespoke description fetcher on top of it. Removed once confirmed.
"""
import sys
import requests

BOARDS = [("sjobs.brassring.com", "16030", "6100"), ("krb-sjobs.brassring.com", "25008", "5131")]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}


def main() -> int:
    session = requests.Session()
    for host, partner_id, site_id in BOARDS:
        print("=" * 70)
        print(f"{host} partnerid={partner_id} siteid={site_id}")
        print("=" * 70)
        base = f"https://{host}"
        # Prime session like scrape_brassring does.
        session.get(f"{base}/TGnewUI/Search/home/Home",
                    params={"partnerid": partner_id, "siteid": site_id}, headers=HEADERS, timeout=30)

        seen_ids = set()
        for pagenum in (1, 2, 3):
            r = session.post(
                f"{base}/TGnewUI/Search/Ajax/MatchedJobs",
                data={"partnerid": partner_id, "siteid": site_id, "keyword": "",
                      "location": "", "pagenum": str(pagenum), "sortBy": "posteddate", "SortType": "desc"},
                headers=HEADERS, timeout=30,
            )
            print(f"page {pagenum}: status={r.status_code}")
            if r.status_code != 200:
                continue
            try:
                data = r.json()
            except Exception as e:
                print(f"  JSON parse failed: {e}")
                continue
            print(f"  top-level keys: {sorted(data.keys()) if isinstance(data, dict) else type(data)}")
            arr = data.get("Jobs") if isinstance(data, dict) else None
            if isinstance(arr, list):
                ids_this_page = [j.get("AutoReqId") or j.get("JobId") or j.get("Areq") for j in arr if isinstance(j, dict)]
                new_ids = [i for i in ids_this_page if i not in seen_ids]
                seen_ids.update(ids_this_page)
                print(f"  {len(arr)} jobs, {len(new_ids)} new ids, sample ids: {ids_this_page[:3]}")
                if arr:
                    sample = arr[0]
                    desc_val = sample.get("formattedShortDescription") or sample.get("Description") or ""
                    print(f"  sample job keys: {sorted(sample.keys())}")
                    print(f"  sample description field length: {len(desc_val)}, first 200 chars: {desc_val[:200]!r}")
            # Look for any pagination metadata anywhere in the payload.
            for key in ("totalCount", "TotalCount", "total", "Total", "hasMore", "HasMore", "pageCount", "PageCount"):
                if isinstance(data, dict) and key in data:
                    print(f"  pagination field {key!r} = {data[key]!r}")
        print(f"total unique ids seen across pages 1-3: {len(seen_ids)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
