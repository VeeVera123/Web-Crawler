"""
Scrapability Test — 8 Previously-Blacklisted ATSs
===================================================
Hits one real endpoint per ATS to verify we can get job data back.
Run: python test_blacklisted_ats.py
"""

import requests
import re
import json
import time
import uuid
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
RESULTS = {}


def test_result(name, success, jobs_found, sample_title="", sample_location="", notes=""):
    RESULTS[name] = {"success": success, "jobs": jobs_found, "sample": sample_title, "location": sample_location, "notes": notes}
    status = "OK" if success else "FAIL"
    log.info(f"  [{status}] {name}: {jobs_found} jobs | sample: \"{sample_title[:60]}\" | loc: \"{sample_location[:60]}\" | {notes}")


# ═══════════════════════════════════════════════════════════════
# 1. TALEO — REST API
# ═══════════════════════════════════════════════════════════════
def test_taleo():
    log.info("\n=== 1. TALEO ===")
    # Using a known Taleo company: Oracle itself
    company = "oracle"
    section = "ex"
    base = f"https://{company}.taleo.net"

    # Step 1: Get the career page to extract portal ID and CSRF token
    try:
        careers_url = f"{base}/careersection/{section}/jobsearch.ftl?lang=en"
        r = requests.get(careers_url, timeout=15, headers=HEADERS, allow_redirects=True)
        log.info(f"  Career page status: {r.status_code}, length: {len(r.text)}")

        # Extract portal ID from the page
        portal_match = re.search(r'portal\s*=\s*(\d+)', r.text)
        if not portal_match:
            portal_match = re.search(r'portalId\s*[:=]\s*["\']?(\d+)', r.text)

        if portal_match:
            portal_id = portal_match.group(1)
            log.info(f"  Portal ID: {portal_id}")
        else:
            portal_id = "101430233"  # fallback known portal
            log.info(f"  Portal ID not found in page, using fallback: {portal_id}")

        # Step 2: Try the REST API
        api_url = f"{base}/careersection/rest/jobboard/searchjobs?lang=en&portal={portal_id}"
        payload = {
            "multilineEnabled": False,
            "sortingSelection": {"sortBySelectionParam": "3", "ascendingSortingOrder": "false"},
            "fieldData": {"fields": {"KEYWORD": "", "LOCATION": "", "ORGANIZATION": "", "JOB_NUMBER": ""},
                          "valid": True},
            "filterSelectionParam": {"searchFilterSelections": []},
            "advancedSearchFiltersSelectionParam": {"searchFilterSelections": []},
            "pageNo": 1,
        }

        # Need to use session to carry cookies from the career page
        session = requests.Session()
        session.headers.update(HEADERS)
        session.get(careers_url, timeout=15)

        r2 = session.post(api_url, json=payload, timeout=15)
        log.info(f"  API status: {r2.status_code}, content-type: {r2.headers.get('content-type', '')}")

        if r2.status_code == 200:
            try:
                data = r2.json()
                jobs = data.get("requisitionList", [])
                total = data.get("pagingData", {}).get("totalCount", len(jobs))
                if jobs:
                    j = jobs[0]
                    title = j.get("column", [""])[0] if isinstance(j.get("column"), list) else ""
                    location = j.get("column", ["", ""])[1] if isinstance(j.get("column"), list) and len(j.get("column", [])) > 1 else ""
                    test_result("Taleo", True, total, title, location, "REST API works")
                else:
                    test_result("Taleo", False, 0, notes=f"API returned empty. Keys: {list(data.keys())}")
            except Exception as e:
                # Maybe it's not JSON
                test_result("Taleo", False, 0, notes=f"Response not JSON: {str(e)[:100]}")
        else:
            test_result("Taleo", False, 0, notes=f"API returned {r2.status_code}")
    except Exception as e:
        test_result("Taleo", False, 0, notes=f"Error: {str(e)[:150]}")


# ═══════════════════════════════════════════════════════════════
# 2. ORACLE CLOUD HCM — Public REST API
# ═══════════════════════════════════════════════════════════════
def test_oracle_cloud():
    log.info("\n=== 2. ORACLE CLOUD HCM ===")
    # Using a known Oracle Cloud tenant
    domain = "eeho.fa.us2.oraclecloud.com"
    site_number = "CX_1"

    try:
        listings_url = (
            f"https://{domain}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
            f"?onlyData=true&expand=requisitionList.workLocation"
            f"&finder=findReqs;siteNumber={site_number},limit=5,offset=0"
        )
        headers = {
            **HEADERS,
            "ora-irc-cx-userid": str(uuid.uuid4()),
            "ora-irc-language": "en",
            "content-type": "application/vnd.oracle.adf.resourceitem+json;charset=utf-8",
        }

        r = requests.get(listings_url, timeout=20, headers=headers)
        log.info(f"  API status: {r.status_code}")

        if r.status_code == 200:
            data = r.json()
            items = data.get("items", [])
            if items:
                search_item = items[0]
                jobs = search_item.get("requisitionList", [])
                total = search_item.get("TotalJobsCount", len(jobs))
                if jobs:
                    j = jobs[0]
                    test_result("Oracle Cloud HCM", True, total,
                               j.get("Title", ""), j.get("PrimaryLocation", ""),
                               f"REST API works, job ID: {j.get('Id', '')}")
                else:
                    test_result("Oracle Cloud HCM", False, 0, notes="No jobs in requisitionList")
            else:
                test_result("Oracle Cloud HCM", False, 0, notes=f"Empty items. Keys: {list(data.keys())}")
        else:
            test_result("Oracle Cloud HCM", False, 0, notes=f"HTTP {r.status_code}")
    except Exception as e:
        test_result("Oracle Cloud HCM", False, 0, notes=f"Error: {str(e)[:150]}")


# ═══════════════════════════════════════════════════════════════
# 3. BRASSRING — AJAX API
# ═══════════════════════════════════════════════════════════════
def test_brassring():
    log.info("\n=== 3. BRASSRING ===")
    # Using IBM careers (known BrassRing user)
    partner_id = "26059"
    site_id = "5016"

    try:
        # First visit the search page to get cookies
        session = requests.Session()
        session.headers.update(HEADERS)
        home_url = f"https://krb-sjobs.brassring.com/TGnewUI/Search/Home/Home?partnerid={partner_id}&siteid={site_id}"
        r1 = session.get(home_url, timeout=15)
        log.info(f"  Home page status: {r1.status_code}, length: {len(r1.text)}")

        # Try the AJAX endpoint
        ajax_url = "https://krb-sjobs.brassring.com/TgNewUI/Search/Ajax/MatchedJobs"
        payload = {
            "PartnerId": partner_id,
            "SiteId": site_id,
            "Keyword": "",
            "Location": "",
            "LanguageCode": "EN",
            "PageNumber": 1,
            "ExactROC": "false",
            "ExactRad": "false",
            "ExactTitle": "false",
        }

        r2 = session.post(ajax_url, json=payload, timeout=15,
                         headers={**HEADERS, "Content-Type": "application/json"})
        log.info(f"  AJAX status: {r2.status_code}")

        if r2.status_code == 200:
            data = r2.json()
            # BrassRing returns various structures
            jobs = data.get("Jobs", data.get("jobs", []))
            total = data.get("TotalHits", data.get("totalHits", len(jobs)))

            if isinstance(jobs, list) and jobs:
                j = jobs[0]
                title = j.get("Title", j.get("title", ""))
                location = j.get("Location", j.get("location", ""))
                test_result("BrassRing", True, total, title, location, "AJAX API works")
            else:
                # Log what we got back
                test_result("BrassRing", True if total > 0 else False, total,
                           notes=f"Response keys: {list(data.keys())[:10]}")
        else:
            test_result("BrassRing", False, 0, notes=f"HTTP {r2.status_code}")
    except Exception as e:
        test_result("BrassRing", False, 0, notes=f"Error: {str(e)[:150]}")


# ═══════════════════════════════════════════════════════════════
# 4. PAYLOCITY — Embedded window.pageData JSON
# ═══════════════════════════════════════════════════════════════
def test_paylocity():
    log.info("\n=== 4. PAYLOCITY ===")
    # Try a known Paylocity career page
    url = "https://recruiting.paylocity.com/Recruiting/Jobs/All/e2bcef5a-b6e5-4c5a-8fdd-c4da179dd98c"

    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        log.info(f"  Page status: {r.status_code}, length: {len(r.text)}")

        # Look for window.pageData
        pd_match = re.search(r'window\.pageData\s*=\s*(\{.*?\});\s*</script>', r.text, re.DOTALL)
        if pd_match:
            try:
                page_data = json.loads(pd_match.group(1))
                jobs = page_data.get("jobs", page_data.get("Jobs", []))
                if isinstance(jobs, list) and jobs:
                    j = jobs[0]
                    title = j.get("JobTitle", j.get("Title", ""))
                    location = j.get("LocationName", j.get("Location", ""))
                    test_result("Paylocity", True, len(jobs), title, location, "window.pageData works")
                else:
                    test_result("Paylocity", False, 0, notes=f"pageData keys: {list(page_data.keys())[:10]}")
            except json.JSONDecodeError as e:
                test_result("Paylocity", False, 0, notes=f"JSON parse error: {str(e)[:100]}")
        else:
            # Try the feed API instead
            log.info("  No window.pageData found, trying feed API...")
            # Also check for JSON-LD
            ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', r.text, re.DOTALL)
            if ld_matches:
                test_result("Paylocity", True, len(ld_matches), notes=f"Found {len(ld_matches)} JSON-LD blocks")
            else:
                # Try alternative: look for job data in any script tag
                job_count = len(re.findall(r'job-listing|JobTitle|jobTitle', r.text, re.I))
                test_result("Paylocity", False, 0, notes=f"No pageData or JSON-LD. Job markers: {job_count}. HTML has {len(r.text)} chars")
    except Exception as e:
        test_result("Paylocity", False, 0, notes=f"Error: {str(e)[:150]}")


# ═══════════════════════════════════════════════════════════════
# 5. ZOHO RECRUIT — JSON-LD / embedded JSON
# ═══════════════════════════════════════════════════════════════
def test_zoho_recruit():
    log.info("\n=== 5. ZOHO RECRUIT ===")
    # Try a known Zoho Recruit career page
    # Let's try to find one first
    test_urls = [
        "https://careers.zohocorp.com/jobs/Careers",
        "https://www.zoho.com/recruit/",  # Zoho's own
    ]

    for url in test_urls:
        try:
            r = requests.get(url, timeout=15, headers=HEADERS, allow_redirects=True)
            log.info(f"  {url}: status={r.status_code}, length={len(r.text)}")

            # Check for JSON-LD
            ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', r.text, re.DOTALL)
            for i, ld_text in enumerate(ld_matches):
                try:
                    ld = json.loads(ld_text)
                    if isinstance(ld, dict) and ld.get("@type") == "JobPosting":
                        test_result("Zoho Recruit", True, 1,
                                   ld.get("title", ""),
                                   str(ld.get("jobLocation", {}).get("address", {}).get("addressLocality", "")),
                                   "JSON-LD JobPosting found")
                        return
                    elif isinstance(ld, list):
                        for item in ld:
                            if isinstance(item, dict) and item.get("@type") == "JobPosting":
                                test_result("Zoho Recruit", True, len(ld),
                                           item.get("title", ""), "", "JSON-LD array found")
                                return
                except json.JSONDecodeError:
                    pass

            # Check for embedded input#jobs JSON
            jobs_input = re.search(r'<input[^>]*id=["\']jobs["\'][^>]*value=["\']([^"\']+)["\']', r.text, re.I)
            if jobs_input:
                try:
                    import html as html_mod
                    jobs_json = html_mod.unescape(jobs_input.group(1))
                    jobs = json.loads(jobs_json)
                    if isinstance(jobs, list) and jobs:
                        j = jobs[0]
                        test_result("Zoho Recruit", True, len(jobs),
                                   j.get("title", j.get("Posting_Title", "")), "",
                                   "Embedded input#jobs JSON works")
                        return
                except:
                    pass

            # Check for job listing elements
            job_markers = re.findall(r'ziabot-job-listing|cw-job-listing|job-listing-row', r.text, re.I)
            if job_markers:
                test_result("Zoho Recruit", True, len(job_markers), notes=f"Found {len(job_markers)} job listing elements")
                return

        except Exception as e:
            log.info(f"  Error with {url}: {str(e)[:100]}")

    test_result("Zoho Recruit", False, 0, notes="Could not access any Zoho Recruit career page")


# ═══════════════════════════════════════════════════════════════
# 6. YCOMBINATOR — Public JSON API
# ═══════════════════════════════════════════════════════════════
def test_ycombinator():
    log.info("\n=== 6. YCOMBINATOR ===")
    try:
        # The public companies endpoint
        url = "https://www.workatastartup.com/companies"
        r = requests.get(url, timeout=15, headers={**HEADERS, "Accept": "application/json"})
        log.info(f"  /companies status: {r.status_code}, content-type: {r.headers.get('content-type', '')}")

        if r.status_code == 200 and 'json' in r.headers.get('content-type', ''):
            data = r.json()
            if isinstance(data, list) and data:
                company = data[0]
                title = company.get("name", "")
                jobs = company.get("jobs", company.get("job_count", 0))
                test_result("YCombinator", True, len(data), title, "",
                           f"Public API works, {len(data)} companies with jobs")
                return
            elif isinstance(data, dict):
                companies = data.get("companies", data.get("results", []))
                if companies:
                    test_result("YCombinator", True, len(companies),
                               companies[0].get("name", ""), "",
                               f"Public API works. Keys: {list(data.keys())[:5]}")
                    return

        # Fallback: try the HTML page and look for embedded JSON
        r2 = requests.get("https://www.workatastartup.com/", timeout=15, headers=HEADERS)
        log.info(f"  Homepage status: {r2.status_code}, length: {len(r2.text)}")

        # Check for __NEXT_DATA__ or similar embedded JSON
        next_data = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', r2.text, re.DOTALL)
        if next_data:
            test_result("YCombinator", True, 0, notes="Has __NEXT_DATA__ embedded JSON")
        else:
            # Check if it's at least rendering job content
            job_count = len(re.findall(r'company|startup|job', r2.text, re.I))
            test_result("YCombinator", False, 0, notes=f"No JSON API. HTML job markers: {job_count}")
    except Exception as e:
        test_result("YCombinator", False, 0, notes=f"Error: {str(e)[:150]}")


# ═══════════════════════════════════════════════════════════════
# 7. JAZZHR — HTML scrape + potential JSON-LD
# ═══════════════════════════════════════════════════════════════
def test_jazzhr():
    log.info("\n=== 7. JAZZHR ===")
    # Try a known JazzHR career page
    test_urls = [
        "https://theapplicantmanager.com",  # skip, different ATS
    ]

    # JazzHR pages are at app.jazz.co/{company}
    # Let's try to find job links
    try:
        # Try a sample JazzHR URL
        url = "https://app.jazz.co/app"
        r = requests.get(url, timeout=15, headers=HEADERS, allow_redirects=True)
        log.info(f"  jazz.co status: {r.status_code}, length: {len(r.text)}")

        # The Resumator API (JazzHR's legacy API name)
        # Try: https://api.resumatorapi.com/v1/jobs?apikey=... — needs API key
        # Instead, try the public feed endpoint
        # JazzHR embeds job board at https://[company].applytojob.com or app.jazz.co/[company]

        # Let's try a known JazzHR company feed
        # According to OpenPostings, JazzHR uses resumator-job-title-link class
        test_result("JazzHR", False, 0, notes="Need a specific company slug to test. Will test during implementation.")
    except Exception as e:
        test_result("JazzHR", False, 0, notes=f"Error: {str(e)[:150]}")


# ═══════════════════════════════════════════════════════════════
# 8. HRMDIRECT — Simple HTML scrape
# ═══════════════════════════════════════════════════════════════
def test_hrmdirect():
    log.info("\n=== 8. HRMDIRECT ===")
    # Try a known HRMDirect company
    test_urls = [
        "https://www.hrmdirect.com/employment/job-openings.php?search=true",
    ]

    try:
        # First, search for an actual HRMDirect customer
        # HRMDirect URLs are like: https://[company].hrmdirect.com/employment/job-openings.php
        # Let's try a few known ones
        for company in ["colliersint", "apexgroup", "meijer"]:
            url = f"https://{company}.hrmdirect.com/employment/job-openings.php"
            try:
                r = requests.get(url, timeout=10, headers=HEADERS, allow_redirects=True)
                log.info(f"  {company}: status={r.status_code}, length={len(r.text)}")

                if r.status_code == 200 and len(r.text) > 1000:
                    # Look for job table rows
                    # HRMDirect uses: reqitem, posTitle, cities, state, departments
                    req_items = re.findall(r'class=["\']reqitem["\']', r.text, re.I)
                    pos_titles = re.findall(r'class=["\']posTitle["\']', r.text, re.I)

                    # Also try generic job-link patterns
                    job_links = re.findall(r'href=["\']([^"\']*job-opening\.php\?req_id=\d+[^"\']*)["\']', r.text, re.I)

                    job_count = max(len(req_items), len(pos_titles), len(job_links))

                    if job_count > 0:
                        # Extract a sample title
                        title_match = re.search(r'class=["\']posTitle["\'][^>]*>([^<]+)<', r.text, re.I)
                        title = title_match.group(1).strip() if title_match else ""

                        loc_match = re.search(r'class=["\']cities["\'][^>]*>([^<]+)<', r.text, re.I)
                        location = loc_match.group(1).strip() if loc_match else ""

                        test_result("HRMDirect", True, job_count, title, location, f"HTML scrape works ({company})")
                        return
                    else:
                        log.info(f"    No job markers found in {company}")
            except Exception as e:
                log.info(f"    {company} error: {str(e)[:80]}")

        test_result("HRMDirect", False, 0, notes="No working HRMDirect company found in test set")
    except Exception as e:
        test_result("HRMDirect", False, 0, notes=f"Error: {str(e)[:150]}")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    log.info("=" * 70)
    log.info("SCRAPABILITY TEST — 8 Previously-Blacklisted ATSs")
    log.info("=" * 70)

    tests = [
        ("Taleo", test_taleo),
        ("Oracle Cloud HCM", test_oracle_cloud),
        ("BrassRing", test_brassring),
        ("Paylocity", test_paylocity),
        ("Zoho Recruit", test_zoho_recruit),
        ("YCombinator", test_ycombinator),
        ("JazzHR", test_jazzhr),
        ("HRMDirect", test_hrmdirect),
    ]

    for name, test_fn in tests:
        try:
            test_fn()
        except Exception as e:
            test_result(name, False, 0, notes=f"Unexpected error: {str(e)[:150]}")
        time.sleep(0.5)  # Be polite between tests

    # Summary
    log.info("\n" + "=" * 70)
    log.info("SUMMARY")
    log.info("=" * 70)

    ok = []
    fail = []
    for name, result in RESULTS.items():
        if result["success"]:
            ok.append(name)
            log.info(f"  ✓ {name}: {result['jobs']} jobs — {result['notes']}")
        else:
            fail.append(name)
            log.info(f"  ✗ {name}: {result['notes']}")

    log.info(f"\nPassed: {len(ok)}/{len(RESULTS)}")
    log.info(f"Failed: {len(fail)}/{len(RESULTS)}")
    if fail:
        log.info(f"Failed ATSs: {', '.join(fail)}")


if __name__ == "__main__":
    main()
