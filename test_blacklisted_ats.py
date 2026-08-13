"""
Scrapability Test — 8 Previously-Blacklisted ATSs (v2)
=======================================================
Hits one real endpoint per ATS to verify we can get job data back.
Run: python test_blacklisted_ats.py
"""

import requests
import re
import json
import time
import uuid
import logging
from bs4 import BeautifulSoup

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
    # Try multiple known Taleo companies
    companies = [
        ("capps", "479", None),        # Texas state government
        ("hdr", "ex", None),            # HDR Engineering
        ("jacobs", "ex", None),         # Jacobs Engineering
    ]

    for company, section, portal_override in companies:
        base = f"https://{company}.taleo.net"
        careers_url = f"{base}/careersection/{section}/jobsearch.ftl?lang=en"
        log.info(f"  Trying {company}.taleo.net section={section}...")

        try:
            session = requests.Session()
            session.headers.update(HEADERS)
            r1 = session.get(careers_url, timeout=15, allow_redirects=True)
            log.info(f"    Career page: status={r1.status_code}, length={len(r1.text)}")

            if r1.status_code != 200:
                continue

            # Extract portal ID
            portal_id = portal_override
            if not portal_id:
                for pat in [
                    r'ftlcompanyid\s*=\s*["\']?(\d+)',
                    r'portal\s*=\s*["\']?(\d+)',
                    r'portalId\s*[:=]\s*["\']?(\d+)',
                    r'"portal"\s*:\s*"?(\d+)',
                ]:
                    m = re.search(pat, r1.text, re.I)
                    if m:
                        portal_id = m.group(1)
                        break

            if not portal_id:
                log.info(f"    No portal ID found, trying without it...")
                portal_id = "0"

            # Try REST API
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

            r2 = session.post(api_url, json=payload, timeout=15,
                             headers={"Content-Type": "application/json"})
            log.info(f"    API: status={r2.status_code}")

            if r2.status_code == 200:
                try:
                    data = r2.json()
                    jobs = data.get("requisitionList", [])
                    total = data.get("pagingData", {}).get("totalCount", len(jobs))

                    if jobs:
                        j = jobs[0]
                        cols = j.get("column", [])
                        title = cols[0] if len(cols) > 0 else ""
                        location = cols[1] if len(cols) > 1 else ""
                        test_result("Taleo", True, total, title, location,
                                   f"REST API works ({company})")
                        return
                    else:
                        log.info(f"    Empty jobs. Keys: {list(data.keys())}")
                except Exception as e:
                    log.info(f"    JSON parse error: {str(e)[:100]}")

            # Fallback: try AJAX endpoint
            ajax_url = f"{base}/careersection/{section}/jobsearch.ajax"
            r3 = session.post(ajax_url, timeout=15,
                             data={"requisitionListInterface.reqTitleRecordsPerPage": "25",
                                   "requisitionListInterface.viewAllRecords": "1"})
            log.info(f"    AJAX fallback: status={r3.status_code}, length={len(r3.text)}")

            if r3.status_code == 200 and len(r3.text) > 500:
                # Count job links in the AJAX response
                job_links = re.findall(r'jobdetail\.ftl\?job=(\d+)', r3.text)
                if job_links:
                    title_match = re.search(r'class="[^"]*jobTitle[^"]*"[^>]*>([^<]+)', r3.text, re.I)
                    title = title_match.group(1).strip() if title_match else ""
                    test_result("Taleo", True, len(job_links), title, "",
                               f"AJAX fallback works ({company})")
                    return

        except Exception as e:
            log.info(f"    Error: {str(e)[:120]}")

    test_result("Taleo", False, 0, notes="All test companies failed")


# ═══════════════════════════════════════════════════════════════
# 2. ORACLE CLOUD HCM — Public REST API
# ═══════════════════════════════════════════════════════════════
def test_oracle_cloud():
    log.info("\n=== 2. ORACLE CLOUD HCM ===")
    domain = "eeho.fa.us2.oraclecloud.com"
    site_number = "CX_1"

    try:
        finder = f"findReqs;siteNumber={site_number},limit=5,offset=0"
        listings_url = (
            f"https://{domain}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
            f"?onlyData=true&expand=requisitionList.workLocation&finder={finder}"
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
                    return
        test_result("Oracle Cloud HCM", False, 0, notes=f"HTTP {r.status_code}")
    except Exception as e:
        test_result("Oracle Cloud HCM", False, 0, notes=f"Error: {str(e)[:150]}")


# ═══════════════════════════════════════════════════════════════
# 3. BRASSRING — AJAX API
# ═══════════════════════════════════════════════════════════════
def test_brassring():
    log.info("\n=== 3. BRASSRING ===")
    # Try multiple known BrassRing companies
    companies = [
        ("sjobs.brassring.com", "25212", "5164"),   # AAFES
        ("sjobs.brassring.com", "455", "185"),       # Transformco (Sears)
        ("sjobs.brassring.com", "25526", "5032"),    # Home Depot
        ("sjobs.brassring.com", "25633", "5439"),    # Infosys
    ]

    for host, partner_id, site_id in companies:
        log.info(f"  Trying {host} partner={partner_id} site={site_id}...")
        try:
            session = requests.Session()
            session.headers.update(HEADERS)

            # First visit search page to get cookies/tokens
            home_url = f"https://{host}/TGnewUI/Search/Home/Home?partnerid={partner_id}&siteid={site_id}"
            r1 = session.get(home_url, timeout=15, allow_redirects=True)
            log.info(f"    Home: status={r1.status_code}, length={len(r1.text)}")

            if r1.status_code != 200:
                continue

            # Extract any tokens from the page
            token_match = re.search(r'__RequestVerificationToken["\'][^>]*value=["\']([^"\']+)', r1.text, re.I)
            if not token_match:
                token_match = re.search(r'name=["\']__RequestVerificationToken["\'][^>]*value=["\']([^"\']+)', r1.text, re.I)

            # Try the AJAX endpoint with different payload formats
            ajax_url = f"https://{host}/TgNewUI/Search/Ajax/MatchedJobs"

            # Payload format from OpenPostings guide
            payload = {
                "partnerId": partner_id,
                "siteId": site_id,
                "keyword": "",
                "location": "",
                "keywordCustomSol498": "",
                "locationCustomSol498": "",
                "keywordCustomSol498": "",
            }

            extra_headers = {"Content-Type": "application/json",
                            "X-Requested-With": "XMLHttpRequest"}
            if token_match:
                extra_headers["__RequestVerificationToken"] = token_match.group(1)

            r2 = session.post(ajax_url, json=payload, timeout=15, headers=extra_headers)
            log.info(f"    AJAX: status={r2.status_code}, length={len(r2.text)}")

            if r2.status_code == 200 and len(r2.text) > 50:
                try:
                    data = r2.json()
                    log.info(f"    Response keys: {list(data.keys())[:10]}")

                    # BrassRing response varies — check multiple keys
                    jobs = (data.get("Jobs") or data.get("jobs") or
                           data.get("JobList") or data.get("Rows") or [])
                    total = (data.get("TotalHits") or data.get("totalHits") or
                            data.get("TotalCount") or data.get("totalCount") or len(jobs))

                    if isinstance(jobs, list) and jobs:
                        j = jobs[0]
                        title = j.get("Title", j.get("title", j.get("JobTitle", "")))
                        location = j.get("Location", j.get("location", ""))
                        test_result("BrassRing", True, total, title, location,
                                   f"AJAX works (partner={partner_id})")
                        return
                    elif total and int(total) > 0:
                        test_result("BrassRing", True, total, notes=f"Got total={total} but couldn't parse jobs. Keys: {list(data.keys())}")
                        return
                    else:
                        log.info(f"    No jobs found in response")
                except Exception as e:
                    log.info(f"    JSON parse error: {str(e)[:100]}")

            # Fallback: try HTML scraping the search results page
            if len(r1.text) > 1000:
                soup = BeautifulSoup(r1.text, "html.parser")
                job_rows = soup.select("[class*='jobTitle'], [class*='job-title'], a[href*='JobDetails']")
                if job_rows:
                    test_result("BrassRing", True, len(job_rows),
                               job_rows[0].get_text(strip=True)[:60], "",
                               f"HTML scrape fallback (partner={partner_id})")
                    return

        except Exception as e:
            log.info(f"    Error: {str(e)[:120]}")

    test_result("BrassRing", False, 0, notes="All test companies failed")


# ═══════════════════════════════════════════════════════════════
# 4. PAYLOCITY — Embedded JSON + Feed API
# ═══════════════════════════════════════════════════════════════
def test_paylocity():
    log.info("\n=== 4. PAYLOCITY ===")

    # Try multiple known Paylocity career pages
    test_pages = [
        ("The-Guidance-Center", "9b6dbe18-295a-4b4e-bcaf-f7e9bbb28161"),
        ("DCCC", None),  # We'll construct from the job detail URL
    ]

    for company_slug, company_id in test_pages:
        if not company_id:
            continue
        url = f"https://recruiting.paylocity.com/recruiting/jobs/All/{company_id}/{company_slug}"
        log.info(f"  Trying {company_slug}...")

        try:
            r = requests.get(url, timeout=15, headers=HEADERS, allow_redirects=True)
            log.info(f"    Status: {r.status_code}, length: {len(r.text)}")

            if r.status_code != 200:
                continue

            # Method 1: window.pageData
            pd_match = re.search(r'window\.pageData\s*=\s*(\{.*?\});\s*</script>', r.text, re.DOTALL)
            if pd_match:
                try:
                    page_data = json.loads(pd_match.group(1))
                    jobs = page_data.get("jobs", page_data.get("Jobs", []))
                    if isinstance(jobs, list) and jobs:
                        j = jobs[0]
                        title = j.get("JobTitle", j.get("Title", ""))
                        location = j.get("LocationName", j.get("Location", ""))
                        test_result("Paylocity", True, len(jobs), title, location,
                                   "window.pageData works")
                        return
                except json.JSONDecodeError:
                    pass

            # Method 2: JSON-LD
            ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', r.text, re.DOTALL)
            for ld_text in ld_matches:
                try:
                    ld = json.loads(ld_text)
                    if isinstance(ld, dict) and ld.get("@type") == "JobPosting":
                        test_result("Paylocity", True, 1, ld.get("title", ""),
                                   str(ld.get("jobLocation", "")), "JSON-LD found")
                        return
                except:
                    pass

            # Method 3: Check if it's a JS-rendered page
            log.info(f"    Checking for JS rendering markers...")
            is_spa = any(marker in r.text.lower() for marker in
                        ['__next_data__', 'react-root', 'ng-app', 'vue-app', 'app-root'])
            log.info(f"    SPA markers: {is_spa}")

        except Exception as e:
            log.info(f"    Error: {str(e)[:120]}")

    # Method 4: Try the official Paylocity Job Feed API
    log.info("  Trying Paylocity Feed API v2...")
    try:
        feed_url = "https://recruiting.paylocity.com/Recruiting/v2/api/feed/documentation"
        r = requests.get(feed_url, timeout=10, headers=HEADERS)
        log.info(f"    Feed API docs: status={r.status_code}, length={len(r.text)}")
        if r.status_code == 200 and len(r.text) > 500:
            test_result("Paylocity", True, 0, notes=f"Feed API v2 docs accessible ({len(r.text)} chars). Likely JS-rendered career pages but feed API available.")
            return
    except:
        pass

    test_result("Paylocity", False, 0, notes="Career pages appear JS-rendered. Need feed API or headless browser.")


# ═══════════════════════════════════════════════════════════════
# 5. ZOHO RECRUIT — JSON-LD / embedded JSON
# ═══════════════════════════════════════════════════════════════
def test_zoho_recruit():
    log.info("\n=== 5. ZOHO RECRUIT ===")
    # Zoho's own careers page uses Zoho Recruit
    url = "https://careers.zohocorp.com/jobs/Careers"

    try:
        r = requests.get(url, timeout=15, headers=HEADERS, allow_redirects=True)
        log.info(f"  Status: {r.status_code}, length: {len(r.text)}")

        if r.status_code != 200:
            test_result("Zoho Recruit", False, 0, notes=f"HTTP {r.status_code}")
            return

        # Method 1: JSON-LD
        ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', r.text, re.DOTALL)
        for ld_text in ld_matches:
            try:
                ld = json.loads(ld_text)
                if isinstance(ld, dict) and ld.get("@type") == "JobPosting":
                    loc = ld.get("jobLocation", {})
                    if isinstance(loc, dict):
                        loc_str = loc.get("address", {}).get("addressLocality", "") if isinstance(loc.get("address"), dict) else str(loc)
                    else:
                        loc_str = str(loc)
                    test_result("Zoho Recruit", True, len(ld_matches),
                               ld.get("title", ""), loc_str, "JSON-LD JobPosting found")
                    return
                elif isinstance(ld, list):
                    job_postings = [x for x in ld if isinstance(x, dict) and x.get("@type") == "JobPosting"]
                    if job_postings:
                        test_result("Zoho Recruit", True, len(job_postings),
                                   job_postings[0].get("title", ""), "", "JSON-LD array")
                        return
            except:
                pass

        # Method 2: embedded input#jobs
        import html as html_mod
        jobs_input = re.search(r'<input[^>]*id=["\']jobs["\'][^>]*value=["\']([^"\']+)["\']', r.text, re.I)
        if jobs_input:
            try:
                jobs_json = html_mod.unescape(jobs_input.group(1))
                jobs = json.loads(jobs_json)
                if isinstance(jobs, list) and jobs:
                    j = jobs[0]
                    test_result("Zoho Recruit", True, len(jobs),
                               j.get("Posting_Title", j.get("title", "")), "",
                               "input#jobs JSON works")
                    return
            except:
                pass

        # Method 3: HTML parsing
        soup = BeautifulSoup(r.text, "html.parser")
        job_elements = soup.select(".cw-job-listing-container, .ziabot-job-listing-row, [class*='job-listing']")
        if job_elements:
            title = job_elements[0].get_text(strip=True)[:60] if job_elements else ""
            test_result("Zoho Recruit", True, len(job_elements), title, "",
                       f"HTML elements found ({len(job_elements)} listings)")
            return

        # Method 4: check any job-related content
        job_links = soup.select("a[href*='/jobs/'], a[href*='jobid']")
        if job_links:
            test_result("Zoho Recruit", True, len(job_links),
                       job_links[0].get_text(strip=True)[:60], "",
                       "Job links found in HTML")
            return

        test_result("Zoho Recruit", False, 0, notes=f"No extractable job data found. Page has {len(r.text)} chars")

    except Exception as e:
        test_result("Zoho Recruit", False, 0, notes=f"Error: {str(e)[:150]}")


# ═══════════════════════════════════════════════════════════════
# 6. YCOMBINATOR — workatastartup.com
# ═══════════════════════════════════════════════════════════════
def test_ycombinator():
    log.info("\n=== 6. YCOMBINATOR ===")

    # Try fetching the main page
    try:
        url = "https://www.workatastartup.com/companies"
        r = requests.get(url, timeout=15, headers={
            **HEADERS,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        log.info(f"  /companies: status={r.status_code}, length={len(r.text)}, ct={r.headers.get('content-type', '')[:50]}")

        if r.status_code == 200 and len(r.text) > 1000:
            # Check for __NEXT_DATA__ or embedded JSON
            next_data = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.DOTALL)
            if next_data:
                try:
                    nd = json.loads(next_data.group(1))
                    # Navigate the Next.js data structure
                    props = nd.get("props", {}).get("pageProps", {})
                    companies = props.get("companies", props.get("data", []))
                    if isinstance(companies, list) and companies:
                        c = companies[0]
                        name = c.get("name", c.get("company_name", ""))
                        test_result("YCombinator", True, len(companies), name, "",
                                   f"__NEXT_DATA__ with {len(companies)} companies")
                        return
                    else:
                        log.info(f"    __NEXT_DATA__ pageProps keys: {list(props.keys())[:10]}")
                        test_result("YCombinator", True, 0, notes=f"Has __NEXT_DATA__. Props keys: {list(props.keys())[:8]}")
                        return
                except json.JSONDecodeError as e:
                    log.info(f"    __NEXT_DATA__ parse error: {str(e)[:80]}")

            # Check for any React/JS app markers
            is_spa = "__NEXT_DATA__" in r.text or "react" in r.text.lower() or "_app" in r.text
            log.info(f"    SPA page: {is_spa}")

            # Look for any JSON in script tags
            for script in re.finditer(r'<script[^>]*>(.*?)</script>', r.text, re.DOTALL):
                content = script.group(1).strip()
                if len(content) > 500 and ('company' in content.lower() or 'job' in content.lower()):
                    log.info(f"    Found script with job/company data: {len(content)} chars")
                    log.info(f"    First 200: {content[:200]}")
                    break

        # Also try the jobs page directly
        url2 = "https://www.workatastartup.com/jobs"
        r2 = requests.get(url2, timeout=15, headers=HEADERS)
        log.info(f"  /jobs: status={r2.status_code}, length={len(r2.text)}")

        if r2.status_code == 200:
            next_data2 = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', r2.text, re.DOTALL)
            if next_data2:
                try:
                    nd2 = json.loads(next_data2.group(1))
                    props2 = nd2.get("props", {}).get("pageProps", {})
                    jobs = props2.get("jobs", props2.get("data", []))
                    if isinstance(jobs, list) and jobs:
                        j = jobs[0]
                        test_result("YCombinator", True, len(jobs),
                                   j.get("title", j.get("job_title", "")), "",
                                   f"/jobs has {len(jobs)} jobs in __NEXT_DATA__")
                        return
                    log.info(f"    /jobs pageProps keys: {list(props2.keys())[:10]}")
                except:
                    pass

        test_result("YCombinator", False, 0, notes="Likely requires JS rendering or login. Consider keeping blacklisted.")

    except Exception as e:
        test_result("YCombinator", False, 0, notes=f"Error: {str(e)[:150]}")


# ═══════════════════════════════════════════════════════════════
# 7. JAZZHR — HTML scrape
# ═══════════════════════════════════════════════════════════════
def test_jazzhr():
    log.info("\n=== 7. JAZZHR ===")

    # Try known JazzHR company pages
    companies = ["cmsprep", "softwareone"]

    for company in companies:
        url = f"https://app.jazz.co/{company}"
        log.info(f"  Trying {company}...")

        try:
            r = requests.get(url, timeout=15, headers=HEADERS, allow_redirects=True)
            log.info(f"    Status: {r.status_code}, length: {len(r.text)}")

            if r.status_code != 200 or len(r.text) < 500:
                continue

            # Check for JSON-LD
            ld_matches = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', r.text, re.DOTALL)
            for ld_text in ld_matches:
                try:
                    ld = json.loads(ld_text)
                    if isinstance(ld, dict) and ld.get("@type") == "JobPosting":
                        test_result("JazzHR", True, len(ld_matches),
                                   ld.get("title", ""), "", f"JSON-LD works ({company})")
                        return
                    elif isinstance(ld, list):
                        postings = [x for x in ld if isinstance(x, dict) and x.get("@type") == "JobPosting"]
                        if postings:
                            test_result("JazzHR", True, len(postings),
                                       postings[0].get("title", ""), "", f"JSON-LD array ({company})")
                            return
                except:
                    pass

            # Check for job links
            soup = BeautifulSoup(r.text, "html.parser")
            job_links = soup.select("a[href*='/apply/'], a[href*='/jobs/'], [class*='job-title'], [class*='resumator']")
            if job_links:
                title = job_links[0].get_text(strip=True)
                test_result("JazzHR", True, len(job_links), title, "",
                           f"HTML scrape works ({company})")
                return

            # Check for any job content
            job_markers = re.findall(r'job-title|job-listing|resumator-job|jazzhr-job', r.text, re.I)
            log.info(f"    Job markers: {len(job_markers)}")

            # Check if it's a JS-rendered SPA
            is_spa = any(m in r.text.lower() for m in ['react-root', 'ng-app', '__next', 'vue-app'])
            log.info(f"    SPA: {is_spa}")

        except Exception as e:
            log.info(f"    Error: {str(e)[:120]}")

    test_result("JazzHR", False, 0, notes="Could not find working JazzHR career page to test")


# ═══════════════════════════════════════════════════════════════
# 8. HRMDIRECT — Simple HTML scrape
# ═══════════════════════════════════════════════════════════════
def test_hrmdirect():
    log.info("\n=== 8. HRMDIRECT ===")

    # Real HRMDirect companies from search results
    companies = ["gwelec", "inviso", "ogind"]

    for company in companies:
        url = f"https://{company}.hrmdirect.com/employment/job-openings.php"
        log.info(f"  Trying {company}...")

        try:
            r = requests.get(url, timeout=15, headers=HEADERS, allow_redirects=True)
            log.info(f"    Status: {r.status_code}, length: {len(r.text)}")

            if r.status_code != 200 or len(r.text) < 500:
                continue

            # Parse with BeautifulSoup
            soup = BeautifulSoup(r.text, "html.parser")

            # Method 1: reqitem/posTitle table structure
            pos_titles = soup.select(".posTitle, [class*='posTitle']")
            req_items = soup.select(".reqitem, [class*='reqitem']")

            if pos_titles:
                title = pos_titles[0].get_text(strip=True)
                cities = soup.select(".cities, [class*='cities']")
                location = cities[0].get_text(strip=True) if cities else ""
                test_result("HRMDirect", True, len(pos_titles), title, location,
                           f"HTML table scrape works ({company})")
                return

            # Method 2: job links
            job_links = soup.select("a[href*='job-opening.php'], a[href*='job_id=']")
            if job_links:
                test_result("HRMDirect", True, len(job_links),
                           job_links[0].get_text(strip=True), "",
                           f"Job links found ({company})")
                return

            # Method 3: RSS feed
            rss_link = soup.select("a[href*='.rss'], link[type*='rss']")
            if rss_link:
                test_result("HRMDirect", True, 0, notes=f"RSS feed available ({company})")
                return

            # Log what we found
            log.info(f"    Page title: {soup.title.get_text(strip=True) if soup.title else 'N/A'}")
            all_links = soup.select("a[href]")
            log.info(f"    Total links: {len(all_links)}")

        except Exception as e:
            log.info(f"    Error: {str(e)[:120]}")

    test_result("HRMDirect", False, 0, notes="No working HRMDirect company found")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    log.info("=" * 70)
    log.info("SCRAPABILITY TEST v2 — 8 Previously-Blacklisted ATSs")
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
        time.sleep(0.5)

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
        log.info(f"Failed: {', '.join(fail)}")
    if ok:
        log.info(f"READY TO IMPLEMENT: {', '.join(ok)}")


if __name__ == "__main__":
    main()
