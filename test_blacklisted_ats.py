"""
Scrapability Test v3 -- 5 Target ATSs
======================================
Tests: Oracle Cloud HCM, Paylocity, Zoho Recruit, Taleo, HRMDirect
Run: python test_blacklisted_ats.py
"""

import requests
import re
import json
import time
import uuid
import logging
import html as html_mod
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
RESULTS = {}


def test_result(name, success, jobs_found, sample_title="", sample_location="", notes=""):
    RESULTS[name] = {"success": success, "jobs": jobs_found, "sample": sample_title, "location": sample_location, "notes": notes}
    status = "OK" if success else "FAIL"
    log.info(f"  [{status}] {name}: {jobs_found} jobs | sample: \"{sample_title[:60]}\" | loc: \"{sample_location[:60]}\" | {notes}")


# =====================================================================
# 1. ORACLE CLOUD HCM -- Public REST API (confirmed working)
# =====================================================================
def test_oracle_cloud():
    log.info("\n=== 1. ORACLE CLOUD HCM ===")
    domain = "eeho.fa.us2.oraclecloud.com"
    site_number = "CX_1"

    try:
        finder = f"findReqs;siteNumber={site_number},limit=5,offset=0"
        url = (
            f"https://{domain}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
            f"?onlyData=true&expand=requisitionList.workLocation&finder={finder}"
        )
        headers = {
            **HEADERS,
            "ora-irc-cx-userid": str(uuid.uuid4()),
            "ora-irc-language": "en",
            "content-type": "application/vnd.oracle.adf.resourceitem+json;charset=utf-8",
        }
        r = requests.get(url, timeout=20, headers=headers)
        log.info(f"  API status: {r.status_code}")

        if r.status_code == 200:
            data = r.json()
            items = data.get("items", [])
            if items:
                search = items[0]
                jobs = search.get("requisitionList", [])
                total = search.get("TotalJobsCount", len(jobs))
                if jobs:
                    j = jobs[0]
                    test_result("Oracle Cloud HCM", True, total,
                               j.get("Title", ""), j.get("PrimaryLocation", ""),
                               f"REST API works, job ID: {j.get('Id', '')}")
                    return
        test_result("Oracle Cloud HCM", False, 0, notes=f"HTTP {r.status_code}")
    except Exception as e:
        test_result("Oracle Cloud HCM", False, 0, notes=f"Error: {str(e)[:150]}")


# =====================================================================
# 2. PAYLOCITY -- Embedded window.pageData (confirmed working)
# =====================================================================
def test_paylocity():
    log.info("\n=== 2. PAYLOCITY ===")
    url = "https://recruiting.paylocity.com/recruiting/jobs/All/9b6dbe18-295a-4b4e-bcaf-f7e9bbb28161/The-Guidance-Center"

    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        log.info(f"  Status: {r.status_code}, length: {len(r.text)}")

        if r.status_code == 200:
            pd_match = re.search(r'window\.pageData\s*=\s*(\{.*?\});\s*</script>', r.text, re.DOTALL)
            if pd_match:
                page_data = json.loads(pd_match.group(1))
                jobs = page_data.get("jobs", page_data.get("Jobs", []))
                if isinstance(jobs, list) and jobs:
                    j = jobs[0]
                    test_result("Paylocity", True, len(jobs),
                               j.get("JobTitle", j.get("Title", "")),
                               j.get("LocationName", j.get("Location", "")),
                               "window.pageData works")
                    return
        test_result("Paylocity", False, 0, notes="No pageData found")
    except Exception as e:
        test_result("Paylocity", False, 0, notes=f"Error: {str(e)[:150]}")


# =====================================================================
# 3. ZOHO RECRUIT -- Embedded input[name=jobs] JSON
# =====================================================================
def test_zoho_recruit():
    log.info("\n=== 3. ZOHO RECRUIT ===")
    url = "https://careers.zohocorp.com/jobs/Careers"

    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        log.info(f"  Status: {r.status_code}, length: {len(r.text)}")

        if r.status_code != 200:
            test_result("Zoho Recruit", False, 0, notes=f"HTTP {r.status_code}")
            return

        # Debug showed: hidden input name="jobs" has JSON array of job objects
        # Pattern: <input ... name="jobs" ... value="[{...}]" />
        # The value is HTML-encoded JSON
        soup = BeautifulSoup(r.text, "html.parser")

        # Try input with name="jobs"
        jobs_input = soup.select_one("input[name='jobs']") or soup.select_one("input#jobs")
        if jobs_input:
            raw_val = jobs_input.get("value", "")
            if raw_val:
                decoded = html_mod.unescape(raw_val)
                try:
                    jobs = json.loads(decoded)
                    if isinstance(jobs, list) and jobs:
                        j = jobs[0]
                        # Log available keys
                        log.info(f"  Job keys: {list(j.keys())[:15]}")
                        title = (j.get("Posting_Title") or j.get("Job_Title") or
                                j.get("title") or j.get("Title") or "")
                        location = (j.get("City") or j.get("Location") or
                                   j.get("location") or "")
                        desc_len = len(j.get("Job_Description", j.get("description", "")))
                        test_result("Zoho Recruit", True, len(jobs), title, location,
                                   f"input[name=jobs] JSON works, desc={desc_len} chars")
                        return
                except json.JSONDecodeError as e:
                    log.info(f"  JSON parse error: {str(e)[:100]}")
                    log.info(f"  Raw value first 200: {raw_val[:200]}")

        # Fallback: regex for the input value
        jobs_match = re.search(
            r'<input[^>]*name=["\']jobs["\'][^>]*value=["\'](.+?)["\']',
            r.text, re.DOTALL
        )
        if not jobs_match:
            # Try reversed attribute order
            jobs_match = re.search(
                r'<input[^>]*value=["\'](.+?)["\'][^>]*name=["\']jobs["\']',
                r.text, re.DOTALL
            )

        if jobs_match:
            decoded = html_mod.unescape(jobs_match.group(1))
            try:
                jobs = json.loads(decoded)
                if isinstance(jobs, list) and jobs:
                    j = jobs[0]
                    log.info(f"  Job keys (regex): {list(j.keys())[:15]}")
                    title = j.get("Posting_Title", j.get("title", ""))
                    test_result("Zoho Recruit", True, len(jobs), title, "",
                               "input[name=jobs] via regex")
                    return
            except:
                pass

        test_result("Zoho Recruit", False, 0, notes=f"Could not parse jobs from {len(r.text)} char page")
    except Exception as e:
        test_result("Zoho Recruit", False, 0, notes=f"Error: {str(e)[:150]}")


# =====================================================================
# 4. TALEO -- REST API with portal + CSRF token
# =====================================================================
def test_taleo():
    log.info("\n=== 4. TALEO ===")

    companies = [
        ("hdr", "ex"),
        ("capps", "479"),
    ]

    for company, section in companies:
        base = f"https://{company}.taleo.net"
        careers_url = f"{base}/careersection/{section}/jobsearch.ftl?lang=en"
        log.info(f"  Trying {company}.taleo.net section={section}...")

        try:
            session = requests.Session()
            session.headers.update(HEADERS)
            r1 = session.get(careers_url, timeout=15, allow_redirects=True)
            log.info(f"    Career page: status={r1.status_code}, length={len(r1.text)}")

            if r1.status_code != 200 or len(r1.text) < 1000:
                continue

            # Extract portal ID
            portal_id = None
            for pat in [
                r'portal\s*=\s*["\']?(\d+)',
                r'portalId\s*[:=]\s*["\']?(\d+)',
                r'ftlcompanyid\s*=\s*["\']?(\d+)',
            ]:
                m = re.search(pat, r1.text, re.I)
                if m:
                    portal_id = m.group(1)
                    break

            # Extract CSRF token
            csrf = None
            m = re.search(r'csrfToken\s*[:=]\s*["\']([^"\']+)', r1.text, re.I)
            if m:
                csrf = m.group(1)

            log.info(f"    Portal: {portal_id}, CSRF: {csrf[:20] if csrf else 'None'}...")

            # REST API call — must include X-Requested-With and Referer
            api_url = f"{base}/careersection/rest/jobboard/searchjobs?lang=en"
            if portal_id:
                api_url += f"&portal={portal_id}"

            payload = {
                "multilineEnabled": False,
                "sortingSelection": {
                    "sortBySelectionParam": "1",
                    "ascendingSortingOrder": "false"
                },
                "fieldData": {
                    "fields": {
                        "KEYWORD": "",
                        "LOCATION": "",
                        "CATEGORY": ""
                    },
                    "valid": True
                },
                "pageNo": 1,
            }

            post_headers = {
                "Content-Type": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Referer": careers_url,
            }
            if csrf:
                post_headers["X-CSRF-Token"] = csrf

            r2 = session.post(api_url, json=payload, timeout=15, headers=post_headers)
            log.info(f"    REST API: status={r2.status_code}, length={len(r2.text)}")
            log.info(f"    Content-Type: {r2.headers.get('content-type', '')}")

            if r2.status_code == 200 and len(r2.text) > 50:
                # Check if response is JSON
                ct = r2.headers.get('content-type', '')
                if 'json' in ct or r2.text.strip().startswith('{'):
                    try:
                        data = r2.json()
                        jobs = data.get("requisitionList", [])
                        total = data.get("pagingData", {}).get("totalCount", len(jobs))
                        log.info(f"    JSON keys: {list(data.keys())}")
                        log.info(f"    Jobs: {len(jobs)}, Total: {total}")

                        if jobs:
                            j = jobs[0]
                            cols = j.get("column", [])
                            title = cols[0] if len(cols) > 0 else ""
                            location = cols[1] if len(cols) > 1 else ""
                            contest_no = j.get("contestNo", "")
                            log.info(f"    Job keys: {list(j.keys())}")
                            log.info(f"    First job: title={title}, loc={location}, id={contest_no}")
                            test_result("Taleo", True, total, title, location,
                                       f"REST API works ({company})")
                            return
                        elif total > 0:
                            test_result("Taleo", True, total, notes=f"API returned total={total} but empty list ({company})")
                            return
                    except json.JSONDecodeError:
                        log.info(f"    Not valid JSON. First 300: {r2.text[:300]}")
                else:
                    log.info(f"    Response is HTML/other. First 300: {r2.text[:300]}")

        except Exception as e:
            log.info(f"    Error: {str(e)[:120]}")

    test_result("Taleo", False, 0, notes="All test companies failed")


# =====================================================================
# 5. HRMDIRECT -- Search form POST + .reqResult parsing
# =====================================================================
def test_hrmdirect():
    log.info("\n=== 5. HRMDIRECT ===")

    companies = ["ogind", "gwelec", "inviso"]

    for company in companies:
        base_url = f"https://{company}.hrmdirect.com/employment"
        search_url = f"{base_url}/job-openings.php?search=true&"
        log.info(f"  Trying {company}...")

        try:
            session = requests.Session()
            session.headers.update(HEADERS)

            # Step 1: GET the page (with search=true to load all results)
            r1 = session.get(search_url, timeout=15, allow_redirects=True)
            log.info(f"    GET: status={r1.status_code}, length={len(r1.text)}")

            if r1.status_code != 200 or len(r1.text) < 1000:
                continue

            soup = BeautifulSoup(r1.text, "html.parser")

            # Method 1: Look for .reqResult elements (from debug output)
            req_results = soup.select(".reqResult")
            log.info(f"    .reqResult elements: {len(req_results)}")

            if req_results:
                for i, el in enumerate(req_results[:3]):
                    log.info(f"    reqResult {i}: {el.get_text(strip=True)[:80]}")

            # Method 2: Look for job links (job-opening.php?req_id=...)
            job_links = soup.select("a[href*='job-opening.php']")
            log.info(f"    Job links: {len(job_links)}")

            if job_links:
                for a in job_links[:3]:
                    log.info(f"    Link: {a.get('href', '')[:60]} -- {a.get_text(strip=True)[:60]}")

                title = job_links[0].get_text(strip=True)
                test_result("HRMDirect", True, len(job_links), title, "",
                           f"Job links found ({company})")
                return

            # Method 3: POST the search form to load results
            log.info("    Trying POST search...")
            form = soup.select_one("form")
            if form:
                action = form.get("action", "job-openings.php")
                if not action.startswith("http"):
                    action = f"{base_url}/{action}"

                # Build form data
                form_data = {}
                for inp in form.select("input[name]"):
                    form_data[inp["name"]] = inp.get("value", "")
                for sel in form.select("select[name]"):
                    form_data[sel["name"]] = ""  # empty = all

                log.info(f"    Form action: {action}")
                log.info(f"    Form fields: {list(form_data.keys())}")

                r2 = session.post(action, data=form_data, timeout=15)
                log.info(f"    POST: status={r2.status_code}, length={len(r2.text)}")

                if r2.status_code == 200:
                    soup2 = BeautifulSoup(r2.text, "html.parser")
                    job_links2 = soup2.select("a[href*='job-opening.php']")
                    req_results2 = soup2.select(".reqResult")

                    if job_links2:
                        title = job_links2[0].get_text(strip=True)
                        test_result("HRMDirect", True, len(job_links2), title, "",
                                   f"POST search works ({company})")
                        return
                    elif req_results2:
                        title = req_results2[0].get_text(strip=True)[:60]
                        test_result("HRMDirect", True, len(req_results2), title, "",
                                   f"POST + .reqResult works ({company})")
                        return

            # Method 4: just look for any links in body with job text
            all_links = soup.select("a[href]")
            for a in all_links:
                href = a.get("href", "")
                text = a.get_text(strip=True)
                if "job" in href.lower() and text and len(text) > 5:
                    log.info(f"    Potential job link: {href[:60]} -- {text[:60]}")

        except Exception as e:
            log.info(f"    Error: {str(e)[:120]}")

    test_result("HRMDirect", False, 0, notes="No working company found")


# =====================================================================
# MAIN
# =====================================================================
def main():
    log.info("=" * 70)
    log.info("SCRAPABILITY TEST v3 -- 5 Target ATSs")
    log.info("=" * 70)

    tests = [
        ("Oracle Cloud HCM", test_oracle_cloud),
        ("Paylocity", test_paylocity),
        ("Zoho Recruit", test_zoho_recruit),
        ("Taleo", test_taleo),
        ("HRMDirect", test_hrmdirect),
    ]

    for name, test_fn in tests:
        try:
            test_fn()
        except Exception as e:
            test_result(name, False, 0, notes=f"Unexpected: {str(e)[:150]}")
        time.sleep(0.5)

    # Summary
    log.info("\n" + "=" * 70)
    log.info("SUMMARY")
    log.info("=" * 70)
    ok, fail = [], []
    for name, result in RESULTS.items():
        if result["success"]:
            ok.append(name)
            log.info(f"  PASS {name}: {result['jobs']} jobs -- {result['notes']}")
        else:
            fail.append(name)
            log.info(f"  FAIL {name}: {result['notes']}")

    log.info(f"\nPassed: {len(ok)}/{len(RESULTS)}")
    if fail:
        log.info(f"Failed: {', '.join(fail)}")
    if ok:
        log.info(f"READY TO IMPLEMENT: {', '.join(ok)}")


if __name__ == "__main__":
    main()
