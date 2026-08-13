#!/usr/bin/env python3
"""Test all 5 newly enabled ATS scrapers against real companies.
Run on GitHub Actions where there are no proxy restrictions."""

import sys
import requests
import re
import json

PASS = 0
FAIL = 0

def test(name, func):
    global PASS, FAIL
    try:
        result = func()
        if result:
            PASS += 1
            print(f"  ✅ {name}: {result}")
        else:
            FAIL += 1
            print(f"  ❌ {name}: returned nothing")
    except Exception as e:
        FAIL += 1
        print(f"  ❌ {name}: {e}")


# ── 1. HRMDirect ──────────────────────────────────────────

def test_hrmdirect():
    """Test that ?search=true returns job links."""
    url = "https://novabio.hrmdirect.com/employment/openings.php?search=true"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"

    # Count job-opening links
    links = re.findall(r'job-opening\.php\?req=\d+', r.text)
    unique = len(set(links))
    assert unique > 0, "No job-opening links found"

    # Verify table has city/state/country columns
    has_table = '<td' in r.text.lower()
    return f"{unique} jobs, has_table={has_table}"


def test_hrmdirect_without_search():
    """Confirm that WITHOUT ?search=true, some pages hide jobs."""
    url = "https://novabio.hrmdirect.com/employment/openings.php"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    links_no_search = len(set(re.findall(r'job-opening\.php\?req=\d+', r.text)))

    url2 = "https://novabio.hrmdirect.com/employment/openings.php?search=true"
    r2 = requests.get(url2, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    links_with_search = len(set(re.findall(r'job-opening\.php\?req=\d+', r2.text)))

    return f"without_search={links_no_search}, with_search={links_with_search}"


# ── 2. Oracle Cloud HCM ───────────────────────────────────

def test_oracle_cloud_hcm():
    """Test Oracle API with full host prefix (eeho.fa.us2)."""
    import uuid
    base = "https://eeho.fa.us2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "ora-irc-cx-userid": str(uuid.uuid4()),
        "ora-irc-language": "en",
    }
    params = {
        "onlyData": "true",
        "expand": "requisitionList.workLocation",
        "finder": "findReqs;siteNumber=CX_1,limit=5,offset=0",
    }
    r = requests.get(base, params=params, headers=headers, timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"
    data = r.json()
    items = data.get("items", [])
    assert items, "No items returned"
    req_list = items[0].get("requisitionList", [])
    assert req_list, "No requisitions in first item"

    sample = req_list[0]
    title = sample.get("Title", "")
    location = sample.get("PrimaryLocation", "")
    return f"{len(req_list)} jobs, sample: '{title}' @ '{location}'"


def test_oracle_region_discovery():
    """Test that legacy tenant-only slugs can discover region via redirect."""
    tenant = "eeho"

    # Method 1: Career page redirect discovery
    found_domain = None
    for try_site in ("CX_1", "CX", "CX_2"):
        try:
            probe_url = f"https://{tenant}.oraclecloud.com/hcmUI/CandidateExperience/en/sites/{try_site}/requisitions"
            r = requests.get(probe_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15, allow_redirects=True)
            final_host = r.url.split("/")[2] if r.url else ""
            if ".fa." in final_host and "oraclecloud.com" in final_host:
                found_domain = final_host.replace(".oraclecloud.com", "")
                break
        except Exception:
            continue

    # Method 2: Brute-force API
    if not found_domain:
        import uuid
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "ora-irc-cx-userid": str(uuid.uuid4()),
            "ora-irc-language": "en",
        }
        for region in ("fa.us2", "fa.us6", "fa.us1", "fa.em2", "fa.em3", "fa.em4",
                       "fa.ap1", "fa.ap2", "fa.ca1", "fa.sa1", "fa.me1"):
            test_url = f"https://{tenant}.{region}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
            try:
                r = requests.get(test_url,
                    params={"onlyData": "true", "finder": "findReqs;siteNumber=CX_1,limit=1,offset=0"},
                    headers=headers, timeout=8)
                if r.status_code == 200:
                    data = r.json()
                    items = data.get("items", [])
                    if items and items[0].get("requisitionList"):
                        found_domain = f"{tenant}.{region}"
                        break
            except Exception:
                continue

    assert found_domain, f"Could not discover domain for {tenant}"
    return f"tenant={tenant}, discovered domain={found_domain}"


# ── 3. Taleo ──────────────────────────────────────────────

def test_taleo():
    """Test Taleo REST API with tz header."""
    company, section = "hdr", "101430233"
    api_url = f"https://{company}.taleo.net/careersection/rest/jobboard/searchjobs"

    # Auto-discover portal
    career_url = f"https://{company}.taleo.net/careersection/{section}/jobsearch.ftl"
    r = requests.get(career_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    portal_match = re.search(r'portal\s*=\s*["\']?(\d+)', r.text, re.I)
    assert portal_match, "Could not find portal ID"
    portal_id = portal_match.group(1)

    payload = {
        "multilineEnabled": False,
        "sortingSelection": {"sortBySelectionParam": "1", "ascendingSortingOrder": "false"},
        "fieldData": {"fields": {"KEYWORD": "", "LOCATION": ""}, "valid": True},
        "filterSelectionParam": {"searchFilterSelections": [
            {"id": "POSTING_DATE", "selectedValues": []},
            {"id": "LOCATION", "selectedValues": []},
            {"id": "JOB_FIELD", "selectedValues": []},
            {"id": "JOB_TYPE", "selectedValues": []},
            {"id": "JOB_SCHEDULE", "selectedValues": []},
        ]},
        "advancedSearchFiltersSelectionParam": {"searchFilterSelections": [
            {"id": "LOCATION", "selectedValues": []},
            {"id": "JOB_FIELD", "selectedValues": []},
            {"id": "JOB_NUMBER", "selectedValues": []},
            {"id": "ORGANIZATION", "selectedValues": []},
        ]},
        "pageNo": 1,
    }

    resp = requests.post(api_url,
        params={"lang": "en", "portal": portal_id},
        headers={"Content-Type": "application/json", "tz": "GMT-05:00", "User-Agent": "Mozilla/5.0"},
        data=json.dumps(payload), timeout=15)
    assert resp.status_code == 200, f"HTTP {resp.status_code}"
    data = resp.json()
    reqs = data.get("requisitionList", [])
    assert reqs, "No requisitions returned"

    total = data.get("pagingData", {}).get("totalCount", len(reqs))
    sample = reqs[0]
    title = sample.get("column", [""])[0] if sample.get("column") else ""
    return f"{total} total jobs (page 1: {len(reqs)}), sample: '{title}'"


# ── 4. Zoho Recruit ───────────────────────────────────────

def test_zoho():
    """Test Zoho hidden input parsing with both attribute orders."""
    url = "https://rectrain.zohorecruit.com/jobs/Careers"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"

    # Try id="jobs" (both attribute orders)
    jobs_input = None
    for attr in ('id', 'name'):
        if jobs_input:
            break
        jobs_input = re.search(
            rf'<input[^>]*{attr}=["\']jobs["\'][^>]*value=["\']([^"\']+)["\']',
            r.text, re.I
        )
        if not jobs_input:
            jobs_input = re.search(
                rf'<input[^>]*value=["\']([^"\']+)["\'][^>]*{attr}=["\']jobs["\']',
                r.text, re.I
            )

    assert jobs_input, "No input#jobs or input[name=jobs] found"
    raw = jobs_input.group(1)
    raw = raw.replace("&quot;", '"').replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&#39;", "'")
    job_data = json.loads(raw)
    assert isinstance(job_data, list), f"Expected list, got {type(job_data)}"
    assert len(job_data) > 0, "Empty job list"

    sample = job_data[0]
    title = sample.get("Posting_Title") or sample.get("Job_Opening_Name") or ""
    return f"{len(job_data)} jobs, sample: '{title}', keys: {list(sample.keys())[:5]}"


# ── 5. Paylocity ──────────────────────────────────────────

def test_paylocity():
    """Test Paylocity window.pageData extraction (UUID slug)."""
    # UUID slug that we know works
    url = "https://recruiting.paylocity.com/recruiting/jobs/All/c4ba1261-9dee-44e1-b5fd-5f8cb4ee222e/Cinterra"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"
    assert "JobNotFound" not in r.url, "Redirected to JobNotFound"

    pd_match = re.search(r'window\.pageData\s*=\s*(\{.*?\});\s*</script>', r.text, re.DOTALL)
    assert pd_match, "No window.pageData found"

    page_data = json.loads(pd_match.group(1))
    jobs = page_data.get("Jobs", page_data.get("jobs", []))
    assert isinstance(jobs, list), f"Expected list, got {type(jobs)}"
    assert len(jobs) > 0, "Empty job list"

    company = page_data.get("companyName") or page_data.get("ModuleTitle") or "unknown"
    sample = jobs[0]
    title = sample.get("JobTitle", sample.get("Title", ""))
    dept = sample.get("HiringDepartment", sample.get("Department", ""))
    return f"{len(jobs)} jobs, company='{company}', sample: '{title}', dept='{dept}'"


def test_paylocity_numeric_id_fails():
    """Confirm numeric IDs redirect to JobNotFound."""
    url = "https://recruiting.paylocity.com/recruiting/jobs/All/3828033/Providence-of-Maryland-Inc"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15, allow_redirects=True)
    assert "JobNotFound" in r.url or r.status_code == 404, "Numeric ID should fail"
    return "Correctly returns 404/redirect for numeric IDs"


# ── Run all tests ─────────────────────────────────────────

if __name__ == "__main__":
    print("\n=== Testing HRMDirect ===")
    test("HRMDirect with ?search=true", test_hrmdirect)
    test("HRMDirect search param comparison", test_hrmdirect_without_search)

    print("\n=== Testing Oracle Cloud HCM ===")
    test("Oracle API with full host prefix", test_oracle_cloud_hcm)
    test("Oracle region auto-discovery", test_oracle_region_discovery)

    print("\n=== Testing Taleo ===")
    test("Taleo REST API", test_taleo)

    print("\n=== Testing Zoho Recruit ===")
    test("Zoho hidden input parsing", test_zoho)

    print("\n=== Testing Paylocity ===")
    test("Paylocity UUID slug", test_paylocity)
    test("Paylocity numeric ID rejection", test_paylocity_numeric_id_fails)

    print(f"\n{'='*50}")
    print(f"Results: {PASS} passed, {FAIL} failed out of {PASS+FAIL}")

    if FAIL > 0:
        sys.exit(1)
    print("All tests passed!")
