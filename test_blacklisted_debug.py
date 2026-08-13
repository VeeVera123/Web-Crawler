"""
Debug Test — Dump HTML structure for failing ATSs
===================================================
Run: python test_blacklisted_debug.py
"""

import requests
import re
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


# ═══════════════════════════════════════════════════════════════
# 1. TALEO — Debug REST API
# ═══════════════════════════════════════════════════════════════
def debug_taleo():
    log.info("\n" + "="*70)
    log.info("TALEO DEBUG")
    log.info("="*70)

    company = "hdr"
    section = "ex"
    base = f"https://{company}.taleo.net"
    careers_url = f"{base}/careersection/{section}/jobsearch.ftl?lang=en"

    session = requests.Session()
    session.headers.update(HEADERS)
    r = session.get(careers_url, timeout=15)
    html = r.text

    log.info(f"Career page: {len(html)} chars")
    log.info(f"Cookies: {dict(session.cookies)}")

    # Find ALL potential tokens/IDs
    log.info("\n--- Token/ID search ---")
    patterns = {
        "ftlcompanyid": r'ftlcompanyid\s*=\s*["\']?(\w+)',
        "portal": r'portal\s*=\s*["\']?(\w+)',
        "portalId": r'portalId\s*[:=]\s*["\']?(\w+)',
        "csrfToken": r'csrfToken\s*[:=]\s*["\']([^"\']+)',
        "CSRF": r'name=["\']csrf["\'][^>]*value=["\']([^"\']+)',
        "tngportalid": r'tngportalid\s*[:=]\s*["\']?(\w+)',
        "companyNo": r'companyNo\s*[:=]\s*["\']?(\w+)',
        "siteNo": r'siteNo\s*[:=]\s*["\']?(\w+)',
        "orgId": r'orgId\s*[:=]\s*["\']?(\w+)',
        "careerSiteNum": r'careerSiteNum\s*[:=]\s*["\']?(\w+)',
    }
    found = {}
    for name, pat in patterns.items():
        m = re.search(pat, html, re.I)
        if m:
            found[name] = m.group(1)
            log.info(f"  {name} = {m.group(1)}")

    # Check all cookies
    log.info(f"\nSession cookies:")
    for c in session.cookies:
        log.info(f"  {c.name} = {c.value[:80]}")

    # Find hidden inputs
    log.info("\n--- Hidden inputs ---")
    for m in re.finditer(r'<input[^>]*type=["\']hidden["\'][^>]*>', html, re.I):
        tag = m.group(0)
        name_m = re.search(r'name=["\']([^"\']+)', tag)
        value_m = re.search(r'value=["\']([^"\']+)', tag)
        if name_m:
            log.info(f"  {name_m.group(1)} = {value_m.group(1)[:80] if value_m else '(empty)'}")

    # Try REST API with different approaches
    log.info("\n--- REST API attempts ---")

    # Attempt 1: with cookies only (no portal param)
    api_url = f"{base}/careersection/rest/jobboard/searchjobs?lang=en"
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
    log.info(f"  Attempt 1 (no portal): status={r2.status_code}")
    if r2.status_code != 200:
        log.info(f"    Response: {r2.text[:300]}")

    # Attempt 2: with section in URL
    api_url2 = f"{base}/careersection/{section}/rest/jobboard/searchjobs?lang=en"
    r3 = session.post(api_url2, json=payload, timeout=15,
                     headers={"Content-Type": "application/json"})
    log.info(f"  Attempt 2 (with section): status={r3.status_code}")
    if r3.status_code == 200:
        log.info(f"    Content-Type: {r3.headers.get('content-type', '')}")
        log.info(f"    Response length: {len(r3.text)}")
        log.info(f"    First 500: {r3.text[:500]}")

    # Attempt 3: AJAX with form data instead of JSON
    ajax_url = f"{base}/careersection/{section}/jobsearch.ajax"
    form_data = {
        "requisitionListInterface.sortingSelection.sortBySelectionParam": "3",
        "requisitionListInterface.sortingSelection.ascendingSortingOrder": "false",
        "requisitionListInterface.reqTitleRecordsPerPage": "25",
    }
    r4 = session.post(ajax_url, data=form_data, timeout=15)
    log.info(f"  Attempt 3 (AJAX form): status={r4.status_code}, length={len(r4.text)}")
    if r4.status_code == 200 and 'jobdetail' in r4.text.lower():
        jobs = re.findall(r'jobdetail\.ftl\?job=(\d+)', r4.text)
        log.info(f"    Found {len(jobs)} job links!")
        log.info(f"    First 500: {r4.text[:500]}")


# ═══════════════════════════════════════════════════════════════
# 2. BRASSRING — Debug AJAX
# ═══════════════════════════════════════════════════════════════
def debug_brassring():
    log.info("\n" + "="*70)
    log.info("BRASSRING DEBUG")
    log.info("="*70)

    host = "sjobs.brassring.com"
    partner_id = "25212"
    site_id = "5164"

    session = requests.Session()
    session.headers.update(HEADERS)

    home_url = f"https://{host}/TGnewUI/Search/Home/Home?partnerid={partner_id}&siteid={site_id}"
    r1 = session.get(home_url, timeout=15)
    log.info(f"Home page: {len(r1.text)} chars")

    # Find the actual search form and API endpoint
    log.info("\n--- Looking for API endpoints in HTML ---")
    api_urls = re.findall(r'["\']([^"\']*(?:Ajax|api|search|jobs)[^"\']*)["\']', r1.text, re.I)
    for url in api_urls[:15]:
        log.info(f"  Potential endpoint: {url}")

    # Find any script-embedded config
    log.info("\n--- Script config ---")
    for m in re.finditer(r'var\s+(\w+)\s*=\s*(\{[^}]+\}|"[^"]*"|\d+)', r1.text):
        if any(kw in m.group(1).lower() for kw in ['config', 'setting', 'search', 'api', 'url', 'partner', 'site']):
            log.info(f"  {m.group(1)} = {m.group(2)[:100]}")

    # Find verification token
    token = re.search(r'__RequestVerificationToken["\'][^>]*value=["\']([^"\']+)', r1.text)
    if token:
        log.info(f"\n  Verification token found: {token.group(1)[:40]}...")

    # Look for the actual job results in the initial page load
    log.info("\n--- Job content in initial page ---")
    job_elements = re.findall(r'class=["\']([^"\']*(?:job|position|title|listing)[^"\']*)["\']', r1.text, re.I)
    for cls in set(job_elements)[:20]:
        log.info(f"  Class: {cls}")

    # Try AJAX with the 500 error — what does it actually say?
    ajax_url = f"https://{host}/TgNewUI/Search/Ajax/MatchedJobs"
    payload = {
        "partnerId": partner_id,
        "siteId": site_id,
        "keyword": "",
        "location": "",
    }
    r2 = session.post(ajax_url, json=payload, timeout=15,
                     headers={"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"})
    log.info(f"\nAJAX response: status={r2.status_code}")
    log.info(f"  Content-Type: {r2.headers.get('content-type', '')}")
    log.info(f"  Body: {r2.text[:500]}")

    # Check if jobs are already in the initial HTML
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(r1.text, "html.parser")

    # Find job title elements
    for selector in ["a[href*='JobDetails']", "[class*='jobTitle']", "[class*='JobTitle']",
                     "tr[class*='job']", ".SearchResultsGridRow", "[data-job]"]:
        found = soup.select(selector)
        if found:
            log.info(f"\n  Selector '{selector}' matched {len(found)} elements")
            for el in found[:3]:
                log.info(f"    Text: {el.get_text(strip=True)[:80]}")


# ═══════════════════════════════════════════════════════════════
# 3. ZOHO RECRUIT — Debug 1.7MB page
# ═══════════════════════════════════════════════════════════════
def debug_zoho():
    log.info("\n" + "="*70)
    log.info("ZOHO RECRUIT DEBUG")
    log.info("="*70)

    url = "https://careers.zohocorp.com/jobs/Careers"
    r = requests.get(url, timeout=15, headers=HEADERS)
    html = r.text
    log.info(f"Page: {len(html)} chars")

    # Check what kind of content this 1.7MB is
    log.info(f"\n--- Content analysis ---")
    log.info(f"  <script> tags: {len(re.findall(r'<script', html, re.I))}")
    log.info(f"  <style> tags: {len(re.findall(r'<style', html, re.I))}")

    # Find all CSS classes with 'job' in them
    job_classes = set(re.findall(r'class=["\']([^"\']*job[^"\']*)["\']', html, re.I))
    log.info(f"\n--- Classes with 'job' ({len(job_classes)}) ---")
    for cls in sorted(job_classes)[:20]:
        log.info(f"  {cls}")

    # Find all IDs with 'job' in them
    job_ids = set(re.findall(r'id=["\']([^"\']*job[^"\']*)["\']', html, re.I))
    log.info(f"\n--- IDs with 'job' ({len(job_ids)}) ---")
    for jid in sorted(job_ids)[:15]:
        log.info(f"  {jid}")

    # Look for JSON-LD more carefully
    log.info(f"\n--- All <script type=application/ld+json> ---")
    for m in re.finditer(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL | re.I):
        content = m.group(1).strip()
        log.info(f"  JSON-LD block: {len(content)} chars")
        log.info(f"  First 200: {content[:200]}")

    # Look for hidden inputs
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    hidden_inputs = soup.select("input[type='hidden']")
    log.info(f"\n--- Hidden inputs ({len(hidden_inputs)}) ---")
    for inp in hidden_inputs[:10]:
        name = inp.get("name", inp.get("id", "unnamed"))
        val = inp.get("value", "")[:100]
        log.info(f"  {name} = {val}")

    # Look for any element with job title content
    log.info(f"\n--- Elements with job-like text patterns ---")
    # Find links that look like job postings
    links = soup.select("a[href]")
    job_links = [a for a in links if '/jobs/' in (a.get('href', '') or '') or 'jobid' in (a.get('href', '').lower() or '')]
    log.info(f"  Links with /jobs/ or jobid: {len(job_links)}")
    for a in job_links[:5]:
        log.info(f"    {a.get('href', '')[:80]} — {a.get_text(strip=True)[:60]}")


# ═══════════════════════════════════════════════════════════════
# 4. HRMDIRECT — Debug HTML structure
# ═══════════════════════════════════════════════════════════════
def debug_hrmdirect():
    log.info("\n" + "="*70)
    log.info("HRMDIRECT DEBUG")
    log.info("="*70)

    url = "https://ogind.hrmdirect.com/employment/job-openings.php"
    r = requests.get(url, timeout=15, headers=HEADERS)
    html = r.text
    log.info(f"Page: {len(html)} chars")

    # Dump all classes
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    log.info(f"\n--- All unique CSS classes ---")
    all_classes = set()
    for el in soup.find_all(True, class_=True):
        for cls in el.get("class", []):
            all_classes.add(cls)
    for cls in sorted(all_classes):
        log.info(f"  .{cls}")

    log.info(f"\n--- All links ---")
    for a in soup.select("a[href]"):
        log.info(f"  {a.get('href', '')[:80]} — {a.get_text(strip=True)[:60]}")

    log.info(f"\n--- All tables ---")
    tables = soup.select("table")
    for i, table in enumerate(tables):
        rows = table.select("tr")
        log.info(f"  Table {i}: {len(rows)} rows")
        for row in rows[:3]:
            cells = row.select("td, th")
            cell_text = [c.get_text(strip=True)[:30] for c in cells]
            log.info(f"    {cell_text}")

    # Check if content is in an iframe
    iframes = soup.select("iframe")
    log.info(f"\n--- Iframes: {len(iframes)} ---")
    for iframe in iframes:
        log.info(f"  src={iframe.get('src', '')[:100]}")

    # Dump first 2000 chars of body
    body = soup.select_one("body")
    if body:
        body_text = body.get_text(separator="\n", strip=True)
        log.info(f"\n--- Body text (first 1500 chars) ---")
        log.info(body_text[:1500])


def main():
    debug_taleo()
    debug_brassring()
    debug_zoho()
    debug_hrmdirect()


if __name__ == "__main__":
    main()
