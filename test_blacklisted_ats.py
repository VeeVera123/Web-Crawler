Run python test_blacklisted_ats.py
13:54:41  INFO      ======================================================================
13:54:41  INFO      SCRAPABILITY TEST v2 — 8 Previously-Blacklisted ATSs
13:54:41  INFO      ======================================================================
13:54:41  INFO      
=== 1. TALEO ===
13:54:41  INFO        Trying capps.taleo.net section=479...
13:54:41  INFO          Career page: status=200, length=45925
13:54:41  INFO          API: status=500
13:54:41  INFO          AJAX fallback: status=500, length=1584
13:54:41  INFO        Trying hdr.taleo.net section=ex...
13:54:42  INFO          Career page: status=200, length=54268
13:54:42  INFO          API: status=500
13:54:42  INFO          AJAX fallback: status=500, length=1580
13:54:42  INFO        Trying jacobs.taleo.net section=ex...
13:54:50  INFO          Career page: status=200, length=2353
13:54:50  INFO          No portal ID found, trying without it...
13:54:58  INFO          API: status=200
13:54:58  INFO          JSON parse error: Expecting value: line 1 column 1 (char 0)
13:55:06  INFO          AJAX fallback: status=200, length=2353
13:55:06  INFO        [FAIL] Taleo: 0 jobs | sample: "" | loc: "" | All test companies failed
13:55:07  INFO      
=== 2. ORACLE CLOUD HCM ===
13:55:08  INFO        API status: 200
13:55:08  INFO        [OK] Oracle Cloud HCM: 2205 jobs | sample: "Federal Project Manager II - General Readiness" | loc: "United States" | REST API works, job ID: 330061
13:55:08  INFO      
=== 3. BRASSRING ===
13:55:08  INFO        Trying sjobs.brassring.com partner=25212 site=5164...
13:55:08  INFO          Home: status=200, length=987237
13:55:08  INFO          AJAX: status=500, length=316
13:55:08  INFO        [OK] BrassRing: 1 jobs | sample: "Close" | loc: "" | HTML scrape fallback (partner=25212)
13:55:09  INFO      
=== 4. PAYLOCITY ===
13:55:09  INFO        Trying The-Guidance-Center...
13:55:09  INFO          Status: 200, length: 37892
13:55:09  INFO        [OK] Paylocity: 21 jobs | sample: "Therapist - Infant and Early Childhood Mental Health" | loc: "Horizon Building" | window.pageData works
13:55:10  INFO      
=== 5. ZOHO RECRUIT ===
13:55:12  INFO        Status: 200, length: 1770109
13:55:12  INFO        [FAIL] Zoho Recruit: 0 jobs | sample: "" | loc: "" | No extractable job data found. Page has 1770109 chars
13:55:12  INFO      
=== 6. YCOMBINATOR ===
13:55:13  INFO        /companies: status=200, length=75042, ct=text/html; charset=utf-8
13:55:13  INFO          SPA page: True
13:55:13  INFO          Found script with job/company data: 16412 chars
13:55:13  INFO          First 200: window.RAILS_ENV = 'production';
window.AlgoliaOpts = {"app":"45BWZJ1SGC","key":"NzJmNzljYjRkY2VhYzBhMWU3MTRlMWY3NTRlMzBlNzVhMzUxN2UzZmJiMjdlZDc2ZDMxZTIyOTM5MjNmNWY2NmFuYWx5dGljc1RhZ3M9d2FhcyZyZXN0cml
13:55:13  INFO        /jobs: status=406, length=0
13:55:13  INFO        [FAIL] YCombinator: 0 jobs | sample: "" | loc: "" | Likely requires JS rendering or login. Consider keeping blacklisted.
13:55:14  INFO      
=== 7. JAZZHR ===
13:55:14  INFO        Trying cmsprep...
13:55:14  INFO          Status: 530, length: 8230
13:55:14  INFO        Trying softwareone...
13:55:14  INFO          Status: 530, length: 8230
13:55:14  INFO        [FAIL] JazzHR: 0 jobs | sample: "" | loc: "" | Could not find working JazzHR career page to test
13:55:15  INFO      
=== 8. HRMDIRECT ===
13:55:15  INFO        Trying gwelec...
13:55:15  INFO          Status: 200, length: 35894
13:55:15  INFO          Page title: 
13:55:15  INFO          Total links: 7
13:55:15  INFO        Trying inviso...
13:55:15  INFO          Status: 200, length: 35285
13:55:15  INFO          Page title: 
13:55:15  INFO          Total links: 8
13:55:15  INFO        Trying ogind...
13:55:15  INFO          Status: 200, length: 30144
13:55:15  INFO          Page title: Careers At O&G Industries Inc
13:55:15  INFO          Total links: 4
13:55:15  INFO        [FAIL] HRMDirect: 0 jobs | sample: "" | loc: "" | No working HRMDirect company found
13:55:16  INFO      
======================================================================
13:55:16  INFO      SUMMARY
13:55:16  INFO      ======================================================================
13:55:16  INFO        ✗ Taleo: All test companies failed
13:55:16  INFO        ✓ Oracle Cloud HCM: 2205 jobs — REST API works, job ID: 330061
13:55:16  INFO        ✓ BrassRing: 1 jobs — HTML scrape fallback (partner=25212)
13:55:16  INFO        ✓ Paylocity: 21 jobs — window.pageData works
13:55:16  INFO        ✗ Zoho Recruit: No extractable job data found. Page has 1770109 chars
13:55:16  INFO        ✗ YCombinator: Likely requires JS rendering or login. Consider keeping blacklisted.
13:55:16  INFO        ✗ JazzHR: Could not find working JazzHR career page to test
13:55:16  INFO        ✗ HRMDirect: No working HRMDirect company found
13:55:16  INFO      
