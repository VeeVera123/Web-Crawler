"""Offline checks for the Comeet platform: URL -> slug extraction, location text, and scrape_comeet's parsing
(API path, embedded-data fallback, internal positions, duplicates). No network.

    python check_comeet.py
"""
import json
import os
import sys

for _k in ("SUPABASE_URL", "SUPABASE_KEY", "CEREBRAS_API_KEY", "GROQ_API_KEY", "GROQ_API_KEY_C",
           "OPENAI_API_KEY", "NVIDIA_API_KEY", "ANTHROPIC_API_KEY"):
    os.environ.setdefault(_k, "x")
import logging  # noqa: E402

logging.disable(logging.CRITICAL)
import ats_scrapers as A  # noqa: E402
import discovery as D  # noqa: E402

fails, n = [], 0


def check(cond, msg):
    global n
    n += 1
    if not cond:
        fails.append(msg)


# ── URL -> slug ──
for url, want in [("https://www.comeet.com/jobs/liveu/90.00C", "liveu|90.00C"),
                  ("https://www.comeet.com/jobs/mitiga/26.00B/principal-cloud-security-researcher/45.768", "mitiga|26.00B"),
                  ("https://www.comeet.co/jobs/Primusgroup/59.007", "primusgroup|59.007"),
                  ("https://www.comeet.com/jobs/hub-technologies/07.00f", "hub-technologies|07.00F"),
                  ("https://www.comeet.com/", None), ("https://www.comeet.com/jobs/x", None),
                  ("https://www.comeet.com/blog/post/12.345", None), ("https://www.comeet.com/jobs/liveu/notauid", None),
                  ("https://example.com/jobs/liveu/90.00C", None)]:
    check(D._url_to_slug_comeet(url) == want, f"{url} -> {D._url_to_slug_comeet(url)!r}, want {want!r}")
check("comeet" in D.URL_TO_SLUG and "comeet" in D.SUPPORTED_ATS and "comeet" in A.SCRAPERS, "comeet registered")

# ── location text ──
for p, want in [({"location": {"name": "Israel", "city": "Kefar Sava", "country": "IL"}, "workplace_type": "Hybrid"}, ("Kefar Sava, Israel", "Hybrid")),
                ({"location": {"name": "United States", "city": "remote"}, "workplace_type": "Remote"}, ("Remote, United States", "Remote")),
                ({"location": {"name": "Germany", "city": "Berlin", "is_remote": True}}, ("Remote, Berlin, Germany", "Remote")),
                ({"location": {"name": "Germany", "city": "Germany"}, "workplace_type": "On-site"}, ("Germany", "On-site")),
                ({"location": {}, "workplace_type": "Remote"}, ("Remote", "Remote")), ({"location": None}, ("", ""))]:
    got = A._comeet_location(p)
    check(got == want, f"_comeet_location({p}) = {got}, want {want}")

# ── scrape_comeet with a stubbed network ──
PAGE = ('<script>var COMPANY_DATA; COMPANY_DATA = ' + json.dumps({"name": "LiveU", "company_uid": "90.00C", "token": "TOK"}) +
        '; var COMPANY_POSITIONS_DATA; COMPANY_POSITIONS_DATA = ' +
        json.dumps([{"name": "Bookkeeper", "uid": "7E.F62", "url_active_page": "https://www.comeet.com/jobs/liveu/90.00C/bookkeeper/7E.F62",
                     "location": {"name": "Israel", "city": "Kefar Sava"}, "workplace_type": "Hybrid"}]) + ';</script>')
API = [{"name": "Customer Success Manager", "uid": "AA.111", "company_name": "LiveU", "department": "CS",
        "url_active_page": "https://www.comeet.com/jobs/liveu/90.00C/csm/AA.111?utm=x", "employment_type": "Permanent",
        "location": {"name": "United Kingdom", "city": "London"}, "workplace_type": "Remote",
        "details": [{"name": "Description", "value": "<p>Own the accounts. Salary $90,000 - $110,000.</p>"}]},
       {"name": "Customer Success Manager", "uid": "AA.111", "url_active_page": "https://www.comeet.com/jobs/liveu/90.00C/csm/AA.111",
        "location": {}},                                  # duplicate url
       {"name": "Internal only", "uid": "BB.222", "is_internal": True, "url_active_page": "https://www.comeet.com/jobs/liveu/90.00C/x/BB.222"},
       {"name": "", "url_active_page": "https://www.comeet.com/jobs/liveu/90.00C/y/CC.333"}]    # no title


class R:
    def __init__(self, status, text="", data=None):
        self.status_code, self.text, self._d = status, text, data

    def json(self):
        if self._d is None:
            raise ValueError("no json")
        return self._d


def stub(api_ok):
    def get(url, **kw):
        if "careers-api" in url:
            return R(200, data=API) if api_ok else R(500)
        return R(200, PAGE) if "liveu/90.00C" in url else R(302)
    return get


A._get_requests_sync = stub(True)
jobs = A.scrape_comeet("LiveU|90.00c")           # case-insensitive slug
check(len(jobs) == 1, f"API path: duplicate / internal / untitled positions dropped, got {len(jobs)}")
j = jobs[0]
check(j["title"] == "Customer Success Manager" and j["url"].endswith("/AA.111") and "?" not in j["url"], f"url/title: {j['url']}")
check(j["location"] == "Remote, London, United Kingdom" and j["workplace_type"] == "Remote" and j["country"] == "", f"location: {j['location']!r}")
check("Own the accounts" in j["description_snippet"] and j["source_ats"] == "Comeet" and j["slug"] == "LiveU|90.00c" and j["department"] == "CS",
      f"fields: {j}")
A._get_requests_sync = stub(False)               # API down -> the page's embedded positions (no description)
jobs = A.scrape_comeet("liveu|90.00C")
check(len(jobs) == 1 and jobs[0]["title"] == "Bookkeeper" and jobs[0]["description_snippet"] == "", f"fallback: {jobs}")
check(A.scrape_comeet("nosuchcompany|00.000") == [] and A.scrape_comeet("bad") == [] and A.scrape_comeet("") == [] and A.scrape_comeet(None) == [],
      "unknown tenant / bad slugs return []")

print(f"comeet checks: {n - len(fails)}/{n} passed")
for f in fails:
    print("  FAIL", f)
sys.exit(1 if fails else 0)
