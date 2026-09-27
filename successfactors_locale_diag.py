"""TEMPORARY diagnostic (not part of the pipeline) — answers a real
question before deciding whether to cut SuccessFactors's locale scraping
down to English-only: are the extra locales the SAME job postings just
translated, or do they expose DIFFERENT jobs (e.g. country-specific
postings only visible under their own locale)?

Reuses the exact _SF_JOB_ROW_RE/_SF_LOCALE_RE regexes from
ats_scrapers.py so this tests the real parsing logic, not a guess.
Removed after use — see the session's established discipline of live
verification via a temporary GitHub Actions workflow before shipping
any change based on an assumption about live third-party site behavior.
"""
import re
import sys
import httpx

_SF_JOB_ROW_RE = re.compile(
    r'<a[^>]+href="(/job/[^"?#]+?/(\d{5,})/?)"[^>]*>(.*?)</a>(.{0,400}?)'
    r'(?=<a[^>]+href="/job/|\Z)',
    re.I | re.S,
)
_SF_LOCALE_RE = re.compile(r'[?&]locale=([a-z]{2}_[A-Z]{2})\b')

TENANTS = [
    "careerstore.munichre.com",
    "empleo.es.deloitte.com",
    "jobs.alfanar.com",
    "jobs.congatec.com",
]

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def fetch_jobs(client, origin, locale=None):
    params = {}
    if locale:
        params["locale"] = locale
    try:
        r = client.get(f"{origin}/search/", headers=HEADERS, params=params, timeout=20)
    except Exception as e:
        print(f"    ERROR fetching locale={locale}: {type(e).__name__}: {e}")
        return {}
    if r.status_code != 200:
        print(f"    locale={locale}: HTTP {r.status_code}")
        return {}
    jobs = {}
    for m in _SF_JOB_ROW_RE.finditer(r.text):
        job_path, job_id, title_html, _trailer = m.groups()
        title = re.sub(r"<[^>]+>", " ", title_html)
        title = re.sub(r"\s+", " ", title).strip()
        jobs[job_id] = title
    return jobs


def main():
    with httpx.Client(follow_redirects=True, http2=True) as client:
        for host in TENANTS:
            origin = f"https://{host}"
            print(f"\n{'='*70}\n{host}\n{'='*70}")
            try:
                r = client.get(f"{origin}/search/", headers=HEADERS, timeout=20)
            except Exception as e:
                print(f"  ERROR: {type(e).__name__}: {e}")
                continue
            if r.status_code != 200:
                print(f"  default /search/ -> HTTP {r.status_code}, skipping")
                continue
            locales = list(dict.fromkeys(_SF_LOCALE_RE.findall(r.text)))[:10]
            default_jobs = fetch_jobs(client, origin, None)
            print(f"  discovered locales: {locales}")
            print(f"  default (no-locale) pass: {len(default_jobs)} jobs")

            all_ids = set(default_jobs.keys())
            for loc in locales:
                loc_jobs = fetch_jobs(client, origin, loc)
                shared = set(loc_jobs) & set(default_jobs)
                only_in_locale = set(loc_jobs) - set(default_jobs)
                all_ids |= set(loc_jobs)
                # Sample a shared job's title to see if it's translated or identical
                sample_line = ""
                if shared:
                    sample_id = sorted(shared)[0]
                    same_title = default_jobs[sample_id] == loc_jobs[sample_id]
                    sample_line = (
                        f" | sample shared job {sample_id}: "
                        f"{'IDENTICAL title' if same_title else 'DIFFERENT title'} "
                        f"(default={default_jobs[sample_id]!r} vs {loc!r}={loc_jobs[sample_id]!r})"
                    )
                print(f"  locale={loc}: {len(loc_jobs)} jobs, "
                      f"{len(shared)} shared with default, "
                      f"{len(only_in_locale)} ONLY in this locale{sample_line}")

            print(f"  TOTAL UNIQUE job IDs across default+all locales: {len(all_ids)} "
                  f"(vs {len(default_jobs)} from English/default alone)")


if __name__ == "__main__":
    main()
