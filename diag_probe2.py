"""TEMPORARY diagnostic (2026-09) — raw-dump probe for the ATS capability
audit's follow-up fixes. Not part of the production pipeline; deleted after
use. Dumps raw HTTP responses/JSON shapes for each platform flagged broken
in the audit, so fixes to ats_scrapers.py can be based on real evidence
instead of guesses. Run via GitHub Actions (this sandbox's egress proxy
blocks these domains directly).
"""
import asyncio
import json
import logging
import random
import re
import sys
import time

logging.basicConfig(level=logging.WARNING)

import ats_scrapers as A
import supabase_handler


def hr(title):
    print(f"\n{'='*90}\n{title}\n{'='*90}")


def sample_slugs(ats: str, n: int = 6):
    rows = supabase_handler._get("archive_i", f"select=slug&ats=eq.{ats}", limit=5000)
    slugs = list({r["slug"] for r in (rows or []) if r.get("slug")})
    random.shuffle(slugs)
    return slugs[:n]


def dump(label, text, n=2500):
    print(f"--- {label} (first {n} chars) ---")
    print((text or "")[:n])
    print("--- end ---")


async def diag_ashby():
    hr("ASHBY — posting-api raw JSON")
    for slug in sample_slugs("ashby", 3):
        jobs = await A.scrape_ashby(slug)
        if not jobs:
            continue
        job = jobs[0]
        m = re.search(r"ashbyhq\.com/([^/]+)/([a-f0-9-]+)", job["url"])
        if not m:
            print(f"URL didn't match expected shape: {job['url']}")
            continue
        s, jid = m.group(1), m.group(2)
        api_url = f"https://api.ashbyhq.com/posting-api/posting/{s}/{jid}"
        r = A._get_requests_sync(api_url)
        print(f"\nSLUG={slug} STATUS={r.status_code if r else 'FAILED'} URL={api_url}")
        if r:
            try:
                data = r.json()
                print("TOP-LEVEL KEYS:", list(data.keys()))
                for k in ("applicationFormDefinition", "formDefinition", "surveyQuestions"):
                    if k in data:
                        print(f"  {k} = {json.dumps(data[k])[:1500]}")
            except Exception as e:
                dump("non-JSON body", r.text, 1000)
        break_only_one = True
        if break_only_one:
            pass


async def diag_adp():
    hr("ADP — requisition detail raw JSON")
    for slug in sample_slugs("adp", 3):
        jobs = await A.scrape_adp(slug)
        if not jobs:
            continue
        job = jobs[0]
        r = A._get_requests_sync(job["url"], headers={"User-Agent": random.choice(A.USER_AGENTS), "Accept": "application/json"})
        print(f"\nSLUG={slug} STATUS={r.status_code if r else 'FAILED'} URL={job['url']}")
        if r:
            try:
                data = r.json()
                print("TOP-LEVEL KEYS:", list(data.keys()) if isinstance(data, dict) else type(data))
                print(json.dumps(data, indent=None)[:3000])
            except Exception:
                dump("non-JSON body", r.text, 1000)
        return


async def diag_oracle():
    hr("ORACLE CLOUD HCM — CE requisition-details raw JSON")
    for slug in sample_slugs("oracle_cloud_hcm", 4):
        jobs = await A.scrape_oracle_cloud_hcm(slug)
        if not jobs:
            continue
        job = jobs[0]
        print(f"\nSLUG={slug} JOB URL={job['url']} listing-desc-len={len(job.get('description_snippet') or '')}")
        m = A._ORACLE_JOB_URL_RE.match(job["url"])
        if not m:
            print("URL didn't match _ORACLE_JOB_URL_RE")
            continue
        host, _site, job_id = m.groups()
        api_url = f"{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails/{job_id}"
        import uuid as _uuid
        headers = {
            "User-Agent": random.choice(A.USER_AGENTS), "Accept": "application/json",
            "ora-irc-cx-userid": str(_uuid.uuid4()), "ora-irc-language": "en",
        }
        r = A._get_requests_sync(api_url, params={"onlyData": "true", "expand": "all"}, headers=headers)
        print(f"DETAIL STATUS={r.status_code if r else 'FAILED'} URL={api_url}")
        if r:
            try:
                data = r.json()
                print("TOP-LEVEL KEYS:", list(data.keys()) if isinstance(data, dict) else type(data))
                # look for description-shaped and question-shaped keys anywhere shallow
                def shallow_keys(d, depth=0, path=""):
                    if depth > 2 or not isinstance(d, dict):
                        return
                    for k, v in d.items():
                        print(f"    {'  '*depth}{path}{k}: {type(v).__name__}" + (f" len={len(v)}" if isinstance(v, (list, str)) else ""))
                        if isinstance(v, dict):
                            shallow_keys(v, depth+1, path+k+".")
                        elif isinstance(v, list) and v and isinstance(v[0], dict):
                            shallow_keys(v[0], depth+1, path+k+"[0].")
                shallow_keys(data)
            except Exception:
                dump("non-JSON body", r.text, 1000)
        return


async def diag_taleo():
    hr("TALEO — jobapply.ftl raw HTML")
    for slug in sample_slugs("taleo", 3):
        jobs = await A.scrape_taleo(slug)
        if not jobs:
            continue
        job = jobs[0]
        url = job["url"]
        print(f"\nSLUG={slug} URL={url}")
        if "jobdetail.ftl" in url:
            apply_url = url.replace("jobdetail.ftl", "jobapply.ftl")
            r = A._get_requests_sync(apply_url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
            print(f"APPLY STATUS={r.status_code if r else 'FAILED'} FINAL_URL={r.url if r else ''} LEN={len(r.text) if r else 0}")
            if r:
                has_form = "<form" in r.text.lower()
                has_login = bool(re.search(r"sign\s*in|log\s*in|create.*account|password", r.text, re.I))
                print(f"has_form={has_form} has_login_wall_words={has_login}")
                dump("apply page body", r.text, 2000)
        return


async def diag_bamboohr():
    hr("BAMBOOHR — listing + /apply raw HTML")
    for slug in sample_slugs("bamboohr", 3):
        jobs = A.scrape_bamboohr(slug)
        if not jobs:
            continue
        job = jobs[0]
        url = job["url"]
        print(f"\nSLUG={slug} URL={url}")
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"LISTING STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
        if r:
            dump("listing body", r.text, 1500)
        apply_url = url.rstrip("/") + "/apply"
        r2 = A._get_requests_sync(apply_url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"APPLY STATUS={r2.status_code if r2 else 'FAILED'} LEN={len(r2.text) if r2 else 0}")
        if r2:
            dump("apply body", r2.text, 1500)
        return


async def diag_workday():
    hr("WORKDAY — job page raw HTML + CXS API check")
    for slug in sample_slugs("workday", 3):
        jobs = await A.scrape_workday(slug)
        if not jobs:
            continue
        job = jobs[0]
        url = job["url"]
        print(f"\nSLUG={slug} URL={url}")
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
        if r:
            has_login = bool(re.search(r"sign\s*in|log\s*in|create.*account", r.text, re.I))
            has_qform = bool(re.search(r"questionnaire|screening|application.{0,20}question", r.text, re.I))
            print(f"has_login_wall_words={has_login} has_question_words={has_qform}")
            dump("job page body", r.text, 1500)
        return


async def diag_smartrecruiters():
    hr("SMARTRECRUITERS — job page raw HTML")
    for slug in sample_slugs("smartrecruiters", 3):
        jobs = await A.scrape_smartrecruiters(slug)
        if not jobs:
            continue
        job = jobs[0]
        url = job["url"]
        print(f"\nSLUG={slug} URL={url}")
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
        if r:
            dump("job page body", r.text, 2000)
        return


async def diag_zoho():
    hr("ZOHO — career page raw HTML (fresh slugs)")
    for slug in sample_slugs("zoho", 5):
        url = f"https://{slug}.zohorecruit.com/jobs/Careers"
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"\nSLUG={slug} STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
        if r:
            has_jobs_input = bool(re.search(r'(?:id|name)=["\']jobs["\']', r.text, re.I))
            has_ldjson = "application/ld+json" in r.text
            print(f"has_jobs_input={has_jobs_input} has_ldjson={has_ldjson}")
            dump("career page body", r.text, 2000)


async def diag_paylocity():
    hr("PAYLOCITY — timing + question-candidate fetch timing")
    for slug in sample_slugs("paylocity", 3):
        t0 = time.time()
        jobs = A.scrape_paylocity(slug)
        t1 = time.time()
        print(f"\nSLUG={slug} scrape_paylocity took {t1-t0:.2f}s -> {len(jobs)} jobs")
        if not jobs:
            continue
        job = jobs[0]
        url = job["url"]
        print(f"JOB URL={url}")
        for suffix in ("", "/apply", "/application", "/apply-now", "/application-form"):
            cand = url.rstrip("/") + suffix if suffix else url
            t2 = time.time()
            r = A._get_requests_sync(cand, headers={"User-Agent": random.choice(A.USER_AGENTS)})
            t3 = time.time()
            print(f"  candidate={cand} status={r.status_code if r else 'FAILED'} took={t3-t2:.2f}s")
        return


async def diag_rippling():
    hr("RIPPLING — plain job-page Next.js data route (non-apply)")
    for slug in sample_slugs("rippling", 3):
        jobs = A.scrape_rippling(slug)
        if not jobs:
            continue
        job = jobs[0]
        url = job["url"]
        print(f"\nSLUG={slug} URL={url} listing-desc-len={len(job.get('description_snippet') or '')}")
        m = A._RIPPLING_JOB_URL_RE.search(url)
        if not m:
            print("URL didn't match _RIPPLING_JOB_URL_RE")
            continue
        board_slug, job_id = m.group(1), m.group(2)
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        bm = A._NEXT_BUILD_ID_RE.search(r.text) if r else None
        print(f"page fetch status={r.status_code if r else 'FAILED'} buildId_found={bool(bm)}")
        if not bm:
            continue
        build_id = bm.group(1)
        # plain (non-apply) data route
        data_url = f"https://ats.rippling.com/_next/data/{build_id}/en-US/{board_slug}/jobs/{job_id}.json"
        r2 = A._get_requests_sync(data_url, headers={"User-Agent": random.choice(A.USER_AGENTS), "Accept": "application/json"})
        print(f"PLAIN DATA ROUTE status={r2.status_code if r2 else 'FAILED'} url={data_url}")
        if r2:
            try:
                data = r2.json()
                job_post = (((data.get("pageProps") or {}).get("apiData") or {}).get("jobPost")) or {}
                print("jobPost KEYS:", list(job_post.keys()))
                for k in ("description", "descriptionHtml", "jobDescription", "content"):
                    if k in job_post:
                        print(f"  {k} (len={len(str(job_post[k]))}): {str(job_post[k])[:500]}")
            except Exception as e:
                dump("non-JSON body", r2.text, 1500)
        return


async def diag_personio():
    hr("PERSONIO — raw XML feed for companies with missing JD")
    for slug in sample_slugs("personio", 6):
        for domain in ("jobs.personio.de", "jobs.personio.com"):
            url = f"https://{slug}.{domain}/xml?language=en"
            r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
            if r and r.text.strip().startswith("<?xml"):
                jobs = A.scrape_personio(slug)
                empties = sum(1 for j in jobs if not j.get("description_snippet"))
                print(f"\nSLUG={slug} domain={domain} jobs={len(jobs)} empty_desc={empties}/{len(jobs)}")
                if empties:
                    dump("raw XML (first position block)", r.text[:4000], 4000)
                break


async def diag_folkshr():
    hr("FOLKSHR — job detail page raw HTML (location markup)")
    for slug in sample_slugs("folkshr", 3):
        jobs = await A.scrape_folkshr(slug)
        if not jobs:
            continue
        job = jobs[0]
        url = job["url"]
        print(f"\nSLUG={slug} URL={url}")
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
        if r:
            has_ldjson = "application/ld+json" in r.text
            print(f"has_ldjson={has_ldjson}")
            dump("job detail body", r.text, 3000)
        return


async def diag_jobadder():
    hr("JOBADDER — listing + detail raw HTML (known-good boards)")
    for slug in ("57292|the-north-australian-pastoral-company", "21713|lkm-recruitment"):
        client_id, board_slug = slug.split("|", 1)
        base = f"https://clientapps.jobadder.com/{client_id}/{board_slug}"
        r = A._get_requests_sync(base, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"\nSLUG={slug} LISTING STATUS={r.status_code if r else 'FAILED'} FINAL_URL={r.url if r else ''} LEN={len(r.text) if r else 0}")
        if r:
            dump("listing body", r.text, 3000)
            m = re.search(rf'href=["\']([^"\']*/{re.escape(client_id)}/{re.escape(board_slug)}/\d+[^"\']*)["\']', r.text, re.I)
            if m:
                detail_url = m.group(1)
                if not detail_url.startswith("http"):
                    detail_url = "https://clientapps.jobadder.com" + detail_url if detail_url.startswith("/") else base.rstrip("/") + "/" + detail_url
                r2 = A._get_requests_sync(detail_url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
                print(f"DETAIL STATUS={r2.status_code if r2 else 'FAILED'} URL={detail_url}")
                if r2:
                    dump("detail body", r2.text, 3000)


async def diag_avature():
    hr("AVATURE — job detail page raw HTML (location markup)")
    for slug in sample_slugs("avature", 4):
        jobs = A.scrape_avature(slug)
        if not jobs:
            continue
        job = jobs[0]
        url = job["url"]
        print(f"\nSLUG={slug} URL={url} listing_location={job.get('location')!r}")
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
        if r:
            has_ldjson = "application/ld+json" in r.text
            print(f"has_ldjson={has_ldjson}")
            dump("job detail body", r.text, 3000)
        return


async def diag_jobylon():
    hr("JOBYLON — bare-numeric-id redirect resolution test")
    for cid in ("3002", "2160", "1", "500", "1000"):
        for path in (f"/companies/{cid}/", f"/companies/{cid}"):
            url = f"https://emp.jobylon.com{path}"
            r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
            print(f"cid={cid} path={path} status={r.status_code if r else 'FAILED'} final_url={r.url if r else ''}")


async def diag_brassring_dns():
    hr("BRASSRING — DNS resolution check for all 5 fallback hosts")
    import socket
    for host in ("sjobs.brassring.com", "xjobs.brassring.com", "krb-sjobs.brassring.com",
                 "krb-xjobs.brassring.com", "krbcn-sjobs.brassring.com"):
        try:
            ip = socket.gethostbyname(host)
            print(f"{host}: RESOLVES -> {ip}")
        except Exception as e:
            print(f"{host}: FAILS -> {type(e).__name__}: {e}")


async def diag_flatchr():
    hr("FLATCHR — spot check real slugs")
    for slug in sample_slugs("flatchr", 8):
        jobs = A.scrape_flatchr(slug)
        print(f"slug={slug} -> {len(jobs)} jobs")


async def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    diags = {
        "ashby": diag_ashby, "adp": diag_adp, "oracle": diag_oracle, "taleo": diag_taleo,
        "bamboohr": diag_bamboohr, "workday": diag_workday, "smartrecruiters": diag_smartrecruiters,
        "zoho": diag_zoho, "paylocity": diag_paylocity, "rippling": diag_rippling,
        "personio": diag_personio, "folkshr": diag_folkshr, "jobadder": diag_jobadder,
        "avature": diag_avature, "jobylon": diag_jobylon, "brassring_dns": diag_brassring_dns,
        "flatchr": diag_flatchr,
    }
    if which == "all":
        for name, fn in diags.items():
            try:
                await fn()
            except Exception as e:
                print(f"\n!!! {name} diagnostic CRASHED: {type(e).__name__}: {e}")
    else:
        await diags[which]()
    await A.aclose_http_client()


if __name__ == "__main__":
    asyncio.run(main())
