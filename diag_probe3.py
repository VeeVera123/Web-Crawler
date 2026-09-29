"""TEMPORARY diagnostic round 2 (2026-09) — fills gaps left by
diag_probe2.py's first pass. Deleted after use."""
import asyncio
import json
import logging
import random
import re
import sys

logging.basicConfig(level=logging.WARNING)

import ats_scrapers as A


def hr(title):
    print(f"\n{'='*90}\n{title}\n{'='*90}")


def dump(label, text, n=4000):
    print(f"--- {label} (first {n} chars) ---")
    print((text or "")[:n])
    print("--- end ---")


async def diag_ashby_error():
    hr("ASHBY — real exception detail")
    import httpx
    urls = [
        "https://api.ashbyhq.com/posting-api/posting/abby-care/9a213b20-035d-458c-921e-01c80bc2f3df",
        "https://api.ashbyhq.com/posting-api/posting/alchemi/f1254076-a795-4417-8793-764cdb33b7a2",
    ]
    for api_url in urls:
        for hdrs in (
            {"User-Agent": random.choice(A.USER_AGENTS)},
            {"User-Agent": random.choice(A.USER_AGENTS), "Accept": "application/json"},
            {},
        ):
            try:
                r = A.requests.get(api_url, timeout=15, headers=hdrs)
                print(f"url={api_url} headers={hdrs} -> status={r.status_code} len={len(r.text)}")
                if r.status_code != 200:
                    print("  body:", r.text[:500])
                else:
                    print("  body:", r.text[:500])
            except Exception as e:
                print(f"url={api_url} headers={hdrs} -> EXCEPTION {type(e).__name__}: {e}")


async def diag_taleo_known():
    hr("TALEO — known-good real URL")
    url = "https://agnicoeagle.taleo.net/careersection/fr_profile/jobdetail.ftl?job=ONT00103"
    r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
    print(f"LISTING STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
    if r:
        apply_url = url.replace("jobdetail.ftl", "jobapply.ftl")
        r2 = A._get_requests_sync(apply_url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"APPLY STATUS={r2.status_code if r2 else 'FAILED'} FINAL_URL={r2.url if r2 else ''} LEN={len(r2.text) if r2 else 0}")
        if r2:
            has_form = "<form" in r2.text.lower()
            has_input = bool(re.search(r"<input|<select|<textarea", r2.text, re.I))
            print(f"has_form={has_form} has_input_elements={has_input}")
            dump("apply body", r2.text, 3000)
            found = A._find_embedded_questions(r2.text)
            print("embedded questions found:", found)
            found2 = A._parse_form_elements(r2.text)
            print("form elements found:", found2)


async def diag_smartrecruiters_known():
    hr("SMARTRECRUITERS — known-good real URL")
    for url in ("https://jobs.smartrecruiters.com/AceTate/744000152451649",
                "https://jobs.smartrecruiters.com/AllyeEnergy/744000146404860"):
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"\nURL={url} STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
        if r:
            dump("body", r.text, 3000)
            found = A._find_embedded_questions(r.text)
            print("embedded questions found:", found)


async def diag_bamboohr_deep():
    hr("BAMBOOHR — full-page search for form/question signal")
    for url in ("https://aeyangon.bamboohr.com/careers/25", "https://aaxiscommerce.bamboohr.com/careers/58"):
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"\nURL={url} STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
        if not r:
            continue
        text = r.text
        for kw in ("application/ld+json", "__NEXT_DATA__", "ApplicationForm", "applyUrl", "apply-button",
                   "window.APP_", "job-detail", "CustomFields", "questions"):
            idx = text.find(kw)
            print(f"  contains {kw!r}: {idx != -1} (idx={idx})")
        # dump around any 'apply' href
        m = re.search(r'href=["\']([^"\']*apply[^"\']*)["\']', text, re.I)
        if m:
            print("  found apply-ish href:", m.group(1))
        found = A._find_embedded_questions(text)
        print("  embedded questions found:", found)
        found2 = A._parse_form_elements(text)
        print("  form elements found:", found2)


async def diag_folkshr_ldjson():
    hr("FOLKSHR — JSON-LD block content")
    url = "https://jobs.folksats.app/cafewilliam/20260914001"
    r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
    if r:
        for m in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', r.text, re.I | re.DOTALL):
            print("LD-JSON BLOCK:")
            print(m.group(1)[:2500])
        loc = A._extract_location_from_html(r.text)
        print("extract_location_from_html result:", repr(loc))


async def diag_personio_jobpage():
    hr("PERSONIO — bare job page (non-apply) for an empty-desc job")
    # ahead-gmbh job 2761882 had empty jobDescriptions in the XML feed
    url = "https://ahead-gmbh.jobs.personio.de/job/2761882"
    r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
    print(f"URL={url} STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
    if r:
        dump("job page body", r.text, 3000)
        desc = await A._fetch_generic_description({"url": url, "location": ""})
        print("generic_description result len:", len(desc or ""))
        print(desc[:500] if desc else "(empty)")
        apply_url = url.rstrip("/") + "/apply"
        r2 = A._get_requests_sync(apply_url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"\nAPPLY URL={apply_url} STATUS={r2.status_code if r2 else 'FAILED'} LEN={len(r2.text) if r2 else 0}")
        if r2:
            dump("apply page body", r2.text, 2000)


async def diag_brassring_zones():
    hr("BRASSRING — real JobDetails page zone dump")
    urls = [
        "https://sjobs.brassring.com/TGnewUI/Search/home/HomeWithPreLoad?PageType=JobDetails&partnerid=25212&siteid=6065&jobid=3391762",
        "https://krb-sjobs.brassring.com/TGnewUI/Search/home/HomeWithPreLoad?PageType=JobDetails&partnerid=30147&siteid=5040&jobid=98391",
    ]
    for url in urls:
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        print(f"\nURL={url} STATUS={r.status_code if r else 'FAILED'} LEN={len(r.text) if r else 0}")
        if not r:
            continue
        from html import unescape
        html = unescape(r.text)
        fields = [(m.group("zone"), m.group("qtype"), m.group("value")[:80])
                  for m in A._BRASSRING_QA_FIELD_RE.finditer(html)]
        print(f"  {len(fields)} QA fields found. Zone names:")
        for zone, qtype, val in fields[:40]:
            print(f"    zone={zone!r} qtype={qtype!r} value_preview={val!r}")
        disp = A._BRASSRING_FIELDS_TO_DISPLAY_RE.search(html)
        if disp:
            print("  JobDetailFieldsToDisplay block:", disp.group("block")[:1500])
        # look for title too
        tmatch = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
        print("  <title>:", tmatch.group(1)[:200] if tmatch else None)


async def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    diags = {
        "ashby_error": diag_ashby_error,
        "taleo_known": diag_taleo_known,
        "smartrecruiters_known": diag_smartrecruiters_known,
        "bamboohr_deep": diag_bamboohr_deep,
        "folkshr_ldjson": diag_folkshr_ldjson,
        "personio_jobpage": diag_personio_jobpage,
        "brassring_zones": diag_brassring_zones,
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
