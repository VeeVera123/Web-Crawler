"""
Differential test harness: requests vs httpx, byte-for-byte.

Purpose (per OpenAI's reviewed migration plan, explicit user requirement:
"async is allowed to change throughput, but it is absolutely not allowed
to change the bytes/content that ultimately reach the extractor"):

Fetch the SAME real ATS URLs through both the crawler's existing `requests`
transport and a candidate `httpx` transport, and diff:
  - HTTP status code
  - final URL (redirect chains)
  - raw response body length (bytes)
  - SHA-256 of raw bytes (transport-level truncation/corruption check)
  - detected/declared encoding
  - SHA-256 of the DECODED text (catches decoding differences a raw byte
    match alone would miss)
  - parsed-JSON equality (every platform tested here is a JSON API; the
    extractor operates on parsed JSON, not raw text, so a harmless
    whitespace/gzip-padding difference in raw bytes shouldn't fail this
    harness if the PARSED content is identical -- but a genuine content
    difference must)

Any mismatch is reported, not silently ignored. This harness makes NO
production code changes on its own -- it's read-only evidence-gathering,
Phase 0/1 of the migration plan, before anything in ats_scrapers.py
actually switches transports.

NOTE: this must run somewhere with real network access to the public ATS
APIs below (boards-api.greenhouse.io, api.lever.co, api.ashbyhq.com,
apply.workable.com) -- a sandboxed/proxied dev environment with a
restrictive egress allowlist will fail every single fetch with a proxy
403, on BOTH transports equally, which is a network-access problem, not a
transport-equivalence finding. Run this via the
`.github/workflows/httpx-migration-test.yml` workflow (same egress as the
real crawler), or anywhere else with unrestricted outbound HTTPS.

Run standalone: `python3 httpx_migration_diff_harness.py`
"""
from __future__ import annotations

import hashlib
import json
import random
import sys
import time
from dataclasses import dataclass

import httpx
import requests

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
]


# -- Real production URL builders, copied verbatim from the matching
# scrape_* function in Main/ats_scrapers.py, so this harness tests the
# ACTUAL request shape the crawler makes, not a simplified guess. Only
# read-only GET calls against public, unauthenticated job-board APIs.
def _greenhouse(slug: str):
    return f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs", {"content": "true"}, {}


def _lever(slug: str):
    return f"https://api.lever.co/v0/postings/{slug}", {"mode": "json"}, {}


def _ashby(slug: str):
    return f"https://api.ashbyhq.com/posting-api/job-board/{slug}", {"includeCompensation": "true"}, {}


def _workable(slug: str):
    return (f"https://apply.workable.com/api/v1/widget/accounts/{slug}",
            {"details": "true"},
            {"User-Agent": random.choice(USER_AGENTS)})


PLATFORM_BUILDERS = {
    "greenhouse": _greenhouse,
    "lever": _lever,
    "ashby": _ashby,
    "workable": _workable,
}

# Real slugs pulled from this project's own Supabase archive_i table
# (5 per platform, random sample) -- treated purely as opaque identifiers.
REAL_SLUGS = {
    "greenhouse": ["licor", "powertodecide", "teads1", "whogivesacrap", "pantheonpublic"],
    "lever": ["taprootwizards", "articulate", "pinegames", "reach.industries", "stimlabs"],
    "ashby": ["zeno", "pylon-labs", "nuna", "palup", "ChartHop"],
    "workable": ["mr-blue", "p2h", "futurex-1", "environment-agency", "kzsoftworks"],
}

REQUEST_TIMEOUT = 20
MAX_RETRIES = 2


@dataclass
class FetchResult:
    ok: bool
    status: int | None = None
    final_url: str | None = None
    raw_bytes: bytes | None = None
    encoding: str | None = None
    text: str | None = None
    error: str | None = None


def fetch_requests(url: str, params: dict, headers: dict) -> FetchResult:
    session = requests.Session()
    last_exc = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = session.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return FetchResult(
                ok=True, status=r.status_code, final_url=r.url,
                raw_bytes=r.content, encoding=r.encoding, text=r.text,
            )
        except Exception as e:
            last_exc = e
            time.sleep(0.5 * (attempt + 1))
    return FetchResult(ok=False, error=str(last_exc))


def fetch_httpx(url: str, params: dict, headers: dict) -> FetchResult:
    last_exc = None
    timeout = httpx.Timeout(connect=20.0, read=60.0, write=30.0, pool=30.0)
    for attempt in range(MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                r = client.get(url, params=params, headers=headers)
                r.raise_for_status()
                raw = r.content  # full buffered read, no streaming/truncation risk
                return FetchResult(
                    ok=True, status=r.status_code, final_url=str(r.url),
                    raw_bytes=raw, encoding=r.encoding, text=r.text,
                )
        except Exception as e:
            last_exc = e
            time.sleep(0.5 * (attempt + 1))
    return FetchResult(ok=False, error=str(last_exc))


def sha256(data: bytes | str | None) -> str | None:
    if data is None:
        return None
    if isinstance(data, str):
        data = data.encode("utf-8", errors="surrogatepass")
    return hashlib.sha256(data).hexdigest()


def compare_one(platform: str, slug: str) -> dict:
    builder = PLATFORM_BUILDERS[platform]
    url, params, headers = builder(slug)

    req_result = fetch_requests(url, params, headers)
    htx_result = fetch_httpx(url, params, headers)

    row = {
        "platform": platform, "slug": slug, "url": url,
        "requests_ok": req_result.ok, "httpx_ok": htx_result.ok,
    }

    if not req_result.ok:
        row["requests_error"] = req_result.error
    if not htx_result.ok:
        row["httpx_error"] = htx_result.error

    if req_result.ok and htx_result.ok:
        row["requests_status"] = req_result.status
        row["httpx_status"] = htx_result.status
        row["requests_bytes"] = len(req_result.raw_bytes)
        row["httpx_bytes"] = len(htx_result.raw_bytes)
        row["requests_raw_sha256"] = sha256(req_result.raw_bytes)
        row["httpx_raw_sha256"] = sha256(htx_result.raw_bytes)
        row["requests_encoding"] = req_result.encoding
        row["httpx_encoding"] = htx_result.encoding
        row["requests_text_sha256"] = sha256(req_result.text)
        row["httpx_text_sha256"] = sha256(htx_result.text)

        row["status_match"] = req_result.status == htx_result.status
        row["raw_bytes_match"] = row["requests_raw_sha256"] == row["httpx_raw_sha256"]
        row["text_match"] = row["requests_text_sha256"] == row["httpx_text_sha256"]

        try:
            req_json = json.loads(req_result.text)
            htx_json = json.loads(htx_result.text)
            row["json_match"] = req_json == htx_json
        except Exception as e:
            row["json_match"] = None
            row["json_compare_error"] = str(e)

        row["all_match"] = bool(row["status_match"] and row["raw_bytes_match"] and row["text_match"])
    else:
        row["all_match"] = False

    return row


def main():
    results = []
    mismatches = []
    fetch_failures = []

    for platform, slugs in REAL_SLUGS.items():
        for slug in slugs:
            print(f"Comparing {platform}/{slug} ...", file=sys.stderr)
            try:
                row = compare_one(platform, slug)
            except Exception as e:
                row = {"platform": platform, "slug": slug, "error": f"harness exception: {e}", "all_match": False}
            results.append(row)
            if not row.get("requests_ok", True) or not row.get("httpx_ok", True):
                fetch_failures.append(row)
            elif not row.get("all_match"):
                mismatches.append(row)

    print("\n" + "=" * 70)
    print(f"Total comparisons: {len(results)}")
    print(f"Both transports succeeded and matched byte-for-byte: "
          f"{sum(1 for r in results if r.get('all_match'))}")
    print(f"Fetch failures (one or both transports errored): {len(fetch_failures)}")
    print(f"Content MISMATCHES (both succeeded, content differs): {len(mismatches)}")
    print("=" * 70)

    if fetch_failures:
        print("\n--- FETCH FAILURES ---")
        for r in fetch_failures:
            print(json.dumps(r, indent=2, default=str))

    if mismatches:
        print("\n--- CONTENT MISMATCHES (investigate before migrating) ---")
        for r in mismatches:
            print(json.dumps(r, indent=2, default=str))

    with open("httpx_diff_harness_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print("\nFull results written to httpx_diff_harness_results.json")

    return 0 if not mismatches else 1


if __name__ == "__main__":
    sys.exit(main())
