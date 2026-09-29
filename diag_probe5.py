"""TEMPORARY diagnostic round 4 (2026-09) — Ashby window.__appData content.
Deleted after use."""
import asyncio
import json
import logging
import random
import re

logging.basicConfig(level=logging.WARNING)

import ats_scrapers as A

_APPDATA_RE = re.compile(r"window\.__appData\s*=\s*(.*?);\s*</script>", re.DOTALL)
_APPDATA_RE2 = re.compile(r"window\.__appData\s*=\s*(\{.*)", re.DOTALL)


async def main():
    urls = [
        "https://jobs.ashbyhq.com/abby-care/9a213b20-035d-458c-921e-01c80bc2f3df",
        "https://jobs.ashbyhq.com/alchemi/f1254076-a795-4417-8793-764cdb33b7a2",
    ]
    for url in urls:
        r = A._get_requests_sync(url, headers={"User-Agent": random.choice(A.USER_AGENTS)})
        if not r:
            print(f"{url}: FAILED")
            continue
        text = r.text
        m = _APPDATA_RE.search(text)
        print(f"\nURL={url}")
        print("regex1 (up to </script>) matched:", bool(m))
        if m:
            raw = m.group(1)
            print("captured length:", len(raw))
            print("first 800 chars:", raw[:800])
            print("last 300 chars:", raw[-300:])
            try:
                data = json.loads(raw)
                print("JSON PARSE: SUCCESS. top keys:", list(data.keys()) if isinstance(data, dict) else type(data))
            except Exception as e:
                print("JSON PARSE FAILED:", type(e).__name__, e)
        # also find the whole <script nonce=...> block for context
        idx = text.find("window.__appData")
        print("--- raw context (600 chars before, 1200 after) ---")
        print(text[max(0, idx-600):idx+1200])
    await A.aclose_http_client()


if __name__ == "__main__":
    asyncio.run(main())
