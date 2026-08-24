"""
CLASS A SEED-AND-PROBE EXTRACTION (2026-08) — a genuinely different
technique from the CT-logs pipeline (ctlog_extract.py), built specifically
for the platforms CT logs structurally can't touch: Greenhouse, Lever,
Ashby, Workable. See CTLOGS_CLASS_A_HELP_REQUEST.md and
CLASS_A_SUGGESTIONS_REVIEW.md for the full background — short version:
these platforms put the company identifier in a URL PATH, not a hostname
(e.g. boards.greenhouse.io/{slug}), and a TLS certificate can never carry
a path. CT logs return zero tenant signal for them no matter how large
the platform is.

WHY THIS WORKS INSTEAD: all four platforms expose a real, documented,
UNAUTHENTICATED public job-board API keyed by the company's slug:
  Greenhouse: boards-api.greenhouse.io/v1/boards/{slug}/jobs
  Lever:      api.lever.co/v0/postings/{slug}?mode=json
  Ashby:      api.ashbyhq.com/posting-api/job-board/{slug}
  Workable:   www.workable.com/api/accounts/{slug}?details=true
Each returns 200 for a real slug and 404 for a wrong guess. That turns
"discover Greenhouse/Lever/Ashby/Workable companies" into "cheaply check
whether a GUESSED slug is real" — i.e. a seed-and-probe problem, not a
crawl. The seed list (candidate company names) comes from
yc-oss/api — the same free, no-auth Y Combinator company list
discovery.py's fetch_yc_companies() already uses for its own resolver,
reused here rather than building a second seed source. Every YC company
name is normalized into a handful of plausible slug variants (see
_slug_variants) and each variant is probed against all four APIs.

WORKABLE XML FEED: also fetches workable.com/boards/workable.xml
directly — a documented, intentional Workable feature (see Workable's own
help docs) that aggregates postings across many customer accounts in ONE
request, no per-slug guessing needed. Whatever slugs that feed contains
are pure bonus signal on top of the seed-and-probe sweep.

SmartRecruiters was considered and DROPPED from this script — checked
SmartRecruiters' own official API docs directly and confirmed there is no
public company-directory endpoint; every endpoint needs the company
identifier already known, which is the same seed-and-probe shape as the
four platforms above, just without the confirmed free public seed
overlap this project already has for them via YC. Can be added later with
the same _probe_one() shape if a good seed list is worth spending on it.

THIS IS PROBABILISTIC, UNLIKE CT LOGS: CT logs are exhaustive (every
issued cert gets swept). Seed-and-probe only finds a candidate if (a) the
company is in the seed list and (b) its real slug is one of the guessed
variants. Expect a real but partial hit rate, not anywhere close to the
seed list's total size. That's fine — this is explicitly a supplemental
source per the same "thousands would be great, hundreds is still real"
bar the rest of this pipeline uses, not a replacement for a proper
crawl-based Class A solution (the deferred company-domain-CNAME idea is
still the more exhaustive approach, see CTLOGS_CLASS_A_HELP_REQUEST.md).

Writes into the SAME ctlog_probe_results staging table as the CT-logs
pipeline (source_hostname is set to a descriptive marker instead of a
real hostname, since there's no CT-log host involved) — Phase 2
verification already covers everything in that table regardless of which
extraction technique populated it, no changes needed there.

Usage:
    pip install aiohttp requests python-dotenv
    python class_a_probe.py --platform greenhouse
    python class_a_probe.py --platform lever
    python class_a_probe.py --platform ashby
    python class_a_probe.py --platform workable
"""
import argparse
import asyncio
import logging
import os
import re
import sys

import aiohttp
import requests
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from discovery import SKIP_SLUGS, YC_ALL_COMPANIES_URL, YC_USER_AGENT  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("class_a_probe")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=12)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
PROBE_CONCURRENCY = 40  # these are cheap 200/404 HEAD-ish checks against
                          # well-provisioned vendor APIs, not crt.sh — safe
                          # to run much more concurrent than the CT/live-
                          # resolve steps elsewhere in this project.

# ── platform probe URLs — each must return something CLEARLY different
# for a real vs. fake slug (200 w/ real JSON body vs. 404), confirmed
# against each platform's own public API docs (see module docstring). ──
PROBE_URL = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
    "lever": "https://api.lever.co/v0/postings/{slug}?mode=json",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/{slug}",
    "workable": "https://www.workable.com/api/accounts/{slug}?details=true",
}

WORKABLE_FEED_URL = "https://www.workable.com/boards/workable.xml"


# ── seed: company names -> plausible slug variants ──────────────────

def fetch_yc_companies() -> list[dict]:
    """Same free, no-auth YC company list discovery.py's own YC resolver
    uses (see fetch_yc_companies there) — reused here as the seed source
    rather than standing up a second one. ~6k companies, static JSON,
    GitHub Pages-hosted."""
    try:
        r = requests.get(YC_ALL_COMPANIES_URL, timeout=60,
                          headers={"User-Agent": YC_USER_AGENT})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"Failed to fetch YC company list: {e}")
        return []


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_SUFFIX_WORDS = {"inc", "llc", "ltd", "co", "corp", "corporation", "company",
                  "the", "group", "holdings", "technologies", "labs", "hq"}


def _slug_variants(name: str) -> list[str]:
    """Normalize a company name into a handful of plausible ATS slug
    guesses. Real companies pick their own slug at signup, so this is
    inherently a guess — ordered roughly most-to-least likely, callers
    can stop at the first HIT since ATS slugs are unique per platform
    (getting company X's REAL slug via one variant doesn't mean trying
    the others too — see _probe_one)."""
    lowered = name.lower().strip()
    compact = _NON_ALNUM_RE.sub("", lowered)               # "acmecorp"
    hyphenated = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")  # "acme-corp"

    words = [w for w in re.split(r"[^a-z0-9]+", lowered) if w]
    words_no_suffix = [w for w in words if w not in _SUFFIX_WORDS]
    stripped_hyphen = "-".join(words_no_suffix) if words_no_suffix else hyphenated
    stripped_compact = "".join(words_no_suffix) if words_no_suffix else compact

    variants = []
    for v in (compact, hyphenated, stripped_compact, stripped_hyphen):
        if v and v not in SKIP_SLUGS and v not in variants:
            variants.append(v)
    return variants


# ── probing (async) ──────────────────────────────────────────────────

async def _probe_one(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                      ats: str, company_name: str, variants: list[str]) -> tuple[str, str] | None:
    """Try each slug variant for one company in order, stop at the first
    real hit (200 + body that actually looks like a job-board payload,
    not just any 200 — some of these APIs 200 a malformed/empty request
    too, see the per-platform checks below)."""
    url_template = PROBE_URL[ats]
    async with sem:
        for slug in variants:
            url = url_template.format(slug=slug)
            try:
                async with session.get(url, timeout=REQUEST_TIMEOUT,
                                        headers={"User-Agent": USER_AGENT}) as r:
                    if r.status != 200:
                        continue
                    body = await r.text()
            except Exception:
                continue

            if _looks_like_real_board(ats, body):
                return slug, company_name
    return None


def _looks_like_real_board(ats: str, body: str) -> bool:
    """A 200 status alone isn't proof — confirm the body actually looks
    like a job-board payload for this platform, not an empty/error JSON
    shell some of these APIs return with 200 on a bad slug."""
    if not body or len(body) < 2:
        return False
    lowered = body.lower()
    if ats == "greenhouse":
        return '"jobs"' in lowered
    if ats == "lever":
        # Lever returns a bare JSON array — "[]" is a VALID real company
        # with zero current postings, still worth keeping (see BambooHR
        # precedent: a live-but-currently-empty board still counts).
        return body.strip().startswith("[")
    if ats == "ashby":
        return '"jobs"' in lowered or '"joblist"' in lowered or '"organizationname"' in lowered
    if ats == "workable":
        return '"name"' in lowered or '"jobs"' in lowered
    return True


async def run_platform(ats: str) -> None:
    log.info(f"── {ats} (seed-and-probe via YC company list) ──")
    companies = fetch_yc_companies()
    if not companies:
        log.error("  No seed companies fetched — aborting.")
        return
    log.info(f"  {len(companies)} seed companies to probe")

    sem = asyncio.Semaphore(PROBE_CONCURRENCY)
    found: dict[str, str] = {}   # slug -> company_name

    async with aiohttp.ClientSession() as session:
        existing = await fetch_existing_slug_registry_slugs(session, ats)
        log.info(f"  slug_registry already has {len(existing)} {ats} slugs")

        tasks = []
        for c in companies:
            name = (c.get("name") or "").strip()
            if not name:
                continue
            variants = _slug_variants(name)
            if not variants:
                continue
            tasks.append(_probe_one(session, sem, ats, name, variants))

        BATCH = 500  # write incrementally, same "don't lose progress on
                      # an interrupted run" principle as ctlog_extract.py
        for i in range(0, len(tasks), BATCH):
            batch = tasks[i:i + BATCH]
            results = await asyncio.gather(*batch)
            batch_found = {slug: name for r in results if r for slug, name in [r]}
            new_this_batch = {s: n for s, n in batch_found.items() if s not in found}
            found.update(new_this_batch)
            if new_this_batch:
                await write_to_staging_table(session, ats, new_this_batch,
                                              source_marker="seed_probe:ycombinator")
            log.info(f"  probed {min(i + BATCH, len(tasks))}/{len(tasks)} — "
                     f"{len(found)} real slugs found so far")

        if ats == "workable":
            feed_slugs = await fetch_workable_feed_slugs(session)
            new_from_feed = {s: "" for s in feed_slugs if s not in found and s not in existing}
            if new_from_feed:
                await write_to_staging_table(session, ats, new_from_feed,
                                              source_marker="workable_master_xml_feed")
                found.update(new_from_feed)
                log.info(f"  +{len(new_from_feed)} additional slugs from the Workable master XML feed")

    net_new = len(set(found) - existing)
    log.info(f"  TOTAL: {len(found)} real slugs found ({net_new} net-new vs slug_registry)")


_WORKABLE_FEED_SLUG_RE = re.compile(r"apply\.workable\.com/([a-z0-9\-]+)/", re.I)


async def fetch_workable_feed_slugs(session: aiohttp.ClientSession) -> set[str]:
    """Workable's own documented aggregated jobs feed — one request,
    covers many customer accounts at once, no per-slug guessing. See
    module docstring / help.workable.com/hc/en-us/articles/4420464031767.

    STREAMED rather than buffered with r.text(): this feed is large
    enough that a full-body read can hit a transfer-length mismatch
    partway through (confirmed live — aiohttp raises
    ClientPayloadError/TransferEncodingError when the connection is cut
    or the server's Content-Length doesn't match what actually arrives),
    and r.text() discards everything collected so far when that happens.
    Reading in chunks and regex-scanning as data arrives means a
    truncated transfer still yields every slug seen before the cutoff,
    instead of losing the whole feed to one late error."""
    slugs: set[str] = set()
    tail = ""  # carries a possibly-split match pattern across chunk boundaries
    bytes_read = 0
    try:
        async with session.get(WORKABLE_FEED_URL,
                                timeout=aiohttp.ClientTimeout(total=300),
                                headers={"User-Agent": USER_AGENT}) as r:
            if r.status != 200:
                log.warning(f"  Workable feed returned HTTP {r.status} — skipping.")
                return set()
            try:
                async for chunk in r.content.iter_chunked(1 << 20):  # 1MB chunks
                    bytes_read += len(chunk)
                    text = tail + chunk.decode("utf-8", errors="ignore")
                    slugs.update(_WORKABLE_FEED_SLUG_RE.findall(text))
                    tail = text[-200:]  # keep enough overlap that a URL split
                                        # across the chunk boundary still matches
                                        # on the next iteration
            except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
                log.warning(f"  Workable feed cut off after {bytes_read:,} bytes "
                            f"({type(e).__name__}) — using the {len(slugs)} slugs "
                            f"seen before the cutoff rather than discarding them.")
    except Exception as e:
        log.warning(f"  Workable feed fetch failed: {type(e).__name__}: {e or '(no message)'} — skipping.")
        return set()

    slugs = {s.lower() for s in slugs if s.lower() not in SKIP_SLUGS}
    log.info(f"  Workable feed: {len(slugs)} distinct account slugs found "
             f"({bytes_read:,} bytes read)")
    return slugs


# ── Supabase I/O (async) — same shape as ctlog_extract.py's, source_hostname
# repurposed as a free-text source marker since there's no CT-log host here ──

async def fetch_existing_slug_registry_slugs(session: aiohttp.ClientSession, ats: str) -> set[str]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("  SUPABASE_URL/SUPABASE_KEY not set — skipping net-new check.")
        return set()
    all_slugs = set()
    page_size = 1000
    offset = 0
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    while True:
        headers["Range"] = f"{offset}-{offset + page_size - 1}"
        try:
            async with session.get(
                f"{SUPABASE_URL}/rest/v1/slug_registry",
                headers=headers,
                params={"ats": f"eq.{ats}", "select": "slug"},
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                r.raise_for_status()
                batch = await r.json()
        except Exception as e:
            log.warning(f"  slug_registry lookup failed at offset {offset}: {e}")
            break
        if not batch:
            break
        all_slugs.update(row["slug"] for row in batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_slugs


async def write_to_staging_table(session: aiohttp.ClientSession, ats: str,
                                  slugs: dict[str, str], source_marker: str) -> int:
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("  SUPABASE_URL/SUPABASE_KEY not set — cannot write to staging table.")
        return 0
    rows = [
        {"ats": ats, "slug": slug, "source_hostname": source_marker, "root_domain": source_marker}
        for slug in slugs
    ]
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "resolution=merge-duplicates",
    }
    chunk_size = 1000
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

    async def _write_chunk(chunk):
        try:
            async with session.post(
                f"{SUPABASE_URL}/rest/v1/ctlog_probe_results",
                headers=headers,
                params={"on_conflict": "ats,slug"},
                json=chunk,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                r.raise_for_status()
                return len(chunk)
        except Exception as e:
            log.error(f"  Failed to write a chunk to staging table: {e}")
            return 0

    results = await asyncio.gather(*(_write_chunk(c) for c in chunks))
    written = sum(results)
    log.info(f"  Wrote {written}/{len(rows)} rows to ctlog_probe_results")
    return written


def main():
    parser = argparse.ArgumentParser(description="Class A seed-and-probe extraction (async)")
    parser.add_argument("--platform", choices=list(PROBE_URL.keys()), required=True)
    args = parser.parse_args()
    asyncio.run(run_platform(args.platform))


if __name__ == "__main__":
    main()
