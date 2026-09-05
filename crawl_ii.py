"""
CRAWL II — heuristic generic job-listing scraper for archive_ii (in-house/
unsupported career pages; archive_ii was archive_iii before the 2026-08
Crawl I/Crawl II restructure — see node.py's and crawl_i.py's module
docstrings for the full renaming history).

Crawl I (crawl_i.py) only knows how to read the ~20 recognized ATS
platforms in ats_scrapers.py's SCRAPERS dict. Every career page node.py's
crawl_batch() found that did NOT match one of those platforms — an
in-house careers CMS, a small/unrecognized ATS, a WordPress job-listing
plugin, etc. — lands in archive_ii instead, unscraped. Crawl II is what
finally reads those.

Two independent extraction methods, tried in order, per archive_ii page:

  1. JSON-LD JobPosting structured data (schema.org). The highest-
     confidence source when present — many career-page builders emit this
     for SEO even with no ATS-recognizable URL pattern at all. If a page
     has ANY valid, non-expired JobPosting objects, they are trusted and
     the heuristic pass below is skipped entirely for that page (avoids
     double-counting the same postings two different ways).

  2. Heuristic candidate-link detection, for pages with no JSON-LD:
     (a) anchors whose href path itself looks like an individual job
         posting (/job/, /careers/, /position/, /vacancy/, etc.), and
     (b) groups of ≥3 sibling anchors sharing the same (parent tag,
         parent class) fingerprint — a real job-listing grid/table
         renders every card through the same template, which is a much
         stronger and more general signal than any fixed CSS class name
         could be, and a nav menu or footer never has this shape.
     Anchor text is run through a nav-word blocklist and a word-count
     sanity check before anything counts as a candidate. Every surviving
     candidate is then INDIVIDUALLY FETCHED and must clear a real-job-page
     confirmation gate (minimum text length + at least one strong
     job-page phrase like "job description"/"responsibilities"/"apply
     now") before it becomes a posting — a candidate link alone is never
     trusted. This confirmation fetch is the single biggest lever against
     letting junk in, and is deliberately not skipped to save requests.

Every surviving posting — from either method — still goes through the
EXACT SAME role/location/visa classification funnel Crawl I uses
(classifier.py: keyword_classify_role → ai_classify_roles,
_keyword_classify_location_detail → ai_classify_locations,
detect_visa_sponsorship) before being written. Nothing here bypasses that
filter; a JobPosting hit or a confirmed heuristic hit is a CANDIDATE for
the jobs table, never an automatic write. This is what "as perfect as
possible... does not let junk in" means in practice: three independent
gates (structural/confirmation, role, location) all have to agree.

Every row this writes to `jobs` is tagged source_pipeline='crawl_ii' (via
supabase_handler.add_jobs_batch's source_pipeline param) so it can be
bulk-identified and deleted independently of Crawl I's rows if the
heuristic scraper turns out to have quality problems on some class of
site, without touching a single Crawl I row.

CLI modes (mirrors crawl_i.py; see .github/workflows/crawl.yml):
  python crawl_ii.py --shard 0 --total-shards 10   This shard's 1/10 slice of archive_ii.
  python crawl_ii.py --finalize                     Cleanup only (mark/delete stale
                                                      crawl_ii jobs) — run once, after every
                                                      shard has finished (gate with `needs:`).
"""

import argparse
import asyncio
import concurrent.futures
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import aiohttp
from dotenv import load_dotenv
from selectolax.lexbor import LexborHTMLParser

load_dotenv()
_MAIN_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_MAIN_DIR)  # repo root — node.py lives here
sys.path.insert(0, _ROOT)
sys.path.insert(0, _MAIN_DIR)

import node  # noqa: E402 — reuse _fetch_page, USER_AGENT, new_connector, new_parse_pool
# (2026-08: this file used to do all its HTML parsing inline on the event
# loop with no pool at all — see extract_postings_from_page's docstring —
# it now shares node.py's new_parse_pool() ThreadPoolExecutor pattern.)
from classifier import (  # noqa: E402
    keyword_classify_role, ai_classify_roles,
    _keyword_classify_location_detail, ai_classify_locations,
    detect_visa_sponsorship,
    PRIORITY_GLOBAL, PRIORITY_AFRICA, PRIORITY_UNSURE,
)
from supabase_handler import (  # noqa: E402
    add_jobs_batch, cleanup_stale_jobs, get_archive_ii_pages, SupabaseFetchError,
    get_existing_urls, touch_seen_jobs_raw, touch_archive_ii_last_seen,
    log_egress_summary,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crawl_ii")

SOURCE_PIPELINE = "crawl_ii"
DEFAULT_ATS_LABEL = "in_house"  # jobs.ats value for every Crawl II row — free-text column, no CHECK

CRAWL_CONCURRENCY = int(os.environ.get("CRAWL_II_CONCURRENCY", "60"))
TIME_BUDGET_MINUTES = int(os.environ.get("CRAWL_II_TIME_BUDGET_MINUTES", "300"))
BATCH_SIZE = int(os.environ.get("CRAWL_II_BATCH_SIZE", "300"))  # pages per micro-batch before pushing
MAX_HEURISTIC_CANDIDATES_PER_PAGE = 25  # bounds worst-case detail-page fetches for one company


# ── Sharding (same deterministic hash approach as crawl_i.py's _shard_of) ──

def _shard_of(website_url: str, total_shards: int) -> int:
    h = hashlib.md5(website_url.encode()).hexdigest()
    return int(h, 16) % total_shards


def load_pages(shard: int = 0, total_shards: int = 1) -> list[dict]:
    """Load {career_page_url, website_url} pairs from archive_ii, sharded
    the same way crawl_i.py shards archive_i — a stable hash rather than a
    running index so every shard gets an even, source-agnostic slice.

    2026-09: sharding now happens server-side (get_archive_ii_pages() passes
    shard_index/shard_count straight through to the archive_ii_shard
    Postgres RPC) so each shard's Supabase fetch only ever downloads its
    own ~1/total_shards slice of archive_ii, instead of every shard
    downloading the full table and discarding the rest client-side.
    get_archive_ii_pages() falls back to the old full-table-then-filter
    behavior (using this same _shard_of() hash) if the RPC is ever
    unavailable."""
    if total_shards > 1:
        pages = get_archive_ii_pages(shard_index=shard, shard_count=total_shards)
    else:
        pages = get_archive_ii_pages()
    if not pages:
        log.warning("No pages found in Supabase archive_ii!")
        return []

    if total_shards > 1:
        log.info(f"Shard {shard}/{total_shards}: {len(pages)} career pages assigned")

    return pages


# ── JSON-LD extraction ──────────────────────────────────────────────────

_JSONLD_SCRIPT_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.I | re.S)
_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str, max_len: int = 4000) -> str:
    if not text:
        return ""
    text = _TAG_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len]


def _iter_jsonld_objects(html: str):
    """Yields every dict-shaped JSON-LD object on the page, flattening
    both top-level arrays and @graph wrappers — real-world JSON-LD shows
    up in all three shapes depending on the CMS/plugin that emitted it."""
    for m in _JSONLD_SCRIPT_RE.finditer(html):
        raw = m.group(1).strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError, RecursionError):
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            graph = item.get("@graph")
            if isinstance(graph, list):
                for g in graph:
                    if isinstance(g, dict):
                        yield g
            else:
                yield item


def _is_jobposting(item: dict) -> bool:
    t = item.get("@type")
    types = t if isinstance(t, list) else [t]
    return any(isinstance(x, str) and x.lower() == "jobposting" for x in types)


def _is_expired(valid_through) -> bool:
    """A JobPosting still present on the page but past its own
    validThrough date is a stale listing the site just hasn't taken down
    yet — real evidence it shouldn't be trusted as a currently-open role."""
    if not valid_through or not isinstance(valid_through, str):
        return False
    try:
        d = datetime.fromisoformat(valid_through.replace("Z", "+00:00"))
    except ValueError:
        return False
    now = datetime.now(d.tzinfo) if d.tzinfo else datetime.now()
    return d < now


def _jsonld_location(item: dict) -> str:
    loc = item.get("jobLocation")
    if isinstance(loc, list):
        loc = loc[0] if loc else None
    if isinstance(loc, dict):
        addr = loc.get("address")
        if isinstance(addr, dict):
            parts = [addr.get(k) for k in ("addressLocality", "addressRegion", "addressCountry")]
            parts = [p for p in parts if p and isinstance(p, str)]
            if parts:
                return ", ".join(parts)
    # Remote-style postings often carry this instead of (or alongside) jobLocation
    if item.get("jobLocationType") == "TELECOMMUTE":
        req = item.get("applicantLocationRequirements")
        names = []
        if isinstance(req, dict):
            names = [req.get("name")] if req.get("name") else []
        elif isinstance(req, list):
            names = [r.get("name") for r in req if isinstance(r, dict) and r.get("name")]
        return f"Remote ({', '.join(names)})" if names else "Remote"
    return ""


def _extract_jsonld_jobs(html: str, page_url: str, company: str) -> list[dict]:
    postings = []
    seen_urls = set()
    for item in _iter_jsonld_objects(html):
        if not _is_jobposting(item):
            continue
        title = str(item.get("title") or "").strip()
        if not title or _is_expired(item.get("validThrough")):
            continue
        url = item.get("url") or item.get("directApply") or page_url
        if isinstance(url, dict):
            url = url.get("url", page_url)
        try:
            url = urljoin(page_url, str(url))
        except ValueError:
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        org = item.get("hiringOrganization")
        org_name = org.get("name") if isinstance(org, dict) else None
        postings.append({
            "title": title[:500],
            "url": url,
            "location": _jsonld_location(item),
            "description": _strip_html(item.get("description", "")),
            "company": org_name or company,
            "source_ats": DEFAULT_ATS_LABEL,
            "clearance": "",
        })
    return postings


# ── Heuristic repeated-card extraction (used only when JSON-LD found nothing) ──

_JOB_HREF_RE = re.compile(
    r"/(?:job|jobs|career|careers|position|positions|opening|openings|"
    r"vacanc(?:y|ies)|opportunit(?:y|ies)|role|roles)/[\w\-./%]+", re.I)

_NAV_TEXT_BLOCKLIST_RE = re.compile(
    r"^(home|about( us)?|contact( us)?|blog|news|press|privacy( policy)?|terms"
    r"( (of|and) (service|conditions|use))?|cookies?( policy)?|"
    r"sign[\s-]?in|log[\s-]?in|sign[\s-]?up|register|faq|help|support|our team|"
    r"careers?|open positions?|current openings?|view all( jobs)?|see all|"
    r"learn more|read more|apply( now)?|search|filter|next|previous|"
    r"load more|back to (search|jobs|careers)|share this job)$", re.I)


def _find_heuristic_candidates(html: str, page_url: str) -> list[dict]:
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return []

    candidates: dict[str, dict] = {}
    fingerprint_groups: dict[tuple, list] = {}

    for a in tree.css("a[href]"):
        href = a.attributes.get("href") or ""
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        text = a.text(deep=True, separator=" ").strip()
        text = re.sub(r"\s+", " ", text)
        word_count = len(text.split())
        # A real job title reads like a short phrase, not a single nav word
        # and not a whole sentence/paragraph — 2-12 words in practice.
        if not text or word_count < 2 or word_count > 12:
            continue
        if _NAV_TEXT_BLOCKLIST_RE.match(text.strip()):
            continue

        # 2026-09: a real crash killed a whole crawl_ii.py shard —
        # urljoin/urlparse can raise ValueError on a malformed href (seen
        # live: an href attribute value of 'sjm code="11" ', almost
        # certainly a parser mis-grab from broken/non-HTML markup on some
        # page, not a real URL at all). One bad <a> tag on one page must
        # not take down the whole batch — skip just that link.
        try:
            full_url = urljoin(page_url, href)
            parsed = urlparse(full_url)
        except ValueError:
            continue
        if parsed.scheme not in ("http", "https"):
            continue

        if _JOB_HREF_RE.search(href):
            candidates.setdefault(full_url, {"title": text[:300], "url": full_url})
            continue

        parent = a.parent
        if parent is None:
            continue
        fp = (parent.tag, parent.attributes.get("class") or "")
        fingerprint_groups.setdefault(fp, []).append((full_url, text))

    for (tag, cls), items in fingerprint_groups.items():
        # A shared class is a strong repetition signal (3+ siblings); with
        # no class to key on at all, require a bigger group (5+) since
        # tag-only repetition (e.g. every <li> on the page) is much weaker.
        min_group = 3 if cls else 5
        seen_in_group = set()
        unique_items = []
        for url, text in items:
            if url in seen_in_group:
                continue
            seen_in_group.add(url)
            unique_items.append((url, text))
        if len(unique_items) < min_group:
            continue
        for url, text in unique_items:
            candidates.setdefault(url, {"title": text[:300], "url": url})

    return list(candidates.values())


_STRONG_JOB_PAGE_PHRASES = [
    "apply now", "apply today", "apply for this", "apply for this job",
    "apply for this position", "job description", "responsibilities",
    "qualifications", "requirements", "what you'll do", "what you will do",
    "about the role", "about this role", "employment type", "job type",
    "submit your application", "submit an application", "job summary",
    "key responsibilities", "who you are", "what we're looking for",
]
_MIN_JOB_DETAIL_TEXT_CHARS = 200


def _confirm_and_build_posting(detail_html: str, candidate: dict, company: str) -> dict | None:
    """A candidate link alone is never trusted — this is the gate that
    keeps a heuristic hit from becoming a written job. Requires BOTH a
    real amount of body text (rules out a soft-404/stub/redirect-to-
    homepage-in-disguise, same failure mode node.py's career-page quality
    gate exists for) AND at least one phrase that specifically reads like
    a job posting, not just any content page of similar length."""
    text = _strip_html(detail_html, max_len=20000)
    if len(text) < _MIN_JOB_DETAIL_TEXT_CHARS:
        return None
    text_lower = text.lower()
    if not any(p in text_lower for p in _STRONG_JOB_PAGE_PHRASES):
        return None
    return {
        "title": candidate["title"],
        "url": candidate["url"],
        # No reliable structured location signal from a heuristic hit —
        # left blank deliberately so classifier.py's own "blank → unsure,
        # let the AI stage look at it" path handles it, exactly like an
        # ATS board with a blank location field would.
        "location": "",
        "description": text[:4000],
        "company": company,
        "source_ats": DEFAULT_ATS_LABEL,
        "clearance": "",
    }


# ── 2026-09: best-effort /apply page augmentation ──────────────────────
# Some ATS platforms structure a job's own URL as {base}/{company}/
# {opaque-id}[/...] — e.g. Ashby (jobs.ashbyhq.com/{company}/{uuid}) and
# Gem-hosted boards (jobs.gem.com/{company}/{opaque-token}) — and
# additionally serve a SEPARATE /apply sibling page under that same
# opaque id, carrying the actual application FORM fields (visa
# sponsorship, work-authorization, clearance, EEO questions, etc.) —
# wording that's sometimes absent from the job posting/description page
# itself but present only on the form a candidate would actually see.
# Deliberately conservative: only guessed when the URL's last path
# segment looks like an opaque id (long alnum/-/_ token, not a readable
# word/slug), never on a URL that's already an /apply page, and skipped
# entirely when the posting's own description already carries visa/
# clearance language (no point spending an extra request confirming what
# is already known). Best-effort only — any failure here just means the
# posting is returned as-is, exactly like before this existed.
_ID_LIKE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_-]{16,}$")
_HAS_VISA_OR_CLEARANCE_SIGNAL_RE = re.compile(
    r"visa|sponsor|clearance|work\s*authoriz|eligib(le|ility)\s*to\s*work", re.I)


def _guess_apply_url(job_url: str) -> str | None:
    try:
        parsed = urlparse(job_url)
    except Exception:
        return None
    path = parsed.path.rstrip("/")
    if not path or path.endswith("/apply"):
        return None
    segments = [s for s in path.split("/") if s]
    if len(segments) < 2 or not _ID_LIKE_SEGMENT_RE.match(segments[-1]):
        return None
    return f"{parsed.scheme}://{parsed.netloc}{path}/apply"


async def _augment_with_apply_page(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                                    stats: dict, job: dict) -> None:
    """Mutates job["description"] in place if the guessed /apply page is
    reachable and actually carries visa/clearance-relevant text — never
    raises, never removes/blocks the posting either way."""
    if _HAS_VISA_OR_CLEARANCE_SIGNAL_RE.search(job.get("description") or ""):
        return
    apply_url = _guess_apply_url(job.get("url", ""))
    if not apply_url:
        return
    try:
        async with sem:
            fetched = await node._fetch_page(session, apply_url, stats)
    except Exception:
        return
    if not fetched:
        return
    _, apply_html = fetched
    apply_text = _strip_html(apply_html, max_len=4000)
    if _HAS_VISA_OR_CLEARANCE_SIGNAL_RE.search(apply_text):
        job["description"] = f"{job.get('description') or ''}\n\n{apply_text}"
        stats["apply_page_augmented"] += 1


def _company_name_from_domain(website_url: str) -> str:
    host = urlparse(website_url).netloc or website_url
    host = re.sub(r"^www\.", "", host)
    return host.split(":")[0] or website_url


# ── Per-page extraction ─────────────────────────────────────────────────

async def extract_postings_from_page(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                                      page: dict, stats: dict,
                                      parse_pool: concurrent.futures.Executor) -> list[dict]:
    """page: {"career_page_url","website_url"}. Returns candidate job dicts
    — NOT yet role/location filtered, see _run_pipeline_ii for that.

    2026-08 — two real bottlenecks fixed here:

    1. Every CPU-bound parse call (_extract_jsonld_jobs, LexborHTMLParser-
       based _find_heuristic_candidates, _confirm_and_build_posting) used
       to run INLINE on the event loop — worse than node.py's old
       ProcessPoolExecutor(1 worker) bug, since it wasn't even offloaded
       to a second worker: it blocked the entire event loop, including
       every other page's in-flight fetch, for the full duration of each
       parse. Now offloaded via loop.run_in_executor(parse_pool, ...),
       same ThreadPoolExecutor pattern as node.py's crawl_one/PARSE_WORKERS.

    2. The up-to-MAX_HEURISTIC_CANDIDATES_PER_PAGE (25) detail-page
       fetches were a sequential `for cand in candidates: await ...` loop
       — one at a time, not concurrent, on a page that could have up to
       25 candidates. Now fetched concurrently via asyncio.gather (same
       per-fetch semaphore gating as before, just no longer serialized)."""
    loop = asyncio.get_running_loop()
    async with sem:
        fetched = await node._fetch_page(session, page["career_page_url"], stats)
    if not fetched:
        stats["page_unreachable"] += 1
        return []
    final_url, html = fetched
    company = _company_name_from_domain(page["website_url"])

    jsonld_jobs = await loop.run_in_executor(parse_pool, _extract_jsonld_jobs, html, final_url, company)
    if jsonld_jobs:
        stats["jsonld_pages"] += 1
        stats["jsonld_postings"] += len(jsonld_jobs)
        await asyncio.gather(*(_augment_with_apply_page(session, sem, stats, j) for j in jsonld_jobs))
        return jsonld_jobs

    candidates = await loop.run_in_executor(parse_pool, _find_heuristic_candidates, html, final_url)
    if not candidates:
        stats["no_postings_found"] += 1
        return []
    stats["heuristic_pages"] += 1
    candidates = candidates[:MAX_HEURISTIC_CANDIDATES_PER_PAGE]

    async def _fetch_and_confirm(cand: dict) -> dict | None:
        async with sem:
            detail = await node._fetch_page(session, cand["url"], stats)
        if not detail:
            return None
        _, detail_html = detail
        return await loop.run_in_executor(parse_pool, _confirm_and_build_posting, detail_html, cand, company)

    results = await asyncio.gather(*(_fetch_and_confirm(c) for c in candidates))
    confirmed = [r for r in results if r]
    stats["heuristic_postings"] += len(confirmed)
    await asyncio.gather(*(_augment_with_apply_page(session, sem, stats, j) for j in confirmed))
    return confirmed


# ── Classification + push (mirrors crawl_i.py's role/location/visa funnel) ──

def _filter_roles(jobs: list[dict]) -> list[dict]:
    included, unsure = [], []
    for job in jobs:
        result = keyword_classify_role(job["title"])
        if result == "include":
            included.append(job)
        elif result == "unsure":
            unsure.append(job)
    if unsure:
        ai_results = ai_classify_roles([j["title"] for j in unsure])
        for job in unsure:
            if ai_results.get(job["title"], False):
                included.append(job)
    return included


def _filter_locations(jobs: list[dict]) -> tuple[list[dict], list[str]]:
    matched, confidences, unsure_jobs = [], [], []
    for job in jobs:
        result, priority = _keyword_classify_location_detail(job)
        if result == "match":
            job["clearance"] = "regex"
            job["location_priority"] = priority
            matched.append(job)
            confidences.append("match")
        elif result == "unsure":
            unsure_jobs.append(job)

    if unsure_jobs:
        ai_results = ai_classify_locations(unsure_jobs)
        for job, (label, provider_name) in zip(unsure_jobs, ai_results):
            # 2026-09: use the ACTUAL provider that classified this job
            # (now returned directly by ai_classify_locations — see its
            # docstring) instead of the literal string "ai", which is what
            # this used to hardcode regardless of whether keyword/regex,
            # Gemini, or OpenAI (or now NVIDIA) made the call. Matches
            # crawl_i.py's filter_locations, which already did this right.
            clearance = provider_name or "ai"
            if label == "match_global":
                job["clearance"] = clearance
                job["location_priority"] = PRIORITY_GLOBAL
                matched.append(job)
                confidences.append("match")
            elif label == "match_africa":
                job["clearance"] = clearance
                job["location_priority"] = PRIORITY_AFRICA
                matched.append(job)
                confidences.append("match")
            elif label == "uncertain":
                job["clearance"] = clearance
                job["location_priority"] = PRIORITY_UNSURE
                matched.append(job)
                confidences.append("uncertain")
            # "no_match" → drop

    return matched, confidences


# ── Shared batch driver ─────────────────────────────────────────────────

async def crawl_batch_ii(pages: list[dict], session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                          stats: dict, crawl_start: float, time_budget_seconds: float,
                          time_budget_minutes: int, parse_pool: concurrent.futures.Executor,
                          batch_size: int = BATCH_SIZE) -> tuple[int, int, bool]:
    """Crawls archive_ii pages, then classifies and writes everything ONCE
    at the end. Returns (pages_done, jobs_added, time_budget_hit).

    2026-09 restructure, at explicit user instruction: previously this
    fetched+extracted a sub-batch of pages, immediately ran that
    sub-batch through role/location classification, and pushed straight
    to Supabase — repeated per sub-batch, so a shard's log interleaved
    fetch progress, AI-provider HTTP noise, and "Added N jobs to Supabase"
    lines from dozens of separate small writes throughout the run, and a
    crash mid-run left Supabase in a half-written state for that shard.
    Now: fetch+extract every page first (still in sub-batches internally,
    purely to keep memory/concurrency bounded — see the loop below), with
    ONLY plain crawl-progress logging during that stage; THEN one
    dedup pass, one role-classification pass, one location-classification
    pass, and ONE Supabase write for the whole shard's survivors — each
    stage gets its own clearly-labeled log block instead of everything
    interleaved. Tradeoff worth knowing: a crash or timeout DURING the
    fetch stage still writes nothing for this shard (nothing to write yet
    — see the time-budget-hit path below, which still classifies+writes
    whatever was fetched before stopping); a crash AFTER fetching but
    during classification loses that shard's writes for this run, where
    the old per-sub-batch design would have kept whatever had already
    been pushed. If that tradeoff turns out to bite in practice, the
    fix is a periodic flush (e.g. every 2000 pages) rather than reverting
    to per-300-page pushes — ask and it can be added.
    """
    all_candidate_jobs: list[dict] = []
    all_pages_with_roles: set[str] = set()
    time_budget_hit = False
    i = 0

    log.info(f"── Crawling entries ({len(pages)} pages) ──")
    for i in range(0, len(pages), batch_size):
        if time.monotonic() - crawl_start >= time_budget_seconds:
            time_budget_hit = True
            log.warning(f"  time budget ({time_budget_minutes}min) reached at {i}/{len(pages)} "
                        f"pages — stopping the crawl here; everything fetched so far still "
                        f"goes through dedup/classification/write below.")
            break
        batch = pages[i:i + batch_size]
        results = await asyncio.gather(
            *(extract_postings_from_page(session, sem, p, stats, parse_pool) for p in batch))
        batch_candidates = [job for page_jobs in results for job in page_jobs]
        all_candidate_jobs.extend(batch_candidates)

        # 2026-09: repurpose archive_ii.last_seen to mean "last time this
        # page had ANY role at all" — these are the RAW batch_candidates,
        # before role/location filtering below, so any posting counts,
        # not just CSM/AM ones.
        all_pages_with_roles |= {p["website_url"] for p, page_jobs in zip(batch, results) if page_jobs}

        done = min(i + batch_size, len(pages))
        elapsed = time.monotonic() - crawl_start
        rate = stats["requests_attempted"] / elapsed if elapsed > 0 else 0
        log.info(f"  {done}/{len(pages)} pages — {rate:.1f} req/sec — {elapsed:.0f}s elapsed — "
                 f"{len(batch_candidates)} candidates this batch ({len(all_candidate_jobs)} total)")

    pages_done = min(len(pages), i + batch_size) if pages else 0

    if all_pages_with_roles:
        touch_archive_ii_last_seen(all_pages_with_roles)

    if not all_candidate_jobs:
        log.info("No candidate postings found on any page — nothing to classify or write.")
        return pages_done, 0, time_budget_hit

    log.info("── Deduplication ──")
    existing_urls = get_existing_urls()
    new_jobs, already_seen = [], []
    for job in all_candidate_jobs:
        url = job.get("url", "")
        if url and url in existing_urls:
            already_seen.append(job)
        else:
            new_jobs.append(job)
    if already_seen:
        touch_seen_jobs_raw(already_seen)
    log.info(f"  {len(all_candidate_jobs)} candidates → {len(already_seen)} already known "
             f"(skipped, last_seen refreshed only), {len(new_jobs)} new — only new ones go to AI")

    if not new_jobs:
        log.info("No new candidates to classify.")
        return pages_done, 0, time_budget_hit

    log.info("── Role classification ──")
    role_matched = _filter_roles(new_jobs)
    log.info(f"  {len(new_jobs)} candidates → {len(role_matched)} CSM/AM roles")
    if not role_matched:
        return pages_done, 0, time_budget_hit

    log.info("── Location classification ──")
    global_jobs, confidences = _filter_locations(role_matched)
    log.info(f"  {len(role_matched)} roles → {len(global_jobs)} global/Africa-eligible")
    if not global_jobs:
        return pages_done, 0, time_budget_hit

    for job in global_jobs:
        job["visa_sponsorship"] = detect_visa_sponsorship(job)

    log.info("── Writing to Supabase ──")
    added = add_jobs_batch(global_jobs, confidences, source_pipeline=SOURCE_PIPELINE,
                            existing_urls=existing_urls)
    log.info(f"  {added} new jobs written")

    return pages_done, added, time_budget_hit


def new_connector() -> aiohttp.TCPConnector:
    return node.new_connector()


# ── Finalize ─────────────────────────────────────────────────────────────

def run_finalize() -> None:
    """Cleanup pass for Crawl II's own rows only (source_pipeline='crawl_ii')
    — call ONCE, after every Crawl II shard has finished.

    Deletion policy (2026-09, at explicit user instruction: both Crawl I
    and Crawl II must delete jobs past 30 days): mark-inactive at 30 days,
    hard-delete at 31 — same as Crawl I's window (see crawl_i.py's
    run_finalize). Previously used a more conservative 45-day hard-delete
    window (2026-08, my own default at the time, chosen because Crawl II
    was a brand-new heuristic pipeline with no production track record —
    see git history for that original reasoning) — superseded by the
    explicit 30-day instruction rather than left as a standing exception."""
    log.info("=" * 60)
    log.info("CRAWL II — finalize (cleanup stale jobs)")
    log.info("=" * 60)
    summary = cleanup_stale_jobs(inactive_days=30, delete_days=31, source_pipeline=SOURCE_PIPELINE)
    log.info(f"Crawl II finalize summary: inactive cutoff {summary['inactive_cutoff']} "
             f"(ok={summary['mark_inactive_ok']}), delete cutoff {summary['delete_cutoff']} "
             f"(ok={summary['delete_ok']})")


# ── CLI ──────────────────────────────────────────────────────────────────

async def _run_shard(shard: int, total_shards: int) -> None:
    log.info("=" * 60)
    log.info(f"CRAWL II — starting (shard {shard}/{total_shards})")
    log.info("=" * 60)

    log.info("── Getting entries ──")
    try:
        pages = load_pages(shard=shard, total_shards=total_shards)
    except SupabaseFetchError as e:
        log.error(f"Failed to load archive_ii pages from Supabase after retries — aborting shard: {e}")
        sys.exit(1)
    log.info(f"  {len(pages)} archive_ii pages assigned to this shard")

    if not pages:
        log.warning(f"Shard {shard}/{total_shards}: no pages assigned, nothing to do.")
        return

    stats = {
        "requests_attempted": 0, "fetched_ok": 0, "http_error": 0, "status_404": 0,
        "non_html": 0, "timeout": 0, "unreachable": 0,
        "page_unreachable": 0, "jsonld_pages": 0, "jsonld_postings": 0,
        "heuristic_pages": 0, "heuristic_postings": 0, "no_postings_found": 0,
    }
    sem = asyncio.Semaphore(CRAWL_CONCURRENCY)
    connector = new_connector()
    crawl_start = time.monotonic()
    time_budget_seconds = TIME_BUDGET_MINUTES * 60
    # Shared ThreadPoolExecutor for every CPU-bound parse call this shard
    # makes (see extract_postings_from_page's docstring) — same
    # new_parse_pool() node.py's own crawl engine uses.
    parse_pool = node.new_parse_pool()

    try:
        async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
            done, added, time_budget_hit = await crawl_batch_ii(
                pages, session, sem, stats, crawl_start, time_budget_seconds,
                TIME_BUDGET_MINUTES, parse_pool)
    finally:
        parse_pool.shutdown(wait=False)

    status = "STOPPED EARLY (time budget)" if time_budget_hit else "complete"
    log.info("── Summary ──")
    log.info(f"  shard {shard}/{total_shards} {status}: {done}/{len(pages)} pages, "
             f"{added} new jobs written")
    log.info(f"  JSON-LD: {stats['jsonld_pages']} pages, {stats['jsonld_postings']} postings found")
    log.info(f"  Heuristic: {stats['heuristic_pages']} pages, {stats['heuristic_postings']} "
             f"postings confirmed")
    log.info(f"  Unreachable/no-signal: {stats['page_unreachable']} pages unreachable, "
             f"{stats['no_postings_found']} pages with no postings found")

    log_egress_summary(label=f"crawl_ii shard {shard}/{total_shards}")


def main():
    parser = argparse.ArgumentParser(description="Crawl II — heuristic archive_ii scraper")
    parser.add_argument("--shard", type=int, default=0,
                         help="This shard's index (0-based), for GitHub Actions matrix parallelism")
    parser.add_argument("--total-shards", type=int, default=1,
                         help="Total number of shards; each processes ~1/N of archive_ii")
    parser.add_argument("--finalize", action="store_true",
                         help="Only run cleanup (mark/delete stale crawl_ii jobs) — call once "
                              "after all shards finish")
    args = parser.parse_args()

    if args.finalize:
        run_finalize()
        return

    asyncio.run(_run_shard(args.shard, args.total_shards))


if __name__ == "__main__":
    main()
