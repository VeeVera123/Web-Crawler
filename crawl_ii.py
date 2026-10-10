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

  3. (2026-09) If BOTH of the above find nothing, the page is checked for
     an "Explore Roles"/"View All Jobs"-style outbound link — the sign of
     a landing/stub page whose real listings sit one click away, possibly
     on a different domain — via node._extract_job_listing_link_candidates
     (the exact same detector/vocabulary node.py's own crawl uses). The
     single best-scoring link found is followed and re-run through
     methods 1 and 2 above. See extract_postings_from_page and
     MAX_CAREER_LINK_FOLLOW for the bounds on this.

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
import html as html_lib
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
import page_extract as PE  # noqa: E402 — pure HTML->job helpers (main text, JD scoring, state JSON, frames, feeds)
# (2026-08: this file used to do all its HTML parsing inline on the event
# loop with no pool at all — see extract_postings_from_page's docstring —
# it now shares node.py's new_parse_pool() ThreadPoolExecutor pattern.)
import config  # noqa: E402
import location_diagnostics  # noqa: E402
import excluded_cache  # noqa: E402
from classifier import (  # noqa: E402
    keyword_classify_role, ai_classify_roles,
    _keyword_classify_location_detail, ai_classify_locations,
    detect_visa_sponsorship, PLACEHOLDER_LOC_RE,
    classify_role_category,
    classify_rank4, RANK4_ELIGIBLE_ATS, prefilter_jobs_by_location,
    PRIORITY_GLOBAL, PRIORITY_AFRICA,
    PRIORITY_UNSURE_BLANK, PRIORITY_UNSURE_SILENT,
)
# 2026-09 ROUND 2 (explicit user instruction: "Make sure that all jobs have
# their application questions fetched. All of them. ... everything under
# our control should have application questions in it. Everything!"): this
# file previously had NO application-question enrichment step at all —
# unlike crawl_i.py, it never imported or called enrich_application_
# questions, so none of its jobs ever got their ATS screening/work-auth
# questions folded into description_snippet before location classification.
# ats_scrapers.py's generic _fetch_wild_questions fallback (used for any
# platform without a dedicated fetcher — which covers most of this file's
# unsupported/"wild" company career sites) needed no new code to support
# this; the only gap was that nothing here ever called it.
import ats_scrapers  # noqa: E402
from ats_scrapers import enrich_application_questions_async  # noqa: E402
from supabase_handler import (  # noqa: E402
    add_jobs_batch, cleanup_stale_jobs, get_archive_ii_pages, SupabaseFetchError,
    get_existing_urls, get_known_jobs_meta, touch_seen_jobs_raw, mark_jobs_vetoed,
    touch_archive_ii_last_seen,
    log_egress_summary, bump_scan_report, finish_scan_report_for_pipeline,
)
from supabase_handler import CLASSIFIER_VERSION  # noqa: E402
from job_url import UrlSet, split_known_jobs  # noqa: E402
import revalidate  # noqa: E402
# 2026-09 (second pass): Notion sync moved OUT of this file entirely, into
# prefix_supabase.py (before shards)/postfix_notion.py (after shards) —
# see notion_sync.py's module docstring for why. This file is back to
# being a pure Supabase writer.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crawl_ii")

SOURCE_PIPELINE = "crawl_ii"

# 2026-09 (explicit user request — see excluded_cache.py's module docstring
# and crawl_i.py's matching constants for the full design). "Excluded 2" is
# this crawl's own separate file, per explicit user instruction to keep
# Crawl I/II/III's caches separate rather than merged into one.
EXCLUDED_CACHE_PATH = "excluded_2.json"
EXCLUDED_CACHE_SHARD_GLOB = "excluded_2_shard_*.json"


def _excluded_cache_shard_path(shard: int) -> str:
    return f"excluded_2_shard_{shard}.json"


DEFAULT_ATS_LABEL = "in_house"  # jobs.ats value for every Crawl II row — free-text column, no CHECK

# 2026-10 (explicit user request): how many confirmed application-form
# boilerplate fields (name/email/phone/resume/cover letter/etc. — see
# ats_scrapers.py's _count_confirmed_application_form_fields) a Crawl II
# job needs before its captured form counts as genuinely real rather than
# an unfetched/blank page — see _try_rank4 below for the full policy.
# Picked the middle of the "maybe three or even 5" range the user floated:
# high enough that a stray 1-2 field contact form (e.g. just "email" and
# "message") can't pass as a real application form, low enough that a
# normal company careers form (which routinely has name+email+phone+
# resume+cover letter, i.e. 5 boilerplate fields on its own) clears it
# easily off just the standard fields alone.
RANK4_CONFIRMED_FORM_FIELD_THRESHOLD = 3

CRAWL_CONCURRENCY = int(os.environ.get("CRAWL_II_CONCURRENCY", "300"))
# 2026-09 (explicit user report: crawl_ii "took a shit ton of time...
# increase concurrency"; raised 60 -> 150, then explicit follow-up "bump
# concurrency to 300... node.py safely did 400, it can do 300 here"):
# every target here is an independent company's own career site (not a
# shared ATS platform), so unlike crawl_i.py's per-platform host
# semaphores there's no single host whose concurrency needs protecting
# from this bump — node.new_connector() already sizes its aiohttp
# connector off node.py's own CRAWL_CONCURRENCY (400, giving a
# 550-connection pool), comfortably above this. See PARSE_POOL_WORKERS
# just below for the other half of this fix — raising fetch concurrency
# alone without also widening the CPU-bound parse pool just moves the
# bottleneck instead of removing it; scaled proportionally with this bump
# (32 -> 64) for the same reason.
PARSE_POOL_WORKERS = int(os.environ.get("CRAWL_II_PARSE_WORKERS", "64"))
TIME_BUDGET_MINUTES = int(os.environ.get("CRAWL_II_TIME_BUDGET_MINUTES", "300"))
BATCH_SIZE = int(os.environ.get("CRAWL_II_BATCH_SIZE", "300"))  # pages per micro-batch before pushing
MAX_HEURISTIC_CANDIDATES_PER_PAGE = 25  # bounds worst-case detail-page fetches for one company

# 2026-09: an archive_ii page that yields NOTHING via either JSON-LD or
# the heuristic repeated-card detector is exactly the shape of a landing/
# stub page whose real listings sit one click away behind a button
# ("Explore Roles", "View All Jobs", ...) — the SAME gap node.py's
# crawl_one fixed for its own tiers, now confirmed to affect a real,
# already-captured slice of the 120k+ archive_ii pages this file
# re-crawls every run. Reuses node.py's shared link-candidate detector
# and phrase list verbatim (node._extract_job_listing_link_candidates) —
# one vocabulary, not a second copy that could drift out of sync.
#
# Deliberately tighter than node.py's per-tier cap of 3: a followed page
# here can ITSELF trigger up to MAX_HEURISTIC_CANDIDATES_PER_PAGE (25)
# further detail-page fetches if it turns out to be a real heuristic-
# shaped listings page — so each follow attempt already carries a much
# bigger worst-case cost here than it does in node.py (where a followed
# page is only ever detected against URL_TO_SLUG + hiring-vocab text, no
# further fan-out). Capping at 1 (the single best-scoring candidate link)
# keeps that worst case bounded to +1 request on a genuine dead end, and
# +up to 26 on a genuine find — a real cost increase across 120k+ pages,
# but a bounded and self-limiting one (a page with nothing worth
# following costs exactly one extra parse, no extra network request).
# Overridable via env var without a redeploy if a shard run shows the
# added cost needs tuning against TIME_BUDGET_MINUTES/CRAWL_CONCURRENCY.
MAX_CAREER_LINK_FOLLOW = int(os.environ.get("CRAWL_II_MAX_LINK_FOLLOW", "1"))

# 2026-10: same-size hash shards of the SAME registry showed 4,138 to 13,431
# unreachable pages per shard in one run (13%-44%), while a sample fetched from
# a quiet network reached 97% of the registry — so most "unreachable" pages were
# runner-load failures (DNS/timeouts at CRAWL_CONCURRENCY=400), not dead sites.
# Pages that failed with a transient error get one more attempt after the main
# pass, at this much lower concurrency.
RETRY_CONCURRENCY = int(os.environ.get("CRAWL_II_RETRY_CONCURRENCY", "100"))

# 2026-09: archive_ii previously only ever read ONE listing page per
# company career URL. A landing/stub page with no listings at all was
# already handled (MAX_CAREER_LINK_FOLLOW above), but a genuine listings
# page that itself spans multiple pages ("Page 1 of 5", a "Next"/"Load
# more" link) was NOT — everything past page 1 was silently missed. This
# caps how many listing pages of ONE company's job board this file will
# walk before giving up, so a company with an unusually deep or looping
# pagination scheme can't blow the time budget for the whole shard.
MAX_ARCHIVE_II_PAGES_PER_LISTING = int(os.environ.get("CRAWL_II_MAX_PAGES_PER_LISTING", "15"))

# Matches the exact "next page" phrasings _NAV_TEXT_BLOCKLIST_RE already
# treats as nav chrome, not a job title (see that regex above) — reused
# here as a POSITIVE signal instead: an <a> this project already knows
# isn't a job link, but whose text says "next"/"load more"/etc., is
# exactly the pagination control we want to follow.
_NEXT_PAGE_TEXT_RE = re.compile(
    r"^\s*(next(\s*page)?|older\s*(jobs|postings|roles)?|"
    r"more\s*(jobs|roles|postings)?|show\s*more|load\s*more|view\s*more|"
    r"»|>|›)\s*$",
    re.I,
)


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
# 2026-09: <script>/<style>/<noscript> TAG CONTENTS, not just the tags —
# _TAG_RE above only strips the tags themselves, so a page's minified JS/
# CSS body text used to leak straight into the "cleaned" text as noise
# (same bug ats_scrapers.py's _SCRIPT_STYLE_RE was added to fix there —
# ported here verbatim rather than reinvented). Confirmed to matter here
# specifically: a real GFL Environmental posting (careers.gflenv.com)
# whose page text included "Primary Location: Indianapolis, Indiana"
# further down the page got NO location captured at all — this noise,
# combined with the old 4000-char cap below, is exactly the kind of thing
# that pushes real content past a truncation point before either regex or
# the AI stage ever sees it.
_SCRIPT_STYLE_RE = re.compile(r"<(script|style|noscript)\b[^>]*>.*?</\1>", re.I | re.S)


def _coerce_text(value) -> str:
    """Real-world JSON-LD is often malformed relative to the schema.org
    spec a field documented as a plain string (JobPosting.description,
    most commonly) sometimes shows up instead as a nested object
    ({"@type": "TextObject", "value": "..."}, or similar) or a list
    (multiple language variants, or a stray array where a scalar was
    expected). Extract a usable string from any of these shapes instead
    of crashing — returns "" for anything with no recoverable text.
    2026-09: added after a real crash — a live page's JSON-LD had
    "description" as a dict, and _strip_html's old `if not text: return
    ""` check let it straight through (a non-empty dict is truthy) into
    a plain string-only regex .sub(), crashing the whole shard with
    `TypeError: expected string or bytes-like object, got 'dict'`."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("value", "@value", "text", "description", "name"):
            v = value.get(key)
            if isinstance(v, str) and v:
                return v
        return ""
    if isinstance(value, list):
        parts = [v for v in value if isinstance(v, str) and v]
        return " ".join(parts)
    return ""


def _strip_html(text, max_len: int = 30_000) -> str:
    # 2026-09: default raised 4000 -> 30_000 to match classifier.py's own
    # MAX_DESC_CHARS safety ceiling — that value is documented there as
    # "large enough that no real job description is ever actually cut off
    # by it"; there's no reason a heuristic archive_ii page's raw text
    # should be truncated far more aggressively than every other
    # extraction path in this project. Individual call sites can still
    # pass a smaller max_len for genuinely small snippets (e.g. the
    # apply-page augmentation below).
    text = _coerce_text(text)
    if not text:
        return ""
    text = _SCRIPT_STYLE_RE.sub(" ", text)
    text = _TAG_RE.sub(" ", text)
    # 2026-09 BUG FIX (explicit user report, real posting: viaquestinc.com's
    # Paycor/Gnewton-hosted "Day Program Coordinator" listing): this never
    # decoded HTML entities, so a raw "<b>Location:</b>&nbsp;" immediately
    # followed by a sibling <td>'s "Bowling Green, OH" collapsed, after tag
    # stripping, into the literal text "Location: &nbsp; Bowling Green, OH"
    # — the UNDECODED "&nbsp;" entity sat between the label and its value
    # as literal non-whitespace text, which _HEURISTIC_LOCATION_RE's
    # `\s*` (whitespace only) couldn't skip over, so the regex never
    # matched at all and this job's location stayed blank. Confirmed live
    # against the real page's fetched HTML. html.unescape() here fixes
    # every entity (&nbsp;, &amp;, &#038;, ...) project-wide, not just this
    # one page — any other heuristic-page label/value pair sitting either
    # side of an entity had the identical silent failure mode.
    text = html_lib.unescape(text)
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
    """2026-09: two real gaps fixed here, both losing genuine multi-country
    hiring evidence the classifier needs:
    (1) jobLocation as an array (real, common for Greenhouse/Ashby/Lever
        postings open across several offices) used to only read loc[0] —
        every other office/country in the array was silently discarded,
        so a job open in 5 countries reported just its first office's
        city, which then correctly (but wrongly, for THIS posting) fails
        the classifier's strict allowlist.
    (2) applicantLocationRequirements used to only be read when
        jobLocationType=="TELECOMMUTE" — but real postings often carry
        BOTH a physical jobLocation (HQ office) AND a separate
        applicantLocationRequirements list for remote-eligible countries
        (a hybrid "based at HQ, remote OK from these countries" posting).
        That combination used to only ever surface the single HQ office,
        hiding the actual remote-eligibility breadth.
    Both fixes just read MORE of what schema.org's own JobPosting fields
    already carry — no new signal invented, no extra request."""
    parts: list[str] = []
    loc = item.get("jobLocation")
    locs = loc if isinstance(loc, list) else ([loc] if loc else [])
    for l in locs:
        if not isinstance(l, dict):
            continue
        addr = l.get("address")
        if isinstance(addr, dict):
            place = [addr.get(k) for k in ("addressLocality", "addressRegion", "addressCountry")]
            place = [p for p in place if p and isinstance(p, str)]
            if place:
                joined = ", ".join(place)
                if joined not in parts:
                    parts.append(joined)

    def _flatten_names(val) -> list[str]:
        # 2026-09: `name` is supposed to be a single string per schema.org,
        # but real-world JSON-LD sometimes puts a list there instead (e.g.
        # {"name": ["United States", "Canada"]}) — that unhashable list
        # used to reach dict.fromkeys() below and crash the whole page's
        # extraction with `TypeError: unhashable type: 'list'`. Flatten one
        # level and keep only strings so a single malformed entry can't
        # take down an otherwise-good posting.
        if isinstance(val, str):
            return [val] if val else []
        if isinstance(val, list):
            out = []
            for v in val:
                if isinstance(v, str) and v:
                    out.append(v)
            return out
        return []

    req = item.get("applicantLocationRequirements")
    req_names: list[str] = []
    if isinstance(req, dict):
        req_names = _flatten_names(req.get("name"))
    elif isinstance(req, list):
        for r in req:
            if isinstance(r, dict):
                req_names.extend(_flatten_names(r.get("name")))
    req_names = list(dict.fromkeys(req_names))  # dedupe, keep order

    if req_names:
        parts.append(f"Remote ({', '.join(req_names)})")
    elif item.get("jobLocationType") == "TELECOMMUTE" and not parts:
        parts.append("Remote")

    return "; ".join(parts)


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
        # 2026-10: jobLocationType TELECOMMUTE is schema.org's structured "remote" flag; it was dropped, so a
        # JSON-LD job never carried a workplace_type (the hybrid/on-site hard check had nothing to read either).
        loc_type = item.get("jobLocationType")
        loc_types = loc_type if isinstance(loc_type, list) else [loc_type]
        workplace = "Remote" if any(isinstance(x, str) and x.upper() == "TELECOMMUTE" for x in loc_types) else ""
        postings.append({
            "title": title[:500],
            "url": url,
            "location": _jsonld_location(item),
            "workplace_type": workplace,
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


def _find_next_page_url(html: str, page_url: str) -> str | None:
    """Best-effort 'next page' detection for a paginated job-listing page.

    Two signals, in order of confidence:
      1. <link rel="next" href="..."> in <head> — the standards-based
         signal, when a site bothers to emit it.
      2. An <a> whose rel="next", OR whose visible text/aria-label matches
         a common 'next page' phrasing (_NEXT_PAGE_TEXT_RE — the same
         phrase list _NAV_TEXT_BLOCKLIST_RE already recognizes as non-job
         nav chrome, just used here as the positive signal it actually is).

    Deliberately NEVER constructs or guesses a URL (e.g. incrementing a
    ?page=N query param) — only a link that actually appears as a real
    href in this page's own HTML is ever returned, so a company using a
    URL scheme this project hasn't seen before can't cause a fabricated
    fetch. Returns an absolute URL, or None if no next-page signal found.
    """
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return None

    link_next = tree.css_first('link[rel="next"]')
    if link_next is not None:
        href = link_next.attributes.get("href")
        if href:
            try:
                return urljoin(page_url, href)
            except ValueError:
                pass

    for a in tree.css("a[href]"):
        href = a.attributes.get("href") or ""
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        rel = (a.attributes.get("rel") or "").lower().split()
        text = a.text(deep=True, separator=" ").strip()
        aria = (a.attributes.get("aria-label") or "").strip()
        if "next" in rel or _NEXT_PAGE_TEXT_RE.match(text) or _NEXT_PAGE_TEXT_RE.match(aria):
            try:
                resolved = urljoin(page_url, href)
                parsed = urlparse(resolved)
            except ValueError:
                continue
            if parsed.scheme in ("http", "https") and resolved != page_url:
                return resolved
    return None


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


# 2026-09: real, evidence-backed gap — a GFL Environmental posting
# (careers.gflenv.com/account-manager/...) whose page plainly showed
# "Primary Location: Indianapolis, Indiana" got NO location captured at
# all via the heuristic path, because that path never even tried to read
# one out of the page text (see the old comment this replaces: "No
# reliable structured location signal from a heuristic hit — left blank
# deliberately"). That was true for the general case (most heuristic
# pages genuinely have no clean location line) but not for pages that DO
# print one in plain text next to a recognizable label — exactly the
# thing regex is good at. Tried in order, first match wins; deliberately
# narrow (a real label word immediately before the value) to avoid
# grabbing an unrelated sentence that happens to contain a place name.
_HEURISTIC_LOCATION_RE = re.compile(
    r"(?:primary\s*location|job\s*location|work\s*location|location)\s*[:\-]\s*"
    r"([A-Z][^:\n]{1,80}?)"
    r"(?=\s+(?:Apply|Department|Job\s*Type|Employment|Requirements|Responsibilities|"
    r"Qualifications|About|Benefits|Salary|Schedule|Description|Overview|Summary|"
    # 2026-09 BUG FIX (explicit user report, real posting: viaquestinc.com's
    # Paycor/Gnewton "Day Program Coordinator" listing): the page's own
    # table layout puts "Remote Status: On-Site", "Job Id: 42921", and
    # "# of Openings: 1" immediately after the location value with no
    # period between them ("Location: Bowling Green, OH Remote Status:
    # On-Site Job Id: ..."), none of which this boundary list recognized —
    # so even after the &nbsp;-decoding fix above, the non-greedy capture
    # kept expanding past the real location looking for a stop word it
    # would never find, most often exceeding the 80-char cap and getting
    # discarded entirely (see _extract_heuristic_location's len<=80 guard).
    r"Remote\s*Status|Workplace\s*(?:Setting|Type)|Job\s*Id|#\s*of\s*Openings|"
    r"Who\s|What\s|We\s|Click|View|Full[- ]?Time|Part[- ]?Time|Posted|Date|Category)\b"
    r"|[.]\s|\n|$)",
    re.I,
)


# 2026-09 BUG FIX #2 (avidtr.com — see _JD_NARROWING_QUALIFIER_RE_CI's note
# above for the full story): the real posting's location tag was
# "Remote, California", sitting in plain text right under the title with
# NO "Location:"-style label in front of it at all — so
# _HEURISTIC_LOCATION_RE never matched it, location stayed blank, and the
# job fell through to _enrich_location_from_description instead (which
# then had its own separate bug). This second, narrower pattern catches
# the common unlabeled "Remote, <City/State/Country>" short-tag
# convention directly. Restricted to the first 400 characters of the
# page text specifically so it can ONLY match a tag near the title —
# never a sentence buried in body copy that happens to start with the
# word "Remote" (e.g. "Remote work has become the norm, California-based
# companies report..." deep in a JD would NOT match this, since it's well
# past the 400-char window).
_BARE_REMOTE_TAG_RE = re.compile(
    r"\bRemote\s*,\s*([A-Z][a-zA-Z.]+(?:\s+[A-Z][a-zA-Z.]+){0,2})\b"
)
_HEURISTIC_LOCATION_SEARCH_WINDOW = 400


def _extract_heuristic_location(text: str) -> str:
    """Best-effort "Location:"/"Primary Location:"/"Job Location:" label
    scan over a heuristic hit's own page text (see _HEURISTIC_LOCATION_RE
    above for the real posting this closes), falling back to the
    unlabeled "Remote, <place>" near-title tag pattern (see
    _BARE_REMOTE_TAG_RE above) if the labeled scan finds nothing. Returns
    "" only if NEITHER pattern matches — classifier.py's existing
    blank/unsure handling still applies in that case."""
    m = _HEURISTIC_LOCATION_RE.search(text)
    if m:
        loc = re.sub(r"\s+", " ", m.group(1)).strip(" ,.-")
        # A label match with almost nothing captured after it, or an
        # implausibly long run-on (the lookahead failed to find a real
        # boundary), is more likely noise than a real place name — skip
        # it rather than write something worse than blank.
        if loc and len(loc) <= 80:
            return loc

    m2 = _BARE_REMOTE_TAG_RE.search(text[:_HEURISTIC_LOCATION_SEARCH_WINDOW])
    if m2:
        return f"Remote, {m2.group(1).strip()}"

    return ""


# 2026-09 (explicit user report, real posting: viaquestinc.com's
# Paycor/Gnewton "Day Program Coordinator" listing — page text reads
# "Remote Status: On-Site" as a labeled value, same table-layout shape as
# the location label _HEURISTIC_LOCATION_RE already reads). This project
# never populated a workplace_type field for ANY Crawl II job before now
# — classifier.py's has_non_remote_workplace_type existed but had nothing
# to read here. Mirrors _extract_heuristic_location's approach exactly.
_HEURISTIC_WORKPLACE_TYPE_RE = re.compile(
    r"(?:remote\s*status|workplace\s*(?:setting|type)|work\s*(?:mode|arrangement|style))"
    r"\s*[:\-]\s*([A-Za-z][^:\n]{1,40}?)"
    r"(?=\s+(?:Apply|Department|Job\s*Type|Employment|Requirements|Responsibilities|"
    r"Qualifications|About|Benefits|Salary|Schedule|Description|Overview|Summary|"
    r"Job\s*Id|#\s*of\s*Openings|"
    r"Who\s|What\s|We\s|Click|View|Full[- ]?Time|Part[- ]?Time|Posted|Date|Category)\b"
    r"|[.]\s|\n|$)",
    re.I,
)


def _extract_heuristic_workplace_type(text: str) -> str:
    """Best-effort "Remote Status:"/"Workplace setting:"/"Workplace type:"
    label scan, mirroring _extract_heuristic_location above. Populates a
    real workplace_type field so classifier.py's has_non_remote_
    workplace_type (structured-field check) catches a Hybrid/On-site
    posting too, not only the freeform description-text scan
    (has_non_remote_labeled_text_signal) that has to run for every OTHER
    pipeline that never gets a structured field at all."""
    m = _HEURISTIC_WORKPLACE_TYPE_RE.search(text)
    if not m:
        return ""
    value = re.sub(r"\s+", " ", m.group(1)).strip(" ,.-")
    return value if value and len(value) <= 40 else ""


_GENERIC_ANCHOR_TOKENS = frozenset({
    "view", "read", "learn", "see", "show", "more", "detail", "details", "apply", "open", "position", "role", "job",
    "jobs", "posting", "opening", "vacancy", "click", "here", "info", "information", "now", "this", "the", "full",
    "description", "listing", "for", "to", "about", "and", "angebot", "ansehen", "mehr", "lire", "voir", "plus", "ver",
    "roles", "positions", "openings", "vacancies", "learnmore", "readmore",
})


def _is_generic_anchor(text: str) -> bool:
    """'View position', 'Read more', 'Angebot ansehen' ... -- link text that is a button label, not a job
    title. The real title is then read from the detail page (h1 / og:title) instead of being judged as-is."""
    toks = [t for t in re.split(r"[^a-z\u00c0-\u024f]+", (text or "").lower()) if t]
    return not toks or all(t in _GENERIC_ANCHOR_TOKENS for t in toks)


def _worth_detail_fetch(title: str) -> bool:
    """Role pre-filter applied BEFORE spending a request on a posting's detail page. _filter_roles drops
    keyword 'exclude' titles with no AI call and no appeal, so fetching their detail page is pure waste
    (it used to be done for up to 25 candidates per page, then thrown away). Generic button-label anchors
    are kept: their real title is only known after the fetch."""
    return _is_generic_anchor(title) or keyword_classify_role(title) != "exclude"


def _confirm_and_build_posting(detail_html: str, candidate: dict, company: str) -> dict | None:
    """A candidate link alone is never trusted -- this is the gate that keeps a heuristic hit from becoming a
    written job.

    1. A JobPosting JSON-LD on the detail page wins (structured title/location/description/remote flag).
    2. Otherwise the page's MAIN text (nav/footer/cookie/related-jobs removed -- see page_extract.main_text)
       must read as ONE job description (page_extract.is_job_description: score + a JD-specific anchor such
       as an Apply CTA with a core section, or several core sections). 2026-10: this replaced "200 chars and
       any of 20 phrases anywhere in the whole page", which let nav buttons ("Apply now" in the header) and
       marketing pages through and stored the entire page chrome as the job description."""
    ld = _extract_jsonld_jobs(detail_html, candidate["url"], company)
    if ld:
        pick = next((j for j in ld if j["url"] == candidate["url"]), ld[0])
        pick = dict(pick)
        pick["url"] = candidate["url"]
        if not pick.get("location"):
            pick["location"] = _extract_heuristic_location(_strip_html(detail_html))
        return pick

    text, li = PE.main_text(detail_html)
    if not PE.is_job_description(text, li):
        return None
    title = candidate["title"]
    if _is_generic_anchor(title) or candidate.get("_slug_title"):
        better = PE.page_title(detail_html)
        if better and not PE.is_generic_title(better):
            title = better
    full = _strip_html(detail_html)
    return {
        "title": title,
        "url": candidate["url"],
        # label scan on the job's own text first, then the whole page (a hero/header "Location:" line)
        "location": _extract_heuristic_location(text) or _extract_heuristic_location(full),
        "workplace_type": _extract_heuristic_workplace_type(text) or _extract_heuristic_workplace_type(full),
        "description": text,
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


# 2026-09: real "Apply" link discovery, in place of guessing a `/apply`
# sibling path from the job URL's shape alone (_guess_apply_url above) —
# that guess only ever fires for a narrow URL shape (opaque trailing id)
# and, even then, is just a shape match, not evidence the link actually
# exists. Screening questions (work-authorization/visa/sponsorship — the
# exact restriction language _HAS_VISA_OR_CLEARANCE_SIGNAL_RE looks for)
# often live only on the real apply/application page, so finding the link
# the page ITSELF already points to is both more accurate and covers far
# more sites than the shape-based guess.
#
# Deliberately HTML-only: this scores links already present in the
# page's own markup and fetches whatever wins — it never executes
# JavaScript or simulates a click. A JS-only apply flow (href="#" /
# "javascript:..." with no literal URL recoverable from the element's own
# attributes or its inline onclick handler) is left alone; that job just
# keeps whatever description text was already extracted, exactly like
# before this existed.
_APPLY_VOCAB_RE = re.compile(
    r"apply\s*(now|online|for\s+this\s+(job|position|role))?|"
    r"submit\s+(your\s+)?application|start\s+application|begin\s+application", re.I)
_APPLY_HREF_HINT_RE = re.compile(r"/(apply|application|applications|candidate|candidates)(?:/|$|\?)", re.I)
_BAD_HREF_RE = re.compile(r"^(javascript:|mailto:|tel:|#)", re.I)
# Matches the first quoted string that looks like a path/URL inside an
# onclick (or similar) handler's literal source — e.g.
# onclick="openApply('/jobs/123/apply')" — WITHOUT evaluating any JS.
_INLINE_HANDLER_URL_RE = re.compile(r"""['"]((?:https?://|/)[^'"\s]{2,300})['"]""")
_APPLY_DATA_ATTRS = ("data-apply-url", "data-application-url", "data-apply-href", "data-href", "data-url")


def _score_apply_candidate(href: str, text: str, aria_label: str, title: str, class_attr: str) -> int:
    score = 0
    if _APPLY_VOCAB_RE.search(text or ""):
        score += 100
    if _APPLY_VOCAB_RE.search(aria_label or ""):
        score += 80
    if _APPLY_VOCAB_RE.search(title or ""):
        score += 50
    if _APPLY_HREF_HINT_RE.search(href or ""):
        score += 40
    if re.search(r"apply|application", class_attr or "", re.I):
        score += 20
    return score


def _find_apply_url_in_html(html: str, page_url: str) -> str | None:
    """Best-effort real "Apply" URL, scored from links/elements already in
    `html` — see the module comment above for what this deliberately does
    NOT do (execute JS). Checked, in order: scored <a>/<button> hrefs,
    then an iframe's own src (an embedded application widget, e.g.
    Greenhouse's job_app iframe, IS the apply target), then data-* apply
    attributes, then a literal URL sitting inside an onclick handler.
    Returns None (never a guess) when nothing usable is found."""
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return None

    best_url, best_score = None, 0
    for node_ in tree.css("a[href], button"):
        href = node_.attributes.get("href") or ""
        if href and _BAD_HREF_RE.match(href.strip()):
            href = ""
        text = node_.text(strip=True) or ""
        aria_label = node_.attributes.get("aria-label") or ""
        title = node_.attributes.get("title") or ""
        class_attr = node_.attributes.get("class") or ""
        score = _score_apply_candidate(href, text, aria_label, title, class_attr)
        if href and score > best_score:
            try:
                best_url, best_score = urljoin(page_url, href), score
            except Exception:
                continue
        if not href and score >= 100:
            # Strong "Apply"-labeled control with no usable href — check
            # its own onclick (or similar) attribute for a literal URL
            # before giving up on it, per the module comment above.
            for attr in ("onclick", "data-onclick"):
                handler = node_.attributes.get(attr) or ""
                m = _INLINE_HANDLER_URL_RE.search(handler)
                if m:
                    try:
                        candidate = urljoin(page_url, m.group(1))
                    except Exception:
                        continue
                    if score > best_score:
                        best_url, best_score = candidate, score
                    break
    if best_url and best_score >= 40:
        return best_url

    try:
        iframe = tree.css_first("iframe[src]")
        if iframe:
            src = iframe.attributes.get("src") or ""
            if src and not _BAD_HREF_RE.match(src.strip()):
                return urljoin(page_url, src)
    except Exception:
        pass

    try:
        for node_ in tree.css("[" + "], [".join(_APPLY_DATA_ATTRS) + "]"):
            for attr in _APPLY_DATA_ATTRS:
                val = node_.attributes.get(attr)
                if val and not _BAD_HREF_RE.match(val.strip()):
                    return urljoin(page_url, val)
    except Exception:
        pass

    return None


async def _augment_with_apply_page(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                                    stats: dict, job: dict, html: str | None = None) -> None:
    """Mutates job["description"] in place if the job's real Apply page
    (or, failing that, a guessed /apply sibling) is reachable and
    actually carries visa/clearance-relevant text — never raises, never
    removes/blocks the posting either way.

    `html` is the page this job's own posting was extracted from (its
    detail page, or the listing page for a JSON-LD job extracted
    directly off it) — when given, _find_apply_url_in_html is tried
    first since it's real evidence rather than a URL-shape guess;
    _guess_apply_url remains the fallback for when no real link is found
    (or no html was passed in), same as before this existed."""
    if _HAS_VISA_OR_CLEARANCE_SIGNAL_RE.search(job.get("description") or ""):
        return
    if keyword_classify_role(job.get("title", "")) == "exclude":
        return  # _filter_roles will drop it anyway -- do not spend a request on its /apply page
    job_url = job.get("url", "")
    apply_url = None
    if html:
        try:
            apply_url = _find_apply_url_in_html(html, job_url)
        except Exception:
            apply_url = None
    if not apply_url:
        apply_url = _guess_apply_url(job_url)
    if not apply_url or apply_url == job_url:
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

class _Postings(list):
    """A list of postings plus two facts the role pre-filter would otherwise hide:
      all_urls         every job-looking URL seen on the page (kept or skipped) -- pagination progress is
                       judged on this, so a page of only-irrelevant roles still lets the walk reach page 2
      irrelevant_only  the page demonstrably lists real postings but none worth a detail fetch -- the page still
                       counts as "had roles" for archive_ii.last_seen (Verification prunes registry rows whose
                       last_seen is ~6 months old, so this must stay true for a board that only has e.g.
                       Engineer openings today)."""
    all_urls: set
    irrelevant_only: bool = False

    def __init__(self, items=(), all_urls=None, irrelevant_only=False):
        super().__init__(items)
        self.all_urls = set(all_urls) if all_urls is not None else {j.get("url") for j in self if j.get("url")}
        self.irrelevant_only = irrelevant_only


MAX_DETAIL_ENRICH_PER_PAGE = int(os.environ.get("CRAWL_II_MAX_DETAIL_ENRICH", "40"))
_SHORT_DESCRIPTION_CHARS = 400


async def _enrich_from_detail_pages(session: aiohttp.ClientSession, sem: asyncio.Semaphore, jobs: list[dict],
                                    page_url: str, company: str, stats: dict,
                                    parse_pool: concurrent.futures.Executor) -> None:
    """Listing-style sources (embedded state JSON, microdata, RSS/WP feeds, even JSON-LD ItemLists) usually
    carry title + url but little or no description, and the description is what the visa / restriction /
    remote-scope checks read. For role-relevant jobs whose description is short, fetch the job's own page and
    take its JobPosting JSON-LD or its main text. Bounded per page; never raises; never drops a job."""
    loop = asyncio.get_running_loop()
    todo = [j for j in jobs
            if len(j.get("description") or "") < _SHORT_DESCRIPTION_CHARS and j.get("url") and j["url"] != page_url
            and keyword_classify_role(j.get("title", "")) != "exclude"][:MAX_DETAIL_ENRICH_PER_PAGE]

    async def _one(job: dict) -> None:
        try:
            async with sem:
                got = await node._fetch_page(session, job["url"], stats)
            if not got:
                return
            _, dh = got
            built = await loop.run_in_executor(parse_pool, _confirm_and_build_posting, dh,
                                               {"title": job["title"], "url": job["url"]}, company)
        except Exception:
            return
        if not built:
            return
        if len(built.get("description") or "") > len(job.get("description") or ""):
            job["description"] = built["description"]
        for k in ("location", "workplace_type"):
            if built.get(k) and not job.get(k):
                job[k] = built[k]
        stats["detail_enriched"] += 1

    await asyncio.gather(*(_one(j) for j in todo))


async def _extract_via_jsonld_or_heuristic(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                                            html: str, page_url: str, company: str, stats: dict,
                                            parse_pool: concurrent.futures.Executor) -> list[dict]:
    """The actual extraction ladder for one fetched page, factored out of extract_postings_from_page so a page
    reached via the link-follow / iframe / pagination steps gets IDENTICAL treatment to the original page.

    Ladder (first rung that yields postings wins; each rung is a different "where do sites hide their jobs"):
      1. JSON-LD JobPosting                     (schema.org, highest confidence)
      2. microdata JobPosting + embedded state  (page_extract: itemprop markup; __NEXT_DATA__ / __NUXT__ /
         JSON                                    window.__X__ / application/json / data-props / hidden-input
                                                 JSON such as Zoho Recruit custom domains)
      3. heuristic job links, each CONFIRMED by fetching the page and requiring it to read as one JD

    2026-08 history kept: every CPU-bound parse runs in `parse_pool`, detail fetches are concurrent.
    2026-10: rungs 2 and the role pre-filter are new -- detail pages are only fetched for roles that could
    survive _filter_roles (it drops keyword-'exclude' titles with no appeal), and the cap of
    MAX_HEURISTIC_CANDIDATES_PER_PAGE now applies AFTER that filter, so a big board no longer loses its
    relevant roles behind 25 irrelevant ones."""
    loop = asyncio.get_running_loop()

    jsonld_jobs = await loop.run_in_executor(parse_pool, _extract_jsonld_jobs, html, page_url, company)
    if jsonld_jobs:
        stats["jsonld_pages"] += 1
        stats["jsonld_postings"] += len(jsonld_jobs)
        await _enrich_from_detail_pages(session, sem, jsonld_jobs, page_url, company, stats, parse_pool)
        await asyncio.gather(*(_augment_with_apply_page(session, sem, stats, j, html) for j in jsonld_jobs))
        return _Postings(jsonld_jobs)

    structured = await loop.run_in_executor(parse_pool, PE.extract_microdata_jobs, html, page_url, company)
    if structured:
        stats["microdata_pages"] += 1
    else:
        structured = await loop.run_in_executor(parse_pool, PE.extract_state_jobs, html, page_url, company)
        if structured:
            stats["state_json_pages"] += 1
    if structured:
        stats["structured_postings"] += len(structured)
        await _enrich_from_detail_pages(session, sem, structured, page_url, company, stats, parse_pool)
        return _Postings(structured)

    candidates = await loop.run_in_executor(parse_pool, _find_heuristic_candidates, html, page_url)
    if not candidates:
        return _Postings()
    stats["heuristic_pages"] += 1
    all_urls = {c["url"] for c in candidates}
    worth = [c for c in candidates if _worth_detail_fetch(c["title"])]
    skipped = [c for c in candidates if not _worth_detail_fetch(c["title"])]
    stats["role_prefilter_skipped"] += len(skipped)
    candidates = worth[:MAX_HEURISTIC_CANDIDATES_PER_PAGE]

    async def _fetch_and_confirm(cand: dict) -> tuple[dict, str] | None:
        async with sem:
            detail = await node._fetch_page(session, cand["url"], stats)
        if not detail:
            return None
        _, detail_html = detail
        built = await loop.run_in_executor(parse_pool, _confirm_and_build_posting, detail_html, cand, company)
        if not built:
            return None
        return built, detail_html

    results = await asyncio.gather(*(_fetch_and_confirm(c) for c in candidates))
    confirmed_pairs = [r for r in results if r]
    confirmed = [job for job, _detail_html in confirmed_pairs]
    stats["heuristic_postings"] += len(confirmed)
    await asyncio.gather(*(_augment_with_apply_page(session, sem, stats, job, detail_html)
                            for job, detail_html in confirmed_pairs))
    out = _Postings(confirmed, all_urls=all_urls)
    if not confirmed and skipped:
        # nothing relevant -- but is this a real job board? confirm up to 2 of the skipped links (2 requests)
        probes = await asyncio.gather(*(_fetch_and_confirm(c) for c in skipped[:2]))
        out.irrelevant_only = any(probes)
    return out


async def _walk_pagination(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                            first_html: str, first_url: str, company: str, stats: dict,
                            parse_pool: concurrent.futures.Executor,
                            first_postings: list[dict]) -> list[dict]:
    """2026-09: a genuine listings page can itself span multiple pages ("Page 1 of 5", a "Next"/"Load more"
    control). This follows _find_next_page_url forward from a page that already yielded postings, merging each
    subsequent page's postings in (deduped by job URL) until: no next-page link is found,
    MAX_ARCHIVE_II_PAGES_PER_LISTING is reached, a next link points somewhere already visited (loop guard), or
    a page adds nothing new.

    2026-10: "adds nothing new" is judged on every job-looking URL the page showed (_Postings.all_urls), not
    only the role-relevant ones kept -- otherwise a page whose roles were all pre-filtered out would end the
    walk even though page 3 has the Customer Success opening. Also callable with an empty/irrelevant-only
    first page for the same reason."""
    all_postings = list(first_postings)
    seen_urls = {p["url"] for p in all_postings if p.get("url")}
    seen_all = set(getattr(first_postings, "all_urls", set())) | seen_urls
    seen_pages = {first_url}
    loop = asyncio.get_running_loop()
    current_html, current_url = first_html, first_url

    for _ in range(MAX_ARCHIVE_II_PAGES_PER_LISTING - 1):
        next_url = await loop.run_in_executor(parse_pool, _find_next_page_url, current_html, current_url)
        if not next_url or next_url in seen_pages:
            break
        seen_pages.add(next_url)
        async with sem:
            fetched = await node._fetch_page(session, next_url, stats)
        if not fetched:
            break
        stats["pagination_pages_followed"] += 1
        next_final_url, next_html = fetched
        next_postings = await _extract_via_jsonld_or_heuristic(
            session, sem, next_html, next_final_url, company, stats, parse_pool)

        new_count = 0
        for p in next_postings:
            u = p.get("url")
            if u and u in seen_urls:
                continue
            if u:
                seen_urls.add(u)
            all_postings.append(p)
            new_count += 1
        fresh_urls = set(getattr(next_postings, "all_urls", set())) - seen_all
        seen_all |= fresh_urls
        if new_count == 0 and not fresh_urls:
            break
        stats["pagination_extra_postings"] += new_count
        current_html, current_url = next_html, next_final_url

    return _Postings(all_postings, all_urls=seen_all)


MAX_FRAME_FOLLOW = int(os.environ.get("CRAWL_II_MAX_FRAME_FOLLOW", "3"))
SITEMAP_FALLBACK = os.environ.get("CRAWL_II_SITEMAP_FALLBACK", "1") != "0"
MAX_ATS_BRIDGE_BOARDS = 2


async def _bridge_known_ats(urls: set[str], stats: dict) -> list[dict]:
    """A page whose HTML / iframe / script URLs point at one of the platforms ats_scrapers can read gets
    scraped through that platform's own API (full, structured, with questions) instead of parsed as HTML.
    node.py already does this at discovery time, so an archive_ii page normally has no such hit -- this
    catches pages whose embed was added or changed since, and iframe documents (an embed inside an embed)."""
    try:
        hits = node._detect_ats_hits(urls)
    except Exception:
        return []
    boards, seen = [], set()
    for ats, slug, _u in hits:
        if ats.lower() in ats_scrapers.SCRAPERS and (ats, slug) not in seen:
            seen.add((ats, slug))
            boards.append((ats, slug))
    jobs: list[dict] = []
    for ats, slug in boards[:MAX_ATS_BRIDGE_BOARDS]:
        try:
            got = await ats_scrapers.scrape_board(ats, slug)
        except Exception as e:
            log.debug(f"ATS bridge {ats}/{slug} failed: {e}")
            continue
        for j in got or []:
            j.setdefault("description", j.get("description_snippet") or "")
            j.setdefault("clearance", "")
            jobs.append(j)
    if jobs:
        stats["ats_bridge_pages"] += 1
        stats["ats_bridge_postings"] += len(jobs)
    return jobs


async def _fetch_json_or_xml(session: aiohttp.ClientSession, sem: asyncio.Semaphore, url: str, stats: dict) -> str | None:
    async with sem:
        got = await node._fetch_page(session, url, stats)
    return got[1] if got else None


async def _dead_end_fallbacks(session: aiohttp.ClientSession, sem: asyncio.Semaphore, html: str, final_url: str,
                              company: str, stats: dict, parse_pool: concurrent.futures.Executor,
                              depth: int = 0) -> list[dict]:
    """Everything tried when the page itself (and its best 'Explore roles' link) produced nothing. In order of
    cost/confidence: known-ATS bridge -> embedded frames / widgets -> RSS/Atom + WordPress REST -> the page
    being a single posting. Frames recurse once (a frame document can itself embed the board)."""
    loop = asyncio.get_running_loop()

    urls = await loop.run_in_executor(parse_pool, node._extract_candidate_urls, html, final_url)
    jobs = await _bridge_known_ats(urls, stats)
    if jobs:
        return jobs

    frames = await loop.run_in_executor(parse_pool, PE.find_job_frames, html, final_url, MAX_FRAME_FOLLOW)
    if frames:
        jobs = await _bridge_known_ats(set(frames), stats)
        if jobs:
            return jobs
    for furl in frames:
        if furl == final_url:
            continue
        async with sem:
            got = await node._fetch_page(session, furl, stats)
        stats["frames_followed"] += 1
        if not got:
            continue
        fu, fh = got
        found = await _extract_via_jsonld_or_heuristic(session, sem, fh, fu, company, stats, parse_pool)
        if found or getattr(found, "irrelevant_only", False):
            stats["frame_found_postings"] += 1
            return await _walk_pagination(session, sem, fh, fu, company, stats, parse_pool, found)
        if depth < 1:
            found = await _dead_end_fallbacks(session, sem, fh, fu, company, stats, parse_pool, depth + 1)
            if found:
                stats["frame_found_postings"] += 1
                return found

    for feed in await loop.run_in_executor(parse_pool, PE.find_feed_links, html, final_url):
        xml = await _fetch_json_or_xml(session, sem, feed, stats)
        found = PE.parse_feed_jobs(xml or "", feed, company)
        if found:
            stats["feed_found_postings"] += len(found)
            await _enrich_from_detail_pages(session, sem, found, final_url, company, stats, parse_pool)
            return found

    root = PE.wp_api_root(html, final_url)
    if root:
        types_txt = await _fetch_json_or_xml(session, sem, root.rstrip("/") + "/wp/v2/types", stats)
        try:
            bases = PE.wp_job_endpoints(json.loads(types_txt)) if types_txt else []
        except ValueError:
            bases = []
        for base in bases:
            txt = await _fetch_json_or_xml(session, sem, f"{root.rstrip('/')}/wp/v2/{base}?per_page=100", stats)
            try:
                found = PE.parse_wp_posts(json.loads(txt), company) if txt else []
            except ValueError:
                found = []
            if found:
                stats["wp_api_found_postings"] += len(found)
                await _enrich_from_detail_pages(session, sem, found, final_url, company, stats, parse_pool)
                return found

    single = await loop.run_in_executor(parse_pool, PE.single_job_page, html, final_url, company)
    if single and _worth_detail_fetch(single["title"]):
        loc = _extract_heuristic_location(single["description"])
        single["location"] = single["location"] or loc
        single["workplace_type"] = single.get("workplace_type") or _extract_heuristic_workplace_type(single["description"])
        stats["single_page_postings"] += 1
        return [single]

    if depth == 0 and SITEMAP_FALLBACK:
        return await _sitemap_fallback(session, sem, final_url, company, stats, parse_pool)
    return []


async def _sitemap_fallback(session: aiohttp.ClientSession, sem: asyncio.Semaphore, page_url: str, company: str,
                            stats: dict, parse_pool: concurrent.futures.Executor) -> list[dict]:
    """JS-rendered boards usually still have server-rendered detail pages, and their sitemap lists them. Reads
    {origin}/sitemap.xml (else the robots.txt `Sitemap:` line), follows at most two job-looking child sitemaps,
    keeps URLs shaped like /jobs/<title-slug>, role-filters by the SLUG (no request wasted on irrelevant roles),
    then fetches + confirms the survivors exactly like heuristic candidates. <= 4 listing requests per
    dead-end page; disable with CRAWL_II_SITEMAP_FALLBACK=0."""
    loop = asyncio.get_running_loop()
    p = urlparse(page_url)
    origin = f"{p.scheme}://{p.netloc}"
    xml = await _fetch_json_or_xml(session, sem, origin + "/sitemap.xml", stats)
    if not xml or "<loc" not in xml.lower():
        robots = await _fetch_json_or_xml(session, sem, origin + "/robots.txt", stats)
        sm_url = next((ln.split(":", 1)[1].strip() for ln in (robots or "").splitlines()
                       if ln.strip().lower().startswith("sitemap:")), "")
        xml = await _fetch_json_or_xml(session, sem, sm_url, stats) if sm_url.startswith("http") else None
    if not xml:
        return []
    children, urls = PE.parse_sitemap(xml)
    for child in PE.pick_job_sitemaps(children):
        cxml = await _fetch_json_or_xml(session, sem, child, stats)
        if cxml:
            urls.extend(PE.parse_sitemap(cxml)[1])
    cands = [c for c in PE.sitemap_job_candidates(urls) if _worth_detail_fetch(c["title"])]
    if not cands:
        return []
    stats["sitemap_pages_with_candidates"] += 1

    async def _one(cand: dict):
        async with sem:
            got = await node._fetch_page(session, cand["url"], stats)
        if not got:
            return None
        return await loop.run_in_executor(parse_pool, _confirm_and_build_posting, got[1], cand, company)

    built = [b for b in await asyncio.gather(*(_one(c) for c in cands[:MAX_HEURISTIC_CANDIDATES_PER_PAGE])) if b]
    stats["sitemap_postings"] += len(built)
    return built


async def extract_postings_from_page(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                                      page: dict, stats: dict,
                                      parse_pool: concurrent.futures.Executor) -> list[dict]:
    """page: {"career_page_url","website_url"}. Returns candidate job dicts
    — NOT yet role/location filtered, see _run_pipeline_ii for that.

    2026-09: if neither JSON-LD nor the heuristic detector finds ANYTHING
    on this page, that's exactly the signature of a landing/stub page
    whose real listings sit one click away behind a button ("Explore
    Roles", "View All Jobs", ...) — confirmed to be a real, non-trivial
    slice of archive_ii's 120k+ already-captured pages, same root cause
    node.py's crawl_one fixed for its own discovery tiers. Follows the
    single best-scoring candidate link (MAX_CAREER_LINK_FOLLOW — see its
    comment for why this is intentionally tighter than node.py's cap of
    3) found via node._extract_job_listing_link_candidates (the SAME
    detector and phrase list node.py uses — one shared vocabulary, so
    node.py and this file can never silently drift apart on what counts
    as a "go look at jobs" link) and retries the IDENTICAL extraction on
    whatever it finds. A dead end here costs exactly one extra parse (no
    network request); a real find costs one extra fetch plus whatever
    that page's own extraction needs — same bounded, best-effort pattern
    as every other fetch in this pipeline."""
    fail_kind: list[str] = []
    async with sem:
        fetched = await node._fetch_page(session, page["career_page_url"], stats, fail_kind=fail_kind)
    if not fetched:
        stats["page_unreachable"] += 1
        if fail_kind and fail_kind[0] in node.TRANSIENT_FETCH_KINDS:
            page["_retry"] = True
        return []
    final_url, html = fetched
    company = _company_name_from_domain(page["website_url"])

    postings = await _extract_via_jsonld_or_heuristic(session, sem, html, final_url, company, stats, parse_pool)
    if postings or getattr(postings, "irrelevant_only", False):
        walked = await _walk_pagination(session, sem, html, final_url, company, stats, parse_pool, postings)
        if not walked and getattr(postings, "irrelevant_only", False):
            page["_had_postings"] = True  # a real board, just nothing relevant on it today
        return walked

    loop = asyncio.get_running_loop()
    link_candidates = await loop.run_in_executor(
        parse_pool, node._extract_job_listing_link_candidates, html, final_url)
    ranked = sorted(link_candidates, key=lambda kv: -kv[1])[:MAX_CAREER_LINK_FOLLOW]

    for url, _score in ranked:
        if url == final_url:
            continue
        async with sem:
            followed = await node._fetch_page(session, url, stats)
        stats["career_link_follow_attempted"] += 1
        if not followed:
            continue
        followed_url, followed_html = followed
        postings = await _extract_via_jsonld_or_heuristic(
            session, sem, followed_html, followed_url, company, stats, parse_pool)
        if postings or getattr(postings, "irrelevant_only", False):
            stats["career_link_follow_found_postings"] += 1
            walked = await _walk_pagination(
                session, sem, followed_html, followed_url, company, stats, parse_pool, postings)
            if not walked and getattr(postings, "irrelevant_only", False):
                page["_had_postings"] = True
            return walked

    fallback = await _dead_end_fallbacks(session, sem, html, final_url, company, stats, parse_pool)
    if fallback:
        return fallback

    stats["no_postings_found"] += 1
    return []


# ── Description-text global-hiring enrichment (2026-09, Crawl II only) ──
# crawl_i.py's ATS-API sources give a clean, structured location field per
# posting; crawl_ii's heuristic/JSON-LD sources often leave location blank
# or a bare "Remote" while the REAL hiring scope ("open to candidates
# worldwide", "work from anywhere") is stated in the job description body
# text instead — text crawl_ii already has in hand (JSON-LD description,
# or the apply-page augmentation) but which classifier.py's shared funnel
# never looks at (it only ever reads job["location"]/job["country"]).
#
# This scans that description for a DELIBERATELY narrow subset of
# classifier.py's own GLOBAL_KEYWORDS phrases — copied verbatim from
# there, not reinvented — restricted to ones that explicitly reference
# hiring/candidate location flexibility. classifier.py's full list also
# has generic corporate-branding entries ("global network", "global
# presence", "global scale", "earth", "all over the world") that read
# fine as evidence in a short location field but are common, unrelated
# marketing copy in a JD body ("our global network of partners") — those
# are deliberately excluded here to avoid false-positiving a job into
# "open worldwide" on marketing language alone.
#
# Only fires when location is bare/placeholder (same bare-check
# classifier.py's own _enrich_location_from_title uses) — never
# overrides a location that already has real content, and only ever
# ADDS a clean "Worldwide" token for the existing classifier to evaluate
# through its own tested keyword/residue logic — it does not decide
# match/no-match itself.
_JD_STRONG_GLOBAL_HIRING_RE = tuple(re.compile(p, re.I) for p in (
    r"\bwork\s*from\s*anywhere\b",
    r"\bwfa\b",
    r"\bopen\s*to\s*(all|any)\s*location",
    r"\bopen\s*to\s*(all|any)\s*countr",
    r"\blocation\s*[\-–—:]?\s*anywhere\b",
    r"\blocation\s*agnostic\b",
    r"\blocation\s*independent\b",
    r"\bgeo[\-\s]*agnostic\b",
    r"\bunrestricted\s*location\b",
    r"\bno\s*location\s*restriction\b",
    r"\bno\s*location\s*(requirement|restriction|preference)\b",
    r"\bno\s*geographic\s*restriction\b",
    r"\bno\s*country\s*restriction\b",
    r"\btime[\-\s]*zone\s*agnostic\b",
    r"\bany\s*time\s*zone\b",
    r"\bany\s*timezone\b",
    r"\bregardless\s*of\s*(location|country|time\s*zone|timezone)\b",
    r"\birrespective\s*of\s*(location|country)\b",
    r"\bcountry[\-\s]*agnostic\b",
    r"\bwork\s*from\s*any\s*(country|location)\b",
    r"\bopen\s*to\s*(candidates|applicants)\s*(worldwide|globally|from\s*anywhere|in\s*any\s*country)\b",
    # 2026-09 BUG FIX (avidtr.com Senior Project Manager — see BUG FIX note
    # below): "hire/hiring globally" and "hire talent globally" REMOVED
    # from this list. They read as candidate-eligibility signals in
    # isolation, but in real postings they're just as likely to be generic
    # company-branding or EEO-boilerplate copy ("We're proud to hire
    # talent globally...") that says nothing about whether THIS role is
    # open worldwide. The "open to (candidates|applicants) ..." phrase
    # above stays — it's explicitly framed around who can apply, not the
    # company's general reach — and is the safer way to catch the
    # legitimate version of this same claim.
))
_BARE_LOCATION_VALUES = ("", "remote", "remote worker", "remote job", "fully remote")

# 2026-09 BUG FIX: a real posting (ehryourway.com Senior CSM — Enterprise)
# said "Fully remote — work from anywhere in the United States." and got
# written to the DB as location="Worldwide" — a straight false positive
# that then sailed through classifier.py's location gate as a top-tier
# Global match. Root cause: _JD_STRONG_GLOBAL_HIRING_RE matched the bare
# phrase "work from anywhere" and stopped there, never checking what
# immediately qualifies it. "anywhere in the United States" is not
# "anywhere" — it's exactly one country, stated three words later.
#
# This mirrors a lesson classifier.py's own location-FIELD parser already
# learned (see its "residue check" comments): a keyword hit alone is not
# evidence — you have to also confirm nothing narrowing survives right
# next to it. This JD-body enrichment (added after that lesson, for a
# different input source) was written without the equivalent check, so it
# had the identical blind spot. Fix applies to every phrase in
# _JD_STRONG_GLOBAL_HIRING_RE uniformly (not just "work from anywhere"),
# since any of them could just as easily be followed/preceded by "...in
# the US" / "...within Canada" / etc. in real posting text.
#
# 2026-09 BUG FIX #2 (avidtr.com "Senior Project Manager — Remote,
# California"): the on-page location tag was literally "Remote,
# California" — a single-STATE restriction — but got written as
# "Worldwide" anyway. Live re-fetch found none of this file's global-
# hiring trigger phrases anywhere on the page, so the actual cause here
# wasn't a missing qualifier check on a real phrase — it's the phrase
# list itself being too permissive (see the "hire/hiring globally"
# removal above) combined with this qualifier list having NO US state
# names at all, only countries. A posting whose real restriction is "one
# US state" rather than "the whole US" was invisible to this check
# either way. Fixed on both fronts: the riskiest phrase removed above,
# and every US state (plus DC) added below so a state-level restriction
# right next to a trigger phrase is caught exactly like a country-level
# one already was.
_US_STATES_RE_FRAGMENT = (
    r"Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|"
    r"Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|"
    r"Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New\s+Hampshire|New\s+Jersey|"
    r"New\s+Mexico|New\s+York|North\s+Carolina|North\s+Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|"
    r"Rhode\s+Island|South\s+Carolina|South\s+Dakota|Tennessee|Texas|Utah|Vermont|Virginia|"
    r"Washington|West\s+Virginia|Wisconsin|Wyoming|District\s+of\s+Columbia"
)
_JD_QUALIFIER_WINDOW = 80  # chars scanned on each side of a phrase hit
# 2026-09 BUG FIX (real posting: IrisCX's "Customer Success Manager",
# iriscx.com/career/, an in-house/archive_ii page): said "Calgary, AB or
# Remote in Canada" as its actual location, plus "Work from anywhere (as
# long as it's in the Pacific or Mountain time zones)" — a Canada-only,
# two-timezone-restricted role that still got enriched all the way to
# location="Worldwide" at PRIORITY_GLOBAL (the TOP tier). Two compounding
# gaps, both fixed here: (1) this regex had no notion of a NAMED time
# zone as a narrowing signal at all — "Pacific or Mountain time zones" is
# every bit as narrowing as a named country, it just wasn't in the list;
# (2) the window-based scan below only checked ±80 chars around the
# trigger phrase itself, so "Calgary, AB or Remote in Canada" (from a
# SEPARATE bullet/line elsewhere on the page) was never seen at all —
# _enrich_location_from_description now also does a whole-description
# scan for this same regex, not just a window around each phrase hit.
# 2026-09: split into a case-INSENSITIVE pattern (full country/state/timezone
# names — "Canada", "Alabama", "Pacific time zone" etc. don't collide with
# ordinary lowercase English words, so matching them regardless of case is
# safe) and a case-SENSITIVE pattern (bare "US"/"UK"-style abbreviations,
# which DO collide with extremely common lowercase words — "us" as in "join
# us"/"contact us"/"for us", "uk" far less commonly but still). Testing this
# fix's whole-description scan against a synthetic genuinely-global posting
# ("...we hire globally with no location restrictions...for us.") surfaced
# this immediately: under a single case-insensitive regex, the word "us" in
# "for us" matched the "US" alternative and incorrectly blocked the
# Worldwide promotion for a posting with zero real narrowing content. Only
# an ALL-CAPS "US"/"USA"/"UK" (or the dotted "U.S."/"U.S.A."/"U.K." forms,
# which never collide with plain words) counts as a real narrowing signal.
_JD_NARROWING_QUALIFIER_RE_CI = re.compile(
    r"\b(?:U\.S\.|U\.S\.A\.|United\s+States|U\.K\.|United\s+Kingdom|Canada|Australia|"
    r"Germany|France|Netherlands|Mexico|Philippines|Nigeria|Kenya|South\s+Africa|India|Ireland|"
    r"Spain|Italy|Brazil|Japan|Singapore|China|Sweden|Norway|Denmark|Finland|Poland|Portugal|"
    r"Switzerland|Austria|Belgium|New\s+Zealand|Israel|United\s+Arab\s+Emirates|Egypt|Ghana|"
    r"APAC|LATAM|ANZ|NAM|MENA|" + _US_STATES_RE_FRAGMENT + r"|"
    # Named North American time zone(s), singular or an "X or Y" /
    # "X and Y" disjunction right before "time zone(s)" — e.g. "Pacific
    # time zone", "Pacific or Mountain time zones", "Eastern and Central
    # time zones". A BARE "any time zone"/"any timezone" is NOT matched
    # here (no named zone token) — that phrase is its own POSITIVE
    # global-hiring signal in _JD_STRONG_GLOBAL_HIRING_RE above and is
    # deliberately left alone.
    r"(?:Pacific|Mountain|Central|Eastern|Atlantic|Hawaii|Alaska)"
    r"(?:\s*(?:,|or|and)\s*(?:Pacific|Mountain|Central|Eastern|Atlantic|Hawaii|Alaska))*"
    r"\s+time\s*zones?"
    r")\b",
    re.I,
)
# Case-SENSITIVE: only an all-caps "US"/"USA"/"UK" counts — see note above.
_JD_NARROWING_QUALIFIER_RE_CS = re.compile(r"\b(?:US|USA|UK)\b")


def _narrowing_qualifier_search(text: str):
    """Combined case-insensitive + case-sensitive narrowing-qualifier search
    — see _JD_NARROWING_QUALIFIER_RE_CI/_CS above for why these can't just be
    one case-insensitive regex."""
    return _JD_NARROWING_QUALIFIER_RE_CI.search(text) or _JD_NARROWING_QUALIFIER_RE_CS.search(text)


def _enrich_location_from_description(job: dict) -> None:
    """Mutates job["location"] in place — see module note above. No-op
    when location already has real content, when the description carries
    none of the narrow phrase set, when every phrase hit found is itself
    narrowed to one specific country/region/timezone right next to it
    (e.g. "work from anywhere in the United States" — see BUG FIX note
    above), or (2026-09) when a narrowing qualifier appears ANYWHERE ELSE
    in the description even if not adjacent to the trigger phrase (the
    IrisCX case: the restriction was stated in a different bullet/line
    entirely — see the BUG FIX note on _JD_NARROWING_QUALIFIER_RE_CI above).
    In any of these cases the location is deliberately left bare rather
    than guessed at, so classifier.py's existing blank→"unsure"→AI path
    still gets a look at it instead of being short-circuited."""
    loc = (job.get("location") or "").strip()
    if loc.lower() not in _BARE_LOCATION_VALUES and not PLACEHOLDER_LOC_RE.match(loc):
        return
    desc = job.get("description") or ""
    if not desc:
        return
    # Whole-description check FIRST — a narrowing qualifier stated
    # anywhere on the page (not just next to the trigger phrase) is real
    # evidence this isn't actually a global/worldwide-open posting, even
    # if it's in a separate bullet from the "work from anywhere"-style
    # phrase that would otherwise trigger the enrichment below.
    if _narrowing_qualifier_search(desc):
        return
    for rx in _JD_STRONG_GLOBAL_HIRING_RE:
        for m in rx.finditer(desc):
            start = max(0, m.start() - _JD_QUALIFIER_WINDOW)
            end = min(len(desc), m.end() + _JD_QUALIFIER_WINDOW)
            before, after = desc[start:m.start()], desc[m.end():end]
            if _narrowing_qualifier_search(before) or _narrowing_qualifier_search(after):
                continue  # narrowed to one country/region right here — not real evidence
            # A bare "Remote" is worth keeping (distinguishes "remote, open
            # worldwide" from a placeholder like "TBD"/"See description",
            # which carries no information worth preserving).
            job["location"] = f"{loc}, Worldwide" if loc.lower() in ("remote", "remote worker", "remote job", "fully remote") else "Worldwide"
            return


# ── Classification + push (mirrors crawl_i.py's role/location/visa funnel) ──

def _filter_roles(jobs: list[dict]) -> list[dict]:
    """2026-09 (explicit user request): every included job is also tagged
    job["role_category"] — one of "CS"/"AM"/"PM"/"OM" — mirrors
    crawl_i.py's filter_roles."""
    included, unsure = [], []
    for job in jobs:
        result = keyword_classify_role(job["title"])
        if result == "include":
            job["role_category"] = classify_role_category(job["title"])
            included.append(job)
        elif result == "unsure":
            unsure.append(job)
    if unsure:
        ai_results = ai_classify_roles([j["title"] for j in unsure])
        for job in unsure:
            if ai_results.get(job["title"], False):
                job["role_category"] = classify_role_category(job["title"])
                included.append(job)
    return included


def _filter_locations(jobs: list[dict], excluded_urls: dict | None = None,
                       new_exclusions: set | None = None) -> tuple[list[dict], list[str]]:
    """2026-09 (explicit user request — see excluded_cache.py's module
    docstring and crawl_i.py's filter_locations for the full design):
    `excluded_urls` is an optional {url: excluded_at_iso} mapping of jobs a
    past run already sent to the LLM and confirmed excluded — a job whose
    keyword-stage result is "unsure" and whose URL is in this mapping skips
    the LLM call (still gets a fresh, cheap Rank 4 attempt regardless).
    `new_exclusions` is an optional set this function ADDS TO with the URL
    of every job genuinely dropped this run, for the caller to persist as
    this shard's own newly-excluded partial file. Both default to
    None/disabled for any caller that doesn't need the cache."""
    if excluded_urls is None:
        excluded_urls = {}
    if new_exclusions is None:
        new_exclusions = set()

    matched, confidences, unsure_jobs = [], [], []
    # Parallel to unsure_jobs — 'blank' or 'bare_remote', see
    # _keyword_classify_location_detail's docstring and crawl_i.py's
    # filter_locations for the full reasoning on why these are treated
    # differently below.
    unsure_reasons = []

    rank4_enabled = getattr(config, "ENABLE_RANK4_COUNTRY_SPECIFIC", False)

    def _try_rank4(job: dict) -> bool:
        # Same Rank 4 gate as crawl_i.py's filter_locations — see that
        # function's docstring. 2026-09 (explicit user correction): Crawl
        # II is NOT excluded from Rank 4 the way Crawl III is — it runs
        # the same enrich_application_questions_async() as Crawl I (see
        # this file's imports), so a Crawl II job CAN carry a real
        # "Application Question:" marker. In practice every Crawl II row
        # was tagged source_ats=DEFAULT_ATS_LABEL ("in_house"), never one
        # of RANK4_ELIGIBLE_ATS's real platform names, so this gate used
        # to be correct-but-permanently-inert here — there was no path
        # for an in-house/unsupported-ATS job to ever earn Rank 4.
        #
        # 2026-10 (explicit user request): Crawl II now has its OWN
        # earned path in, alongside the RANK4_ELIGIBLE_ATS one. The
        # original "Application Question:" marker check only ever
        # confirmed we'd genuinely fetched the real form as a SIDE
        # EFFECT of finding a substantive, non-boilerplate screening
        # question in it (see _format_screening_questions in
        # ats_scrapers.py) — a real captured form that happens to ask NO
        # custom screening questions beyond the standard fields would
        # never set that marker at all, and was being dropped as if it
        # had never been fetched. _fetch_wild_questions (the one fetcher
        # every in-house/unsupported-ATS job always goes through) now
        # separately counts how many of the RAW fetched fields are
        # confirmed application-form boilerplate (name, email, phone,
        # resume/CV, cover letter, LinkedIn, EEO fields, etc. — the
        # fields every real application form has, screening questions or
        # not) and stores that count on the job as
        # _confirmed_application_form_fields. Seeing enough of them
        # (RANK4_CONFIRMED_FORM_FIELD_THRESHOLD) is direct, positive
        # proof the page rendered a real application form — not an
        # unfetched/blank/wrong-URL result — so it earns the exact same
        # trust RANK4_ELIGIBLE_ATS membership already grants, and skips
        # requiring the "Application Question:" marker too (whatever
        # description_snippet actually holds, even just boilerplate
        # fields and no screening questions at all, is now trustworthy
        # input). classify_rank4() itself is completely unchanged — it
        # still scans for the identical auth/sponsorship/restriction
        # signals either way; this only changes how Crawl II earns the
        # right to be scanned at all.
        if not rank4_enabled:
            return False
        if job.get("role_category") not in ("CS", "AM"):
            return False
        confirmed_form = (
            job.get("_confirmed_application_form_fields", 0)
            >= RANK4_CONFIRMED_FORM_FIELD_THRESHOLD
        )
        if job.get("source_ats") not in RANK4_ELIGIBLE_ATS and not confirmed_form:
            return False
        if not confirmed_form and "Application Question:" not in (job.get("description_snippet") or ""):
            return False
        priority, reason = classify_rank4(job)
        if not priority:
            return False
        job["clearance"] = "rank4"
        job["location_priority"] = priority
        matched.append(job)
        confidences.append(f"rank4_{reason}")
        return True

    for job in jobs:
        # 2026-09 BUG FIX (this investigation, crawl_ii's own bug — separate
        # from both of today's earlier fixes): classifier.py is shared by
        # both pipelines, but several of its location-funnel functions read
        # job["description_snippet"] specifically, not job["description"]:
        #   - _classify_location_batch (builds the AI location-classifier
        #     prompt: `desc = job.get("description_snippet", "")` — used
        #     verbatim as the "Description: ..." line the AI reads)
        #   - ai_classify_locations' post-AI "safety net" (re-checks
        #     title+description_snippet for keyword evidence before
        #     trusting an AI match_global/match_africa verdict)
        #   - has_hard_country_specific_auth_signal / has_state_list_
        #     restriction_signal (both description_snippet-only)
        # ats_scrapers.py (crawl_i.py's extractor) has always populated
        # "description_snippet" on every job it returns. crawl_ii.py's own
        # extractor (extract_postings_from_page, above) only ever set
        # job["description"] and never mirrored it into
        # "description_snippet" — not a crash anywhere (every reader above
        # uses `.get(...) or ""`/`.get(..., "")`, so it fails silently), just
        # every one of those functions seeing an empty string for every
        # single crawl_ii job, forever.
        #
        # Confirmed live (synthetic repro, see test_description_snippet_
        # backfill.py): a blank-location job with a description that
        # plainly says "we hire globally, no location restrictions,
        # candidates from every continent have joined us" still produces
        # the literal AI prompt line `Description: [No description
        # available]` — the AI is given zero JD text to work with and can
        # only guess from title+blank-location, so it defaults to
        # UNCERTAIN far more often than a sighted read of the same JD
        # would. crawl_ii's in-house/unrecognized-ATS pages lean on this
        # AI stage far more than crawl_i's structured-ATS pages do (messier
        # location data → more "blank"/"bare_remote" unsure jobs reach it
        # in the first place), so this blind spot hits crawl_ii's survival
        # rate especially hard — independent of, and in addition to, the
        # shared classifier.py has_hard_country_specific_auth_signal fix
        # (which never even fired for crawl_ii jobs, since it's also
        # description_snippet-only and was therefore already a permanent
        # no-op here either way).
        #
        # Fix: mirror "description" into "description_snippet" right here,
        # before any shared classifier.py function — or
        # detect_visa_sponsorship, called on these same job dicts further
        # down crawl_batch_ii, which has the identical description_snippet-
        # only read — ever sees the job. Only backfills when missing/blank
        # so a job that already carries a real description_snippet from
        # some other source is left alone.
        if not job.get("description_snippet"):
            job["description_snippet"] = job.get("description") or ""
        _enrich_location_from_description(job)
        result, priority, unsure_reason = _keyword_classify_location_detail(job)
        if result == "match":
            job["clearance"] = "regex"
            job["location_priority"] = priority
            matched.append(job)
            confidences.append("match")
        elif result == "unsure":
            url = job.get("url") or ""
            if url and url in excluded_urls:
                # Already sent to the LLM and confirmed excluded by a past
                # run (still within the TTL window) — see crawl_i.py's
                # filter_locations for the full reasoning.
                _try_rank4(job)
            else:
                unsure_jobs.append(job)
                unsure_reasons.append(unsure_reason)
        else:
            # Keyword-stage "no_match" — Rank 4 gets one last look before
            # this job is dropped for good (see crawl_i.py's equivalent).
            # Not recorded into new_exclusions — never going to the LLM
            # regardless of caching.
            _try_rank4(job)

    # 2026-09 (Phase 2, "Do all 3") — see crawl_i.py's filter_locations for
    # the full reasoning. Diagnostics now live in the shared
    # location_diagnostics module (baseline-relative spike detection +
    # location_status breakdown) so both pipelines report identically and
    # share one persisted baseline file per ATS/pipeline combination.
    location_diagnostics.report_and_update("CRAWL II", jobs, unsure_jobs, unsure_reasons)

    if unsure_jobs:
        ai_results = ai_classify_locations(unsure_jobs)
        for job, (label, provider_name), unsure_reason in zip(unsure_jobs, ai_results, unsure_reasons):
            # 2026-09: use the ACTUAL provider that classified this job
            # (now returned directly by ai_classify_locations — see its
            # docstring) instead of the literal string "ai", which is what
            # this used to hardcode regardless of whether keyword/regex,
            # Gemini, OpenAI, or NVIDIA made the call. Matches
            # crawl_i.py's filter_locations, which already did this right.
            clearance = provider_name or "ai"
            # 2026-09 (explicit user policy — see crawl_i.py's
            # filter_locations for the full "application questions are a
            # MUST for Rank 3b/4, not Rank 3a" writeup): Rank 3b now
            # requires real "Application Question:" text in
            # description_snippet, mirroring Rank 4's existing requirement
            # — a bare "Remote" field alone proves nothing, and the only
            # place a hidden Hybrid/On-site/country restriction usually
            # surfaces is the application questions.
            has_app_questions = "Application Question:" in (job.get("description_snippet") or "")
            # 2026-09 Phase 2 (explicit user request — see crawl_i.py's
            # filter_locations for the full "regex pass: rank 1 or 2, LLM
            # pass: Rank 3b" policy writeup): the AI is no longer
            # authoritative for PRIORITY_GLOBAL/PRIORITY_AFRICA here
            # either — only a keyword match sets those. An AI match_global/
            # match_africa verdict is kept, but demoted to Rank 3
            # (3a/blank or 3b/bare_remote, same split as crawl_i.py).
            if label in ("match_global", "match_africa") and unsure_reason == "blank":
                job["clearance"] = clearance
                job["location_priority"] = PRIORITY_UNSURE_BLANK
                matched.append(job)
                confidences.append("uncertain")
            elif (label in ("match_global", "match_africa") and unsure_reason == "bare_remote"
                    and has_app_questions):
                job["clearance"] = clearance
                job["location_priority"] = PRIORITY_UNSURE_SILENT
                matched.append(job)
                confidences.append("uncertain")
            elif (label in ("match_global", "match_africa") and unsure_reason == "global_plus_place"
                    and has_app_questions):
                # 2026-10: Global/Worldwide keyword plus a place that neither narrows nor excludes cleanly
                # ("Worldwide - US"); see crawl_i.py. Only a real AI match keeps it, at Rank 3b.
                job["clearance"] = clearance
                job["location_priority"] = PRIORITY_UNSURE_SILENT
                matched.append(job)
                confidences.append("uncertain")
            elif (label == "uncertain" and unsure_reason == "bare_remote" and has_app_questions
                    and provider_name is not None):
                # 2026-09 policy, refined per explicit user follow-up —
                # see crawl_i.py's filter_locations for the full
                # reasoning. Short version: the location field explicitly
                # said "Remote" — a real signal from the company — so a
                # GENUINE AI review that comes back uncertain is kept at
                # PRIORITY_UNSURE. A BLANK location field can't be told
                # apart from a scraper extraction bug (confirmed live
                # twice this week — see ats_scrapers.py's scrape_jazzhr/
                # scrape_successfactors fixes), so it no longer gets kept
                # on "AI looked and still couldn't tell" alone — see the
                # "blank" case in the comment below.
                #
                # 2026-10 POLICY SUPERSESSION (explicit user instruction —
                # see crawl_i.py's filter_locations for the full
                # writeup): the `provider_name is not None` requirement
                # this branch dropped in 2026-09 (so a never-reviewed job
                # was kept identically to a genuinely-uncertain one) is
                # back, now that ai_classify_locations() runs a
                # last-chance retry (wait, then one forced attempt
                # bypassing the circuit breaker) before ever returning
                # provider_name=None for good. A job that still comes back
                # unreviewed after that real retry has had its fair shot
                # and failed it — falls through to the `else` below and is
                # discarded, same as any other drop.
                job["clearance"] = clearance
                job["location_priority"] = PRIORITY_UNSURE_SILENT
                matched.append(job)
                confidences.append("uncertain")
            else:
                # "no_match" → drop. "uncertain" with unsure_reason ==
                # "blank" → also drop (see above), REGARDLESS of
                # provider_name — a blank location field only survives
                # via a real match_global/match_africa AI verdict, never
                # on genuine (or missing) AI uncertainty alone. A
                # bare_remote job with no application questions at all
                # also lands here now (see has_app_questions above) — Rank
                # 4 requires the same "Application Question:" marker
                # anyway, so this is the correct final drop for it either
                # way. Rank 4 gets one last look before the drop is final.
                before = len(matched)
                _try_rank4(job)
                if len(matched) == before:
                    # 2026-10 (explicit user request — see crawl_i.py's
                    # filter_locations for the full reasoning): never
                    # cache this as an exclusion when provider_name is
                    # None — no AI ever actually reviewed this job, even
                    # after the last-chance retry, so caching it
                    # identically to a genuine negative verdict would make
                    # a future run (possibly with healthy providers again)
                    # silently skip the LLM call for this URL for the full
                    # 21-day TTL.
                    if provider_name is not None:
                        url = job.get("url")
                        if url:
                            new_exclusions.add(url)

    return matched, confidences


# ── Shared batch driver ─────────────────────────────────────────────────

async def crawl_batch_ii(pages: list[dict], session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                          stats: dict, crawl_start: float, time_budget_seconds: float,
                          time_budget_minutes: int, parse_pool: concurrent.futures.Executor,
                          batch_size: int = BATCH_SIZE,
                          excluded_urls: dict | None = None,
                          new_exclusions: set | None = None) -> tuple[int, int, bool, dict]:
    """Crawls archive_ii pages, then classifies and writes everything ONCE
    at the end. Returns (pages_done, jobs_added, time_budget_hit,
    report_stats) — report_stats is {total_jobs_raw, csm_roles,
    global_jobs, duplicates}, for _run_shard's bump_scan_report() call
    (2026-09 — see that function's docstring; Crawl II previously wrote no
    scan_reports rows at all).

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
    retry_pages: list[dict] = []
    time_budget_hit = False
    i = 0
    report_stats = {"total_jobs_raw": 0, "csm_roles": 0, "global_jobs": 0, "duplicates": 0}

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
        all_pages_with_roles |= {p["website_url"] for p, page_jobs in zip(batch, results) if page_jobs or p.get("_had_postings")}
        for p in batch:
            p.pop("_had_postings", None)
        retry_pages.extend(p for p in batch if p.pop("_retry", False))

        done = min(i + batch_size, len(pages))
        elapsed = time.monotonic() - crawl_start
        rate = stats["requests_attempted"] / elapsed if elapsed > 0 else 0
        log.info(f"  {done}/{len(pages)} pages checked — {rate:.1f}/sec — {elapsed:.0f}s so far — "
                 f"{len(batch_candidates)} postings found this batch ({len(all_candidate_jobs)} total)")

    pages_done = min(len(pages), i + batch_size) if pages else 0

    if retry_pages and time.monotonic() - crawl_start < time_budget_seconds:
        log.info(f"── Retry pass: {len(retry_pages)} pages failed with a transient error "
                 f"(timeout/DNS/connection) — retrying at concurrency {RETRY_CONCURRENCY} ──")
        retry_sem = asyncio.Semaphore(RETRY_CONCURRENCY)
        unreachable_before = stats["page_unreachable"]
        recovered_with_postings = 0
        for j in range(0, len(retry_pages), batch_size):
            if time.monotonic() - crawl_start >= time_budget_seconds:
                log.warning(f"  time budget reached during retry pass at {j}/{len(retry_pages)}")
                break
            rbatch = retry_pages[j:j + batch_size]
            rresults = await asyncio.gather(
                *(extract_postings_from_page(session, retry_sem, p, stats, parse_pool) for p in rbatch))
            for p, page_jobs in zip(rbatch, rresults):
                p.pop("_retry", None)
                if not page_jobs and p.pop("_had_postings", None):
                    all_pages_with_roles.add(p["website_url"])
                if page_jobs:
                    recovered_with_postings += 1
                    all_candidate_jobs.extend(page_jobs)
                    all_pages_with_roles.add(p["website_url"])
        still_failing = stats["page_unreachable"] - unreachable_before
        stats["retry_attempted"] = len(retry_pages)
        stats["retry_still_unreachable"] = still_failing
        stats["retry_recovered_with_postings"] = recovered_with_postings
        log.info(f"  retry pass done: {len(retry_pages) - still_failing}/{len(retry_pages)} pages fetched this time, "
                 f"{recovered_with_postings} of them with postings")

    if all_pages_with_roles:
        touch_archive_ii_last_seen(all_pages_with_roles)

    report_stats["total_jobs_raw"] = len(all_candidate_jobs)

    if not all_candidate_jobs:
        log.info("No job postings found — nothing to do.")
        return pages_done, 0, time_budget_hit, report_stats

    log.info("── Deduplication ──")
    # 2026-10 (pipeline audit): see crawl_i.py's matching block and
    # revalidate.py's module docstring — already-stored rows classified
    # under older rules get one deterministic, veto-only re-check.
    known_meta = get_known_jobs_meta()
    existing_urls = UrlSet(known_meta) if known_meta else get_existing_urls()
    new_jobs, already_seen = split_known_jobs(all_candidate_jobs, existing_urls)
    if already_seen:
        stale, rest = revalidate.select_stale(already_seen, known_meta, CLASSIFIER_VERSION)
        if stale:
            log.info(f"── Re-validation ({len(stale)} stored postings classified under older rules "
                     f"than v{CLASSIFIER_VERSION}) ──")
            for job in stale:
                # Same prep _filter_locations does before classifying: this
                # file's extractor only sets job["description"], while every
                # shared classifier function reads description_snippet.
                if not job.get("description_snippet"):
                    job["description_snippet"] = job.get("description") or ""
                _enrich_location_from_description(job)
            stale = await enrich_application_questions_async(stale)
            results = revalidate.evaluate(stale, known_meta)
            revalidate.summarize(results, "Crawl II")
            veto_jobs, stamp_jobs, retry_jobs = revalidate.plan(results)
            mark_jobs_vetoed(veto_jobs)
            touch_seen_jobs_raw(stamp_jobs, classifier_version=CLASSIFIER_VERSION)
            touch_seen_jobs_raw(rest + retry_jobs)
        else:
            touch_seen_jobs_raw(already_seen)
    report_stats["duplicates"] = len(already_seen)
    log.info(f"  Found {len(all_candidate_jobs)} postings: {len(already_seen)} already in the "
             f"database (skipped), {len(new_jobs)} new — only the new ones get reviewed")

    if not new_jobs:
        log.info("No new postings to review.")
        return pages_done, 0, time_budget_hit, report_stats

    log.info("── Role check (is this a CSM/AM role?) ──")
    role_matched = _filter_roles(new_jobs)
    report_stats["csm_roles"] = len(role_matched)
    log.info(f"  {len(new_jobs)} postings checked → {len(role_matched)} are CSM/AM roles")
    if not role_matched:
        return pages_done, 0, time_budget_hit, report_stats

    # Cost gate (see classifier.location_prefilter_keep): skip the application-question fetch for jobs whose
    # structured location already makes them certain rejects.
    role_matched, gated = prefilter_jobs_by_location(role_matched)
    log.info(f"  location pre-gate: {gated} role matches are certain rejects on their location alone "
             f"-- skipping their question fetch ({len(role_matched)} left)")
    if not role_matched:
        return pages_done, 0, time_budget_hit, report_stats

    log.info("── Location check (open to global/Africa hires?) ──")
    # Fetch application questions for EVERY role-matched job (2026-09 ROUND
    # 2, explicit user instruction — see the import comment above). Work
    # authorization / visa screening questions help both the keyword
    # classifier's hard overrides and the AI stage catch country-restricted
    # roles that the bare description text alone wouldn't reveal.
    log.info("  enriching application questions across all ATS platforms...")
    # 2026-09 BUG FIX (real production crash: "RuntimeError: asyncio.run()
    # cannot be called from a running event loop", shard aborted entirely):
    # crawl_batch_ii is itself `async def`, already running inside
    # _run_shard's own asyncio.run() -- calling the sync
    # enrich_application_questions() wrapper (which starts its OWN nested
    # asyncio.run()) from here always raised. Awaiting the async core
    # directly is the fix; see that function's own BUG FIX note.
    role_matched = await enrich_application_questions_async(role_matched)

    global_jobs, confidences = _filter_locations(role_matched, excluded_urls=excluded_urls,
                                                  new_exclusions=new_exclusions)
    report_stats["global_jobs"] = len(global_jobs)
    log.info(f"  {len(role_matched)} roles checked → {len(global_jobs)} are eligible")
    if not global_jobs:
        return pages_done, 0, time_budget_hit, report_stats

    for job in global_jobs:
        job["visa_sponsorship"] = detect_visa_sponsorship(job)

    log.info("── Writing to Supabase ──")
    added, _inserted_rows = add_jobs_batch(global_jobs, confidences, source_pipeline=SOURCE_PIPELINE,
                                            existing_urls=existing_urls)
    log.info(f"  {added} new jobs written")

    # `duplicates` now counts BOTH kinds, same convention as crawl_i.py/
    # crawl_iii.py: pre-classification skips (already_seen) and any
    # post-classification re-matches (global_jobs that still weren't a
    # true first-insert).
    report_stats["duplicates"] += (len(global_jobs) - added)

    return pages_done, added, time_budget_hit, report_stats


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
    explicit 30-day instruction rather than left as a standing exception.

    2026-09 (second change): dropped again, from 31 to 3 — same explicit
    instruction and same reasoning as crawl_i.py's run_finalize (three
    consecutive misses on a roughly-daily run means the job is gone, not
    just unconfirmed for a month). inactive_days matched to 3 as well for
    the same reason given there — see crawl_i.py's run_finalize docstring
    for the full explanation of why inactive_days has to move with
    delete_days here, not stay at 30."""
    log.info("=" * 60)
    log.info("CRAWL II — finalize (cleanup stale jobs)")
    log.info("=" * 60)
    summary = cleanup_stale_jobs(inactive_days=3, delete_days=3, source_pipeline=SOURCE_PIPELINE)
    log.info(f"Crawl II finalize summary: inactive cutoff {summary['inactive_cutoff']} "
             f"(ok={summary['mark_inactive_ok']}), delete cutoff {summary['delete_cutoff']} "
             f"(ok={summary['delete_ok']})")
    # 2026-09: closes out today's single scan_reports row for crawl_ii —
    # finished_at + status='completed' (unless a shard already marked it
    # 'failed' via bump_scan_report).
    finish_scan_report_for_pipeline(SOURCE_PIPELINE)


def merge_excluded_cache() -> None:
    """Excluded-jobs cache finalize pass for Crawl II — call ONCE, after
    every Crawl II shard has finished (same timing as run_finalize() above,
    called right alongside it from postfix_notion.py). See crawl_i.py's
    merge_excluded_cache for the full design — identical logic, just this
    crawl's own separate "Excluded 2" file."""
    count = excluded_cache.merge_excluded_caches(
        existing_path=EXCLUDED_CACHE_PATH,
        shard_glob=EXCLUDED_CACHE_SHARD_GLOB,
        output_path=EXCLUDED_CACHE_PATH,
    )
    log.info(f"Crawl II excluded-cache finalize: {count} URLs cached as known-excluded")


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
        bump_scan_report(SOURCE_PIPELINE)
        return

    stats = {
        "requests_attempted": 0, "fetched_ok": 0, "http_error": 0, "status_404": 0,
        "non_html": 0, "timeout": 0, "unreachable": 0,
        "page_unreachable": 0, "jsonld_pages": 0, "jsonld_postings": 0,
        "heuristic_pages": 0, "heuristic_postings": 0, "no_postings_found": 0,
        "apply_page_augmented": 0,
        "career_link_follow_attempted": 0, "career_link_follow_found_postings": 0,
        "pagination_pages_followed": 0, "pagination_extra_postings": 0,
        "microdata_pages": 0, "state_json_pages": 0, "structured_postings": 0, "detail_enriched": 0,
        "role_prefilter_skipped": 0, "frames_followed": 0, "frame_found_postings": 0,
        "ats_bridge_pages": 0, "ats_bridge_postings": 0, "feed_found_postings": 0, "wp_api_found_postings": 0,
        "single_page_postings": 0, "sitemap_pages_with_candidates": 0, "sitemap_postings": 0,
    }
    sem = asyncio.Semaphore(CRAWL_CONCURRENCY)
    connector = new_connector()
    crawl_start = time.monotonic()
    time_budget_seconds = TIME_BUDGET_MINUTES * 60
    # Shared ThreadPoolExecutor for every CPU-bound parse call this shard
    # makes (see extract_postings_from_page's docstring) — same
    # new_parse_pool() node.py's own crawl engine uses, but sized to THIS
    # file's own (now higher) fetch concurrency via the explicit override
    # (see PARSE_POOL_WORKERS above) rather than node.py's shared
    # PARSE_WORKERS default, which every OTHER new_parse_pool() caller
    # still gets unchanged.
    parse_pool = node.new_parse_pool(max_workers=PARSE_POOL_WORKERS)

    # 2026-09 (explicit user request — see excluded_cache.py's module
    # docstring and crawl_i.py's _run_pipeline for the full design).
    excluded_urls = excluded_cache.load_excluded_cache(EXCLUDED_CACHE_PATH)
    new_exclusions: set = set()

    try:
        try:
            async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.DummyCookieJar()) as session:
                done, added, time_budget_hit, report_stats = await crawl_batch_ii(
                    pages, session, sem, stats, crawl_start, time_budget_seconds,
                    TIME_BUDGET_MINUTES, parse_pool,
                    excluded_urls=excluded_urls, new_exclusions=new_exclusions)
        finally:
            parse_pool.shutdown(wait=False)
            excluded_cache.save_new_exclusions(_excluded_cache_shard_path(shard), new_exclusions)
    except Exception as e:
        # 2026-09: crawl_i.py's/crawl_iii.py's _run_pipeline() have always
        # had this outer try/except to mark a shard's contribution
        # 'failed' in scan_reports rather than silently vanishing from the
        # daily summary — crawl_ii.py never had one at all (no scan_reports
        # writes here before this same 2026-09 change), added now for
        # parity now that this shard's numbers feed the shared row.
        log.error(f"Crawl II shard failed: {e}")
        bump_scan_report(SOURCE_PIPELINE, status="failed")
        raise

    # 2026-09: reports this shard's OWN contribution — bump_scan_report()
    # atomically adds it into the single (run_date, 'crawl_ii') row every
    # shard shares, rather than creating a new Supabase row per shard (see
    # that function's docstring).
    bump_scan_report(
        SOURCE_PIPELINE,
        total_jobs_raw=report_stats["total_jobs_raw"],
        csm_roles=report_stats["csm_roles"],
        global_jobs=report_stats["global_jobs"],
        new_jobs_added=added,
        duplicates=report_stats["duplicates"],
    )

    status = "STOPPED EARLY (time budget)" if time_budget_hit else "complete"
    log.info("── Summary ──")
    log.info(f"  shard {shard}/{total_shards} {status}: {done}/{len(pages)} pages, "
             f"{added} new jobs written")
    log.info(f"  JSON-LD: {stats['jsonld_pages']} pages, {stats['jsonld_postings']} postings found")
    log.info(f"  Heuristic: {stats['heuristic_pages']} pages, {stats['heuristic_postings']} "
             f"postings confirmed")
    log.info(f"  Unreachable/no-signal: {stats['page_unreachable']} pages unreachable, "
             f"{stats['no_postings_found']} pages with no postings found")
    fail_breakdown = ", ".join(f"{k[5:]}={v}" for k, v in sorted(stats.items()) if k.startswith("fail_"))
    log.info(f"  Fetch failures by kind (every request, incl. link-follow/pagination): {fail_breakdown or 'none'}")
    if stats.get("retry_attempted"):
        log.info(f"  Retry pass: {stats['retry_attempted']} pages retried, {stats['retry_still_unreachable']} "
                 f"still unreachable, {stats['retry_recovered_with_postings']} recovered with postings")
    log.info(f"  Apply-page augmented: {stats['apply_page_augmented']} postings enriched with apply URL")
    log.info(f"  Career-link follow: {stats['career_link_follow_attempted']} links followed, "
             f"{stats['career_link_follow_found_postings']} of those pages had postings")
    log.info(f"  Pagination: {stats['pagination_pages_followed']} extra listing pages followed, "
             f"{stats['pagination_extra_postings']} extra postings found on them")

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
        merge_excluded_cache()
        return

    asyncio.run(_run_shard(args.shard, args.total_shards))


if __name__ == "__main__":
    main()
