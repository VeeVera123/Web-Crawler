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
    detect_visa_sponsorship, PLACEHOLDER_LOC_RE,
    classify_role_category,
    PRIORITY_GLOBAL, PRIORITY_AFRICA, PRIORITY_UNSURE,
)
from supabase_handler import (  # noqa: E402
    add_jobs_batch, cleanup_stale_jobs, get_archive_ii_pages, SupabaseFetchError,
    get_existing_urls, touch_seen_jobs_raw, touch_archive_ii_last_seen,
    log_egress_summary,
)
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
DEFAULT_ATS_LABEL = "in_house"  # jobs.ats value for every Crawl II row — free-text column, no CHECK

CRAWL_CONCURRENCY = int(os.environ.get("CRAWL_II_CONCURRENCY", "60"))
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
    r"Who\s|What\s|We\s|Click|View|Full[- ]?Time|Part[- ]?Time|Posted|Date|Category)\b"
    r"|[.]\s|\n|$)",
    re.I,
)


# 2026-09 BUG FIX #2 (avidtr.com — see _JD_NARROWING_QUALIFIER_RE's note
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


def _confirm_and_build_posting(detail_html: str, candidate: dict, company: str) -> dict | None:
    """A candidate link alone is never trusted — this is the gate that
    keeps a heuristic hit from becoming a written job. Requires BOTH a
    real amount of body text (rules out a soft-404/stub/redirect-to-
    homepage-in-disguise, same failure mode node.py's career-page quality
    gate exists for) AND at least one phrase that specifically reads like
    a job posting, not just any content page of similar length."""
    text = _strip_html(detail_html)
    if len(text) < _MIN_JOB_DETAIL_TEXT_CHARS:
        return None
    text_lower = text.lower()
    if not any(p in text_lower for p in _STRONG_JOB_PAGE_PHRASES):
        return None
    return {
        "title": candidate["title"],
        "url": candidate["url"],
        # 2026-09: try a real regex extraction first (see
        # _extract_heuristic_location) — falls back to blank, exactly the
        # old behavior, only when the page has no recognizable location
        # label at all. classifier.py's "blank → unsure, let the AI stage
        # look at it" path still handles that case unchanged.
        "location": _extract_heuristic_location(text),
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

async def _extract_via_jsonld_or_heuristic(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                                            html: str, page_url: str, company: str, stats: dict,
                                            parse_pool: concurrent.futures.Executor) -> list[dict]:
    """The actual JSON-LD-then-heuristic extraction, factored out of
    extract_postings_from_page so a page reached via the 2026-09 link-
    follow step below gets IDENTICAL treatment to the original page —
    not a second, potentially-drifting copy of the same logic.

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

    jsonld_jobs = await loop.run_in_executor(parse_pool, _extract_jsonld_jobs, html, page_url, company)
    if jsonld_jobs:
        stats["jsonld_pages"] += 1
        stats["jsonld_postings"] += len(jsonld_jobs)
        await asyncio.gather(*(_augment_with_apply_page(session, sem, stats, j, html) for j in jsonld_jobs))
        return jsonld_jobs

    candidates = await loop.run_in_executor(parse_pool, _find_heuristic_candidates, html, page_url)
    if not candidates:
        return []
    stats["heuristic_pages"] += 1
    candidates = candidates[:MAX_HEURISTIC_CANDIDATES_PER_PAGE]

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
    return confirmed


async def _walk_pagination(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                            first_html: str, first_url: str, company: str, stats: dict,
                            parse_pool: concurrent.futures.Executor,
                            first_postings: list[dict]) -> list[dict]:
    """2026-09: a genuine listings page (as opposed to the landing/stub-page
    case MAX_CAREER_LINK_FOLLOW handles above) can itself span multiple
    pages — "Page 1 of 5", a "Next"/"Load more" control. Previously this
    file read exactly one listing page per company and stopped, silently
    missing every posting past page 1. This follows _find_next_page_url
    forward from a page that already yielded at least one posting, merging
    each subsequent page's postings in (deduped by job URL) until: no
    next-page link is found, MAX_ARCHIVE_II_PAGES_PER_LISTING is reached,
    a next link points somewhere already visited (loop guard against a
    self-referential/cyclic pagination scheme), or a page adds zero new
    postings (real pagination always advances; a page that doesn't is
    either the true end or a broken/looping "next" link either way)."""
    all_postings = list(first_postings)
    seen_urls = {p["url"] for p in all_postings if p.get("url")}
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
        if new_count == 0:
            break
        stats["pagination_extra_postings"] += new_count
        current_html, current_url = next_html, next_final_url

    return all_postings


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
    async with sem:
        fetched = await node._fetch_page(session, page["career_page_url"], stats)
    if not fetched:
        stats["page_unreachable"] += 1
        return []
    final_url, html = fetched
    company = _company_name_from_domain(page["website_url"])

    postings = await _extract_via_jsonld_or_heuristic(session, sem, html, final_url, company, stats, parse_pool)
    if postings:
        return await _walk_pagination(session, sem, html, final_url, company, stats, parse_pool, postings)

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
        if postings:
            stats["career_link_follow_found_postings"] += 1
            return await _walk_pagination(
                session, sem, followed_html, followed_url, company, stats, parse_pool, postings)

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
_JD_NARROWING_QUALIFIER_RE = re.compile(
    r"\b(?:US|U\.S\.|USA|U\.S\.A\.|United\s+States|UK|U\.K\.|United\s+Kingdom|Canada|Australia|"
    r"Germany|France|Netherlands|Mexico|Philippines|Nigeria|Kenya|South\s+Africa|India|Ireland|"
    r"Spain|Italy|Brazil|Japan|Singapore|China|Sweden|Norway|Denmark|Finland|Poland|Portugal|"
    r"Switzerland|Austria|Belgium|New\s+Zealand|Israel|UAE|United\s+Arab\s+Emirates|Egypt|Ghana|"
    r"APAC|LATAM|ANZ|NAM|MENA|" + _US_STATES_RE_FRAGMENT + r")\b",
    re.I,
)


def _enrich_location_from_description(job: dict) -> None:
    """Mutates job["location"] in place — see module note above. No-op
    when location already has real content, when the description carries
    none of the narrow phrase set, or when every phrase hit found is
    itself narrowed to one specific country/region right next to it
    (e.g. "work from anywhere in the United States" — see BUG FIX note
    above). In that last case the location is deliberately left bare
    rather than guessed at, so classifier.py's existing blank→"unsure"→AI
    path still gets a look at it instead of being short-circuited."""
    loc = (job.get("location") or "").strip()
    if loc.lower() not in _BARE_LOCATION_VALUES and not PLACEHOLDER_LOC_RE.match(loc):
        return
    desc = job.get("description") or ""
    if not desc:
        return
    for rx in _JD_STRONG_GLOBAL_HIRING_RE:
        for m in rx.finditer(desc):
            start = max(0, m.start() - _JD_QUALIFIER_WINDOW)
            end = min(len(desc), m.end() + _JD_QUALIFIER_WINDOW)
            before, after = desc[start:m.start()], desc[m.end():end]
            if _JD_NARROWING_QUALIFIER_RE.search(before) or _JD_NARROWING_QUALIFIER_RE.search(after):
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


def _filter_locations(jobs: list[dict]) -> tuple[list[dict], list[str]]:
    matched, confidences, unsure_jobs = [], [], []
    # Parallel to unsure_jobs — 'blank' or 'bare_remote', see
    # _keyword_classify_location_detail's docstring and crawl_i.py's
    # filter_locations for the full reasoning on why these are treated
    # differently below.
    unsure_reasons = []
    for job in jobs:
        _enrich_location_from_description(job)
        result, priority, unsure_reason = _keyword_classify_location_detail(job)
        if result == "match":
            job["clearance"] = "regex"
            job["location_priority"] = priority
            matched.append(job)
            confidences.append("match")
        elif result == "unsure":
            unsure_jobs.append(job)
            unsure_reasons.append(unsure_reason)

    # 2026-09 canary — see crawl_i.py's filter_locations for the full
    # reasoning: a blank location is now excluded more strictly, but a
    # single ATS platform's blank-location share spiking is the real
    # early signal that platform's scraper regex just broke, so log it.
    blank_by_ats: dict[str, int] = {}
    total_by_ats: dict[str, int] = {}
    for job in jobs:
        ats_name = job.get("source_ats") or "unknown"
        total_by_ats[ats_name] = total_by_ats.get(ats_name, 0) + 1
    for job, reason in zip(unsure_jobs, unsure_reasons):
        if reason == "blank":
            ats_name = job.get("source_ats") or "unknown"
            blank_by_ats[ats_name] = blank_by_ats.get(ats_name, 0) + 1
    for ats_name, blanks in sorted(blank_by_ats.items(), key=lambda kv: -kv[1]):
        ats_total = total_by_ats.get(ats_name, 0)
        if ats_total >= 10 and blanks / ats_total >= 0.25:
            log.warning(
                f"Location filter: {ats_name} has {blanks}/{ats_total} jobs "
                f"({blanks / ats_total:.0%}) with a BLANK location field this "
                f"run — check whether {ats_name}'s scraper's location regex "
                f"still matches that platform's current HTML/markup."
            )

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
            elif label == "uncertain" and provider_name is not None and unsure_reason == "bare_remote":
                # 2026-09 policy change, refined per explicit user
                # follow-up — see crawl_i.py's filter_locations for the
                # full reasoning. Short version: a job an AI provider
                # ACTUALLY reviewed and still couldn't classify (real
                # provider_name) is kept at PRIORITY_UNSURE; a job no
                # provider ever got to look at at all (provider_name is
                # None — every provider failed/was exhausted/was never
                # reached) is dropped, same as "no_match".
                #
                # 2026-09 (second refinement): that benefit-of-the-doubt
                # now only applies when unsure_reason == "bare_remote" —
                # the location field explicitly said "Remote", a real
                # signal from the company. A BLANK location field can't be
                # told apart from a scraper extraction bug (confirmed live
                # twice this week — see ats_scrapers.py's scrape_jazzhr/
                # scrape_successfactors fixes), so it no longer gets kept
                # on "AI looked and still couldn't tell" alone — see the
                # "blank" case in the comment below.
                job["clearance"] = clearance
                job["location_priority"] = PRIORITY_UNSURE
                matched.append(job)
                confidences.append("uncertain")
            # "no_match" → drop. "uncertain" with no provider_name (never
            # actually reviewed) → also drop. "uncertain" with
            # unsure_reason == "blank" → also drop (see above) — a blank
            # location field only survives via a real match_global/
            # match_africa AI verdict, never on genuine AI uncertainty.

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
        log.info(f"  {done}/{len(pages)} pages checked — {rate:.1f}/sec — {elapsed:.0f}s so far — "
                 f"{len(batch_candidates)} postings found this batch ({len(all_candidate_jobs)} total)")

    pages_done = min(len(pages), i + batch_size) if pages else 0

    if all_pages_with_roles:
        touch_archive_ii_last_seen(all_pages_with_roles)

    if not all_candidate_jobs:
        log.info("No job postings found — nothing to do.")
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
    log.info(f"  Found {len(all_candidate_jobs)} postings: {len(already_seen)} already in the "
             f"database (skipped), {len(new_jobs)} new — only the new ones get reviewed")

    if not new_jobs:
        log.info("No new postings to review.")
        return pages_done, 0, time_budget_hit

    log.info("── Role check (is this a CSM/AM role?) ──")
    role_matched = _filter_roles(new_jobs)
    log.info(f"  {len(new_jobs)} postings checked → {len(role_matched)} are CSM/AM roles")
    if not role_matched:
        return pages_done, 0, time_budget_hit

    log.info("── Location check (open to global/Africa hires?) ──")
    global_jobs, confidences = _filter_locations(role_matched)
    log.info(f"  {len(role_matched)} roles checked → {len(global_jobs)} are eligible")
    if not global_jobs:
        return pages_done, 0, time_budget_hit

    for job in global_jobs:
        job["visa_sponsorship"] = detect_visa_sponsorship(job)

    log.info("── Writing to Supabase ──")
    added, _inserted_rows = add_jobs_batch(global_jobs, confidences, source_pipeline=SOURCE_PIPELINE,
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
        "apply_page_augmented": 0,
        "career_link_follow_attempted": 0, "career_link_follow_found_postings": 0,
        "pagination_pages_followed": 0, "pagination_extra_postings": 0,
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
        return

    asyncio.run(_run_shard(args.shard, args.total_shards))


if __name__ == "__main__":
    main()
