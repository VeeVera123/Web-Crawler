"""
NODE — the one permanent crawl/detect/write engine every seed source
depends on. Seed scripts (people_data_labs_probe.py, host_crawl_v2.py, and any
future one) are thin and disposable: they find domains and hand them to
crawl_batch(). This file is not disposable — fix a bug here once, every
source gets the fix.

Detection runs 3 independent parsing methods per page (href links, raw
URL regex scan, JSON-LD structured data) and 4 fallback tiers if a page
yields nothing (homepage -> career paths -> guessed sitemap paths ->
robots.txt Sitemap: directive). Every candidate URL goes through
discovery.py's URL_TO_SLUG — detection logic lives there, not here.

Country is opportunistic metadata (JSON-LD addressCountry, then
footer/<address> text via geo.py), attached when confidently resolved,
never a gate on whether a hit gets written.
"""
import asyncio
import concurrent.futures
import gzip
import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import aiohttp
from dotenv import load_dotenv
from selectolax.lexbor import LexborHTMLParser

load_dotenv()
_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # geo.py/discovery.py live here

import geo  # noqa: E402
from discovery import URL_TO_SLUG  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("node")

# 2026-08, REVERTED (SIGTERM only — see 2026-09 below for SIGINT):
# a prior version of this file installed a custom Python-level SIGTERM
# handler here (os._exit-based "fast exit"), reasoning that Python's
# default handling might be slow to respond mid-blocking-call. That
# reasoning was backwards for SIGTERM specifically and made cancellation
# WORSE, not better — proven by a live comparison: enrich_slugs.py (a
# separate, plain synchronous script that never touches signal handling
# at all) stops within moments of a Cancel click, while every node.py-
# based crawler (which HAD the custom handler) did not.
#
# The actual mechanics: SIGTERM's real OS default disposition (SIG_DFL)
# is an unconditional KERNEL-LEVEL terminate — the kernel kills the
# process outright, with zero cooperation required from the Python
# interpreter, no matter what the process is doing at that instant
# (deep in a C extension, blocked on I/O, anything). Python does NOT
# override SIGTERM's default at startup. By installing our OWN
# Python-level SIGTERM handler, we threw away that free, unconditional,
# instant kill and replaced it with one that can only run once the
# interpreter's bytecode eval loop gets a chance to check for pending
# signals. Fix: leave SIGTERM's disposition alone (SIG_DFL) — nothing
# below touches it.
#
# 2026-09, SIGINT — different signal, different problem, NOT the same
# mistake as above: GitHub Actions' "Cancel workflow" was traced (with a
# live test — a fresh run cancelled via the UI, watched directly, still
# running 6+ minutes later, had to be force-cancelled) to send SIGINT
# first, not SIGTERM — and unlike SIGTERM, Python installs its OWN
# default handler for SIGINT unconditionally at startup, converting it to
# a KeyboardInterrupt raised in the main thread. There is no "raw kernel
# default" being lost here the way there was for SIGTERM — Python always
# intercepts SIGINT, default or not. The problem is what that default
# interception DOES: the KeyboardInterrupt propagates up through every
# `finally`/`async with` block on the way out — including
# parse_pool.shutdown(wait=True) below and aiohttp's ClientSession
# teardown with however many requests are in flight — and those can each
# block for a long time. A handler installed here that just calls
# os._exit() immediately runs INSTEAD of that unwind, not after it — an
# outer try/except KeyboardInterrupt around asyncio.run() does NOT help,
# because by the time that outer except is reached the slow teardown has
# already run (exceptions propagate through intervening finally blocks
# on their way up, they don't skip them). This still can't do anything
# about signal delivery arriving late from GitHub's own infrastructure
# (cancellation is relayed to the runner asynchronously, not instant) —
# that's a real, separate, unavoidable latency; force-cancel remains the
# manual fallback for it. What this DOES fix is every case where the
# signal arrives and the process is anywhere Python's bytecode loop can
# see it (which is most of the time — this is the same limitation
# _run_with_deadline's cancellation and the ThreadPoolExecutor parse
# stragglers already have, not a new one).
def _hard_exit_on_sigint(signum, frame):
    os._exit(130)


signal.signal(signal.SIGINT, _hard_exit_on_sigint)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
ARCHIVE_I_TABLE = "archive_i"  # was slug_registry — the trusted, actually-scraped-daily list.
ARCHIVE_II_TABLE = "archive_ii"  # was archive_iii — in-house/unsupported career pages.
CHECKPOINT_TABLE = "crawl_checkpoints"  # 2026-08: per-shard resume — see save/load/clear_crawl_checkpoint below.
# 2026-08 restructure: the OLD archive_ii (an ATS-match staging/quarantine
# table that a separate verify step promoted into slug_registry) is GONE —
# dropped entirely. ATS hits now write directly to ARCHIVE_I_TABLE with no
# intermediate verify/promote step. See node.py's module docstring and
# Verification/verification.py's docstring for what still removes rows
# (dead-link confirmation only, never "0 jobs right now").

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10, connect=6)
MAX_PAGE_BYTES = 3_000_000  # safety cap, not a realistic limit

CRAWL_CONCURRENCY = int(os.environ.get("CRAWL_CONCURRENCY", "400"))
CONNECTOR_LIMIT = int(os.environ.get("CRAWL_CONNECTOR_LIMIT", str(CRAWL_CONCURRENCY + 150)))
# 2026-08: PARSE_WORKERS used to size a ProcessPoolExecutor at cpu_count-1 —
# on GitHub's standard ubuntu-latest runners (4 vCPUs) that's only THREE
# workers, meaning up to CRAWL_CONCURRENCY (400) concurrent async fetches per
# shard all queued behind just 3 processes for HTML parsing — the real
# bottleneck, not the network layer (which was already async/aiohttp the
# whole time). Switched new_parse_pool() to a ThreadPoolExecutor instead:
# selectolax's C-level parsing releases the GIL for the actual parse work,
# and threads skip the pickling overhead of shipping multi-MB HTML strings
# across a process boundary that ProcessPoolExecutor required. Threads are
# cheap, so the default is no longer tied to cpu_count — a fixed, generous
# worker count (overridable via PARSE_WORKERS) removes the 3-worker
# queueing bottleneck without needing more real CPU cores than the runner has.
PARSE_WORKERS = int(os.environ.get("PARSE_WORKERS", "16"))
TIME_BUDGET_MINUTES = int(os.environ.get("CRAWL_TIME_BUDGET_MINUTES", "330"))

# Expanded 2026-08 to cover every common phrasing seen across company
# sites, not just the half-dozen most frequent ones — this list also
# feeds CAREER_LIKE_RE below (used to filter sitemap URLs), so a wider
# list improves BOTH the direct-guess tier and the sitemap tier at once.
CAREER_PATHS = [
    "/careers", "/career", "/careers-home", "/careers-and-jobs",
    "/jobs", "/job-openings", "/open-positions", "/open-roles", "/open-jobs",
    "/openings", "/current-openings", "/vacancies", "/vacancy", "/job-search",
    "/find-a-job", "/positions", "/opportunities",
    "/join-us", "/join", "/join-our-team", "/join-the-team",
    "/work-with-us", "/work-for-us", "/work-here",
    "/about/careers", "/about-us/careers", "/company/careers", "/company/jobs",
    "/about/jobs", "/about-us/jobs", "/team/careers",
    "/hiring", "/we-are-hiring", "/now-hiring", "/were-hiring",
    "/employment", "/employment-opportunities",
    "/recruitment", "/recruiting", "/talent",
    "/apply", "/apply-now",
]
# 2026-08: wrapped in \b...\b (word-boundary anchored) — the unanchored
# version matched as a raw substring ANYWHERE in a sitemap URL, so
# "unemployment" matched via "employment", and "joints" matched via
# "join" (e.g. a blog post at /blog/3-pizza-joints-near-me was pulled in
# as a "career-like" URL purely because of that substring). Confirmed via
# a real archive_iii audit — see _BLOG_LIKE_PATH_RE and
# _looks_like_sentence_slug below for the other two fixes from that
# same audit; this one alone doesn't catch every case (a blog post
# titled "you-bet-your-career" genuinely contains the whole word
# "career"), which is why those two additional checks exist.
CAREER_LIKE_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(p.strip("/")).replace("-", "-?") for p in CAREER_PATHS) + r")\b",
    re.I)
SITEMAP_MAX_FOLLOW = 8
SITEMAP_INDEX_PATHS = ("/sitemap.xml", "/sitemap_index.xml")

# 2026-08 archive_iii audit: sitemap URLs shaped like a blog post, news
# article, or press release — even ones CAREER_LIKE_RE legitimately
# matches on a real word (e.g. "...why-hiring-a-property-manager-is-
# smart", "...how-to-stand-out-in-energy-recruitment") — are almost never
# real career pages; they're editorial content that happens to discuss
# hiring/recruiting as a TOPIC. Filtering these out before they're even
# fetched also saves the request.
_BLOG_LIKE_PATH_RE = re.compile(
    r"/(?:blog|news|press|media|insights|articles?|resources|case-studies)/"
    r"|/\d{4}/\d{1,2}(?:/\d{1,2})?/", re.I)


def _looks_like_sentence_slug(path: str, max_words: int = 6) -> bool:
    """2026-08 archive_iii audit: a real career page's path is a short
    page NAME ("careers", "current-openings", "join-our-team" — 1-3
    hyphen-separated words). An article/blog TITLE used as a slug runs
    much longer ("sap-successfactors-talent-modules-implementation-for-
    global-organizations" — 9 words; confirmed real false-positive from
    an HR-consulting company's own site, not caught by the blog-path
    check above since it wasn't under /blog/). True means "too long,
    reads like a sentence, reject it" — checked against the LAST
    non-empty path segment, since that's where the descriptive title
    lives (earlier segments are usually just category folders)."""
    segments = [s for s in path.strip("/").split("/") if s]
    if not segments:
        return False
    words = [w for w in segments[-1].split("-") if w]
    return len(words) > max_words

# archive_iii quality gate (2026-08): a career-path/sitemap fetch that
# 200'd isn't automatically "a real career page" — plenty of sites soft-
# redirect any unknown path back to the homepage with a 200 status, or
# serve a near-empty stub, or just happen to have SOME unrelated content
# (a blog post, a generic "solutions" page) sitting at a guessed path
# like /join or /opportunities. A page must clear the text length AND not
# be the homepage in disguise AND actually read like a jobs page to count
# as a genuine in-house career page candidate. None of this applies to
# ATS-matched hits — a real ATS link found on a page is trusted regardless
# of surrounding text; this gate exists only for the no-known-ATS branch.
MIN_CAREER_PAGE_TEXT_CHARS = 250

# Hiring-vocabulary check (2026-08): deliberately NOT role-specific (no
# "customer success"/"account management" here — that's ats_scrapers.py's
# job later, once archive_iii's candidates have been reviewed). This only
# answers "does this page actually read like it's about jobs at all,"
# which a bare text-length check can't tell apart from an unrelated page
# of similar length. STRONG phrases are specific enough that finding just
# one is real evidence on its own; WEAK single words are common enough
# elsewhere that two DISTINCT ones are required together before they
# count (one incidental word shouldn't be enough).
_STRONG_HIRING_PHRASES = [
    "current openings", "current opening", "current vacancies", "current vacancy",
    "open positions", "open position", "open roles", "open role",
    "job openings", "job opening", "we're hiring", "we are hiring", "now hiring",
    "join our team", "join the team", "join our growing team",
    "submit your application", "submit an application", "submit your resume",
    "send us your resume", "send your resume", "send your cv", "submit your cv",
    "employment opportunities", "career opportunities", "job opportunities",
    "view openings", "view our openings", "view current openings", "view all jobs",
    "see our openings", "browse openings", "browse our jobs", "browse open positions",
    "explore careers", "explore our careers", "explore open positions",
    "search jobs", "search openings", "search open positions",
    "find your next role", "find a job", "meet our hiring team",
    "equal opportunity employer", "we are an equal opportunity employer",
    "join us and", "grow your career with us", "build your career with us",
]
_WEAK_HIRING_WORDS = [
    "career", "careers", "job", "jobs", "position", "positions",
    "vacancy", "vacancies", "hiring", "recruit", "recruiting", "recruitment",
    "recruiter", "talent", "opening", "openings", "employment",
    "internship", "internships", "apprenticeship", "apprenticeships",
    "resume", "cv", "candidate", "candidates", "applicant", "applicants",
    "onboarding", "workforce", "headcount",
]
# "apply now"/"apply today"/"apply here"/"apply online" moved from STRONG to
# WEAK 2026-09: a real false positive was traced to these — delaware.pro's
# customer-experience SERVICES page (not a jobs page at all) has a global
# nav "Apply Now" link pointing at a startup/ventures program, and that one
# generic phrase alone was enough to get the page captured into archive_ii
# as a career page. Unlike "current openings"/"we're hiring"/etc., "apply
# now" is common site-wide boilerplate CTA text with no hiring-specific
# meaning on its own (loan applications, course enrollment, accelerator
# programs all use it too) — it shouldn't single-handedly qualify a page.
# As a weak word it still counts fine: a genuine career page pairs it with
# other distinct hiring vocabulary ("careers", "position", "job", ...)
# easily enough to clear the 2-distinct-weak-word bar below.
_WEAK_HIRING_PHRASES = ["apply now", "apply today", "apply here", "apply online"]
_STRONG_HIRING_RE = re.compile("|".join(re.escape(p) for p in _STRONG_HIRING_PHRASES), re.I)
_WEAK_HIRING_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(w) for w in _WEAK_HIRING_WORDS + _WEAK_HIRING_PHRASES) + r")\b",
    re.I,
)


def _has_hiring_vocabulary(text: str) -> bool:
    """True if the page's visible text actually reads like a jobs page —
    one STRONG phrase is sufficient on its own; otherwise at least two
    DIFFERENT weak words are required (guards against one incidental word
    — e.g. a single stray "career" in an About Us blurb — being enough)."""
    if _STRONG_HIRING_RE.search(text):
        return True
    weak_hits = {m.group(0).lower() for m in _WEAK_HIRING_RE.finditer(text)}
    return len(weak_hits) >= 2

ACCEPT_ANY_COUNTRY = set(geo.COUNTRY_ALIASES.values()) | set(geo.COUNTRY_CONTINENT.keys())

# PDL's country-column spelling doesn't always match geo.py's canonical
# output 1:1 — only known mismatch found so far.
_TO_GEO_COUNTRY_OVERRIDES = {"czechia": "Czech Republic"}


def target_countries_geo_form(style_countries: set[str]) -> set[str]:
    """Converts a lowercase country-name set into geo.py's canonical
    Title-Case spelling, for comparison against detect_country()'s output."""
    return {_TO_GEO_COUNTRY_OVERRIDES.get(c, c.title()) for c in style_countries}


# ── URL extraction / cleanup ────────────────────────────────────────────

_URL_RE = re.compile(r'https?://[^\s"\'<>\\]{4,300}', re.I)
_MAX_CANDIDATE_URLS_PER_PAGE = 4000

# _URL_RE stops at a literal quote but not an HTML-encoded one (&quot;)
# or a curly/smart quote — both show up right after a URL sitting inside
# an already-entity-escaped JS/JSON blob, and can run on for hundreds of
# chars past the real URL end.
_HTML_ENTITY_RE = re.compile(r'&(?:quot|apos|amp|gt|lt|nbsp|#[0-9]+|#x[0-9a-fA-F]+);', re.I)
_CURLY_QUOTE_RE = re.compile(r'[“”‘’]')
_TRAILING_STATUS_CODE_RE = re.compile(r'(?:;[0-9]{2,4})+;?$')  # e.g. ';307;'


def _clean_extracted_url(url: str) -> str:
    """Cut at the first entity/curly-quote occurrence (not just trim the
    end — an entity-encoded blob can run on for a long time), then
    iteratively strip ordinary trailing junk (comma/semicolon, a stray
    status-code fragment, unbalanced closing brackets)."""
    m = _HTML_ENTITY_RE.search(url)
    if m:
        url = url[:m.start()]
    m = _CURLY_QUOTE_RE.search(url)
    if m:
        url = url[:m.start()]
    prev = None
    while prev != url:
        prev = url
        url = url.strip().rstrip(",;")
        url = _TRAILING_STATUS_CODE_RE.sub("", url)
        for open_c, close_c in (("(", ")"), ("[", "]"), ("{", "}")):
            while url.endswith(close_c) and url.count(open_c) < url.count(close_c):
                url = url[:-1]
    return url


def _extract_candidate_urls(html: str, base_url: str) -> set[str]:
    """Method A (parsed <a href>) + B (raw-text URL regex scan, catches
    links in <script>/JS/iframe src that A misses)."""
    urls: set[str] = set()
    try:
        tree = LexborHTMLParser(html)
        for node in tree.css("a[href]"):
            href = node.attributes.get("href")
            if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            try:
                urls.add(_clean_extracted_url(urljoin(base_url, href)))
            except ValueError:
                continue
            if len(urls) >= _MAX_CANDIDATE_URLS_PER_PAGE:
                break
    except Exception:
        pass  # a malformed page shouldn't kill the crawl of this company
    for m in _URL_RE.finditer(html):
        urls.add(_clean_extracted_url(m.group(0)))
        if len(urls) >= _MAX_CANDIDATE_URLS_PER_PAGE:
            break
    return urls


def _walk_json_strings(obj, depth: int = 0):
    if depth > 12:
        return
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _walk_json_strings(v, depth + 1)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_json_strings(v, depth + 1)


def _extract_jsonld_urls(html: str) -> set[str]:
    """Method C: parses <script type=application/ld+json> as real JSON and
    walks it for URL-shaped values — catches values a raw-text scan could
    mangle (escaped chars, values split across fields)."""
    urls: set[str] = set()
    try:
        tree = LexborHTMLParser(html)
        for node in tree.css('script[type="application/ld+json"]'):
            text = node.text(strip=True)
            if not text:
                continue
            try:
                parsed = json.loads(text)
            except (ValueError, TypeError):
                continue
            for s in _walk_json_strings(parsed):
                if s.startswith("http://") or s.startswith("https://"):
                    urls.add(s)
                    if len(urls) >= _MAX_CANDIDATE_URLS_PER_PAGE:
                        return urls
    except Exception:
        pass
    return urls


def _detect_ats_hits(urls: set[str]) -> list[tuple[str, str, str]]:
    """Every candidate URL through discovery.py's real URL_TO_SLUG
    converters — the one trusted detection path, not reinvented here."""
    hits = []
    seen = set()
    for url in urls:
        for ats, converter in URL_TO_SLUG.items():
            try:
                slug = converter(url)
            except Exception:
                continue
            if slug:
                key = (ats, slug)
                if key not in seen:
                    seen.add(key)
                    hits.append((ats, slug, url))
    return hits


# ── country detection ───────────────────────────────────────────────────

def _extract_address_zone_text(html: str) -> str:
    """Only <footer>/<address> text — scanning the whole page risks false
    matches from incidental mentions (blog posts, "we ship to 40 countries")."""
    try:
        tree = LexborHTMLParser(html)
        parts = []
        for tag in ("footer", "address"):
            for node in tree.css(tag):
                text = node.text(separator=" ", strip=True)
                if text:
                    parts.append(text)
        return " | ".join(parts)
    except Exception:
        return ""


def _walk_for_address_country(obj, depth: int = 0) -> str | None:
    if depth > 12:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k.lower() == "addresscountry" and isinstance(v, str) and v.strip():
                return v.strip()
        for v in obj.values():
            result = _walk_for_address_country(v, depth + 1)
            if result:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _walk_for_address_country(item, depth + 1)
            if result:
                return result
    return None


def detect_country(html: str, target_geo_countries: set[str]) -> tuple[str | None, str | None]:
    """Tier 1: JSON-LD addressCountry (highest confidence, explicit and
    trusted outright if it resolves to exactly one country — stops here
    even on a confident non-target match rather than letting a weaker
    footer signal overrule it). Tier 2: footer/<address> text via geo.py's
    extract_countries(), trusted only if exactly one country is found.
    Deliberately no ccTLD/IP geolocation — both proven unreliable for a
    company's actual HQ (ccTLD is global-.com-dominated, IP reflects the
    CDN edge node, not the company)."""
    jsonld_country = None
    try:
        tree = LexborHTMLParser(html)
        for node in tree.css('script[type="application/ld+json"]'):
            text = node.text(strip=True)
            if not text:
                continue
            try:
                parsed = json.loads(text)
            except (ValueError, TypeError):
                continue
            raw = _walk_for_address_country(parsed)
            if raw:
                resolved = geo.extract_countries(raw)
                if len(resolved) == 1:
                    jsonld_country = next(iter(resolved))
                    break
    except Exception:
        pass
    if jsonld_country:
        if jsonld_country in target_geo_countries:
            return jsonld_country, "jsonld"
        return None, None
    zone_text = _extract_address_zone_text(html)
    if zone_text:
        resolved = geo.extract_countries(zone_text)
        if len(resolved) == 1:
            only = next(iter(resolved))
            if only in target_geo_countries:
                return only, "footer_address"
    return None, None


def _extract_visible_text(html: str) -> str:
    """Visible body text, reusing the tree already being parsed in
    _parse_detect (no extra fetch/parse pass) — feeds both the text-length
    check and the hiring-vocabulary check, so it's deliberately cheap and
    approximate rather than a full readability-style extraction."""
    try:
        tree = LexborHTMLParser(html)
        body = tree.css_first("body")
        return body.text(strip=True) if body else tree.root.text(strip=True)
    except Exception:
        return ""


def _parse_detect(html: str, base_url: str, target_geo_countries: set[str]
                   ) -> tuple[list[tuple[str, str, str]], str | None, str | None, int, bool]:
    """CPU-bound per-page work, offloaded to new_parse_pool()'s
    ThreadPoolExecutor via loop.run_in_executor() so it never blocks the
    event loop while other domains' fetches are in flight (see
    crawl_one/PARSE_WORKERS)."""
    urls = _extract_candidate_urls(html, base_url) | _extract_jsonld_urls(html)
    hits = _detect_ats_hits(urls)
    country, method = detect_country(html, target_geo_countries)
    text = _extract_visible_text(html)
    text_len = len(text)
    has_hiring_vocab = _has_hiring_vocabulary(text)
    return hits, country, method, text_len, has_hiring_vocab


# 2026-08 archive_iii audit: a real case found — vaxcare.com's site had a
# bare "https://www.bamboohr.com" badge/footer link (their real job-board
# subdomain apparently wasn't linked anywhere findable), which
# _url_to_slug_bamboohr correctly returns None for (slug would be "www"),
# so it fell through and got captured as if it were an "in-house" career
# page — when the company is actually ON a supported ATS, just not
# detectably so from this page. Mirrors the 29 supported platforms'
# domains from discovery.py's URL_TO_SLUG (kept in sync manually — update
# both if a platform's domain changes). Any hit here means "this is ATS-
# related, not in-house," full stop, regardless of whether a slug could
# be extracted — better to drop the candidate than mislabel it.
_ATS_VENDOR_DOMAINS = (
    "greenhouse.io", "lever.co", "ashbyhq.com", "bamboohr.com", "icims.com",
    "myworkdayjobs.com", "rippling.com", "workable.com", "recruitee.com",
    "smartrecruiters.com", "taleo.net", "oraclecloud.com", "brassring.com",
    "teamtailor.com", "successfactors.com", "successfactors.eu", "sapsf.com", "sapsf.eu",
    "breezy.hr", "hrmdirect.com", "softgarden.io", "softgarden.de", "zohorecruit.com",
    "zohorecruit.eu", "paylocity.com", "join.com", "personio.de", "personio.com",
    "workatastartup.com", "ycombinator.com", "eploy.net", "folksats.app", "glowinthecloud.com",
    "jobadder.com", "jobvite.com", "adp.com", "avature.net",
    # 2026-09: PageUp / Pinpoint / Flatchr / Jobylon / Occupop added.
    # Occupop included even though it has no working scraper yet (see
    # discovery.py's SUPPORTED_ATS comment) — a page on occupop-careers.com
    # is still ATS-related, not in-house, regardless of whether this
    # project can currently scrape it. Homerun deliberately NOT added
    # here — its customers run on their OWN domain (jobs.{company-domain}),
    # not a fixed vendor suffix, so there's nothing to list; a Homerun
    # in-house-looking page will slip through this particular check.
    "pageuppeople.com", "pinpointhq.com", "flatchr.io", "jobylon.com",
    "occupop-careers.com",
)


def _looks_like_real_career_page(url: str, text_len: int, has_hiring_vocab: bool, origin: str) -> bool:
    """Rejects near-empty stub pages, homepage-in-disguise redirects
    (unknown path -> 200 the actual homepage), pages that are simply
    unrelated content sitting at a guessed path (a blog post at /join,
    a generic page at /opportunities), and pages that ARE on a known ATS
    vendor's own domain but didn't yield a parseable slug — all common
    enough on real sites that skipping these checks would flood
    archive_iii with junk that isn't a genuine in-house career page."""
    if text_len < MIN_CAREER_PAGE_TEXT_CHARS:
        return False
    if not has_hiring_vocab:
        return False
    host = (urlparse(url).hostname or "").lower()
    # Suffix match, NOT substring — "d in host" would falsely flag a real
    # company like multilever.com as being on lever.co (it isn't; that's
    # the exact unanchored-substring bug this whole audit started from).
    if any(host == d or host.endswith("." + d) for d in _ATS_VENDOR_DOMAINS):
        return False
    path = urlparse(url).path.strip("/")
    if path == "" and url.rstrip("/") == origin.rstrip("/"):
        return False
    return True


def _best_inhouse_candidate(candidates: list[dict], origin: str) -> dict | None:
    """Picks the longest-text page among fetched candidates that had NO
    ATS hit but passes the quality gate — the best guess at a genuine
    in-house/unsupported-ATS career page worth capturing into archive_iii."""
    best = None
    for c in candidates:
        if c["hits"]:
            continue
        if not _looks_like_real_career_page(c["url"], c["text_len"], c["has_hiring_vocab"], origin):
            continue
        if best is None or c["text_len"] > best["text_len"]:
            best = c
    return best


# ── Quality Index (2026-09) ──────────────────────────────────────────────
# This is the named gate that decides archive_ii eligibility for
# opendata_probe.py and common_crawl_probe.py (the two sources with no
# employee-count/size signal of their own) — see crawl_batch's docstring
# for how apply_maturity_gate wires a source into this vs. a real size
# filter (PDL/BigPicture).
# archive_ii (in-house/unrecognized-ATS career pages) was accumulating a
# lot of noise from single-person sites, hobbyist projects, and tiny shops
# that happen to have a "Careers" or "Join us" page with no real hiring
# activity behind it. These signals score how much a company's homepage
# looks like an established, multi-person organization — reusing HTML
# crawl_one has ALREADY fetched, with zero extra requests per domain, PLUS
# one DNS MX lookup (2026-09, see below — added AFTER the signals below were
# first shipped without it).
#
# Still deliberately excludes: WHOIS domain-registration age (registries
# rate-limit/block bulk WHOIS at this pipeline's scale, and response formats
# aren't consistently parseable across registrars/TLDs) and TLS certificate
# issuer (needs a real per-domain TLS handshake, not just the HTTP fetch
# already done, and free Let's Encrypt certs are ubiquitous across company
# sizes — a weak signal for real added cost).
#
# DNS MX-record provider (2026-09, ADDED): unlike WHOIS/TLS, this needed no
# new dependency and no workflow.yml edits — aiodns is already a project
# dependency (see new_connector() above) and already installed in every
# workflow that runs node.py. A lightweight, cheap async DNS query, fired
# only for domains that actually reach the archive_ii quality-gate decision
# (not every domain crawled), so it never adds latency to the far more
# common known-ATS path.
#
# Common Crawl WebGraph (2026-09, ADDED): the domain-level ranks file
# (harmonic centrality + PageRank, computed by Common Crawl from real
# observed inbound links across their whole crawl) turned out to be a
# ~2.7 GiB compressed one-time-per-release download, not the multi-
# terabyte dataset this was originally assumed to be. It's built and
# refreshed by a separate batch job (see WebGraph/webgraph_seed.py) into a
# small reduced (domain, rank band) CSV published as a GitHub Release
# asset; this module downloads it ONCE per process and holds it in memory
# from then on — see _webgraph_score() below and its graduated S+/S/A/B/C
# rank bands further down this section.
#
# SIGNAL RELIABILITY, and why the weights below aren't all equal: a link-
# authority measure computed externally from the real web graph (WebGraph)
# is the hardest of all these signals to fake — it requires other real,
# independent sites to actually link to you. Being on file with a national
# securities regulator/company registrar (sec.gov and its 17 per-country
# equivalents — see _ORG_REGULATOR_SAMEAS_RE) is next: it carries real
# legal reporting obligations, a harder thing to fake than markup alone —
# and verifying a claimed Wikipedia sameAs link by actually fetching the
# article and checking it names this domain (_wikipedia_mention_score)
# turns that particular signal from "this page's own markup claims a wiki
# link" into "an independent page really does reference this domain".
# Structured org data that names a checkable external authority (a real
# Wikipedia/Crunchbase/Bloomberg page, unverified) and a dedicated,
# procured enterprise mail-security gateway are next hardest — still
# self-reported/self-installed, but not something a two-person shop does
# by accident. A schema-claimed employee count only counts once parsed and
# found to actually clear EMPLOYEE_COUNT_MIN_FOR_CREDIT — the bare
# presence of the key proves nothing, since a schema can claim any number
# including 1. Enterprise martech/observability tags and corporate footer
# links are real but easy for anyone to add. Compliance banners (OneTrust
# etc.), a bare legal-entity suffix, and mainstream hosted email
# (Workspace/M365) are the weakest — all three are now just as common on a
# one-person LLC as on a large company, so they're weighted low: useful for
# breaking ties, not for carrying the score alone.
#
# This is a soft heuristic score, not a hard proof of company size — it
# exists to cut obvious noise before it reaches archive_ii, not to
# perfectly classify company scale. QUALITY_INDEX_THRESHOLD is deliberately
# tunable (not a magic constant scattered across the file) since the
# right cutoff will need calibrating against real archive_ii output.
_ORG_SCHEMA_TYPE_RE = re.compile(r'"@type"\s*:\s*"(?:Organization|Corporation)"', re.I)
# 2026-09: the old regex only checked that the KEY existed, so a schema
# claiming a single employee got the same credit as one claiming 5,000 —
# a real gap, fixed by actually parsing the number out and requiring it
# clear EMPLOYEE_COUNT_MIN_FOR_CREDIT before awarding anything. Handles
# both shapes schema.org allows: a bare number/string
# ("numberOfEmployees": 250) and a QuantitativeValue object
# ("numberOfEmployees": {"@type": "QuantitativeValue", "value": 250}, or
# ..."minValue": 250 for a range) — the {0,160} window is generous enough
# to span a QuantitativeValue object's other fields without also reaching
# into an unrelated later key.
EMPLOYEE_COUNT_MIN_FOR_CREDIT = 50
_ORG_EMPLOYEE_COUNT_RE = re.compile(
    r'"numberOfEmployees"\s*:\s*(?:\{[^}]{0,160}?"(?:value|minValue)"\s*:\s*)?"?(\d{1,9})"?', re.I)
# Weighted 2026-09 up from +5 to +10 as part of the reliability rebalance
# above — a fabricated Organization schema is cheap, but linking it to a
# real, checkable Wikipedia/Crunchbase/Bloomberg page is a much harder
# thing for a tiny operation to fake convincingly. sec.gov and its
# per-country equivalents were split out below into their own, higher-
# weighted bucket (formal regulator/registrar filing is a different, and
# stronger, kind of evidence than a wiki/directory profile). linkedin.com
# dropped entirely — a LinkedIn company page is something businesses of
# every size, including solo founders, routinely create; it was never a
# real size signal here.
_ORG_AUTHORITY_SAMEAS_RE = re.compile(
    r'"sameAs"\s*:\s*\[[^\]]{0,600}(?:wikipedia\.org|crunchbase\.com|bloomberg\.com)',
    re.I)
# 2026-09: national securities-regulator / company-registrar sites — one
# per country in DEFAULT_COUNTRIES (see opendata_probe.py/
# people_data_labs_probe.py) plus the US's sec.gov, since this project
# targets all 18, not just the US. Weighted higher than the generic
# authority bucket above: being on file with a national regulator/
# registrar carries real legal reporting obligations, which is a harder
# and more consequential thing to fake than a Wikipedia/Crunchbase
# profile. Not a perfect size signal on its own — some of these registries
# (e.g. company-registration ones like the Netherlands' KVK or Ireland's
# CRO) cover ordinary registered businesses of any size, not just large
# public companies — which is exactly why this stays a soft score
# contributor rather than a gate by itself, same as everything else here.
_ORG_REGULATOR_SAMEAS_RE = re.compile(
    r'"sameAs"\s*:\s*\[[^\]]{0,600}(?:'
    r'sec\.gov|'                                          # United States — SEC EDGAR
    r'company-information\.service\.gov\.uk|'              # United Kingdom — Companies House
    r'sedarplus\.ca|'                                       # Canada — SEDAR+
    r'asic\.gov\.au|'                                       # Australia — ASIC
    r'cro\.ie|'                                             # Ireland — Companies Registration Office
    r'companies-register\.companiesoffice\.govt\.nz|'       # New Zealand — Companies Office
    r'bizfile\.gov\.sg|'                                    # Singapore — ACRA BizFile+
    r'kvk\.nl|'                                             # Netherlands — Kamer van Koophandel
    r'brreg\.no|'                                           # Norway — Brønnøysund Register Centre
    r'bolagsverket\.se|'                                    # Sweden — Bolagsverket
    r'virk\.dk|'                                            # Denmark — Erhvervsstyrelsen/CVR
    r'ytj\.fi|'                                             # Finland — Business Information System (PRH)
    r'justizonline\.gv\.at|justiz\.gv\.at|'                 # Austria — Firmenbuch (justizonline.gv.at is the
                                                             # current live query tool; justiz.gv.at kept too,
                                                             # still a real government domain hosting a Firmenbuch
                                                             # subpath, just not the primary tool anymore — 2026-09)
    r'kbo-bce\.be|'                                         # Belgium — Crossroads Bank for Enterprises
    r'skatturinn\.is|'                                      # Iceland — Directorate of Internal Revenue
    r'lbr\.lu|'                                             # Luxembourg — Luxembourg Business Registers
    r'annuaire-entreprises\.data\.gouv\.fr|infogreffe\.fr|'  # France — Annuaire des Entreprises (RNE, the
                                                             # purely-.gouv.fr official successor) preferred;
                                                             # Infogreffe kept too, still real/valid — 2026-09
    r'handelsregister\.de'                                  # Germany — Handelsregister
    r')', re.I)
# 2026-09: expanded beyond the original 7-phrase set with more terms large,
# established organizations routinely have pages for but a small shop
# essentially never does (annual reporting/governance/leadership
# structure, earnings disclosure) — same reasoning as the original set.
_CORP_FOOTER_LINKS_RE = re.compile(
    r'\b(?:investor relations|investors?|newsroom|press releases?|board of directors|'
    r'esg\b|sustainability report|annual report|shareholders?|corporate governance|'
    r'executive (?:team|leadership)|leadership team|media kit|quarterly results|'
    r'earnings call|form 10-k|proxy statement)\b', re.I)
# Legal entity suffixes near a copyright line — expanded 2026-09 beyond the
# original US/UK/DE/FR-heavy set to cover the major suffixes for the rest of
# the countries this project actually targets for "global jobs" (AU, NL, IT,
# JP, wider-Asia "Co., Ltd.", Nordics, Switzerland/Austria's "AG").
# Weighted 2026-09 down to +5 (from +10) in the reliability rebalance —
# virtually any registered business, including a one-person LLC, has a
# legal-entity suffix; it's real evidence of "not a pure hobby site", not
# evidence of scale.
_LEGAL_ENTITY_SUFFIX_RE = re.compile(
    r'(?:©|copyright)[^\n<]{0,80}\b(?:inc\.?|llc|l\.l\.c\.|corp(?:oration)?\.?|gmbh|plc|s\.a\.|ltd\.?|'
    r'pty\.?\s*ltd\.?|pte\.?\s*ltd\.?|b\.?v\.?|s\.?p\.?a\.?|s\.?r\.?l\.?|s\.?a\.?s\.?|a\.?g\.?|a\.?b\.?|'
    r'a/s|k\.?k\.?|co\.,?\s*ltd\.?|oy|ug)\b',
    re.I)
# Weighted 2026-09 down from +10 to +5 as part of the reliability
# rebalance above — GDPR/CCPA compliance tooling is now routine for any
# registered business regardless of size, not just larger ones.
_COMPLIANCE_BANNER_RE = re.compile(r'(?:onetrust\.com|cookielaw\.org|trustarc\.com|cookiebot\.com)', re.I)
# Enterprise martech — expanded 2026-09 with more ABM/enterprise-marketing
# platforms in the same tier as the original 6sense/Demandbase/Marketo/
# Pardot set (all effectively only bought by companies with a real B2B
# marketing/sales-ops function, not something a two-person shop installs).
_ENTERPRISE_MARTECH_RE = re.compile(
    r'(?:6sense\.com|demandbase\.com|marketo\.net|pardot\.com|omtrdc\.net|2o7\.net|'
    r'exacttarget\.com|salesforceliveagent\.com|eloqua\.com|terminus\.com|rollworks\.com|'
    r'zoominfo\.com|bombora\.com)', re.I)
# Observability/APM — expanded 2026-09 with more enterprise-tier platforms
# (AppDynamics, Instana, Splunk Cloud) alongside the original Datadog/
# Dynatrace/New Relic/Sentry set. Weighted down from +10 to +5 in the same
# 2026-09 rebalance — free/cheap tiers of these same tools are common
# among small startups too, so this is a weaker size signal than it looks.
_OBSERVABILITY_RE = re.compile(
    r'(?:datadoghq\.com|dynatrace\.com|newrelic\.com|nr-data\.net|sentry\.io|js\.sentry-cdn\.com|'
    r'appdynamics\.com|instana\.io|splunkcloud\.com)', re.I)

QUALITY_INDEX_THRESHOLD = 20

# ── DNS MX-record provider (2026-09) ────────────────────────────────────
# Two tiers, scored differently because they mean different things:
#  - A dedicated enterprise email-security gateway (Mimecast, Proofpoint,
#    Cisco Secure Email Cloud/IronPort, Barracuda, Forcepoint) is a real
#    company-size signal — these are procured/IT-managed products, not
#    something a tiny shop buys. Scored the same as the other strong
#    signals above.
#  - Mainstream hosted business email (Google Workspace, Microsoft 365) is
#    used by companies of every size, including solo founders — it only
#    confirms "this domain has real, working email infrastructure" (i.e.
#    rules out a dead/parked domain), so it's scored much lower.
#  - No MX record, or the lookup times out/fails: no score change either
#    way — DNS lookups fail for benign, unrelated reasons often enough
#    that penalizing on a miss would be its own source of noise.
_MX_ENTERPRISE_GATEWAY_RE = re.compile(
    r'(?:mimecast\.com|pphosted\.com|ppe-hosted\.com|iphmx\.com|barracudanetworks\.com|'
    r'forcepoint\.com|mailcontrol\.com|messagelabs\.com)', re.I)
# Weighted 2026-09 down to +3 (from +5) in the reliability rebalance —
# Workspace/M365 is used by companies of every size, including solo
# founders; it only rules out a dead/parked domain, nothing more.
_MX_MAINSTREAM_HOSTED_RE = re.compile(
    r'(?:google\.com|googlemail\.com|aspmx\.l\.google\.com|outlook\.com|protection\.outlook\.com)', re.I)
MX_LOOKUP_TIMEOUT = 3.0  # seconds — DNS should be fast; never worth blocking the crawl over

_mx_resolver = None  # lazy singleton, reused across every crawl_one call


def _get_mx_resolver():
    """Returns a shared aiodns.DNSResolver, or None if aiodns isn't
    installed (same graceful-degradation pattern as new_connector() above
    — MX scoring is a bonus signal, never a hard requirement)."""
    global _mx_resolver
    if _mx_resolver is None:
        try:
            import aiodns
            _mx_resolver = aiodns.DNSResolver()
        except ImportError:
            _mx_resolver = False  # sentinel: "checked, not available"
    return _mx_resolver or None


async def _mx_provider_score(domain: str) -> tuple[int, str | None]:
    """One async MX query for `domain`. Returns (score, signal_name) — never
    raises; any DNS error, timeout, or missing aiodns just means no signal
    (0, None), not a penalty."""
    resolver = _get_mx_resolver()
    if resolver is None:
        return 0, None
    try:
        records = await asyncio.wait_for(resolver.query(domain, "MX"), timeout=MX_LOOKUP_TIMEOUT)
    except Exception:
        return 0, None
    hosts = " ".join(getattr(r, "host", "") or "" for r in (records or []))
    if not hosts:
        return 0, None
    if _MX_ENTERPRISE_GATEWAY_RE.search(hosts):
        return 15, "enterprise_mail_security"
    if _MX_MAINSTREAM_HOSTED_RE.search(hosts):
        return 3, "hosted_business_email"
    return 0, None


# ── Common Crawl WebGraph rank bands (2026-09) ──────────────────────────
# The strongest signal in this whole scoring system — see the reliability
# note in this section's header comment above. Built separately by
# WebGraph/webgraph_seed.py, which downloads Common Crawl's published
# domain-level ranks file (harmonic centrality + PageRank, real link
# authority computed from their own crawl — see that script's docstring
# for the exact source/format) and writes a small reduced domain->band CSV
# (~tens of millions of rows, tens of MB gzipped — nowhere near GitHub's 2GB release-asset
# cap). 2026-09, REVISED: that CSV is uploaded to a GitHub Release, NOT a
# Supabase table — a live per-domain Supabase read for every quality-gate
# candidate would mean real ongoing egress/storage cost for data that
# never changes mid-run and is cheap to just hold in memory. Instead this
# file is downloaded ONCE per crawl process (lazy singleton, same pattern
# as the MX resolver above) and every lookup after that is a plain local
# dict read — zero network cost, zero latency, for the rest of the run.
#
# Six graduated rank bands (S+/S/A/B/C/D), not a flat "in the top N or
# nothing" cutoff — 2026-09, revised after checking a REAL, known-
# legitimate company (heli.technology, already confirmed hiring globally
# in the jobs DB) against the actual ranks file: it landed at rank
# 44,860,492 out of ~118.0M domain nodes (Common Crawl's own published
# count for this release — see their blog) — roughly the 62nd percentile,
# nowhere near any of the flat cutoffs (10M/15M/20M/25M) that were being
# considered. A binary cutoff would have given a real, legitimately-hiring
# company ZERO credit just for not being especially link-popular. Graduated
# bands let a domain like that earn a modest, honest amount of credit
# instead of an all-or-nothing cliff.
#
# Band boundaries and points, reasoned (not just round numbers) from
# Common Crawl's own published graph statistics for this era of releases —
# domain-level: 118.0M nodes, 2.8B edges; the largest strongly-connected
# component (the "real, mutually-linked core" of the web) is 30.0M nodes
# (25.4%); 63.2% (74.5M) of all nodes are "dangling" (no outbound links —
# a rough, imperfect proxy for peripheral/thin sites, since it measures
# outbound links only, not inbound authority). Boundaries are INCLUSIVE
# (rank <= bound, 0-based) per band, checked top-down, first match wins —
# webgraph_seed.py's _band_for_rank() is the single source of truth for
# the actual cutoff logic; this list only needs to match its boundaries:
#   S+ : rank <=  1,000,000 (top ~0.8%)  — unmistakably major (Google,
#        Wikipedia, Facebook-tier). Strong enough to clear the Quality
#        Index bar alone.
#   S  : rank <= 10,000,000 (top ~8.5%)  — clearly well-established. Also
#        clears the bar alone.
#   A  : rank <= 25,000,000 (top ~21%, close to the graph's own 25.4%
#        strongly-connected-component size) — solidly connected, real
#        established orgs. Needs one more smallish signal to pass.
#   B  : rank <= 50,000,000 (top ~42%) — modestly connected; real but
#        unremarkable companies land here (this is heli.technology's
#        band). A cautious, small credit — needs several other signals
#        to actually clear the bar, per "safely exclude rather than
#        include."
#   C  : rank <= 80,000,000 (top ~68%, near where "dangling"/peripheral
#        nodes start being the majority) — very weakly connected. A
#        token credit only.
#   D  : rank >  80,000,000, OR not found in the ranks file at all — no
#        credit. This is where most parked/junk domains live (nobody
#        links to a parking page), so this band intentionally contributes
#        nothing rather than risk rewarding noise.
# These bands are still a judgment call, calibrated against one real data
# point plus the graph's own published shape stats — not a rigorously
# derived cutoff. Revisit if more known-good/known-bad companies get
# checked against real ranks (see webgraph_seed.py's lookup_ranks()).
WEBGRAPH_TIERS_URL = os.environ.get("WEBGRAPH_TIERS_URL", "")  # GitHub Release asset URL, .csv or .csv.gz
WEBGRAPH_RANK_BANDS = (
    # (label, inclusive rank upper bound (0-based), points) — checked in order, first match wins
    ("S+", 1_000_000, 30),
    ("S", 10_000_000, 25),
    ("A", 25_000_000, 20),
    ("B", 50_000_000, 15),
    ("C", 80_000_000, 10),
)

_webgraph_ranks: dict[str, str] | None = None  # domain -> band label ("S+".."C"), lazy singleton
_webgraph_load_lock: asyncio.Lock | None = None


async def _load_webgraph_ranks(session: aiohttp.ClientSession) -> dict[str, str]:
    """Downloads+parses WEBGRAPH_TIERS_URL exactly once per process, no
    matter how many concurrent crawl_one() calls ask for it at once (the
    lock makes every caller but the first just wait on the same in-flight
    load). Returns {} (never raises) if the URL isn't configured or the
    download/parse fails — WebGraph just contributes no signal that run,
    same graceful-degradation shape as the MX resolver above. The CSV
    (built by webgraph_seed.py) carries a band LABEL per domain directly
    (domain,band) — bands are assigned once at seed time, not recomputed
    per lookup."""
    global _webgraph_ranks, _webgraph_load_lock
    if _webgraph_ranks is not None:
        return _webgraph_ranks
    if _webgraph_load_lock is None:
        _webgraph_load_lock = asyncio.Lock()
    async with _webgraph_load_lock:
        if _webgraph_ranks is not None:  # someone else finished it while we waited
            return _webgraph_ranks
        if not WEBGRAPH_TIERS_URL:
            _webgraph_ranks = {}
            return _webgraph_ranks
        log.info(f"Loading WebGraph rank bands from {WEBGRAPH_TIERS_URL} (once per run)...")
        ranks: dict[str, str] = {}
        try:
            async with session.get(WEBGRAPH_TIERS_URL, timeout=aiohttp.ClientTimeout(total=120)) as r:
                r.raise_for_status()
                raw = await r.read()
            text = (gzip.decompress(raw) if WEBGRAPH_TIERS_URL.endswith(".gz") else raw) \
                .decode("utf-8", errors="ignore")
            for line in text.splitlines()[1:]:  # [1:] skips the "domain,band" header
                domain, _, band = line.strip().partition(",")
                if not domain or not band:
                    continue
                ranks[domain] = band
            log.info(f"  loaded {len(ranks):,} WebGraph-ranked domains")
        except Exception as e:
            log.warning(f"  failed to load WebGraph ranks ({e}) — WebGraph signal disabled this run")
            ranks = {}
        _webgraph_ranks = ranks
        return _webgraph_ranks


async def _webgraph_score(session: aiohttp.ClientSession, domain: str) -> tuple[int, str | None]:
    """One in-memory dict lookup (after the one-time load above). Returns
    (score, signal_name) — a domain simply absent from the ranks (i.e.
    band D — outside the kept bands, or WEBGRAPH_TIERS_URL unset) means no
    signal (0, None), never a penalty — plenty of real small/mid companies
    land in D and that's expected, not evidence of anything."""
    ranks = await _load_webgraph_ranks(session)
    band = ranks.get(domain)
    for label, _, points in WEBGRAPH_RANK_BANDS:
        if band == label:
            return points, f"webgraph_rank_{label.lower().replace('+', 'plus')}"
    return 0, None


def _quality_index_score(html: str) -> tuple[int, list[str]]:
    """Scores how much a homepage looks like an established, multi-person
    company site rather than a micro-site — purely from HTML already in
    hand (no extra requests). Returns (score, [matched signal names]) —
    the names are for stats/debugging, not stored anywhere yet."""
    score = 0
    signals: list[str] = []
    if _ORG_SCHEMA_TYPE_RE.search(html):
        score += 15
        signals.append("org_schema")
        employee_match = _ORG_EMPLOYEE_COUNT_RE.search(html)
        if employee_match and int(employee_match.group(1)) >= EMPLOYEE_COUNT_MIN_FOR_CREDIT:
            score += 10
            signals.append("employee_count")
        if _ORG_AUTHORITY_SAMEAS_RE.search(html):
            score += 10
            signals.append("authority_sameas")
        if _ORG_REGULATOR_SAMEAS_RE.search(html):
            score += 20
            signals.append("regulator_listing")
    if _CORP_FOOTER_LINKS_RE.search(html):
        score += 15
        signals.append("corp_footer_links")
    if _COMPLIANCE_BANNER_RE.search(html):
        score += 5
        signals.append("compliance_banner")
    if _ENTERPRISE_MARTECH_RE.search(html):
        score += 15
        signals.append("enterprise_martech")
    if _OBSERVABILITY_RE.search(html):
        score += 5
        signals.append("observability")
    if _LEGAL_ENTITY_SUFFIX_RE.search(html):
        score += 5
        signals.append("legal_entity_suffix")
    return score, signals


# 2026-09: a sameAs link to wikipedia.org (scored by _ORG_AUTHORITY_SAMEAS_RE
# above) only proves the page's OWN markup points somewhere on
# wikipedia.org — anyone can write that, whether or not the article is
# really about them. This regex captures the actual article URL out of the
# sameAs array so _wikipedia_mention_score() below can fetch it and check
# for a real cross-reference back to this domain.
_WIKIPEDIA_SAMEAS_URL_RE = re.compile(
    r'"sameAs"\s*:\s*\[[^\]]{0,600}?"(https?://[a-z]{2,3}\.wikipedia\.org/wiki/[^"]+)"', re.I)
WIKIPEDIA_VERIFY_TIMEOUT = 5.0


async def _wikipedia_mention_score(session: aiohttp.ClientSession, html: str,
                                    domain: str) -> tuple[int, str | None]:
    """One extra fetch, only for domains that already claimed a Wikipedia
    sameAs link: pulls the real article and checks whether it actually
    names THIS domain — a company's Wikipedia infobox "Website" field
    almost always links (or plain-text names) its real official domain, so
    finding this domain's string anywhere in the article page is a
    meaningful, if soft, check that the claimed article is really about
    this company rather than a same-named unrelated topic or a wrong/
    copy-pasted link. This is a BONUS on top of the base
    "authority_sameas" credit, never a replacement for it — a domain that
    links to Wikipedia but fails this check still gets the base +10, just
    not this extra credit. Limited to Wikipedia (not Crunchbase/Bloomberg):
    those sit behind paywalls/bot-blocking that would make a fetch-based
    check unreliable rather than just conservative. Any failure (no
    sameAs link, fetch error, timeout, 4xx/5xx) returns (0, None) — never
    a penalty, same graceful-degradation shape as every other network
    signal here."""
    m = _WIKIPEDIA_SAMEAS_URL_RE.search(html)
    if not m:
        return 0, None
    wiki_url = m.group(1)
    try:
        async with session.get(wiki_url, timeout=aiohttp.ClientTimeout(total=WIKIPEDIA_VERIFY_TIMEOUT),
                                headers={"User-Agent": USER_AGENT}) as r:
            if r.status >= 400:
                return 0, None
            article_html = await r.text(errors="ignore")
    except Exception:
        return 0, None
    if domain.lower() in article_html.lower():
        return 15, "wikipedia_mention_verified"
    return 0, None


async def _quality_index_score_async(session: aiohttp.ClientSession, html: str,
                                      domain: str) -> tuple[int, list[str]]:
    """_quality_index_score() plus the MX lookup, the WebGraph rank-band
    lookup, and the Wikipedia cross-reference check — kept as a separate
    async wrapper so every existing (sync, HTML-only) call site and test
    of _quality_index_score keeps working unchanged."""
    score, signals = _quality_index_score(html)
    mx_score, mx_signal = await _mx_provider_score(domain)
    if mx_signal:
        score += mx_score
        signals.append(mx_signal)
    wg_score, wg_signal = await _webgraph_score(session, domain)
    if wg_signal:
        score += wg_score
        signals.append(wg_signal)
    wiki_score, wiki_signal = await _wikipedia_mention_score(session, html, domain)
    if wiki_signal:
        score += wiki_score
        signals.append(wiki_signal)
    return score, signals


def log_quality_index_summary(stats: dict) -> None:
    """Logs what share of Quality-Index-GATED archive_ii acceptances used
    each individual signal this run — e.g. "webgraph_rank_b=45.2%
    regulator_listing=12.0% org_schema=68.4%" — so a run's actual signal
    mix is visible, not just the pass/fail count crawl_one already logs.
    Call once at the end of a run, alongside the existing
    career-pages-found summary line (see opendata_probe.py's/
    common_crawl_probe.py's run_crawl()).

    Percentages are OF ACCEPTED ENTRIES, not of all candidates checked —
    "45% used WebGraph" means 45% of the archive_ii rows this run actually
    kept had a WebGraph signal contribute to their score, not that 45% of
    everything crawled did. They don't sum to 100%: most accepted entries
    clear QUALITY_INDEX_THRESHOLD on more than one signal at once (that's
    the whole point of a multi-signal score), so this is deliberately a
    per-signal coverage breakdown, not a partition.

    No-op if this run had zero Quality-Index-gated acceptances — a
    capture_inhouse_domains-based caller (PDL/BigPicture) never runs the
    Quality Index at all (see crawl_one's docstring on apply_maturity_gate)
    and so never populates quality_gated_accepted/quality_signal__* in the
    first place; nothing to summarize there, and this stays silent rather
    than logging a misleading all-zero line."""
    accepted = stats.get("quality_gated_accepted", 0)
    if not accepted:
        return
    prefix = "quality_signal__"
    signal_counts = {k[len(prefix):]: v for k, v in stats.items() if k.startswith(prefix)}
    if not signal_counts:
        return
    ranked = sorted(signal_counts.items(), key=lambda kv: -kv[1])
    breakdown = "  ".join(f"{name}={count / accepted * 100:.1f}%" for name, count in ranked)
    log.info(f"  Quality Index signal mix ({accepted:,} Quality-Index-gated archive_ii "
             f"acceptances this run — % of THOSE that had each signal, not of all candidates "
             f"checked; doesn't sum to 100%, most accepted entries clear the bar on more than "
             f"one signal at once): {breakdown}")
# ── fetching ─────────────────────────────────────────────────────────────

# Content types that are never worth reading as text — everything else
# (html, xml, plain text, missing header) is accepted. Used to be an
# "html"-only allowlist, which silently rejected real sitemap.xml
# (application/xml) and robots.txt (text/plain) responses — a real bug
# that made the sitemap fallback tier dead on arrival. Fixed here.
_BINARY_CONTENT_PREFIXES = ("image/", "video/", "audio/", "font/",
                             "application/pdf", "application/zip", "application/octet-stream")


async def _fetch_page(session: aiohttp.ClientSession, url: str, stats: dict) -> tuple[str, str] | None:
    """One page, capped at MAX_PAGE_BYTES. No retries/backoff — a single
    miss just means this path didn't pan out, not worth re-hammering."""
    stats["requests_attempted"] += 1
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT,
                                headers={"User-Agent": USER_AGENT},
                                allow_redirects=True, max_redirects=5, ssl=False) as r:
            if r.status >= 400:
                stats["http_error"] += 1
                if r.status == 404:
                    stats["status_404"] += 1
                return None
            content_type = r.headers.get("Content-Type", "").lower()
            if content_type.startswith(_BINARY_CONTENT_PREFIXES):
                stats["non_html"] += 1
                return None
            chunks = []
            total = 0
            async for chunk in r.content.iter_chunked(65536):
                chunks.append(chunk)
                total += len(chunk)
                if total >= MAX_PAGE_BYTES:
                    break
            text = b"".join(chunks).decode("utf-8", errors="ignore")
            if not text.strip():
                stats["non_html"] += 1
                return None
            stats["fetched_ok"] += 1
            return str(r.url), text
    except asyncio.TimeoutError:
        stats["timeout"] += 1
        return None
    except Exception:
        stats["unreachable"] += 1
        return None


async def _fetch_sitemap(session: aiohttp.ClientSession, origin: str, stats: dict):
    """Tier 3: guessed paths (SITEMAP_INDEX_PATHS). Tier 4, only if those
    both miss: robots.txt's `Sitemap:` directive — the actual standard way
    a site declares a non-default sitemap location."""
    for path in SITEMAP_INDEX_PATHS:
        page = await _fetch_page(session, urljoin(origin, path), stats)
        if page:
            return page
    robots = await _fetch_page(session, urljoin(origin, "/robots.txt"), stats)
    if robots:
        _, robots_text = robots
        for line in robots_text.splitlines():
            if line.strip().lower().startswith("sitemap:"):
                sm_url = line.split(":", 1)[1].strip()
                if sm_url:
                    page = await _fetch_page(session, sm_url, stats)
                    if page:
                        return page
    return None


def _collapse_hits(hit_lists: list[list[tuple[str, str, str]]]) -> list[tuple[str, str, str]]:
    """Merges hits from every page fetched in a tier (not just the first
    hit-bearing one) — a company running two ATS platforms at once (e.g.
    mid-migration) gets both, at zero extra request cost (pages were
    already fetched either way)."""
    seen: set[tuple[str, str]] = set()
    merged: list[tuple[str, str, str]] = []
    for hits in hit_lists:
        for ats, slug, url in hits:
            key = (ats, slug)
            if key in seen:
                continue
            seen.add(key)
            merged.append((ats, slug, url))
    return merged


# ── the crawl ────────────────────────────────────────────────────────────

async def crawl_one(session: aiohttp.ClientSession, sem: asyncio.Semaphore, domain: str,
                     stats: dict, parse_pool: concurrent.futures.Executor,
                     target_geo_countries: set[str] = ACCEPT_ANY_COUNTRY,
                     capture_inhouse: bool = True,
                     apply_maturity_gate: bool = True,
                     ) -> tuple[list[tuple[str, str, str, str, str, str | None, str | None]], dict | None]:
    """Homepage -> career paths -> sitemap (guessed, then robots.txt).
    Every page fetched at a hit-bearing tier is merged (_collapse_hits),
    not just the first one. Returns (ats_hit_rows, career_page_capture):
    ats_hit_rows is (ats, slug, matched_url, domain, tier, country,
    country_method) same as before; country is opportunistic, never a
    gate — these rows go to `archive_i` (formerly slug_registry) exactly
    as they always have. career_page_capture (2026-08, for archive_ii,
    formerly archive_iii) is ONLY ever populated when NO known ATS was
    matched anywhere on the company's site — a genuine in-house/
    unrecognized-platform career page — since anything that DID match a
    known ATS already has a full archive_i row and doesn't need a second,
    separate record. Shape:
    {"career_page_url","website_url"} — no root_domain (dropped 2026-08,
    was a pure duplicate of website_url; website_url is now archive_ii's
    own unique/upsert key) and no company_name (dropped 2026-08 — the
    domain/website_url is already the identifier; a separate name lookup
    just cost extra space for no real use) — or None if nothing worth
    capturing turned up (homepage unreachable, a known ATS was found
    instead, or no page cleared the quality gate)."""
    loop = asyncio.get_running_loop()

    def _capture(career_url: str) -> dict:
        return {
            "career_page_url": career_url, "website_url": f"https://{domain}",
        }

    async with sem:
        stats["companies_attempted"] += 1
        candidates = [f"https://{domain}"]
        if not domain.startswith("www."):
            candidates.append(f"https://www.{domain}")
        candidates.append(f"http://{domain}")  # last resort

        page = None
        for base_url in candidates:
            page = await _fetch_page(session, base_url, stats)
            if page:
                break
        if not page:
            stats["homepage_unreachable"] += 1
            return [], None

        final_url, html = page
        stats["homepage_fetched"] += 1
        best_country, best_method = None, None

        async def _detect(html_, url_):
            nonlocal best_country, best_method
            hits, country, method, text_len, has_hiring_vocab = await loop.run_in_executor(
                parse_pool, _parse_detect, html_, url_, target_geo_countries)
            if country and best_country is None:
                best_country, best_method = country, method
            return hits, text_len, has_hiring_vocab

        hits, _, _ = await _detect(html, final_url)
        if hits:
            stats["hits_from_homepage"] += 1
            stats["known_ats_found"] += 1  # -> archive_ii only, not archive_iii
            return ([(ats, slug, url, domain, "homepage", best_country, best_method) for ats, slug, url in hits],
                    None)

        origin_parts = urlparse(final_url)
        origin = f"{origin_parts.scheme}://{origin_parts.netloc}"
        career_pages = await asyncio.gather(
            *[_fetch_page(session, urljoin(origin, p), stats) for p in CAREER_PATHS])
        career_candidates = []
        for cp in career_pages:
            if not cp:
                continue
            cp_url, cp_html = cp
            cp_hits, cp_text_len, cp_hiring_vocab = await _detect(cp_html, cp_url)
            career_candidates.append({"url": cp_url, "hits": cp_hits, "text_len": cp_text_len,
                                       "has_hiring_vocab": cp_hiring_vocab})
        merged = _collapse_hits([c["hits"] for c in career_candidates])
        if merged:
            stats["hits_from_career_path"] += 1
            stats["known_ats_found"] += 1  # -> archive_ii only, not archive_iii
            return ([(ats, slug, url, domain, "career_path", best_country, best_method) for ats, slug, url in merged],
                    None)

        best_inhouse = _best_inhouse_candidate(career_candidates, origin)

        sitemap = await _fetch_sitemap(session, origin, stats)
        if sitemap:
            sm_url, sm_xml = sitemap
            loc_urls = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", sm_xml, re.I)
            # 2026-08: added the blog-path and sentence-slug exclusions
            # (see their definitions above) alongside the existing
            # CAREER_LIKE_RE match — confirmed via a archive_iii audit
            # that CAREER_LIKE_RE alone lets through a meaningful amount
            # of editorial content (blog posts, press releases) that
            # merely discusses hiring/recruiting as a topic.
            career_like = [u for u in loc_urls
                           if CAREER_LIKE_RE.search(u)
                           and not _BLOG_LIKE_PATH_RE.search(urlparse(u).path)
                           and not _looks_like_sentence_slug(urlparse(u).path)]
            sm_pages = await asyncio.gather(
                *[_fetch_page(session, u, stats) for u in career_like[:SITEMAP_MAX_FOLLOW]])
            sitemap_candidates = []
            for sp in sm_pages:
                if not sp:
                    continue
                sp_url, sp_html = sp
                sp_hits, sp_text_len, sp_hiring_vocab = await _detect(sp_html, sp_url)
                sitemap_candidates.append({"url": sp_url, "hits": sp_hits, "text_len": sp_text_len,
                                            "has_hiring_vocab": sp_hiring_vocab})
            merged = _collapse_hits([c["hits"] for c in sitemap_candidates])
            if merged:
                stats["hits_from_sitemap"] += 1
                stats["known_ats_found"] += 1  # -> archive_ii only, not archive_iii
                hit_url = next(c["url"] for c in sitemap_candidates if c["hits"])
                return ([(ats, slug, url, domain, "sitemap", best_country, best_method) for ats, slug, url in merged],
                        None)
            sitemap_inhouse = _best_inhouse_candidate(sitemap_candidates, origin)
            if sitemap_inhouse and (not best_inhouse or sitemap_inhouse["text_len"] > best_inhouse["text_len"]):
                best_inhouse = sitemap_inhouse

        stats["dropped_no_ats"] += 1
        # 2026-09, REVISED: capture_inhouse=False used to mean "this source
        # never feeds archive_ii" for opendata_probe.py/common_crawl_probe.py
        # (neither carries an employee-count/size signal). Both now pass
        # capture_inhouse=True instead and rely on apply_maturity_gate below
        # to do the filtering the size column can't — so they DO feed
        # archive_ii now, gated by the Quality Index instead of size.
        #
        # apply_maturity_gate distinguishes the two gating mechanisms a
        # caller can use (see crawl_batch's docstring):
        #   - capture_inhouse_domains given (people_data_labs_probe.py,
        #     bigpicture_probe.py): the caller already
        #     pre-filtered to companies at/above its own employee-count
        #     threshold before this domain ever reached crawl_one — that
        #     size gate IS the quality bar for these sources, so the
        #     Quality Index is skipped here (crawl_batch computed
        #     apply_maturity_gate=False for these).
        #   - flat capture_inhouse bool, no domains set (opendata_probe.py,
        #     common_crawl_probe.py, and any other source with no size
        #     signal at all): the Quality Index below IS the quality bar
        #     (crawl_batch computed apply_maturity_gate=True).
        if best_inhouse and capture_inhouse:
            if apply_maturity_gate:
                # Quality Index — scored from the homepage HTML already
                # fetched above (+ one DNS MX lookup + one WebGraph rank-
                # band lookup), no extra HTTP request. Only gates archive_ii
                # (in-house capture); archive_i (known-ATS hits, returned
                # earlier in this function) is never touched by this.
                quality_score, quality_signals = await _quality_index_score_async(session, html, domain)
                if quality_score < QUALITY_INDEX_THRESHOLD:
                    stats["inhouse_dropped_low_quality"] += 1
                    log.debug(f"  archive_ii candidate dropped (Quality Index={quality_score} "
                              f"< {QUALITY_INDEX_THRESHOLD}, signals={quality_signals}): {domain}")
                    return [], None
                # 2026-09: per-signal acceptance tally — lets a run report
                # "what % of accepted archive_ii entries used each signal"
                # (see opendata_probe.py/common_crawl_probe.py's end-of-run
                # summary) instead of just the pass/fail count above, which
                # said nothing about WHICH signals actually did the work.
                # quality_gated_accepted is the denominator: only entries
                # that actually went through this gate (apply_maturity_gate
                # True) ever computed quality_signals at all — a
                # capture_inhouse_domains-based caller's accepted rows never
                # reach this branch, so they're correctly excluded rather
                # than silently diluting the percentages.
                stats["quality_gated_accepted"] += 1
                for signal_name in quality_signals:
                    stats[f"quality_signal__{signal_name}"] += 1
            stats["inhouse_career_page_captured"] += 1  # -> archive_ii
            return [], _capture(best_inhouse["url"])
        return [], None


# ── Supabase I/O ─────────────────────────────────────────────────────────

async def _upsert_rows(session: aiohttp.ClientSession, table: str, on_conflict: str,
                        rows: list[dict]) -> int:
    """Shared upsert plumbing for every Supabase staging table this engine
    writes to (archive_ii, archive_iii, and any future one) — retried
    (unlike crawl requests — this hits ONE shared endpoint many shards
    write to concurrently, where a transient 500 is worth retrying; a
    crawl miss against an independent company domain is not)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning(f"SUPABASE_URL/SUPABASE_KEY not set — cannot write to {table}.")
        return 0
    if not rows:
        return 0
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "resolution=merge-duplicates",
    }
    chunk_size = 1000
    chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

    async def _write_chunk(chunk):
        last_err = None
        for attempt in range(3):
            if attempt:
                await asyncio.sleep(1.5 * (2 ** (attempt - 1)))
            try:
                async with session.post(
                    f"{SUPABASE_URL}/rest/v1/{table}",
                    headers=headers, params={"on_conflict": on_conflict}, json=chunk,
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as r:
                    if r.status >= 500:
                        last_err = f"{r.status} {await r.text()}"
                        continue
                    r.raise_for_status()
                    return len(chunk)
            except Exception as e:
                last_err = str(e)
                if isinstance(e, aiohttp.ClientResponseError) and e.status < 500:
                    break
        log.error(f"Failed to write a chunk of {len(chunk)} rows to {table} after retries — DATA LOST: {last_err}")
        return 0

    results = await asyncio.gather(*(_write_chunk(c) for c in chunks))
    return sum(results)


async def write_ats_hits_to_archive_i(session: aiohttp.ClientSession, rows: list[dict]) -> int:
    """rows come in shaped {"ats","slug","source_hostname","root_domain",
    "country","discovery_method"} (crawl_batch's internal shape, kept for
    found_rows/logging) but archive_i's actual schema is just
    {ats, slug, source, first_seen, last_seen} — no source_hostname/
    root_domain/country columns. 2026-08 restructure: this now writes
    STRAIGHT to archive_i (formerly slug_registry) with no intermediate
    staging/verify table — the old archive_ii (ATS-match quarantine table
    a separate verify step promoted from) is dropped entirely.
    discovery_method maps to archive_i's `source` column, which is
    CHECK-constrained — every discovery_method string a live caller
    passes (people_data_labs_probe, github_org_probe, common_crawl_probe,
    plus discovery.py's own set) must already be in archive_i_source_check
    or the upsert fails for that whole batch.

    2026-09: `last_seen` is DELIBERATELY OMITTED from this payload — per
    an explicit user instruction, archive_i.last_seen was repurposed from
    "last time discovery re-confirmed this ATS page/pattern exists" (what
    this function used to set on every single hit, empty board or not) to
    "last time a real role was actually found there." Discovery merely
    recognizing a URL pattern is no longer a signal of anything but the
    page's existence — Crawl I (crawl_i.py, via
    supabase_handler.touch_archive_i_last_seen) is now the ONLY thing that
    touches this column, and only for slugs where a role was actually
    found. Omitting the field here means: on a re-discovery of an
    existing row (ON CONFLICT), Postgres's merge-duplicates leaves
    last_seen exactly as Crawl I last set it, untouched (same "omit to
    leave alone" trick already used for date_added below); on a genuine
    first INSERT, the column's own DEFAULT (now()) fires, giving a
    brand-new slug a full verification cycle's grace period before it
    could ever be considered stale."""
    slim_rows = [{"ats": r["ats"], "slug": r["slug"], "source": r["discovery_method"]}
                 for r in rows]
    return await _upsert_rows(session, ARCHIVE_I_TABLE, "ats,slug", slim_rows)


async def write_career_pages_to_archive_ii(session: aiohttp.ClientSession, rows: list[dict]) -> int:
    """rows: {"career_page_url","website_url",
    "discovery_method"} (from crawl_one's career_page_capture — only ever
    produced when no known ATS matched — plus discovery_method attached by
    crawl_batch). Upserts on website_url (2026-08: root_domain was dropped
    as a pure duplicate of this field, so website_url is now archive_ii's
    identity key instead) — a re-crawled company updates career_page_url
    in place rather than duplicating. date_added is deliberately left OUT
    of the payload: the column's DEFAULT now() only fires on a true first
    INSERT, and since we never send it on an UPDATE, Postgres's
    merge-duplicates ON CONFLICT leaves the original date_added untouched.

    2026-09: `last_seen` is ALSO now deliberately left out of this
    payload, for the exact same reason and by the exact same "omit to
    leave alone / fall back to the column DEFAULT on true insert" trick
    as archive_i above — see write_ats_hits_to_archive_i's 2026-09 note.
    Crawl II (crawl_ii.py, via supabase_handler.touch_archive_ii_last_seen)
    is now the only thing that touches archive_ii.last_seen, and only for
    pages where a role was actually found.

    2026-08 restructure: this table was archive_iii before the old
    archive_ii (ATS-match staging table) was dropped and archive_iii was
    renamed to take its place — archive_ii now means "in-house/unsupported
    career pages," feeding Crawl II's heuristic job-listing scraper."""
    return await _upsert_rows(session, ARCHIVE_II_TABLE, "website_url", rows)


# ── per-shard resume checkpointing ──────────────────────────────────────
# 2026-08: low --concurrency finishes fewer companies per time-budget
# window, but re-running always restarted every shard from row 0 — hours
# of already-crawled work redone every time. crawl_batch() now checkpoints
# each shard's progress to Supabase right after every batch of 3000
# commits (so a checkpoint value is ALWAYS a clean, fully-written boundary
# — safe to resume from no matter how the process ends: self-stopped,
# cancelled, or crashed). Each source's run_crawl() loads this
# automatically at startup and skips straight past what's already done —
# no manual bookkeeping across N shards required.

async def save_crawl_checkpoint(session: aiohttp.ClientSession, source: str, shard_index: int,
                                 shard_count: int, companies_done: int) -> None:
    """Upserts this shard's progress. Best-effort: a failed write just
    means a future resume falls back to an earlier batch boundary, never
    data loss (the archive_i/archive_ii rows for that batch are already
    safely committed regardless of whether this call succeeds)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
               "Prefer": "resolution=merge-duplicates"}
    row = {"source": source, "shard_index": shard_index, "shard_count": shard_count,
           "companies_done": companies_done, "updated_at": datetime.now(timezone.utc).isoformat()}
    try:
        async with session.post(f"{SUPABASE_URL}/rest/v1/{CHECKPOINT_TABLE}", headers=headers,
                                 params={"on_conflict": "source,shard_index,shard_count"},
                                 json=[row], timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
    except Exception as e:
        log.warning(f"  couldn't save crawl checkpoint at {companies_done:,} (non-fatal — a future "
                    f"resume may just redo one extra batch): {e}")


async def load_crawl_checkpoint(session: aiohttp.ClientSession, source: str, shard_index: int,
                                 shard_count: int) -> int:
    """How many companies this exact (source, shard_index, shard_count)
    already finished on a prior run. 0 if never run, already fully
    completed (checkpoint cleared on completion), or explicitly reset."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return 0
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    params = {"source": f"eq.{source}", "shard_index": f"eq.{shard_index}",
              "shard_count": f"eq.{shard_count}", "select": "companies_done"}
    try:
        async with session.get(f"{SUPABASE_URL}/rest/v1/{CHECKPOINT_TABLE}", headers=headers,
                                params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
            data = await r.json()
            return data[0]["companies_done"] if data else 0
    except Exception as e:
        log.warning(f"  couldn't load crawl checkpoint (starting this shard from 0): {e}")
        return 0


async def clear_crawl_checkpoint(session: aiohttp.ClientSession, source: str, shard_index: int,
                                  shard_count: int) -> None:
    """Deletes a shard's checkpoint — called once its company list is
    fully exhausted (not time-budget-stopped), so a LATER run of the same
    shard layout starts fresh instead of wrongly skipping everything.
    Also used to honor an explicit restart_index=0 (force full restart)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    params = {"source": f"eq.{source}", "shard_index": f"eq.{shard_index}", "shard_count": f"eq.{shard_count}"}
    try:
        async with session.delete(f"{SUPABASE_URL}/rest/v1/{CHECKPOINT_TABLE}", headers=headers,
                                   params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
    except Exception as e:
        log.warning(f"  couldn't clear crawl checkpoint (non-fatal, just means a future full-restart "
                    f"run might unnecessarily skip ahead once): {e}")


# ── shared batch driver ───────────────────────────────────────────────────

# 2026-08: the time budget used to only be checked BETWEEN batches, right
# before starting the next asyncio.gather(*batch) — which blocks until
# EVERY task in that batch finishes. crawl_one holds a semaphore slot
# through up to 3 homepage candidates (10s timeout each) plus career-path
# and sitemap fallback tiers, so a batch of up to 3000 domains, bottlenecked
# by a low --concurrency semaphore, could take many minutes to fully drain
# — dominated by whichever handful of dead/slow domains land in the last
# concurrency wave. The time-budget check simply never got a chance to run
# until that entire batch finished, so a run could blow way past its
# budget (a 5-minute budget finishing at 12+ minutes was one real case).
#
# _run_with_deadline replaces the blocking asyncio.gather(*batch) with a
# polling wait: it reaps each task's result as soon as that task finishes,
# and re-checks the wall clock every POLL_INTERVAL seconds. The instant the
# deadline passes, it cancels every task still in flight immediately —
# even mid-batch — rather than waiting for stragglers. 0.5s (not 0.5ms —
# that would just busy-loop the event loop for zero real benefit, since no
# human or downstream system can tell the difference between a 2ms delay
# and a 500ms one, and constant re-polling steals scheduler time from the
# actual crawling) keeps the worst-case overshoot past the budget under a
# second, which is what "immediately" actually means here.
POLL_INTERVAL = 0.5


async def _run_with_deadline(coros, deadline: float) -> tuple[list, bool]:
    """Runs `coros` concurrently, starting them as real asyncio Tasks
    right away (not just creating coroutine objects), and returns
    (results_from_whatever_completed, hit_deadline). The moment
    time.monotonic() passes `deadline`, cancels every task still pending
    and waits for that cancellation to actually land (crawl_one's awaits —
    aiohttp requests, executor calls — all raise CancelledError and unwind
    cleanly) before returning, so nothing is left running in the
    background once this returns.

    2026-09 bug fix: a single crawl_one() task raising an unhandled
    exception used to blow up THIS function immediately (t.result() just
    re-raises it) — skipping the `if pending: cancel...` cleanup below
    entirely. Every other still-in-flight task in this sub-batch was left
    running, orphaned, with nothing left to await it ("Task exception was
    never retrieved"). Those orphans kept running detached from the crawl
    that spawned them, and if one of them was mid-await on
    loop.run_in_executor(parse_pool, ...) when the caller later ran
    parse_pool.shutdown() (once the whole crawl had already moved on,
    thinking everything was cleanly stopped), it would crash trying to
    submit new work to an already-shut-down executor — the actual crash
    reported 2026-09 (RuntimeError: cannot schedule new futures after
    shutdown). Fixed by catching each task's exception individually so one
    bad domain can never skip cleanup of the others."""
    tasks = [asyncio.ensure_future(c) for c in coros]
    pending = set(tasks)
    results = []
    hit_deadline = False
    try:
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                hit_deadline = True
                break
            done, pending = await asyncio.wait(pending, timeout=min(remaining, POLL_INTERVAL),
                                                return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                try:
                    results.append(t.result())
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning(f"  crawl_one task failed unexpectedly (skipping this one domain, "
                                f"continuing the rest of the batch): {e}")
    finally:
        # Runs even if the loop above raised (e.g. CancelledError bubbling
        # from THIS coroutine being cancelled by an outer caller) — nothing
        # from this sub-batch is ever left orphaned.
        if pending:
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
    return results, hit_deadline


async def crawl_batch(domains: list[str], session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                       stats: dict, parse_pool: concurrent.futures.Executor,
                       target_geo_countries: set[str], discovery_method: str,
                       found_rows: list[dict], crawl_start: float, time_budget_seconds: float,
                       time_budget_minutes: int, batch_size: int = 3000, unit_label: str = "companies",
                       capture_inhouse: bool = True,
                       capture_inhouse_domains: set[str] | None = None,
                       shard_index: int | None = None, shard_count: int | None = None,
                       start_at: int = 0,
                       ) -> tuple[int, float, float, bool]:
    """Crawls a list of domains in sub-batches, writing each sub-batch to
    Supabase as it completes. One driver for every seed source — used to
    be duplicated (people_data_labs_probe's inline loop, host_crawl_v2's
    _crawl_and_write_hosts) with the same batching/dedup/time-budget logic
    copy-pasted twice. Returns (done, elapsed, rate, time_budget_hit).

    2026-08 restructure: ATS-pattern hits now write DIRECTLY to
    ARCHIVE_I_TABLE (archive_i, formerly slug_registry) — there is no more
    intermediate staging/verify table; Verification/verification.py is the
    only thing that ever removes a row, and only once confirmed dead. ALL
    domains passed in get crawled and checked for a known-ATS match
    regardless of size — archive_i is never size-gated.

    Every in-house/unsupported career page crawl_one finds (never a
    known-ATS one — those already got a full archive_i row) writes to
    ARCHIVE_II_TABLE (archive_ii, formerly archive_iii) — gated per-domain:

      - capture_inhouse_domains, if given, is the exact set of domains
        allowed to produce an archive_ii row this run (every other domain
        behaves as capture_inhouse=False for that one company only, ATS
        matching unaffected). This is the 2026-08 fix for over-filtering:
        people_data_labs_probe.py/bigpicture_probe.py used
        to drop small companies from the crawl LIST entirely at seed time,
        which filtered out their archive_i-eligible ATS hits too, for
        almost nothing gained — now every company in the target countries
        gets crawled (feeding archive_i normally no matter its size), and
        the employee-count floor is applied ONLY at the point a company
        would otherwise become an archive_ii in-house-page candidate,
        computed by the caller from its own already-loaded size data.
        2026-09: passing this set is ALSO what turns the Quality Index
        (node.py's _quality_index_score_async — the archive_ii gate for
        sources with no size signal) OFF for this run — a caller with real
        size data has already decided its own quality bar; the Quality
        Index would just be a redundant second filter on companies that
        already cleared a real employee-count threshold.
      - If capture_inhouse_domains is None, falls back to the flat
        `capture_inhouse` bool for every domain. 2026-09: opendata_probe.py
        and common_crawl_probe.py now pass capture_inhouse=True this way
        (they used to pass False — see their run_crawl()s) — with no size
        signal for ANY domain, the Quality Index is what decides archive_ii
        eligibility for these two instead of an employee-count floor; this
        is exactly the case that turns it ON (apply_maturity_gate =
        capture_inhouse_domains is None, computed below).

    shard_index/shard_count/start_at (2026-08, resume support): when both
    shard_index and shard_count are given, this shard's progress is
    checkpointed to Supabase after every batch (as start_at + done, i.e.
    the ABSOLUTE position in the caller's full shard list, since `domains`
    here may already be a checkpoint-resumed suffix) and the checkpoint is
    cleared once the whole list is exhausted without hitting the time
    budget. Callers don't need to do anything with this beyond passing the
    values through — see save/load/clear_crawl_checkpoint above and each
    source's run_crawl() for how the resume-on-startup side works."""
    def _capture_for(domain: str) -> bool:
        if capture_inhouse_domains is not None:
            return domain in capture_inhouse_domains
        return capture_inhouse

    # 2026-09: see this function's docstring above — a caller that supplied
    # its own size-based domain set has already applied its own quality bar,
    # so the maturity check is redundant for it and gets skipped; a caller
    # relying on the flat bool has no size signal at all, so the maturity
    # check IS its quality bar.
    apply_maturity_gate = capture_inhouse_domains is None

    tasks = [crawl_one(session, sem, d, stats, parse_pool, target_geo_countries, _capture_for(d),
                        apply_maturity_gate)
             for d in domains]
    elapsed, rate = 0.0, 0.0
    time_budget_hit = False
    deadline = crawl_start + time_budget_seconds
    for i in range(0, len(tasks), batch_size):
        if time.monotonic() >= deadline:
            for t in tasks[i:]:
                t.close()
            time_budget_hit = True
            log.warning(f"  time budget ({time_budget_minutes}min) reached at {i}/{len(tasks)} "
                        f"{unit_label} — stopping here, everything found so far is written.")
            break
        batch = tasks[i:i + batch_size]
        # Polls the deadline instead of blocking until every task in this
        # batch finishes — see _run_with_deadline above. `results` may be
        # SHORT of the full batch if hit_deadline fired mid-batch; whatever
        # DID complete is still processed/written/checkpointed below like
        # normal, nothing found gets thrown away.
        results, hit_deadline = await _run_with_deadline(batch, deadline)
        batch_rows = []
        scrape_rows = []
        seen_keys = set()
        seen_domains = set()
        duplicates_collapsed = 0
        for hits, career_capture in results:
            for ats, slug, matched_url, domain, tier, country, method in hits:
                # (ats, slug) is the table's real unique constraint —
                # Postgres's ON CONFLICT can't touch the same row twice in
                # one command, so a within-batch duplicate has to be
                # collapsed here or the WHOLE batch errors, not just the dup.
                key = (ats, slug)
                if key in seen_keys:
                    duplicates_collapsed += 1
                    continue
                seen_keys.add(key)
                batch_rows.append({
                    "ats": ats, "slug": slug, "source_hostname": matched_url[:250],
                    "root_domain": domain, "country": country, "discovery_method": discovery_method,
                })
            if career_capture and career_capture["website_url"] not in seen_domains:
                seen_domains.add(career_capture["website_url"])
                scrape_rows.append({**career_capture, "discovery_method": discovery_method})
        written = 0
        if batch_rows:
            written = await write_ats_hits_to_archive_i(session, batch_rows)
            found_rows.extend(batch_rows)
        written_scrape = 0
        if scrape_rows:
            written_scrape = await write_career_pages_to_archive_ii(session, scrape_rows)

        # i + len(results), NOT i + batch_size: if hit_deadline fired mid-
        # batch, results is short of the full batch — this reflects what
        # ACTUALLY finished and got written just above, not what was merely
        # scheduled. When the batch completes normally these are identical.
        done = i + len(results)
        elapsed = time.monotonic() - crawl_start
        rate = stats["companies_attempted"] / elapsed if elapsed > 0 else 0
        hit_n = stats["hits_from_homepage"] + stats["hits_from_career_path"] + stats["hits_from_sitemap"]
        dup_note = f", {duplicates_collapsed} dup collapsed" if duplicates_collapsed else ""
        log.info(f"  {done}/{len(tasks)} {unit_label} — {rate:.1f}/sec — {elapsed:.0f}s elapsed")
        log.info(f"    → {written}/{len(batch_rows)} written to {ARCHIVE_I_TABLE}{dup_note} — {len(found_rows)} hits total "
                 f"(hit rate so far: {hit_n / max(stats['companies_attempted'], 1) * 100:.2f}%)")
        if scrape_rows:
            log.info(f"    → {written_scrape}/{len(scrape_rows)} career pages written to {ARCHIVE_II_TABLE}")
        if shard_index is not None and shard_count is not None:
            # This batch's rows are already durably committed above, so
            # start_at + done is always safe to resume from.
            await save_crawl_checkpoint(session, discovery_method, shard_index, shard_count, start_at + done)
        if hit_deadline:
            # Deadline hit MID-batch (not just between batches, the older
            # check above) — the stragglers still in flight when the clock
            # ran out were already cancelled inside _run_with_deadline.
            # Everything that DID finish is written and checkpointed above;
            # stop here instead of starting another batch.
            time_budget_hit = True
            log.warning(f"  time budget ({time_budget_minutes}min) reached mid-batch at {done}/{len(tasks)} "
                        f"{unit_label} — stopping here, everything found so far is written.")
            break
    if not time_budget_hit and shard_index is not None and shard_count is not None:
        # Ran to the end of this shard's list without self-stopping — fully
        # done, so clear the checkpoint rather than leave a stale "done"
        # count a later, differently-shaped run could misread.
        await clear_crawl_checkpoint(session, discovery_method, shard_index, shard_count)
    return len(tasks), elapsed, rate, time_budget_hit


def new_parse_pool() -> concurrent.futures.Executor:
    """ThreadPoolExecutor (2026-08, was ProcessPoolExecutor) — see
    PARSE_WORKERS' comment above for why. Every caller (people_data_labs_probe.py,
    opendata_probe.py, common_crawl_probe.py, bigpicture_probe.py,
    github_org_probe.py, host_crawl_v2.py) just passes this straight into
    crawl_batch()'s loop.run_in_executor() call, so no caller-side changes
    were needed for this fix to take effect."""
    return concurrent.futures.ThreadPoolExecutor(max_workers=PARSE_WORKERS)


def new_connector() -> aiohttp.TCPConnector:
    """aiodns gives aiohttp a real concurrent async resolver — DNS for
    millions of DIFFERENT new domains can itself bottleneck under the
    default thread-pool resolver at high concurrency. Falls back cleanly
    (slower, still correct) if aiodns isn't installed."""
    resolver = None
    try:
        import aiodns  # noqa: F401
        resolver = aiohttp.AsyncResolver()
    except ImportError:
        log.warning("  aiodns not installed — DNS resolution will use the slower default resolver.")
    return aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, ttl_dns_cache=300, resolver=resolver)
