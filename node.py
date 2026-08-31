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

# 2026-08: "Cancel workflow" in GitHub Actions was reported as unreliable
# on live crawl runs — a click that should stop the job in seconds
# sometimes hung far longer. Python's DEFAULT SIGINT/SIGTERM handling
# should already exit fairly fast, but any moment spent inside a
# blocking C-level call (DNS resolution via aiodns/pycares is the prime
# suspect — it has its own event loop underneath asyncio's, and signal
# delivery into a foreign C event loop isn't as immediate as into plain
# Python bytecode) delays how soon the interpreter even gets to act on
# the pending signal. Installing an explicit handler here — imported by
# every probe/seed source via `import node` — guarantees an immediate,
# unconditional process exit (os._exit skips atexit/finally/GC entirely,
# on purpose: correctness on a cancelled run doesn't matter, speed does)
# the moment Python next gets a chance to run the handler, rather than
# depending on whatever the default disposition happens to be doing
# through several layers of C extensions. Applies project-wide — every
# source (kaggle/PDL, opendata, bigpicture, common_crawl, github_org)
# calls into node.py, so this one fix covers all of them.
def _fast_exit_on_signal(signum, frame):
    log.warning(f"Received signal {signum} (cancel requested) — exiting immediately, no cleanup.")
    os._exit(1)


signal.signal(signal.SIGTERM, _fast_exit_on_signal)
signal.signal(signal.SIGINT, _fast_exit_on_signal)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
ARCHIVE_I_TABLE = "archive_i"  # was slug_registry — the trusted, actually-scraped-daily list.
ARCHIVE_II_TABLE = "archive_ii"  # was archive_iii — in-house/unsupported career pages.
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
    "apply now", "apply today", "apply here", "apply online",
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
_STRONG_HIRING_RE = re.compile("|".join(re.escape(p) for p in _STRONG_HIRING_PHRASES), re.I)
_WEAK_HIRING_RE = re.compile(r"\b(?:" + "|".join(re.escape(w) for w in _WEAK_HIRING_WORDS) + r")\b", re.I)


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
        # 2026-08: capture_inhouse=False (opendata_probe.py, common_crawl_probe.py)
        # means "don't feed archive_ii from this source" — neither of those
        # sources carries an employee-count/size signal, so a no-ATS hit from
        # them is indistinguishable from a genuine small business; archive_ii
        # was getting flooded with exactly that noise. people_data_labs_probe.py (and
        # any future size-aware source) still captures normally — but note
        # both of THOSE also now pre-filter to companies above the size
        # threshold before ever reaching crawl_one, so what lands in
        # archive_ii is a company that's both (a) above the size bar and
        # (b) confirmed to have no recognized ATS.
        if best_inhouse and capture_inhouse:
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
    {ats, slug, source, last_seen} — no source_hostname/root_domain/country
    columns. 2026-08 restructure: this now writes STRAIGHT to archive_i
    (formerly slug_registry) with no intermediate staging/verify table —
    the old archive_ii (ATS-match quarantine table a separate verify step
    promoted from) is dropped entirely. discovery_method maps to archive_i's
    `source` column, which is CHECK-constrained — every discovery_method
    string a live caller passes (people_data_labs_probe, github_org_probe,
    common_crawl_probe, plus discovery.py's own set) must already be in
    archive_i_source_check or the upsert fails for that whole batch."""
    now_iso = datetime.now(timezone.utc).isoformat()
    slim_rows = [{"ats": r["ats"], "slug": r["slug"], "source": r["discovery_method"],
                  "last_seen": now_iso} for r in rows]
    return await _upsert_rows(session, ARCHIVE_I_TABLE, "ats,slug", slim_rows)


async def write_career_pages_to_archive_ii(session: aiohttp.ClientSession, rows: list[dict]) -> int:
    """rows: {"career_page_url","website_url",
    "discovery_method"} (from crawl_one's career_page_capture — only ever
    produced when no known ATS matched — plus discovery_method attached by
    crawl_batch). Upserts on website_url (2026-08: root_domain was dropped
    as a pure duplicate of this field, so website_url is now archive_ii's
    identity key instead) — a re-crawled company updates last_seen/
    career_page_url in place rather than duplicating. date_added is
    deliberately left OUT of the payload: the column's DEFAULT now() only
    fires on a true first INSERT, and since we never send it on an
    UPDATE, Postgres's merge-duplicates ON CONFLICT leaves the original
    date_added untouched.

    2026-08 restructure: this table was archive_iii before the old
    archive_ii (ATS-match staging table) was dropped and archive_iii was
    renamed to take its place — archive_ii now means "in-house/unsupported
    career pages," feeding Crawl II's heuristic job-listing scraper."""
    now_iso = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["last_seen"] = now_iso
    return await _upsert_rows(session, ARCHIVE_II_TABLE, "website_url", rows)


# ── shared batch driver ───────────────────────────────────────────────────

async def crawl_batch(domains: list[str], session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                       stats: dict, parse_pool: concurrent.futures.Executor,
                       target_geo_countries: set[str], discovery_method: str,
                       found_rows: list[dict], crawl_start: float, time_budget_seconds: float,
                       time_budget_minutes: int, batch_size: int = 3000, unit_label: str = "companies",
                       capture_inhouse: bool = True,
                       capture_inhouse_domains: set[str] | None = None,
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
        people_data_labs_probe.py/bigpicture_probe.py used to drop small companies
        from the crawl LIST entirely at seed time, which filtered out
        their archive_i-eligible ATS hits too, for almost nothing gained
        — now every company in the target countries gets crawled (feeding
        archive_i normally no matter its size), and the employee-count
        floor is applied ONLY at the point a company would otherwise
        become an archive_ii in-house-page candidate, computed by the
        caller from its own already-loaded size data.
      - If capture_inhouse_domains is None, falls back to the flat
        `capture_inhouse` bool for every domain (opendata_probe.py/
        common_crawl_probe.py still pass capture_inhouse=False this way —
        they have no size signal for ANY domain, so there's no per-domain
        set to build)."""
    def _capture_for(domain: str) -> bool:
        if capture_inhouse_domains is not None:
            return domain in capture_inhouse_domains
        return capture_inhouse

    tasks = [crawl_one(session, sem, d, stats, parse_pool, target_geo_countries, _capture_for(d))
             for d in domains]
    elapsed, rate = 0.0, 0.0
    time_budget_hit = False
    for i in range(0, len(tasks), batch_size):
        if time.monotonic() - crawl_start >= time_budget_seconds:
            for t in tasks[i:]:
                t.close()
            time_budget_hit = True
            log.warning(f"  time budget ({time_budget_minutes}min) reached at {i}/{len(tasks)} "
                        f"{unit_label} — stopping here, everything found so far is written.")
            break
        batch = tasks[i:i + batch_size]
        results = await asyncio.gather(*batch)
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

        done = min(i + batch_size, len(tasks))
        elapsed = time.monotonic() - crawl_start
        rate = stats["companies_attempted"] / elapsed if elapsed > 0 else 0
        hit_n = stats["hits_from_homepage"] + stats["hits_from_career_path"] + stats["hits_from_sitemap"]
        dup_note = f", {duplicates_collapsed} dup collapsed" if duplicates_collapsed else ""
        log.info(f"  {done}/{len(tasks)} {unit_label} — {rate:.1f}/sec — {elapsed:.0f}s elapsed")
        log.info(f"    → {written}/{len(batch_rows)} written to {ARCHIVE_I_TABLE}{dup_note} — {len(found_rows)} hits total "
                 f"(hit rate so far: {hit_n / max(stats['companies_attempted'], 1) * 100:.2f}%)")
        if scrape_rows:
            log.info(f"    → {written_scrape}/{len(scrape_rows)} career pages written to {ARCHIVE_II_TABLE}")
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
