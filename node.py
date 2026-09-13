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
from collections import Counter
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
# ... [Signal handling comments omitted for brevity, identical to original] ...
def _hard_exit_on_sigint(signum, frame):
    os._exit(130)
signal.signal(signal.SIGINT, _hard_exit_on_sigint)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
ARCHIVE_I_TABLE = "archive_i"  # was slug_registry
ARCHIVE_II_TABLE = "archive_ii"  # was archive_iii
CHECKPOINT_TABLE = "crawl_checkpoints"
STAT_TALLY_TABLE = "crawl_stat_tallies"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# OPTIMIZED (2026-09): Dropped timeouts to drop dead hosts faster and prevent 
# them from clogging async semaphore slots.
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=8, connect=4)

MAX_PAGE_BYTES = 3_000_000  # safety cap, not a realistic limit

# OPTIMIZED (2026-09): Maxed out concurrency limits to fully saturate network I/O 
# on modern GitHub Actions runners.
CRAWL_CONCURRENCY = int(os.environ.get("CRAWL_CONCURRENCY", "800"))
CONNECTOR_LIMIT = int(os.environ.get("CRAWL_CONNECTOR_LIMIT", str(CRAWL_CONCURRENCY + 200)))

# 2026-08: PARSE_WORKERS used to size a ProcessPoolExecutor...
PARSE_WORKERS = int(os.environ.get("PARSE_WORKERS", "16"))
TIME_BUDGET_MINUTES = int(os.environ.get("CRAWL_TIME_BUDGET_MINUTES", "330"))

CAREER_PATHS = [
 "/careers",  "/career",  "/careers-home",  "/careers-and-jobs",
 "/jobs",  "/job-openings",  "/open-positions",  "/open-roles",  "/open-jobs",
 "/openings",  "/current-openings",  "/vacancies",  "/vacancy",  "/job-search",
 "/find-a-job",  "/positions",  "/opportunities",
 "/join-us",  "/join",  "/join-our-team",  "/join-the-team",
 "/work-with-us",  "/work-for-us",  "/work-here",
 "/about/careers",  "/about-us/careers",  "/company/careers",  "/company/jobs",
 "/about/jobs",  "/about-us/jobs",  "/team/careers",
 "/hiring",  "/we-are-hiring",  "/now-hiring",  "/were-hiring",
 "/employment",  "/employment-opportunities",
 "/recruitment",  "/recruiting",  "/talent",
 "/apply",  "/apply-now",
]

CAREER_LIKE_RE = re.compile(
r"\b(?:" + "|".join(re.escape(p.strip("/")).replace("-", "-?") for p in CAREER_PATHS) + r")\b",
re.I)

SITEMAP_MAX_FOLLOW = 8
SITEMAP_INDEX_PATHS = ("/sitemap.xml", "/sitemap_index.xml")

_BLOG_LIKE_PATH_RE = re.compile(
r"/(?:blog|news|press|media|insights|articles?|resources|case-studies)/"
r"|/\d{4}/\d{1,2}(?:/\d{1,2})?/", re.I)

def _looks_like_sentence_slug(path: str, max_words: int = 6) -> bool:
    segments = [s for s in path.strip("/").split("/") if s]
    if not segments:
        return False
    words = [w for w in segments[-1].split("-") if w]
    return len(words) > max_words

_JOB_LISTING_LINK_PHRASES = [
 "view all jobs",  "view jobs",  "view open positions",  "view open roles",
 "view careers",  "view our jobs",  "view our careers",  "view our openings",
 "view current openings",  "view all openings",  "view all roles",  "view roles",
 "view all vacancies",  "view vacancies",  "view all positions",  "view positions",
 "view job openings",  "view current job openings",  "view open jobs",
 "see all jobs",  "see open positions",  "see open roles",  "see our openings",
 "see careers",  "see all openings",  "see current openings",  "see all positions",
 "see all vacancies",  "see open jobs",  "see job openings",
 "browse jobs",  "browse careers",  "browse openings",  "browse open positions",
 "browse our jobs",  "browse all jobs",  "browse vacancies",  "browse job openings",
 "browse open roles",  "browse current openings",  "browse career opportunities",
 "browse positions",  "browse roles",  "browse all positions",
 "explore careers",  "explore our careers",  "explore jobs",  "explore roles",
 "explore our roles",  "explore open positions",  "explore opportunities",
 "explore career opportunities",  "explore all jobs",  "explore current openings",
 "explore job openings",  "explore all positions",  "explore open roles",
 "search jobs",  "search openings",  "search open positions",  "search careers",
 "search all jobs",  "search current openings",  "search vacancies",
 "find a job",  "find jobs",  "find your next role",  "find open positions",
 "find our openings",  "find current openings",  "find open roles",
 "job board",  "careers page",  "our jobs",  "our openings",  "our careers",
 "our current openings",  "our open positions",  "our open roles",  "our vacancies",
 "current openings",  "current opportunities",  "current vacancies",
 "current job openings",  "current job opportunities",
 "open positions",  "open roles",  "job openings",  "job opportunities",
 "all open positions",  "all open roles",  "all current openings",
 "all job openings",  "all vacancies",  "all positions",  "all roles",
 "latest openings",  "latest jobs",  "latest vacancies",  "latest job openings",
 "check out our openings",  "check our openings",  "check out our jobs",
 "career opportunities",  "join our team",  "join the team",
]

_JOB_LISTING_LINK_RE = re.compile(
"|".join(re.escape(p) for p in set(_JOB_LISTING_LINK_PHRASES)), re.I)

_MAX_CAREER_LINK_FOLLOW_PER_TIER = 3

def _extract_job_listing_link_candidates(html: str, base_url: str) -> list[tuple[str, int]]:
    candidates: dict[str, int] = {}
    try:
        tree = LexborHTMLParser(html)
        for a_node in tree.css("a[href]"):
            href = a_node.attributes.get("href")
            if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            try:
                url = _clean_extracted_url(urljoin(base_url, href))
            except ValueError:
                continue
            if not url:
                continue
            text_sources = " ".join([
            a_node.text(strip=True) or "",
            a_node.attributes.get("aria-label") or "",
            a_node.attributes.get("title") or "",
            ])
            score = 0
            if _JOB_LISTING_LINK_RE.search(text_sources):
                score += 1
            path = urlparse(url).path
            if (CAREER_LIKE_RE.search(path)
            and not _BLOG_LIKE_PATH_RE.search(path)
            and not _looks_like_sentence_slug(path)):
                score += 1
            if score == 0:
                continue
            if score > candidates.get(url, -1):
                candidates[url] = score
    except Exception:
        pass
    return list(candidates.items())

MIN_CAREER_PAGE_TEXT_CHARS = 250

_STRONG_HIRING_PHRASES = [
 "current openings",  "current opening",  "current vacancies",  "current vacancy",
 "open positions",  "open position",  "open roles",  "open role",
 "job openings",  "job opening",  "we're hiring",  "we are hiring",  "now hiring",
 "join our team",  "join the team",  "join our growing team",
 "submit your application",  "submit an application",  "submit your resume",
 "send us your resume",  "send your resume",  "send your cv",  "submit your cv",
 "employment opportunities",  "career opportunities",  "job opportunities",
 "view openings",  "view our openings",  "view current openings",  "view all jobs",
 "see our openings",  "browse openings",  "browse our jobs",  "browse open positions",
 "explore careers",  "explore our careers",  "explore open positions",
 "search jobs",  "search openings",  "search open positions",
 "find your next role",  "find a job",  "meet our hiring team",
 "equal opportunity employer",  "we are an equal opportunity employer",
 "join us and",  "grow your career with us",  "build your career with us",
]

_WEAK_HIRING_WORDS = [
 "career",  "careers",  "job",  "jobs",  "position",  "positions",
 "vacancy",  "vacancies",  "hiring",  "recruit",  "recruiting",  "recruitment",
 "recruiter",  "talent",  "opening",  "openings",  "employment",
 "internship",  "internships",  "apprenticeship",  "apprenticeships",
 "resume",  "cv",  "candidate",  "candidates",  "applicant",  "applicants",
 "onboarding",  "workforce",  "headcount",
]

_WEAK_HIRING_PHRASES = ["apply now",  "apply today",  "apply here",  "apply online"]

_STRONG_HIRING_RE = re.compile("|".join(re.escape(p) for p in _STRONG_HIRING_PHRASES), re.I)
_WEAK_HIRING_RE = re.compile(
r"\b(?:" + "|".join(re.escape(w) for w in _WEAK_HIRING_WORDS + _WEAK_HIRING_PHRASES) + r")\b",
re.I,
)

def _has_hiring_vocabulary(text: str) -> bool:
    if _STRONG_HIRING_RE.search(text):
        return True
    weak_hits = {m.group(0).lower() for m in _WEAK_HIRING_RE.finditer(text)}
    return len(weak_hits) >= 2

ACCEPT_ANY_COUNTRY = set(geo.COUNTRY_ALIASES.values()) | set(geo.COUNTRY_CONTINENT.keys())
_TO_GEO_COUNTRY_OVERRIDES = {"czechia": "Czech Republic"}

def target_countries_geo_form(style_countries: set[str]) -> set[str]:
    return {_TO_GEO_COUNTRY_OVERRIDES.get(c, c.title()) for c in style_countries}

_URL_RE = re.compile(r'https?://[^\s"\'<>\`]{4,300}', re.I)
_MAX_CANDIDATE_URLS_PER_PAGE = 4000
_GRNH_SE_RE = re.compile(r'https?://grnh.se/(?!embed/)[A-Za-z0-9]+', re.I)
_MAX_GRNH_SE_PER_PAGE = 3
_HTML_ENTITY_RE = re.compile(r'&(?:quot|apos|amp|gt|lt|nbsp|#[0-9]+|#x[0-9a-fA-F]+);', re.I)
_CURLY_QUOTE_RE = re.compile(r'[""‘’]')
_TRAILING_STATUS_CODE_RE = re.compile(r'(?:;[0-9]{2,4})+;?$')
_JS_TEMPLATE_PLACEHOLDER_RE = re.compile(r'\${')

def _clean_extracted_url(url: str) -> str:
    if _JS_TEMPLATE_PLACEHOLDER_RE.search(url):
        return ""
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
    urls: set[str] = set()
    try:
        tree = LexborHTMLParser(html)
        for node in tree.css("a[href]"):
            href = node.attributes.get("href")
            if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            try:
                cleaned = _clean_extracted_url(urljoin(base_url, href))
            except ValueError:
                continue
            if cleaned:
                urls.add(cleaned)
            if len(urls) >= _MAX_CANDIDATE_URLS_PER_PAGE:
                break
    except Exception:
        pass
    for m in _URL_RE.finditer(html):
        cleaned = _clean_extracted_url(m.group(0))
        if cleaned:
            urls.add(cleaned)
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

def _extract_address_zone_text(html: str) -> str:
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
    try:
        tree = LexborHTMLParser(html)
        body = tree.css_first("body")
        return body.text(strip=True) if body else tree.root.text(strip=True)
    except Exception:
        return ""

def _parse_detect(html: str, base_url: str, target_geo_countries: set[str]
) -> tuple[list[tuple[str, str, str]], str | None, str | None, int, bool]:
    urls = _extract_candidate_urls(html, base_url) | _extract_jsonld_urls(html)
    hits = _detect_ats_hits(urls)
    country, method = detect_country(html, target_geo_countries)
    text = _extract_visible_text(html)
    text_len = len(text)
    has_hiring_vocab = _has_hiring_vocabulary(text)
    return hits, country, method, text_len, has_hiring_vocab

_ATS_VENDOR_DOMAINS = (
 "greenhouse.io",  "lever.co",  "ashbyhq.com",  "bamboohr.com",  "icims.com",
 "myworkdayjobs.com",  "rippling.com",  "workable.com",  "recruitee.com",
 "smartrecruiters.com",  "taleo.net",  "oraclecloud.com",  "brassring.com",
 "teamtailor.com",  "successfactors.com",  "successfactors.eu",  "sapsf.com",  "sapsf.eu",
 "breezy.hr",  "hrmdirect.com",  "softgarden.io",  "softgarden.de",  "zohorecruit.com",
 "zohorecruit.eu",  "paylocity.com",  "join.com",  "personio.de",  "personio.com",
 "workatastartup.com",  "ycombinator.com",  "eploy.net",  "folksats.app",  "glowinthecloud.com",
 "jobadder.com",  "jobvite.com",  "adp.com",  "avature.net",
 "pageuppeople.com",  "pinpointhq.com",  "flatchr.io",  "jobylon.com",
 "occupop-careers.com",
 "csod.com",  "ultipro.com",  "dayforcehcm.com",  "applytojob.com",
 "comeet.co",  "phenompeople.com",  "eightfold.ai",  "clearcompanyhr.com",
 "freshteam.com",  "newtonsoftware.com",  "applicantpro.com",
 "hiringthing.com",  "paycomonline.com",  "isolvedhire.com",
 "getro.com",
)

def _looks_like_real_career_page(url: str, text_len: int, has_hiring_vocab: bool, origin: str) -> bool:
    if text_len < MIN_CAREER_PAGE_TEXT_CHARS:
        return False
    if not has_hiring_vocab:
        return False
    host = (urlparse(url).hostname or "").lower()
    if any(host == d or host.endswith("." + d) for d in _ATS_VENDOR_DOMAINS):
        return False
    path = urlparse(url).path.strip("/")
    if path == "" and url.rstrip("/") == origin.rstrip("/"):
        return False
    return True

def _best_inhouse_candidate(candidates: list[dict], origin: str) -> dict | None:
    best = None
    for c in candidates:
        if c["hits"]:
            continue
        if not _looks_like_real_career_page(c["url"], c["text_len"], c["has_hiring_vocab"], origin):
            continue
        if best is None or c["text_len"] > best["text_len"]:
            best = c
    return best

_ORG_SCHEMA_TYPE_RE = re.compile(r'"@type"\s*:\s*"(?:Organization|Corporation)"', re.I)
EMPLOYEE_COUNT_MIN_FOR_CREDIT = 200
_ORG_EMPLOYEE_COUNT_RE = re.compile(
r'"numberOfEmployees"\s*:\s*(?:{[^}]{0,160}?"(?:value|minValue)"\s*:\s*)?"?(\d{1,9})"?', re.I)
_ORG_AUTHORITY_SAMEAS_RE = re.compile(
r'"sameAs"\s*:\s*\[[^\]]{0,600}(?:wikipedia.org|crunchbase.com|bloomberg.com)',
re.I)
_ORG_REGULATOR_SAMEAS_RE = re.compile(
r'"sameAs"\s*:\s*\[[^\]]{0,600}(?:'
r'sec.gov|'                                          
r'company-information.service.gov.uk|'              
r'sedarplus.ca|'                                       
r'asic.gov.au|'                                       
r'cro.ie|'                                             
r'companies-register.companiesoffice.govt.nz|'       
r'bizfile.gov.sg|'                                    
r'kvk.nl|'                                             
r'brreg.no|'                                           
r'bolagsverket.se|'                                    
r'virk.dk|'                                            
r'ytj.fi|'                                             
r'justizonline.gv.at|justiz.gv.at|'                 
r'kbo-bce.be|'                                         
r'skatturinn.is|'                                      
r'lbr.lu|'                                             
r'annuaire-entreprises.data.gouv.fr|infogreffe.fr|'  
r'handelsregister.de'                                  
r')', re.I)
_CORP_FOOTER_LINKS_RE = re.compile(
r'\b(?:investor relations|investors?|newsroom|press releases?|board of directors|'
r'esg\b|sustainability report|annual report|shareholders?|corporate governance|'
r'executive (?:team|leadership)|leadership team|media kit|quarterly results|'
r'earnings call|form 10-k|proxy statement)\b', re.I)
_LEGAL_ENTITY_SUFFIX_RE = re.compile(
r'(?:©|copyright)[^\n<]{0,80}\b(?:inc.?|llc|l.l.c.|corp(?:oration)?.?|gmbh|plc|s.a.|ltd.?|'
r'pty.?\sltd.?|pte.?\sltd.?|b.?v.?|s.?p.?a.?|s.?r.?l.?|s.?a.?s.?|a.?g.?|a.?b.?|'
r'a/s|k.?k.?|co.,?\s*ltd.?|oy|ug)\b',
re.I)
_COMPLIANCE_BANNER_RE = re.compile(r'(?:onetrust.com|cookielaw.org|trustarc.com|cookiebot.com)', re.I)
_ENTERPRISE_MARTECH_RE = re.compile(
r'(?:6sense.com|demandbase.com|marketo.net|pardot.com|omtrdc.net|2o7.net|'
r'exacttarget.com|salesforceliveagent.com|eloqua.com|terminus.com|rollworks.com|'
r'zoominfo.com|bombora.com)', re.I)
_OBSERVABILITY_RE = re.compile(
r'(?:datadoghq.com|dynatrace.com|newrelic.com|nr-data.net|sentry.io|js.sentry-cdn.com|'
r'appdynamics.com|instana.io|splunkcloud.com)', re.I)

QUALITY_INDEX_THRESHOLD = 35

_MX_ENTERPRISE_GATEWAY_RE = re.compile(
r'(?:mimecast.com|pphosted.com|ppe-hosted.com|iphmx.com|barracudanetworks.com|'
r'forcepoint.com|mailcontrol.com|messagelabs.com)', re.I)
_MX_MAINSTREAM_HOSTED_RE = re.compile(
r'(?:google.com|googlemail.com|aspmx.l.google.com|outlook.com|protection.outlook.com)', re.I)
MX_LOOKUP_TIMEOUT = 3.0
_mx_resolver = None

def _get_mx_resolver():
    global _mx_resolver
    if _mx_resolver is None:
        try:
            import aiodns
            _mx_resolver = aiodns.DNSResolver()
        except ImportError:
            _mx_resolver = False
    return _mx_resolver or None

async def _mx_provider_score(domain: str) -> tuple[int, str | None]:
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

WEBGRAPH_TIERS_URL = os.environ.get("WEBGRAPH_TIERS_URL", "")
WEBGRAPH_RANK_BANDS = (
("S+", 1_000_000, 20),
("S", 10_000_000, 15),
("A", 25_000_000, 12),
("B", 40_000_000, 8),
)
_webgraph_ranks: dict[str, str] | None = None
_webgraph_load_lock: asyncio.Lock | None = None
_WEBGRAPH_MAX_PARTS = 8
_WEBGRAPH_MIN_PART_BYTES = 32 * 1024 * 1024
_WEBGRAPH_PART_RETRIES = 3

async def _fetch_range(session: aiohttp.ClientSession, url: str, start: int, end: int,
timeout: aiohttp.ClientTimeout) -> bytes:
    last_exc: Exception | None = None
    for attempt in range(_WEBGRAPH_PART_RETRIES + 1):
        try:
            async with session.get(url, headers={"Range": f"bytes={start}-{end}"},
                                    timeout=timeout) as r:
                r.raise_for_status()
                return await r.read()
        except Exception as e:
            last_exc = e
            if attempt < _WEBGRAPH_PART_RETRIES:
                await asyncio.sleep(min(2 ** (attempt + 1), 20))
    raise last_exc

async def _load_webgraph_ranks(session: aiohttp.ClientSession) -> dict[str, str]:
    global _webgraph_ranks, _webgraph_load_lock
    if _webgraph_ranks is not None:
        return _webgraph_ranks
    if _webgraph_load_lock is None:
        _webgraph_load_lock = asyncio.Lock()
    async with _webgraph_load_lock:
        if _webgraph_ranks is not None:
            return _webgraph_ranks
        if not WEBGRAPH_TIERS_URL:
            _webgraph_ranks = {}
            return _webgraph_ranks
        log.info(f"Loading WebGraph rank bands from {WEBGRAPH_TIERS_URL} (once per run)...")
        ranks: dict[str, str] = {}
        t0 = time.monotonic()
        downloaded = 0
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=90)
            async with session.get(WEBGRAPH_TIERS_URL, headers={"Range": "bytes=0-0"},
                                    timeout=timeout) as probe:
                probe.raise_for_status()
                if probe.status == 206:
                    content_range = probe.headers.get("Content-Range", "")
                    total_bytes = (int(content_range.rsplit("/", 1)[-1])
                                    if "/" in content_range else 0)
                    resolved_url = str(probe.url)
                    await probe.read()
                else:
                    resolved_url = None
                    total_bytes = 0
                    raw = await probe.read()
                    downloaded = len(raw)
            if resolved_url and total_bytes:
                part_count = max(1, min(_WEBGRAPH_MAX_PARTS, total_bytes // _WEBGRAPH_MIN_PART_BYTES))
                if part_count <= 1:
                    async with session.get(resolved_url, timeout=timeout) as r:
                        r.raise_for_status()
                        raw = await r.read()
                        downloaded = len(raw)
                else:
                    part_size = -(-total_bytes // part_count)
                    byte_ranges = []
                    start = 0
                    while start < total_bytes:
                        end = min(start + part_size, total_bytes) - 1
                        byte_ranges.append((start, end))
                        start = end + 1
                    log.info(f"  host supports Range requests — fetching {len(byte_ranges)} "
                              f"parts of ~{part_size / (1024*1024):.0f}MB each in parallel "
                              f"({total_bytes / (1024*1024):,.0f}MB total)...")
                    async def _get_part(part_index: int, start: int, end: int) -> tuple[int, bytes]:
                        nonlocal downloaded
                        data = await _fetch_range(session, resolved_url, start, end, timeout)
                        downloaded += len(data)
                        log.info(f"    part {part_index + 1}/{len(byte_ranges)} done — "
                                  f"{downloaded / (1024*1024):,.0f}MB downloaded so far "
                                  f"({time.monotonic() - t0:.0f}s elapsed)")
                        return part_index, data
                    parts = await asyncio.gather(
                        *[_get_part(i, s, e) for i, (s, e) in enumerate(byte_ranges)]
                    )
                    parts.sort(key=lambda p: p[0])
                    raw = b"".join(data for _, data in parts)
            elif resolved_url:
                async with session.get(resolved_url, timeout=timeout) as r:
                    r.raise_for_status()
                    raw = await r.read()
                    downloaded = len(raw)
            text = (gzip.decompress(raw) if WEBGRAPH_TIERS_URL.endswith(".gz") else raw) \
                .decode("utf-8", errors="ignore")
            for line in text.splitlines()[1:]:
                domain, _, band = line.strip().partition(",")
                if not domain or not band:
                    continue
                ranks[domain] = band
            log.info(f"  loaded {len(ranks):,} WebGraph-ranked domains "
                      f"({downloaded / (1024*1024):,.0f}MB, {time.monotonic() - t0:.0f}s)")
        except Exception as e:
            log.warning(f"  failed to load WebGraph ranks ({e!r}) after "
                        f"{downloaded / (1024*1024):,.0f}MB in {time.monotonic() - t0:.0f}s "
                        f"— WebGraph signal disabled this run")
            ranks = {}
        _webgraph_ranks = ranks
        return _webgraph_ranks

async def _webgraph_score(session: aiohttp.ClientSession, domain: str) -> tuple[int, str | None]:
    ranks = await _load_webgraph_ranks(session)
    band = ranks.get(domain)
    for label, _, points in WEBGRAPH_RANK_BANDS:
        if band == label:
            return points, f"webgraph_rank{label.lower().replace('+', 'plus')}"
    return 0, None

def _quality_index_score(html: str) -> tuple[int, list[str]]:
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

_WIKIPEDIA_SAMEAS_URL_RE = re.compile(
r'"sameAs"\s*:\s*\[[^\]]{0,600}?"(https?://[a-z]{2,3}\.wikipedia\.org/wiki/[^"]+)"', re.I)
WIKIPEDIA_VERIFY_TIMEOUT = 5.0

async def _wikipedia_mention_score(session: aiohttp.ClientSession, html: str,
domain: str) -> tuple[int, str | None]:
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
    accepted = stats.get("quality_gated_accepted", 0)
    if not accepted:
        return
    prefix = "quality_signal__"
    signal_counts = {k[len(prefix):]: v for k, v in stats.items() if k.startswith(prefix)}
    if not signal_counts:
        return
    ranked = sorted(signal_counts.items(), key=lambda kv: -kv[1])
    breakdown = " ".join(f"{name}={count / accepted * 100:.1f}%" for name, count in ranked)
    log.info(f"  Quality Index signal mix ({accepted:,} Quality-Index-gated archive_ii "
             f"acceptances this run — % of THOSE that had each signal, not of all candidates "
             f"checked; doesn't sum to 100%, most accepted entries clear the bar on more than "
             f"one signal at once): {breakdown}")

_BINARY_CONTENT_PREFIXES = ("image/", "video/", "audio/", "font/",
"application/pdf", "application/zip", "application/octet-stream")

async def _fetch_page(session: aiohttp.ClientSession, url: str, stats: dict) -> tuple[str, str] | None:
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

async def _resolve_grnh_se_hits(session: aiohttp.ClientSession, html: str,
stats: dict) -> list[tuple[str, str, str]]:
    hits: list[tuple[str, str, str]] = []
    seen_short_urls: set[str] = set()
    for short_url in _GRNH_SE_RE.findall(html):
        if short_url in seen_short_urls:
            continue
        if len(seen_short_urls) >= _MAX_GRNH_SE_PER_PAGE:
            break
        seen_short_urls.add(short_url)
        page = await _fetch_page(session, short_url, stats)
        if not page:
            continue
        resolved_url, _ = page
        slug = URL_TO_SLUG["greenhouse"](resolved_url)
        if slug:
            hits.append(("greenhouse", slug, resolved_url))
    return hits

async def _fetch_sitemap(session: aiohttp.ClientSession, origin: str, stats: dict):
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

async def gather_page_candidates(detect_fn, pages: list[tuple[str, str] | None]) -> list[dict]:
    out = []
    for p in pages:
        if not p:
            continue
        url, html = p
        hits, text_len, has_hiring_vocab = await detect_fn(html, url)
        out.append({"url": url, "html": html, "hits": hits,
        "text_len": text_len, "has_hiring_vocab": has_hiring_vocab})
    return out

async def _follow_career_listing_links(detect_fn, session: aiohttp.ClientSession,
candidates: list[dict], already_fetched: set[str],
stats: dict) -> list[dict]:
    link_pool: dict[str, int] = {}
    for c in candidates:
        if c["hits"]:
            continue
        for url, score in _extract_job_listing_link_candidates(c["html"], c["url"]):
            if url in already_fetched:
                continue
            if score > link_pool.get(url, -1):
                link_pool[url] = score
    ranked = sorted(link_pool.items(), key=lambda kv: -kv[1])[:_MAX_CAREER_LINK_FOLLOW_PER_TIER]
    results = []
    for url, _score in ranked:
        already_fetched.add(url)
        page = await _fetch_page(session, url, stats)
        stats["career_link_follow_attempted"] += 1
        if not page:
            continue
        resolved_url, page_html = page
        already_fetched.add(resolved_url)
        direct_hits = _detect_ats_hits({resolved_url})
        page_hits, page_text_len, page_hiring_vocab = await detect_fn(page_html, resolved_url)
        merged_hits = _collapse_hits([direct_hits, page_hits])
        if merged_hits:
            stats["career_link_follow_ats_hit"] += 1
        results.append({"url": resolved_url, "html": page_html, "hits": merged_hits,
                         "text_len": page_text_len, "has_hiring_vocab": page_hiring_vocab})
    return results

async def detect_page_hits(session: aiohttp.ClientSession, parse_pool: concurrent.futures.Executor,
html: str, url: str, target_geo_countries: set[str],
stats: dict) -> tuple[list[tuple[str, str, str]], str | None, str | None, int, bool]:
    loop = asyncio.get_running_loop()
    hits, country, method, text_len, has_hiring_vocab = await loop.run_in_executor(
        parse_pool, _parse_detect, html, url, target_geo_countries)
    grnh_hits = await _resolve_grnh_se_hits(session, html, stats)
    if grnh_hits:
        hits = _collapse_hits([hits, grnh_hits])
    return hits, country, method, text_len, has_hiring_vocab

async def crawl_one(session: aiohttp.ClientSession, sem: asyncio.Semaphore, domain: str,
stats: dict, parse_pool: concurrent.futures.Executor,
target_geo_countries: set[str] = ACCEPT_ANY_COUNTRY,
capture_inhouse: bool = True,
apply_maturity_gate: bool = True,
) -> tuple[list[tuple[str, str, str, str, str, str | None, str | None]], dict | None]:
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
        candidates.append(f"http://{domain}")
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
            hits, country, method, text_len, has_hiring_vocab = await detect_page_hits(
                session, parse_pool, html_, url_, target_geo_countries, stats)
            if country and best_country is None:
                best_country, best_method = country, method
            return hits, text_len, has_hiring_vocab
        hits, home_text_len, home_hiring_vocab = await _detect(html, final_url)
        if hits:
            stats["hits_from_homepage"] += 1
            stats["known_ats_found"] += 1
            return ([(ats, slug, url, domain, "homepage", best_country, best_method) for ats, slug, url in hits],
                    None)
        origin_parts = urlparse(final_url)
        origin = f"{origin_parts.scheme}://{origin_parts.netloc}"
        already_fetched: set[str] = {final_url}
        homepage_as_candidate = [{"url": final_url, "html": html, "hits": [],
                                   "text_len": home_text_len, "has_hiring_vocab": home_hiring_vocab}]
        homepage_link_candidates = await _follow_career_listing_links(
            _detect, session, homepage_as_candidate, already_fetched, stats)
        merged = _collapse_hits([c["hits"] for c in homepage_link_candidates])
        if merged:
            stats["hits_from_homepage_link"] += 1
            stats["known_ats_found"] += 1
            return ([(ats, slug, url, domain, "homepage_link", best_country, best_method) for ats, slug, url in merged],
                    None)
        career_path_urls = [u for p in CAREER_PATHS
                             if (u := urljoin(origin, p)) not in already_fetched]
        career_pages = await asyncio.gather(
            *[_fetch_page(session, u, stats) for u in career_path_urls])
        for cp in career_pages:
            if cp:
                already_fetched.add(cp[0])
        career_candidates = await _gather_page_candidates(_detect, career_pages)
        merged = _collapse_hits([c["hits"] for c in career_candidates])
        if not merged:
            career_candidates += await _follow_career_listing_links(
                _detect, session, career_candidates, already_fetched, stats)
            merged = _collapse_hits([c["hits"] for c in career_candidates])
        if merged:
            stats["hits_from_career_path"] += 1
            stats["known_ats_found"] += 1
            return ([(ats, slug, url, domain, "career_path", best_country, best_method) for ats, slug, url in merged],
                    None)
        best_inhouse = _best_inhouse_candidate(career_candidates + homepage_link_candidates, origin)
        sitemap = await _fetch_sitemap(session, origin, stats)
        if sitemap:
            sm_url, sm_xml = sitemap
            loc_urls = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", sm_xml, re.I)
            career_like = [u for u in loc_urls
                           if CAREER_LIKE_RE.search(u)
                           and not _BLOG_LIKE_PATH_RE.search(urlparse(u).path)
                           and not _looks_like_sentence_slug(urlparse(u).path)
                           and u not in already_fetched]
            sm_pages = await asyncio.gather(
                *[_fetch_page(session, u, stats) for u in career_like[:SITEMAP_MAX_FOLLOW]])
            for sp in sm_pages:
                if sp:
                    already_fetched.add(sp[0])
            sitemap_candidates = await _gather_page_candidates(_detect, sm_pages)
            merged = _collapse_hits([c["hits"] for c in sitemap_candidates])
            if not merged:
                sitemap_candidates += await _follow_career_listing_links(
                    _detect, session, sitemap_candidates, already_fetched, stats)
                merged = _collapse_hits([c["hits"] for c in sitemap_candidates])
            if merged:
                stats["hits_from_sitemap"] += 1
                stats["known_ats_found"] += 1
                hit_url = next(c["url"] for c in sitemap_candidates if c["hits"])
                return ([(ats, slug, url, domain, "sitemap", best_country, best_method) for ats, slug, url in merged],
                        None)
            sitemap_inhouse = _best_inhouse_candidate(sitemap_candidates, origin)
            if sitemap_inhouse and (not best_inhouse or sitemap_inhouse["text_len"] > best_inhouse["text_len"]):
                best_inhouse = sitemap_inhouse
        stats["dropped_no_ats"] += 1
        if best_inhouse and capture_inhouse:
            if apply_maturity_gate:
                quality_score, quality_signals = await _quality_index_score_async(session, html, domain)
                if quality_score < QUALITY_INDEX_THRESHOLD:
                    stats["inhouse_dropped_low_quality"] += 1
                    log.debug(f"  archive_ii candidate dropped (Quality Index={quality_score} "
                              f"< {QUALITY_INDEX_THRESHOLD}, signals={quality_signals}): {domain}")
                    return [], None
                stats["quality_gated_accepted"] += 1
                for signal_name in quality_signals:
                    stats[f"quality_signal__{signal_name}"] += 1
            stats["inhouse_career_page_captured"] += 1
            return [], _capture(best_inhouse["url"])
        return [], None

async def _upsert_rows(session: aiohttp.ClientSession, table: str, on_conflict: str,
rows: list[dict]) -> int:
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
                    if r.status >= 400:
                        last_err = f"{r.status} {await r.text()}"
                        if r.status >= 500:
                            continue
                        break
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
    slim_rows = [{"ats": r["ats"], "slug": r["slug"], "source": r["discovery_method"]}
                 for r in rows if "${" not in r["slug"]]
    return await _upsert_rows(session, ARCHIVE_I_TABLE, "ats,slug", slim_rows)

async def write_career_pages_to_archive_ii(session: aiohttp.ClientSession, rows: list[dict]) -> int:
    return await _upsert_rows(session, ARCHIVE_II_TABLE, "website_url", rows)

async def save_crawl_checkpoint(session: aiohttp.ClientSession, source: str, shard_index: int,
shard_count: int, resume_offset: int,
partition: str | None = None) -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
               "Prefer": "resolution=merge-duplicates"}
    row = {"source": source, "shard_index": shard_index, "shard_count": shard_count,
           "resume_offset": resume_offset, "partition": partition,
           "updated_at": datetime.now(timezone.utc).isoformat()}
    try:
        async with session.post(f"{SUPABASE_URL}/rest/v1/{CHECKPOINT_TABLE}", headers=headers,
                                 params={"on_conflict": "source,shard_index,shard_count"},
                                 json=[row], timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
    except Exception as e:
        log.warning(f"  couldn't save crawl checkpoint at {resume_offset:,} (non-fatal — a future "
                    f"resume may just redo one extra batch): {e}")

async def load_crawl_checkpoint(session: aiohttp.ClientSession, source: str, shard_index: int,
shard_count: int) -> int:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return 0
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    params = {"source": f"eq.{source}", "shard_index": f"eq.{shard_index}",
    "shard_count": f"eq.{shard_count}", "select": "resume_offset"}
    try:
        async with session.get(f"{SUPABASE_URL}/rest/v1/{CHECKPOINT_TABLE}", headers=headers,
        params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
            data = await r.json()
            return data[0]["resume_offset"] if data else 0
    except Exception as e:
        log.warning(f"  couldn't load crawl checkpoint (starting this shard from 0): {e}")
        return 0

async def load_crawl_checkpoint_with_partition(session: aiohttp.ClientSession, source: str,
shard_index: int, shard_count: int
) -> tuple[str | None, int]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None, 0
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    params = {"source": f"eq.{source}", "shard_index": f"eq.{shard_index}",
    "shard_count": f"eq.{shard_count}", "select": "partition,resume_offset"}
    try:
        async with session.get(f"{SUPABASE_URL}/rest/v1/{CHECKPOINT_TABLE}", headers=headers,
        params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
            data = await r.json()
            if not data:
                return None, 0
            return data[0].get("partition"), data[0]["resume_offset"]
    except Exception as e:
        log.warning(f"  couldn't load crawl checkpoint (starting this shard from 0): {e}")
        return None, 0

async def clear_crawl_checkpoint(session: aiohttp.ClientSession, source: str, shard_index: int,
shard_count: int) -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    params = {"source": f"eq.{source}", "shard_index": f"eq.{shard_index}", "shard_count": f"eq.{shard_count}"}
    try:
        async with session.delete(f"{SUPABASE_URL}/rest/v1/{CHECKPOINT_TABLE}", headers=headers,
        params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
    except Exception as e:
        log.warning(f"  couldn't clear crawl checkpoint (non-fatal, just means a future full-restart  "
        f"run might unnecessarily skip ahead once): {e}")

async def flush_stat_tallies(session: aiohttp.ClientSession, source: str, campaign: str,
deltas: dict[str, int]) -> None:
    if not deltas:
        return
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"}
    payload = {
        "p_source": source, "p_campaign": campaign,
        "p_deltas": [{"metric": k, "delta": v} for k, v in deltas.items()],
    }
    try:
        async with session.post(f"{SUPABASE_URL}/rest/v1/rpc/increment_stat_tallies", headers=headers,
        json=payload, timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
    except Exception as e:
        log.warning(f"  couldn't flush {len(deltas)} cumulative stat(s) for campaign {campaign!r}  "
        f"(non-fatal — the final cumulative summary will just undercount by this much): {e}")

async def fetch_cumulative_stats(session: aiohttp.ClientSession, source: str,
campaign: str) -> tuple[dict[str, int], "Counter", "Counter", str | None, str | None]:
    empty = ({}, Counter(), Counter(), None, None)
    if not SUPABASE_URL or not SUPABASE_KEY:
        return empty
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    params = {"source": f"eq.{source}", "campaign": f"eq.{campaign}",
    "select": "metric,count,updated_at", "limit": "10000"}
    try:
        async with session.get(f"{SUPABASE_URL}/rest/v1/{STAT_TALLY_TABLE}", headers=headers,
        params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
            rows = await r.json()
    except Exception as e:
        log.warning(f"  couldn't fetch cumulative stats for campaign {campaign!r}: {e}")
        return empty
    if not rows:
        return empty
    stats: dict[str, int] = {}
    platform_counts: Counter = Counter()
    country_counts: Counter = Counter()
    timestamps = [row["updated_at"] for row in rows if row.get("updated_at")]
    for row in rows:
        metric, count = row["metric"], row["count"]
        if metric.startswith("platform__"):
            platform_counts[metric[len("platform__"):]] += count
        elif metric.startswith("country__"):
            country_counts[metric[len("country__"):]] += count
        else:
            stats[metric] = stats.get(metric, 0) + count
    first_seen = min(timestamps) if timestamps else None
    last_seen = max(timestamps) if timestamps else None
    return stats, platform_counts, country_counts, first_seen, last_seen

async def reset_stat_tallies(session: aiohttp.ClientSession, source: str, campaign: str) -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"}
    payload = {"p_source": source, "p_campaign": campaign}
    try:
        async with session.post(f"{SUPABASE_URL}/rest/v1/rpc/reset_stat_tallies", headers=headers,
        json=payload, timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
            log.info(f"  cumulative stats for campaign {campaign!r} reset (--reset-stats).")
    except Exception as e:
        log.warning(f"  couldn't reset cumulative stats for campaign {campaign!r}: {e}")

def log_crawl_summary(label: str, stats: dict, platform_counts: "Counter", country_counts: "Counter",
status_line: str, hosts_attempted: int, hosts_seeded: int | None = None,
elapsed_seconds: float | None = None, rate_per_sec: float | None = None,
time_budget_seconds: float | None = None) -> None:
    total_hits = (stats.get("hits_from_homepage", 0) + stats.get("hits_from_career_path", 0)
                  + stats.get("hits_from_sitemap", 0))
    hosts_n = max(hosts_attempted, 1)
    written_without_country = stats.get("written_without_country", 0)
    banner = "=" * 60
    log.info("")
    log.info(banner)
    log.info(f"COMMON CRAWL — {label}")
    log.info(banner)
    log.info(f"  status:      {status_line}")
    seeded_note = f", {hosts_seeded:,} seeded" if hosts_seeded is not None else ""
    log.info(f"  hosts:       {hosts_attempted:,} attempted{seeded_note}")
    if elapsed_seconds is not None:
        rate = rate_per_sec if rate_per_sec is not None else (
            hosts_attempted / elapsed_seconds if elapsed_seconds > 0 else 0.0)
        budget_note = f" of {time_budget_seconds:.0f}s budget" if time_budget_seconds is not None else ""
        log.info(f"  time:        {elapsed_seconds:.0f}s{budget_note}, {rate:.1f} hosts/sec avg")
    log.info("")
    log.info("  accuracy:")
    log.info(f"    ATS hits found:  {total_hits:,} ({total_hits / hosts_n * 100:.2f}% of hosts attempted)")
    log.info(f"    with country:    {total_hits - written_without_country:,} "
             f"({(1 - written_without_country / max(total_hits, 1)) * 100:.1f}% of hits)")
    log.info(f"    no ATS found:    {stats.get('dropped_no_ats', 0):,} "
             f"({stats.get('dropped_no_ats', 0) / hosts_n * 100:.1f}%)")
    log.info(f"    unreachable:     {stats.get('homepage_unreachable', 0):,} "
             f"({stats.get('homepage_unreachable', 0) / hosts_n * 100:.1f}%)")
    if total_hits:
        log.info("")
        log.info("  hits by tier:")
        for tier_key, tier_label in (("hits_from_homepage", "homepage"),
                                      ("hits_from_career_path", "career_path"),
                                      ("hits_from_sitemap", "sitemap")):
            n = stats.get(tier_key, 0)
            log.info(f"    {tier_label}:     {n:,} ({n / total_hits * 100:.1f}%)")
    if platform_counts:
        log.info("")
        log.info("  hits by platform:")
        for ats, n in platform_counts.most_common():
            log.info(f"    {ats}: {n:,}")
    if country_counts:
        log.info("")
        log.info("  hits by country:")
        for country, n in country_counts.most_common():
            log.info(f"    {country}: {n:,}")
    log.info("")
    log_quality_index_summary(stats)

POLL_INTERVAL = 0.5

async def _run_with_deadline(coros, deadline: float) -> tuple[list, bool]:
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
    def _capture_for(domain: str) -> bool:
        if capture_inhouse_domains is not None:
            return domain in capture_inhouse_domains
        return capture_inhouse
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
        results, hit_deadline = await _run_with_deadline(batch, deadline)
        batch_rows = []
        scrape_rows = []
        seen_keys = set()
        seen_domains = set()
        duplicates_collapsed = 0
        for hits, career_capture in results:
            for ats, slug, matched_url, domain, tier, country, method in hits:
                key = (ats, slug)
                if key in seen_keys:
                    duplicates_collapsed += 1
                    continue
                seen_keys.add(key)
                if not country:
                    stats["written_without_country"] += 1
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
            await save_crawl_checkpoint(session, discovery_method, shard_index, shard_count, start_at + done)
        if hit_deadline:
            time_budget_hit = True
            log.warning(f"  time budget ({time_budget_minutes}min) reached mid-batch at {done}/{len(tasks)} "
                        f"{unit_label} — stopping here, everything found so far is written.")
            break
    if not time_budget_hit and shard_index is not None and shard_count is not None:
        await clear_crawl_checkpoint(session, discovery_method, shard_index, shard_count)
    return len(tasks), elapsed, rate, time_budget_hit

def new_parse_pool() -> concurrent.futures.Executor:
    return concurrent.futures.ThreadPoolExecutor(max_workers=PARSE_WORKERS)

def new_connector() -> aiohttp.TCPConnector:
    resolver = None
    try:
        import aiodns  # noqa: F401
        resolver = aiohttp.AsyncResolver()
    except ImportError:
        log.warning("  aiodns not installed — DNS resolution will use the slower default resolver.")
    return aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, ttl_dns_cache=300, resolver=resolver)
