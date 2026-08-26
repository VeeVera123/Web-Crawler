"""
NODE — the one permanent crawl/detect/write engine every seed source
depends on. Seed scripts (class_a_probe.py, host_crawl_v2.py, and any
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
import sys
import time
from urllib.parse import urljoin, urlparse

import aiohttp
from dotenv import load_dotenv
from selectolax.lexbor import LexborHTMLParser

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import geo  # noqa: E402
from discovery import URL_TO_SLUG  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("node")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10, connect=6)
MAX_PAGE_BYTES = 3_000_000  # safety cap, not a realistic limit

CRAWL_CONCURRENCY = int(os.environ.get("CRAWL_CONCURRENCY", "400"))
CONNECTOR_LIMIT = int(os.environ.get("CRAWL_CONNECTOR_LIMIT", str(CRAWL_CONCURRENCY + 150)))
PARSE_WORKERS = int(os.environ.get("PARSE_WORKERS", str(max(1, (os.cpu_count() or 4) - 1))))
TIME_BUDGET_MINUTES = int(os.environ.get("CRAWL_TIME_BUDGET_MINUTES", "330"))

CAREER_PATHS = [
    "/careers", "/career",
    "/jobs", "/job-openings", "/open-positions", "/openings",
    "/join-us", "/join", "/work-with-us", "/work-for-us",
    "/about/careers", "/about-us/careers", "/company/careers", "/company/jobs",
    "/team/careers",
    "/hiring", "/we-are-hiring",
    "/apply",
]
CAREER_LIKE_RE = re.compile(
    "|".join(re.escape(p.strip("/")).replace("-", "-?") for p in CAREER_PATHS), re.I)
SITEMAP_MAX_FOLLOW = 8
SITEMAP_INDEX_PATHS = ("/sitemap.xml", "/sitemap_index.xml")

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


def _parse_detect(html: str, base_url: str, target_geo_countries: set[str]
                   ) -> tuple[list[tuple[str, str, str]], str | None, str | None]:
    """CPU-bound per-page work, bundled into one picklable function so it
    can run in a ProcessPoolExecutor worker (see crawl_one/PARSE_WORKERS)."""
    urls = _extract_candidate_urls(html, base_url) | _extract_jsonld_urls(html)
    hits = _detect_ats_hits(urls)
    country, method = detect_country(html, target_geo_countries)
    return hits, country, method


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
                     stats: dict, parse_pool: concurrent.futures.ProcessPoolExecutor,
                     target_geo_countries: set[str] = ACCEPT_ANY_COUNTRY
                     ) -> list[tuple[str, str, str, str, str, str | None, str | None]]:
    """Homepage -> career paths -> sitemap (guessed, then robots.txt).
    Every page fetched at a hit-bearing tier is merged (_collapse_hits),
    not just the first one. Returns (ats, slug, matched_url, domain, tier,
    country, country_method) — country is opportunistic, never a gate;
    rows are returned as soon as a tier produces any ATS hits regardless
    of whether country resolved."""
    loop = asyncio.get_running_loop()
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
            return []

        final_url, html = page
        stats["homepage_fetched"] += 1
        best_country, best_method = None, None

        async def _detect(html_, url_):
            nonlocal best_country, best_method
            hits, country, method = await loop.run_in_executor(
                parse_pool, _parse_detect, html_, url_, target_geo_countries)
            if country and best_country is None:
                best_country, best_method = country, method
            return hits

        hits = await _detect(html, final_url)
        if hits:
            stats["hits_from_homepage"] += 1
            return [(ats, slug, url, domain, "homepage", best_country, best_method) for ats, slug, url in hits]

        origin_parts = urlparse(final_url)
        origin = f"{origin_parts.scheme}://{origin_parts.netloc}"
        career_pages = await asyncio.gather(
            *[_fetch_page(session, urljoin(origin, p), stats) for p in CAREER_PATHS])
        career_hit_lists = []
        for cp in career_pages:
            if not cp:
                continue
            cp_url, cp_html = cp
            career_hit_lists.append(await _detect(cp_html, cp_url))
        merged = _collapse_hits(career_hit_lists)
        if merged:
            stats["hits_from_career_path"] += 1
            return [(ats, slug, url, domain, "career_path", best_country, best_method) for ats, slug, url in merged]

        sitemap = await _fetch_sitemap(session, origin, stats)
        if sitemap:
            sm_url, sm_xml = sitemap
            loc_urls = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", sm_xml, re.I)
            career_like = [u for u in loc_urls if CAREER_LIKE_RE.search(u)]
            sm_pages = await asyncio.gather(
                *[_fetch_page(session, u, stats) for u in career_like[:SITEMAP_MAX_FOLLOW]])
            sitemap_hit_lists = []
            for sp in sm_pages:
                if not sp:
                    continue
                sp_url, sp_html = sp
                sitemap_hit_lists.append(await _detect(sp_html, sp_url))
            merged = _collapse_hits(sitemap_hit_lists)
            if merged:
                stats["hits_from_sitemap"] += 1
                return [(ats, slug, url, domain, "sitemap", best_country, best_method) for ats, slug, url in merged]

        stats["dropped_no_ats"] += 1
        return []


# ── Supabase I/O ─────────────────────────────────────────────────────────

async def write_rows_to_staging_table(session: aiohttp.ClientSession, rows: list[dict]) -> int:
    """rows: {"ats","slug","source_hostname","root_domain","country","discovery_method"}.
    Retried (unlike crawl requests — this hits ONE shared endpoint many
    shards write to concurrently, where a transient 500 is worth retrying;
    a crawl miss against an independent company domain is not)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("SUPABASE_URL/SUPABASE_KEY not set — cannot write to staging table.")
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
                    f"{SUPABASE_URL}/rest/v1/ctlog_probe_results",
                    headers=headers, params={"on_conflict": "ats,slug"}, json=chunk,
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
        log.error(f"Failed to write a chunk of {len(chunk)} rows after retries — DATA LOST: {last_err}")
        return 0

    results = await asyncio.gather(*(_write_chunk(c) for c in chunks))
    return sum(results)


# ── shared batch driver ───────────────────────────────────────────────────

async def crawl_batch(domains: list[str], session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                       stats: dict, parse_pool: concurrent.futures.ProcessPoolExecutor,
                       target_geo_countries: set[str], discovery_method: str,
                       found_rows: list[dict], crawl_start: float, time_budget_seconds: float,
                       time_budget_minutes: int, batch_size: int = 3000, unit_label: str = "companies"
                       ) -> tuple[int, float, float, bool]:
    """Crawls a list of domains in sub-batches, writing each sub-batch to
    Supabase as it completes. One driver for every seed source — used to
    be duplicated (class_a_probe's inline loop, host_crawl_v2's
    _crawl_and_write_hosts) with the same batching/dedup/time-budget logic
    copy-pasted twice. Returns (done, elapsed, rate, time_budget_hit)."""
    tasks = [crawl_one(session, sem, d, stats, parse_pool, target_geo_countries) for d in domains]
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
        seen_keys = set()
        duplicates_collapsed = 0
        for hits in results:
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
        written = 0
        if batch_rows:
            written = await write_rows_to_staging_table(session, batch_rows)
            found_rows.extend(batch_rows)

        done = min(i + batch_size, len(tasks))
        elapsed = time.monotonic() - crawl_start
        rate = stats["companies_attempted"] / elapsed if elapsed > 0 else 0
        hit_n = stats["hits_from_homepage"] + stats["hits_from_career_path"] + stats["hits_from_sitemap"]
        dup_note = f", {duplicates_collapsed} dup collapsed" if duplicates_collapsed else ""
        log.info(f"  {done}/{len(tasks)} {unit_label} — {rate:.1f}/sec — {elapsed:.0f}s elapsed")
        log.info(f"    → {written}/{len(batch_rows)} written{dup_note} — {len(found_rows)} hits total "
                 f"(hit rate so far: {hit_n / max(stats['companies_attempted'], 1) * 100:.2f}%)")
    return len(tasks), elapsed, rate, time_budget_hit


def new_parse_pool() -> concurrent.futures.ProcessPoolExecutor:
    return concurrent.futures.ProcessPoolExecutor(max_workers=PARSE_WORKERS)


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
