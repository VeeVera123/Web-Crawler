"""
CLASS A DOMAIN-CRAWL DISCOVERY (2026-08 rewrite) — replaces the earlier
seed-and-probe (guess-a-slug, ask-the-platform) technique entirely with
the method host_crawl.py used before it was deprecated: visit each
company's OWN website and look for a REAL link to a known ATS platform
that they themselves published, instead of guessing candidate slugs and
asking a platform's API to confirm/deny them.

WHY THIS REPLACES SLUG-GUESSING, NOT JUST SUPPLEMENTS IT:
  1. PRECISION: a match here means the literal URL is present on the
     company's own page — there is no equivalent of the SmartRecruiters
     false-positive bug (where an API 200'd identically for real and
     fake slugs) possible with this method, because we're not asking a
     third party to validate a guess; we're reading what the company
     itself published. See the 4.5M-row false-positive incident this
     project hit with the old technique for exactly the failure mode
     this avoids structurally, not just via a patched check.
  2. NO SHARED CHOKE POINT: the old technique hammered ONE shared host
     per platform (boards-api.greenhouse.io) with millions of requests,
     which is exactly the traffic pattern that got it soft-blocked by
     Greenhouse's WAF (see the now-removed PLATFORM_CONCURRENCY /
     RateLimiter / circuit-breaker machinery this file used to carry).
     This technique instead sends a handful of requests to each of
     millions of DIFFERENT company domains — no single target ever sees
     enough volume from us to trip an anti-abuse system. That's why all
     of that throttling/backoff/circuit-breaker code is GONE here: it
     solved a problem specific to the old technique's shared-host
     traffic shape, and doesn't apply to this one.
  3. ONE PASS, ALL PLATFORMS: the old script probed one ATS platform at a
     time (--platform greenhouse, --platform lever, ...). This script
     detects ALL 25+ platforms discovery.py knows how to recognize
     (see URL_TO_SLUG there) from a SINGLE fetch of a company's page —
     visiting acme.com once can find a Greenhouse link, a Lever link, or
     any other platform's link, whichever is actually there. Far more
     work extracted per request than the old one-platform-per-probe
     design.

HOW DETECTION WORKS (multiple independent fallback methods per page, all
run on every fetch — see _detect_ats_hits / _extract_candidate_urls):
  A. Parsed <a href> links, resolved against the page's own URL (handles
     relative/protocol-relative links a raw-text scan could miss).
  B. A raw full-text regex scan for absolute http(s) URLs anywhere in the
     page source — covers links embedded in <script> blocks, inline JS,
     iframe src attributes, or anywhere else that isn't a plain <a> tag.
  C. Dedicated JSON-LD extraction: every <script type="application/
     ld+json"> block is parsed as real JSON and recursively walked for
     URL-shaped string values (schema.org JobPosting/Organization
     markup commonly carries a hiringOrganization/sameAs/url field).
     This overlaps somewhat with method B (JSON-LD text is also inside
     the raw page source B already scans) but parses it as structured
     data rather than pattern-matching text, catching values method B's
     plain-text regex could mangle (encoded characters, values split
     across formatting) and giving a distinctly-labeled signal.
  Every URL surfaced by A/B/C is run through discovery.py's URL_TO_SLUG —
  the SAME per-platform converters every other source in this project
  already trusts, so detection logic isn't duplicated or reinvented here.

FALLBACK TIERS (only spent if the homepage itself yields nothing — see
_crawl_one): if A/B/C find nothing on the homepage, a widened set of
common career-page paths (see CAREER_PATHS — careers/jobs singular and
plural, "join us"/"work with us" phrasing, nested under /about or
/company, plus /hiring) are fetched CONCURRENTLY and scanned the same
way. If THOSE also find nothing, /sitemap.xml is
checked for any <loc> entries that look career-related and up to 3 of
those are fetched and scanned too. This means a company genuinely gets
several real chances before being marked as no-ATS-found, without
unconditionally quadrupling the request volume for the (large) majority
of companies that already resolve on the homepage alone.

SEED SOURCE — PDL ONLY, DOMAIN REQUIRED: this technique needs a real
domain to crawl. Of the three seed sources the old script combined (PDL,
SEC EDGAR, YC), only PDL carries a domain field — SEC EDGAR and YC only
ever gave a company NAME, which is useless here (there's nothing to
guess-a-domain-from without risking crawling the wrong company entirely,
a correctness problem worse than just skipping them). So this version
uses PDL exclusively, filtered to rows with a non-empty domain. See
fetch_pdl_companies_with_domain.

DELIBERATELY NO RATE LIMITING: unlike the old script, there is no
per-request backoff, no soft-block detection, and no circuit breaker
here. That machinery existed specifically to survive hammering ONE
shared host — it would be actively counterproductive against millions of
independent domains, where the constraint is our own network/CPU
throughput, not any single target's tolerance for our traffic. Per-
request TIMEOUTS still exist (a dead/slow site can't be allowed to stall
the crawl forever), but that's a liveness safeguard, not a rate limit.

Usage:
    pip install aiohttp selectolax python-dotenv requests
    python class_a_probe.py
    python class_a_probe.py --shard-index 0 --shard-count 20
    python class_a_probe.py --concurrency 600
"""
import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from collections import Counter
from urllib.parse import urljoin, urlparse

import aiohttp
import requests
from dotenv import load_dotenv
from selectolax.lexbor import LexborHTMLParser

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from discovery import SKIP_SLUGS, URL_TO_SLUG  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("class_a_probe")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# Per-request safety timeout — NOT a rate limit. A handful of dead/slow
# sites can't be allowed to stall the whole crawl; this just caps how
# long any single fetch is allowed to hang before being abandoned.
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10, connect=6)

# CONCURRENCY: how many companies are processed in parallel. Each company
# may issue 1 request (homepage hit found) up to ~8 (homepage miss ->
# career-path fallback -> sitemap fallback, all attempted). Tunable via
# env/CLI since the right number depends on the runner's own network
# capacity, not any external constraint (see module docstring — there is
# deliberately nothing on the other side of this that we're pacing
# against). Default is high on purpose.
CRAWL_CONCURRENCY = int(os.environ.get("CRAWL_CONCURRENCY", "400"))
# Hard ceiling on simultaneous TCP connections at the connector level —
# set a bit above CRAWL_CONCURRENCY to give the fallback-tier burst
# requests (a company that reaches career-path/sitemap fallback opens
# several requests at once) room without being needlessly serialized.
CONNECTOR_LIMIT = int(os.environ.get("CRAWL_CONNECTOR_LIMIT", str(CRAWL_CONCURRENCY + 150)))

# Cap how much of any single page we read — a handful of pathological
# multi-hundred-MB responses can't be allowed to eat all the crawl's
# time/memory. Way more than any real HTML page needs (few hundred KB is
# typical); this is a safety ceiling, not a realistic limit.
MAX_PAGE_BYTES = 3_000_000

# WIDENED (2026-08): real company sites use a lot of different paths for
# their careers page — the original 4-path list was too narrow and would
# have missed genuinely findable companies. Covers the common English-
# language conventions (careers/jobs singular+plural, "join us"/"work
# with us" phrasing, nested under /about or /company, and an explicit
# "hiring" variant) without exploding into every possible i18n/phrasing
# variant, which would mostly just add request volume for little extra
# yield. All fetched CONCURRENTLY per company (see _crawl_one), so this
# widening costs more total requests, not more time per company.
CAREER_PATHS = [
    "/careers", "/career",
    "/jobs", "/job-openings", "/open-positions", "/openings",
    "/join-us", "/join", "/work-with-us", "/work-for-us",
    "/about/careers", "/about-us/careers", "/company/careers", "/company/jobs",
    "/team/careers",
    "/hiring", "/we-are-hiring",
    "/apply",
]
SITEMAP_MAX_FOLLOW = 3  # cap how many sitemap-discovered URLs get fetched

PDL_DATASET_PATH = os.environ.get("PDL_DATASET_PATH", "people_data_labs_companies.csv")
PDL_ROW_LIMIT = int(os.environ.get("PDL_ROW_LIMIT", "0"))  # 0 = no cap


def fetch_pdl_companies_with_domain(limit: int = PDL_ROW_LIMIT) -> list[dict]:
    """Reads the People Data Labs Free Company Dataset from a LOCAL file
    (Kaggle-hosted, one-time authenticated download — see README/prior
    docs) and keeps ONLY rows with a real, non-empty domain — this
    technique needs something to crawl, and PDL is the only seed source
    this project has that carries a domain field at all. Missing file is
    NOT an error — logs once and returns empty, same as any other
    optional source, so this script still runs (finding nothing) rather
    than crashing for anyone who hasn't done the one-time download yet."""
    import csv

    if not os.path.exists(PDL_DATASET_PATH):
        log.warning(f"PDL dataset not found at '{PDL_DATASET_PATH}' — nothing to crawl without it. "
                    f"Download it once (see project README) and set PDL_DATASET_PATH if needed.")
        return []

    out = []
    total_rows = 0
    try:
        with open(PDL_DATASET_PATH, newline="", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break
                total_rows += 1
                name = (row.get("name") or "").strip()
                domain = (row.get("domain") or "").strip().lower()
                # Strip any accidental scheme/path a dirty CSV value might carry
                domain = re.sub(r"^https?://", "", domain).split("/")[0].strip()
                if name and domain and "." in domain and domain not in SKIP_SLUGS:
                    out.append({"name": name, "domain": domain})
    except Exception as e:
        log.error(f"Failed to read PDL dataset: {e}")
        return []
    log.info(f"PDL dataset: {total_rows} total rows, {len(out)} with a usable domain "
             f"({len(out) / max(total_rows, 1) * 100:.1f}%)")
    return out


# ── detection ─────────────────────────────────────────────────────────

_URL_RE = re.compile(r'https?://[^\s"\'<>\\]{4,300}', re.I)
_MAX_CANDIDATE_URLS_PER_PAGE = 4000  # pathological-page safety cap


def _extract_candidate_urls(html: str, base_url: str) -> set[str]:
    """Method A + B combined: parsed <a href> links (resolved against the
    page's own URL, catching relative/protocol-relative links) PLUS a raw
    full-text scan for absolute URLs anywhere in the source (catching
    links embedded in <script> blocks, JS strings, iframe src, or
    anywhere else that isn't a plain <a> tag)."""
    urls: set[str] = set()

    try:
        tree = LexborHTMLParser(html)
        for node in tree.css("a[href]"):
            href = node.attributes.get("href")
            if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            try:
                urls.add(urljoin(base_url, href))
            except ValueError:
                continue
            if len(urls) >= _MAX_CANDIDATE_URLS_PER_PAGE:
                break
    except Exception:
        pass  # a malformed page shouldn't kill the whole crawl of this company

    for m in _URL_RE.finditer(html):
        urls.add(m.group(0))
        if len(urls) >= _MAX_CANDIDATE_URLS_PER_PAGE:
            break

    return urls


def _walk_json_strings(obj, depth: int = 0):
    """Recursively yield every string value inside a parsed JSON
    structure — used to pull URL-shaped values out of JSON-LD blocks
    regardless of which field they're under (hiringOrganization.url,
    sameAs, publisher.url, etc. all vary by page)."""
    if depth > 12:  # guard against pathological/self-referential structures
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
    """Method C: dedicated JSON-LD structured-data extraction. Distinct
    from the raw-text regex scan (method B) — this actually PARSES each
    <script type="application/ld+json"> block as JSON and walks it, which
    catches values the raw-text pattern-match could mangle (escaped
    characters, values assembled from multiple JSON fields) and gives a
    separately-labeled, more precise signal than pattern-matching text."""
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
    """Runs every candidate URL through discovery.py's real per-platform
    URL_TO_SLUG converters — the same trusted logic every other source in
    this project uses, not reinvented here. Returns (ats, slug,
    matched_url) for every real match found."""
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


# ── fetching ──────────────────────────────────────────────────────────

async def _fetch_page(session: aiohttp.ClientSession, url: str, stats: dict) -> tuple[str, str] | None:
    """Fetches one page, capped at MAX_PAGE_BYTES, returns (final_url,
    html_text) on a usable HTML response or None on anything else
    (unreachable, non-HTML, too large, error status). No retries, no
    backoff — see module docstring for why that machinery doesn't belong
    here; a single miss just means this company/path didn't pan out."""
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT,
                                headers={"User-Agent": USER_AGENT},
                                allow_redirects=True, max_redirects=5,
                                ssl=False) as r:
            if r.status >= 400:
                stats["http_error"] += 1
                return None
            content_type = r.headers.get("Content-Type", "")
            if content_type and "html" not in content_type.lower():
                stats["non_html"] += 1
                return None
            chunks = []
            total = 0
            async for chunk in r.content.iter_chunked(65536):
                chunks.append(chunk)
                total += len(chunk)
                if total >= MAX_PAGE_BYTES:
                    break
            html = b"".join(chunks).decode("utf-8", errors="ignore")
            if not html.strip():
                stats["non_html"] += 1
                return None
            stats["fetched_ok"] += 1
            return str(r.url), html
    except asyncio.TimeoutError:
        stats["timeout"] += 1
        return None
    except Exception:
        stats["unreachable"] += 1
        return None


async def _crawl_one(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                      company_name: str, domain: str, stats: dict) -> list[tuple[str, str, str, str, str]]:
    """Crawls one company's site end to end: homepage -> (if nothing
    found) common career-page paths -> (if still nothing) sitemap.xml
    fallback. Returns a list of (ats, slug, matched_url, domain, tier) —
    a company CAN yield more than one hit (e.g. an old and new ATS both
    still linked during a migration); all distinct (ats, slug) pairs
    found at whichever tier first produced a hit are kept, not just the
    first pair. `tier` is "homepage" / "career_path" / "sitemap",
    whichever fetch tier actually produced this hit."""
    async with sem:
        stats["companies_attempted"] += 1
        candidates = [f"https://{domain}"]
        if not domain.startswith("www."):
            candidates.append(f"https://www.{domain}")
        candidates.append(f"http://{domain}")  # last resort only

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
        urls = _extract_candidate_urls(html, final_url) | _extract_jsonld_urls(html)
        hits = _detect_ats_hits(urls)
        if hits:
            stats["hits_from_homepage"] += 1
            return [(ats, slug, url, domain, "homepage") for ats, slug, url in hits]

        # Fallback tier 1: common career-page paths, fetched concurrently.
        origin_parts = urlparse(final_url)
        origin = f"{origin_parts.scheme}://{origin_parts.netloc}"
        career_pages = await asyncio.gather(
            *[_fetch_page(session, urljoin(origin, p), stats) for p in CAREER_PATHS]
        )
        for cp in career_pages:
            if not cp:
                continue
            cp_url, cp_html = cp
            urls = _extract_candidate_urls(cp_html, cp_url) | _extract_jsonld_urls(cp_html)
            hits = _detect_ats_hits(urls)
            if hits:
                stats["hits_from_career_path"] += 1
                return [(ats, slug, url, domain, "career_path") for ats, slug, url in hits]

        # Fallback tier 2: sitemap.xml, only reached if everything above found nothing.
        sitemap = await _fetch_page(session, urljoin(origin, "/sitemap.xml"), stats)
        if sitemap:
            sm_url, sm_xml = sitemap
            loc_urls = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", sm_xml, re.I)
            career_like = [u for u in loc_urls if re.search(r"career|jobs?|join|work-with-us", u, re.I)]
            sm_pages = await asyncio.gather(
                *[_fetch_page(session, u, stats) for u in career_like[:SITEMAP_MAX_FOLLOW]]
            )
            for sp in sm_pages:
                if not sp:
                    continue
                sp_url, sp_html = sp
                urls = _extract_candidate_urls(sp_html, sp_url) | _extract_jsonld_urls(sp_html)
                hits = _detect_ats_hits(urls)
                if hits:
                    stats["hits_from_sitemap"] += 1
                    return [(ats, slug, url, domain, "sitemap") for ats, slug, url in hits]

        return []


# ── Supabase I/O ─────────────────────────────────────────────────────

async def write_rows_to_staging_table(session: aiohttp.ClientSession, rows: list[dict]) -> int:
    """rows: list of {"ats", "slug", "source_hostname", "root_domain"}.
    root_domain is now the ACTUAL company domain that was crawled (a
    correct, literal use of that column for the first time — the old
    seed-and-probe technique had no real domain to put there and used a
    flat technique-marker string instead). source_hostname carries the
    exact page URL the match was found on, tagged with which detection
    tier found it — the most specific, actionable piece of information
    available, and something the old technique never had access to."""
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
            log.error(f"Failed to write a chunk to staging table: {e}")
            return 0

    results = await asyncio.gather(*(_write_chunk(c) for c in chunks))
    written = sum(results)
    log.info(f"  Wrote {written}/{len(rows)} rows to ctlog_probe_results")
    return written


# ── main crawl loop ──────────────────────────────────────────────────

async def run_crawl(shard_index: int | None = None, shard_count: int | None = None,
                     concurrency: int = CRAWL_CONCURRENCY) -> None:
    label = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
    log.info(f"── Class A domain-crawl discovery{label} ──")
    log.info(f"  concurrency={concurrency} (see module docstring — no rate limiting, "
             f"only a per-request timeout for liveness)")

    companies = fetch_pdl_companies_with_domain()
    if not companies:
        log.error("  No seed companies with a usable domain — aborting.")
        return

    if shard_index is not None and shard_count is not None:
        # MODULO sharding (not a contiguous slice) — PDL's rows are
        # sorted largest-company-first, so a contiguous chunk would skew
        # which shard gets the highest-signal companies.
        companies = companies[shard_index::shard_count]
        log.info(f"  {len(companies)} companies in this shard's slice")

    # At this concurrency, resolving DNS for millions of DIFFERENT new
    # domains (not one warm host reused over and over) can itself become
    # the bottleneck under Python's default resolver (thread-pool based).
    # aiodns gives aiohttp a real concurrent async resolver — use it when
    # available, fall back cleanly (still correct, just slower under
    # heavy load) if the package isn't installed.
    resolver = None
    try:
        import aiodns  # noqa: F401
        resolver = aiohttp.AsyncResolver()
    except ImportError:
        log.warning("  aiodns not installed — DNS resolution will use the slower default "
                    "resolver. `pip install aiodns` for real throughput at this concurrency.")
    connector = aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, ttl_dns_cache=300, resolver=resolver)
    sem = asyncio.Semaphore(concurrency)
    stats = Counter()
    found_rows: list[dict] = []

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_crawl_one(session, sem, c["name"], c["domain"], stats) for c in companies]

        BATCH = 3000
        total_distinct_hits = 0
        crawl_start = time.monotonic()
        for i in range(0, len(tasks), BATCH):
            batch = tasks[i:i + BATCH]
            results = await asyncio.gather(*batch)
            batch_rows = []
            for company_hits in results:
                for ats, slug, matched_url, domain, tier in company_hits:
                    batch_rows.append({
                        "ats": ats,
                        "slug": slug,
                        "source_hostname": f"[{tier}] {matched_url}"[:250],
                        "root_domain": domain,
                    })
            if batch_rows:
                total_distinct_hits += len(batch_rows)
                await write_rows_to_staging_table(session, batch_rows)
                found_rows.extend(batch_rows)

            done = min(i + BATCH, len(tasks))
            elapsed = time.monotonic() - crawl_start
            rate = done / elapsed if elapsed > 0 else 0
            # timeout+unreachable as a share of attempts made so far — the
            # tell for whether the current --concurrency is actually
            # helping or has been pushed past what this runner's single
            # CPU-bound parsing thread can keep up with (see module
            # docstring / the "is 400 safe to raise" question this was
            # added to answer): a rising share here at a HIGHER
            # concurrency than a previous run, with a LOWER companies/sec
            # rate, means back off — you're queueing, not parallelizing.
            attempted = max(stats["companies_attempted"], 1)
            stall_share = (stats["timeout"] + stats["unreachable"]) / attempted * 100
            log.info(f"  crawled {done}/{len(tasks)} companies — {total_distinct_hits} ATS hits found so far "
                     f"— {rate:.1f} companies/sec")
            log.info(f"    fetch breakdown (cumulative): homepage_fetched={stats['fetched_ok']} "
                     f"homepage_unreachable={stats['homepage_unreachable']} "
                     f"http_error={stats['http_error']} timeout={stats['timeout']} "
                     f"non_html={stats['non_html']} unreachable={stats['unreachable']} "
                     f"(timeout+unreachable = {stall_share:.1f}% of attempts)")
            log.info(f"    hit source (cumulative): homepage={stats['hits_from_homepage']} "
                     f"career_path={stats['hits_from_career_path']} sitemap={stats['hits_from_sitemap']}")

    hit_rate = total_distinct_hits / max(len(companies), 1) * 100
    reach_rate = stats['fetched_ok'] / max(len(companies), 1) * 100
    log.info(f"  TOTAL: {total_distinct_hits} ATS hits found across {len(companies)} companies "
             f"({hit_rate:.2f}% of companies yielded a hit, {reach_rate:.1f}% had a reachable homepage)")
    ats_breakdown = Counter(r["ats"] for r in found_rows)
    if ats_breakdown:
        log.info(f"  By platform: {dict(ats_breakdown.most_common())}")


def main():
    parser = argparse.ArgumentParser(description="Class A domain-crawl discovery (async)")
    parser.add_argument("--shard-index", type=int, default=None,
                         help="This job's shard index (0-based) when splitting the seed list "
                              "across multiple parallel jobs — modulo sharding, see run_crawl")
    parser.add_argument("--shard-count", type=int, default=None,
                         help="Total number of shards (must be passed together with --shard-index)")
    parser.add_argument("--concurrency", type=int, default=CRAWL_CONCURRENCY,
                         help=f"Companies processed in parallel (default {CRAWL_CONCURRENCY}, "
                              f"env CRAWL_CONCURRENCY)")
    args = parser.parse_args()
    asyncio.run(run_crawl(args.shard_index, args.shard_count, args.concurrency))


if __name__ == "__main__":
    main()
