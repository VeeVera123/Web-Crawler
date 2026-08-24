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
crawl.

SEED SOURCES (2026-08, expanded after the first run came back small — see
CLASS_A_SEED_LIST_HELP_REQUEST.md for the diagnosis): THREE free, no-auth
bulk sources are combined and deduped by company name (see
fetch_all_seed_companies), largest/broadest first:
  1. People Data Labs Free Company Dataset — CC BY 4.0, 22M+ companies,
     industry-agnostic, includes each company's own WEBSITE DOMAIN (see
     PDL_DATASET_PATH comment below for the one-time local download this
     needs — it's Kaggle-hosted, not fetchable fresh per-run).
  2. SEC EDGAR CIK lookup — free, official (sec.gov), ~13MB text file,
     hundreds of thousands of US-registered entities (skews larger/more
     established than PDL).
  3. Y Combinator companies (yc-oss/api) — the ORIGINAL seed source (see
     discovery.py's fetch_yc_companies for the same list used elsewhere
     in this project), kept as a third source rather than the only one —
     confirmed too small (~6k) and too narrow (VC-backed tech startups
     only) to carry this alone.
OpenCorporates was considered and DROPPED — checked directly and it has
no free tier at any usable bulk volume (cheapest paid plan is a few
hundred pounds/year for 500 calls/month total).

Every seed company is normalized into a handful of plausible slug
variants (see _slug_variants) — DOMAIN-derived variants (from PDL's
website field) are tried before NAME-derived ones, since an ATS slug is
often literally the company's own domain stripped of its TLD. Each
variant is probed against all four APIs, and which strategy (domain vs.
name) actually produced the hit is tracked and logged per run — see
run_platform's strategy_hits benchmark.

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

# ── seed sources ──────────────────────────────────────────────────
# Three free, no-signup bulk seed sources, largest/broadest first. Using
# more than one matters here — see CLASS_A_SEED_LIST_HELP_REQUEST.md:
# the YC list alone was small (~6k) and narrow (VC-backed tech startups
# only), which is why the first probing pass came back small. PDL's
# dataset is the real fix for that — 22M companies, industry-agnostic,
# and (crucially) includes each company's own WEBSITE DOMAIN, not just
# its display name, which enables a second, often more accurate slug-
# guessing strategy (see _slug_variants' domain parameter) since ATS
# slugs are frequently derived from a company's domain rather than its
# full display name. OpenCorporates was considered and DROPPED — checked
# directly and it has no free tier at any usable volume (cheapest paid
# plan works out to hundreds of pounds/year for a few hundred calls a
# month — fine for occasional lookups, useless for bulk seeding).
#
# PEOPLE DATA LABS FREE COMPANY DATASET — CC BY 4.0 (genuinely free, no
# signup, attribution only), 22M+ companies, quarterly updated, ~9M-line
# CSV. Confirmed columns: name, domain, year_founded, industry, country,
# region, locality, founded year, size, id. Hosted on Kaggle; Kaggle's
# own API needs a free account + API token to script a download (no way
# around that for a Kaggle-hosted file), so PDL_DATASET_PATH below points
# at a LOCAL copy — download once via `kaggle datasets download -d
# peopledatalabssf/free-7-million-company-dataset` (or the Kaggle web UI)
# and unzip into the project directory, since the raw file is too large
# to fetch fresh on every GitHub Actions run. Only used if present —
# gracefully skipped otherwise, same "don't hard-fail if a seed source
# isn't wired up yet" idea as everywhere else in this project.
PDL_DATASET_PATH = os.environ.get("PDL_DATASET_PATH", "people_data_labs_companies.csv")
PDL_ROW_LIMIT = int(os.environ.get("PDL_ROW_LIMIT", "500000"))  # cap per run —
    # 22M rows is far more than any single run needs; a rolling cap keeps
    # wall-clock reasonable while still covering the dataset over several
    # runs (idempotent upsert makes re-running safe/cheap, same as every
    # other source in this project).

# SEC EDGAR CIK LOOKUP — confirmed free, official (sec.gov), no signup,
# plain-text, ~13MB. Format per line: "COMPANY NAME:CIK:" — historically
# cumulative (includes renamed/inactive entities), so treat as a name
# source only, not a signal of which companies are currently active.
# Skews US-listed/SEC-registered (larger, more established companies)
# rather than PDL's broader small-business-inclusive coverage — a useful
# different slice, not a replacement for PDL.
SEC_EDGAR_CIK_URL = "https://www.sec.gov/Archives/edgar/cik-lookup-data.txt"
SEC_EDGAR_ROW_LIMIT = int(os.environ.get("SEC_EDGAR_ROW_LIMIT", "100000"))
# SEC.gov requires a descriptive User-Agent identifying the requester
# (their own published policy — generic/browser UAs get blocked) —
# format is "AppName contact@email", not a real browser string.
SEC_EDGAR_USER_AGENT = "ats-global-scanner research contact@example.com"


def fetch_yc_companies() -> list[dict]:
    """Same free, no-auth YC company list discovery.py's own YC resolver
    uses (see fetch_yc_companies there) — reused here as ONE of three seed
    sources now, not the only one. ~6k companies, static JSON, GitHub
    Pages-hosted. Kept because it's free/instant/no-file-download-needed,
    even though it's the smallest and narrowest of the three — every extra
    seed source is still upside as long as it's genuinely free to use."""
    try:
        r = requests.get(YC_ALL_COMPANIES_URL, timeout=60,
                          headers={"User-Agent": YC_USER_AGENT})
        r.raise_for_status()
        return [{"name": c.get("name", ""), "domain": None} for c in r.json()]
    except Exception as e:
        log.error(f"Failed to fetch YC company list: {e}")
        return []


def fetch_pdl_companies(limit: int = PDL_ROW_LIMIT) -> list[dict]:
    """Reads the People Data Labs Free Company Dataset from a LOCAL file
    (see PDL_DATASET_PATH comment above for why — Kaggle-hosted, needs a
    one-time authenticated download, not fetchable fresh per-run). Missing
    file is NOT an error — logs once and returns empty, same as any other
    optional seed source, so this script still runs fine (just smaller)
    for anyone who hasn't done the one-time Kaggle download yet."""
    import csv

    if not os.path.exists(PDL_DATASET_PATH):
        log.warning(f"  PDL dataset not found at '{PDL_DATASET_PATH}' — skipping this seed "
                    f"source. Download it once via the Kaggle CLI/UI (see class_a_probe.py's "
                    f"PDL_DATASET_PATH comment) to enable it.")
        return []

    out = []
    try:
        with open(PDL_DATASET_PATH, newline="", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break
                name = (row.get("name") or "").strip()
                # Kaggle CSV column is "domain" (bare domain, e.g.
                # "acme.com"), NOT "website" (that name is used by a
                # different/newer PDL product with more fields) —
                # confirmed against a direct EDA of this exact dataset.
                domain = (row.get("domain") or "").strip() or None
                if name:
                    out.append({"name": name, "domain": domain})
    except Exception as e:
        log.error(f"  Failed to read PDL dataset: {e}")
        return []
    return out


def fetch_sec_edgar_companies(limit: int = SEC_EDGAR_ROW_LIMIT) -> list[dict]:
    """Free, official, no-signup SEC EDGAR CIK list — see module-level
    comment above for format/caveats."""
    try:
        r = requests.get(SEC_EDGAR_CIK_URL, timeout=120,
                          headers={"User-Agent": SEC_EDGAR_USER_AGENT})
        r.raise_for_status()
    except Exception as e:
        log.error(f"  Failed to fetch SEC EDGAR CIK list: {e}")
        return []

    out = []
    for line in r.text.splitlines():
        if limit and len(out) >= limit:
            break
        # Format: "COMPANY NAME:CIK:" — name may contain colons in rare
        # cases, so split on the LAST two colons rather than the first.
        parts = line.rsplit(":", 2)
        if len(parts) >= 2 and parts[0].strip():
            out.append({"name": parts[0].strip(), "domain": None})
    return out


def fetch_all_seed_companies() -> list[dict]:
    """Combines every available free seed source. Each dict is
    {"name": str, "domain": str|None} — domain is used for a second,
    often more accurate slug-guessing strategy (see _slug_variants)
    where available (PDL only; YC/SEC EDGAR don't carry a domain field)."""
    sources = [
        ("PDL Free Company Dataset", fetch_pdl_companies),
        ("SEC EDGAR CIK list", fetch_sec_edgar_companies),
        ("Y Combinator companies", fetch_yc_companies),
    ]
    combined = []
    seen_names = set()
    for label, fetch_fn in sources:
        companies = fetch_fn()
        added = 0
        for c in companies:
            key = c["name"].lower().strip()
            if key and key not in seen_names:
                seen_names.add(key)
                combined.append(c)
                added += 1
        log.info(f"  seed source '{label}': {len(companies)} companies, {added} new after dedup")
    return combined


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_SUFFIX_WORDS = {"inc", "llc", "ltd", "co", "corp", "corporation", "company",
                  "the", "group", "holdings", "technologies", "labs", "hq"}
_DOMAIN_STRIP_RE = re.compile(
    r"^(https?://)?(www\.)?|\.(com|net|org|io|co|ai|app|dev|us|biz|inc)(/.*)?$", re.I
)


def _slug_variants(name: str, domain: str | None = None) -> list[str]:
    """Normalize a company name (and, where available, its website
    domain) into a handful of plausible ATS slug guesses. Real companies
    pick their own slug at signup, so this is inherently a guess —
    ordered roughly most-to-least likely, callers can stop at the first
    HIT since ATS slugs are unique per platform (getting company X's REAL
    slug via one variant doesn't mean trying the others too — see
    _probe_one).

    DOMAIN-DERIVED variants are tried FIRST when a domain is available:
    ATS slugs are very often literally the company's own domain name
    stripped of its TLD (a company at acme.com is a strong bet to be
    "acme" on Greenhouse/Lever/etc, often a better bet than a guess
    derived from its full legal/display name, which may include suffixes,
    punctuation, or a DBA name that doesn't match what they typed into
    their ATS signup form)."""
    variants = []

    if domain:
        bare = _DOMAIN_STRIP_RE.sub("", domain.lower().strip())
        bare = _NON_ALNUM_RE.sub("", bare)
        if bare and bare not in SKIP_SLUGS:
            variants.append(bare)

    lowered = name.lower().strip()
    compact = _NON_ALNUM_RE.sub("", lowered)               # "acmecorp"
    hyphenated = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")  # "acme-corp"

    words = [w for w in re.split(r"[^a-z0-9]+", lowered) if w]
    words_no_suffix = [w for w in words if w not in _SUFFIX_WORDS]
    stripped_hyphen = "-".join(words_no_suffix) if words_no_suffix else hyphenated
    stripped_compact = "".join(words_no_suffix) if words_no_suffix else compact
    acronym = "".join(w[0] for w in words_no_suffix) if len(words_no_suffix) >= 2 else None

    for v in (compact, hyphenated, stripped_compact, stripped_hyphen, acronym):
        if v and v not in SKIP_SLUGS and v not in variants:
            variants.append(v)
    return variants


# ── probing (async) ──────────────────────────────────────────────────

async def _probe_one(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                      ats: str, company_name: str, variants: list[str],
                      domain_variant_count: int) -> tuple[str, str, str] | None:
    """Try each slug variant for one company in order, stop at the first
    real hit (200 + body that actually looks like a job-board payload,
    not just any 200 — some of these APIs 200 a malformed/empty request
    too, see the per-platform checks below). Returns (slug, company_name,
    strategy) where strategy is "domain" or "name" — domain-derived
    variants are always tried first (see _slug_variants), so a hit within
    the first domain_variant_count variants tried came from the domain,
    everything after came from the name — this is the benchmark data
    needed to answer "which strategy actually yields more hits"."""
    url_template = PROBE_URL[ats]
    async with sem:
        for idx, slug in enumerate(variants):
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
                strategy = "domain" if idx < domain_variant_count else "name"
                return slug, company_name, strategy
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
    log.info(f"── {ats} (seed-and-probe, multi-source) ──")
    companies = fetch_all_seed_companies()
    if not companies:
        log.error("  No seed companies fetched — aborting.")
        return
    log.info(f"  {len(companies)} total distinct seed companies to probe")

    sem = asyncio.Semaphore(PROBE_CONCURRENCY)
    found: dict[str, str] = {}   # slug -> company_name
    strategy_hits = {"domain": 0, "name": 0}

    async with aiohttp.ClientSession() as session:
        existing = await fetch_existing_slug_registry_slugs(session, ats)
        log.info(f"  slug_registry already has {len(existing)} {ats} slugs")

        tasks = []
        for c in companies:
            name = (c.get("name") or "").strip()
            if not name:
                continue
            domain = c.get("domain")
            variants = _slug_variants(name, domain)
            if not variants:
                continue
            domain_variant_count = 1 if domain else 0  # _slug_variants puts
                # at most one domain-derived variant first — see its docstring
            tasks.append(_probe_one(session, sem, ats, name, variants, domain_variant_count))

        BATCH = 500  # write incrementally, same "don't lose progress on
                      # an interrupted run" principle as ctlog_extract.py
        for i in range(0, len(tasks), BATCH):
            batch = tasks[i:i + BATCH]
            results = await asyncio.gather(*batch)
            batch_found = {}
            for r in results:
                if not r:
                    continue
                slug, name, strategy = r
                batch_found[slug] = name
                if slug not in found:
                    strategy_hits[strategy] += 1
            new_this_batch = {s: n for s, n in batch_found.items() if s not in found}
            found.update(new_this_batch)
            if new_this_batch:
                await write_to_staging_table(session, ats, new_this_batch,
                                              source_marker="seed_probe:multi_source")
            log.info(f"  probed {min(i + BATCH, len(tasks))}/{len(tasks)} — "
                     f"{len(found)} real slugs found so far "
                     f"(domain-derived: {strategy_hits['domain']}, name-derived: {strategy_hits['name']})")

        if ats == "workable":
            feed_slugs = await fetch_workable_feed_slugs(session)
            new_from_feed = {s: "" for s in feed_slugs if s not in found and s not in existing}
            if new_from_feed:
                await write_to_staging_table(session, ats, new_from_feed,
                                              source_marker="workable_master_xml_feed")
                found.update(new_from_feed)
                log.info(f"  +{len(new_from_feed)} additional slugs from the Workable master XML feed")

    log.info(f"  Slug-generation strategy benchmark: {strategy_hits['domain']} hits from "
             f"domain-derived variants, {strategy_hits['name']} hits from name-derived variants")

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
