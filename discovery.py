"""
Discovery — Supabase as Single Source of Truth
=====================================================
Pulls company slugs from multiple sources and upserts them
into the Supabase archive_i table (renamed from slug_registry, 2026-08 —
# see upsert_to_supabase's 2026-09 fix comment for why that rename matters).

Sources:
  1. Feashliaa GitHub (50k+ slugs for 6 platforms — greenhouse,
     lever, ashby, bamboohr, icims, workday)
  2. kalil0321/ats-scrapers (CSV inventories for 15 platforms —
     incl. successfactors, smartrecruiters, workable)
  3. OpenPostings jobs.db (110k+ companies across 80+ ATSs)
  4. Common Crawl index (ongoing discovery for 26 platforms — including
     6 also covered by Feashliaa's bulk dump, added as a supplemental
     top-up since dedup is free via the on_conflict upsert. Run as 4
     platform-sharded matrix jobs in discovery.yml — bumped from 2 to 4
     (2026-09) since this was already the slowest single source in the
     matrix and splitting it further is pure wall-clock win, each shard
     already runs as its own independent job — see
     fetch_commoncrawl_slugs docstring for why, and --cc-shard/
     --cc-total-shards below)
  5. Wayback Machine CDX (all-platform supplemental discovery — widened
     2026-09 from ADP-only to every platform in CC_PLATFORM_PATTERNS,
     reusing the same URL_TO_SLUG-based extractors Common Crawl uses;
     source label renamed "wayback_adp" -> "wayback" to match, via a
     migration that also widened archive_i's source CHECK constraint —
     see fetch_wayback_slugs)
  6. Y Combinator (yc-oss/api — ~6k companies, free, no auth. Each
     company's own website is fetched and scanned for a link to a
     known ATS domain — this is net-new discovery, not just another
     dump of the same companies the other sources already have,
     since YC startups skew toward exactly the modern ATS platforms
     — Greenhouse/Lever/Ashby/Rippling — this project already covers
     well, just companies too new/small to be in the bigger dumps yet)
  7. RETIRED 2026-09 — TheirStack (freemium technology-usage API):
     dropped at the user's request. Its GitHub Actions matrix slot was
     reassigned to a 3rd/4th Common Crawl shard instead (see source 4
     above). THEIRSTACK_API_KEY is no longer read anywhere in this file.
  8. HTTP Archive (public BigQuery dataset — real technology-fingerprint
     detection, i.e. the same method commercial "companies using X"
     trackers are built on, run monthly against millions of crawled
     URLs by Google/HTTP Archive. Catches ATS integrations embedded via
     a JS widget with no plain <a href> at all, which link-following
     sources can't see. Needs a free Google Cloud project with BigQuery
     enabled (no credit card — the Sandbox tier's 1TB/month free query
     quota easily covers this) and GCP_PROJECT_ID +
     GOOGLE_APPLICATION_CREDENTIALS set. See fetch_httparchive_slugs
     docstring for the full explanation of how this reuses the Y
     Combinator resolver rather than being a separate pipeline.)

  RETIRED 2026-08 — Web Data Commons (schema.org JobPosting bulk extract):
  built as a 9th source, but its URLs turned out to almost never be
  ATS-hosted directly (they're the company's OWN careers page), so it
  needed the same live-fetch resolver as sources 6/8 to be useful at all.
  Even with that fix, a live run against a 3000-page sample (out of
  ~97k unmatched URLs) returned only 37 net-new slugs for ~4 minutes of
  fetching — a bad enough payoff, run weekly forever, that it wasn't
  worth keeping. Its GitHub Actions matrix slot was reassigned to a
  second Common Crawl shard instead (see source 4 above and
  fetch_commoncrawl_slugs), since Common Crawl was already the slowest
  single source in the matrix and actually benefits from splitting.

Runs weekly (Sunday) via GitHub Actions, as a 7-source, 10-job matrix —
Common Crawl split across 4 platform-sharded jobs (bumped from 2,
2026-09), the other 6 sources one job each, all in parallel (see
.github/workflows/discovery.yml) —
rather than one job running everything back-to-back. Each source (or
Common Crawl shard) is already an independent fetch-and-resolve pass with
its own cost profile (bulk single download vs. thousands of live
per-company fetches), so sharding by source is the natural split here —
there's no single flat pool of "work items" to hash-shard the way main.py
splits ATS boards across its matrix.

The daily scanner reads from Supabase archive_i — no local .txt files
needed.

Usage:
    python discovery.py                        # full enrichment (all sources, sequential)
    python discovery.py --source feashliaa     # Feashliaa only
    python discovery.py --source kalil         # kalil0321 only
    python discovery.py --source openpostings  # OpenPostings only
    python discovery.py --source commoncrawl   # Common Crawl only
    python discovery.py --source wayback       # Wayback CDX (all platforms) only
    python discovery.py --source yc            # Y Combinator only
    python discovery.py --source httparchive   # HTTP Archive (BigQuery) only
    python discovery.py --source commoncrawl --cc-shard 0 --cc-total-shards 4
    python discovery.py --source commoncrawl --cc-shard 1 --cc-total-shards 4
    python discovery.py --source commoncrawl --cc-shard 2 --cc-total-shards 4
    python discovery.py --source commoncrawl --cc-shard 3 --cc-total-shards 4
    python discovery.py --dry-run              # count without writing
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import tempfile
import time
from urllib.parse import urlparse, parse_qs, urljoin

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# OpenPostings raw download (SQLite database)
OPENPOSTINGS_DB_URL = (
    "https://github.com/Masterjx9/OpenPostings/raw/main/jobs.db"
)

# Feashliaa GitHub (JSON arrays of slugs — no URL conversion needed)
FEASHLIAA_BASE = (
    "https://raw.githubusercontent.com/Feashliaa/"
    "job-board-aggregator/main/data"
)
FEASHLIAA_SOURCES = {
    "greenhouse": f"{FEASHLIAA_BASE}/greenhouse_companies.json",
    "lever":      f"{FEASHLIAA_BASE}/lever_companies.json",
    "ashby":      f"{FEASHLIAA_BASE}/ashby_companies.json",
    "bamboohr":   f"{FEASHLIAA_BASE}/bamboohr_companies.json",
    "icims":      f"{FEASHLIAA_BASE}/icims_companies.json",
    "workday":    f"{FEASHLIAA_BASE}/workday_companies.json",
}

# kalil0321/ats-scrapers (CSV inventories for many platforms)
KALIL_BASE = (
    "https://raw.githubusercontent.com/kalil0321/"
    "ats-scrapers/main/ats-companies"
)
KALIL_SOURCES = {
    "greenhouse":      f"{KALIL_BASE}/greenhouse.csv",
    "lever":           f"{KALIL_BASE}/lever.csv",
    "ashby":           f"{KALIL_BASE}/ashby.csv",
    "bamboohr":        f"{KALIL_BASE}/bamboohr.csv",
    "icims":           f"{KALIL_BASE}/icims.csv",
    "workday":         f"{KALIL_BASE}/workday.csv",
    "rippling":        f"{KALIL_BASE}/rippling.csv",
    "workable":        f"{KALIL_BASE}/workable.csv",
    "recruitee":       f"{KALIL_BASE}/recruitee.csv",
    "smartrecruiters": f"{KALIL_BASE}/smartrecruiters.csv",
    "teamtailor":      f"{KALIL_BASE}/teamtailor.csv",
    "breezyhr":        f"{KALIL_BASE}/breezy.csv",
    # Disabled (JS-rendered / auth-required / blocked):
    # "taleo", "successfactors", "softgarden"
}

# Common Crawl
CC_INDEX_URL = "https://index.commoncrawl.org"
CC_COLLINFO = f"{CC_INDEX_URL}/collinfo.json"

# Y Combinator (yc-oss/api — free, static JSON, no auth, GitHub Pages-hosted)
YC_ALL_COMPANIES_URL = "https://yc-oss.github.io/api/companies/all.json"

# RETIRED 2026-09 — TheirStack (freemium technology-usage API): removed
# at the user's request. THEIRSTACK_API_URL / THEIRSTACK_API_KEY /
# THEIRSTACK_ATS_SLUGS and fetch_theirstack_slugs() are all gone; the
# "theirstack" --source choice and its GitHub Actions matrix job/secret
# are gone too (see .github/workflows/Discovery.yml).

# HTTP Archive — public BigQuery dataset of Wappalyzer technology-detection
# results, run monthly against millions of crawled URLs (Chrome UX Report's
# popular-site list). Needs a Google Cloud project with BigQuery enabled
# (free Sandbox tier — no credit card — covers this easily: a full query
# here costs ~1-2GB against a 1TB/month free allowance) and a service
# account key for programmatic access.
HTTPARCHIVE_GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "")
# google-cloud-bigquery bills/quotas against YOUR project but queries
# Google's own public `httparchive` dataset — you don't need write access
# to httparchive itself, just any GCP project with BigQuery turned on.

# Map our ATS keys -> the exact Wappalyzer technology name, VERIFIED
# 2026-08 by downloading the actual fingerprint files from the actively
# maintained Wappalyzer fork (github.com/enthec/webappanalyzer) and
# confirming each key exists verbatim. Platforms with no confirmed
# fingerprint are left out entirely rather than guessed at.
HTTPARCHIVE_ATS_TECH_NAMES = {
    "greenhouse": "Greenhouse",
    "lever": "Lever",
    "workday": "Workday",
    "bamboohr": "BambooHR",
    "icims": "iCIMS",
    "smartrecruiters": "SmartRecruiters",
    "workable": "Workable",
    "recruitee": "Recruitee",
    "teamtailor": "Teamtailor",
    "personio": "Personio",
    "zoho": "Zoho Recruit",
    "paylocity": "Paylocity",
    "jobadder": "JobAdder",
    "avature": "Avature",
    "jobvite": "Jobvite",
    "eploy": "Eploy",
    "breezyhr": "Breezy HR",
    # Confirmed NOT present in the fingerprint set (checked directly, not
    # assumed): Ashby, Rippling, Folks HR, Softgarden, ClearCompany/
    # HRMDirect, ADP, Taleo, SuccessFactors, BrassRing, ApplyToJob,
    # join.com. These platforms just aren't in Wappalyzer's ruleset —
    # this source can't help with them regardless of query design.
}

# ATS platforms we have working scrapers for (20 active)
SUPPORTED_ATS = {
    "greenhouse", "lever", "ashby", "bamboohr", "icims", "workday",
    "rippling", "workable", "recruitee", "smartrecruiters",
    "teamtailor", "breezyhr", "personio", "joincom",
    # REMOVED 2026-08: applytojob — see ats_scrapers.py's ApplyToJob
    # section header for why (JD enrichment wasn't reliably catching
    # disqualifying US-eligibility language; small long-tail ATS, not
    # worth debugging further; replaced by widening Jobicy instead).
    # Newly enabled (confirmed working via test_blacklisted_ats.py):
    "taleo", "oracle_cloud_hcm", "paylocity", "hrmdirect", "zoho",
    # Fixed (2026-08) — was blacklisted with wrong URL/API assumptions,
    # now scrapes correctly (see ats_scrapers.py):
    "softgarden",
}

# Eploy / Folks HR / JobAdder / Jobvite / ADP / Avature (added 2026-08),
# plus JobScore / Trakstar (added 2026-09), are NOT in SUPPORTED_ATS yet:
# none of them appear in the OpenPostings dataset this file enriches
# from, and JobAdder/ADP additionally need composite slugs (client_id|
# board, cid|ccId) that a single URL has no way to fully encode. Their
# slugs currently have to be added to archive_i by hand (or via
# discover_slugs.py, if/when Common Crawl query patterns are added for
# them) — they scrape fine once a slug row exists, this file just
# doesn't discover new ones for them yet.

# BLACKLISTED — scrapers exist but don't work (robots.txt / JS-rendered):
# brassring, successfactors
# ycombinator is no longer per-company here — it moved to
# job_board_scrapers.py as a multi-company aggregator (see there).

# Map OpenPostings ATS names → our ATS keys
# Map OpenPostings ATS names → our ATS keys (case-insensitive lookup below)
_OPENPOSTINGS_ATS_MAP_RAW = {
    "greenhouse": "greenhouse",
    "lever": "lever",
    "ashby": "ashby",
    "ashbyhq": "ashby",           # OpenPostings uses "ashbyhq"
    "bamboohr": "bamboohr",
    "icims": "icims",
    "workday": "workday",
    "rippling": "rippling",
    "recruitee": "recruitee",
    "smartrecruiters": "smartrecruiters",
    "teamtailor": "teamtailor",
    "workable": "workable",
    # New 6 platforms
    "breezyhr": "breezyhr",
    "breezy": "breezyhr",
    "breezy hr": "breezyhr",
    # "applytojob"/"apply to job" removed 2026-08 (ATS retired, see
    # SUPPORTED_ATS comment above) — no longer mapped, so any OpenPostings
    # row tagged ApplyToJob is naturally filtered out downstream.
    "personio": "personio",
    "joincom": "joincom",
    "join": "joincom",
    "join.com": "joincom",
    # Newly enabled platforms:
    "taleo": "taleo",
    "oracle taleo": "taleo",
    "oraclecloud": "oracle_cloud_hcm",
    "oracle cloud": "oracle_cloud_hcm",
    "oracle cloud hcm": "oracle_cloud_hcm",
    "paylocity": "paylocity",
    "hrmdirect": "hrmdirect",
    "clearcompany": "hrmdirect",
    "zoho": "zoho",
    "zoho recruit": "zoho",
    "zohorecruit": "zoho",
    "softgarden": "softgarden",
    # Disabled platforms (kept for reference):
    # "brassring", "successfactors"
    # ycombinator moved to job_board_scrapers.py — no longer mapped here.
}

def _map_ats_name(name: str) -> str | None:
    """Case-insensitive ATS name lookup."""
    return _OPENPOSTINGS_ATS_MAP_RAW.get(name.lower().strip())

# Slugs to skip
SKIP_SLUGS = {
    "api", "www", "app", "static", "assets", "cdn", "docs", "help",
    "support", "blog", "login", "register", "test", "demo", "example",
    "staging", "dev", "sandbox", "admin", "",
    # Defense-in-depth (2026-08): these are literal PATH SEGMENTS from
    # known widget/embed/API URL families, not real company slugs. A
    # converter that blindly trusts path.split("/")[0] without first
    # checking for these families will mis-extract one of these as if it
    # were the company — see _url_to_slug_greenhouse's "embed" case below
    # for the confirmed real-world example (every company using
    # Greenhouse's standard <script src="boards.greenhouse.io/embed/
    # job_board/js?for=...">  embed snippet was being recorded with the
    # literal slug "embed", colliding every such company onto one fake
    # row and crashing the upsert batch with a duplicate-key error the
    # first time two of them landed in the same write).
    "embed", "job_board", "js", "widget", "iframe",
}


# ══════════════════════════════════════════════════════════
# URL → SLUG CONVERTERS (OpenPostings stores full URLs)
# ══════════════════════════════════════════════════════════

def _url_to_slug_greenhouse(url: str) -> str | None:
    """Handles TWO distinct real-world URL families, confirmed via live
    search results (boards.greenhouse.io/embed/job_board/js?for=vaco,
    .../for=onbe, boards.eu.greenhouse.io/embed/job_board/js?for=ANS):
      1. The board URL itself: boards.greenhouse.io/{slug}
      2. Greenhouse's standard embeddable-widget snippet:
         boards.greenhouse.io/embed/job_board(/js)?for={slug} — this is
         THE documented way Greenhouse tells customers to put jobs on
         their OWN site (see support.greenhouse.io "Host internal job
         board outside of Greenhouse"), so it is common, not an edge
         case. Path-family (2) carries NO real slug in parts[0] — that's
         always the literal word "embed" — the slug is in the `for`
         query param instead. Previously mishandled: parts[0]="embed"
         was returned as if it were the company, silently corrupting
         every Greenhouse-embedding company onto one fake ('greenhouse',
         'embed') row (see SKIP_SLUGS comment)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "greenhouse.io" not in host:
        return None
    path = parsed.path.strip("/")
    if path.startswith("embed/"):
        qs = parse_qs(parsed.query)
        slug = (qs.get("for") or [None])[0]
        if slug and slug.lower() not in SKIP_SLUGS:
            return slug
        return None
    parts = path.split("/")
    slug = parts[0] if parts else None
    if slug and slug.lower() not in SKIP_SLUGS:
        return slug
    return None


def _url_to_slug_lever(url: str) -> str | None:
    """2026-08: removed the old second fallback branch (`"lever" in host
    and ".co" in host`) — confirmed via research to be a real false-
    positive risk, since ".co" is also a substring of ".com", so ANY host
    containing "lever" anywhere plus ".com" anywhere (e.g. a coincidental
    "myleverage.example.com") would match and have its first path segment
    wrongly returned as a slug. No alternate real Lever subdomain was
    found to justify a broader match than the documented jobs.lever.co."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host == "lever.co" or host.endswith(".lever.co"):
        parts = parsed.path.strip("/").split("/")
        slug = parts[0] if parts else None
        if slug and slug.lower() not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_ashby(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "ashbyhq.com" in host:
        parts = parsed.path.strip("/").split("/")
        slug = parts[0] if parts else None
        if slug and slug.lower() not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_bamboohr(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "bamboohr.com" in host:
        slug = host.replace(".bamboohr.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None


def _url_to_slug_icims(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "icims.com" in host:
        # Pattern: careers-{slug}.icims.com or {slug}.icims.com
        slug = host.replace(".icims.com", "").lower()
        slug = re.sub(r"^careers-", "", slug)
        if slug and slug not in SKIP_SLUGS:
            return slug
    return None


_WORKDAY_LOCALE_SEGMENT_RE = re.compile(r"^[a-z]{2}-[A-Z]{2}$")


def _url_to_slug_workday(url: str) -> str | None:
    """2026-08: confirmed via research that many real Workday career-site
    URLs carry a locale segment (e.g. /en-US/) before the site id —
    convergys.wd1.myworkdayjobs.com/en-US/external_us/jobs,
    workday.wd5.myworkdayjobs.com/en-US/Workday/?q=... — both real, live
    examples. Previously path_parts[0] was taken unconditionally, so on
    these URLs it wrongly returned "en-US" as the site_id instead of the
    real one. Locale-free URLs (mastercard.wd1.myworkdayjobs.com/
    CorporateCareers) are unaffected either way."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "myworkdayjobs.com" in host:
        # Pattern: {company}.wd{N}.myworkdayjobs.com/[{locale}/]{site_id}
        parts = host.split(".")
        company = parts[0]
        wd = parts[1] if len(parts) > 1 else ""
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        if path_parts and _WORKDAY_LOCALE_SEGMENT_RE.match(path_parts[0]):
            path_parts = path_parts[1:]
        site_id = path_parts[0] if path_parts else ""
        if company and wd and site_id:
            return f"{company}|{wd}|{site_id}"
    return None


def _url_to_slug_rippling(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "rippling.com" in host:
        # Pattern 1: ats.rippling.com/{company}/jobs (most common in OpenPostings)
        parts = parsed.path.strip("/").split("/")
        if parts and parts[0] and parts[0].lower() not in SKIP_SLUGS:
            return parts[0]
        # Pattern 2: {company}.rippling.com (subdomain-based)
        slug = host.replace(".rippling.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug not in ("www", "app", "ats"):
            return slug
    return None


# {company}.workable.com is the confirmed, common, real Workable pattern
# (e.g. https://my-company.workable.com/) — the code previously ONLY
# matched apply.workable.com/{slug} and silently dropped this far more
# common subdomain form entirely. Reserved subdomains excluded below are
# Workable's own infra/marketing hosts, not customer boards.
_WORKABLE_RESERVED_SUBDOMAINS = {"www", "apply", "jobs", "help", "careers",
                                  "jobseekers", "partners", "support", "grow"}
# apply.workable.com/{token}/... also has a legacy job-shortlink family
# (apply.workable.com/j/{id}, /i/{id}) where the first path segment is a
# literal single-letter route marker, not a company slug — excluded so
# it isn't wrongly returned as one.
_WORKABLE_RESERVED_PATH_TOKENS = {"j", "i"}


def _url_to_slug_workable(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "workable.com" not in host:
        return None
    if host == "apply.workable.com":
        parts = parsed.path.strip("/").split("/")
        if parts and parts[0]:
            slug = parts[0].lower()
            if slug and slug not in SKIP_SLUGS and slug not in _WORKABLE_RESERVED_PATH_TOKENS:
                return slug
        return None
    sub = host.replace(".workable.com", "").lower()
    if sub and sub not in SKIP_SLUGS and sub not in _WORKABLE_RESERVED_SUBDOMAINS:
        return sub
    return None


def _url_to_slug_recruitee(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "recruitee.com" in host:
        slug = host.replace(".recruitee.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None


def _url_to_slug_smartrecruiters(url: str) -> str | None:
    """2026-08: tightened to the two real job-board subdomains only.
    The old blanket `"smartrecruiters.com" in host` check also matched
    every OTHER smartrecruiters.com subdomain — www (marketing site),
    developers (API docs), api, etc. — and returned each of THEIR nav
    paths (blog, resources, pricing, docs...) as if they were company
    slugs, since none of those happen to be in SKIP_SLUGS. Given this
    project's past SmartRecruiters false-positive incident (their shared
    API 200'd identically for real and fake slugs), this class of bug
    gets zero benefit of the doubt — restricted to the confirmed real
    job-board hosts, with the old subdomain-fallback branch removed since
    it's now redundant (those two hosts already covered above)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host not in ("jobs.smartrecruiters.com", "careers.smartrecruiters.com"):
        return None
    parts = parsed.path.strip("/").split("/")
    if parts and parts[0]:
        slug = parts[0]
        if slug.lower() not in SKIP_SLUGS and slug.lower() not in ("jobs", "careers", "posting"):
            return slug
    return None


def _url_to_slug_taleo(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "taleo.net" in host:
        company = host.replace(".taleo.net", "").lower()
        path_match = re.search(r"/careersection/([^/]+)/", parsed.path)
        if company and path_match:
            section = path_match.group(1)
            if section.lower() not in ("rest", "api", "admin"):
                return f"{company}|{section}"
    return None


def _url_to_slug_oracle_cloud(url: str) -> str | None:
    """Extract slug from Oracle Cloud HCM URLs.
    URL format: {tenant}.fa.{region}.oraclecloud.com/hcmUI/CandidateExperience/en/sites/{site}/...
    Slug format: '{host_prefix}|{site_number}' where host_prefix is everything before .oraclecloud.com
    e.g. 'eeho.fa.us2|CX_1'

    2026-08: added the /hcmUI/CandidateExperience/ path requirement.
    oraclecloud.com is Oracle's SHARED hosting domain for every Fusion
    Cloud app — ERP, CRM, Financials, HCM, not just recruiting — and the
    old code would fall back to returning the bare host_prefix as a
    "slug" for ANY oraclecloud.com URL that didn't match /sites/, which
    would silently misidentify a login page, an ERP screen, or any other
    non-recruiting page on the same pod as a job board. Same false-
    positive shape as the SmartRecruiters incident, caught before it
    could repeat."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "oraclecloud.com" not in host:
        return None
    if "/hcmui/candidateexperience/" not in parsed.path.lower():
        return None
    # Extract full host prefix (e.g. 'eeho.fa.us2' from 'eeho.fa.us2.oraclecloud.com')
    host_prefix = host.replace(".oraclecloud.com", "").lower()
    if not host_prefix or host_prefix in SKIP_SLUGS:
        return None
    # Extract site from /sites/{id} in path
    site_match = re.search(r"/sites/([^/]+)", parsed.path)
    if site_match:
        return f"{host_prefix}|{site_match.group(1)}"
    # Fallback: host prefix only (site can be discovered later) — safe
    # now that the CandidateExperience path check above gates this branch.
    return host_prefix


def _url_to_slug_brassring(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "brassring.com" in host:
        qs = parse_qs(parsed.query)
        pid = None
        sid = None
        for k, v in qs.items():
            if k.lower() == "partnerid" and v:
                pid = v[0]
            elif k.lower() == "siteid" and v:
                sid = v[0]
        if pid and sid and pid.isdigit() and sid.isdigit():
            return f"{pid}|{sid}"
    return None


def _url_to_slug_teamtailor(url: str) -> str | None:
    """Teamtailor's widget/embed loader (see support.teamtailor.com "job
    list widget") is served from a GENERIC infra subdomain —
    scripts.teamtailor.com/widget/... — with the actual company identified
    by a separate data-key attribute, not the script URL itself. Same
    false-slug shape as the Greenhouse 'embed' bug: subdomain.split(".")[0]
    on that URL is the literal word "scripts", not a company, and would
    otherwise be returned as if it were one. There's no data-key value in
    the URL to fall back to (unlike Greenhouse's ?for= — Teamtailor's key
    isn't in the script URL at all), so this platform's widget form is
    correctly a MISS via URL-only detection rather than a wrong answer;
    excluding "scripts" here just stops it from being a WRONG one."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "teamtailor.com" in host:
        slug = host.replace(".teamtailor.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug not in ("www", "app", "scripts", "cdn", "support"):
            return slug
    return None


def _url_to_slug_successfactors(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    sf_domains = (".successfactors.com", ".successfactors.eu", ".sapsf.com", ".sapsf.eu")
    if any(host.endswith(d) for d in sf_domains):
        # Try ?company= param first
        qs = parse_qs(parsed.query)
        company_key = None
        for k, v in qs.items():
            if k.lower() == "company" and v:
                company_key = v[0]
        if company_key:
            return f"{host}|{company_key}"
        # Fallback: extract subdomain as instance
        instance = host.split(".")[0]
        if instance and instance not in SKIP_SLUGS:
            # Try extracting company from path
            path_match = re.search(r"/career\?company=([^&]+)", url)
            if path_match:
                return f"{instance}|{path_match.group(1)}"
            return instance
    return None


def _url_to_slug_breezyhr(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "breezy.hr" in host:
        slug = host.replace(".breezy.hr", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None


# REMOVED 2026-08: ApplyToJob retired (see SUPPORTED_ATS comment) — no
# longer registered in URL_TO_SLUG/CC_PLATFORM_PATTERNS/CC_EXTRACTORS
# below. Function kept, unused, only in case it's ever needed for
# reference.
def _url_to_slug_applytojob(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "applytojob.com" in host:
        slug = host.replace(".applytojob.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None


def _url_to_slug_hrmdirect(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "hrmdirect.com" in host:
        slug = host.replace(".hrmdirect.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None


def _url_to_slug_softgarden(url: str) -> str | None:
    """2026-08: added .career.softgarden.de / .softgarden.de — confirmed
    via softgarden's own support docs that companyname.career.softgarden.de
    is their STANDARD/default career-page domain (not just the .io form),
    with a real live example found (alloheim.career.softgarden.de). The
    old code only recognized .softgarden.io and silently missed every
    customer on this default domain."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    for suffix in (".softgarden.io", ".career.softgarden.de", ".softgarden.de"):
        if host.endswith(suffix):
            slug = host[: -len(suffix)].lower()
            if slug and slug not in SKIP_SLUGS and slug != "www":
                return slug
    # Also handle api.softgarden.io/api/.../jobboards/{channelId}
    if "softgarden" in host:
        path_match = re.search(r"/jobboards/([^/]+)", parsed.path)
        if path_match:
            return path_match.group(1)
    return None


def _url_to_slug_zoho(url: str) -> str | None:
    """2026-08: added .zohorecruit.eu — confirmed real, in-active-use EU
    region domain (multiple distinct live customer boards found, e.g.
    eu.zohorecruit.eu, bpicnetwork.zohorecruit.eu). Old code only matched
    .zohorecruit.com and silently missed every EU-region customer."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    for suffix in (".zohorecruit.com", ".zohorecruit.eu"):
        if host.endswith(suffix):
            slug = host[: -len(suffix)].lower()
            if slug and slug not in SKIP_SLUGS and slug != "www":
                return slug
    return None


def _url_to_slug_paylocity(url: str) -> str | None:
    """Extract slug from Paylocity URLs.
    Pattern: recruiting.paylocity.com/recruiting/jobs/All/{uuid}/{company_name}
    Only accepts UUID-format IDs (numeric IDs are deprecated and return 404)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "paylocity.com" not in host:
        return None
    # Path: /recruiting/jobs/All/{uuid}/{CompanyName}
    parts = parsed.path.strip("/").split("/")
    # Need at least: recruiting/jobs/All/{id}/{name}. 2026-08: lowercase
    # both segments before comparing — confirmed real customer URLs use
    # capitalized paths too (e.g. .../Recruiting/Jobs/All/...), which the
    # old exact-lowercase comparison silently failed to match at all.
    if len(parts) >= 5 and parts[0].lower() == "recruiting" and parts[1].lower() == "jobs":
        company_id = parts[3]
        company_name = parts[4]
        # Only accept UUID-format IDs (8-4-4-4-12 hex pattern)
        # Numeric IDs are deprecated and return 404
        if company_id and company_name and re.match(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            company_id, re.I
        ):
            return f"{company_id}|{company_name}"
    return None


def _url_to_slug_joincom(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "join.com" not in host:
        return None
    # Pattern: join.com/companies/{slug} or join.com/companies/{slug}/jobs/...
    parts = parsed.path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "companies":
        slug = parts[1].lower()
        if slug and slug not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_personio(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    for suffix in (".jobs.personio.de", ".jobs.personio.com"):
        if host.endswith(suffix):
            slug = host.replace(suffix, "").lower()
            if slug and slug not in SKIP_SLUGS and slug != "www":
                return slug
    return None


def _url_to_slug_ycombinator(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "workatastartup.com" in host:
        parts = parsed.path.strip("/").split("/")
        # Pattern: /companies/{slug}
        if len(parts) >= 2 and parts[0] == "companies":
            slug = parts[1]
            if slug and slug.lower() not in SKIP_SLUGS:
                return slug
        # Pattern: /{slug} (direct slug in path). 2026-08: expanded the
        # reserved-route exclusion list — the site has more top-level nav
        # routes than just jobs/about/faq that would otherwise be wrongly
        # returned as a company slug.
        elif len(parts) >= 1 and parts[0]:
            slug = parts[0]
            if (slug.lower() not in SKIP_SLUGS
                    and slug.lower() not in ("jobs", "about", "faq", "login", "candidates",
                                              "mission", "legal", "privacy", "terms", "apply",
                                              "press", "blog", "signup", "companies")):
                return slug
    # OpenPostings may store YC URLs as ycombinator.com/companies/{slug}
    if "ycombinator.com" in host:
        parts = parsed.path.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "companies":
            slug = parts[1]
            if slug and slug.lower() not in SKIP_SLUGS:
                return slug
    return None


def _url_to_slug_eploy(url: str) -> str | None:
    """Extract slug from Eploy URLs.
    Pattern: {slug}.eploy.net/candidate/jobboard/..."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "eploy.net" not in host:
        return None
    slug = host.replace(".eploy.net", "").lower()
    if slug and slug not in SKIP_SLUGS and slug != "www":
        return slug
    return None


def _url_to_slug_folkshr(url: str) -> str | None:
    """Extract slug from Folks HR URLs.
    Pattern: jobs.folksats.app/{company}/... (post-2025-rebrand domain) or
    jobs.glowinthecloud.com/{company}/... (older "Glow Talents" domain —
    still what most existing customers are actually linked from; Folks
    acquired Glow Talents in Aug 2025 but the legacy domain is still live
    and better-linked). Same path shape on both, same slug format."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "folksats.app" not in host and "glowinthecloud.com" not in host:
        return None
    parts = parsed.path.strip("/").split("/")
    if parts and parts[0]:
        slug = parts[0].lower()
        if slug not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_jobadder(url: str) -> str | None:
    """Extract slug from JobAdder URLs.
    Pattern: clientapps.jobadder.com/{client_id}/{board_slug}/...
    Our internal slug format is '{client_id}|{board_slug}' — both are
    required since JobAdder boards are namespaced per-client, not by
    company name alone."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "jobadder.com" not in host:
        return None
    parts = parsed.path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] and parts[1]:
        client_id, board_slug = parts[0], parts[1]
        if client_id.lower() not in SKIP_SLUGS and board_slug.lower() not in SKIP_SLUGS:
            return f"{client_id}|{board_slug}"
    return None


def _url_to_slug_jobvite(url: str) -> str | None:
    """Extract slug from Jobvite URLs.
    Pattern: jobs.jobvite.com/{company}[/jobs|/job/{id}|/...] — the board
    homepage itself carries no extra path segment. A second, alias URL
    family also exists: jobs.jobvite.com/careers/{company}/... (same
    board, "careers/" literal prefix) — both resolve to the same slug.

    2026-08: added a guard against the legacy app.jobvite.com/CompanyJobs/
    Careers.aspx?c={code}&j={id} family (confirmed still live/referenced)
    — its real company identifier is the `c` query param, not the path;
    the old code would take parts[0] ("companyjobs") as a fake slug since
    that word isn't in SKIP_SLUGS. Now reads `c` directly for that family
    and returns None rather than a wrong slug if it's missing."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "jobvite.com" not in host:
        return None
    if "careers.aspx" in parsed.path.lower():
        qs = parse_qs(parsed.query)
        code = (qs.get("c") or [None])[0]
        if code and code.lower() not in SKIP_SLUGS:
            return code
        return None
    parts = parsed.path.strip("/").split("/")
    if parts and parts[0].lower() == "careers":
        parts = parts[1:]
    if parts and parts[0]:
        slug = parts[0].lower()
        if slug not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_adp(url: str) -> str | None:
    """Extract slug from ADP Workforce Now career-center URLs.
    Both 'cid' and 'ccId' query params are required to hit the public
    job-requisitions API — our internal slug format is '{cid}|{ccId}'."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "adp.com" not in host:
        return None
    qs = parse_qs(parsed.query)
    cid = (qs.get("cid") or qs.get("CID") or [None])[0]
    cc_id = (qs.get("ccId") or qs.get("ccid") or qs.get("CCID") or [None])[0]
    if cid and cc_id:
        return f"{cid}|{cc_id}"
    return None


def _extract_adp_legacy_client(url: str) -> str | None:
    """Pure parse (no network): pull the 'client' shortname out of ADP's
    legacy job-posting URL family (jobs/apply/posting.html?client=...).
    ADP decommissioned this URL family on 2026-06-26 — it no longer serves
    job content, only a redirect notice — but the redirect itself is a
    live, working client→cid resolver (see _resolve_adp_legacy_client)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "adp.com" not in host or "/jobs/apply/posting.html" not in parsed.path:
        return None
    qs = parse_qs(parsed.query)
    client = (qs.get("client") or [None])[0]
    return client or None


def _resolve_adp_legacy_client(client: str) -> str | None:
    """ONE live HTTP call: follow the legacy posting.html URL's redirect
    chain to pick up the modern cid (ADP's own server-side lookup — no
    guessing). ccId=19000101_000001 is the generic 'career center root'
    sentinel that reliably round-trips to the client's real cid in
    testing; the redirect target echoes back whatever ccId is correct
    for that tenant, which we use over the sentinel if present."""
    try:
        r = requests.get(
            "https://workforcenow.adp.com/jobs/apply/posting.html",
            params={"client": client, "ccId": "19000101_000001", "type": "MP"},
            timeout=20, allow_redirects=True,
            headers={"User-Agent": _ROBOTS_UA},
        )
        final_qs = parse_qs(urlparse(r.url).query)
        cid = (final_qs.get("cid") or [None])[0]
        cc_id = (final_qs.get("ccId") or [None])[0] or "19000101_000001"
        if cid:
            return f"{cid}|{cc_id}"
    except Exception as e:
        log.debug(f"ADP legacy client resolve failed for '{client}': {e}")
    return None


# Cap live resolve calls per run — this platform's legacy URL family is
# deprecated and rare, and each hit costs one real HTTP round-trip against
# ADP's own server (unlike the pure-parse extractors above), so we bound
# it rather than risk hammering their server if a crawl surfaces a lot of
# stale legacy links at once.
_ADP_LEGACY_RESOLVE_CAP = 200


def _url_to_slug_adp_discovery(url: str) -> str | None:
    """Combined extractor for CC/Wayback ADP discovery: pure-parses the
    modern cid/ccId URL family, and resolves the deprecated legacy
    client= family via one live redirect-follow per unique client (capped
    — see _ADP_LEGACY_RESOLVE_CAP). Kept separate from _url_to_slug_adp
    (which stays a pure, no-network function used elsewhere, e.g. for
    OpenPostings' 110k+ row scan where a live call per row isn't viable)."""
    modern = _url_to_slug_adp(url)
    if modern:
        return modern
    client = _extract_adp_legacy_client(url)
    if not client:
        return None
    if client in _url_to_slug_adp_discovery._resolved_clients:
        return _url_to_slug_adp_discovery._resolved_clients[client]
    if len(_url_to_slug_adp_discovery._resolved_clients) >= _ADP_LEGACY_RESOLVE_CAP:
        return None
    resolved = _resolve_adp_legacy_client(client)
    _url_to_slug_adp_discovery._resolved_clients[client] = resolved
    time.sleep(0.5)  # be polite — this is a live call against ADP's own server
    return resolved


_url_to_slug_adp_discovery._resolved_clients = {}


def _url_to_slug_avature(url: str) -> str | None:
    """Extract slug from Avature URLs.
    Pattern: {subdomain}.avature.net/... — path structure varies a lot in
    practice (bare /careers/, locale-prefixed /en_US/careers/, or even
    /en_US/main/ with no "careers" segment at all — Avature's own
    corporate site uses that last one), so this only keys off the
    subdomain and ignores path entirely; the subdomain alone is the slug."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "avature.net" not in host:
        return None
    slug = host.replace(".avature.net", "").lower()
    if slug and slug not in SKIP_SLUGS and slug != "www":
        return slug
    return None


# 2026-09: 5 new platforms added from a user-supplied candidate list of 36
# names (sourced from bloomberry.com/data ATS market-share pages). Only
# these 5 got a working extractor — each pattern below was confirmed
# against 2+ live example URLs found via web search, not guessed. The
# other candidates from that list are deliberately NOT added — see the
# comment block right after URL_TO_SLUG for exactly which ones and why
# (some have no fixed per-tenant hosted domain at all — e.g. GoHire lets
# customers set a fully custom URL — some aren't verifiable without a
# live example, and JazzHR is literally the same platform as the already-
# removed "applytojob", not a new one).

def _url_to_slug_trakstar(url: str) -> str | None:
    """Extract slug from Trakstar Hire URLs (formerly Recruiterbox).
    Pattern: {tenant}.hire.trakstar.com/... — confirmed via multiple live
    examples (e.g. recruiterbox.hire.trakstar.com, teamcoworker.hire.
    trakstar.com)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host.endswith(".hire.trakstar.com"):
        return None
    slug = host[: -len(".hire.trakstar.com")].lower()
    if slug and slug not in SKIP_SLUGS and slug != "www":
        return slug
    return None


def _url_to_slug_jobscore(url: str) -> str | None:
    """Extract slug from JobScore URLs. Pattern: careers.jobscore.com/
    careers/{tenant}[/jobs/...] — confirmed via multiple live examples
    (careers/vec, careers/solutions2go, careers/ariasystems)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host.lower() != "careers.jobscore.com":
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 2 and parts[0].lower() == "careers":
        slug = parts[1].lower()
        if slug and slug not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_eightfold(url: str) -> str | None:
    """Extract slug from Eightfold AI URLs. Pattern: {tenant}.eightfold.ai/
    careers — confirmed via a live example (lamresearch.eightfold.ai/
    careers). app.eightfold.ai / preview.eightfold.ai / employee.eightfold.ai
    are Eightfold's OWN generic hosts, not a tenant — explicitly excluded."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host.endswith(".eightfold.ai"):
        return None
    slug = host[: -len(".eightfold.ai")].lower()
    if slug in ("app", "preview", "employee", "www", ""):
        return None
    if slug not in SKIP_SLUGS:
        return slug
    return None


def _url_to_slug_gupy(url: str) -> str | None:
    """Extract slug from Gupy URLs (Brazil-market ATS). Pattern:
    {tenant}.gupy.io/... — confirmed via a live example (atento.gupy.io/
    jobs/...)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host.endswith(".gupy.io"):
        return None
    slug = host[: -len(".gupy.io")].lower()
    if slug and slug not in SKIP_SLUGS and slug not in ("www", "developers", "suporte-candidatos"):
        return slug
    return None


def _url_to_slug_hrmos(url: str) -> str | None:
    """Extract slug from HRMOS URLs (Japan-market ATS). Pattern:
    hrmos.co/pages/{tenant}/... — confirmed via a live example
    (hrmos.co/pages/cornes/jobs/10110)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host.lower() != "hrmos.co":
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 2 and parts[0].lower() == "pages":
        slug = parts[1].lower()
        if slug and slug not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_gem(url: str) -> str | None:
    """Extract slug from Gem (gem.com) career-site URLs. Pattern:
    jobs.gem.com/{tenant} — confirmed 2026-09 via multiple live examples
    (jobs.gem.com/gem, jobs.gem.com/function-health, jobs.gem.com/inception,
    jobs.gem.com/bluesky), each backed by a live, unauthenticated JSON API
    at api.gem.com/job_board/v0/{tenant}/job_posts/ (confirmed live, no
    auth needed — see scrape_gem in ats_scrapers.py). Gem is primarily a
    recruiting CRM, but its "Career Sites" product also hosts real
    candidate-facing public job boards at this domain, same shape as
    Greenhouse/Lever."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host.lower() != "jobs.gem.com":
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        return None
    slug = parts[0].lower()
    if slug and slug not in SKIP_SLUGS:
        return slug
    return None


URL_TO_SLUG = {
    "greenhouse": _url_to_slug_greenhouse,
    "lever": _url_to_slug_lever,
    "ashby": _url_to_slug_ashby,
    "bamboohr": _url_to_slug_bamboohr,
    "icims": _url_to_slug_icims,
    "workday": _url_to_slug_workday,
    "rippling": _url_to_slug_rippling,
    "workable": _url_to_slug_workable,
    "recruitee": _url_to_slug_recruitee,
    "smartrecruiters": _url_to_slug_smartrecruiters,
    "taleo": _url_to_slug_taleo,
    "oracle_cloud_hcm": _url_to_slug_oracle_cloud,
    "brassring": _url_to_slug_brassring,
    "teamtailor": _url_to_slug_teamtailor,
    "successfactors": _url_to_slug_successfactors,
    "breezyhr": _url_to_slug_breezyhr,
    # "applytojob" removed 2026-08 — see SUPPORTED_ATS comment above.
    "hrmdirect": _url_to_slug_hrmdirect,
    "softgarden": _url_to_slug_softgarden,
    "zoho": _url_to_slug_zoho,
    "paylocity": _url_to_slug_paylocity,
    "ycombinator": _url_to_slug_ycombinator,
    "personio": _url_to_slug_personio,
    "joincom": _url_to_slug_joincom,
    # New (2026-08):
    "eploy": _url_to_slug_eploy,
    "folkshr": _url_to_slug_folkshr,
    "jobadder": _url_to_slug_jobadder,
    "jobvite": _url_to_slug_jobvite,
    "adp": _url_to_slug_adp,
    "avature": _url_to_slug_avature,
    # New (2026-09) — see the big comment above URL_TO_SLUG for what was
    # verified vs. deliberately left out from the 36-platform candidate list:
    "trakstar": _url_to_slug_trakstar,
    "jobscore": _url_to_slug_jobscore,
    "eightfold": _url_to_slug_eightfold,
    "gupy": _url_to_slug_gupy,
    "hrmos": _url_to_slug_hrmos,
    # New (2026-09) — Gem, re-investigated at the user's request after an
    # earlier "couldn't verify" answer; it does have a real public
    # candidate-facing job board (jobs.gem.com/{tenant}) backed by a live
    # unauthenticated JSON API, confirmed live — see _url_to_slug_gem.
    "gem": _url_to_slug_gem,
}


# ══════════════════════════════════════════════════════════
# SOURCE 1: Feashliaa GitHub
# ══════════════════════════════════════════════════════════

def fetch_feashliaa_slugs() -> dict[str, set[str]]:
    """Download slug lists from Feashliaa's job-board-aggregator repo.
    Returns JSON arrays of slugs directly — no URL conversion needed.
    Covers 6 platforms: greenhouse, lever, ashby, bamboohr, icims, workday."""
    slugs_by_ats: dict[str, set[str]] = {}

    for ats, url in FEASHLIAA_SOURCES.items():
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                clean = {s.strip() for s in data
                         if isinstance(s, str) and s.strip()
                         and s.strip().lower() not in SKIP_SLUGS}
                slugs_by_ats[ats] = clean
                log.info(f"  {ats}: {len(clean)} slugs from Feashliaa")
            else:
                log.warning(f"  {ats}: unexpected JSON format (not a list)")
                slugs_by_ats[ats] = set()
        except Exception as e:
            log.error(f"  {ats}: failed to fetch from Feashliaa: {e}")
            slugs_by_ats[ats] = set()

    total = sum(len(s) for s in slugs_by_ats.values())
    log.info(f"Feashliaa total: {total} slugs across {len(slugs_by_ats)} platforms")
    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 2: kalil0321/ats-scrapers (CSV inventories)
# ══════════════════════════════════════════════════════════

def _parse_csv_line(line: str) -> tuple[str, str, str] | None:
    """Parse a CSV line with possible quoted fields. Returns (name, slug, url)."""
    line = line.strip()
    if not line:
        return None
    # Handle quoted fields (some company names have commas)
    parts = []
    current = ""
    in_quotes = False
    for ch in line:
        if ch == '"':
            in_quotes = not in_quotes
        elif ch == ',' and not in_quotes:
            parts.append(current)
            current = ""
        else:
            current += ch
    parts.append(current)
    if len(parts) >= 3:
        return parts[0].strip(), parts[1].strip(), parts[2].strip()
    return None


def fetch_kalil_slugs() -> dict[str, dict[str, str]]:
    """Download CSV company lists from kalil0321/ats-scrapers repo.
    CSVs have format: name,slug,url
    Returns {ats: {slug: company_name}}."""
    slugs_by_ats: dict[str, dict[str, str]] = {}

    # Platforms where the CSV slug column can be used directly
    DIRECT_SLUG_PLATFORMS = {
        "greenhouse", "lever", "ashby", "workable", "recruitee",
        "smartrecruiters", "teamtailor", "breezyhr", "softgarden",
    }

    for ats, csv_url in KALIL_SOURCES.items():
        converter = URL_TO_SLUG.get(ats)
        found: dict[str, str] = {}

        try:
            r = requests.get(csv_url, timeout=60)
            r.raise_for_status()
            lines = r.text.strip().split("\n")

            for line in lines[1:]:
                parsed = _parse_csv_line(line)
                if not parsed:
                    continue
                name, raw_slug, url = parsed

                slug = None
                if converter and url:
                    slug = converter(url)

                if not slug and ats in DIRECT_SLUG_PLATFORMS:
                    if raw_slug and raw_slug.lower() not in SKIP_SLUGS:
                        slug = raw_slug

                if slug:
                    found[slug] = name.strip() if name else ""

            slugs_by_ats[ats] = found
            if found:
                log.info(f"  {ats}: {len(found)} slugs from kalil0321")

        except Exception as e:
            log.error(f"  {ats}: failed to fetch from kalil0321: {e}")
            slugs_by_ats[ats] = {}

    total = sum(len(s) for s in slugs_by_ats.values())
    log.info(f"kalil0321 total: {total} slugs across "
             f"{sum(1 for s in slugs_by_ats.values() if s)} platforms")
    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 3: OpenPostings
# ══════════════════════════════════════════════════════════

def fetch_openpostings_slugs() -> dict[str, dict[str, str]]:
    """Download OpenPostings jobs.db and extract company slugs
    for platforms we support. Returns {ats: {slug: company_name}}."""
    log.info("Downloading OpenPostings jobs.db...")
    slugs_by_ats: dict[str, dict[str, str]] = {ats: {} for ats in SUPPORTED_ATS}
    skipped_ats = {}

    try:
        r = requests.get(OPENPOSTINGS_DB_URL, timeout=120, stream=True)
        r.raise_for_status()
    except Exception as e:
        log.error(f"Failed to download OpenPostings DB: {e}")
        return slugs_by_ats

    # Write to temp file and open as SQLite
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name
        for chunk in r.iter_content(chunk_size=8192):
            tmp.write(chunk)

    try:
        conn = sqlite3.connect(tmp_path)
        cursor = conn.execute(
            "SELECT company_name, url_string, ATS_name FROM companies"
        )
        total = 0
        matched = 0

        # Track conversion failures per ATS for debugging
        conversion_failures: dict[str, list[str]] = {}

        for company_name, url_string, ats_name in cursor:
            total += 1
            our_ats = _map_ats_name(ats_name)
            if not our_ats:
                skipped_ats[ats_name] = skipped_ats.get(ats_name, 0) + 1
                continue

            converter = URL_TO_SLUG.get(our_ats)
            if not converter:
                continue

            slug = converter(url_string)
            if slug:
                # Keep company name (first one wins if duplicates)
                if slug not in slugs_by_ats[our_ats]:
                    slugs_by_ats[our_ats][slug] = (company_name or "").strip()
                matched += 1
            else:
                # Track failed conversions for debugging
                if our_ats not in conversion_failures:
                    conversion_failures[our_ats] = []
                if len(conversion_failures[our_ats]) < 3:
                    conversion_failures[our_ats].append(url_string)

        conn.close()
        log.info(f"OpenPostings: {total} total companies, "
                 f"{matched} matched to our {len(SUPPORTED_ATS)} platforms")

        # Log unmapped ATSs (for future expansion)
        if skipped_ats:
            top_skipped = sorted(skipped_ats.items(), key=lambda x: -x[1])[:10]
            log.info(f"Top unmapped ATSs: {', '.join(f'{k}({v})' for k, v in top_skipped)}")

        for ats in sorted(SUPPORTED_ATS):
            count = len(slugs_by_ats.get(ats, {}))
            if count:
                log.info(f"  {ats}: {count} companies")

        # Log sample failing URLs for platforms with 0 matches
        if conversion_failures:
            log.info("URL conversion failures (sample URLs):")
            for ats, samples in sorted(conversion_failures.items()):
                if not slugs_by_ats.get(ats):
                    log.info(f"  {ats}: {samples}")

    except Exception as e:
        log.error(f"Failed to parse OpenPostings DB: {e}")
    finally:
        os.unlink(tmp_path)

    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 4: Common Crawl (ongoing discovery)
# ══════════════════════════════════════════════════════════

CC_PLATFORM_PATTERNS = {
    # ADDED 2026-08: these 6 already have their bulk needs met by Feashliaa
    # (single static JSON dump per platform, much faster than a CDX
    # search), so they were deliberately left out of Common Crawl at
    # first. Adding them here anyway as a supplemental top-up — dedup
    # against Feashliaa's own list is free (on_conflict=ats,slug upsert),
    # so any extra companies CC's independent crawl happens to catch that
    # Feashliaa's dump missed are pure upside, not redundant work. Costs
    # real runtime though (paginated CDX queries + rate-limit sleeps per
    # pattern), so this is a deliberate "worth it as a top-up" choice, not
    # a claim these need CC as their primary source.
    "greenhouse": ["boards.greenhouse.io/*"],
    "lever": ["jobs.lever.co/*"],
    "ashby": ["jobs.ashbyhq.com/*"],
    "bamboohr": ["*.bamboohr.com/careers*", "*.bamboohr.com/jobs*"],
    "icims": ["*.icims.com/jobs/*"],
    # NOTE: Workday's "wd{N}" instance number isn't a small fixed set —
    # verified live examples exist for wd1 through wd12+ (e.g. Walmart on
    # wd5, Salesforce/Capital One on wd12, Desjardins on wd10), assigned
    # essentially arbitrarily per customer with no obvious pattern — so a
    # single wildcarded host pattern is used instead of enumerating
    # specific wd numbers, which would silently miss real companies on
    # any instance not explicitly listed.
    "workday": ["*.myworkdayjobs.com/*"],
    # 2026-08: added the broad *.workable.com/* pattern — {company}.workable.com
    # is the confirmed common real form, not just apply.workable.com/{slug}.
    # Broad on purpose (same reasoning as avature below): the extractor
    # itself (_url_to_slug_workable) already filters out Workable's own
    # reserved/infra subdomains, so this costs nothing in false positives.
    "workable": ["apply.workable.com/*", "*.workable.com/*"],
    "recruitee": ["*.recruitee.com/api/offers*", "*.recruitee.com/o/*"],
    "smartrecruiters": ["jobs.smartrecruiters.com/*", "careers.smartrecruiters.com/*"],
    "rippling": ["*.rippling.com/careers*", "*.rippling.com/jobs*"],
    "teamtailor": ["*.teamtailor.com/jobs*"],
    "breezyhr": ["*.breezy.hr/*"],
    # "applytojob" removed 2026-08 — see SUPPORTED_ATS comment above.
    "personio": ["*.jobs.personio.de/*", "*.jobs.personio.com/*"],
    "joincom": ["join.com/companies/*/jobs*", "join.com/companies/*"],
    # Newly enabled platforms:
    "taleo": ["*.taleo.net/careersection/*/jobsearch.ftl*"],
    "oracle_cloud_hcm": ["*.oraclecloud.com/hcmUI/CandidateExperience/*"],
    "paylocity": ["recruiting.paylocity.com/recruiting/jobs/*"],
    "hrmdirect": ["*.hrmdirect.com/employment/*"],
    # 2026-08: added the .eu region domain — confirmed real, in-active-use
    # (multiple distinct live customer boards found on zohorecruit.eu).
    "zoho": ["*.zohorecruit.com/jobs/*", "*.zohorecruit.eu/jobs/*"],
    # "api.softgarden.io/.../jobboards/{channelId}/..." added 2026-08 —
    # confirmed real (softgarden's own dev docs), and _url_to_slug_softgarden
    # already parses this shape via its /jobboards/ regex — it just wasn't
    # being searched for yet.
    # 2026-08: added career.softgarden.de — confirmed via softgarden's own
    # support docs to be their STANDARD/default career-page domain, not
    # just an alternate — the .io form alone was missing most customers.
    "softgarden": ["*.softgarden.io/en/vacancies*", "*.softgarden.io/vacancies*",
                    "*.softgarden.io/job/*", "api.softgarden.io/*/jobboards/*",
                    "*.career.softgarden.de/*"],
    # New (2026-08):
    "eploy": ["*.eploy.net/candidate/jobboard/*"],
    # Folks HR: folksats.app is the post-2025-rebrand domain; glowinthecloud.com
    # is the older (pre-acquisition "Glow Talents") domain that's still what
    # most existing customers are actually linked from — both are queried.
    "folkshr": ["jobs.folksats.app/*", "jobs.glowinthecloud.com/*"],
    "jobadder": ["clientapps.jobadder.com/*"],
    "jobvite": ["jobs.jobvite.com/*"],
    # NOTE: verified live and unchanged, but expect near-zero real hits even
    # with the query fixed — most ADP customers embed career listings via a
    # JS web component (<recruitment-current-openings cid=... ccid=...>)
    # rather than a plain <a href>, so Common Crawl's link-following crawler
    # has no anchor to discover in the first place. Treat CC as a weak
    # source for ADP; the Wayback Machine source below does much better.
    # Second pattern is ADP's legacy (deprecated 2026-06-26) client= URL
    # family — no longer serves job content, but its redirect resolves to
    # a real modern cid, so old crawled/archived hits are still useful.
    # See _url_to_slug_adp_discovery.
    "adp": ["workforcenow.adp.com/mascsr/*", "workforcenow.adp.com/jobs/apply/posting.html*"],
    # WIDENED 2026-08: real Avature career sites don't reliably use a
    # "/careers/" path segment — verified live examples include
    # {sub}.avature.net/en_US/careers/*, {sub}.avature.net/en_US/main/*
    # (Avature's own corporate site uses this, no "careers" segment at
    # all), and bare {sub}.avature.net/careers/* with no locale prefix.
    # _url_to_slug_avature() already only keys off the subdomain and
    # ignores path entirely, so a single broad */* pattern costs nothing
    # in false positives (there's no separate "not a careers page" host
    # to accidentally match) while catching every real path variant
    # instead of just guessing at path segments one at a time.
    "avature": ["*.avature.net/*"],
    # New (2026-09) — Trakstar/JobScore/Eightfold/Gupy/HRMOS, added
    # alongside their URL_TO_SLUG resolvers (see those for the confirmed
    # live example URLs each pattern below is based on). Wiring these in
    # here also gets them Wayback Machine coverage for free, since
    # fetch_wayback_slugs() now reuses this same dict — see that
    # function's docstring.
    "trakstar": ["*.hire.trakstar.com/*"],
    "jobscore": ["careers.jobscore.com/careers/*"],
    "eightfold": ["*.eightfold.ai/*"],
    "gupy": ["*.gupy.io/*"],
    "hrmos": ["hrmos.co/pages/*"],
    "gem": ["jobs.gem.com/*"],
}

# Reuse URL_TO_SLUG converters for Common Crawl extraction
CC_EXTRACTORS = {
    # Added 2026-08 alongside the same 6 platforms' CC_PLATFORM_PATTERNS
    # entries above — missing here caused a KeyError crash on the very
    # first live run (CC_PLATFORM_PATTERNS and CC_EXTRACTORS are two
    # separate dicts that both need an entry per platform; only the
    # patterns dict got updated the first time).
    "greenhouse": _url_to_slug_greenhouse,
    "lever": _url_to_slug_lever,
    "ashby": _url_to_slug_ashby,
    "bamboohr": _url_to_slug_bamboohr,
    "icims": _url_to_slug_icims,
    "workday": _url_to_slug_workday,
    "workable": _url_to_slug_workable,
    "recruitee": _url_to_slug_recruitee,
    "smartrecruiters": _url_to_slug_smartrecruiters,
    "rippling": _url_to_slug_rippling,
    "teamtailor": _url_to_slug_teamtailor,
    "breezyhr": _url_to_slug_breezyhr,
    # "applytojob" removed 2026-08 — see SUPPORTED_ATS comment above. Kept
    # in sync with CC_PLATFORM_PATTERNS above (these two dicts must always
    # match keys — see the CC_EXTRACTORS KeyError incident earlier this
    # project for why a mismatch here crashes the whole Common Crawl run).
    "personio": _url_to_slug_personio,
    "joincom": _url_to_slug_joincom,
    # Newly enabled platforms:
    "taleo": _url_to_slug_taleo,
    "oracle_cloud_hcm": _url_to_slug_oracle_cloud,
    "paylocity": _url_to_slug_paylocity,
    "hrmdirect": _url_to_slug_hrmdirect,
    "zoho": _url_to_slug_zoho,
    "softgarden": _url_to_slug_softgarden,
    # New (2026-08):
    "eploy": _url_to_slug_eploy,
    "folkshr": _url_to_slug_folkshr,
    "jobadder": _url_to_slug_jobadder,
    "jobvite": _url_to_slug_jobvite,
    "adp": _url_to_slug_adp_discovery,  # combined modern + legacy-resolve, see above
    "avature": _url_to_slug_avature,
    # New (2026-09), kept in sync with CC_PLATFORM_PATTERNS above:
    "trakstar": _url_to_slug_trakstar,
    "jobscore": _url_to_slug_jobscore,
    "eightfold": _url_to_slug_eightfold,
    "gupy": _url_to_slug_gupy,
    "hrmos": _url_to_slug_hrmos,
    "gem": _url_to_slug_gem,
}


def get_latest_crawl_ids(n: int = 3) -> list[str]:
    try:
        r = requests.get(CC_COLLINFO, timeout=30)
        r.raise_for_status()
        return [c["id"] for c in r.json()[:n]]
    except Exception as e:
        log.error(f"Failed to fetch CC crawl list: {e}")
        return []


def query_cc_index(crawl_id: str, url_pattern: str) -> list[str]:
    endpoint = f"{CC_INDEX_URL}/{crawl_id}-index"
    all_urls = []
    page = 0

    while page < 100:
        params = {
            "url": url_pattern,
            "output": "json",
            "fl": "url",
            "limit": 15000,
            "page": page,
        }
        try:
            r = requests.get(endpoint, params=params, timeout=120)
            if r.status_code == 404:
                break
            r.raise_for_status()
            lines = r.text.strip().split("\n")
            if not lines or lines == [""]:
                break
            for line in lines:
                try:
                    record = json.loads(line)
                    url = record.get("url", "")
                    if url:
                        all_urls.append(url)
                except json.JSONDecodeError:
                    continue
            if len(lines) < 15000:
                break
            page += 1
            time.sleep(0.5)
        except Exception as e:
            log.warning(f"CC query error ({crawl_id}, {url_pattern}): {e}")
            break

    return all_urls


def fetch_commoncrawl_slugs(n_crawls: int = 3, cc_shard: int | None = None,
                             cc_total_shards: int = 1) -> dict[str, set[str]]:
    """Discover slugs from Common Crawl for platforms not well-covered
    by OpenPostings.

    cc_shard/cc_total_shards split the 27 PLATFORMS (not a hash of work
    items) across `cc_total_shards` independent runs — added 2026-08 when
    source 9 (Web Data Commons) was retired for a bad cost/payoff ratio
    (37 slugs for ~4 minutes of live fetching against a 3000-page sample)
    and its GitHub Actions matrix slot was handed to a second Common Crawl
    shard instead, since Common Crawl was already the slowest-running
    source in the matrix (26 platforms x up to 6 crawls x however many
    patterns each, all sequential within one job) and splitting it in two
    actually cuts wall-clock, unlike WDC which was just spending runtime
    for near-nothing. Bumped from 2 to 4 shards in 2026-09 (TheirStack's
    retirement freed up a matrix slot, and this remained the slowest
    single source, so splitting further keeps paying off). Pass
    cc_shard=None (default) to run all platforms in one call, same as
    before this existed.
    """
    platforms = list(CC_PLATFORM_PATTERNS.items())
    if cc_shard is not None and cc_total_shards > 1:
        platforms = [item for i, item in enumerate(platforms)
                     if i % cc_total_shards == cc_shard]
        log.info(f"Common Crawl: shard {cc_shard}/{cc_total_shards} — "
                 f"{len(platforms)}/{len(CC_PLATFORM_PATTERNS)} platforms "
                 f"assigned to this shard")

    slugs_by_ats: dict[str, set[str]] = {ats: set() for ats, _ in platforms}

    crawl_ids = get_latest_crawl_ids(n_crawls)
    if not crawl_ids:
        return slugs_by_ats

    log.info(f"Common Crawl: querying {len(crawl_ids)} crawls")

    for ats, patterns in platforms:
        extractor = CC_EXTRACTORS[ats]
        for crawl_id in crawl_ids:
            for pattern in patterns:
                log.info(f"  Querying {crawl_id} for {pattern}...")
                urls = query_cc_index(crawl_id, pattern)
                log.info(f"    Got {len(urls)} URLs")
                for url in urls:
                    slug = extractor(url)
                    if slug:
                        slugs_by_ats[ats].add(slug)
                time.sleep(1)

        count = len(slugs_by_ats[ats])
        if count:
            log.info(f"  {ats}: {count} companies from Common Crawl")

    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# WAYBACK MACHINE CDX — all-platform supplemental discovery
# ══════════════════════════════════════════════════════════
#
# Originally ADP-only: ADP is a bad fit for Common Crawl because most
# customers embed their board via a JS web component
# (<recruitment-current-openings cid=... ccid=...>) rather than a plain
# <a href>, so a link-following crawler like CC never sees a URL to
# follow, while the Wayback Machine's CDX index is a different, broader,
# independently-sourced index (it also ingests URLs via Google Sitemaps,
# third-party "Save Page Now" submissions, etc.) that can have snapshots
# of the actual pages even when Common Crawl has none.
#
# GENERALIZED 2026-09 (at the user's request) from ADP-only to every
# platform in CC_PLATFORM_PATTERNS: that same "IA's index isn't just CC's
# index" advantage isn't ADP-specific, and reusing CC_PLATFORM_PATTERNS/
# CC_EXTRACTORS wholesale means zero new per-platform extraction code —
# a Wayback snapshot's "original" URL is the same kind of real, historical
# URL Common Crawl indexes, so the exact same URL_TO_SLUG-based extractors
# apply unchanged. Only the query construction differs (CDX's own
# `matchType`/wildcard rules vs. CC's `url_pattern` REST param) — see
# _cc_pattern_to_wayback_query. The old ADP-only `_ADP_WAYBACK_PATTERNS`
# list is gone: CC_PLATFORM_PATTERNS["adp"] already carries the identical
# 2 patterns (modern mascsr/* + legacy jobs/apply/posting.html*), so ADP
# keeps getting covered exactly as before, just via the shared dict now.
#
# NOTE on the Supabase `source` column: this now upserts with
# source="wayback" (2026-09) — renamed from "wayback_adp" now that this
# covers every platform, not just ADP. archive_i.source has a Postgres
# CHECK constraint whose allowed-values list previously only had the
# literal string "wayback_adp"; a migration (rename_wayback_adp_source_
# to_wayback) widened it to "wayback" instead and backfilled every
# existing row from "wayback_adp" to "wayback", so this is a real rename,
# not just a broadened label with the old string kept for safety.
#
# The CDX API (web.archive.org/cdx/search/cdx) is IA's own documented,
# public, purpose-built endpoint for exactly this kind of targeted
# URL-pattern lookup — not a scrape of a page meant for browsers — but
# per this project's non-negotiable robots.txt policy we still check
# web.archive.org/robots.txt live before every run rather than assume.

WAYBACK_CDX_URL = "http://web.archive.org/cdx/search/cdx"

_ROBOTS_UA = "ATS-Global-Scanner/1.0"

# Running tallies of *why* a robots.txt check came back "disallowed" —
# incremented by _robots_allows(), read/reset by callers that want a
# one-line end-of-run summary instead of a warning log per dead domain
# (most failures here are just dead/unreachable company sites, not actual
# robots.txt disallow rules — see fetch_yc_slugs for the summary log).
_robots_check_stats = {"unreachable": 0, "disallowed_by_rule": 0}


def _robots_allows(base_url: str, path: str, user_agent: str = _ROBOTS_UA) -> bool:
    """Minimal robots.txt check: fetch {base_url}/robots.txt and verify
    `path` isn't disallowed for '*' or our own UA. Fails CLOSED (returns
    False) on any fetch/parse error — if we can't confirm it's allowed,
    we don't proceed. This mirrors the same non-negotiable policy already
    applied to UKG (excluded from ats_scrapers.py for exactly this)."""
    try:
        r = requests.get(f"{base_url}/robots.txt", timeout=15,
                          headers={"User-Agent": user_agent})
        if r.status_code >= 400:
            # No robots.txt at all is conventionally "allow everything"
            return True
        applicable_disallows = []
        current_ua = None
        for line in r.text.splitlines():
            line = line.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            key, _, value = line.partition(":")
            key = key.strip().lower()
            value = value.strip()
            if key == "user-agent":
                current_ua = value.lower()
            elif key == "disallow" and current_ua in ("*", user_agent.lower()):
                if value:
                    applicable_disallows.append(value)
        allowed = not any(path.startswith(d) for d in applicable_disallows)
        if not allowed:
            _robots_check_stats["disallowed_by_rule"] += 1
        return allowed
    except Exception as e:
        # Almost always a dead/unreachable/misconfigured site (DNS failure,
        # timeout, broken SSL) rather than an actual robots.txt rule — log
        # it at DEBUG (silent unless you pass -v) instead of WARNING so a
        # run against thousands of candidate domains doesn't spam the log
        # with one warning per dead site. Callers hitting this at volume
        # report a one-line summary count instead — see fetch_yc_slugs.
        _robots_check_stats["unreachable"] += 1
        log.debug(f"robots.txt check failed for {base_url}: {e} — treating as disallowed")
        return False


def _cc_pattern_to_wayback_query(pattern: str) -> dict:
    """Translate one CC_PLATFORM_PATTERNS glob (written for Common Crawl's
    own `url_pattern` REST param) into CDX API query params.

    - A leading "*.domain..." wildcard (e.g. "*.bamboohr.com/careers*")
      becomes CDX's matchType=domain against just the host part — CDX's
      domain match already covers every subdomain AND every path, so the
      path-segment specificity Common Crawl's own pattern carries doesn't
      translate 1:1; this costs nothing extra since every CC_EXTRACTORS
      resolver already filters non-matching URLs itself (same reasoning
      already applied to the broad "*.avature.net/*" CC pattern).
    - Anything else (a fixed host with a trailing "*", e.g.
      "boards.greenhouse.io/*") is passed straight through as CDX's `url`
      param — CDX already treats a trailing "*" as an implicit prefix
      match with no matchType needed, exactly like the original ADP-only
      code relied on before this generalization.
    """
    if pattern.startswith("*."):
        host_and_rest = pattern[2:]
        host = host_and_rest.split("/", 1)[0]
        return {"url": host, "matchType": "domain"}
    return {"url": pattern}


def fetch_wayback_slugs(limit_per_pattern: int = 2000) -> dict[str, set[str]]:
    """Query the Wayback Machine CDX index for archived career pages
    across every platform in CC_PLATFORM_PATTERNS (generalized 2026-09
    from the original ADP-only version — see the section header comment
    above for why one shared query-translation function plus the existing
    CC_EXTRACTORS is enough, with no new per-platform extraction code).

    `limit_per_pattern` caps rows returned per individual CDX query (there
    can be several patterns per platform) — default 2000 keeps a full run
    reasonably bounded; CDX itself supports paging beyond this via its own
    `resumeKey`, not used here since this is a supplemental source, not a
    primary bulk one.
    """
    slugs_by_ats: dict[str, set[str]] = {ats: set() for ats in CC_PLATFORM_PATTERNS}

    if not _robots_allows("https://web.archive.org", "/cdx/"):
        log.warning("Wayback CDX: /cdx/ disallowed by web.archive.org/robots.txt "
                     "(or robots.txt unreachable) — skipping Wayback discovery "
                     "for all platforms.")
        return slugs_by_ats

    for ats, patterns in CC_PLATFORM_PATTERNS.items():
        extractor = CC_EXTRACTORS.get(ats)
        if extractor is None:
            continue  # kept in sync with CC_EXTRACTORS, same invariant as Common Crawl above
        for pattern in patterns:
            query = _cc_pattern_to_wayback_query(pattern)
            query.update({
                "output": "json",
                "fl": "original",
                "collapse": "urlkey",
                "limit": limit_per_pattern,
            })
            log.info(f"Wayback CDX: querying archived snapshots for {ats} ({pattern})")
            try:
                r = requests.get(WAYBACK_CDX_URL, params=query, timeout=60,
                                  headers={"User-Agent": _ROBOTS_UA})
                r.raise_for_status()
                rows = r.json()
            except Exception as e:
                log.warning(f"Wayback CDX query failed for {ats} ({pattern}): {e}")
                time.sleep(0.5)
                continue

            # First row is the column header (["original"]) when output=json
            urls = [row[0] for row in rows[1:]] if rows and isinstance(rows, list) else []
            log.info(f"  Wayback CDX: {len(urls)} archived snapshot URLs")

            for url in urls:
                slug = extractor(url)
                if slug:
                    slugs_by_ats[ats].add(slug)
            time.sleep(0.5)  # stay polite to web.archive.org across many queries

        if slugs_by_ats[ats]:
            log.info(f"  {ats}: {len(slugs_by_ats[ats])} companies from Wayback Machine")

    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 6: Y Combinator (yc-oss/api)
# ══════════════════════════════════════════════════════════
#
# Unlike the other sources, this one doesn't come as a pre-built list of
# ATS URLs — yc-oss/api just gives company names + their own websites.
# So the discovery step here is genuinely different: fetch each company's
# homepage, look for a link to a known ATS domain (either right on the
# homepage nav/footer, or one hop through whatever page looks like their
# careers page), and run that URL through the SAME per-platform resolvers
# every other source already uses (URL_TO_SLUG). This is why it's worth
# doing despite the extra work: YC's cohort skews toward exactly the
# modern ATS platforms this project already scrapes well (Greenhouse,
# Lever, Ashby, Rippling), but skews toward companies too new or small to
# have shown up in the bigger static dumps (Feashliaa/kalil/OpenPostings)
# yet — so it's net-new companies, not just the same ones again.

YC_USER_AGENT = _ROBOTS_UA
_YC_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.I)
_YC_CAREER_LINK_RE = re.compile(
    r"\b(careers?|jobs?|join[\s\-]?us|we[\s\-]?re[\s\-]?hiring|work[\s\-]?with[\s\-]?us)\b",
    re.I,
)


def fetch_yc_companies() -> list[dict]:
    """Download the full YC company list (~6k companies, free, no auth)."""
    try:
        r = requests.get(YC_ALL_COMPANIES_URL, timeout=60,
                          headers={"User-Agent": YC_USER_AGENT})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"Failed to fetch YC company list: {e}")
        return []


def _scan_html_for_ats_slug(html: str, base_url: str) -> tuple[str, str] | None:
    """Scan every href in `html` (resolved to absolute against `base_url`)
    against all known ATS URL patterns. Returns (ats, slug) on first hit."""
    for href in _YC_HREF_RE.findall(html):
        try:
            absolute = urljoin(base_url, href)
        except Exception:
            continue
        for ats, resolver in URL_TO_SLUG.items():
            slug = resolver(absolute)
            if slug:
                return ats, slug
    return None


def _find_career_page_link(html: str, base_url: str) -> str | None:
    """Find the first link on the page that looks like a careers page."""
    for href in _YC_HREF_RE.findall(html):
        if _YC_CAREER_LINK_RE.search(href):
            try:
                return urljoin(base_url, href)
            except Exception:
                continue
    return None


def resolve_company_to_ats_slug(website: str, timeout: int = 15) -> tuple[str, str] | None:
    """Given a company's own homepage URL, try to find which ATS it uses
    and that ATS's slug for it. Checks robots.txt before fetching each
    distinct domain touched (homepage, and the careers page if different).
    Returns (ats, slug) or None if nothing was found / not allowed."""
    parsed = urlparse(website if "://" in website else f"https://{website}")
    if not parsed.hostname:
        return None
    base = f"{parsed.scheme}://{parsed.hostname}"

    if not _robots_allows(base, "/"):
        return None

    try:
        r = requests.get(base, timeout=timeout,
                          headers={"User-Agent": YC_USER_AGENT})
        if r.status_code >= 400:
            return None
        html = r.text
    except Exception:
        return None

    # Most modern startups link straight to their ATS from the homepage
    # nav/footer — check that first, no second fetch needed.
    hit = _scan_html_for_ats_slug(html, base)
    if hit:
        return hit

    # Otherwise, follow one hop to whatever looks like a careers page and
    # check again there.
    career_url = _find_career_page_link(html, base)
    if not career_url:
        return None

    career_parsed = urlparse(career_url)
    career_base = f"{career_parsed.scheme}://{career_parsed.hostname}"
    if career_base != base and not _robots_allows(career_base, career_parsed.path or "/"):
        return None

    try:
        r2 = requests.get(career_url, timeout=timeout,
                           headers={"User-Agent": YC_USER_AGENT})
        if r2.status_code >= 400:
            return None
        return _scan_html_for_ats_slug(r2.text, career_url)
    except Exception:
        return None


def fetch_yc_slugs(limit: int = 2000, max_workers: int = 15) -> dict[str, dict[str, str]]:
    """Resolve YC companies' own websites to an ATS slug where possible.

    `limit` caps how many companies are attempted per run (default 2000,
    not the full ~6k) — this source does 1-2 live HTTP fetches PER
    COMPANY (unlike the other sources, which are single bulk downloads),
    so it's meaningfully heavier; capping keeps a single weekly run's
    wall-clock and request volume reasonable. Pass 0 for no cap. Runs are
    idempotent (on_conflict upsert), so a rolling subset across multiple
    weekly runs still converges on full coverage over time.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    all_companies = fetch_yc_companies()
    companies = all_companies[:limit] if limit else all_companies
    log.info(f"Y Combinator: resolving ATS slug for {len(companies)} of "
             f"{len(all_companies)} total companies...")

    _robots_check_stats["unreachable"] = 0
    _robots_check_stats["disallowed_by_rule"] = 0

    slugs_by_ats: dict[str, dict[str, str]] = {}
    resolved = 0

    def _resolve_one(company):
        website = company.get("website", "")
        if not website:
            return None
        result = resolve_company_to_ats_slug(website)
        time.sleep(0.1)  # light rate-limit courtesy across ~6k distinct domains
        if result:
            return company.get("name", ""), result
        return None

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_resolve_one, c): c for c in companies}
        for i, future in enumerate(as_completed(futures), 1):
            try:
                res = future.result()
            except Exception:
                res = None
            if res:
                name, (ats, slug) = res
                slugs_by_ats.setdefault(ats, {})[slug] = name
                resolved += 1
            if i % 500 == 0:
                log.info(f"  ...{i}/{len(companies)} checked, {resolved} resolved so far")

    for ats, slugs in slugs_by_ats.items():
        log.info(f"  {ats}: {len(slugs)} companies from Y Combinator")

    skipped = len(companies) - resolved
    log.info(f"  Y Combinator summary: {resolved} resolved, {skipped} skipped "
             f"({_robots_check_stats['unreachable']} unreachable sites, "
             f"{_robots_check_stats['disallowed_by_rule']} disallowed by "
             f"robots.txt, rest had no detectable ATS link) — run with "
             f"-v/--verbose for the per-site detail.")

    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 7: HTTP Archive (public BigQuery — Wappalyzer detection at scale)
# ══════════════════════════════════════════════════════════
#
# This is a fundamentally different kind of source from everything above:
# it's real technology-FINGERPRINT detection (script-src, DOM, JS globals
# — the same method commercial "companies using X" trackers are built on)
# run by Google/HTTP Archive against millions of crawled URLs every
# month, rather than us following literal <a href> links ourselves. That
# matters because some ATS integrations are embedded via a pure JS widget
# with no visible link at all (this project already hit exactly that
# problem with ADP — see the Wayback Machine source above) — a
# fingerprint-based detector catches those where link-following can't.
#
# What comes back from this query is the COMPANY'S OWN page where the
# technology was detected (e.g. https://acme.com), not necessarily the
# ATS's own URL (e.g. boards.greenhouse.io/acme) — Wappalyzer flags a
# page because it embeds a matching script/DOM pattern, which usually
# means the company's careers page links to or embeds the ATS, but the
# literal ATS URL/slug still needs to be extracted. So rather than
# building a second URL-to-slug pipeline, this reuses the exact same
# resolve_company_to_ats_slug() written for the Y Combinator source
# (fetch the page, scan its links against every URL_TO_SLUG resolver,
# follow one hop to a careers-page link if nothing's found directly) —
# HTTP Archive is really just a much bigger, pre-filtered candidate list
# of "pages that likely link to one of our ATS platforms" than YC's
# company list is.

def fetch_httparchive_candidate_urls(limit_per_tech: int = 2000,
                                      months: int = 6) -> dict[str, list[str]]:
    """Query HTTP Archive's public BigQuery dataset for pages where a
    known ATS technology was detected. Returns {ats: [urls]}, ranked by
    CrUX popularity (most popular/reliable first) within limit_per_tech.

    Widened 2026-08 from a single-month/desktop-only query (limit_per_tech
    200) to querying the last `months` monthly crawl partitions AND both
    `desktop`+`mobile` clients, unioned and deduped — this is the real,
    free way to raise HTTP Archive's ceiling for this project, as opposed
    to just bumping one number. HTTP Archive publishes a full crawl every
    month, and a nontrivial number of sites are crawled successfully on
    one client/month but not another (transient fetch failures, mobile-vs-
    desktop rendering differences that change what Wappalyzer detects) —
    so scanning N months x 2 clients surfaces real additional companies
    that a single-snapshot query structurally cannot see, not just a
    higher score against the same pool. Still cheap: ~1-2GB per
    month/client scanned against BigQuery Sandbox's 1TB/month free
    allowance, so even months=6 (12 scans) is well under 1% of quota.

    NOTE: this does NOT change what HTTP Archive's crawl universe covers
    in the first place (Chrome UX Report's popular-site list) — it only
    recovers the extra names that ARE in that universe but were missed by
    querying just one month/client. A site too low-traffic for CrUX to
    ever crawl still won't appear here no matter how wide this query gets;
    see the module docstring for why this source is a supplemental
    trickle, not a bulk source like Feashliaa/OpenPostings/Common Crawl.

    Requires `pip install google-cloud-bigquery` and a GCP project with
    BigQuery enabled (GCP_PROJECT_ID env var + standard Google
    Application Default Credentials, e.g. GOOGLE_APPLICATION_CREDENTIALS
    pointing at a service account key). Returns {} and logs a one-line
    notice — never raises — if either isn't available, same pattern as
    every other optional source in this file.
    """
    try:
        from google.cloud import bigquery
    except ImportError:
        log.info("HTTP Archive: google-cloud-bigquery not installed — skipping "
                 "(pip install google-cloud-bigquery to enable this source).")
        return {}

    if not HTTPARCHIVE_GCP_PROJECT:
        log.info("HTTP Archive: GCP_PROJECT_ID not set — skipping "
                 "(needs a free Google Cloud project with BigQuery enabled).")
        return {}

    try:
        client = bigquery.Client(project=HTTPARCHIVE_GCP_PROJECT)
    except Exception as e:
        log.warning(f"HTTP Archive: couldn't create BigQuery client "
                    f"(check GOOGLE_APPLICATION_CREDENTIALS): {e}")
        return {}

    # Find the available monthly crawl partitions first — hardcoding dates
    # would silently go stale as new crawls land / old ones age out.
    try:
        date_rows = list(client.query(
            "SELECT DISTINCT date FROM `httparchive.crawl.pages` "
            "WHERE date > DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH) "
            "ORDER BY date DESC "
            "LIMIT @months",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("months", "INT64", months),
            ]),
        ).result())
        crawl_dates = [r.date for r in date_rows]
    except Exception as e:
        log.warning(f"HTTP Archive: failed to find recent crawl dates: {e}")
        return {}

    if not crawl_dates:
        log.warning("HTTP Archive: no recent crawl partitions found — skipping.")
        return {}

    log.info(f"HTTP Archive: querying {len(crawl_dates)} crawl(s) "
             f"({crawl_dates[-1]} .. {crawl_dates[0]}), both desktop+mobile, "
             f"for {len(HTTPARCHIVE_ATS_TECH_NAMES)} known ATS fingerprints...")

    urls_by_ats: dict[str, list[str]] = {}
    tech_to_ats = {v: k for k, v in HTTPARCHIVE_ATS_TECH_NAMES.items()}

    # NOTE 2026-08: `technologies` is UNNESTed into a STRUCT whose field is
    # named `technology` (STRUCT<technology STRING, categories ARRAY<STRING>,
    # info ARRAY<STRING>>) — NOT `name`. Confirmed live via BigQuery's own
    # error message after the original `tech.name` version 400'd with
    # "Field name name does not exist in STRUCT<technology STRING, ...>".
    #
    # `date IN UNNEST(@crawl_dates)` (instead of a single `date = @date`)
    # plus dropping the `client = 'desktop'` filter is the actual widening
    # — QUALIFY still caps each tech to its top limit_per_tech rows overall
    # (by rank, so the best/most-popular pages win regardless of which
    # month/client they came from), and DISTINCT page in the outer query
    # dedupes a site that shows up in more than one month/client.
    query = """
        SELECT DISTINCT tech_name, page
        FROM (
            SELECT tech.technology AS tech_name, page, rank
            FROM `httparchive.crawl.pages`,
            UNNEST(technologies) AS tech
            WHERE date IN UNNEST(@crawl_dates)
              AND tech.technology IN UNNEST(@tech_names)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY tech.technology ORDER BY rank ASC
            ) <= @limit_per_tech
        )
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("crawl_dates", "DATE", crawl_dates),
        bigquery.ArrayQueryParameter("tech_names", "STRING",
                                       list(HTTPARCHIVE_ATS_TECH_NAMES.values())),
        bigquery.ScalarQueryParameter("limit_per_tech", "INT64", limit_per_tech),
    ])

    try:
        rows = list(client.query(query, job_config=job_config).result())
    except Exception as e:
        log.warning(f"HTTP Archive: query failed: {e}")
        return {}

    for row in rows:
        ats = tech_to_ats.get(row.tech_name)
        if ats and row.page:
            urls_by_ats.setdefault(ats, []).append(row.page)

    for ats, urls in urls_by_ats.items():
        log.info(f"  {ats}: {len(urls)} candidate pages from HTTP Archive")

    return urls_by_ats


def fetch_httparchive_slugs(limit_per_tech: int = 2000, months: int = 6,
                             max_workers: int = 30) -> dict[str, dict[str, str]]:
    """Resolve HTTP Archive's candidate pages to real ATS slugs, reusing
    the exact same resolver built for the Y Combinator source.

    max_workers raised 15->30 alongside the higher default limit_per_tech
    (200->2000) and months (1->6) — those changes can produce roughly
    10x+ as many candidate pages to resolve per run, so resolve
    concurrency needs to scale with it or a run's wall-clock would blow up
    proportionally. Each resolve is 1-2 lightweight HTTP fetches, so this
    is still well within what a single GitHub Actions runner handles fine.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    urls_by_ats = fetch_httparchive_candidate_urls(limit_per_tech, months)
    if not urls_by_ats:
        return {}

    all_urls = [(ats, url) for ats, urls in urls_by_ats.items() for url in urls]
    log.info(f"HTTP Archive: resolving {len(all_urls)} candidate pages...")

    _robots_check_stats["unreachable"] = 0
    _robots_check_stats["disallowed_by_rule"] = 0

    slugs_by_ats: dict[str, dict[str, str]] = {}
    resolved = 0

    def _resolve_one(item):
        expected_ats, url = item
        result = resolve_company_to_ats_slug(url)
        time.sleep(0.1)
        return expected_ats, result

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_resolve_one, item) for item in all_urls]
        for future in as_completed(futures):
            try:
                expected_ats, result = future.result()
            except Exception:
                continue
            if result:
                actual_ats, slug = result
                # Trust what we actually found on the page over what the
                # fingerprint hinted at — a company page can legitimately
                # link to a DIFFERENT ATS than the one HTTP Archive
                # flagged (e.g. a stale/removed integration, or Wappalyzer
                # matching a leftover script tag), so this still counts,
                # just filed under the platform actually confirmed.
                slugs_by_ats.setdefault(actual_ats, {})[slug] = ""
                resolved += 1

    log.info(f"HTTP Archive: resolved {resolved}/{len(all_urls)} candidate pages")
    for ats, slugs in slugs_by_ats.items():
        log.info(f"  {ats}: {len(slugs)} companies from HTTP Archive")

    skipped = len(all_urls) - resolved
    log.info(f"  HTTP Archive summary: {resolved} resolved, {skipped} skipped "
             f"({_robots_check_stats['unreachable']} unreachable sites, "
             f"{_robots_check_stats['disallowed_by_rule']} disallowed by "
             f"robots.txt, rest had no detectable ATS link) — run with "
             f"-v/--verbose for the per-site detail.")

    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SUPABASE UPSERT
# ══════════════════════════════════════════════════════════

def _oracle_tenant(slug: str) -> str:
    """Extract the bare tenant name from an oracle_cloud_hcm slug, resolved
    or not. 'eeho|CX_1' -> 'eeho'; 'eeho.fa.us2|CX_1' -> 'eeho'; 'eeho' -> 'eeho'."""
    host_prefix = slug.split("|", 1)[0]
    return host_prefix.split(".", 1)[0]


def _is_resolved_oracle_slug(slug: str) -> bool:
    """True if the slug already carries a discovered '.fa.<region>' domain."""
    host_prefix = slug.split("|", 1)[0]
    return ".fa." in host_prefix


def _fetch_resolved_oracle_tenants() -> set[str]:
    """
    Tenants that already have a resolved oracle_cloud_hcm slug in
    archive_i (e.g. 'eeho.fa.us2|CX_1' -> tenant 'eeho').

    scrape_oracle_cloud_hcm() persists the resolved slug once it discovers a
    legacy tenant's real domain (see supabase_handler.resolve_oracle_slug).
    Sources like OpenPostings/Common Crawl only ever know the legacy,
    unresolved tenant name — without this check, upserting them here would
    re-add the legacy slug next to its resolved twin every week, and the
    scraper would burn an 11-region brute-force discovery on it all over
    again on Monday. See _filter_oracle_slugs below.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return set()

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    tenants = set()
    offset = 0
    batch_size = 1000
    try:
        while True:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/archive_i",
                headers=headers,
                timeout=30,
                params={
                    "select": "slug",
                    "ats": "eq.oracle_cloud_hcm",
                    "offset": offset,
                    "limit": batch_size,
                },
            )
            r.raise_for_status()
            rows = r.json()
            for row in rows:
                slug = row.get("slug", "")
                if _is_resolved_oracle_slug(slug):
                    tenants.add(_oracle_tenant(slug))
            if len(rows) < batch_size:
                break
            offset += batch_size
    except Exception as e:
        log.error(f"Failed to fetch existing oracle_cloud_hcm slugs for de-dup check: {e}")
        return set()

    return tenants


def _filter_oracle_slugs(slug_dict: dict[str, str]) -> dict[str, str]:
    """
    Drop legacy (unresolved) oracle_cloud_hcm slugs whose tenant already has
    a resolved counterpart in archive_i, so re-enrichment never
    re-introduces a duplicate that would trigger discovery all over again.
    Already-resolved slugs in slug_dict (rare, but possible if a source
    somehow captured one) pass through untouched.
    """
    resolved_tenants = _fetch_resolved_oracle_tenants()
    if not resolved_tenants:
        return slug_dict

    filtered = {}
    skipped = 0
    for slug, name in slug_dict.items():
        if not _is_resolved_oracle_slug(slug) and _oracle_tenant(slug) in resolved_tenants:
            skipped += 1
            continue
        filtered[slug] = name

    if skipped:
        log.info(f"  oracle_cloud_hcm: skipped {skipped} legacy slugs already resolved in archive_i")

    return filtered


def upsert_to_supabase(slugs_by_ats: dict[str, set | dict], source: str,
                        dry_run: bool = False) -> int:
    """Upsert slugs to Supabase archive_i. Returns total upserted.

    slugs_by_ats values can be:
      - set[str]          → slugs only (no company name)
      - dict[str, str]    → {slug: company_name}
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL or SUPABASE_KEY not set")
        return 0

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }

    total = 0
    chunk_size = 500

    for ats, slugs in slugs_by_ats.items():
        if not slugs:
            continue

        # Normalize: set → dict with empty names, dict stays as-is
        if isinstance(slugs, set):
            slug_dict = {s: "" for s in slugs}
        else:
            slug_dict = slugs

        # Oracle Cloud HCM: don't re-add a legacy tenant slug that's already
        # been resolved to its real domain — see _filter_oracle_slugs.
        if ats == "oracle_cloud_hcm" and not dry_run:
            slug_dict = _filter_oracle_slugs(slug_dict)
            if not slug_dict:
                continue

        items = list(slug_dict.items())
        ats_total = 0

        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]
            rows = []
            for slug, name in chunk:
                # 2026-09: archive_i (this table, formerly named
                # slug_registry — this upsert was silently 404ing every
                # run against the old name until this fix) has no "name"/
                # company_name column: id, ats, slug, source, first_seen,
                # last_seen only. `name` is still accepted as an input
                # here (some sources hand us {slug: company_name}) purely
                # because a few callers rely on that shape elsewhere —
                # it's just never written to Supabase.
                row = {"ats": ats, "slug": slug, "source": source}
                rows.append(row)

            if dry_run:
                ats_total += len(chunk)
                continue

            r = None
            try:
                r = requests.post(
                    f"{SUPABASE_URL}/rest/v1/archive_i",
                    headers=headers,
                    json=rows,
                    timeout=60,
                    params={"on_conflict": "ats,slug"},
                )
                r.raise_for_status()
                ats_total += len(chunk)
            except Exception as e:
                # requests' own exception message ("400 Client Error: Bad
                # Request for url: ...") never includes PostgREST's actual
                # reason (e.g. a CHECK constraint violation) — without the
                # response body, a genuine schema mismatch looks identical
                # to a transient network blip. Always log it when we have it.
                body = f" — response: {r.text[:500]}" if r is not None else ""
                log.error(f"Supabase upsert failed for {ats}: {e}{body}")

        if ats_total:
            log.info(f"  {ats}: upserted {ats_total} slugs ({source})")
        total += ats_total

    return total


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Discovery: populate Supabase archive_i from 7 sources"
    )
    parser.add_argument(
        "--source",
        choices=["feashliaa", "kalil", "openpostings", "commoncrawl",
                 "wayback", "yc", "httparchive", "all"],
        default="all",
        help="Which source to pull from (default: all)",
    )
    parser.add_argument(
        "--crawls", type=int, default=6,
        help="Number of Common Crawl archives to query (default: 6 — "
             "raised from 3 for deeper historical discovery)",
    )
    parser.add_argument(
        "--cc-shard", type=int, default=None,
        help="Which Common Crawl platform-shard this run covers (0-indexed, "
             "used with --cc-total-shards). Default: None = all platforms "
             "in one run. See fetch_commoncrawl_slugs docstring.",
    )
    parser.add_argument(
        "--cc-total-shards", type=int, default=1,
        help="Total number of Common Crawl platform-shards (default: 1, "
             "i.e. no sharding). Discovery.yml runs this as 4 (shards 0-3, "
             "bumped from 2 in 2026-09) as separate matrix jobs.",
    )
    parser.add_argument(
        "--yc-limit", type=int, default=2000,
        help="Max YC companies to attempt per run (default: 2000; 0 = all "
             "~6k — see fetch_yc_slugs docstring for why this is capped)",
    )
    parser.add_argument(
        "--httparchive-limit", type=int, default=2000,
        help="Max candidate pages to pull PER ATS platform from HTTP "
             "Archive's BigQuery dataset, ranked by popularity (default: "
             "2000, raised from 200 — the resolve step is 1-2 live fetches "
             "per candidate, same cost profile as --yc-limit)",
    )
    parser.add_argument(
        "--httparchive-months", type=int, default=6,
        help="Number of recent monthly HTTP Archive crawl partitions to "
             "query, unioned with both desktop+mobile clients and deduped "
             "(default: 6) — see fetch_httparchive_candidate_urls docstring "
             "for why multi-month/multi-client is the real lever for more "
             "coverage here, not just a bigger --httparchive-limit alone",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count slugs without writing to Supabase",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Log every individual robots.txt/site-fetch failure (DEBUG "
             "level) instead of just the one-line per-source summary. Off "
             "by default because most of these are just dead/unreachable "
             "company domains, not real problems — see fetch_yc_slugs.",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 60)
    log.info("DISCOVERY — Supabase as single source of truth")
    log.info("  Sources: Feashliaa + kalil0321 + OpenPostings + Common Crawl")
    log.info("           + Wayback CDX (ADP) + Y Combinator")
    log.info("           + HTTP Archive (BigQuery)")
    log.info("=" * 60)

    grand_total = 0

    # Source 1: Feashliaa (50k+ slugs for 6 platforms)
    if args.source in ("feashliaa", "all"):
        log.info("\n--- FEASHLIAA (6 platforms, 50k+ slugs) ---")
        fa_slugs = fetch_feashliaa_slugs()
        fa_total = sum(len(s) for s in fa_slugs.values())

        if not args.dry_run:
            upserted = upsert_to_supabase(fa_slugs, source="feashliaa",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += fa_total

    # Source 2: kalil0321/ats-scrapers (15 platforms, CSV inventories)
    if args.source in ("kalil", "all"):
        log.info("\n--- KALIL0321 (15 platforms, CSV inventories) ---")
        ka_slugs = fetch_kalil_slugs()
        ka_total = sum(len(s) for s in ka_slugs.values())

        if not args.dry_run:
            upserted = upsert_to_supabase(ka_slugs, source="kalil",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += ka_total

    # Source 3: OpenPostings (110k+ companies across 80+ ATSs)
    if args.source in ("openpostings", "all"):
        log.info("\n--- OPENPOSTINGS (110k+ companies) ---")
        op_slugs = fetch_openpostings_slugs()
        op_total = sum(len(s) for s in op_slugs.values())
        log.info(f"OpenPostings total: {op_total} slugs across "
                 f"{sum(1 for s in op_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(op_slugs, source="openpostings",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += op_total

    # Source 4: Common Crawl (ongoing discovery for 26 platforms — run as
    # 2 shards in discovery.yml, see fetch_commoncrawl_slugs docstring)
    if args.source in ("commoncrawl", "all"):
        log.info("\n--- COMMON CRAWL (ongoing discovery) ---")
        cc_slugs = fetch_commoncrawl_slugs(args.crawls, cc_shard=args.cc_shard,
                                            cc_total_shards=args.cc_total_shards)
        cc_total = sum(len(s) for s in cc_slugs.values())
        log.info(f"Common Crawl total: {cc_total} slugs across "
                 f"{sum(1 for s in cc_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(cc_slugs, source="commoncrawl",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += cc_total

    # Source 5: Wayback Machine CDX (all platforms, generalized 2026-09
    # from ADP-only — see fetch_wayback_slugs docstring, and the section
    # header comment above it for the archive_i.source label note)
    if args.source in ("wayback", "all"):
        log.info("\n--- WAYBACK MACHINE CDX (all-platform supplemental discovery) ---")
        wb_slugs = fetch_wayback_slugs()
        wb_total = sum(len(s) for s in wb_slugs.values())
        log.info(f"Wayback CDX total: {wb_total} slugs across "
                 f"{sum(1 for s in wb_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(wb_slugs, source="wayback",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += wb_total

    # Source 6: Y Combinator (net-new companies too small/new for the
    # bulk dumps above — see fetch_yc_slugs docstring)
    if args.source in ("yc", "all"):
        log.info("\n--- Y COMBINATOR (own-website ATS discovery) ---")
        yc_slugs = fetch_yc_slugs(limit=args.yc_limit)
        yc_total = sum(len(s) for s in yc_slugs.values())
        log.info(f"Y Combinator total: {yc_total} slugs across "
                 f"{sum(1 for s in yc_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(yc_slugs, source="yc",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += yc_total

    # Source 7: HTTP Archive (BigQuery — real technology-fingerprint
    # detection at scale, see fetch_httparchive_slugs docstring)
    if args.source in ("httparchive", "all"):
        log.info("\n--- HTTP ARCHIVE (BigQuery, technology-fingerprint detection) ---")
        ha_slugs = fetch_httparchive_slugs(limit_per_tech=args.httparchive_limit,
                                            months=args.httparchive_months)
        ha_total = sum(len(s) for s in ha_slugs.values())
        if ha_total:
            log.info(f"HTTP Archive total: {ha_total} slugs across "
                     f"{sum(1 for s in ha_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(ha_slugs, source="httparchive",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += ha_total

    action = "would upsert" if args.dry_run else "upserted"
    log.info(f"\nDone! {action} {grand_total} total slugs to Supabase.")


if __name__ == "__main__":
    main()
