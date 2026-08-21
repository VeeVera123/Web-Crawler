"""
Slug Enrichment — Supabase as Single Source of Truth
=====================================================
Pulls company slugs from multiple sources and upserts them
into the Supabase slug_registry table.

Sources:
  1. Feashliaa GitHub (50k+ slugs for 6 platforms — greenhouse,
     lever, ashby, bamboohr, icims, workday)
  2. kalil0321/ats-scrapers (CSV inventories for 15 platforms —
     incl. successfactors, smartrecruiters, workable)
  3. OpenPostings jobs.db (110k+ companies across 80+ ATSs)
  4. Common Crawl index (ongoing discovery for 15 platforms)
  5. Wayback Machine CDX (ADP-only supplemental discovery)
  6. Y Combinator (yc-oss/api — ~6k companies, free, no auth. Each
     company's own website is fetched and scanned for a link to a
     known ATS domain — this is net-new discovery, not just another
     dump of the same companies the other sources already have,
     since YC startups skew toward exactly the modern ATS platforms
     — Greenhouse/Lever/Ashby/Rippling — this project already covers
     well, just companies too new/small to be in the bigger dumps yet)
  7. TheirStack (freemium technology-usage API — 50 company credits/
     month on the free tier, so this is a small monthly trickle for
     gap-filling thin platforms, not a bulk source. Needs a free
     THEIRSTACK_API_KEY — sign up at theirstack.com, no credit card)
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

Runs weekly (Sunday) via GitHub Actions, as an 8-way matrix — one job per
source, all in parallel (see .github/workflows/discovery.yml) — rather
than one job running all 8 back-to-back. Each source is already an
independent fetch-and-resolve pass with its own cost profile (bulk single
download vs. thousands of live per-company fetches), so sharding by
source is the natural split here — there's no single flat pool of "work
items" to hash-shard the way main.py splits ATS boards across its matrix.

The daily scanner reads from Supabase slug_registry — no local .txt files
needed.

Usage:
    python discovery.py                        # full enrichment (all sources, sequential)
    python discovery.py --source feashliaa     # Feashliaa only
    python discovery.py --source kalil         # kalil0321 only
    python discovery.py --source openpostings  # OpenPostings only
    python discovery.py --source commoncrawl   # Common Crawl only
    python discovery.py --source wayback_adp   # Wayback CDX (ADP) only
    python discovery.py --source yc            # Y Combinator only
    python discovery.py --source theirstack    # TheirStack only
    python discovery.py --source httparchive   # HTTP Archive (BigQuery) only
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

# TheirStack (freemium technology-usage API)
THEIRSTACK_API_URL = "https://api.theirstack.com/v1/companies/search"
THEIRSTACK_API_KEY = os.environ.get("THEIRSTACK_API_KEY", "")
# Free tier: 50 company credits/month, 200 API credits/month, 2 req/sec,
# max 5 pages x 25 results per search. Deliberately spent on the thinner,
# newer platforms (poorly covered by the bulk dumps above) rather than
# Greenhouse/Lever/Workday, which are already well covered elsewhere —
# no point burning a scarce monthly budget on companies we likely already
# have. NOTE: these are OUR internal ATS keys on the left; the right side
# is TheirStack's own technology slug — VERIFIED live (2026-08) by fetching
# each https://theirstack.com/en/technology/{slug} page directly and
# confirming it 200s with a real company count (shown in the comment).
# These counts are TheirStack's own tracked totals (their site, not ours)
# — useful context for how much this source can realistically add, but
# note our free-tier budget (40/run, ~50/month) only pulls a small slice
# of each, and every count below almost certainly includes companies we
# already have from other sources — see the response this was added in
# reply to for why these are gap-filling, not the primary source.
THEIRSTACK_ATS_SLUGS = {
    "softgarden": "softgarden",       # verified: 10,805 companies tracked
    "eploy": "eploy",                 # verified: 209 companies tracked
    "jobadder": "jobadder",           # verified: 393 companies tracked
    "jobvite": "jobvite",             # verified: 4,832 companies tracked
    "avature": "avature",             # verified: 3,217 companies tracked
    "hrmdirect": "clearcompany",      # verified: 736 (HRMDirect rebranded to ClearCompany)
    "paylocity": "paylocity",         # verified: 60,976 companies tracked
    "zoho": "zoho-recruit",           # verified: 4,766 companies tracked
    # "folkshr" deliberately omitted: neither "folks-hr" nor "folkshr"
    # resolves on TheirStack (both confirmed 404 live) — they don't appear
    # to track this platform at all (FolksHR/Glow Talents is a small,
    # UK/Ireland-focused ATS). Not worth spending a query on a guaranteed
    # empty result every run.
}

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

# ATS platforms we have working scrapers for (21 active)
SUPPORTED_ATS = {
    "greenhouse", "lever", "ashby", "bamboohr", "icims", "workday",
    "rippling", "workable", "recruitee", "smartrecruiters",
    "teamtailor", "breezyhr", "applytojob", "personio", "joincom",
    # Newly enabled (confirmed working via test_blacklisted_ats.py):
    "taleo", "oracle_cloud_hcm", "paylocity", "hrmdirect", "zoho",
    # Fixed (2026-08) — was blacklisted with wrong URL/API assumptions,
    # now scrapes correctly (see ats_scrapers.py):
    "softgarden",
}

# Eploy / Folks HR / JobAdder / Jobvite / ADP / Avature (added 2026-08) are
# NOT in SUPPORTED_ATS yet: none of them appear in the OpenPostings dataset
# this file enriches from, and JobAdder/ADP additionally need composite
# slugs (client_id|board, cid|ccId) that a single URL has no way to fully
# encode. Their slugs currently have to be added to slug_registry by hand
# (or via discover_slugs.py, if/when Common Crawl query patterns are added
# for them) — they scrape fine once a slug row exists, this file just
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
    "applytojob": "applytojob",
    "apply to job": "applytojob",
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
}


# ══════════════════════════════════════════════════════════
# URL → SLUG CONVERTERS (OpenPostings stores full URLs)
# ══════════════════════════════════════════════════════════

def _url_to_slug_greenhouse(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "greenhouse.io" in host:
        parts = parsed.path.strip("/").split("/")
        slug = parts[0] if parts else None
        if slug and slug.lower() not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_lever(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    # Match lever.co and jobs.lever.co
    if "lever.co" in host:
        parts = parsed.path.strip("/").split("/")
        slug = parts[0] if parts else None
        if slug and slug.lower() not in SKIP_SLUGS:
            return slug
    # Some OpenPostings URLs use levergreen or leverjobs subdomains
    if "lever" in host and ".co" in host:
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


def _url_to_slug_workday(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "myworkdayjobs.com" in host:
        # Pattern: {company}.wd{N}.myworkdayjobs.com/{site_id}
        parts = host.split(".")
        company = parts[0]
        wd = parts[1] if len(parts) > 1 else ""
        path_parts = parsed.path.strip("/").split("/")
        site_id = path_parts[0] if path_parts and path_parts[0] else ""
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


def _url_to_slug_workable(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "workable.com" in host:
        parts = parsed.path.strip("/").split("/")
        # apply.workable.com/{slug}
        if host == "apply.workable.com" and parts:
            slug = parts[0].lower()
            if slug and slug not in SKIP_SLUGS:
                return slug
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
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "smartrecruiters.com" in host:
        # Pattern: jobs.smartrecruiters.com/{CompanyName} or
        # careers.smartrecruiters.com/{CompanyName}
        parts = parsed.path.strip("/").split("/")
        if parts and parts[0]:
            slug = parts[0]
            if slug.lower() not in SKIP_SLUGS and slug.lower() not in ("jobs", "careers", "posting"):
                return slug
        # Fallback: subdomain-based {company}.smartrecruiters.com
        sub = host.replace(".smartrecruiters.com", "").lower()
        if sub and sub not in SKIP_SLUGS and sub not in ("jobs", "careers", "www"):
            return sub
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
    e.g. 'eeho.fa.us2|CX_1'"""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "oraclecloud.com" not in host:
        return None
    # Extract full host prefix (e.g. 'eeho.fa.us2' from 'eeho.fa.us2.oraclecloud.com')
    host_prefix = host.replace(".oraclecloud.com", "").lower()
    if not host_prefix or host_prefix in SKIP_SLUGS:
        return None
    # Extract site from /sites/{id} in path
    site_match = re.search(r"/sites/([^/]+)", parsed.path)
    if site_match:
        return f"{host_prefix}|{site_match.group(1)}"
    # Fallback: host prefix only (site can be discovered later)
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
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "teamtailor.com" in host:
        slug = host.replace(".teamtailor.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug not in ("www", "app"):
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
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "softgarden.io" in host:
        slug = host.replace(".softgarden.io", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    # Also handle api.softgarden.io/api/.../jobboards/{channelId}
    if "softgarden" in host:
        path_match = re.search(r"/jobboards/([^/]+)", parsed.path)
        if path_match:
            return path_match.group(1)
    return None


def _url_to_slug_zoho(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "zohorecruit.com" in host:
        slug = host.replace(".zohorecruit.com", "").lower()
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
    # Need at least: recruiting/jobs/All/{id}/{name}
    if len(parts) >= 5 and parts[0] == "recruiting" and parts[1] == "jobs":
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
        # Pattern: /{slug} (direct slug in path)
        elif len(parts) >= 1 and parts[0]:
            slug = parts[0]
            if slug.lower() not in SKIP_SLUGS and slug not in ("jobs", "about", "faq"):
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
    board, "careers/" literal prefix) — both resolve to the same slug."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "jobvite.com" not in host:
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
    Pattern: {subdomain}.avature.net/careers/... (locale prefix varies,
    e.g. /en_US/careers/... — the subdomain alone is the slug)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "avature.net" not in host:
        return None
    slug = host.replace(".avature.net", "").lower()
    if slug and slug not in SKIP_SLUGS and slug != "www":
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
    "applytojob": _url_to_slug_applytojob,
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
    "workable": ["apply.workable.com/*"],
    "recruitee": ["*.recruitee.com/api/offers*", "*.recruitee.com/o/*"],
    "smartrecruiters": ["jobs.smartrecruiters.com/*", "careers.smartrecruiters.com/*"],
    "rippling": ["*.rippling.com/careers*", "*.rippling.com/jobs*"],
    "teamtailor": ["*.teamtailor.com/jobs*"],
    "breezyhr": ["*.breezy.hr/*"],
    "applytojob": ["*.applytojob.com/*"],
    "personio": ["*.jobs.personio.de/*", "*.jobs.personio.com/*"],
    "joincom": ["join.com/companies/*/jobs*", "join.com/companies/*"],
    # Newly enabled platforms:
    "taleo": ["*.taleo.net/careersection/*/jobsearch.ftl*"],
    "oracle_cloud_hcm": ["*.oraclecloud.com/hcmUI/CandidateExperience/*"],
    "paylocity": ["recruiting.paylocity.com/recruiting/jobs/*"],
    "hrmdirect": ["*.hrmdirect.com/employment/*"],
    "zoho": ["*.zohorecruit.com/jobs/*"],
    "softgarden": ["*.softgarden.io/en/vacancies*", "*.softgarden.io/vacancies*", "*.softgarden.io/job/*"],
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
    "avature": ["*.avature.net/careers/*"],
}

# Reuse URL_TO_SLUG converters for Common Crawl extraction
CC_EXTRACTORS = {
    "workable": _url_to_slug_workable,
    "recruitee": _url_to_slug_recruitee,
    "smartrecruiters": _url_to_slug_smartrecruiters,
    "rippling": _url_to_slug_rippling,
    "teamtailor": _url_to_slug_teamtailor,
    "breezyhr": _url_to_slug_breezyhr,
    "applytojob": _url_to_slug_applytojob,
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


def fetch_commoncrawl_slugs(n_crawls: int = 3) -> dict[str, set[str]]:
    """Discover slugs from Common Crawl for platforms not well-covered
    by OpenPostings."""
    slugs_by_ats: dict[str, set[str]] = {ats: set() for ats in CC_PLATFORM_PATTERNS}

    crawl_ids = get_latest_crawl_ids(n_crawls)
    if not crawl_ids:
        return slugs_by_ats

    log.info(f"Common Crawl: querying {len(crawl_ids)} crawls")

    for ats, patterns in CC_PLATFORM_PATTERNS.items():
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
# WAYBACK MACHINE CDX — ADP-only supplemental discovery
# ══════════════════════════════════════════════════════════
#
# ADP is a bad fit for Common Crawl: most customers embed their board via
# a JS web component (<recruitment-current-openings cid=... ccid=...>)
# rather than a plain <a href>, so a link-following crawler like CC never
# sees a URL to follow. The Wayback Machine's CDX index is a different,
# broader, independently-sourced index (it also ingests URLs via Google
# Sitemaps, third-party "Save Page Now" submissions, etc.), so it can
# have snapshots of the actual workforcenow.adp.com recruitment.html
# pages themselves even when Common Crawl has none — and those URLs
# already carry cid/ccId directly in the query string, so no HTML
# fetching or parsing is needed at all, just the CDX index lookup.
#
# The CDX API (web.archive.org/cdx/search/cdx) is IA's own documented,
# public, purpose-built endpoint for exactly this kind of targeted
# URL-pattern lookup — not a scrape of a page meant for browsers — but
# per this project's non-negotiable robots.txt policy we still check
# web.archive.org/robots.txt live before every run rather than assume.

WAYBACK_CDX_URL = "http://web.archive.org/cdx/search/cdx"
_ADP_WAYBACK_PATTERNS = [
    "workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html*",
    # Legacy (deprecated 2026-06-26) family — no longer serves job content,
    # but Wayback may still have snapshots from before the sunset, and its
    # redirect chain resolves client= to a real modern cid — see
    # _url_to_slug_adp_discovery.
    "workforcenow.adp.com/jobs/apply/posting.html*",
]

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


def fetch_wayback_adp_slugs(limit: int = 5000) -> dict[str, set[str]]:
    """Query the Wayback Machine CDX index for archived ADP career pages
    (both the modern cid/ccId family and the deprecated legacy client=
    family) and extract cid|ccId slugs — modern URLs parse directly from
    the query string, legacy ones resolve via one live redirect-follow
    each (capped, see _ADP_LEGACY_RESOLVE_CAP)."""
    slugs: set[str] = set()

    if not _robots_allows("https://web.archive.org", "/cdx/"):
        log.warning("Wayback CDX: /cdx/ disallowed by web.archive.org/robots.txt "
                     "(or robots.txt unreachable) — skipping ADP Wayback discovery.")
        return {"adp": slugs}

    for pattern in _ADP_WAYBACK_PATTERNS:
        log.info(f"Wayback CDX: querying archived snapshots of {pattern}")
        try:
            r = requests.get(WAYBACK_CDX_URL, params={
                "url": pattern,
                "output": "json",
                "fl": "original",
                "collapse": "urlkey",
                "limit": limit,
            }, timeout=60, headers={"User-Agent": _ROBOTS_UA})
            r.raise_for_status()
            rows = r.json()
        except Exception as e:
            log.warning(f"Wayback CDX query failed for {pattern}: {e}")
            continue

        # First row is the column header (["original"]) when output=json
        urls = [row[0] for row in rows[1:]] if rows and isinstance(rows, list) else []
        log.info(f"  Wayback CDX: {len(urls)} archived snapshot URLs")

        for url in urls:
            slug = _url_to_slug_adp_discovery(url)
            if slug:
                slugs.add(slug)

    if slugs:
        log.info(f"  adp: {len(slugs)} companies from Wayback Machine")
    return {"adp": slugs}


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
# SOURCE 7: TheirStack (freemium technology-usage API)
# ══════════════════════════════════════════════════════════

def fetch_theirstack_slugs(max_companies: int = 40) -> dict[str, dict[str, str]]:
    """Pull companies for the thinner platforms from TheirStack's free
    tier. Requires THEIRSTACK_API_KEY (free signup, no credit card) —
    returns empty and logs a one-line notice if it's not set, rather than
    failing the whole enrichment run.

    `max_companies` caps TOTAL companies fetched across all platforms
    this run (default 40, under the free tier's 50 company-credits/month
    so a couple of runs a month stay comfortably inside the free budget —
    raise it if you're on a paid plan).
    """
    if not THEIRSTACK_API_KEY:
        log.info("TheirStack: THEIRSTACK_API_KEY not set — skipping "
                 "(free signup at https://theirstack.com, no credit card needed).")
        return {}

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {THEIRSTACK_API_KEY}",
    }

    slugs_by_ats: dict[str, dict[str, str]] = {}
    spent = 0

    for ats, ts_slug in THEIRSTACK_ATS_SLUGS.items():
        if spent >= max_companies:
            log.info(f"TheirStack: hit max_companies budget ({max_companies}) — stopping.")
            break
        remaining = max_companies - spent
        page_limit = min(25, remaining)

        try:
            r = requests.post(
                THEIRSTACK_API_URL,
                headers=headers,
                json={
                    "company_technology_slug_or": [ts_slug],
                    "limit": page_limit,
                    "page": 0,
                },
                timeout=30,
            )
            if r.status_code == 401:
                log.error("TheirStack: 401 Unauthorized — check THEIRSTACK_API_KEY.")
                break
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning(f"TheirStack query failed for {ats} (slug={ts_slug!r}): {e}")
            time.sleep(0.6)  # stay under the 2 req/sec free-tier rate limit
            continue

        companies = data.get("data") or data.get("companies") or []
        added = 0
        for c in companies:
            domain = c.get("domain") or c.get("website") or ""
            name = c.get("name", "")
            if not domain:
                continue
            host = urlparse(domain if "://" in domain else f"https://{domain}").hostname or domain
            slug = host.split(".")[0] if host else None
            if slug and slug.lower() not in SKIP_SLUGS:
                slugs_by_ats.setdefault(ats, {})[slug] = name
                added += 1

        if added:
            log.info(f"  {ats}: {added} companies from TheirStack (slug={ts_slug!r})")
        elif companies == [] and not added:
            log.info(f"  {ats}: 0 results for TheirStack slug {ts_slug!r} — "
                      f"double-check it against theirstack.com/en/technology/{ts_slug}")

        spent += added
        time.sleep(0.6)

    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 8: HTTP Archive (public BigQuery — Wappalyzer detection at scale)
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
    the TheirStack source above.
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
    slug_registry (e.g. 'eeho.fa.us2|CX_1' -> tenant 'eeho').

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
                f"{SUPABASE_URL}/rest/v1/slug_registry",
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
    a resolved counterpart in slug_registry, so re-enrichment never
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
        log.info(f"  oracle_cloud_hcm: skipped {skipped} legacy slugs already resolved in slug_registry")

    return filtered


def upsert_to_supabase(slugs_by_ats: dict[str, set | dict], source: str,
                        dry_run: bool = False) -> int:
    """Upsert slugs to Supabase slug_registry. Returns total upserted.

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
                row = {"ats": ats, "slug": slug, "source": source}
                if name:
                    row["name"] = name[:300]
                rows.append(row)

            if dry_run:
                ats_total += len(chunk)
                continue

            r = None
            try:
                r = requests.post(
                    f"{SUPABASE_URL}/rest/v1/slug_registry",
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
        description="Enrich Supabase slug_registry from 8 sources"
    )
    parser.add_argument(
        "--source",
        choices=["feashliaa", "kalil", "openpostings", "commoncrawl",
                 "wayback_adp", "yc", "theirstack", "httparchive", "all"],
        default="all",
        help="Which source to pull from (default: all)",
    )
    parser.add_argument(
        "--crawls", type=int, default=6,
        help="Number of Common Crawl archives to query (default: 6 — "
             "raised from 3 for deeper historical discovery)",
    )
    parser.add_argument(
        "--yc-limit", type=int, default=2000,
        help="Max YC companies to attempt per run (default: 2000; 0 = all "
             "~6k — see fetch_yc_slugs docstring for why this is capped)",
    )
    parser.add_argument(
        "--theirstack-max", type=int, default=40,
        help="Max companies to pull from TheirStack per run, across all "
             "platforms (default: 40, under the free tier's 50/month)",
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
    log.info("SLUG ENRICHMENT — Supabase as single source of truth")
    log.info("  Sources: Feashliaa + kalil0321 + OpenPostings + Common Crawl")
    log.info("           + Wayback CDX (ADP) + Y Combinator + TheirStack")
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

    # Source 4: Common Crawl (ongoing discovery for 21 platforms)
    if args.source in ("commoncrawl", "all"):
        log.info("\n--- COMMON CRAWL (ongoing discovery) ---")
        cc_slugs = fetch_commoncrawl_slugs(args.crawls)
        cc_total = sum(len(s) for s in cc_slugs.values())
        log.info(f"Common Crawl total: {cc_total} slugs across "
                 f"{sum(1 for s in cc_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(cc_slugs, source="commoncrawl",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += cc_total

    # Source 5: Wayback Machine CDX (ADP-only — see fetch_wayback_adp_slugs
    # docstring for why ADP specifically needs a second discovery source)
    if args.source in ("wayback_adp", "all"):
        log.info("\n--- WAYBACK MACHINE CDX (ADP-only supplemental discovery) ---")
        wb_slugs = fetch_wayback_adp_slugs()
        wb_total = sum(len(s) for s in wb_slugs.values())
        log.info(f"Wayback CDX total: {wb_total} slugs")

        if not args.dry_run:
            upserted = upsert_to_supabase(wb_slugs, source="wayback_adp",
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

    # Source 7: TheirStack (freemium — small monthly trickle for thin
    # platforms, see fetch_theirstack_slugs docstring)
    if args.source in ("theirstack", "all"):
        log.info("\n--- THEIRSTACK (freemium, thin-platform gap-fill) ---")
        ts_slugs = fetch_theirstack_slugs(max_companies=args.theirstack_max)
        ts_total = sum(len(s) for s in ts_slugs.values())
        if ts_total:
            log.info(f"TheirStack total: {ts_total} slugs across "
                     f"{sum(1 for s in ts_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(ts_slugs, source="theirstack",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += ts_total

    # Source 8: HTTP Archive (BigQuery — real technology-fingerprint
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
