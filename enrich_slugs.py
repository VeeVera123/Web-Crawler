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

Runs weekly (Sunday) via GitHub Actions. The daily scanner
reads from Supabase slug_registry — no local .txt files needed.

Usage:
    python enrich_slugs.py                        # full enrichment (all 4)
    python enrich_slugs.py --source feashliaa     # Feashliaa only
    python enrich_slugs.py --source kalil         # kalil0321 only
    python enrich_slugs.py --source openpostings  # OpenPostings only
    python enrich_slugs.py --source commoncrawl   # Common Crawl only
    python enrich_slugs.py --dry-run              # count without writing
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import tempfile
import time
from urllib.parse import urlparse, parse_qs

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
    Pattern: jobs.folksats.app/{company}/..."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "folksats.app" not in host:
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
    Pattern: jobs.jobvite.com/{company}/jobs or /{company}/job/{id}"""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "jobvite.com" not in host:
        return None
    parts = parsed.path.strip("/").split("/")
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
    "folkshr": ["jobs.folksats.app/*"],
    "jobadder": ["clientapps.jobadder.com/*/*"],
    "jobvite": ["jobs.jobvite.com/*/jobs*", "jobs.jobvite.com/*/job/*"],
    "adp": ["workforcenow.adp.com/mascsr/*/mdf/recruitment/*", "workforcenow.adp.com/mascsr/*/careercenter/public/*"],
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
    "adp": _url_to_slug_adp,
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
                log.error(f"Supabase upsert failed for {ats}: {e}")

        if ats_total:
            log.info(f"  {ats}: upserted {ats_total} slugs ({source})")
        total += ats_total

    return total


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Enrich Supabase slug_registry from 4 sources"
    )
    parser.add_argument(
        "--source",
        choices=["feashliaa", "kalil", "openpostings", "commoncrawl", "all"],
        default="all",
        help="Which source to pull from (default: all)",
    )
    parser.add_argument(
        "--crawls", type=int, default=3,
        help="Number of Common Crawl archives to query (default: 3)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count slugs without writing to Supabase",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SLUG ENRICHMENT — Supabase as single source of truth")
    log.info("  Sources: Feashliaa + kalil0321 + OpenPostings + Common Crawl")
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

    action = "would upsert" if args.dry_run else "upserted"
    log.info(f"\nDone! {action} {grand_total} total slugs to Supabase.")


if __name__ == "__main__":
    main()
