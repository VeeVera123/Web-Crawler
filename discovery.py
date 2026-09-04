"""
Discovery — Supabase as Single Source of Truth
Pulls company slugs from multiple sources and upserts them into the
Supabase archive_i table. Sources: Feashliaa, kalil0321, OpenPostings,
Common Crawl (4 platform-sharded jobs), Wayback Machine CDX (all
platforms), Y Combinator, HTTP Archive (BigQuery).

Usage:
    python discovery.py                        # all sources
    python discovery.py --source wayback       # one source
    python discovery.py --source commoncrawl --cc-shard 0 --cc-total-shards 4
    python discovery.py --dry-run
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

OPENPOSTINGS_DB_URL = (
    "https://github.com/Masterjx9/OpenPostings/raw/main/jobs.db"
)

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

CC_INDEX_URL = "https://index.commoncrawl.org"
CC_COLLINFO = f"{CC_INDEX_URL}/collinfo.json"

YC_ALL_COMPANIES_URL = "https://yc-oss.github.io/api/companies/all.json"

HTTPARCHIVE_GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "")
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
}

# ATS platforms we have working scrapers for.
# EDIT: successfactors + brassring re-added — they have working scrapers
# (restored 2026-09) but were missing here, so OpenPostings discovery
# skipped them entirely.
SUPPORTED_ATS = {
    "greenhouse", "lever", "ashby", "bamboohr", "icims", "workday",
    "rippling", "workable", "recruitee", "smartrecruiters",
    "teamtailor", "breezyhr", "personio", "joincom",
    "taleo", "oracle_cloud_hcm", "paylocity", "hrmdirect", "zoho",
    "softgarden",
    "successfactors", "brassring",
}

_OPENPOSTINGS_ATS_MAP_RAW = {
    "greenhouse": "greenhouse",
    "lever": "lever",
    "ashby": "ashby",
    "ashbyhq": "ashby",
    "bamboohr": "bamboohr",
    "icims": "icims",
    "workday": "workday",
    "rippling": "rippling",
    "recruitee": "recruitee",
    "smartrecruiters": "smartrecruiters",
    "teamtailor": "teamtailor",
    "workable": "workable",
    "breezyhr": "breezyhr",
    "breezy": "breezyhr",
    "breezy hr": "breezyhr",
    "personio": "personio",
    "joincom": "joincom",
    "join": "joincom",
    "join.com": "joincom",
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
    "brassring": "brassring",
    "successfactors": "successfactors",
}


def _map_ats_name(name: str) -> str | None:
    return _OPENPOSTINGS_ATS_MAP_RAW.get(name.lower().strip())


SKIP_SLUGS = {
    "api", "www", "app", "static", "assets", "cdn", "docs", "help",
    "support", "blog", "login", "register", "test", "demo", "example",
    "staging", "dev", "sandbox", "admin", "",
    "embed", "job_board", "js", "widget", "iframe",
}


# ══════════════════════════════════════════════════════════
# URL → SLUG CONVERTERS
# ══════════════════════════════════════════════════════════

def _url_to_slug_greenhouse(url: str) -> str | None:
    """boards.greenhouse.io/{slug} and the embed widget
    boards.greenhouse.io/embed/job_board/js?for={slug}. Also covers
    job-boards.greenhouse.io and boards.eu.greenhouse.io (same host check)."""
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
    """jobs.lever.co/{slug} and jobs.eu.lever.co/{slug}."""
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
        slug = host.replace(".icims.com", "").lower()
        slug = re.sub(r"^careers-", "", slug)
        if slug and slug not in SKIP_SLUGS:
            return slug
    return None


_WORKDAY_LOCALE_SEGMENT_RE = re.compile(r"^[a-z]{2}-[A-Z]{2}$")


def _url_to_slug_workday(url: str) -> str | None:
    """{company}.wd{N}.myworkdayjobs.com/[{locale}/]{site_id}. Skips a
    leading locale segment (e.g. /en-US/) before reading the site id."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "myworkdayjobs.com" in host:
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
        parts = parsed.path.strip("/").split("/")
        if parts and parts[0] and parts[0].lower() not in SKIP_SLUGS:
            return parts[0]
        slug = host.replace(".rippling.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug not in ("www", "app", "ats"):
            return slug
    return None


_WORKABLE_RESERVED_SUBDOMAINS = {"www", "apply", "jobs", "help", "careers",
                                 "jobseekers", "partners", "support", "grow"}
_WORKABLE_RESERVED_PATH_TOKENS = {"j", "i"}


def _url_to_slug_workable(url: str) -> str | None:
    """apply.workable.com/{slug} and {company}.workable.com."""
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
    """Restricted to the two real job-board hosts only (jobs./careers.) —
    a blanket match previously returned marketing nav paths as fake slugs."""
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
    """oraclecloud.com is Oracle's SHARED hosting domain for every Fusion
    app, so the /hcmUI/CandidateExperience/ path check is mandatory —
    never fall back to a bare host prefix for a non-recruiting page."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "oraclecloud.com" not in host:
        return None
    if "/hcmui/candidateexperience/" not in parsed.path.lower():
        return None
    host_prefix = host.replace(".oraclecloud.com", "").lower()
    if not host_prefix or host_prefix in SKIP_SLUGS:
        return None
    site_match = re.search(r"/sites/([^/]+)", parsed.path)
    if site_match:
        return f"{host_prefix}|{site_match.group(1)}"
    return host_prefix


def _url_to_slug_brassring(url: str) -> str | None:
    """BrassRing needs BOTH partnerid and siteid query params."""
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
    """Excludes Teamtailor's own infra subdomains (scripts/cdn/support)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "teamtailor.com" in host:
        slug = host.replace(".teamtailor.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug not in ("www", "app", "scripts", "cdn", "support"):
            return slug
    return None


# EDIT: rewritten. The scraper REQUIRES a company key (it's the `company`
# API param), so never emit a slug without one. Always use the FULL host —
# scrape_successfactors builds https://{instance} from the part before the
# pipe, so a bare subdomain would build a broken URL. The old code mixed
# full-host and bare-subdomain shapes for the same company.
def _url_to_slug_successfactors(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    sf_domains = (".successfactors.com", ".successfactors.eu", ".sapsf.com", ".sapsf.eu")
    if not any(host.endswith(d) for d in sf_domains):
        return None
    company_key = None
    for k, v in parse_qs(parsed.query).items():
        if k.lower() == "company" and v:
            company_key = v[0]
    if not company_key:
        path_match = re.search(r"company=([^&]+)", url)
        if path_match:
            company_key = path_match.group(1)
    if not company_key:
        return None
    return f"{host}|{company_key}"


def _url_to_slug_breezyhr(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "breezy.hr" in host:
        slug = host.replace(".breezy.hr", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None


def _url_to_slug_applytojob(url: str) -> str | None:
    """REMOVED 2026-08 — kept unused for reference only."""
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
    """Covers .softgarden.io, .career.softgarden.de (the default domain),
    .softgarden.de, and api.softgarden.io/.../jobboards/{channelId}."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    for suffix in (".softgarden.io", ".career.softgarden.de", ".softgarden.de"):
        if host.endswith(suffix):
            slug = host[: -len(suffix)].lower()
            if slug and slug not in SKIP_SLUGS and slug != "www":
                return slug
    if "softgarden" in host:
        path_match = re.search(r"/jobboards/([^/]+)", parsed.path)
        if path_match:
            return path_match.group(1)
    return None


def _url_to_slug_zoho(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    for suffix in (".zohorecruit.com", ".zohorecruit.eu"):
        if host.endswith(suffix):
            slug = host[: -len(suffix)].lower()
            if slug and slug not in SKIP_SLUGS and slug != "www":
                return slug
    return None


def _url_to_slug_paylocity(url: str) -> str | None:
    """recruiting.paylocity.com/recruiting/jobs/All/{uuid}/{Company}.
    Only UUID-format ids (numeric ids are deprecated/404)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "paylocity.com" not in host:
        return None
    parts = parsed.path.strip("/").split("/")
    if len(parts) >= 5 and parts[0].lower() == "recruiting" and parts[1].lower() == "jobs":
        company_id = parts[3]
        company_name = parts[4]
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
        if len(parts) >= 2 and parts[0] == "companies":
            slug = parts[1]
            if slug and slug.lower() not in SKIP_SLUGS:
                return slug
        elif len(parts) >= 1 and parts[0]:
            slug = parts[0]
            if (slug.lower() not in SKIP_SLUGS
                    and slug.lower() not in ("jobs", "about", "faq", "login", "candidates",
                                             "mission", "legal", "privacy", "terms", "apply",
                                             "press", "blog", "signup", "companies")):
                return slug
    if "ycombinator.com" in host:
        parts = parsed.path.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "companies":
            slug = parts[1]
            if slug and slug.lower() not in SKIP_SLUGS:
                return slug
    return None


def _url_to_slug_eploy(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "eploy.net" not in host:
        return None
    slug = host.replace(".eploy.net", "").lower()
    if slug and slug not in SKIP_SLUGS and slug != "www":
        return slug
    return None


def _url_to_slug_folkshr(url: str) -> str | None:
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
    """clientapps.jobadder.com/{client_id}/{board_slug} — both required."""
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
    """jobs.jobvite.com/{company}, plus the /careers/{company} alias and
    the legacy app.jobvite.com/CompanyJobs/Careers.aspx?c={code} family."""
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
    """Pure parse (no network) of the modern cid/ccId URL family."""
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
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "adp.com" not in host or "/jobs/apply/posting.html" not in parsed.path:
        return None
    qs = parse_qs(parsed.query)
    client = (qs.get("client") or [None])[0]
    return client or None


_ROBOTS_UA = "ATS-Global-Scanner/1.0"


def _resolve_adp_legacy_client(client: str) -> str | None:
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


_ADP_LEGACY_RESOLVE_CAP = 200


def _url_to_slug_adp_discovery(url: str) -> str | None:
    """Combined modern-parse + legacy-resolve for CC/Wayback ADP discovery.
    Kept separate from _url_to_slug_adp (which must stay network-free for
    OpenPostings' 110k-row scan)."""
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
    time.sleep(0.5)
    return resolved


_url_to_slug_adp_discovery._resolved_clients = {}


def _url_to_slug_avature(url: str) -> str | None:
    """{subdomain}.avature.net — subdomain-only, path ignored (Avature
    path structure varies too much across customers)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "avature.net" not in host:
        return None
    slug = host.replace(".avature.net", "").lower()
    if slug and slug not in SKIP_SLUGS and slug != "www":
        return slug
    return None


def _url_to_slug_trakstar(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host.endswith(".hire.trakstar.com"):
        return None
    slug = host[: -len(".hire.trakstar.com")].lower()
    if slug and slug not in SKIP_SLUGS and slug != "www":
        return slug
    return None


def _url_to_slug_jobscore(url: str) -> str | None:
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


def _url_to_slug_gem(url: str) -> str | None:
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
    "hrmdirect": _url_to_slug_hrmdirect,
    "softgarden": _url_to_slug_softgarden,
    "zoho": _url_to_slug_zoho,
    "paylocity": _url_to_slug_paylocity,
    "ycombinator": _url_to_slug_ycombinator,
    "personio": _url_to_slug_personio,
    "joincom": _url_to_slug_joincom,
    "eploy": _url_to_slug_eploy,
    "folkshr": _url_to_slug_folkshr,
    "jobadder": _url_to_slug_jobadder,
    "jobvite": _url_to_slug_jobvite,
    "adp": _url_to_slug_adp,
    "avature": _url_to_slug_avature,
    "trakstar": _url_to_slug_trakstar,
    "jobscore": _url_to_slug_jobscore,
    "gem": _url_to_slug_gem,
}


# ══════════════════════════════════════════════════════════
# SLUG SANITY GATE (NEW)
# ══════════════════════════════════════════════════════════
# Wayback/CC surface a lot of archived junk — 404 pages, redirect chains,
# and query-string garbage like '80,%2045,%2060,%200' or '404-careers' —
# that the per-platform extractors will happily turn into fake slugs.
# Every discovered slug passes through _is_valid_slug() before acceptance.
# Allows real composite slugs (a|b|c) and host-prefix slugs (eeho.fa.us2|CX_1).
_VALID_SLUG_CHARS_RE = re.compile(r"^[A-Za-z0-9._\-|]+$")
_PCT_ENCODED_RE = re.compile(r"%[0-9A-Fa-f]{2}")
_NO_LETTERS_RE = re.compile(r"^[\d.,\-]+$")
_JUNK_SLUG_RE = re.compile(
    r"(?:^|[-_.])(?:404|500|error|notfound|not-found|page-not-found|"
    r"redirect|cache|null|undefined|nan|untitled)(?:[-_.]|$)", re.I)


def _is_valid_slug(slug: str) -> bool:
    if not slug or not isinstance(slug, str):
        return False
    slug = slug.strip()
    if len(slug) < 2 or len(slug) > 120:
        return False
    if _PCT_ENCODED_RE.search(slug):
        return False   # '%20'-style encoded junk
    if not _VALID_SLUG_CHARS_RE.match(slug):
        return False   # spaces, commas, quotes, slashes, etc.
    if _NO_LETTERS_RE.match(slug):
        return False   # pure number/ID fragment
    if _JUNK_SLUG_RE.search(slug):
        return False   # 404-careers, error-page, etc.
    return True


# ══════════════════════════════════════════════════════════
# SOURCE 1: Feashliaa GitHub
# ══════════════════════════════════════════════════════════

def fetch_feashliaa_slugs() -> dict[str, set[str]]:
    slugs_by_ats: dict[str, set[str]] = {}
    for ats, url in FEASHLIAA_SOURCES.items():
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                clean = {s.strip() for s in data
                         if isinstance(s, str) and s.strip()
                         and s.strip().lower() not in SKIP_SLUGS
                         and _is_valid_slug(s.strip())}
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
# SOURCE 2: kalil0321/ats-scrapers
# ══════════════════════════════════════════════════════════

def _parse_csv_line(line: str) -> tuple[str, str, str] | None:
    line = line.strip()
    if not line:
        return None
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
    slugs_by_ats: dict[str, dict[str, str]] = {}
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
                if slug and _is_valid_slug(slug):
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
    log.info("Downloading OpenPostings jobs.db...")
    slugs_by_ats: dict[str, dict[str, str]] = {ats: {} for ats in SUPPORTED_ATS}
    skipped_ats = {}

    try:
        r = requests.get(OPENPOSTINGS_DB_URL, timeout=120, stream=True)
        r.raise_for_status()
    except Exception as e:
        log.error(f"Failed to download OpenPostings DB: {e}")
        return slugs_by_ats

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
            if slug and _is_valid_slug(slug):
                # EDIT: setdefault guards against a mapped ATS that isn't
                # pre-seeded in slugs_by_ats (would otherwise KeyError and
                # abort the whole parse).
                slugs_by_ats.setdefault(our_ats, {})
                if slug not in slugs_by_ats[our_ats]:
                    slugs_by_ats[our_ats][slug] = (company_name or "").strip()
                matched += 1
            else:
                if our_ats not in conversion_failures:
                    conversion_failures[our_ats] = []
                if len(conversion_failures[our_ats]) < 3:
                    conversion_failures[our_ats].append(url_string)

        conn.close()
        log.info(f"OpenPostings: {total} total companies, "
                 f"{matched} matched to our {len(SUPPORTED_ATS)} platforms")

        if skipped_ats:
            top_skipped = sorted(skipped_ats.items(), key=lambda x: -x[1])[:10]
            log.info(f"Top unmapped ATSs: {', '.join(f'{k}({v})' for k, v in top_skipped)}")

        for ats in sorted(SUPPORTED_ATS):
            count = len(slugs_by_ats.get(ats, {}))
            if count:
                log.info(f"  {ats}: {count} companies")

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
# EDIT: added successfactors + brassring (they have scrapers but had zero
# CC discovery wiring). Expanded greenhouse (job-boards.* is Greenhouse's
# newer domain; boards.eu.* is the EU region) and lever (jobs.eu.*).
CC_PLATFORM_PATTERNS = {
    "greenhouse": ["boards.greenhouse.io/*", "job-boards.greenhouse.io/*",
                   "boards.eu.greenhouse.io/*"],
    "lever": ["jobs.lever.co/*", "jobs.eu.lever.co/*"],
    "ashby": ["jobs.ashbyhq.com/*"],
    "bamboohr": ["*.bamboohr.com/careers", "*.bamboohr.com/jobs"],
    "icims": ["*.icims.com/jobs/"],
    "workday": ["*.myworkdayjobs.com/"],
    "workable": ["apply.workable.com/", "*.workable.com/*"],
    "recruitee": ["*.recruitee.com/api/offers", "*.recruitee.com/o/"],
    "smartrecruiters": ["jobs.smartrecruiters.com/", "careers.smartrecruiters.com/"],
    "rippling": ["*.rippling.com/careers", "*.rippling.com/jobs"],
    "teamtailor": ["*.teamtailor.com/jobs"],
    "breezyhr": ["*.breezy.hr/"],
    "personio": ["*.jobs.personio.de/", "*.jobs.personio.com/"],
    "joincom": ["join.com/companies/*/jobs", "join.com/companies/*"],
    "taleo": ["*.taleo.net/careersection/*/jobsearch.ftl*"],
    "oracle_cloud_hcm": ["*.oraclecloud.com/hcmUI/CandidateExperience/"],
    "paylocity": ["recruiting.paylocity.com/recruiting/jobs/*"],
    "hrmdirect": ["*.hrmdirect.com/employment/"],
    "zoho": ["*.zohorecruit.com/jobs/", "*.zohorecruit.eu/jobs/"],
    "softgarden": ["*.softgarden.io/en/vacancies", "*.softgarden.io/vacancies",
                   "*.softgarden.io/job/", "api.softgarden.io/*/jobboards/",
                   "*.career.softgarden.de/"],
    "eploy": ["*.eploy.net/candidate/jobboard/"],
    "folkshr": ["jobs.folksats.app/", "jobs.glowinthecloud.com/"],
    "jobadder": ["clientapps.jobadder.com/*"],
    "jobvite": ["jobs.jobvite.com/*"],
    "adp": ["workforcenow.adp.com/mascsr/", "workforcenow.adp.com/jobs/apply/posting.html"],
    "avature": ["*.avature.net/"],
    "trakstar": ["*.hire.trakstar.com/"],
    "jobscore": ["careers.jobscore.com/careers/*"],
    "gem": ["jobs.gem.com/*"],
    # EDIT (restored discovery wiring for the 2026-09-restored scrapers):
    "successfactors": ["*.successfactors.com/career", "*.successfactors.eu/career",
                       "*.sapsf.com/career", "*.sapsf.eu/career"],
    "brassring": ["sjobs.brassring.com/*"],
}

CC_EXTRACTORS = {
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
    "personio": _url_to_slug_personio,
    "joincom": _url_to_slug_joincom,
    "taleo": _url_to_slug_taleo,
    "oracle_cloud_hcm": _url_to_slug_oracle_cloud,
    "paylocity": _url_to_slug_paylocity,
    "hrmdirect": _url_to_slug_hrmdirect,
    "zoho": _url_to_slug_zoho,
    "softgarden": _url_to_slug_softgarden,
    "eploy": _url_to_slug_eploy,
    "folkshr": _url_to_slug_folkshr,
    "jobadder": _url_to_slug_jobadder,
    "jobvite": _url_to_slug_jobvite,
    "adp": _url_to_slug_adp_discovery,
    "avature": _url_to_slug_avature,
    "trakstar": _url_to_slug_trakstar,
    "jobscore": _url_to_slug_jobscore,
    "gem": _url_to_slug_gem,
    # EDIT: must match CC_PLATFORM_PATTERNS keys exactly (a mismatch
    # KeyErrors the whole Common Crawl run).
    "successfactors": _url_to_slug_successfactors,
    "brassring": _url_to_slug_brassring,
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
                    if slug and _is_valid_slug(slug):
                        slugs_by_ats[ats].add(slug)
                time.sleep(1)
        count = len(slugs_by_ats[ats])
        if count:
            log.info(f"  {ats}: {count} companies from Common Crawl")
    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# WAYBACK MACHINE CDX — all-platform supplemental discovery
# ══════════════════════════════════════════════════════════

WAYBACK_CDX_URL = "http://web.archive.org/cdx/search/cdx"
_robots_check_stats = {"unreachable": 0, "disallowed_by_rule": 0}


def _robots_allows(base_url: str, path: str, user_agent: str = _ROBOTS_UA) -> bool:
    try:
        r = requests.get(f"{base_url}/robots.txt", timeout=15,
                         headers={"User-Agent": user_agent})
        if r.status_code >= 400:
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
        _robots_check_stats["unreachable"] += 1
        log.debug(f"robots.txt check failed for {base_url}: {e} — treating as disallowed")
        return False


def _cc_pattern_to_wayback_query(pattern: str) -> dict:
    if pattern.startswith("*."):
        host_and_rest = pattern[2:]
        host = host_and_rest.split("/", 1)[0]
        return {"url": host, "matchType": "domain"}
    return {"url": pattern}


def fetch_wayback_slugs(limit_per_pattern: int = 2000) -> dict[str, set[str]]:
    slugs_by_ats: dict[str, set[str]] = {ats: set() for ats in CC_PLATFORM_PATTERNS}

    if not _robots_allows("https://web.archive.org", "/cdx/"):
        log.warning("Wayback CDX: /cdx/ disallowed by web.archive.org/robots.txt "
                    "(or robots.txt unreachable) — skipping Wayback discovery "
                    "for all platforms.")
        return slugs_by_ats

    for ats, patterns in CC_PLATFORM_PATTERNS.items():
        extractor = CC_EXTRACTORS.get(ats)
        if extractor is None:
            continue
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
            urls = [row[0] for row in rows[1:]] if rows and isinstance(rows, list) else []
            log.info(f"  Wayback CDX: {len(urls)} archived snapshot URLs")
            for url in urls:
                slug = extractor(url)
                if slug and _is_valid_slug(slug):
                    slugs_by_ats[ats].add(slug)
            time.sleep(0.5)
        if slugs_by_ats[ats]:
            log.info(f"  {ats}: {len(slugs_by_ats[ats])} companies from Wayback Machine")
    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 6: Y Combinator (yc-oss/api)
# ══════════════════════════════════════════════════════════

YC_USER_AGENT = _ROBOTS_UA
_YC_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.I)
_YC_CAREER_LINK_RE = re.compile(
    r"\b(careers?|jobs?|join[\s-]?us|we[\s-]?re[\s-]?hiring|work[\s-]?with[\s-]?us)\b",
    re.I,
)


def fetch_yc_companies() -> list[dict]:
    try:
        r = requests.get(YC_ALL_COMPANIES_URL, timeout=60,
                         headers={"User-Agent": YC_USER_AGENT})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"Failed to fetch YC company list: {e}")
        return []


def _scan_html_for_ats_slug(html: str, base_url: str) -> tuple[str, str] | None:
    for href in _YC_HREF_RE.findall(html):
        try:
            absolute = urljoin(base_url, href)
        except Exception:
            continue
        for ats, resolver in URL_TO_SLUG.items():
            slug = resolver(absolute)
            if slug and _is_valid_slug(slug):
                return ats, slug
    return None


def _find_career_page_link(html: str, base_url: str) -> str | None:
    for href in _YC_HREF_RE.findall(html):
        if _YC_CAREER_LINK_RE.search(href):
            try:
                return urljoin(base_url, href)
            except Exception:
                continue
    return None


def resolve_company_to_ats_slug(website: str, timeout: int = 15) -> tuple[str, str] | None:
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

    hit = _scan_html_for_ats_slug(html, base)
    if hit:
        return hit

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
        time.sleep(0.1)
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
             f"robots.txt, rest had no detectable ATS link).")
    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 7: HTTP Archive (public BigQuery)
# ══════════════════════════════════════════════════════════

def fetch_httparchive_candidate_urls(limit_per_tech: int = 2000,
                                     months: int = 6) -> dict[str, list[str]]:
    try:
        from google.cloud import bigquery
    except ImportError:
        log.info("HTTP Archive: google-cloud-bigquery not installed — skipping.")
        return {}

    if not HTTPARCHIVE_GCP_PROJECT:
        log.info("HTTP Archive: GCP_PROJECT_ID not set — skipping.")
        return {}

    try:
        client = bigquery.Client(project=HTTPARCHIVE_GCP_PROJECT)
    except Exception as e:
        log.warning(f"HTTP Archive: couldn't create BigQuery client: {e}")
        return {}

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

    log.info(f"HTTP Archive: querying {len(crawl_dates)} crawl(s) for "
             f"{len(HTTPARCHIVE_ATS_TECH_NAMES)} known ATS fingerprints...")

    urls_by_ats: dict[str, list[str]] = {}
    tech_to_ats = {v: k for k, v in HTTPARCHIVE_ATS_TECH_NAMES.items()}

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
                slugs_by_ats.setdefault(actual_ats, {})[slug] = ""
                resolved += 1

    log.info(f"HTTP Archive: resolved {resolved}/{len(all_urls)} candidate pages")
    for ats, slugs in slugs_by_ats.items():
        log.info(f"  {ats}: {len(slugs)} companies from HTTP Archive")
    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SUPABASE UPSERT
# ══════════════════════════════════════════════════════════

def _oracle_tenant(slug: str) -> str:
    host_prefix = slug.split("|", 1)[0]
    return host_prefix.split(".", 1)[0]


def _is_resolved_oracle_slug(slug: str) -> bool:
    host_prefix = slug.split("|", 1)[0]
    return ".fa." in host_prefix


def _fetch_resolved_oracle_tenants() -> set[str]:
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

        if isinstance(slugs, set):
            slug_dict = {s: "" for s in slugs}
        else:
            slug_dict = slugs

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
    parser.add_argument("--crawls", type=int, default=6)
    parser.add_argument("--cc-shard", type=int, default=None)
    parser.add_argument("--cc-total-shards", type=int, default=1)
    parser.add_argument("--yc-limit", type=int, default=2000)
    parser.add_argument("--httparchive-limit", type=int, default=2000)
    parser.add_argument("--httparchive-months", type=int, default=6)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 60)
    log.info("DISCOVERY — Supabase as single source of truth")
    log.info("=" * 60)

    grand_total = 0

    if args.source in ("feashliaa", "all"):
        log.info("\n--- FEASHLIAA ---")
        fa_slugs = fetch_feashliaa_slugs()
        fa_total = sum(len(s) for s in fa_slugs.values())
        if not args.dry_run:
            grand_total += upsert_to_supabase(fa_slugs, source="feashliaa", dry_run=args.dry_run)
        else:
            grand_total += fa_total

    if args.source in ("kalil", "all"):
        log.info("\n--- KALIL0321 ---")
        ka_slugs = fetch_kalil_slugs()
        ka_total = sum(len(s) for s in ka_slugs.values())
        if not args.dry_run:
            grand_total += upsert_to_supabase(ka_slugs, source="kalil", dry_run=args.dry_run)
        else:
            grand_total += ka_total

    if args.source in ("openpostings", "all"):
        log.info("\n--- OPENPOSTINGS ---")
        op_slugs = fetch_openpostings_slugs()
        op_total = sum(len(s) for s in op_slugs.values())
        log.info(f"OpenPostings total: {op_total} slugs")
        if not args.dry_run:
            grand_total += upsert_to_supabase(op_slugs, source="openpostings", dry_run=args.dry_run)
        else:
            grand_total += op_total

    if args.source in ("commoncrawl", "all"):
        log.info("\n--- COMMON CRAWL ---")
        cc_slugs = fetch_commoncrawl_slugs(args.crawls, cc_shard=args.cc_shard,
                                           cc_total_shards=args.cc_total_shards)
        cc_total = sum(len(s) for s in cc_slugs.values())
        log.info(f"Common Crawl total: {cc_total} slugs")
        if not args.dry_run:
            grand_total += upsert_to_supabase(cc_slugs, source="commoncrawl", dry_run=args.dry_run)
        else:
            grand_total += cc_total

    if args.source in ("wayback", "all"):
        log.info("\n--- WAYBACK MACHINE CDX ---")
        wb_slugs = fetch_wayback_slugs()
        wb_total = sum(len(s) for s in wb_slugs.values())
        log.info(f"Wayback CDX total: {wb_total} slugs")
        if not args.dry_run:
            grand_total += upsert_to_supabase(wb_slugs, source="wayback", dry_run=args.dry_run)
        else:
            grand_total += wb_total

    if args.source in ("yc", "all"):
        log.info("\n--- Y COMBINATOR ---")
        yc_slugs = fetch_yc_slugs(limit=args.yc_limit)
        yc_total = sum(len(s) for s in yc_slugs.values())
        log.info(f"Y Combinator total: {yc_total} slugs")
        if not args.dry_run:
            grand_total += upsert_to_supabase(yc_slugs, source="yc", dry_run=args.dry_run)
        else:
            grand_total += yc_total

    if args.source in ("httparchive", "all"):
        log.info("\n--- HTTP ARCHIVE ---")
        ha_slugs = fetch_httparchive_slugs(limit_per_tech=args.httparchive_limit,
                                           months=args.httparchive_months)
        ha_total = sum(len(s) for s in ha_slugs.values())
        if not args.dry_run:
            grand_total += upsert_to_supabase(ha_slugs, source="httparchive", dry_run=args.dry_run)
        else:
            grand_total += ha_total

    action = "would upsert" if args.dry_run else "upserted"
    log.info(f"\nDone! {action} {grand_total} total slugs to Supabase.")


if __name__ == "__main__":
    main()
