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
#  "taleo",  "successfactors",  "softgarden"
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
    "applicantstack": "ApplicantStack",
    "catsone": "CATS",
    "bullhorn": "Bullhorn",
    "paycor": "Paycor",
    "pageup": "PageUp",
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
    "applicantstack", "catsone", "bullhorn", "paycor", "pageup",
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
    "applicantstack": "applicantstack",
    "catsone": "catsone",
    "bullhorn": "bullhorn",
    "paycor": "paycor",
    "pageup": "pageup",
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
    leading locale segment (e.g. /en-US/) before reading the site id.
    EDIT: Added myworkdaysite.com which is Workday's alternate career domain."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "myworkdayjobs.com" in host or "myworkdaysite.com" in host:
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
    if "taleo.net" in host or "tbe.taleo.net" in host:
        company = host.replace(".taleo.net", "").replace(".tbe.taleo.net", "").lower()
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
    if "hrmdirect.com" in host or "clearcompany.com" in host:
        slug = host.replace(".hrmdirect.com", "").replace(".clearcompany.com", "").lower()
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
    if host.endswith(".hire.trakstar.com") or ".recruiterbox.com" in host:
        slug = host.replace(".hire.trakstar.com", "").replace(".recruiterbox.com", "").lower()
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

# ── NEW ATS CONVERTERS ──────────────────────────────────────
def _url_to_slug_applicantstack(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "applicantstack.com" in host:
        slug = host.replace(".applicantstack.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None

def _url_to_slug_catsone(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "catsone.com" in host:
        slug = host.replace(".catsone.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None

def _url_to_slug_bullhorn(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "bullhornstaffing.com" in host:
        slug = host.replace(".bullhornstaffing.com", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www" and not slug.startswith("public-rest"):
            return slug
    return None

def _url_to_slug_paycor(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "recruitingbypaycor.com" in host or "gnewton.com" in host:
        qs = parse_qs(parsed.query)
        client_id = (qs.get("clientId") or [None])[0]
        if client_id:
            return client_id
    return None

def _url_to_slug_pageup(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "pageuppeople.com" in host:
        parts = parsed.path.strip("/").split("/")
        if parts and parts[0].isdigit():
            return parts[0]
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
    "applicantstack": _url_to_slug_applicantstack,
    "catsone": _url_to_slug_catsone,
    "bullhorn": _url_to_slug_bullhorn,
    "paycor": _url_to_slug_paycor,
    "pageup": _url_to_slug_pageup,
}

# ══════════════════════════════════════════════════════════
# SLUG SANITY GATE (NEW)
# ══════════════════════════════════════════════════════════

# Wayback/CC surface a lot of archived junk — 404 pages, redirect chains,
# and query-string garbage like '80,%2045,%2060,%200' or '404-careers' —
# that the per-platform extractors will happily turn into fake slugs.
# Every discovered slug passes through _is_valid_slug() before acceptance.
# Allows real composite slugs (a|b|c) and host-prefix slugs (eeho.fa.us2|CX_1).
_VALID_SLUG_CHARS_RE = re.compile(r"^[A-Za-z0-9._-|]+$")
_PCT_ENCODED_RE = re.compile(r"%[0-9A-Fa-f]{2}")
_NO_LETTERS_RE = re.compile(r"^[\d.,-]+$")
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
    "greenhouse": ["boards.greenhouse.io/", "job-boards.greenhouse.io/",
                   "boards.eu.greenhouse.io/*"],
    "lever": ["jobs.lever.co/", "jobs.eu.lever.co/"],
    "ashby": ["jobs.ashbyhq.com/*"],
    "bamboohr": ["*.bamboohr.com/careers", "*.bamboohr.com/jobs"],
    "icims": ["*.icims.com/jobs/"],
    "workday": ["*.myworkdayjobs.com/", "*.myworkdaysite.com/"],
    "workable": ["apply.workable.com/", "*.workable.com/"],
    "recruitee": ["*.recruitee.com/api/offers", "*.recruitee.com/o/"],
    "smartrecruiters": ["jobs.smartrecruiters.com/", "careers.smartrecruiters.com/"],
    "rippling": ["*.rippling.com/careers", "*.rippling.com/jobs"],
    "teamtailor": ["*.teamtailor.com/jobs"],
    "breezyhr": ["*.breezy.hr/"],
    "personio": ["*.jobs.personio.de/", "*.jobs.personio.com/"],
    "joincom": ["join.com/companies/*/jobs", "join.com/companies/"],
    "taleo": ["*.taleo.net/careersection/*/jobsearch.ftl*", "*.tbe.taleo.net/*"],
    "oracle_cloud_hcm": ["*.oraclecloud.com/hcmUI/CandidateExperience/"],
    "paylocity": ["recruiting.paylocity.com/recruiting/jobs/*"],
    "hrmdirect": ["*.hrmdirect.com/employment/", "*.clearcompany.com/careers/"],
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
    "trakstar": ["*.hire.trakstar.com/", "*.recruiterbox.com/"],
    "jobscore": ["careers.jobscore.com/careers/*"],
    "gem": ["jobs.gem.com/*"],
# EDIT (restored discovery wiring for the 2026-09-restored scrapers):
    "successfactors": ["*.successfactors.com/career", "*.successfactors.eu/career",
                       "*.sapsf.com/career", "*.sapsf.eu/career"],
    "brassring": ["sjobs.brassring.com/*"],
# NEW ATS discovery wiring:
    "applicantstack": ["*.applicantstack.com/"],
    "catsone": ["*.catsone.com/"],
    "bullhorn": ["*.bullhornstaffing.com/"],
    "paycor": ["recruitingbypaycor.com/career/*"],
    "pageup": ["careers.pageuppeople.com/*", "*.pageuppeople.com/"],
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
# NEW ATS extractors:
    "applicantstack": _url_to_slug_applicantstack,
    "catsone": _url_to_slug_catsone,
    "bullhorn": _url_to_slug_bullhorn,
    "paycor": _url_to_slug_paycor,
    "pageup": _url_to_slug_pageup,
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
       
</parameter>
</function>
