"""
Discovery — Supabase as Single Source of Truth
=====================================================
Pulls company slugs from multiple sources and upserts them
into the Supabase slug_registry table.

Sources:
  1. Feashliaa GitHub (50k+ slugs for 6 platforms — greenhouse,
     lever, ashby, bamboohr, icims, workday)
  2. kalil0321/ats-scrapers (CSV inventories for 15 platforms —
     incl. successfactors, smartrecruiters, workable)
  3. OpenPostings jobs.db (110k+ companies across 80+ ATSs)
  4. Common Crawl index (ongoing discovery for 27 platforms — including
     6 also covered by Feashliaa's bulk dump, added as a supplemental
     top-up since dedup is free via the on_conflict upsert. Run as 2
     platform-sharded matrix jobs in discovery.yml — see
     fetch_commoncrawl_slugs docstring for why, and --cc-shard/
     --cc-total-shards below)
  5. Wayback Machine CDX (ADP-only supplemental discovery)
  6. Y Combinator — REMOVED 2026-09 (see main()'s Source 6 comment).
     fetch_yc_slugs() itself is left defined/unused.
  7. Latmay H.F (huggingface.co/datasets/latmay/ats-career-page-urls —
     69,638 rows, each already an ATS URL resolved by the dataset
     owner. No live crawl needed — an offline pass through URL_TO_SLUG.
     Logs to Supabase under source="Latmay H.F".)
  8. Edward H.F (huggingface.co/datasets/edwarddgao/open-apply-jobs —
     31M+ individual job-posting rows across 375 Parquet shards, no
     ATS label or dedup by the owner. Only apply_url is ever read
     (column-projected at the Parquet level); every URL is matched
     against URL_TO_SLUG. Logs to Supabase under source="Edward H.F".)
  9. TheirStack (freemium technology-usage API — 50 company credits/
     month on the free tier, so this is a small monthly trickle for
     gap-filling thin platforms, not a bulk source. Needs a free
     THEIRSTACK_API_KEY — sign up at theirstack.com, no credit card)
  10. HTTP Archive (public BigQuery dataset — real technology-fingerprint
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

Runs weekly (Sunday) via GitHub Actions, as a 7-source, 8-job matrix
(YC removed 2026-09, Latmay H.F + Edward H.F added) — Common Crawl
split across 2 platform-sharded jobs, the other 6 sources one job
each, all in parallel (see .github/workflows/discovery.yml) —
rather than one job running everything back-to-back. Each source (or
Common Crawl shard) is already an independent fetch-and-resolve pass with
its own cost profile (bulk single download vs. thousands of live
per-company fetches), so sharding by source is the natural split here —
there's no single flat pool of "work items" to hash-shard the way main.py
splits ATS boards across its matrix.

The daily scanner reads from Supabase slug_registry — no local .txt files
needed.

Usage:
    python discovery.py                        # full enrichment (all sources, sequential)
    python discovery.py --source feashliaa     # Feashliaa only
    python discovery.py --source kalil         # kalil0321 only
    python discovery.py --source openpostings  # OpenPostings only
    python discovery.py --source commoncrawl   # Common Crawl only
    python discovery.py --source wayback_adp   # Wayback CDX (ADP) only
    python discovery.py --source latmay        # Latmay H.F (Hugging Face) only
    python discovery.py --source edwarddgao    # Edward H.F (Hugging Face) only
    python discovery.py --source theirstack    # TheirStack only
    python discovery.py --source httparchive   # HTTP Archive (BigQuery) only
    python discovery.py --source commoncrawl --cc-shard 0 --cc-total-shards 2
    python discovery.py --source commoncrawl --cc-shard 1 --cc-total-shards 2
    python discovery.py --dry-run              # count without writing
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import tempfile
import threading
import time
from urllib.parse import urlparse, parse_qs, urljoin, unquote

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
# (free Sandbox tier — no credit card — covers this easily) and a service
# account key for programmatic access.
#
# 2026-09 cost re-check against HTTP Archive's own current schema docs
# (har.fyi) and Google's current Sandbox docs: the Sandbox's free quota is
# still a flat 1 TiB/month of bytes PROCESSED (unchanged), and a single
# date x client slice of this exact query shape (selecting page + the
# technology/rank fields only, no categories/info) realistically costs
# more like ~5-10GB, not the ~1-2GB this comment used to say — the old
# number was an optimistic guess, this one's from HTTP Archive's own
# worked query-cost examples. See fetch_httparchive_candidate_urls'
# docstring for how that revised number sizes the `months` default below.
HTTPARCHIVE_GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "")
# google-cloud-bigquery bills/quotas against YOUR project but queries
# Google's own public `httparchive` dataset — you don't need write access
# to httparchive itself, just any GCP project with BigQuery turned on.

# Map our ATS keys -> the exact Wappalyzer technology name, VERIFIED
# 2026-08 by downloading the actual fingerprint files from the actively
# maintained Wappalyzer fork (github.com/enthec/webappanalyzer) and
# confirming each key exists verbatim. Platforms with no confirmed
# fingerprint are left out entirely rather than guessed at.
#
# RE-VERIFIED 2026-09, exhaustively (every one of the 17 below re-fetched
# and re-checked byte-for-byte, not sampled) — zero discrepancies, all 17
# names below are still exactly correct with no renames/removals. Also
# closed a real gap from the 2026-08 pass: Oracle Cloud HCM/Fusion
# Recruiting/Taleo Cloud had never actually been checked either way before
# (silently missing from both this dict AND the "confirmed NOT present"
# list below) — now confirmed absent too, added to that list.
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
    # New (2026-09) — confirmed via direct fingerprint-file fetch against
    # the enthec/webappanalyzer fork, same verification standard as the
    # rest of this dict:
    "pageup": "PageUp",     # scriptSrc: careers-static.pageuppeople.com
                             # (works even on a fully custom domain, since
                             # the fingerprint is asset-host-based, not
                             # relying on the shared pageuppeople.com
                             # career-site domain)
    "jobylon": "Jobylon",   # scriptSrc: *.jobylon.com
    "homerun": "Homerun",   # js globals: homerunI18n / homerunPrivacySettings
    # Pinpoint, Flatchr, Occupop: checked, NO fingerprint found under any
    # plausible name (including "Cezanne" for Occupop, post-rebrand) —
    # left out entirely rather than guessed at, per this dict's own rule.
    # Confirmed NOT present in the fingerprint set (checked directly, not
    # assumed, re-verified 2026-09): Ashby, Rippling, Folks HR, Softgarden,
    # ClearCompany/HRMDirect, ADP, Taleo, SuccessFactors, BrassRing,
    # ApplyToJob, join.com, Oracle Cloud HCM/Fusion Recruiting, Pinpoint,
    # Flatchr, and Occupop.
    # These platforms just aren't in Wappalyzer's ruleset — this source
    # can't help with them regardless of query design.
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
    # New (2026-09) — PageUp (AU/NZ, HIGHEST priority of this batch),
    # Pinpoint (UK), Flatchr (France), Jobylon (Nordics), Homerun
    # (Netherlands). All 5 confirmed to have a genuinely working scraper
    # (server-rendered HTML or a real public JSON API — see
    # ats_scrapers.py for each). Occupop (Ireland) deliberately NOT added
    # here — see the BLACKLISTED comment below.
    "pageup", "pinpoint", "flatchr", "jobylon", "homerun",
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
# occupop (2026-09): every checked customer subdomain
# ({slug}.occupop-careers.com) is a JS-rendered SPA shell with zero job
# data in the raw HTML — no confirmed public unauthenticated API (the
# official api.occupop.com/rest/jobs endpoint requires a Bearer token,
# confirmed via a live 403). Genuinely not scrapeable with this project's
# plain-HTTP architecture without further investigation (e.g. a headless-
# browser network trace to find whatever XHR call the SPA itself makes).
# Kept OUT of SUPPORTED_ATS on purpose rather than shipping a scraper that
# would silently return zero jobs for every real company.
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
    # New (2026-09):
    "pageup": "pageup",
    "pinpoint": "pinpoint",
    "flatchr": "flatchr",
    "jobylon": "jobylon",
    "homerun": "homerun",
    # "occupop" deliberately NOT mapped here — occupop is not in
    # SUPPORTED_ATS (no working scraper yet, see that comment), and
    # fetch_openpostings_slugs()'s slugs_by_ats dict is only pre-seeded
    # with SUPPORTED_ATS keys — mapping an ATS name here that isn't in
    # SUPPORTED_ATS would KeyError the very first time OpenPostings
    # actually contains an Occupop row. Add this mapping back once/if a
    # working scraper lands and occupop joins SUPPORTED_ATS.
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
    if slug and slug.lower() not in SKIP_SLUGS and _looks_like_real_slug(slug):
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
        if slug and slug.lower() not in SKIP_SLUGS and _looks_like_real_slug(slug):
            return slug
    return None


def _url_to_slug_ashby(url: str) -> str | None:
    """2026-09: confirmed live 118 archive_i rows stored with a literal
    "%20" (and other percent-escapes) instead of a decoded space/char —
    e.g. "Abode%20Money" — because `parsed.path` is never decoded by
    urlparse; org names with spaces or punctuation come through Ashby's
    URL still percent-encoded, and this was stored as-is. unquote() here
    normalizes it to the real org name (matching what a browser/API
    consumer would see), so this stops silently doubling up storage for
    the same company under an encoded vs. would-be-decoded spelling."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "ashbyhq.com" in host:
        parts = parsed.path.strip("/").split("/")
        slug = unquote(parts[0]) if parts and parts[0] else None
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
        # 2026-09: confirmed 61 real archive_i rows where site_id was an
        # asset request caught on this same host (favicon.ico) — e.g.
        # "2fasmglobal|favicon.ico" — because site_id was trusted
        # unconditionally. Same guard as rippling/pageup's fix below.
        if company and wd and site_id and _looks_like_real_slug(site_id):
            return f"{company}|{wd}|{site_id}"
    return None


# 2026-09: real archive_i rows confirmed this was mis-extracting Rippling
# logo/asset URLs as if they were company slugs — e.g.
# ats.rippling.com/fd30a211c9541cb8751de95c09ed87e98ac64a91.png stored the
# literal hash+extension as the "slug". Root cause: Pattern 1 took
# parts[0] UNCONDITIONALLY, despite its own comment saying the real shape
# is "{company}/jobs" — nothing ever checked that "jobs" was actually
# anywhere in the path, so a bare one-segment asset URL with no "/jobs"
# at all matched just as happily as a real board link. Fixed two ways,
# belt-and-suspenders: (1) Pattern 1 now requires "jobs" to actually
# appear later in the path, and (2) both patterns reject a candidate that
# LOOKS like an asset filename (known extension) or a bare hex hash
# (logo/image ids on this ATS, confirmed 5-for-5 in the real bad rows) —
# so a future asset URL shape this project hasn't seen yet still can't
# sneak through pattern (1) alone.
_ASSET_FILENAME_RE = re.compile(
    r"\.(png|jpe?g|gif|svg|webp|ico|bmp|css|js|mjs|woff2?|ttf|eot|pdf|mp4|webm|json|map)$",
    re.I,
)
_BARE_HEX_HASH_RE = re.compile(r"^[0-9a-f]{16,64}$", re.I)


def _looks_like_real_slug(candidate: str) -> bool:
    """Shared guard for path-segment-based extractors: rejects the two
    confirmed-in-production shapes of "this isn't a company slug, it's an
    asset" — a filename with a known static-asset extension, or a bare
    hex hash/id with no extension at all (e.g. a CDN object key)."""
    if not candidate:
        return False
    if _ASSET_FILENAME_RE.search(candidate):
        return False
    if _BARE_HEX_HASH_RE.match(candidate):
        return False
    return True


def _url_to_slug_rippling(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "rippling.com" in host:
        # Pattern 1: ats.rippling.com/{company}/jobs[/...] — "jobs" must
        # actually be present somewhere after the company segment, not
        # just assumed from parts[0] alone (see the comment above).
        parts = [p for p in parsed.path.strip("/").split("/") if p]
        if (parts and "jobs" in [p.lower() for p in parts[1:]]
                and parts[0].lower() not in SKIP_SLUGS
                and _looks_like_real_slug(parts[0])):
            return parts[0]
        # Pattern 2: {company}.rippling.com (subdomain-based)
        slug = host.replace(".rippling.com", "").lower()
        if (slug and slug not in SKIP_SLUGS and slug not in ("www", "app", "ats")
                and _looks_like_real_slug(slug)):
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
            if (slug and slug not in SKIP_SLUGS and slug not in _WORKABLE_RESERVED_PATH_TOKENS
                    and _looks_like_real_slug(slug)):
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
        if (slug.lower() not in SKIP_SLUGS and slug.lower() not in ("jobs", "careers", "posting")
                and _looks_like_real_slug(slug)):
            return slug
    return None


def _url_to_slug_taleo(url: str) -> str | None:
    """Handles TWO structurally distinct real Taleo products, confirmed
    live 2026-09:
      1. "Career Section" (OTM) — the original/classic product:
         {company}.taleo.net/careersection/{section}/jobsearch.ftl or
         .../jobdetail.ftl (e.g. capps.taleo.net/careersection/ex/
         jobdetail.ftl?job=00055524). Slug: '{company}|{section}'.
      2. Taleo Business Edition (TBE) — a SEPARATE product with a totally
         different host suffix (.tbe.taleo.net, not bare .taleo.net) AND
         path shape (/{siteCode}/ats/careers/v2/{jobSearch,searchResults,
         viewRequisition}?org={company}), e.g. tre.tbe.taleo.net/tre01/
         ats/careers/v2/jobSearch?org=NVRINC&cws=52. The company identity
         here is the `org` query param, NOT anything in the host or path
         — previously this whole product family returned None from every
         URL (no /careersection/ segment exists in TBE's path at all),
         silently missing every TBE customer even after TBE's own CDX
         query patterns were added, since discovery and extraction are two
         separate steps that both need to agree on the URL shape. Slug:
         '{tbe_instance}|{org}' (tbe_instance keeps this distinguishable
         from a same-named org on the classic product, since TBE and
         Career Section are unrelated Oracle products with independent
         customer bases)."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host.endswith(".tbe.taleo.net"):
        if "/ats/careers/v2/" not in parsed.path.lower():
            return None
        qs = parse_qs(parsed.query)
        org = (qs.get("org") or [None])[0]
        tbe_instance = host[: -len(".tbe.taleo.net")]
        if org and tbe_instance and org.lower() not in SKIP_SLUGS:
            return f"{tbe_instance}|{org}"
        return None
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
    """2026-09: confirmed live archive_i rows with a trailing space baked
    into the stored slug (e.g. "career8.successfactors.com|management ")
    — root cause: `company_key`/`path_match.group(1)` were never
    stripped, so a source URL with a raw (technically invalid, but real-
    world-common) unencoded space in its query string carried that space
    straight through into the stored slug. .strip() added at every return
    point that builds a slug from external input."""
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
            return f"{host}|{company_key.strip()}"
        # Fallback: extract subdomain as instance
        instance = host.split(".")[0]
        if instance and instance not in SKIP_SLUGS:
            # Try extracting company from path
            path_match = re.search(r"/career\?company=([^&]+)", url)
            if path_match:
                return f"{instance}|{path_match.group(1).strip()}"
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
    """2026-09: added .clearcompany.com — HRMDirect was acquired by/
    rebranded as ClearCompany, and this is the domain family the code's
    own _OPENPOSTINGS_ATS_MAP_RAW already anticipated (maps OpenPostings'
    "clearcompany" -> this "hrmdirect" key) without actually recognizing
    the domain anywhere. Confirmed real and in active use via 12 distinct
    live customer subdomains, all sharing the identical /careers/portal
    path shape (e.g. drbronners.clearcompany.com, hunter.clearcompany.com,
    laseraway.clearcompany.com) — old code only matched hrmdirect.com and
    silently missed this entire, now more common, domain family."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    for suffix in (".hrmdirect.com", ".clearcompany.com"):
        if host.endswith(suffix):
            slug = host[: -len(suffix)].lower()
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
    .zohorecruit.com and silently missed every EU-region customer.
    2026-09: .zohorecruit.in (India data-center domain) was briefly added
    and then deliberately REMOVED — this scanner is scoped to the 18
    countries in OpenData/opendata_seed.py's DEFAULT_COUNTRIES, which does
    not include India, so an India-only regional domain is out of scope
    here regardless of how many live customer boards it has. Japan
    customers use a subdomain of the existing .com domain (e.g.
    zohojapan.zohorecruit.com), already covered by the .com suffix below,
    so no .jp/.cn suffix is needed either.
    2026-09: added .zohorecruit.com.au — confirmed real, in-scope-country
    (Australia IS in DEFAULT_COUNTRIES) live customer board
    (crossapac.zohorecruit.com.au). This is genuinely a separate suffix
    from plain ".zohorecruit.com" (host.endswith(".zohorecruit.com") is
    False for a ".com.au" host — the old suffix list silently missed every
    Australia-region customer even though Australia is squarely in
    scope)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    for suffix in (".zohorecruit.com", ".zohorecruit.eu", ".zohorecruit.com.au"):
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
        if slug not in SKIP_SLUGS and _looks_like_real_slug(slug):
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
        if (client_id.lower() not in SKIP_SLUGS and board_slug.lower() not in SKIP_SLUGS
                and _looks_like_real_slug(client_id) and _looks_like_real_slug(board_slug)):
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


# 2026-09: real archive_i rows confirmed a real, live source of garbage
# here — 34 of ~12k stored ADP slugs weren't a clean "{cid}|{ccId}" pair
# at all, e.g. literal Word "HYPERLINK" field-code text, doubled/nested
# URLs, HTML entities (&lang;, &lt;br&gt;), stray whitespace inside a
# UUID, and truncation ellipses ("c858a3[…]bf") all ended up INSIDE the
# stored cid/ccId values. Root cause: `parse_qs` faithfully returns
# whatever raw text sits between "cid=" and the next "&" (or end of
# string) with no shape check at all — and some real captured pages
# (Wayback/Common Crawl) have a malformed second "?...cid=..." embedded
# in what was scraped as a single query value, e.g. from a pasted-Word
# job posting whose "link" is literal visible text rather than a real
# `<a href>`. urlparse/parse_qs can't tell that apart from a genuinely
# messy-but-real query string — so this only catches it by validating
# the RESULT looks like ADP's actual cid/ccId shape before trusting it,
# same principle as _looks_like_real_slug above.
_ADP_CID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_ADP_CCID_RE = re.compile(r"^\d+_\d+$")


def _url_to_slug_adp(url: str) -> str | None:
    """Extract slug from ADP Workforce Now career-center URLs.
    Both 'cid' and 'ccId' query params are required to hit the public
    job-requisitions API — our internal slug format is '{cid}|{ccId}'.
    Both are validated against ADP's own confirmed shapes (cid: a UUID;
    ccId: digits_digits) before being trusted — see the comment above for
    the real garbage this rejects."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "adp.com" not in host:
        return None
    qs = parse_qs(parsed.query)
    cid = (qs.get("cid") or qs.get("CID") or [None])[0]
    cc_id = (qs.get("ccId") or qs.get("ccid") or qs.get("CCID") or [None])[0]
    if cid and cc_id and _ADP_CID_RE.match(cid.strip()) and _ADP_CCID_RE.match(cc_id.strip()):
        return f"{cid.strip()}|{cc_id.strip()}"
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
        # Same shape validation as _url_to_slug_adp — this is ADP's own
        # redirect response, not scraped page text, so garbage here is
        # less likely, but there's no reason to trust it any less
        # carefully than the other path just because the source differs.
        if cid and _ADP_CID_RE.match(cid.strip()) and _ADP_CCID_RE.match(cc_id.strip()):
            return f"{cid.strip()}|{cc_id.strip()}"
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


def _url_to_slug_pageup(url: str) -> str | None:
    """Extract slug from PageUp URLs (AU/NZ enterprise ATS — Telstra,
    Commonwealth Bank, Coles, etc.). Only handles the shared
    careers.pageuppeople.com domain — customers on a fully custom domain
    (e.g. careers.telstra.com) front the same backend but carry no
    PageUp-specific path shape to key off of; those are only detectable
    live via the careers-static.pageuppeople.com script-src Wappalyzer
    fingerprint (see HTTPARCHIVE_ATS_TECH_NAMES), not from a bare URL.

    Pattern: careers.pageuppeople.com/{portalId}/{source}/{lang}/...
    (also /job/{jobId}/{slug} for individual postings — portalId/source
    are still the first two path segments there). Our internal slug
    format is '{portalId}|{source}' — both are required since PageUp
    boards are namespaced per-portal, not by company name alone.

    2026-09: the legacy /{portalId}/ci/{lang} source shape is blocked by
    PageUp's own robots.txt (matches a '/ci' disallow rule); the newer
    /{portalId}/fb/{lang} shape is not — prefer 'fb' when both are seen
    for the same portalId, but this extractor itself is shape-agnostic
    and just returns whatever 'source' segment is actually in the URL."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "pageuppeople.com" not in host:
        return None
    parts = parsed.path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] and parts[1]:
        portal_id, source = parts[0], parts[1]
        if (portal_id.lower() not in SKIP_SLUGS and source.lower() not in SKIP_SLUGS
                and _looks_like_real_slug(portal_id) and _looks_like_real_slug(source)):
            return f"{portal_id}|{source}"
    return None


def _url_to_slug_pinpoint(url: str) -> str | None:
    """Extract slug from Pinpoint URLs (UK ATS).
    Pattern: {company}.pinpointhq.com/... — the subdomain alone is the
    slug; Pinpoint's public JSON API (postings.json) is keyed by the same
    subdomain, no further path parsing needed."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host.endswith(".pinpointhq.com"):
        return None
    slug = host[: -len(".pinpointhq.com")].lower()
    if slug and slug not in SKIP_SLUGS and slug != "www":
        return slug
    return None


def _url_to_slug_flatchr(url: str) -> str | None:
    """Extract slug from Flatchr URLs (France).
    Two real URL families, both confirmed live:
      1. Shared board domain: {slug}.flatchr.io/...
      2. Company page: careers.flatchr.io/company/{slug}
         (job postings under careers.flatchr.io/vacancy/{slug}/... use
         the same company slug as the first path segment)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host.endswith(".flatchr.io") and host != "careers.flatchr.io":
        slug = host[: -len(".flatchr.io")].lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
        return None
    if host == "careers.flatchr.io":
        parts = parsed.path.strip("/").split("/")
        if len(parts) >= 2 and parts[0] in ("company", "vacancy") and parts[1]:
            slug = parts[1].lower()
            if slug not in SKIP_SLUGS:
                return slug
    return None


def _url_to_slug_jobylon(url: str) -> str | None:
    """Extract slug from Jobylon URLs (Nordics).
    Pattern: emp.jobylon.com/companies/{id}-{slug}/ — only the shared
    emp.jobylon.com domain's /companies/ path carries a company
    identifier; customers on a fully custom domain (footer-credit only)
    aren't discoverable from a bare URL and rely on the
    careers-static-style Wappalyzer fingerprint instead (see
    HTTPARCHIVE_ATS_TECH_NAMES). Our internal slug is the whole
    '{id}-{slug}' segment — scrape_jobylon needs the numeric id (not
    just the human-readable slug) to enumerate that company's jobs via
    the sitemap."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host != "emp.jobylon.com":
        return None
    parts = parsed.path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "companies" and parts[1]:
        slug = parts[1].lower()
        if slug not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_homerun(url: str) -> str | None:
    """Extract slug from Homerun URLs (Netherlands).
    Unlike most platforms here, Homerun customers run on their OWN
    domain (conventionally jobs.{company-domain}, e.g.
    jobs.acme.com) rather than a shared {company}.homerun.co subdomain
    — there is no company-name segment to parse out of the URL at all,
    so the full hostname itself IS the slug (this is safe: node.py/
    ats_scrapers.py only ever use this slug to reconstruct the same
    hostname when fetching, never to look up a shared-domain subdomain).
    Only matches hosts that actually look like a Homerun careers site
    (a 'jobs.' subdomain) to avoid capturing an unrelated URL that just
    happens to flow through this converter.

    2026-09: found (via the new Latmay H.F/Edward H.F sources' own unit
    tests) to be over-broad enough to steal Dayforce's
    jobs.dayforcehcm.com — the ONE OTHER platform in this file that
    also happens to use a 'jobs.' subdomain, but on a SHARED vendor
    domain (dayforcehcm.com), not each customer's own domain the way
    Homerun actually works. Since URL_TO_SLUG is iterated in
    insertion order and this entry comes before "dayforce" in the
    dict, every dayforce URL was silently resolving to a bogus
    "homerun" slug (the literal hostname jobs.dayforcehcm.com) instead
    of ever reaching _url_to_slug_dayforce — a real, pre-existing bug,
    not something introduced by the new sources; it just took a URL
    outside this file's earlier live-crawl code paths (which happened
    to never hit this specific host) to surface it. Excluding the one
    known shared-domain collision here is the minimal, safe fix —
    _url_to_slug_dayforce's own host check already requires the exact
    jobs.dayforcehcm.com host, so nothing about Dayforce resolution
    depends on this exclusion, only Homerun's false-positive on it."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host or host in SKIP_SLUGS:
        return None
    if host.startswith("jobs.") and host != "jobs.dayforcehcm.com":
        slug = host
        if slug not in SKIP_SLUGS:
            return slug
    return None


def _url_to_slug_occupop(url: str) -> str | None:
    """Extract slug from Occupop URLs (Ireland).
    Pattern: {company-slug}.occupop-careers.com/... — NOT occupop.com,
    which now redirects to cezannehr.com post-rebrand ("Cezanne
    Recruitment, powered by Occupop"). NOTE (2026-09): every checked
    customer page on this domain is a JS-rendered SPA shell with zero
    job data in the raw HTML and no confirmed public API — this
    converter is kept so slugs can still be discovered/stored, but see
    scrape_occupop's docstring for the real scraping-feasibility gap."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host.endswith(".occupop-careers.com"):
        return None
    slug = host[: -len(".occupop-careers.com")].lower()
    if slug and slug not in SKIP_SLUGS and slug != "www":
        return slug
    return None


# 2026-09: Dayforce / Getro / JazzHR added — the top 3 platforms by volume
# in the latmay/ats-career-page-urls HF dataset (2181/1804/1325 rows
# respectively) that this project didn't already recognize. URL shapes
# below are confirmed against REAL sample rows pulled live from that
# dataset (not guessed from memory) — see this session's own research:
#   Dayforce: jobs.dayforcehcm.com/api/geo/associated, /api/geo/e0229, ...
#   Getro:    getro.getro.com/, 1up.getro.com/, 3m.getro.com/, ...
#   JazzHR:   l2t.applytojob.com/, 10xhealthsystem.applytojob.com/, ...
# These three are SLUG-DISCOVERY ONLY for now — none are in SUPPORTED_ATS,
# and no ats_scrapers.py scraper was added for them. Dayforce/Getro are
# brand new here and would need their own real job-listing-API research
# (this dataset only confirms the CAREERS-PAGE URL shape, not the
# underlying jobs API) before a scraper could be written responsibly.
# JazzHR is a special case: it's the SAME platform as the old "applytojob"
# entry removed 2026-08 (see SUPPORTED_ATS comment above) — that removal
# was for a JD-enrichment/US-eligibility-filtering reliability problem in
# the SCRAPER, not the URL pattern, so re-enabling scraping here would
# resurrect that same known issue unless it's actually fixed first.
def _url_to_slug_dayforce(url: str) -> str | None:
    """Dayforce (Ceridian) — ALL customers share one domain
    (jobs.dayforcehcm.com); the tenant code is the last /api/geo/{tenant}
    path segment, not a subdomain."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host != "jobs.dayforcehcm.com":
        return None
    m = re.match(r"^/api/geo/([^/]+)/?$", parsed.path)
    if not m:
        return None
    tenant = m.group(1)
    if tenant.lower() not in SKIP_SLUGS and _looks_like_real_slug(tenant):
        return tenant
    return None


def _url_to_slug_getro(url: str) -> str | None:
    """Getro — subdomain-per-tenant on getro.com (VC-portfolio/talent-
    network job boards)."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host.endswith(".getro.com"):
        return None
    tenant = host[: -len(".getro.com")]
    if (tenant and tenant not in ("www", "app") and tenant not in SKIP_SLUGS
            and _looks_like_real_slug(tenant)):
        return tenant
    return None


def _url_to_slug_jazzhr(url: str) -> str | None:
    """JazzHR — subdomain-per-tenant on applytojob.com. Same platform as
    the old 'applytojob' entry removed 2026-08 for a scraper-side JD-
    filtering issue (see comment above) — URL pattern itself is unaffected
    and was always correct."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host.endswith(".applytojob.com"):
        return None
    tenant = host[: -len(".applytojob.com")]
    if tenant and tenant != "www" and tenant not in SKIP_SLUGS and _looks_like_real_slug(tenant):
        return tenant
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
    # New (2026-09): PageUp / Pinpoint / Flatchr / Jobylon / Homerun / Occupop
    "pageup": _url_to_slug_pageup,
    "pinpoint": _url_to_slug_pinpoint,
    "flatchr": _url_to_slug_flatchr,
    "jobylon": _url_to_slug_jobylon,
    "homerun": _url_to_slug_homerun,
    "occupop": _url_to_slug_occupop,
    # New (2026-09): slug-discovery only, see the block comment above these
    # three functions — no scraper/SUPPORTED_ATS entry yet.
    "dayforce": _url_to_slug_dayforce,
    "getro": _url_to_slug_getro,
    "jazzhr": _url_to_slug_jazzhr,
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
                # 2026-09: this upstream repo's own lists have turned out to
                # contain garbage of exactly the same shape node.py's own
                # extractors were fixed for this session (bare hex hashes —
                # confirmed live via archive_i rows like a greenhouse/ashby
                # "slug" that's actually a raw internal object id, not a
                # company name) — Feashliaa's data isn't immune just
                # because it skips our own URL-parsing code. Same guard,
                # applied here instead of at extraction time since there's
                # no URL to parse in the first place.
                clean = {s.strip() for s in data
                         if isinstance(s, str) and s.strip()
                         and s.strip().lower() not in SKIP_SLUGS
                         and _looks_like_real_slug(s.strip())}
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
    # 2026-09: added boards.eu.greenhouse.io — Greenhouse's EU-data-residency
    # board host, confirmed live (boards.eu.greenhouse.io/embed/job_board/
    # js?for=interpetrolsa) and already anticipated by _url_to_slug_greenhouse's
    # own docstring — the extractor's substring host check already handles
    # it, only this query pattern was missing.
    # 2026-09: added job-boards.greenhouse.io(+.eu) — Greenhouse's newer
    # "Job Boards 2.0" hosted-board domain, confirmed real and CURRENTLY
    # GROWING (Greenhouse's own support docs describe a legacy
    # boards.greenhouse.io deprecation plan) via live examples
    # (job-boards.greenhouse.io/remotecom, /hubspotjobs, /current81,
    # job-boards.eu.greenhouse.io/openup). Missing this domain would mean
    # missing an increasing share of current/future Greenhouse customers,
    # not just a handful of edge cases — kept the legacy boards.* patterns
    # too since that domain still carries huge historical volume and isn't
    # fully retired. _url_to_slug_greenhouse's substring host check
    # ("greenhouse.io" in host) already matches this domain with no
    # extractor change needed.
    "greenhouse": ["boards.greenhouse.io/*", "boards.eu.greenhouse.io/*",
                   "job-boards.greenhouse.io/*", "job-boards.eu.greenhouse.io/*"],
    # 2026-09: added jobs.eu.lever.co — Lever's own EU-hosted board domain,
    # confirmed live (Lever's OWN careers page is hosted there:
    # jobs.eu.lever.co/lever, plus 3 other distinct customer boards).
    # _url_to_slug_lever's .endswith(".lever.co") already matches this.
    "lever": ["jobs.lever.co/*", "jobs.eu.lever.co/*"],
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
    # 2026-09: replaced the bare /jobs* pattern with */jobs* — the
    # extractor's own primary/most-common shape is ats.rippling.com/
    # {company}/jobs (company slug FIRST, then /jobs), confirmed live
    # (ats.rippling.com/skillable-careers/jobs/9157db7b-...); the old
    # "/jobs*" pattern requires "jobs" as the literal first path segment,
    # which never matches that shape at all. Kept /careers* alongside it
    # for the extractor's separate {company}.rippling.com subdomain form.
    "rippling": ["*.rippling.com/careers*", "*.rippling.com/*/jobs*"],
    # 2026-09: added the */jobs* locale-prefixed form alongside the bare
    # /jobs* one — Teamtailor's own multi-language career-site docs
    # (support.teamtailor.com "Career sites in multiple languages")
    # confirm a locale code gets prepended to the path for translated
    # sites (e.g. {company}.teamtailor.com/no/jobs/..., /de/jobs/...),
    # which the old bare "/jobs*" pattern (requiring /jobs immediately
    # after the domain) can't match at all — real impact given
    # Teamtailor's heavy Nordic/European multi-language customer base.
    # _url_to_slug_teamtailor already extracts the slug from the
    # SUBDOMAIN, not the path, so no extractor change is needed — this is
    # purely a CDX query-pattern widening.
    "teamtailor": ["*.teamtailor.com/jobs*", "*.teamtailor.com/*/jobs*"],
    "breezyhr": ["*.breezy.hr/*"],
    # "applytojob" removed 2026-08 — see SUPPORTED_ATS comment above.
    "personio": ["*.jobs.personio.de/*", "*.jobs.personio.com/*"],
    "joincom": ["join.com/companies/*/jobs*", "join.com/companies/*"],
    # Newly enabled platforms:
    # 2026-09: added jobdetail.ftl — confirmed to be the dominant real-world
    # family (6 distinct live customer job-detail pages found, none using
    # jobsearch.ftl) since individual job postings, not the generic search
    # form, are what actually gets linked/shared externally for Common
    # Crawl to discover. _url_to_slug_taleo's regex only looks at the
    # /careersection/{section}/ segment and doesn't care what filename
    # follows, so no extractor change needed.
    # 2026-09: added the 3 Taleo Business Edition (TBE) patterns — a wholly
    # separate Oracle product from the classic "Career Section" patterns
    # above (different host suffix .tbe.taleo.net, different path shape
    # entirely), confirmed via real live customers (tre.tbe.taleo.net/
    # tre01/ats/careers/v2/jobSearch?org=NVRINC, phg.tbe.taleo.net/phg01/
    # ats/careers/v2/searchResults?org=BVHS, City of Delta's .../
    # viewRequisition?org=XNZ8Q7&rid=1737). _url_to_slug_taleo was updated
    # alongside this to actually parse TBE's shape (org= query param) —
    # adding the query pattern alone would have been a silent no-op trap,
    # since the old extractor only recognized /careersection/ URLs.
    "taleo": ["*.taleo.net/careersection/*/jobsearch.ftl*", "*.taleo.net/careersection/*/jobdetail.ftl*",
              "*.tbe.taleo.net/*/ats/careers/v2/jobSearch*", "*.tbe.taleo.net/*/ats/careers/v2/searchResults*",
              "*.tbe.taleo.net/*/ats/careers/v2/viewRequisition*"],
    "oracle_cloud_hcm": ["*.oraclecloud.com/hcmUI/CandidateExperience/*"],
    # 2026-09: added the capitalized-path form — real live customer URLs
    # confirmed to commonly use .../Recruiting/Jobs/... (capitalized), not
    # just the all-lowercase form; Common Crawl's CDX index path matching
    # is case-sensitive so the old lowercase-only pattern silently missed
    # these even though _url_to_slug_paylocity's own path comparison is
    # already case-insensitive (would have parsed them fine once found —
    # this was purely a discovery-side gap, not an extractor one). Also
    # added a leading-wildcard host form — one confirmed live example
    # (2000recruiting.paylocity.com) uses a numeric-prefixed subdomain
    # instead of the bare recruiting.paylocity.com host; weaker/single-
    # example evidence but the extractor's own host check is already a
    # substring match ("paylocity.com" in host), so this costs nothing in
    # false positives to also query for.
    "paylocity": ["recruiting.paylocity.com/recruiting/jobs/*", "recruiting.paylocity.com/Recruiting/Jobs/*",
                  "*recruiting.paylocity.com/*ecruiting/Jobs/*"],
    # 2026-09: added *.clearcompany.com/careers/portal* — see
    # _url_to_slug_hrmdirect's updated docstring for why (rebrand, 12
    # confirmed live customer subdomains, entirely missing before).
    "hrmdirect": ["*.hrmdirect.com/employment/*", "*.clearcompany.com/careers/portal*"],
    # 2026-08: added the .eu region domain — confirmed real, in-active-use
    # (multiple distinct live customer boards found on zohorecruit.eu).
    # .zohorecruit.in (India) deliberately excluded — out of scope, see
    # _url_to_slug_zoho's docstring.
    # 2026-09: added .zohorecruit.com.au — confirmed real Australia-region
    # customer (crossapac.zohorecruit.com.au, Australia IS in scope,
    # unlike India). _url_to_slug_zoho's suffix list was updated alongside
    # this — the old ".zohorecruit.com" endswith-check does NOT match a
    # ".com.au" host, so adding just the query pattern without the
    # extractor fix would have been a silent no-op.
    "zoho": ["*.zohorecruit.com/jobs/*", "*.zohorecruit.eu/jobs/*", "*.zohorecruit.com.au/jobs/*"],
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
    # NEW (2026-09): BrassRing had NO Common Crawl discovery at all before
    # this — a real gap, since the scraper for it (scrape_brassring) is
    # actively re-enabled/working, unlike SuccessFactors below. Confirmed
    # live customer examples: sjobs.brassring.com/TGnewUI/Search/Home/Home
    # (Lowe's, Kodak), krb-sjobs.brassring.com/TGnewUI/Search/Home/Home
    # (IBM, Ahold) — a leading wildcard subdomain catches both the
    # standard "sjobs." and the "krb-sjobs." enterprise-customer variant.
    # _url_to_slug_brassring extracts entirely from the partnerid/siteid
    # query params, not the path, so this one pattern is sufficient
    # regardless of which exact path/casing (TGnewUI vs TGNewUI) a
    # specific customer's URL happens to use.
    "brassring": ["*.brassring.com/TGnewUI/*"],
    # SuccessFactors deliberately has NO Common Crawl pattern — confirmed
    # 2026-09 still genuinely blocked from scraping (SAP's own Career Site
    # Builder architecture renders job listings client-side via an OData
    # call, not present in the initial HTML; no evidence this has changed).
    # Discovering SuccessFactors slugs via Common Crawl would be pure
    # wasted effort while the scraper itself can't turn them into job
    # data — see ats_scrapers.py's scrape_successfactors comment for why
    # it's blacklisted. Revisit only if that scraping blocker is ever lifted.
    # New (2026-09): PageUp / Pinpoint / Flatchr / Jobylon — all 4 have a
    # real shared-domain URL shape to query for. Homerun deliberately has
    # NO entry here — its customers run on their OWN domain (jobs.
    # {company-domain}), not a shared *.homerun.co subdomain, so there is
    # no single host pattern to query Common Crawl for; Occupop also has
    # NO entry here — same reasoning as SuccessFactors above (confirmed
    # JS-rendered SPA, no working scraper yet, so discovering slugs for it
    # would be wasted effort until that's fixed).
    "pageup": ["careers.pageuppeople.com/*"],
    "pinpoint": ["*.pinpointhq.com/*"],
    "flatchr": ["*.flatchr.io/*", "careers.flatchr.io/company/*"],
    "jobylon": ["emp.jobylon.com/companies/*"],
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
    # New (2026-09): brassring — see the CC_PLATFORM_PATTERNS entry above
    # for why this platform had no Common Crawl discovery at all before.
    # No SuccessFactors entry here on purpose — kept out of BOTH dicts
    # together, matching keys as this dict's own comment above requires.
    "brassring": _url_to_slug_brassring,
    # New (2026-09) — kept in sync with CC_PLATFORM_PATTERNS above (no
    # Homerun/Occupop entries here either — see that dict's comment):
    "pageup": _url_to_slug_pageup,
    "pinpoint": _url_to_slug_pinpoint,
    "flatchr": _url_to_slug_flatchr,
    "jobylon": _url_to_slug_jobylon,
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
    source in the matrix (27 platforms x up to 6 crawls x however many
    patterns each, all sequential within one job) and splitting it in two
    actually cuts wall-clock, unlike WDC which was just spending runtime
    for near-nothing. Pass cc_shard=None (default) to run all platforms
    in one call, same as before this existed.
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

# 2026-09: robots.txt rules are now cached per (base_url, user_agent) for the
# life of the process instead of re-fetched on every call. HTTP Archive's
# resolve step routinely calls this twice for the SAME host in one candidate
# (once for the exact confirmed page in resolve_candidate_page_to_ats_slug,
# again for the homepage in resolve_company_to_ats_slug's fallback) — that
# was a guaranteed-duplicate, fully-serial robots.txt fetch (up to 15s each)
# for every single candidate that fell through to the fallback, on top of
# whatever legitimate cross-candidate host repeats exist. Caching the parsed
# rule list (not the per-path decision, since different calls check
# different paths on the same host) turns that into one fetch per host per
# run — a real, measurable chunk of this source's wall-clock cost, not just
# a log-visibility issue.
_robots_rules_cache: dict[tuple[str, str], list[str] | None] = {}
_robots_cache_lock = threading.Lock()
_ROBOTS_FETCH_FAILED = object()  # sentinel distinguishing a cached failure from a cached "no rules"


def _fetch_robots_rules(base_url: str, user_agent: str) -> list[str] | None:
    """Fetch+parse {base_url}/robots.txt once, cached thereafter for this
    process. Returns the list of Disallow patterns applicable to '*'/our UA,
    or None if the site has no robots.txt (or one that doesn't apply) —
    None means 'allow everything', distinct from an empty-but-fetched list
    which also means allow everything but for a different reason (fetched
    fine, no applicable rules). Raises on fetch/parse failure so the caller
    can distinguish "confirmed allowed" from "couldn't confirm" and fail
    closed, same policy as before."""
    cache_key = (base_url, user_agent)
    with _robots_cache_lock:
        if cache_key in _robots_rules_cache:
            cached = _robots_rules_cache[cache_key]
            if cached is _ROBOTS_FETCH_FAILED:
                raise RuntimeError("cached robots.txt fetch failure")
            return cached
    try:
        r = requests.get(f"{base_url}/robots.txt", timeout=15,
                          headers={"User-Agent": user_agent})
        if r.status_code >= 400:
            # No robots.txt at all is conventionally "allow everything"
            rules: list[str] | None = None
        else:
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
            rules = applicable_disallows
        with _robots_cache_lock:
            _robots_rules_cache[cache_key] = rules
        return rules
    except Exception:
        with _robots_cache_lock:
            _robots_rules_cache[cache_key] = _ROBOTS_FETCH_FAILED
        raise


def _robots_allows(base_url: str, path: str, user_agent: str = _ROBOTS_UA) -> bool:
    """Minimal robots.txt check: verify `path` isn't disallowed for '*' or
    our own UA, using a per-host cached copy of {base_url}/robots.txt (see
    _fetch_robots_rules). Fails CLOSED (returns False) on any fetch/parse
    error — if we can't confirm it's allowed, we don't proceed. This
    mirrors the same non-negotiable policy already applied to UKG (excluded
    from ats_scrapers.py for exactly this)."""
    try:
        applicable_disallows = _fetch_robots_rules(base_url, user_agent)
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
    if not applicable_disallows:
        return True
    allowed = not any(path.startswith(d) for d in applicable_disallows)
    if not allowed:
        _robots_check_stats["disallowed_by_rule"] += 1
    return allowed


_WAYBACK_MAX_PAGES = 25  # safety cap on resumeKey pagination, see below


def _fetch_wayback_cdx_urls(pattern: str, page_limit: int) -> list[str]:
    """One CDX pattern, fully paginated via resumeKey.

    2026-09 bug fix: the old code sent one request with a flat `limit`
    (5000) and NO pagination at all — any pattern with more than 5000
    archived snapshots silently truncated there, with no warning, and
    (worse) CDX's own snapshot ordering isn't guaranteed to be "most
    useful first," so which 5000 got kept was arbitrary. `workforcenow.
    adp.com/mascsr/default/mdf/recruitment/recruitment.html*` alone is
    exactly the kind of high-volume, long-lived pattern likely to have
    cleared 5000 archived snapshots over the pattern's lifetime — a real,
    plausible source of the under-coverage this source was flagged for.
    `showResumeKey` + a resumeKey follow-up loop (IA's own documented CDX
    pagination mechanism) now keeps fetching until a page comes back
    without a resume key, capped at _WAYBACK_MAX_PAGES as a hard safety
    stop against a runaway loop (logs a warning if that cap is actually
    hit, rather than truncating silently like before)."""
    urls: list[str] = []
    resume_key = None
    for page_num in range(1, _WAYBACK_MAX_PAGES + 1):
        params = {
            "url": pattern,
            "output": "json",
            "fl": "original",
            "collapse": "urlkey",
            "limit": page_limit,
            "showResumeKey": "true",
        }
        if resume_key:
            params["resumeKey"] = resume_key
        try:
            r = requests.get(WAYBACK_CDX_URL, params=params, timeout=60,
                              headers={"User-Agent": _ROBOTS_UA})
            r.raise_for_status()
            rows = r.json()
        except Exception as e:
            log.warning(f"Wayback CDX query failed for {pattern} (page {page_num}): {e}")
            break
        if not rows or not isinstance(rows, list):
            break

        # A resumeKey page is: [header, ...data rows..., [], [resume_key]]
        # — an empty row followed by a one-element row. Anything else
        # means this was the final page.
        data_rows = rows[1:]
        next_resume_key = None
        if len(data_rows) >= 2 and data_rows[-2] == [] and len(data_rows[-1]) == 1:
            next_resume_key = data_rows[-1][0]
            data_rows = data_rows[:-2]

        urls.extend(row[0] for row in data_rows if row)
        if not next_resume_key:
            break
        resume_key = next_resume_key
        if page_num == _WAYBACK_MAX_PAGES:
            log.warning(f"Wayback CDX: hit the {_WAYBACK_MAX_PAGES}-page safety cap for "
                        f"{pattern} — more snapshots may exist beyond what was fetched "
                        f"({len(urls)} so far). Raise _WAYBACK_MAX_PAGES if this recurs.")
    return urls


def fetch_wayback_adp_slugs(limit: int = 5000) -> dict[str, set[str]]:
    """Query the Wayback Machine CDX index for archived ADP career pages
    (both the modern cid/ccId family and the deprecated legacy client=
    family) and extract cid|ccId slugs — modern URLs parse directly from
    the query string, legacy ones resolve via one live redirect-follow
    each (capped, see _ADP_LEGACY_RESOLVE_CAP). `limit` is now the
    PER-PAGE size for CDX's resumeKey pagination, not a hard overall cap —
    see _fetch_wayback_cdx_urls for why the old flat-limit version was
    silently truncating on high-volume patterns."""
    slugs: set[str] = set()

    if not _robots_allows("https://web.archive.org", "/cdx/"):
        log.warning("Wayback CDX: /cdx/ disallowed by web.archive.org/robots.txt "
                     "(or robots.txt unreachable) — skipping ADP Wayback discovery.")
        return {"adp": slugs}

    for pattern in _ADP_WAYBACK_PATTERNS:
        log.info(f"Wayback CDX: querying archived snapshots of {pattern}")
        urls = _fetch_wayback_cdx_urls(pattern, limit)
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


def resolve_candidate_page_to_ats_slug(url: str, timeout: int = 15) -> tuple[str, str] | None:
    """Like resolve_company_to_ats_slug, but for a candidate URL a source
    (HTTP Archive) already told us has a matching ATS fingerprint ON THAT
    EXACT PAGE. Tries the specific page first, only falling back to the
    homepage-based rediscovery resolve_company_to_ats_slug does if the
    exact page itself doesn't pan out.

    2026-09: HTTP Archive's resolve step used to call
    resolve_company_to_ats_slug(url) directly on the candidate page, which
    immediately throws away everything but the URL's bare hostname
    (scheme://host) and starts fresh from THAT host's homepage — e.g.
    "acme.com/careers/greenhouse-widget" gets stripped down to "acme.com"
    before any fetch even happens. That discards the one concrete fact we
    already had: the exact path where BigQuery/Wappalyzer confirmed the
    fingerprint. A real loss of recall follows from that — a homepage
    that doesn't directly link to the confirmed page (a few clicks deep,
    or only reachable via the traffic CrUX itself tracked, not top nav)
    would resolve to nothing even though a real match is already known to
    exist at that exact URL.

    Order of attempts, cheapest/most-specific first, and STRICTLY additive
    over the old behavior (falls through to the exact same logic as
    before as its last resort, so this can only resolve MORE than it used
    to, never less):
      1. Check the candidate URL itself against every known ATS pattern
         (URL_TO_SLUG) — free, no network — covers the case where the
         "page" IS already hosted on the vendor's own domain (e.g. a
         boards.greenhouse.io/... page Wappalyzer flagged directly).
      2. Fetch that EXACT page (not the homepage) and scan its own
         outbound links — covers the case where the candidate page is
         the company's own career page embedding an ATS widget/link,
         which is exactly the page BigQuery already told us has one.
      3. Fall back to resolve_company_to_ats_slug(url)'s existing
         homepage + one-hop-to-careers logic, unchanged.
    """
    parsed = urlparse(url if "://" in url else f"https://{url}")
    if not parsed.hostname:
        return None

    # (1) the URL itself, no network needed.
    for ats, resolver in URL_TO_SLUG.items():
        slug = resolver(url)
        if slug:
            return ats, slug

    # (2) the exact candidate page — the one BigQuery already confirmed.
    base = f"{parsed.scheme}://{parsed.hostname}"
    if _robots_allows(base, parsed.path or "/"):
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": YC_USER_AGENT})
            if r.status_code < 400:
                hit = _scan_html_for_ats_slug(r.text, url)
                if hit:
                    return hit
        except Exception:
            pass

    # (3) fall back to the pre-existing homepage + one-hop rediscovery —
    # never resolves worse than before this function existed.
    return resolve_company_to_ats_slug(url, timeout=timeout)


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
# SOURCE 7 & 8: Hugging Face bulk datasets (Latmay + Edward)
# ══════════════════════════════════════════════════════════
# Two public HF datasets hand over an ATS URL directly per row — unlike
# YC/Common Crawl/OpenData, which discover an ATS link by live-crawling
# a company's own homepage from just a name+domain, these need no crawl
# at all: every row already IS an ATS URL, so this is an offline pass
# through the existing URL_TO_SLUG dispatch table, not a live-discovery
# source. Each gets its own function (never run on a shared seed/probe
# pipeline, per the user's explicit instruction) and its own literal
# Supabase `source` value: "Latmay H.F" / "Edward H.F".
#
#   latmay/ats-career-page-urls (69,638 rows: canonical_url,
#     ats_platform). ats_platform is pre-labeled by the dataset owner,
#     but rather than trust a fragile label->URL_TO_SLUG-key mapping,
#     canonical_url is matched against EVERY known extractor — same
#     "try every resolver" pattern this file already uses in
#     resolve_candidate_page_to_ats_slug's step (1) — so this stays
#     correct even if a label's wording doesn't match this file's slug
#     naming exactly.
#   edwarddgao/open-apply-jobs (31M+ individual job-posting rows,
#     no ats_platform label, no dedup by the owner — the same
#     company's board can appear thousands of times across its own job
#     postings). apply_url is the only field ever read: every other
#     column (description_html etc.) is projected away at the Parquet
#     read itself so it's never pulled over the wire or held in memory.
#
# Both resolve via HF's auto-converted Parquet export
# (huggingface.co/api/datasets/{repo}/parquet) rather than parsing the
# dataset's original storage format directly — confirmed live (2026-09)
# for both repos: {"default": {"train": [...file urls...]}}, 1 file for
# Latmay, 375 for Edward.
#
# HF egress: contrary to this project's own assumption of a 20TB cap,
# huggingface.co/docs/hub/storage-limits documents NO egress/bandwidth
# limit for public dataset downloads of any size — only a rolling
# 5-minute REQUEST-RATE window on /resolve/ URLs is documented
# (huggingface.co/docs/hub/rate-limits: 3,000/5min anonymous). A
# handful of Parquet file downloads, however large each file, costs a
# handful of requests — nowhere near that window regardless.

def _hf_parquet_urls(repo: str) -> list[str]:
    """Resolve a public HF dataset's auto-converted Parquet export file
    URL(s) via the datasets-server Parquet API. Works for any public
    dataset regardless of its original storage format. Returns [] on
    any failure (network, unexpected response shape, dataset not yet
    Parquet-converted) rather than raising — a source outage degrades
    to "0 slugs from this source" instead of crashing the whole
    discovery run."""
    try:
        r = requests.get(f"https://huggingface.co/api/datasets/{repo}/parquet",
                          timeout=30)
        r.raise_for_status()
        data = r.json()
        urls: list[str] = []
        for config_splits in data.values():
            for split_urls in config_splits.values():
                urls.extend(split_urls)
        return urls
    except Exception as e:
        log.warning(f"HF Parquet resolve failed for {repo}: {e}")
        return []


def _resolve_url_via_url_to_slug(url: str) -> tuple[str, str] | None:
    """Match `url` against every known ATS URL pattern (URL_TO_SLUG).
    Same 'try every resolver' logic already used in
    resolve_candidate_page_to_ats_slug's step (1) — pulled out standalone
    here since both HF sources need it directly, with no page fetch/
    HTML-scan step around it."""
    if not url:
        return None
    for ats, resolver in URL_TO_SLUG.items():
        try:
            slug = resolver(url)
        except Exception:
            slug = None
        if slug:
            return ats, slug
    return None


def fetch_latmay_slugs() -> dict[str, dict[str, str]]:
    """latmay/ats-career-page-urls — 69,638 rows of {canonical_url,
    ats_platform}. Small enough to load in one pass, no time-budget/
    streaming logic needed (contrast fetch_edwarddgao_slugs below)."""
    import pyarrow.parquet as pq

    file_urls = _hf_parquet_urls("latmay/ats-career-page-urls")
    if not file_urls:
        log.warning("Latmay H.F: no Parquet files resolved, skipping source")
        return {}

    slugs_by_ats: dict[str, dict[str, str]] = {}
    processed = 0
    start = time.monotonic()
    _PROGRESS_EVERY = 5_000

    for file_url in file_urls:
        try:
            r = requests.get(file_url, timeout=120)
            r.raise_for_status()
        except Exception as e:
            log.warning(f"Latmay H.F: failed to download {file_url}: {e}")
            continue

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            tmp.write(r.content)
            tmp.flush()
            table = pq.read_table(tmp.name, columns=["canonical_url"])

        for url in table.column("canonical_url").to_pylist():
            processed += 1
            hit = _resolve_url_via_url_to_slug(url)
            if hit:
                actual_ats, slug = hit
                slugs_by_ats.setdefault(actual_ats, {})[slug] = ""

            if processed % _PROGRESS_EVERY == 0:
                elapsed = max(time.monotonic() - start, 0.001)
                resolved = sum(len(s) for s in slugs_by_ats.values())
                log.info(f"Latmay H.F: {processed:,} processed "
                         f"({processed / elapsed:,.1f}/sec), {resolved:,} "
                         f"resolved ({resolved / processed * 100:.1f}%)")

    total = sum(len(s) for s in slugs_by_ats.values())
    for ats, slugs in slugs_by_ats.items():
        log.info(f"  {ats}: {len(slugs)} slugs from Latmay H.F")
    log.info(f"Latmay H.F summary: {processed:,} rows processed, {total:,} "
             f"slugs resolved ({total / max(processed, 1) * 100:.1f}%)")
    return slugs_by_ats


def fetch_edwarddgao_slugs(time_budget_minutes: int = 300) -> dict[str, dict[str, str]]:
    """edwarddgao/open-apply-jobs — 31M+ individual job-posting rows
    across 375 Parquet shards. Only `apply_url` is ever read — every
    other column (description_html, salary fields, etc.) is projected
    away at the Parquet read itself, never pulled over the wire or held
    in memory. No dedup by the dataset owner (same company's board can
    appear thousands of times across its postings) — harmless here
    since slugs accumulate into a dict keyed by slug, naturally deduped.

    `time_budget_minutes` self-stops gracefully and keeps whatever was
    resolved so far, same pattern as fetch_httparchive_slugs — 375
    shards at 31M+ total rows is real download+parse volume, and a
    hard CI job timeout mid-shard would otherwise lose an entire run's
    progress instead of the partial-but-real result a graceful stop
    keeps. Runs are idempotent (on_conflict upsert), so an
    incomplete-shard-coverage run still converges over repeat runs."""
    import pyarrow.parquet as pq

    file_urls = _hf_parquet_urls("edwarddgao/open-apply-jobs")
    if not file_urls:
        log.warning("Edward H.F: no Parquet files resolved, skipping source")
        return {}

    log.info(f"Edward H.F: {len(file_urls)} Parquet shards to process "
             f"(time budget: {time_budget_minutes}min, 0 = no budget)")

    slugs_by_ats: dict[str, dict[str, str]] = {}
    processed = 0
    start = time.monotonic()
    _PROGRESS_EVERY = 5_000
    budget_seconds = time_budget_minutes * 60 if time_budget_minutes else None

    for shard_i, file_url in enumerate(file_urls):
        if budget_seconds and (time.monotonic() - start) >= budget_seconds:
            log.info(f"Edward H.F: time budget reached after {shard_i}/"
                     f"{len(file_urls)} shards — stopping gracefully, "
                     f"keeping {processed:,} rows' worth of progress.")
            break

        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                with requests.get(file_url, timeout=300, stream=True) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        tmp.write(chunk)
                tmp.flush()

                pf = pq.ParquetFile(tmp.name)
                for batch in pf.iter_batches(columns=["apply_url"], batch_size=50_000):
                    for url in batch.column("apply_url").to_pylist():
                        processed += 1
                        hit = _resolve_url_via_url_to_slug(url)
                        if hit:
                            actual_ats, slug = hit
                            slugs_by_ats.setdefault(actual_ats, {})[slug] = ""

                        if processed % _PROGRESS_EVERY == 0:
                            elapsed = max(time.monotonic() - start, 0.001)
                            resolved = sum(len(s) for s in slugs_by_ats.values())
                            log.info(f"Edward H.F: shard {shard_i + 1}/{len(file_urls)}, "
                                     f"{processed:,} processed ({processed / elapsed:,.1f}/sec), "
                                     f"{resolved:,} resolved ({resolved / processed * 100:.1f}%)")
        except Exception as e:
            log.warning(f"Edward H.F: shard {shard_i + 1}/{len(file_urls)} "
                        f"({file_url}) failed, skipping: {e}")
            continue

    total = sum(len(s) for s in slugs_by_ats.values())
    for ats, slugs in slugs_by_ats.items():
        log.info(f"  {ats}: {len(slugs)} slugs from Edward H.F")
    log.info(f"Edward H.F summary: {processed:,} rows processed, {total:,} "
             f"slugs resolved ({total / max(processed, 1) * 100:.1f}%)")
    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 9: TheirStack (freemium technology-usage API)
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

def fetch_httparchive_candidate_urls(limit_per_tech: int = 200_000,
                                      months: int = 24,
                                      ha_shard: int | None = None,
                                      ha_total_shards: int = 1) -> dict[str, list[str]]:
    """Query HTTP Archive's public BigQuery dataset for pages where a
    known ATS technology was detected. Returns {ats: [urls]}, ranked by
    CrUX popularity (most popular/reliable first) within limit_per_tech.

    2026-09: ha_shard/ha_total_shards split the resolved `crawl_dates`
    list across `ha_total_shards` independent runs — added so this source
    (previously a single ~90-minute job) can be sharded WITHOUT the cost
    blowup sharding by ATS tech would cause. `httparchive.crawl.pages` is
    partitioned by `date` (HTTP Archive's own published schema), and this
    query already filters on `date IN UNNEST(@crawl_dates)` — so a shard
    given HALF the dates scans roughly HALF the partition bytes, same as
    querying that half-range alone; summed across shards, total bytes
    scanned is the same as one unsharded run, just parallelized. Sharding
    by TECH instead (one shard per ATS fingerprint) would NOT have this
    property: `technology` isn't a partition/clustering key, so a
    1-tech-of-20 query scans the exact same bytes as a 20-tech query,
    multiplying cost by the shard count for zero benefit — see
    discovery.yml's comment on this source for why that path was
    rejected. Pass ha_shard=None (default) to query all resolved dates in
    one call, same as before this existed.

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
    higher score against the same pool.

    2026-09, raised again after re-checking actual costs against HTTP
    Archive's own current schema docs (har.fyi) and Google's current
    Sandbox docs:
      - limit_per_tech raised 2000 -> 200,000 (100x) at ZERO added query
        cost: this only affects the QUALIFY/ROW_NUMBER() window function,
        which BigQuery evaluates AFTER reading the matching bytes from the
        source partitions — bytes billed depend on what's SCANNED, never
        on output row count, so LIMIT 2000 vs LIMIT 200000 costs exactly
        the same. There was never a reason for the old low cap once that
        was understood; effectively unbounded now, capped only high enough
        to rule out a truly pathological result set.
      - months raised 13 -> 24 (so 48 date x client scans per run, up from
        26): re-checking real per-partition costs against HTTP Archive's
        own worked query-cost examples put this exact query shape (page +
        rank + technology fields only, no categories/info) closer to
        ~5-10GB per date x client, not the ~1-2GB this file used to assume
        — the old number was an optimistic guess, not a measured one. At
        the revised, more conservative ~10GB/slice: 48 slices ~= 480GB,
        under half of the Sandbox's still-current 1 TiB/month free quota,
        leaving real headroom for a few repeated runs in the same month
        (e.g. iterating on this query during development) without risking
        the quota. Not pushed further than 24 months for exactly that
        margin-of-safety reason — this is deliberately aggressive, not
        reckless.

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
    #
    # 2026-09: the WHERE bound below used to be a HARDCODED "INTERVAL 13
    # MONTH", left over from before `months` was raised 13 -> 24 — meaning
    # that whole widening never actually did anything: this bound filtered
    # out everything older than 13 months BEFORE "LIMIT @months" ever got
    # a chance to return more, so months=24 (or any value > 13) silently
    # behaved identically to months=13. Confirmed live: a real run asking
    # for 24 months got back exactly 12 dates (Sept 2025 .. Aug 2026) —
    # consistent with this 13-month wall, not with the wider window the
    # docstring above describes. Now the lookback window itself scales
    # with `months` (+3 slack for any gap month/late-published crawl), so
    # raising --httparchive-months actually reaches further back. This
    # also means the real BigQuery cost this whole time has been roughly
    # HALF of what the docstring's "48 slices ~= 480GB" estimate assumed
    # (at most ~13 dates x 2 clients, not 24 x 2) — more quota headroom
    # than documented, not less.
    try:
        date_rows = list(client.query(
            "SELECT DISTINCT date FROM `httparchive.crawl.pages` "
            "WHERE date > DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_months MONTH) "
            "ORDER BY date DESC "
            "LIMIT @months",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("months", "INT64", months),
                bigquery.ScalarQueryParameter("lookback_months", "INT64", months + 3),
            ]),
        ).result())
        crawl_dates = [r.date for r in date_rows]
    except Exception as e:
        # 2026-09: "Quota exceeded: ... free query bytes scanned" is
        # Google's BigQuery SANDBOX quota specifically — a monthly cap that
        # applies ONLY to a project with no Cloud Billing account attached,
        # separate from (and much stricter than) the standard 1 TiB/month
        # BigQuery free tier every billing-enabled project also gets at no
        # charge. Once a sandbox project hits it, EVERY query fails this
        # way until next month's reset — including a trivial metadata query
        # like this one, which is why this can happen even right after a
        # "cheap" query succeeded elsewhere. Confirmed via Google's own
        # troubleshooting docs (cloud.google.com/bigquery/docs/
        # troubleshoot-quotas) and HTTP Archive's own BigQuery community
        # forum: the fix is attaching a Cloud Billing account to
        # GCP_PROJECT_ID (console.cloud.google.com/billing) — this is
        # unrelated to actually being charged; on the standard 1 TiB/month
        # free tier, staying under that amount still costs nothing, it
        # just isn't hard-blocked the way the no-billing sandbox is. Called
        # out explicitly here (rather than left as a generic "why did this
        # 403" mystery) since this exact error string is otherwise easy to
        # mistake for a code bug.
        if "free query bytes scanned" in str(e):
            log.warning(
                "HTTP Archive: BigQuery SANDBOX quota exhausted for this "
                "project (this is Google's no-billing-account monthly cap, "
                "not this project's own crawl-date query being expensive — "
                "every query fails this way until it resets or a Cloud "
                "Billing account is attached). Fix: attach a billing "
                "account to GCP_PROJECT_ID at "
                "console.cloud.google.com/billing — this unlocks the "
                "standard 1 TiB/month BigQuery free tier, which is NOT the "
                "same limit and isn't consumed yet. Skipping HTTP Archive "
                f"for this run. ({e})")
        else:
            log.warning(f"HTTP Archive: failed to find recent crawl dates: {e}")
        return {}

    if not crawl_dates:
        log.warning("HTTP Archive: no recent crawl partitions found — skipping.")
        return {}

    if ha_shard is not None and ha_total_shards > 1:
        full_count = len(crawl_dates)
        crawl_dates = [d for i, d in enumerate(crawl_dates) if i % ha_total_shards == ha_shard]
        log.info(f"HTTP Archive: shard {ha_shard}/{ha_total_shards} — "
                 f"{len(crawl_dates)}/{full_count} crawl dates assigned to this shard")
        if not crawl_dates:
            log.warning("HTTP Archive: this shard got zero dates (ha_total_shards > "
                        "months available) — nothing to query.")
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


def fetch_httparchive_slugs(limit_per_tech: int = 200_000, months: int = 24,
                             max_workers: int = 100,
                             resolve_time_budget_minutes: int = 300,
                             ha_shard: int | None = None,
                             ha_total_shards: int = 1) -> dict[str, dict[str, str]]:
    """Resolve HTTP Archive's candidate pages to real ATS slugs, reusing
    the exact same resolver built for the Y Combinator source.

    max_workers raised 15->30->100 alongside the much higher default
    limit_per_tech (200->2000->200,000) — each resolve is only 1-2
    lightweight HTTP fetches, and unlike a single-API source (TheirStack,
    BigQuery itself), these fetches hit thousands of DIFFERENT company
    domains, so there's no single server to be impolite to by running
    100 at once; a GitHub Actions runner handles this fine.

    2026-09: limit_per_tech's old low caps (2000, before that 200) were
    based on a mistaken assumption that a bigger cap cost more BigQuery
    money — it doesn't (see fetch_httparchive_candidate_urls' docstring:
    QUALIFY/ROW_NUMBER() only trims OUTPUT rows after BigQuery has already
    scanned the same bytes regardless of the cap). So the query-side cap
    is now effectively unbounded (200,000/tech). But raising ONLY that
    number, with nothing else changed, would have been a real mistake: it
    can produce up to ~17 techs x 200,000 = a few million candidate URLs
    in one run, each needing its own live HTTP fetch to resolve — that's
    real wall-clock cost this project has no way to make free, and every
    resolved slug was only being accumulated in memory and written to
    Supabase in one shot at the very end, meaning a run that ran out of
    CI job time would lose EVERYTHING resolved that run, not just the
    unresolved remainder. resolve_time_budget_minutes fixes that: past
    this many minutes of resolving, the loop stops WAITING on any not-yet-
    finished fetch (in-flight ones are abandoned, not force-killed) and
    returns whatever's already resolved, same self-stop-gracefully shape
    every other long-running crawl in this project already uses (see
    node.py/opendata_probe.py/common_crawl_probe.py's --time-budget-minutes).
    0 disables the budget (run to full completion) — kept non-zero by
    default here specifically because the query-side cap is no longer a
    natural ceiling on resolve work the way it used to be.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    urls_by_ats = fetch_httparchive_candidate_urls(limit_per_tech, months, ha_shard, ha_total_shards)
    if not urls_by_ats:
        return {}

    all_urls = [(ats, url) for ats, urls in urls_by_ats.items() for url in urls]
    log.info(f"HTTP Archive: resolving up to {len(all_urls)} candidate pages "
             + (f"(time budget: {resolve_time_budget_minutes}min)..." if resolve_time_budget_minutes
                else "(no time budget — will run to completion)..."))

    _robots_check_stats["unreachable"] = 0
    _robots_check_stats["disallowed_by_rule"] = 0

    slugs_by_ats: dict[str, dict[str, str]] = {}
    resolved = 0
    processed = 0
    stopped_early = False
    start = time.monotonic()
    last_heartbeat = start
    last_logged_processed = 0
    last_hit: tuple[str, str] | None = None
    # 2026-09: this source used to log nothing at all between the initial
    # "resolving up to N candidate pages" line and the final summary —
    # against a candidate list in the tens/hundreds of thousands with a
    # multi-hour time budget, that made a perfectly healthy run look
    # indistinguishable from a hung one in the CI log. FIX (this session):
    # was logging a line on every single hit — against a real run with
    # thousands of resolutions/minute that's the opposite problem (log
    # spam, four lines in the same second). Now batched to the same
    # cadence every other bulk source in this project uses: one line every
    # 5,000 candidates PROCESSED (not every hit), showing the last company
    # actually resolved plus rows/sec and running hit% — a 60s wall-clock
    # heartbeat stays as backup for a slow stretch that never reaches 5,000.
    _PROGRESS_EVERY = 5_000
    _HEARTBEAT_SECONDS = 60
    budget_seconds = resolve_time_budget_minutes * 60 if resolve_time_budget_minutes else None

    def _resolve_one(item):
        expected_ats, url = item
        # resolve_candidate_page_to_ats_slug, not resolve_company_to_ats_slug
        # directly — this source already knows the EXACT page BigQuery
        # confirmed has the fingerprint, so try that page itself first
        # before falling back to the homepage-based rediscovery the YC
        # source uses (which only ever has a bare homepage URL to start
        # from, never a confirmed page). See that function's docstring.
        result = resolve_candidate_page_to_ats_slug(url)
        time.sleep(0.1)
        return expected_ats, result

    pool = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {pool.submit(_resolve_one, item) for item in all_urls}
        for future in as_completed(futures):
            if budget_seconds and time.monotonic() - start >= budget_seconds:
                stopped_early = True
                log.warning(f"HTTP Archive: resolve time budget ({resolve_time_budget_minutes}min) "
                            f"reached at {resolved}/{len(all_urls)} resolved — stopping here rather "
                            f"than risk the whole run's progress to a hard CI timeout. Everything "
                            f"resolved so far is still kept and written to Supabase normally.")
                break
            try:
                expected_ats, result = future.result()
            except Exception:
                processed += 1
                continue
            processed += 1
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
                last_hit = (actual_ats, slug)
            now = time.monotonic()
            if processed - last_logged_processed >= _PROGRESS_EVERY:
                last_logged_processed = processed
                last_heartbeat = now
                elapsed = now - start
                hit_note = f", last: {last_hit[0]} -> {last_hit[1]}" if last_hit else ""
                log.info(f"HTTP Archive: ...{processed:,}/{len(all_urls):,} processed "
                         f"({processed / max(elapsed, 0.001):,.1f}/sec), {resolved:,} resolved "
                         f"({resolved / processed * 100:.1f}%){hit_note}")
            elif now - last_heartbeat >= _HEARTBEAT_SECONDS:
                last_heartbeat = now
                log.info(f"HTTP Archive: still working — {processed}/{len(all_urls)} processed, "
                         f"{resolved} resolved so far, {(now - start) / 60:.1f}min elapsed")
    finally:
        # cancel_futures=True drops anything not yet STARTED; anything
        # already mid-fetch in a worker thread is abandoned (its result is
        # simply never collected), not force-killed — same trade-off
        # every graceful-stop in this project makes, never a hard kill.
        pool.shutdown(wait=False, cancel_futures=True)

    log.info(f"HTTP Archive: resolved {resolved}/{len(all_urls)} candidate pages"
             + (" (stopped early on time budget)" if stopped_early else ""))
    for ats, slugs in slugs_by_ats.items():
        log.info(f"  {ats}: {len(slugs)} companies from HTTP Archive")

    skipped = len(all_urls) - resolved
    log.info(f"  HTTP Archive summary: {resolved} resolved, {skipped} skipped/not-yet-attempted "
             f"({_robots_check_stats['unreachable']} unreachable sites, "
             f"{_robots_check_stats['disallowed_by_rule']} disallowed by "
             f"robots.txt, rest had no detectable ATS link or weren't reached before the time "
             f"budget) — run with -v/--verbose for the per-site detail.")

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
                # 2026-09: was "slug_registry" — that table doesn't exist
                # any more (renamed to archive_i a while back; node.py's
                # ARCHIVE_I_TABLE comment says as much: "was slug_registry").
                # This function's try/except swallowed the resulting 404
                # silently every run, so the oracle_cloud_hcm de-dup check
                # has been returning {} (no resolved tenants found)
                # unconditionally — legacy tenant slugs could have been
                # re-added every week instead of being filtered. See
                # upsert_to_supabase's matching fix for the bigger half of
                # this same bug (the actual write path — confirmed live via
                # a real "Could not find the table 'public.slug_registry'"
                # PostgREST 404 in a run's own logs).
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
    """Upsert slugs to Supabase archive_i. Returns total upserted.

    slugs_by_ats values can be:
      - set[str]          → slugs only (no company name)
      - dict[str, str]    → {slug: company_name}

    2026-09: was writing to "slug_registry", a table that no longer
    exists — it was renamed to archive_i at some point (node.py's
    ARCHIVE_I_TABLE comment: "was slug_registry"), but this file was never
    updated to match. Confirmed live via a real run's own logs: every
    single upsert across every source (Feashliaa, Common Crawl, YC,
    HTTP Archive, all of it) was failing with PostgREST 404 "Could not
    find the table 'public.slug_registry'" and just logging an ERROR line
    per chunk rather than crashing the run — meaning this whole pipeline's
    actual writes had been silently going nowhere for however long that
    rename has been live, while every fetch/query/live-HTTP-resolve step
    still ran (and cost/rate-limited) for nothing. Also drops the "name"
    field entirely: archive_i has no such column (id/ats/slug/source/
    first_seen/last_seen only — confirmed against the live schema), so
    sending it once the table name was fixed would have just traded one
    failure mode for another (a PostgREST "column not found" 400)."""
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
            # name (company name, when a source has one) has nowhere to
            # go — archive_i doesn't carry that column — so it's dropped
            # here rather than sent and rejected. Slug/ATS is still the
            # part every downstream consumer (node.py's crawl) actually
            # needs; the name was never more than a nice-to-have.
            rows = [{"ats": ats, "slug": slug, "source": source} for slug, _name in chunk]

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
        description="Discovery: populate Supabase slug_registry from 7 sources"
    )
    parser.add_argument(
        "--source",
        choices=["feashliaa", "kalil", "openpostings", "commoncrawl",
                 "wayback_adp", "theirstack", "httparchive",
                 "latmay", "edwarddgao", "all"],
        default="all",
        help="Which source to pull from (default: all). 'yc' removed "
             "2026-09 — see the module docstring.",
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
             "i.e. no sharding). discovery.yml runs this as 2 (shards 0 "
             "and 1) as separate matrix jobs.",
    )
    parser.add_argument(
        "--theirstack-max", type=int, default=40,
        help="Max companies to pull from TheirStack per run, across all "
             "platforms (default: 40, under the free tier's 50/month)",
    )
    parser.add_argument(
        "--httparchive-limit", type=int, default=200_000,
        help="Max candidate pages to pull PER ATS platform from HTTP "
             "Archive's BigQuery dataset, ranked by popularity (default: "
             "200,000, raised 2026-09 from 2000 — this cap costs nothing "
             "extra in BigQuery (QUALIFY only trims OUTPUT rows after the "
             "same bytes are scanned regardless), so it's now effectively "
             "unbounded. The real cost is downstream: each candidate needs "
             "a live HTTP fetch to resolve to a slug — see "
             "--httparchive-resolve-budget-minutes, which bounds that.",
    )
    parser.add_argument(
        "--httparchive-months", type=int, default=24,
        help="Number of recent monthly HTTP Archive crawl partitions to "
             "query, unioned with both desktop+mobile clients and deduped "
             "(default: 24, raised 2026-09 from 13 — re-checked against "
             "HTTP Archive's own current query-cost docs: ~5-10GB per "
             "date x client, so 24 months x 2 clients = 48 scans stays "
             "under half of BigQuery Sandbox's 1TiB/month free quota, "
             "leaving headroom for repeat runs in the same month. QUALIFY "
             "still caps OUTPUT rows per tech at --httparchive-limit "
             "regardless of how many months are scanned, so this widens "
             "candidate DIVERSITY the popularity ranking picks from, "
             "without increasing the live-fetch resolve cost at all — see "
             "fetch_httparchive_candidate_urls docstring for why multi-"
             "month/multi-client is the real lever for more coverage here, "
             "not just a bigger --httparchive-limit alone)",
    )
    parser.add_argument(
        "--httparchive-resolve-budget-minutes", type=int, default=300,
        help="Self-stop gracefully after this many minutes of resolving "
             "HTTP Archive candidate pages to real slugs, keeping whatever "
             "was resolved so far rather than losing it all to a hard CI "
             "job timeout (default: 300; 0 = no budget, run to full "
             "completion — see fetch_httparchive_slugs docstring for why "
             "this matters now that --httparchive-limit is effectively "
             "unbounded).",
    )
    parser.add_argument(
        "--ha-shard", type=int, default=None,
        help="Which HTTP Archive date-shard this run covers (0-indexed, "
             "used with --ha-total-shards). Splits the resolved crawl-date "
             "list, NOT the tech list — see fetch_httparchive_candidate_urls "
             "docstring for why date-sharding is cost-neutral (partition "
             "pruning) while tech-sharding would multiply BigQuery cost. "
             "Default: None = all dates in one run.",
    )
    parser.add_argument(
        "--ha-total-shards", type=int, default=1,
        help="Total number of HTTP Archive date-shards (default: 1, i.e. "
             "no sharding).",
    )
    parser.add_argument(
        "--edwarddgao-time-budget-minutes", type=int, default=300,
        help="Self-stop gracefully after this many minutes downloading/"
             "resolving Edward H.F's 375 Parquet shards, keeping whatever "
             "was resolved so far (default: 300; 0 = no budget, run to "
             "full completion — see fetch_edwarddgao_slugs docstring).",
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
    log.info("           + Wayback CDX (ADP) + Latmay H.F + Edward H.F")
    log.info("           + TheirStack + HTTP Archive (BigQuery)")
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

    # Source 4: Common Crawl (ongoing discovery for 27 platforms — run as
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

    # Source 5: Wayback Machine CDX (ADP-only — see fetch_wayback_adp_slugs
    # docstring for why ADP specifically needs a second discovery source)
    if args.source in ("wayback_adp", "all"):
        log.info("\n--- WAYBACK MACHINE CDX (ADP-only supplemental discovery) ---")
        wb_slugs = fetch_wayback_adp_slugs()
        wb_total = sum(len(s) for s in wb_slugs.values())
        log.info(f"Wayback CDX total: {wb_total} slugs")

        if not args.dry_run:
            # 2026-09: "wayback_adp" isn't in archive_i's own source CHECK
            # constraint (only the bare "wayback" is, matching 2,910 real
            # historical rows already written under that label before this
            # source's --source flag was renamed to wayback_adp) — writing
            # "wayback_adp" here would fail the constraint on every row
            # even after the table-name fix above. "wayback" it is, to
            # match both the constraint and this source's own prior data.
            upserted = upsert_to_supabase(wb_slugs, source="wayback",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += wb_total

    # Source 6 (Y Combinator) REMOVED 2026-09 at the user's request: YC-
    # batch companies aren't ATS-specific — they surface through Common
    # Crawl/OpenPostings/the HF sources below just as well, so a dedicated
    # own-website-crawl source for them wasn't earning its keep.
    # fetch_yc_slugs() itself is left defined (unused) rather than deleted —
    # zero risk, and YC_USER_AGENT (a genuinely shared constant, unrelated
    # to YC Combinator specifically) is still used elsewhere in this file.

    # Source 7: Latmay H.F (huggingface.co/datasets/latmay/ats-career-page-urls
    # — 69,638 rows, ATS URLs already resolved by the dataset owner)
    if args.source in ("latmay", "all"):
        log.info("\n--- LATMAY H.F (Hugging Face, 69,638 ATS career page URLs) ---")
        lm_slugs = fetch_latmay_slugs()
        lm_total = sum(len(s) for s in lm_slugs.values())
        if lm_total:
            log.info(f"Latmay H.F total: {lm_total} slugs across "
                     f"{sum(1 for s in lm_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(lm_slugs, source="Latmay H.F",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += lm_total

    # Source 8: Edward H.F (huggingface.co/datasets/edwarddgao/open-apply-jobs
    # — 31M+ individual job postings, apply_url resolved through URL_TO_SLUG)
    if args.source in ("edwarddgao", "all"):
        log.info("\n--- EDWARD H.F (Hugging Face, 31M+ job postings) ---")
        ed_slugs = fetch_edwarddgao_slugs(
            time_budget_minutes=args.edwarddgao_time_budget_minutes)
        ed_total = sum(len(s) for s in ed_slugs.values())
        if ed_total:
            log.info(f"Edward H.F total: {ed_total} slugs across "
                     f"{sum(1 for s in ed_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(ed_slugs, source="Edward H.F",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += ed_total

    # Source 9: TheirStack (freemium — small monthly trickle for thin
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
                                            months=args.httparchive_months,
                                            resolve_time_budget_minutes=args.httparchive_resolve_budget_minutes,
                                            ha_shard=args.ha_shard,
                                            ha_total_shards=args.ha_total_shards)
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
