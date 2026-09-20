"""
Discovery — Supabase as Single Source of Truth
=====================================================
Pulls company slugs from multiple sources and upserts them into
Supabase's archive_i table (renamed from slug_registry a while back —
see node.py's ARCHIVE_I_TABLE comment).

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
  5. Wayback Machine CDX (cross-platform supplemental discovery — every
     platform with a CC_PLATFORM_PATTERNS entry, not just ADP; see
     fetch_wayback_slugs docstring. Originally ADP-only, generalized
     2026-09.)
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
  11. GitHub repo registries (--source github; 2026-09, new — pre-built ATS
     slug registries from known public repos, e.g. datascry/openroles'
     data/tenants/*.json, pulled via jsDelivr's CDN mirror, no GitHub
     API/auth needed. See GITHUB_REGISTRY_REPOS and
     fetch_github_registries_slugs docstring — including the research
     trail for why this is a manually-curated repo list, not a live
     GitHub-wide search, and why csod needs one live per-tenant resolve
     while workday/brassring/oracle_cloud_hcm are free local reassembly.)
  12. Open Jobs Daily H.F (--source openjobsdaily; 2026-09, new —
     huggingface.co/datasets/Yigit-Karaman/open-jobs-daily, ~9.8M rows
     across both HF configs, CC0-1.0. Same shape as Edward H.F: only the
     `url` column is read, resolved through URL_TO_SLUG — the dataset's
     own pre-labeled ats/slug columns aren't trusted directly. See
     fetch_openjobsdaily_slugs docstring.)
  13. Zalize H.F — REMOVED 2026-09 at the user's request (see main()'s
     Source 13 comment). fetch_zalizedata_slugs() itself is left
     defined/unused.
  14. Aramente H.F — REMOVED 2026-09 at the user's request (see main()'s
     Source 14 comment). fetch_eutechjobs_slugs() itself is left
     defined/unused.
  15. Certificate Transparency logs (--source ct_logs; 2026-09, new —
     crt.sh's free CT-log index, queried directly, no seed file/dataset
     needed. Covers 18 subdomain-per-tenant platforms — see
     fetch_ct_log_slugs docstring and the CT_LOG_SUFFIXES comment above it
     for exactly which platforms this can/can't help and why.)

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

Runs weekly (Sunday) via GitHub Actions, as a 7-source, 14-job matrix
(YC removed 2026-09, Latmay H.F + Edward H.F added) — Common Crawl and
HTTP Archive each split across 3 jobs (see fetch_commoncrawl_slugs/
fetch_httparchive_slugs docstrings for why), Edward H.F split across
3 jobs too (375 Parquet files — see fetch_edwarddgao_slugs' own
hf_shard/hf_total_shards docstring), Latmay H.F left as ONE job (a
single ~69k-row Parquet file — one small download either way, so
sharding it would only spread the per-row URL_TO_SLUG work without
cutting any actual download volume, not worth another job slot for),
the remaining 3 sources one job each, all in parallel
(see .github/workflows/Discovery.yml) —
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
    python discovery.py --source ct_logs       # Certificate Transparency (crt.sh) only
    python discovery.py --source latmay        # Latmay H.F (Hugging Face) only
    python discovery.py --source edwarddgao    # Edward H.F (Hugging Face) only
    python discovery.py --source openjobsdaily # Open Jobs Daily H.F (Hugging Face) only
    python discovery.py --source theirstack    # TheirStack only
    python discovery.py --source httparchive   # HTTP Archive (BigQuery) only
    python discovery.py --source github        # GitHub repo registries only
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
    # 2026-09: also checked Cornerstone OnDemand and Paycom (never checked
    # before, an open gap noticed while auditing discovery coverage for
    # their reversal) — direct fetch of the enthec/webappanalyzer fork's
    # technology files confirmed NEITHER has an entry (no key containing
    # "Corner" anywhere in the c.json file; no "Paycom" key in p.json).
    # Same conclusion as the rest of this list: not in Wappalyzer's
    # ruleset, this source can't help regardless of query design.
    # 2026-09: Hireology / isolvedhire — checked against this same
    # enthec/webappanalyzer fork alongside the other 2026-09 additions.
    # Hireology HAS a real fingerprint (dom: a[href*='sites.hireology.com/']
    # — note this is a DIFFERENT host than the careers.hireology.com
    # career-board URL confirmed in ats_scrapers.scrape_hireology; likely
    # a separate widget/badge link Wappalyzer keys off, but "hireology.com"
    # is already a full-suffix match in node.py's _ATS_VENDOR_DOMAINS so
    # this needs no extra domain wiring). isolvedhire has NO fingerprint —
    # confirmed absent from the i.json technology file — so it's left out
    # of this dict entirely, same as Pinpoint/Flatchr/Occupop/etc above.
    "hireology": "Hireology",
}

# ATS platforms we have working scrapers for (20 active)
SUPPORTED_ATS = {
    "greenhouse", "lever", "ashby", "bamboohr", "icims", "workday",
    "rippling", "workable", "recruitee", "smartrecruiters",
    "teamtailor", "breezyhr", "personio", "joincom",
    # REVIVED 2026-09 as "jazzhr" (was "applytojob", REMOVED 2026-08): the
    # listing scraper itself (ats_scrapers.scrape_jazzhr) was never the
    # problem — it's a straightforward HTML scrape that was working fine.
    # The 2026-08 removal was because JD-enrichment via the then-current
    # _fetch_generic_description wasn't reliably surfacing real US-work-
    # authorization language, and the visa-sponsorship detector it fed
    # into had its own false-negative bug — the exact harmonyworks.com
    # "must be authorized to work in the US; not able to sponsor visas"
    # case that slipped through. Both of those have since been rewritten
    # for unrelated reasons: _fetch_generic_description gained JSON-LD/
    # embedded-JSON/itemprop/container fallbacks (see its docstring), and
    # detect_visa_sponsorship in classifier.py was rewritten 2026-09
    # specifically citing that harmonyworks.com case as the bug it fixed.
    # Could not live-verify this specific combination end-to-end on a real
    # JazzHR posting this session (no browser tool connected, direct fetch
    # to applytojob.com blocked from this sandbox) — reviving on the
    # strength of those two independently-documented fixes, not a guess.
    # Spot-check early scraped JazzHR output for eligibility-language leaks.
    "jazzhr",
    # Newly enabled (confirmed working via test_blacklisted_ats.py):
    "taleo", "oracle_cloud_hcm", "paylocity", "hrmdirect", "zoho",
    # Fixed (2026-08) — was blacklisted with wrong URL/API assumptions,
    # now scrapes correctly (see ats_scrapers.py):
    "softgarden",
    # New (2026-09) — PageUp (AU/NZ, HIGHEST priority of this batch),
    # Pinpoint (UK), Flatchr (France), Jobylon (Nordics). All 4 confirmed
    # to have a genuinely working scraper (server-rendered HTML or a real
    # public JSON API — see ats_scrapers.py for each).
    #
    # REMOVED 2026-09: "homerun" — its extractor's only signal was "any
    # host starting with jobs." (Homerun customers run on their own
    # domain, not a shared subdomain, so there was never a real vendor
    # pattern to match against). Confirmed live this session that this
    # was catching mostly non-Homerun pages: of 5 sampled archive_i
    # "homerun" rows, 0 were actually Homerun — jobs.cambly.com is an
    # in-house landing page that itself links out to Ashby
    # (jobs.ashbyhq.com/Cambly), jobs.hireart.com redirects to an
    # unrelated login portal, jobs.wrkhq.com redirects to a completely
    # different company's site, jobs.we-mng.com is a WordPress+JobSearch-
    # plugin board with zero live listings. 11,892 archive_i rows and 0
    # ever reached the `jobs` table. Removed everywhere: this set, the
    # extractor function, ats_scrapers.py's scraper, and the stale
    # archive_i rows (see BLACKLISTED_ATS.md for the full writeup).
    "pageup", "pinpoint", "flatchr", "jobylon",
    # FIXED 2026-09: these 7 all had a real, working, REGISTERED scraper in
    # ats_scrapers.py's SCRAPERS dict already (confirmed live: archive_i
    # holds 17,368 adp rows, 1,064 jobvite, 1,048 jobadder, 537 brassring,
    # 390 folkshr, 314 avature, 132 eploy — all discovered via Common
    # Crawl/URL_TO_SLUG, which don't gate on SUPPORTED_ATS) but were never
    # added to this set. The only real-world effect of that gap:
    # fetch_openpostings_slugs() is the one source that DOES gate on
    # SUPPORTED_ATS (its slugs_by_ats dict is pre-seeded from this set),
    # so any of these 7 tagged in OpenPostings' own dataset were being
    # silently dropped rather than upserted. brassring specifically was
    # also still wrongly listed as "blacklisted" below even though it was
    # re-enabled (see ats_scrapers.py's scrape_brassring docstring: the
    # real root cause was missing session priming, not JS-rendering/auth/
    # robots) — that stale blacklist entry is why this whole gap went
    # unnoticed.
    "adp", "brassring", "jobvite", "jobadder", "folkshr", "avature", "eploy",
    # 2026-09: Cornerstone OnDemand (csod) — REVERSED out of discovery-only.
    # Confirmed live (real browser, two independent tenants) that the
    # bootstrap career-site page embeds a genuine anonymous bearer JWT in
    # its raw HTTP response, honored with zero cookies/session by a real
    # public job-search API (see ats_scrapers.scrape_csod and
    # _url_to_slug_csod's docstring for the full evidence trail, and
    # GREYLIST_ATS.md for the before/after writeup). Dayforce/Getro
    # remain discovery-only.
    "csod",
    # 2026-09: Paycom — ALSO reversed out of discovery-only, same session,
    # same pattern (a different anonymous bearer JWT embedded in its own
    # career-page bootstrap HTML, honoring a real POST search API with no
    # per-customer OAuth needed) — see ats_scrapers.scrape_paycom's block
    # comment and GREYLIST_ATS.md. Slug format UNCHANGED (still just the
    # 32-hex clientkey) — unlike Cornerstone, no per-tenant region/page-id
    # value needs to travel in the slug; scrape_paycom discovers the
    # tenant's regional API host itself from the bootstrap page.
    "paycom",
    # 2026-09: SAP SuccessFactors (Career Site Builder tenants only) —
    # REVERSED out of "genuinely blocked" for a DIFFERENT reason than
    # csod/paycom above: this isn't a hidden anonymous API, it's plain
    # server-rendered HTML that was always robots.txt-legal — the old
    # "blocked on every live host checked" verdict was true only for the
    # legacy shared-host successfactors.com/.eu/sapsf.com/.eu/jobs2web.com
    # tenants (confirmed still robots.txt-disallowed live, e.g.
    # career2.successfactors.eu — those stay OUT of SUPPORTED_ATS, see
    # GREYLIST_ATS.md), which is all that was tested before. A modern CSB
    # tenant runs on the CUSTOMER'S OWN branded domain (careers.swissre.com,
    # jobs.sap.com) with its own robots.txt, and confirmed live on two
    # independent such tenants: neither blocks /search/ (paginated HTML
    # job listing, "Results 1-N of TOTAL") nor /job/{slug}/{id}/ (full,
    # untruncated description server-rendered, no JS/API needed at all).
    # The one real API (`POST {origin}/services/recruiting/v1/jobs`) IS
    # confirmed robots.txt-disallowed on both tenants (`Disallow: /services/`)
    # — not used here, by design, per this project's hard robots.txt rule.
    # Slug is just the tenant's host (e.g. "careers.swissre.com") — there's
    # no vendor domain suffix to key URL_TO_SLUG off, so unlike every other
    # entry here there is NO URL_TO_SLUG["successfactors"]; discovery
    # happens by content fingerprint (any already-fetched page referencing
    # SAP's rmkcdn.successfactors.com asset CDN), wired into node.py's
    # _parse_detect exactly like grnh.se's special-cased resolver — see
    # node.py's _detect_successfactors_hit and ats_scrapers.scrape_successfactors
    # for the full evidence trail and GREYLIST_ATS.md for the writeup.
    "successfactors",
    # 2026-09: Hireology / isolvedhire — new platforms, found via the
    # datascry/openroles GitHub-registry discovery source (source 11
    # below) and confirmed live+scrapable end-to-end (real public JSON
    # APIs, no robots.txt block, no auth/JS needed). See
    # ats_scrapers.scrape_hireology/scrape_isolvedhire for the full
    # evidence trail.
    "hireology", "isolvedhire",
    # 2026-09: Gem — a Relay/GraphQL-rendered per-company job board at
    # jobs.gem.com/{slug} (no robots.txt at all — confirmed 404 on
    # jobs.gem.com/robots.txt). Confirmed live via real Chrome browser
    # network inspection (WebFetch can't see it — client-rendered, no
    # data in raw HTML): a public, keyless POST to
    # jobs.gem.com/api/public/graphql/batch with operation
    # "JobBoardList" (query field oatsExternalJobPostings(boardId:
    # $boardId)) returns the full jobPostings list for a real slug
    # (dragonfly-careers) with real title/location/department data —
    # confirmed 200 with no auth header of any kind. Per-job full
    # description comes from a second query, "ExternalJobPosting"
    # (oatsExternalJobPosting(boardId, extId) { descriptionHtml }),
    # same endpoint. See ats_scrapers.scrape_gem for the full request
    # shapes. No Wappalyzer fingerprint exists for Gem (checked
    # cdn.jsdelivr.net/gh/enthec/webappanalyzer@main/src/technologies/
    # g.json live — absent) so, like isolvedhire, it is NOT added to
    # HTTPARCHIVE_ATS_TECH_NAMES.
    "gem",
}

# The 4 genuinely dead-end ATS platforms (confirmed unscrapeable — robots.txt
# disallow, JS-only rendering, or an auth-gated API with no public
# alternative) are documented in ONE place now: Main/BLACKLISTED_ATS.md.
# Don't add per-platform detail back here — that file is the single source
# of truth for "why can't we scrape this."
#
# ycombinator: NOT an ATS at all (a multi-company job-board aggregator, not
# a single-company ATS) — a different kind of exclusion than the 4 above,
# so it isn't in that doc. There is no YC code path left anywhere in this
# project — not here, not in URL_TO_SLUG, not in SCRAPERS.

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
    # "applytojob"/"apply to job" re-mapped to "jazzhr" 2026-09 (revived —
    # see SUPPORTED_ATS comment above). OpenPostings' own platform list
    # still uses the "ApplyToJob" name, so both raw variants map here.
    "applytojob": "jazzhr",
    "apply to job": "jazzhr",
    "jazzhr": "jazzhr",
    "jazz hr": "jazzhr",
    "resumator": "jazzhr",
    "the resumator": "jazzhr",
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
    # 2026-09: added now that all 7 joined SUPPORTED_ATS (see that set's
    # comment for why) — a real working scraper exists for each. These
    # label-string variants follow the same defensive-alias pattern as
    # every other entry above, but — unlike those — haven't been
    # individually confirmed against OpenPostings' actual live ATS_name
    # values for these 7 specifically. Harmless either way: an unmatched
    # variant just means those rows keep falling through to "unmapped ATS"
    # in the log, same as today; it can't cause a wrong match.
    "adp": "adp", "adp workforce now": "adp", "workforce now": "adp",
    "brassring": "brassring", "ibm brassring": "brassring", "kenexa brassring": "brassring",
    "jobvite": "jobvite",
    "jobadder": "jobadder", "job adder": "jobadder",
    "folkshr": "folkshr", "folks hr": "folkshr",
    "avature": "avature",
    "eploy": "eploy",
    # occupop deliberately NOT mapped — occupop is NOT in SUPPORTED_ATS
    # (confirmed unscrapeable, see Main/BLACKLISTED_ATS.md), and
    # fetch_openpostings_slugs()'s slugs_by_ats dict is only pre-seeded
    # with SUPPORTED_ATS keys — mapping an ATS name here that isn't in
    # SUPPORTED_ATS would KeyError the first time OpenPostings actually
    # contains an Occupop row.
    # ukg/phenom: same reasoning — see Main/BLACKLISTED_ATS.md.
    # 2026-09: closed a real gap found while auditing discovery coverage
    # for the csod/paycom/successfactors reversals — OpenPostings' own
    # README (github.com/Masterjx9/OpenPostings) lists "PaycomOnline" as
    # one of its 80+ supported ATS labels, but it was never mapped here,
    # so paycom rows were silently falling into the "unmapped ATS" bucket
    # even after paycom got a real URL_TO_SLUG entry. Confirmed the same
    # README has NO "Cornerstone OnDemand"/"csod" entry at all, and no
    # entry that's actually SuccessFactors (its "SAP HR Cloud" listing is
    # a different, unconfirmed product — not assumed to be the same
    # thing, so deliberately NOT mapped to "successfactors" here) — so
    # there's genuinely nothing to add for either of those two.
    "paycomonline": "paycom",
    "paycom": "paycom",
    # "ycombinator" intentionally not mapped — see SUPPORTED_ATS comment
    # above (not a real ATS, no code path left in this project at all).
    # 2026-09: Hireology / isolvedhire — OpenPostings' own README
    # (github.com/Masterjx9/OpenPostings#supported-ats) confirmed live to
    # list BOTH of these (found while auditing that repo for other
    # unsupported platforms — see GREYLIST_ATS.md's 2026-09 writeup).
    # "Hireology" is a normal, cleanly-spelled label. isolvedhire's own
    # entry is genuinely mis-typed in their README as "isolvisolvedhire"
    # (confirmed verbatim via a raw-text search of their README, not a
    # transcription error on this end) — mapped as-is since that's the
    # literal string their own data will actually contain, plus the
    # sane spellings as defensive aliases in case their real ATS_name
    # field values differ from the README's own typo.
    "hireology": "hireology",
    "isolvisolvedhire": "isolvedhire",
    "isolvedhire": "isolvedhire",
    "isolved hire": "isolvedhire",
    "isolved": "isolvedhire",
    # 2026-09: Gem — confirmed live in the same OpenPostings README as a
    # clean, single "Gem" list entry (github.com/Masterjx9/OpenPostings —
    # "Supported ATS" section).
    "gem": "gem",
}

def _map_ats_name(name: str) -> str | None:
    """Case-insensitive ATS name lookup."""
    return _OPENPOSTINGS_ATS_MAP_RAW.get(name.lower().strip())

# Slugs to skip
SKIP_SLUGS = {
    "api", "www", "app", "static", "assets", "cdn", "docs", "help",
    "support", "blog", "login", "register", "test", "demo", "example",
    "staging", "dev", "sandbox", "admin", "",
    # 2026-09: confirmed real archive_i row — ats.rippling.com/careers/jobs
    # is Rippling's OWN careers page (bare "careers"), not a customer
    # board; real customer boards on this platform always compound it
    # ("routeware-careers", "asc-careers", ...), never use it bare.
    "careers",
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

# 2026-09: the ONLY real Greenhouse job-board hosts — exactly the 4 host
# patterns queried in CC_PLATFORM_PATTERNS["greenhouse"]. Kept as an exact
# allowlist rather than the old "greenhouse.io" in host substring check,
# which also matched Greenhouse's own marketing/corporate site
# (www.greenhouse.io and bare greenhouse.io) — confirmed live (2026-09)
# that generic marketing pages there (e.g. /contact, /about, /users —
# the client login portal) were being fed through this same extractor by
# other discovery sources that scan arbitrary company websites
# (resolve_candidate_page_to_ats_slug, node.py's _detect_ats_hits), each
# one's first path segment silently stored as a fake "company slug"
# ("contact", "about", "users", ...). A substring check on the host was
# never safe here — it also matches any unrelated host that merely
# CONTAINS the string "greenhouse.io" anywhere (e.g. "notgreenhouse.io").
# An exact-host allowlist has no such failure mode and doesn't lose any
# real coverage: every legitimate board/embed URL for this platform lives
# on one of these 4 hosts, so a URL on any other host was never a real
# per-company board to begin with.
_GREENHOUSE_BOARD_HOSTS = frozenset({
    "boards.greenhouse.io", "boards.eu.greenhouse.io",
    "job-boards.greenhouse.io", "job-boards.eu.greenhouse.io",
})


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
         'embed') row (see SKIP_SLUGS comment).

    Host check is an exact allowlist (_GREENHOUSE_BOARD_HOSTS), not a
    substring match — see that constant's comment for why the old
    substring check was letting marketing-site pages like
    www.greenhouse.io/contact through as fake company slugs."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host not in _GREENHOUSE_BOARD_HOSTS:
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


# 2026-09: Greenhouse's "Job Board" embed — a THIRD real deployment mode,
# distinct from both cases _url_to_slug_greenhouse already handles. Here
# the job lives on the CUSTOMER'S OWN domain with the numeric job id in a
# `gh_jid` query param (e.g. https://www.example.com/careers/?gh_jid=123),
# not on any *.greenhouse.io host at all — so _GREENHOUSE_BOARD_HOSTS'
# exact-host check (correctly) never matches it, and this job was falling
# through to a generic/unknown-ATS scrape that only ever sees a thin
# meta-description fallback (the real content loads client-side via
# Greenhouse's own API) — confirmed live on two real postings
# (spins.com/work-at-spins/?gh_jid=..., zesty.ai/open-jobs?gh_jid=...),
# both of which fetch as an empty JS shell with a plain GET.
#
# gh_jid alone is NOT proof of Greenhouse — a site can reuse that exact
# query-param name for its own unrelated job id (confirmed real-world:
# a HubSpot careers page uses gh_jid as HubSpot's own id, unconnected to
# any Greenhouse board) — so this is a two-step process: recover a
# CANDIDATE board token from the page's own HTML (never guessed from the
# hostname), then the caller (node._resolve_gh_jid_hits) must verify that
# token+id against Greenhouse's real API before trusting it.
_GH_JID_RE = re.compile(r"[?&]gh_jid=(\d+)")

# Every documented/observed way a Greenhouse embed leaves its board token
# sitting in the page's own HTML, checked in order of confidence:
#   1. the standard embeddable-widget script tag (support.greenhouse.io
#      "Host internal job board outside of Greenhouse")
#   2. the job_app iframe/embed URL (used by boards.greenhouse.io/embed/
#      job_app?for=SLUG&token=ID — the application-iframe variant)
#   3. a data-* attribute on the widget's mount element some custom
#      integrations use instead of (or alongside) the script tag
#   4. an inline JS variable assignment (gh_slug/ghSlug) some hand-rolled
#      or page-builder (e.g. Webflow) integrations use instead of the
#      standard script tag entirely
# All checked as plain substring/regex scans over the raw HTML text (not
# just <script src>) since the embed URL can just as easily sit inside an
# iframe's src, an inline <script> block, or already-escaped JSON — the
# same reasoning _extract_candidate_urls' Method B raw-text scan uses.
_GH_EMBED_TOKEN_PATTERNS = (
    re.compile(r"greenhouse\.io/embed/job_board(?:/js)?\?[^\"'\s>]*\bfor=([a-zA-Z0-9_-]+)", re.I),
    re.compile(r"greenhouse\.io/embed/job_app\?[^\"'\s>]*\bfor=([a-zA-Z0-9_-]+)", re.I),
    re.compile(r"data-(?:board-)?token=[\"']([a-zA-Z0-9_-]+)[\"']", re.I),
    re.compile(r"\b(?:gh_slug|ghSlug)\s*[=:]\s*[\"']([a-zA-Z0-9_-]+)[\"']"),
)


def extract_gh_jid_ids(url: str, html: str) -> set[str]:
    """All gh_jid numeric ids found on this page — the current page's own
    URL (the common case: the job itself was fetched at a ?gh_jid= URL)
    plus any gh_jid-carrying links elsewhere in its HTML (a listing page
    linking out to several individual jobs). Returns ids, not URLs — the
    caller already has the page's html to re-derive a token from."""
    ids = set(_GH_JID_RE.findall(url))
    ids.update(_GH_JID_RE.findall(html))
    return ids


def extract_greenhouse_embed_token(html: str) -> str | None:
    """Best-effort Greenhouse board-token recovery from a page's raw HTML —
    see _GH_EMBED_TOKEN_PATTERNS' comment for the specific signals tried,
    in confidence order. Returns None (never a guess) when no signal is
    present; the caller (node._resolve_gh_jid_hits) is responsible for
    verifying whatever token this DOES return against the real Greenhouse
    API before treating it as fact — a found token is a candidate, not a
    confirmed fact, exactly like every other slug this file extracts."""
    for pattern in _GH_EMBED_TOKEN_PATTERNS:
        m = pattern.search(html)
        if m:
            token = m.group(1)
            if token and token.lower() not in SKIP_SLUGS:
                return token
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
    the same company under an encoded vs. would-be-decoded spelling.

    2026-09 fix: host check was a bare `"ashbyhq.com" in host` substring,
    which also matched ashbyhq.com's own bare marketing/corporate domain —
    verified live this session (ashbyhq.com is Ashby's corporate site;
    jobs.ashbyhq.com/<org> is the real, sole customer-board pattern,
    confirmed against jobs.ashbyhq.com/ramp). That let short path segments
    off Ashby's own marketing pages (blog slugs, redirect stubs) get
    stored as if they were real customer org slugs — live archive_i rows
    included bare 1-2 character "slugs" like "D", "ha", "og", "up", "fr".
    Restricted to the real subdomain, same pattern as the Greenhouse/Lever
    fixes; also added the shared _looks_like_real_slug guard used by
    every sibling extractor, for the asset-filename/hash-shaped garbage
    it catches (it does not screen for short garbage by itself — length
    alone isn't a safe cutoff here since real short slugs exist, e.g.
    "0x", "ai", "g2")."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host != "jobs.ashbyhq.com":
        return None
    parts = parsed.path.strip("/").split("/")
    slug = unquote(parts[0]) if parts and parts[0] else None
    if slug and slug.lower() not in SKIP_SLUGS and _looks_like_real_slug(slug):
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


# 2026-09: real confirmed iCIMS customer subdomains, found live — the
# original code only stripped a leading "careers-", but real customers
# also use "jobs-" (jobs-selective.icims.com — the exact case that let a
# real company's iCIMS backend go unrecognized: selective.com/careers
# links to jobs.selective.com, which itself links to
# jobs-selective.icims.com), "jobs1-"/"jobs2-" (jobs1-donohoe.icims.com),
# and a 2-letter locale glued onto "careers" (encareers-cmh.icims.com,
# uscareers-acuren.icims.com). \d* covers a trailing digit some of these
# carry (jobs1-, jobs2-); the trailing "-" is required so a company whose
# real name happens to START with one of these words (no separator) is
# never touched — e.g. a hypothetical "jobsco.icims.com" keeps its full
# name, since "jobsco" has no "-" after "jobs".
_ICIMS_PORTAL_PREFIX_RE = re.compile(
    r"^(?:[a-z]{2}careers\d*|careers\d*|career\d*|jobs\d*|hiring|employment|recruiting|talent)-",
    re.I,
)


def _url_to_slug_icims(url: str) -> str | None:
    """Pattern: {optional-portal-prefix-}{company}.icims.com — see
    _ICIMS_PORTAL_PREFIX_RE's comment for the confirmed real prefixes.

    2026-09 fix: host check was a bare `"icims.com" in host` substring,
    same bug class as Rippling's — it also matched iCIMS's own bare
    marketing domain (icims.com itself), which would have survived the
    prefix-strip untouched and been stored as the literal slug
    "icims.com". Switched to a suffix check (host.endswith(".icims.com")),
    which excludes the bare root domain automatically — no special case
    needed, unlike Rippling's fix (icims.com's own subdomain form always
    has a leading dot to require). iCIMS's OWN careers hub
    (careers.icims.com) is still separately caught by SKIP_SLUGS
    containing "careers" (added during the Rippling fix)."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host.endswith(".icims.com"):
        return None
    slug = host[:-len(".icims.com")]
    slug = _ICIMS_PORTAL_PREFIX_RE.sub("", slug)
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
# 2026-09: confirmed via real archive_i rows (_url_to_slug_rippling) —
# ats.rippling.com/{locale}/{company}/jobs puts the locale code where
# Pattern 1 blindly grabs parts[0], so a locale-prefixed board URL stored
# the LOCALE as the "company slug" instead. 12 confirmed real rows: de-DE,
# en-AU, en-CA, en-GB, en-US, es-ES, fr-CA, fr-FR, nl-NL, pl-PL, pt-BR,
# pt-PT. Added here (not just in the Rippling extractor) because a real
# company slug matching this exact 2-letter or 2-letter-hyphen-2-letter
# shape is not a realistic collision for any extractor that uses this
# shared guard.
_LOCALE_CODE_RE = re.compile(r"^[a-z]{2}(-[A-Z]{2})?$")

# 2026-09: real archive_i rows confirmed a slug shaped like ONE SPECIFIC
# job's own posting — e.g. "events-marketing-manager-job-description",
# "executive-director-job-description" — getting stored as if it were a
# company's ATS tenant slug. This is the same false-positive shape
# node.py's own career-page detection had to guard against (see
# node._looks_like_single_job_posting_path); this shared guard is used by
# EVERY _url_to_slug_* extractor in this file, including the GitHub bulk
# source, so fixing it here covers all of them at once. Two independent
# signals, EITHER sufficient: (1) an explicit single-posting suffix or a
# trailing numeric posting ID, or (2) the slug ends in a job-ROLE word
# (manager/director/engineer/...) AND also contains a job-posting-title-
# shaped word earlier in it (seniority/department/etc) — a real company
# slug is a name, never shaped like one job's own title.
_JOB_POSTING_SLUG_SUFFIX_RE = re.compile(
    r"[-_](job-description|job-details|position-description|job-posting)$", re.I)
_JOB_POSTING_TRAILING_ID_RE = re.compile(r"[-_]\d{4,}$")
_JOB_ROLE_ENDING_WORDS_RE = re.compile(
    r"[-_](manager|director|engineer|analyst|specialist|coordinator|associate|"
    r"assistant|officer|lead|head|representative|executive|consultant|"
    r"administrator|supervisor|technician|developer)s?$", re.I)
_JOB_POSTING_TITLE_WORDS_RE = re.compile(
    r"(senior|junior|sr|jr|entry-level|full-time|part-time|remote|"
    r"marketing|sales|customer|account|product|project|program|"
    r"operations|finance|human-resources|software|data|"
    r"business-development|engineering)[-_]", re.I)


def _looks_like_job_posting_slug(candidate: str) -> bool:
    """True when a slug is shaped like one SPECIFIC job posting's own
    page rather than a company/tenant slug — see the regexes above this
    function for the two independent signals checked."""
    if _JOB_POSTING_SLUG_SUFFIX_RE.search(candidate) or _JOB_POSTING_TRAILING_ID_RE.search(candidate):
        return True
    if _JOB_ROLE_ENDING_WORDS_RE.search(candidate) and _JOB_POSTING_TITLE_WORDS_RE.search(candidate):
        return True
    return False


def _looks_like_real_slug(candidate: str) -> bool:
    """Shared guard for path-segment-based extractors: rejects the
    confirmed-in-production shapes of "this isn't a company slug" — a
    filename with a known static-asset extension, a bare hex hash/id with
    no extension at all (e.g. a CDN object key), a bare locale code
    (e.g. "en-US") picked up from a locale-prefixed path, or (2026-09) a
    slug shaped like one specific job posting's own page rather than a
    company/tenant slug."""
    if not candidate:
        return False
    if _ASSET_FILENAME_RE.search(candidate):
        return False
    if _BARE_HEX_HASH_RE.match(candidate):
        return False
    if _LOCALE_CODE_RE.match(candidate):
        return False
    if _looks_like_job_posting_slug(candidate):
        return False
    return True


def _url_to_slug_rippling(url: str) -> str | None:
    """2026-09 fix: two confirmed real archive_i contamination sources,
    both from bare host.replace()/parts[0] not accounting for cases where
    there's no real per-company segment to find:
    (1) host == "rippling.com" exactly (no subdomain at all) survived
        Pattern 2's `.replace(".rippling.com", "")` unchanged (that
        substring isn't present without a leading-dot subdomain), so the
        literal marketing domain got stored as slug "rippling.com" itself.
        Now requires an actual subdomain to be present first.
    (2) ats.rippling.com/{locale}/{company}/jobs (e.g. .../en-US/acme/jobs)
        put the locale code in parts[0], which Pattern 1 took
        unconditionally as "the company" — 12 confirmed real locale-code
        rows (en-US, nl-NL, de-DE, ...). Now caught by
        _looks_like_real_slug's locale-code rejection.
    """
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
        # Pattern 2: {company}.rippling.com (subdomain-based) — host must
        # actually HAVE a subdomain (bare "rippling.com" has none, so the
        # replace() below would otherwise leave the literal domain intact
        # and it would sail through as a fake "slug").
        if host != "rippling.com":
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


# SuccessFactors: still no URL_TO_SLUG entry here, but NOT because it's
# unscrapeable anymore — see SUPPORTED_ATS's comment above for the 2026-09
# reversal (modern Career Site Builder tenants ARE scraped now, via
# ats_scrapers.scrape_successfactors). It's absent from this dict
# specifically because there's no vendor domain suffix to key a URL-string
# extractor off (every tenant runs on its own branded domain) — discovery
# instead happens via node.py's _detect_successfactors_hit, which checks a
# fetched page's own CONTENT for SAP's rmkcdn.successfactors.com asset
# fingerprint, the same "needs real content, not just the URL" reasoning
# as grnh.se below. node.py's _ATS_VENDOR_DOMAINS still separately lists
# the legacy successfactors.com/.eu/sapsf.com/.eu host suffixes (those
# ARE a real, matchable domain, and stay genuinely robots.txt-blocked —
# see GREYLIST_ATS.md) for its own unrelated job: correctly classifying
# a page as "ATS-related, not in-house" even when it's a platform this
# project doesn't (or, for the legacy hosts, still can't) scrape.

def _url_to_slug_breezyhr(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "breezy.hr" in host:
        slug = host.replace(".breezy.hr", "").lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
    return None


# REMOVED 2026-09: this stale reference copy of the old (2026-08-retired)
# ApplyToJob extractor. Superseded by _url_to_slug_jazzhr below (added
# 2026-09, already registered in URL_TO_SLUG) — same platform, same host
# suffix, but with the _looks_like_real_slug guard this old copy lacked.
# See SUPPORTED_ATS's 2026-09 JazzHR revival comment for the full history.


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
    customer on this default domain.

    2026-09 BUG FIX: added a `"." not in slug` guard. A real Softgarden
    slug is always a single DNS label (e.g. "alloheim") — it can never
    itself contain a dot, since it's exactly the part of the hostname
    before one of the suffixes above. Real malformed examples that got
    through without this guard: "koelnbaeder.dekoelnbaeder" (a
    Wayback-Machine-sourced URL whose captured host didn't cleanly match
    any suffix above, so the fallback below let a dot-containing, clearly
    non-single-label string through), "app.career" and "aegps.comaegps"
    (same shape). None of these were ever going to resolve as a real
    tenant subdomain, so scrape_softgarden failed on every one of them —
    rejecting a dot-containing "slug" up front turns that into a clean
    skip at discovery time instead of a guaranteed scrape failure later."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    for suffix in (".softgarden.io", ".career.softgarden.de", ".softgarden.de"):
        if host.endswith(suffix):
            slug = host[: -len(suffix)].lower()
            if slug and slug not in SKIP_SLUGS and slug != "www" and "." not in slug:
                return slug
    # Also handle api.softgarden.io/api/.../jobboards/{channelId}
    if "softgarden" in host:
        path_match = re.search(r"/jobboards/([^/]+)", parsed.path)
        if path_match:
            candidate = path_match.group(1)
            if candidate and "." not in candidate:
                return candidate
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


# _url_to_slug_ycombinator REMOVED 2026-09 (at the user's request): YC/
# Work at a Startup is a multi-company job-board AGGREGATOR, not a
# per-company ATS — it has no entry in SCRAPERS or SUPPORTED_ATS, so any
# URL this resolved to ("ycombinator", slug) could never actually be
# scraped by crawl_i.py's scrape_board() dispatch. Worse, "ycombinator"
# sits in verification.py's _UNVERIFIABLE_ATS (no safe not-found check),
# so a row like that wouldn't even get cleaned up by the verification
# engine once created — a permanent dead-end row that only ever
# displaced a genuine ATS resolution for that same URL. Removing this
# entry (and its URL_TO_SLUG registration below) means a workatastartup.
# com/ycombinator.com URL now correctly falls through to "no ATS match"
# instead of being wrongly captured as a fake "ycombinator" ATS slug.


def _url_to_slug_eploy(url: str) -> str | None:
    """Extract slug from Eploy URLs.
    Pattern: {slug}.eploy.net/candidate/jobboard/...

    2026-09 BUG FIX: this used to check `"eploy.net" not in host` (a bare
    substring test) and strip via `host.replace(".eploy.net", "")` — both
    of which are fooled by any domain that merely CONTAINS "eploy.net" as
    a substring without actually being a *.eploy.net subdomain. Real
    example that got through: jawsdeploy.net ("...jaws-d[eploy.net]") —
    an unrelated deployment-tooling domain that happens to spell "eploy"
    right after a "d". The substring check let it past validation, and
    since the host doesn't contain the literal ".eploy.net" (with a
    leading dot), .replace() was a silent no-op — the "slug" ended up
    being the entire hostname (app.jawsdeploy.net, www.jawsdeploy.net,
    status.jawsdeploy.net), none of which are real Eploy customers at
    all. Fixed with a proper suffix check (same pattern already used by
    _url_to_slug_hrmdirect above)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host.endswith(".eploy.net"):
        return None
    slug = host[: -len(".eploy.net")].lower()
    if slug and slug not in SKIP_SLUGS and slug != "www" and "." not in slug:
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
    and just returns whatever 'source' segment is actually in the URL.

    2026-09 fix: confirmed live via ~100 real archive_i rows that every
    genuine PageUp portalId is a bare NUMERIC tenant code (820, 873,
    1097, 1151, ...) — never an English word. Without this check, a
    completely unrelated real-world path shape —
    careers.pageuppeople.com/employees/{person-name} — false-positived
    through undetected: "employees" is a fixed PageUp platform route (an
    employee-directory/bio page, not a per-tenant careers board at all;
    careers.pageuppeople.com/employees/ 404s on its own), and the actual
    second path segment on every one of these was a person's first-name-
    plus-last-initial (e.g. "damien-p", "celestine-h"), not a job-board
    'source' tag. Over 60 confirmed-fake rows of this exact shape were
    found live in production, all tagged 'employees|{name}', none a real
    company. Requiring portal_id to be all-digits rejects this whole
    class at the source instead of chasing each generic-word variant
    individually.

    NOTE: the 'source' segment deliberately does NOT go through the
    shared _looks_like_real_slug() guard — that guard's locale-code
    rejection (added for Rippling's locale-prefixed URLs) would reject
    genuine PageUp source tags too, since real ones are routinely short
    alphabetic codes shaped exactly like a locale ('cw', 'ci', 'fb') —
    confirmed live: '1000|cw' etc. are real, already-captured production
    rows. Only the SKIP_SLUGS check applies to source."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if "pageuppeople.com" not in host:
        return None
    parts = parsed.path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] and parts[1]:
        portal_id, source = parts[0], parts[1]
        if portal_id.isdigit() and source.lower() not in SKIP_SLUGS:
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


def _url_to_slug_hireology(url: str) -> str | None:
    """Extract slug from Hireology URLs (2026-09, new platform).
    Pattern: careers.hireology.com/{slug}/{job_id}/description — the
    slug is the FIRST path segment on this one shared host (unlike
    Pinpoint/etc, Hireology customers don't get their own subdomain).
    Confirmed live via openroles' own scraper source + a real tenant
    (careers.hireology.com/1sthonda/170266/description)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host != "careers.hireology.com":
        return None
    parts = parsed.path.strip("/").split("/")
    if parts and parts[0] and parts[0].lower() not in SKIP_SLUGS:
        return parts[0].lower()
    return None


def _url_to_slug_isolvedhire(url: str) -> str | None:
    """Extract slug from isolvedhire (iSolved Hire) URLs (2026-09, new
    platform). Pattern: {slug}.isolvedhire.com/... — subdomain-per-tenant,
    same shape as Pinpoint/BreezyHR. Confirmed live via real browser
    network inspection against 1stccu.isolvedhire.com."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host.endswith(".isolvedhire.com"):
        return None
    slug = host[: -len(".isolvedhire.com")].lower()
    if slug and slug not in SKIP_SLUGS and slug != "www":
        return slug
    return None


def _url_to_slug_gem(url: str) -> str | None:
    """Extract slug from Gem job-board URLs (2026-09, new platform).
    Pattern: jobs.gem.com/{slug}[/...] — shared host, slug is the first
    path segment, same shape as Hireology. The slug is exactly the
    "boardId" GraphQL variable used against jobs.gem.com's public
    api/public/graphql/batch endpoint — confirmed live via real Chrome
    browser network inspection against jobs.gem.com/dragonfly-careers
    (see ats_scrapers.scrape_gem for the confirmed request/response
    shapes)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host != "jobs.gem.com":
        return None
    parts = parsed.path.strip("/").split("/")
    if parts and parts[0] and parts[0].lower() not in SKIP_SLUGS:
        return parts[0].lower()
    return None


def _url_to_slug_flatchr(url: str) -> str | None:
    """Extract slug from Flatchr URLs (France).

    2026-09 BUG FIX: this used to also accept careers.flatchr.io/vacancy/
    {X}/... and return X as the company slug, on the assumption a vacancy
    URL's first path segment is the same company slug ats_scrapers.
    scrape_flatchr uses to build its OWN vacancy links
    (/vacancy/{company_slug}/{vacancy_id}). Confirmed LIVE that's wrong:
    a real Flatchr vacancy page is a single path segment,
    careers.flatchr.io/vacancy/{vacancy.slug} where vacancy.slug is
    "{lowercased per-vacancy id}-{job-title-slug}" (e.g.
    "8aby1n7jw70dlgjn-agent-de-surveillance-point-ecole") — fetched live
    from a real posting and cross-checked against the company/{slug}.json
    API response for the SAME job, whose real company slug
    ("mairiedesaintbrice") shares no relationship with that vacancy string
    at all. So parts[1] on a /vacancy/ URL was never the company slug —
    it's a per-job string with no company identifier recoverable from the
    URL alone (the real slug only exists inside the page/API payload,
    which a bare URL-to-slug extractor never fetches). Confirmed this was
    live, active corruption, not a one-off: 11,360 of 12,320 (92%) of
    flatchr's current archive_i rows carry this per-vacancy-shaped value
    instead of a real company slug as of 2026-09 — see verification.py's
    _UNVERIFIABLE_ATS entry for flatchr for why that also blocks adding a
    verifier until those rows are corrected. Fix: drop the /vacancy/
    branch entirely — only extract from the two URL families that
    genuinely carry the company slug:
      1. Shared board domain: {slug}.flatchr.io/...
      2. Company page: careers.flatchr.io/company/{slug}
    A /vacancy/ URL now yields no slug at all (None) rather than a wrong
    one — a real company posted under one of these two other URL
    families will still be discovered normally; this only stops seeding
    the SAME company from its vacancy URLs under a fabricated identifier."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host.endswith(".flatchr.io") and host != "careers.flatchr.io":
        slug = host[: -len(".flatchr.io")].lower()
        if slug and slug not in SKIP_SLUGS and slug != "www":
            return slug
        return None
    if host == "careers.flatchr.io":
        parts = parsed.path.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "company" and parts[1]:
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


# REMOVED 2026-09: _url_to_slug_homerun. Its only signal was "any host
# starting with jobs." — Homerun customers run on their own domain, not a
# shared subdomain, so there was never a real vendor-specific pattern to
# match. Confirmed live this session that this was catching mostly
# non-Homerun pages (see SUPPORTED_ATS's 2026-09 removal comment above for
# the concrete evidence) — removed everywhere: URL_TO_SLUG, SUPPORTED_ATS,
# ats_scrapers.py's scraper, and the stale archive_i rows.
# Its false-positive collision with Dayforce's jobs.dayforcehcm.com (the
# other platform sharing a "jobs." prefix, but on ONE shared vendor
# domain rather than each customer's own) no longer needs a guard now
# that this function is gone — _url_to_slug_dayforce below matches only
# that exact host regardless.


# Occupop: no extractor here anymore — confirmed genuinely unscrapeable
# (see Main/BLACKLISTED_ATS.md). node.py's _ATS_VENDOR_DOMAINS still lists
# occupop-careers.com on purpose (a different job: correctly classifying a
# page as "ATS-related, not in-house" regardless of scrapeability) — that
# list is NOT affected by this removal.

# 2026-09: Dayforce / Getro / JazzHR added — the top 3 platforms by volume
# in the latmay/ats-career-page-urls HF dataset (2181/1804/1325 rows
# respectively) that this project didn't already recognize. URL shapes
# below are confirmed against REAL sample rows pulled live from that
# dataset (not guessed from memory) — see this session's own research:
#   Dayforce: jobs.dayforcehcm.com/api/geo/associated, /api/geo/e0229, ...
#   Getro:    getro.getro.com/, 1up.getro.com/, 3m.getro.com/, ...
#   JazzHR:   l2t.applytojob.com/, 10xhealthsystem.applytojob.com/, ...
# Dayforce and Getro are SLUG-DISCOVERY ONLY for now — neither is in
# SUPPORTED_ATS, and no ats_scrapers.py scraper was added for either.
# Both are brand new here and would need their own real job-listing-API
# research (this dataset only confirms the CAREERS-PAGE URL shape, not the
# underlying jobs API) before a scraper could be written responsibly.
# Dayforce's real job-listing API was researched 2026-09 (see
# ats_scrapers.py's Dayforce section header) but NOT wired up — the
# response schema of the candidate-portal search API this session found
# (jobs.dayforcehcm.com/api/geo/<tenant>/jobposting/search, POST-only)
# could not be verified live (no browser tool connected this session,
# direct fetch blocked from this sandbox); Ceridian's officially
# documented JobFeeds REST API was also found and would avoid the schema-
# guessing problem entirely, but it lives on www.dayforcehcm.com, whose
# robots.txt disallows the whole site — off-limits per this project's
# robots.txt policy regardless of technical feasibility.
# JazzHR was revived 2026-09 — it's the SAME platform as the old
# "applytojob" entry removed 2026-08 (see SUPPORTED_ATS comment above),
# and IS now in SUPPORTED_ATS: that removal was a JD-enrichment/US-
# eligibility-filtering reliability problem, not a URL-pattern problem,
# and the two things responsible for it have both since been rewritten
# for unrelated reasons — see SUPPORTED_ATS's revival comment for the
# full evidence trail.
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


_CSOD_CAREERSITE_PATH_RE = re.compile(r"^/ux/ats/careersite/(\d+)/", re.I)


def _url_to_slug_csod(url: str) -> str | None:
    """Cornerstone OnDemand — subdomain-per-tenant on csod.com
    ({tenant}.csod.com/ux/ats/careersite/{siteId}/home[/requisition/{id}]);
    confirmed live via real examples: cn360.csod.com, ama-assn.csod.com,
    msc.csod.com, survitec.csod.com, merlin.csod.com, each carrying a
    matching '?c={tenant}' query param.

    2026-09 REVERSAL: promoted out of discovery-only. The original
    verdict here (client-side-rendered app shell, no server-rendered job
    content, "same failure class as SuccessFactors") was correct about
    the RENDERED page but wrong about the underlying platform — live
    testing (real browser, two independent tenants: CN Rail/cn360 and
    Survitec) found the ~5KB bootstrap HTML response for ANY
    /ux/ats/careersite/{siteId}/home URL embeds a genuine anonymous
    bearer JWT as plain text (`"token":"eyJ..."`) plus the tenant's
    regional API host (`"cloud":"https://{region}.api.csod.com/"`) —
    confirmed present in the RAW HTTP response body (fetched with
    fetch(..., {cache:'no-store'}) before any JS ran), not something
    client-JS synthesizes — so a plain requests.get() sees it exactly
    like scrape_csod's bootstrap fetch does. That token is honored by
    POST {cloud}rec-job-search/external/jobs with zero cookies/session,
    and its own `rurls` JWT claim explicitly whitelists
    "rec-job-search/external" as an anonymous-accessible route — this is
    a deliberate anonymous API, not an accident. See ats_scrapers.py's
    scrape_csod for the full request shape and the careerSitePageId
    quirk (a per-tenant value that must be discovered, doesn't reliably
    match the URL's siteId) and GREYLIST_ATS.md for the evidence trail.

    Slug format is now 'tenant|siteId' (siteId from the URL path, e.g.
    'cn360|3') — scrape_csod needs both to rebuild the bootstrap URL,
    same pipe-delimited convention as _url_to_slug_workday above. Falls
    back to careersite id '1' (confirmed the most common default/home
    site id) when a matched URL's path doesn't carry a numeric siteId
    segment, so a plain tenant-root URL still yields a usable slug
    instead of being dropped."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host.endswith(".csod.com"):
        return None
    tenant = host[: -len(".csod.com")]
    if not (tenant and tenant != "www" and tenant not in SKIP_SLUGS and _looks_like_real_slug(tenant)):
        return None
    m = _CSOD_CAREERSITE_PATH_RE.match(parsed.path or "")
    site_id = m.group(1) if m else "1"
    return f"{tenant}|{site_id}"


_PAYCOM_CLIENTKEY_RE = re.compile(r"^[0-9A-Fa-f]{32}$")


def _url_to_slug_paycom(url: str) -> str | None:
    """Paycom — ALL customers share one domain (www.paycomonline.net,
    NOT paycomonline.com — node.py's _ATS_VENDOR_DOMAINS had the wrong
    TLD before this fix, confirmed live: every real example found
    resolves under .net, .com was never seen live). Tenant is a 32-hex-
    character 'clientkey' in the path
    (/v4/ats/web.php/portal/{clientkey}/jobs or /career-page), not a
    subdomain — confirmed live via multiple real examples (e.g.
    74B8425BF3D1B3ACB19CC1353DC5FA0E, 5AA9970AFB7E7320DA597F2CF00E6958).

    2026-09 REVERSAL: promoted out of discovery-only, same session as
    Cornerstone's. The original verdict here (below, kept for the record)
    was right about the RENDERED page (both the /career-page listing and
    an individual /jobs/{id} detail page really are client-side-rendered
    app shells — "You need to enable JavaScript to run this app.") but
    wrong about the underlying platform: live testing (real browser, two
    independent tenants: FUTEK on clientkey
    5AA9970AFB7E7320DA597F2CF00E6958, plus a second unrelated tenant on
    74B8425BF3D1B3ACB19CC1353DC5FA0E) found the career-page bootstrap
    HTML embeds a genuine anonymous bearer JWT and the tenant's own
    regional API base URL ("atsPortalMantleServiceUrl") as plain text —
    confirmed present in the raw response, same as Cornerstone's token.
    That token genuinely authenticates a real job-search API
    (POST {base}api/ats/job-posting-previews/search) with no per-customer
    OAuth needed. See ats_scrapers.py's scrape_paycom for the full
    request shape (the exact body needed a specific "filtersForQuery"
    wrapper with every filter category present as an empty array — a
    naive {skip,take} body silently returns zero results, the same class
    of quirk as Cornerstone's careerSitePageId) and GREYLIST_ATS.md for
    the evidence trail. Slug format is UNCHANGED by this reversal (still
    just the 32-hex clientkey) — scrape_paycom discovers the tenant's
    regional API host itself from the bootstrap page, so no second value
    needs to travel in the slug the way Cornerstone's siteId does.

    Original discovery-only verdict (2026-09, superseded above): no
    documented public unauthenticated job-search API was found; third-
    party job aggregators do index Paycom postings at real scale, but
    describe their own scraping/normalization layer, not a Paycom
    endpoint that's usable here. That was true of Paycom's *documented*
    API — it's the separate anonymous bootstrap-token side channel above
    that this reversal found instead."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host not in ("www.paycomonline.net", "paycomonline.net"):
        return None
    m = re.match(r"^/v4/ats/web\.php/portal/([0-9A-Fa-f]{32})(?:/|$)", parsed.path)
    if not m:
        return None
    clientkey = m.group(1)
    if _PAYCOM_CLIENTKEY_RE.match(clientkey):
        return clientkey.upper()
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
    # ukg/phenom: no entry — confirmed unscrapeable, see Main/BLACKLISTED_ATS.md.
    # Kept out of here on purpose: node.py's _detect_ats_hits shares this
    # same dict, so an entry here would keep flagging pages as this
    # platform with no way to ever turn that into real job data.
    # successfactors: ALSO no entry here (see the standalone comment near
    # this dict's SuccessFactors extractor placeholder above), but for a
    # different reason — it IS scraped now (2026-09 reversal, see
    # SUPPORTED_ATS's comment), just not detectable from a URL string
    # alone. Its hits come from node.py's _detect_successfactors_hit
    # (content-fingerprint check) instead of this dict.
    "breezyhr": _url_to_slug_breezyhr,
    # "applytojob" removed 2026-08 — see SUPPORTED_ATS comment above.
    "hrmdirect": _url_to_slug_hrmdirect,
    "softgarden": _url_to_slug_softgarden,
    "zoho": _url_to_slug_zoho,
    "paylocity": _url_to_slug_paylocity,
    # "ycombinator" removed 2026-09 — see the comment above where its
    # extractor function used to live (aggregator, not a real per-company
    # ATS; was producing permanently-unscrapable archive_i rows).
    "personio": _url_to_slug_personio,
    "joincom": _url_to_slug_joincom,
    # New (2026-08):
    "eploy": _url_to_slug_eploy,
    "folkshr": _url_to_slug_folkshr,
    "jobadder": _url_to_slug_jobadder,
    "jobvite": _url_to_slug_jobvite,
    "adp": _url_to_slug_adp,
    "avature": _url_to_slug_avature,
    # New (2026-09): PageUp / Pinpoint / Flatchr / Jobylon / Occupop
    "pageup": _url_to_slug_pageup,
    "pinpoint": _url_to_slug_pinpoint,
    "flatchr": _url_to_slug_flatchr,
    "jobylon": _url_to_slug_jobylon,
    # homerun: REMOVED 2026-09 — see the removal comment right above
    # _url_to_slug_dayforce below (its extractor used to live here).
    # occupop: no entry — confirmed unscrapeable, see Main/BLACKLISTED_ATS.md
    # and the comment just above (successfactors) for the same reasoning.
    # New (2026-09): Dayforce/Getro are still slug-discovery only, see the
    # block comment above these functions — no scraper/SUPPORTED_ATS entry
    # yet. Cornerstone (csod) AND Paycom are DIFFERENT as of 2026-09: both
    # were reversed out of discovery-only into real scrapers (see
    # _url_to_slug_csod/_url_to_slug_paycom's docstrings above and
    # ats_scrapers.scrape_csod/scrape_paycom) — both are now also in
    # SUPPORTED_ATS and CC_EXTRACTORS/CC_PLATFORM_PATTERNS below, unlike
    # Dayforce/Getro.
    "dayforce": _url_to_slug_dayforce,
    "getro": _url_to_slug_getro,
    "csod": _url_to_slug_csod,
    "paycom": _url_to_slug_paycom,
    "jazzhr": _url_to_slug_jazzhr,
    # New (2026-09): Hireology / isolvedhire — see SUPPORTED_ATS comment above.
    "hireology": _url_to_slug_hireology,
    "isolvedhire": _url_to_slug_isolvedhire,
    # New (2026-09): Gem — see SUPPORTED_ATS comment above.
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
# SOURCE: iCIMS HR Jobs (centralized multi-tenant board)
# ══════════════════════════════════════════════════════════

_ICIMS_HRJOBS_API = "https://hrjobs.icims.com/api/jobs"


def fetch_icims_hrjobs_slugs(max_pages: int = 500) -> dict[str, dict[str, str]]:
    """hrjobs.icims.com — iCIMS's own centralized board for HR-professional
    roles across its customer base (NOT all iCIMS customers/industries —
    a narrower vertical board, confirmed live via its page copy: "iCIMS
    customers are hiring... for HR professionals"). Real, public,
    unauthenticated, paginated JSON — confirmed live via Chrome network
    capture, not guessed: GET .../api/jobs?page=N&sortBy=relevance&
    descending=false&internal=false, no auth, 10 jobs/page, no page-count
    field (stop condition is an empty page). Each entry's data.apply_url
    is a real careers-{company}.icims.com/jobs/{id}/... URL — exactly the
    subdomain shape _url_to_slug_icims already parses, so this reuses that
    converter as-is rather than re-deriving a slug locally.

    2026-09: added — a much cheaper way to catch NEW iCIMS customers than
    waiting for them to surface via Common Crawl/HTTP Archive: iCIMS's own
    board lists them directly."""
    converter = URL_TO_SLUG["icims"]
    found: dict[str, str] = {}
    page = 1
    while page <= max_pages:
        try:
            r = requests.get(_ICIMS_HRJOBS_API, params={
                "page": page, "sortBy": "relevance", "descending": "false", "internal": "false",
            }, timeout=30)
            r.raise_for_status()
            jobs = r.json().get("jobs") or []
        except Exception as e:
            log.warning(f"iCIMS HR Jobs: page {page} failed, stopping: {e}")
            break
        if not jobs:
            break
        for entry in jobs:
            data = entry.get("data") or {}
            url = data.get("apply_url")
            name = data.get("brand") or data.get("hiring_organization") or ""
            if not url:
                continue
            slug = converter(url)
            if slug:
                found[slug] = name
        page += 1

    log.info(f"iCIMS HR Jobs: {len(found)} slugs across {page - 1} page(s)")
    return {"icims": found}


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
    # own docstring — the extractor's _GREENHOUSE_BOARD_HOSTS allowlist
    # already includes it, only this query pattern was missing.
    # 2026-09: added job-boards.greenhouse.io(+.eu) — Greenhouse's newer
    # "Job Boards 2.0" hosted-board domain, confirmed real and CURRENTLY
    # GROWING (Greenhouse's own support docs describe a legacy
    # boards.greenhouse.io deprecation plan) via live examples
    # (job-boards.greenhouse.io/remotecom, /hubspotjobs, /current81,
    # job-boards.eu.greenhouse.io/openup). Missing this domain would mean
    # missing an increasing share of current/future Greenhouse customers,
    # not just a handful of edge cases — kept the legacy boards.* patterns
    # too since that domain still carries huge historical volume and isn't
    # fully retired. _url_to_slug_greenhouse's _GREENHOUSE_BOARD_HOSTS
    # allowlist already includes this domain, no extractor change needed.
    # (2026-09: that allowlist replaced an earlier "greenhouse.io" in host
    # substring check, which also matched Greenhouse's own marketing site
    # and produced fake slugs like "contact"/"about"/"users" — see
    # _GREENHOUSE_BOARD_HOSTS' comment.)
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
    # "jazzhr" (formerly "applytojob", removed 2026-08) revived 2026-09 —
    # see SUPPORTED_ATS comment above. Subdomain-per-tenant, same as
    # breezyhr above — any path under the tenant's host is a real board.
    "jazzhr": ["*.applytojob.com/*"],
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
    # actively re-enabled/working. Confirmed
    # live customer examples: sjobs.brassring.com/TGnewUI/Search/Home/Home
    # (Lowe's, Kodak), krb-sjobs.brassring.com/TGnewUI/Search/Home/Home
    # (IBM, Ahold) — a leading wildcard subdomain catches both the
    # standard "sjobs." and the "krb-sjobs." enterprise-customer variant.
    # _url_to_slug_brassring extracts entirely from the partnerid/siteid
    # query params, not the path, so this one pattern is sufficient
    # regardless of which exact path/casing (TGnewUI vs TGNewUI) a
    # specific customer's URL happens to use.
    "brassring": ["*.brassring.com/TGnewUI/*"],
    # SuccessFactors deliberately has NO Common Crawl pattern still — this
    # is unchanged by the 2026-09 reversal (see SUPPORTED_ATS's comment):
    # it's scraped now, but a CSB tenant runs on its own branded domain
    # with no shared, CC-indexable URL shape to query for (unlike every
    # other platform in this dict). Also means it has no Wayback CDX
    # pattern either, since fetch_wayback_slugs reuses this exact dict —
    # discovery for it instead runs entirely through node.py's own crawl
    # (content-fingerprint detection), not this Common-Crawl/Wayback path.
    # New (2026-09): PageUp / Pinpoint / Flatchr / Jobylon — all 4 have a
    # real shared-domain URL shape to query for. Occupop has NO entry
    # here — same reasoning as SuccessFactors above (confirmed JS-rendered
    # SPA, no working scraper yet, so discovering slugs for it would be
    # wasted effort until that's fixed). Homerun (also never entered here)
    # was removed from this project entirely 2026-09 — see SUPPORTED_ATS's
    # removal comment above for why.
    "pageup": ["careers.pageuppeople.com/*"],
    "pinpoint": ["*.pinpointhq.com/*"],
    "flatchr": ["*.flatchr.io/*", "careers.flatchr.io/company/*"],
    "jobylon": ["emp.jobylon.com/companies/*"],
    # 2026-09: Cornerstone OnDemand (csod) — REVERSED out of discovery-only,
    # now has a real working scraper (see SUPPORTED_ATS comment above and
    # ats_scrapers.scrape_csod), so discovering its slugs via Common Crawl
    # is worth doing now (it wasn't when this was discovery-only, same
    # reasoning SuccessFactors/Occupop are excluded above). Dayforce/
    # Getro stay excluded here — no scraper exists for them yet.
    "csod": ["*.csod.com/ux/ats/careersite/*"],
    # 2026-09: Paycom — ALSO reversed, same reasoning (see SUPPORTED_ATS
    # comment above and ats_scrapers.scrape_paycom). _url_to_slug_paycom's
    # existing pattern already matches both real path shapes.
    "paycom": ["www.paycomonline.net/v4/ats/web.php/portal/*/jobs*",
               "www.paycomonline.net/v4/ats/web.php/portal/*/career-page*"],
    # New (2026-09): Hireology / isolvedhire — see SUPPORTED_ATS comment above.
    "hireology": ["careers.hireology.com/*/*/description"],
    "isolvedhire": ["*.isolvedhire.com/*"],
    # New (2026-09): Gem — see SUPPORTED_ATS comment above.
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
    # "jazzhr" revived 2026-09 — see SUPPORTED_ATS comment above. Kept in
    # sync with CC_PLATFORM_PATTERNS above (these two dicts must always
    # match keys — see the CC_EXTRACTORS KeyError incident earlier this
    # project for why a mismatch here crashes the whole Common Crawl run).
    "jazzhr": _url_to_slug_jazzhr,
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
    # Still no SuccessFactors entry here either — kept out of BOTH dicts
    # together, matching keys as this dict's own comment above requires —
    # but (2026-09 reversal) it IS scraped now via a different discovery
    # path entirely; see CC_PLATFORM_PATTERNS's comment above and
    # SUPPORTED_ATS's comment for the full detail.
    "brassring": _url_to_slug_brassring,
    # New (2026-09) — kept in sync with CC_PLATFORM_PATTERNS above (no
    # Occupop entry here either — see that dict's comment; Homerun never
    # had one here and was removed from this project entirely 2026-09):
    "pageup": _url_to_slug_pageup,
    "pinpoint": _url_to_slug_pinpoint,
    "flatchr": _url_to_slug_flatchr,
    "jobylon": _url_to_slug_jobylon,
    # 2026-09: Cornerstone OnDemand — kept in sync with CC_PLATFORM_PATTERNS
    # above (see SUPPORTED_ATS comment for the reversal). _url_to_slug_csod
    # already returns the 'tenant|siteId' shape scrape_csod expects.
    "csod": _url_to_slug_csod,
    # 2026-09: Paycom — kept in sync with CC_PLATFORM_PATTERNS above (see
    # SUPPORTED_ATS comment for the reversal). _url_to_slug_paycom's
    # existing 32-hex-clientkey extraction needs no changes.
    "paycom": _url_to_slug_paycom,
    # New (2026-09): Hireology / isolvedhire — see CC_PLATFORM_PATTERNS above.
    "hireology": _url_to_slug_hireology,
    "isolvedhire": _url_to_slug_isolvedhire,
    # New (2026-09): Gem — see CC_PLATFORM_PATTERNS above.
    "gem": _url_to_slug_gem,
}


# ── Live pre-write dead-check for Common-Crawl/Wayback-sourced slugs
# (2026-09) ─────────────────────────────────────────────────────────
# Both fetch_commoncrawl_slugs() and fetch_wayback_slugs() below derive
# every candidate (ats, slug) pair purely from a HISTORICAL URL INDEX —
# Common Crawl's CDX index, or the Wayback Machine's CDX index — neither
# one ever fetches the resulting board itself. A URL sitting in that
# index from some past crawl date is no guarantee the board is still
# there today. Real case this closes (2026-09): a Common-Crawl-indexed
# https://boards.greenhouse.io/moonpay-style URL got written to
# archive_i (source="common_crawl_probe"), but the company had since
# moved off Greenhouse entirely — fetching that exact URL live now
# returns Greenhouse's own "Page not found ... no longer active" page.
# Every OTHER discovery source either fetches the page live moments
# before recording a hit (node.py) or comes from a maintained, curated
# feed (OpenPostings) — this dead-index problem is specific to CC/
# Wayback, so the fix is scoped to just those two.
#
# For the handful of platforms with a cheap, safe, already-proven
# existence check (mirrors verification.py's ARCHIVE_II_VERIFIERS —
# same endpoints, same 404-means-dead interpretation, just called
# synchronously here instead of via aiohttp), do that same check before
# a CC/Wayback-derived slug is ever written. Anything confirmed dead
# (404) is dropped outright; anything else — a real 200, a timeout, a
# 5xx, any other ambiguous response — is KEPT, same conservative
# "never delete on ambiguity" default verification.py itself uses.
# Platforms not in this dict (no cheap safe check exists for them, same
# reasoning as verification.py's _UNVERIFIABLE_ATS) pass through
# unchecked, exactly as before this fix.
def _cc_check_status(url: str) -> bool | None:
    """True = confirmed alive (200). False = confirmed dead (404).
    None = ambiguous (any other status or a network/timeout error) —
    ambiguous is NEVER treated as dead."""
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": _ROBOTS_UA})
        if r.status_code == 404:
            return False
        if r.status_code == 200:
            return True
        return None
    except Exception:
        return None


def _cc_check_lever(slug: str) -> bool | None:
    """Only DEAD if BOTH the main and EU endpoints confirm 404 — a real
    board can legitimately live on either one, same as
    verification.py's _verify_lever."""
    results = [
        _cc_check_status(f"https://api.lever.co/v0/postings/{slug}?mode=json"),
        _cc_check_status(f"https://api.eu.lever.co/v0/postings/{slug}?mode=json"),
    ]
    if True in results:
        return True
    if all(r is False for r in results):
        return False
    return None


# ── 2026-09: extended to EVERY platform verification.py already has a
# proven-safe check for (its ARCHIVE_II_VERIFIERS registry — 19
# platforms total), not just the original 5. There was no technical
# reason the other 14 were left out; sync ports of each verifier below,
# faithful to the exact same signal verification.py's own async version
# uses, just called synchronously here instead of via aiohttp. ──

def _cc_check_subdomain_tenant(host: str, path: str = "/") -> bool | None:
    """Sync mirror of the several verification.py checks (teamtailor,
    recruitee, softgarden, zoho, hrmdirect, icims, personio) whose real
    signal is: a dead tenant's subdomain request gets redirected AWAY
    from that exact subdomain (bounced to the platform's own generic
    marketing site), while a real tenant — even an empty one — always
    resolves and stays on its own subdomain, 200 OK."""
    try:
        r = requests.get(f"https://{host}{path}", timeout=10,
                          headers={"User-Agent": _ROBOTS_UA}, allow_redirects=True)
    except Exception:
        return None
    final_host = urlparse(r.url).hostname or ""
    if final_host != host:
        return False  # bounced away — confirmed dead, same as verification.py
    return True if r.status_code == 200 else None


def _cc_check_multi_host_tenant(hosts_paths: list) -> bool | None:
    """For platforms whose tenant can live under more than one hostname
    (iCIMS, Personio) — alive if EITHER resolves; dead only if BOTH
    cleanly bounce away (never on a bare network error)."""
    results = [_cc_check_subdomain_tenant(h, p) for h, p in hosts_paths]
    if True in results:
        return True
    if all(r is False for r in results):
        return False
    return None


def _cc_check_bamboohr(slug: str) -> bool | None:
    host = f"{slug}.bamboohr.com"
    try:
        r = requests.get(f"https://{host}/careers/list", timeout=10, allow_redirects=True,
                          headers={"Accept": "application/json", "User-Agent": _ROBOTS_UA})
    except Exception:
        return None
    final_host = urlparse(r.url).hostname or ""
    if final_host != host:
        return False
    if r.status_code != 200 or "application/json" not in r.headers.get("Content-Type", ""):
        return None
    try:
        data = r.json()
    except Exception:
        return None
    return True if isinstance(data, dict) and "result" in data else None


def _cc_check_jobvite(slug: str) -> bool | None:
    """A nonexistent Jobvite company 302s to jobvite.com's own support
    page with a distinctive '?invalid=1' query param — a real board
    (even an empty one) serves its own jobs page directly."""
    try:
        r = requests.get(f"https://jobs.jobvite.com/{slug}/jobs", timeout=10,
                          headers={"User-Agent": _ROBOTS_UA}, allow_redirects=False)
    except Exception:
        return None
    if r.status_code == 200:
        return True
    if r.status_code in (301, 302, 303, 307, 308):
        return False if "invalid=1" in r.headers.get("Location", "") else None
    return None


def _cc_check_paylocity(slug: str) -> bool | None:
    """slug is 'company_id|company_name_slug' (see
    discovery._url_to_slug_paylocity). A real board's page embeds a
    window.pageData JSON blob regardless of open-job count; a fake
    company_id serves a static 'does not exist'/'job not found' page."""
    parts = slug.split("|", 1)
    if len(parts) != 2:
        return None
    company_id, company_name_slug = parts
    url = f"https://recruiting.paylocity.com/recruiting/jobs/All/{company_id}/{company_name_slug}"
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": _ROBOTS_UA})
    except Exception:
        return None
    if r.status_code != 200:
        return None
    if "window.pageData" in r.text:
        return True
    lowered = r.text.lower()
    if "does not exist" in lowered or "job not found" in lowered:
        return False
    return None


_DNS_FAILURE_RE = re.compile(
    r"nodename nor servname provided|name or service not known|"
    r"getaddrinfo failed|no address associated with hostname|failed to resolve",
    re.I,
)


def _cc_dns_dead_check(url: str) -> bool | None:
    """Sync mirror of verification.py's _dns_dead_check (avature/eploy/
    taleo): True only when the failure is a genuine DNS resolution
    failure — the subdomain was never provisioned at all. Any other
    failure (refused, timeout, a non-DNS connection error) is ambiguous,
    never dead."""
    try:
        requests.get(url, timeout=10, headers={"User-Agent": _ROBOTS_UA}, allow_redirects=True)
        return True  # resolved and got SOME response — tenant exists
    except requests.exceptions.ConnectionError as e:
        return False if _DNS_FAILURE_RE.search(str(e)) else None
    except Exception:
        return None


_CC_LIVE_CHECK = {
    "greenhouse": lambda slug: _cc_check_status(
        f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"),
    "lever": _cc_check_lever,
    "ashby": lambda slug: _cc_check_status(
        f"https://api.ashbyhq.com/posting-api/job-board/{slug}"),
    "workable": lambda slug: _cc_check_status(
        f"https://apply.workable.com/api/v1/widget/accounts/{slug}"),
    "rippling": lambda slug: _cc_check_status(
        f"https://ats.rippling.com/api/v2/board/{slug}/jobs"),
    "bamboohr": _cc_check_bamboohr,
    "icims": lambda slug: _cc_check_multi_host_tenant(
        [(f"{slug}.icims.com", "/"), (f"careers-{slug}.icims.com", "/")]),
    "teamtailor": lambda slug: _cc_check_subdomain_tenant(f"{slug}.teamtailor.com"),
    "recruitee": lambda slug: _cc_check_subdomain_tenant(f"{slug}.recruitee.com"),
    "softgarden": lambda slug: _cc_check_subdomain_tenant(f"{slug}.softgarden.io"),
    "zoho": lambda slug: _cc_check_subdomain_tenant(f"{slug}.zohorecruit.com"),
    "hrmdirect": lambda slug: _cc_check_subdomain_tenant(f"{slug}.hrmdirect.com"),
    "personio": lambda slug: _cc_check_multi_host_tenant(
        [(f"{slug}.jobs.personio.de", "/"), (f"{slug}.jobs.personio.com", "/")]),
    "joincom": lambda slug: _cc_check_status(f"https://join.com/companies/{slug}"),
    "paylocity": _cc_check_paylocity,
    "jobvite": _cc_check_jobvite,
    "avature": lambda slug: _cc_dns_dead_check(f"https://{slug}.avature.net/careers/SearchJobs"),
    "eploy": lambda slug: _cc_dns_dead_check(
        f"https://{slug}.eploy.net/candidate/jobboard/vacancysearchresults.aspx"),
    "taleo": lambda slug: _cc_dns_dead_check(f"https://{slug.split('|', 1)[0]}.taleo.net/"),
}
# NOT included, deliberately:
#  - workday, smartrecruiters, breezyhr, oracle_cloud_hcm, jobadder,
#    folkshr, adp, brassring — same platforms in verification.py's own
#    _UNVERIFIABLE_ATS, for the exact same researched reasons (e.g.
#    Workday: confirmed 2026-09 that a fake tenant subdomain resolves
#    anyway, so there's no safe "doesn't exist" signal to check at all —
#    verification.py never checks these either, archive_i rows on them
#    are left completely alone, only counted).
#  - jazzhr, pageup, pinpoint, flatchr, jobylon — newer CC_PLATFORM_
#    PATTERNS entries that simply haven't been researched into
#    verification.py yet (no verifier AND no _UNVERIFIABLE_ATS entry —
#    genuinely unresearched, not confirmed either way). Per this
#    project's zero-guessed-claims rule, they stay unchecked here too
#    until that research happens — same list to extend in both files.


def _drop_dead_cc_slugs(slugs_by_ats: dict, label: str, max_workers: int = 20) -> dict:
    """Applied to a CC/Wayback fetch_*_slugs() result right before it's
    returned — see the module comment above _CC_LIVE_CHECK for why only
    these two sources need this. `slugs_by_ats` values may be a set[str]
    (no company name) or a dict[str, str] ({slug: name}); the returned
    dict preserves whichever shape each ATS's value came in as.

    2026-09 fix: this used to check every candidate slug fully serially —
    one `requests.get(timeout=10)` (up to TWO sequential ones for the
    multi-host-tenant checkers like icims/personio, so up to 20s) plus a
    flat time.sleep(0.3) between EVERY single slug, across all 19
    platforms _CC_LIVE_CHECK now covers. With the hundreds-to-thousands of
    candidate slugs a real CC/Wayback fetch produces, that serial loop's
    worst case ran into the tens of minutes — confirmed live: a real
    fetch_wayback_slugs() run got killed by the workflow's own cancellation
    (KeyboardInterrupt mid-request inside _cc_check_paylocity) after
    running long past what a "quick live pre-check" should ever take.
    Each check hits its own distinct per-slug host (a different company's
    subdomain/tenant), so — same reasoning as fetch_yc_slugs's per-domain
    concurrent checks elsewhere in this file — there's no single shared
    endpoint here that concurrency would overload. Runs all checks across
    every ATS at once via a bounded thread pool instead of one ATS-then-
    the-next serial pass.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    dropped = checked = 0
    out = {}
    is_dict_by_ats = {}
    kept_by_ats = {}
    work = []  # (ats, slug, name)

    for ats, slugs in slugs_by_ats.items():
        checker = _CC_LIVE_CHECK.get(ats)
        if not checker or not slugs:
            out[ats] = slugs
            continue
        is_dict = isinstance(slugs, dict)
        is_dict_by_ats[ats] = is_dict
        kept_by_ats[ats] = {}
        items = list(slugs.items()) if is_dict else [(s, "") for s in slugs]
        for slug, name in items:
            work.append((ats, slug, name))

    def _check_one(ats, slug, name):
        checker = _CC_LIVE_CHECK[ats]
        try:
            is_dead = checker(slug) is False
        except Exception:
            is_dead = False  # a checker crashing is not evidence of death
        return ats, slug, name, is_dead

    if work:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_check_one, ats, slug, name) for ats, slug, name in work]
            for future in as_completed(futures):
                ats, slug, name, is_dead = future.result()
                checked += 1
                if is_dead:
                    dropped += 1
                else:
                    kept_by_ats[ats][slug] = name

    for ats in kept_by_ats:
        kept = kept_by_ats[ats]
        out[ats] = kept if is_dict_by_ats[ats] else set(kept)
    if checked:
        log.info(f"{label}: live pre-check confirmed {dropped}/{checked} candidate "
                 f"slugs are already dead — dropped before ever reaching archive_i")
    return out


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

    cc_shard/cc_total_shards split the PLATFORMS in CC_PLATFORM_PATTERNS
    (36 as of 2026-09 — this count has drifted upward many times since
    this docstring was first written; not re-pinning it to a literal
    number here) (not a hash of work
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

    return _drop_dead_cc_slugs(slugs_by_ats, "Common Crawl")


# ══════════════════════════════════════════════════════════
# WAYBACK MACHINE CDX — cross-platform supplemental discovery
# ══════════════════════════════════════════════════════════
#
# Originally built ADP-only: ADP is a bad fit for Common Crawl, since most
# customers embed their board via a JS web component
# (<recruitment-current-openings cid=... ccid=...>) rather than a plain
# <a href>, so a link-following crawler like CC never sees a URL to
# follow. The Wayback Machine's CDX index is a different, broader,
# independently-sourced index (it also ingests URLs via Google Sitemaps,
# third-party "Save Page Now" submissions, etc.), so it can have
# snapshots of a platform's real board pages even when Common Crawl has
# none.
#
# GENERALIZED 2026-09: there's no reason that benefit is ADP-specific —
# this now runs the exact SAME real, already-vetted URL patterns Common
# Crawl discovery uses (CC_PLATFORM_PATTERNS) through Wayback's CDX index
# for every platform that has one, extracting slugs with the exact same
# extractors (CC_EXTRACTORS) Common Crawl already uses. No new patterns
# were guessed for this — it's the identical query list, just pointed at
# a second, independent index. This naturally still excludes whatever
# CC_PLATFORM_PATTERNS itself excludes (occupop/successfactors have no
# entry there, each for its own documented reason — successfactors because
# a CSB tenant's branded domain has no shared, CC/Wayback-indexable URL
# shape, NOT because it's unscrapeable (see SUPPORTED_ATS's comment for
# the 2026-09 reversal); homerun was removed entirely 2026-09, see
# SUPPORTED_ATS's removal comment), so this doesn't need its own separate
# exclusion list.
#
# The CDX API (web.archive.org/cdx/search/cdx) is IA's own documented,
# public, purpose-built endpoint for exactly this kind of targeted
# URL-pattern lookup — not a scrape of a page meant for browsers — but
# per this project's non-negotiable robots.txt policy we still check
# web.archive.org/robots.txt live before every run rather than assume.

WAYBACK_CDX_URL = "http://web.archive.org/cdx/search/cdx"
# ADP's own patterns, still called out by name here since the legacy
# family below needs its own explanation — CC_PLATFORM_PATTERNS["adp"]
# holds these same two entries verbatim, this isn't a second definition
# to keep in sync, just documenting WHY they look the way they do:
#   "workforcenow.adp.com/mascsr/*"                    — modern cid/ccId family
#   "workforcenow.adp.com/jobs/apply/posting.html*"    — legacy (deprecated
# 2026-06-26) family — no longer serves job content, but Wayback may still
# have snapshots from before the sunset, and its redirect chain resolves
# client= to a real modern cid — see _url_to_slug_adp_discovery.

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


_WAYBACK_MAX_PAGES = 500  # safety cap on resumeKey pagination, see below
# 2026-09: raised from 25 — 25 pages x 5000/page = 125,000 snapshots, which
# turned out to be exactly enough to get hit (and the cap logged as hit) by
# every single Greenhouse pattern (boards.greenhouse.io/*, boards.eu.
# greenhouse.io/*, job-boards.greenhouse.io/*, job-boards.eu.greenhouse.io/*)
# in the same run — Greenhouse alone has been on the Wayback Machine for
# years and is one of the most-archived ATS platforms there is, so 125K
# snapshots is a real, recurring ceiling, not a one-off. The cap's actual
# JOB is to stop a genuinely pathological loop (CDX repeatedly handing back
# a resumeKey with no forward progress, a network wedge, etc.) — it was
# never meant to model "how many snapshots a busy pattern could have."
# 500 pages x 5000/page = 2.5M snapshots per pattern, ~20x more headroom
# than the largest count actually observed so far, while still being a
# hard, finite stop against a genuine runaway. Paired with the new
# --wayback-shard/--wayback-total-shards platform-sharding below (mirrors
# Common Crawl's cc_shard/cc_total_shards) so raising this doesn't turn
# into one giant sequential job — each of the 5 wayback shards below only
# covers ~1/5 of the platforms, so the extra pages this cap now allows are
# spent in parallel, not stacked onto one run's wall-clock.


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


def fetch_wayback_slugs(limit: int = 5000, platforms: list[str] | None = None,
                         wb_shard: int | None = None,
                         wb_total_shards: int = 1) -> dict[str, set[str]]:
    """Query the Wayback Machine CDX index for archived career-page URLs
    across every ATS platform that has a CC_PLATFORM_PATTERNS entry, and
    extract slugs with the matching CC_EXTRACTORS parser — the exact same
    patterns/extractors Common Crawl discovery uses, just against a
    second, independent index (see the module header comment above for
    why that's worth doing at all, not just for ADP).

    `limit` is the PER-PAGE size for CDX's resumeKey pagination, not a
    hard overall cap — see _fetch_wayback_cdx_urls for why the old
    flat-limit version was silently truncating on high-volume patterns.
    `platforms` restricts which ATS keys to query (default: every key in
    CC_PLATFORM_PATTERNS).

    wb_shard/wb_total_shards (2026-09) split the PLATFORMS across
    `wb_total_shards` independent runs, same platform-sharding scheme as
    fetch_commoncrawl_slugs' cc_shard/cc_total_shards — added alongside
    the _WAYBACK_MAX_PAGES raise above so a heavily-archived platform
    (Greenhouse) being allowed many more resumeKey pages doesn't turn one
    unsharded Wayback run into a single long sequential job. discovery.yml
    runs this as 5 shards. Pass wb_shard=None (default) to run every
    platform in one call, same as before this existed. If `platforms` is
    ALSO given explicitly, sharding is applied on top of that narrowed
    list, not on the full CC_PLATFORM_PATTERNS set."""
    slugs_by_ats: dict[str, set[str]] = {}

    if not _robots_allows("https://web.archive.org", "/cdx/"):
        log.warning("Wayback CDX: /cdx/ disallowed by web.archive.org/robots.txt "
                     "(or robots.txt unreachable) — skipping Wayback discovery entirely.")
        return slugs_by_ats

    target_platforms = platforms if platforms is not None else list(CC_PLATFORM_PATTERNS.keys())
    if wb_shard is not None and wb_total_shards > 1:
        target_platforms = [p for i, p in enumerate(target_platforms)
                             if i % wb_total_shards == wb_shard]
        log.info(f"Wayback CDX: shard {wb_shard}/{wb_total_shards} — "
                 f"{len(target_platforms)} platform(s) assigned to this shard")

    for ats in target_platforms:
        patterns = CC_PLATFORM_PATTERNS.get(ats)
        extractor = CC_EXTRACTORS.get(ats)
        if not patterns or not extractor:
            continue

        slugs: set[str] = set()
        for pattern in patterns:
            log.info(f"Wayback CDX: querying archived snapshots of {pattern}")
            urls = _fetch_wayback_cdx_urls(pattern, limit)
            log.info(f"  Wayback CDX: {len(urls)} archived snapshot URLs")

            for url in urls:
                try:
                    slug = extractor(url)
                except Exception:
                    continue
                if slug:
                    slugs.add(slug)

        if slugs:
            log.info(f"  {ats}: {len(slugs)} companies from Wayback Machine")
            slugs_by_ats[ats] = slugs

    return _drop_dead_cc_slugs(slugs_by_ats, "Wayback")


# ══════════════════════════════════════════════════════════
# SOURCE 5b: Certificate Transparency logs (crt.sh)
# ══════════════════════════════════════════════════════════
#
# 2026-09, added at the user's request. CT logs (every publicly-trusted CA
# is required to log every cert it issues, since ~2018) let you enumerate
# every hostname that has EVER had a certificate issued for it under a
# given domain SUFFIX — crt.sh indexes this and exposes it as a free public
# SQL-backed lookup (`?q=%.suffix&output=json`). This is a genuinely
# different discovery mechanism from Common Crawl/Wayback above: those find
# a tenant only if some crawled page happened to link to it; CT logs find
# EVERY tenant that ever requested HTTPS for their subdomain, whether or
# not any page on the public web links to it yet, with none of Common
# Crawl/Wayback's crawl-frequency lag.
#
# Critically, this ONLY works for platforms where the tenant identity
# lives in the SUBDOMAIN itself (each customer gets their own hostname, so
# each one shows up as its own cert). It does NOT help for:
#   - SuccessFactors Career Site Builder: every tenant runs on its own
#     fully custom BRANDED domain (careers.company.com) — there's no
#     shared suffix to query in the first place. This is exactly why
#     SuccessFactors has no URL_TO_SLUG/CC_PLATFORM_PATTERNS entry at all
#     (see the comment above _url_to_slug_breezyhr) and is instead found
#     via node.py's content-fingerprint check.
#   - Platforms that put the tenant in the PATH on a shared host, not the
#     subdomain — Greenhouse, Lever, Ashby, SmartRecruiters, Jobvite,
#     JobAdder, ADP, BrassRing, Jobylon, PageUp, Paylocity, Join.com — a
#     CT query here would just return the platform's own one or two fixed
#     hosts, over and over, with zero new information.
#   - Workday, Taleo, and Oracle Cloud HCM: these DO put the tenant in the
#     subdomain, so CT logs would confirm a tenant exists — but the
#     scrape-ready slug also needs a site_id/section/org value that only
#     lives in the URL PATH or a query param, not the hostname, so a CT
#     hit alone isn't enough to build a usable slug for these three (a bare
#     tenant hostname resolves to None in _url_to_slug_workday/_taleo/
#     _oracle_cloud). Left out of CT_LOG_SUFFIXES below for that reason —
#     revisit if a reliable default site_id/org pattern is ever confirmed.
#
# For every platform below, this reuses the EXACT SAME real, already-
# hardened URL_TO_SLUG extractor each other source uses — a discovered
# hostname is turned into a synthetic "https://{host}/" URL and run
# through that platform's own extractor, so every existing validation
# guard (SKIP_SLUGS, _looks_like_real_slug, reserved-subdomain lists, the
# softgarden/csod fallback logic, etc.) applies unchanged. No new parsing
# logic was written — this is a new URL SOURCE, not a new URL parser.
CT_LOG_SUFFIXES: dict[str, list[str]] = {
    "bamboohr": [".bamboohr.com"],
    "icims": [".icims.com"],
    "rippling": [".rippling.com"],
    "workable": [".workable.com"],
    "recruitee": [".recruitee.com"],
    "teamtailor": [".teamtailor.com"],
    "breezyhr": [".breezy.hr"],
    "hrmdirect": [".hrmdirect.com", ".clearcompany.com"],
    "softgarden": [".softgarden.io", ".career.softgarden.de", ".softgarden.de"],
    "zoho": [".zohorecruit.com", ".zohorecruit.eu", ".zohorecruit.com.au"],
    "eploy": [".eploy.net"],
    "pinpoint": [".pinpointhq.com"],
    "isolvedhire": [".isolvedhire.com"],
    "flatchr": [".flatchr.io"],
    "getro": [".getro.com"],
    "jazzhr": [".applytojob.com"],
    "csod": [".csod.com"],
    "avature": [".avature.net"],
}

CRTSH_URL = "https://crt.sh/"
# crt.sh has no documented per-IP rate limit, but it's a small, free,
# community-run service backed by a single Postgres instance — a short
# sleep between queries is just good citizenship, same spirit as the
# 0.3s sleep _drop_dead_cc_slugs' predecessor used per-slug.
_CRTSH_QUERY_SLEEP_SECONDS = 1.0
_CRTSH_MAX_RETRIES = 3


def _fetch_crtsh_hostnames(suffix: str) -> set[str]:
    """All distinct hostnames crt.sh has ever seen a certificate issued for
    under `suffix` (e.g. ".bamboohr.com"). `output=json` returns one row
    per matching CERTIFICATE, not per hostname — a single cert's
    `name_value` field can itself contain multiple SANs newline-separated
    (e.g. a wildcard cert or a multi-domain cert), so every row's
    name_value is split on newlines and each line is checked against the
    suffix independently, rather than assuming one hostname per row.

    crt.sh is a free, best-effort community service (a single Postgres
    instance behind a web UI, not a paid API) — it can be slow or briefly
    503 under load, so this retries a couple of times with backoff before
    giving up on this one suffix (not the whole source)."""
    last_error = None
    for attempt in range(1, _CRTSH_MAX_RETRIES + 1):
        try:
            r = requests.get(
                CRTSH_URL,
                params={"q": f"%{suffix}", "output": "json"},
                timeout=90,
                headers={"User-Agent": _ROBOTS_UA},
            )
            r.raise_for_status()
            rows = r.json()
            break
        except Exception as e:
            last_error = e
            if attempt < _CRTSH_MAX_RETRIES:
                time.sleep(5 * attempt)
            continue
    else:
        log.warning(f"crt.sh: query failed for {suffix} after {_CRTSH_MAX_RETRIES} "
                    f"attempts: {last_error}")
        return set()

    hostnames: set[str] = set()
    if not isinstance(rows, list):
        return hostnames
    for row in rows:
        name_value = (row or {}).get("name_value", "")
        for line in name_value.splitlines():
            host = line.strip().lower().lstrip("*.")
            if host.endswith(suffix.lstrip(".")) or (suffix.startswith(".") and host.endswith(suffix)):
                hostnames.add(host)
    return hostnames


def fetch_ct_log_slugs(platforms: list[str] | None = None) -> dict[str, set[str]]:
    """Query crt.sh's Certificate Transparency index for every platform in
    CT_LOG_SUFFIXES (or the subset named in `platforms`), turn each
    discovered hostname into a slug via that platform's own URL_TO_SLUG
    extractor, and live-drop dead ones exactly like Common Crawl/Wayback —
    see the module comment above CT_LOG_SUFFIXES for which platforms this
    can and can't help, and why.

    Unlike Wayback/Common Crawl, this needs no CDX-style pagination — a
    single crt.sh query returns crt.sh's FULL known history for that
    suffix in one response (crt.sh itself doesn't page this endpoint)."""
    slugs_by_ats: dict[str, set[str]] = {}

    if not _robots_allows("https://crt.sh", "/"):
        log.warning("crt.sh: disallowed by crt.sh/robots.txt (or robots.txt "
                    "unreachable) — skipping CT log discovery entirely.")
        return slugs_by_ats

    target_platforms = platforms if platforms is not None else list(CT_LOG_SUFFIXES.keys())

    for ats in target_platforms:
        suffixes = CT_LOG_SUFFIXES.get(ats)
        extractor = URL_TO_SLUG.get(ats)
        if not suffixes or not extractor:
            continue

        hostnames: set[str] = set()
        for suffix in suffixes:
            log.info(f"crt.sh: querying %{suffix}")
            found = _fetch_crtsh_hostnames(suffix)
            log.info(f"  crt.sh: {len(found)} distinct hostnames")
            hostnames.update(found)
            time.sleep(_CRTSH_QUERY_SLEEP_SECONDS)

        slugs: set[str] = set()
        for host in hostnames:
            try:
                slug = extractor(f"https://{host}/")
            except Exception:
                continue
            if slug:
                slugs.add(slug)

        if slugs:
            log.info(f"  {ats}: {len(slugs)} companies from CT logs")
            slugs_by_ats[ats] = slugs

    return _drop_dead_cc_slugs(slugs_by_ats, "CT logs")


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


def fetch_latmay_slugs(hf_shard: int | None = None, hf_total_shards: int | None = None) -> dict[str, dict[str, str]]:
    """latmay/ats-career-page-urls — 69,638 rows of {canonical_url,
    ats_platform}. Small enough to load in one pass, no time-budget/
    streaming logic needed (contrast fetch_edwarddgao_slugs below).

    2026-09: `hf_shard`/`hf_total_shards` are supported (row-index
    slicing, after the one download — unlike Edward's per-file sharding
    below, since only ONE Parquet file backs this whole dataset) but
    Discovery.yml runs this as a SINGLE unsharded job — one ~69k-row
    file is a small, cheap download either way, so splitting it would
    only spread the per-row URL_TO_SLUG work across more jobs without
    cutting any actual download volume, not worth another matrix slot
    for. The params stay here for manual/ad-hoc use (--hf-shard /
    --hf-total-shards on the CLI) rather than being removed outright."""
    import pyarrow.parquet as pq

    file_urls = _hf_parquet_urls("latmay/ats-career-page-urls")
    if not file_urls:
        log.warning("Latmay H.F: no Parquet files resolved, skipping source")
        return {}

    all_urls: list[str] = []
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
        all_urls.extend(table.column("canonical_url").to_pylist())

    shard_note = ""
    # hf_total_shards defaults to 1 ("no sharding") on the CLI, while
    # hf_shard defaults to None — so gating on hf_total_shards alone
    # crashed here (None * shard_size) whenever --source latmay was run
    # standalone without an explicit --hf-shard. Sharding only actually
    # applies when a shard index was given.
    if hf_total_shards and hf_shard is not None:
        shard_size = -(-len(all_urls) // hf_total_shards)  # ceil division
        start_i = hf_shard * shard_size
        end_i = min(start_i + shard_size, len(all_urls))
        all_urls = all_urls[start_i:end_i]
        shard_note = f" [shard {hf_shard}/{hf_total_shards}]"

    slugs_by_ats: dict[str, dict[str, str]] = {}
    processed = 0
    start = time.monotonic()
    _PROGRESS_EVERY = 5_000

    for url in all_urls:
        processed += 1
        hit = _resolve_url_via_url_to_slug(url)
        if hit:
            actual_ats, slug = hit
            slugs_by_ats.setdefault(actual_ats, {})[slug] = ""

        if processed % _PROGRESS_EVERY == 0:
            elapsed = max(time.monotonic() - start, 0.001)
            resolved = sum(len(s) for s in slugs_by_ats.values())
            log.info(f"Latmay H.F{shard_note}: {processed:,}/{len(all_urls):,} processed "
                     f"({processed / elapsed:,.1f}/sec), {resolved:,} "
                     f"resolved ({resolved / processed * 100:.1f}%)")

    total = sum(len(s) for s in slugs_by_ats.values())
    for ats, slugs in slugs_by_ats.items():
        log.info(f"  {ats}: {len(slugs)} slugs from Latmay H.F{shard_note}")
    log.info(f"Latmay H.F{shard_note} summary: {processed:,} rows processed, {total:,} "
             f"slugs resolved ({total / max(processed, 1) * 100:.1f}%)")
    return slugs_by_ats


def fetch_edwarddgao_slugs(time_budget_minutes: int = 270, hf_shard: int | None = None,
                            hf_total_shards: int | None = None) -> dict[str, dict[str, str]]:
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
    incomplete-shard-coverage run still converges over repeat runs.

    2026-09: `hf_shard`/`hf_total_shards` split 3-way in discovery.yml,
    same as commoncrawl/httparchive — unlike Latmay's single-file
    row-index split, this dataset already comes as 375 separate Parquet
    FILES, so each shard just takes its own contiguous ~1/N slice of
    the FILE list before downloading anything — cutting download volume
    per shard too, not just per-row work."""
    import pyarrow.parquet as pq

    file_urls = _hf_parquet_urls("edwarddgao/open-apply-jobs")
    if not file_urls:
        log.warning("Edward H.F: no Parquet files resolved, skipping source")
        return {}

    shard_note = ""
    # Same guard fix as fetch_latmay_slugs above: hf_total_shards defaults
    # to 1 ("no sharding") while hf_shard defaults to None, so gating on
    # hf_total_shards alone crashed (None * shard_size) whenever
    # --source edwarddgao was run standalone without --hf-shard.
    if hf_total_shards and hf_shard is not None:
        shard_size = -(-len(file_urls) // hf_total_shards)  # ceil division
        start_i = hf_shard * shard_size
        end_i = min(start_i + shard_size, len(file_urls))
        file_urls = file_urls[start_i:end_i]
        shard_note = f" [shard {hf_shard}/{hf_total_shards}]"

    log.info(f"Edward H.F{shard_note}: {len(file_urls)} Parquet shards to process "
             f"(time budget: {time_budget_minutes}min, 0 = no budget)")

    slugs_by_ats: dict[str, dict[str, str]] = {}
    processed = 0
    start = time.monotonic()
    _PROGRESS_EVERY = 5_000
    budget_seconds = time_budget_minutes * 60 if time_budget_minutes else None

    for file_i, file_url in enumerate(file_urls):
        if budget_seconds and (time.monotonic() - start) >= budget_seconds:
            log.info(f"Edward H.F{shard_note}: time budget reached after {file_i}/"
                     f"{len(file_urls)} files — stopping gracefully, "
                     f"keeping {processed:,} rows' worth of progress.")
            break

        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                download_timed_out = False
                with requests.get(file_url, timeout=300, stream=True) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        tmp.write(chunk)
                        # 2026-09: the between-files check above only fires
                        # once a whole file is done — a single slow/large
                        # Parquet download (network-bound, no fixed size
                        # cap) could otherwise run well past budget_seconds
                        # unnoticed and get hard-killed by GitHub's own job
                        # timeout with a bare KeyboardInterrupt, losing
                        # nothing extra (this partial file was never
                        # counted anyway) but skipping the graceful-stop
                        # log line and, more importantly, leaving zero
                        # margin for the Supabase upsert that runs AFTER
                        # this function returns (see --edwarddgao-time-
                        # budget-minutes' default vs. this source's job
                        # timeout). Checking here — cheap, one
                        # time.monotonic() call per 1MB chunk — lets a
                        # stuck-mid-download file abandon itself instead.
                        if budget_seconds and (time.monotonic() - start) >= budget_seconds:
                            download_timed_out = True
                            break
                if download_timed_out:
                    log.info(f"Edward H.F{shard_note}: time budget reached mid-download "
                              f"of file {file_i + 1}/{len(file_urls)} — stopping "
                              f"gracefully, keeping {processed:,} rows' worth of "
                              f"progress (this in-flight file's partial download is "
                              f"discarded, not counted).")
                    break
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
                            log.info(f"Edward H.F{shard_note}: file {file_i + 1}/{len(file_urls)}, "
                                     f"{processed:,} processed ({processed / elapsed:,.1f}/sec), "
                                     f"{resolved:,} resolved ({resolved / processed * 100:.1f}%)")
        except Exception as e:
            log.warning(f"Edward H.F{shard_note}: file {file_i + 1}/{len(file_urls)} "
                        f"({file_url}) failed, skipping: {e}")
            continue

    total = sum(len(s) for s in slugs_by_ats.values())
    for ats, slugs in slugs_by_ats.items():
        log.info(f"  {ats}: {len(slugs)} slugs from Edward H.F{shard_note}")
    log.info(f"Edward H.F{shard_note} summary: {processed:,} rows processed, {total:,} "
             f"slugs resolved ({total / max(processed, 1) * 100:.1f}%)")
    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 12 & 13: two more Hugging Face bulk datasets, 2026-09
# (Yigit-Karaman/open-jobs-daily + zalizedata/tech-job-postings-salary-dataset)
# ══════════════════════════════════════════════════════════
# Source 13 (zalizedata) REMOVED 2026-09 at the user's request — see
# main()'s Source 13 comment. fetch_zalizedata_slugs() below is left
# defined/unused; this block comment (including its zalizedata research
# trail) is kept as-is for reference rather than deleted.
# Same "offline pass through URL_TO_SLUG" shape as Latmay/Edward above —
# both datasets hand over a real per-job `url` column directly, so there's
# no live crawl step, just _resolve_url_via_url_to_slug reused as-is.
# Each of these datasets ALSO carries its own pre-labeled `ats`/slug-style
# columns (open-jobs-daily: "ats"+"slug"; zalizedata: "ats"+"ats_token"),
# but those are deliberately NOT trusted directly — same reasoning as
# Latmay's own docstring above: a label's exact wording isn't guaranteed
# to match this file's own ATS key naming, and re-deriving the slug via
# URL_TO_SLUG is the one path already confirmed correct against every
# platform this project actually supports. A row whose `ats` label names
# a platform we don't support at all (confirmed live: open-jobs-daily has
# real "gohire" rows, e.g. jobs.gohire.io — not in URL_TO_SLUG) is simply
# not resolved, exactly like any other unsupported-platform row from any
# other bulk source — a genuine future candidate, not silently trusted in.
#
#   Yigit-Karaman/open-jobs-daily (huggingface.co/datasets/
#     Yigit-Karaman/open-jobs-daily — CC0-1.0, public domain). Two HF
#     configs, confirmed live via the datasets-server API: "default"
#     (12 Parquet files, ~3.08M rows) and "ledger" (3 files, ~6.69M rows,
#     adds first_seen_at/last_seen_at/removed_at/is_open — a change-
#     tracking history of the same underlying postings, not a disjoint
#     dataset). Both configs are pulled — anything the "default" snapshot
#     missed that "ledger" still has (or vice versa) is pure upside, and
#     slugs naturally dedupe via the shared dict. ~24.8GB total across
#     both configs (confirmed via the dataset's own listed file size) —
#     real bulk volume, same as Edward H.F, so this reuses that exact
#     streaming-download + time-budget + file-sharding shape rather than
#     Latmay's simpler single-shot pattern.
#   zalizedata/tech-job-postings-salary-dataset (huggingface.co/datasets/
#     zalizedata/tech-job-postings-salary-dataset — CC-BY-NC-4.0,
#     NON-COMMERCIAL. Confirmed with the user 2026-09 that this project's
#     current use is non-commercial; if that ever changes, this source
#     must be removed — see GREYLIST_ATS.md-style reasoning, documented
#     here since there's no per-ATS-platform doc for a bulk slug SOURCE).
#     Three HF configs (L/M/S), confirmed live via the datasets-server
#     API: L is the largest at ~394k rows (M ~19.3k, S ~35.4k) — the
#     dataset card doesn't document the exact L/M/S relationship, but
#     since L already has the most rows, only L is fetched here rather
#     than guessing M/S are disjoint from it and risking real duplicate
#     download/processing work for no new coverage. Single Parquet file,
#     small enough for Latmay's simpler single-shot pattern — no
#     streaming/time-budget/file-sharding needed.

def fetch_openjobsdaily_slugs(time_budget_minutes: int = 270, hf_shard: int | None = None,
                                hf_total_shards: int | None = None) -> dict[str, dict[str, str]]:
    """Yigit-Karaman/open-jobs-daily — ~9.8M rows across 15 Parquet files
    (12 "default" + 3 "ledger", ~24.8GB total). Only `url` is ever read —
    every other column (title, location, timestamps, etc.) is projected
    away at the Parquet read itself. See the module-level block comment
    above this function for the CC0 license, why both configs are pulled,
    and why the dataset's own pre-labeled `ats`/`slug` columns are NOT
    trusted directly.

    Same streaming-download + graceful time-budget + file-level sharding
    shape as fetch_edwarddgao_slugs — copied deliberately rather than
    factored into a shared helper, matching this file's existing pattern
    of one dedicated function per HF source (never a shared seed/probe
    pipeline, per the user's standing instruction for these)."""
    import pyarrow.parquet as pq

    file_urls = _hf_parquet_urls("Yigit-Karaman/open-jobs-daily")
    if not file_urls:
        log.warning("Open Jobs Daily H.F: no Parquet files resolved, skipping source")
        return {}

    shard_note = ""
    if hf_total_shards and hf_shard is not None:
        shard_size = -(-len(file_urls) // hf_total_shards)  # ceil division
        start_i = hf_shard * shard_size
        end_i = min(start_i + shard_size, len(file_urls))
        file_urls = file_urls[start_i:end_i]
        shard_note = f" [shard {hf_shard}/{hf_total_shards}]"

    log.info(f"Open Jobs Daily H.F{shard_note}: {len(file_urls)} Parquet files to process "
             f"(time budget: {time_budget_minutes}min, 0 = no budget)")

    slugs_by_ats: dict[str, dict[str, str]] = {}
    processed = 0
    start = time.monotonic()
    _PROGRESS_EVERY = 5_000
    budget_seconds = time_budget_minutes * 60 if time_budget_minutes else None

    for file_i, file_url in enumerate(file_urls):
        if budget_seconds and (time.monotonic() - start) >= budget_seconds:
            log.info(f"Open Jobs Daily H.F{shard_note}: time budget reached after "
                     f"{file_i}/{len(file_urls)} files — stopping gracefully, "
                     f"keeping {processed:,} rows' worth of progress.")
            break

        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                download_timed_out = False
                with requests.get(file_url, timeout=300, stream=True) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        tmp.write(chunk)
                        if budget_seconds and (time.monotonic() - start) >= budget_seconds:
                            download_timed_out = True
                            break
                if download_timed_out:
                    log.info(f"Open Jobs Daily H.F{shard_note}: time budget reached "
                             f"mid-download of file {file_i + 1}/{len(file_urls)} — "
                             f"stopping gracefully, keeping {processed:,} rows' worth "
                             f"of progress (this in-flight file's partial download is "
                             f"discarded, not counted).")
                    break
                tmp.flush()

                pf = pq.ParquetFile(tmp.name)
                for batch in pf.iter_batches(columns=["url"], batch_size=50_000):
                    for url in batch.column("url").to_pylist():
                        processed += 1
                        hit = _resolve_url_via_url_to_slug(url)
                        if hit:
                            actual_ats, slug = hit
                            slugs_by_ats.setdefault(actual_ats, {})[slug] = ""

                        if processed % _PROGRESS_EVERY == 0:
                            elapsed = max(time.monotonic() - start, 0.001)
                            resolved = sum(len(s) for s in slugs_by_ats.values())
                            log.info(f"Open Jobs Daily H.F{shard_note}: file "
                                     f"{file_i + 1}/{len(file_urls)}, {processed:,} "
                                     f"processed ({processed / elapsed:,.1f}/sec), "
                                     f"{resolved:,} resolved ({resolved / processed * 100:.1f}%)")
        except Exception as e:
            log.warning(f"Open Jobs Daily H.F{shard_note}: file {file_i + 1}/"
                        f"{len(file_urls)} ({file_url}) failed, skipping: {e}")
            continue

    total = sum(len(s) for s in slugs_by_ats.values())
    for ats, slugs in slugs_by_ats.items():
        log.info(f"  {ats}: {len(slugs)} slugs from Open Jobs Daily H.F{shard_note}")
    log.info(f"Open Jobs Daily H.F{shard_note} summary: {processed:,} rows processed, "
             f"{total:,} slugs resolved ({total / max(processed, 1) * 100:.1f}%)")
    return slugs_by_ats


def fetch_zalizedata_slugs() -> dict[str, dict[str, str]]:
    """zalizedata/tech-job-postings-salary-dataset — "L" config only
    (~394k rows, one Parquet file). Only `url` is ever read. See the
    module-level block comment above fetch_openjobsdaily_slugs for the
    CC-BY-NC-4.0 non-commercial license (confirmed acceptable for this
    project's current, non-commercial use — remove this source if that
    ever changes) and why only the "L" config is fetched.

    Small enough for Latmay's simpler single-shot pattern — no
    streaming/time-budget/sharding needed."""
    import pyarrow.parquet as pq

    r = requests.get("https://huggingface.co/api/datasets/"
                      "zalizedata/tech-job-postings-salary-dataset/parquet",
                      timeout=30)
    try:
        r.raise_for_status()
        file_urls = r.json().get("L", {}).get("train", [])
    except Exception as e:
        log.warning(f"Zalize H.F: Parquet resolve failed: {e}")
        return {}
    if not file_urls:
        log.warning("Zalize H.F: no Parquet files resolved for the 'L' config, skipping source")
        return {}

    all_urls: list[str] = []
    for file_url in file_urls:
        try:
            resp = requests.get(file_url, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            log.warning(f"Zalize H.F: failed to download {file_url}: {e}")
            continue

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            tmp.write(resp.content)
            tmp.flush()
            table = pq.read_table(tmp.name, columns=["url"])
        all_urls.extend(table.column("url").to_pylist())

    slugs_by_ats: dict[str, dict[str, str]] = {}
    processed = 0
    start = time.monotonic()
    _PROGRESS_EVERY = 5_000

    for url in all_urls:
        processed += 1
        hit = _resolve_url_via_url_to_slug(url)
        if hit:
            actual_ats, slug = hit
            slugs_by_ats.setdefault(actual_ats, {})[slug] = ""

        if processed % _PROGRESS_EVERY == 0:
            elapsed = max(time.monotonic() - start, 0.001)
            resolved = sum(len(s) for s in slugs_by_ats.values())
            log.info(f"Zalize H.F: {processed:,}/{len(all_urls):,} processed "
                     f"({processed / elapsed:,.1f}/sec), {resolved:,} "
                     f"resolved ({resolved / processed * 100:.1f}%)")

    total = sum(len(s) for s in slugs_by_ats.values())
    for ats, slugs in slugs_by_ats.items():
        log.info(f"  {ats}: {len(slugs)} slugs from Zalize H.F")
    log.info(f"Zalize H.F summary: {processed:,} rows processed, {total:,} "
             f"slugs resolved ({total / max(processed, 1) * 100:.1f}%)")
    return slugs_by_ats


# ══════════════════════════════════════════════════════════
# SOURCE 14: Aramente H.F (huggingface.co/datasets/Aramente/eu-tech-jobs),
# 2026-09
# ══════════════════════════════════════════════════════════
# REMOVED 2026-09 at the user's request — see main()'s Source 14 comment.
# fetch_eutechjobs_slugs() below is left defined/unused; this block
# comment (including its schema-discrepancy research trail) is kept
# as-is for reference rather than deleted.
#
# License: cc-by-4.0 (attribution only, NOT non-commercial — confirmed live
# from the dataset repo's raw README.md YAML frontmatter, e.g.
# huggingface.co/datasets/Aramente/eu-tech-jobs/raw/main/README.md).
#
# IMPORTANT — the dataset's own rendered card/README prose (as opposed to
# its YAML frontmatter) describes a schema that does NOT match the real
# data: it talks about job titles, salaries, `description_md`, ISO country
# codes, etc., as if this were shaped like Open Jobs Daily/Zalizedata
# above. Two independent, direct queries against the datasets-server JSON
# APIs (`/first-rows` and `/size` — not the HTML-rendered page, which is
# what produced the mismatched description) instead confirm a real,
# self-consistent 15-column schema (num_columns=15 from `/size` matches
# exactly): slug, name, country, categories, industry_tags, ats_provider,
# ats_handle, career_url, github_org, funding_stage, size_bucket, notes,
# oss_signal, top_repo_stars, primary_language. 3,180,028 rows across 280
# Parquet files (~2.8GB total), confirmed via the same `/size` endpoint and
# the HF Parquet-export API's file listing. Sample rows show one row per
# job posting (`career_url` is a specific posting URL, e.g. a RemoteOK
# listing or an ATS-hosted board page), with `ats_provider`/`ats_handle`
# populated only when the source is a real ATS — null when it's an
# aggregator like RemoteOK. As with every other bulk H.F source here, the
# dataset's own `slug`/`ats_provider`/`ats_handle` labels are NOT trusted
# directly — only `career_url` is read, resolved through URL_TO_SLUG.

def fetch_eutechjobs_slugs(time_budget_minutes: int = 270, hf_shard: int | None = None,
                             hf_total_shards: int | None = None) -> dict[str, dict[str, str]]:
    """Aramente/eu-tech-jobs — 3,180,028 rows across 280 Parquet files
    (~2.8GB total). Only `career_url` is ever read — every other column
    (name, country, categories, ats_provider, github_org, etc.) is
    projected away at the Parquet read itself. See the module-level block
    comment above this function for the cc-by-4.0 license, the real
    (datasets-server-confirmed) 15-column schema — which does NOT match
    the dataset's own rendered card/README prose — and why the dataset's
    own ats_provider/ats_handle/slug columns are NOT trusted directly.

    Same streaming-download + graceful time-budget + file-level sharding
    shape as fetch_edwarddgao_slugs/fetch_openjobsdaily_slugs — copied
    deliberately rather than factored into a shared helper, matching this
    file's existing pattern of one dedicated function per HF source."""
    import pyarrow.parquet as pq

    file_urls = _hf_parquet_urls("Aramente/eu-tech-jobs")
    if not file_urls:
        log.warning("Aramente H.F: no Parquet files resolved, skipping source")
        return {}

    shard_note = ""
    if hf_total_shards and hf_shard is not None:
        shard_size = -(-len(file_urls) // hf_total_shards)  # ceil division
        start_i = hf_shard * shard_size
        end_i = min(start_i + shard_size, len(file_urls))
        file_urls = file_urls[start_i:end_i]
        shard_note = f" [shard {hf_shard}/{hf_total_shards}]"

    log.info(f"Aramente H.F{shard_note}: {len(file_urls)} Parquet files to process "
             f"(time budget: {time_budget_minutes}min, 0 = no budget)")

    slugs_by_ats: dict[str, dict[str, str]] = {}
    processed = 0
    start = time.monotonic()
    _PROGRESS_EVERY = 5_000
    budget_seconds = time_budget_minutes * 60 if time_budget_minutes else None

    for file_i, file_url in enumerate(file_urls):
        if budget_seconds and (time.monotonic() - start) >= budget_seconds:
            log.info(f"Aramente H.F{shard_note}: time budget reached after "
                     f"{file_i}/{len(file_urls)} files — stopping gracefully, "
                     f"keeping {processed:,} rows' worth of progress.")
            break

        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                download_timed_out = False
                with requests.get(file_url, timeout=300, stream=True) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        tmp.write(chunk)
                        if budget_seconds and (time.monotonic() - start) >= budget_seconds:
                            download_timed_out = True
                            break
                if download_timed_out:
                    log.info(f"Aramente H.F{shard_note}: time budget reached "
                             f"mid-download of file {file_i + 1}/{len(file_urls)} — "
                             f"stopping gracefully, keeping {processed:,} rows' worth "
                             f"of progress (this in-flight file's partial download is "
                             f"discarded, not counted).")
                    break
                tmp.flush()

                pf = pq.ParquetFile(tmp.name)
                # 2026-09: this dataset's 280 Parquet files do NOT all share
                # one schema — confirmed live (some files genuinely lack a
                # `career_url` field, raising pyarrow's own "Field ... does
                # not exist in schema" on column-projected reads). Rather
                # than let that surface as an opaque per-file exception
                # (caught below, but with no clue what that file's real
                # columns are), check the schema up front and log the
                # actual column names so a real, evidence-based fallback
                # column name can be added later if one of these turns out
                # to hold the same data under a different name — nothing
                # is guessed here, this just makes the next occurrence
                # diagnostic instead of opaque.
                if "career_url" not in pf.schema.names:
                    log.warning(f"Aramente H.F{shard_note}: file {file_i + 1}/"
                                f"{len(file_urls)} has no 'career_url' column "
                                f"— skipping this file. Its actual columns: "
                                f"{pf.schema.names}")
                    continue
                for batch in pf.iter_batches(columns=["career_url"], batch_size=50_000):
                    for url in batch.column("career_url").to_pylist():
                        processed += 1
                        hit = _resolve_url_via_url_to_slug(url)
                        if hit:
                            actual_ats, slug = hit
                            slugs_by_ats.setdefault(actual_ats, {})[slug] = ""

                        if processed % _PROGRESS_EVERY == 0:
                            elapsed = max(time.monotonic() - start, 0.001)
                            resolved = sum(len(s) for s in slugs_by_ats.values())
                            log.info(f"Aramente H.F{shard_note}: file "
                                     f"{file_i + 1}/{len(file_urls)}, {processed:,} "
                                     f"processed ({processed / elapsed:,.1f}/sec), "
                                     f"{resolved:,} resolved ({resolved / processed * 100:.1f}%)")
        except Exception as e:
            log.warning(f"Aramente H.F{shard_note}: file {file_i + 1}/"
                        f"{len(file_urls)} ({file_url}) failed, skipping: {e}")
            continue

    total = sum(len(s) for s in slugs_by_ats.values())
    for ats, slugs in slugs_by_ats.items():
        log.info(f"  {ats}: {len(slugs)} slugs from Aramente H.F{shard_note}")
    log.info(f"Aramente H.F{shard_note} summary: {processed:,} rows processed, "
             f"{total:,} slugs resolved ({total / max(processed, 1) * 100:.1f}%)")
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
# SOURCE 11: GitHub repo registries (2026-09, new — source label "github")
# ══════════════════════════════════════════════════════════
# Built at the user's explicit request after datascry/openroles
# (github.com/datascry/openroles) surfaced UKG/UltiPro slugs we don't have —
# investigated live and confirmed that repo's OWN scraper hits UKG's
# robots.txt-disallowed LoadSearchResults API directly (not a compliant
# trick; see the module's UKG research), but its DISCOVERY side is a
# genuinely separate, reusable asset: data/tenants/{ats}.json files, each a
# JSON array of {ats, slug, status, first_seen_at, metadata} records,
# pre-vetted by the repo's own weekly liveness probing (we only keep
# status=="live" rows here — a real quality signal most of our other
# sources don't have).
#
# Fetched via jsDelivr's GitHub CDN mirror (cdn.jsdelivr.net/gh/... for raw
# files, data.jsdelivr.com's package API for the file listing), NOT
# GitHub's own API — confirmed live this session that GitHub's REST/GraphQL
# code-search endpoints 403 without a personal token and even authenticated
# cap at 10 req/min for code search, while jsDelivr needs no auth, isn't
# robots-blocked, and mirrors any public repo's files directly. This is a
# different GitHub-based technique from the retired github_org_probe.py
# (Crawler/github_org - Retired/) — that one enumerated GitHub
# ORGANIZATIONS via the metered GitHub API to harvest their profile
# "website" field as a company-domain seed (a company-discovery source);
# this one reads specific known repos' own pre-built slug-registry files
# (an ATS-slug-discovery source) and never touches GitHub's API at all.
#
# GITHUB_REGISTRY_REPOS is deliberately a short, manually-verified seed
# list, not a live "search all of GitHub" crawl — there is no compliant,
# unauthenticated way to search GitHub broadly (confirmed live: the search
# API needs a token this project doesn't have configured, and this
# session's own sandboxed token is repo-scoped, not search-scoped). Finding
# more repos shaped like this one is a manual research task (WebSearch
# turned up only datascry/openroles as a genuine multi-platform slug
# registry this round — several "career-ops" repos that looked similar are
# personal AI job-search CLI tools, not slug databases, confirmed by their
# own descriptions, not assumed). Add more entries here as they're found;
# the fetch/parse logic below is generic per-repo, not hardcoded to this one.
GITHUB_REGISTRY_REPOS = [
    {"repo": "datascry/openroles", "branch": "main", "path_prefix": "data/tenants/"},
]

# openroles filename (their "ats" field) -> our SUPPORTED_ATS key. Every
# platform below was individually checked LIVE this session — either the
# slug is already a bare single string matching what our own scraper
# expects, or openroles' own metadata carries enough to reconstruct our
# compound slug format (workday/brassring/oracle_cloud_hcm — see their
# dedicated _assemble_* functions below). A few platforms were checked and
# genuinely EXCLUDED because neither the bare slug nor the metadata gets us
# to a working slug, and guessing would silently write broken rows (the
# exact "contamination" failure mode this project has fought all session
# with Feashliaa/Rippling):
#   - successfactors: openroles' "slug" is a short nickname ("sap",
#     "adidas"), not the tenant HOSTNAME scrape_successfactors takes as its
#     slug — and every host sampled this session (career5.successfactors.eu,
#     career10.successfactors.com, career4.successfactors.com) is a LEGACY
#     shared domain, i.e. exactly the genuinely-robots.txt-blocked tenant
#     shape this project already confirmed is NOT the reversed
#     branded-CSB-domain case. Also, our own node.py content-fingerprint
#     detection (rmkcdn.successfactors.com) is a fundamentally better fit
#     for this platform than a static snapshot list — it finds branded
#     tenants dynamically wherever they're crawled, rather than depending
#     on openroles having separately discovered and probed them.
#   - ultipro, phenom: not in SUPPORTED_ATS at all (UKG stays blacklisted —
#     see the module's UKG research and GREYLIST_ATS.md; nothing to map to).
# csod is handled separately (see _resolve_csod_career_site_id) — it needs
# ONE live per-tenant HTTP resolve (openroles' own csod.ts scraper does the
# identical bootstrap: GET {slug}.csod.com/ats/careersite/search.aspx —
# confirmed live from their actual source — the redirect target embeds the
# careerSiteId our scrape_csod needs as the 2nd half of its slug; openroles'
# own tenant file carries no such value directly, so it must be resolved,
# not just read).
_GITHUB_REGISTRY_ATS_MAP = {
    "ashby": "ashby",
    "bamboohr": "bamboohr",
    "breezy": "breezyhr",
    "greenhouse": "greenhouse",
    # 2026-09: Hireology / isolvedhire — new platforms, added directly to
    # this map since openroles' bare slug is exactly what our scrapers
    # need too (careers.hireology.com/{slug}/... and
    # {slug}.isolvedhire.com — see ats_scrapers.scrape_hireology/
    # scrape_isolvedhire and this file's _url_to_slug_hireology/
    # _url_to_slug_isolvedhire for the full evidence trail).
    "hireology": "hireology",
    "isolvedhire": "isolvedhire",
    # 2026-09: Gem — checked, NOT present. openroles' data/tenants/ and
    # scraper/src/ats/ file listings (via data.jsdelivr.com's flat
    # structure endpoint) confirmed live to have no "gem" entry at all.
    # No key added here; Gem is still fully wired via SUPPORTED_ATS/
    # URL_TO_SLUG/CC_PLATFORM_PATTERNS/CC_EXTRACTORS/
    # _OPENPOSTINGS_ATS_MAP_RAW above — this source just doesn't carry it.
    "hrmdirect": "hrmdirect",
    "icims": "icims",
    "jazzhr": "jazzhr",
    "jobvite": "jobvite",
    "lever": "lever",
    "pageup": "pageup",
    "paycom": "paycom",  # verified live: their 32-hex slug matches
                          # scrape_paycom's ^[0-9A-F]{32}$ clientkey exactly
    "personio": "personio",
    "pinpointhq": "pinpoint",
    "recruitee": "recruitee",
    "rippling": "rippling",
    "smartrecruiters": "smartrecruiters",
    "taleo": "taleo",
    "teamtailor": "teamtailor",
    "workable": "workable",
    "zohorecruit": "zoho",
}

_WORKDAY_WD_NUM_RE = re.compile(r"\.wd(\d+)\.myworkdayjobs\.com$", re.I)


def _assemble_workday_slug(entry: dict) -> str | None:
    """Reconstruct our 'company|wd#|site_id' format from an openroles
    workday.json entry's metadata (host + site) — confirmed live this
    session against real entries, e.g.
    {"slug": "2020companies", "metadata": {"host":
    "2020companies.wd1.myworkdayjobs.com", "site": "External_Careers"}}
    -> "2020companies|wd1|External_Careers". Returns None (skip the row)
    when metadata.site is missing — confirmed live that some entries omit
    it, and site_id isn't safely guessable (scrape_workday would just get
    a 404 from a wrong guess, silently producing a dead slug)."""
    meta = entry.get("metadata") or {}
    host = (meta.get("host") or "").strip()
    site = (meta.get("site") or "").strip()
    company = (entry.get("slug") or "").strip()
    if not company or not site:
        return None
    m = _WORKDAY_WD_NUM_RE.search(host)
    if not m:
        return None
    return f"{company}|wd{m.group(1)}|{site}"


def _assemble_brassring_slug(entry: dict) -> str | None:
    """Reconstruct our 'partnerId|siteId' format from an openroles
    brassring.json entry's metadata — confirmed live this session against
    real entries, e.g. {"slug": "aafes", "metadata": {"partnerid": "25212",
    "siteid": "5164"}} -> "25212|5164", matching _url_to_slug_brassring's
    own format exactly. Returns None when either value is missing/non-
    numeric rather than guess (mirrors scrape_csod's/_url_to_slug_brassring's
    own PARTNER_ID_RE/SITE_ID_RE digit-only validation)."""
    meta = entry.get("metadata") or {}
    pid = str(meta.get("partnerid") or "").strip()
    sid = str(meta.get("siteid") or "").strip()
    if not (pid.isdigit() and sid.isdigit()):
        return None
    return f"{pid}|{sid}"


def _assemble_oracle_cloud_slug(entry: dict) -> str | None:
    """Reconstruct our 'host_prefix|site_number' format from an openroles
    oraclecloud.json entry's metadata — confirmed live this session against
    real entries, e.g. {"metadata": {"host": "ejhp.fa.us6.oraclecloud.com",
    "site": "CX_2"}} -> "ejhp.fa.us6|CX_2", matching
    _url_to_slug_oracle_cloud's own 'host_prefix|site_number' format (see
    its docstring's own 'eeho.fa.us2|CX_1' example). Returns None if the
    host doesn't actually end in .oraclecloud.com or site is missing."""
    meta = entry.get("metadata") or {}
    host = (meta.get("host") or "").strip().lower()
    site = (meta.get("site") or "").strip()
    if not host.endswith(".oraclecloud.com") or not site:
        return None
    host_prefix = host[:-len(".oraclecloud.com")]
    if not host_prefix:
        return None
    return f"{host_prefix}|{site}"


_CSOD_CAREERSITE_ID_RE = re.compile(r"/ux/ats/careersite/(\d+)/")
CSOD_RESOLVE_TIME_BUDGET_MINUTES = 15  # see fetch_github_registries_slugs docstring


def _resolve_csod_career_site_id(portal_slug: str) -> str | None:
    """One live per-tenant HTTP resolve, mirroring openroles' own csod.ts
    scraper exactly (confirmed live this session by reading their source):
    GET {slug}.csod.com/ats/careersite/search.aspx?site=1&c={slug} — modern
    tenants 302-redirect to a URL containing the careerSiteId
    (/ux/ats/careersite/{csid}/home?c={slug}). openroles' own tenant file
    carries no such value (confirmed live — csod.json entries have no
    metadata field at all), so this can't be a free local assembly like
    workday/brassring/oracle_cloud_hcm above; it costs one real request per
    candidate slug, bounded by CSOD_RESOLVE_TIME_BUDGET_MINUTES below.
    Returns None on any non-redirect response, timeout, or unparseable
    target — never guesses."""
    url = f"https://{portal_slug}.csod.com/ats/careersite/search.aspx?site=1&c={portal_slug}"
    try:
        r = requests.get(url, timeout=15, allow_redirects=True)
    except Exception:
        return None
    m = _CSOD_CAREERSITE_ID_RE.search(r.url or "")
    return m.group(1) if m else None


def _github_registry_list_files(repo: str, branch: str, path_prefix: str) -> list[str]:
    """List every file path under path_prefix in repo@branch via jsDelivr's
    package API (data.jsdelivr.com) — no GitHub API call, no auth, no
    robots block (confirmed live this session; GitHub's own code-search API
    403s unauthenticated). Returns [] on any failure — logged, not raised,
    same as every other source's per-repo/per-file error handling below."""
    url = f"https://data.jsdelivr.com/v1/packages/gh/{repo}@{branch}?structure=flat"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.error(f"  {repo}: failed to list files via jsDelivr: {e}")
        return []
    files = data.get("files") or []
    return [f["name"].lstrip("/") for f in files
            if isinstance(f, dict) and f.get("name", "").lstrip("/").startswith(path_prefix)
            and f["name"].endswith(".json")]


def fetch_github_registries_slugs(csod_resolve_time_budget_minutes: int = CSOD_RESOLVE_TIME_BUDGET_MINUTES
                                   ) -> dict[str, dict[str, str]]:
    """Pull pre-built ATS slug registries from known public GitHub repos
    (see GITHUB_REGISTRY_REPOS) via jsDelivr's CDN mirror. Returns
    {ats: {slug: company_name}}. See the module comment above this
    function for the full research trail (why jsDelivr not GitHub's API,
    why this repo list is short and manual, and why a couple of platforms
    are deliberately excluded from the ATS map rather than guessed).

    csod is special-cased: unlike workday/brassring/oracle_cloud_hcm (a free
    local reassembly from openroles' own metadata), a working csod slug
    needs one live per-tenant HTTP resolve (see
    _resolve_csod_career_site_id) — run with a small thread pool, bounded by
    csod_resolve_time_budget_minutes so a large/slow csod.json can't turn
    one run into an unbounded wall-clock cost; past the budget, whatever's
    already resolved is kept (same self-stop-gracefully shape as every
    other long-running source in this file), the rest is simply skipped
    this run rather than lost (a future run re-attempts them)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    slugs_by_ats: dict[str, dict[str, str]] = {ats: {} for ats in SUPPORTED_ATS}
    skipped_ats: dict[str, int] = {}

    for reg in GITHUB_REGISTRY_REPOS:
        repo, branch, prefix = reg["repo"], reg["branch"], reg["path_prefix"]
        files = _github_registry_list_files(repo, branch, prefix)
        if not files:
            log.warning(f"  {repo}: no tenant files found under '{prefix}' — skipping")
            continue
        log.info(f"  {repo}: {len(files)} tenant files found")

        for path in files:
            basename = path[len(prefix):].removesuffix(".json").lower()
            is_workday = basename == "workday"
            is_brassring = basename == "brassring"
            is_oracle = basename == "oraclecloud"
            is_csod = basename == "csod"
            if is_csod:
                our_ats = "csod"
            elif is_workday:
                our_ats = "workday"
            elif is_brassring:
                our_ats = "brassring"
            elif is_oracle:
                our_ats = "oracle_cloud_hcm"
            else:
                our_ats = _GITHUB_REGISTRY_ATS_MAP.get(basename)
            if not our_ats:
                skipped_ats[basename] = skipped_ats.get(basename, 0) + 1
                continue

            raw_url = f"https://cdn.jsdelivr.net/gh/{repo}@{branch}/{path}"
            try:
                r = requests.get(raw_url, timeout=60)
                r.raise_for_status()
                entries = r.json()
            except Exception as e:
                log.error(f"  {repo}/{path}: failed to fetch/parse: {e}")
                continue
            if not isinstance(entries, list):
                log.warning(f"  {repo}/{path}: unexpected JSON shape (not a list) — skipping")
                continue

            live_entries = [e for e in entries if isinstance(e, dict) and e.get("status") == "live"]

            if is_csod:
                # Live per-tenant resolve, bounded by time budget — see
                # docstring. Each entry's bare portal slug ("a-talent")
                # becomes our 'tenant|careerSiteId' format only if the
                # resolve succeeds.
                added = 0
                deadline = time.monotonic() + csod_resolve_time_budget_minutes * 60
                budget_hit = False
                with ThreadPoolExecutor(max_workers=30) as pool:
                    futures = {}
                    for entry in live_entries:
                        if time.monotonic() >= deadline:
                            budget_hit = True
                            break
                        portal_slug = (entry.get("slug") or "").strip()
                        if not portal_slug or portal_slug.lower() in SKIP_SLUGS:
                            continue
                        futures[pool.submit(_resolve_csod_career_site_id, portal_slug)] = entry
                    for fut in as_completed(futures):
                        entry = futures[fut]
                        try:
                            csid = fut.result()
                        except Exception:
                            csid = None
                        if not csid:
                            continue
                        portal_slug = (entry.get("slug") or "").strip()
                        slug = f"{portal_slug}|{csid}"
                        name = (entry.get("display_name") or "").strip()
                        if slug not in slugs_by_ats[our_ats]:
                            slugs_by_ats[our_ats][slug] = name
                            added += 1
                if budget_hit:
                    log.warning(f"    csod: resolve time budget "
                                f"({csod_resolve_time_budget_minutes}min) reached — "
                                f"remaining candidates skipped this run, will be "
                                f"re-attempted next run")
                if added:
                    log.info(f"    csod -> {our_ats}: {added} live slugs (resolved)")
                continue

            added = 0
            for entry in live_entries:
                if is_workday:
                    slug = _assemble_workday_slug(entry)
                elif is_brassring:
                    slug = _assemble_brassring_slug(entry)
                elif is_oracle:
                    slug = _assemble_oracle_cloud_slug(entry)
                else:
                    slug = (entry.get("slug") or "").strip()

                if not slug:
                    continue
                bare_part = slug.split("|", 1)[0] if "|" in slug else slug
                if bare_part.lower() in SKIP_SLUGS:
                    continue
                if not (is_workday or is_brassring or is_oracle) and not _looks_like_real_slug(slug):
                    continue

                name = (entry.get("display_name") or "").strip()
                if slug not in slugs_by_ats[our_ats]:
                    slugs_by_ats[our_ats][slug] = name
                    added += 1
            if added:
                log.info(f"    {basename} -> {our_ats}: {added} live slugs")

    total = sum(len(s) for s in slugs_by_ats.values())
    log.info(f"GitHub registries total: {total} slugs across "
             f"{sum(1 for s in slugs_by_ats.values() if s)} platforms")
    if skipped_ats:
        top_skipped = sorted(skipped_ats.items(), key=lambda x: -x[1])[:10]
        log.info(f"  unmapped registry files (excluded, see module comment): "
                 f"{', '.join(f'{k}({v})' for k, v in top_skipped)}")
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
        description="Discovery: populate Supabase archive_i from 9 active sources"
    )
    parser.add_argument(
        "--source",
        choices=["feashliaa", "kalil", "openpostings", "commoncrawl",
                 "wayback", "ct_logs", "theirstack", "httparchive",
                 "latmay", "edwarddgao", "openjobsdaily",
                 "icims_hrjobs", "github", "all"],
        default="all",
        help="Which source to pull from (default: all). 'yc' removed "
             "2026-09 — see the module docstring. 'wayback_adp' renamed "
             "to 'wayback' 2026-09 when this source was generalized to "
             "every ATS platform, not just ADP.",
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
        "--wayback-shard", type=int, default=None,
        help="Which Wayback Machine platform-shard this run covers (0-indexed, "
             "used with --wayback-total-shards). Default: None = all "
             "platforms in one run. See fetch_wayback_slugs docstring.",
    )
    parser.add_argument(
        "--wayback-total-shards", type=int, default=1,
        help="Total number of Wayback Machine platform-shards (default: 1, "
             "i.e. no sharding). discovery.yml runs this as 5 (shards 0-4) "
             "as separate matrix jobs — added 2026-09 alongside raising "
             "_WAYBACK_MAX_PAGES so heavily-archived platforms don't turn "
             "one unsharded run into a single long sequential job.",
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
        "--edwarddgao-time-budget-minutes", type=int, default=270,
        help="Self-stop gracefully after this many minutes downloading/"
             "resolving Edward H.F's 375 Parquet shards, keeping whatever "
             "was resolved so far (default: 270, deliberately 30min under "
             "Discovery.yml's 300min job timeout — this function returning "
             "isn't the end of the run, upsert_to_supabase() still has to "
             "write everything resolved so far afterward, so the internal "
             "budget needs real margin before GitHub's hard kill, not the "
             "same number; 0 = no budget, run to full completion — see "
             "fetch_edwarddgao_slugs docstring).",
    )
    parser.add_argument(
        "--openjobsdaily-time-budget-minutes", type=int, default=270,
        help="Self-stop gracefully after this many minutes downloading/"
             "resolving Open Jobs Daily H.F's 15 Parquet files (~24.8GB, "
             "both configs), keeping whatever was resolved so far "
             "(default: 270, same margin-under-job-timeout reasoning as "
             "--edwarddgao-time-budget-minutes; 0 = no budget, run to full "
             "completion — see fetch_openjobsdaily_slugs docstring).",
    )
    parser.add_argument(
        "--hf-shard", type=int, default=None,
        help="Which Hugging Face shard this run covers (0-indexed, used "
             "with --hf-total-shards) — applies to latmay, edwarddgao, "
             "AND openjobsdaily. For edwarddgao/openjobsdaily this "
             "slices the Parquet FILE list (cuts download volume per "
             "shard); for latmay (a single file) this slices ROW "
             "INDEXES after the one download. Default: None = all "
             "rows/files in one run.",
    )
    parser.add_argument(
        "--hf-total-shards", type=int, default=1,
        help="Total number of Hugging Face shards (default: 1, i.e. no "
             "sharding). discovery.yml runs this as 3 for edwarddgao "
             "and openjobsdaily (real per-shard Parquet-file download "
             "reduction); latmay stays a single unsharded job (one "
             "small file — sharding it would only spread per-row "
             "URL_TO_SLUG work, not cut download volume).",
    )
    parser.add_argument(
        "--csod-resolve-budget-minutes", type=int, default=CSOD_RESOLVE_TIME_BUDGET_MINUTES,
        help="Self-stop gracefully after this many minutes resolving GitHub-registry "
             "csod (Cornerstone) bare portal slugs to our 'tenant|careerSiteId' format "
             "via one live HTTP redirect check per candidate (default: "
             f"{CSOD_RESOLVE_TIME_BUDGET_MINUTES} — see fetch_github_registries_slugs docstring). "
             "Unresolved candidates past the budget are simply skipped this run, not lost.",
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
    log.info("           + Wayback CDX (all ATS) + Latmay H.F + Edward H.F")
    log.info("           + Open Jobs Daily H.F")
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

    # Source 5: Wayback Machine CDX — generalized 2026-09 to every platform
    # with a CC_PLATFORM_PATTERNS entry, not just ADP (see
    # fetch_wayback_slugs' docstring and the module header comment above it)
    if args.source in ("wayback", "all"):
        log.info("\n--- WAYBACK MACHINE CDX (cross-platform supplemental discovery) ---")
        wb_slugs = fetch_wayback_slugs(wb_shard=args.wayback_shard,
                                        wb_total_shards=args.wayback_total_shards)
        wb_total = sum(len(s) for s in wb_slugs.values())
        log.info(f"Wayback CDX total: {wb_total} slugs across {len(wb_slugs)} platforms")

        if not args.dry_run:
            # "wayback" matches archive_i's own source CHECK constraint
            # (2,910+ real historical rows already written under this label
            # from the original ADP-only version — unchanged by this
            # generalization, still the right value).
            upserted = upsert_to_supabase(wb_slugs, source="wayback",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += wb_total

    # Source 5b: Certificate Transparency logs (crt.sh) — 2026-09, new.
    # Only helps the subdomain-per-tenant platforms in CT_LOG_SUFFIXES —
    # see that dict's module comment for the full reasoning.
    if args.source in ("ct_logs", "all"):
        log.info("\n--- CERTIFICATE TRANSPARENCY LOGS (crt.sh) ---")
        ct_slugs = fetch_ct_log_slugs()
        ct_total = sum(len(s) for s in ct_slugs.values())
        log.info(f"CT logs total: {ct_total} slugs across {len(ct_slugs)} platforms")

        if not args.dry_run:
            # NOTE: archive_i.source has a CHECK constraint allowlist —
            # 'ct_logs' must be added to it (ALTER TABLE ... DROP/ADD
            # CONSTRAINT archive_i_source_check) before this upsert will
            # succeed. See the chat history for the exact migration SQL —
            # it was blocked from being applied directly from this session
            # by the same auto-mode classifier that blocks mass deletes.
            upserted = upsert_to_supabase(ct_slugs, source="ct_logs",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += ct_total

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
        lm_slugs = fetch_latmay_slugs(hf_shard=args.hf_shard, hf_total_shards=args.hf_total_shards)
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

    # Source: iCIMS HR Jobs (hrjobs.icims.com — centralized multi-tenant
    # board, HR-professional roles across iCIMS's customer base)
    if args.source in ("icims_hrjobs", "all"):
        log.info("\n--- ICIMS HR JOBS (hrjobs.icims.com centralized board) ---")
        ihr_slugs = fetch_icims_hrjobs_slugs()
        ihr_total = sum(len(s) for s in ihr_slugs.values())
        if not args.dry_run:
            upserted = upsert_to_supabase(ihr_slugs, source="icims_hrjobs",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += ihr_total

    # Source 11: GitHub repo registries (pre-built ATS slug files from
    # known public repos, e.g. datascry/openroles — see
    # fetch_github_registries_slugs docstring)
    if args.source in ("github", "all"):
        log.info("\n--- GITHUB REGISTRIES (pre-built ATS slug files) ---")
        gr_slugs = fetch_github_registries_slugs(
            csod_resolve_time_budget_minutes=args.csod_resolve_budget_minutes)
        gr_total = sum(len(s) for s in gr_slugs.values())
        if not args.dry_run:
            upserted = upsert_to_supabase(gr_slugs, source="github",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += gr_total

    # Source 8: Edward H.F (huggingface.co/datasets/edwarddgao/open-apply-jobs
    # — 31M+ individual job postings, apply_url resolved through URL_TO_SLUG)
    if args.source in ("edwarddgao", "all"):
        log.info("\n--- EDWARD H.F (Hugging Face, 31M+ job postings) ---")
        ed_slugs = fetch_edwarddgao_slugs(
            time_budget_minutes=args.edwarddgao_time_budget_minutes,
            hf_shard=args.hf_shard, hf_total_shards=args.hf_total_shards)
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

    # Source 12: Open Jobs Daily H.F (huggingface.co/datasets/Yigit-Karaman/
    # open-jobs-daily — ~9.8M rows across both configs, CC0-1.0)
    if args.source in ("openjobsdaily", "all"):
        log.info("\n--- OPEN JOBS DAILY H.F (Hugging Face, ~9.8M job postings) ---")
        ojd_slugs = fetch_openjobsdaily_slugs(
            time_budget_minutes=args.openjobsdaily_time_budget_minutes,
            hf_shard=args.hf_shard, hf_total_shards=args.hf_total_shards)
        ojd_total = sum(len(s) for s in ojd_slugs.values())
        if ojd_total:
            log.info(f"Open Jobs Daily H.F total: {ojd_total} slugs across "
                     f"{sum(1 for s in ojd_slugs.values() if s)} platforms")

        if not args.dry_run:
            upserted = upsert_to_supabase(ojd_slugs, source="Open Jobs Daily H.F",
                                           dry_run=args.dry_run)
            grand_total += upserted
        else:
            grand_total += ojd_total

    # Source 13 (Zalize H.F) and Source 14 (Aramente H.F) REMOVED 2026-09
    # at the user's request. fetch_zalizedata_slugs()/fetch_eutechjobs_slugs()
    # themselves are left defined/unused, same treatment as fetch_yc_slugs()
    # above — zero risk, easy to restore if ever wanted back. Their
    # 'Zalize H.F'/'Aramente H.F' matrix jobs were removed from
    # Discovery.yml in the same change; their archive_i.source CHECK
    # constraint values were deliberately left in place (harmless unused
    # allowed values, same as legacy 'wdc'/'tranco').

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
