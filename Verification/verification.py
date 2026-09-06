"""
VERIFICATION ENGINE (2026-08) — scans archive_ii (ats + slug, formerly
quarantine) and archive_iii (career pages, formerly scrape_test) and
removes ONLY rows that are CONFIRMED DEAD: the board/page genuinely no
longer exists. It does NOT remove rows just because a real board
currently has zero open postings, or a career page's content has
changed — see THE RULE below for why, and the 2026-08 research summary
for how each platform's rule was derived.

THE RULE, same spirit as certificate_transparency_probe.py's Phase-2
verifier (this file follows and extends that one's design, and directly
reuses several of its already-proven per-platform checks):
  - A clear, structural "this slug/company does not exist here" signal
    (a definite 404 from the platform's OWN API, a redirect that bounces
    away to the ATS VENDOR's marketing/root domain instead of staying on
    the tenant's own subdomain, a DNS resolution failure for a
    subdomain-per-tenant platform, or an explicit "does not exist" page
    the platform itself serves) -> DEAD, delete the row.
  - Anything else — a real response, even an empty one (zero current
    jobs), a redirect that lands somewhere else entirely (ambiguous, not
    the vendor's own marketing bounce), a rate-limit, a 5xx, a timeout,
    an unexpected status code -> LEFT ALONE, never deleted. A row we
    couldn't cleanly confirm dead is not evidence it's dead.
  - Per explicit instruction (2026-08): a real, valid board that simply
    has zero open postings right now is KEPT, not deleted — that's what
    ats_scrapers.py's own future 183-day-since-last-hit cleanup (a
    separate, later piece of work) is for, not this file. This file's
    only job is pruning slugs/pages that never existed, were mistyped,
    or the company has genuinely torn down — not "currently quiet".

REPORTING (2026-08): every archive_ii row that verifies as NOT dead gets
a second, best-effort pass through ats_scrapers.scrape_board() — the
exact same production scraper the daily ATS scanner uses — purely to
split the report into ACTIVE (>=1 open job right now) vs EMPTY (real,
live board, 0 open jobs right now). This split is reporting-only: it
NEVER affects the delete decision (that's the safety-checked verifier
above, alone) — scrape_board() can itself return an empty list on a
transient scrape error, so an "empty" count is a best-effort read, not
an authoritative one, while "dead" always is.

WHY 18 OF 26 ATS PLATFORMS, NOT ALL: 2026-08, four parallel research
passes empirically tested real vs fake slugs against every SCRAPERS-
registered platform's actual endpoint (WebFetch against live real and
obviously-fake slugs, cross-checked against each platform's own docs).
18 platforms have a confirmed-safe, structurally distinct "does not
exist" signal. 8 do not (see _UNVERIFIABLE_ATS below) — either the
platform returns an identical-looking response for "doesn't exist" and
"real board, 0 jobs" (oracle_cloud_hcm, confirmed empirically: a real
empty tenant returns the exact same 200+empty-array shape a nonexistent
one would), the check requires JS/POST semantics no lightweight HTTP
probe can safely replicate (workday, smartrecruiters, taleo), or no
live example could be found/reached to confirm a rule at all (breezyhr,
jobadder — jobadder's "Nothing here I'm afraid..." page was found to be
plausibly the SAME message a real empty board shows, an explicitly
UNSAFE signal — folkshr, adp). Rows on unverifiable platforms, plus
brassring/successfactors (JS-rendered, no scraper at all — see
ats_scrapers.py's SCRAPERS dict) and ycombinator (a job-board aggregator,
not a per-company ATS — see discovery.py's URL_TO_SLUG comment), are
left completely untouched by this engine and only counted (as
"unverified") and logged.

SAFETY MODEL (a wrong delete here is real, silent, permanent data loss):
  - report-only is the DEFAULT — run this with no flags and it only logs
    and summarizes what it WOULD delete. The only way anything is ever
    actually deleted is the explicit --execute flag (or checking the
    "Delete confirmed-dead rows" box in the GitHub Actions UI). Read a
    report-only run's summary before ever turning that on.
  - every per-row check is wrapped so ANY exception (timeout, connection
    reset, unexpected error) is treated as unverified/leave-alone — only
    an explicit, structural "confirmed dead" return value can lead to a
    delete, mirroring certificate_transparency_probe.py's own
    verify_row() pattern exactly.
  - archive_iii verification requires BOTH the career_page_url AND the
    company's own website_url (root domain) to independently fail before
    a row is treated as dead — a career page alone returning 404 usually
    just means the page moved (common on a real, live site), which is
    not the same as the company disappearing.

SHARDING + FINALIZE (2026-08, same overall shape as Main/main.py's ATS
scanner, but NOT the same sharding mechanism): this file runs as N
parallel shards (--shard-index/--shard-count), each writing its own JSON
summary (--summary-out). Unlike main.py's hash-based sharding (which
needs the WHOLE slug list in memory to hash-partition it), each shard
here fetches ONLY its own ~1/N slice of rows directly from Supabase — a
cheap COUNT query first, then a server-side Range-based slice ordered by
id (see fetch_rows). This matters: an earlier version fetched the ENTIRE
table in every shard and threw away the other (N-1)/N client-side, so N
shards collectively pulled N times the table's actual size from Supabase
at once — confirmed real 2026-08 as the cause of a 20-shard run seeing
GETs and DELETEs alike time out ("increased errors" on Supabase's own
side). A separate finalize pass (--summarize DIR) then reads every
shard's JSON summary out of that directory and prints ONE combined
report — total active/empty/dead/unverified counts across all shards —
mirroring how main.py's `--finalize` step runs once after every shard
has finished. See verification.yml for how the two are wired together
in CI.

NOTE ON CEREBRAS_API_KEY: this file imports ats_scrapers.scrape_board()
for the active/empty split above, which transitively imports Main/
config.py — and config.py unconditionally requires SOME LLM provider key
(CEREBRAS_API_KEY by default) to even finish importing, even though
nothing in this file ever calls an LLM. This is a preexisting config.py
behavior, not something to work around here — just make sure whatever
already-configured secret satisfies it (e.g. the same CEREBRAS_API_KEY
daily_scan.yml uses) is also passed to this script's environment, or
the import fails before verification even starts. See verification.yml.

Usage:
    pip install aiohttp python-dotenv requests beautifulsoup4
    python verification.py --table archive_ii                        # report-only
    python verification.py --table archive_ii --ats greenhouse
    python verification.py --table archive_ii --shard-index 0 --shard-count 10 --summary-out shard0.json
    python verification.py --summarize ./summaries                    # combine + print all shard*.json in a dir
    python verification.py --table both --execute                     # ACTUALLY deletes confirmed-dead rows
"""
import argparse
import asyncio
import concurrent.futures
import glob
import json
import logging
import os
import random
import re
import socket
import sys
from urllib.parse import urlparse

import aiohttp
from dotenv import load_dotenv

load_dotenv()

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)  # for node.py
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # for ats_scrapers.py (job-count reporting)
sys.path.insert(0, os.path.join(_ROOT, "Certificate Transparency"))  # for proven verifiers below
import node  # noqa: E402
from ats_scrapers import scrape_board  # noqa: E402
# Reused as-is — these 8 already check the exact host/path
# ats_scrapers.py's real production scraper uses (or, for icims/teamtailor/
# recruitee/softgarden/zoho/hrmdirect, the tenant's own subdomain root,
# which is a safe, path-independent existence proxy for a per-tenant-
# subdomain platform), and already treat any connection failure as
# inconclusive rather than dead — exactly this file's own safety model,
# just proven in production first. See that file's own module docstring
# for the full "final redirect host must match, else dead" rationale.
from certificate_transparency_probe import (  # noqa: E402
    _verify_bamboohr, _verify_icims, _verify_teamtailor, _verify_recruitee,
    _verify_softgarden, _verify_zoho, _verify_hrmdirect, _verify_personio,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("verification")

REQUEST_TIMEOUT = node.REQUEST_TIMEOUT
USER_AGENT = node.USER_AGENT
# 2026-08-28 incident: confirmed via direct query that this Supabase project's
# max_connections=60, and ~30 of those are permanently held by Supabase's own
# internals (pooler/realtime/auth/storage/dashboard) even at idle — leaving
# roughly 30 connections of real headroom, TOTAL, shared across every shard
# AND every other workflow (daily_scan, people_data_labs_probe) that might be running
# at the same time. The old default of 30 here, multiplied across
# shard_count shards (10 by default in verification.yml), asked for up to
# 300 simultaneous requests — ~10x the actual budget — and reliably caused
# "PGRST003 Timed out acquiring connection from connection pool" followed by
# cascading 504s (visible directly in Supabase's postgres/edge logs). This
# is NOT fixable with client-side retry/backoff: the server-side pool is
# genuinely out of connections, so retrying just resubmits into the same
# exhausted queue.
#
# Follow-up finding: Main/supabase_handler.py (used by main.py / the
# 8-shard daily_scan.yml and every other proven-stable workflow here) makes
# ONE synchronous `requests` call at a time, per shard — no asyncio, no
# in-process concurrency at all. N shards there means ~N connections, ever,
# further softened because each shard's own scrape/parse work naturally
# staggers its calls instead of firing them in a synchronized burst.
# verification.py's asyncio.Semaphore(concurrency) is fundamentally
# different: it lets ONE shard hold `concurrency` connections open
# simultaneously, in a tight synchronized burst every time the semaphore
# admits a new batch — so shard_count x concurrency is both a higher total
# AND a burstier one than the same total spread across more shards. Given
# that, prefer MORE shards over higher per-shard concurrency — it mirrors
# the pattern that's actually been proven safe. Lowered so
# shard_count(10, see verification.yml) x concurrency(2) = 20 stays
# comfortably under the ~30-connection headroom with margin for other
# workflows, while keeping per-shard concurrency close to main.py's
# 1-at-a-time norm. Raise only after confirming real headroom via
# `select count(*) from pg_stat_activity` and this project's
# max_connections — never by guessing, and raise shard_count before
# concurrency.
DEFAULT_CONCURRENCY = 2


def _new_connector(concurrency: int) -> aiohttp.TCPConnector:
    """Deliberately NOT node.new_connector() — that one is sized (limit=550
    by default, see CONNECTOR_LIMIT) for crawling millions of DIFFERENT
    external company domains, where each domain's own rate limit is
    independent of every other. Every call this file makes either hits ONE
    shared Supabase REST endpoint or, at most, one host per verified row —
    reusing node's mass-fan-out sizing here means each of N parallel GitHub
    Actions shards independently opens up to 550 connections, so N shards
    can aggregate into thousands of concurrent connections all hammering
    the SAME Supabase project at once. Confirmed real 2026-08: a 20-shard
    run saw its GET *and every single DELETE* start failing/timing out
    across multiple shards simultaneously — the signature of Supabase's own
    connection pooler/rate limits being overwhelmed by aggregate load, not
    random per-request flakiness. Sizing the connector to this run's own
    --concurrency (with a little headroom) keeps each shard's footprint
    proportional to what was actually asked for, so aggregate load across
    N shards scales with N * concurrency, not N * 550."""
    return aiohttp.TCPConnector(limit=concurrency + 10, ttl_dns_cache=300)


# ── archive_ii: platforms with NO safe "does not exist" signal ────────────
# Left completely untouched — never checked, never deleted, only counted.
# See the module docstring above for why each one is here.
_UNVERIFIABLE_ATS = {
    "workday",           # SPA + POST-based API; no reliable GET-based signal found
    "smartrecruiters",   # docs suggest a 404, but shared-domain API is robots-gated
                          # and couldn't be empirically re-confirmed live
    "breezyhr",           # no live customer example could be found/reached to confirm any rule
    "taleo",              # host unreachable from this research pass (env/network limitation)
    "oracle_cloud_hcm",   # CONFIRMED UNSAFE: a real, empty tenant returns the exact same
                          # 200 + {"items":[],"count":0} shape a nonexistent one plausibly would
    "jobadder",           # CONFIRMED UNSAFE: "Nothing here I'm afraid..." could be the same
                          # message a real, empty board shows — no way to distinguish
    "folkshr",            # no live customer example could be found/reached to confirm any rule
    "adp",                 # no live customer example could be found/reached to confirm any rule
    "brassring",          # JS-rendered, no HTTP scraper at all (see ats_scrapers.py SCRAPERS)
    "successfactors",     # JS-rendered, no HTTP scraper at all (see ats_scrapers.py SCRAPERS)
    "ycombinator",        # job-board aggregator, not a per-company ATS slug (see discovery.py)
}


# ── archive_ii: NEW verifiers (platforms certificate_transparency_probe.py
# never covered) ────────────────────────────────────────────────────────

async def _verify_rippling(session: aiohttp.ClientSession, slug: str) -> bool:
    """Rippling's real production scraper (ats_scrapers.scrape_rippling)
    hits the shared ats.rippling.com API, NOT a {slug}.rippling.com
    subdomain — checking the actual API endpoint here (unlike a bare
    subdomain check) is what keeps this consistent with what the daily
    scanner would actually find."""
    url = f"https://ats.rippling.com/api/v2/board/{slug}/jobs"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")


async def _verify_greenhouse(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")


async def _verify_lever(session: aiohttp.ClientSession, slug: str) -> bool:
    """Only DEAD if BOTH the main and EU endpoints 404 — a real board
    could legitimately live on either one (see ats_scrapers.scrape_lever's
    own EU fallback)."""
    for url in (f"https://api.lever.co/v0/postings/{slug}?mode=json",
                f"https://api.eu.lever.co/v0/postings/{slug}?mode=json"):
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT,
                                    headers={"User-Agent": USER_AGENT}) as r:
                if r.status == 200:
                    return True
        except Exception:
            continue
    # Neither endpoint returned 200 — confirm both actually 404'd (not just
    # errored) before calling it dead.
    for url in (f"https://api.lever.co/v0/postings/{slug}?mode=json",
                f"https://api.eu.lever.co/v0/postings/{slug}?mode=json"):
        async with session.get(url, timeout=REQUEST_TIMEOUT,
                                headers={"User-Agent": USER_AGENT}) as r:
            if r.status != 404:
                raise RuntimeError(f"ambiguous status {r.status} on {url}")
    return False


async def _verify_ashby(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")


async def _verify_workable(session: aiohttp.ClientSession, slug: str) -> bool:
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")


async def _verify_joincom(session: aiohttp.ClientSession, slug: str) -> bool:
    """Page-level check only (join.com/companies/{slug}) — the real jobs
    API needs a company_id resolved from this same page first, so a clean
    404 on the page itself is already a safe existence proxy."""
    url = f"https://join.com/companies/{slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 404:
            return False
        if r.status == 200:
            return True
        raise RuntimeError(f"ambiguous status {r.status}")


async def _verify_paylocity(session: aiohttp.ClientSession, slug: str) -> bool:
    """slug is 'company_id|company_name' (see discovery._url_to_slug_paylocity
    and ats_scrapers.scrape_paylocity). A real board's page embeds a
    window.pageData JSON blob (checked here) regardless of whether it
    currently has any open jobs; a fake company_id instead serves a static
    'Job Not Found'/'does not exist' page with no such blob."""
    parts = slug.split("|", 1)
    if len(parts) != 2:
        raise RuntimeError(f"unexpected paylocity slug format: {slug!r}")
    company_id, company_name_slug = parts
    url = f"https://recruiting.paylocity.com/recruiting/jobs/All/{company_id}/{company_name_slug}"
    async with session.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status != 200:
            raise RuntimeError(f"ambiguous status {r.status}")
        text = await r.text()
    if "window.pageData" in text:
        return True
    lowered = text.lower()
    if "does not exist" in lowered or "job not found" in lowered:
        return False
    raise RuntimeError("neither pageData nor a recognized not-found page — ambiguous")


async def _verify_jobvite(session: aiohttp.ClientSession, slug: str) -> bool:
    """A nonexistent Jobvite company 302s to jobvite.com's own support page
    with a distinctive '?invalid=1' query param — a real board (even an
    empty one) serves its own jobs page directly, no such redirect."""
    url = f"https://jobs.jobvite.com/{slug}/jobs"
    async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=False,
                            headers={"User-Agent": USER_AGENT}) as r:
        if r.status == 200:
            return True
        if r.status in (301, 302, 303, 307, 308):
            location = r.headers.get("Location", "")
            if "invalid=1" in location:
                return False
            raise RuntimeError(f"redirected somewhere unrecognized: {location!r}")
        raise RuntimeError(f"ambiguous status {r.status}")


async def _dns_dead_check(session: aiohttp.ClientSession, url: str) -> bool:
    """Shared helper for subdomain-per-tenant platforms whose ONLY safe
    'does not exist' signal is the subdomain simply failing to resolve at
    all (confirmed for avature/eploy: a nonexistent tenant subdomain isn't
    provisioned in DNS at all, vs a real tenant which resolves and serves
    something — even an error page — every time). Any other failure mode
    (connection refused post-DNS, timeout, non-2xx after resolving) is
    left ambiguous, not dead — only a genuine name-resolution failure
    counts."""
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                                headers={"User-Agent": USER_AGENT}):
            return True  # resolved and got SOME response — tenant exists
    except aiohttp.ClientConnectorError as e:
        os_err = getattr(e, "os_error", None)
        if isinstance(os_err, socket.gaierror):
            return False  # genuine DNS resolution failure — confirmed dead
        raise  # some other connection failure (refused, unreachable) — ambiguous


async def _verify_avature(session: aiohttp.ClientSession, slug: str) -> bool:
    return await _dns_dead_check(session, f"https://{slug}.avature.net/careers/SearchJobs")


async def _verify_eploy(session: aiohttp.ClientSession, slug: str) -> bool:
    return await _dns_dead_check(
        session, f"https://{slug}.eploy.net/candidate/jobboard/vacancysearchresults.aspx")


ARCHIVE_II_VERIFIERS = {
    "bamboohr": _verify_bamboohr,
    "icims": _verify_icims,
    "teamtailor": _verify_teamtailor,
    "recruitee": _verify_recruitee,
    "softgarden": _verify_softgarden,
    "zoho": _verify_zoho,
    "hrmdirect": _verify_hrmdirect,
    "personio": _verify_personio,
    "rippling": _verify_rippling,
    "greenhouse": _verify_greenhouse,
    "lever": _verify_lever,
    "ashby": _verify_ashby,
    "workable": _verify_workable,
    "joincom": _verify_joincom,
    "paylocity": _verify_paylocity,
    "jobvite": _verify_jobvite,
    "avature": _verify_avature,
    "eploy": _verify_eploy,
}


# ── archive_iii: career-page verification ─────────────────────────────

# ── 2026-09: soft-404 / parked-domain / masked-error detection ─────────
# A 200 OK is not, by itself, proof a career page is still real — three
# real failure modes were confirmed to slip past the plain "404 or DNS
# failure" check above:
#   1. PARKED DOMAINS: a company's root domain lapses and gets pointed at
#      a registrar's parking page (GoDaddy, Dan.com, Sedo, etc.) — the
#      request still gets a 200, on a real, resolvable host.
#   2. SOFT-404s: a site serves an actual "page not found"/"suspended"
#      error page but with an HTTP 200 status instead of a real 404 —
#      common on static hosts and some CMSs.
#   3. MASKED ERROR PAYLOADS: a platform's API returns HTTP 200 with a
#      JSON body that is ITSELF an error object (a graceful-failure
#      pattern some ATS platforms use instead of a real HTTP error code).
# All three checks below are deliberately conservative — an explicit
# parking-registrar host, an explicit "not found"/"suspended"/"for sale"
# title, or an explicit error field in a JSON body. A merely short,
# unusual, or empty-but-real page never matches any of these and is left
# alone, same as before.
_PARKED_DOMAIN_HOSTS = {
    "sedoparking.com", "sedo.com", "dan.com", "afternic.com",
    "hugedomains.com", "parkingcrew.net", "bodis.com", "parklogic.com",
    "above.com", "uniregistrymarket.link", "parked.com", "voodoo.com",
    "domainmarket.com", "godaddy.com",  # GoDaddy's own parked-page landing host
}
_PARKED_OR_SOFT_404_TITLE_RE = re.compile(
    r"domain\s+(has\s+expired|for\s+sale|is\s+parked)"
    r"|this\s+domain\s+(may\s+be\s+for\s+sale|is\s+for\s+sale)"
    r"|buy\s+this\s+domain"
    r"|(account|website|site)\s+(has\s+been\s+)?suspended"
    r"|^\s*(404|not\s+found)\s*$"
    r"|404\b.{0,10}not\s+found"
    r"|page\s+not\s+found"
    r"|the\s+page\s+you.{0,15}(are\s+)?(looking|searching)\s+for.{0,25}"
    r"(not\s+found|does\s*n[o']t\s+exist)",
    re.I,
)
_TITLE_TAG_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
_JSON_CONTENT_TYPE_RE = re.compile(r"\bjson\b", re.I)
_JSON_ERROR_VALUE_RE = re.compile(
    r"not\s*found|does\s*n[o']t\s+exist|invalid\s*(tenant|company|slug|account)"
    r"|no\s+such|unknown\s*(tenant|company|account)",
    re.I,
)


def _looks_like_parked_or_soft_404(final_url: str, body_text: str) -> bool:
    """True only for an explicit parking-registrar host or an explicit
    'not found'/'suspended'/'for sale'-style <title> — see block comment
    above. Never fires on a merely unusual or short page."""
    try:
        host = (urlparse(final_url).hostname or "").lower()
    except Exception:
        host = ""
    if host and any(host == h or host.endswith("." + h) for h in _PARKED_DOMAIN_HOSTS):
        return True
    m = _TITLE_TAG_RE.search(body_text or "")
    return bool(m and _PARKED_OR_SOFT_404_TITLE_RE.search(m.group(1)))


def _looks_like_masked_error_payload(content_type: str, body_text: str) -> bool:
    """True only when the body parses as a JSON object AND that object's
    own error/message field unambiguously reads as a 'this tenant/slug
    doesn't exist' error — never fires on a body that doesn't parse as
    JSON, or that parses but isn't error-shaped."""
    if not _JSON_CONTENT_TYPE_RE.search(content_type or ""):
        return False
    try:
        data = json.loads(body_text)
    except Exception:
        return False
    if not isinstance(data, dict):
        return False
    error_val = str(data.get("error") or data.get("message") or data.get("errors") or "")
    return bool(error_val and _JSON_ERROR_VALUE_RE.search(error_val))


async def _url_confirmed_dead(session: aiohttp.ClientSession, url: str) -> bool | None:
    """Returns True if this specific URL is confirmed dead — a real 404,
    the domain failing to resolve at all, a certificate hostname mismatch
    (see below), or a 200 that's actually a parked domain/soft-404/masked
    error payload (see the block comment above _looks_like_parked_or_soft_404).
    Returns False if it responded with anything else usable, or None if
    the check was inconclusive (timeout, 5xx, other connection error, an
    ambiguous TLS error) — PERSISTENT DEFAULT SAFETY: none of those are
    ever treated as dead, unchanged from before."""
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                                headers={"User-Agent": USER_AGENT}) as r:
            if r.status == 404:
                return True
            if r.status == 200:
                content_type = r.headers.get("Content-Type", "")
                try:
                    body_text = await r.text(errors="ignore")
                except Exception:
                    body_text = ""
                if _looks_like_parked_or_soft_404(str(r.url), body_text):
                    return True
                if _looks_like_masked_error_payload(content_type, body_text):
                    return True
            return False
    except aiohttp.ClientConnectorCertificateError as e:
        # Some ATS platforms de-provision a tenant subdomain's routing/
        # certificate before the DNS record itself is flushed — the
        # connection succeeds but presents a certificate for a DIFFERENT
        # hostname than the one requested. That specific mismatch is a
        # real structural "this tenant no longer exists here" signal.
        # An expired cert, a self-signed cert, or an untrusted CA on an
        # otherwise-live site is NOT the same thing — those stay
        # ambiguous, same as any other unexpected error.
        underlying = str(getattr(e, "certificate_error", None) or e)
        if re.search(r"hostname\s*mismatch|doesn.?t\s*match|not\s*valid\s*for", underlying, re.I):
            return True
        return None
    except aiohttp.ClientConnectorError as e:
        os_err = getattr(e, "os_error", None)
        if isinstance(os_err, socket.gaierror):
            return True  # domain doesn't resolve at all
        return None
    except Exception:
        return None


async def verify_archive_iii_row(session: aiohttp.ClientSession, row: dict) -> bool:
    """DEAD only if BOTH the specific career_page_url AND the company's
    root website_url independently confirm dead — a career page 404 alone
    usually just means the page moved on an otherwise-live site, which is
    not the same as the company disappearing (see module docstring)."""
    career_dead = await _url_confirmed_dead(session, row["career_page_url"])
    if career_dead is not True:
        return False  # career page still resolves (or check was inconclusive) — keep
    website_dead = await _url_confirmed_dead(session, row["website_url"])
    return website_dead is True


# ── Supabase I/O (async) ──────────────────────────────────────────────

# Same known-flaky-Supabase-gateway class main.py/supabase_handler.py's
# _get() already retries against (see supabase_handler.py's _RETRYABLE_STATUSES
# comment): 401/403 can be a cold-start/gateway hiccup, not just a bad key —
# a genuinely bad key just keeps failing and we find out after MAX_HTTP_RETRIES
# anyway. Confirmed real 2026-08: shard 1/20 hard-crashed the whole shard on a
# one-off 401 with zero retry, exactly the failure mode main.py's own
# SupabaseFetchError docstring describes happening to it before this fix.
_RETRYABLE_STATUSES = {401, 403, 429, 500, 502, 503, 504}
# 4 -> 6 (2026-08): a COUNT query failure blocks the ENTIRE shard from doing
# any work at all (see _get_count) — worth spending more retry budget on
# than an ordinary per-row check would need. _MAX_RETRY_WAIT caps the
# exponential backoff so this doesn't itself balloon into a multi-minute
# wait per attempt (2^5 * 2.0 would be 64s uncapped).
MAX_HTTP_RETRIES = 6
_RETRY_BASE_DELAY = 2.0  # seconds
_MAX_RETRY_WAIT = 20.0   # seconds


def _backoff_wait(attempt: int) -> float:
    return min(_RETRY_BASE_DELAY * (2 ** attempt), _MAX_RETRY_WAIT) + random.uniform(0, 1)


class VerificationFetchError(Exception):
    """Raised when a Supabase GET fails after every retry — let this
    propagate and crash the shard loudly (non-zero exit) rather than
    silently returning an empty/partial row list that would look
    identical to "this table/shard legitimately has nothing"."""
    pass


# Split into connect/read phases (not just one bare "total") so a stalled
# DNS/TLS handshake to Supabase's own gateway is distinguished from a slow
# response body — and, critically, so a hang actually gets cut off at a
# predictable ~20s instead of silently running past whatever "total" turns
# out to mean for a given hang location. Confirmed real 2026-08: a first
# attempt against Supabase once took ~148s to fail even with total=60s set,
# consistent with a cold-start/gateway-wake stall somewhere connect-phase
# aiohttp's own total timeout doesn't fully bound.
_GET_TIMEOUT = aiohttp.ClientTimeout(total=45, connect=20, sock_connect=20, sock_read=30)


async def _get_with_retries(session: aiohttp.ClientSession, url: str, headers: dict,
                             params: dict | None = None) -> list:
    last_error = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            async with session.get(url, headers=headers, params=params,
                                    timeout=_GET_TIMEOUT) as r:
                if r.status in _RETRYABLE_STATUSES and attempt < MAX_HTTP_RETRIES - 1:
                    wait = _backoff_wait(attempt)
                    log.warning(f"  GET {url} got HTTP {r.status}, retrying in {wait:.1f}s "
                                f"(attempt {attempt + 1}/{MAX_HTTP_RETRIES})")
                    await asyncio.sleep(wait)
                    continue
                r.raise_for_status()
                return await r.json()
        except Exception as e:
            # str(e) is EMPTY for several common failures here (e.g.
            # asyncio.TimeoutError, aiohttp.ServerTimeoutError) — always
            # include the exception's own type name too, or the log just
            # reads "failed ()" with zero diagnostic value (exactly what
            # happened on the bamboohr/shard-9 failure this was added for).
            last_error = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            if attempt < MAX_HTTP_RETRIES - 1:
                wait = _backoff_wait(attempt)
                log.warning(f"  GET {url} failed ({last_error}), retrying in {wait:.1f}s "
                            f"(attempt {attempt + 1}/{MAX_HTTP_RETRIES})")
                await asyncio.sleep(wait)
    raise VerificationFetchError(f"GET {url} failed after {MAX_HTTP_RETRIES} attempts: {last_error}")


async def _get_count(session: aiohttp.ClientSession, table: str, headers: dict, params: dict) -> int:
    """Cheap COUNT via Prefer: count=exact on a single-row request — reads
    the real total straight out of the Content-Range response header
    (e.g. '0-0/186494'), no need to pull any actual rows to get it."""
    url = f"{node.SUPABASE_URL}/rest/v1/{table}"
    count_headers = {**headers, "Range": "0-0", "Prefer": "count=exact"}
    last_error = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            async with session.get(url, headers=count_headers, params=params,
                                    timeout=_GET_TIMEOUT) as r:
                if r.status in _RETRYABLE_STATUSES and attempt < MAX_HTTP_RETRIES - 1:
                    wait = _backoff_wait(attempt)
                    await asyncio.sleep(wait)
                    continue
                r.raise_for_status()
                content_range = r.headers.get("Content-Range", "")
                return int(content_range.split("/")[-1])
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            if attempt < MAX_HTTP_RETRIES - 1:
                wait = _backoff_wait(attempt)
                await asyncio.sleep(wait)
    raise VerificationFetchError(f"COUNT {url} failed after {MAX_HTTP_RETRIES} attempts: {last_error}")


async def fetch_rows(session: aiohttp.ClientSession, table: str, select: str,
                      ats_filter: str | None, limit: int | None,
                      shard_index: int | None = None, shard_count: int | None = None) -> list[dict]:
    """Fetches rows ordered by id, optionally restricted to ONE contiguous
    shard's row range (computed server-side from a COUNT query — see
    _get_count). This is deliberately NOT "fetch everything, then keep only
    this shard's rows in Python": confirmed real 2026-08 — with the old
    fetch-everything approach, N parallel shards each independently
    downloaded the ENTIRE table (every shard re-paginating through all
    ~186K rows just to discard (N-1)/N of them), so N shards put N times
    the load on Supabase's REST endpoint that the actual work needed. That
    matches exactly what was observed: GETs and DELETEs alike timing out
    ("increased errors" on Supabase's own side) under a 20-shard run. This
    version has each shard fetch ONLY its own ~1/N slice directly, so total
    aggregate row-fetch volume across all shards is ~1x the table, not Nx.
    `order=id.asc` is required for this to be correct at all — without a
    stable order, two separate Range-paginated requests (whether within
    one shard's own pagination, or across different shards' independent
    slices) aren't guaranteed to return consistent results, which could
    silently skip or duplicate rows."""
    rows = []
    page_size = 1000
    headers = {"apikey": node.SUPABASE_KEY, "Authorization": f"Bearer {node.SUPABASE_KEY}"}
    params = {"select": select, "order": "id.asc"}
    if ats_filter:
        params["ats"] = f"eq.{ats_filter}"

    start_offset = 0
    end_offset = None  # exclusive upper bound on absolute row position; None = no shard cap
    if shard_count:
        total = await _get_count(session, table, headers, params)
        shard_size = -(-total // shard_count)  # ceil division
        start_offset = shard_index * shard_size
        end_offset = min(start_offset + shard_size, total)
        if start_offset >= total:
            return []  # more shards than rows — this shard legitimately gets nothing

    offset = start_offset
    while True:
        if limit is not None and len(rows) >= limit:
            rows = rows[:limit]
            break
        page_end = offset + page_size - 1
        if end_offset is not None:
            page_end = min(page_end, end_offset - 1)
            if offset > page_end:
                break
        page_headers = {**headers, "Range": f"{offset}-{page_end}"}
        batch = await _get_with_retries(session, f"{node.SUPABASE_URL}/rest/v1/{table}",
                                         page_headers, params)
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < (page_end - offset + 1):
            break
        offset = page_end + 1
        if end_offset is not None and offset >= end_offset:
            break
    return rows


_DELETE_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=15, sock_connect=15, sock_read=20)


async def delete_row(session: aiohttp.ClientSession, table: str, row_id: int) -> bool:
    headers = {"apikey": node.SUPABASE_KEY, "Authorization": f"Bearer {node.SUPABASE_KEY}"}
    url = f"{node.SUPABASE_URL}/rest/v1/{table}"
    last_error = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            async with session.delete(url, headers=headers, params={"id": f"eq.{row_id}"},
                                       timeout=_DELETE_TIMEOUT) as r:
                if r.status in _RETRYABLE_STATUSES and attempt < MAX_HTTP_RETRIES - 1:
                    wait = _backoff_wait(attempt)
                    await asyncio.sleep(wait)
                    continue
                r.raise_for_status()
                return True
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            if attempt < MAX_HTTP_RETRIES - 1:
                wait = _backoff_wait(attempt)
                await asyncio.sleep(wait)
    log.error(f"    Failed to delete row id={row_id} from {table} after {MAX_HTTP_RETRIES} attempts: {last_error}")
    return False


# ── orchestration ───────────────────────────────────────────────────

async def verify_archive_ii_row(session: aiohttp.ClientSession, row: dict, dry_run: bool,
                                 sem: asyncio.Semaphore, executor: concurrent.futures.ThreadPoolExecutor,
                                 counts: dict, lock: asyncio.Lock) -> None:
    ats, slug = row["ats"], row["slug"]
    verifier = ARCHIVE_II_VERIFIERS[ats]
    async with sem:
        try:
            is_live = await verifier(session, slug)
        except Exception as e:
            async with lock:
                counts["unverified"] += 1
            log.debug(f"    {ats}/{slug}: check failed ({e}) — leaving in place")
            return

        if not is_live:
            if dry_run:
                async with lock:
                    counts["dead"] += 1
                log.info(f"    {ats}/{slug}: DEAD — would delete (report-only)")
                return
            ok = await delete_row(session, node.ARCHIVE_I_TABLE, row["id"])
            async with lock:
                counts["dead" if ok else "unverified"] += 1
            if ok:
                log.info(f"    {ats}/{slug}: DEAD — deleted")
            return

        # Live — best-effort active/empty split via the real production
        # scraper (reporting only, never affects the keep decision above).
        loop = asyncio.get_running_loop()
        try:
            jobs = await loop.run_in_executor(executor, scrape_board, ats, slug)
        except Exception:
            jobs = []
        async with lock:
            counts["active" if jobs else "empty"] += 1


async def verify_archive_iii_row_task(session: aiohttp.ClientSession, row: dict, dry_run: bool,
                                       sem: asyncio.Semaphore, counts: dict, lock: asyncio.Lock) -> None:
    # The delete stays INSIDE the semaphore, same as verify_archive_ii_row —
    # otherwise a batch of rows all confirming dead at once could fire every
    # delete concurrently, uncapped by --concurrency.
    async with sem:
        try:
            is_dead = await verify_archive_iii_row(session, row)
        except Exception as e:
            async with lock:
                counts["unverified"] += 1
            log.debug(f"    {row['website_url']}: check failed ({e}) — leaving in place")
            return

        if not is_dead:
            async with lock:
                counts["live"] += 1
            return

        if dry_run:
            async with lock:
                counts["dead"] += 1
            log.info(f"    {row['website_url']}: DEAD — would delete (report-only)")
            return

        ok = await delete_row(session, node.ARCHIVE_II_TABLE, row["id"])
        async with lock:
            counts["dead" if ok else "unverified"] += 1
        if ok:
            log.info(f"    {row['website_url']}: DEAD — deleted")


async def _run_progress(counts: dict, total: int, label: str):
    last = -1
    while True:
        await asyncio.sleep(5)
        done = sum(counts.values())
        if done != last:
            log.info(f"  [{label}] progress: {done}/{total} ({counts})")
            last = done
        if done >= total:
            return


def _print_summary(label: str, checked: int, counts: dict, dry_run: bool):
    # "unverified" already includes both rows whose live check was genuinely
    # inconclusive AND rows on a platform with no safe not-found signal at
    # all (see _UNVERIFIABLE_ATS) — every row in the table lands in exactly
    # one bucket below, so these always sum to the table's true total; no
    # row is ever silently left out of the count.
    total_rows = sum(counts.values())
    retained = total_rows - counts.get("dead", 0)
    print("\n" + "=" * 70)
    print(f"VERIFICATION SUMMARY — {label}")
    print("=" * 70)
    print(f"  Total rows:                 {total_rows}")
    print(f"  Checked (live network call):{checked}")
    for key, display in (("active", "Active (>=1 open job)"),
                          ("empty", "Empty (real board, 0 jobs)"),
                          ("live", "Live (kept)"),
                          ("dead", f"Dead ({'would be ' if dry_run else ''}removed)"),
                          ("unverified", "Unverified (kept — incl. unverifiable platforms)")):
        if key in counts:
            print(f"  {display + ':':<45} {counts[key]}")
    print(f"  Total retained (everything NOT deleted):    {retained}")
    print("=" * 70)


def _empty_counts_ii() -> dict:
    return {"active": 0, "empty": 0, "dead": 0, "unverified": 0}


def _empty_counts_iii() -> dict:
    return {"live": 0, "dead": 0, "unverified": 0}


async def _stagger_shard_start(shard_index: int | None, shard_count: int | None) -> None:
    """GitHub Actions matrix jobs for the same workflow typically start
    within a few seconds of each other — with N shards, that's N COUNT
    queries (see _get_count) all landing on Supabase in the same instant,
    right as this run's very first request. Spreading each shard's first
    request out by a small, index-proportional delay turns that burst into
    a rolling ramp instead, cutting the odds of the exact bootstrapping
    request (which blocks the WHOLE shard if it fails — see fetch_rows)
    getting caught in a self-inflicted thundering herd."""
    if not shard_count or shard_index is None:
        return
    delay = shard_index * 1.5 + random.uniform(0, 1)
    if delay > 0:
        await asyncio.sleep(delay)


async def run_archive_ii(ats_filter: str | None, limit: int | None, dry_run: bool,
                          concurrency: int, shard_index: int | None, shard_count: int | None) -> dict:
    sem = asyncio.Semaphore(concurrency)
    counts = _empty_counts_ii()
    lock = asyncio.Lock()

    if ats_filter and ats_filter in _UNVERIFIABLE_ATS:
        log.warning(f"  '{ats_filter}' has no confirmed-safe not-found signal — nothing to verify, "
                    f"see _UNVERIFIABLE_ATS in this file's module docstring for why.")
        return counts

    async with aiohttp.ClientSession(connector=_new_connector(concurrency)) as session:
        shard_note = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
        log.info(f"── Verifying archive_ii{f' ({ats_filter})' if ats_filter else ' (all verifiable platforms)'}{shard_note} ──")
        await _stagger_shard_start(shard_index, shard_count)
        # Sharded server-side (see fetch_rows) — each shard fetches ONLY its
        # own ~1/N slice, not the whole table filtered down client-side.
        all_rows = await fetch_rows(session, node.ARCHIVE_I_TABLE, "id,ats,slug", ats_filter, None,
                                     shard_index, shard_count)

        verifiable_rows = [r for r in all_rows if r["ats"] in ARCHIVE_II_VERIFIERS]
        skipped_rows = [r for r in all_rows if r["ats"] not in ARCHIVE_II_VERIFIERS]
        counts["unverified"] += len(skipped_rows)

        if limit is not None:
            verifiable_rows = verifiable_rows[:limit]
        checked = len(verifiable_rows)

        if skipped_rows and not ats_filter:
            skipped_by_ats = {}
            for r in skipped_rows:
                skipped_by_ats[r["ats"]] = skipped_by_ats.get(r["ats"], 0) + 1
            log.info(f"  {len(skipped_rows)} rows on unverifiable platforms (kept, counted as unverified): "
                     f"{dict(sorted(skipped_by_ats.items(), key=lambda kv: -kv[1]))}")

        log.info(f"  {checked} rows to check" + (" (report-only)" if dry_run else " (EXECUTE MODE — confirmed-dead rows WILL be deleted)"))
        if not checked:
            _print_summary("archive_ii" + shard_note, checked, counts, dry_run)
            return counts

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            tasks = [verify_archive_ii_row(session, row, dry_run, sem, executor, counts, lock)
                     for row in verifiable_rows]
            progress_task = asyncio.create_task(_run_progress(counts, checked + len(skipped_rows), "archive_ii"))
            await asyncio.gather(*tasks)
            progress_task.cancel()

    _print_summary("archive_ii" + shard_note, checked, counts, dry_run)
    return counts


async def run_archive_iii(limit: int | None, dry_run: bool, concurrency: int,
                           shard_index: int | None, shard_count: int | None) -> dict:
    sem = asyncio.Semaphore(concurrency)
    counts = _empty_counts_iii()
    lock = asyncio.Lock()

    async with aiohttp.ClientSession(connector=_new_connector(concurrency)) as session:
        shard_note = f" [shard {shard_index}/{shard_count}]" if shard_count else ""
        log.info(f"── Verifying archive_iii (career pages){shard_note} ──")
        await _stagger_shard_start(shard_index, shard_count)
        # Sharded server-side (see fetch_rows) — each shard fetches ONLY its
        # own ~1/N slice, not the whole table filtered down client-side.
        rows = await fetch_rows(session, node.ARCHIVE_II_TABLE, "id,career_page_url,website_url",
                                 None, limit, shard_index, shard_count)
        total = len(rows)

        log.info(f"  {total} rows to check" + (" (report-only)" if dry_run else " (EXECUTE MODE — confirmed-dead rows WILL be deleted)"))
        if not total:
            _print_summary("archive_iii" + shard_note, total, counts, dry_run)
            return counts

        tasks = [verify_archive_iii_row_task(session, row, dry_run, sem, counts, lock) for row in rows]
        progress_task = asyncio.create_task(_run_progress(counts, total, "archive_iii"))
        await asyncio.gather(*tasks)
        progress_task.cancel()

    _print_summary("archive_iii" + shard_note, total, counts, dry_run)
    return counts


def _summarize_dir(directory: str) -> None:
    """finalize-style aggregation — reads every *.json summary a shard
    wrote (via --summary-out) out of `directory` and prints ONE combined
    report, the same shape run_archive_ii/run_archive_iii print on their
    own, just totaled across every shard. Mirrors Main/main.py's
    `--finalize` step running once after every shard has finished."""
    paths = sorted(glob.glob(os.path.join(directory, "*.json")))
    if not paths:
        log.error(f"No *.json shard summaries found in {directory!r} — nothing to combine.")
        sys.exit(1)

    combined_ii = _empty_counts_ii()
    combined_iii = _empty_counts_iii()
    dry_run = None
    shards_seen = 0

    for p in paths:
        with open(p) as f:
            data = json.load(f)
        shards_seen += 1
        if dry_run is None:
            dry_run = data.get("dry_run", True)
        for key, val in (data.get("archive_ii") or {}).items():
            combined_ii[key] = combined_ii.get(key, 0) + val
        for key, val in (data.get("archive_iii") or {}).items():
            combined_iii[key] = combined_iii.get(key, 0) + val

    log.info(f"Combined {shards_seen} shard summaries from {directory}")
    if any(combined_ii.values()):
        checked_ii = combined_ii["active"] + combined_ii["empty"] + combined_ii["dead"]
        _print_summary("archive_ii — ALL SHARDS COMBINED", checked_ii, combined_ii, bool(dry_run))
    if any(combined_iii.values()):
        checked_iii = combined_iii["live"] + combined_iii["dead"]
        _print_summary("archive_iii — ALL SHARDS COMBINED", checked_iii, combined_iii, bool(dry_run))


async def _fetch_pair_set(session: aiohttp.ClientSession, table: str) -> set[tuple[str, str]]:
    rows = await fetch_rows(session, table, "ats,slug", None, None)
    return {(r["ats"], r["slug"]) for r in rows}


async def compare_slug_registry() -> None:
    """OBSOLETE as of the 2026-08 restructure (see node.py's module-level
    comment: 'the OLD archive_ii (an ATS-match staging/quarantine table
    that a separate verify step promoted into slug_registry) is GONE —
    ATS hits now write directly to ARCHIVE_I_TABLE with no intermediate
    verify/promote step'). This function's whole premise — a separate
    quarantine table vs. a production registry table, comparing which
    (ats,slug) pairs have been 'promoted' from one to the other — no
    longer applies: there is only ONE table (archive_i) now. Left in
    place (rather than deleted) as a clearly-marked no-op instead of
    silently 'fixing' its two stale table-name references (node.
    STAGING_TABLE, literal 'slug_registry') into comparing archive_i
    against itself, which would run without error but produce a
    meaningless always-100%-overlap report — worse than refusing to run,
    since it looks like real output. Rewriting this into something
    meaningful (e.g. comparing archive_i against archive_ii's current,
    unrelated in-house/unsupported-career-page role) is a separate task,
    not a same-shape fix."""
    log.error("compare_slug_registry is obsolete since the 2026-08 archive_ii "
              "restructure (no more separate staging/quarantine table to compare "
              "against production) — see this function's docstring. Not running.")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Verify archive_ii/archive_iii rows against live ATS boards/career pages (async). "
                     "Report-only by default — pass --execute to actually delete confirmed-dead rows.")
    parser.add_argument("--table", choices=["archive_ii", "archive_iii", "both"], default="both")
    parser.add_argument("--ats", default=None,
                         help="archive_ii only — restrict to one ATS platform (e.g. greenhouse)")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--execute", action="store_true",
                         help="Actually delete confirmed-dead rows. Without this, always report-only.")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--shard-index", type=int, default=None,
                         help="This shard's index (0-based) — requires --shard-count")
    parser.add_argument("--shard-count", type=int, default=None,
                         help="Total shards — each processes ~1/N of the rows")
    parser.add_argument("--summary-out", default=None,
                         help="Write this run's counts as JSON to this path (for --summarize to combine later)")
    parser.add_argument("--summarize", default=None,
                         help="Skip verification entirely — just combine every *.json in this directory "
                              "(written by earlier --summary-out runs) into one final report")
    parser.add_argument("--compare-slug-registry", action="store_true",
                         help="Skip verification entirely — just compare archive_ii's (ats,slug) pairs "
                              "against slug_registry's and report overlap/unique-to-each counts")
    args = parser.parse_args()

    if args.summarize:
        _summarize_dir(args.summarize)
        return

    if args.compare_slug_registry:
        if not node.SUPABASE_URL or not node.SUPABASE_KEY:
            log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot compare.")
            sys.exit(1)
        asyncio.run(compare_slug_registry())
        return

    if (args.shard_index is None) != (args.shard_count is None):
        parser.error("--shard-index and --shard-count must be given together")

    if not node.SUPABASE_URL or not node.SUPABASE_KEY:
        log.error("SUPABASE_URL/SUPABASE_KEY not set — cannot verify.")
        sys.exit(1)

    dry_run = not args.execute

    async def _run_all():
        summary = {"dry_run": dry_run}
        if args.table in ("archive_ii", "both"):
            summary["archive_ii"] = await run_archive_ii(
                args.ats, args.limit, dry_run, args.concurrency, args.shard_index, args.shard_count)
        if args.table in ("archive_iii", "both"):
            summary["archive_iii"] = await run_archive_iii(
                args.limit, dry_run, args.concurrency, args.shard_index, args.shard_count)
        if args.summary_out:
            os.makedirs(os.path.dirname(args.summary_out) or ".", exist_ok=True)
            with open(args.summary_out, "w") as f:
                json.dump(summary, f)
            log.info(f"Wrote shard summary to {args.summary_out}")

    asyncio.run(_run_all())


if __name__ == "__main__":
    main()
