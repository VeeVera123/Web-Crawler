"""
GITHUB ORG SEED (2026-08, new) — enumerates every GitHub organization and
pulls its public "website" field, writing a companies CSV in the SAME
{name, domain, country} shape fetch_pdl_companies_with_domain() already
reads in class_a_probe.py. This is a genuinely DIFFERENT discovery channel
from PDL — a different population of companies entirely (ones that
maintain a public GitHub org), not a bigger scoop of the same list — see
the "what other mega list" research this was born from.

THIS SCRIPT ONLY SEEDS. IT DOES NOT CRAWL. That split is deliberate and is
the opposite choice from host_crawl_v2.py, which collapsed seed+crawl into
one pass. The reason is that GitHub's API is a METERED, PER-TOKEN budget
(5,000 REST requests/hour, 5,000 GraphQL points/hour) — every concurrent
shard sharing one token would fight over that ONE budget, not multiply it,
which is exactly the opposite of Common Crawl's Parquet files (a free,
unmetered download host_crawl_v2.py can shard freely). Keeping this a
single seed pass means the metered part only ever happens once, and the
actual crawl-and-detect step (visiting each discovered company's own
website — NOT a GitHub API call, no rate limit involved) is handed off to
class_a_probe.py itself afterward, sharded exactly like the existing PDL
run already is. See github_org_probe.yml.

HOW ORG ENUMERATION WORKS: GET /organizations?since={id} returns
organizations in ascending ID order, 100 per page — this is the same
technique long used to enumerate every GitHub user account (orgs and
users share one account-ID sequence). Walking it from a starting ID up to
the current frontier (the highest org ID that exists right now) is the
ONLY way to get a genuinely COMPLETE list — there is no "list every org"
search endpoint. --id-ceiling is a deliberately generous, unverified upper
bound on where that frontier currently sits (GitHub doesn't publish this
number). Running past the real frontier just gets an empty page back
immediately — a handful of wasted requests, not a real cost — so
overestimating is free while underestimating would silently truncate real
coverage. Same reasoning class_a_probe.py's DEFAULT_COUNTRIES comment
already uses for the identical trade-off.

WHY GRAPHQL FOR THE PROFILE LOOKUP, NOT REST: GET /orgs/{login} (REST)
does carry the website field (as `blog`), but costs one full request per
org — at 5,000 req/hour that's an unworkable bottleneck across millions
of orgs. GitHub's GraphQL API instead prices a query at
ceil(unique_resource_lookups / 100), so aliasing 100 organizations into
ONE GraphQL call costs ~1 point instead of 100 separate REST calls —
confirmed directly against GitHub's own published schema
(octokit/graphql-schema: Organization.websiteUrl/.email/.name/.login all
verified to exist as real fields, not assumed) and its documented cost
formula. At the same 5,000-points/hour cap that's up to ~500,000 org
profile lookups/hour instead of 5,000 — the difference between "feasible"
and "not," on the exact same free token.

ONE JOB, NOT A MATRIX: unlike every other seed/crawl script in this
project, this one deliberately does NOT run as a parallel GitHub Actions
matrix, because the rate limit it's up against is a single shared budget
per token — concurrent shards on one token would just divide (or collide
over) that budget, not multiply it. --shard-index/--shard-count still
exist below, but for splitting the ID range across SEPARATE, SEQUENTIAL
runs (resuming across days, or across more than one personal token if the
user sets that up) — not for concurrent parallelism against one token.

FILTERING JUNK (the actual point of this script, not just enumeration):
  - Skip orgs with no websiteUrl at all — most orgs. The end-of-run log
    reports the real fill rate; this project doesn't pretend every org
    fills this field in.
  - Skip a small denylist of link-in-bio/social-profile hosts that show
    up constantly in this field but are never a real company site worth
    crawling (twitter/x, linkedin, facebook, instagram, linktr.ee,
    bio.link, t.me, discord, youtube, github.com/github.io itself,
    medium, substack, npm, pypi). These are DOMAINS, not ATS platforms —
    a seed-quality problem, unrelated to discovery.py's ATS-slug
    SKIP_SLUGS denylist, which is why this list lives here instead.
  - Basic shape validation (real-looking domain, has a dot, not a bare
    IP literal, not absurdly long) — the same defensive parsing
    class_a_probe.py already applies to PDL's own domain column, applied
    here too since a free-text profile field is at least as likely to
    carry junk as a spreadsheet column was.

Usage:
    pip install aiohttp python-dotenv
    export GITHUB_PAT=ghp_xxx   # or GH_TOKEN / GITHUB_TOKEN — see module docstring
    python github_org_seed.py --id-start 1 --id-ceiling 300000000
    python github_org_seed.py --shard-index 0 --shard-count 4   # sequential chunks, see docstring
"""
import argparse
import asyncio
import csv
import logging
import os
import re
import sys
import time
from collections import Counter
from urllib.parse import urlparse

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("github_org_seed")

# Accept whichever of these is set — GITHUB_PAT is the recommended name
# for a real personal token (5,000/hour, the full budget); GH_TOKEN /
# GITHUB_TOKEN are accepted too since GITHUB_TOKEN is the name GitHub
# Actions' own ambient token uses (works, but see the workflow file for
# why that ambient token's budget is smaller than a real PAT's).
GITHUB_TOKEN = os.environ.get("GITHUB_PAT") or os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN", "")

REST_API = "https://api.github.com"
GRAPHQL_API = "https://api.github.com/graphql"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=8)

ORG_PAGE_SIZE = 100        # GitHub's own max page size for /organizations
GRAPHQL_BATCH_SIZE = 100   # orgs aliased per GraphQL call — see module docstring cost math

# Deliberately generous, unverified upper bound on the current org/user ID
# frontier — see module docstring for why overestimating is free.
DEFAULT_ID_CEILING = 300_000_000

OUTPUT_FIELDNAMES = ["name", "domain", "country"]  # matches fetch_pdl_companies_with_domain()'s
# read shape exactly — see class_a_probe.py. country is always left blank
# (GitHub org profiles don't reliably carry one) — this is why
# github_org_probe.yml runs class_a_probe.py with --all-countries: a
# country filter would otherwise gut nearly this entire seed file.

# Real domains, never a real company website even though they show up
# constantly in the free-text website/blog field — see module docstring.
JUNK_WEBSITE_DOMAINS = {
    "github.com", "github.io",
    "twitter.com", "x.com",
    "linkedin.com", "facebook.com", "instagram.com",
    "linktr.ee", "bio.link", "lnk.bio",
    "t.me", "discord.gg", "discord.com",
    "youtube.com", "youtu.be",
    "medium.com", "substack.com",
    "npmjs.com", "pypi.org",
}


def _clean_website_domain(raw: str) -> str | None:
    """Same spirit as class_a_probe.py's PDL domain cleanup — a free-text
    profile field needs at least as much defensive parsing as a
    spreadsheet column did. Returns None for anything not worth crawling."""
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = "https://" + raw
    try:
        host = (urlparse(raw).hostname or "").lower().strip()
    except ValueError:
        return None
    if not host or "." not in host or len(host) > 253:
        return None
    if re.fullmatch(r"[\d.]+", host):  # bare IPv4 literal — never a real company domain
        return None
    bare = host[4:] if host.startswith("www.") else host
    if any(bare == d or bare.endswith("." + d) for d in JUNK_WEBSITE_DOMAINS):
        return None
    return host


async def _enumerate_org_logins(session: aiohttp.ClientSession, since_start: int, id_ceiling: int,
                                 stats: Counter):
    """Yields (logins, last_id_in_page) — walking /organizations forward
    from since_start. Stops at id_ceiling or the first empty page (the
    real current frontier — see module docstring). last_id_in_page is
    surfaced so the caller can log a resume point if the time budget cuts
    the walk short."""
    since = since_start
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    while since < id_ceiling:
        stats["rest_requests"] += 1
        try:
            async with session.get(f"{REST_API}/organizations",
                                    params={"since": since, "per_page": ORG_PAGE_SIZE},
                                    headers=headers, timeout=REQUEST_TIMEOUT) as r:
                if r.status in (403, 429):
                    reset = r.headers.get("X-RateLimit-Reset")
                    wait_s = max(int(reset) - int(time.time()), 5) if reset else 60
                    log.warning(f"  rate limited ({r.status}) at since={since} — sleeping {wait_s}s")
                    await asyncio.sleep(wait_s)
                    continue
                r.raise_for_status()
                page = await r.json()
        except Exception as e:
            stats["rest_errors"] += 1
            log.warning(f"  /organizations?since={since} failed: {e} — retrying in 5s")
            await asyncio.sleep(5)
            continue
        if not page:
            log.info(f"  reached the current org frontier at since={since} (empty page — "
                     f"everything above this ID is unassigned as of this run)")
            return
        logins = [org["login"] for org in page if org.get("login")]
        last_id = page[-1]["id"]
        yield logins, last_id
        since = last_id


def _build_graphql_query(logins: list[str]) -> str:
    parts = []
    for i, login in enumerate(logins):
        safe = login.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'o{i}: organization(login: "{safe}") {{ login name email websiteUrl }}')
    return "query {\n" + "\n".join(parts) + "\n}"


async def _fetch_org_profiles(session: aiohttp.ClientSession, logins: list[str], stats: Counter) -> list[dict]:
    """One GraphQL call for up to GRAPHQL_BATCH_SIZE orgs — see module
    docstring for why this is ~100x cheaper than one REST call per org.
    A handful of individual aliases can legitimately come back null (an
    org deleted/renamed between the REST enumeration and this call, or
    blocked) without the whole batch failing."""
    if not logins:
        return []
    query = _build_graphql_query(logins)
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    for attempt in range(3):
        if attempt:
            await asyncio.sleep(2 * attempt)
        try:
            async with session.post(GRAPHQL_API, json={"query": query}, headers=headers,
                                     timeout=REQUEST_TIMEOUT) as r:
                if r.status in (403, 429):
                    reset = r.headers.get("X-RateLimit-Reset")
                    wait_s = max(int(reset) - int(time.time()), 5) if reset else 60
                    log.warning(f"  GraphQL rate limited ({r.status}) — sleeping {wait_s}s")
                    await asyncio.sleep(wait_s)
                    continue
                body = await r.json()
        except Exception as e:
            stats["graphql_errors"] += 1
            log.warning(f"  GraphQL batch of {len(logins)} failed (attempt {attempt + 1}/3): {e}")
            continue
        stats["graphql_requests"] += 1
        data = body.get("data") or {}
        return [v for v in data.values() if v]
    stats["graphql_batches_dropped"] += 1
    log.error(f"  GraphQL batch of {len(logins)} orgs failed after retries — this batch's "
              f"orgs are skipped (not retried further), not a fatal error for the run")
    return []


async def run_seed(id_start: int, id_ceiling: int, output_path: str, time_budget_minutes: int) -> None:
    if not GITHUB_TOKEN:
        log.error("No GitHub token set (GITHUB_PAT / GH_TOKEN / GITHUB_TOKEN) — unauthenticated "
                   "calls are capped at 60 req/hour, unworkable at this scale. Set one and re-run.")
        return

    log.info(f"── GitHub org seed: ids [{id_start}, {id_ceiling}) ──")
    log.info(f"  time_budget={time_budget_minutes}min")
    time_budget_seconds = time_budget_minutes * 60
    start = time.monotonic()

    stats = Counter()
    written = 0
    buffer: list[str] = []
    last_since = id_start
    time_budget_hit = False

    connector = aiohttp.TCPConnector(limit=20)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES)
        writer.writeheader()

        async def flush(batch: list[str]):
            nonlocal written
            if not batch:
                return
            profiles = await _fetch_org_profiles(session, batch, stats)
            for org in profiles:
                domain = _clean_website_domain(org.get("websiteUrl") or "")
                if not domain:
                    stats["no_usable_website"] += 1
                    continue
                name = (org.get("name") or org.get("login") or "").strip()
                if not name:
                    continue
                writer.writerow({"name": name, "domain": domain, "country": ""})
                written += 1
            stats["orgs_checked"] += len(batch)

        async with aiohttp.ClientSession(connector=connector) as session:
            async for logins, last_id in _enumerate_org_logins(session, id_start, id_ceiling, stats):
                last_since = last_id
                buffer.extend(logins)
                while len(buffer) >= GRAPHQL_BATCH_SIZE:
                    batch, buffer = buffer[:GRAPHQL_BATCH_SIZE], buffer[GRAPHQL_BATCH_SIZE:]
                    await flush(batch)

                if stats["orgs_checked"] and stats["orgs_checked"] % 5000 < GRAPHQL_BATCH_SIZE:
                    elapsed = time.monotonic() - start
                    rate = stats["orgs_checked"] / elapsed if elapsed > 0 else 0
                    log.info(f"  {stats['orgs_checked']} orgs checked, {written} with a usable "
                             f"website ({written / max(stats['orgs_checked'], 1) * 100:.1f}%), "
                             f"{elapsed:.0f}s elapsed, {rate:.1f} orgs/sec, since={last_since}")

                if time.monotonic() - start >= time_budget_seconds:
                    time_budget_hit = True
                    log.warning(f"  time budget ({time_budget_minutes}min) reached at "
                                f"since={last_since} — stopping here. Resume this exact point "
                                f"later with --id-start {last_since}.")
                    break

            # Always flush whatever's left in the buffer, time-cut or not —
            # these logins were already pulled from a REST page already
            # paid for; the only remaining cost to profile-query them is
            # one more GraphQL call, so dropping them on a time cutoff
            # would throw away already-fetched data for no reason.
            await flush(buffer)

    elapsed = time.monotonic() - start
    log.info(f"── done{' (time budget cut short)' if time_budget_hit else ''}: "
             f"{stats['orgs_checked']} orgs checked, {written} written to '{output_path}' "
             f"({written / max(stats['orgs_checked'], 1) * 100:.1f}% had a usable, non-junk "
             f"website), {elapsed:.0f}s ──")
    if stats["rest_errors"] or stats["graphql_errors"] or stats["graphql_batches_dropped"]:
        log.info(f"  errors: rest_errors={stats['rest_errors']} graphql_errors={stats['graphql_errors']} "
                 f"graphql_batches_dropped={stats['graphql_batches_dropped']} "
                 f"({stats['graphql_batches_dropped'] * GRAPHQL_BATCH_SIZE} orgs' worth skipped)")


def main():
    parser = argparse.ArgumentParser(description="GitHub org seed — enumerate orgs, pull their "
                                                  "website field, write a class_a_probe-compatible CSV")
    parser.add_argument("--id-start", type=int, default=1,
                         help="Lowest org ID to start enumerating from (default 1)")
    parser.add_argument("--id-ceiling", type=int, default=DEFAULT_ID_CEILING,
                         help=f"Highest org ID to walk up to — deliberately generous, see module "
                              f"docstring (default {DEFAULT_ID_CEILING:,})")
    parser.add_argument("--shard-index", type=int, default=None,
                         help="This run's shard index (0-based) — splits [id-start, id-ceiling) "
                              "into --shard-count SEQUENTIAL chunks (see module docstring for why "
                              "this is NOT meant for concurrent parallelism against one token)")
    parser.add_argument("--shard-count", type=int, default=None,
                         help="Total number of sequential chunks (must be passed with --shard-index)")
    parser.add_argument("--time-budget-minutes", type=int,
                         default=int(os.environ.get("SEED_TIME_BUDGET_MINUTES", "330")),
                         help="Graceful stop after this many minutes (default 330, env "
                              "SEED_TIME_BUDGET_MINUTES) — logs the exact --id-start to resume from")
    parser.add_argument("--output", default="github_org_companies.csv",
                         help="Output CSV path (default github_org_companies.csv)")
    args = parser.parse_args()

    id_start, id_ceiling = args.id_start, args.id_ceiling
    if args.shard_index is not None and args.shard_count is not None:
        # Computed from the ORIGINAL args.id_start/args.id_ceiling only —
        # never from a progressively-mutated local — so chunk boundaries
        # are exact and contiguous (each shard's end == the next shard's
        # start) with no drift, and the LAST shard's ceiling always lands
        # exactly on args.id_ceiling regardless of integer-division
        # remainder (a naive `id_start + chunk` on the last shard would
        # silently drop however many IDs didn't divide evenly).
        span = args.id_ceiling - args.id_start
        chunk = span // args.shard_count
        id_start = args.id_start + args.shard_index * chunk
        id_ceiling = (args.id_ceiling if args.shard_index == args.shard_count - 1
                      else args.id_start + (args.shard_index + 1) * chunk)
        log.info(f"  shard {args.shard_index}/{args.shard_count} -> ids [{id_start}, {id_ceiling})")

    asyncio.run(run_seed(id_start, id_ceiling, args.output, args.time_budget_minutes))


if __name__ == "__main__":
    main()
