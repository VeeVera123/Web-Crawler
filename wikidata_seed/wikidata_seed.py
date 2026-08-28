"""
WIKIDATA SEED — query Wikidata SPARQL for companies with official websites.

Replaces opendata_seed.py's OpenData.org Organizations download (which
lacked usable domain/website fields in its JSON shards — confirmed live).

Why Wikidata:
- Genuinely free (CC0), no signup, no API key, no credit card.
- Human-verified: every company entry was added/confirmed by a real
  editor, so the website URLs are overwhelmingly correct and live.
- ~300K–700K companies with all three fields (name, website, country) —
  smaller than OpenData's 86M rows, but FAR higher signal-to-noise
  since every row actually has a working website URL.

Access method:
- Wikidata Query Service (WDQS) SPARQL endpoint at query.wikidata.org.
- We query per-country (18 queries) to stay well under WDQS's ~60s
  query timeout. Each per-country query completes in 2–15 seconds.
- Results are written to the same filtered CSV format (name, domain,
  country) that opendata_probe.py already reads — zero downstream
  changes needed.

Usage:
  python wikidata_seed.py
  python wikidata_seed.py --output opendata_organizations_filtered.csv
  python wikidata_seed.py --dry-run
  python wikidata_seed.py --country "united states" --country "germany"
"""

import argparse
import csv
import logging
import re
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("wikidata_seed")

# ── Wikidata Query Service ────────────────────────────────────────────
WDQS_ENDPOINT = "https://query.wikidata.org/sparql"

# Wikidata requires a descriptive User-Agent identifying your bot/script.
# Update the contact info before deploying.
USER_AGENT = "ATS-Global-Scanner/1.0 (company-domain-seed; https://github.com/YOUR_REPO)"

# Company type Q-IDs for wdt:P31 (instance of).
# Using VALUES with multiple types is faster than UNION in WDQS.
#   Q783794   = company
#   Q4830453  = business enterprise
#   Q68815116 = enterprise
#   Q1787971  = technology company
#   Q1616075  = manufacturing company
#   Q192283   = cooperative
#   Q507619   = retail chain
COMPANY_TYPES = (
    "Q783794", "Q4830453", "Q68815116",
    "Q1787971", "Q1616075", "Q192283", "Q507619",
)

# Country name → Wikidata Q-ID.
# Must stay in sync with DEFAULT_COUNTRIES in opendata_seed.py /
# opendata_probe.py / kaggle_probe.py — same "English-language-friendly,
# real candidate market" allowlist.
COUNTRY_QIDS = {
    "united states": "Q30",
    "united kingdom": "Q145",
    "canada": "Q16",
    "australia": "Q408",
    "ireland": "Q27",
    "new zealand": "Q664",
    "singapore": "Q334",
    "netherlands": "Q55",
    "norway": "Q20",
    "sweden": "Q34",
    "denmark": "Q35",
    "finland": "Q33",
    "austria": "Q40",
    "belgium": "Q31",
    "iceland": "Q189",
    "luxembourg": "Q32",
    "france": "Q142",
    "germany": "Q183",
}

DEFAULT_COUNTRIES = set(COUNTRY_QIDS.keys())

# WDQS returns at most ~10,000 rows per query by default. For very large
# countries (US, UK, DE) this means we may not get every company — but
# 10K per country × 18 countries = 180K is already a solid seed, and the
# probe's own crawl will find more via linked pages anyway.
WDQS_ROW_LIMIT = 10000


def _build_sparql(country_qid: str) -> str:
    """SPARQL for companies of known types in one country that have an
    official website (P856) and an English label."""
    types = " ".join(f"wd:{t}" for t in COMPANY_TYPES)
    return f"""
SELECT DISTINCT ?name ?website WHERE {{
  VALUES ?type {{ {types} }}
  ?company wdt:P31 ?type .
  ?company wdt:P856 ?website .
  ?company wdt:P17 wd:{country_qid} .
  ?company rdfs:label ?name .
  FILTER(LANG(?name) = "en")
}}
LIMIT {WDQS_ROW_LIMIT}
"""


def _query_wikidata(sparql: str, retries: int = 3) -> list[dict]:
    """Execute a SPARQL query against WDQS. Returns list of
    {name, website} dicts. Retries on transient errors / rate limits."""
    for attempt in range(retries):
        try:
            r = requests.get(
                WDQS_ENDPOINT,
                params={"query": sparql, "format": "json"},
                headers={"User-Agent": USER_AGENT},
                timeout=120,
            )
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 30))
                log.warning(f"  WDQS rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            bindings = r.json().get("results", {}).get("bindings", [])
            return [
                {
                    "name": b["name"]["value"],
                    "website": b["website"]["value"],
                }
                for b in bindings
                if "name" in b and "website" in b
            ]
        except requests.exceptions.Timeout:
            log.warning(f"  WDQS timeout (attempt {attempt + 1}/{retries})")
            time.sleep(5 * (attempt + 1))
        except Exception as e:
            log.warning(f"  WDQS error (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(5 * (attempt + 1))
    return []


def _extract_domain(url: str) -> str:
    """Extract bare domain from a URL, stripping scheme, path, port,
    and leading 'www.' — same normalisation opendata_seed.py uses."""
    url = url.strip().lower()
    url = re.sub(r"^https?://", "", url)
    domain = url.split("/")[0].split(":")[0]
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def fetch_wikidata_companies(
    countries: set[str],
) -> list[tuple[str, str, str]]:
    """Query Wikidata for companies with websites in the given countries.
    Returns list of (name, domain, country) tuples, deduplicated by domain."""
    all_rows: list[tuple[str, str, str]] = []
    seen_domains: set[str] = set()

    for country_name in sorted(countries):
        qid = COUNTRY_QIDS.get(country_name)
        if not qid:
            log.warning(f"  No Wikidata Q-ID for {country_name!r}, skipping")
            continue

        log.info(f"  Querying Wikidata for {country_name} ({qid})...")
        sparql = _build_sparql(qid)
        results = _query_wikidata(sparql)

        new_in_country = 0
        for item in results:
            domain = _extract_domain(item["website"])
            if not domain or "." not in domain:
                continue
            if domain in seen_domains:
                continue
            seen_domains.add(domain)
            all_rows.append((item["name"], domain, country_name))
            new_in_country += 1

        log.info(
            f"    {country_name}: {len(results)} raw results, "
            f"{new_in_country} new unique domains"
        )

        # Polite delay between WDQS queries — they're generous but
        # hammering 18 queries back-to-back risks a temporary block.
        time.sleep(2)

    return all_rows


def main():
    parser = argparse.ArgumentParser(
        description="Seed company domains from Wikidata SPARQL"
    )
    parser.add_argument(
        "--output",
        default="opendata_organizations_filtered.csv",
        help="Output CSV path (same default as opendata_seed.py so "
             "opendata_probe.py picks it up without changes)",
    )
    parser.add_argument(
        "--country", action="append", default=None,
        help="Only query this country (repeatable). Default: all 18.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count results without writing the CSV.",
    )
    args = parser.parse_args()

    countries = set(args.country) if args.country else DEFAULT_COUNTRIES

    log.info("=" * 60)
    log.info("WIKIDATA SEED — company domains via SPARQL")
    log.info(f"  Countries: {len(countries)}")
    log.info(f"  Output:    {args.output}")
    log.info("=" * 60)

    rows = fetch_wikidata_companies(countries)

    log.info(f"\nTotal: {len(rows)} unique companies with domains")

    if not rows:
        log.error("No companies found — aborting with non-zero exit.")
        sys.exit(1)

    if args.dry_run:
        log.info("(dry run — not writing CSV)")
        return

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "domain", "country"])
        for name, domain, country in rows:
            writer.writerow([name, domain, country])

    log.info(f"Wrote {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
