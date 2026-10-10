"""
WDC SEED - the Web Data Commons schema.org JobPosting domain list as an archive_ii probe seed.

Web Data Commons extracts schema.org JobPosting markup (JSON-LD / microdata) from Common Crawl. Its per-domain stats file
(~13 MB, 63k domains, 4.3M JobPosting entities in the 2024-12 extraction) is therefore a list of sites that publish their
OWN structured job postings - exactly the in-house career pages Crawl II extracts from. This writes the domains with at least
--min-entities postings (default 3) in the same name,domain,country CSV shape opendata_seed.py produces, so the existing
probe consumes it unchanged. Domains of known ATS vendors / job boards are dropped (those are Crawl I's job).

    python OpenData/wdc_seed.py --output wdc_organizations_filtered.csv [--min-entities 3] [--source-url URL]
"""
import argparse
import csv
import io
import logging
import os
import sys

import requests

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("wdc_seed")

WDC_DOMAIN_STATS_URL = ("https://data.dws.informatik.uni-mannheim.de/structureddata/2024-12/quads/classspecific/"
                        "JobPosting/JobPosting_domain_stats.csv")
# vendor / aggregator registrable domains: their postings are reached through ATS scrapers or are not employer pages
_VENDOR_SUFFIXES = (
    "greenhouse.io", "lever.co", "ashbyhq.com", "myworkdayjobs.com", "workday.com", "icims.com", "smartrecruiters.com", "workable.com",
    "bamboohr.com", "recruitee.com", "personio.de", "personio.com", "teamtailor.com", "breezy.hr", "applytojob.com", "jobvite.com",
    "taleo.net", "oraclecloud.com", "successfactors.com", "successfactors.eu", "brassring.com", "ultipro.com", "paylocity.com",
    "paycomonline.net", "adp.com", "dayforcehcm.com", "join.com", "softgarden.io", "catsone.com", "trakstar.com", "freshteam.com",
    "jobscore.com", "crelate.com", "rippling.com", "eightfold.ai", "phenompeople.com", "pinpointhq.com", "hibob.com",
    "linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com", "monster.com", "careerbuilder.com", "simplyhired.com",
    "jooble.org", "adzuna.com", "talent.com", "jobboardsearch.com", "snagajob.com", "lensa.com", "learn4good.com", "jobrapido.com",
    "neuvoo.com", "careerjet.com", "recruit.net", "jora.com", "seek.com.au", "reed.co.uk", "totaljobs.com", "stepstone.de", "stepstone.com",
)


def _is_vendor(domain: str) -> bool:
    d = domain.lower().strip(".")
    return any(d == s or d.endswith("." + s) for s in _VENDOR_SUFFIXES)


def build_rows(text: str, min_entities: int) -> list[tuple[str, str, str]]:
    rows = []
    reader = csv.reader(io.StringIO(text), delimiter="\t")
    next(reader, None)  # header: Domain, #Quads, #Entities, Properties
    for rec in reader:
        if len(rec) < 3:
            continue
        domain = rec[0].strip().lower()
        try:
            entities = int(rec[2])
        except ValueError:
            continue
        if entities < min_entities or "." not in domain or _is_vendor(domain):
            continue
        rows.append((domain.split(".")[0], domain, ""))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", default="wdc_organizations_filtered.csv")
    ap.add_argument("--min-entities", type=int, default=3)
    ap.add_argument("--source-url", default=os.environ.get("WDC_SOURCE_URL", WDC_DOMAIN_STATS_URL))
    args = ap.parse_args()
    r = requests.get(args.source_url, timeout=180)
    r.raise_for_status()
    rows = build_rows(r.text, args.min_entities)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "domain", "country"])
        w.writerows(rows)
    log.info(f"{len(rows):,} domains with >= {args.min_entities} JobPosting entities written to {args.output}")


if __name__ == "__main__":
    main()
