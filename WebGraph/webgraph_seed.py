"""
WEBGRAPH SEED — one-time (periodically re-run, roughly whenever Common
Crawl republishes a new webgraph release) download + reduce of Common
Crawl's own domain-level WebGraph dataset (harmonic centrality + PageRank,
computed by Common Crawl from real observed inbound links across their
whole crawl). This is the single hardest-to-fake authority signal node.py's
company-maturity quality gate uses — see that file's "SIGNAL RELIABILITY"
comment for why it's weighted highest.

THIS SCRIPT ONLY DOWNLOADS + REDUCES. It does not crawl anything. Its
output is a small local CSV (domain,tier — a few tens of MB gzipped for
~10M rows, nowhere near GitHub's 2GB release-asset cap) that YOU upload as
a GitHub Release asset; node.py's crawl_one() downloads that release asset
ONCE per crawl process and holds it in memory from then on (see
WEBGRAPH_TIERS_URL in node.py) — deliberately NOT a live Supabase table:
that would mean real ongoing egress/storage cost for data that never
changes mid-run and is cheap to just hold in memory instead.

SOURCE (confirmed via Common Crawl's own blog announcing this release —
https://commoncrawl.org/blog/host--and-domain-level-web-graphs): for a
release named e.g. "cc-main-2025-mar-apr-may", two files live under
https://data.commoncrawl.org/projects/hyperlinkgraph/<release>/domain/:
  - <release>-domain-vertices.txt.gz — one line per domain: "id, reversed
    domain, host count" (confirmed format).
  - <release>-domain-ranks.txt.gz — confirmed to contain harmonic
    centrality + PageRank, in RANK order (line 0 = most authoritative).

WHAT'S NOT INDEPENDENTLY CONFIRMED, READ THIS BEFORE TRUSTING THE OUTPUT:
this dev environment could not reach data.commoncrawl.org to inspect the
real ranks file byte-for-byte (network-restricted sandbox), so the exact
column layout below is this project's best-informed inference from how
Common Crawl's own tooling (built on the LAW/webgraph framework, whose
"ranks" files list node ids IN rank order, not a value per node in node-id
order) is documented to work — NOT a verified fact. _parse_ranks_line()
below auto-detects the column count and logs exactly what it saw, and
main() prints a handful of sample (domain, tier) rows before writing
anything — READ that output on the FIRST real run (this needs actual
network access, i.e. run it in GitHub Actions, not this dev sandbox) and
confirm the sample domains look like real, sensibly-ranked sites before
trusting the table for production filtering. If the shape doesn't match
any case _parse_ranks_line() recognizes, the script aborts loudly with the
raw line printed rather than silently writing a guess.

Usage:
    python webgraph_seed.py --release cc-main-2025-mar-apr-may
    python webgraph_seed.py --release cc-main-2025-mar-apr-may --dry-run

    # then, once the output looks right:
    gh release create webgraph-cc-main-2025-mar-apr-may \\
        webgraph_domain_tiers.csv.gz --title "WebGraph domain tiers (cc-main-2025-mar-apr-may)"
    # and point node.py at the resulting asset URL:
    #   WEBGRAPH_TIERS_URL=https://github.com/<owner>/<repo>/releases/download/
    #                      webgraph-cc-main-2025-mar-apr-may/webgraph_domain_tiers.csv.gz
"""
import argparse
import gzip
import io
import logging
import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("webgraph_seed")

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "Main"))

BASE_URL = "https://data.commoncrawl.org/projects/hyperlinkgraph"

# How many top-ranked domains (by harmonic centrality — the metric Common
# Crawl's own blog post describes first and the one most literature treats
# as the more robust of the two for "is this a real, well-connected site")
# get kept at all. Domains outside this cutoff simply get no signal (0
# points) from _webgraph_score() — not a penalty, just "unranked", which
# is the overwhelmingly common case even for perfectly legitimate small/mid
# companies.
TIER_1_CUTOFF = 1_000_000   # WEBGRAPH_STRONG_TIER_SCORE in node.py
TIER_2_CUTOFF = 10_000_000  # WEBGRAPH_MODERATE_TIER_SCORE in node.py

PROGRESS_EVERY = 1_000_000
SAMPLE_PRINT_N = 15


def _err(e: Exception) -> str:
    return str(e) or repr(e) or type(e).__name__


def _stream_gz_lines(url: str):
    """Streams a remote .gz text file line-by-line without ever holding
    the whole decompressed file in memory — same shape as opendata_seed.py's
    streaming reads, just gzip directly (no zip container to open here)."""
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    raw = r.raw
    raw.decode_content = True
    with gzip.GzipFile(fileobj=raw) as gz:
        text = io.TextIOWrapper(gz, encoding="utf-8", errors="ignore")
        for line in text:
            yield line.rstrip("\n")


def _reverse_domain(rev_domain: str) -> str:
    """"com.example.www" (Common Crawl's reversed-label notation, TLD
    first) -> "www.example.com". A bare reversal of the dot-split labels —
    no other normalization, since domain values elsewhere in this codebase
    (node.py, the probes) are already lowercase/stripped by the time
    they're compared."""
    parts = rev_domain.strip().split(".")
    return ".".join(reversed(parts))


def load_vertices(vertices_url: str) -> dict[int, str]:
    """id -> normal-order domain. Loaded fully into memory (a dict of ints
    -> short strings for ~100-200M domains is a few GB — real, but a one-
    time cost on a machine that isn't this dev sandbox; GitHub Actions
    runners have enough RAM for this, and it only runs when a new Common
    Crawl webgraph release needs seeding, not on every crawl)."""
    log.info(f"Loading vertices: {vertices_url}")
    id_to_domain: dict[int, str] = {}
    start = time.monotonic()
    n = 0
    for line in _stream_gz_lines(vertices_url):
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            node_id = int(parts[0])
        except ValueError:
            continue
        domain = _reverse_domain(parts[1])
        if domain:
            id_to_domain[node_id] = domain
        n += 1
        if n % PROGRESS_EVERY == 0:
            elapsed = time.monotonic() - start
            log.info(f"  ...{n:,} vertices loaded ({n / max(elapsed, 0.001):,.0f}/sec)")
    log.info(f"Loaded {len(id_to_domain):,} vertices in {time.monotonic() - start:.0f}s")
    return id_to_domain


def _parse_ranks_line(line: str, line_index: int) -> int | None:
    """Returns the node id ranked at `line_index` (0-based, i.e. line 0 is
    the #1 most authoritative domain) by harmonic centrality, or None if
    the line is blank/unparseable. Handles the column shapes this project
    could reasonably expect from a LAW-webgraph-style ranks file — see
    this module's docstring for what's confirmed vs. inferred here:
      - 1 column:  just the node id, already in rank order.
      - 2 columns: (harmonic_node_id, pagerank_node_id) — this is the
        PRIMARY expected shape; harmonic centrality is column 1.
      - 4 columns: (harmonic_node_id, harmonic_value, pagerank_node_id,
        pagerank_value) — same idea with the raw metric values kept too.
    Any other column count raises ValueError with the raw line included,
    so main() aborts loudly instead of silently mis-parsing."""
    if not line.strip():
        return None
    parts = line.split()
    try:
        if len(parts) == 1:
            return int(parts[0])
        if len(parts) in (2, 4):
            return int(parts[0])
    except ValueError:
        pass
    raise ValueError(f"Unrecognized ranks-file line shape ({len(parts)} columns) at line "
                      f"{line_index}: {line!r}")


def build_tiers(ranks_url: str, id_to_domain: dict[int, str]) -> dict[str, int]:
    """Streams the ranks file (already in rank order — see module
    docstring) and assigns tier 1/2 to the first TIER_1_CUTOFF /
    TIER_2_CUTOFF domains it maps to a known vertex id. Stops reading once
    past TIER_2_CUTOFF (no need to stream the rest)."""
    log.info(f"Loading ranks (rank-ordered): {ranks_url}")
    domain_tier: dict[str, int] = {}
    start = time.monotonic()
    unmapped = 0
    for i, line in enumerate(_stream_gz_lines(ranks_url)):
        if i >= TIER_2_CUTOFF:
            break
        node_id = _parse_ranks_line(line, i)
        if node_id is None:
            continue
        domain = id_to_domain.get(node_id)
        if domain is None:
            unmapped += 1
            continue
        if domain in domain_tier:
            continue  # keep the FIRST (best) rank a domain appears at
        domain_tier[domain] = 1 if i < TIER_1_CUTOFF else 2
        if (i + 1) % PROGRESS_EVERY == 0:
            elapsed = time.monotonic() - start
            log.info(f"  ...rank {i + 1:,} processed ({(i + 1) / max(elapsed, 0.001):,.0f}/sec), "
                      f"{len(domain_tier):,} domains tiered so far, {unmapped:,} unmapped ids")
    log.info(f"Done: {len(domain_tier):,} domains tiered, {unmapped:,} rank entries had no matching "
             f"vertex id, in {time.monotonic() - start:.0f}s")
    return domain_tier


def write_tiers_csv(domain_tier: dict[str, int], output_path: str, gzip_output: bool = True) -> None:
    """Writes the reduced (domain,tier) CSV locally — this is what YOU
    upload as a GitHub Release asset (see module docstring), not something
    this script pushes anywhere itself (no git/gh push capability is
    assumed here)."""
    items = list(domain_tier.items())
    log.info(f"Writing {len(items):,} rows to {output_path}...")
    opener = gzip.open if gzip_output else open
    mode = "wt" if gzip_output else "w"
    with opener(output_path, mode, encoding="utf-8", newline="") as f:
        f.write("domain,tier\n")
        for domain, tier in items:
            f.write(f"{domain},{tier}\n")
    size_mb = os.path.getsize(output_path) / 1e6
    log.info(f"Done: {output_path} ({size_mb:.1f} MB) — upload this as a GitHub Release asset, then "
             f"point node.py's WEBGRAPH_TIERS_URL env var at the asset's download URL.")


def main():
    parser = argparse.ArgumentParser(description="Common Crawl WebGraph domain-tier seed")
    parser.add_argument("--release", required=True,
                         help="e.g. cc-main-2025-mar-apr-may — see "
                              "https://commoncrawl.org/blog for the current release name")
    parser.add_argument("--output", default="webgraph_domain_tiers.csv.gz",
                         help="Local output path for the reduced (domain,tier) CSV. Gzipped if the "
                              "path ends in .gz (default), matching what node.py's WEBGRAPH_TIERS_URL "
                              "loader expects for a .gz asset URL.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Parse everything and print a sample, but don't write the output CSV. "
                              "Use this on the FIRST run against a new release to sanity-check the "
                              "parsed domains/tiers before trusting them (see module docstring).")
    args = parser.parse_args()

    vertices_url = f"{BASE_URL}/{args.release}/domain/{args.release}-domain-vertices.txt.gz"
    ranks_url = f"{BASE_URL}/{args.release}/domain/{args.release}-domain-ranks.txt.gz"

    id_to_domain = load_vertices(vertices_url)
    if not id_to_domain:
        log.error("No vertices loaded — aborting.")
        sys.exit(1)

    try:
        domain_tier = build_tiers(ranks_url, id_to_domain)
    except ValueError as e:
        log.error(f"Ranks file didn't match any recognized shape — aborting rather than guessing. {e}")
        log.error("Share a few raw lines from the actual ranks file so this parser can be fixed for "
                  "the real format.")
        sys.exit(1)

    if not domain_tier:
        log.error("No domains tiered — aborting, nothing to upload.")
        sys.exit(1)

    log.info(f"Sample of tiered domains:")
    for domain, tier in list(domain_tier.items())[:SAMPLE_PRINT_N]:
        log.info(f"    {domain:40s} tier={tier}")

    if args.dry_run:
        log.info(f"[dry-run] would write {len(domain_tier):,} rows to {args.output} — nothing written.")
        return

    write_tiers_csv(domain_tier, args.output, gzip_output=args.output.endswith(".gz"))


if __name__ == "__main__":
    main()
