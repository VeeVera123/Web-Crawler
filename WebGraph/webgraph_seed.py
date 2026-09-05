"""
WEBGRAPH SEED — one-time (periodically re-run, roughly whenever Common
Crawl republishes a new webgraph release) download + reduce of Common
Crawl's own domain-level WebGraph dataset (harmonic centrality + PageRank,
computed by Common Crawl from real observed inbound links across their
whole crawl). This is the single hardest-to-fake authority signal node.py's
company-maturity quality gate uses — see that file's "SIGNAL RELIABILITY"
comment for why it's weighted highest.

THIS SCRIPT ONLY DOWNLOADS + REDUCES. It does not crawl anything. Its
output is a small local CSV (domain,band — a few tens of MB gzipped for
~10M rows, nowhere near GitHub's 2GB release-asset cap) that YOU upload as
a GitHub Release asset; node.py's crawl_one() downloads that release asset
ONCE per crawl process and holds it in memory from then on (see
WEBGRAPH_TIERS_URL in node.py) — deliberately NOT a live Supabase table:
that would mean real ongoing egress/storage cost for data that never
changes mid-run and is cheap to just hold in memory instead.

SOURCE — CONFIRMED 2026-09 against the real file (a live run's error
output printed the actual header line, resolving what an earlier version
of this script could only infer): for a release named e.g.
"cc-main-2026-jun-jul-aug", the ranks file lives at
https://data.commoncrawl.org/projects/hyperlinkgraph/<release>/domain/<release>-domain-ranks.txt.gz
— TAB-separated, one '#'-prefixed header row, then data rows in RANK order
(row 0 = most authoritative by harmonic centrality):
    #harmonicc_pos  #harmonicc_val  #pr_pos  #pr_val  #host_rev  #n_hosts
Column 5 (host_rev) IS the domain itself (Common Crawl's reversed-label
notation, e.g. "com.example"), so no join against a separate vertices
file is needed — this script never downloads the vertices file at all.
That matters beyond just simplicity: an earlier version of this script DID
load the vertices file (119.7M rows) fully into memory to resolve node ids
the ranks file turns out to name directly, and that was a real out-of-
memory failure on a live run. Skipping it means this script only ever
holds domains ranked within the last band's boundary (40M as of the
2026-09 recalibration, was 80M — see RANK_BANDS below) in memory, never
the full ~120M-domain graph.

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
import urllib3.exceptions
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("webgraph_seed")

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "Main"))

BASE_URL = "https://data.commoncrawl.org/projects/hyperlinkgraph"

# Four graduated rank bands (S+/S/A/B, plus implicit D — the old,
# weakest "C" tier was dropped 2026-09) by harmonic centrality — the
# metric Common Crawl's own blog post describes first and the one most
# literature treats as the more robust of the two for "is this a real,
# well-connected site". MUST stay in sync with node.py's
# WEBGRAPH_RANK_BANDS — that's the single source of truth for what each
# label is worth in the Quality Index; this list only needs the
# boundaries, to assign the same labels here.
#
# 2026-09, REVISED from a flat top-1M/top-10M cutoff after checking a
# REAL, known-legitimate company (heli.technology — already confirmed
# hiring globally in the jobs DB) against the actual ranks file: it landed
# at rank 44,860,492 out of ~118.0M domain nodes (roughly the 62nd
# percentile) — nowhere near any flat cutoff being considered, which would
# have given it ZERO WebGraph credit despite being real. Graduated bands
# let a domain like that earn a small, honest amount of credit instead of
# an all-or-nothing cliff.
#
# 2026-09 RECALIBRATION: the last band's ceiling lowered from 80,000,000
# to 40,000,000 (the old, weakest "C" band dropped entirely) and every
# band's POINT VALUE lowered too (see node.py's WEBGRAPH_RANK_BANDS —
# points live there, not here) — a real archive_ii false positive
# (olphnm.org, a small parish church site with none of the Quality
# Index's other signals) made it through purely on WebGraph rank alone,
# because the old points let even a single top band clear the whole
# acceptance bar by itself. MUST stay in sync with node.py's
# WEBGRAPH_RANK_BANDS boundaries — see that comment for the full
# reasoning (current published graph stats, the precision/recall
# tradeoff, and the explicit note that heli.technology itself — rank
# 44,860,492 — now falls just past this tighter 40M ceiling and gets
# zero credit, an intentional result of the new, higher target).
#
# Boundaries reasoned from Common Crawl's own current published graph
# stats (cc-main-2026-jun-jul-aug: 119,722,885 domain nodes; 76,352,306
# (63.77%) "dangling"/no-outbound-links nodes — a rough proxy for
# peripheral sites) — see node.py's WEBGRAPH_RANK_BANDS comment for the
# full per-band reasoning. Domains ranked below the last band (or never
# found in the ranks file) get NO signal — band "D", intentionally worth
# 0: this is where most parked/junk domains (and, post-recalibration,
# most small/midsize real businesses too) live.
RANK_BANDS = (
    # (label, INCLUSIVE rank upper bound, 0-based) — checked in order,
    # first match wins, so rank <= bound assigns that label. Points for
    # each label live in node.py's WEBGRAPH_RANK_BANDS, not here — this
    # script only needs to assign the right LABEL per domain.
    ("S+", 1_000_000),
    ("S", 10_000_000),
    ("A", 25_000_000),
    ("B", 40_000_000),
)

PROGRESS_EVERY = 1_000_000
SAMPLE_PRINT_N = 15


def _err(e: Exception) -> str:
    return str(e) or repr(e) or type(e).__name__


# 2026-09: a real run on a residential/office connection hit
# urllib3.exceptions.ReadTimeoutError partway through — data.commoncrawl.org
# stalled for >60s mid-stream (confirmed: the traceback's own line numbers
# point straight at the `for line in text:` loop below, not the initial
# connect). The old bare `requests.get(url, stream=True, timeout=60)` had
# no retry at any layer: one stalled read killed the whole multi-hundred-
# million-row scan, and lookup_ranks() (used for calibration, no early
# exit) is the flow most exposed to this since it may need to stream the
# ENTIRE file. gzip can't resume mid-stream from a byte offset without
# re-implementing DEFLATE's block-resumption (not worth it here), so on a
# stalled/dropped connection this just re-opens the request from byte 0
# and re-decodes — cheap relative to the network wait — but skips the
# lines already yielded so a caller mid-way through a scan sees no
# duplicates and no gap.
_STREAM_CONNECT_TIMEOUT = 15   # seconds to establish the TCP/TLS connection
_STREAM_READ_TIMEOUT = 120     # seconds of silence on an open connection before giving up on it
_STREAM_MAX_RETRIES = 6        # whole-request retries after a stall/drop, not counting the first try


def _make_retrying_session() -> requests.Session:
    """A `Retry` adapter only covers connection-establishment failures and
    a handful of retryable HTTP statuses — it does NOT cover a read that
    times out after the connection is already open and streaming (that's
    exactly what hit us live), so this session still needs the outer
    retry loop in _stream_gz_lines below. This adapter just means a
    dropped/refused connection on RE-request doesn't need its own extra
    layer of backoff on top of that loop's."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504),
                   allowed_methods=frozenset(["GET"]))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _stream_gz_lines(url: str):
    """Streams a remote .gz text file line-by-line without ever holding
    the whole decompressed file in memory — same shape as opendata_seed.py's
    streaming reads, just gzip directly (no zip container to open here).
    Resilient to a mid-stream stall or dropped connection (see the
    comment above _STREAM_CONNECT_TIMEOUT): re-opens the request from
    scratch, re-decodes, and skips lines already yielded to this
    generator's caller, up to _STREAM_MAX_RETRIES times."""
    session = _make_retrying_session()
    lines_yielded = 0
    attempt = 0
    while True:
        try:
            r = session.get(url, stream=True,
                             timeout=(_STREAM_CONNECT_TIMEOUT, _STREAM_READ_TIMEOUT))
            r.raise_for_status()
            raw = r.raw
            raw.decode_content = True
            skip = lines_yielded
            with gzip.GzipFile(fileobj=raw) as gz:
                text = io.TextIOWrapper(gz, encoding="utf-8", errors="ignore")
                for line in text:
                    if skip:
                        skip -= 1
                        continue
                    yield line.rstrip("\n")
                    lines_yielded += 1
            return  # stream reached EOF cleanly
        except (requests.exceptions.RequestException, urllib3.exceptions.HTTPError, OSError) as e:
            # NOTE: because this reads straight from `r.raw` (bypassing
            # requests' own iter_content, which normally re-wraps urllib3
            # errors as requests.exceptions.*), a stall surfaces as the
            # RAW urllib3.exceptions.ReadTimeoutError, not
            # requests.exceptions.ReadTimeout — confirmed by the real
            # traceback this fix was written against, which named the
            # urllib3 class directly. Catching only requests.exceptions.*
            # would have missed it entirely, which is exactly what happened
            # before this fix.
            attempt += 1
            if attempt > _STREAM_MAX_RETRIES:
                log.error(f"  giving up after {attempt - 1} retries and {lines_yielded:,} lines "
                          f"yielded ({_err(e)})")
                raise
            wait = min(2 ** attempt, 60)
            log.warning(f"  stream stalled/dropped after {lines_yielded:,} lines ({_err(e)}) — "
                        f"retrying whole download from the top, skipping already-seen lines "
                        f"(attempt {attempt}/{_STREAM_MAX_RETRIES}, waiting {wait}s)...")
            time.sleep(wait)


def _reverse_domain(rev_domain: str) -> str:
    """"com.example.www" (Common Crawl's reversed-label notation, TLD
    first) -> "www.example.com". A bare reversal of the dot-split labels —
    no other normalization, since domain values elsewhere in this codebase
    (node.py, the probes) are already lowercase/stripped by the time
    they're compared."""
    parts = rev_domain.strip().split(".")
    return ".".join(reversed(parts))


# CONFIRMED 2026-09 against the real file (a live run's error output
# printed the actual header line): the ranks file is TAB-separated with a
# leading '#' header/comment row, 6 columns:
#   #harmonicc_pos  #harmonicc_val  #pr_pos  #pr_val  #host_rev  #n_hosts
# Column 5 (host_rev) is the domain itself, already in Common Crawl's
# reversed-label notation — meaning the ranks file needs NO join against
# the vertices file at all to get a domain string. That vertices file
# (119.7M rows) was the actual cause of a real OOM on a prior run: loading
# it fully into a dict just to resolve node ids the ranks file already
# names directly. Dropping the vertices step entirely fixes the memory
# problem at its root instead of working around it with sharding — this
# script now only ever holds the ranked (band S+ through C) domains in
# memory, never the full ~120M-domain graph.
_RANKS_HEADER_PREFIX = "#"


def _band_for_rank(rank: int) -> str | None:
    """0-based rank -> band label ("S+".."B"), or None if it falls below
    every band (band "D" — no signal, see RANK_BANDS' comment above).
    Boundaries are INCLUSIVE — rank <= upper_bound earns that label."""
    for label, upper_bound in RANK_BANDS:
        if rank <= upper_bound:
            return label
    return None


def _parse_ranks_row(line: str, line_index: int) -> str | None:
    """Returns the domain named at `line_index` (0-based, i.e. line 0 is
    the #1 most authoritative domain by harmonic centrality), or None for
    a blank/header/comment line. Raises ValueError (raw line included) for
    any row that isn't the confirmed 6-column tab-separated shape, so
    main() aborts loudly instead of silently mis-parsing a format change."""
    if not line.strip() or line.startswith(_RANKS_HEADER_PREFIX):
        return None
    parts = line.split("\t")
    if len(parts) != 6:
        raise ValueError(f"Unrecognized ranks-file line shape ({len(parts)} tab-separated columns) "
                          f"at line {line_index}: {line!r}")
    host_rev = parts[4].strip()
    if not host_rev:
        return None
    return _reverse_domain(host_rev)


_LAST_BAND_CUTOFF = RANK_BANDS[-1][1]  # 40,000,000 (2026-09, was 80,000,000) — INCLUSIVE, so rank 40,000,000 itself still counts


def build_ranks(ranks_url: str) -> dict[str, str]:
    """Streams the ranks file (already in rank order — see the confirmed-
    format note above) and assigns a band label (S+.."B") straight from
    each row's own host_rev column via _band_for_rank() — no vertices
    file, no id join, no ~120M-entry dict. Stops reading once past the
    last band's boundary (no need to stream the rest — anything beyond
    gets no signal anyway, band "D")."""
    log.info(f"Loading ranks (rank-ordered): {ranks_url}")
    domain_band: dict[str, str] = {}
    start = time.monotonic()
    i = 0  # counts DATA rows only (header/blank lines don't consume a rank slot)
    for line in _stream_gz_lines(ranks_url):
        if i > _LAST_BAND_CUTOFF:  # > not >= — the cutoff rank itself is still in-band (inclusive)
            break
        domain = _parse_ranks_row(line, i)
        if domain is None:
            continue
        if domain not in domain_band:  # keep the FIRST (best) rank a domain appears at
            domain_band[domain] = _band_for_rank(i)
        i += 1
        if i % PROGRESS_EVERY == 0:
            elapsed = time.monotonic() - start
            log.info(f"  ...rank {i:,} processed ({i / max(elapsed, 0.001):,.0f}/sec), "
                      f"{len(domain_band):,} domains banded so far")
    log.info(f"Done: {len(domain_band):,} domains banded (of {i:,} rank rows read) in "
             f"{time.monotonic() - start:.0f}s")
    return domain_band


def lookup_ranks(ranks_url: str, target_domains: list[str]) -> dict[str, int | None]:
    """Calibration tool, not part of the normal seed flow: streams the FULL
    ranks file once (no early-exit at the last band's boundary — a target
    domain could rank anywhere, including below every band) looking for
    `target_domains`, and returns {domain: rank} (0-based rank position,
    i.e. the same numbering build_ranks() uses to decide bands) or
    {domain: None} if never found. Use this to check where a real,
    known-legitimate company actually ranks before picking/moving a band
    boundary — evidence beats guessing at another round number."""
    remaining = {d.strip().lower() for d in target_domains}
    found: dict[str, int | None] = {d: None for d in remaining}
    log.info(f"Looking up {len(remaining)} domain(s) against the full ranks file: {ranks_url}")
    start = time.monotonic()
    i = 0
    for line in _stream_gz_lines(ranks_url):
        domain = _parse_ranks_row(line, i)
        if domain is None:
            continue
        if domain in remaining:
            found[domain] = i
            remaining.discard(domain)
            log.info(f"  found {domain} at rank {i:,}")
            if not remaining:
                break
        i += 1
        if i % PROGRESS_EVERY == 0:
            elapsed = time.monotonic() - start
            log.info(f"  ...rank {i:,} scanned ({i / max(elapsed, 0.001):,.0f}/sec), "
                      f"still looking for: {sorted(remaining)}")
    log.info(f"Done scanning {i:,} rank rows in {time.monotonic() - start:.0f}s")
    for domain, rank in found.items():
        if rank is None:
            log.info(f"  {domain}: NOT FOUND in {i:,} rank rows scanned "
                     f"(either genuinely unranked, or beyond the graph's edge) — band D")
        else:
            band = _band_for_rank(rank) or "D"
            log.info(f"  {domain}: rank {rank:,} — band {band}")
    return found


def write_ranks_csv(domain_band: dict[str, str], output_path: str, gzip_output: bool = True) -> None:
    """Writes the reduced (domain,band) CSV locally — this is what YOU
    upload as a GitHub Release asset (see module docstring), not something
    this script pushes anywhere itself (no git/gh push capability is
    assumed here)."""
    items = list(domain_band.items())
    log.info(f"Writing {len(items):,} rows to {output_path}...")
    opener = gzip.open if gzip_output else open
    mode = "wt" if gzip_output else "w"
    with opener(output_path, mode, encoding="utf-8", newline="") as f:
        f.write("domain,band\n")
        for domain, band in items:
            f.write(f"{domain},{band}\n")
    size_mb = os.path.getsize(output_path) / 1e6
    log.info(f"Done: {output_path} ({size_mb:.1f} MB) — upload this as a GitHub Release asset, then "
             f"point node.py's WEBGRAPH_TIERS_URL env var at the asset's download URL.")


def main():
    parser = argparse.ArgumentParser(description="Common Crawl WebGraph domain-tier seed")
    parser.add_argument("--release", required=True,
                         help="e.g. cc-main-2025-mar-apr-may — see "
                              "https://commoncrawl.org/blog for the current release name")
    parser.add_argument("--output", default="webgraph_domain_tiers.csv.gz",
                         help="Local output path for the reduced (domain,band) CSV. Gzipped if the "
                              "path ends in .gz (default), matching what node.py's WEBGRAPH_TIERS_URL "
                              "loader expects for a .gz asset URL.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Parse everything and print a sample, but don't write the output CSV. "
                              "Use this on the FIRST run against a new release to sanity-check the "
                              "parsed domains/bands before trusting them (see module docstring).")
    parser.add_argument("--lookup-domain", action="append", default=None,
                         help="Calibration mode — repeatable. Scans the FULL ranks file for this "
                              "domain's actual rank and reports which band (S+/S/A/B/C/D) it would "
                              "land in, instead of building the banded CSV. Use this against a "
                              "known-real company to pick/sanity-check band boundaries from evidence "
                              "instead of another guess.")
    args = parser.parse_args()

    ranks_url = f"{BASE_URL}/{args.release}/domain/{args.release}-domain-ranks.txt.gz"

    if args.lookup_domain:
        lookup_ranks(ranks_url, args.lookup_domain)
        return

    try:
        domain_band = build_ranks(ranks_url)
    except ValueError as e:
        log.error(f"Ranks file didn't match any recognized shape — aborting rather than guessing. {e}")
        log.error("Share a few raw lines from the actual ranks file so this parser can be fixed for "
                  "the real format.")
        sys.exit(1)

    if not domain_band:
        log.error("No domains banded — aborting, nothing to upload.")
        sys.exit(1)

    log.info(f"Sample of banded domains:")
    for domain, band in list(domain_band.items())[:SAMPLE_PRINT_N]:
        log.info(f"    {domain:40s} band={band}")

    if args.dry_run:
        log.info(f"[dry-run] would write {len(domain_band):,} rows to {args.output} — nothing written.")
        return

    write_ranks_csv(domain_band, args.output, gzip_output=args.output.endswith(".gz"))


if __name__ == "__main__":
    main()
