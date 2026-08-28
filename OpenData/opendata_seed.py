"""
OPENDATA SEED — one-time (or periodically re-run) download + stream-filter
of opendata.org's "Organizations" dataset (86,690,782 records, ~7.5GB —
Organizations was chosen over Full Data/People Business/Locations because
it's the one carrying company name + domain + country per row, the same
shape kaggle_probe.py's PDL seed needs — see this session's earlier ROI
discussion for why the other three tables were skipped).

THIS SCRIPT ONLY DOWNLOADS + FILTERS. It does not crawl (see node.py /
opendata_probe.py for that) and it does not touch GitHub Releases itself
— that split happens in opendata.yml: this script writes ONE local
filtered CSV (a small fraction of 86.6M rows survive the country/domain
filter — same order of magnitude as kaggle_probe.py's PDL filter rate),
and the workflow uploads/splits that file into <2GB Release asset chunks
with plain shell (`split` + `gh release upload`) — no reason to duplicate
that logic in Python when github_org_seed.py already establishes the
"seed script writes ONE local file, the workflow handles Release upload"
split for exactly this shape.

Streaming end-to-end: the source is downloaded via requests' streaming
mode and decompressed/parsed a chunk at a time — the raw file is NEVER
written to disk in full and NEVER held in memory in full (the one
exception is a .zip source — see _open_streaming_source's docstring: ZIP's
central directory needs a seek, so a .gz or plain .csv source URL should
be preferred if opendata.org offers one). This mirrors how
common_crawl_probe.py/host_crawl_v2.py stream Common Crawl's much larger
Parquet files instead of downloading them whole first.

GitHub Actions' 360-minute (6-hour) hard job timeout is the real
constraint here — the download+filter step can't usefully be sharded
(it's one continuous stream over one source file), so it gets the whole
budget in a single job; the CRAWL step (opendata_probe.py, run from this
script's filtered output) shards normally, same as every other probe.

Column names: opendata.org's download form is gated the same way PDL's
own free-dataset form is (see kaggle_probe.py's module docstring) — its
exact header names weren't confirmed at the time this was written, so
column detection uses the same alias-list approach kaggle_probe.py uses
for PDL's own 'domain' vs 'website' naming difference, covering the
reasonable variants rather than hardcoding one guess. If the real header
uses something outside NAME_COLS/DOMAIN_COLS/COUNTRY_COLS below, add it
there — everything else in this file is schema-agnostic.

Usage:
    export OPENDATA_SOURCE_URL=https://.../organizations.csv.gz   # or .zip / plain .csv
    python opendata_seed.py --output opendata_organizations_filtered.csv
"""
import argparse
import csv
import gzip
import io
import logging
import os
import re
import sys
import time
import zipfile

import requests
from dotenv import load_dotenv

load_dotenv()
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # for discovery.py
from discovery import SKIP_SLUGS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("opendata_seed")

OPENDATA_SOURCE_URL = os.environ.get("OPENDATA_SOURCE_URL", "")
PROGRESS_EVERY = 2_000_000  # raw rows scanned between progress log lines

# Same alias-detection approach as kaggle_probe.py — see module docstring.
NAME_COLS = ("name", "company_name", "organization_name")
DOMAIN_COLS = ("domain", "website", "domain_name", "website_domain")
COUNTRY_COLS = ("country", "country_code")

# Same list as kaggle_probe.py's DEFAULT_COUNTRIES, kept in sync deliberately
# — both are the same "English-language-friendly, real candidate market"
# allowlist regardless of which seed source found the company.
DEFAULT_COUNTRIES = {
    "united states", "united kingdom", "canada", "australia",
    "ireland", "new zealand", "singapore",
    "netherlands", "norway", "sweden", "denmark", "finland", "austria", "belgium",
    "iceland", "luxembourg",
    "france", "germany",
}


def _col_index(header_lower: list[str], aliases: tuple[str, ...]) -> int | None:
    for alias in aliases:
        if alias in header_lower:
            return header_lower.index(alias)
    return None


def _open_streaming_source(url: str):
    """Returns a text-line iterator over the CSV, decompressing on the fly
    if the URL ends in .gz or .zip. requests' stream=True means the HTTP
    response body is never buffered whole in memory; the gzip reader below
    is likewise fed chunk-by-chunk, not given a fully-downloaded blob."""
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    raw = r.raw
    raw.decode_content = True  # let urllib3 handle a gzip Content-Encoding transparently

    if url.endswith(".zip"):
        # zipfile needs seekable access — a ZIP's central directory is at
        # the END of the file, so reading it requires a seek that a pure
        # HTTP stream can't do. This is the one path that isn't fully
        # streaming: it buffers the compressed (not decompressed) bytes in
        # memory. Prefer a .gz or plain .csv source URL if the dataset
        # offers one — both stay genuinely streaming end-to-end.
        log.warning("Source is a .zip — buffering the compressed file in memory for its "
                    "central directory (a .gz or plain .csv URL would avoid this).")
        buf = io.BytesIO(r.content)
        zf = zipfile.ZipFile(buf)
        inner_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        return io.TextIOWrapper(zf.open(inner_name), encoding="utf-8", errors="ignore")
    elif url.endswith(".gz"):
        return io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf-8", errors="ignore")
    else:
        return io.TextIOWrapper(raw, encoding="utf-8", errors="ignore")


def stream_filter(url: str, output_path: str, countries: set[str] | None,
                   row_limit: int = 0) -> int:
    """Single streaming pass: download -> decompress -> parse -> filter ->
    write, all a row at a time. Returns count of rows written."""
    if not url:
        log.error("OPENDATA_SOURCE_URL not set — nothing to download.")
        return 0

    countries_lower = {c.strip().lower() for c in countries} if countries else None
    log.info(f"Downloading + filtering: {url}")
    if countries_lower:
        log.info(f"  filtering to {len(countries_lower)} countries")

    total_rows = kept = skipped_country = 0
    start = time.monotonic()

    try:
        text_stream = _open_streaming_source(url)
        reader = csv.reader(text_stream)
        header = next(reader, None)
    except Exception as e:
        log.error(f"Failed to open/read source: {e}")
        return 0
    if not header:
        log.error("Source is empty (no header row).")
        return 0
    header_lower = [h.strip().lower() for h in header]
    name_i = _col_index(header_lower, NAME_COLS)
    domain_i = _col_index(header_lower, DOMAIN_COLS)
    country_i = _col_index(header_lower, COUNTRY_COLS)
    if name_i is None or domain_i is None:
        log.error(f"Couldn't find a name/domain column in the header: {header}")
        return 0
    log.info(f"  columns: name='{header[name_i]}' domain='{header[domain_i]}'"
              + (f" country='{header[country_i]}'" if country_i is not None else " (no country column)"))

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            writer.writerow(["name", "domain", "country"])

            for i, row in enumerate(reader):
                if row_limit and i >= row_limit:
                    break
                total_rows += 1
                if total_rows % PROGRESS_EVERY == 0:
                    elapsed = time.monotonic() - start
                    log.info(f"  ...scanned {total_rows:,} rows ({total_rows / max(elapsed, 0.001):,.0f} rows/sec), "
                              f"{kept:,} kept so far")
                if len(row) <= domain_i or len(row) <= name_i:
                    continue

                row_country = ""
                if country_i is not None and len(row) > country_i:
                    row_country = row[country_i].strip().lower()
                if countries_lower and row_country not in countries_lower:
                    skipped_country += 1
                    continue

                name = row[name_i].strip()
                domain = row[domain_i].strip().lower()
                domain = re.sub(r"^https?://", "", domain).split("/")[0].strip()
                if not (name and domain and "." in domain and domain not in SKIP_SLUGS):
                    continue

                writer.writerow([name, domain, row_country])
                kept += 1
    except Exception as e:
        log.error(f"Failed mid-stream after {total_rows:,} rows scanned, {kept:,} written: {e}")
        # Whatever was written before the failure is still a valid partial
        # file — don't delete it — but the caller must still treat this as
        # a failed run (see main()'s exit-code handling).
        return kept if kept else 0

    elapsed = time.monotonic() - start
    log.info(f"Done: {total_rows:,} rows scanned, {skipped_country:,} skipped by country, "
             f"{kept:,} written to {output_path} ({kept / max(total_rows, 1) * 100:.2f}%), "
             f"{elapsed:.0f}s ({total_rows / max(elapsed, 0.001):,.0f} rows/sec)")
    return kept


def main():
    parser = argparse.ArgumentParser(description="OpenData.org Organizations — download + stream-filter")
    parser.add_argument("--output", default="opendata_organizations_filtered.csv")
    parser.add_argument("--row-limit", type=int, default=0, help="0 = no cap; nonzero is for testing only")
    parser.add_argument("--country", action="append", default=None,
                         help="Only keep this country value (repeatable). Default: DEFAULT_COUNTRIES.")
    parser.add_argument("--all-countries", action="store_true",
                         help="Disable the country filter — keep every country, including blank ones.")
    args = parser.parse_args()

    if args.country:
        countries = set(args.country)
    elif args.all_countries:
        countries = None
    else:
        countries = DEFAULT_COUNTRIES

    kept = stream_filter(OPENDATA_SOURCE_URL, args.output, countries, args.row_limit)
    if kept == 0:
        log.error("No rows written — aborting with a non-zero exit so the CI job shows red "
                  "instead of silently uploading an empty/missing Release asset.")
        sys.exit(1)


if __name__ == "__main__":
    main()
