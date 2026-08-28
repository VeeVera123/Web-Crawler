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
import tempfile
import time
import zipfile

import requests
from dotenv import load_dotenv

load_dotenv()
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)                       # discovery.py lives at repo root on GitHub
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # fallback, for a local Main/ layout
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
        # HTTP stream can't do. FIXED 2026-08 (was io.BytesIO(r.content) —
        # buffered the whole compressed file in RAM and reliably OOM'd on
        # a multi-GB zip: GitHub-hosted runners have ~7GB RAM but ~14GB
        # disk, and a bare MemoryError's str() is EMPTY, which is exactly
        # what showed up as "Failed to open/read source: " with nothing
        # after the colon). Streams to a local temp file on disk instead —
        # only the compressed zip touches disk in full; the inner CSV is
        # still read back via ZipExtFile, which decompresses on read, so
        # the decompressed data is never held in full anywhere.
        tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        log.info(f"  Zip source — streaming download to disk first ({tmp.name}), not RAM...")
        downloaded = 0
        try:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                tmp.write(chunk)
                downloaded += len(chunk)
        finally:
            tmp.close()
        log.info(f"  Downloaded {downloaded / 1e9:.2f}GB to disk")
        zf = zipfile.ZipFile(tmp.name)
        inner_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        return io.TextIOWrapper(zf.open(inner_name), encoding="utf-8", errors="ignore")
    elif url.endswith(".gz"):
        return io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf-8", errors="ignore")
    else:
        return io.TextIOWrapper(raw, encoding="utf-8", errors="ignore")


def stream_filter(url: str, output_path: str, countries: set[str] | None,
                   row_limit: int = 0, skip_rows: int = 0,
                   time_budget_minutes: int = 0) -> tuple[int, bool, int]:
    """Single streaming pass: download -> decompress -> parse -> filter ->
    write, all a row at a time. Returns (kept, stopped_early, raw_row_index)
    — raw_row_index is the "Restart ID": how many raw source rows had been
    read (including any already skipped from a previous run) when this run
    ended, whether that's because it finished, hit row_limit, or hit
    time_budget_minutes. Pass that number back in as skip_rows next run to
    continue from exactly there.

    RESUME CAVEAT: there is no byte-range/row-index API into a plain HTTP
    CSV/GZ stream, so skip_rows still has to download+decompress+parse
    every row up to that point — it does NOT save network time or CPU on
    the skipped prefix. What it DOES save: output_path is opened in
    APPEND mode (not overwritten) when skip_rows > 0, so a resumed run's
    already-filtered/written rows from earlier runs are never lost or
    redone, and the run can stop and resume as many times as needed to
    get through the full 86.6M-row source without ever losing progress.
    If opendata.org's actual download ever turns out to support HTTP
    Range requests on the raw file, a byte-offset-based resume could
    replace this and would be genuinely faster — not implemented here
    since that support isn't confirmed."""
    if not url:
        log.error("OPENDATA_SOURCE_URL not set — nothing to download.")
        return 0, False, skip_rows

    countries_lower = {c.strip().lower() for c in countries} if countries else None
    log.info(f"Downloading + filtering: {url}")
    if countries_lower:
        log.info(f"  filtering to {len(countries_lower)} countries")
    if skip_rows:
        log.info(f"  resuming: skipping the first {skip_rows:,} raw rows (already processed in a prior run)")

    kept = skipped_country = 0
    raw_row_index = 0  # counts every row seen this run, including skipped ones — this IS the Restart ID
    stopped_early = False
    time_budget_seconds = time_budget_minutes * 60 if time_budget_minutes else None
    start = time.monotonic()

    try:
        text_stream = _open_streaming_source(url)
        reader = csv.reader(text_stream)
        header = next(reader, None)
    except Exception as e:
        # str(e) can be EMPTY for some exceptions (MemoryError raised by
        # the allocator, for one) — falling back to repr()/type name means
        # the log line always says something instead of a bare trailing
        # colon (see _open_streaming_source's zip-OOM fix for the actual
        # case this happened).
        log.error(f"Failed to open/read source: {e or repr(e) or type(e).__name__}")
        return 0, False, skip_rows
    if not header:
        log.error("Source is empty (no header row).")
        return 0, False, skip_rows
    header_lower = [h.strip().lower() for h in header]
    name_i = _col_index(header_lower, NAME_COLS)
    domain_i = _col_index(header_lower, DOMAIN_COLS)
    country_i = _col_index(header_lower, COUNTRY_COLS)
    if name_i is None or domain_i is None:
        log.error(f"Couldn't find a name/domain column in the header: {header}")
        return 0, False, skip_rows
    log.info(f"  columns: name='{header[name_i]}' domain='{header[domain_i]}'"
              + (f" country='{header[country_i]}'" if country_i is not None else " (no country column)"))

    # Append + no header-rewrite when resuming, so a resumed run's output
    # concatenates cleanly onto what earlier runs already wrote.
    resuming = skip_rows > 0 and os.path.exists(output_path)
    file_mode = "a" if resuming else "w"

    try:
        with open(output_path, file_mode, newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            if not resuming:
                writer.writerow(["name", "domain", "country"])

            for row in reader:
                raw_row_index += 1
                if raw_row_index <= skip_rows:
                    continue  # already processed in a prior run — see docstring

                if time_budget_seconds and time.monotonic() - start >= time_budget_seconds:
                    # This row was READ from the stream but not yet
                    # evaluated/written — "un-count" it so the Restart ID
                    # points at it, not past it. Otherwise it would be
                    # silently skipped forever: skip_rows=raw_row_index on
                    # the next run would skip straight past an unprocessed
                    # row, permanently losing it.
                    raw_row_index -= 1
                    stopped_early = True
                    log.warning(f"Time budget ({time_budget_minutes}min) reached at raw row "
                                f"{raw_row_index:,} — stopping here. To resume, set the Restart ID "
                                f"input to {raw_row_index} on the next seed run.")
                    break
                if row_limit and (raw_row_index - skip_rows) > row_limit:
                    raw_row_index -= 1  # same "un-count" reasoning as above
                    break

                if (raw_row_index - skip_rows) % PROGRESS_EVERY == 0:
                    elapsed = time.monotonic() - start
                    log.info(f"  ...scanned {raw_row_index - skip_rows:,} new rows this run "
                              f"({(raw_row_index - skip_rows) / max(elapsed, 0.001):,.0f} rows/sec), "
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
        log.error(f"Failed mid-stream at raw row {raw_row_index:,}, {kept:,} written this run: "
                  f"{e or repr(e) or type(e).__name__}. "
                  f"To resume, set the Restart ID input to {raw_row_index} on the next seed run.")
        # Whatever was written before the failure is still a valid partial
        # file — don't delete it — but the caller must still treat this as
        # a failed run (see main()'s exit-code handling).
        return kept, True, raw_row_index

    elapsed = time.monotonic() - start
    new_rows = raw_row_index - skip_rows
    log.info(f"Done: {new_rows:,} new rows scanned this run, {skipped_country:,} skipped by country, "
             f"{kept:,} written to {output_path} ({kept / max(new_rows, 1) * 100:.2f}%), "
             f"{elapsed:.0f}s ({new_rows / max(elapsed, 0.001):,.0f} rows/sec). "
             f"Total raw rows processed so far (this + prior runs): {raw_row_index:,}.")
    return kept, stopped_early, raw_row_index


def main():
    parser = argparse.ArgumentParser(description="OpenData.org Organizations — download + stream-filter")
    parser.add_argument("--output", default="opendata_organizations_filtered.csv")
    parser.add_argument("--row-limit", type=int, default=0, help="0 = no cap; nonzero is for testing only")
    parser.add_argument("--country", action="append", default=None,
                         help="Only keep this country value (repeatable). Default: DEFAULT_COUNTRIES.")
    parser.add_argument("--all-countries", action="store_true",
                         help="Disable the country filter — keep every country, including blank ones.")
    parser.add_argument("--skip-rows", type=int, default=0,
                         help="Restart ID — raw source rows to skip (already processed in a prior "
                              "run). Get this number from a previous run's 'stopping here' or "
                              "'Total raw rows processed' log line. 0 = start from the beginning.")
    parser.add_argument("--time-budget-minutes", type=int, default=0,
                         help="Self-stop gracefully after this many minutes and log a Restart ID for "
                              "next time, instead of risking a hard kill mid-write at GitHub's "
                              "360-minute job timeout. 0 = no internal budget (run to completion).")
    args = parser.parse_args()

    if args.country:
        countries = set(args.country)
    elif args.all_countries:
        countries = None
    else:
        countries = DEFAULT_COUNTRIES

    kept, stopped_early, raw_row_index = stream_filter(
        OPENDATA_SOURCE_URL, args.output, countries, args.row_limit,
        args.skip_rows, args.time_budget_minutes)

    if kept == 0 and not stopped_early:
        log.error("No rows written — aborting with a non-zero exit so the CI job shows red "
                  "instead of silently uploading an empty/missing Release asset.")
        sys.exit(1)
    if stopped_early:
        log.warning(f"Run stopped before finishing the source — Restart ID for next run: {raw_row_index}")
        # Non-fatal on purpose: whatever was filtered/kept so far is still
        # valid and worth uploading — the workflow's split/upload steps
        # run regardless, same as a clean finish. The Restart ID above is
        # what tells you it isn't the FULL dataset yet.


if __name__ == "__main__":
    main()
