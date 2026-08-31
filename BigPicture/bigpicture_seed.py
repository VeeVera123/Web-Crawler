"""
BIGPICTURE SEED — one-time (or periodically re-run) download + stream-
filter of BigPicture.io's free companies dataset, hosted publicly on
HuggingFace (no login/API key needed — unlike app.bigpicture.io's own
signup-gated download, which requires a business email):
  https://huggingface.co/datasets/bigpictureio/companies-2023-q4-sm
17.2M companies, a single companies-2023-q4-sm.csv.gz (~629MB compressed).
Columns: handle, name, website, industry, size, type, founded, city,
state, country_code — the 'size' column (an employee-count RANGE string
like "501-1K", "1-10", "10K+") is exactly what kaggle_probe.py's PDL seed
also carries and OpenData/Common Crawl don't, which is why this source —
like Kaggle — is allowed to feed BOTH archive_i (known-ATS hits) AND
archive_ii (in-house/unsupported career pages): a no-ATS hit from a
company already confirmed to be above MIN_COMPANY_SIZE isn't the same
kind of noise a no-signal source's hit would be. See node.py's
crawl_batch docstring / opendata_probe.py's capture_inhouse comment for
the full reasoning.

THIS SCRIPT ONLY DOWNLOADS + FILTERS (by country + a usable domain). It
does NOT filter by employee size — that used to happen here, but
crawl-eligibility must never be size-gated (every company in the target
countries needs an ATS check; archive_i is never size-gated). The size
column is instead carried straight through into the output CSV so
bigpicture_probe.py can use it later, purely to decide which no-ATS-match
companies are allowed to become archive_ii candidates — see node.py's
crawl_batch docstring (capture_inhouse_domains) for the full mechanism.

It does not crawl (see node.py / bigpicture_probe.py for that) and it
does not touch GitHub Releases itself — bigpicture.yml's `seed` job
uploads/splits this script's one output CSV into <2GB Release asset
chunks with plain shell (`split` + `gh release upload`), the exact same
split opendata.yml already uses for opendata_seed.py's output.

Much simpler than opendata_seed.py: the source is a single plain gzip CSV
(never a zip of many shard files), so there's no zip/JSON-shard handling
at all here — streamed directly via requests + gzip.GzipFile, nothing
ever buffered fully in memory or written to a temp file first.

Usage:
    python bigpicture_seed.py --output bigpicture_companies_filtered.csv
    python bigpicture_seed.py --skip-rows 0 --time-budget-minutes 340
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

import requests
from dotenv import load_dotenv

load_dotenv()
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)                       # discovery.py lives at repo root on GitHub
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # fallback, for a local Main/ layout
from discovery import SKIP_SLUGS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                     datefmt="%H:%M:%S")
log = logging.getLogger("bigpicture_seed")

# Public, no-auth HuggingFace URL — hardcoded as the default since (unlike
# OPENDATA_SOURCE_URL) there's no signup-gated secret needed at all; still
# overridable via env var in case BigPicture publishes a newer quarterly file.
BIGPICTURE_SOURCE_URL = os.environ.get(
    "BIGPICTURE_SOURCE_URL",
    "https://huggingface.co/datasets/bigpictureio/companies-2023-q4-sm/resolve/main/companies-2023-q4-sm.csv.gz",
)
PROGRESS_EVERY = 2_000_000  # raw records scanned between progress log lines

NAME_COLS = ("name",)
DOMAIN_COLS = ("website", "domain")
COUNTRY_COLS = ("country_code", "country")
SIZE_COLS = ("size",)

# ISO 3166-1 alpha-2 codes — country_code is a 2-letter code here (unlike
# kaggle_probe.py's/opendata_seed.py's full lowercase country NAMES), so
# this is the same underlying country list re-expressed as codes, not a
# different policy. Keep in sync with those two if the list changes.
DEFAULT_COUNTRY_CODES = {
    "us", "gb", "ca", "au", "ie", "nz", "sg",
    "nl", "no", "se", "dk", "fi", "at", "be",
    "is", "lu", "fr", "de",
}

_SIZE_NUM_RE = re.compile(r"[\d,]+")


def _err(e: Exception) -> str:
    """Same fix as opendata_seed.py's _err() — str(e) can be empty for
    some exceptions, and an exception object is always truthy, so a naive
    `e or repr(e)` never actually falls through. See that file's docstring
    for the full story."""
    return str(e) or repr(e) or type(e).__name__


def _col_index(header_lower: list[str], aliases: tuple[str, ...]) -> int | None:
    for alias in aliases:
        if alias in header_lower:
            return header_lower.index(alias)
    return None


def _open_row_source(url: str):
    """Streams the gzip CSV directly — no temp file, no zip handling (see
    module docstring for why this is simpler than opendata_seed.py's
    version). Returns a (name, domain, country, size)-quadruple generator,
    or None (having already logged why) if the header can't be parsed."""
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    raw = r.raw
    raw.decode_content = True  # let urllib3 handle a gzip Content-Encoding transparently too
    if url.endswith(".gz"):
        text = io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf-8", errors="ignore")
    else:
        text = io.TextIOWrapper(raw, encoding="utf-8", errors="ignore")

    reader = csv.reader(text)
    header = next(reader, None)
    if not header:
        log.error("Source is empty (no header row).")
        return None
    header_lower = [h.strip().lower() for h in header]
    name_i = _col_index(header_lower, NAME_COLS)
    domain_i = _col_index(header_lower, DOMAIN_COLS)
    country_i = _col_index(header_lower, COUNTRY_COLS)
    size_i = _col_index(header_lower, SIZE_COLS)
    if name_i is None or domain_i is None:
        log.error(f"Couldn't find a name/domain column in the header: {header}")
        return None
    if size_i is None:
        log.warning(f"No size column found in the header ({header}) — proceeding WITHOUT "
                    f"the employee-size filter this run.")
    log.info(f"  columns: name='{header[name_i]}' domain='{header[domain_i]}'"
              + (f" country='{header[country_i]}'" if country_i is not None else " (no country column)")
              + (f" size='{header[size_i]}'" if size_i is not None else ""))

    def _gen():
        for row in reader:
            if len(row) <= domain_i or len(row) <= name_i:
                continue
            name = row[name_i].strip()
            domain = row[domain_i].strip()
            country = (row[country_i].strip() if country_i is not None and len(row) > country_i else "")
            size = (row[size_i].strip() if size_i is not None and len(row) > size_i else "")
            yield name, domain, country, size

    return _gen()


# ── Shared filter/write loop (mirrors opendata_seed.py's stream_filter) ──

def stream_filter(url: str, output_path: str, country_codes: set[str] | None,
                   row_limit: int = 0, skip_rows: int = 0,
                   time_budget_minutes: int = 0) -> tuple[int, bool, int]:
    """Same shape/semantics as opendata_seed.py's stream_filter() — see
    that function's docstring for the full Restart-ID/resume explanation.
    No employee-size filtering here (see module docstring — that used to
    happen here and wrongly gated crawl-eligibility; the raw size string
    is now just carried through into the output CSV's 'size' column for
    bigpicture_probe.py to use post-crawl). Country matching is against
    2-letter codes instead of full lowercase names."""
    if not url:
        log.error("BIGPICTURE_SOURCE_URL not set — nothing to download.")
        return 0, False, skip_rows

    codes_upper = {c.strip().upper() for c in country_codes} if country_codes else None
    log.info(f"Downloading + filtering: {url}")
    if codes_upper:
        log.info(f"  filtering to {len(codes_upper)} country codes")
    if skip_rows:
        log.info(f"  resuming: skipping the first {skip_rows:,} records (already processed in a prior run)")

    kept = skipped_country = 0
    raw_row_index = 0
    stopped_early = False
    time_budget_seconds = time_budget_minutes * 60 if time_budget_minutes else None
    start = time.monotonic()

    try:
        row_source = _open_row_source(url)
    except Exception as e:
        log.error(f"Failed to open/read source: {_err(e)}")
        return 0, False, skip_rows
    if row_source is None:
        return 0, False, skip_rows

    resuming = skip_rows > 0 and os.path.exists(output_path)
    file_mode = "a" if resuming else "w"

    try:
        with open(output_path, file_mode, newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            if not resuming:
                writer.writerow(["name", "domain", "country", "size"])

            for name, domain, row_country, row_size in row_source:
                raw_row_index += 1
                if raw_row_index <= skip_rows:
                    continue

                if time_budget_seconds and time.monotonic() - start >= time_budget_seconds:
                    raw_row_index -= 1  # un-count — see opendata_seed.py's identical comment
                    stopped_early = True
                    log.warning(f"Time budget ({time_budget_minutes}min) reached at record "
                                f"{raw_row_index:,} — stopping here. To resume, set the Restart ID "
                                f"input to {raw_row_index} on the next seed run.")
                    break
                if row_limit and (raw_row_index - skip_rows) > row_limit:
                    raw_row_index -= 1
                    break

                if (raw_row_index - skip_rows) % PROGRESS_EVERY == 0:
                    elapsed = time.monotonic() - start
                    log.info(f"  ...scanned {raw_row_index - skip_rows:,} new records this run "
                              f"({(raw_row_index - skip_rows) / max(elapsed, 0.001):,.0f} rec/sec), "
                              f"{kept:,} kept so far")

                if codes_upper:
                    row_country_upper = row_country.strip().upper()
                    if row_country_upper not in codes_upper:
                        skipped_country += 1
                        continue
                    row_country_lower = row_country_upper.lower()
                else:
                    row_country_lower = row_country.strip().lower()

                name = (name or "").strip()
                domain = (domain or "").strip().lower()
                domain = re.sub(r"^https?://", "", domain).split("/")[0].strip()
                if not (name and domain and "." in domain and domain not in SKIP_SLUGS):
                    continue

                writer.writerow([name, domain, row_country_lower, (row_size or "").strip()])
                kept += 1
    except Exception as e:
        log.error(f"Failed mid-stream at record {raw_row_index:,}, {kept:,} written this run: "
                  f"{_err(e)}. To resume, set the Restart ID input to {raw_row_index} on the next "
                  f"seed run.")
        return kept, True, raw_row_index

    elapsed = time.monotonic() - start
    new_rows = raw_row_index - skip_rows
    log.info(f"Done: {new_rows:,} new records scanned this run, {skipped_country:,} skipped by country, "
             f"{kept:,} written to {output_path} ({kept / max(new_rows, 1) * 100:.2f}%), "
             f"{elapsed:.0f}s ({new_rows / max(elapsed, 0.001):,.0f} rec/sec). "
             f"Total records processed so far (this + prior runs): {raw_row_index:,}.")
    return kept, stopped_early, raw_row_index


def main():
    parser = argparse.ArgumentParser(description="BigPicture.io companies dataset — download + stream-filter")
    parser.add_argument("--output", default="bigpicture_companies_filtered.csv")
    parser.add_argument("--row-limit", type=int, default=0, help="0 = no cap; nonzero is for testing only")
    parser.add_argument("--country", action="append", default=None,
                         help="Only keep this 2-letter country code (repeatable). Default: DEFAULT_COUNTRY_CODES.")
    parser.add_argument("--all-countries", action="store_true",
                         help="Disable the country filter — keep every country, including blank ones.")
    parser.add_argument("--skip-rows", type=int, default=0,
                         help="Restart ID — records to skip (already processed in a prior run).")
    parser.add_argument("--time-budget-minutes", type=int, default=0,
                         help="Self-stop gracefully after this many minutes and log a Restart ID. "
                              "0 = no internal budget (run to completion).")
    args = parser.parse_args()

    if args.country:
        country_codes = set(args.country)
    elif args.all_countries:
        country_codes = None
    else:
        country_codes = DEFAULT_COUNTRY_CODES

    kept, stopped_early, raw_row_index = stream_filter(
        BIGPICTURE_SOURCE_URL, args.output, country_codes,
        args.row_limit, args.skip_rows, args.time_budget_minutes)

    if kept == 0 and not stopped_early:
        log.error("No rows written — aborting with a non-zero exit so the CI job shows red "
                  "instead of silently uploading an empty/missing Release asset.")
        sys.exit(1)
    if stopped_early:
        log.warning(f"Run stopped before finishing the source — Restart ID for next run: {raw_row_index}")


if __name__ == "__main__":
    main()
