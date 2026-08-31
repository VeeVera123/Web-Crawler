"""
PEOPLE DATA LABS SEED — one-time (or periodically re-run) download +
stream-filter of PDL's Free Company Dataset, using the direct CSV link
(http://pdl.ai/company-dataset-csv), which needs no signup form/email
gate the way peopledatalabs.com/company-dataset's landing page does.
Confirmed schema (docs.peopledatalabs.com/docs/free-company-dataset):
id, name, website, country, locality, region, industry, size, founded,
linkedin_url. 'size' is a canonical range string ("501-1000", "10001+"),
exactly what people_data_labs_probe.py's own SIZE_COLS/_size_floor
already expect — no format work needed there.

THIS SCRIPT ONLY DOWNLOADS + FILTERS (by country + a usable domain). It
does NOT filter by employee size — crawl-eligibility must never be
size-gated (every company in the target countries needs an ATS check;
archive_i is never size-gated). The size column is carried straight
through into the output CSV so people_data_labs_probe.py can use it
later, purely to decide which no-ATS-match companies are allowed to
become archive_ii candidates — see node.py's crawl_batch docstring
(capture_inhouse_domains) for the full mechanism. Same shape as
bigpicture_seed.py/opendata_seed.py.

It does not crawl (see node.py / people_data_labs_probe.py for that) and
it does not touch GitHub Releases itself — people_data_labs.yml's `seed`
job splits this script's one output CSV into <2GB Release asset chunks
with plain shell (`split` + `gh release upload`), same as
opendata.yml/bigpicture.yml.

Streamed directly via requests — nothing ever buffered fully in memory or
written to a temp file first. The response is plain CSV; requests/urllib3
already transparently gunzips a proper `Content-Encoding: gzip` response
via decode_content=True if PDL ever adds one.

Usage:
    python people_data_labs_seed.py --output pdl_companies_filtered.csv
    python people_data_labs_seed.py --skip-rows 0 --time-budget-minutes 340
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
log = logging.getLogger("people_data_labs_seed")

# Direct, no-signup CSV link — bypasses peopledatalabs.com/company-dataset's
# gated landing-page form entirely. Still overridable via env var in case
# PDL moves/rotates this URL.
PDL_SOURCE_URL = os.environ.get("PDL_SOURCE_URL", "http://pdl.ai/company-dataset-csv")
PROGRESS_EVERY = 2_000_000  # raw records scanned between progress log lines

NAME_COLS = ("name", "company_name")
DOMAIN_COLS = ("website", "domain")
COUNTRY_COLS = ("country",)
SIZE_COLS = ("size",)

# Full lowercase country names — matches people_data_labs_probe.py's own
# DEFAULT_COUNTRIES exactly, since both read the same PDL 'country' field.
DEFAULT_COUNTRIES = {
    "united states", "united kingdom", "canada", "australia",
    "ireland", "new zealand", "singapore",
    "netherlands", "norway", "sweden", "denmark", "finland", "austria", "belgium",
    "iceland", "luxembourg",
    "france", "germany",
}

_SIZE_NUM_RE = re.compile(r"[\d,]+")


def _err(e: Exception) -> str:
    """Same fix as opendata_seed.py's/bigpicture_seed.py's _err() — str(e)
    can be empty for some exceptions, and an exception object is always
    truthy, so a naive `e or repr(e)` never actually falls through."""
    return str(e) or repr(e) or type(e).__name__


def _col_index(header_lower: list[str], aliases: tuple[str, ...]) -> int | None:
    for alias in aliases:
        if alias in header_lower:
            return header_lower.index(alias)
    return None


def _open_row_source(url: str):
    """Streams the CSV directly — no temp file. requests/urllib3 already
    transparently gunzips a proper `Content-Encoding: gzip` response via
    decode_content=True, which covers the normal case; a `.gz`-suffixed
    URL (not expected here, but see bigpicture_seed.py's identical check)
    is handled explicitly too. Returns a (name, domain, country, size)-
    quadruple generator, or None (having already logged why) if the
    header can't be parsed."""
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    raw = r.raw
    raw.decode_content = True
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
        log.warning(f"No size column found in the header ({header}) — proceeding WITHOUT it.")
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


# ── Shared filter/write loop (mirrors bigpicture_seed.py/opendata_seed.py) ──

def stream_filter(url: str, output_path: str, countries: set[str] | None,
                   row_limit: int = 0, skip_rows: int = 0,
                   time_budget_minutes: int = 0) -> tuple[int, bool, int]:
    """Same shape/semantics as bigpicture_seed.py's stream_filter() — see
    that function's docstring for the full Restart-ID/resume explanation.
    No employee-size filtering here (crawl-eligibility is never
    size-gated) — the raw size string is carried through into the output
    CSV's 'size' column for people_data_labs_probe.py to use post-crawl.
    Country matching is against full lowercase names, same as PDL's own
    field."""
    if not url:
        log.error("PDL_SOURCE_URL not set — nothing to download.")
        return 0, False, skip_rows

    countries_lower = {c.strip().lower() for c in countries} if countries else None
    log.info(f"Downloading + filtering: {url}")
    if countries_lower:
        log.info(f"  filtering to {len(countries_lower)} countries")
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
                    raw_row_index -= 1  # un-count — see bigpicture_seed.py's identical comment
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

                row_country_lower = (row_country or "").strip().lower()
                if countries_lower and row_country_lower not in countries_lower:
                    skipped_country += 1
                    continue

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
    parser = argparse.ArgumentParser(description="PDL Free Company Dataset — download + stream-filter")
    parser.add_argument("--output", default="pdl_companies_filtered.csv")
    parser.add_argument("--row-limit", type=int, default=0, help="0 = no cap; nonzero is for testing only")
    parser.add_argument("--country", action="append", default=None,
                         help="Only keep this country name (repeatable). Default: DEFAULT_COUNTRIES.")
    parser.add_argument("--all-countries", action="store_true",
                         help="Disable the country filter — keep every country, including blank ones.")
    parser.add_argument("--skip-rows", type=int, default=0,
                         help="Restart ID — records to skip (already processed in a prior run).")
    parser.add_argument("--time-budget-minutes", type=int, default=0,
                         help="Self-stop gracefully after this many minutes and log a Restart ID. "
                              "0 = no internal budget (run to completion).")
    args = parser.parse_args()

    if args.country:
        countries = set(args.country)
    elif args.all_countries:
        countries = None
    else:
        countries = DEFAULT_COUNTRIES

    kept, stopped_early, raw_row_index = stream_filter(
        PDL_SOURCE_URL, args.output, countries,
        args.row_limit, args.skip_rows, args.time_budget_minutes)

    if kept == 0 and not stopped_early:
        log.error("No rows written — aborting with a non-zero exit so the CI job shows red "
                  "instead of silently uploading an empty/missing Release asset.")
        sys.exit(1)
    if stopped_early:
        log.warning(f"Run stopped before finishing the source — Restart ID for next run: {raw_row_index}")


if __name__ == "__main__":
    main()
