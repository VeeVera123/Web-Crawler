"""
OPENDATA SEED — one-time (or periodically re-run) download + stream-filter
of opendata.org's "Organizations" dataset (86,690,782 records, ~7.5GB).

THIS SCRIPT ONLY DOWNLOADS + FILTERS. It does not crawl (see node.py /
opendata_probe.py for that) and it does not touch GitHub Releases itself.

SOURCE FORMAT (confirmed 2026-08 against the real Senzing Open Data
"Organizations" zip): NOT a single CSV. It's a zip of 500 shard files
named like "Organization/bq_organization_000000000123.json" — a BigQuery
export split into many same-shaped pieces.

Usage:
    export OPENDATA_SOURCE_URL=https://.../organizations.zip
    python opendata_seed.py --output opendata_organizations_filtered.csv
"""

import argparse
import csv
import gzip
import io
import json
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

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "Main"))
from discovery import SKIP_SLUGS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("opendata_seed")

OPENDATA_SOURCE_URL = os.environ.get("OPENDATA_SOURCE_URL", "")
PROGRESS_EVERY = 2_000_000

NAME_COLS = ("name", "company_name", "organization_name")
DOMAIN_COLS = ("domain", "website", "domain_name", "website_domain")
COUNTRY_COLS = ("country", "country_code")

NAME_ALIASES_JSON = {"name", "name_org", "primary_name_org", "org_name", "business_name",
                     "legal_name", "company_name", "organization_name", "entity_name",
                     "full_name", "display_name"}
DOMAIN_ALIASES_JSON = {"domain", "website", "website_address", "url", "web", "homepage",
                       "web_address", "site", "webaddress"}
COUNTRY_ALIASES_JSON = {"country", "addr_country", "primary_country", "nationality",
                        "registration_country", "nat_country", "country_code",
                        "country_of_registration"}

DEFAULT_COUNTRIES = {
    "united states", "united kingdom", "canada", "australia",
    "ireland", "new zealand", "singapore",
    "netherlands", "norway", "sweden", "denmark", "finland", "austria", "belgium",
    "iceland", "luxembourg",
    "france", "germany",
}

# FIX 2026-08: Senzing Open Data uses ISO codes (US, GB, DE) or abbreviations.
# We map them to the full names expected by DEFAULT_COUNTRIES and opendata_probe.py.
COUNTRY_NORMALIZATION = {
    "us": "united states", "usa": "united states", "united states of america": "united states",
    "gb": "united kingdom", "gbr": "united kingdom", "uk": "united kingdom", "great britain": "united kingdom", "england": "united kingdom",
    "ca": "canada", "can": "canada",
    "au": "australia", "aus": "australia",
    "ie": "ireland", "irl": "ireland",
    "nz": "new zealand", "nzl": "new zealand",
    "sg": "singapore", "sgp": "singapore",
    "nl": "netherlands", "nld": "netherlands", "holland": "netherlands",
    "no": "norway", "nor": "norway",
    "se": "sweden", "swe": "sweden",
    "dk": "denmark", "dnk": "denmark",
    "fi": "finland", "fin": "finland",
    "at": "austria", "aut": "austria",
    "be": "belgium", "bel": "belgium",
    "is": "iceland", "isl": "iceland",
    "lu": "luxembourg", "lux": "luxembourg",
    "fr": "france", "fra": "france",
    "de": "germany", "deu": "germany", "deutschland": "germany",
}


def _err(e: Exception) -> str:
    return str(e) or repr(e) or type(e).__name__


# ── CSV row source ─────────────────────────────────────────────────────
def _col_index(header_lower: list[str], aliases: tuple[str, ...]) -> int | None:
    for alias in aliases:
        if alias in header_lower:
            return header_lower.index(alias)
    return None


def _csv_row_source(text_stream):
    reader = csv.reader(text_stream)
    header = next(reader, None)
    if not header:
        log.error("Source is empty (no header row).")
        return None
    header_lower = [h.strip().lower() for h in header]
    name_i = _col_index(header_lower, NAME_COLS)
    domain_i = _col_index(header_lower, DOMAIN_COLS)
    country_i = _col_index(header_lower, COUNTRY_COLS)
    if name_i is None or domain_i is None:
        log.error(f"Couldn't find a name/domain column in the header: {header}")
        return None
    log.info(f"  columns: name='{header[name_i]}' domain='{header[domain_i]}'"
             + (f" country='{header[country_i]}'" if country_i is not None else " (no country column)"))

    def _gen():
        for row in reader:
            if len(row) <= domain_i or len(row) <= name_i:
                continue
            name = row[name_i].strip()
            domain = row[domain_i].strip()
            country = (row[country_i].strip() if country_i is not None and len(row) > country_i else "")
            yield name, domain, country

    return _gen()


# ── JSON row source ────────────────────────────────────────────────────
def _normalize_key(k: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(k).lower())


def _deep_find(obj, aliases: set, max_depth: int = 4, _path: tuple = ()):
    if max_depth < 0:
        return None
    norm_aliases = {_normalize_key(a) for a in aliases}
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and v.strip() and _normalize_key(k) in norm_aliases:
                return _path + (k,), v.strip()
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                found = _deep_find(v, aliases, max_depth - 1, _path + (k,))
                if found:
                    return found
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            found = _deep_find(item, aliases, max_depth - 1, _path + (i,))
            if found:
                return found
    return None


def _get_by_path(obj, path: tuple) -> str:
    cur = obj
    try:
        for p in path:
            cur = cur[p]
        return cur.strip() if isinstance(cur, str) else ""
    except (KeyError, IndexError, TypeError):
        return ""


def _detect_json_shard_mode(zip_path: str, first_name: str) -> str:
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(first_name) as fh:
            first_line = io.TextIOWrapper(fh, encoding="utf-8", errors="ignore").readline()
            try:
                json.loads(first_line)
                return "lines"
            except (json.JSONDecodeError, ValueError):
                return "whole"


def _iter_zip_json_records(zip_path: str, json_names: list[str], mode: str):
    for name in sorted(json_names):
        with zipfile.ZipFile(zip_path) as zf:
            if mode == "lines":
                with zf.open(name) as fh:
                    text = io.TextIOWrapper(fh, encoding="utf-8", errors="ignore")
                    for line in text:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            yield json.loads(line)
                        except (json.JSONDecodeError, ValueError):
                            continue
            else:
                with zf.open(name) as fh:
                    try:
                        data = json.load(fh)
                    except (json.JSONDecodeError, ValueError):
                        continue
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        yield item


def _json_row_source(zip_path: str, json_names: list[str]):
    SCAN_LIMIT = 100
    mode = _detect_json_shard_mode(zip_path, sorted(json_names)[0])
    log.info(f"  JSON shard format detected: {mode} ({len(json_names)} files)")
    
    records = _iter_zip_json_records(zip_path, json_names, mode)
    
    name_path = domain_path = country_path = None
    name_val = domain_val = country_val = ""
    first_record = None
    records_scanned = 0
    
    for rec in records:
        if first_record is None:
            first_record = rec
        if name_path is None:
            name_hit = _deep_find(rec, NAME_ALIASES_JSON)
            if name_hit:
                name_path, name_val = name_hit
        if domain_path is None:
            domain_hit = _deep_find(rec, DOMAIN_ALIASES_JSON)
            if domain_hit:
                domain_path, domain_val = domain_hit
        if country_path is None:
            country_hit = _deep_find(rec, COUNTRY_ALIASES_JSON)
            if country_hit:
                country_path, country_val = country_hit
        
        records_scanned += 1
        if name_path and domain_path and records_scanned >= SCAN_LIMIT:
            break
        if name_path and domain_path and country_path:
            break
            
    if not name_path or not domain_path:
        log.error(
            f"Couldn't confidently find a name/domain field in the first {records_scanned} JSON records. "
            f"name match: {name_path}, domain match: {domain_path}, country match: {country_path}. "
            f"First record: {json.dumps(first_record)[:3000]}")
        return None
        
    log.info(f"  name field: {name_path} = '{name_val}' (found at record #{records_scanned})")
    log.info(f"  domain field: {domain_path} = '{domain_val}'")
    if country_path:
        log.info(f"  country field: {country_path} = '{country_val}'")
    else:
        log.info("  country field: none found")
        
    records = _iter_zip_json_records(zip_path, json_names, mode)
    
    def _gen():
        for rec in records:
            yield (_get_by_path(rec, name_path),
                   _get_by_path(rec, domain_path),
                   _get_by_path(rec, country_path) if country_path else "")
    return _gen()


# ── Download + dispatch ─────────────────────────────────────────────────
def _download_to_tempfile(url: str) -> tuple[str, int]:
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
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
    return tmp.name, downloaded


def _open_row_source(url: str):
    if url.endswith(".zip"):
        zip_path, _ = _download_to_tempfile(url)
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            csv_names = [n for n in names if n.lower().endswith((".csv", ".tsv"))]
            json_names = [n for n in names if n.lower().endswith(".json")]
            if csv_names:
                inner_name = csv_names[0]
                log.info(f"  Reading '{inner_name}' from the zip ({len(names)} total entries)")
                with zipfile.ZipFile(zip_path) as zf:
                    text = io.TextIOWrapper(zf.open(inner_name), encoding="utf-8", errors="ignore")
                    return _csv_row_source(text)
            if json_names:
                log.info(f"  {len(json_names)} JSON shard files found in the zip")
                return _json_row_source(zip_path, json_names)
            preview = names[:30]
            more = f" ...and {len(names) - 30} more" if len(names) > 30 else ""
            log.error(f"No .csv/.tsv/.json files found inside the zip. Contents ({len(names)} entries): "
                      f"{preview}{more}")
            return None
    
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    raw = r.raw
    raw.decode_content = True
    if url.endswith(".gz"):
        text = io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf-8", errors="ignore")
    else:
        text = io.TextIOWrapper(raw, encoding="utf-8", errors="ignore")
    return _csv_row_source(text)


# ── Shared filter/write loop ────────────────────────────────────────────
def stream_filter(url: str, output_path: str, countries: set[str] | None,
                  row_limit: int = 0, skip_rows: int = 0,
                  time_budget_minutes: int = 0) -> tuple[int, bool, int]:
    if not url:
        log.error("OPENDATA_SOURCE_URL not set — nothing to download.")
        return 0, False, skip_rows
    
    countries_lower = {c.strip().lower() for c in countries} if countries else None
    log.info(f"Downloading + filtering: {url}")
    if countries_lower:
        log.info(f"  filtering to {len(countries_lower)} countries")
    if skip_rows:
        log.info(f"  resuming: skipping the first {skip_rows:,} records")
    
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
                writer.writerow(["name", "domain", "country"])
            
            for name, domain, row_country in row_source:
                raw_row_index += 1
                
                if raw_row_index <= skip_rows:
                    continue
                
                if time_budget_seconds and time.monotonic() - start >= time_budget_seconds:
                    raw_row_index -= 1
                    stopped_early = True
                    log.warning(f"Time budget reached at record {raw_row_index:,}")
                    break
                
                if row_limit and (raw_row_index - skip_rows) > row_limit:
                    raw_row_index -= 1
                    break
                
                if (raw_row_index - skip_rows) % PROGRESS_EVERY == 0:
                    elapsed = time.monotonic() - start
                    log.info(f"  ...scanned {raw_row_index - skip_rows:,} new records this run "
                             f"({(raw_row_index - skip_rows) / max(elapsed, 0.001):,.0f} rec/sec), "
                             f"{kept:,} kept so far")
                
                # FIX: Normalize ISO codes (US, GB, DE) to full names (united states, etc.)
                row_country_raw = (row_country or "").strip().lower()
                normalized_country = COUNTRY_NORMALIZATION.get(row_country_raw, row_country_raw)

                if countries_lower:
                    if normalized_country not in countries_lower:
                        skipped_country += 1
                        continue
                else:
                    normalized_country = row_country_raw

                name = (name or "").strip()
                domain = (domain or "").strip().lower()
                domain = re.sub(r"^https?://", "", domain).split("/")[0].strip()

                if not (name and domain and "." in domain and domain not in SKIP_SLUGS):
                    continue

                writer.writerow([name, domain, normalized_country])
                kept += 1
    
    except Exception as e:
        log.error(f"Failed mid-stream at record {raw_row_index:,}, {kept:,} written: {_err(e)}")
        return kept, True, raw_row_index
    
    elapsed = time.monotonic() - start
    new_rows = raw_row_index - skip_rows
    log.info(f"Done: {new_rows:,} new records scanned, {skipped_country:,} skipped by country, "
             f"{kept:,} written to {output_path} ({kept / max(new_rows, 1) * 100:.2f}%), "
             f"{elapsed:.0f}s. Total processed: {raw_row_index:,}.")
    return kept, stopped_early, raw_row_index


def main():
    parser = argparse.ArgumentParser(description="OpenData.org Organizations — download + stream-filter")
    parser.add_argument("--output", default="opendata_organizations_filtered.csv")
    parser.add_argument("--row-limit", type=int, default=0)
    parser.add_argument("--country", action="append", default=None)
    parser.add_argument("--all-countries", action="store_true")
    parser.add_argument("--skip-rows", type=int, default=0)
    parser.add_argument("--time-budget-minutes", type=int, default=0)
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
        log.error("No rows written — aborting.")
        sys.exit(1)
    
    if stopped_early:
        log.warning(f"Run stopped early — Restart ID for next run: {raw_row_index}")


if __name__ == "__main__":
    main()
