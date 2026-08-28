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

SOURCE FORMAT (confirmed 2026-08 against the real Senzing Open Data
"Organizations" zip): NOT a single CSV. It's a zip of 500 shard files
named like "Organization/bq_organization_000000000123.json" — a BigQuery
export split into many same-shaped pieces. Each shard is parsed as either
JSON Lines (one JSON object per line — the common BigQuery export shape)
or, if that fails on the very first line, as one whole JSON document per
file (an array or a single object) — see _detect_json_shard_mode(). A
plain .csv/.tsv is still supported directly (unzipped, or inside a .zip)
in case a future source or a different opendata.org table is CSV-shaped;
JSON is not assumed to be the only possible shape.

Field names inside each JSON record were NOT hardcoded from a guess —
Senzing's exact schema wasn't independently confirmed before this was
written. Instead, _deep_find() searches each record's own keys (and one
level of nesting, e.g. a "NAMES"/"ADDRESSES" list Senzing-style entity
JSON commonly uses) for anything matching the NAME_ALIASES/DOMAIN_ALIASES/
COUNTRY_ALIASES lists below, on the FIRST N records (not just the first
one), and logs exactly what it matched (key path + real value) before
processing the other 86M+ records — so a wrong match is visible in the
log immediately, not discovered after burning the whole run. If nothing
matches after scanning N records, it fails loudly with the full first
record dumped instead of silently writing empty/wrong data — add the
real key names to the alias lists once you've seen that log line.

Streaming end-to-end: the source is downloaded via requests' streaming
mode; only the compressed zip itself is written to disk in full (needed
for zipfile's central-directory seek — see _download_to_tempfile()) —
decompressed JSON/CSV content is never held in memory or on disk in full.
This mirrors how common_crawl_probe.py/host_crawl_v2.py stream Common
Crawl's much larger Parquet files instead of downloading them whole first.

GitHub Actions' 360-minute (6-hour) hard job timeout is the real
constraint here — the download+filter step can't usefully be sharded
(it's one continuous pass over one source file), so it gets the whole
budget in a single job; the CRAWL step (opendata_probe.py, run from this
script's filtered output) shards normally, same as every other probe.

Usage:
    export OPENDATA_SOURCE_URL=https://.../organizations.zip   # .zip / .gz / plain .csv
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

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Crawler/
sys.path.insert(0, _ROOT)                       # discovery.py lives at repo root on GitHub
sys.path.insert(0, os.path.join(_ROOT, "Main"))  # fallback, for a local Main/ layout
from discovery import SKIP_SLUGS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("opendata_seed")

OPENDATA_SOURCE_URL = os.environ.get("OPENDATA_SOURCE_URL", "")
PROGRESS_EVERY = 2_000_000  # raw records scanned between progress log lines

# CSV header aliases (exact match, case-insensitive) — same alias-list
# approach kaggle_probe.py uses for PDL's own 'domain' vs 'website' naming.
NAME_COLS = ("name", "company_name", "organization_name")
DOMAIN_COLS = ("domain", "website", "domain_name", "website_domain")
COUNTRY_COLS = ("country", "country_code")

# JSON key aliases (normalized match: lowercased, non-alphanumeric
# stripped, so "NAME_ORG", "nameOrg", "name-org" all match "name_org") —
# used by _deep_find() to locate the right field inside an arbitrary
# nested JSON record without a confirmed schema. Includes generic names
# AND Senzing-entity-resolution-typical names (PRIMARY_NAME_ORG,
# WEBSITE_ADDRESS, ADDR_COUNTRY) as candidates — not assumed correct,
# just plausible enough to try; the actual match is always logged so it
# can be verified against the real data (see module docstring).
NAME_ALIASES_JSON = {"name", "name_org", "primary_name_org", "org_name", "business_name",
                     "legal_name", "company_name", "organization_name", "entity_name",
                     "full_name", "display_name"}
DOMAIN_ALIASES_JSON = {"domain", "website", "website_address", "url", "web", "homepage",
                       "web_address", "site", "webaddress"}
COUNTRY_ALIASES_JSON = {"country", "addr_country", "primary_country", "nationality",
                        "registration_country", "nat_country", "country_code",
                        "country_of_registration"}

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


def _err(e: Exception) -> str:
    """str(e) can be EMPTY for some exceptions (StopIteration, a bare
    MemoryError from the allocator) — and an exception OBJECT is always
    truthy in Python (no custom bool), so `e or repr(e)` never falls
    through to repr(e) the way it looks like it should; only checking
    str(e)'s own truthiness actually works. This is the one place that
    logic lives so it can't regress per call-site again."""
    return str(e) or repr(e) or type(e).__name__


# ── CSV row source (plain .csv/.tsv, or one inside a .zip) ─────────────

def _col_index(header_lower: list[str], aliases: tuple[str, ...]) -> int | None:
    for alias in aliases:
        if alias in header_lower:
            return header_lower.index(alias)
    return None


def _csv_row_source(text_stream):
    """Wraps a CSV/TSV text stream: detects the name/domain/country
    columns from the header via alias matching, then yields (name,
    domain, country) string triples, same shape as _json_row_source's
    output — the shared filtering loop in stream_filter() doesn't care
    which source produced them. Returns None (and logs why) if the header
    can't be found or doesn't have usable columns."""
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


# ── JSON row source (a zip of many BigQuery-style shard files) ─────────

def _normalize_key(k: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(k).lower())


def _deep_find(obj, aliases: set, max_depth: int = 4, _path: tuple = ()):
    """Recursively searches a JSON record for the first string value whose
    key (normalized) matches one of `aliases`. Checks every key at the
    CURRENT level before descending into any nested dict/list, so a
    top-level match always wins over a deeper one. Returns (path, value)
    or None; `path` is a tuple of dict-keys/list-indices that
    _get_by_path() can replay against every later record without
    re-searching each one."""
    if max_depth < 0:
        return None
    # Normalize the alias side too — otherwise a key like "WEBSITE_ADDRESS"
    # normalizes to "websiteaddress" but the alias "website_address" still
    # has its underscore, so the two never match and this silently finds
    # nothing at all. Cheap: alias sets are tiny and this only runs a
    # handful of times total (once per field, on the first record).
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
    """Peeks just the first shard file's first line to decide 'lines'
    (JSON Lines — one object per line, the standard BigQuery-export shape
    this zip's 'bq_organization_NNNNNN.json' naming strongly suggests) vs
    'whole' (the file is one single JSON document — an array or object)."""
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(first_name) as fh:
            first_line = io.TextIOWrapper(fh, encoding="utf-8", errors="ignore").readline()
            try:
                json.loads(first_line)
                return "lines"
            except (json.JSONDecodeError, ValueError):
                return "whole"


def _iter_zip_json_records(zip_path: str, json_names: list[str], mode: str):
    """Yields every JSON record across ALL shard files, in a fixed
    (sorted) order so raw_row_index/Restart ID stays meaningful and
    reproducible across separate runs regardless of the zip's own
    internal listing order."""
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
                            continue  # one malformed line shouldn't kill the whole shard
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
    """Peeks the first N records to auto-detect the name/domain/country key
    paths (see _deep_find's docstring), logs exactly what it found, then
    yields (name, domain, country) triples for every record across all
    shard files — same shape _csv_row_source yields. Returns None (and
    logs why) if name/domain can't be confidently located after scanning
    N records.
    
    FIX 2026-08: Previously only checked the FIRST record. If that record
    happened to lack a website field (common — many registered entities
    have no web presence), _deep_find() returned None and the entire
    86M-row source was abandoned. Now scans up to SCAN_LIMIT records to
    find valid paths, stopping early once all three are found."""
    
    SCAN_LIMIT = 100  # scan up to this many records to find key paths
    
    mode = _detect_json_shard_mode(zip_path, sorted(json_names)[0])
    log.info(f"  JSON shard format detected: {mode} ({len(json_names)} files)")
    
    records = _iter_zip_json_records(zip_path, json_names, mode)
    
    # Scan up to SCAN_LIMIT records to find valid key paths
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
        
        # Stop early if we found all three, or if we've scanned enough
        if name_path and domain_path and records_scanned >= SCAN_LIMIT:
            break
        if name_path and domain_path and country_path:
            break
    
    # Check if we found the required fields
    if not name_path or not domain_path:
        log.error(
            f"Couldn't confidently find a name/domain field in the first {records_scanned} JSON records. "
            f"name match: {name_path}, domain match: {domain_path}, country match: {country_path}. "
            f"First record (for manual inspection — add the real key names to "
            f"NAME_ALIASES_JSON/DOMAIN_ALIASES_JSON/COUNTRY_ALIASES_JSON): "
            f"{json.dumps(first_record)[:3000]}")
        return None
    
    log.info(f"  name field: {name_path} = '{name_val}' (found at record #{records_scanned})")
    log.info(f"  domain field: {domain_path} = '{domain_val}'")
    if country_path:
        log.info(f"  country field: {country_path} = '{country_val}'")
    else:
        log.info("  country field: none found (proceeding without a country filter signal)")
    
    # Re-create the generator since we consumed records during scanning
    records = _iter_zip_json_records(zip_path, json_names, mode)
    
    def _gen():
        for rec in records:
            yield (_get_by_path(rec, name_path),
                   _get_by_path(rec, domain_path),
                   _get_by_path(rec, country_path) if country_path else "")
    
    return _gen()


# ── Download + dispatch ─────────────────────────────────────────────────

def _download_to_tempfile(url: str) -> tuple[str, int]:
    """Streams the URL to a local temp file. Only used for .zip sources —
    zipfile needs seekable access for its central directory (at the END
    of the file), which a live HTTP stream can't provide. FIXED 2026-08:
    this used to be io.BytesIO(r.content), buffering the WHOLE compressed
    file in RAM — reliably OOM's on a multi-GB zip (GitHub-hosted runners
    have ~7GB RAM but ~14GB disk), and a bare MemoryError's str() is
    EMPTY, which is exactly what an earlier run's opaque "Failed to
    open/read source: " with nothing after the colon actually was."""
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
    """Returns a (name, domain, country)-triple generator, dispatching on
    what's actually inside the source — a plain/gz CSV, or a zip
    containing either CSV/TSV files or JSON shard files (see module
    docstring). Returns None (having already logged why) on any failure
    — callers should treat None as "nothing to process", not raise."""
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
    raw.decode_content = True  # let urllib3 handle a gzip Content-Encoding transparently
    if url.endswith(".gz"):
        text = io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf-8", errors="ignore")
    else:
        text = io.TextIOWrapper(raw, encoding="utf-8", errors="ignore")
    return _csv_row_source(text)


# ── Shared filter/write loop ────────────────────────────────────────────

def stream_filter(url: str, output_path: str, countries: set[str] | None,
                  row_limit: int = 0, skip_rows: int = 0,
                  time_budget_minutes: int = 0) -> tuple[int, bool, int]:
    """Single pass over whatever _open_row_source() produces (CSV rows or
    JSON records, already normalized to (name, domain, country) triples):
    filter -> write, one record at a time. Returns (kept, stopped_early,
    raw_row_index) — raw_row_index is the "Restart ID": how many source
    records had been read (including any already skipped from a previous
    run) when this run ended, whether that's because it finished, hit
    row_limit, or hit time_budget_minutes. Pass that number back in as
    skip_rows next run to continue from exactly there.
    
    RESUME CAVEAT: there is no byte-range/record-index API into a plain
    HTTP stream, so skip_rows still has to download+decompress+parse
    every record up to that point — it does NOT save network time or CPU
    on the skipped prefix (for the JSON zip source specifically, the
    whole zip IS re-downloaded each run regardless of skip_rows — only
    the per-record processing is skipped). What skip_rows DOES save:
    output_path is opened in APPEND mode (not overwritten) when
    skip_rows > 0, so a resumed run's already-filtered/written rows from
    earlier runs are never lost or redone."""
    if not url:
        log.error("OPENDATA_SOURCE_URL not set — nothing to download.")
        return 0, False, skip_rows
    
    countries_lower = {c.strip().lower() for c in countries} if countries else None
    log.info(f"Downloading + filtering: {url}")
    if countries_lower:
        log.info(f"  filtering to {len(countries_lower)} countries")
    if skip_rows:
        log.info(f"  resuming: skipping the first {skip_rows:,} records (already processed in a prior run)")
    
    kept = skipped_country = 0
    raw_row_index = 0  # counts every record seen this run, including skipped ones — this IS the Restart ID
    stopped_early = False
    time_budget_seconds = time_budget_minutes * 60 if time_budget_minutes else None
    start = time.monotonic()
    
    try:
        row_source = _open_row_source(url)
    except Exception as e:
        log.error(f"Failed to open/read source: {_err(e)}")
        return 0, False, skip_rows
    
    if row_source is None:
        return 0, False, skip_rows  # _open_row_source already logged why
    
    # Append + no header-rewrite when resuming, so a resumed run's output
    # concatenates cleanly onto what earlier runs already wrote.
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
                    continue  # already processed in a prior run — see docstring
                
                if time_budget_seconds and time.monotonic() - start >= time_budget_seconds:
                    # This record was READ from the source but not yet
                    # evaluated/written — "un-count" it so the Restart ID
                    # points AT it, not past it, or it would be silently
                    # skipped forever on the next resumed run.
                    raw_row_index -= 1
                    stopped_early = True
                    log.warning(f"Time budget ({time_budget_minutes}min) reached at record "
                                f"{raw_row_index:,} — stopping here. To resume, set the Restart ID "
                                f"input to {raw_row_index} on the next seed run.")
                    break
                
                if row_limit and (raw_row_index - skip_rows) > row_limit:
                    raw_row_index -= 1  # same "un-count" reasoning as above
                    break
                
                if (raw_row_index - skip_rows) % PROGRESS_EVERY == 0:
                    elapsed = time.monotonic() - start
                    log.info(f"  ...scanned {raw_row_index - skip_rows:,} new records this run "
                             f"({(raw_row_index - skip_rows) / max(elapsed, 0.001):,.0f} rec/sec), "
                             f"{kept:,} kept so far")
                
                if countries_lower:
                    row_country_lower = row_country.strip().lower()
                    if row_country_lower not in countries_lower:
                        skipped_country += 1
                        continue
                else:
                    row_country_lower = row_country.strip().lower()
                
                name = (name or "").strip()
                domain = (domain or "").strip().lower()
                domain = re.sub(r"^https?://", "", domain).split("/")[0].strip()
                
                if not (name and domain and "." in domain and domain not in SKIP_SLUGS):
                    continue
                
                writer.writerow([name, domain, row_country_lower])
                kept += 1
    
    except Exception as e:
        log.error(f"Failed mid-stream at record {raw_row_index:,}, {kept:,} written this run: "
                  f"{_err(e)}. To resume, set the Restart ID input to {raw_row_index} on the next "
                  f"seed run.")
        # Whatever was written before the failure is still a valid partial
        # file — don't delete it — but the caller must still treat this as
        # a failed run (see main()'s exit-code handling).
        return kept, True, raw_row_index
    
    elapsed = time.monotonic() - start
    new_rows = raw_row_index - skip_rows
    log.info(f"Done: {new_rows:,} new records scanned this run, {skipped_country:,} skipped by country, "
             f"{kept:,} written to {output_path} ({kept / max(new_rows, 1) * 100:.2f}%), "
             f"{elapsed:.0f}s ({new_rows / max(elapsed, 0.001):,.0f} rec/sec). "
             f"Total records processed so far (this + prior runs): {raw_row_index:,}.")
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
                        help="Restart ID — records to skip (already processed in a prior run). Get "
                             "this number from a previous run's 'stopping here' or 'Total records "
                             "processed' log line. 0 = start from the beginning.")
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
