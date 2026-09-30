"""Shared "already sent to the LLM and excluded" cache for Crawl I/II/III.

2026-09 (explicit user request): a job whose location-classification reaches
the "unsure" stage gets sent to the LLM (ai_classify_locations) every single
run, forever, if it ends up excluded — excluded jobs are never written to
Supabase's `jobs` table, so the existing `existing_urls` dedup (which only
recognizes jobs already IN that table) never catches them, and the exact
same URL gets reclassified by the LLM on every subsequent crawl that still
finds it. This module is the fix: each crawl pipeline persists the URLs it
excludes after AI classification to a GitHub Release asset (see .github/
workflows/crawl.yml's download/upload steps around each crawl's shard step,
and the finalize-merge step in postfix_notion.py), and loads that cache back
at the start of its next run to skip the LLM call for anything still in it.

Storage shape: one JSON object per crawl (excluded_1.json / excluded_2.json
/ excluded_3.json for Crawl I/II/III respectively — kept as three SEPARATE
files per explicit user instruction, not merged into one), mapping
{url: excluded_at_iso}. A shard never talks to the canonical file directly —
it downloads the canonical file as its read-only starting point, writes only
ITS OWN newly-excluded URLs to a separate per-shard partial file (avoiding
the git-push/Release-asset race that ~10 concurrent shards writing the same
file at once would cause — see .github/workflows/workable.yml's
"shard-partials Release" pattern, which this mirrors), and a single
`needs`-gated finalize step (not sharded, runs once after every shard
finishes) merges all of this run's shard partials into the new canonical
file.

TTL policy (explicit user instruction: "Time-based expirey is fine, just
make sure this is perfect so we dont get the wrong thing in or right stuff
out. And to be clear, after the 14 to 30 days, the excluded junk does not
get added to a batch automatically, it has to have been refound."):
EXCLUDED_CACHE_TTL_DAYS (picked the midpoint of that range) is the only
place this number lives. An entry older than the TTL is silently dropped
the next time the cache is LOADED — nothing proactively re-queues it. If a
live crawl happens to find that same URL again afterward (the "refound"
case), its keyword-stage classification just runs fresh as if the URL had
never been cached at all, because the now-expired entry is no longer in the
dict this module hands back. If the posting is gone for good, it simply
never resurfaces and nothing happens — no special re-processing logic
exists or is needed.

This module does no network/Release-asset I/O itself — the CI workflow
downloads/uploads the actual files via `gh release download`/`gh release
upload`; this module only ever reads and writes local paths it's given.
"""
import glob
import json
import logging
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

# 2026-09 (explicit user instruction: "Time-based expirey is fine ... 14 to
# 30 days"): 21 is the midpoint of that range. Easy to retune later —
# nothing else in this file, or any caller, hardcodes a specific number.
EXCLUDED_CACHE_TTL_DAYS = 21


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_expired(excluded_at_iso, ttl_days: int) -> bool:
    """True if `excluded_at_iso` is older than `ttl_days`, OR if it isn't a
    readable timestamp at all. Per explicit user instruction ("make sure
    this is perfect so we dont get the wrong thing in or right stuff out"),
    an entry this function can't actually age-check must NOT be trusted to
    suppress reclassification — treating it as expired (drop it, let the
    job go through the normal pipeline again) is the only safe default;
    treating an unreadable entry as "still valid" risks silently hiding a
    job forever on a parse quirk alone."""
    if not isinstance(excluded_at_iso, str):
        return True
    try:
        excluded_at = datetime.fromisoformat(excluded_at_iso)
    except ValueError:
        return True
    if excluded_at.tzinfo is None:
        excluded_at = excluded_at.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - excluded_at
    return age > timedelta(days=ttl_days)


def load_excluded_cache(path: str, ttl_days: int = EXCLUDED_CACHE_TTL_DAYS) -> dict:
    """Reads a local JSON file of {url: excluded_at_iso} — already
    downloaded by the CI workflow step from this crawl's canonical Release
    asset, or simply absent on the very first run / a fresh cache — and
    returns only the entries that are NOT expired. This is the entire TTL
    mechanism: an expired entry is silently left out of the returned dict,
    so a caller checking `url in cache` naturally stops treating it as
    known-excluded the moment it ages out, with no separate cleanup step
    needed on the read side.

    Any read/parse failure (missing file, corrupt JSON, wrong top-level
    type) is treated as "no cache" rather than raised — a broken cache
    file must never be able to crash a crawl shard; it just means this
    run reclassifies everything fresh, same as the very first run ever
    did."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        log.info(f"No excluded-cache file at {path!r} (first run, or nothing cached yet) — starting empty.")
        return {}
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"Excluded-cache at {path!r} unreadable ({e.__class__.__name__}: {e}) — treating as empty.")
        return {}

    if not isinstance(raw, dict):
        log.warning(f"Excluded-cache at {path!r} wasn't a JSON object (got {type(raw).__name__}) — ignoring it.")
        return {}

    kept, expired, malformed = {}, 0, 0
    for url, excluded_at in raw.items():
        if not isinstance(url, str) or not url:
            malformed += 1
            continue
        if _is_expired(excluded_at, ttl_days):
            expired += 1
            continue
        kept[url] = excluded_at

    log.info(f"Excluded-cache {path!r}: {len(kept)} active, {expired} expired, "
             f"{malformed} malformed (TTL={ttl_days}d)")
    return kept


def save_new_exclusions(path: str, urls) -> None:
    """Writes THIS shard's own newly-excluded URLs (only — never the full
    merged cache; see the finalize-merge step for that) to a local JSON
    file, for the CI workflow to upload as a per-shard partial Release
    asset. Every entry gets the same 'now' timestamp (this run's wall-clock
    time) — day-granularity TTL doesn't need per-job precision, and this
    avoids a separate now() call per URL for no benefit. Writes nothing
    (not even an empty file) when `urls` is empty, so the workflow's
    upload step can skip a genuinely-empty shard cleanly via a file-exists
    check."""
    urls = set(urls)
    if not urls:
        return
    now = _now_iso()
    payload = {url: now for url in sorted(urls)}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    log.info(f"Wrote {len(payload)} newly-excluded URLs to {path!r}")


def merge_excluded_caches(existing_path: str, shard_glob: str, output_path: str,
                           ttl_days: int = EXCLUDED_CACHE_TTL_DAYS) -> int:
    """Finalize-step merge, run ONCE after every shard of one crawl has
    finished (mirrors run_finalize()'s own "needs every shard done" gate —
    see each crawl_*.py's run_finalize docstring). Unions the existing
    canonical cache (already downloaded to `existing_path`, or simply
    absent on a fresh cache) with every shard's newly-excluded partial file
    matching `shard_glob` from THIS run, drops anything expired under the
    SAME TTL rule load_excluded_cache uses (so a stale entry can't survive
    by being merged just before it would otherwise have been dropped on
    the next load), and writes the result to `output_path` for the
    workflow to publish as the new canonical Release asset.

    A URL that's already in the existing canonical cache (non-expired, so
    it survived load_excluded_cache's own filtering) always keeps that
    entry's original excluded_at, even if some shard also independently
    re-found and re-excluded it this run — the cache tracks "how long has
    this consistently been excluded", and letting a re-confirmation reset
    the clock would mean a URL that's excluded on every single run could
    never actually expire. A genuinely NEW exclusion (not already in the
    existing cache) gets this run's fresh timestamp, same as any first-time
    entry.

    Returns the final merged entry count (for the caller to log)."""
    merged = dict(load_excluded_cache(existing_path, ttl_days))
    existing_count = len(merged)
    new_count = 0
    shard_paths = sorted(glob.glob(shard_glob))

    for shard_path in shard_paths:
        try:
            with open(shard_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
            log.warning(f"Skipping unreadable shard partial {shard_path!r}: {e}")
            continue
        if not isinstance(raw, dict):
            log.warning(f"Skipping shard partial {shard_path!r} — not a JSON object.")
            continue
        for url, excluded_at in raw.items():
            if not isinstance(url, str) or not url:
                continue
            if _is_expired(excluded_at, ttl_days):
                continue
            if url not in merged:
                merged[url] = excluded_at
                new_count += 1

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, sort_keys=True)
    log.info(f"Merged excluded-cache -> {output_path!r}: {existing_count} carried over + "
             f"{new_count} new from {len(shard_paths)} shard partial(s) = {len(merged)} total")
    return len(merged)
