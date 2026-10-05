"""Re-validation of jobs that are ALREADY in the `jobs` table.

2026-10 (pipeline audit): crawl_i.py/crawl_ii.py skip every job whose URL is
already stored — "skipping LLM classification, just refreshing last_seen" —
which is correct for cost, but it also means a classifier fix is NEVER
applied retroactively. A job that leaked in under an old bug (the Rank 4
rows for "Kenya", "India", "Los Angeles", the Insurity/Ping Identity
postings, ...) stays in the table for as long as the posting is live on its
board. Real data confirming this: 1,596 of 5,729 active rows were Rank 4,
including locations the current policy forbids.

This module is the shared, PURE decision logic (no network, no DB) for a
cheap, evidence-only second look at those rows:

  * It only ever VETOES. It never promotes a job or re-runs an LLM, so it
    costs no model calls — the old verdict stands unless today's
    deterministic rules positively contradict it.
  * It vetoes only on POSITIVE evidence. A missing description or a failed
    application-question fetch is "undecided" (retry next run), never a
    reason to delete — the absence of evidence must not delete a good job.
  * It never touches a job the user is already tracking (application_status
    other than 'not_applied').

The callers (crawl_i.py, crawl_ii.py) enrich the stale rows' description and
application questions with the normal enrichment functions, call decide() on
each, mark vetoed rows via supabase_handler.mark_jobs_vetoed(), and stamp the
rest with config.CLASSIFIER_VERSION so each row is re-checked once per
version, not every run. postfix_notion.py later archives the Notion page and
deletes the vetoed rows (supabase_handler.purge_vetoed_jobs).
"""
import logging
import os

from classifier import (
    classify_rank4,
    _keyword_classify_location_detail,
    _is_bare_location,
    STANDALONE_GLOBAL_RE,
)

log = logging.getLogger(__name__)

VETO = "veto"
KEEP = "keep"
UNDECIDED = "undecided"
PROTECTED = "protected"

# Kill switch + per-shard ceiling (a stale row costs 1-2 HTTP fetches).
REVALIDATE_ENABLED = os.environ.get("REVALIDATE_KNOWN_JOBS", "true").strip().lower() != "false"
REVALIDATE_MAX_PER_RUN = int(os.environ.get("REVALIDATE_MAX_PER_SHARD", "3000"))
# REVALIDATE_DRY_RUN=true: evaluate and LOG every verdict but write nothing
# (no vetoes, no version stamps) — for observing the first run on real data.
REVALIDATE_DRY_RUN = os.environ.get("REVALIDATE_DRY_RUN", "").strip().lower() == "true"

_QUESTION_MARKER = "Application Question:"


def _location_text(job: dict) -> str:
    loc = job.get("location") or ""
    country = job.get("country") or ""
    if isinstance(loc, list):
        loc = ", ".join(str(x) for x in loc)
    if isinstance(country, list):
        country = ", ".join(str(x) for x in country)
    return f"{loc} {country}".strip()


def _location_is_open_shaped(job: dict) -> bool:
    """True when the location field is blank/bare-"Remote"/an explicit
    Global/EMEA/Africa claim — the only shapes a Rank 1/2/3 job can have
    been admitted on. For those, a "no_match" from the main pipeline can
    only come from a hard override (a restriction found in the text),
    never from the location-field allowlist — so it is safe to treat as a
    veto. Any other (legacy, old-policy) location shape is left alone."""
    loc = _location_text(job)
    if _is_bare_location(loc):
        return True
    low = loc.lower()
    return bool(STANDALONE_GLOBAL_RE.search(loc) or "emea" in low or "africa" in low)


def decide(job: dict, meta: dict) -> tuple[str, str]:
    """(verdict, reason) for one already-stored job.

    `job` is the freshly scraped + enriched dict; `meta` is the stored row's
    {"clearance", "location_priority", "application_status"}."""
    status = (meta.get("application_status") or "not_applied").lower()
    clearance = (meta.get("clearance") or "").lower()
    desc = job.get("description_snippet") or ""

    verdict, reason = KEEP, "no contradicting evidence"

    if clearance == "ai_unreviewed":
        # Policy (explicit user instruction, 2026-10): a job no LLM ever
        # reviewed is discarded, not kept under an "ai_unreviewed" tag.
        verdict, reason = VETO, "ai_unreviewed (no model ever reviewed it)"
    elif clearance == "rank4":
        if _QUESTION_MARKER not in desc:
            # Rank 4's admission rests on having seen the application
            # questions; if they weren't fetched this time we can't judge.
            return UNDECIDED, "no application-question text fetched this run"
        priority, why = classify_rank4(job)
        if priority:
            verdict, reason = KEEP, f"rank4 still eligible ({why})"
        else:
            verdict, reason = VETO, "rank4 no longer eligible under current rules"
    else:
        result, _, _ = _keyword_classify_location_detail(job)
        if result == "no_match" and _location_is_open_shaped(job):
            verdict, reason = VETO, "hard override fires on current rules"

    if verdict == VETO and status != "not_applied":
        return PROTECTED, f"would veto ({reason}) but application_status={status}"
    return verdict, reason


def summarize(results: list[tuple[dict, dict, str, str]], label: str) -> dict:
    """Log a compact, countable summary (the pipeline's "numbers add up"
    convention) and return the counts."""
    counts = {VETO: 0, KEEP: 0, UNDECIDED: 0, PROTECTED: 0}
    reasons: dict[str, int] = {}
    for _job, _meta, verdict, reason in results:
        counts[verdict] = counts.get(verdict, 0) + 1
        if verdict == VETO:
            reasons[reason] = reasons.get(reason, 0) + 1
    log.info(f"  {label} re-validation: {len(results)} stale rows -> "
             f"{counts[VETO]} vetoed, {counts[KEEP]} kept, "
             f"{counts[UNDECIDED]} undecided (retry next run), "
             f"{counts[PROTECTED]} protected (already applied/tracked)")
    for reason, n in sorted(reasons.items(), key=lambda kv: -kv[1]):
        log.info(f"    vetoed x{n}: {reason}")
    return counts


# ── Orchestration helpers shared by crawl_i.py / crawl_ii.py ────────────

def select_stale(already_seen: list[dict], meta: dict[str, dict],
                 current_version: int) -> tuple[list[dict], list[dict]]:
    """Split the already-known jobs into (stale, rest). `stale` = rows
    stored under an older rules version, capped at REVALIDATE_MAX_PER_RUN
    (the overflow stays at its old version and is picked up next run).
    Returns ([], already_seen) when re-validation is disabled."""
    if not REVALIDATE_ENABLED or not meta:
        return [], list(already_seen)
    stale, rest = [], []
    for job in already_seen:
        row = meta.get(job.get("url", ""))
        if not row:
            # No stored metadata for this URL (the metadata fetch failed or
            # came back partial): without the row's clearance we cannot tell
            # a Rank 4 row from any other, and stamping it as validated
            # would be wrong — leave it for a run with complete metadata.
            rest.append(job)
            continue
        if int(row.get("classifier_version") or 0) < current_version and len(stale) < REVALIDATE_MAX_PER_RUN:
            stale.append(job)
        else:
            rest.append(job)
    return stale, rest


def evaluate(stale_jobs: list[dict], meta: dict[str, dict]) -> list[tuple[dict, dict, str, str]]:
    """decide() for every enriched stale job -> [(job, meta_row, verdict, reason)]."""
    out = []
    for job in stale_jobs:
        row = meta.get(job.get("url", "")) or {}
        try:
            verdict, reason = decide(job, row)
        except Exception as e:  # a bug here must never fail a shard or delete a job
            verdict, reason = UNDECIDED, f"decide() raised {type(e).__name__}: {e}"
            log.warning(f"re-validation: decide() failed for {job.get('url')!r}: {e}")
        out.append((job, row, verdict, reason))
    return out


def plan(results: list[tuple[dict, dict, str, str]]) -> tuple[list[dict], list[dict], list[dict]]:
    """(veto_jobs, stamp_jobs, retry_jobs): vetoed rows get marked; kept/
    protected rows are stamped with the current version (never re-checked
    again until the version bumps); undecided rows are only touched, so they
    are retried next run."""
    if REVALIDATE_DRY_RUN:
        for job, row, verdict, reason in results:
            if verdict == VETO:
                log.info(f"    [dry-run] would veto {job.get('url')} "
                         f"[{row.get('clearance')}/{row.get('location_priority')}] "
                         f"{(job.get('location') or '')[:40]!r}: {reason}")
        return [], [], [j for j, *_ in results]
    veto, stamp, retry = [], [], []
    for job, _row, verdict, _reason in results:
        if verdict == VETO:
            veto.append(job)
        elif verdict in (KEEP, PROTECTED):
            stamp.append(job)
        else:
            retry.append(job)
    return veto, stamp, retry
