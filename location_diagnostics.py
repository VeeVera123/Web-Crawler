"""
location_diagnostics.py — shared per-ATS location-extraction health check,
used by both crawl_i.py's filter_locations() and crawl_ii.py's
_filter_locations().

2026-09 (Phase 2 of the location-extraction hardening, done after two real
scraper bugs shipped wrong US locations — Inabia/JazzHR and Sonepar/
SuccessFactors — and after synthesizing two external LLM reviews of the
architecture). Replaces the original flat "blank rate >= 25%" canary
(which fired on absolute rate alone, with no notion of what's normal for
a given platform) with two upgrades:

  1. HISTORICAL BASELINE instead of a flat threshold. A platform that is
     ALWAYS ~40% blank (some tenants genuinely never fill in state/
     country) shouldn't warn every single run just for being who it is.
     A platform that jumps from a ~2% baseline to 40% in one run should
     warn immediately, even though 40% alone might sit under some
     arbitrary flat cutoff. The baseline is a small JSON file
     (location_baseline.json, next to this module) updated via an
     exponential moving average (EMA) after every run — no new Supabase
     table needed for what's fundamentally a lightweight, best-effort
     diagnostic signal, not a data record.

  2. EXTRACTION-STATUS BREAKDOWN using the location_status metadata that
     the BeautifulSoup-migrated scrapers (JazzHR, Eploy, JobAdder,
     Jobvite, PageUp), HRMDirect's header-mapping path, and the
     SuccessFactors detail-page backfill now populate. This distinguishes
     three genuinely different situations that a flat blank-count used to
     collapse into one number:
       - "marker_not_found"      → the scraper looked for its location
                                    marker/selector and didn't find it at
                                    all. Real extraction-failure signal —
                                    the platform's markup likely changed.
       - "marker_found_empty"    → the scraper found the right spot in the
                                    markup, and it was genuinely empty.
                                    Not a bug; the company just didn't
                                    fill it in.
       - (no status at all)      → an older, not-yet-instrumented scraper.
                                    Its blanks still count toward the
                                    baseline/spike check above, just
                                    without the finer breakdown.
     "extracted_by_header" vs. "extracted_positional" (HRMDirect) and
     "extracted_from_detail_page" (SuccessFactors) are also surfaced, so a
     platform quietly falling back to the weaker/positional path more
     than usual is visible too — a lightweight stand-in for a full
     primary-vs-fallback success-rate metric.

Call report_and_update() once per filter_locations() run, after the
keyword-classify pass has produced unsure_jobs/unsure_reasons, and before
sending unsure_jobs to the AI stage (timing doesn't matter functionally,
just needs the full job list + those two parallel lists).
"""
import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

_BASELINE_PATH = Path(__file__).parent / "location_baseline.json"
_MIN_SAMPLE = 10          # don't judge an ATS on fewer than this many jobs this run
_SPIKE_MULTIPLIER = 2.0   # current rate must be >= this many times the baseline...
_SPIKE_ABS_FLOOR = 0.15   # ...AND at least this many percentage points higher,
                          # so a tiny ATS wobbling 1%→3% doesn't fire on ratio alone
_EMA_ALPHA = 0.3          # how fast the baseline adapts toward each new run
_STATUS_BREAKDOWN_FLAG_RATE = 0.25  # marker_not_found share that gets flagged inline


def _load_baseline() -> dict:
    if not _BASELINE_PATH.exists():
        return {}
    try:
        return json.loads(_BASELINE_PATH.read_text())
    except Exception:
        return {}


def _save_baseline(data: dict) -> None:
    try:
        _BASELINE_PATH.write_text(json.dumps(data, indent=2, sort_keys=True))
    except Exception as e:
        log.warning(f"Location diagnostics: couldn't persist baseline file: {e}")


def report_and_update(
    pipeline_tag: str,
    jobs: list[dict],
    unsure_jobs: list[dict],
    unsure_reasons: list[str],
) -> None:
    """Logs a clean, bounded per-ATS location-extraction health block
    (using the same `"=" * 60` section-bounding convention used
    elsewhere in this codebase) and updates the persisted baseline.

    pipeline_tag: "CRAWL I" or "CRAWL II" — only affects the log header
                  and keeps the two pipelines' baselines separate (they
                  scrape overlapping but not identical ATS boards, on
                  different schedules).
    jobs: the full job list this run saw, post-scrape, pre-filter.
    unsure_jobs / unsure_reasons: parallel lists already computed by the
                  caller's keyword-classify pass; reason == "blank" is
                  what counts against an ATS's blank rate here.
    """
    total_by_ats: dict[str, int] = {}
    blank_by_ats: dict[str, int] = {}
    status_by_ats: dict[str, dict[str, int]] = {}

    for job in jobs:
        ats_name = job.get("source_ats") or "unknown"
        total_by_ats[ats_name] = total_by_ats.get(ats_name, 0) + 1
        status = job.get("location_status")
        if status:
            status_by_ats.setdefault(ats_name, {})
            status_by_ats[ats_name][status] = status_by_ats[ats_name].get(status, 0) + 1

    for job, reason in zip(unsure_jobs, unsure_reasons):
        if reason == "blank":
            ats_name = job.get("source_ats") or "unknown"
            blank_by_ats[ats_name] = blank_by_ats.get(ats_name, 0) + 1

    baseline = _load_baseline()
    pipeline_baseline = baseline.setdefault(pipeline_tag, {})

    spikes = []
    for ats_name, ats_total in total_by_ats.items():
        if ats_total < _MIN_SAMPLE:
            continue
        blanks = blank_by_ats.get(ats_name, 0)
        rate = blanks / ats_total
        prev = pipeline_baseline.get(ats_name)
        prev_rate = prev["rate"] if prev else None

        if prev_rate is not None and rate >= prev_rate * _SPIKE_MULTIPLIER and (rate - prev_rate) >= _SPIKE_ABS_FLOOR:
            spikes.append((ats_name, blanks, ats_total, rate, prev_rate))

        new_rate = rate if prev_rate is None else (_EMA_ALPHA * rate + (1 - _EMA_ALPHA) * prev_rate)
        pipeline_baseline[ats_name] = {
            "rate": round(new_rate, 4),
            "n_runs": (prev.get("n_runs", 0) + 1) if prev else 1,
        }

    _save_baseline(baseline)

    log.info("=" * 60)
    log.info(f"{pipeline_tag} — location extraction diagnostics")
    log.info("=" * 60)

    if not spikes:
        log.info("No ATS platform's blank-location rate spiked vs. its historical baseline this run.")
    else:
        for ats_name, blanks, ats_total, rate, prev_rate in sorted(spikes, key=lambda s: -s[3]):
            log.warning(
                f"{ats_name}: BLANK location rate spiked to {blanks}/{ats_total} "
                f"({rate:.0%}) this run, vs. a {prev_rate:.0%} historical baseline "
                f"— check whether {ats_name}'s scraper's location extraction "
                f"still matches that platform's current HTML/markup."
            )

    instrumented_ats = sorted(status_by_ats.keys())
    if instrumented_ats:
        log.info("-" * 60)
        log.info("Extraction-status breakdown (instrumented scrapers only):")
        for ats_name in instrumented_ats:
            counts = status_by_ats[ats_name]
            ats_total = total_by_ats.get(ats_name, 0)
            parts = ", ".join(f"{k}={v}" for k, v in sorted(counts.items(), key=lambda kv: -kv[1]))
            not_found = counts.get("marker_not_found", 0)
            flag = ""
            if ats_total >= _MIN_SAMPLE and (not_found / ats_total) >= _STATUS_BREAKDOWN_FLAG_RATE:
                flag = "  <-- marker_not_found is high; this scraper's DOM selector may be stale"
            log.info(f"  {ats_name} ({ats_total} jobs): {parts}{flag}")

    log.info("=" * 60)
