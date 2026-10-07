"""Board-slug case: one board must be one registry row.

archive_i is unique on (ats, slug) with a CASE-SENSITIVE compare, but for many
ATSs the slug is case-insensitive on the ATS side: `Ashby` and `ashby` are the
same board. Discovery sources spell slugs however the URL they saw spelled them,
so the registry ended up holding ~10k boards twice (Workday 5,862, SmartRecruiters
2,831, Ashby 884, ...). Registry sharding is `id % 1000` computed in the database,
so the two spellings land in DIFFERENT shards and each shard scraped "its" copy:
every such board was fetched, role-filtered and enriched twice per run (and its
jobs inserted twice, see job_url.py).

Two defences, both driven by CASE_INSENSITIVE_SLUG_ATS:

  * on write (canonical_registry_rows): slugs of these ATSs are stored
    lowercase, so a rediscovery never creates a new spelling;
  * on load (drop_case_twins, used by supabase_handler.get_all_slugs): a
    mixed-case row whose all-lowercase twin exists in the registry is not
    scraped. The twin's own shard scrapes the board once. Nothing is deleted,
    so a wrong entry here costs a skipped duplicate at worst, never lost data.

CASE_INSENSITIVE_SLUG_ATS is deliberately an allow-list. Each entry was checked
by running that ATS's real scraper on both spellings of several registry twins
and getting identical job lists (2026-10). Lever is NOT in it: `ContactOut`
returned a job and `contactout` returned none. Taleo and PageUp are not in it
because every sampled pair was an empty board (inconclusive). Add an ATS only
after repeating that check.

Pure functions, no I/O.
"""

from __future__ import annotations

CASE_INSENSITIVE_SLUG_ATS = frozenset({
    "ashby",             # also visible in prod: both spellings produced job rows
    "workday",           # slug is "tenant|wdN|site"; tenant is already lowercase
    "smartrecruiters",
    "greenhouse",
    "dayforce",
    "oracle_cloud_hcm",  # slug is "host|SITE"
    "paycom",            # hex client key
})


def is_case_insensitive(ats: str) -> bool:
    return (ats or "").lower() in CASE_INSENSITIVE_SLUG_ATS


def canonical_slug(ats: str, slug: str) -> str:
    """The spelling a registry row is stored under."""
    if slug and is_case_insensitive(ats):
        return slug.lower()
    return slug


def canonical_registry_rows(rows: list[dict]) -> list[dict]:
    """Rows about to be upserted into archive_i ({ats, slug, ...}): canonical
    slugs, and one row per (ats, slug) — two spellings of one board arriving in
    the same batch would otherwise make Postgres reject the whole statement
    ("ON CONFLICT DO UPDATE command cannot affect row a second time")."""
    out: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for r in rows:
        slug = r.get("slug")
        if slug:
            canon = canonical_slug(r.get("ats", ""), slug)
            if canon != slug:
                r = {**r, "slug": canon}
            key = (r.get("ats", ""), canon)
            if key in seen:
                continue
            seen.add(key)
        out.append(r)
    return out


def twin_candidates(pairs: list[tuple[str, str]]) -> dict[str, list[str]]:
    """{ats: sorted lowercase slugs} for the mixed-case rows whose all-lowercase
    twin might exist, i.e. the ones get_all_slugs has to look up."""
    found: dict[str, set[str]] = {}
    for ats, slug in pairs:
        if slug and slug != slug.lower() and is_case_insensitive(ats):
            found.setdefault(ats, set()).add(slug.lower())
    return {a: sorted(s) for a, s in found.items()}


def drop_case_twins(pairs: list[tuple[str, str]], present: set[tuple[str, str]]) -> list[tuple[str, str]]:
    """Drop every mixed-case (ats, slug) whose (ats, slug.lower()) is in
    `present` (the lowercase rows that exist in the registry)."""
    return [
        (ats, slug) for ats, slug in pairs
        if not (slug and slug != slug.lower() and is_case_insensitive(ats) and (ats, slug.lower()) in present)
    ]
