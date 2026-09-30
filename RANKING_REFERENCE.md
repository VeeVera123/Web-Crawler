# Location Ranking — Quick Reference

Tables only. For the narrative version (why each rule exists, real postings
that motivated it), see `CLASSIFICATION.md`.

## 1. Ranks at a glance

| Rank | Value | Set by | Role restriction | ATS restriction | App Questions required? | LLM involved? |
|---|---|---|---|---|---|---|
| Global | `1` | Regex only | None (CS/AM/PM/OM) | None | No | No |
| EMEA/Africa | `2` | Regex only | None | None | No | No |
| Blank+confirmed | `3a` | Regex + LLM | None | None | **No** (this tier exists *because* app questions are usually missing — Crawl II/III) | **Yes** — LLM verdict is authoritative here |
| Bare-Remote | `3b` | Regex + LLM (verdict doesn't matter, see below) | None | None | **Yes** | Only to *reject* (LLM `no_match` can still drop it); cannot promote or add requirements |
| Bare country/region | `4a` | Regex only | CS/AM only | Must be in `RANK4_ELIGIBLE_ATS` | **Yes** | No |
| City in eligible country / mixed signal | `4b` | Regex only | CS/AM only | Must be in `RANK4_ELIGIBLE_ATS` | **Yes** | No |

**Bottom line: the LLM only ever has real decision-making power at Rank 3a.**
At every other rank it's either absent (1/2/4a/4b) or can only say "no" (3b),
never "yes."

## 2. What the location field has to look like

| Location field content | Outcome |
|---|---|
| Contains `global`/`worldwide`/`anywhere`/`distributed`/etc. (~80 variants), nothing else left over | Rank `1` |
| Contains `EMEA` alone, `Africa` (continent), 2+ African countries, or 2+ business regions together | Rank `2` |
| Blank / placeholder (`N/A`, `TBD`, `—`) | → LLM review → `3a` if confirmed, else dropped |
| Bare `"Remote"`, nothing else | → LLM review (only to veto) → `3b` if app questions present, else dropped |
| A real, specific place (city/state/country) that isn't Global/EMEA/Africa | Rejected outright — **never reaches the LLM** |
| A real, specific place, but it's one of Rank 4's 15 eligible countries/regions, role is CS/AM, ATS is eligible, app questions present | `4a` (bare) or `4b` (city+country / title-JD mismatch) |

Before any of the above runs, the location field can be **enriched** (filled in) from two other sources, in order:
1. Title (e.g. title says `"CSM - EMEA"`, location field is blank → treated as EMEA)
2. Description body text, if it has a `"Location:"`/`"Primary Location:"`/`"Work from:"`/`"Based in:"`-style label

Enrichment only fires when the location field is genuinely bare (blank/placeholder/bare-Remote) — it never overwrites a location field that already says something real.

## 3. Universal hard-exclusion checks (apply to EVERY rank, checked first, in this order)

These run **before** any rank is assigned, for every job, regardless of which rank it would otherwise reach. Any single one firing = instant reject, no LLM, no Rank 4 fallback for that reason.

| # | Check | Catches |
|---|---|---|
| 1 | `has_hard_no_sponsorship_signal` | "we cannot sponsor visas," any phrasing |
| 2 | `has_role_specific_place_restriction_signal` | A concrete place named for this specific role/candidate |
| 3 | `has_non_remote_workplace_type` | Structured `workplace_type` field = Hybrid/On-site/In-office/In-person |
| 4 | `has_non_remote_labeled_text_signal` | Same, but as free text: `"Remote status: On-site"`, `"Workplace setting: Hybrid"`, or standalone phrases like `"Hybrid Working"`, `"carried out ... in the company's premises"` |
| 5 | `has_non_remote_title_signal` | Title itself says `"(Hybrid)"`, `"- Onsite"` |
| 6 | `has_hard_country_specific_auth_signal` | "Authorized to work in `<country>`," names the country directly |
| 7 | `has_referential_auth_question_with_named_place_signal` | "Authorized to work in the country **this role is located in**" — doesn't name a country, but the job's own location field already does |
| 8 | `has_state_list_restriction_signal` | Enumerated US state list ("open to CA, NY, TX...") |
| 9 | `has_hard_country_based_restriction_signal` | "This role can be based anywhere in `<country>`" |
| 10 | `has_extra_restrictive_geography_signal` | Other geography-restriction phrasing families |
| 11 | `has_hard_metadata_location_signal` | ATS platform metadata names a specific place the location field didn't |
| 12 | `has_hard_location_symbol_signal` | A map-pin icon next to a specific place (in-house ATS pages) |
| 13 | `has_title_region_restriction_signal` | Title names a single narrow region ("Account Manager - LATAM") |
| 14 | `has_office_attendance_signal` | "N days/week in office," "willing to work at our office" |
| 15 | `has_entity_or_exclusion_restriction_signal` | "We don't have a legal entity in your country," "not open to candidates outside the US" |
| 16 | `has_timezone_relocation_or_hyphenated_restriction_signal` | "Must be in a US timezone," "`<place>`-based candidates only" |

None of these require application questions to be present — they scan whatever `location`/`title`/`workplace_type`/`description_snippet` the job already has.

## 4. Rank 3 — the two sub-tiers compared

| | 3a (blank) | 3b (bare Remote) |
|---|---|---|
| Location field | Empty/missing | Literally `"Remote"`, nothing else |
| Why this tier exists | Give Crawl II/III entries (which routinely have no location field at all) a chance | The company gave a real signal ("Remote"), just not region-specific |
| App questions required | **No** | **Yes** — no `"Application Question:"` text present → dropped, no exceptions |
| LLM verdict `match_global`/`match_africa` | Kept at 3a | Kept at 3b (same outcome as `uncertain`) |
| LLM verdict `uncertain` | **Dropped** | Kept at 3b (benefit of the doubt — company already said "Remote") |
| LLM verdict `no_match` | Dropped | Dropped |
| Can ever reach Rank 1/2 | No — capped at 3a even with a confident LLM verdict | No — capped at 3b |

## 5. Rank 4 — eligibility gate (all 4 must hold before Rank 4 is even attempted)

| # | Condition |
|---|---|
| 1 | `role_category` is `CS` or `AM` (never PM/OM) |
| 2 | `source_ats` is in `RANK4_ELIGIBLE_ATS`: Greenhouse, Workable, Personio, JazzHR, Teamtailor, Recruitee, Lever, Eploy, PageUp, isolvedhire, Pinpoint, Rippling |
| 3 | `ENABLE_RANK4_COUNTRY_SPECIFIC` config flag is on |
| 4 | `"Application Question:"` literally present in `description_snippet` |

## 6. Rank 4 — what location values are accepted

| Type | Accepted values |
|---|---|
| Countries (bare, exact list — nothing else) | US, UK, Canada, Australia, Germany, Ireland, Singapore, Luxembourg, Norway, Switzerland, Denmark, Netherlands, Iceland, Sweden, Italy (+ spelling variants: USA/U.S./United States, etc.) |
| Continents | Europe, North America |
| Business regions (unrestricted list, broader than the country list) | APAC, LATAM, MENA, AMER/Americas, ANZ, DACH, Benelux, Nordics, Gulf/GCC, EU/European Union, Asia (+ sub-regions), Oceania, Caribbean, CEE, CIS, and more |
| City + eligible country | `"Sydney, Australia"`, `"London, United Kingdom"` — admitted same as bare country |
| **Never accepted** | US states / Canadian provinces alone or paired with a country (`"California"`, `"California, United States"`) — always rejected |
| City + **non**-eligible country | `"Lagos, Nigeria"` — rejected (Nigeria isn't on the list) |

**4a vs 4b:**
| | 4a | 4b |
|---|---|---|
| Shape | Location field is *entirely* an eligible country/region/city+country | Location field is something else (a city with no eligible country attached) but the **title or JD** separately names an eligible region/country |
| Example | `location = "Australia"` | `title = "CSM, APAC"`, `location = "Tokyo"` |

## 7. Rank 4 — additional exclusion check (on top of the 16 universal ones in §3)

| Check | Catches |
|---|---|
| `_rank4_has_country_tied_restrictive_question` | Sponsorship/work-permit/residency **question or requirement** tied to a named country (e.g. "Will you require visa sponsorship in the United States?"), plus referential phrasing ("authorized to work where this role is located") — no extra gate needed here since Rank 4 already knows the specific place. Has its own benefit-language guard (a company *offering* relocation help isn't a requirement). |

## 8. Order of operations for one job

| Step | What happens |
|---|---|
| 1 | Run all 16 universal checks (§3). Any hit → reject, skip to step 5. |
| 2 | Check location field against Global/EMEA/Africa keywords (§2). Match → Rank `1` or `2`, done. |
| 3 | If location field is blank/bare-Remote only: loose free-text scan for Global/EMEA/Africa evidence in title+description. Match → Rank `1`/`2` **directly via regex** (this is a field-independent rescue, only allowed when the field itself was genuinely bare — a specific city/country here does NOT get this second chance). |
| 4 | Still no match, and field is blank or bare-Remote → send to LLM. Apply §4's 3a/3b rules. |
| 5 | Job dropped by any of the above → last-chance Rank 4 attempt (§5–§7). Pass → `4a`/`4b`. Fail → dropped for good. |
