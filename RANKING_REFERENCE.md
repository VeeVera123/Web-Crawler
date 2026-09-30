# Location Ranking — Quick Reference

Tables only. For the narrative version (why each rule exists, real postings
that motivated it), see `CLASSIFICATION.md`.

## 1. Common conditions — apply to EVERY job, before any rank is even considered

### 1a. The 17 universal hard-exclusion checks (checked first, in this order)

Any single one firing = instant reject. No rank, no LLM, no Rank 4 fallback.

| # | Check | Catches |
|---|---|---|
| 1 | `has_hard_no_sponsorship_signal` | "we cannot sponsor visas," any phrasing |
| 2 | `has_role_specific_place_restriction_signal` | A concrete place named for this specific role/candidate |
| 3 | `has_non_remote_workplace_type` | Structured `workplace_type` field = Hybrid/On-site/In-office/In-person |
| 4 | `has_non_remote_labeled_text_signal` | Same, as free text: `"Remote status: On-site"`, standalone phrases (`"Hybrid Working"`, `"hybrid capacity"`, `"carried out...in the company's premises"`, `"on-site/in-office/in-person role"`) |
| 5 | `has_non_remote_title_signal` | Title says `"(Hybrid)"`, `"- Onsite"` |
| 6 | `has_hard_country_specific_auth_signal` | "Authorized to work in `<country>`," names the country directly |
| 7 | `has_country_tied_sponsorship_permit_residency_signal` | Sponsorship/work-permit/residency phrasing tied to a named country — "require visa sponsorship to work in `<country>`," "need a work permit for `<country>`," "maintain residence in `<country>`" (distinct from #6: not an "authorized to work" statement) |
| 8 | `has_referential_auth_question_with_named_place_signal` | "Authorized to work in the country **this role is located in**" — doesn't name a country, but the job's own location field already does |
| 9 | `has_state_list_restriction_signal` | Enumerated US state list |
| 10 | `has_hard_country_based_restriction_signal` | "Based anywhere in `<country>`," "located in other U.S. states," "live and work in `<country>`," "resident of `<country>`," "citizen of `<country>`," "worked from `<country>`" |
| 11 | `has_extra_restrictive_geography_signal` | Other geography-restriction phrasing families |
| 12 | `has_hard_metadata_location_signal` | ATS metadata names a place the location field didn't |
| 13 | `has_hard_location_symbol_signal` | Map-pin icon next to a specific place |
| 14 | `has_title_region_restriction_signal` | Title names a single narrow region ("- LATAM") |
| 15 | `has_office_attendance_signal` | "N days/week in office," "able to/must work from the office" |
| 16 | `has_entity_or_exclusion_restriction_signal` | "No legal/local entity in your country," "not open to candidates outside the US," "must be in a supported payroll country" |
| 17 | `has_timezone_relocation_or_hyphenated_restriction_signal` | "Must be in a US timezone," "`<place>`-based candidates only," "`<place>` only.," "`<timezone>` business hours only" |

Check 7 was added 2026-09 during an adversarial fuzz-test verification pass —
the underlying logic already existed for Rank 4 only
(`_rank4_has_country_tied_restrictive_question`) but was never wired into
this universal chain. See `CLASSIFICATION.md`'s "Adversarial fuzz-test
round" section for the full list of gaps closed in that pass, and which
claims from the test were investigated and deliberately left unfixed.

### 1b. How the location value is determined, before any check runs

Enrichment order (each step only fires if the previous left the field bare — blank/placeholder/bare-`"Remote"`):
1. Raw `location`/`country` fields
2. Title (`"CSM - EMEA"` → treated as EMEA if location field is bare)
3. Description body label (`"Location: Bowling Green, OH"`, `"Based in: ..."`)

### 1c. The "accepted broad signal" family, referenced everywhere below

| Family | Members |
|---|---|
| Global | ~80 keyword variants: `global`, `worldwide`, `anywhere`, `distributed`, `work from anywhere`, etc. |
| EMEA/Africa | Literal `EMEA` (no qualifier), `Africa` (continent), 2+ African countries together, 2+ business regions together |

## 2. Where a job lands — decision order

| Step | What happens |
|---|---|
| 1 | Run the 17 universal checks (§1a). Any hit → reject, skip to step 5. |
| 2 | Location field alone matches the accepted broad-signal family (§1c) → Rank `1` or `2`, done — regex only, no LLM. |
| 3 | Location field is bare (blank/`"Remote"` only) → loose scan of title+description for the SAME broad-signal family → Rank `1`/`2` directly via regex. **A location field that already names a specific place does NOT get this second chance** — unrelated boilerplate elsewhere can't rescue it. |
| 4 | Still bare → send to LLM. See §3 for what happens next. |
| 5 | Dropped by any step above → last-chance Rank 4 attempt (§4). Pass → `4a`/`4b`. Fail → dropped for good. |

## 3. Rank 3 — the LLM's only real say in the whole system

| | 3a (blank field) | 3b (bare `"Remote"`) |
|---|---|---|
| Why it exists | Crawl II/III entries routinely have no location field at all | Company gave a real signal ("Remote"), just not region-specific |
| App questions required | **No** | **Yes** — none present → dropped |
| LLM says `match_global`/`match_africa` | Kept at 3a | Kept at 3b |
| LLM says `uncertain` | **Dropped** | Kept at 3b (benefit of the doubt) |
| LLM says `no_match` | Dropped | Dropped |
| Ever reaches Rank 1/2 | No — capped at 3a | No — capped at 3b |

**The LLM cannot promote a job past 3a/3b, ever.** At 3b its verdict barely
matters (`match_global` and `uncertain` land in the same place) — its only
real power there is saying `no_match` to veto. **3a is the only tier where
the LLM's verdict is what actually decides the outcome.**

## 4. Rank 4 — CS/AM only, last-chance tier

### 4a. Eligibility gate (all 4 must hold before Rank 4 is even attempted)

| # | Condition |
|---|---|
| 1 | `role_category` is `CS` or `AM` |
| 2 | `source_ats` in `RANK4_ELIGIBLE_ATS`: Greenhouse, Workable, Personio, JazzHR, Teamtailor, Recruitee, Lever, Eploy, PageUp, isolvedhire, Pinpoint, Rippling |
| 3 | `ENABLE_RANK4_COUNTRY_SPECIFIC` config flag is on |
| 4 | `"Application Question:"` literally present in `description_snippet` |

### 4b. Additional exclusion check, on top of the 17 universal ones

| Check | Catches |
|---|---|
| `_rank4_has_country_tied_restrictive_question` | Sponsorship/work-permit/residency question tied to a named country, or a *referential* one ("authorized to work where this role is located") — no extra gate needed since Rank 4 already knows the specific place |

### 4c. What "eligible" means here

| Type | Accepted values |
|---|---|
| Countries (exact 15, nothing else) | US, UK, Canada, Australia, Germany, Ireland, Singapore, Luxembourg, Norway, Switzerland, Denmark, Netherlands, Iceland, Sweden, Italy (+ spelling variants) |
| Continents | Europe, North America |
| Business regions (broader, unrestricted list) | APAC, LATAM, MENA, AMER/Americas, ANZ, DACH, Benelux, Nordics, Gulf/GCC, EU, Asia + sub-regions, Oceania, Caribbean, CEE, CIS, and more |
| **Never accepted** | US states/Canadian provinces, alone or paired with a country (`"California"`, `"California, United States"`) |

### 4d. `4a` vs `4b` — the actual rule

**Everything in this section only ever runs after the 17 universal checks
(§1a) and 4b's own extra check (§4b) have already passed — so "4b" never
means "we found a restriction and let it slide," it means no restriction
was found at all, just two signals that don't fully agree.**

| | `4a` | `4b` |
|---|---|---|
| Definition | A **single, complete, unmixed** signal — the whole location field is made ONLY of eligible country/region names (§4c), nothing else | A **mixed signal** — one thing points toward "accepted" (§1c's broad family, OR §4c's country/region list) and something else points toward "narrower/different," at the same time |
| Example | `location = "Australia"` — nothing else | `title = "CSM, EMEA"`, `location = "London"` — title says accepted-broad, location narrows to one city — **both present, neither wins alone** |
| Example | `location = "Sydney, Australia"` — city + eligible country, still one clean combined signal | `title = "CSM London"`, `location = "EMEA (UK based)"` — same mix, direction reversed: location itself carries both the broad claim AND the narrowing qualifier |
| Example | — | `description` says `"we hire globally"`, `location = "Tokyo"` — broad claim in the JD, specific city in the field |
| Where the "yes" can come from | N/A — 4a has only one signal | Title, description, OR the location field itself — any of: §1c's Global/EMEA/Africa family, or §4c's country/region list |
| Where the "no"/narrower part can come from | N/A | A city, or any other concrete place that isn't itself disqualifying (if it WERE disqualifying, §1a/§4b would have already rejected the job before reaching here) |

**Plain-language rule: one clean signal with nothing else = `4a`. Two
different signals present at once, one broader and one narrower, neither
of which is actually restrictive = `4b`. A single restrictive signal alone
(with or without a broad one nearby) was already caught in §1a/§4b and
never reaches this table at all.**
