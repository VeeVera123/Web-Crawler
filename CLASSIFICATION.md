# Location Classification (2026-09 revamp)

How `classifier.py` decides whether a job survives Crawl I/II/III's location
filter, and what `jobs.location_priority` means. This replaces the old
single "kept, unsure" tier with four ranks — 1 (best) through 4 (narrowest,
Crawl I/II only) — plus 3a/3b sub-tiers for genuine uncertainty.

## The core rule: regex decides Rank 1/2, the LLM never does

A job only reaches `PRIORITY_GLOBAL` ("1") or `PRIORITY_AFRICA` ("2") via a
**keyword/regex match** in `_keyword_classify_location_detail()`. If the
location field doesn't carry an explicit global/EMEA/Africa signal, the job
is `"unsure"` and gets sent to the LLM — but the LLM's job from here on is
demotion insurance, not promotion authority. An AI `match_global`/
`match_africa` verdict on an unsure job is **kept, never promoted** — it
lands at 3a or 3b instead (see below). This was an explicit, twice-repeated
policy decision: regex is trusted; anything that needed the LLM to find is,
by definition, missing the explicit keyword vocabulary regex looks for, so
it can never outrank a real keyword hit.

### Hard overrides run before ANY rank is assigned — Rank 1/2/3a/3b included

`_keyword_classify_location_detail()` starts with a long chain of
deterministic "hard override" checks (`has_non_remote_workplace_type`,
`has_non_remote_title_signal`, `has_non_remote_labeled_text_signal`,
`has_hard_country_specific_auth_signal`, `has_office_attendance_signal`,
and others) — any one of them returning true immediately forces `no_match`,
before the location field or the AI ever gets a say. These are NOT
Rank-4-only: they gate every job, at every rank, since a job that is
genuinely Hybrid/On-site/country-restricted must never survive just because
its location field (or unrelated boilerplate elsewhere in the JD) also
happens to say something global/EMEA-sounding.

2026-09 (explicit user report, real postings: Fresha's "Account Manager
(Amsterdam) — Danish Speaking", leva-eu.com's Amsterdam listing, Kraft
Heinz's Eightfold "Hybrid Working" listing, viaquestinc.com's Paycor
"Remote Status: On-Site" listing — all four wrongly survived at Rank
1/2/3a): two systemic gaps let these through, both now fixed:

1. **The "JD text can rescue a bare field" step used to run unconditionally.**
   `_keyword_classify_location_detail`'s step 5 — "positive evidence in the
   JD can rescue a bare Remote field" — used to fire whenever the LOCATION
   FIELD itself didn't match Global/EMEA/Africa, regardless of whether that
   field was genuinely ambiguous or already named a real, specific place
   (a city pulled in from the title via `_enrich_location_from_title`, or a
   place the structured field itself named). A company's JD commonly
   carries loose "EMEA"/"global" language elsewhere (department tags,
   About-Us boilerplate) that has nothing to do with THIS specific
   posting's actual place. Now gated: step 5 only runs when the location
   value is genuinely bare (blank, a placeholder, or plain "Remote" with
   nothing attached) — the same `_is_bare_location()` test
   `_enrich_location_from_title` already used. A location that names a
   real place is trusted over unrelated text elsewhere in the JD.
2. **No check caught a workplace-type LABEL sitting in free description
   text.** `has_non_remote_workplace_type` only ever reads a scraper-
   populated `workplace_type` FIELD; `has_non_remote_title_signal` only
   reads the TITLE; `has_office_attendance_signal` only matches "N
   days/week in office"-shaped attendance phrasing. None of them caught a
   bare labeled line like `"Remote Status: On-Site"` or an unambiguous
   standalone phrase like `"Hybrid Working"` sitting in the JD body as
   plain text — which is exactly what a page shows when this project's own
   scraper never mapped that label into a structured field.
   `has_non_remote_labeled_text_signal()` closes this: it scans
   `description_snippet`/title for a broad label vocabulary ("Remote
   status:", "Workplace setting:", "Workplace type:", "Location status:",
   "Work from:", "Work mode:", "Working arrangement:") paired with a
   disqualifying value (Hybrid/On-site/In-office/In-person/Office), plus a
   short list of unambiguous standalone phrases ("Hybrid Working", "On-site
   working", …) — deliberately NOT a bare `\bhybrid\b` scan, since that
   word alone is too overloaded ("hybrid cloud", "hybrid event") to trust
   without a label or role-model pairing.

A companion function, `_enrich_location_from_description()`, mirrors
`_enrich_location_from_title()` but recovers a REAL place name from a
labeled line in the description body ("Location:", "Primary Location:",
"Work location:", "Based in:", …) whenever the field is still bare after
title enrichment — this is what lets a scraper's missed "Location: Bowling
Green, OH" still get correctly rejected as a specific US city instead of
falling through to the LLM with nothing to go on. It only ever recovers a
value to re-test through the normal Africa/EMEA/Global keyword path; it
never decides match/no-match itself. This is the main lever for Crawl
III/stapply.ai postings too, where there is no structured location field
at all — a labeled mention in the raw JD text is the only way to recover
it.

## Rank 1 — Global (`location_priority = "1"`)

Explicit worldwide/anywhere/global-hiring language in the location field
(`GLOBAL_KEYWORDS`, ~80 variants — "global", "worldwide", "anywhere",
"international", "distributed", etc.). Regex only. Open to all roles (CS,
AM, PM, OM).

## Rank 2 — EMEA / Africa (`location_priority = "2"`)

One of:
- The literal word **EMEA** with no city/country qualifier attached.
- **Africa** as a continent (the word itself, or 2+ different African
  countries named together — proof of continent-wide reach, not "based in
  one African country").
- Any combination of the above with other regions (e.g. "EMEA, LATAM,
  AMER").

Regex only, same as Rank 1. Open to all roles.

## Rank 3 — Uncertain

Only reached when the keyword stage found no explicit signal (`"unsure"`)
and the AI stage ran. Split by *why* the job was unsure, using
`_keyword_classify_location_detail`'s own `unsure_reason`:

- **3a — `PRIORITY_UNSURE_BLANK`**: the location field was **blank/missing**
  (no field to read at all — the paradigm case is Crawl III's stapply.ai
  JDs, which never carry a location field). The AI is given the title/JD
  and asked to spot missed global/EMEA/Africa language. Kept **only** if
  the AI actually found real evidence (`match_global`/`match_africa`) — a
  blank field the AI still can't back up is dropped. This higher bar
  exists because a blank field is indistinguishable from a scraper
  extraction bug (two confirmed real cases: JazzHR/Inabia and
  SuccessFactors/Sonepar both silently produced `location=""` for postings
  that were, on the live page, plainly country-specific).
- **3b — `PRIORITY_UNSURE_SILENT`**: the location field said something like
  bare **"Remote"** — a real signal from the company, just not
  region-specific ("dead location silence"). Kept whether the AI backed it
  with real evidence, was genuinely uncertain, or never got reviewed at all
  (every LOCATION_PROVIDERS entry exhausted) — a bare-Remote job never
  needs AI confirmation to survive, since the company itself said
  something real. **2026-09 policy addition (explicit user instruction:
  "application questions are a MUST" for 3b): also requires a real
  `"Application Question:"` marker in `description_snippet`, same
  requirement Rank 4 already had.** A bare "Remote" field alone proves
  nothing — the real postings that motivated this (a job whose location
  said "Remote" but the JD body separately said "Hybrid Working"; a job
  whose ONLY restriction showed up in a screening question) both show
  that the application questions are usually the only place a hidden
  Hybrid/On-site/country restriction actually surfaces. A bare-Remote job
  with zero application questions gives this pipeline no way to rule that
  out, so it's dropped instead of kept. This does **not** apply to 3a
  (blank location) — that tier explicitly exists to give Crawl II/III
  entries, which routinely have no application questions at all, a
  chance; see 3a above.

Both sub-tiers are open to all roles.

### A referential ("wherever this role is") work-authorization question

Distinct from the direct kind ("authorized to work in the United
States?", which names a country in the question itself — see
`_COUNTRY_AUTH_RE`, a universal hard override for every rank). Some
postings instead ask a question that REFERS to wherever the job's own
location already says, without naming it directly — e.g. "Are you legally
authorized to work in the country in which this role is located?" or
"What is the source of your right to work where this role is listed?"
(both real, from a Greenhouse posting whose location field named
Australia). `_COUNTRY_AUTH_RE` never matches these since no country
appears in the question text — but when this job's location already names
one specific, narrow place, the question is exactly as real a restriction
as if it had named that place directly.

Two call sites, same underlying `_REFERENTIAL_AUTH_QUESTION_RE`:
- `_rank4_has_country_tied_restrictive_question` (Rank 4 only) — no extra
  "is a place named" gate needed, since Rank 4 by definition is only ever
  evaluating a job whose location already resolved to one bare
  country/region.
- `has_referential_auth_question_with_named_place_signal` (universal,
  Rank 1/2/3a/3b) — does its own check first: not disqualifying when no
  real place is named at all (blank/placeholder/bare "Remote" — the
  question is uninformative with nothing to refer to), or when the named
  place is already a broad, accepted scope (Global/Worldwide, EMEA,
  Africa, or 2+ business regions together) — the question then ties to
  that broad scope, not a single country.

## Rank 4 — Mixed signals (CS/AM only, Crawl I & II only)

The newest tier. Admits a **bare country/region/continent** location (or a
title/JD naming one while the location field is a different concrete
place) for **Customer Success and Account Management roles only**, as long
as nothing else in the posting ties a restriction to that place. This is
the one tier the LLM has no part in at all — `classify_rank4()` is pure
regex/keyword logic, called directly on jobs the main pipeline would
otherwise drop.

**Eligibility gate** (all four must hold before `classify_rank4()` is even
called):
1. `role_category` is `"CS"` or `"AM"` (never PM/OM).
2. The job's ATS platform is in `RANK4_ELIGIBLE_ATS` — the 12 platforms
   live-verified (2026-09 probe, ~25-30 real companies sampled per
   platform) to reliably return **both** a location and application-question
   value on the same job: **Greenhouse, Workable, Personio, JazzHR,
   Teamtailor, Recruitee, Lever, Eploy, PageUp, isolvedhire, Pinpoint,
   Rippling**. Notably, Ashby did **not** qualify (application questions
   are auth-walled) despite looking like an obvious candidate.
3. `config.ENABLE_RANK4_COUNTRY_SPECIFIC` is on (a workflow_dispatch
   checkbox in `crawl.yml`, default unchecked — Rank 4 doesn't silently
   start writing rows until a run explicitly opts in).
4. The job's `description_snippet` actually contains an `"Application
   Question:"` marker — Rank 4's whole premise is that questions were
   fetched and are genuinely silent on the topic, not that they were never
   fetched at all. This is also why Crawl III can never qualify (its
   stapply.ai CSVs carry no application questions at all) even though it
   shares the same `filter_locations()` code as Crawl I via import — and
   why Crawl II *can* qualify in principle (it runs the same
   `enrich_application_questions_async()` Crawl I does) even though every
   current Crawl II row is tagged a generic `"in_house"` ATS label, so the
   gate is correct but presently inert there.

**4a — bare country/region/continent.** The location field, once every
recognized place name is stripped out, has nothing left over: it's
*entirely* made of one or more allowed names (`_RANK4_PLACE_RE`).
**US states and equivalents never qualify** — a bare "California" or
"Ontario" alone never matches, by design.

2026-09 (explicit user instruction, verbatim country list): the COUNTRY
portion of `_RANK4_PLACE_RE` is a deliberately narrow, Rank-4-specific
allowlist (`_RANK4_ELIGIBLE_COUNTRIES_RE_FRAGMENT`) — **only** US, UK,
Canada, Australia, Germany, Ireland, Singapore, Luxembourg, Norway,
Switzerland, Denmark, Netherlands, Iceland, Sweden, and Italy (plus
"variations of these," e.g. USA/U.S./United States all count as one).
This is distinct from `_COUNTRY_AUTH_NAMES_RE_FRAGMENT`, the much broader
country list used everywhere ELSE in this file for *exclusion* purposes
("does this text name a specific, non-global place") — that one stays
broad on purpose, since a JD naming e.g. Japan or Brazil as a restriction
has to be caught regardless of whether Japan/Brazil is a market this
project ever wants Rank 4 *admitting*. The region/continent portion is
**unchanged** and stays broad: business regions (APAC/LATAM/MENA/AMER/
ANZ/DACH/Benelux/Nordics/Gulf/EU/…) and continents (Europe/Asia/North
America/…) all still qualify — explicit user confirmation ("regions are
allowed too, like LATAM, AMER, etc.").

**4a (city variant) — "City, Country."** A city named alongside one of
the 15 eligible countries (`_RANK4_CITY_COMMA_COUNTRY_RE`) — e.g. "Sydney,
Australia", "London, United Kingdom" — is admitted the same as the bare
country, since it's at least as specific. Anchored to the whole location
string (not a bare city name floating in a longer sentence) and excludes
a US state in the city position ("California, United States" still never
qualifies — same "states don't count" policy as 4a's bare-country case).
A city paired with a country NOT on the 15-country list (e.g. "Lagos,
Nigeria") is still rejected.

**4b — mixed signal.** The location field is something else (a city, most
often) but the title or JD independently names an allowed region/country —
a real signal pointing a different direction, not a contradiction. Worked
example that motivated this tier: a "Customer Success Manager, APAC" role
with location "Tokyo" — the location is a specific city, but the title's
own region tag is real, unaddressed-elsewhere evidence. Note: since the
region/continent portion of `_RANK4_PLACE_RE` is unchanged, a posting
naming "European Union"/"EU" (still recognized, distinct from the 15-
country allowlist above) can still admit at 4b through that path.

**The exclusion gate** (any one of these forces `None` — job stays
dropped): 13 existing hard-override functions, reused unchanged from the
main pipeline (`has_hard_no_sponsorship_signal`,
`has_non_remote_workplace_type`, `has_non_remote_title_signal`,
`has_non_remote_labeled_text_signal`, `has_hard_country_specific_auth_signal`,
`has_state_list_restriction_signal`,
`has_hard_country_based_restriction_signal`,
`has_hard_metadata_location_signal`, `has_hard_location_symbol_signal`,
`has_office_attendance_signal`, `has_entity_or_exclusion_restriction_signal`,
`has_timezone_relocation_or_hyphenated_restriction_signal`,
`has_extra_restrictive_geography_signal`), plus one Rank-4-specific
addition: `_rank4_has_country_tied_restrictive_question` — catches a
sponsorship/work-permit/residency **question or requirement** tied to a
named country (e.g. "Will you require visa sponsorship to work in the
United States?"), which none of the 13 reused checks caught on their own
(they only caught "authorized to work in `<country>`" phrasing, not
sponsorship/permit/residency phrasing), **plus** (2026-09) a *referential*
work-authorization question that refers to "wherever this role is
located/listed" instead of naming a country directly — see "A referential
work-authorization question" above. It has its own benefit-language guard
so a company *offering* relocation/residency help isn't misread as
requiring it.

A country-agnostic version of the same question (no country named) is
**not** disqualifying — that's the tier's other worked example: a
"Customer Success Manager, EMEA" role with location "London" that asks
"Will you now or in the future require sponsorship for a work visa?" with
no country attached is admitted, because nothing ties that question to a
specific place. (In practice this exact EMEA case is already caught
earlier, at Rank 2, by the base pipeline's own EMEA handling — true new
Rank 4 territory is non-EMEA bare regions/countries.)

## Priority values at a glance

| Value | Meaning | Set by | Roles |
|---|---|---|---|
| `1` | Global | regex only | all |
| `2` | EMEA / Africa | regex only | all |
| `3a` | Blank location, LLM found real evidence | LLM (demoted, never promoted) | all |
| `3b` | Bare "Remote", real signal, AI-confirmed or genuinely uncertain | LLM or kept-by-default | all |
| `4a` | Bare country/region/continent, no restrictive tie | regex only, Crawl I/II | CS/AM only |
| `4b` | Title/JD names a region, location is a different place | regex only, Crawl I/II | CS/AM only |
| *(dropped)* | Keyword no-match, or AI no_match/blank-unconfirmed, or a Rank-4 candidate that fails its exclusion gate | — | — |

## Where this lives in code

- `classifier.py`: `_keyword_classify_location_detail()` (Rank 1/2 +
  unsure-reason), `classify_rank4()` (Rank 4), the priority constants.
- `crawl_i.py` / `crawl_ii.py`: `filter_locations()` /
  `_filter_locations()` — wire the AI-demotion policy (3a/3b) and the
  Rank 4 eligibility gate + fallback call. `crawl_iii.py` imports
  `crawl_i.py`'s `filter_locations()` directly, so it inherits all of this
  without its own copy.
- `config.py`: `ENABLE_RANK4_COUNTRY_SPECIFIC`.
- `.github/workflows/crawl.yml`: the `enable_rank4_country_specific`
  checkbox, wired into the Crawl I and Crawl II job steps (not Crawl III —
  its stapply.ai CSVs never carry application questions, so the gate can
  never pass there regardless of this flag).
