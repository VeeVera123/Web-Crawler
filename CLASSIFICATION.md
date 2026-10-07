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

2026-09 BUG FIX (explicit user report, real posting: ShipBob's Greenhouse
listing — location field "Sydney, New South Wales, Australia", screening
question just "What's your citizenship / employment eligibility?"): every
alternative above requires the question to explicitly SAY "where this
role is located/based/listed" — a much terser question like this one,
with no country named and no referential wording at all, matched
nothing. But the underlying intent is identical: a company asking a
Sydney applicant their "citizenship / employment eligibility" obviously
means "eligible to work in Australia" — the verbose "where this role is
located" phrasing was just one way some companies happen to phrase the
same question, not a requirement of the intent itself. Added a genuinely
BARE citizenship/work-authorization/eligibility-status question (no
place named anywhere) as its own alternative, gated by the exact same
"does the job already name one specific, narrow place" check the
existing referential alternatives already use — so it's still safe on a
genuinely location-agnostic job (blank/bare-Remote/broad location),
and requires an interrogative/imperative framing ("what's your...",
"please confirm...", "are you...", "do you have...") so it doesn't also
match plain DEI/company-values prose that merely mentions "citizenship"
in passing.

### Sentence-splitting bug (fixed, affected every sentence-level check)

2026-09 BUG FIX (explicit user report, real posting: CentralReach's
Greenhouse "Customer Success Lead" — "...we will consider remote
candidates located in other U.S. states..." was silently passing every
restriction check). Six separate hard-override functions (`has_hard_
country_based_restriction_signal`, `has_extra_restrictive_geography_
signal`, `has_role_specific_place_restriction_signal`,
`_rank4_has_country_tied_restrictive_question`, and others) split JD text
into sentences via a naive `re.split(r"(?<=[.!?])\s+|\n+", text)` before
checking each one — and that naive split treats the period INSIDE "U.S."
as a sentence end, silently cutting "...located in other U.S." away from
"states for the right individual...", so neither half matched anything.
Same risk for "U.K.", "U.A.E.", or any short dot-separated abbreviation
appearing mid-sentence. Fixed once, centrally: `_split_into_sentences()`
does the same split, then re-joins a fragment that ends in a short
ALL-CAPS-style abbreviation shape immediately followed by a lowercase
continuation (a real sentence essentially never does this) — used by all
six call sites now, so the fix applies everywhere at once.

### Adversarial fuzz-test round (2026-09, explicit user-commissioned test)

An external LLM (OpenAI) ran ~4,820 generated restrictive phrasings directly
against `_keyword_classify_location_detail` with a bare-`"Remote"` location,
claiming 41.3% leaked to `"unsure"` instead of `no_match`. Per the user's own
instruction ("verify and ignore where it is wrong, you have full context here
so you know better what to and what not to let in"), every specific claimed
leak was independently re-tested against the live code before any fix was
written — the report's numbers were not trusted at face value, but its core
finding held up: real, confirmed gaps across most of the categories it named.

**The single largest contributor**, closing roughly a third of the leaked
phrasings on its own: `_RANK4_COUNTRY_TIED_RESTRICTION_RE` (sponsorship/
work-permit/residency tied to a named country — "require visa sponsorship to
work in the United States," "need a work permit for Germany," "maintain
residence in the UK") already existed and worked correctly, but was wired
into **Rank 4's own exclusion check only**, never into the universal
`_keyword_classify_location_detail` hard-override chain every other rank
runs. Extracted into a new universal function,
`has_country_tied_sponsorship_permit_residency_signal()`, added to the main
override chain (right after `has_hard_country_specific_auth_signal`) so it
now gates Rank 1/2/3a/3b too, not just Rank 4.
`_rank4_has_country_tied_restrictive_question` is now a thin wrapper: it
still checks the Rank-4-only referential-question case first, then delegates
to the new universal function.

**Remaining gaps, closed with targeted extensions to the existing, already-
established regexes** (each verified as a real gap before writing the fix,
not applied on the report's say-so alone):
- Residence: "live AND work in X" (interposed verb broke the plain clause),
  bare noun-phrase "resident of/in X", "work remotely (only) from X",
  passive "worked from X", noun-phrase "citizen of X".
- Employment infrastructure: "cannot employ ... where we LACK a legal
  entity" (vs. the existing "don't have"), and — in both this new clause and
  the pre-existing "can only employ ... where we have" clause — accepting
  "local entity" as well as "legal entity". Also a vague "must be in a
  supported/approved payroll country" family with no country actually named.
- Country/region exclusivity: `_COUNTRY_AUTH_NAMES_RE_FRAGMENT` was missing
  the bare abbreviation `"amer"` (only `"americas"` was present). A bare
  `"<place> only."` end-anchored pattern was added to
  `_HIRING_LIMITED_TO_PLACE_RE`, using the combined countries+US-states
  fragment so it also catches `"California only."`.
- Exclusion-outside: reversed sentence order, "outside X ... are not
  eligible" (the existing pattern only covered the forward order).
- Timezone: declarative (non-verb) phrasing — `"<region> business hours
  only"`, `"<timezone> required"`.
- Office attendance: "able to/must work from an/the office".
- Workplace: `"on-site/in-office/in-person role/position/job"` (either word
  order) added to `_STANDALONE_NON_REMOTE_PHRASE_RE`.

**Deliberately NOT changed**: the report flagged `"Candidates must overlap
9am-5pm Pacific Time"` as a leak. Left as-is — "overlap" scheduling language
was already an established, evidence-based "safe" carve-out documented in
`_TIMEZONE_LOCATION_RE`'s own comments (overlap hours describe a scheduling
courtesy, not a hard location restriction), and that prior real-evidence
decision was trusted over an unverified synthetic fuzz example, per the
user's explicit instruction to ignore the report where it's wrong.

**A genuine, separate bug the report also surfaced**: `_DESC_LOCATION_LABEL_RE`
(the regex that recovers a labeled location value like `"Location: Bowling
Green, OH"` from free JD text) required only optional whitespace around a
hyphen separator — so the compound word `"Location-agnostic"` (itself a
`GLOBAL_KEYWORDS` member) was misparsed as label `"Location"` + value
`"agnostic role"`, silently overwriting the bare location field with
nonsense text and permanently dropping an explicitly global-hiring job
(`no_match`, before the LLM ever saw it) instead of matching it. Fixed by
requiring a hyphen separator to have whitespace on both sides to count as a
label (a colon may still abut the value directly, since `"Location:X"` is
unambiguous) — confirmed the original motivating real case
("Location: Bowling Green, OH") still parses correctly, and the job now
correctly falls through to the LLM as `"unsure"` instead of being dropped.

**Investigated and left alone**: the report also claimed the post-AI
`_apply_location_ai_authority_gate()` only rechecks 2 of the ~17 hard-
override functions (`has_role_specific_place_restriction_signal`,
`has_extra_restrictive_geography_signal`), rather than the full set. This is
accurate as a description of the code, but every job that reaches this gate
already got `"unsure"` from `_keyword_classify_location_detail` first — which
means all ~17 checks, including the new universal ones above, already ran
clean against that exact job before the LLM was ever asked. Widening the
gate to recheck more functions would be redundant under every current call
path. Deliberately left as a pure LLM-authority veto scoped to the two
checks it already has: once a job reaches the LLM, the LLM's verdict stands
unless one of those two catches it — that's the explicit, single-source-of-
truth review this pass confirmed, not a gap to close.

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
2. The job's ATS platform is in `RANK4_ELIGIBLE_ATS` — the 13 platforms
   live-verified to reliably return **both** a location and
   application-question value on the same job: **Greenhouse, Workable,
   Personio, JazzHR, Teamtailor, Recruitee, Lever, PageUp, isolvedhire,
   Pinpoint, Rippling, Ashby, BambooHR**. BambooHR was added (2026-10): its
   careers page is JavaScript-rendered, but the public `/careers/{id}/detail` JSON
   carries `formFields.customQuestions` (15/15 sampled postings readable). Eploy was removed (2026-10): it has no
   dedicated question fetcher. Ashby was excluded in 2026-09 because its
   posting API started returning 401, and added in 2026-10 once questions
   were fetched through its GraphQL endpoint (72 sampled jobs: location on
   all, form fetch OK on all, at least one real question on 58).

   **2026-10 re-check (explicit user request — "expand the list... maybe
   Ashby... top 5", then "increase company count"):** re-verified live
   against real companies in `archive_i` (see `ats_probe.py` /
   `.github/workflows/ats_probe.yml`, a standalone diagnostic, not part
   of the crawl pipeline) rather than trusting the 2026-09 note at face
   value. Two passes: 12 companies/platform, then 40 companies/platform
   (240 companies total) once the first pass already looked decisive.
   Final (40-per-platform) result:

   | Platform | Companies w/ jobs | Location % | Description % | Questions % |
   |---|---|---|---|---|
   | Ashby | 31/40 | 100% | 100% | **0%** |
   | Workday | 16/40 | 100% | 0% | **0%** |
   | BambooHR | 26/40 | 96% | 0% | **0%** |
   | SmartRecruiters | 8/40 | 100% | 0% | **0%** |
   | ADP | 14/40 | 100% | 0% | **0%** |
   | iCIMS | 2/40 | 0% | 0% | 50%¹ |

   ¹ iCIMS's one "hit" (out of 40 real companies) turned out to be a
   confirmed **false positive**, not a real question — see below.

   Root cause confirmed per platform, not just observed as a number:
   - **Ashby**: its only form-schema endpoint
     (`posting-api/posting/{slug}/{id}`) returns a hard `401 Unauthorized`
     for every company tested (ramp, vesta, cambly, notion, linear), and
     the public `/application` page is a pure client-rendered SPA with no
     form schema anywhere in the static HTML. The real candidate-facing
     flow uses a *different*, non-staff GraphQL endpoint
     (`jobs.ashbyhq.com/api/non-user-graphql`, operation
     `ApiJobPostingForApplicationRequest`) — but `jobs.ashbyhq.com/robots.txt`
     explicitly disallows `/api/`, so this path is closed on explicit
     instruction, not just inconvenient. (Unrelated: this doesn't affect
     the job-*listing* data every scraper already pulls from
     `api.ashbyhq.com/posting-api/job-board/{slug}`, a different host
     whose own robots.txt has no such rule.)
   - **ADP**: has a genuine dedicated API (`_fetch_adp_questions` reads
     `screeningRequirements` from a real per-requisition JSON endpoint) —
     confirmed live across 5 companies / ~36 jobs that `screeningRequirements`
     and `sponsoredVisaTypeCodes` are an empty array *every time*. Not a
     parsing bug: ADP Workforce Now's custom-screening-question feature
     appears to see near-zero real-world adoption among sampled customers.
   - **iCIMS**: the dominant failure is an **adaptive bot-defense system**
     (likely Akamai Bot Manager), not a scraper bug — direct requests to
     real tenant sitemaps returned `"Your IP address is not on a trusted
     network"` for most live tenants, and varying request headers changed
     the block to a different wall (`"Human Verification"` CAPTCHA)
     rather than removing it — the signature of an adaptive WAF, not a
     static check a header tweak can satisfy. A smaller share of sampled
     slugs were simply stale/deprovisioned tenants (`410 gone`), a
     discovery-data-staleness issue, not a scraper fault. Redirects for
     consolidated tenants (e.g. one tenant 301-redirecting to another)
     already work correctly via `requests.Session`'s default behavior —
     confirmed NOT a bug.
   - **iCIMS's one "success"** (`academiccareers-udst`, a university career
     site) traced to a real, separate, now-fixed bug: `_discover_real_apply_link`
     found a generic "How to Apply" link elsewhere on the page that
     actually led to the university's own *student admissions* page
     (`udst.edu.qa/admissions/how-apply`), not a job form — that page's own
     "Search by Keyword" field then got scraped and reported as
     `"Application Question: Search by Keyword"`. Fixed with a new
     negative-context guard (`_APPLY_LINK_NEGATIVE_CONTEXT_RE`) rejecting
     admissions/financial-aid/scholarship-shaped links even when they
     otherwise score as apply-shaped — see its own comment in
     `ats_scrapers.py`. This affects every platform using the shared
     Level-3 generic-form fallback, not just iCIMS, so it was a real,
     previously-undetected false-positive risk for Rank 4 generally,
     caught only because the larger sample happened to include a
     university career site.
   - **SmartRecruiters**: at 12 companies its `archive_i` rows were all
     garbage slugs (`--x3e`, `.well-known`, `%22https:`); at 40 companies,
     8 real ones turned up with real jobs and real locations — still 0%
     questions.

   **Conclusion: `RANK4_ELIGIBLE_ATS` stays unchanged.** None of the 6
   candidates clears the bar at either sample size — adding any of them
   would make Rank 4 admit jobs on "no restrictive question found" when
   the real reason is "no questions were ever fetched," reopening the
   exact false-negative risk this gate exists to prevent. The apply-link
   false-positive bug found along the way, however, was real and is
   fixed regardless of this conclusion.
3. `config.ENABLE_RANK4_COUNTRY_SPECIFIC` is on — a `crawl.yml` checkbox,
   on by default (2026-10, explicit user instruction: "rank 4 included by
   default but can still be manually turned off"). Always on for the
   unattended cron schedule (`github.event_name == 'schedule' || inputs.
   enable_rank4_country_specific` — schedule events never populate
   `inputs.*` at all, so the bare input alone would otherwise evaluate to
   off); a manual dispatch run still gets it unless the box is explicitly
   unticked. Was opt-in/default-unchecked while the tier was new and
   unproven; that caution is no longer needed.
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

2026-09 EXPANSION (explicit user correction, verbatim: "if the title is:
CSM, EMEA or global or Africa or variations of these, and location is
London, its let in because we allow EMEA, global/Africa are things we
accept... the same applies vice versa... to be let into 4b you must have a
mixed signal, something saying yes and no"): 4b's "independently names an
allowed region/country" test now ALSO recognizes the Rank 1/2 accepted
broad-signal family — Global (~80 GLOBAL_KEYWORDS variants), EMEA, Africa,
2+ business regions together — not just Rank 4's own 15-country/region
list. This is checked in THREE places, matching each half of the user's
worked example: the location field itself (`loc`, post-enrichment — the
"location says EMEA [but qualified with a city]" direction), the title
(via `_TITLE_MULTI_REGION_WORDS_RE`, the same regex `_enrich_location_
from_title` already uses for "CSM - EMEA"-style suffixes — a bare word is
safe to trust in a short, curated field), and the description
(`_text_has_global_evidence`/`_text_has_africa_or_emea_evidence`/
`_has_multi_region_breadth`, deliberately more conservative — requires an
explicit phrase like "we hire globally," not just the bare word "global,"
to avoid marketing-copy false positives like "our global network" in
free body text). A CLEAN, unqualified Global/EMEA/Africa location field
never reaches this code at all — it's already Rank 1/2 upstream — so
a hit here always means the signal was mixed with something else.

2026-09 BUG FIX (explicit user report, real production data: bare
DISALLOWED locations — `"India"`, `"Shanghai"`, `"South Africa"`,
`"Bengaluru, India Office"` — were being admitted at 4b, and separately,
`"Germany Remote"` was landing in 4b/being dropped instead of the clean 4a
signal it should be):

- **Unscoped boilerplate leak (both 4b paths).** Both of 4b's "does the
  title/JD independently name an allowed place" checks — the country/
  region-list path (`_RANK4_PLACE_RE.search(full_text)`) and the Global/
  EMEA/Africa path described above — used to scan the ENTIRE title+
  description blob with no requirement that the match have anything to do
  with *this role's own* hiring scope. Routine "About us" company
  boilerplate ("we have teams across EMEA, APAC, and the Americas") was
  enough to admit an otherwise-disallowed location at 4b, since it isn't
  covered by the Global/EMEA path's own (narrower) marketing-boilerplate
  exclusion list. Fixed by requiring a hiring-context word (work/hire/
  recruit/employ/candidate/applicant/based/located/available/open/role/
  position) within 80 characters of the matched place — same proximity-
  guard shape `_STRICT_BROAD_REGION_RE` already used for EMEA/Africa
  elsewhere in this file, generalized to Rank 4's own vocabulary
  (`_RANK4_STRICT_PLACE_RE`) and to the Global/EMEA/Africa full-text scan
  (now sentence-scoped via `_split_into_sentences`, same idea). Genuine
  evidence ("we hire globally," "hiring across LATAM for this role") still
  admits; generic company description no longer does.
- **"Germany Remote" wrongly excluded from 4a.** 4a's "remainder must be
  empty once every place name is stripped" check didn't strip generic
  non-geographic filler ("remote," "office," "role," …) the way the main
  pipeline's own EMEA/Global residue checks already do via
  `NON_GEO_WORDS_RE` — so "remote" survived as leftover residue and made
  an otherwise-clean single-country signal look like a second, unrecognized
  place. Now applies the same `NON_GEO_WORDS_RE` strip Rank 1/2's residue
  checks already use, so "Germany Remote"/"Remote - Germany" correctly
  land at 4a alongside bare "Germany".

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

**The silent-form rule (2026-10 rewrite — supersedes the paragraph that used to
sit here, which said a country-agnostic sponsorship question was not
disqualifying).** Rank 4's premise is "if the form never asks, the company may let
you work from anywhere". So a job whose location is ONE country or city is admitted
only if its application form is *silent on eligibility*: **any** question about
work authorization, right to work, visa/sponsorship, immigration, citizenship,
residency, relocation, commuting or on-site presence rejects it — whether or not it
names a country, in any phrasing, in English or the major EU languages
(`rank4_question_eligibility_hit`, a topic list, not a phrase list) — and so does a
title/JD sentence that *requires* eligibility, sponsorship or residency
(`rank4_text_eligibility_requirement_hit`; a sentence that only offers help, "we offer
visa support", is not a requirement). Real leaks this closed: OpenLoop ("Will you now
or in the future require visa sponsorship? This includes initiating, continuing or
transferring your visa…", location United States), Tenable ("Do you have the legal
right to work in the country within which you are applying?", "…require
sponsorship?", location Australia), Mollie ("Do you require visa sponsorship or a visa
transfer?", location London), Sanity ("…require visa sponsorship… (H1B, Blue Card)",
"Remote in the United States"). Each had been "fixed" before by adding one more
phrase to `_REFERENTIAL_AUTH_QUESTION_RE`; the wording changed and it leaked again.

The scope decides how strict the form check is: **narrow** (the location field names
a curated country, "City, Country", or only a country-level signal) rejects every
eligibility question; **broad** (a region NAME such as APAC/LATAM/Europe, or a bare
city whose title/JD claims EMEA/Global) rejects only questions that name a specific
place (the documented "CSM, EMEA" + London + bare sponsorship question stays
admitted). A "Global" word in a title never makes a job broad when the location
field itself names a country ("Remote in the United States" + "Global Account
Management" is a US job). Relocation / commute / on-site / clearance questions
reject at any scope.

**The location must be an allowed place (2026-10).** The old guard was a deny-list
(`_RANK4_ANY_NAMED_COUNTRY_RE`) followed by "admit on any title/JD regional wording",
so "Bangkok" and "Brasil" (not in the deny-list) were admitted. Now the location
field must positively resolve (`_rank4_location_resolves_to_allowed`): curated
countries (plus native names such as Deutschland), region NAMES, "City, <curated
country>", or a known city of a curated non-US country plus a regional/global signal.
A region's member countries and cities (Thailand/Bangkok for APAC, Brazil for LATAM),
states and provinces, foreign-city lists ("Singapore, Bangkok"), non-Latin scripts and
workplace words in the location text (Hybrid, Office, On-site, without Remote) all
reject. Known limit: "`<foreign city>, <curated country>`" is only caught when the
foreign city is in `_RANK4_FOREIGN_CITIES_FRAGMENT` (about 400 cities).

`rank4_rejection_reason(job)` names the requirement that failed, and revalidate logs
it. `check_rank4.py` is the regression corpus (17,000+ generated cases: location
shapes, eligibility questions in ~60 phrasings plus a grammar fuzz, JD sentences,
benefit wording, ordinary questions that must still pass).

`check_corpus.py` runs an externally written, tagged question corpus
(`corpus/*.txt`, one `CATEGORY [NAMED|BARE|BENIGN|AMBIGUOUS] question` per line)
through four checkpoints: Rank 4 for a one-country job, Rank 4 for "City, Country",
Rank 4 for a region-name job (named-place questions only) and the universal Rank 1/2/3
path (named-place questions only). `corpus/restrictive_questions_openai_1.txt` is the
first such file (about 620 lines: work authorization, visa/sponsorship and named visa
categories, immigration status, citizenship, residence, relocation, commuting, on-site,
clearance, export control / "US person", licensing, background checks, time zones, tax
residency, payroll / legal-entity gates, language proxies, German/French/Italian/Dutch/
Nordic/Portuguese/Spanish). Two deliberate departures from its tags: a *generic* background
/ credit / screening check is kept (nearly every form asks one and it is not an
eligibility test), and a question that names no place ("where this role is located") is
only rejected through the referential check once the job's own location names a country.
Four more corpora sit beside it: `corpus/benign_look_alikes.txt` (about 335 ordinary questions
that contain eligibility-sounding words: "Visa" the card network, event sponsorships, citizen
developers, Passport.js, resident processes, relocating servers...; all must be kept),
`corpus/jd_sentences.txt` (about 195 description sentences, restrictive and benign),
`corpus/adversarial.txt` (word-order variants, EU/EEA/Schengen, city-level residency, federal/DoD,
data residency, shipping/embargo, PAYE, non-English requirement sentences, soft language) and
`check_form_fields.py` (answer options, hard-wrapped questions, a transformation fuzz that re-writes
every restrictive question with fullwidth letters, zero-width / soft-hyphen characters, NBSPs,
upper-casing and decoration, and the fetcher helpers).

**Answer options.** Fetchers append `Application Options: <question> => opt | opt ...` after a
question (Greenhouse `fields[].values`, Ashby `selectableValues`, embedded JSON option lists and
`<select>` elements; Yes/No answers and lists over 40 are dropped). An option that is itself a
place-bound eligibility statement, or a short closed country list (2-15 places, no "Other / Rest of
world", none African) on a "where do you live / which country" question, rejects the job on every
path; bare option statuses ("I require sponsorship", "H-1B") are Rank 4 narrow-scope rejections like
a bare question. Questions are collapsed to one line at the source, and the classifier re-joins a
wrapped question in rows stored earlier.

**Description sentences** are read by `has_candidate_binding_jd_signal`: a sentence must carry
requirement framing, be about the candidate, match one binding family, and not be a skills or
benefit sentence ("experience with ITAR", "we offer relocation assistance"). Plain company facts
("we have offices in Germany", "our EOR partner operates in 100 countries") never match.

Questions framed as *experience with* a topic ("experience with payroll software",
"Visa or Mastercard") are exempt from the Rank 4 eligibility vocabulary, and a place that
only appears as a format example ("e.g. San Jose, CA") never binds an English question.

### "Informational" region mentions vs. a residency enforcement

2026-09 (explicit user policy, verbatim: "a JD saying based in one our
(allowed country/region) offices should not be allowed. Regions are
allowed... The aim of rank 4 is that maybe if they did not ask an app
question they may be willing to allow you work from anywhere and don't
really need u to work from there. So them asking you to be based there is
a no. Just bare location field is what we are taking, an enforcement
stating in the sentence/jd that they require you to stay/reside there is
a no."): real posting, Hex Trust's Workable listing — title
"Relationship Manager - Wealth Management (Middle East)", description
tying the role to Dubai/Riyadh/Istanbul specifically. 4b's own "does the
title/JD independently name an allowed region" check (above) was treating
a sentence like "the successful candidate will be based in one of our
Middle East offices" as supporting MIXED-SIGNAL evidence — the opposite
of what it should do. Middle East/MENA/Gulf stay accepted regions (the
user's explicit instruction: "regions are allowed") — the distinction
isn't about WHICH region is named, it's about HOW it's mentioned:

- **Informational** — a title suffix ("CSM - EMEA"), or a JD clause like
  "we hire across LATAM for this role" — still legitimate 4b evidence,
  unchanged.
- **Enforcement** — a sentence that ties the CANDIDATE/ROLE to physically
  being in that region: "this role is based in our Middle East offices,"
  "must reside in APAC," "residency in EMEA is required." Exactly as
  disqualifying as naming one specific country, even though the place
  itself is an otherwise-accepted region — this is new, Rank-4-specific
  logic (`has_rank4_region_residency_enforcement_signal`), since region
  words were never part of any restriction vocabulary before (they were
  only ever treated as ACCEPTED evidence elsewhere in this file).

Two follow-on fixes this required:
- `based`/`located` were removed from the hiring-context word list that
  gates 4b's existing "is this region mention actually about hiring"
  check (§ above) — those two words describe physical presence, not
  merely "this text is about hiring" the way `hire`/`role`/`candidates`
  do, so keeping them there would have let the exact enforcement
  sentences this section exists to catch also count as supporting
  evidence for admission.
- A sentence describing the company's EXISTING workforce ("our globally
  distributed team includes engineers based in Germany, India, and
  Brazil") is NOT an enforcement on the candidate, just company
  description — excluded via a workforce-noun guard (team/engineers/
  employees/staff/workforce/colleagues/workers/people in the same
  sentence), the same "is this about THIS role, or the company in
  general" distinction already used for marketing-boilerplate exclusions
  elsewhere in this file.

### Application-question leaks: state licenses, metro-area residency, interrogative onsite, language fluency

2026-09 (explicit user report, three real postings, plus a user-initiated
policy mid-fix). First confirmed, directly against `ats_scrapers.py`'s
Greenhouse question-fetching code (`_fetch_greenhouse_questions` /
`_format_screening_questions`), that application questions genuinely were
being scraped and appended into `description_snippet` for all of these —
`_BOILERPLATE_QUESTION_RE` only filters pure PII/EEO fields (name, email,
resume, race, gender, ...), and none of the three real questions below
match it. This was a classifier-regex gap, not a scraper gap.

1. **State-specific professional/occupational license** — SideCar
   Health's Greenhouse posting asked "Do you have a Texas State Health
   and Life insurance license?" No existing check anywhere in this file
   looked for the word "license" at all. A US state licenses insurance,
   nursing, real-estate, and similar professions per-state, so a company
   screening for one is screening for someone already licensed there (or
   willing to become so) — exactly as restrictive as naming the state
   directly. New universal check: `has_state_specific_license_signal`
   (`_STATE_PROFESSIONAL_LICENSE_RE`).

2. **City/metro area + 2-letter state abbreviation** — the same SideCar
   Health posting also asked "Do you currently reside in the Dallas/Fort
   Worth, TX area?" Every existing residence-verb check
   (`_COUNTRY_BASED_RESTRICTION_RE`) only recognized a full country name
   or a full, spelled-out US state name (`_US_STATE_FULL_NAMES_FRAGMENT`)
   — never a city/metro name paired with a 2-letter abbreviation. Added
   as a **separate** function, `_has_metro_area_state_abbr_signal`,
   deliberately NOT folded into `_COUNTRY_BASED_RESTRICTION_RE`'s
   case-insensitive alternatives: a bug caught during this fix's own
   testing showed that matching the 2-letter abbreviation
   case-insensitively lets an ordinary lowercase word standing right
   after a comma get misread as a state ("reside in a place that is
   calm, **in** order to focus" matched "in" as Indiana). Fixed by
   matching case-insensitively for the verb/city portion but validating
   the captured 2-letter token's exact written case against
   `_US_STATE_ABBRS` (a set of literal uppercase strings) — a lowercase
   or mixed-case incidental match fails that membership check, while a
   real "TX"/"CA" (always written uppercase in real postings) passes.
   Reuses the existing team/office/company-context sentence guard (the
   same one `_COUNTRY_BASED_RESTRICTION_RE` already gets), so "our
   headquarters is located in Austin, TX" is correctly NOT treated as a
   candidate residency requirement.

3. **Interrogative onsite/hybrid question** — Project A Services GmbH &
   Co KG's Greenhouse posting asked "Are you open to working fully onsite
   in Berlin?" `has_office_attendance_signal`'s existing patterns all
   required the literal word "office" ("willing to work **in our
   office**"); a question built around "onsite"/"in-office"/"in-person"/
   "hybrid" directly, with no "office" noun, fell through untouched.
   Extended `_OFFICE_ATTENDANCE_RE` with an alternative for "open/
   willing/able to work(ing) [up to 2 filler words] onsite/in-office/
   in-person/hybrid".

4. **Hard language-fluency requirement** — net-new policy, not a single
   posting but an explicit user instruction given mid-session: "roles
   with say CSM - GERMAN speaking, French speaking should be excluded
   too. If its phrased as a hard requirement in the requirements section
   it should not make it in. [if] its added as something that would be
   nice/the bonus section, then let it in." A role that genuinely
   requires fluency in a specific non-English language is, in practice,
   tied to that language's market the same way a named-country residency
   requirement is. New universal check,
   `has_language_fluency_restriction_signal`:
   - A **title** qualifier ("CSM - German Speaking", "French-Speaking
     Customer Success Manager") is always a hard reject — titles are
     definitional, there's no "nice to have" reading of a title.
   - A **description/application-question** mention only rejects when it
     sits in a hard-requirement context. Implemented by walking the text
     clause-by-clause (reusing `_split_into_sentences`, which already
     splits on newlines, so a standalone section-header line is its own
     entry), tracking an "in a nice-to-have section" flag that flips on
     recognizing a header like "Nice to Haves:"/"Bonus:"/"Preferred
     Qualifications:" and flips back off on a hard-requirement header
     like "Requirements:"/"Must Haves:"/"What You'll Need:". A mention
     with no section markers at all defaults to hard (the common case —
     most JDs stating a language requirement in plain prose mean it).
     An inline softener on the SAME clause ("German speaking is a
     plus") also neutralizes that one mention regardless of section.
   - English is deliberately excluded from the recognized language list
     — it's the global baseline for this project's postings, not a
     restrictive differentiator the way German/French/etc. are.
   - Both new checks are wired into `_RANK4_GENUINE_RESTRICTION_CHECKS`
     too, so a CS/AM role reaching Rank 4 (where a "CSM - German
     Speaking" title is actually most likely to show up) gets the same
     treatment.

### A sponsorship question naming a country past an "e.g./etc." aside

2026-09 BUG FIX (explicit user report, real posting: Pave's Greenhouse
"Account Manager" listing — job-boards.greenhouse.io/
paveakatroveinformationtechnologies/jobs/4726158005). The posting itself
was already correctly rejected for independent reasons (its location
field names "San Francisco, California, United States" directly, plus a
hybrid in-office schedule and a "which office are you applying to"
question) — confirmed via fetching the live posting before concluding
anything. But the sponsorship QUESTION itself, in isolation, revealed a
real latent gap: "Do you now, or will you in the future, require
sponsorship for employment visa status (e.g., H-1B visa status, etc.) to
work legally for our Company in the United States?" was NOT recognized
as a country-tied restriction by `has_country_tied_sponsorship_permit_
residency_signal` at all — meaning on a DIFFERENT posting where the
location field is bare/blank/Remote (and so can't independently kill the
job the way Pave's did), this exact phrasing would have fallen through to
the LLM stage instead of a deterministic reject.

This is a real gap, not a judgment call — `_RANK4_COUNTRY_TIED_
RESTRICTION_RE`'s own module comment already states the governing policy
explicitly (the Notabene example): "Will you now or in the future require
sponsorship for a work visa?" is fine ONLY because it names no country —
the identical question naming a country is a genuine restriction and must
exclude the job." Two separate things broke the existing alternatives on
this specific phrasing:

1. Every existing alternative measures the country's distance from
   "sponsorship" via a single preposition (for/to/in/within) landing
   within a tight ~60+30 character window. Here the real country-naming
   clause ("to work legally for our Company in the United States") sits
   100+ characters past the word "sponsorship", with a long intervening
   "(e.g., H-1B visa status, etc.)" aside — fixed by anchoring a new
   alternative on the actual authorization-shaped clause directly
   ("sponsorship ... to work ... in `<country>`") instead of a bare
   preposition, since that clause reliably sits close to the country name
   regardless of how verbose the preceding sponsorship clause gets.
2. Independently, "e.g." and "etc." each contain a literal period —
   which the existing alternatives' `[^.!?\n]` filler character class
   refuses to cross, even though neither one is an actual sentence
   boundary (`_split_into_sentences`'s own split regex only splits at a
   period directly followed by whitespace, which "e.g.," and "etc.)"
   aren't). Fixed, for the new alternative only, by using a plain `.` gap
   instead — safe here specifically because `has_country_tied_
   sponsorship_permit_residency_signal` already operates one already-
   split sentence at a time, so there's no risk of a loosened gap
   bleeding across two unrelated sentences.

Confirmed unaffected by this fix: the canonical country-FREE boilerplate
("Will you now or in the future require sponsorship for employment visa
status?") still does not reject on its own — that's deliberate,
documented behavior from an earlier fix (see `has_hard_country_specific_
auth_signal`'s own docstring: this exact question is "ubiquitous,
industry-standard EEO/I-9 compliance screening language" the overwhelming
majority of US-headquartered companies ask regardless of whether the role
is actually globally open). A benefit-framed country mention ("we offer
visa sponsorship to work in Canada") also still correctly falls through
to this same function's existing benefit-vs-requirement framing guard
(`_RANK4_BENEFIT_FRAMING_RE`/`_RANK4_REQUIREMENT_FRAMING_RE`), unaffected
by this fix since that guard runs on the whole sentence after the new
alternative matches it.

Known, separate, NOT-yet-fixed finding from this investigation: the
`[^.!?\n]` filler class used by essentially every OTHER gap-based regex
in this file carries the same "e.g./etc. defeats the match" weakness —
this fix only loosened it for the one new alternative added here. Any
restrictive sentence elsewhere containing "e.g." or "etc." between the
trigger word and the place name could, in principle, silently defeat that
check the same way. Left alone for now since fixing it everywhere is a
much larger, separate change than the one reported bug asked for.

### A city/subdivision name sitting between the preposition and the country

2026-09 SYSTEMIC BUG FIX (explicit user report, real posting: International
EOR's Greenhouse listing — job-boards.greenhouse.io/internationaleor/
jobs/8771051002 — "Are you legally authorized to work in London, England,
United Kingdom?"). The user's own framing: application questions that
straight-up ask for employment eligibility and name the country are
supposed to be a universal reject before anything else happens — so
finding one that slipped through meant something structural was wrong,
not a one-off phrasing gap.

Root cause, confirmed via a systematic test sweep (not a guess): nearly
every "(?:in|within) `<country>`"-shaped construction in this file —
across the authorization family (`_COUNTRY_AUTH_RE`: authorized/eligible/
entitled/permitted to work, work authorization, right to work, work
rights, permanent resident), the sponsorship/permit/residency family
(`_RANK4_COUNTRY_TIED_RESTRICTION_RE`), the residence-verb family
(`_COUNTRY_BASED_RESTRICTION_RE`: reside/live/based/located, citizen of,
resident of, worked from), the "hiring limited to" family
(`_HIRING_LIMITED_TO_PLACE_RE`), a title `"(<place> Based)"` parenthetical
(`_TITLE_COUNTRY_PAREN_RE`), and the entity/payroll and "`<place>`
residents only" families — assumed the country name sits IMMEDIATELY
after the preposition (give or take a bare "the"). That's true for "...in
the United Kingdom" but false for the extremely common real-world shape
where a city and/or subdivision name comes first and the country comes
LAST: "London, England, United Kingdom", "Austin, Texas, United States",
"Toronto, Ontario, Canada", "Sydney, New South Wales, Australia". None of
these matched anywhere in the file before this fix — this wasn't one
regex's gap, it was the same unstated assumption baked into roughly 30
separate alternatives across five different regexes.

Fixed with one shared, bounded fragment, `_PLACE_CHAIN_PREFIX_FRAGMENT`:
0-2 "`<word(s)>`, " segments (each up to 4 words), inserted between every
preposition and the country/region fragment it governs across all of the
families above. Deliberately bounded on both word count and segment
count, and requires each segment to end in a literal comma leading
directly into the next segment or the country — a shape real, unrelated
prose essentially never produces by coincidence (an ordinary sentence
with two arbitrary comma-separated phrases immediately followed by a
recognized country name, with nothing else in between, is specifically
how a place is written, not how English prose incidentally reads) — so
this doesn't meaningfully widen what these checks accept beyond genuine
"city, subdivision, country" phrasing. Confirmed via an extensive
adversarial test pass: a bare country still matches directly (unaffected,
control case), a company-office or marketing mention of a city with no
eligibility verb nearby stays unrejected (the existing team/office-context
guard already in place for several of these families keeps working
unmodified), the canonical country-free sponsorship boilerplate this
project already deliberately treats as non-disqualifying is untouched,
and a 4-segment chain exceeding the deliberate 2-segment cap correctly
does NOT match (by design — real postings essentially never need more
than "City, Subdivision, Country").

Also fixed in the same pass, found during the same test sweep rather than
reported separately: `_COUNTRY_AUTH_RE`'s "work authorization ... for
`<country>`" alternative required "authorization" to be followed
DIRECTLY by "in"/"for", missing the equally common "work authorization
**status** for `<country>`" phrasing with "status" interposed.

Left deliberately unfixed (lower realistic value, wrong grammatical
direction for a city-chain): the "`<country>`-based candidates" compound-
adjective family, where the country comes FIRST and a city/subdivision
chain would need to sit awkwardly BEFORE it rather than after; and
"employment through our `<country>` entity only," where a city-chain
would read unnaturally ("through our London, United Kingdom entity" isn't
how this phrasing actually appears in practice — it's almost always just
"our UK entity").

### A bare sponsorship question, and the pronoun "us" vs. the country "US"

2026-09 (explicit user question: "we delt with referential questions
right? Like it won't accept situations where a company says USA as
location but then asks: would you require sponsorship to work with us.
Here, USA is not mentioned in the application questions so idk if it can
tell this is referential"). Two separate bugs surfaced while answering
this directly, confirmed by testing rather than assumed.

**Bug 1 (a false positive, found while investigating — not what was
reported):** `_COUNTRY_AUTH_NAMES_RE_FRAGMENT`'s US entry was
`u\.?s\.?a?\.?` — every period AND the "a" were optional, so its minimal
possible match was the bare two letters "us". That collides with the
ordinary English pronoun "us" — "work with us," "join us," "tell us
about..." Confirmed live: "Would you require sponsorship to work with
us?" on a genuinely bare/Remote location (no country named at all) was
being hard-rejected, because the word "us" in "with us" was being read as
the country code. This would have caused real false-positive rejections
in production — a genuinely open, global role dropped just because its
sponsorship question happened to say "work with us" instead of "work for
this role." Fixed by requiring the bare, period-less form to be written
in **exact uppercase** ("US", never "us"/"Us"/"uS") via a scoped case-
sensitive sub-pattern (`\b(?-i:US)\b`) — real postings always write the
country abbreviation in caps, so this loses no real coverage. Every
period-containing variant ("U.S.", "U.S.A.") and the full "USA"/"United
States" forms stay case-insensitive as before, since none of those
collide with any ordinary English word in any casing.

**Bug 2 (the real gap the user asked about, confirmed once Bug 1 was
fixed so it could be tested cleanly without the pronoun collision masking
it):** a genuinely bare sponsorship question — no country named anywhere,
not even referentially ("where this role is located") — was not being
tied back to the job's own already-named location the way the bare
CITIZENSHIP question already is (see the ShipBob fix above). Confirmed:
`classify_rank4` admitted "Would you require sponsorship to work with
us?" as `4a` on a job whose location field said "United States" outright.
`has_country_tied_sponsorship_permit_residency_signal`/
`_rank4_has_country_tied_restrictive_question` both require the country
to be **named in the question text itself**, with no referential
fallback — unlike the citizenship family, which already had one. Fixed
by extending `_REFERENTIAL_AUTH_QUESTION_RE` with a bare-sponsorship
alternative ("would/will/do/does you/the candidate/the applicant ...
require/need ... sponsorship ... to work with/for us/here/this company"
or "... to join us/our team"), gated by the exact same "is a real,
specific, non-broad place already named" check both existing call sites
already apply — so it stays safe on a genuinely blank/bare-Remote/broad
location, benefit-of-the-doubt preserved, identical to the citizenship
fix.

## 2026-10: real production leak — 4b admitting disallowed US states and a raw street address

User-reported, from a real crawl run, not a hypothetical: rows written with
location `"New York, NY"`, `"Remote, New York"`,
`"Denver, CO; New York City, NY; San Francisco, CA"`, a long enumerated
multi-state list (`"Boston, Massachusetts; Chicago, Illinois; ..."`), and a
raw European street address (`"AT002 Industriestraße 2, 5303 Thalgau"`) —
every one `clearance='rank4'`, `location_priority='4b'`.

Root cause, confirmed by directly reproducing it against the exact title/
location pairs from the real rows: once `classify_rank4()`'s two 4a checks
both fail (the location is neither a bare eligible country/region nor a
clean "City, eligible-country" pair), the function falls through to 4b's
Global/EMEA/Africa-family broad-evidence check — which tests `loc`/`title`
for words like "Global"/"EMEA"/multi-region breadth with **no verification
at all that `loc` itself isn't already a flatly disallowed shape**. A title
as mundane as "Global Account Manager, Strategics (New York)" — "Global"
used as a job-level flourish, nothing to do with hiring geography — was
enough to grant 4b despite the location field unambiguously naming a
specific, disqualifying US state. 4b's own premise (title/JD supplies a
broader claim while the location field is merely a narrower-but-still-
*eligible* place, e.g. "EMEA" + "London") was never meant to let title/JD
language override a location field that's already concretely disqualifying
on its own.

Fixed with `_rank4_location_field_is_hard_disqualified(loc)`, checked
once, right after 4a fails, blocking **both** 4b paths (not just the
Global/EMEA/Africa one — `_RANK4_STRICT_PLACE_RE`'s mixed_title_or_jd_signal
path gets the same guard). Scoped to exactly what real evidence showed:

1. A US state, spelled out in full anywhere in `loc`.
2. A US state abbreviation immediately after a `"<word(s)>, "` prefix,
   validated against the real, closed `_US_STATE_ABBRS` set — not a bare
   `re.I` alternation of 2-letter codes, which would also match "ca"/"ny"
   inside ordinary words like "Canada"/"many" (same case-sensitivity
   discipline as `_has_metro_area_state_abbr_signal`, and the same
   `re.I`-vs-bare-letters bug class the 2026-09 top-to-bottom audit below
   found repeatedly elsewhere in this file).
3. A raw street address: a short digit run (a postal code, 4-6 digits)
   sitting directly in front of a word (the city name) — "5303 Thalgau" —
   a shape no legitimate bare country/region/city name ever takes.

Deliberately **not** extended to Canadian provinces or other shapes with
no real posting evidence yet — scoped to what was actually observed, same
discipline this file applies everywhere else. Verified the fix doesn't
regress the legitimate cases it sits right next to: `"Germany"` (4a),
`"Sydney, Australia"` (4a), `"CSM, EMEA"` + `"London"` (4b) all still
classify exactly as before. (Bare `"Kenya"`/`"Brazil"`/`"South Africa"`
with broad-hiring JD text used to also still admit at 4b — see the
immediately following policy change, which supersedes that for good
reason.)

## 2026-10: policy change — a non-curated country is never rescuable, even by genuine "we hire globally" text

Same day, explicit user instruction, directly prompted by the leak above:
*"even though jobs in Kenya and co mention global hiring, they should
still not make it. They must be in the list of allowed countries/
regions/continents to do so."*

Before this, a bare country **not** on Rank 4's own curated eligible list
(`_RANK4_PLACE_INNER_FRAGMENT` — US/UK/Canada/Australia/Germany/Ireland/
Singapore/Luxembourg/Norway/Switzerland/Denmark/Netherlands/Iceland/
Sweden/Italy, plus the named business regions) could still reach 4b
purely on the strength of genuine broad-hiring JD language ("we're a
fully remote, globally distributed team, hiring from anywhere") — this
was the actual, confirmed mechanism admitting the real Kenya/Brazil/
South Africa/Bangalore-India rows from the leak above. Not a bug in the
narrow sense (the broad-evidence detection itself was working correctly
and finding real language) — but a policy gap: genuine company-wide
hiring openness was being treated as sufficient on its own, when the
user's actual bar is narrower — the location itself must *also* be
somewhere Rank 4 already trusts.

Fixed with `_RANK4_ANY_NAMED_COUNTRY_RE`, reusing the project-wide,
much broader `_COUNTRY_AUTH_NAMES_RE_FRAGMENT` vocabulary (every real
country name this file recognizes anywhere, not just Rank 4's short
curated list). Checked against `remainder` — the location field with
every Rank-4-eligible place name already stripped out, the same value
4a's own bare-country check computes — not raw `loc`: this is what
keeps a bare eligible region abbreviation that also happens to sit in
the broader fragment (APAC/LATAM/Europe appear in both) from false-
positiving, since by construction `remainder` is only ever non-empty
here because 4a's bare-country check already failed to recognize
everything in it. If what's left over names a real country, that's an
unconditional reject — no title/JD language can override it. A bare
city with no country word at all (`"London"`) leaves no such match and
is unaffected, preserving the legitimate "title: `CSM, EMEA` / location:
`London`" 4b case the project has relied on since Rank 4 shipped.

Also closes a related, previously-accepted case the same way: Belgium
named in the location field (`"Brussels, Brussels, Belgium"`) used to
still admit at 4b when the JD separately mentioned "European Union
institutions" — Belgium itself was never curated, so this is now
rejected too, regardless of the EU mention.

Verified: Kenya/Brazil/South Africa/Bangalore-India (even restated with
genuine broad-hiring JD text) and the Belgium+EU case all now correctly
reject; `"Germany"` (4a), `"APAC"`/`"LATAM"` alone (4a, confirming no
false-positive from the broader fragment also containing those words),
`"Sydney, Australia"` (4a), and `"CSM, EMEA"` + `"London"` (4b) all still
classify exactly as before. 26 tests in
`test_rank4_location_hard_disqualify.py`, plus the pre-existing
`test_rank4_allowlist.py` updated to match the new, intentionally
stricter policy (not a reverted regression — the old expectation was
exactly the gap this change closes).

## Top-to-bottom audit (2026-09, explicit user-commissioned sweep)

The user asked for a full top-to-bottom pass over this file — every check,
hundreds of adversarial tests, no exceptions — rather than continuing to
fix one reported leak at a time. This section documents what that sweep
found. Every bug here was confirmed by direct testing before being fixed,
not guessed, and every fix was re-verified against the genuine true-
positive phrasing the check exists to catch, so no real coverage was lost.

### The systemic bug: `re.I` also case-folds a bare `[A-Z]` character class

This was, by far, the most consequential finding. Several patterns in this
file use `[A-Z]` specifically to require "this must be a real place name"
(proper nouns are capitalized) — but every one of those patterns is
compiled with `re.I` for the OTHER words in the same pattern (keywords
like "based"/"located"/"must"), and Python's `re.I` flag case-folds a
character class too: `[A-Z]` under `re.I` matches a lowercase letter just
as readily as an uppercase one. The "require a capital letter" guard was
silently doing nothing.

Confirmed, with real consequences, in:

- **`_ROLE_SPECIFIC_PLACE_RE` / `_CANDIDATE_PLACE_RE`** — the regexes
  behind `has_role_specific_place_restriction_signal`, **universal check
  #2**, one of the very first checks every job passes through. "This role
  is based in a hybrid of creativity and structure," "The candidate must
  be based in a culture of excellence," "This position is located in a
  world of endless possibility" — pure marketing language, naming no place
  at all — were all captured as if they named a real, specific location,
  and hard-rejected. Given how common this kind of "based in a culture
  of X" marketing phrasing is in real job descriptions, this was likely
  one of the single highest-volume sources of wrongly-dropped jobs in the
  whole pipeline.
- **`_EXTRA_RESTRICTIVE_PATTERNS`** (17 alternatives, feeding
  `has_extra_restrictive_geography_signal`, **universal check #11**) — same
  bug, same consequence: "must be based in a fast-paced, dynamic team
  environment," "we can only hire in the most talented and driven
  individuals" were both captured and rejected.
- **`_RELOCATION_REQUIRED_RE`** — "willing to relocate to a new city for
  this role," "...to wherever opportunity takes you," "...to pursue growth
  opportunities," "...to advance your career" all matched, since none of
  them actually need to name a real place for the old pattern to fire.
- **`_EXCLUSION_OUTSIDE_RE`** — same root issue, slightly different shape:
  one alternative's tail had no place-validation at all, so "not currently
  open to candidates outside of standard business hours," "cannot accept
  applications from outside of our normal review process," "cannot
  consider applicants outside of business hours" were all rejected as
  geographic exclusions despite not naming a place.

All four fixed the same way: a scoped case-sensitive sub-pattern,
`(?-i:[A-Z])`, at every "this must start a capitalized word" position —
this turns off case-insensitivity for just that one character while the
surrounding keywords stay governed by the outer `re.I` exactly as before.
`_RELOCATION_REQUIRED_RE` additionally needed an `"our "` prefix allowance
("relocation to our Austin office" doesn't start with a capital letter
even though "Austin" — the actual place — is right there), added alongside
the existing `"the "` allowance.

Re-verified after the fix that the ORIGINAL bug this exact case-folding
quirk was already half-noticed for — "This role is based in APAC and
LATAM" correctly registering as multi-region breadth rather than a single
restriction, because the old `[A-Z]` match didn't stop at the lowercase
connector "and" — still works: `APAC`/`LATAM`/`EMEA`/`Africa` are written
in genuine uppercase in real postings, so the new case-sensitive version
still matches them correctly; only a genuinely-lowercase tail (marketing
fluff) is now excluded.

### Negation-shaped words standing in for actual negation

Same root bug CLASS as the pre-existing `has_hard_country_specific_auth_signal`/`_sponsorship_sentence_has_negative_signal`
fixes (a topic-adjacent word treated as the thing that's actually
disqualifying) — found twice more in this sweep:

- **`_SPONSOR_NEGATION_RE`'s blanket `"without"`** — "We offer visa
  sponsorship **without** any restrictions," "...**without** hesitation,"
  "...**without** exception," "Sponsorship is available **without** any
  geographic limitation" are all POSITIVE sponsorship statements, but
  "without" modifying "restrictions"/"hesitation"/"exception"/"limitation"
  (not sponsorship itself) was read as negating the sponsorship that sits
  nearby in the same clause. Fixed by removing the blanket trigger and
  adding `_SPONSOR_WITHOUT_TOPIC_RE`, a narrow pattern requiring "without"
  to be directly followed by the sponsorship/visa/work-permit noun itself
  ("work without sponsorship," "without a visa") — which is how a genuine
  negative statement actually reads, and which none of the four false
  positives above ever do.
- **The idiom "can't wait"** — "We **can't wait** to sponsor the right
  candidate's visa!" contains "can't," a perfectly good negation trigger
  everywhere else, but "can't wait" means "excited to," not a negation of
  the sponsorship that follows. Fixed by stripping the idiom out of the
  clause before the negation check runs, rather than adding it as a
  standalone exception to the negation word list (which would have made
  "can't" stop working as a real negation trigger everywhere else too).
- **`_HYPHENATED_BASED_ONLY_RE`'s missing negation guard** — "This role is
  **NOT restricted to** US-based candidates only" is an explicitly
  INCLUSIVE statement (openly reassuring candidates the role isn't
  US-only), but the hyphenated pattern matched "US-based candidates only"
  inside it regardless, with no check for the negation sitting right
  before it. Fixed with a new `_HYPHENATED_BASED_NEGATION_RE` guard
  ("not restricted to," "isn't restricted to," "no longer restricted to,"
  "not just for") checked before either hyphenated pattern runs.

### What this sweep deliberately did NOT change

Checked but found to be adequately safe already, or appropriately scoped,
with no fix needed: `_WORKPLACE_LABEL_RE`'s captured-value validation (a
second, narrow regex already validates the captured label value
separately); `_TIMEZONE_LOCATION_RE`'s generic timezone-code catch-all
(already bounded by requiring "time zone" to close the match); `GLOBAL_
KEYWORDS`'s bare `"earth"` entry (already excluded from the loose free-text
safety net via `_SAFETY_NET_EXCLUDED_GLOBAL_KEYWORDS`, and only usable
unscoped against the narrow, structured location FIELD value, not free
prose); common name/place-word collisions ("Chad"/"Jordan" as person
names, "turkey" the food, "china" the homeware) — none of these false-
positive, since none of the hard-reject checks fire on a bare place word
with no restrictive verb/construction around it.

## Priority values at a glance

| Value | Meaning | Set by | Roles |
|---|---|---|---|
| `1` | Global | regex only | all |
| `2` | EMEA / Africa | regex only | all |
| `3a` | Blank location, LLM found real evidence | LLM (demoted, never promoted) | all |
| `3b` | Bare "Remote", real signal, AI-confirmed or genuinely uncertain | LLM, incl. after a last-chance retry | all |
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

## 2026-10: a bare-remote job that no provider ever reviewed is now retried once, then discarded — not kept as "ai_unreviewed"

Explicit user complaint, verbatim: *"why do some of the clearance notes
say: ai_unreviewed like tf?? if an LLM did not see it, have it wait and
then try one more time after which you should discard it should it fail.
Although this issue likely just happened cause I chose one provider."*

**Before this:** `ai_classify_locations()`'s cross-provider cascade
(classifier.py) only ever reassigns a job to a provider it HASN'T tried
yet, and skips any provider `_mark_exhausted` has already marked dead for
the rest of the run. Once every *configured* provider has failed once for
a job — trivially easy with only one provider active (exactly the
scenario the user called out: `USE_OPENAI=true`/`USE_NVIDIA=true` with
Groq excluded, or any run with just one provider enabled) — that job lands
on `('uncertain', None)` with zero further chance in that run, no matter
how transient the real failure was. crawl_i.py's `filter_locations()` and
crawl_ii.py's `_filter_locations()` both then kept a bare-"Remote" job in
exactly this state at `PRIORITY_UNSURE_SILENT`, tagged
`clearance="ai_unreviewed"` — a job that had never actually been read by
any model at all, indistinguishable in the output from a genuine AI
verdict.

**Fix, in two parts:**

1. **A real last-chance retry** (`ai_classify_locations()`, right before
   the final AI-authority gate): if any job is still unreviewed
   (`no_ai_read` non-empty) after the normal cascade, wait
   `_LAST_CHANCE_RETRY_WAIT_SECONDS` (45s), then force exactly ONE more
   real network attempt per still-unreviewed job against every configured
   provider — bypassing `_mark_exhausted`'s circuit breaker for this
   single attempt only. `_ai_call()` and `_classify_location_batch()` both
   grew a `force: bool = False` parameter for this; every other call site
   leaves it `False`, so normal circuit-breaker behavior is completely
   unaffected everywhere else. This is a deliberate, bounded exception —
   the breaker's whole point is to stop *wasting* traffic on a provider
   expected to keep failing for the rest of a multi-hour run, which
   doesn't apply to one last, explicitly-accepted-cost attempt before a
   job is discarded for good.

2. **Caller-side policy supersession** (crawl_i.py's `filter_locations()`,
   crawl_ii.py's `_filter_locations()`): the bare-remote branch now
   requires `provider_name is not None` again — the opposite of the
   2026-09 fix that removed this requirement (full history kept in both
   files' comments). That 2026-09 fix existed because a job could reach
   that point having genuinely never gotten a real shot at review; that's
   no longer true now that the last-chance retry exists, so a job that
   *still* comes back `provider_name=None` after it has had a genuine fair
   shot and failed — it now falls through to the same drop path as a
   `no_match`/`blank` job (Rank 4 still gets one last look first, same as
   everything else). The `"ai_unreviewed"` clearance string no longer
   exists anywhere in the codebase.

3. **Exclusion-cache side effect, also fixed:** the final drop path caches
   a job's URL into `new_exclusions` (skips the LLM call on a future run,
   TTL 21 days — see `excluded_cache.py`) whenever Rank 4 doesn't rescue
   it. That cache is meant for a job the AI *genuinely reviewed and
   rejected* — caching "nobody ever actually looked at this" identically
   would mean a future run (possibly with healthy providers again)
   silently skips the LLM call for up to 21 days, compounding the exact
   provider-outage problem that caused the drop in the first place. Both
   files now skip the `new_exclusions.add(url)` call whenever
   `provider_name is None`.

Verified (`test_location_last_chance_retry.py`, scratch-only, not
committed): a single-provider setup where every normal-round call fails —
confirms the wait actually happens, a forced retry that succeeds recovers
a real `(label, provider_name)` verdict, a forced retry that also fails
leaves the job genuinely `('uncertain', None)`, and crawl_i.py's
`filter_locations()` discards such a job entirely (and does NOT cache its
URL into `new_exclusions`) rather than keeping it.

## 2026-10: Crawl II earns its own path into Rank 4 — confirmed application-form field count, not ATS platform membership

Explicit user request, verbatim: *"for crawl 2, cant we make it have an
entry into rank 4 too. Like have it detect questions that would show it
successfully captured the application questions part like email address,
phone number, name, resume filed, cover letter field and boilerplate
sections that all application questions have. Like if it means maybe
three or even 5 of these? its confiremed that those are questions and its
scanned for auth and sponsorship questions just like crawl 1 and then let
in."*

**Before this:** Rank 4's eligibility gate (`_try_rank4()`, duplicated in
both crawl_i.py and crawl_ii.py) required `job["source_ats"] in
RANK4_ELIGIBLE_ATS` — a fixed list of named, structured ATS platforms
(Greenhouse, Lever, etc.) where the pipeline has a dedicated fetcher it
trusts to reliably capture the real application form. Every Crawl II row
is tagged `source_ats="in_house"` (crawl_ii.py's `DEFAULT_ATS_LABEL`,
never a `RANK4_ELIGIBLE_ATS` member), so this gate was correct but
**permanently inert** for Crawl II — there was no path for an in-house/
unsupported-ATS job to ever earn Rank 4, no matter what its real captured
form looked like.

The second half of the old gate — requiring a literal `"Application
Question:"` line in `description_snippet` — was also only an *indirect*
proxy for "we genuinely captured the real form": that marker only appears
as a side effect of finding at least one *substantive, non-boilerplate*
screening question (see `_format_screening_questions`'s
`_BOILERPLATE_QUESTION_RE` filter in ats_scrapers.py). A real, fully
captured application form that happens to ask zero custom screening
questions — just the standard name/email/phone/resume/cover-letter
fields, which plenty of real company career forms do — would never set
that marker at all, indistinguishable from a page that was never fetched.

**Fix:** a direct, positive confirmation signal instead of an indirect
proxy.

- `ats_scrapers.py`'s `_fetch_wild_questions()` — the one fetcher every
  in-house/unsupported-ATS job always goes through (it's never in
  `QUESTION_FETCHERS`) — now also counts how many of the RAW fetched form
  fields match `_BOILERPLATE_QUESTION_RE` (name, email, phone, resume/CV,
  cover letter, LinkedIn, website, EEO fields, etc. — exactly the fields
  the user named, reusing the existing pattern rather than writing a new
  one) via the new `_count_confirmed_application_form_fields()` helper,
  and stashes the count on `job["_confirmed_application_form_fields"]`.
  This is a side-channel annotation only — the formatted string that
  feeds `description_snippet` is completely unchanged, so every other
  caller/behavior is unaffected.
- crawl_ii.py's `_try_rank4()`: a job with
  `_confirmed_application_form_fields >= RANK4_CONFIRMED_FORM_FIELD_THRESHOLD`
  (set to 3 — the midpoint of the "maybe three or even 5" range the user
  floated; low enough that a normal company form, which routinely has 5
  boilerplate fields on its own, clears it easily, high enough that a
  stray 1-2-field contact form can't) now earns the exact same trust
  `RANK4_ELIGIBLE_ATS` membership already grants — including skipping the
  `"Application Question:"` marker requirement, since a confirmed-real
  form with zero screening questions is still trustworthy input.
  `classify_rank4()` itself is completely unchanged: it scans whatever
  `description_snippet` actually holds for the identical
  auth/sponsorship/restriction signals either way. This only changes how
  Crawl II earns the right to be scanned at all — crawl_i.py is untouched.

Verified (`test_crawl_ii_rank4_form_confirmation.py`, scratch-only, not
committed): the field-counter correctly counts a realistic form (6
boilerplate fields) vs. a tiny 2-field contact form (1); `_fetch_wild_questions`
correctly annotates the job without leaking boilerplate fields into the
returned question text; and — driven through crawl_ii.py's real
`_filter_locations()` — a Germany-located job with a confirmed real form
and zero screening questions is now admitted to Rank 4 (the old gate would
have rejected it outright), the same job with only 1 confirmed field
(below threshold) is correctly NOT admitted, a hard-disqualified US-state
location is still rejected even with a confirmed form (proving this only
changes the capture-confirmation gate, not `classify_rank4()`'s own
restriction scanning), and a confirmed-form job whose captured text
carries a real restriction signal is still correctly rejected.

## 2026-10: Insurity / Ping Identity leaks — application-question and hiring-scope phrasings the filters never matched

Explicit user report, verbatim: *"These fucking made it in:
job-boards.greenhouse.io/insurityllc/jobs/4297878009 ... both asking if you
are legally allowed to work in the US ... Are you legally eligible for
employment in the United States? / Do you now or will you in the future
require Insurity to petition for, sponsor, or transfer a nonimmigrant or
immigrant employment visa in order for you to work in the United States?"* —
and a second report: Ping Identity (`.../pingidentity/jobs/8807392002`):
*"Will you now or in the future require sponsorship to work in the country
where this job is located? / Upon hire, can you provide verification of your
identity and legal right to work in the country where this job is located?"*
Both were real production rows (`clearance=rank4`, `location_priority=4a`,
locations "Remote - US" and "UK - Remote"). The user's instruction: generate
every wording, in the hundreds, for the application-question filters and then
for every other filter, including false positives.

**Root causes (all confirmed by reproducing against the committed code):**

1. `_COUNTRY_AUTH_RE` hardcoded the bridge "authorized/eligible **to work**
   in <country>" — "eligible **for employment** in", "to be employed/hired
   in", "to accept employment in" never matched. Rewritten with a bridge group
   covering those, plus a noun-phrase "eligibility/authorization for
   employment in <country>" alternative.
2. Nothing matched the *sponsorship-requirement* question shape ("require
   <X> to petition for, sponsor, or transfer a ... visa in order for you to
   work in <country>", "require/need visa sponsorship to work in <country>").
   `has_hard_no_sponsorship_signal` needs sponsor **+ negation** in one
   clause, and this phrasing has none. Two new `_COUNTRY_AUTH_RE`
   alternatives, both requiring a named country in the same sentence.
3. `_REFERENTIAL_AUTH_QUESTION_RE` (the "...in the country where this job is
   located" family; Rank 4 treats any hit as disqualifying since its location
   is already one known country) only handled "right to work *where* this
   role is located" and "authoriz\* to work in the country where...". Now
   split into a HEAD fragment (right to work / authorized|eligible|permitted|
   allowed|able to work or for employment / work permit|visa|rights /
   require sponsorship|a visa / citizenship|residency status / verification
   of identity) and a PLACE-TAIL fragment (in the country|location|
   jurisdiction where|in which this role|job|position is located|based|
   listed, "where you are applying", "of this role"), matched head-then-tail
   within one sentence. `_referential_auth_hit()` additionally skips a
   sentence that is pure company-benefit framing ("we provide work permit
   support to help you work in the country where this role is located").
4. Gaps found by the generated fuzz suite in *other* filters (each a real
   leak path, each requires a named place so none fires on a bare word):
   - sponsorship: "immigration support" was not a sponsorship topic, and
     "work without a visa" required a topic word it deliberately doesn't have
     (`_SPONSOR_WITHOUT_TOPIC_RE` now fires on its own);
   - workplace_type field: "On-Premise/On-Premises" (field only — NOT titles,
     where "On-Premise" usually names the product: "Account Executive -
     On-Premise Software" must keep passing);
   - hyphenated "US-based candidates only" / "open only to UK-based
     applicants" (`_EXTRA_RESTRICTIVE_BASED_ONLY_RE`, with a negation guard so
     "not restricted to US-based candidates only" still passes — caught by an
     existing audit test);
   - "This role is restricted to <country>", "We are not hiring outside <X>",
     "Candidates outside <X> will not be considered", "We can only hire
     candidates who are in <X>", "Only candidates in the following countries
     will be considered", "We can only employ in countries where we have an
     EOR", "Must be in a European/Pacific timezone", passive "Relocation to
     <City> is required", "valid <State> insurance producer license"
     (`_EXTRA_RESTRICTIVE_SCOPE_RE`, same negation guard);
   - office attendance: reversed word order ("You will be in the office 3
     days per week", "4 days per week in-office", "onsite 4 days a week",
     "Hybrid schedule: 3 days in office, 2 remote") — every new alternative
     still needs a per-week frequency or an explicit remote-days contrast;
   - state lists spelled as FULL names ("We can only hire in these states:
     California, Texas, New York"), gated on hiring/residency framing and
     not company/office context; and the abbreviation-list check no longer
     fires on a pure footprint sentence ("We have offices in CA, TX, NY,
     FL, WA.").
5. False-rejection bug found on the way: structured locations such as
   "Remote - Global", "Global (Remote)", "Remote, Worldwide" were hard-
   rejected by `has_role_specific_place_restriction_signal` because the
   accepted-set only knew the bare words alone. Work-mode filler is now
   stripped before comparing.

**Verification (scratch suites, not committed):** each was run against the
committed code first to prove it detects the bug, then against the fix.
- `test_referential_auth_massive.py`: the four real leaked questions through
  `classify_rank4()`, plus 20 templates x 15 tails x 5 Rank-4 locations (1,500
  rejections) and the universal named-place gate on 4 named / 4 agnostic
  locations per phrasing, plus benefit-framing and ordinary-question
  false-positive banks: 4,073 cases, committed code 2,373 failing, fix 0.
- `test_auth_question_coverage_massive.py`: 341 cases (146 country-auth
  phrasings, 31 no-sponsorship, 52 false positives, end-to-end), 114 failing
  before, 0 after.
- `test_filters_fuzz_every_filter.py`: >=100 true positives per filter family
  (no-sponsorship, workplace_type, title qualifiers, workplace labels,
  country residency, hyphenated "-based only", entity/exclusion, timezone/
  relocation, state lists, office attendance, state licences, language
  fluency, metadata/location-symbol, country auth) run through the real Rank
  4 gate: 2,333 cases, 367 failing before, 4 after. The 4 remaining are
  *false rejections* (not leaks), deliberately left: "Our customers are based
  in the United States and Europe." and "Our official conference sponsor does
  not affect hiring." — widening the company-context guards to cover them
  would let "You must be based in the US to serve our US customers" past one
  of the filters, and leaks are the priority.
- `test_filters_comprehensive.py`: 362 end-to-end pipeline cases, 0 failing.
- All 36 earlier scratch suites unchanged vs. the committed baseline (the
  only differences are the new suites above).

## 2026-10: pipeline audit — classifier fixes were never applied to jobs already stored (re-validation), plus three smaller findings

User request: *"check the pipeline once more from ats scrapper to classifier
and config and everything else in between and even the logs ... make sure
that there is nothing we are missing."*

**1. Already-stored jobs bypassed every classifier fix (the big one).**
`crawl_i.py`/`crawl_ii.py` skip any job whose URL is already in `jobs`
("skipping LLM classification, just refreshing last_seen"). That saves LLM
calls, but it means a fix is never applied retroactively: a job that leaked
under an old bug stays for as long as the posting is live. Confirmed on
production data (2026-10-05): 1,596 of 5,729 active rows were Rank 4 (815 at
4b), including rows the current policy forbids — location "India"/"Kenya"
(4a), "El Salvador" (8), "Jamaica" (6), "Nicaragua" (5), "Hong Kong",
"Tokyo, Japan", "Cape Town", plus the Insurity and Ping Identity postings;
28 rows still carried the retired `ai_unreviewed` clearance.

Fix — `revalidate.py` (pure decision logic, no network/DB/LLM), wired into
both crawls' dedupe step:
- New column `jobs.classifier_version smallint default 0` (migration
  `add_jobs_classifier_version`) and `supabase_handler.CLASSIFIER_VERSION`
  (currently 1; **bump it whenever the deterministic rules or Rank 4 policy
  change in a way that should reach stored jobs**). Every freshly classified
  row is stamped; every pre-existing row is 0, so each is re-checked once.
- A stored job whose version is below current is re-enriched (description +
  application questions, the normal enrichment functions) and checked:
  Rank 4 rows with `classify_rank4()` (full current policy incl. the
  curated-country allowlist), `ai_unreviewed` rows always vetoed (the user's
  "discard it" rule), all other rows vetoed only if a hard override fires on
  an open-shaped location (blank / bare Remote / Global / EMEA / Africa — a
  legacy row with a concrete location shape is left alone).
- **Veto-only, evidence-only.** It never promotes, never calls an LLM, and
  an absent description or a failed question fetch is `undecided` (retried
  next run), never a reason to delete. A row the user is tracking
  (`application_status != 'not_applied'`) is `protected`, never vetoed.
- Vetoed rows are marked `clearance='vetoed', is_active=false` by the shard;
  `postfix_notion.py` Step 1b (`supabase_handler.purge_vetoed_jobs`) archives
  the Notion page (best-effort — a missing page is tolerated, as elsewhere in
  this project) and deletes the row. `get_jobs_pending_notion_sync` skips
  vetoed rows so a never-pushed one is never offered to Notion.
- Safety valves: `REVALIDATE_KNOWN_JOBS=false` (off), `REVALIDATE_DRY_RUN=true`
  (log every would-be veto, write nothing), `REVALIDATE_MAX_PER_SHARD`
  (default 3000), and nothing is re-validated when the metadata fetch failed
  or returned no row for a URL (an unidentifiable row is never stamped).
- Verified by `test_revalidate.py` (34 cases: the four real leaked
  postings vetoed, clean rows kept, no-evidence rows undecided, tracked rows
  protected, cap/disable/partial-metadata handling, dry-run, purge with
  mocked HTTP). **Not yet verified on a full real-data dry run** — an attempt
  to enrich 250 stored Rank 4 rows through the production enrichment hung in
  the sandbox and was abandoned, so the first scheduled run's
  "Re-validation ... vetoed xN" log lines are the real check (use
  `REVALIDATE_DRY_RUN=true` for a manual first look).

**2. Rank 4's non-curated-country guard was a blocklist with gaps.**
`_RANK4_ANY_NAMED_COUNTRY_RE` only knew the ~80 countries in the shared
`_COUNTRY_AUTH_NAMES_RE_FRAGMENT`, so El Salvador/Jamaica/Nicaragua (live 4b
rows) were never recognised. The policy is an allowlist, so the guard now also
uses `_RANK4_WORLD_COUNTRIES_FRAGMENT` (~200 sovereign states and common
territories). A second bug was found while testing it: the guard scanned
`remainder`, which has filler words like "new"/"of" stripped, so "New
Zealand" -> "Zealand", "Isle of Man" -> "Isle Man" slipped through; it now
scans the location with only the curated places removed. Test:
`test_rank4_world_countries.py` — 207 names x 3 location shapes x 2 title
variants all rejected, plus 42 legitimate curated locations (the top 4a/4b
shapes from the live table) still admitted: 1,286/1,286.

**3. `AI_RATE_SHARDS` was a hardcoded "12" on all three crawl jobs.** It
divides NVIDIA's per-process call interval (config.py:
`_NVIDIA_BASE_INTERVAL * AI_RATE_SHARDS`) so N concurrent processes together
stay under the key's ~40 RPM — correct only if it equals the processes
actually running. The default schedule runs 10+10+10 = 30 at once (~2.5x over
the quota); 20 shards of one crawl, 1.7x over. `prepare-matrix` now computes
it from the shard counts of the crawls that really run and passes it to all
three jobs (verified for four dispatch scenarios: 30 / 40 / 10 / 7).

**4. Platform/registry consistency check (scripted, no gaps found).**
SCRAPERS == SUPPORTED_ATS (41 each); every scraper's `source_ats` label has
a matching fetcher-table convention; every `RANK4_ELIGIBLE_ATS` label is
emitted by a scraper. Left as-is on purpose: 12 platforms use the default
worker cap of 8; `csod`/`paycom`/`recruiterbox` have neither a verifier nor an
`_UNVERIFIABLE_ATS` entry (verification.py skips any platform without a
verifier, so the default is already safe).

**Observations from `scan_reports` / logs (not changed):** a handful of
`crawl_ii`/`crawl_iii` rows from 10-01/10-02 are stuck at `status='running'`
with no `finished_at` (their finalize step only runs when that pipeline's
cleanup flag is passed, and a cancelled run never reaches it — cosmetic);
`csm_roles` is ~225k per Crawl I day because only *stored* jobs are skipped,
so every role-matched job rejected on an earlier run is re-enriched (this is
the explicit "application questions for EVERY job" instruction, and the real
runtime driver — not changed here).


## 2026-10: the country field no longer narrows a broad location; US regions and time zones in the description

Found on a real posting (Pencil, Ashby, "Account Manager, Customer Success"): location `EMEA`, but the scraper also carries
Ashby's `addressCountry` ("European Union", the employer's postal address) and the classifier joined the two into
"EMEA European Union", which counts as EMEA plus a place and was dropped. Its description also contained a stray
"Candidates must be located on the East Coast and within the Eastern Time Zone.", which nothing caught.

- **Country field.** If the location FIELD already states a broad scope (`classifier._location_field_states_broad_scope`:
  Africa, bare EMEA, several regions, or a Global/Worldwide keyword with nothing else), the separate `country` value is
  ignored. A place written inside the location field still narrows it ("EMEA, Germany", "Global (US only)").
  Real restrictions still reject: the description and application-question detectors run independently of the country
  field. The shared logic is `_broad_scope_check`, used by the main location stage and by `_combined_location`, and
  `has_role_specific_place_restriction_signal` now uses it instead of its own shorter list (it used to reject
  "EMEA-wide", "Work from anywhere" and "EMEA - All Countries" before the main stage saw them).
- **Description.** `_Q_REGIONS` gained informal US regions (East/West/Gulf Coast, Midwest, Northeast/Southeast/...,
  Pacific Northwest, New England, Mid-Atlantic, ...); the binding family accepts "located **on** the East Coast"; and
  `_JD_TZ_RESIDENCE_RE` replaces the old plain time-zone family: a residence verb, up to 50 characters, then a time
  zone. It is skipped for a sentence with Global / EMEA / Africa evidence, or with "overlap" wording around an UNNAMED
  zone ("a time zone that overlaps with our European team"); a named zone still rejects even with "overlap" in the
  sentence ("must reside in the Eastern or Central Time Zone to ensure adequate overlap").
- **Measured** on 5,078 real jobs (4,625 freshly scraped from 296 boards plus 453 stored): 14 location verdicts changed, all
  Ashby EMEA postings with a UK / European Union address country, dropped -> Rank 2; 3 description verdicts changed, all
  the Pencil East Coast sentence. Nothing else moved. Corpus lines added to `corpus/jd_sentences.txt` and
  `corpus/benign_look_alikes.txt`; `check_location_field.py` covers the country-field policy.
- **Known, separate:** the country-based detector rejects "located in Europe or Africa ..." and the commuting family
  reads "within 3 hours of <time zone>" as a drive; both predate this change and are not touched here.


## 2026-10: "Worldwide + a place" is asked, not rejected; "Worldwide, except X" passes

A Global/Worldwide keyword together with a place used to be one outcome: reject. It is now three
(`classifier._global_scope_with_extras`, reached from `_broad_scope_check`):

| Location field | Result | Why |
|---|---|---|
| `Worldwide - US`, `Worldwide (US)`, `Global, Berlin`, `Global - United States`, `Worldwide; US`, `Global, Remote (US preferred)` | **unsure, reason `global_plus_place`** | Contradictory or unclear. The LLM reads it. |
| `Worldwide, except US`, `Worldwide (excluding US)`, `Global (excl. US & Canada)`, `Worldwide ex-US`, `Anywhere except the UK`, `Worldwide, outside the US` | **match, Rank 1** | Everything except places that are not Africa, so Africa is still eligible. |
| `Global (US only)`, `Anywhere in the US`, `Worldwide, US-based only`, `Global within Germany`, `Global, hybrid London` | reject | A restriction word (`only`, `based`, `must`, `office`, `hybrid`, ...) or the keyword governing a place (`in`, `within`, `across`, ...). |
| `Worldwide, except Africa`, `Global ex-Africa`, `Worldwide (excluding Nigeria)`, `Worldwide except EMEA` | reject | The exclusion names Africa, EMEA or an African country (it could be the candidate's own). |

- **Tier.** `global_plus_place` is handled in `crawl_i.filter_locations` and `crawl_ii._filter_locations` like bare Remote
  (Rank 3b, application-question line required) but stricter: only a real AI `match_global`/`match_africa` verdict keeps
  it. `uncertain` does not, because the location text itself is contradictory. Rank 4 still gets its last look.
- **Bare "Global".** `Global`/`Anywhere`/`Worldwide` that is not in `GLOBAL_KEYWORDS` (too ambiguous in free text) counts only
  as its own segment of the location ("Worldwide - US") or heading an exclusion ("Anywhere except the UK"); a name that
  contains it ("Global Business Services, Manila") is still an ordinary place.
- **Africa inside an exclusion.** `Worldwide, except Africa` used to match Rank 2 because the Africa test fired on the word
  anywhere in the field. It now ignores Africa (and African countries) named inside an exclusion clause.
- **Unrecognised exclusion tail** (`Worldwide except sanctioned countries`) -> asked, not passed.
- **Measured:** 5,078 real jobs contained no "Worldwide + place" location (25 had a Global word, all plain), so no real
  verdict moved. The rule is covered by `check_location_field.py` (107 checks, including the tier behaviour of both
  crawlers with the LLM stubbed).
