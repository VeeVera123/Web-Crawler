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
2. The job's ATS platform is in `RANK4_ELIGIBLE_ATS` — the 12 platforms
   live-verified (2026-09 probe, ~25-30 real companies sampled per
   platform) to reliably return **both** a location and application-question
   value on the same job: **Greenhouse, Workable, Personio, JazzHR,
   Teamtailor, Recruitee, Lever, Eploy, PageUp, isolvedhire, Pinpoint,
   Rippling**. Notably, Ashby did **not** qualify (application questions
   are auth-walled) despite looking like an obvious candidate.
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

A country-agnostic version of the same question (no country named) is
**not** disqualifying — that's the tier's other worked example: a
"Customer Success Manager, EMEA" role with location "London" that asks
"Will you now or in the future require sponsorship for a work visa?" with
no country attached is admitted, because nothing ties that question to a
specific place. (In practice this exact EMEA case is already caught
earlier, at Rank 2, by the base pipeline's own EMEA handling — true new
Rank 4 territory is non-EMEA bare regions/countries.)

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
