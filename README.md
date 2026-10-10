# Web Crawler — Global CS/AM Job Scanner

Finds **Customer Success, Account Management, Project Management, and
Operations** roles that are open to candidates **outside a single country**
(global, EMEA/Africa, or a named eligible country/region), across ~40 ATS
platforms, and syncs the matches to Notion for tracking.

This file explains the moving parts and how they fit together. For the
full reasoning behind each classification rule (real postings that
motivated it), see **`CLASSIFICATION.md`**. For a pure table-format
reference (no prose), see **`RANKING_REFERENCE.md`**.

## The three pipelines

Everything writes to one Supabase `jobs` table, tagged by which pipeline
found it. They run independently, in parallel, as separate GitHub Actions
jobs (`.github/workflows/crawl.yml`), each sharded 10-way by default.

| | Source | What's different |
|---|---|---|
| **Crawl I** (`crawl_i.py`) | Live-scrapes ~40 known ATS platforms (Greenhouse, Lever, Workday, Workable, …) from a list of company slugs (`archive_i` table) | The main, daily-reliable pipeline |
| **Crawl II** (`crawl_ii.py`) | Heuristically scrapes career pages that *aren't* on a known ATS (`archive_ii` table). Extraction ladder (all pure helpers in `page_extract.py`): JSON-LD → microdata → embedded JSON state (`__NEXT_DATA__`, `window.__X__`, hidden-input JSON) → confirmed job links; dead ends try a known-ATS bridge, iframes/embeds, RSS + WordPress REST, the page being one posting, then the sitemap. Descriptions are boilerplate-free main text; a page only becomes a job if it reads as one JD (`page_extract.is_job_description`) | Messier data, leans on the LLM stage more. Role pre-filter skips detail fetches for titles the role filter would drop anyway; `CRAWL_II_SITEMAP_FALLBACK=0` turns the sitemap step off |
| **Crawl III** (`crawl_iii.py`) | Consumes stapply.ai's pre-scraped bulk CSVs for platforms we don't scrape ourselves (Phenom, UKG, SAP, Dayforce, Eightfold, MokaHR, …) | No per-job HTTP fetch needed — full JD is already in the CSV. Never has application questions, so it can't reach Rank 4 |

The company lists Crawl I/II scrape from (`archive_i`/`archive_ii`) are
themselves built by `discovery.py` + `node.py` — a separate crawler that
finds and classifies company career pages from several public slug
inventories, independent of the ranking logic below.

**ATS coverage research (2026-10)** — platforms checked for a public,
robots-allowed job API before being added to Crawl I:

| Platform | Result |
|---|---|
| Dayforce (Ceridian) | **Added** (`scrape_dayforce`): CSRF handshake + `jobposting/search` POST on `jobs.dayforcehcm.com`, full JD inline. Slug is `tenant` or `tenant\|board` (board names are per-tenant). 1,737 registry slugs via open-jobs |
| HireHive | **Added** (`scrape_hirehive`): `{slug}.hirehive.com/api/v1/jobs`, robots allows all |
| Manatal (`careers-page.com`) | **Added** (`scrape_manatal`): paginated server-rendered board HTML (`/{slug}?page=N`, 10 per page, two themes handled) gives title/location/short job code; JD from the detail page's `redactor-styles` block. Not the JSON API: it has no job-URL code and 404s for many live boards. 2,481 live slugs via openroles |
| JobScore | **Added** (`scrape_jobscore`): public `careers.jobscore.com/jobs/{slug}/feed.json` (full JD, location, remote flag); robots only blocks `/apply_flow/`. 171 openroles + 49 open-jobs slugs |
| Crelate | **Added** (`scrape_crelate`): the portal is a JS shell, but every portal publishes `jobs.crelate.com/portal/{slug}/rss` (permalink, full JD, location); robots only blocks static dirs. 367 open-jobs slugs |
| Comeet | **Added** (`scrape_comeet`): slug `{name}\|{uid}` from `comeet.com/jobs/{name}/{uid}`; the page embeds the company token, then the public `comeet.co/careers-api/2.0/company/{uid}/positions?details=true` gives full JDs, location and workplace type; robots allows `/jobs/`. Found via unmatched hosts in `archive_ii` (76 pages). |
| Emply | **Added** (`scrape_emply`): `{tenant}.career.emply.com`; `/vacancies` embeds a sectionId and the page's own anonymous `POST /api/integration/vacancy/get-page` returns every vacancy with its full description. robots allows all. 27 `archive_ii` tenants. |
| CATS (`catsone.com`) | **Added** (`scrape_cats`): slug `{tenant}\|{id}`; `/careers/{id}/jobs` lists every job on one page (title, location); descriptions via `_fetch_cats_description` (`div.job-description`). Mostly staffing / local US roles. |
| Elmo Talent (`elmotalent.com.au`) | **Added** (`scrape_elmo`): slug `{tenant}\|{board}`; `/careers/{board}/jobs?page=N` (10 per page); robots explicitly allows `/careers/*/job*`; descriptions via `_fetch_elmo_description`. Australian employers. |
| Easy Apply (`easyapply.co`) | **Added** (`scrape_easyapply`): `{tenant}.easyapply.co` lists all jobs in one page; job pages `easyapply.co/job/{slug}` through the generic description fetcher. Canadian / US local roles. |
| HiBob (`careers.hibob.com`) | **Added** (`scrape_hibob`): `{tenant}.careers.hibob.com/api/job-ad` returns every open job with full description, but only answers (else 401) when the board's own origin is sent as `Referer`; found by reading colophon-group/jobseek's monitor, verified live |
| Deel (`jobs.deel.com`) | **Added** (`scrape_deel`): anonymous `api-prod.letsdeel.com/guest/ats` — `organizations/{slug}/career_page_settings` gives the org + board id, then `job_postings` lists everything with rich-text descriptions; verified live (klarna 101 jobs) |
| Getro (`{tenant}.getro.com`, custom VC-board domains) | **Added** (`scrape_getro`): ~hundreds of VC-fund talent networks, each listing every job across the fund's portfolio. Public `POST api.getro.com/api/v2/collections/{network id}/search/jobs` (needs `Accept: application/json`; 20 per page); the scraper asks each network for our role families with `filters.work_mode=remote` (a few pages instead of 20k jobs). slug = the `{tenant}` (1,892 already in `archive_i` from earlier discovery, never scraped before) or a numeric network id (`discovery.py --source getro` sweeps ids; manual). Adds jobs on in-house careers sites no ATS scraper reaches (Stripe, Revolut, Databricks...). Descriptions come from the apply URL via the generic fetcher |
| Freshteam, Factorial, PeopleForce, Loxo | **Added** (`_scrape_html_board`): no public JSON feed, but each tenant's list page is plain HTML with one link per job; the shared link/next-page finders in `page_extract` plus a small per-platform card reader read title/location/department, and the job page supplies description (and location where the list shows none). Live-verified on 6 tenants. An unknown Freshteam tenant answers 200 with an `invalid-domain-wrapper` page, so liveness reads the body |
| Jobsoid (`{tenant}.jobsoid.com`) | **Added** (`scrape_jobsoid`): keyless `/api/v1/jobs` JSON with full descriptions and structured location; live-verified (music-ministry, 42 jobs). A dead tenant redirects its board page to `portal.jobsoid.com/?notfound=true` |
| Remote.com (job board) | **Added as a virtual board** (`scrape_remote`): the board's own keyless API `talent-api.remote.com/api/v1/public/jobs` (6k jobs) with the employer-declared hiring location (global / country list / time-zone window) and a per-job description call. A Deel competitor (employer of record); the other EOR vendors (Oyster, Multiplier, Papaya, Velocity Global, Omnipresent) have no public job board or career-page product |
| Keka Hire (`{tenant}.keka.com/careers`) | **Added** (`scrape_keka`): the board page's raw HTML holds the org id, then `/careers/api/embedjobs/default/active/{id}` returns every job with its description; pattern from rishilahoti/ashby-job-scraper, live-verified on inc42 / mosaicwellness. Mostly India-based roles |
| Recruiterflow (`recruiterflow.com/{tenant}/jobs`) | **Added** (`scrape_recruiterflow`): the board page embeds the full list as `window.jobsList` JSON; the REST API itself needs a key. Live-verified on the vendor's own board |
| Homerun (`{tenant}.homerun.co`) | **Re-added** (`scrape_homerun`): list = Vue `<job-list v-bind>` props resolved by `page_extract.extract_state_jobs`; the 2026-09 removal was about a wrong `jobs.*` slug guess, not the platform. Tenants with no openings return `vacancies: []` (why earlier tries "showed no jobs") |
| TalentLyft, onlyfy | No dedicated scraper — client-rendered boards, read by Crawl II's generic ladder when they appear as in-house career pages |
| Bullhorn, Fountain, iSmartRecruit, Recruiterflow, HiringThing | Not added — keyed/bearer APIs or JS-only boards with no public feed found |
| Traffit (`{tenant}.traffit.com`) | **Added** (`scrape_traffit`): public `GET /public/job_posts/published` JSON, paged by request headers, full descriptions; live-verified (bat.traffit.com). An unknown tenant answers a 503 HTML page |
| Paycor (`recruitingbypaycor.com`) | Not added — robots.txt is `Disallow: /` for all agents |
| Eightfold | Not added — public `/api/apply/v2/jobs` returns 403 "Not authorized for PCSX" |
| UKG / UltiPro | Not added — robots.txt disallows `JobBoardView` (the search endpoint) |
| Dover | Not added — robots.txt disallows `/api/` |
| Employment Hero | Not added — `jobs.employmenthero.com` is a client-rendered job marketplace: no sitemap, no per-employer slug in URLs, and every data route is under the robots-disallowed `/api/` / `/_next/` |
| GoHire | Not added — `app.gohire.io/{slug}` is an SPA shell (200 for any slug, so no dead-slug signal) and no public list endpoint was found |
| Comeet | Not added — the careers API needs a per-company `uid` + token that the 73-slug registry doesn't carry; `comeet.com/jobs/{name}` 404s |
| ApplicantPro (`applicantpro.com`) | **Added** (`scrape_applicantpro`): the tenant id sits in the board page's raw HTML, then `/core/jobs/{id}` returns every job as JSON; live-verified on 4 tenants. (An earlier "no jobs" result came from tenants with nothing open.) |
| CareerPlug | Not added yet — HTML-only, mostly local/hourly US roles |

**Slug sources added 2026-10** (discovery's GitHub-registry list; all read through jsDelivr, no tokens): colophon-group/jobseek
`boards.csv` (7.9k boards), outscal/OpenJobs `companies_v2.json` (12k gaming companies, ~2.5k on a supported ATS),
crypto-jobs-fyi/crawler (~530 crypto/AI boards) and ElliotGbaum/upstreamit's `*-live.txt` lists (Greenhouse, Lever, Ashby slugs its
own probe confirmed live). A generic parser (`_parse_url_records`) maps any board / careers URL in a JSON or CSV registry through the
project's own `URL_TO_SLUG` converters, so a new registry needs no ATS column or per-repo field mapping.

## Pipeline stages

For every new job (URL not already in Supabase):

1. **Role filter** — keyword regex first, LLM fallback for ambiguous
   titles. Classifies into `CS` / `AM` / `PM` / `OM`, or drops the job.
2. **Enrichment** — fetches the full job description and application
   questions if the scrape didn't already include them. Application
   questions get appended into the description text as literal
   `"Application Question: <label>"` lines — from this point on they're
   just part of the same text everything else reads.
3. **Location filter** — the ranking system below. This is where most of
   the complexity lives.
4. **Write** — matches go to Supabase with a `location_priority` value
   (`1`/`2`/`3a`/`3b`/`4a`/`4b`), then get pushed to Notion.

## The ranking system

### Step 1: 19 restriction detectors, checked first, for every job

Before anything else, ~19 regex functions each look for one specific kind
of *restrictive phrasing* — not a specific place name, a specific
**sentence shape**: "authorized to work in `<country>`," "must be
hybrid/on-site," "no legal entity in your country," "based anywhere in
`<country>`," enumerated US state lists, work-permit/sponsorship questions
tied to a country, and so on. Each one reads whichever fields are
relevant to what it's looking for (title, the structured `workplace_type`
field, or the combined description+application-questions text) — no
single check reads everything, but between all 17, title/location/JD/app
questions are all covered.

**Any single hit = instant reject**, before any rank, before the LLM, before
Rank 4's fallback is even attempted. This is universal — it isn't specific
to any one rank, and a restriction sitting inside an application question
rejects a job exactly the same as one sitting in the JD prose, because by
this point they're the same text.

Crucially: these checks look for *restrictive language*, not *specific
place names*. A location field that just says `"Australia"` — no
qualifying sentence anywhere — trips **zero** of the 17 checks. It isn't
flagged as a problem at all.

### Step 2: does the location field itself say something broad enough?

If nothing tripped step 1, the location field is checked against the
accepted "broad" family: explicit **Global**/worldwide/anywhere language
(~80 keyword variants), or **EMEA**/**Africa** (the literal word, or 2+
business regions/African countries together). A match here is an instant
**Rank 1** (Global) or **Rank 2** (EMEA/Africa) — pure regex, no LLM
involved, ever, for this decision.

`"Australia"` isn't on that list. So even though nothing was *wrong* with
it, it still doesn't pass step 2 — this is a "not broad enough" rejection,
a completely different reason from step 1's "something's wrong" rejection.

### Step 3: Rank 3 — send the ambiguous ones to the LLM

A location field that's **blank** or just says **"Remote"** (nothing else)
gets sent to the LLM for a judgment call:

| | 3a (blank field) | 3b (bare "Remote") |
|---|---|---|
| Why | No location data captured at all | Real signal, just not region-specific |
| Needs application questions present? | No | **Yes** — none present → dropped |
| LLM says match → | Kept at 3a | Kept at 3b |
| LLM says uncertain → | **Dropped** | Kept anyway (benefit of the doubt) |

The LLM can never promote a job all the way to Rank 1/2 — a keyword match
is the only way there. The LLM's role is narrow: confirm or veto an
already-ambiguous case.

### Step 4: Rank 4 — the CS/AM-only fallback for "not broad enough"

This is specifically for jobs like the `"Australia"` example: cleared step
1 (nothing restrictive), failed step 2 (not Global/EMEA/Africa), and would
otherwise just be dropped. Rank 4 gives **Customer Success and Account
Management roles only** one more look, under a strict gate:

1. Role is `CS` or `AM` (not PM/OM)
2. ATS is one of 19 verified platforms (`RANK4_ELIGIBLE_ATS`; Gem, HiBob, Deel, Paylocity, Dayforce and Cornerstone OnDemand added 2026-10) — chosen because they reliably
   return *both* a location field *and* real application questions on the
   same job, not just one or the other. Added only after a live probe through the real
   pipeline showed the question reader works from structured data; the probe results and the
   platforms left out (and why) are in the comment above the set in `classifier.py`
3. `ENABLE_RANK4_COUNTRY_SPECIFIC` is turned on for this run — on by
   default (both the unattended cron schedule and an untouched manual
   dispatch), untick the checkbox on a manual dispatch to turn it off
4. An `"Application Question:"` marker is actually present — Rank 4's
   premise is "we confirmed the questions are silent on this," not "we
   never checked"

A job that clears the gate is re-run through step 1's full 19-check chain
again internally (defense in depth — nothing restrictive gets a free
pass just for being CS/AM), plus the Rank 4 rule: the location field must itself be
an allowed place (a region's member countries and cities do not count), and for a
job tied to one country or city the application form must be *silent on
eligibility* — any work-authorization, visa/sponsorship, citizenship, residency,
relocation or commute question rejects it, named country or not. Only then:

- **4a** — the location field, on its own, is made *entirely* of one or
  more names from a 15-country allowlist (US, UK, Canada, Australia,
  Germany, …) plus broader regions (APAC, LATAM, Europe, …). One clean
  signal, nothing else. `location = "Australia"` → **4a**.
- **4b** — the location field *doesn't* cleanly resolve on its own (it
  names something narrower, usually a city), but a broad signal shows up
  somewhere else — the title, the JD, or mixed into the location field
  itself (`"EMEA (UK based)"`). E.g. title = `"CSM, EMEA"`, location =
  `"London"`. Two signals, neither restrictive, that don't fully agree —
  so it's let in, just flagged as less clean-cut than 4a.

Nothing in 4a or 4b was ever restrictive — if it had been, step 1's recheck
would have already dropped it before 4a/4b logic is ever reached.

## The excluded-jobs cache

A job that reaches the LLM (Rank 3) and gets rejected is never written to
Supabase — so the normal "already seen this URL" dedup never catches it,
and the same posting gets re-sent to the LLM every single day for as long
as it stays listed.

`excluded_cache.py` fixes this: each pipeline keeps its own
`{url: date_excluded}` file (`excluded_1.json` / `excluded_2.json` /
`excluded_3.json`, stored as a GitHub Release asset). Before sending a job
to the LLM, it's checked against this cache first — a hit within the last
**21 days** skips the LLM call (it still gets a free, regex-only Rank 4
check regardless). After 21 days an entry just quietly expires; nothing
re-queues it automatically — it only gets reclassified if a live crawl
happens to find that same URL again.

Mechanically: with ~30 shards running in parallel across the three
pipelines, they can't all write to one shared file without constant
conflicts, so each shard uploads its own small file, and a single
once-per-run step merges them all into the three canonical files.

## Notion sync (`notion_sync.py`)

Two scheduled passes, both best-effort (silently skipped if Notion isn't
configured):

1. **Before** the crawl shards run: reads every page's `Status` out of
   Notion, writes anything other than "Not Applied" back to Supabase.
2. **After** every shard finishes: pushes every un-synced Supabase row as
   a new Notion page — title, company, URL, date, salary, role category,
   a broad "Globally Hiring" label, the raw **Rank** (`1`/`2`/`3a`/`3b`/
   `4a`/`4b`, same values as `location_priority`), and the Supabase row id
   (the join key step 1 reads back).

Every property is checked against Notion's *live* schema before being
sent — a missing or wrong-typed property is skipped with a warning for
that one field, not a failed page create.

A one-off maintenance command backfills `Rank` onto pages created before
that property existed (not part of the daily schedule — `location_priority`
never changes for an existing row, so this only ever needs to run once):

```bash
python notion_sync.py --backfill-rank            # dry run — logs changes, writes nothing
python notion_sync.py --backfill-rank --apply    # actually writes
```

## Setup

### Supabase

The `jobs` table (plus `archive_i`/`archive_ii`/`crawl_checkpoints`/
`scan_reports`) already exists in the configured project — see
`supabase_handler.py` for the schema each function expects.

### Notion

Create a database and share it with your integration. Required
properties (checked by *name* and *type* at runtime — get these exact):

| Property | Type |
|---|---|
| (any title-type property) | Title |
| Job URL | URL |
| Supabase ID | Number |
| Status | Select (`Not Applied` / `Applied` / `Interviewing` / `Offer` / `Rejected`) |
| Company Name | Rich text |
| Date Added | Date |
| Salary | Rich text |
| Role Category | Select |
| Globally Hiring | Select |
| Rank | Select — options `1`, `2`, `3a`, `3b`, `4a`, `4b` |

### GitHub Secrets

- `SUPABASE_URL`, `SUPABASE_KEY`
- `NOTION_TOKEN`, `NOTION_DATABASE_ID`
- `CEREBRAS_API_KEY` (default LLM provider) — plus any of
  `ANTHROPIC_API_KEY`, `GROQ_API_KEY`, `GROQ_API_KEY_C`, `OPENAI_API_KEY`,
  `NVIDIA_API_KEY` as additional/fallback providers (see `config.py` for
  how `LLM_PROVIDER`/`USE_OPENAI`/`USE_NVIDIA` select between them)

### Run

Scheduled Mon–Fri at 6:00 UTC via `.github/workflows/crawl.yml`, or
trigger manually from the Actions tab (checkboxes control which of the
three pipelines run, shard counts, and the Rank 4 opt-in).

Locally:

```bash
cp .env.example .env   # fill in your keys
pip install -r requirements.txt
python crawl_i.py      # or crawl_ii.py / crawl_iii.py
```
