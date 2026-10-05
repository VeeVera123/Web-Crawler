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
| **Crawl II** (`crawl_ii.py`) | Heuristically scrapes career pages that *aren't* on a known ATS — JSON-LD first, then pattern-matching fallback (`archive_ii` table) | Messier data, leans on the LLM stage more |
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
| Eightfold | Not added — public `/api/apply/v2/jobs` returns 403 "Not authorized for PCSX" |
| UKG / UltiPro | Not added — robots.txt disallows `JobBoardView` (the search endpoint) |
| Dover | Not added — robots.txt disallows `/api/` |
| Employment Hero | Not added — `jobs.employmenthero.com` is a client-rendered job marketplace: no sitemap, no per-employer slug in URLs, and every data route is under the robots-disallowed `/api/` / `/_next/` |
| GoHire | Not added — `app.gohire.io/{slug}` is an SPA shell (200 for any slug, so no dead-slug signal) and no public list endpoint was found |
| Comeet | Not added — the careers API needs a per-company `uid` + token that the 73-slug registry doesn't carry; `comeet.com/jobs/{name}` 404s |
| ApplicantPro/Stack, CareerPlug | Not added — HTML-only / token-gated, and mostly local/hourly US roles that the location filter would drop |

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
2. ATS is one of 12 verified platforms — chosen because they reliably
   return *both* a location field *and* real application questions on the
   same job, not just one or the other
3. `ENABLE_RANK4_COUNTRY_SPECIFIC` is turned on for this run — on by
   default (both the unattended cron schedule and an untouched manual
   dispatch), untick the checkbox on a manual dispatch to turn it off
4. An `"Application Question:"` marker is actually present — Rank 4's
   premise is "we confirmed the questions are silent on this," not "we
   never checked"

A job that clears the gate is re-run through step 1's full 19-check chain
again internally (defense in depth — nothing restrictive gets a free
pass just for being CS/AM), plus one extra check specific to Rank 4
(a sponsorship/work-permit question tied to a named country). Only then:

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
