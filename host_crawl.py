# Bulk domain discovery for ATS-slug seeding — full history, what was tried, and the open question

## UPDATE 2026-08-22 — cancellation cause + Supabase 500MB storage plan

**"Error: The operation was canceled." (v5 seed step) diagnosis:** this is
GitHub's own runner-level message, not a Python exception — the log stops
right after the "Querying..." line with no error ever logged by
`host_crawl_seed.py` itself, meaning something upstream of the process is
what ended it. The prime suspect, confirmed by the user: `HF_TOKEN` was
NOT set, so every request ran fully anonymous against huggingface.co from
GitHub Actions' shared/well-known IP pool — exactly the traffic pattern
Hugging Face's own docs flag as most exposed to throttling, and DuckDB's
httpfs had **no timeout configured at all**, so a throttled/stalled
connection just blocks forever with nothing for Python to catch, until
some external layer (the runner's own network stack) kills it. Fix
applied in `host_crawl_seed.py`: `http_timeout`/`http_retries` set on the
DuckDB connection, plus a hard wall-clock cap via a worker thread +
`.result(timeout=...)` around both the crawl-partition-listing glob (60s)
and the main query (600s) — a stall now raises a clean, logged
`TimeoutError` instead of ever needing GitHub to be the one that ends it.
`HF_TOKEN` is now called out as strongly recommended (not just optional)
in both the script's logging and the workflow file's comments — this
can't be proven as *the* root cause without a live re-run (sandbox
network here can't reach huggingface.co either), but it's the best-fit
explanation given the evidence and it's a free, low-effort mitigation
either way.

**Supabase 500MB free-tier storage plan:** real, measured Postgres row
sizes for `host_crawl_queue`'s schema (`host` PK + 4 small cols) come out
to **~167 bytes/row**, and `host_crawl_visited`'s schema to **~175
bytes/row**, both including their single PK index (measured directly by
creating each schema and inserting 200K realistic rows, not estimated).
At Tranco's full 4,324,899-host scale, queue+visited would be **~1.4 GB
combined — nearly 3x the free tier** even before the ~51 MB already used
by `jobs`/`scan_reports`/`slug_registry`. Applied:
  1. **A DB trigger (`trg_host_crawl_dequeue`)** on `host_crawl_visited`
     that deletes a host's `host_crawl_queue` row the instant it's
     recorded as visited — the queue's only job is "not-yet-visited
     hosts," so a claimed+visited row served no further purpose (verified
     `host_crawl_claim_batch` already excludes anything in
     `host_crawl_visited` via its `NOT EXISTS` check). This roughly halves
     steady-state storage since queue no longer accumulates permanently.
  2. **Switched `added_at`/`claimed_at`/`checked_at` from `timestamptz`
     (8 bytes) to `date` (4 bytes)** on both tables — free, lossless
     (nothing here needs second-level precision), saves ~4-8 bytes/row.
  3. **Dropped `slug_registry.name`** — confirmed via grep that no code
     in this repo reads or writes it; was dead weight (773 kB currently,
     grows with every future slug).
  4. **Recommended cap: seed to ~900K-1M hosts total, not Tranco's full
     4.3M.** Even with fixes 1-2 applied, `host_crawl_visited` alone
     still grows without bound (by design — it's the permanent
     don't-re-crawl-this-dead-host ledger) and approaches the 500MB
     ceiling around 2.5-3M hosts ever visited. At ~900K hosts (achievable
     by pinning `--limit` and/or `TARGET_TLDS`+`hcrank`-ranked selection
     already built into `host_crawl_seed.py`), steady-state usage lands
     around ~150 MB for `host_crawl_visited` plus existing usage — safely
     under budget indefinitely. Going broader than that reliably requires
     either a paid Supabase tier ($25/mo Pro → 8GB) or periodically
     pruning old `host_crawl_visited` rows by a time window (trades
     storage for re-crawling previously-dead hosts later).

**On hashing hosts to save space:** checked the math — a hash short
enough to meaningfully shrink storage (e.g. 5 base36 chars, ~60M keyspace)
has an unacceptable collision rate at ~4.3M hosts (near-certain collision
by the birthday paradox); a collision-safe hash needs ~64 bits (8 bytes),
which is barely smaller than Postgres already stores for a typical short
hostname string (Postgres text storage is a 1-4 byte length-prefix plus
the raw UTF-8 bytes, not fixed/padded) — and a hash can't be reversed back
to the actual hostname, which both `host_crawl_visited` (needs to know
*which* host to skip) and `host_crawl_results` (needs the actual
`source_host` for the match to be useful) require. Verdict: not a real
win here — the schema/dequeue fixes above deliver more actual savings for
free, with no functionality lost.

**On moving to Google Cloud:** verified live — Firestore has a genuine
**standing 1 GB free tier** (not a time-limited trial, resets don't apply
to storage), 2x Supabase's 500MB. Cloud SQL for PostgreSQL, by contrast,
is only a **30-day free trial** (100GB, but expires, then a 90-day
grace period, then deletion) — not a real long-term free option. So a
genuine free upgrade path exists on Google Cloud, but only via Firestore's
NoSQL document model, not a drop-in Postgres replacement — moving would
mean rewriting `host_crawl.py`/`host_crawl_seed.py`'s Supabase REST calls
into Firestore's SDK/API, a real (if not huge) migration cost, not a
config change. Given the ~900K-host cap above keeps this comfortably on
Supabase's free tier indefinitely, this migration isn't necessary unless
the goal changes to "seed everything, no cap" — worth revisiting then.

## RESOLVED 2026-08 — a free path was found

**Update:** this note was originally written to hand to a second model
(Qwen/Gemini) for a fresh pair of eyes after every path this session tried
hit a real wall. Gemini's second opinion surfaced three candidate ideas
(Tranco, Hugging-Face-hosted CC derivatives like FineWeb, Rapid7 Sonar).
Verification of each found: Rapid7 Sonar is a dead end (anonymous access
was shut down in 2022, current ToS explicitly bars "marketing/lead
generation" use); Tranco is real but overstated (flat 1M domains, not
1-5M+, and is a traffic-popularity ranking that under-represents quiet
but legitimate SMBs); but **the FineWeb lead led to the actual fix**:
Common Crawl's own org re-hosts the Host Index — the exact dataset Path 1
below found completely blocked — on Hugging Face
(`huggingface.co/datasets/commoncrawl/host-index-testing-v2`), where
anonymous file listing genuinely works (confirmed live: an unauthenticated
fetch of its `tree/main/data` API returned a real listing of 26 crawl
partitions). DuckDB has native `hf://` read support (shipped in `httpfs`
since v0.10.3), so the exact `_looks_dead()` dead-domain filtering logic
this project had already designed for the Host Index could be reused
unmodified — just pointed at a different, non-blocked host. This is now
live in `host_crawl_seed.py` (v5) and the schedule is re-enabled. The rest
of this document is kept as-is below as the historical record of every
path that WAS a genuine dead end, since that reasoning is still correct
for the specific hosts/methods it covers (data.commoncrawl.org and raw S3
remain blocked exactly as described) — Hugging Face was simply a
different, previously-unconsidered host for the same underlying data.

## The goal

`ats-global-scanner` finds companies that use a known ATS/recruiting platform
(Greenhouse, Lever, Ashby, Workday, iCIMS, SmartRecruiters, BambooHR, etc.) by
resolving `(ats, company_slug)` pairs and then scraping each company's job
board directly. It currently has 8 working discovery sources (curated repos,
Wayback Machine/ADP, YC's company list, TheirStack, HTTP Archive via BigQuery,
Common Crawl's platform-URL-pattern search, etc.) that together produce a
`slug_registry` of ~136,000 known (ats, slug) pairs.

The idea explored in this note is a **fundamentally different, bulk
"crawl-then-detect" approach**: instead of relying on curated lists or
pattern-matching known ATS URL shapes, get a very large list of raw company
domains (ideally millions), visit each one's homepage/careers page live, and
detect an ATS integration by scanning for a matching link. This is
architecturally similar to what a company-intelligence/tech-detection vendor
does at scale (e.g. how BuiltWith or Wappalyzer's own crawlers work), just
self-hosted on free CI compute.

Two sub-problems had to be solved:
1. **Where do we get a huge list of candidate domains from, for free?**
   (this note is entirely about that sub-problem)
2. Given the domain list, how do we crawl millions of them cheaply and
   respectfully? — this part is solved and working: `host_crawl.py`, an
   async (aiohttp + selectolax) crawler with robots.txt compliance,
   per-host checkpointing (`host_crawl_visited` table so nothing is
   re-crawled), and 16-way GitHub Actions sharding. This code is done,
   tested, and sitting idle waiting for a domain list to actually drain.

Everything below is about sub-problem #1: **the domain list**. This has
been a repeated dead end, and this note exists so a second/third model
(Qwen, Gemini) can look at the same facts and see if there's an angle
this session missed.

---

## Why not just ask Common Crawl for "all .com domains", like we did for ATS URL patterns?

This is a fair question because on the *ATS pattern* side of this project,
Common Crawl genuinely does work well already: `discovery.py`'s Source 4
queries Common Crawl for URLs matching known ATS platform patterns (e.g.
`boards.greenhouse.io/*`, `jobs.lever.co/*`) and that works fine, for free,
today, in production. So "why not do the same thing but for `*.com`
generally?" is the obvious next question — and the honest answer is that
those two queries are **not the same shape of query**, and Common Crawl
exposes different products with very different access rules for each.

- The ATS-pattern search is a **narrow, host-prefix pattern match**
  (`boards.greenhouse.io.*`) that Common Crawl's own **CDX Index API**
  (`index.commoncrawl.org`) is explicitly built for — it's designed for
  "give me every URL matching this specific pattern" style lookups.
- "Give me every `.com` domain" is a **bulk enumeration / aggregate query**
  over the entire index — a completely different access pattern that the
  CDX API is structurally incapable of (see "Path 3" below). The only
  Common Crawl product built for bulk aggregate queries like this is the
  **columnar (Parquet) index**, which lives on raw S3 and has its own,
  much more restrictive access story (see "Path 2" below).

So it's not that Common Crawl arbitrarily blocks the `.com` version — it's
that the *tool that works great for ATS-pattern lookups* and *the tool that
would be needed for bulk `.com` enumeration* are two different systems
inside Common Crawl, and only the first one turned out to have a genuinely
free, practical access path. The second one (detailed below) does not.

---

## Path 1: Common Crawl's Host Index — DEAD END (no free access exists)

**What it is:** A separate dataset (not the columnar index) — one row per
known web host per crawl, with aggregated fetch-status counts and a
ranking score (`hcrank`/`prank`). This looked ideal: one row per host,
already aggregated, with a "did we ever successfully fetch this" signal
built in for free dead-domain filtering.

**What was tried:**
- Free HTTPS access via `data.commoncrawl.org/projects/host-index-testing/`.
- **Confirmed live** (not a sandbox artifact — verified independently via
  a real browser fetch of `https://data.commoncrawl.org/robots.txt`):
  ```
  User-Agent: *
  Disallow: /
  ```
  This is a **blanket disallow of the entire domain for every crawler**,
  not just this one path. No compliant automated tool can fetch anything
  from this host at all.
- Checked the alternative: `cc-host-index`'s own GitHub README documents
  S3 access as `aws s3 cp s3://commoncrawl/projects/host-index-testing/...`
  **explicitly under a heading "Setup — duckdb from inside AWS —
  us-east-1"** — i.e. Common Crawl's own docs frame this as free *only
  from inside AWS*, not from an arbitrary CI runner like GitHub Actions.
- Checked for a BigQuery mirror (since HTTP Archive's dataset lives there
  and this project already uses it for free) — confirmed via Common
  Crawl's own mailing list history that this was requested twice (2016,
  and again later) and **explicitly declined both times**, citing
  insufficient engineering staff to support it.

**Verdict:** No free, no-AWS-account access path exists for the Host Index
at all. This is not "hard to get to" — it is fully walled off from anyone
without an AWS account and a reason to eat the cost of running from
inside AWS specifically.

---

## Path 2: Common Crawl's Columnar Index via anonymous/free S3 — DEAD END (structurally, not just policy)

This is the dataset that *should* have worked, and three genuinely different
technical approaches were tried against it, each one revealing a new wall.

**What it is:** A URL-level Parquet index (not host-level) of every page
Common Crawl has successfully or unsuccessfully fetched, with fields
including `url_host_registered_domain`, `url_host_tld`, `fetch_status`,
partitioned by `crawl` (e.g. `CC-MAIN-2026-30`) and `subset` (`warc`).
Common Crawl's own docs state plainly: *"Crawl data is free to access by
anyone from anywhere... There is no need to create an AWS account... The
argument `--no-sign-request` allows for anonymous access."* This applies
to the underlying **S3 bucket** (`s3://commoncrawl/`), which is a
different, separate access surface from `data.commoncrawl.org`'s blocked
HTTPS mirror — critically, **S3 API calls are not HTTP crawling and are
not covered by robots.txt at all** (robots.txt only governs
website-crawling conventions; it has no jurisdiction over the S3 protocol).
This was a real, correct distinction — the problem turned out to be
elsewhere.

### Attempt 2a — DuckDB's httpfs extension reading `s3://commoncrawl` directly

The plan: point DuckDB's Parquet reader straight at the S3 path, remotely,
with column/row-group pruning, no download needed — cheapest and fastest
possible design if it worked.

**Result, live on GitHub Actions:**
```
HTTP Error: HTTP GET error reading 's3://commoncrawl/cc-index/table/cc-main/warc/crawl=...'
in region 'us-east-1' (HTTP 403 Forbidden)
AccessDenied: Access Denied
Authentication Failure - this is usually caused by invalid or missing credentials.
```

Researched why: DuckDB's S3 client **always attempts to sign requests**
once its `httpfs` extension is loaded. Checked DuckDB's own current docs
for an anonymous/unsigned secret provider — there isn't one. The only two
documented `CREATE SECRET ... TYPE s3` providers are `config` (explicit
access key/secret) and `credential_chain` (delegates to the AWS SDK's
normal credential resolution: env vars, `~/.aws/credentials`, IMDS, SSO,
etc.). Neither of these sends a genuinely unsigned request the way
`aws s3 --no-sign-request` does. This is a real, structural gap in DuckDB's
S3 support versus the AWS CLI/SDK — not a Common Crawl policy problem at
all. **Verdict: DuckDB cannot do what the AWS CLI's `--no-sign-request`
does, full stop, as of the version tested.**

### Attempt 2b — Switch to the AWS CLI's genuine `--no-sign-request`

Rewrote to shell out to `aws s3 ls` / `aws s3 cp ... --no-sign-request`
instead of asking DuckDB to talk to S3 at all — this really does send a
fully unsigned request, confirmed by AWS's own documentation as the
correct anonymous-access method, and is literally what Common Crawl's own
"Get Started" page recommends.

**Result, live on GitHub Actions:**
```
aws: [ERROR]: An error occurred (AccessDenied) when calling the
ListObjectsV2 operation: Access Denied
```

This is a **different** operation being denied — not `GetObject` (fetching
a file you already know the exact name of), but `ListObjectsV2`
(browsing/enumerating what files exist in a folder). Researched this
pattern specifically: it is common and documented for public S3 buckets to
allow anonymous `GetObject` while denying anonymous `ListBucket`/
`ListObjectsV2` — this is a deliberate anti-enumeration bucket-policy
choice some data publishers make (found at least one independent report
of the exact same `commoncrawl` bucket behavior on a community forum).

This matters enormously here because of how the actual Parquet files are
named. Confirmed via the `cc-index-table` GitHub repo: part files are
named like `part-00000-4b2c091d-24db-4248-8c3c-817fd04b7a85.c000.gz.parquet`
— this is Spark's default output naming, where the middle segment is a
**randomly generated UUID per Spark write job**, not a predictable or
guessable sequence. There is no published manifest file for this dataset
(unlike the raw WARC/WAT/WET crawl segments, which do get a
`warc.paths.gz`-style manifest) and no alternate host that publishes one.

**Verdict: you cannot discover the object keys you'd need to `GetObject`
without either (a) list permission, which is denied, or (b) some other
catalog of what exists, which doesn't exist anonymously.** This isn't a
policy inconvenience to work around — it's a structural chicken-and-egg:
anonymous reads work, but only if you already know the exact random
filename, and there's no free way to learn what that filename is.

### Attempt 2c — Common Crawl's CDX Index API as a listing-free alternative

Checked whether `index.commoncrawl.org`'s CDX/CDXJ search API (a separate,
free, no-signup system built for URL lookups) could substitute for bulk
enumeration.

**Finding:** structurally, no. The API's `url` parameter is mandatory and
must resolve to a specific domain/host/URL/prefix you already know
(`matchType=domain` expands *one* domain to its own subdomains — it does
not expand a TLD to its sibling domains). There is no wildcard-by-TLD mode,
no "list N domains under `.com`" mode, no enumeration cursor over the
domain namespace at all. Common Crawl's own docs point people who want
bulk/aggregate queries at the columnar index instead — i.e. Common Crawl
itself considers the CDX API the wrong tool for this, which matches what
was found. (Also separately noted: `index.commoncrawl.org` has its own
robots.txt that disallows automated access, and the API is explicitly
rate-limited/IP-banned for abuse — so even if it could do bulk queries,
it wouldn't be viable at real scale.)

**Verdict: no listing-free path exists inside Common Crawl's own free
tooling.**

---

## Path 3: AWS Athena — WORKS, but requires a real, billed AWS account

**Why this is different from 2a/2b:** Athena is a managed SQL query
service. When you run a query, **Athena's own service credentials** (via
AWS Glue's metastore) do the partition/file discovery server-side — not
your anonymous/unauthenticated client. The `ListObjectsV2`-denied wall from
Attempt 2b simply doesn't apply, because your code never lists S3 objects
directly at all; Athena does that internally on your account's behalf,
using your account's real IAM permissions (not the public "anonymous
principal" that the bucket policy specifically restricts).

This is the **only approach in this whole investigation that is proven, on
paper, to actually work** — it's also literally what Common Crawl's own
official docs recommend for querying this table (`cc-index-table`'s
README walks through `CREATE DATABASE`, `CREATE EXTERNAL TABLE`,
`MSCK REPAIR TABLE`, then `SELECT ... WHERE crawl = '...'`).

**The catch, in full:**
- Requires a real AWS account with billing enabled — cannot be anonymous.
- Athena bills ~$5 per TB of data actually scanned. Partition pruning
  (`WHERE crawl = 'CC-MAIN-2026-30' AND subset = 'warc'`) keeps a single
  query scoped to one month's data (~200-300GB), so a realistic query
  costs roughly **$1-1.50 per crawl-month scanned** — small, bounded, and
  controllable via an Athena workgroup's "bytes scanned cutoff" safety
  net, but genuinely not zero, and genuinely requires handing AWS a
  payment method.
- Requires standing up supporting infrastructure: an IAM user scoped to
  least-privilege (Athena + Glue + read on `commoncrawl` + read/write on
  your own results bucket), and your own S3 bucket for Athena's mandatory
  query-results output location.

**Current status in this repo:** fully implemented
(`host_crawl_seed.py` + `.github/workflows/host_crawl.yml`), fully unit/
integration-tested against mocked Athena responses (query submission,
polling, pagination, TLD/fetch_status filtering, dedup — all verified
correct), but **intentionally left disabled/dormant** (`on:` trigger is
`workflow_dispatch` only, no schedule) because the account owner decided
not to open a billed AWS account for this right now. The code is ready to
turn on the moment that decision changes — see the AWS setup checklist
delivered alongside this note for the exact account/IAM/budget-alert
steps needed first.

---

## Other bulk-domain-list ideas considered and ruled out (not Common-Crawl-specific)

Asked more broadly: "is there ANY free list of companies/domains anywhere,
government or otherwise?" Findings, briefly:

- **ICANN CZDS (zone file access)** — genuinely free (contractually
  required by ICANN's Registry Agreements, not just current policy), but
  gated by a **manual, per-registry human approval process** (each TLD
  registry, including Verisign for `.com`/`.net`, approves individually;
  days to a few months, ~90%+ eventual approval rate per community
  reports but not guaranteed or fast). Gives ONLY a domain-exists +
  nameserver list (no company/industry signal at all — you'd still need
  to crawl every one of ~150M+ `.com` domains yourself to find real
  companies). Not pursued further given the approval lag plus the same
  "huge haystack, tiny needle" yield problem below.
- **SEC EDGAR** — free bulk CIK list exists, but scope is companies that
  filed with the SEC (funds, public companies, some private placements) —
  maybe a few thousand real employers of interest out of ~1M raw entries,
  no domain/website field included at all.
- **IRS** — publishes a bulk list of **nonprofits only** (EO BMF). Regular
  for-profit EIN/business data is not public at all, by design — genuinely
  a dead end, not a "hard to find" situation.
- **SAM.gov** — free bulk entity extract exists (~674K entities), but
  scoped to companies that registered to do business with the US federal
  government (contractors/grantees) — a biased, narrow slice, no domain
  field.
- **State-level registries** — no unified federal list exists at all; US
  business registration is fragmented across 50+ independent state
  Secretary-of-State systems with no free federal aggregator.
  OpenCorporates is the one project that stitched all 50 together, but
  that access is a paid commercial product beyond limited free lookups.
- **Bottom line:** every free government/public source is a biased
  partial slice (skewed toward large/public/government-adjacent
  companies), never a real "all companies" list — that gap is what paid
  data brokers (Clearbit, ZoomInfo, D&B, Crunchbase) actually get paid to
  fill.

---

## The yield-economics problem (separate from all the access-path problems above)

Worth stating clearly since it's easy to lose track of amid all the access
troubleshooting: **even if a free, complete domain list existed, the
actual payoff of crawling it may still be small relative to effort.**

This project already ran a real, live test of exactly this "crawl a bunch
of raw company domains looking for ATS links" strategy, using Web Data
Commons' JSON-LD JobPosting graph as the domain/URL source (a different,
now-fully-removed source — see `discovery.py`'s history/comments for
"WDC"). Against a 3,000-page sample, live-fetching each one, it found only
**37 net-new ATS slugs** after ~4 minutes of fetching. The person running
this project judged that too small a payoff for the crawl infrastructure
cost and killed the source entirely, reassigning its GitHub Actions slot
to something with a better hit rate.

The bulk domain lists discussed in this note (Common Crawl's ~18M+
`.com` domains in a single monthly crawl, CZDS's full ~150M+ `.com`
registry) are the same fundamental shape of problem, just orders of
magnitude bigger: the overwhelming majority of any bulk domain list is
retail sites, personal blogs, parked domains, and small businesses with no
careers page or ATS integration at all — not the companies this project is
actually looking for. Solving the access problem (getting the domain list)
does not by itself solve the yield problem (most of that list will never
produce a hit). This is worth keeping in mind when evaluating whether any
alternative access path proposed elsewhere is actually worth pursuing, even
if it turns out to be technically free.

---

## The open question for a second opinion

Is there a genuinely free (or materially cheaper than ~$1-1.50/query-run)
way to either:

(a) **Enumerate the Parquet part-file names** under
    `s3://commoncrawl/cc-index/table/cc-main/warc/crawl=X/subset=warc/`
    without S3 `ListObjectsV2` permission and without an AWS-billed Athena
    query — e.g. is there some other public catalog, a Glue Data Catalog
    that's itself publicly shared/subscribable without needing your own
    Athena spend, a community-maintained mirror of the file listing, or
    some other AWS service (Lake Formation shared catalogs? A public
    Requester-Pays exception? something else entirely) that exposes just
    the *list of keys* for free even though full `ListBucket` is denied?

(b) **Get the same underlying data (domain + TLD + fetch-success signal)
    from a genuinely free alternative source entirely** — a mirror of
    Common Crawl's index hosted somewhere with less restrictive access
    (Hugging Face datasets, Kaggle, academic mirrors, a research
    institution's re-host), or a different bulk web-crawl dataset
    altogether (not necessarily Common Crawl) that publishes host/domain
    lists with a liveness signal, free, with either no listing restriction
    or a real manifest file.

Everything above reflects real, live-tested results (not just reading
documentation) except where explicitly marked as researched-but-untested,
and the sandbox this was developed in has its own separate outbound
network restriction that blocked *direct* verification of some of these
findings from within that sandbox — but the failures described above (the
DuckDB 403, the ListObjectsV2 AccessDenied) were captured from real
GitHub Actions runs, not the sandbox, so they are genuine results, not
sandbox artifacts.
