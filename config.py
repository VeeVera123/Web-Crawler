"""
Configuration — multi-provider architecture.

Role classification:     Groq-O + Groq-C, running concurrently
Location classification: Groq-O + Groq-C + NVIDIA NIM + OpenAI, running concurrently

2026-09 ROUND 4 — Gemini and Mistral REMOVED entirely, replaced by a
second independent Groq account (explicit user instruction, after
discovering they already held two separate Groq accounts — and two
separate Gemini accounts, though Gemini is dropped here regardless, not
doubled): "Groq-O" (the original GROQ_API_KEY, already in use) and
"Groq-C" (a second, genuinely separate Groq account/API key — new env var
GROQ_API_KEY_C) are two INDEPENDENT accounts, each with its own real free-
tier quota pool (30 RPM / 8K TPM / 1K RPD / 200K TPD per
console.groq.com/docs/rate-limits — see the base-interval comment block
below), not two views of the same key. Each name is used in BOTH role and
location classification (mirroring the existing single-"groq" pattern),
so classifier.py's per-provider throttle correctly tracks TWO separate
pools instead of doubling load on one. This roughly doubles this
project's total Groq-family throughput without touching a single other
provider, and sidesteps Gemini's daily-quota problems and Mistral's
403-then-429 saga entirely (see below for that history) rather than
continuing to fight either. Gemini is removed outright (not kept as a
third leg) and Mistral is removed outright (not kept as a fourth,
worst-performing leg) — explicit instruction, and there's no remaining
reason to keep either once two independent, reliable Groq quotas cover
the volume that prompted adding them in the first place.
GROQ_API_KEY_C is a NEW secret the account owner needs to add (GitHub
Actions secret + local .env, same as any other provider key here) — it
does not exist yet purely from this code change.

SUPERSEDED HISTORY (Gemini and Mistral, both fully removed as of ROUND 4
above — kept as a condensed record so nobody re-adds either without
knowing why they were dropped):
  - Gemini was added early on for role classification, moved between
    stages once (it kept hitting its free-tier DAILY quota in location
    classification's heavier workload — moved to role's shorter title-only
    calls instead), and is now removed outright per ROUND 4.
  - Mistral was added later ("another generous free AI provider" request)
    and went through 3 rounds of real production trouble before removal:
    (1) mistral-large-2512 looked like a straight upgrade over
    mistral-small-2603 based on admin.mistral.ai/plateforme/limits'
    published TPM figures (250,000 vs 20,000, same 1 RPS) — but a real
    crawl run hit Large with a live 403 tier_not_allowed (this account's
    plan didn't include it; the limits page's number didn't mean the model
    was actually callable — a live 403 is stronger evidence than a
    numbers-only page). (2) Downgraded to mistral-small-2603, dropped from
    location classification (Small's real 20,000 TPM budget was too small
    for description-length batches, role-only from here on). (3) Even
    Small's "confirmed" 1 RPS console figure didn't match reality — a live
    run got HTTP 429 on every batch, three in a row, exhausting all
    retries each time; widened to 30s/call based on a community tool's
    real-world ~1-req/30s finding. Around the same time, Cerebras (requires
    a card now — inference-docs.cerebras.ai/support/rate-limits) and
    OpenRouter (free tier capped at 20 RPM/50 RPD; 1000 RPD needs a
    one-time $10 purchase, a real cost decision left to the account owner)
    were both researched and NOT added. Net lesson from the whole Mistral
    saga: a provider's own rate-limit console page can be flatly wrong
    about both model access AND real throughput — trust production
    evidence over documentation. ROUND 4's two-Groq-account fix made all
    of this moot rather than pursuing a 4th round of Mistral tuning.

Together with the failover in classifier.py's ai_classify_roles()/
ai_classify_locations() — if one of a stage's providers fails a batch
(after exhausting its own retries, OR immediately once flagged as
exhausted for the rest of this run — see classifier.py's
_exhausted_providers_today), the other providers for that stage pick up
its remaining work — this means either stage can survive any single
provider being down, rate-limited, or quota-exhausted.

Legacy single-provider mode still works via LLM_PROVIDER env var.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Supabase ───────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# ── Scraper settings (shared, provider-independent) ──────
# 15s (was 20s) — most ATS APIs respond in <5s; a genuinely dead board still
# gets MAX_RETRIES attempts, so this only speeds up abandoning dead boards
# (15s x 3 attempts = 45s worst case, vs 60s before), not real ones.
REQUEST_TIMEOUT = 15
MAX_RETRIES = 2

# ── Multi-provider configuration ─────────────────────────
# Each provider is a dict with: name, api_key, model, base_url, max_batch_chars, min_call_interval
# Role providers run concurrently for speed.
# Location provider runs alone (needs the smartest model).

# ── Cross-process rate-limit scaling ──────────────────────
# classifier.py's _last_call_times throttle is in-process memory only — it
# has no way to see other processes hitting the SAME provider API key.
# GitHub Actions matrix sharding runs each shard as a separate runner/process,
# so with the daily scan's 8 ATS shards + 1 job-boards process all classifying
# concurrently, a per-shard min_call_interval tuned for "one process, whole
# quota" lets all 9 processes race for that one shared-key quota at once —
# real risk of 429s and wasted retry time on a free-tier key (this is the
# issue Qwen flagged for Gemini's 15 RPM).
#
# Fix: each concurrent process throttles itself to a FAIR SHARE of the quota
# instead of the whole thing, by multiplying its interval by how many AI
# processes are running at once. daily_scan.yml sets AI_RATE_SHARDS=9 (8
# scrape-ats shards + 1 scrape-job-boards) for exactly this reason. Local /
# manual runs (--total-shards 1, no env var set) default to 1 — i.e. no
# scaling, same behavior as before sharding existed.
AI_RATE_SHARDS = max(1, int(os.environ.get("AI_RATE_SHARDS", "1")))


def _make_provider(name, api_key_env, model, base_url, max_batch_chars, min_call_interval=0.0):
    """Build a provider config dict. Skips if API key env var is not set."""
    key = os.environ.get(api_key_env, "")
    if not key:
        return None
    return {
        "name": name,
        "api_key": key,
        "model": model,
        "base_url": base_url,
        "max_batch_chars": max_batch_chars,
        "min_call_interval": min_call_interval,
    }

# Base, single-process-safe intervals for shared-free-tier-key providers.
# These get multiplied by AI_RATE_SHARDS below so N concurrent processes
# collectively stay under the same quota one process was tuned against.
# Verified against each provider's own docs/pricing pages (2026-08/2026-09)
# — all are ORG/PROJECT-scoped (or, for NVIDIA, per-API-key) quotas, not
# per-process, so N processes sharing one key genuinely do divide one pool
# between them (confirming the AI_RATE_SHARDS fair-share approach is the
# right model here, not an over-cautious one):
#   Cerebras (inference-docs.cerebras.ai/support/rate-limits) — Free Trial:
#     5 RPM / 30K TPM / 1M TPD, org-wide. RPM is the binding constraint by
#     far, so batches should be as LARGE as the TPM/context budget allows —
#     fewer, bigger calls make better use of a 5-RPM ceiling than many
#     small ones would. (Legacy single-provider fallback only — not in the
#     current multi-provider roster; also now requires a card to activate
#     at all, see ROUND 4's superseded-history note above.)
#   Groq (console.groq.com/docs/rate-limits), openai/gpt-oss-120b: 30 RPM /
#     8K TPM / 1K RPD / 200K TPD PER ACCOUNT, org-wide. TPM is the binding
#     constraint here (30 RPM is loose by comparison), so batch size stays
#     the limiter. 2026-09 ROUND 4: this project now uses TWO independent
#     Groq accounts ("Groq-O"/GROQ_API_KEY and "Groq-C"/GROQ_API_KEY_C),
#     each with this exact same quota, so total available Groq-family
#     throughput is roughly double a single account's — see the module
#     docstring for the full story.
#   OpenAI (platform.openai.com/docs/guides/rate-limits), gpt-4.1-nano,
#     Tier 1: 500 RPM / 200K TPM, org+project-scoped. Current interval
#     already runs at ~12% of the confirmed limit even at 9 concurrent
#     shards — left unchanged since OpenAI isn't the provider with a
#     reported rate-limit problem, but there's real headroom if needed later.
#   NVIDIA NIM (build.nvidia.com / integrate.api.nvidia.com): no single
#     published per-model free-tier number — NVIDIA's own docs say limits
#     are model/account-specific — but ~40 RPM shared across the whole key
#     (not per-model) is the figure consistently reported for NIM-hosted
#     chat models as of 2026-09. Treated as a single pool shared by BOTH
#     the role and location NVIDIA entries below (same provider name
#     "nvidia", same key), since that's what's actually true of the quota.
_CEREBRAS_BASE_INTERVAL = 12.0   # 5 RPM free tier -> 60/5 = 12s/call, single process
                                  # (legacy fallback only — see comment above)
_GROQ_BASE_INTERVAL = 15.0       # 8K TPM free tier, ~1.5K tokens/call -> ~4 calls/min
                                  # (6K TPM, 75% of cap — was 30s/2-calls-min, doubled
                                  # throughput while keeping a real safety margin).
                                  # Shared by BOTH Groq-O and Groq-C below — each is
                                  # its own independent account with this same real
                                  # quota, not two views of one pool.
# NVIDIA NIM (integrate.api.nvidia.com) — no single published per-model RPM;
# NVIDIA's own docs say free-tier limits are model/account-specific, but the
# commonly reported free-tier figure across NIM-hosted chat models is ~40
# RPM, SHARED ACROSS THE WHOLE KEY (not per-model). Location-classification
# only (see LOCATION_PROVIDER_DEFS below) — 60/40 = 1.5s/call single-process
# baseline.
_NVIDIA_BASE_INTERVAL = 1.5

# ── Role classification providers (free tiers, concurrent) ──
# 2026-09 ROUND 4: Groq-O + Groq-C ONLY (explicit user instruction — Gemini
# and Mistral both removed entirely, see module docstring for the full
# history/reasoning). Two independent Groq accounts stand in for what used
# to be a 3-provider roster (Gemini/Groq/Mistral), because two genuinely
# separate free-tier Groq quotas cover more real throughput than three
# providers where one (Gemini) hit daily quotas and the other (Mistral)
# never reliably worked at all (403s, then persistent 429s).
_ROLE_PROVIDER_DEFS = [
    # Groq-O ("Original"): the account already in use before this round —
    # same GROQ_API_KEY env var as always, no secret change needed for this
    # one. GPT OSS 120B — confirmed live 2026-09 via
    # console.groq.com/docs/rate-limits and console.groq.com/docs/model/
    # openai/gpt-oss-120b as Groq's largest/most-capable model with
    # published free-tier access (120B-parameter open-weight reasoning
    # model, 131,072 token context, 65,536 max output; free tier: 30 RPM /
    # 1K RPD / 8K TPM / 200K TPD). Same provider name "groq-o" is
    # deliberately reused in LOCATION_PROVIDERS below, so classifier.py's
    # per-provider throttle treats both stages' Groq-O calls as sharing ONE
    # real 8K TPM pool, not two independent ones (same pattern already
    # used for the shared NVIDIA key elsewhere in this file).
    _make_provider(
        "groq-o",
        "GROQ_API_KEY",
        "openai/gpt-oss-120b",
        "https://api.groq.com/openai/v1",
        max_batch_chars=4_000,       # ~1500 tokens, fits in 8K TPM with overhead
        min_call_interval=_GROQ_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
    # Groq-C ("Clone"/second account): a genuinely SEPARATE Groq account —
    # its own signup, own API key, own independent 30 RPM / 8K TPM / 1K RPD
    # quota, identical tier/model to Groq-O above but tracked as its own
    # pool. NEW env var GROQ_API_KEY_C — the account owner needs to add
    # this as a GitHub Actions secret (and local .env) for this entry to
    # activate; until then _make_provider silently skips it, same as any
    # other provider with an unset key. Same model/limits/reasoning as
    # Groq-O; the only difference is which account's quota it draws from.
    _make_provider(
        "groq-c",
        "GROQ_API_KEY_C",
        "openai/gpt-oss-120b",
        "https://api.groq.com/openai/v1",
        max_batch_chars=4_000,
        min_call_interval=_GROQ_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
]

ROLE_PROVIDERS = [p for p in _ROLE_PROVIDER_DEFS if p is not None]

# ── Location classification providers (concurrent) ──
# 2026-09 ROUND 4: OpenAI + NVIDIA + Groq-O + Groq-C (explicit user
# instruction — Mistral removed entirely, see module docstring; Groq-O and
# Groq-C use the SAME provider names as their role-classification entries
# above so each stays tracked as one real pool per account, not doubled).
_LOCATION_PROVIDER_DEFS = [
    # OpenAI: GPT-4.1 nano, paid tier. Confirmed Tier 1: 500 RPM / 200K TPM,
    # org+project-scoped. Not scaled by AI_RATE_SHARDS — even at 9 concurrent
    # processes x 12 req/min each (~108 RPM aggregate), that's ~22% of the
    # confirmed 500 RPM ceiling. Revisit if your OpenAI account is on a
    # lower tier than Tier 1.
    # Context/max_batch_chars verified live 2026-09 via
    # https://developers.openai.com/api/docs/models/gpt-4.1-nano —
    # 1,047,576 token context, 32,768 max output. 0.8 headroom x ~4
    # chars/tok heuristic. NOTE: this char budget is a safety net, not the
    # real limiter — the actual quality-driven cap on batch size is
    # MAX_JOBS_PER_BATCH (5-7, see classifier.py's
    # _build_dynamic_batches/_dynamic_job_cap), since a real job batch of
    # even 7 long (30K-char) descriptions is ~210K chars, nowhere near
    # this ceiling.
    _make_provider(
        "openai",
        "OPENAI_API_KEY",
        "gpt-4.1-nano",
        "https://api.openai.com/v1",
        max_batch_chars=3_300_000,   # 1,047,576 tok * 0.8 * 4 chars/tok ≈ 3.35M, rounded down
        min_call_interval=5.0,       # Tier 1: ~12 req/min
    ),
    # NVIDIA NIM: same key/model/quota pool as elsewhere in this file
    # ("nvidia" — deliberately the same provider name, so classifier.py's
    # per-provider throttle correctly treats all NVIDIA calls as sharing
    # ONE real 40 RPM quota). Now used for location ONLY (2026-09 — moved
    # out of role classification, consolidating it here instead of
    # splitting its one quota across both stages).
    # Context/max_batch_chars verified live 2026-09 via
    # https://build.nvidia.com/nvidia/nemotron-3.5-lightning-30b-a3b/modelcard
    # — up to 1,000,000 token context. Same 0.8 headroom x ~4 chars/tok
    # heuristic and "safety net, not the real limiter" caveat as above.
    _make_provider(
        "nvidia",
        "NVIDIA_API_KEY",
        "nvidia/nemotron-3.5-lightning-30b-a3b",
        "https://integrate.api.nvidia.com/v1",
        max_batch_chars=3_200_000,   # 1,000,000 tok * 0.8 * 4 chars/tok
        min_call_interval=_NVIDIA_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
    # Groq-O + Groq-C: GPT OSS 120B, two independent accounts — see the
    # role-classification entries above for the live-verified model/rate-
    # limit details and the "genuinely separate quota, not double-counted"
    # explanation. max_batch_chars is higher than the role entries' 4_000
    # (real job descriptions need more room than a bare title) but still
    # small relative to OpenAI/NVIDIA above — each account's real 8K TPM
    # ceiling is by far the tightest of the four location providers, and
    # each is a SHARED pool with that same account's role-classification
    # traffic above, so this stays conservative on purpose.
    # MAX_JOBS_PER_BATCH's 5-7 job cap (see classifier.py) still does most
    # of the real batch-size limiting here, same as for OpenAI/NVIDIA.
    _make_provider(
        "groq-o",
        "GROQ_API_KEY",
        "openai/gpt-oss-120b",
        "https://api.groq.com/openai/v1",
        max_batch_chars=6_000,
        min_call_interval=_GROQ_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
    _make_provider(
        "groq-c",
        "GROQ_API_KEY_C",
        "openai/gpt-oss-120b",
        "https://api.groq.com/openai/v1",
        max_batch_chars=6_000,
        min_call_interval=_GROQ_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
]

LOCATION_PROVIDERS = [p for p in _LOCATION_PROVIDER_DEFS if p is not None]

# Backward compat: single LOCATION_PROVIDER for code that expects one
LOCATION_PROVIDER = LOCATION_PROVIDERS[0] if LOCATION_PROVIDERS else None

# ── Legacy single-provider fallback ──────────────────────
# If no role providers are configured, fall back to LLM_PROVIDER
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "cerebras").lower()

if not ROLE_PROVIDERS:
    # No multi-provider keys set — use legacy single provider
    if LLM_PROVIDER == "cerebras":
        LLM_API_KEY = os.environ["CEREBRAS_API_KEY"]
        LLM_MODEL = "gpt-oss-120b"
        LLM_BASE_URL = "https://api.cerebras.ai/v1"
    elif LLM_PROVIDER == "groq":
        LLM_API_KEY = os.environ["GROQ_API_KEY"]
        LLM_MODEL = "openai/gpt-oss-120b"
        LLM_BASE_URL = "https://api.groq.com/openai/v1"
    elif LLM_PROVIDER == "anthropic":
        LLM_API_KEY = os.environ["ANTHROPIC_API_KEY"]
        LLM_MODEL = "claude-haiku-4-5-20251001"
        LLM_BASE_URL = None
    elif LLM_PROVIDER == "openai":
        LLM_API_KEY = os.environ["OPENAI_API_KEY"]
        LLM_MODEL = "gpt-4.1-nano"
        LLM_BASE_URL = "https://api.openai.com/v1"
    else:
        raise ValueError(f"Unknown LLM_PROVIDER: {LLM_PROVIDER!r}")
else:
    # Multi-provider mode — set legacy vars from first role provider for backward compat
    LLM_API_KEY = ROLE_PROVIDERS[0]["api_key"]
    LLM_MODEL = ROLE_PROVIDERS[0]["model"]
    LLM_BASE_URL = ROLE_PROVIDERS[0]["base_url"]

if not LOCATION_PROVIDERS:
    # No Gemini/OpenAI keys for location — fall back to legacy provider
    LOCATION_PROVIDER = {
        "name": LLM_PROVIDER,
        "api_key": LLM_API_KEY,
        "model": LLM_MODEL,
        "base_url": LLM_BASE_URL,
        "max_batch_chars": 300_000 if LLM_PROVIDER != "cerebras" else 6_000,
        "min_call_interval": 0.0,
    }
    LOCATION_PROVIDERS = [LOCATION_PROVIDER]

# AI_PARALLEL_REQUESTS — kept for backward compat but not used in multi-provider mode
AI_PARALLEL_REQUESTS = 1
AI_BATCH_SIZE = 25  # legacy, not used by char-based batching
