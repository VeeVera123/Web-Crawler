"""
Configuration — multi-provider architecture.

Role classification:     Gemini + Groq, running concurrently
Location classification: NVIDIA NIM + OpenAI + Groq, running concurrently

2026-09: swapped Gemini and NVIDIA between the two stages, and added Groq
to both (explicit user request). Gemini was repeatedly hitting its
free-tier DAILY quota in location classification (long job descriptions,
heavier real workload) — moved to role classification instead, where
calls are short (just titles) and its daily budget goes much further.
NVIDIA moved the other way, consolidating into location-only rather than
splitting its one 40 RPM quota across both stages. Groq (openai/
gpt-oss-120b — confirmed live as Groq's largest/most-capable free-tier
model) now runs in BOTH stages under the same provider name, so its one
real 8K TPM quota is tracked as shared, not double-counted.

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
# — all five are ORG/PROJECT-scoped (or, for NVIDIA, per-API-key) quotas,
# not per-process, so N processes sharing one key genuinely do divide one
# pool between them (confirming the AI_RATE_SHARDS fair-share approach is
# the right model here, not an over-cautious one):
#   Cerebras (inference-docs.cerebras.ai/support/rate-limits) — Free Trial:
#     5 RPM / 30K TPM / 1M TPD, org-wide. RPM is the binding constraint by
#     far, so batches should be as LARGE as the TPM/context budget allows —
#     fewer, bigger calls make better use of a 5-RPM ceiling than many
#     small ones would.
#   Groq (console.groq.com/docs/rate-limits), openai/gpt-oss-120b: 30 RPM /
#     8K TPM / 1K RPD / 200K TPD, org-wide. TPM is the binding constraint
#     here (30 RPM is loose by comparison), so batch size stays the limiter.
#   Gemini (ai.google.dev/gemini-api/docs/rate-limits) — Google no longer
#     publishes a static free-tier RPM/TPM table; it now varies by account
#     usage tier and must be read from https://aistudio.google.com/rate-limit
#     directly. The 15 RPM figure below is the long-standing historical
#     Flash-tier free-tier number and a reasonable conservative default,
#     but if you're still seeing 429s after this change, check your actual
#     dashboard number and adjust _GEMINI_BASE_INTERVAL to match.
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
_GROQ_BASE_INTERVAL = 15.0       # 8K TPM free tier, ~1.5K tokens/call -> ~4 calls/min
                                  # (6K TPM, 75% of cap — was 30s/2-calls-min, doubled
                                  # throughput while keeping a real safety margin)
_GEMINI_BASE_INTERVAL = 4.0      # 15 RPM free tier (historical figure — verify your
                                  # own account at aistudio.google.com/rate-limit)
# NVIDIA NIM (integrate.api.nvidia.com) — no single published per-model RPM;
# NVIDIA's own docs say free-tier limits are model/account-specific, but the
# commonly reported free-tier figure across NIM-hosted chat models is ~40
# RPM, SHARED ACROSS THE WHOLE KEY (not per-model) — this matters here
# because the SAME NVIDIA_API_KEY is used for both role and location
# classification below, so both stages' calls draw from one 40 RPM pool,
# not two separate ones. 60/40 = 1.5s/call single-process baseline.
_NVIDIA_BASE_INTERVAL = 1.5

# ── Role classification providers (free tiers, concurrent) ──
# 2026-09: Gemini + Groq (explicit user request — swapped with location's
# roster below: Gemini moves here from location, NVIDIA moves OUT of role
# entirely and consolidates into location-only, Groq is now used by BOTH
# stages). Reasoning given: Gemini was hitting its free-tier DAILY quota
# repeatedly in location classification and needed a break/reroute of its
# own traffic; moving it to role (shorter, cheaper calls — titles, not
# full job descriptions) gives it a much lighter real workload while
# keeping it in the rotation instead of dropping it outright.
_ROLE_PROVIDER_DEFS = [
    # Gemini: same model/base_url as before, but a role-appropriate (much
    # smaller) max_batch_chars — titles are short, so there's no reason to
    # approach anywhere near Gemini's real ~1M-token context here. Kept
    # generous relative to Groq/NVIDIA below since Gemini's free-tier RPM
    # (not TPM) is the binding constraint for short-text batches like this.
    _make_provider(
        "gemini",
        "GEMINI_API_KEY",
        "gemini-3.5-flash",
        "https://generativelanguage.googleapis.com/v1beta/openai/",
        max_batch_chars=400_000,
        min_call_interval=_GEMINI_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
    # Groq: GPT OSS 120B — confirmed live 2026-09 via
    # console.groq.com/docs/rate-limits and console.groq.com/docs/model/
    # openai/gpt-oss-120b as Groq's largest/most-capable model with
    # published free-tier access (120B-parameter open-weight reasoning
    # model, 131,072 token context, 65,536 max output; free tier: 30 RPM /
    # 1K RPD / 8K TPM / 200K TPD) — the smartest free-tier option
    # available, per explicit user request to use it since free tier
    # costs no usage credits either way. Same provider name "groq" is
    # deliberately reused in LOCATION_PROVIDERS below, so classifier.py's
    # per-provider throttle treats both stages' Groq calls as sharing ONE
    # real 8K TPM pool, not two independent ones (same pattern already
    # used for the shared NVIDIA key elsewhere in this file).
    _make_provider(
        "groq",
        "GROQ_API_KEY",
        "openai/gpt-oss-120b",
        "https://api.groq.com/openai/v1",
        max_batch_chars=4_000,       # ~1500 tokens, fits in 8K TPM with overhead
        min_call_interval=_GROQ_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
]

ROLE_PROVIDERS = [p for p in _ROLE_PROVIDER_DEFS if p is not None]

# ── Location classification providers (concurrent) ──
# 2026-09: NVIDIA NIM + OpenAI + Groq (explicit user request — Gemini
# moved OUT to role classification above, since it kept hitting its free
# daily quota here; Groq added as a replacement third leg, using the SAME
# provider name "groq" as its role-classification entry above so both
# stages' calls draw from Groq's one real 8K TPM pool instead of being
# tracked as if they were separate quotas).
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
    # Groq: GPT OSS 120B — see the role-classification entry above for the
    # live-verified model/rate-limit details (same model, same account,
    # same "smartest free-tier option" reasoning). max_batch_chars is
    # higher than the role entry's 4_000 (real job descriptions need more
    # room than a bare title) but still small relative to
    # OpenAI/NVIDIA above — Groq's real 8K TPM ceiling is by far the
    # tightest of the three location providers, and it's a SHARED pool
    # with the role-classification Groq traffic above, so this stays
    # conservative on purpose. MAX_JOBS_PER_BATCH's 5-7 job cap (see
    # classifier.py) still does most of the real batch-size limiting here,
    # same as for OpenAI/NVIDIA.
    _make_provider(
        "groq",
        "GROQ_API_KEY",
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
