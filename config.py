"""
Configuration — multi-provider architecture.

Role classification:  Cerebras + Groq + NVIDIA NIM running concurrently
  (all three free tiers — NVIDIA added 2026-09)
Location classification: Gemini + OpenAI + NVIDIA NIM running concurrently
  (Gemini free, OpenAI paid, NVIDIA NIM free — added 2026-09)

NVIDIA NIM is the one provider used for BOTH stages — see its comment in
_ROLE_PROVIDER_DEFS below for why that's safe (one shared per-name
throttle clock, not two independent 40 RPM budgets against one account).

Changed 2026-08: role classification moved off Gemini (was Gemini + Groq)
onto Cerebras + Groq. Gemini was previously doing double duty — every
process runs BOTH role classification (stage 2) and location
classification (stage 4) sequentially, and both shared one Gemini API
key/project quota. Across 9 concurrent GitHub Actions processes, that's
18 independent streams of Gemini calls (9 processes x 2 stages) fighting
over one quota, not 9 — which is what was actually driving the ~70
"too many requests" hits, not an under-calibrated interval. Giving role
classification its own dedicated providers (Cerebras + Groq) roughly
halves Gemini's total call volume with no code-path changes needed.

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
# Verified against each provider's own docs (2026-08) — all four are
# ORG/PROJECT-scoped quotas, not per-process or per-API-key, so N processes
# sharing one key genuinely do divide one pool between them (confirming
# the AI_RATE_SHARDS fair-share approach is the right model here, not an
# over-cautious one):
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
_CEREBRAS_BASE_INTERVAL = 12.0   # 5 RPM free tier -> 60/5 = 12s/call, single process
_GROQ_BASE_INTERVAL = 15.0       # 8K TPM free tier, ~1.5K tokens/call -> ~4 calls/min
                                  # (6K TPM, 75% of cap — was 30s/2-calls-min, doubled
                                  # throughput while keeping a real safety margin)
_GEMINI_BASE_INTERVAL = 4.0      # 15 RPM free tier (historical figure — verify your
                                  # own account at aistudio.google.com/rate-limit)
_NVIDIA_BASE_INTERVAL = 1.5   # ~40 RPM free tier (community-confirmed on
                              # NVIDIA's own dev forum, not a published SLA —
                              # see the location-provider comment below for
                              # the full sourcing note). Defined here (not
                              # down by the location providers) because
                              # 2026-09 added NVIDIA to BOTH role and
                              # location classification, sharing ONE
                              # NVIDIA_API_KEY/account quota across both
                              # stages — see the role-provider entry below
                              # for how that sharing is actually enforced.

# ── Role classification providers (free tiers, concurrent) ──
# Cerebras + Groq — moved off Gemini 2026-08, see module docstring for why.
# NVIDIA NIM added 2026-09 (also serves location classification below).
_ROLE_PROVIDER_DEFS = [
    # Cerebras: gpt-oss-120b, confirmed live/non-deprecated (2026-08). Free
    # tier is 5 RPM ORG-WIDE — the tightest budget of any provider here —
    # so batches are sized up (24K chars, ~6K tokens) to minimize how many
    # calls are needed per shard rather than firing lots of small ones.
    _make_provider(
        "cerebras",
        "CEREBRAS_API_KEY",
        "gpt-oss-120b",
        "https://api.cerebras.ai/v1",
        max_batch_chars=24_000,      # ~6K tokens, well under the 30K TPM cap
        min_call_interval=_CEREBRAS_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
    # Groq: GPT OSS 120B, free tier 8K TPM — need small batches + throttle
    _make_provider(
        "groq",
        "GROQ_API_KEY",
        "openai/gpt-oss-120b",
        "https://api.groq.com/openai/v1",
        max_batch_chars=4_000,       # ~1500 tokens, fits in 8K TPM with overhead
        min_call_interval=_GROQ_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
    # NVIDIA NIM, added 2026-09 at explicit user request: role titles are
    # short, so this doesn't need the huge-context model — reuses the same
    # nemotron-3-super-120b-a12b as location classification below (one
    # already-verified model instead of introducing a second unverified
    # one). IMPORTANT: this is the SAME provider name ("nvidia") as the
    # location-classification entry further down, and classifier.py's
    # _last_call_times throttle is keyed by provider NAME, not by which
    # classification stage called it — so a role-classification nvidia
    # call and a location-classification nvidia call in the same process
    # share ONE clock and naturally respect a combined ~40 RPM budget
    # across both stages, rather than each stage getting its own 40 RPM
    # and doubling real usage against the one account/key. No extra code
    # needed for that — it falls out of _ai_call's existing per-name
    # throttle dict.
    _make_provider(
        "nvidia",
        "NVIDIA_API_KEY",
        "nvidia/nemotron-3-super-120b-a12b",
        "https://integrate.api.nvidia.com/v1",
        max_batch_chars=100_000,     # titles are short — no need for the full 200K used for locations
        min_call_interval=_NVIDIA_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
]

ROLE_PROVIDERS = [p for p in _ROLE_PROVIDER_DEFS if p is not None]

# ── Location classification providers (concurrent) ──
# Location needs the smartest models — Gemini (1M context, free) + OpenAI (paid)
# + NVIDIA NIM (free, added 2026-09 — also serves role classification above,
# see that entry's comment on why one shared _NVIDIA_BASE_INTERVAL clock is
# correct for both). Gemini serves ONLY this stage (role classification
# moved off it in 2026-08), roughly halving its total call volume across a
# full run.
_LOCATION_PROVIDER_DEFS = [
    # Gemini: 1M context, ~15 RPM free tier (see note above on why this
    # isn't a hard-confirmed current number)
    _make_provider(
        "gemini",
        "GEMINI_API_KEY",
        "gemini-3.5-flash",
        "https://generativelanguage.googleapis.com/v1beta/openai/",
        max_batch_chars=400_000,     # 1M context
        min_call_interval=_GEMINI_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
    # OpenAI: GPT-4.1 nano, paid tier. Confirmed Tier 1: 500 RPM / 200K TPM,
    # org+project-scoped. Not scaled by AI_RATE_SHARDS — even at 9 concurrent
    # processes x 12 req/min each (~108 RPM aggregate), that's ~22% of the
    # confirmed 500 RPM ceiling. Revisit if your OpenAI account is on a
    # lower tier than Tier 1.
    _make_provider(
        "openai",
        "OPENAI_API_KEY",
        "gpt-4.1-nano",
        "https://api.openai.com/v1",
        max_batch_chars=300_000,     # 400K - 100K breathing space
        min_call_interval=5.0,       # Tier 1: ~12 req/min
    ),
    # NVIDIA NIM (build.nvidia.com): free, no credit card, explicitly
    # "unlimited API requests without daily limits" per NVIDIA's own free-
    # tier page (phone verification exists specifically to gate that
    # against abuse, not to cap real usage) — confirmed by the user
    # directly quoting that page. RPM still applies: ~40 RPM is the
    # community-confirmed baseline from NVIDIA's own dev forum ("I have
    # reached the default rate limit of 40 Requests Per Minute") — no
    # published SLA, treat as a working baseline. Chosen over OpenRouter's
    # free tier, whose daily cap (50/day with no prior spend, 1,000/day
    # only after $10 lifetime credit purchase) is too small here.
    #
    # base_url below is live-confirmed 2026-09 (fetched the actual GET
    # /v1/models listing from https://integrate.api.nvidia.com/v1 — NVIDIA's
    # real, current NIM catalog, not a third-party guide).
    #
    # 2026-09 CORRECTION: this provider originally pointed at
    # nemotron-4-340b-instruct (present in that catalog, and a plain
    # instruct model — no reasoning-preamble risk) without checking its
    # context window. Checked after the fact: it's a 2024-era model with
    # only a 4,096-token context (confirmed via its own Hugging Face
    # README and OpenRouter's listing, matching each other) — far too
    # small for this pipeline's location-classification batches
    # (max_batch_chars up to 200K chars, ~50K+ tokens, deliberately sized
    # for full, untruncated job descriptions — see
    # _classify_location_batch's docstring on why). Every real batch
    # would have overflowed it and either errored or silently truncated
    # descriptions server-side.
    #
    # Replaced with nemotron-3-super-120b-a12b: also confirmed present in
    # the same live /v1/models listing, a newer (Nemotron 3, not 4)
    # non-reasoning instruct-style model (no "-reasoning" suffix, unlike
    # e.g. nemotron-3-nano-omni-30b-a3b-reasoning in the same catalog —
    # same direct-answer shape as Gemini/OpenAI here), and a context
    # window corroborated by two independent sources: NVIDIA's own
    # research page (research.nvidia.com/labs/nemotron/Nemotron-3-Super,
    # "up to 1M tokens") and OpenRouter's model listing ("262,144 token
    # context window" as actually served) — either figure comfortably
    # covers this pipeline's batches with room to spare.
    _make_provider(
        "nvidia",
        "NVIDIA_API_KEY",
        "nvidia/nemotron-3-super-120b-a12b",
        "https://integrate.api.nvidia.com/v1",
        max_batch_chars=200_000,     # well inside the confirmed 262K-token+ window
        min_call_interval=_NVIDIA_BASE_INTERVAL * AI_RATE_SHARDS,
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
