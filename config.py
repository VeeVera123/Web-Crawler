"""
Configuration — multi-provider architecture.

Role classification:  Gemini + Groq running concurrently (both free tiers)
Location classification: Gemini + OpenAI running concurrently (Gemini free, OpenAI paid)

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
_GEMINI_BASE_INTERVAL = 4.0     # 15 RPM free tier -> 60/15 = 4s/call, single process
_GROQ_BASE_INTERVAL = 30.0      # 8K TPM free tier -> 2 req/min, single process

# ── Role classification providers (free tiers, concurrent) ──
_ROLE_PROVIDER_DEFS = [
    # Gemini: 3.5 Flash, 1M context, 15 RPM / 1M TPD free tier.
    # Shared with location (same provider "gemini", same _last_call_times
    # entry in classifier.py) — role and location run sequentially within
    # one process, so they share one throttle clock safely.
    _make_provider(
        "gemini",
        "GEMINI_API_KEY",
        "gemini-3.5-flash",
        "https://generativelanguage.googleapis.com/v1beta/openai/",
        max_batch_chars=400_000,     # 1M context
        min_call_interval=_GEMINI_BASE_INTERVAL * AI_RATE_SHARDS,
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
]

ROLE_PROVIDERS = [p for p in _ROLE_PROVIDER_DEFS if p is not None]

# ── Location classification providers (concurrent) ──
# Location needs the smartest models — Gemini (1M context, free) + OpenAI (paid).
_LOCATION_PROVIDER_DEFS = [
    # Gemini: 1M context, 15 RPM / 1M TPD free tier
    _make_provider(
        "gemini",
        "GEMINI_API_KEY",
        "gemini-3.5-flash",
        "https://generativelanguage.googleapis.com/v1beta/openai/",
        max_batch_chars=400_000,     # 1M context
        min_call_interval=_GEMINI_BASE_INTERVAL * AI_RATE_SHARDS,
    ),
    # OpenAI: GPT-4.1 nano, paid tier. Not scaled by AI_RATE_SHARDS — Tier 1
    # paid limits (hundreds of RPM) comfortably absorb 9 concurrent processes
    # at ~12 req/min each (~108 RPM aggregate). Revisit if your OpenAI account
    # is on a lower tier than that.
    _make_provider(
        "openai",
        "OPENAI_API_KEY",
        "gpt-4.1-nano",
        "https://api.openai.com/v1",
        max_batch_chars=300_000,     # 400K - 100K breathing space
        min_call_interval=5.0,       # Tier 1: ~12 req/min
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
