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

# ── Role classification providers (free tiers, concurrent) ──
_ROLE_PROVIDER_DEFS = [
    # Gemini: 3.5 Flash, 1M context, 15 RPM / 1M TPD free tier
    # Shared with location — role batches are tiny (titles only), so minimal impact
    _make_provider(
        "gemini",
        "GEMINI_API_KEY",
        "gemini-3.5-flash",
        "https://generativelanguage.googleapis.com/v1beta/openai/",
        max_batch_chars=400_000,     # 1M context
    ),
    # Groq: GPT OSS 120B, free tier 8K TPM — need small batches + throttle
    _make_provider(
        "groq",
        "GROQ_API_KEY",
        "openai/gpt-oss-120b",
        "https://api.groq.com/openai/v1",
        max_batch_chars=4_000,       # ~1500 tokens, fits in 8K TPM with overhead
        min_call_interval=30.0,      # 2 req/min to stay under 8K TPM
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
    ),
    # OpenAI: GPT-4.1 nano, paid, smartest for classification
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
