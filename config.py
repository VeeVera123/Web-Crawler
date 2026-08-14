"""
Configuration — multi-provider architecture.
Role classification:  Cerebras + Groq + Gemini running concurrently (all free tiers)
Location classification: OpenAI GPT-4.1 nano (paid, smartest, 1M context)
Legacy single-provider mode still works via LLM_PROVIDER env var.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Supabase ───────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# ── Scraper settings (shared, provider-independent) ──────
REQUEST_TIMEOUT = 20
MAX_RETRIES = 2

# ── Async HTTP / concurrency settings ───────────────────
# Connection-pool ceiling for the shared httpx.AsyncClient.
MAX_HTTP_CONNECTIONS = 300
MAX_KEEPALIVE_CONNECTIONS = 150
# Concurrent per-job description/question enrichment tasks.
ENRICH_CONCURRENCY = 40
QUESTION_ENRICH_CONCURRENCY = 15
# Concurrent AI classification batches (rate limiter still enforces spacing).
ROLE_BATCH_CONCURRENCY = 12
LOCATION_BATCH_CONCURRENCY = 6


# ── Multi-provider configuration ─────────────────────────
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
    _make_provider(
        "cerebras", "CEREBRAS_API_KEY", "llama-3.3-70b",
        "https://api.cerebras.ai/v1",
        max_batch_chars=6_000,       # 8K context — keep small
        min_call_interval=12.5,      # ~5 req/min free tier
    ),
    _make_provider(
        "groq", "GROQ_API_KEY", "llama-3.3-70b-versatile",
        "https://api.groq.com/openai/v1",
        max_batch_chars=100_000,     # 128K context, ~100K safe
    ),
    _make_provider(
        "gemini", "GEMINI_API_KEY", "gemini-2.0-flash",
        "https://generativelanguage.googleapis.com/v1beta/openai/",
        max_batch_chars=400_000,     # 1M context
    ),
]
ROLE_PROVIDERS = [p for p in _ROLE_PROVIDER_DEFS if p is not None]

# ── Location classification provider (paid, smartest) ──
LOCATION_PROVIDER = _make_provider(
    "openai", "OPENAI_API_KEY", "gpt-4.1-nano",
    "https://api.openai.com/v1",
    max_batch_chars=300_000,
    min_call_interval=5.0,           # Tier 1: ~12 req/min
)

# ── Legacy single-provider fallback ──────────────────────
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "cerebras").lower()

if not ROLE_PROVIDERS:
    if LLM_PROVIDER == "cerebras":
        LLM_API_KEY = os.environ["CEREBRAS_API_KEY"]
        LLM_MODEL = "llama-3.3-70b"
        LLM_BASE_URL = "https://api.cerebras.ai/v1"
    elif LLM_PROVIDER == "groq":
        LLM_API_KEY = os.environ["GROQ_API_KEY"]
        LLM_MODEL = "llama-3.3-70b-versatile"
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
    LLM_API_KEY = ROLE_PROVIDERS[0]["api_key"]
    LLM_MODEL = ROLE_PROVIDERS[0]["model"]
    LLM_BASE_URL = ROLE_PROVIDERS[0]["base_url"]

if not LOCATION_PROVIDER:
    LOCATION_PROVIDER = {
        "name": LLM_PROVIDER,
        "api_key": LLM_API_KEY,
        "model": LLM_MODEL,
        "base_url": LLM_BASE_URL,
        "max_batch_chars": 300_000 if LLM_PROVIDER != "cerebras" else 6_000,
        "min_call_interval": 0.0,
    }

AI_PARALLEL_REQUESTS = 1
AI_BATCH_SIZE = 25  # legacy, not used by char-based batching
