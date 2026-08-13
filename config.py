"""
Configuration — unified for all LLM providers.
Switch provider by setting LLM_PROVIDER env var:
    LLM_PROVIDER=cerebras   →  Llama 3.3 70B on Cerebras (free tier, 1M tokens/day)
    LLM_PROVIDER=groq       →  Llama 3.3 70B on Groq (free tier)
    LLM_PROVIDER=anthropic  →  Claude Haiku 4.5 (with prompt caching)
    LLM_PROVIDER=openai     →  GPT-4o mini (auto prompt caching, 128K context)
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── LLM Provider Switch ──────────────────────────────────
# This is the ONE LINE you change (or set in GitHub Secrets / .env)
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "cerebras").lower()

# ── Supabase ───────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# ── Provider-specific settings ────────────────────────────
if LLM_PROVIDER == "cerebras":
    LLM_API_KEY = os.environ["CEREBRAS_API_KEY"]
    LLM_MODEL = "gpt-oss-120b"    # $0.35/M input — cheapest on Cerebras, 120B params
    LLM_BASE_URL = "https://api.cerebras.ai/v1"
elif LLM_PROVIDER == "groq":
    LLM_API_KEY = os.environ["GROQ_API_KEY"]
    LLM_MODEL = "llama-3.3-70b-versatile"
    LLM_BASE_URL = "https://api.groq.com/openai/v1"
elif LLM_PROVIDER == "anthropic":
    LLM_API_KEY = os.environ["ANTHROPIC_API_KEY"]
    LLM_MODEL = "claude-haiku-4-5-20251001"
    LLM_BASE_URL = None  # Anthropic uses its own SDK
elif LLM_PROVIDER == "openai":
    LLM_API_KEY = os.environ["OPENAI_API_KEY"]
    LLM_MODEL = "gpt-4.1-nano"      # $0.10/M input, 1M context, auto prompt caching
    LLM_BASE_URL = "https://api.openai.com/v1"
else:
    raise ValueError(f"Unknown LLM_PROVIDER: {LLM_PROVIDER!r}. Use 'cerebras', 'groq', 'anthropic', or 'openai'.")

# ── Scraper settings (shared, provider-independent) ──────
REQUEST_TIMEOUT = 20
MAX_RETRIES = 2

# ── AI classification settings (provider-aware) ─────────
if LLM_PROVIDER == "cerebras":
    AI_BATCH_SIZE = 3          # 8K context — 3 full JDs per request
    AI_PARALLEL_REQUESTS = 1   # sequential (rate-limited to 30 RPM)
elif LLM_PROVIDER == "openai":
    AI_BATCH_SIZE = 25         # 1M context, 500K char budget → 80% = 400K chars, ~25 JDs fit in MAX_BATCH=50
    AI_PARALLEL_REQUESTS = 1   # sequential — TPM is the bottleneck, not RPM
else:
    AI_BATCH_SIZE = 30         # 128K+ context — 30 jobs per request
    AI_PARALLEL_REQUESTS = 3   # fire 3 requests concurrently (Groq/Haiku)
