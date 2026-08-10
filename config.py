"""
Configuration — unified for all LLM providers.
Switch provider by setting LLM_PROVIDER env var:
    LLM_PROVIDER=cerebras   →  Llama 3.3 70B on Cerebras (free tier, 1M tokens/day)
    LLM_PROVIDER=groq       →  Llama 3.3 70B on Groq (free tier)
    LLM_PROVIDER=anthropic  →  Claude Haiku 4.5 (with prompt caching)
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
else:
    raise ValueError(f"Unknown LLM_PROVIDER: {LLM_PROVIDER!r}. Use 'cerebras', 'groq', or 'anthropic'.")

# ── Scraper settings (shared, provider-independent) ──────
REQUEST_TIMEOUT = 20
MAX_RETRIES = 2
CONCURRENT_WORKERS = 8        # legacy fallback; main.py uses per-platform limits
AI_BATCH_SIZE = 3             # jobs per AI classification call (small to fit full JDs in context)
