"""
Configuration — unified for all LLM providers.
Switch provider by setting LLM_PROVIDER env var:
    LLM_PROVIDER=anthropic  →  Claude Haiku 4.5 (with prompt caching)
    LLM_PROVIDER=groq       →  Llama 3.3 70B (free tier)
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── LLM Provider Switch ──────────────────────────────────
# This is the ONE LINE you change (or set in GitHub Secrets / .env)
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "groq").lower()

# ── Supabase ───────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# ── Provider-specific settings ────────────────────────────
if LLM_PROVIDER == "anthropic":
    LLM_API_KEY = os.environ["ANTHROPIC_API_KEY"]
    LLM_MODEL = "claude-haiku-4-5-20251001"
elif LLM_PROVIDER == "groq":
    LLM_API_KEY = os.environ["GROQ_API_KEY"]
    LLM_MODEL = "llama-3.3-70b-versatile"
else:
    raise ValueError(f"Unknown LLM_PROVIDER: {LLM_PROVIDER!r}. Use 'anthropic' or 'groq'.")

# ── Scraper settings (shared, provider-independent) ──────
REQUEST_TIMEOUT = 20
MAX_RETRIES = 2
CONCURRENT_WORKERS = 8        # legacy fallback; main.py uses per-platform limits
AI_BATCH_SIZE = 25            # jobs per AI classification call
