"""
config_test.py — Test config (identical to config.py but with AI_BATCH_SIZE=1
and no rate throttle). Used by main_test.py for quick AI testing.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── LLM Provider Switch ──────────────────────────────────
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "cerebras").lower()

# ── Supabase ───────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# ── Provider-specific settings ────────────────────────────
if LLM_PROVIDER == "cerebras":
    LLM_API_KEY = os.environ["CEREBRAS_API_KEY"]
    LLM_MODEL = "gpt-oss-120b"
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
    LLM_MODEL = "gpt-4o-mini"
    LLM_BASE_URL = "https://api.openai.com/v1"
else:
    raise ValueError(f"Unknown LLM_PROVIDER: {LLM_PROVIDER!r}. Use 'cerebras', 'groq', 'anthropic', or 'openai'.")

# ── Scraper settings ──────────────────────────────────────
REQUEST_TIMEOUT = 20
MAX_RETRIES = 2

# ── AI classification settings (provider-aware) ─────────
if LLM_PROVIDER == "cerebras":
    AI_BATCH_SIZE = 3
    AI_PARALLEL_REQUESTS = 1
elif LLM_PROVIDER == "openai":
    AI_BATCH_SIZE = 30
    AI_PARALLEL_REQUESTS = 10
else:
    AI_BATCH_SIZE = 30
    AI_PARALLEL_REQUESTS = 3
