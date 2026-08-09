"""
Configuration — GROQ VARIANT
Swap this into config.py to use Groq (free LLM) for classification.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Supabase ───────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]  # service_role key (server-side only)

# ── Groq (free LLM for classification) ─────────────────
GROQ_API_KEY = os.environ["GROQ_API_KEY"]
GROQ_MODEL = "llama-3.3-70b-versatile"

# ── Scraper settings ────────────────────────────────────
REQUEST_TIMEOUT = 20
MAX_RETRIES = 2
CONCURRENT_WORKERS = 8        # legacy fallback; main.py uses per-platform limits
AI_BATCH_SIZE = 25            # jobs per AI location-filter call
SLUGS_DIR = os.path.join(os.path.dirname(__file__), "slugs")
