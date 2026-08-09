"""
Configuration — ANTHROPIC VARIANT
Swap this into config.py to use Anthropic Claude for classification.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Supabase ───────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]  # service_role key (server-side only)

# ── Anthropic (Claude for classification) ──────────────
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"

# ── Scraper settings ────────────────────────────────────
REQUEST_TIMEOUT = 20
MAX_RETRIES = 2
CONCURRENT_WORKERS = 8        # legacy fallback; main.py uses per-platform limits
AI_BATCH_SIZE = 25            # jobs per AI location-filter call
SLUGS_DIR = os.path.join(os.path.dirname(__file__), "slugs")
