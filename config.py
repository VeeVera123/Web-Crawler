"""
Configuration — all secrets come from environment variables.
Local dev: create a .env file (never commit it).
GitHub Actions: store them as repository secrets.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Notion ──────────────────────────────────────────────
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
JOBS_DB_ID = os.environ.get("NOTION_JOBS_DB_ID", "")  # Output: filtered jobs land here

# ── Groq (free LLM for classification) ─────────────────
GROQ_API_KEY = os.environ["GROQ_API_KEY"]
GROQ_MODEL = "llama-3.3-70b-versatile"

# ── Scraper settings ────────────────────────────────────
REQUEST_TIMEOUT = 20
MAX_RETRIES = 2
CONCURRENT_WORKERS = 8        # legacy fallback; main.py uses per-platform limits
AI_BATCH_SIZE = 25            # jobs per AI location-filter call
SLUGS_DIR = os.path.join(os.path.dirname(__file__), "slugs")
