# ATS Global Scanner

Daily automated scanner that sweeps **Rippling, Greenhouse, Lever, and Ashby** job boards for **Customer Success / Account Management** roles that hire **globally or in Africa**.

## How it works

1. **Slug files** (`slugs/*.txt`) list every company board to check on each ATS platform.
2. **Scrapers** hit each platform's public JSON API (no auth needed) and pull all open positions.
3. **Two-stage role filter** — fast keyword regex first, then Groq AI (Llama 3.3 70B) for ambiguous titles.
4. **Two-stage location filter** — keyword match for African countries / "global" / "worldwide", then Groq AI for edge cases.
5. **Notion push** — matching jobs are deduplicated by URL and added to a Notion database.

## Setup

### 1. Notion

Create a database with these properties:

| Property | Type |
|---|---|
| Job Title | Title |
| Company | Rich text |
| Job URL | URL |
| Location | Rich text |
| Department | Rich text |
| ATS | Select |
| Date Added | Date |
| Location Confidence | Select (Match / Uncertain) |
| Salary | Rich text |
| Workplace Type | Rich text |
| Employment Type | Rich text |
| Country | Rich text |

Share the database with your Notion integration.

### 2. Groq

Get a free API key at [console.groq.com](https://console.groq.com).

### 3. GitHub Secrets

Add these repository secrets:

- `NOTION_TOKEN` — your Notion integration token
- `NOTION_JOBS_DB_ID` — the database ID (32-char hex from the URL)
- `GROQ_API_KEY` — your Groq API key

### 4. Run

The workflow runs daily at **7:00 AM WAT** (6:00 UTC). You can also trigger it manually from the Actions tab.

To run locally:

```bash
cp .env.example .env
# fill in your keys
pip install -r requirements.txt
python main.py
```

## Adding companies

Add slugs to the text files in `slugs/`:

- `rippling.txt` — slugs from `ats.rippling.com/{slug}`
- `greenhouse.txt` — tokens from `boards.greenhouse.io/{token}`
- `lever.txt` — slugs from `jobs.lever.co/{slug}`
- `ashby.txt` — slugs from `jobs.ashbyhq.com/{slug}`

One slug per line. Lines starting with `#` are comments.

## Cost

- **ATS APIs**: Free, no auth required
- **Groq (Llama 3.3 70B)**: Free tier (30 req/min, 14,400 req/day)
- **Notion API**: Free
- **GitHub Actions**: Free for public repos, 2,000 min/month for private
