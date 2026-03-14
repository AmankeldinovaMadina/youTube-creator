# Higgsfield Creator Scoring

Automated system to discover, enrich, score, classify, and prioritize YouTube creators whose audience is likely to convert into paid Higgsfield users.

## What this implements

- Category-driven discovery queries (7 strategic categories)
- Raw discovery hit logging and channel dedup by `channel_id`
- Channel + recent upload enrichment (10-20 videos)
- Signal extraction across titles/descriptions/comments
- Scoring engine with base dimensions, bonuses, penalties, and tiering
- Fit labeling (`strong_fit`, `medium_fit`, `weak_fit`, `disqualify`)
- OpenAI-powered 1-3 sentence fit comment (with fallback)
- Google Sheets output tabs:
  - `category_config`
  - `raw_discovery`
  - `creator_master`

## Setup

1. Create virtual environment and install deps:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Copy env vars and fill values:

```bash
cp .env.example .env
```

3. Export env vars from `.env` (or use your preferred env loader).

Required environment variables:

- `APIFY` (Apify API token)
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON_PATH`

Optional:

- `OPENAI_API_KEY` (for commentary generation)

4. Ensure your Google service account has edit access to the target sheet.

## Run

```bash
python main.py --config config/default_config.yaml --log-level INFO
```

## Notes

- If `OPENAI_API_KEY` is missing, the system still runs and uses deterministic fallback commentary.
- Quota-safe defaults are enabled in `config/default_config.yaml` (`include_channel_search: false`, `use_comment_analysis: false`).
- Scoring logic is deterministic and data-first; OpenAI is used only for label/comment support.
- YouTube data is sourced via Apify actors, not the YouTube Data API.
