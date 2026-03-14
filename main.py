from __future__ import annotations

import argparse
import os
import sys

from src.higgsfield_creator_scoring.pipeline import run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Higgsfield creator scoring pipeline")
    parser.add_argument(
        "--config", default="config/default_config.yaml", help="Path to YAML config"
    )
    parser.add_argument("--log-level", default="INFO", help="Log level")
    return parser.parse_args()


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def main() -> int:
    args = parse_args()
    try:
        summary = run_pipeline(
            config_path=args.config,
            apify_token=required_env("APIFY"),
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            spreadsheet_id=required_env("GOOGLE_SHEETS_SPREADSHEET_ID"),
            gcp_service_account_json=required_env("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"),
            log_level=args.log_level,
        )
        print("Run complete:", summary)
        return 0
    except Exception as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
