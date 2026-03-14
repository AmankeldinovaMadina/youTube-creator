from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dateutil.parser import isoparse

from src.higgsfield_creator_scoring.commentary_generator import CommentaryGenerator
from src.higgsfield_creator_scoring.config_loader import load_config
from src.higgsfield_creator_scoring.enrichment import enrich_creators
from src.higgsfield_creator_scoring.logging_utils import configure_logging
from src.higgsfield_creator_scoring.models import CreatorRecord, VideoRecord
from src.higgsfield_creator_scoring.scoring import score_creators
from src.higgsfield_creator_scoring.sheets_writer import SheetsWriter

logger = logging.getLogger(__name__)


class YouTubeDataApiClient:
    BASE_URL = "https://www.googleapis.com/youtube/v3"

    def __init__(self, api_key: str, max_retries: int = 3, retry_backoff_seconds: int = 2) -> None:
        self.api_key = api_key
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        query = dict(params)
        query["key"] = self.api_key
        url = f"{self.BASE_URL}/{path}?{urlencode(query, doseq=True)}"

        for attempt in range(1, self.max_retries + 1):
            try:
                req = Request(url, headers={"Accept": "application/json"})
                with urlopen(req, timeout=60) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except Exception as exc:
                if attempt == self.max_retries:
                    raise
                sleep_for = self.retry_backoff_seconds * attempt
                logger.warning(
                    "YouTube API transient error: %s; retrying in %ss",
                    exc,
                    sleep_for,
                )
                time.sleep(sleep_for)
        return {}

    def get_channels(self, channel_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not channel_ids:
            return {}

        out: dict[str, dict[str, Any]] = {}
        for i in range(0, len(channel_ids), 50):
            batch = channel_ids[i : i + 50]
            data = self._get(
                "channels",
                {
                    "part": "snippet,statistics,contentDetails",
                    "id": ",".join(batch),
                    "maxResults": len(batch),
                },
            )
            for item in data.get("items", []):
                channel_id = item.get("id", "")
                if channel_id:
                    out[channel_id] = item
        return out

    def get_recent_videos(self, channel: dict[str, Any], limit: int) -> list[VideoRecord]:
        channel_id = channel.get("id", "")
        if not channel_id:
            return []

        search_data = self._get(
            "search",
            {
                "part": "snippet",
                "channelId": channel_id,
                "order": "date",
                "type": "video",
                "maxResults": min(max(limit, 1), 50),
            },
        )
        video_ids = [
            item.get("id", {}).get("videoId", "")
            for item in search_data.get("items", [])
            if item.get("id", {}).get("videoId")
        ]
        if not video_ids:
            return []

        details = self._get(
            "videos",
            {
                "part": "snippet,statistics",
                "id": ",".join(video_ids),
                "maxResults": len(video_ids),
            },
        )
        items_by_id = {
            item.get("id", ""): item for item in details.get("items", []) if item.get("id")
        }

        videos: list[VideoRecord] = []
        for video_id in video_ids:
            item = items_by_id.get(video_id)
            if not item:
                continue
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            published_raw = snippet.get("publishedAt")
            if not published_raw:
                continue
            try:
                published_at = isoparse(published_raw).astimezone(timezone.utc)
            except Exception:
                continue
            videos.append(
                VideoRecord(
                    video_id=video_id,
                    title=snippet.get("title", ""),
                    description=snippet.get("description", ""),
                    url=f"https://www.youtube.com/watch?v={video_id}",
                    views=int(stats.get("viewCount", 0) or 0),
                    comment_count=int(stats.get("commentCount", 0) or 0),
                    published_at=published_at,
                )
            )
        videos.sort(key=lambda v: v.published_at, reverse=True)
        return videos[:limit]

    def get_recent_comments(self, video_id: str, max_comments: int) -> list[str]:
        if max_comments <= 0:
            return []
        comments: list[str] = []
        page_token = ""
        while len(comments) < max_comments:
            params: dict[str, Any] = {
                "part": "snippet",
                "videoId": video_id,
                "maxResults": min(100, max_comments - len(comments)),
                "order": "relevance",
                "textFormat": "plainText",
            }
            if page_token:
                params["pageToken"] = page_token
            try:
                data = self._get("commentThreads", params)
            except Exception:
                return comments

            for item in data.get("items", []):
                text = (
                    item.get("snippet", {})
                    .get("topLevelComment", {})
                    .get("snippet", {})
                    .get("textDisplay", "")
                )
                if text:
                    comments.append(text)
                if len(comments) >= max_comments:
                    break

            page_token = data.get("nextPageToken", "")
            if not page_token:
                break
        return comments


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build creator_master_2 from raw_discovery_uniques using YouTube Data API"
    )
    parser.add_argument(
        "--config", default="config/default_config.yaml", help="Path to YAML config"
    )
    parser.add_argument("--log-level", default="INFO", help="Log level")
    parser.add_argument(
        "--source-tab", default="raw_discovery_uniques", help="Source Google Sheets tab"
    )
    parser.add_argument(
        "--output-tab", default="creator_master_2", help="Destination Google Sheets tab"
    )
    return parser.parse_args()


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def _header_index(header: list[str]) -> dict[str, int]:
    return {name.strip(): idx for idx, name in enumerate(header)}


def load_yes_creators_from_uniques(
    sheets: SheetsWriter, tab_name: str
) -> tuple[dict[str, CreatorRecord], int]:
    ws = sheets.sheet.worksheet(tab_name)
    rows = ws.get_all_values()
    if len(rows) <= 1:
        return {}, 0

    idx = _header_index(rows[0])
    required_columns = [
        "query",
        "category",
        "video_url",
        "channel_name",
        "channel_id",
        "search?",
    ]
    missing = [name for name in required_columns if name not in idx]
    if missing:
        raise ValueError(f"Missing required columns in {tab_name}: {missing}")

    creators: OrderedDict[str, CreatorRecord] = OrderedDict()
    kept_rows = 0
    for row in rows[1:]:
        if len(row) <= idx["search?"]:
            continue
        if row[idx["search?"]].strip().lower() != "yes":
            continue
        channel_id = row[idx["channel_id"]].strip()
        if not channel_id:
            continue
        kept_rows += 1
        category = row[idx["category"]].strip()
        query = row[idx["query"]].strip()
        channel_name = row[idx["channel_name"]].strip()
        video_url = row[idx["video_url"]].strip()

        if channel_id not in creators:
            creators[channel_id] = CreatorRecord(
                channel_id=channel_id,
                channel_name=channel_name,
                channel_url=f"https://www.youtube.com/channel/{channel_id}",
                description="",
                primary_category=category,
            )

        creator = creators[channel_id]
        creator.matched_categories.add(category)
        creator.matched_queries.add(query)
        if not creator.example_video_1 and video_url:
            creator.example_video_1 = video_url
        elif (
            creator.example_video_1
            and not creator.example_video_2
            and video_url
            and video_url != creator.example_video_1
        ):
            creator.example_video_2 = video_url

    return dict(creators), kept_rows


def write_creators_to_tab(
    sheets: SheetsWriter, tab_name: str, creators: list[CreatorRecord]
) -> None:
    rows = [
        [
            "channel_name",
            "channel_url",
            "primary_category",
            "matched_categories",
            "matched_queries",
            "subscribers",
            "median_views_recent_10",
            "median_views_to_sub_ratio",
            "uploads_last_30_days",
            "audience_signal_count",
            "monetization_signal_count",
            "workflow_signal_count",
            "tool_signal_count",
            "comment_tool_intent_count",
            "comment_creator_intent_count",
            "comment_business_intent_count",
            "higgsfield_specific_signal_count",
            "premium_aesthetic_signal_count",
            "creative_replication_intent_count",
            "audience_fit_score",
            "monetization_focus_score",
            "tool_relevance_score",
            "higgsfield_specific_fit_score",
            "engagement_quality_score",
            "posting_frequency_score",
            "demoability_score",
            "premium_aesthetic_fit_score",
            "bonus_score",
            "penalty_score",
            "normalized_score",
            "priority_tier",
            "fit_label",
            "fit_comment",
            "disqualify_reason",
        ]
    ]
    rows.extend(creator.to_master_sheet_row() for creator in creators)
    sheets._upsert_tab(tab_name, rows)


def run_pipeline_from_uniques(
    config_path: str,
    youtube_api_key: str,
    openai_api_key: str | None,
    spreadsheet_id: str,
    gcp_service_account_json: str,
    source_tab: str,
    output_tab: str,
    log_level: str = "INFO",
) -> dict[str, Any]:
    configure_logging(log_level)
    config = load_config(config_path)
    yt = YouTubeDataApiClient(
        api_key=youtube_api_key,
        max_retries=config.max_retries,
        retry_backoff_seconds=config.retry_backoff_seconds,
    )
    commentator = CommentaryGenerator(api_key=openai_api_key)
    sheets = SheetsWriter(
        spreadsheet_id=spreadsheet_id,
        service_account_json_path=gcp_service_account_json,
    )

    creators_by_channel, kept_rows = load_yes_creators_from_uniques(sheets, source_tab)
    logger.info(
        "loaded raw_discovery_uniques | kept_rows=%s unique_channels=%s",
        kept_rows,
        len(creators_by_channel),
    )

    def checkpoint(creators_snapshot: list[CreatorRecord], channel_id: str) -> None:
        scored_snapshot = score_creators(list(creators_snapshot), config)
        for creator in scored_snapshot:
            if not creator.fit_comment:
                result = commentator.generate(creator)
                creator.fit_label = result["fit_label"]
                creator.fit_comment = result["comment"]
        scored_snapshot.sort(key=lambda c: c.normalized_score, reverse=True)
        write_creators_to_tab(sheets, output_tab, scored_snapshot)
        logger.info(
            "checkpoint written | output_tab=%s | channel_id=%s | creators=%s",
            output_tab,
            channel_id,
            len(scored_snapshot),
        )

    enriched = enrich_creators(
        yt=yt,
        config=config,
        creators=creators_by_channel,
        existing_enriched=[],
        completed_channel_ids=set(),
        on_channel_completed=checkpoint,
    )

    scored = score_creators(enriched, config)
    for creator in scored:
        if not creator.fit_comment:
            result = commentator.generate(creator)
            creator.fit_label = result["fit_label"]
            creator.fit_comment = result["comment"]

    scored.sort(key=lambda c: c.normalized_score, reverse=True)
    write_creators_to_tab(sheets, output_tab, scored)

    return {
        "source_tab": source_tab,
        "output_tab": output_tab,
        "source_rows_yes": kept_rows,
        "unique_candidates": len(creators_by_channel),
        "written_creators": len(scored),
    }


def main() -> int:
    args = parse_args()
    try:
        summary = run_pipeline_from_uniques(
            config_path=args.config,
            youtube_api_key=required_env("YOUTUBE_API_KEY"),
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            spreadsheet_id=required_env("GOOGLE_SHEETS_SPREADSHEET_ID"),
            gcp_service_account_json=required_env("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"),
            source_tab=args.source_tab,
            output_tab=args.output_tab,
            log_level=args.log_level,
        )
        print("Run complete:", summary)
        return 0
    except Exception as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
