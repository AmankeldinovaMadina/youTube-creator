from __future__ import annotations

import logging
from collections import defaultdict
from typing import Callable
from typing import Any

from .models import AppConfig, CategoryConfig, CreatorRecord, RawDiscoveryRow
from .youtube_client import YouTubeClient, iso_now

logger = logging.getLogger(__name__)


def run_discovery(
    yt: YouTubeClient,
    config: AppConfig,
    existing_raw_rows: list[RawDiscoveryRow] | None = None,
    completed_queries: set[tuple[str, str]] | None = None,
    on_query_completed: Callable[[list[RawDiscoveryRow], str, str], None] | None = None,
) -> tuple[dict[str, CreatorRecord], list[RawDiscoveryRow]]:
    creators: dict[str, CreatorRecord] = {}
    raw_rows: list[RawDiscoveryRow] = list(existing_raw_rows or [])
    category_counts: defaultdict[str, int] = defaultdict(int)
    completed_query_keys = set(completed_queries or set())

    if raw_rows:
        _rebuild_state_from_raw_rows(creators, raw_rows, category_counts)

    for category in config.categories:
        logger.info("query started | category=%s", category.name)
        for query in category.queries:
            query_key = (category.name, query)
            if query_key in completed_query_keys:
                logger.info(
                    "query skipped | category=%s | query=%s | reason=resume_checkpoint",
                    category.name,
                    query,
                )
                continue
            if len(creators) >= config.max_total_candidates:
                break
            video_results = yt.search_videos(query, config.results_per_query)
            _ingest_results(
                creators=creators,
                raw_rows=raw_rows,
                category=category,
                query=query,
                items=video_results,
                result_type="video",
                category_counts=category_counts,
                max_candidates_per_category=config.max_candidates_per_category,
            )
            if config.include_channel_search:
                channel_results = yt.search_channels(query, config.results_per_query)
                _ingest_results(
                    creators=creators,
                    raw_rows=raw_rows,
                    category=category,
                    query=query,
                    items=channel_results,
                    result_type="channel",
                    category_counts=category_counts,
                    max_candidates_per_category=config.max_candidates_per_category,
                )
            completed_query_keys.add(query_key)
            if on_query_completed:
                on_query_completed(raw_rows, category.name, query)
        logger.info(
            "query completed | category=%s | candidates=%s",
            category.name,
            category_counts.get(category.name, 0),
        )

    return creators, raw_rows


def _ingest_results(
    creators: dict[str, CreatorRecord],
    raw_rows: list[RawDiscoveryRow],
    category: CategoryConfig,
    query: str,
    items: list[dict[str, Any]],
    result_type: str,
    category_counts: defaultdict[str, int],
    max_candidates_per_category: int,
) -> None:
    for idx, item in enumerate(items, start=1):
        snippet = item.get("snippet", {})
        channel_id = snippet.get("channelId")
        channel_name = snippet.get("channelTitle", "")
        if not channel_id:
            continue
        if category_counts[category.name] >= max_candidates_per_category:
            break

        title = snippet.get("title", "")
        hay = f"{title} {snippet.get('description', '')}".lower()
        if any(neg.lower() in hay for neg in category.negative_keywords):
            continue

        video_id = item.get("id", {}).get("videoId") if result_type == "video" else ""
        video_url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""

        raw_rows.append(
            RawDiscoveryRow(
                timestamp=iso_now(),
                query=query,
                category=category.name,
                result_type=result_type,
                video_title=title,
                video_url=video_url,
                channel_name=channel_name,
                channel_id=channel_id,
                source_rank=idx,
            )
        )

        if channel_id not in creators:
            creators[channel_id] = CreatorRecord(
                channel_id=channel_id,
                channel_name=channel_name,
                channel_url=f"https://www.youtube.com/channel/{channel_id}",
                description="",
                primary_category=category.name,
            )
            category_counts[category.name] += 1

        creator = creators[channel_id]
        creator.matched_categories.add(category.name)
        creator.matched_queries.add(query)
        if not creator.example_video_1 and video_url:
            creator.example_video_1 = video_url
        elif (
            creator.example_video_1
            and not creator.example_video_2
            and video_url != creator.example_video_1
        ):
            creator.example_video_2 = video_url


def _rebuild_state_from_raw_rows(
    creators: dict[str, CreatorRecord],
    raw_rows: list[RawDiscoveryRow],
    category_counts: defaultdict[str, int],
) -> None:
    seen_new_channels: set[tuple[str, str]] = set()
    for row in raw_rows:
        if row.channel_id not in creators:
            creators[row.channel_id] = CreatorRecord(
                channel_id=row.channel_id,
                channel_name=row.channel_name,
                channel_url=f"https://www.youtube.com/channel/{row.channel_id}",
                description="",
                primary_category=row.category,
            )

        creator = creators[row.channel_id]
        creator.matched_categories.add(row.category)
        creator.matched_queries.add(row.query)
        if row.video_url:
            if not creator.example_video_1:
                creator.example_video_1 = row.video_url
            elif (
                creator.example_video_1 != row.video_url
                and not creator.example_video_2
            ):
                creator.example_video_2 = row.video_url

        category_channel_key = (row.category, row.channel_id)
        if category_channel_key not in seen_new_channels:
            category_counts[row.category] += 1
            seen_new_channels.add(category_channel_key)
