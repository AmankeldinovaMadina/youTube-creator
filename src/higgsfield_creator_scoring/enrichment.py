from __future__ import annotations

import logging
from datetime import datetime, timezone
from statistics import mean, median
from typing import Callable

from .models import AppConfig, CreatorRecord
from .signals import extract_signal_counts
from .youtube_client import YouTubeClient, iso_now

logger = logging.getLogger(__name__)


def enrich_creators(
    yt: YouTubeClient,
    config: AppConfig,
    creators: dict[str, CreatorRecord],
    existing_enriched: list[CreatorRecord] | None = None,
    completed_channel_ids: set[str] | None = None,
    on_channel_completed: Callable[[list[CreatorRecord], str], None] | None = None,
) -> list[CreatorRecord]:
    channel_cache: dict[str, dict] = {}
    completed_channels = set(completed_channel_ids or set())
    enriched_by_channel = {
        creator.channel_id: creator for creator in (existing_enriched or [])
    }

    channel_ids = [
        channel_id for channel_id in creators.keys() if channel_id not in completed_channels
    ]
    channel_data = yt.get_channels(channel_ids)
    channel_cache.update(channel_data)

    failures = 0

    for channel_id, creator in creators.items():
        if channel_id in completed_channels:
            logger.info(
                "enrichment skipped | channel_id=%s | reason=resume_checkpoint",
                channel_id,
            )
            continue
        try:
            channel = channel_cache.get(channel_id)
            if not channel:
                continue
            _fill_channel_fields(creator, channel)
            if creator.subscribers < config.min_subscribers:
                continue

            videos = yt.get_recent_videos(
                channel, limit=max(20, config.recent_videos_to_check)
            )
            creator.recent_videos = videos
            _fill_recent_metrics(creator)

            comments: list[str] = []
            if config.use_comment_analysis:
                for video in videos[: config.recent_videos_to_check]:
                    comments.extend(
                        yt.get_recent_comments(
                            video.video_id,
                            max_comments=config.recent_comments_per_video,
                        )
                    )

            titles = [v.title for v in videos[: config.recent_videos_to_check]]
            descriptions = [
                v.description for v in videos[: config.recent_videos_to_check]
            ]
            signal_counts = extract_signal_counts(
                titles=titles,
                descriptions=descriptions,
                comments=comments,
                keyword_dictionaries=config.keyword_dictionaries,
            )
            creator.audience_signal_count = signal_counts["audience_signal_count"]
            creator.monetization_signal_count = signal_counts[
                "monetization_signal_count"
            ]
            creator.workflow_signal_count = signal_counts["workflow_signal_count"]
            creator.tool_signal_count = signal_counts["tool_signal_count"]
            creator.higgsfield_specific_signal_count = signal_counts[
                "higgsfield_specific_signal_count"
            ]
            creator.premium_aesthetic_signal_count = signal_counts[
                "premium_aesthetic_signal_count"
            ]
            creator.comment_tool_intent_count = signal_counts[
                "comment_tool_intent_count"
            ]
            creator.creative_replication_intent_count = signal_counts[
                "creative_replication_intent_count"
            ]
            creator.comment_creator_intent_count = signal_counts[
                "comment_creator_intent_count"
            ]
            creator.comment_business_intent_count = signal_counts[
                "comment_business_intent_count"
            ]
            creator.last_updated_at = iso_now()

            enriched_by_channel[channel_id] = creator
            completed_channels.add(channel_id)
            if on_channel_completed:
                on_channel_completed(list(enriched_by_channel.values()), channel_id)
        except Exception:
            failures += 1
            logger.exception("failed enrichment | channel_id=%s", channel_id)

    enriched = list(enriched_by_channel.values())
    logger.info("channels enriched=%s failures=%s", len(enriched), failures)
    return enriched


def _fill_channel_fields(creator: CreatorRecord, channel: dict) -> None:
    snippet = channel.get("snippet", {})
    stats = channel.get("statistics", {})

    creator.description = snippet.get("description", "")
    creator.channel_name = snippet.get("title", creator.channel_name)
    creator.channel_url = f"https://www.youtube.com/channel/{creator.channel_id}"
    creator.custom_url = snippet.get("customUrl", "")
    creator.subscribers = int(stats.get("subscriberCount", 0) or 0)
    creator.total_video_count = int(stats.get("videoCount", 0) or 0)
    creator.total_view_count = int(stats.get("viewCount", 0) or 0)
    creator.published_at = snippet.get("publishedAt", "")
    creator.country = snippet.get("country", "")


def _fill_recent_metrics(creator: CreatorRecord) -> None:
    videos = creator.recent_videos
    creator.recent_uploads_checked = len(videos)
    creator.recent_video_titles = [v.title for v in videos]
    creator.recent_video_descriptions = [v.description for v in videos]
    creator.recent_video_urls = [v.url for v in videos]
    creator.recent_video_views = [v.views for v in videos]
    creator.recent_video_publish_dates = [v.published_at.isoformat() for v in videos]
    creator.recent_video_comment_counts = [v.comment_count for v in videos]
    if not videos:
        creator.avg_views_recent_10 = 0
        creator.avg_views_recent_20 = 0
        creator.median_views_recent_10 = 0
        creator.days_since_last_upload = 9999
        creator.views_to_sub_ratio = 0.0
        return

    views_10 = [v.views for v in videos[:10]]
    views_20 = [v.views for v in videos[:20]]

    creator.avg_views_recent_10 = float(mean(views_10)) if views_10 else 0.0
    creator.median_views_recent_10 = float(median(views_10)) if views_10 else 0.0
    creator.avg_views_recent_20 = (
        float(mean(views_20)) if views_20 else creator.avg_views_recent_10
    )

    now = datetime.now(timezone.utc)
    creator.uploads_last_30_days = sum(
        1 for v in videos if (now - v.published_at).days <= 30
    )
    creator.uploads_last_90_days = sum(
        1 for v in videos if (now - v.published_at).days <= 90
    )
    creator.days_since_last_upload = (now - videos[0].published_at).days
    creator.views_to_sub_ratio = creator.avg_views_recent_10 / max(
        creator.subscribers, 1
    )
    creator.median_views_to_sub_ratio = creator.median_views_recent_10 / max(
        creator.subscribers, 1
    )
