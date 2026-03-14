from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from apify_client import ApifyClient
from dateutil.parser import isoparse

from .models import AppConfig, VideoRecord

logger = logging.getLogger(__name__)


class YouTubeClient:
    SEARCH_ACTOR_ID = "streamers/youtube-scraper"
    COMMENTS_ACTOR_ID = "streamers/youtube-comments-scraper"

    def __init__(self, apify_token: str, config: AppConfig) -> None:
        self.config = config
        self.client = ApifyClient(apify_token)

    def _with_retry(self, fn, *args, **kwargs):
        for attempt in range(1, self.config.max_retries + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                if attempt == self.config.max_retries:
                    raise
                sleep_for = self.config.retry_backoff_seconds * attempt
                logger.warning(
                    "Apify transient error: %s; retrying in %ss", exc, sleep_for
                )
                time.sleep(sleep_for)

    def _run_actor_items(
        self, actor_id: str, run_input: dict[str, Any]
    ) -> list[dict[str, Any]]:
        def _call() -> list[dict[str, Any]]:
            run = self.client.actor(actor_id).call(run_input=run_input)
            dataset_id = run.get("defaultDatasetId") if run else None
            if not dataset_id:
                return []
            return list(self.client.dataset(dataset_id).iterate_items())

        return self._with_retry(_call)

    @staticmethod
    def _search_item_to_discovery_row(item: dict[str, Any]) -> dict[str, Any]:
        channel_id = item.get("channelId", "")
        channel_name = item.get("channelName", "")
        video_id = item.get("id", "")
        return {
            "id": {"videoId": video_id, "channelId": channel_id},
            "snippet": {
                "title": item.get("title", ""),
                "description": item.get("text", "") or item.get("description", ""),
                "channelId": channel_id,
                "channelTitle": channel_name,
            },
        }

    def search_videos(self, query: str, max_results: int) -> list[dict[str, Any]]:
        items = self._run_actor_items(
            self.SEARCH_ACTOR_ID,
            {
                "searchKeywords": query,
                "maxResults": max_results,
            },
        )
        videos = [item for item in items if item.get("type") == "video"]
        return [self._search_item_to_discovery_row(item) for item in videos]

    def search_channels(self, query: str, max_results: int) -> list[dict[str, Any]]:
        items = self._run_actor_items(
            self.SEARCH_ACTOR_ID,
            {
                "searchKeywords": query,
                "maxResults": max_results,
            },
        )

        channels_by_id: dict[str, dict[str, Any]] = {}
        for item in items:
            channel_id = item.get("channelId", "")
            if not channel_id or channel_id in channels_by_id:
                continue
            channels_by_id[channel_id] = {
                "id": {"channelId": channel_id},
                "snippet": {
                    "title": item.get("channelName", ""),
                    "description": item.get("channelDescription", "")
                    or item.get("text", ""),
                    "channelId": channel_id,
                    "channelTitle": item.get("channelName", ""),
                },
            }
        return list(channels_by_id.values())[:max_results]

    def get_channels(self, channel_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not channel_ids:
            return {}
        out: dict[str, dict[str, Any]] = {}
        for i in range(0, len(channel_ids), 20):
            batch = channel_ids[i : i + 20]

            items = self._run_actor_items(
                self.SEARCH_ACTOR_ID,
                {
                    "startUrls": [
                        {"url": f"https://www.youtube.com/channel/{channel_id}"}
                        for channel_id in batch
                    ],
                    "maxResults": 1,
                },
            )

            for item in items:
                channel_id = item.get("channelId", "")
                if not channel_id:
                    continue
                out[channel_id] = {
                    "id": channel_id,
                    "snippet": {
                        "title": item.get("channelName", ""),
                        "description": item.get("channelDescription", ""),
                        "publishedAt": item.get("channelJoinedDate", ""),
                        "country": item.get("channelLocation", ""),
                        "customUrl": item.get("channelUsername", ""),
                    },
                    "statistics": {
                        "subscriberCount": int(item.get("numberOfSubscribers", 0) or 0),
                        "videoCount": int(item.get("channelTotalVideos", 0) or 0),
                        "viewCount": int(item.get("channelTotalViews", 0) or 0),
                    },
                    "contentDetails": {
                        "relatedPlaylists": {"uploads": f"channel:{channel_id}"}
                    },
                }
        return out

    def get_recent_videos(
        self, channel: dict[str, Any], limit: int
    ) -> list[VideoRecord]:
        channel_id = channel.get("id")
        if not channel_id:
            return []

        items = self._run_actor_items(
            self.SEARCH_ACTOR_ID,
            {
                "startUrls": [
                    {"url": f"https://www.youtube.com/channel/{channel_id}/videos"}
                ],
                "maxResults": limit,
            },
        )

        videos: list[VideoRecord] = []
        for item in items:
            video_id = str(item.get("id", "") or "")
            published_raw = item.get("date")
            if not video_id or not published_raw:
                continue

            try:
                published_at = isoparse(published_raw).astimezone(timezone.utc)
            except Exception:
                continue

            videos.append(
                VideoRecord(
                    video_id=video_id,
                    title=item.get("title", ""),
                    description=item.get("text", "") or "",
                    url=item.get("url", f"https://www.youtube.com/watch?v={video_id}"),
                    views=int(item.get("viewCount", 0) or 0),
                    comment_count=int(item.get("commentsCount", 0) or 0),
                    published_at=published_at,
                )
            )

        videos.sort(key=lambda v: v.published_at, reverse=True)
        return videos[:limit]

    def get_recent_comments(self, video_id: str, max_comments: int) -> list[str]:
        try:
            items = self._run_actor_items(
                self.COMMENTS_ACTOR_ID,
                {
                    "startUrls": [
                        {"url": f"https://www.youtube.com/watch?v={video_id}"}
                    ],
                    "maxComments": max_comments,
                },
            )
        except Exception:
            return []

        comments: list[str] = []
        for item in items:
            text = (
                item.get("text")
                or item.get("comment")
                or item.get("commentText")
                or item.get("content")
            )
            if text:
                comments.append(str(text))
            if len(comments) >= max_comments:
                break
        return comments


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()
