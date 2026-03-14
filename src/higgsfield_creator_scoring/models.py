from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class CategoryConfig:
    name: str
    weight: float
    queries: list[str]
    keywords: list[str]
    negative_keywords: list[str] = field(default_factory=list)


@dataclass
class AppConfig:
    project_name: str
    results_per_query: int
    recent_videos_to_check: int
    recent_comments_per_video: int
    use_comment_analysis: bool
    include_channel_search: bool
    min_subscribers: int
    max_total_candidates: int
    max_candidates_per_category: int
    search_order: str
    max_retries: int
    retry_backoff_seconds: int
    audience_signal_threshold_high: int
    workflow_signal_threshold_high: int
    comment_tool_intent_threshold_high: int
    uploads_30d_consistency_threshold: int
    product_feature_signal_threshold_high: int
    creative_replication_comment_threshold_high: int
    premium_aesthetic_signal_threshold_high: int
    demoability_threshold_high: int
    weights: dict[str, float]
    bonuses: dict[str, float]
    penalties: dict[str, float]
    keyword_dictionaries: dict[str, list[str]]
    categories: list[CategoryConfig]


@dataclass
class RawDiscoveryRow:
    timestamp: str
    query: str
    category: str
    result_type: str
    video_title: str
    video_url: str
    channel_name: str
    channel_id: str
    source_rank: int

    def to_sheet_row(self) -> list[Any]:
        return [
            self.timestamp,
            self.query,
            self.category,
            self.result_type,
            self.video_title,
            self.video_url,
            self.channel_name,
            self.channel_id,
            self.source_rank,
        ]


@dataclass
class VideoRecord:
    video_id: str
    title: str
    description: str
    url: str
    views: int
    comment_count: int
    published_at: datetime


@dataclass
class CreatorRecord:
    channel_id: str
    channel_name: str
    channel_url: str
    description: str
    primary_category: str
    matched_categories: set[str] = field(default_factory=set)
    matched_queries: set[str] = field(default_factory=set)
    subscribers: int = 0
    total_video_count: int = 0
    total_view_count: int = 0
    published_at: str = ""
    custom_url: str = ""
    country: str = ""
    recent_uploads_checked: int = 0
    recent_video_titles: list[str] = field(default_factory=list)
    recent_video_descriptions: list[str] = field(default_factory=list)
    recent_video_urls: list[str] = field(default_factory=list)
    recent_video_views: list[int] = field(default_factory=list)
    recent_video_publish_dates: list[str] = field(default_factory=list)
    recent_video_comment_counts: list[int] = field(default_factory=list)
    avg_views_recent_10: float = 0.0
    median_views_recent_10: float = 0.0
    avg_views_recent_20: float = 0.0
    uploads_last_30_days: int = 0
    uploads_last_90_days: int = 0
    days_since_last_upload: int = 9999
    views_to_sub_ratio: float = 0.0
    median_views_to_sub_ratio: float = 0.0
    audience_signal_count: int = 0
    monetization_signal_count: int = 0
    workflow_signal_count: int = 0
    tool_signal_count: int = 0
    higgsfield_specific_signal_count: int = 0
    premium_aesthetic_signal_count: int = 0
    comment_tool_intent_count: int = 0
    creative_replication_intent_count: int = 0
    comment_creator_intent_count: int = 0
    comment_business_intent_count: int = 0
    audience_fit_score: float = 0.0
    monetization_focus_score: float = 0.0
    tool_relevance_score: float = 0.0
    higgsfield_specific_fit_score: float = 0.0
    engagement_quality_score: float = 0.0
    posting_frequency_score: float = 0.0
    demoability_score: float = 0.0
    premium_aesthetic_fit_score: float = 0.0
    bonus_score: float = 0.0
    penalty_score: float = 0.0
    normalized_score: float = 0.0
    priority_tier: str = "D"
    fit_label: str = "weak_fit"
    fit_comment: str = ""
    disqualify_reason: str | None = None
    example_video_1: str = ""
    example_video_2: str = ""
    last_updated_at: str = ""
    recent_videos: list[VideoRecord] = field(default_factory=list)

    def to_master_sheet_row(self) -> list[Any]:
        return [
            self.channel_name,
            self.channel_url,
            self.primary_category,
            " | ".join(sorted(self.matched_categories)),
            " | ".join(sorted(self.matched_queries)),
            self.subscribers,
            int(self.median_views_recent_10),
            round(self.median_views_to_sub_ratio, 4),
            self.uploads_last_30_days,
            self.audience_signal_count,
            self.monetization_signal_count,
            self.workflow_signal_count,
            self.tool_signal_count,
            self.comment_tool_intent_count,
            self.comment_creator_intent_count,
            self.comment_business_intent_count,
            self.higgsfield_specific_signal_count,
            self.premium_aesthetic_signal_count,
            self.creative_replication_intent_count,
            round(self.audience_fit_score, 2),
            round(self.monetization_focus_score, 2),
            round(self.tool_relevance_score, 2),
            round(self.higgsfield_specific_fit_score, 2),
            round(self.engagement_quality_score, 2),
            round(self.posting_frequency_score, 2),
            round(self.demoability_score, 2),
            round(self.premium_aesthetic_fit_score, 2),
            round(self.bonus_score, 2),
            round(self.penalty_score, 2),
            round(self.normalized_score, 2),
            self.priority_tier,
            self.fit_label,
            self.fit_comment,
            self.disqualify_reason or "",
        ]
