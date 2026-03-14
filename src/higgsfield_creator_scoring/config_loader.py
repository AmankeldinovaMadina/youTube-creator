from __future__ import annotations

from pathlib import Path

import yaml

from .models import AppConfig, CategoryConfig


def load_config(config_path: str) -> AppConfig:
    raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    categories = [CategoryConfig(**item) for item in raw["categories"]]
    return AppConfig(
        project_name=raw["project_name"],
        results_per_query=raw["results_per_query"],
        recent_videos_to_check=raw["recent_videos_to_check"],
        recent_comments_per_video=raw["recent_comments_per_video"],
        use_comment_analysis=raw["use_comment_analysis"],
        include_channel_search=raw.get("include_channel_search", True),
        min_subscribers=raw["min_subscribers"],
        max_total_candidates=raw["max_total_candidates"],
        max_candidates_per_category=raw["max_candidates_per_category"],
        search_order=raw.get("search_order", "sequential"),
        max_retries=raw.get("max_retries", 3),
        retry_backoff_seconds=raw.get("retry_backoff_seconds", 2),
        audience_signal_threshold_high=raw.get("audience_signal_threshold_high", 8),
        workflow_signal_threshold_high=raw.get("workflow_signal_threshold_high", 6),
        comment_tool_intent_threshold_high=raw.get(
            "comment_tool_intent_threshold_high", 8
        ),
        uploads_30d_consistency_threshold=raw.get(
            "uploads_30d_consistency_threshold", 4
        ),
        product_feature_signal_threshold_high=raw.get(
            "product_feature_signal_threshold_high", 6
        ),
        creative_replication_comment_threshold_high=raw.get(
            "creative_replication_comment_threshold_high", 5
        ),
        premium_aesthetic_signal_threshold_high=raw.get(
            "premium_aesthetic_signal_threshold_high", 5
        ),
        demoability_threshold_high=raw.get("demoability_threshold_high", 4),
        weights=raw["weights"],
        bonuses=raw["bonuses"],
        penalties=raw["penalties"],
        keyword_dictionaries=raw["keyword_dictionaries"],
        categories=categories,
    )
