from __future__ import annotations

from .models import AppConfig, CreatorRecord


def _cfg_value(
    cfg: dict[str, float],
    key: str,
    fallback_key: str | None = None,
    default: float = 0.0,
) -> float:
    if key in cfg:
        return cfg[key]
    if fallback_key and fallback_key in cfg:
        return cfg[fallback_key]
    return default


def score_creators(
    creators: list[CreatorRecord], config: AppConfig
) -> list[CreatorRecord]:
    max_base_score = (
        3.0 * _cfg_value(config.weights, "audience_fit", default=2.5)
        + 2.0 * _cfg_value(config.weights, "monetization_focus", default=1.5)
        + 2.0 * _cfg_value(config.weights, "generic_tool_relevance", default=1.0)
        + 3.0 * _cfg_value(config.weights, "higgsfield_specific_fit", default=2.5)
        + 2.0 * _cfg_value(config.weights, "engagement_quality", default=1.5)
        + 1.0 * _cfg_value(config.weights, "posting_frequency", default=0.75)
        + 2.0 * _cfg_value(config.weights, "demoability", default=1.5)
        + 2.0 * _cfg_value(config.weights, "premium_aesthetic_fit", default=1.5)
    ) or 1.0

    for creator in creators:
        _apply_base_scores(creator)
        _apply_bonus_penalties(creator, config)

        base_score = (
            creator.audience_fit_score
            * _cfg_value(config.weights, "audience_fit", default=2.5)
            + creator.monetization_focus_score
            * _cfg_value(config.weights, "monetization_focus", default=1.5)
            + creator.tool_relevance_score
            * _cfg_value(
                config.weights, "generic_tool_relevance", default=1.0
            )
            + creator.higgsfield_specific_fit_score
            * _cfg_value(config.weights, "higgsfield_specific_fit", default=2.5)
            + creator.engagement_quality_score
            * _cfg_value(config.weights, "engagement_quality", default=1.5)
            + creator.posting_frequency_score
            * _cfg_value(config.weights, "posting_frequency", default=0.75)
            + creator.demoability_score
            * _cfg_value(config.weights, "demoability", default=1.5)
            + creator.premium_aesthetic_fit_score
            * _cfg_value(config.weights, "premium_aesthetic_fit", default=1.5)
        )
        total_score = (base_score / max_base_score) * 10.0
        total_score += creator.bonus_score - creator.penalty_score
        creator.normalized_score = min(10.0, max(0.0, total_score))
        creator.priority_tier = _tier_from_score(creator.normalized_score)
        _apply_fit_label(creator, config)
    return sorted(creators, key=lambda c: c.normalized_score, reverse=True)


def _apply_base_scores(creator: CreatorRecord) -> None:
    category_bonus = _visual_category_bonus(creator)

    creator.audience_fit_score = min(3.0, creator.audience_signal_count / 8.0)
    creator.monetization_focus_score = min(
        2.0, creator.monetization_signal_count / 8.0
    )
    creator.tool_relevance_score = min(
        2.0,
        (
            creator.tool_signal_count
            + creator.workflow_signal_count
            + (creator.comment_tool_intent_count * 0.5)
        )
        / 8.0,
    )
    creator.higgsfield_specific_fit_score = min(
        3.0,
        (
            creator.higgsfield_specific_signal_count
            + creator.premium_aesthetic_signal_count
            + creator.creative_replication_intent_count
            + category_bonus
        )
        / 6.0,
    )

    comment_intent = (
        creator.comment_tool_intent_count
        + creator.comment_creator_intent_count
        + creator.comment_business_intent_count
        + creator.creative_replication_intent_count
    )
    creator.engagement_quality_score = (
        2.0 if comment_intent >= 8 else (1.0 if comment_intent >= 3 else 0.0)
    )

    creator.posting_frequency_score = (
        1.0
        if creator.uploads_last_30_days >= 4
        else (0.5 if creator.uploads_last_30_days >= 2 else 0.0)
    )

    creator.demoability_score = min(
        2.0,
        (
            creator.workflow_signal_count
            + creator.tool_signal_count
            + creator.creative_replication_intent_count
            + category_bonus
        )
        / 6.0,
    )
    creator.premium_aesthetic_fit_score = min(
        2.0, (creator.premium_aesthetic_signal_count + category_bonus) / 4.0
    )


def _apply_bonus_penalties(creator: CreatorRecord, config: AppConfig) -> None:
    creator.bonus_score = 0.0
    creator.penalty_score = 0.0

    if 30_000 <= creator.subscribers <= 300_000:
        creator.bonus_score += _cfg_value(config.bonuses, "mid_tier_bonus")

    if creator.median_views_to_sub_ratio >= 0.12:
        creator.bonus_score += _cfg_value(config.bonuses, "view_health_bonus")

    if (
        creator.workflow_signal_count >= config.workflow_signal_threshold_high
        and creator.tool_signal_count >= 2
    ):
        creator.bonus_score += _cfg_value(config.bonuses, "workflow_bonus")

    if creator.uploads_last_30_days >= config.uploads_30d_consistency_threshold:
        creator.bonus_score += _cfg_value(config.bonuses, "consistency_bonus")

    if (
        creator.higgsfield_specific_signal_count
        >= config.product_feature_signal_threshold_high
    ):
        creator.bonus_score += _cfg_value(config.bonuses, "feature_match_bonus")

    if (
        creator.premium_aesthetic_signal_count
        >= config.premium_aesthetic_signal_threshold_high
        or _visual_category_bonus(creator) >= 3.0
    ):
        creator.bonus_score += _cfg_value(config.bonuses, "creative_direction_bonus")

    if (
        "ai" in creator.description.lower()
        and creator.higgsfield_specific_signal_count <= 1
        and creator.audience_signal_count < 4
    ):
        creator.penalty_score += _cfg_value(
            config.penalties,
            "generic_ai_novelty_penalty",
            fallback_key="curiosity_penalty",
        )

    if creator.audience_signal_count <= 1 and creator.tool_signal_count == 0:
        creator.penalty_score += _cfg_value(
            config.penalties, "broad_entertainment_penalty"
        )

    if _is_low_visual_relevance(creator):
        creator.penalty_score += _cfg_value(
            config.penalties, "low_visual_relevance_penalty"
        )

    if creator.demoability_score < 0.5:
        creator.penalty_score += _cfg_value(
            config.penalties, "weak_demoability_penalty"
        )

    if creator.subscribers >= 300_000 and creator.median_views_to_sub_ratio < 0.03:
        creator.penalty_score += _cfg_value(
            config.penalties, "inflated_subscriber_penalty"
        )

    if creator.uploads_last_30_days <= 1 or creator.days_since_last_upload > 45:
        creator.penalty_score += _cfg_value(config.penalties, "inactive_penalty")


def _tier_from_score(score: float) -> str:
    if score >= 8.5:
        return "A"
    if score >= 7.0:
        return "B"
    if score >= 5.5:
        return "C"
    return "D"


def _apply_fit_label(creator: CreatorRecord, config: AppConfig) -> None:
    disqualify_topics = [
        "music",
        "reaction",
        "meme",
        "celebrity",
        "gaming highlights",
        "gossip",
        "news",
    ]
    text = (
        f"{creator.description} {creator.channel_name} "
        f"{' '.join(sorted(creator.matched_categories))}"
    ).lower()

    if creator.recent_uploads_checked == 0 or any(t in text for t in disqualify_topics):
        creator.fit_label = "disqualify"
        creator.disqualify_reason = (
            "Unrelated or inactive content profile for Higgsfield's visual creator use case"
        )
        return

    if _is_low_visual_relevance(creator):
        creator.fit_label = "disqualify"
        creator.disqualify_reason = (
            "Content is too generic business or workflow-focused without strong visual or demoable fit for Higgsfield"
        )
        return

    strong_fit = (
        creator.audience_fit_score >= 1.5
        and creator.higgsfield_specific_fit_score >= 1.25
        and creator.demoability_score >= 0.75
        and (
            creator.monetization_focus_score >= 1.0
            or creator.higgsfield_specific_signal_count
            >= config.product_feature_signal_threshold_high
            or creator.creative_replication_intent_count
            >= config.creative_replication_comment_threshold_high
            or creator.premium_aesthetic_signal_count
            >= config.premium_aesthetic_signal_threshold_high
        )
    )

    medium_fit = (
        creator.audience_fit_score >= 1.0
        and creator.higgsfield_specific_fit_score >= 0.75
        and (
            creator.demoability_score >= 0.5
            or creator.tool_relevance_score >= 1.0
            or creator.premium_aesthetic_fit_score >= 0.75
        )
    )

    if strong_fit:
        creator.fit_label = "strong_fit"
        creator.disqualify_reason = None
    elif medium_fit:
        creator.fit_label = "medium_fit"
        creator.disqualify_reason = None
    elif creator.audience_fit_score <= 0.5 and creator.higgsfield_specific_fit_score < 0.5:
        creator.fit_label = "disqualify"
        creator.disqualify_reason = (
            "Audience appears passive or generic and the channel shows weak Higgsfield-specific visual relevance"
        )
    else:
        creator.fit_label = "weak_fit"
        creator.disqualify_reason = None


def _visual_category_bonus(creator: CreatorRecord) -> float:
    text = " ".join(
        [creator.primary_category, *sorted(creator.matched_categories)]
    ).lower()
    bonus = 0.0

    strong_visual = [
        "creative directors",
        "video production",
        "filmmaking",
        "visual direction",
    ]
    moderate_visual = [
        "tutorial-adjacent",
        "tool-curious",
        "ad creators",
    ]
    weak_visual = [
        "workflow optimizers",
        "creator business",
        "automation creators",
    ]

    if any(keyword in text for keyword in strong_visual):
        bonus += 3.0
    elif any(keyword in text for keyword in moderate_visual):
        bonus += 1.5

    if any(keyword in text for keyword in weak_visual):
        bonus -= 1.0

    return max(0.0, bonus)


def _is_low_visual_relevance(creator: CreatorRecord) -> bool:
    business_heavy = (
        creator.monetization_focus_score >= 1.0 and creator.audience_fit_score >= 1.0
    )
    lacks_visual = (
        creator.higgsfield_specific_fit_score < 0.5
        and creator.premium_aesthetic_fit_score < 0.5
        and creator.demoability_score < 0.5
    )
    return business_heavy and lacks_visual
