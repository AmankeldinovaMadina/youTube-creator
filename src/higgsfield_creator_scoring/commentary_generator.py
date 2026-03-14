from __future__ import annotations

import json
import logging
import os
from typing import Any

from openai import OpenAI

from .models import CreatorRecord

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are evaluating YouTube creators for partnership fit with Higgsfield, an AI-powered video creation product positioned as production leverage for creators and content businesses.

Your job is not to reward generic AI relevance. Your job is to identify whether the creator's audience is likely to treat content as a monetized workflow or income-generating asset.

Use only the provided evidence. Do not invent facts.

Classify each creator as:
- strong_fit
- medium_fit
- weak_fit
- disqualify

Then write a concise 1-3 sentence comment explaining why.
Return valid JSON only.
"""


class CommentaryGenerator:
    def __init__(self, api_key: str | None, model: str = "gpt-4o-mini") -> None:
        self.client = OpenAI(api_key=api_key) if api_key else None
        self.model = os.getenv("OPENAI_MODEL", model)

    def generate(self, creator: CreatorRecord) -> dict[str, Any]:
        if not self.client:
            return self._fallback_commentary(creator)

        user_prompt = self._build_user_prompt(creator)
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            text = response.choices[0].message.content or "{}"
            parsed = json.loads(text)
            return self._sanitize(parsed, creator)
        except Exception:
            logger.exception(
                "OpenAI commentary failed for channel_id=%s", creator.channel_id
            )
            return self._fallback_commentary(creator)

    def _build_user_prompt(self, creator: CreatorRecord) -> str:
        titles = [v.title for v in creator.recent_videos[:10]]
        return f"""Evaluate this YouTube creator for Higgsfield partnership fit.

Creator Data:
- Channel name: {creator.channel_name}
- Description: {creator.description}
- Matched categories: {sorted(creator.matched_categories)}
- Recent video titles: {titles}
- Subscribers: {creator.subscribers}
- Avg views recent 10: {creator.avg_views_recent_10}
- Uploads last 30 days: {creator.uploads_last_30_days}
- Signal counts:
  - audience: {creator.audience_signal_count}
  - monetization: {creator.monetization_signal_count}
  - workflow: {creator.workflow_signal_count}
  - tool: {creator.tool_signal_count}
  - comment tool intent: {creator.comment_tool_intent_count}
- Score breakdown:
  - audience_fit: {creator.audience_fit_score}
  - monetization_focus: {creator.monetization_focus_score}
  - tool_relevance: {creator.tool_relevance_score}
  - engagement_quality: {creator.engagement_quality_score}
  - posting_frequency: {creator.posting_frequency_score}
  - bonus_score: {creator.bonus_score}
  - penalty_score: {creator.penalty_score}
  - normalized_score: {creator.normalized_score}

Return valid JSON only:
{{
  "fit_label": "...",
  "confidence": 0.0,
  "comment": "...",
  "reason_summary": ["...", "...", "..."]
}}"""

    def _sanitize(
        self, payload: dict[str, Any], creator: CreatorRecord
    ) -> dict[str, Any]:
        fit_label = str(payload.get("fit_label", creator.fit_label))
        if fit_label not in {"strong_fit", "medium_fit", "weak_fit", "disqualify"}:
            fit_label = creator.fit_label

        comment = str(payload.get("comment", "")).strip()
        if not comment:
            comment = self._fallback_commentary(creator)["comment"]

        reasons = payload.get("reason_summary", [])
        if not isinstance(reasons, list):
            reasons = []

        confidence = payload.get("confidence", 0.6)
        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            confidence = 0.6

        return {
            "fit_label": fit_label,
            "confidence": max(0.0, min(1.0, confidence)),
            "comment": comment,
            "reason_summary": [str(r) for r in reasons][:3],
        }

    def _fallback_commentary(self, creator: CreatorRecord) -> dict[str, Any]:
        if creator.fit_label == "strong_fit":
            comment = (
                "This creator is a strong fit for Higgsfield because their audience appears creator/operator-heavy "
                "and recent content repeatedly covers tools, workflow leverage, and production scale. "
                "The channel context suggests paid-tool intent rather than passive curiosity."
            )
        elif creator.fit_label == "medium_fit":
            comment = (
                "This creator is a medium fit for Higgsfield because the audience appears creator-adjacent "
                "with some workflow and tool relevance, but monetization intent is mixed. "
                "Worth testing if trust and recommendation behavior are strong."
            )
        elif creator.fit_label == "disqualify":
            comment = (
                "This creator is disqualified for Higgsfield prioritization because recent evidence suggests low operator intent "
                "or weak relevance to creator-business workflows. "
                "Keep for auditability but exclude from top-priority outreach."
            )
        else:
            comment = (
                "This creator is a weak fit for Higgsfield because audience and content signals are more curiosity-driven "
                "than business/workflow-driven. "
                "Potential awareness value exists, but paid conversion likelihood appears limited."
            )

        return {
            "fit_label": creator.fit_label,
            "confidence": 0.6,
            "comment": comment,
            "reason_summary": [
                f"Audience signal count={creator.audience_signal_count}",
                f"Workflow+tool signal count={creator.workflow_signal_count + creator.tool_signal_count}",
                f"Comment tool intent count={creator.comment_tool_intent_count}",
            ],
        }
