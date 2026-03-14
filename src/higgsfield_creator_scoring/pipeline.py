from __future__ import annotations

import logging
from typing import Any

from .commentary_generator import CommentaryGenerator
from .config_loader import load_config
from .discovery import run_discovery
from .enrichment import enrich_creators
from .logging_utils import configure_logging
from .scoring import score_creators
from .sheets_writer import SheetsWriter
from .youtube_client import YouTubeClient

logger = logging.getLogger(__name__)


def run_pipeline(
    config_path: str,
    apify_token: str,
    openai_api_key: str | None,
    spreadsheet_id: str,
    gcp_service_account_json: str,
    log_level: str = "INFO",
) -> dict[str, Any]:
    configure_logging(log_level)
    config = load_config(config_path)

    yt = YouTubeClient(apify_token=apify_token, config=config)
    commentator = CommentaryGenerator(api_key=openai_api_key)
    sheets = SheetsWriter(
        spreadsheet_id=spreadsheet_id,
        service_account_json_path=gcp_service_account_json,
    )

    raw_discovery, completed_queries, incomplete_query = (
        sheets.load_resume_discovery_state()
    )
    existing_enriched, completed_channel_ids = sheets.load_resume_enrichment_state()
    if completed_queries:
        logger.info("resume loaded | completed_queries=%s", len(completed_queries))
    if incomplete_query:
        logger.info(
            "resume loaded | rerunning_incomplete_query | category=%s | query=%s",
            incomplete_query[0],
            incomplete_query[1],
        )
    if completed_channel_ids:
        logger.info(
            "resume loaded | completed_enriched_channels=%s",
            len(completed_channel_ids),
        )

    creators_by_channel = {}
    enriched_creators = list(existing_enriched)

    def checkpoint_query(raw_rows, category_name: str, query: str) -> None:
        completed_queries.add((category_name, query))
        sheets.write_discovery_progress(
            config=config,
            raw_discovery=raw_rows,
            completed_queries=completed_queries,
        )
        logger.info(
            "checkpoint written | stage=discovery_query | category=%s | query=%s | raw_hits=%s",
            category_name,
            query,
            len(raw_rows),
        )

    creators_by_channel, raw_discovery = run_discovery(
        yt=yt,
        config=config,
        existing_raw_rows=raw_discovery,
        completed_queries=completed_queries,
        on_query_completed=checkpoint_query,
    )
    logger.info("candidates found=%s", len(creators_by_channel))

    try:
        sheets.write_discovery_progress(
            config=config,
            raw_discovery=raw_discovery,
            completed_queries=completed_queries,
        )
        logger.info(
            "checkpoint written | stage=discovery | raw_hits=%s", len(raw_discovery)
        )
    except Exception:
        logger.exception("failed to write discovery checkpoint")

    def checkpoint_channel(creators_snapshot, channel_id: str) -> None:
        completed_channel_ids.add(channel_id)
        sheets.write_enrichment_progress(
            creators=creators_snapshot,
            completed_channel_ids=completed_channel_ids,
        )
        logger.info(
            "checkpoint written | stage=enrichment_channel | channel_id=%s | creators=%s",
            channel_id,
            len(creators_snapshot),
        )

    enriched_creators = enrich_creators(
        yt=yt,
        config=config,
        creators=creators_by_channel,
        existing_enriched=enriched_creators,
        completed_channel_ids=completed_channel_ids,
        on_channel_completed=checkpoint_channel,
    )

    try:
        # Snapshot enriched rows even before scoring/commentary, so crashes later do not lose progress.
        sheets.write_enrichment_progress(
            creators=enriched_creators,
            completed_channel_ids=completed_channel_ids,
        )
        logger.info(
            "checkpoint written | stage=enrichment | creators=%s",
            len(enriched_creators),
        )
    except Exception:
        logger.exception("failed to write enrichment checkpoint")

    scored = score_creators(enriched_creators, config)
    for creator in scored:
        if not creator.fit_comment:
            result = commentator.generate(creator)
            creator.fit_label = result["fit_label"]
            creator.fit_comment = result["comment"]

    scored.sort(key=lambda c: c.normalized_score, reverse=True)
    sheets.write_all(
        config=config,
        raw_discovery=raw_discovery,
        creators=scored,
    )

    return {
        "raw_hits": len(raw_discovery),
        "unique_candidates": len(creators_by_channel),
        "enriched_creators": len(enriched_creators),
        "written_creators": len(scored),
        "top_tier_count": len(
            [
                c
                for c in scored
                if c.priority_tier in {"A", "B"} and c.fit_label != "disqualify"
            ]
        ),
    }
