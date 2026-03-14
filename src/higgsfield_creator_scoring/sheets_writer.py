from __future__ import annotations

import json
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from .models import AppConfig, CategoryConfig, CreatorRecord, RawDiscoveryRow


class SheetsWriter:
    def __init__(self, spreadsheet_id: str, service_account_json_path: str) -> None:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(
            service_account_json_path, scopes=scopes
        )
        self.gc = gspread.authorize(creds)
        self.sheet = self.gc.open_by_key(spreadsheet_id)

    def write_all(
        self,
        config: AppConfig,
        raw_discovery: list[RawDiscoveryRow],
        creators: list[CreatorRecord],
    ) -> None:
        self._write_category_config(config.categories)
        self._write_raw_discovery(raw_discovery)
        self._write_creator_master(creators)

    def write_discovery_snapshot(
        self,
        config: AppConfig,
        raw_discovery: list[RawDiscoveryRow],
    ) -> None:
        self._write_category_config(config.categories)
        self._write_raw_discovery(raw_discovery)

    def write_discovery_progress(
        self,
        config: AppConfig,
        raw_discovery: list[RawDiscoveryRow],
        completed_queries: set[tuple[str, str]],
    ) -> None:
        self.write_discovery_snapshot(config=config, raw_discovery=raw_discovery)
        self._write_discovery_state(completed_queries)

    def write_creators_snapshot(self, creators: list[CreatorRecord]) -> None:
        self._write_creator_master(creators)

    def write_enrichment_progress(
        self,
        creators: list[CreatorRecord],
        completed_channel_ids: set[str],
    ) -> None:
        self.write_creators_snapshot(creators)
        self._write_enrichment_state(completed_channel_ids)

    def load_resume_discovery_state(
        self,
    ) -> tuple[list[RawDiscoveryRow], set[tuple[str, str]], tuple[str, str] | None]:
        raw_rows = self._read_raw_discovery()
        explicit_completed = self._read_discovery_state()
        if explicit_completed:
            filtered_rows = [
                row
                for row in raw_rows
                if (row.category, row.query) in explicit_completed
            ]
            return filtered_rows, explicit_completed, None

        ordered_queries: OrderedDict[tuple[str, str], None] = OrderedDict()
        for row in raw_rows:
            ordered_queries[(row.category, row.query)] = None
        if not ordered_queries:
            return [], set(), None

        query_order = list(ordered_queries.keys())
        completed = set(query_order[:-1])
        incomplete = query_order[-1]
        filtered_rows = [
            row for row in raw_rows if (row.category, row.query) in completed
        ]
        return filtered_rows, completed, incomplete

    def load_resume_enrichment_state(
        self,
    ) -> tuple[list[CreatorRecord], set[str]]:
        creators = self._read_creator_master()
        completed_channel_ids = self._read_enrichment_state()
        if completed_channel_ids:
            creators = [
                creator
                for creator in creators
                if creator.channel_id in completed_channel_ids
            ]
        else:
            completed_channel_ids = {creator.channel_id for creator in creators}
        return creators, completed_channel_ids

    def _upsert_tab(self, title: str, rows: list[list[Any]]) -> None:
        try:
            ws = self.sheet.worksheet(title)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = self.sheet.add_worksheet(
                title=title, rows=max(1000, len(rows) + 10), cols=40
            )
        ws.update(rows)

    def _write_category_config(self, categories: list[CategoryConfig]) -> None:
        rows: list[list[Any]] = [
            ["category", "weight", "queries", "keywords", "negative_keywords"]
        ]
        for c in categories:
            rows.append(
                [
                    c.name,
                    c.weight,
                    json.dumps(c.queries),
                    json.dumps(c.keywords),
                    json.dumps(c.negative_keywords),
                ]
            )
        self._upsert_tab("category_config", rows)

    def _write_raw_discovery(self, raw_discovery: list[RawDiscoveryRow]) -> None:
        rows = [
            [
                "timestamp",
                "query",
                "category",
                "result_type",
                "video_title",
                "video_url",
                "channel_name",
                "channel_id",
                "source_rank",
            ]
        ]
        rows.extend(row.to_sheet_row() for row in raw_discovery)
        self._upsert_tab("raw_discovery", rows)

    def _read_raw_discovery(self) -> list[RawDiscoveryRow]:
        try:
            ws = self.sheet.worksheet("raw_discovery")
        except gspread.WorksheetNotFound:
            return []

        rows = ws.get_all_values()
        if len(rows) <= 1:
            return []

        out: list[RawDiscoveryRow] = []
        for row in rows[1:]:
            if len(row) < 9:
                continue
            out.append(
                RawDiscoveryRow(
                    timestamp=row[0],
                    query=row[1],
                    category=row[2],
                    result_type=row[3],
                    video_title=row[4],
                    video_url=row[5],
                    channel_name=row[6],
                    channel_id=row[7],
                    source_rank=int(row[8] or 0),
                )
            )
        return out

    def _write_discovery_state(self, completed_queries: set[tuple[str, str]]) -> None:
        rows = [["category", "query", "status", "updated_at"]]
        updated_at = datetime.now(timezone.utc).isoformat()
        for category, query in sorted(completed_queries):
            rows.append([category, query, "completed", updated_at])
        self._upsert_tab("discovery_state", rows)

    def _read_discovery_state(self) -> set[tuple[str, str]]:
        try:
            ws = self.sheet.worksheet("discovery_state")
        except gspread.WorksheetNotFound:
            return set()

        rows = ws.get_all_values()
        completed: set[tuple[str, str]] = set()
        for row in rows[1:]:
            if len(row) < 3 or row[2] != "completed":
                continue
            completed.add((row[0], row[1]))
        return completed

    def _write_creator_master(self, creators: list[CreatorRecord]) -> None:
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
        rows.extend(c.to_master_sheet_row() for c in creators)
        self._upsert_tab("creator_master", rows)

    def _read_creator_master(self) -> list[CreatorRecord]:
        try:
            ws = self.sheet.worksheet("creator_master")
        except gspread.WorksheetNotFound:
            return []

        rows = ws.get_all_values()
        if len(rows) <= 1:
            return []

        creators: list[CreatorRecord] = []
        for row in rows[1:]:
            if len(row) < 25:
                continue
            creator = CreatorRecord(
                channel_id=self._channel_id_from_url(row[1]),
                channel_name=row[0],
                channel_url=row[1],
                description="",
                primary_category=row[2],
            )
            creator.matched_categories = set(
                part.strip() for part in row[3].split(" | ") if part.strip()
            )
            creator.matched_queries = set(
                part.strip() for part in row[4].split(" | ") if part.strip()
            )
            creator.subscribers = self._int_value(row[5])
            creator.median_views_recent_10 = float(self._int_value(row[6]))
            creator.median_views_to_sub_ratio = self._float_value(row[7])
            creator.uploads_last_30_days = self._int_value(row[8])
            creator.audience_signal_count = self._int_value(row[9])
            creator.monetization_signal_count = self._int_value(row[10])
            if len(row) >= 34:
                creator.workflow_signal_count = self._int_value(row[11])
                creator.tool_signal_count = self._int_value(row[12])
                creator.comment_tool_intent_count = self._int_value(row[13])
                creator.comment_creator_intent_count = self._int_value(row[14])
                creator.comment_business_intent_count = self._int_value(row[15])
                creator.higgsfield_specific_signal_count = self._int_value(row[16])
                creator.premium_aesthetic_signal_count = self._int_value(row[17])
                creator.creative_replication_intent_count = self._int_value(row[18])
                creator.audience_fit_score = self._float_value(row[19])
                creator.monetization_focus_score = self._float_value(row[20])
                creator.tool_relevance_score = self._float_value(row[21])
                creator.higgsfield_specific_fit_score = self._float_value(row[22])
                creator.engagement_quality_score = self._float_value(row[23])
                creator.posting_frequency_score = self._float_value(row[24])
                creator.demoability_score = self._float_value(row[25])
                creator.premium_aesthetic_fit_score = self._float_value(row[26])
                creator.bonus_score = self._float_value(row[27])
                creator.penalty_score = self._float_value(row[28])
                creator.normalized_score = self._float_value(row[29])
                creator.priority_tier = row[30] or "D"
                creator.fit_label = row[31] or "weak_fit"
                creator.fit_comment = row[32]
                creator.disqualify_reason = row[33] or None
            else:
                creator.tool_signal_count = self._int_value(row[11])
                creator.comment_tool_intent_count = self._int_value(row[12])
                creator.audience_fit_score = self._float_value(row[13])
                creator.monetization_focus_score = self._float_value(row[14])
                creator.tool_relevance_score = self._float_value(row[15])
                creator.engagement_quality_score = self._float_value(row[16])
                creator.posting_frequency_score = self._float_value(row[17])
                creator.bonus_score = self._float_value(row[18])
                creator.penalty_score = self._float_value(row[19])
                creator.normalized_score = self._float_value(row[20])
                creator.priority_tier = row[21] or "D"
                creator.fit_label = row[22] or "weak_fit"
                creator.fit_comment = row[23]
                creator.disqualify_reason = row[24] or None
            creators.append(creator)
        return creators

    def _write_enrichment_state(self, completed_channel_ids: set[str]) -> None:
        rows = [["channel_id", "status", "updated_at"]]
        updated_at = datetime.now(timezone.utc).isoformat()
        for channel_id in sorted(completed_channel_ids):
            rows.append([channel_id, "completed", updated_at])
        self._upsert_tab("enrichment_state", rows)

    def _read_enrichment_state(self) -> set[str]:
        try:
            ws = self.sheet.worksheet("enrichment_state")
        except gspread.WorksheetNotFound:
            return set()

        rows = ws.get_all_values()
        completed: set[str] = set()
        for row in rows[1:]:
            if len(row) < 2 or row[1] != "completed":
                continue
            completed.add(row[0])
        return completed

    @staticmethod
    def _channel_id_from_url(url: str) -> str:
        marker = "/channel/"
        if marker not in url:
            return ""
        return url.split(marker, 1)[1].split("/", 1)[0]

    @staticmethod
    def _int_value(value: str) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _float_value(value: str) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
