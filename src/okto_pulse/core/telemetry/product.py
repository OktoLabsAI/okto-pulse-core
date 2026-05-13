"""Anonymous product telemetry aggregation from local domain state."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from okto_pulse.core.infra.config import CoreSettings

PRODUCT_METRIC_KEYS = {
    "product_feature_usage_counts",
    "product_flow_origin_counts",
    "product_flow_completion_counts",
    "product_work_item_type_counts",
    "product_workflow_stage_counts",
    "product_quality_signal_counts",
    "product_advanced_capability_counts",
}

PRODUCT_AGGREGATE_FAMILIES = tuple(sorted(PRODUCT_METRIC_KEYS))


def _sqlite_path(database_url: str) -> Path | None:
    prefixes = ("sqlite+aiosqlite:///", "sqlite:///")
    for prefix in prefixes:
        if database_url.startswith(prefix):
            raw = database_url[len(prefix):]
            if raw.startswith("/") and len(raw) > 2 and raw[2] == ":":
                raw = raw[1:]
            return Path(unquote(raw)).expanduser().resolve()
    return None


def _safe_count_key(value: Any, *, fallback: str = "unknown") -> str:
    text = str(value or fallback).strip().lower().replace(" ", "_").replace("-", "_")
    cleaned = "".join(ch for ch in text if ch.isalnum() or ch in "._:/{}")
    return cleaned[:80] or fallback


def _load_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


class ProductTelemetryAggregator:
    """Build count-only product metrics without exporting artifact identifiers."""

    def __init__(self, settings: CoreSettings, metrics_dir: Path):
        self.settings = settings
        self.metrics_dir = metrics_dir

    @property
    def state_path(self) -> Path:
        return self.metrics_dir / "product_state.json"

    def aggregate(self) -> dict[str, dict[str, int]]:
        db_path = _sqlite_path(str(getattr(self.settings, "database_url", "")))
        if db_path is None or not db_path.exists():
            return {}
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            metrics = self._aggregate_conn(conn)
            self._save_state(metrics)
            return metrics
        finally:
            conn.close()

    def _aggregate_conn(self, conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
        metrics: dict[str, Counter[str]] = {key: Counter() for key in PRODUCT_METRIC_KEYS}
        if _table_exists(conn, "domain_events"):
            self._aggregate_domain_events(conn, metrics)
        self._aggregate_current_shapes(conn, metrics)
        return {
            key: dict(sorted(counter.items()))
            for key, counter in sorted(metrics.items())
            if counter
        }

    def _aggregate_domain_events(self, conn: sqlite3.Connection, metrics: dict[str, Counter[str]]) -> None:
        rows = conn.execute("SELECT event_type, payload_json FROM domain_events").fetchall()
        for row in rows:
            event_type = _safe_count_key(row["event_type"])
            payload = _load_json(row["payload_json"])
            metrics["product_feature_usage_counts"][event_type] += 1

            if event_type == "spec.created":
                metrics["product_flow_origin_counts"][_origin_from_spec_source(payload.get("source"))] += 1
            elif event_type == "spec.moved":
                to_status = _safe_count_key(payload.get("to_status"))
                metrics["product_workflow_stage_counts"][f"spec.{to_status}"] += 1
                if to_status == "done":
                    metrics["product_flow_completion_counts"]["completed"] += 1
            elif event_type == "card.created":
                card_type = _safe_count_key(payload.get("card_type"), fallback="normal")
                metrics["product_work_item_type_counts"][card_type] += 1
                if card_type == "bug":
                    metrics["product_quality_signal_counts"]["bugs_created"] += 1
                elif card_type in {"test", "test_scenario"}:
                    metrics["product_quality_signal_counts"]["tests_created"] += 1
                else:
                    metrics["product_quality_signal_counts"]["tasks_created"] += 1
            elif event_type == "card.moved":
                to_status = _safe_count_key(payload.get("to_status"))
                metrics["product_workflow_stage_counts"][f"card.{to_status}"] += 1
                if to_status in {"validation", "done"}:
                    metrics["product_quality_signal_counts"][f"cards_{to_status}"] += 1
            elif event_type.startswith("kg."):
                metrics["product_advanced_capability_counts"][event_type] += 1
            elif event_type in {"ideation.derived_to_spec", "refinement.derived_to_spec"}:
                metrics["product_flow_origin_counts"][event_type.split(".", 1)[0]] += 1

    def _aggregate_current_shapes(self, conn: sqlite3.Connection, metrics: dict[str, Counter[str]]) -> None:
        if _table_exists(conn, "specs"):
            spec_rows = conn.execute(
                "SELECT id, status, ideation_id, refinement_id, test_scenarios, decisions "
                "FROM specs"
            ).fetchall()
            for row in spec_rows:
                status = _safe_count_key(row["status"])
                metrics["product_workflow_stage_counts"][f"spec.current.{status}"] += 1
                origin = self._origin_from_spec_row(conn, row)
                metrics["product_flow_origin_counts"][f"current.{origin}"] += 1
                if status == "done":
                    metrics["product_flow_completion_counts"][origin] += 1
                metrics["product_quality_signal_counts"]["test_scenarios_total"] += _json_array_len(row["test_scenarios"])
                metrics["product_advanced_capability_counts"]["decisions_total"] += _json_array_len(row["decisions"])

        if _table_exists(conn, "cards"):
            for row in conn.execute("SELECT status, card_type FROM cards").fetchall():
                status = _safe_count_key(row["status"])
                card_type = _safe_count_key(row["card_type"], fallback="normal")
                metrics["product_workflow_stage_counts"][f"card.current.{status}"] += 1
                metrics["product_work_item_type_counts"][f"current.{card_type}"] += 1

        if _table_exists(conn, "sprints"):
            for row in conn.execute("SELECT status FROM sprints").fetchall():
                metrics["product_workflow_stage_counts"][f"sprint.current.{_safe_count_key(row['status'])}"] += 1

        if _table_exists(conn, "architecture_designs"):
            count = conn.execute("SELECT COUNT(*) FROM architecture_designs").fetchone()[0]
            if count:
                metrics["product_advanced_capability_counts"]["architecture_designs_total"] += int(count)

    def _origin_from_spec_row(self, conn: sqlite3.Connection, row: sqlite3.Row) -> str:
        if row["refinement_id"]:
            return "refinement"
        if row["ideation_id"]:
            if _table_exists(conn, "story_ideation_links"):
                linked_story = conn.execute(
                    "SELECT 1 FROM story_ideation_links WHERE ideation_id = ? LIMIT 1",
                    (row["ideation_id"],),
                ).fetchone()
                if linked_story:
                    return "story"
            return "ideation"
        return "spec"

    def _save_state(self, metrics: dict[str, dict[str, int]]) -> None:
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "families": sorted(metrics),
            "last_aggregate_total": sum(sum(group.values()) for group in metrics.values()),
        }
        self.state_path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")


def _origin_from_spec_source(source: Any) -> str:
    value = _safe_count_key(source, fallback="manual")
    if value == "derived_ideation":
        return "ideation"
    if value == "derived_refinement":
        return "refinement"
    return "spec"


def _json_array_len(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, list):
        return len(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return 0
        return len(parsed) if isinstance(parsed, list) else 0
    return 0
