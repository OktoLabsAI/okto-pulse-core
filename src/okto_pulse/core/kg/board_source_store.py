"""Pure source-materialization contracts for KG rebuild.

This module owns only deterministic DTO/hash/source-ref rules shared by the
core rebuild pipeline and edition adapters. Raw SQLite access is implemented by
the Community ``BoardSourceReader`` adapter behind the KG registry port.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


# Spec source manifest versioning (spec eaf185c9 / card 5ec8c75c). V2 adds
# integration_requirements + observability_requirements to the spec content
# hash. V1 is kept so a legacy board's stored baseline can be PROVEN
# schema-rebaseline (the v1-compatible hash is unchanged) versus genuinely
# drifted (real content change).
SPEC_SOURCE_MANIFEST_VERSION = 2
_SPEC_CONTENT_COLUMNS_BASE: tuple[str, ...] = (
    "title",
    "description",
    "context",
    "version",
    "functional_requirements",
    "technical_requirements",
    "acceptance_criteria",
    "test_scenarios",
    "business_rules",
    "api_contracts",
    "decisions",
)
SPEC_CONTENT_COLUMNS_V1: tuple[str, ...] = _SPEC_CONTENT_COLUMNS_BASE
SPEC_CONTENT_COLUMNS_V2: tuple[str, ...] = _SPEC_CONTENT_COLUMNS_BASE + (
    "integration_requirements",
    "observability_requirements",
)


CARD_CONTENT_COLUMNS: tuple[str, ...] = (
    "title",
    "description",
    "details",
    "status",
    "priority",
    "card_type",
    "spec_id",
    "sprint_id",
    "test_scenario_ids",
    "conclusions",
    "screen_mockups",
    "knowledge_bases",
    "validations",
    "origin_task_id",
    "severity",
    "expected_behavior",
    "observed_behavior",
    "steps_to_reproduce",
    "action_plan",
    "linked_test_task_ids",
)


#: Load-bearing fields for the Path B amendment content hash (spec 7ea1e4be).
#: Includes the exact-membership lineage sets (origin/affected task ids) so a
#: lineage change re-hashes and re-enqueues the amendment. ``validation_metadata``
#: is intentionally excluded because it is audit/evidence-pointer data.
AMENDMENT_CONTENT_COLUMNS: tuple[str, ...] = (
    "original_spec_id",
    "origin_bug_id",
    "origin_task_ids",
    "affected_task_ids",
    "revision_spec_id",
    "regression_scenario_ids",
    "regression_test_task_ids",
    "automated_regression_refs",
    "status",
    "lineage_state",
)


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError, TypeError):
        return default


def _canonical_content_hash(row: Any, columns: tuple[str, ...]) -> str:
    """SHA-256 over canonical JSON of the load-bearing fields.

    ``row`` can be a mapping, SQLAlchemy row proxy, or sqlite3.Row supplied by an
    edition adapter. JSON strings are parsed before hashing so whitespace changes
    in serialized JSON columns do not alter the hash.
    """

    payload: dict[str, Any] = {}
    for col in columns:
        value = _row_value(row, col)
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="replace")
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (json.JSONDecodeError, ValueError):
                pass
        payload[col] = value
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _canonical_payload_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _to_iso(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _card_artifact_type(row: Any) -> str:
    raw = str(_row_value(row, "card_type", "normal") or "normal").lower()
    if raw == "test":
        return "test"
    if raw == "bug":
        return "bug"
    return "task"


def _row_status(row: Any, status_col: str = "status") -> str:
    if bool(_row_value(row, "archived", 0)):
        return "archived"
    return str(_row_value(row, status_col, "") or "")


def _updated_at(row: Any) -> str:
    return _to_iso(_row_value(row, "updated_at", _row_value(row, "created_at")))


def _load_json_array(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _bug_has_minimal_evidence(row: Any) -> bool:
    if _card_artifact_type(row) != "bug":
        return True
    evidence_fields = ("observed_behavior", "expected_behavior", "steps_to_reproduce")
    has_text = any(str(_row_value(row, field, "") or "").strip() for field in evidence_fields)
    linked_tests = _load_json_array(_row_value(row, "linked_test_task_ids"))
    conclusions = _load_json_array(_row_value(row, "conclusions"))
    return has_text and (bool(linked_tests) or bool(conclusions))


def _decision_id(decision: dict[str, Any], index: int) -> str:
    explicit = decision.get("id")
    if explicit:
        return str(explicit)
    fingerprint = _canonical_payload_hash(decision)[:12]
    return f"idx-{index}-{fingerprint}"


def _decision_sources_from_spec(row: Any) -> list[dict[str, Any]]:
    decisions = _load_json_array(_row_value(row, "decisions"))
    if not decisions:
        return []

    spec_id = str(_row_value(row, "id", ""))
    source_version = str(_row_value(row, "version", 1) or 1)
    spec_title = _row_value(row, "title", "")
    created_at = _row_value(row, "created_at", "")

    out: list[dict[str, Any]] = []
    for index, raw in enumerate(decisions):
        if not isinstance(raw, dict):
            continue
        status = str(raw.get("status", "active") or "active").lower()
        if status != "active":
            continue
        title = str(raw.get("title") or "").strip()
        rationale = str(raw.get("rationale") or raw.get("description") or "").strip()
        if not title and not rationale:
            continue
        local_id = _decision_id(raw, index)
        payload = {
            "parent_type": "spec",
            "parent_id": spec_id,
            "parent_title": spec_title,
            "decision": raw,
        }
        out.append(
            {
                "artifact_type": "decision",
                "id": f"{spec_id}:{local_id}",
                "source_ref": f"decision:{spec_id}:{local_id}",
                "source_version": source_version,
                "content_hash": _canonical_payload_hash(payload),
                "created_at": _to_iso(created_at),
            }
        )
    return out


__all__ = [
    "AMENDMENT_CONTENT_COLUMNS",
    "CARD_CONTENT_COLUMNS",
    "SPEC_CONTENT_COLUMNS_V1",
    "SPEC_CONTENT_COLUMNS_V2",
    "SPEC_SOURCE_MANIFEST_VERSION",
    "_bug_has_minimal_evidence",
    "_canonical_content_hash",
    "_canonical_payload_hash",
    "_card_artifact_type",
    "_decision_id",
    "_decision_sources_from_spec",
    "_load_json_array",
    "_row_status",
    "_to_iso",
    "_updated_at",
]
