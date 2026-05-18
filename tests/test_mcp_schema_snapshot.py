"""Snapshot regression test for MCP tool schemas (TR-P4 + TR-P6).

Spec P2 (ddbd4724): ensures MCP schemas remain stable across changes.
If this test fails, the schema drifted from the baseline.
To regenerate the baseline, run:

    python scripts/update_mcp_snapshot.py

Do this ONLY in a dedicated PR that intentionally changes a schema.
"""
from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import Any

import pytest

from okto_pulse.core.mcp.schema_generator import generate_tool_schema
from okto_pulse.core.models.schemas import CardMove, CardUpdate

# Path to the committed baseline snapshot
_SNAPSHOT_PATH = Path(__file__).parent / "fixtures" / "mcp_schema_snapshot.json"

# Tool names that are part of the pilot (TR-P7)
_PILOT_TOOLS = {
    "okto_pulse_get_card",
    "okto_pulse_update_card",
    "okto_pulse_move_card",
    "okto_pulse_delete_card",
    "okto_pulse_list_cards_by_status",
}


# ---------------------------------------------------------------------------
# Deep equality diff (TR-P6)
# ---------------------------------------------------------------------------


def _deep_equal(a: Any, b: Any, path: str = "") -> list[str]:
    """Return list of diff messages between two JSON-serialisable values.

    Order of dict keys is ignored (structural equality only).
    List element order IS significant.
    """
    if type(a) != type(b):  # noqa: E721  intentional exact-type compare
        return [f"{path or '.'}: type mismatch — got {type(a).__name__!r}, expected {type(b).__name__!r}"]
    if isinstance(a, dict):
        diffs: list[str] = []
        for k in set(a) | set(b):
            child_path = f"{path}.{k}" if path else k
            if k not in a:
                diffs.append(f"{child_path}: key missing in actual (only in snapshot)")
            elif k not in b:
                diffs.append(f"{child_path}: key missing in snapshot (only in actual)")
            else:
                diffs.extend(_deep_equal(a[k], b[k], child_path))
        return diffs
    if isinstance(a, list):
        if len(a) != len(b):
            return [f"{path or '.'}: list length differs — got {len(a)}, expected {len(b)}"]
        return [
            d
            for i, (x, y) in enumerate(zip(a, b))
            for d in _deep_equal(x, y, f"{path}[{i}]")
        ]
    return [] if a == b else [f"{path or '.'}: {a!r} vs {b!r}"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_snapshot() -> dict[str, Any]:
    if not _SNAPSHOT_PATH.exists():
        pytest.fail(
            f"Snapshot file not found: {_SNAPSHOT_PATH}\n"
            "Generate it with: python scripts/update_mcp_snapshot.py"
        )
    return json.loads(_SNAPSHOT_PATH.read_text())


def _build_actual_schemas() -> dict[str, Any]:
    """Build the current schemas for comparison against snapshot."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return {
            "okto_pulse_get_card": {
                "title": "Get card details.",
                "description": "Returns full card details including attachments, Q&A, and comments.",
                "type": "object",
                "properties": {
                    "board_id": {"type": "string", "description": "Board ID."},
                    "card_id": {"type": "string", "description": "Card ID."},
                },
                "required": ["board_id", "card_id"],
            },
            "okto_pulse_update_card": generate_tool_schema(
                CardUpdate,
                "Update card details.",
                "Pass only fields you want to change; omit the rest. Fields not passed are unchanged.",
            ),
            "okto_pulse_move_card": generate_tool_schema(
                CardMove,
                "Move a card to a different column on the board.",
                "Moving to validation or done requires conclusion, completeness, and drift.",
            ),
            "okto_pulse_delete_card": {
                "title": "Delete a card from the board.",
                "description": "Permanent deletion; cannot be undone.",
                "type": "object",
                "properties": {
                    "board_id": {"type": "string", "description": "Board ID."},
                    "card_id": {"type": "string", "description": "Card ID."},
                },
                "required": ["board_id", "card_id"],
            },
            "okto_pulse_list_cards_by_status": {
                "title": "List cards with optional filters and pagination.",
                "description": "Returns filtered/paginated cards and summary counts.",
                "type": "object",
                "properties": {
                    "board_id": {"type": "string", "description": "Board ID."},
                    "status": {"type": "string", "description": "Filter by status (optional, empty = all). Values: not_started, started, in_progress, validation, on_hold, done, cancelled, open."},
                    "spec_id": {"type": "string", "description": "Filter to cards linked to this spec (optional)."},
                    "priority": {"type": "string", "description": "Filter by priority (optional). Values: none, low, medium, high, very_high, critical."},
                    "assignee_id": {"type": "string", "description": "Filter by assignee ID (optional)."},
                    "offset": {"type": "integer", "description": "Skip first N cards (default 0)."},
                    "limit": {"type": "integer", "description": "Max cards to return (default 50, max 200)."},
                },
                "required": ["board_id"],
            },
        }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSnapshotPresence:
    def test_snapshot_file_exists(self) -> None:
        assert _SNAPSHOT_PATH.exists(), (
            f"Snapshot missing: {_SNAPSHOT_PATH}\n"
            "Run: python scripts/update_mcp_snapshot.py"
        )

    def test_snapshot_contains_all_pilot_tools(self) -> None:
        snapshot = _load_snapshot()
        tools = set(snapshot.get("tools", {}).keys())
        missing = _PILOT_TOOLS - tools
        assert not missing, f"Snapshot is missing pilot tools: {missing}"

    def test_snapshot_version_present(self) -> None:
        snapshot = _load_snapshot()
        assert "version" in snapshot, "Snapshot must include a 'version' key."

    def test_each_pilot_tool_has_required_schema_keys(self) -> None:
        snapshot = _load_snapshot()
        for name in _PILOT_TOOLS:
            tool_schema = snapshot["tools"][name]
            assert "title" in tool_schema, f"{name}: missing 'title'"
            assert "description" in tool_schema, f"{name}: missing 'description'"
            assert "type" in tool_schema, f"{name}: missing 'type'"
            assert "properties" in tool_schema, f"{name}: missing 'properties'"
            assert "required" in tool_schema, f"{name}: missing 'required'"


class TestSnapshotDiff:
    def test_no_drift_from_snapshot(self) -> None:
        """Deep structural diff between generated schemas and baseline.

        If this fails, either:
        - A model annotation changed unexpectedly (regression), or
        - A schema was intentionally updated but the snapshot was not refreshed.

        To refresh: python scripts/update_mcp_snapshot.py
        """
        snapshot = _load_snapshot()
        actual = _build_actual_schemas()

        all_diffs: list[str] = []
        for tool_name in _PILOT_TOOLS:
            snap_schema = snapshot["tools"].get(tool_name)
            act_schema = actual.get(tool_name)
            if snap_schema is None:
                all_diffs.append(f"{tool_name}: missing from snapshot")
                continue
            if act_schema is None:
                all_diffs.append(f"{tool_name}: missing from actual (internal error)")
                continue
            tool_diffs = _deep_equal(act_schema, snap_schema, tool_name)
            all_diffs.extend(tool_diffs)

        if all_diffs:
            diff_text = "\n  ".join(all_diffs)
            pytest.fail(
                f"Schema drift detected in {len(all_diffs)} location(s):\n  {diff_text}\n\n"
                "To regenerate snapshot: python scripts/update_mcp_snapshot.py"
            )

    def test_deep_equal_helper_detects_value_diff(self) -> None:
        a = {"x": 1, "y": "hello"}
        b = {"x": 2, "y": "hello"}
        diffs = _deep_equal(a, b)
        assert len(diffs) == 1
        assert "x" in diffs[0]

    def test_deep_equal_helper_detects_missing_key(self) -> None:
        a = {"x": 1}
        b = {"x": 1, "y": 2}
        diffs = _deep_equal(a, b)
        assert any("y" in d for d in diffs)

    def test_deep_equal_helper_detects_list_length_diff(self) -> None:
        a = [1, 2, 3]
        b = [1, 2]
        diffs = _deep_equal(a, b)
        assert any("length" in d for d in diffs)

    def test_deep_equal_helper_detects_type_mismatch(self) -> None:
        diffs = _deep_equal("string", 42)
        assert any("type mismatch" in d for d in diffs)

    def test_deep_equal_helper_no_diff_on_identical(self) -> None:
        obj = {"a": 1, "b": [1, 2], "c": {"nested": True}}
        assert _deep_equal(obj, obj) == []

    def test_deep_equal_helper_dict_order_irrelevant(self) -> None:
        a = {"z": 1, "a": 2}
        b = {"a": 2, "z": 1}
        assert _deep_equal(a, b) == []
