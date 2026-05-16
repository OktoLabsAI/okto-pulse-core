"""Regenerate the MCP schema snapshot baseline.

Spec P2 (ddbd4724): zero-drift schema via runtime Pydantic v2 introspection.

Usage
-----
Run ONLY when intentionally changing a schema (e.g. adding a new Field
description, adding a tool, or changing invariants). Always commit the
updated snapshot in the SAME PR as the schema change.

    python scripts/update_mcp_snapshot.py

The script writes ``tests/fixtures/mcp_schema_snapshot.json``.

WARNING
-------
Never run this to silence a failing snapshot test without understanding WHY
the schema drifted. The test failing is the signal — investigate first.
"""
from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

# Allow running from any cwd by resolving paths relative to this script
_REPO_ROOT = Path(__file__).resolve().parent.parent
_FIXTURES_DIR = _REPO_ROOT / "tests" / "fixtures"
_OUTPUT_PATH = _FIXTURES_DIR / "mcp_schema_snapshot.json"

# Add src/ to sys.path so we can import without installing
sys.path.insert(0, str(_REPO_ROOT / "src"))

try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ImportError:
        tomllib = None  # type: ignore[assignment]


def _read_version() -> str:
    pyproject = _REPO_ROOT / "pyproject.toml"
    if tomllib is not None and pyproject.exists():
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        return data.get("project", {}).get("version", "unknown")
    return "unknown"


def _build_snapshot() -> dict:
    from okto_pulse.core.mcp.schema_generator import generate_tool_schema
    from okto_pulse.core.models.schemas import CardMove, CardUpdate

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)

        return {
            "version": _read_version(),
            "tools": {
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
                        "status": {
                            "type": "string",
                            "description": "Filter by status (optional, empty = all). Values: not_started, started, in_progress, validation, on_hold, done, cancelled, open.",
                        },
                        "spec_id": {
                            "type": "string",
                            "description": "Filter to cards linked to this spec (optional).",
                        },
                        "priority": {
                            "type": "string",
                            "description": "Filter by priority (optional). Values: none, low, medium, high, very_high, critical.",
                        },
                        "assignee_id": {
                            "type": "string",
                            "description": "Filter by assignee ID (optional).",
                        },
                        "offset": {
                            "type": "integer",
                            "description": "Skip first N cards (default 0).",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max cards to return (default 50, max 200).",
                        },
                    },
                    "required": ["board_id"],
                },
            },
        }


def main() -> None:
    print(f"[update_mcp_snapshot] Building schemas from runtime Pydantic introspection...")
    snapshot = _build_snapshot()

    _FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    _OUTPUT_PATH.write_text(json.dumps(snapshot, indent=2))

    tool_count = len(snapshot["tools"])
    print(f"[update_mcp_snapshot] Written {tool_count} tool schemas to: {_OUTPUT_PATH}")
    print(f"[update_mcp_snapshot] Version: {snapshot['version']}")
    print("[update_mcp_snapshot] Commit this file in the SAME PR as the schema change.")


if __name__ == "__main__":
    main()
