"""R5-IMP5 — bounded-label in-process counter for agent-facing
``human_control_required`` rejections.

Applying or clearing a skip / no_action is a HUMAN decision (R5-IMP1): the
agent-facing MCP surface fails closed. This counter gives observability over how
often agents hit that boundary, WITHOUT inventing a heavy pipeline — it mirrors
the canonical ``kg/global_discovery/metrics.py`` pattern (``threading.Lock`` +
bounded samples + getters/reset).

Labels are deliberately bounded: ``board_id``, ``blocked_tool``, ``blocked_action``.
A ``target_ref`` / artifact id is high cardinality and lives in the structured
``human_control_required`` envelope, NEVER in a metric label. In-process only — no
DB, no audit write on the fail-closed path (so the refusal stays no-mutation).
"""

from __future__ import annotations

import threading
from typing import Any

from okto_pulse.core.observability.sample_buffer import BoundedCounterSampleBuffer

_HUMAN_CONTROL_LABELS = ("board_id", "blocked_tool", "blocked_action")
_human_control_samples = BoundedCounterSampleBuffer(_HUMAN_CONTROL_LABELS)
_human_control_lock = threading.Lock()


def emit_human_control_required(
    *, board_id: str, blocked_tool: str, blocked_action: str,
) -> None:
    """Record one agent-facing skip/no_action mutation refused as human-only."""
    with _human_control_lock:
        _human_control_samples.append({
            "board_id": board_id,
            "blocked_tool": blocked_tool,
            "blocked_action": blocked_action,
        })


def get_human_control_required_count(
    *,
    board_id: str | None = None,
    blocked_tool: str | None = None,
    blocked_action: str | None = None,
) -> int:
    with _human_control_lock:
        return _human_control_samples.count(
            board_id=board_id,
            blocked_tool=blocked_tool,
            blocked_action=blocked_action,
        )


def get_human_control_required_labels() -> tuple[str, ...]:
    return _HUMAN_CONTROL_LABELS


def get_human_control_required_samples() -> list[dict[str, Any]]:
    with _human_control_lock:
        return _human_control_samples.snapshot()


def reset_human_control_required_counter() -> None:
    """Test helper — clear the in-process counter."""
    with _human_control_lock:
        _human_control_samples.clear()


__all__ = [
    "emit_human_control_required",
    "get_human_control_required_count",
    "get_human_control_required_labels",
    "get_human_control_required_samples",
    "reset_human_control_required_counter",
]
