"""Runtime-stable telemetry emitters for the maintained EventTypes (spec R5A,
card R5A-B).

These are the canonical, real emit points the core uses to turn the
``pending:R5A-B`` types in the EventType contract into wired coverage: ``cli``,
``mcp``, ``kg``, ``lifecycle`` and ``pipeline_transition``. Each helper accepts
ONLY the bounded, closed-schema fields for its type (never free ids / args / paths
/ payloads), builds the closed payload, and records it through
:class:`TelemetryService`. Like the HTTP middleware, emission NEVER raises into the
caller — telemetry must not break a real operation.

Settings resolve from the configured global (:func:`get_settings`) so the helpers
are callable at any core seam without threading ``settings`` through; tests inject
``settings`` explicitly. The aggregate each type feeds is pinned by the EventType
contract (event_contract.LIVE_AGGREGATE_MAPS) and exercised emit->store->aggregate
by the R5A-B tests.
"""

from __future__ import annotations

import logging
from typing import Any

from okto_pulse.core.infra.config import CoreSettings, get_settings
from okto_pulse.core.telemetry.service import TelemetryService

logger = logging.getLogger("okto_pulse.telemetry.emitters")


def _compact(payload: dict[str, Any]) -> dict[str, Any]:
    """Drop None-valued fields; the closed schema (sanitize_payload) still enforces
    the allowlist + anti-exfiltration on whatever remains."""
    return {key: value for key, value in payload.items() if value is not None}


def _emit(event_type: str, payload: dict[str, Any], *, settings: CoreSettings | None) -> dict[str, Any]:
    try:
        return TelemetryService(settings or get_settings()).record_event(event_type, _compact(payload))
    except Exception:
        # Telemetry must never break the operation it observes (cf. app.py middleware).
        logger.debug("telemetry.emit_failed", exc_info=True, extra={"event_type": event_type})
        return {"written": False}


def emit_cli_event(
    *,
    command: str,
    exit_code: int | None = None,
    duration_ms: int | float | None = None,
    error_class: str | None = None,
    settings: CoreSettings | None = None,
) -> dict[str, Any]:
    """Emit a ``cli`` event. ``command`` is the bounded command NAME (never args).
    Aggregates into ``cli_counts`` keyed by command."""
    return _emit(
        "cli",
        {"command": command, "exit_code": exit_code, "duration_ms": duration_ms, "error_class": error_class},
        settings=settings,
    )


def emit_mcp_event(
    *,
    tool_name: str,
    status: str | None = None,
    duration_ms: int | float | None = None,
    error_class: str | None = None,
    settings: CoreSettings | None = None,
) -> dict[str, Any]:
    """Emit an ``mcp`` event. ``tool_name`` is the bounded tool NAME (never the tool
    input). Aggregates into ``mcp_tool_counts`` keyed by tool_name."""
    return _emit(
        "mcp",
        {"tool_name": tool_name, "status": status, "duration_ms": duration_ms, "error_class": error_class},
        settings=settings,
    )


def emit_kg_event(
    *,
    operation: str,
    status: str | None = None,
    duration_ms: int | float | None = None,
    node_type: str | None = None,
    result_count: int | None = None,
    settings: CoreSettings | None = None,
) -> dict[str, Any]:
    """Emit a ``kg`` event. ``operation`` is the bounded operation TYPE (never a
    free node/artifact/payload). Aggregates into ``kg_operation_counts`` keyed by
    operation."""
    return _emit(
        "kg",
        {
            "operation": operation,
            "status": status,
            "duration_ms": duration_ms,
            "node_type": node_type,
            "result_count": result_count,
        },
        settings=settings,
    )


def emit_lifecycle_event(
    *,
    action: str,
    status: str | None = None,
    duration_ms: int | float | None = None,
    settings: CoreSettings | None = None,
) -> dict[str, Any]:
    """Emit a ``lifecycle`` event. ``action`` is the bounded controlled action
    (never an id/payload). Aggregates into ``lifecycle_counts`` keyed by action."""
    return _emit(
        "lifecycle",
        {"action": action, "status": status, "duration_ms": duration_ms},
        settings=settings,
    )


def emit_pipeline_transition_event(
    *,
    phase: str,
    from_status: str | None = None,
    to_status: str | None = None,
    status: str | None = None,
    settings: CoreSettings | None = None,
) -> dict[str, Any]:
    """Emit a ``pipeline_transition`` event. ``phase`` is the bounded controlled
    phase (never an id/payload). Aggregates into ``pipeline_transition_counts``
    keyed by phase."""
    return _emit(
        "pipeline_transition",
        {"phase": phase, "from_status": from_status, "to_status": to_status, "status": status},
        settings=settings,
    )
