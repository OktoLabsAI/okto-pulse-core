"""Closed telemetry schema and anti-exfiltration helpers."""

from __future__ import annotations

import json
import math
import platform
import re
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

CURRENT_SCHEMA_VERSION = "1.1.0"
TELEMETRY_EVENT_TYPES = (
    "cli",
    "http",
    "mcp",
    "kg",
    "lifecycle",
    "pipeline_transition",
    "guided_help",
)
TelemetryEventType = Literal[
    "cli",
    "http",
    "mcp",
    "kg",
    "lifecycle",
    "pipeline_transition",
    "guided_help",
]

ALLOWED_PAYLOAD_KEYS: dict[str, set[str]] = {
    "cli": {"command", "exit_code", "duration_ms", "error_class"},
    "http": {"method", "route_template", "status_code", "duration_ms", "error_class"},
    "mcp": {"tool_name", "status", "duration_ms", "error_class"},
    "kg": {"operation", "status", "duration_ms", "node_type", "result_count"},
    "lifecycle": {"action", "status", "duration_ms"},
    "pipeline_transition": {"phase", "from_status", "to_status", "status"},
    "guided_help": {"action", "tour_surface", "step_kind", "status", "duration_ms"},
}

GUIDED_HELP_ALLOWED_VALUES: dict[str, set[str]] = {
    "action": {"viewed", "step_completed", "skipped_step", "skipped_all", "completed", "replayed", "reset"},
    "tour_surface": {"board", "specs", "tasks", "kg", "metrics", "agents", "help"},
    "step_kind": {"navigation", "feature", "settings", "validation", "replay"},
    "status": {"success", "skipped", "disabled", "fallback"},
}
GUIDED_HELP_REQUIRED_KEYS = frozenset(GUIDED_HELP_ALLOWED_VALUES)

FORBIDDEN_KEY_RE = re.compile(
    r"(^|_)(id|ids|title|name|email|mail|path|query|payload|body|prompt|"
    r"content|stack|trace|token|secret|password|ip|address|url|uri)$",
    re.IGNORECASE,
)
EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
PATH_RE = re.compile(r"(^[A-Za-z]:\\|^/[^/]|\\|/\w+/\w+)")
QUERY_RE = re.compile(r"\?.+=|&.+=|https?://", re.IGNORECASE)
SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_.:/{} -]{1,120}$")


class SchemaReject(ValueError):
    """Raised when a telemetry event cannot be made safe."""


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    """Stable JSON used by client and backend HMAC contracts."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def runtime_payload(deployment: str = "pypi") -> dict[str, str]:
    return {
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}",
        "platform": platform.system().lower() or "unknown",
        "deployment": deployment,
    }


def _safe_scalar(value: Any, *, key: str = "") -> str | int | float | bool | None:
    if key == "duration_ms":
        return _safe_duration_ms(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    text = str(value).strip()
    if not text:
        return None
    if key == "route_template":
        if EMAIL_RE.search(text) or UUID_RE.search(text) or QUERY_RE.search(text) or not SAFE_TOKEN_RE.match(text):
            raise SchemaReject("payload value rejected by anti-exfiltration policy")
        return text[:120]
    if EMAIL_RE.search(text) or UUID_RE.search(text) or PATH_RE.search(text) or QUERY_RE.search(text) or not SAFE_TOKEN_RE.match(text):
        raise SchemaReject("payload value rejected by anti-exfiltration policy")
    return text[:120]


def _safe_duration_ms(value: Any) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise SchemaReject("duration_ms must be numeric")
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)) or value < 0:
            raise SchemaReject("duration_ms must be a non-negative finite number")
        return int(value) if isinstance(value, float) and value.is_integer() else value
    text = str(value).strip()
    if not text:
        return None
    try:
        number = float(text) if "." in text else int(text)
    except ValueError as exc:
        raise SchemaReject("duration_ms must be numeric") from exc
    if not math.isfinite(float(number)) or number < 0:
        raise SchemaReject("duration_ms must be a non-negative finite number")
    return int(number) if isinstance(number, float) and number.is_integer() else number


def sanitize_payload(event_type: str, payload: dict[str, Any] | None) -> tuple[dict[str, Any], int]:
    """Return a closed-schema payload and count dropped/rejected fields."""
    if event_type not in ALLOWED_PAYLOAD_KEYS:
        raise SchemaReject(f"unsupported telemetry event_type: {event_type}")
    allowed = ALLOWED_PAYLOAD_KEYS[event_type]
    safe: dict[str, Any] = {}
    rejected = 0
    for key, value in (payload or {}).items():
        if key not in allowed or FORBIDDEN_KEY_RE.search(key):
            rejected += 1
            continue
        try:
            scalar = _safe_scalar(value, key=key)
        except SchemaReject:
            rejected += 1
            continue
        if event_type == "guided_help" and key in GUIDED_HELP_ALLOWED_VALUES:
            if not isinstance(scalar, str) or scalar not in GUIDED_HELP_ALLOWED_VALUES[key]:
                rejected += 1
                continue
        if scalar is not None:
            safe[key] = scalar
    if event_type == "guided_help" and not GUIDED_HELP_REQUIRED_KEYS.issubset(safe):
        raise SchemaReject("guided_help payload missing required categorical fields")
    return safe, rejected


def count_rejected_payload_fields(event_type: str, payload: dict[str, Any] | None) -> int:
    """Count payload fields that would be dropped by the closed schema."""
    if event_type not in ALLOWED_PAYLOAD_KEYS:
        return len(payload or {}) or 1
    allowed = ALLOWED_PAYLOAD_KEYS[event_type]
    rejected = 0
    for key, value in (payload or {}).items():
        if key not in allowed or FORBIDDEN_KEY_RE.search(key):
            rejected += 1
            continue
        try:
            scalar = _safe_scalar(value, key=key)
        except SchemaReject:
            rejected += 1
            continue
        if event_type == "guided_help" and key in GUIDED_HELP_ALLOWED_VALUES:
            if not isinstance(scalar, str) or scalar not in GUIDED_HELP_ALLOWED_VALUES[key]:
                rejected += 1
    return rejected


def normalize_event(
    event_type: str,
    payload: dict[str, Any] | None = None,
    *,
    app_version: str = "0.0.0+local",
    schema_version: str = CURRENT_SCHEMA_VERSION,
    deployment: str = "pypi",
    occurred_at: str | None = None,
) -> tuple[dict[str, Any], int]:
    """Normalize an event for local JSONL storage.

    The returned event is safe to persist locally and to aggregate later. Raw
    board/card/spec IDs, titles, paths, query strings, tool payloads and PII are
    dropped before persistence.
    """
    clean_payload, rejected = sanitize_payload(event_type, payload)
    event = {
        "schema_version": schema_version,
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": occurred_at or now_utc(),
        "runtime": runtime_payload(deployment),
        "app_version": app_version,
        "payload": clean_payload,
    }
    return event, rejected
