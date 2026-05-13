"""Closed telemetry schema and anti-exfiltration helpers."""

from __future__ import annotations

import json
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
)
TelemetryEventType = Literal[
    "cli",
    "http",
    "mcp",
    "kg",
    "lifecycle",
    "pipeline_transition",
]

ALLOWED_PAYLOAD_KEYS: dict[str, set[str]] = {
    "cli": {"command", "exit_code", "duration_ms", "error_class"},
    "http": {"method", "route_template", "status_code", "duration_ms", "error_class"},
    "mcp": {"tool_name", "status", "duration_ms", "error_class"},
    "kg": {"operation", "status", "duration_ms", "node_type", "result_count"},
    "lifecycle": {"action", "status", "duration_ms"},
    "pipeline_transition": {"phase", "from_status", "to_status", "status"},
}

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
        if scalar is not None:
            safe[key] = scalar
    return safe, rejected


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
