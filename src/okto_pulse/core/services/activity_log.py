"""Shared activity-log presentation helpers.

The ActivityLog table stores structured JSON details for auditability. API and
MCP callers need concise, deterministic summaries without leaking raw payloads
or relying on Python object stringification.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from enum import Enum
from typing import Any


SUMMARY_MAX_CHARS = 200
DETAIL_STRING_MAX_CHARS = 500
DETAIL_COLLECTION_MAX_ITEMS = 40
DETAIL_MAX_DEPTH = 6
DETAIL_MAX_JSON_CHARS = 12000

_SENSITIVE_KEY_FRAGMENTS = (
    "api_key",
    "authorization",
    "bearer",
    "credential",
    "email",
    "jwt",
    "password",
    "secret",
    "token",
)


def _clip(text: str, max_chars: int = SUMMARY_MAX_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return text[:max_chars]
    return text[: max_chars - 3] + "..."


def _scalar_text(value: Any, max_chars: int = 80) -> str:
    """Return a safe scalar string; never stringify dict/list/object payloads."""
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return _clip(str(value), max_chars)
    return ""


def _first_scalar(details: Mapping[str, Any], *keys: str, max_chars: int = 80) -> str:
    for key in keys:
        value = _scalar_text(details.get(key), max_chars=max_chars)
        if value:
            return value
    return ""


def activity_log_trigger(details: Any) -> str | None:
    """Extract the activity trigger from a details payload, when present."""
    if not isinstance(details, Mapping):
        return None
    trigger = _scalar_text(details.get("trigger"), max_chars=120)
    return trigger or None


def activity_log_changes(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    fields: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Build the canonical field-level ``old``/``new`` activity diff.

    Values are normalized into detached JSON-safe structures so a later
    in-memory mutation cannot rewrite the audit payload. This matches the
    existing Spec history contract consumed by the UI.
    """
    changes: list[dict[str, Any]] = []
    for field in fields if fields is not None else after:
        old_value = _activity_change_value(before.get(field))
        new_value = _activity_change_value(after.get(field))
        if old_value != new_value:
            changes.append(
                {"field": field, "old": old_value, "new": new_value}
            )
    return changes


def _activity_change_value(value: Any) -> Any:
    """Detach and normalize one value for a durable activity diff."""
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    if isinstance(value, Enum):
        return _activity_change_value(value.value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _activity_change_value(item)
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        return [_activity_change_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def activity_log_summary(action: str, details: Any) -> str:
    """Build a deterministic one-line summary string for an activity log row.

    Format is stable within a major API version. Summaries are intentionally
    compact and never call ``str()`` on arbitrary nested structures.
    """
    if not isinstance(details, Mapping):
        return ""

    if action == "card_moved":
        frm = _scalar_text(details.get("from_status"))
        to = _scalar_text(details.get("to_status"))
        card_id = _scalar_text(details.get("card_id"), max_chars=64)
        if frm and to:
            base = f"{frm}->{to}"
        else:
            status = _scalar_text(details.get("status")) or "?"
            base = f"status={status}"
        if card_id:
            base = f"card={card_id[:8]} {base}"
        return _clip(base)

    if action in ("card_created", "card_updated", "card_deleted"):
        title = _scalar_text(details.get("title"), max_chars=60)
        status = _scalar_text(details.get("status"))
        parts = [action.replace("card_", "")]
        if title:
            parts.append(f'"{title}"')
        if status:
            parts.append(f"status={status}")
        return _clip(" ".join(parts))

    if action == "validation_submitted":
        outcome = _scalar_text(details.get("outcome")) or "?"
        card_title = _scalar_text(details.get("card_title"), max_chars=50)
        confidence = _scalar_text(details.get("confidence"))
        parts = [f"outcome={outcome}"]
        if card_title:
            parts.append(f'card="{card_title}"')
        if confidence:
            parts.append(f"confidence={confidence}")
        return _clip(" ".join(parts))

    if action in ("spec_validation_submitted", "spec_validated"):
        outcome = _scalar_text(details.get("outcome")) or "?"
        spec_id = _scalar_text(details.get("spec_id"), max_chars=64)
        frm = _scalar_text(details.get("from_status"))
        to = _scalar_text(details.get("to_status"))
        parts = [f"outcome={outcome}"]
        if spec_id:
            parts.append(f"spec={spec_id[:8]}")
        if frm and to:
            parts.append(f"{frm}->{to}")
        return _clip(" ".join(parts))

    if action == "task_validated":
        outcome = _scalar_text(details.get("outcome")) or "?"
        card_id = _scalar_text(details.get("card_id"), max_chars=64)
        parts = [f"task outcome={outcome}"]
        if card_id:
            parts.append(f"card={card_id[:8]}")
        return _clip(" ".join(parts))

    if action in (
        "ideation_created",
        "spec_created",
        "refinement_created",
        "sprint_created",
    ):
        resource = action.replace("_created", "")
        resource_id = (
            _scalar_text(details.get(f"{resource}_id"), max_chars=64)
            or _scalar_text(details.get("id"), max_chars=64)
        )
        title = _scalar_text(details.get("title"), max_chars=60)
        parts = [f"{resource} created"]
        if title:
            parts.append(f'"{title}"')
        if resource_id:
            parts.append(f"id={resource_id[:8]}")
        return _clip(": ".join([parts[0], " ".join(parts[1:])]) if len(parts) > 1 else parts[0])

    if action in ("ideation_moved", "spec_moved", "refinement_moved"):
        frm = _scalar_text(details.get("from_status")) or "?"
        to = _scalar_text(details.get("to_status")) or "?"
        resource = action.replace("_moved", "")
        resource_id = (
            _scalar_text(details.get(f"{resource}_id"), max_chars=64)
            or _scalar_text(details.get("id"), max_chars=64)
        )
        parts = [f"{resource} {frm}->{to}"]
        if resource_id:
            parts.append(f"id={resource_id[:8]}")
        return _clip(" ".join(parts))

    if action in ("comment_added", "choice_comment_added"):
        content = _scalar_text(details.get("content"), max_chars=80)
        question = _scalar_text(details.get("question"), max_chars=80)
        text = question or content
        if action == "choice_comment_added":
            opt_count = _scalar_text(details.get("option_count")) or "?"
            base = f"choice_comment options={opt_count}"
        else:
            base = "comment"
        if text:
            base += f': "{text}"'
        return _clip(base)

    if action == "spec_resources_auto_propagated":
        return _summarize_spec_resource_propagation(details)

    structured = _summarize_structured_object(action, details)
    if structured:
        return structured

    domain = _summarize_known_domain(action, details)
    if domain:
        return domain

    trigger = activity_log_trigger(details)
    if trigger:
        return _clip(f"trigger={trigger}")
    return ""


def _summarize_spec_resource_propagation(details: Mapping[str, Any]) -> str:
    results = details.get("results") or {}
    parts: list[str] = []
    trigger = activity_log_trigger(details)
    if trigger:
        parts.append(f"trigger={trigger}")
    if not isinstance(results, Mapping):
        return _clip(" ".join(parts))

    for rtype in ("knowledge_base", "architecture", "mockup"):
        r = results.get(rtype)
        if not isinstance(r, Mapping):
            continue
        short = "kb" if rtype == "knowledge_base" else ("arch" if rtype == "architecture" else "mockup")
        copied = _scalar_text(r.get("copied_count")) or "0"
        ignored = _scalar_text(r.get("ignored_count")) or "0"
        removed = _scalar_text(r.get("removed_count")) or "0"
        piece = f"{short}.copied={copied}"
        if ignored != "0":
            piece += f" {short}.ignored={ignored}"
        if removed != "0":
            piece += f" {short}.removed={removed}"
        parts.append(piece)
    return _clip(" ".join(parts))


def _summarize_structured_object(action: str, details: Mapping[str, Any]) -> str:
    normalized = action.replace(".", "_").replace("-", "_")
    trigger = activity_log_trigger(details) or ""
    if not (
        normalized.startswith("structured_")
        or trigger.startswith("structured_")
        or _first_scalar(details, "entity_type", "field", "field_name")
    ):
        return ""

    entity = _first_scalar(
        details,
        "entity_type",
        "target_entity_type",
        "resource_type",
        max_chars=40,
    )
    operation = _first_scalar(
        details,
        "operation",
        "op",
        "change_type",
        max_chars=40,
    )
    if not operation and "_" in normalized:
        operation = normalized.rsplit("_", 1)[-1]
    field = _first_scalar(details, "field", "field_name", "attribute", max_chars=50)
    entity_id = _first_scalar(
        details,
        "entity_id",
        "target_id",
        "resource_id",
        "id",
        max_chars=64,
    )
    changed_count = ""
    changed_fields = details.get("changed_fields")
    if isinstance(changed_fields, Sequence) and not isinstance(changed_fields, (str, bytes, bytearray)):
        changed_count = str(len(changed_fields))

    parts = ["structured_entity"]
    if operation:
        parts.append(operation)
    if entity:
        parts.append(f"type={entity}")
    if entity_id:
        parts.append(f"id={entity_id[:8]}")
    if field:
        parts.append(f"field={field}")
    if changed_count:
        parts.append(f"fields={changed_count}")
    if trigger and not trigger.startswith("structured_"):
        parts.append(f"trigger={trigger}")
    return _clip(" ".join(parts))


def _summarize_known_domain(action: str, details: Mapping[str, Any]) -> str:
    normalized = action.replace(".", "_").replace("-", "_")
    trigger = activity_log_trigger(details) or ""
    for domain in ("kg", "metrics", "architecture"):
        if normalized.startswith(f"{domain}_") or trigger.startswith(f"{domain}_"):
            return _clip(_domain_parts(domain, action, details, trigger))
    return ""


def _domain_parts(domain: str, action: str, details: Mapping[str, Any], trigger: str) -> str:
    operation = _first_scalar(details, "operation", "event", "kind", max_chars=50)
    status = _first_scalar(details, "status", "outcome", "state", max_chars=50)
    reason = _first_scalar(details, "reason", "error_code", max_chars=60)
    target = _first_scalar(
        details,
        "entity_type",
        "resource_type",
        "target_type",
        "metric_name",
        "setting",
        max_chars=50,
    )
    count = _first_scalar(
        details,
        "count",
        "issue_count",
        "blocking_issue_count",
        "changed_count",
        max_chars=20,
    )
    parts = [domain, action]
    if trigger:
        parts.append(f"trigger={trigger}")
    if operation:
        parts.append(f"op={operation}")
    if status:
        parts.append(f"status={status}")
    if target:
        parts.append(f"target={target}")
    if count:
        parts.append(f"count={count}")
    if reason:
        parts.append(f"reason={reason}")
    return " ".join(parts)


def sanitize_activity_details(details: Any) -> dict[str, Any] | None:
    """Return a JSON-safe, bounded, secret-masked copy of activity details."""
    if not isinstance(details, Mapping):
        return None
    sanitized = _sanitize_value(details, depth=0)
    if not isinstance(sanitized, dict):
        return None
    return _bound_serialized_dict(sanitized)


def _sanitize_value(value: Any, *, depth: int) -> Any:
    if depth >= DETAIL_MAX_DEPTH:
        return "[truncated:max_depth]"

    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        items = list(value.items())
        for key, item_value in items[:DETAIL_COLLECTION_MAX_ITEMS]:
            key_text = str(key)
            if _is_sensitive_key(key_text):
                result[key_text] = "[redacted]"
            else:
                result[key_text] = _sanitize_value(item_value, depth=depth + 1)
        omitted = len(items) - DETAIL_COLLECTION_MAX_ITEMS
        if omitted > 0:
            result["__truncated__"] = f"{omitted} keys omitted"
        return result

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        values = list(value)
        result = [
            _sanitize_value(item, depth=depth + 1)
            for item in values[:DETAIL_COLLECTION_MAX_ITEMS]
        ]
        omitted = len(values) - DETAIL_COLLECTION_MAX_ITEMS
        if omitted > 0:
            result.append(f"[truncated:{omitted} items omitted]")
        return result

    if isinstance(value, str):
        return _clip(value, DETAIL_STRING_MAX_CHARS)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return f"[unsupported:{type(value).__name__}]"


def _is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return any(fragment in lowered for fragment in _SENSITIVE_KEY_FRAGMENTS)


def _bound_serialized_dict(payload: dict[str, Any]) -> dict[str, Any]:
    serialized = json.dumps(payload, default=str, ensure_ascii=False, sort_keys=True)
    if len(serialized) <= DETAIL_MAX_JSON_CHARS:
        return payload
    return {
        "__truncated__": (
            f"details exceeded {DETAIL_MAX_JSON_CHARS} serialized characters"
        ),
        "preview": _clip(serialized, DETAIL_MAX_JSON_CHARS),
    }
