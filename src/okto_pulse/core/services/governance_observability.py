"""Safe audit and metrics helpers for board governance events."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import re
from typing import Any, Mapping

from okto_pulse.core.observability.sample_buffer import runtime_sample_buffer
from okto_pulse.core.runtime_context import runtime_lock


logger = logging.getLogger(__name__)

METRIC_BOARD_GOVERNANCE_SETTING_CHANGED = "board_governance_setting_changed_total"
METRIC_QA_SELF_ANSWER_DENIED = "qa_self_answer_denied_total"
METRIC_CRITICAL_CONTEXT_GUARD_DECISION = "critical_context_guard_decision_total"
METRIC_CRITICAL_CONTEXT_RESOLUTION_FAILURE = "critical_context_resolution_failure_total"
METRIC_CRITICAL_CONTEXT_RESOLUTION_LATENCY_MS = "critical_context_resolution_latency_ms"
METRIC_BOARD_MISSING_CONTEXT_WARNING = "board_missing_context_warning_total"
METRIC_GOVERNANCE_AUDIT_SAFE_LABEL_VIOLATION = (
    "governance_audit_safe_label_violation_total"
)

GOVERNANCE_METRIC_NAMES = frozenset(
    {
        METRIC_BOARD_GOVERNANCE_SETTING_CHANGED,
        METRIC_QA_SELF_ANSWER_DENIED,
        METRIC_CRITICAL_CONTEXT_GUARD_DECISION,
        METRIC_CRITICAL_CONTEXT_RESOLUTION_FAILURE,
        METRIC_CRITICAL_CONTEXT_RESOLUTION_LATENCY_MS,
        METRIC_BOARD_MISSING_CONTEXT_WARNING,
        METRIC_GOVERNANCE_AUDIT_SAFE_LABEL_VIOLATION,
    }
)

_METRIC_LABEL_KEYS: dict[str, frozenset[str]] = {
    METRIC_BOARD_GOVERNANCE_SETTING_CHANGED: frozenset(
        {
            "board_id",
            "actor_id",
            "setting_key",
            "old_effective_value",
            "new_effective_value",
            "surface",
            "outcome",
        }
    ),
    METRIC_QA_SELF_ANSWER_DENIED: frozenset(
        {
            "board_id",
            "actor_id",
            "entity_type",
            "question_id",
            "reason",
            "surface",
            "outcome",
        }
    ),
    METRIC_CRITICAL_CONTEXT_GUARD_DECISION: frozenset(
        {
            "board_id",
            "actor_id",
            "entity_type",
            "critical_action",
            "surface",
            "outcome",
            "reason",
            "context_profile",
        }
    ),
    METRIC_CRITICAL_CONTEXT_RESOLUTION_FAILURE: frozenset(
        {
            "board_id",
            "entity_type",
            "critical_action",
            "resolver_name",
            "failure_reason",
            "surface",
            "outcome",
        }
    ),
    METRIC_CRITICAL_CONTEXT_RESOLUTION_LATENCY_MS: frozenset(
        {
            "board_id",
            "entity_type",
            "critical_action",
            "surface",
            "outcome",
            "context_profile",
        }
    ),
    METRIC_BOARD_MISSING_CONTEXT_WARNING: frozenset(
        {
            "board_id",
            "warning_code",
            "surface",
            "outcome",
        }
    ),
    METRIC_GOVERNANCE_AUDIT_SAFE_LABEL_VIOLATION: frozenset(
        {
            "event_type",
            "violation_kind",
            "surface",
            "outcome",
        }
    ),
}

_AUDIT_EXTRA_KEYS: dict[str, frozenset[str]] = {
    METRIC_CRITICAL_CONTEXT_GUARD_DECISION: frozenset(
        {"entity_id", "context_fingerprint", "context_resolved_at"}
    ),
}

_SAFE_LABEL_VALUE_RE = re.compile(r"^[A-Za-z0-9_.:/-]{1,160}$")
_FORBIDDEN_VALUE_FRAGMENTS = (
    "password",
    "secret",
    "token",
    "bearer ",
    "description",
    "guideline",
    "question:",
    "answer:",
    "payload",
    "context body",
)

_METRIC_SAMPLES_LOCK = runtime_lock("services.governance.samples")
_METRIC_SAMPLES = runtime_sample_buffer("services.governance")
_TOKEN_RE = re.compile(r"[^A-Za-z0-9_.:/-]+")


class GovernanceAuditPayloadError(ValueError):
    """Raised when a governance audit/metric payload is unsafe."""


@dataclass(frozen=True)
class GovernanceMetricEvent:
    metric_name: str
    value: int | float = 1
    labels: dict[str, Any] = field(default_factory=dict)


class GovernanceMetricsSink:
    """Default governance metric sink backed by logs and test-visible samples."""

    def emit(self, event: GovernanceMetricEvent) -> None:
        safe = sanitize_governance_metric_event(event)
        with _METRIC_SAMPLES_LOCK:
            _METRIC_SAMPLES.append(
                {
                    "metric_name": safe.metric_name,
                    "value": safe.value,
                    "labels": dict(safe.labels),
                }
            )
        logger.info(
            "governance.metric name=%s value=%s",
            safe.metric_name,
            safe.value,
            extra={
                "event": "governance.metric",
                "metric_name": safe.metric_name,
                "value": safe.value,
                **safe.labels,
            },
        )


_DEFAULT_METRICS_SINK = GovernanceMetricsSink()


def reset_governance_metric_samples() -> None:
    with _METRIC_SAMPLES_LOCK:
        _METRIC_SAMPLES.clear()


def get_governance_metric_samples() -> list[dict[str, Any]]:
    with _METRIC_SAMPLES_LOCK:
        return [
            {
                "metric_name": item["metric_name"],
                "value": item["value"],
                "labels": dict(item["labels"]),
            }
            for item in _METRIC_SAMPLES.snapshot()
        ]


def sanitize_governance_metric_event(
    event: GovernanceMetricEvent,
) -> GovernanceMetricEvent:
    try:
        return _sanitize_governance_metric_event(event)
    except GovernanceAuditPayloadError as exc:
        _emit_safe_label_violation(
            event_type=event.metric_name or "unknown_metric",
            violation_kind=_violation_kind(exc),
            surface=str(event.labels.get("surface") or "unknown"),
        )
        raise


def _sanitize_governance_metric_event(
    event: GovernanceMetricEvent,
) -> GovernanceMetricEvent:
    if event.metric_name not in GOVERNANCE_METRIC_NAMES:
        raise GovernanceAuditPayloadError(
            f"unsupported_governance_metric: {event.metric_name}"
        )

    allowed = _METRIC_LABEL_KEYS[event.metric_name]
    provided = set(event.labels)
    if provided != allowed:
        extra = sorted(provided - allowed)
        missing = sorted(allowed - provided)
        raise GovernanceAuditPayloadError(
            "governance_metric_label_set_mismatch: "
            f"metric={event.metric_name} extra={extra} missing={missing}"
        )

    return GovernanceMetricEvent(
        metric_name=event.metric_name,
        value=event.value,
        labels={key: _safe_label_value(key, value) for key, value in event.labels.items()},
    )


def governance_metric_labels_from_audit(details: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return _governance_metric_labels_from_audit(details)
    except GovernanceAuditPayloadError as exc:
        _emit_safe_label_violation(
            event_type=str(details.get("metric_name") or "unknown_metric"),
            violation_kind=_violation_kind(exc),
            surface=str(details.get("surface") or "unknown"),
        )
        raise


def _governance_metric_labels_from_audit(details: Mapping[str, Any]) -> dict[str, Any]:
    metric_name = _metric_name(details)
    allowed = _METRIC_LABEL_KEYS[metric_name]
    allowed_audit = {"metric_name"} | set(allowed) | set(
        _AUDIT_EXTRA_KEYS.get(metric_name, frozenset())
    )
    extra = sorted(set(details) - allowed_audit)
    if extra:
        raise GovernanceAuditPayloadError(
            f"governance_metric_payload_extra_keys: metric={metric_name} extra={extra}"
        )
    missing = sorted(key for key in allowed if key not in details)
    if missing:
        raise GovernanceAuditPayloadError(
            f"governance_metric_labels_missing: metric={metric_name} missing={missing}"
        )
    return {
        "metric_name": metric_name,
        **{key: details[key] for key in allowed},
    }


def emit_governance_metric(
    details_or_labels: Mapping[str, Any],
    *,
    value: int | float = 1,
    metrics_sink: GovernanceMetricsSink | None = None,
    raise_on_violation: bool = True,
) -> None:
    try:
        payload = governance_metric_labels_from_audit(details_or_labels)
        metric_name = str(payload.pop("metric_name"))
        sink = metrics_sink or _DEFAULT_METRICS_SINK
        sink.emit(GovernanceMetricEvent(metric_name, value, payload))
    except GovernanceAuditPayloadError:
        if raise_on_violation:
            raise


def validate_governance_audit_details(details: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return _validate_governance_audit_details(details)
    except GovernanceAuditPayloadError as exc:
        _emit_safe_label_violation(
            event_type=str(details.get("metric_name") or "unknown_metric"),
            violation_kind=_violation_kind(exc),
            surface=str(details.get("surface") or "unknown"),
        )
        raise


def _validate_governance_audit_details(details: Mapping[str, Any]) -> dict[str, Any]:
    metric_name = _metric_name(details)
    required = {"metric_name"} | set(_METRIC_LABEL_KEYS[metric_name])
    optional = set(_AUDIT_EXTRA_KEYS.get(metric_name, frozenset()))
    allowed = required | optional
    provided = set(details)
    if not required.issubset(provided) or not provided.issubset(allowed):
        extra = sorted(provided - allowed)
        missing = sorted(required - provided)
        raise GovernanceAuditPayloadError(
            "governance_audit_label_set_mismatch: "
            f"metric={metric_name} extra={extra} missing={missing}"
        )
    return {
        key: _safe_label_value(key, value, audit_key=True)
        for key, value in details.items()
    }


def build_board_governance_setting_changed_details(
    *,
    board_id: str,
    actor_id: str,
    setting_key: str,
    old_effective_value: bool,
    new_effective_value: bool,
    surface: str,
    outcome: str = "changed",
) -> dict[str, Any]:
    return validate_governance_audit_details(
        {
            "metric_name": METRIC_BOARD_GOVERNANCE_SETTING_CHANGED,
            "board_id": board_id,
            "actor_id": actor_id,
            "setting_key": setting_key,
            "old_effective_value": old_effective_value,
            "new_effective_value": new_effective_value,
            "surface": surface,
            "outcome": outcome,
        }
    )


def build_qa_self_answer_denied_details(
    *,
    board_id: str,
    actor_id: str,
    entity_type: str,
    question_id: str,
    surface: str,
) -> dict[str, Any]:
    return validate_governance_audit_details(
        {
            "metric_name": METRIC_QA_SELF_ANSWER_DENIED,
            "board_id": board_id,
            "actor_id": actor_id,
            "entity_type": entity_type,
            "question_id": question_id,
            "reason": "self_answering_not_allowed",
            "surface": surface,
            "outcome": "deny",
        }
    )


def build_critical_context_decision_metric_labels(
    *,
    board_id: str,
    actor_id: str,
    entity_type: str,
    critical_action: str,
    surface: str,
    outcome: str,
    reason: str,
    context_profile: str,
) -> dict[str, Any]:
    labels = {
        "metric_name": METRIC_CRITICAL_CONTEXT_GUARD_DECISION,
        "board_id": board_id,
        "actor_id": actor_id,
        "entity_type": entity_type,
        "critical_action": critical_action,
        "surface": surface,
        "outcome": outcome,
        "reason": reason,
        "context_profile": context_profile,
    }
    return governance_metric_labels_from_audit(labels)


def build_critical_context_decision_audit_details(
    *,
    board_id: str,
    actor_id: str,
    entity_type: str,
    entity_id: str,
    critical_action: str,
    surface: str,
    outcome: str,
    reason: str,
    context_profile: str,
    context_fingerprint: str,
    context_resolved_at: str,
) -> dict[str, Any]:
    details = {
        "metric_name": METRIC_CRITICAL_CONTEXT_GUARD_DECISION,
        "board_id": board_id,
        "actor_id": actor_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "critical_action": critical_action,
        "surface": surface,
        "outcome": outcome,
        "reason": reason,
        "context_profile": context_profile,
    }
    if context_fingerprint:
        details["context_fingerprint"] = context_fingerprint
    if context_resolved_at:
        details["context_resolved_at"] = context_resolved_at
    return validate_governance_audit_details(details)


def build_critical_context_resolution_failure_metric_labels(
    *,
    board_id: str,
    entity_type: str,
    critical_action: str,
    resolver_name: str,
    failure_reason: str,
    surface: str,
    outcome: str = "deny",
) -> dict[str, Any]:
    return governance_metric_labels_from_audit(
        {
            "metric_name": METRIC_CRITICAL_CONTEXT_RESOLUTION_FAILURE,
            "board_id": board_id,
            "entity_type": entity_type,
            "critical_action": critical_action,
            "resolver_name": resolver_name,
            "failure_reason": failure_reason,
            "surface": surface,
            "outcome": outcome,
        }
    )


def build_critical_context_resolution_latency_metric_labels(
    *,
    board_id: str,
    entity_type: str,
    critical_action: str,
    surface: str,
    outcome: str,
    context_profile: str,
) -> dict[str, Any]:
    return governance_metric_labels_from_audit(
        {
            "metric_name": METRIC_CRITICAL_CONTEXT_RESOLUTION_LATENCY_MS,
            "board_id": board_id,
            "entity_type": entity_type,
            "critical_action": critical_action,
            "surface": surface,
            "outcome": outcome,
            "context_profile": context_profile,
        }
    )


def build_board_missing_context_warning_details(
    *,
    board_id: str,
    warning_code: str,
    surface: str,
    outcome: str = "warning",
) -> dict[str, Any]:
    return validate_governance_audit_details(
        {
            "metric_name": METRIC_BOARD_MISSING_CONTEXT_WARNING,
            "board_id": board_id,
            "warning_code": warning_code,
            "surface": surface,
            "outcome": outcome,
        }
    )


def build_governance_safe_label_violation_details(
    *,
    event_type: str,
    violation_kind: str,
    surface: str,
    outcome: str = "rejected",
) -> dict[str, Any]:
    return validate_governance_audit_details(
        {
            "metric_name": METRIC_GOVERNANCE_AUDIT_SAFE_LABEL_VIOLATION,
            "event_type": event_type,
            "violation_kind": violation_kind,
            "surface": surface,
            "outcome": outcome,
        }
    )


def _metric_name(details: Mapping[str, Any]) -> str:
    metric_name = str(details.get("metric_name") or "")
    if metric_name not in GOVERNANCE_METRIC_NAMES:
        raise GovernanceAuditPayloadError(
            f"unsupported_governance_metric: {metric_name}"
        )
    return metric_name


def _safe_label_value(key: str, value: Any, *, audit_key: bool = False) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value
    if value is None:
        return value
    if not isinstance(value, str):
        raise GovernanceAuditPayloadError(
            f"unsafe_governance_label_value_type: key={key} type={type(value).__name__}"
        )
    lowered = value.lower()
    if any(fragment in lowered for fragment in _FORBIDDEN_VALUE_FRAGMENTS):
        raise GovernanceAuditPayloadError(f"unsafe_governance_label_value: key={key}")
    if "\n" in value or "\r" in value or "{" in value or "}" in value or "<" in value or ">" in value:
        raise GovernanceAuditPayloadError(f"unsafe_governance_label_value: key={key}")
    if audit_key and key in {"context_fingerprint", "context_resolved_at"}:
        if len(value) > 256 or "\n" in value or "\r" in value:
            raise GovernanceAuditPayloadError(
                f"unsafe_governance_audit_value: key={key}"
            )
        return value
    if not _SAFE_LABEL_VALUE_RE.match(value):
        raise GovernanceAuditPayloadError(f"unsafe_governance_label_value: key={key}")
    return value


def _violation_kind(exc: GovernanceAuditPayloadError) -> str:
    message = str(exc) or "unsafe_payload"
    return _safe_token(message.split(":", 1)[0] or "unsafe_payload")


def _safe_token(value: str) -> str:
    cleaned = _TOKEN_RE.sub("_", value.strip().lower()).strip("_")
    return (cleaned or "unknown")[:80]


def _emit_safe_label_violation(
    *,
    event_type: str,
    violation_kind: str,
    surface: str,
) -> None:
    labels = {
        "event_type": _safe_token(event_type),
        "violation_kind": _safe_token(violation_kind),
        "surface": _safe_token(surface),
        "outcome": "rejected",
    }
    with _METRIC_SAMPLES_LOCK:
        _METRIC_SAMPLES.append(
            {
                "metric_name": METRIC_GOVERNANCE_AUDIT_SAFE_LABEL_VIOLATION,
                "value": 1,
                "labels": labels,
            }
        )
    logger.warning(
        "governance.metric violation=%s event_type=%s",
        labels["violation_kind"],
        labels["event_type"],
        extra={
            "event": "governance.metric.safe_label_violation",
            "metric_name": METRIC_GOVERNANCE_AUDIT_SAFE_LABEL_VIOLATION,
            "value": 1,
            **labels,
        },
    )
