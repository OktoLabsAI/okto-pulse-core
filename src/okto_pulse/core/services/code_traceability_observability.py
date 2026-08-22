"""Cardinality-safe observability for Code Traceability."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import math
import re
from typing import Any, Mapping, Protocol

from okto_pulse.core.observability.sample_buffer import runtime_sample_buffer
from okto_pulse.core.runtime_context import runtime_lock


logger = logging.getLogger(__name__)

METRIC_CODE_INVESTIGATION_RECEIPT_TOTAL = "code_investigation_receipt_total"
METRIC_CODE_INVESTIGATION_RECEIPT_REJECTED_TOTAL = (
    "code_investigation_receipt_rejected_total"
)
METRIC_CODE_INVESTIGATION_RECEIPT_AGE_SECONDS = (
    "code_investigation_receipt_age_seconds"
)
METRIC_CODE_EVIDENCE_SUBMISSION_TOTAL = "code_evidence_submission_total"
METRIC_CODE_EVIDENCE_ATTESTATION_TOTAL = "code_evidence_attestation_total"
METRIC_CODE_EVIDENCE_DISPOSITION_TOTAL = "code_evidence_disposition_total"
METRIC_IMPLEMENTATION_TARGET_CREATED_TOTAL = "implementation_target_created_total"
METRIC_IMPLEMENTATION_TARGET_RESOLUTION_RECEIPT_TOTAL = (
    "implementation_target_resolution_receipt_total"
)
METRIC_IMPLEMENTATION_TARGET_RESOLUTION_SUBMISSION_DURATION_SECONDS = (
    "implementation_target_resolution_submission_duration_seconds"
)
METRIC_IMPLEMENTATION_OVERLAP_TOTAL = "implementation_overlap_total"
METRIC_IMPLEMENTATION_OVERLAP_ACKNOWLEDGED_TOTAL = (
    "implementation_overlap_acknowledged_total"
)
METRIC_CODE_TRACEABILITY_GATE_TOTAL = "code_traceability_gate_total"
METRIC_CODE_TRACEABILITY_GATE_BLOCKER_TOTAL = (
    "code_traceability_gate_blocker_total"
)

CODE_TRACEABILITY_METRIC_NAMES = frozenset(
    {
        METRIC_CODE_INVESTIGATION_RECEIPT_TOTAL,
        METRIC_CODE_INVESTIGATION_RECEIPT_REJECTED_TOTAL,
        METRIC_CODE_INVESTIGATION_RECEIPT_AGE_SECONDS,
        METRIC_CODE_EVIDENCE_SUBMISSION_TOTAL,
        METRIC_CODE_EVIDENCE_ATTESTATION_TOTAL,
        METRIC_CODE_EVIDENCE_DISPOSITION_TOTAL,
        METRIC_IMPLEMENTATION_TARGET_CREATED_TOTAL,
        METRIC_IMPLEMENTATION_TARGET_RESOLUTION_RECEIPT_TOTAL,
        METRIC_IMPLEMENTATION_TARGET_RESOLUTION_SUBMISSION_DURATION_SECONDS,
        METRIC_IMPLEMENTATION_OVERLAP_TOTAL,
        METRIC_IMPLEMENTATION_OVERLAP_ACKNOWLEDGED_TOTAL,
        METRIC_CODE_TRACEABILITY_GATE_TOTAL,
        METRIC_CODE_TRACEABILITY_GATE_BLOCKER_TOTAL,
    }
)

CODE_TRACEABILITY_METRIC_LABELS = frozenset(
    {
        "outcome",
        "state",
        "evidence_type",
        "selector_kind",
        "role",
        "tooling_family",
        "trust_level",
        "language",
        "gate",
        "reason_code",
        "overlap_severity",
    }
)

_MAX_LABEL_VALUE_CHARS = 64
_SAFE_LABEL_VALUE = re.compile(r"^[A-Za-z0-9_.:-]+$")
_FORBIDDEN_VALUE_MARKERS = ("/", "\\", "://", "\r", "\n", "\x00")
_SAMPLES_LOCK = runtime_lock("services.code_traceability.samples")
_SAMPLES = runtime_sample_buffer("services.code_traceability")


@dataclass(frozen=True, slots=True)
class CodeTraceabilityMetricEvent:
    metric_name: str
    value: int | float = 1
    labels: Mapping[str, Any] = field(default_factory=dict)


class CodeTraceabilityMetricsSink(Protocol):
    def emit(self, event: CodeTraceabilityMetricEvent) -> None: ...


class LoggingCodeTraceabilityMetricsSink:
    """Default structured-log sink with bounded test-visible samples."""

    def emit(self, event: CodeTraceabilityMetricEvent) -> None:
        safe = sanitize_code_traceability_metric_event(event)
        sample = {
            "metric_name": safe.metric_name,
            "value": safe.value,
            "labels": dict(safe.labels),
        }
        with _SAMPLES_LOCK:
            _SAMPLES.append(sample)
        logger.info(
            "code_traceability.metric name=%s value=%s",
            safe.metric_name,
            safe.value,
            extra={"event": "code_traceability.metric", **sample},
        )


_DEFAULT_SINK = LoggingCodeTraceabilityMetricsSink()


def sanitize_code_traceability_metric_event(
    event: CodeTraceabilityMetricEvent,
) -> CodeTraceabilityMetricEvent:
    if event.metric_name not in CODE_TRACEABILITY_METRIC_NAMES:
        raise ValueError("code_traceability_metric_name_invalid")
    if isinstance(event.value, bool) or not isinstance(event.value, int | float):
        raise ValueError("code_traceability_metric_value_invalid")
    numeric_value = float(event.value)
    if not math.isfinite(numeric_value) or numeric_value < 0:
        raise ValueError("code_traceability_metric_value_invalid")
    if not isinstance(event.labels, Mapping):
        raise ValueError("code_traceability_metric_labels_invalid")

    labels: dict[str, str] = {}
    for raw_key, raw_value in event.labels.items():
        key = str(raw_key)
        if key not in CODE_TRACEABILITY_METRIC_LABELS:
            raise ValueError("code_traceability_metric_label_invalid")
        labels[key] = _safe_label_value(raw_value)
    return CodeTraceabilityMetricEvent(event.metric_name, event.value, labels)


def observe_code_traceability_metric(
    metric_name: str,
    *,
    value: int | float = 1,
    labels: Mapping[str, Any] | None = None,
    metrics_sink: CodeTraceabilityMetricsSink | None = None,
) -> None:
    """Emit one of the exact §22 metrics with only allowlisted labels."""

    (metrics_sink or _DEFAULT_SINK).emit(
        CodeTraceabilityMetricEvent(
            metric_name=metric_name,
            value=value,
            labels=dict(labels or {}),
        )
    )


def get_code_traceability_metric_samples() -> list[dict[str, Any]]:
    with _SAMPLES_LOCK:
        return [
            {
                "metric_name": sample["metric_name"],
                "value": sample["value"],
                "labels": dict(sample["labels"]),
            }
            for sample in _SAMPLES.snapshot()
        ]


def reset_code_traceability_observability_for_tests() -> None:
    with _SAMPLES_LOCK:
        _SAMPLES.clear()


def assert_code_traceability_metric_payload_is_safe(
    payload: Mapping[str, Any],
) -> None:
    sanitize_code_traceability_metric_event(
        CodeTraceabilityMetricEvent(
            metric_name=str(payload.get("metric_name") or ""),
            value=payload.get("value", 1),
            labels=payload.get("labels", {}),
        )
    )


def _safe_label_value(value: Any) -> str:
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("code_traceability_metric_label_value_invalid")
        text = str(value)
    elif isinstance(value, str | int):
        text = str(value)
    elif value is None:
        text = "none"
    else:
        raise ValueError("code_traceability_metric_label_value_invalid")
    if (
        not text
        or len(text) > _MAX_LABEL_VALUE_CHARS
        or any(marker in text for marker in _FORBIDDEN_VALUE_MARKERS)
        or _SAFE_LABEL_VALUE.fullmatch(text) is None
    ):
        raise ValueError("code_traceability_metric_label_value_invalid")
    return text


__all__ = [
    "CODE_TRACEABILITY_METRIC_LABELS",
    "CODE_TRACEABILITY_METRIC_NAMES",
    "CodeTraceabilityMetricEvent",
    "CodeTraceabilityMetricsSink",
    "LoggingCodeTraceabilityMetricsSink",
    "METRIC_CODE_EVIDENCE_ATTESTATION_TOTAL",
    "METRIC_CODE_EVIDENCE_DISPOSITION_TOTAL",
    "METRIC_CODE_EVIDENCE_SUBMISSION_TOTAL",
    "METRIC_CODE_INVESTIGATION_RECEIPT_AGE_SECONDS",
    "METRIC_CODE_INVESTIGATION_RECEIPT_REJECTED_TOTAL",
    "METRIC_CODE_INVESTIGATION_RECEIPT_TOTAL",
    "METRIC_CODE_TRACEABILITY_GATE_BLOCKER_TOTAL",
    "METRIC_CODE_TRACEABILITY_GATE_TOTAL",
    "METRIC_IMPLEMENTATION_OVERLAP_ACKNOWLEDGED_TOTAL",
    "METRIC_IMPLEMENTATION_OVERLAP_TOTAL",
    "METRIC_IMPLEMENTATION_TARGET_CREATED_TOTAL",
    "METRIC_IMPLEMENTATION_TARGET_RESOLUTION_RECEIPT_TOTAL",
    "METRIC_IMPLEMENTATION_TARGET_RESOLUTION_SUBMISSION_DURATION_SECONDS",
    "assert_code_traceability_metric_payload_is_safe",
    "get_code_traceability_metric_samples",
    "observe_code_traceability_metric",
    "reset_code_traceability_observability_for_tests",
    "sanitize_code_traceability_metric_event",
]
