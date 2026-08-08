"""Cardinality-safe observability for SK-A projections and contract gates.

The three counters in this module deliberately reject entity identifiers and
free-form values.  Retained samples are diagnostic only; monotonic counters are
kept independently by :func:`runtime_counter_sample_buffer`.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import math
from typing import Any

from okto_pulse.core.observability.sample_buffer import (
    runtime_counter_sample_buffer,
)


logger = logging.getLogger(__name__)

METRIC_PROJECTION_QUERIES_TOTAL = "pulse_ska_projection_queries_total"
METRIC_CONTRACT_PARITY_FAILURES_TOTAL = (
    "pulse_ska_contract_parity_failures_total"
)
METRIC_RESOURCE_CATALOG_DRIFT_TOTAL = (
    "pulse_ska_resource_catalog_drift_total"
)

_METRIC_LABEL_VALUES: dict[str, dict[str, frozenset[str]]] = {
    METRIC_PROJECTION_QUERIES_TOTAL: {
        "surface": frozenset(
            {
                "parent_summary",
                "quality_assessments",
                "quality_findings",
                "research_decisions",
                "checklist_history",
            }
        ),
        "subject_type": frozenset(
            {"ideation", "refinement", "spec", "mixed"}
        ),
        "outcome": frozenset({"success", "error"}),
    },
    METRIC_CONTRACT_PARITY_FAILURES_TOTAL: {
        "surface": frozenset({"rest", "mcp", "frontend", "resource"}),
        "family": frozenset(
            {
                "quality",
                "ambiguity_gate",
                "research_decision",
                "checklist",
                "test_scenario",
            }
        ),
        "reason_code": frozenset(
            {
                "answered_at",
                "currentness",
                "error_envelope",
                "mockup_path",
                "nullish_default",
                "pagination",
                "permission",
                "response_shape",
                "schema",
                "tool_schema",
            }
        ),
    },
    METRIC_RESOURCE_CATALOG_DRIFT_TOTAL: {
        "surface": frozenset({"source", "installed"}),
        "drift_kind": frozenset(
            {
                "broken_link",
                "digest",
                "extra_uri",
                "generated_catalog",
                "missing_uri",
                "permission",
                "tool_classification",
                "tool_schema",
            }
        ),
    },
}

_SAMPLES = runtime_counter_sample_buffer(
    "services.ska_contracts",
    counter_fields=(
        "metric_name",
        "surface",
        "subject_type",
        "outcome",
        "family",
        "reason_code",
        "drift_kind",
    ),
    sum_fields=("value", "duration_ms", "payload_bytes"),
)


@dataclass(frozen=True, slots=True)
class SkaMetricEvent:
    """One safe metric observation with numeric, non-label measurements."""

    metric_name: str
    value: int | float = 1
    labels: dict[str, str] | None = None
    duration_ms: int | float = 0
    payload_bytes: int = 0


def emit_ska_metric(event: SkaMetricEvent) -> None:
    """Validate and retain one SK-A metric event."""

    sample = sanitize_ska_metric(event)
    _SAMPLES.append(sample)
    logger.info(
        "ska.metric name=%s value=%s",
        sample["metric_name"],
        sample["value"],
        extra={"event": "ska.metric", **sample},
    )


def sanitize_ska_metric(event: SkaMetricEvent) -> dict[str, Any]:
    """Reject arbitrary labels, non-finite values, and negative measurements."""

    allowed = _METRIC_LABEL_VALUES.get(event.metric_name)
    if allowed is None:
        raise ValueError("ska_metric_name_unsupported")
    labels = dict(event.labels or {})
    if set(labels) != set(allowed):
        raise ValueError("ska_metric_labels_invalid")
    safe_labels: dict[str, str] = {}
    for key, value in labels.items():
        if not isinstance(value, str) or value not in allowed[key]:
            raise ValueError(f"ska_metric_label_value_invalid:{key}")
        safe_labels[key] = value
    value = _non_negative_number(event.value, "value")
    duration_ms = _non_negative_number(event.duration_ms, "duration_ms")
    if (
        not isinstance(event.payload_bytes, int)
        or isinstance(event.payload_bytes, bool)
        or event.payload_bytes < 0
    ):
        raise ValueError("ska_metric_payload_bytes_invalid")
    return {
        "metric_name": event.metric_name,
        "value": value,
        **safe_labels,
        "duration_ms": duration_ms,
        "payload_bytes": event.payload_bytes,
    }


def observe_ska_projection_queries(
    *,
    surface: str,
    subject_type: str,
    query_count: int,
    duration_ms: int | float,
    payload_bytes: int,
    outcome: str = "success",
) -> None:
    """Record query count plus duration/payload for a bounded list surface."""

    if (
        not isinstance(query_count, int)
        or isinstance(query_count, bool)
        or query_count < 0
    ):
        raise ValueError("ska_projection_query_count_invalid")
    emit_ska_metric(
        SkaMetricEvent(
            metric_name=METRIC_PROJECTION_QUERIES_TOTAL,
            value=query_count,
            labels={
                "surface": surface,
                "subject_type": subject_type,
                "outcome": outcome,
            },
            duration_ms=duration_ms,
            payload_bytes=payload_bytes,
        )
    )


def observe_ska_contract_parity_failure(
    *,
    surface: str,
    family: str,
    reason_code: str,
) -> None:
    """Count an observed contract mismatch; successes do not increment it."""

    emit_ska_metric(
        SkaMetricEvent(
            metric_name=METRIC_CONTRACT_PARITY_FAILURES_TOTAL,
            labels={
                "surface": surface,
                "family": family,
                "reason_code": reason_code,
            },
        )
    )


def observe_ska_resource_catalog_drift(
    *,
    surface: str,
    drift_kind: str,
) -> None:
    """Count an observed installed/source resource or tool-catalog drift."""

    emit_ska_metric(
        SkaMetricEvent(
            metric_name=METRIC_RESOURCE_CATALOG_DRIFT_TOTAL,
            labels={
                "surface": surface,
                "drift_kind": drift_kind,
            },
        )
    )


def ska_metric_samples() -> tuple[dict[str, Any], ...]:
    return tuple(_SAMPLES.snapshot())


def ska_metric_counters() -> tuple[dict[str, Any], ...]:
    return tuple(_SAMPLES.counter_snapshot())


def reset_ska_metric_samples_for_tests() -> None:
    _SAMPLES.clear()


def _non_negative_number(value: int | float, field: str) -> int | float:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(float(value))
        or value < 0
    ):
        raise ValueError(f"ska_metric_{field}_invalid")
    return value


__all__ = [
    "METRIC_CONTRACT_PARITY_FAILURES_TOTAL",
    "METRIC_PROJECTION_QUERIES_TOTAL",
    "METRIC_RESOURCE_CATALOG_DRIFT_TOTAL",
    "SkaMetricEvent",
    "emit_ska_metric",
    "observe_ska_contract_parity_failure",
    "observe_ska_projection_queries",
    "observe_ska_resource_catalog_drift",
    "reset_ska_metric_samples_for_tests",
    "sanitize_ska_metric",
    "ska_metric_counters",
    "ska_metric_samples",
]
