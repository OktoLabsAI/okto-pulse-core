"""Bounded observability for Architecture Design finding gates."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import threading
from typing import Any, Mapping


logger = logging.getLogger(__name__)

METRIC_FINDING_RUN_PERSIST_TOTAL = "architecture_finding_run_persist_total"
METRIC_WARNING_ACK_TOTAL = "architecture_warning_ack_total"
METRIC_DONE_BLOCKER_TOTAL = "architecture_findings_block_done_total"
METRIC_GATE_EVAL_DURATION_MS = "architecture_finding_gate_eval_duration_ms"
METRIC_PROJECTION_TOTAL = "architecture_finding_projection_total"
METRIC_PAYLOAD_PARITY_VIOLATION_TOTAL = "architecture_finding_payload_contract_violation_total"
METRIC_PROPAGATION_LEGACY_REPORT_TOTAL = "architecture_propagation_legacy_report_total"

ARCHITECTURE_METRIC_NAMES = frozenset(
    {
        METRIC_FINDING_RUN_PERSIST_TOTAL,
        METRIC_WARNING_ACK_TOTAL,
        METRIC_DONE_BLOCKER_TOTAL,
        METRIC_GATE_EVAL_DURATION_MS,
        METRIC_PROJECTION_TOTAL,
        METRIC_PAYLOAD_PARITY_VIOLATION_TOTAL,
        METRIC_PROPAGATION_LEGACY_REPORT_TOTAL,
    }
)

_ALLOWED_LABEL_KEYS = frozenset(
    {
        "active_count_bucket",
        "board_id",
        "code_count_bucket",
        "design_count_bucket",
        "entity_type",
        "outcome",
        "owner_type",
        "phase",
        "reason_code",
        "source_surface",
        "surface",
        "warning_count_bucket",
    }
)
_FORBIDDEN_LABEL_FRAGMENTS = (
    "body",
    "content",
    "description",
    "diagram",
    "json",
    "message",
    "password",
    "path",
    "payload",
    "secret",
    "text",
    "title",
    "token",
)
_MAX_LABEL_VALUE_CHARS = 128
_METRIC_SAMPLES_LOCK = threading.Lock()
_METRIC_SAMPLES: list[dict[str, Any]] = []


@dataclass(frozen=True)
class ArchitectureMetricEvent:
    """Safe metric event with bounded labels only."""

    metric_name: str
    value: int | float
    labels: dict[str, Any] = field(default_factory=dict)


class ArchitectureMetricsSink:
    """Default sink backed by structured logs plus test-visible samples."""

    def emit(self, event: ArchitectureMetricEvent) -> None:
        safe = sanitize_architecture_metric_event(event)
        with _METRIC_SAMPLES_LOCK:
            _METRIC_SAMPLES.append(
                {
                    "metric_name": safe.metric_name,
                    "value": safe.value,
                    "labels": dict(safe.labels),
                }
            )
        logger.info(
            "architecture.metric name=%s value=%s",
            safe.metric_name,
            safe.value,
            extra={
                "event": "architecture.metric",
                "metric_name": safe.metric_name,
                "value": safe.value,
                **safe.labels,
            },
        )


_DEFAULT_METRICS_SINK = ArchitectureMetricsSink()


def sanitize_architecture_metric_event(event: ArchitectureMetricEvent) -> ArchitectureMetricEvent:
    """Reject unsafe metric names/labels and coerce values to bounded strings."""

    if event.metric_name not in ARCHITECTURE_METRIC_NAMES:
        raise ValueError(f"Unsupported architecture metric: {event.metric_name}")

    safe_labels: dict[str, Any] = {}
    for key, value in event.labels.items():
        key_text = str(key)
        lowered = key_text.lower()
        if key_text not in _ALLOWED_LABEL_KEYS:
            raise ValueError(f"Unsupported architecture metric label: {key_text}")
        if any(fragment in lowered for fragment in _FORBIDDEN_LABEL_FRAGMENTS):
            raise ValueError(f"Forbidden architecture metric label: {key_text}")
        safe_labels[key_text] = _safe_label_value(value)

    return ArchitectureMetricEvent(
        metric_name=event.metric_name,
        value=event.value,
        labels=safe_labels,
    )


def observe_architecture_finding_run_persist(
    *,
    board_id: str,
    entity_type: str,
    warning_count: int,
    outcome: str,
    source_surface: str = "service",
    metrics_sink: ArchitectureMetricsSink | None = None,
) -> None:
    sink = metrics_sink or _DEFAULT_METRICS_SINK
    sink.emit(
        ArchitectureMetricEvent(
            METRIC_FINDING_RUN_PERSIST_TOTAL,
            1,
            {
                "board_id": board_id,
                "entity_type": entity_type,
                "outcome": outcome,
                "source_surface": source_surface,
                "warning_count_bucket": _count_bucket(warning_count),
            },
        )
    )


def observe_architecture_warning_ack(
    *,
    board_id: str,
    entity_type: str,
    outcome: str,
    warning_count: int,
    code_count: int,
    surface: str = "service",
    metrics_sink: ArchitectureMetricsSink | None = None,
) -> None:
    sink = metrics_sink or _DEFAULT_METRICS_SINK
    sink.emit(
        ArchitectureMetricEvent(
            METRIC_WARNING_ACK_TOTAL,
            1,
            {
                "board_id": board_id,
                "entity_type": entity_type,
                "outcome": outcome,
                "surface": surface,
                "warning_count_bucket": _count_bucket(warning_count),
                "code_count_bucket": _count_bucket(code_count),
            },
        )
    )


def observe_architecture_done_blocker(
    *,
    board_id: str,
    owner_type: str,
    active_count: int,
    design_count: int,
    phase: str = "done",
    surface: str = "service",
    metrics_sink: ArchitectureMetricsSink | None = None,
) -> None:
    sink = metrics_sink or _DEFAULT_METRICS_SINK
    sink.emit(
        ArchitectureMetricEvent(
            METRIC_DONE_BLOCKER_TOTAL,
            1,
            {
                "board_id": board_id,
                "owner_type": owner_type,
                "phase": phase,
                "surface": surface,
                "outcome": "blocked",
                "active_count_bucket": _count_bucket(active_count),
                "design_count_bucket": _count_bucket(design_count),
            },
        )
    )


def observe_architecture_propagation_legacy_report(
    *,
    board_id: str,
    scanned_count: int,
    legacy_count: int,
    surface: str = "service",
    metrics_sink: ArchitectureMetricsSink | None = None,
) -> None:
    """Spec C OR-C1: read-only diagnostic of legacy architecture snapshots whose source
    is now ineligible for propagation. Bounded labels only — never the diagram payload."""
    sink = metrics_sink or _DEFAULT_METRICS_SINK
    sink.emit(
        ArchitectureMetricEvent(
            METRIC_PROPAGATION_LEGACY_REPORT_TOTAL,
            1,
            {
                "board_id": board_id,
                "outcome": "report_generated",
                "surface": surface,
                "design_count_bucket": _count_bucket(scanned_count),
                "active_count_bucket": _count_bucket(legacy_count),
            },
        )
    )


def observe_architecture_gate_eval(
    *,
    board_id: str,
    owner_type: str,
    duration_ms: int | float,
    active_count: int,
    design_count: int,
    outcome: str,
    metrics_sink: ArchitectureMetricsSink | None = None,
) -> None:
    sink = metrics_sink or _DEFAULT_METRICS_SINK
    sink.emit(
        ArchitectureMetricEvent(
            METRIC_GATE_EVAL_DURATION_MS,
            round(float(duration_ms), 3),
            {
                "board_id": board_id,
                "owner_type": owner_type,
                "outcome": outcome,
                "active_count_bucket": _count_bucket(active_count),
                "design_count_bucket": _count_bucket(design_count),
            },
        )
    )


def observe_architecture_projection(
    *,
    board_id: str,
    owner_type: str,
    active_count: int,
    design_count: int,
    outcome: str,
    metrics_sink: ArchitectureMetricsSink | None = None,
) -> None:
    sink = metrics_sink or _DEFAULT_METRICS_SINK
    sink.emit(
        ArchitectureMetricEvent(
            METRIC_PROJECTION_TOTAL,
            1,
            {
                "board_id": board_id,
                "owner_type": owner_type,
                "outcome": outcome,
                "active_count_bucket": _count_bucket(active_count),
                "design_count_bucket": _count_bucket(design_count),
            },
        )
    )


def observe_architecture_payload_parity_violation(
    *,
    board_id: str,
    surface: str,
    reason_code: str,
    outcome: str = "violation",
    metrics_sink: ArchitectureMetricsSink | None = None,
) -> None:
    sink = metrics_sink or _DEFAULT_METRICS_SINK
    sink.emit(
        ArchitectureMetricEvent(
            METRIC_PAYLOAD_PARITY_VIOLATION_TOTAL,
            1,
            {
                "board_id": board_id,
                "surface": surface,
                "outcome": outcome,
                "reason_code": reason_code,
            },
        )
    )


def get_architecture_metric_samples() -> list[dict[str, Any]]:
    with _METRIC_SAMPLES_LOCK:
        return [
            {
                "metric_name": sample["metric_name"],
                "value": sample["value"],
                "labels": dict(sample["labels"]),
            }
            for sample in _METRIC_SAMPLES
        ]


def get_architecture_metric_labels() -> tuple[str, ...]:
    return tuple(sorted(_ALLOWED_LABEL_KEYS))


def reset_architecture_observability_for_tests() -> None:
    with _METRIC_SAMPLES_LOCK:
        _METRIC_SAMPLES.clear()


def assert_architecture_metric_payload_is_safe(payload: Mapping[str, Any]) -> None:
    """Test helper/invariant for metric labels."""

    serialized = repr(dict(payload)).lower()
    leaked = [fragment for fragment in _FORBIDDEN_LABEL_FRAGMENTS if fragment in serialized]
    if leaked:
        raise ValueError(f"Unsafe architecture metric payload fragments: {leaked}")


def _safe_label_value(value: Any) -> str:
    if value is None:
        return "none"
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, (str, int, float)):
        text = str(value)
    else:
        raise ValueError("Architecture metric labels must be scalar")
    return text[:_MAX_LABEL_VALUE_CHARS]


def _count_bucket(count: int | float | None) -> str:
    try:
        value = int(count or 0)
    except Exception:
        value = 0
    if value <= 0:
        return "0"
    if value == 1:
        return "1"
    if value <= 5:
        return "2_5"
    if value <= 20:
        return "6_20"
    return "21_plus"
