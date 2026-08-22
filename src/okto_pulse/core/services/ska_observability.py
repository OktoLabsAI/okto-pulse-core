"""Cardinality-safe observability for SK-A projections and contract gates.

The three counters in this module deliberately reject entity identifiers and
free-form values.  Retained samples are diagnostic only; monotonic counters are
kept independently by :func:`runtime_counter_sample_buffer`.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging
import math
import re
from time import perf_counter
from typing import Any, Mapping

from okto_pulse.core.observability.sample_buffer import (
    runtime_counter_sample_buffer,
    runtime_sample_buffer,
)


logger = logging.getLogger(__name__)

VALIDATION_EDITION_CONFLICT_CODES = frozenset(
    {
        "assessment_subject_edition_conflict",
        "checklist_spec_edition_conflict",
        "guideline_policy_edition_conflict",
        "spec_validation_edition_conflict",
    }
)
_SAFE_AUDIT_ID = re.compile(r"^[A-Za-z0-9_.:-]+$")

METRIC_PROJECTION_QUERIES_TOTAL = "pulse_ska_projection_queries_total"
METRIC_CONTRACT_PARITY_FAILURES_TOTAL = (
    "pulse_ska_contract_parity_failures_total"
)
METRIC_RESOURCE_CATALOG_DRIFT_TOTAL = (
    "pulse_ska_resource_catalog_drift_total"
)
METRIC_VALIDATION_EXTERNAL_COGNITION_UOW_DURATION_SECONDS = (
    "pulse_validation_external_cognition_uow_duration_seconds"
)
METRIC_VALIDATION_EDITION_CONFLICT_TOTAL = (
    "pulse_validation_edition_conflict_total"
)
METRIC_VALIDATION_CYCLE_SUMMARY_SELECTS_TOTAL = (
    "pulse_validation_cycle_summary_selects_total"
)
METRIC_VALIDATION_EDITION_MIGRATION_ROWS = (
    "pulse_validation_edition_migration_rows"
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
    METRIC_VALIDATION_EXTERNAL_COGNITION_UOW_DURATION_SECONDS: {
        "assessment_kind": frozenset(
            {
                "ambiguity",
                "requirement_lint",
                "spec_validation",
                "curated_checklist",
                "policy_compliance",
            }
        ),
        "subject_type": frozenset({"ideation", "refinement", "spec"}),
        "outcome": frozenset({"success", "error", "conflict"}),
    },
    METRIC_VALIDATION_EDITION_CONFLICT_TOTAL: {
        "operation": frozenset(
            {
                "ambiguity",
                "requirement_lint",
                "spec_validation",
                "curated_checklist",
                "policy_compliance",
            }
        ),
        "subject_type": frozenset({"ideation", "refinement", "spec"}),
    },
    METRIC_VALIDATION_CYCLE_SUMMARY_SELECTS_TOTAL: {
        "subject_type": frozenset(
            {"ideation", "refinement", "spec", "mixed"}
        ),
        "query_mode": frozenset({"single", "batch"}),
        "outcome": frozenset({"success", "error"}),
    },
    METRIC_VALIDATION_EDITION_MIGRATION_ROWS: {
        "subject_type": frozenset(
            {"ideation", "refinement", "spec", "mixed"}
        ),
        "outcome": frozenset({"migrated", "already_current", "error"}),
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
        "assessment_kind",
        "operation",
        "query_mode",
    ),
    sum_fields=("value", "duration_ms", "payload_bytes"),
)
_VALIDATION_EDITION_CONFLICT_EVENTS = runtime_sample_buffer(
    "services.validation_edition_conflicts"
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


def observe_validation_external_cognition_uow(
    *,
    assessment_kind: str,
    subject_type: str,
    outcome: str,
    duration_seconds: int | float,
) -> None:
    """Observe only bounded UoW timing labels, never assessment content."""

    emit_ska_metric(
        SkaMetricEvent(
            metric_name=(
                METRIC_VALIDATION_EXTERNAL_COGNITION_UOW_DURATION_SECONDS
            ),
            value=_non_negative_number(duration_seconds, "duration_seconds"),
            labels={
                "assessment_kind": assessment_kind,
                "subject_type": subject_type,
                "outcome": outcome,
            },
        )
    )


def observe_validation_edition_conflict(
    *,
    operation: str,
    subject_type: str,
    subject_id: str | None = None,
    expected_edition: int | None = None,
    actual_edition: int | None = None,
    correlation_id: str | None = None,
    conflict_code: str | None = None,
) -> None:
    """Count a conflict and optionally retain its bounded structured audit.

    Entity and correlation identifiers are audit fields, never metric labels,
    so the monotonic metric remains cardinality-safe. Findings and repository
    content are not accepted by this API.
    """

    emit_ska_metric(
        SkaMetricEvent(
            metric_name=METRIC_VALIDATION_EDITION_CONFLICT_TOTAL,
            labels={
                "operation": operation,
                "subject_type": subject_type,
            },
        )
    )
    audit_values = (
        subject_id,
        expected_edition,
        actual_edition,
        correlation_id,
        conflict_code,
    )
    if all(value is None for value in audit_values):
        return
    if subject_id is None or correlation_id is None or conflict_code is None:
        raise ValueError("validation_edition_conflict_audit_incomplete")
    safe_subject_id = _safe_validation_audit_id(
        subject_id,
        "subject_id",
    )
    safe_correlation_id = _safe_validation_audit_id(
        correlation_id,
        "correlation_id",
    )
    if conflict_code not in VALIDATION_EDITION_CONFLICT_CODES:
        raise ValueError("validation_edition_conflict_code_invalid")
    safe_expected = _optional_positive_int(
        expected_edition,
        "expected_edition",
    )
    safe_actual = _optional_positive_int(
        actual_edition,
        "actual_edition",
    )
    event = {
        "event": "validation.edition_conflict",
        "operation": operation,
        "subject_type": subject_type,
        "subject_id": safe_subject_id,
        "expected_edition": safe_expected,
        "actual_edition": safe_actual,
        "correlation_id": safe_correlation_id,
        "conflict_code": conflict_code,
    }
    _VALIDATION_EDITION_CONFLICT_EVENTS.append(event)
    logger.warning(
        "validation.edition_conflict operation=%s subject_type=%s "
        "subject_id=%s expected_edition=%s actual_edition=%s "
        "correlation_id=%s conflict_code=%s",
        operation,
        subject_type,
        safe_subject_id,
        safe_expected,
        safe_actual,
        safe_correlation_id,
        conflict_code,
        extra=event,
    )


def observe_validation_edition_conflict_from_error(
    error: BaseException,
    *,
    operation: str,
    subject_type: str,
    subject_id: str,
    expected_edition: int,
    correlation_id: str,
) -> bool:
    """Emit exactly one structured event for a stable edition-conflict code."""

    code = str(getattr(error, "code", "") or str(error)).strip().lower()
    if code not in VALIDATION_EDITION_CONFLICT_CODES:
        return False
    raw_details = getattr(error, "details", {})
    if isinstance(raw_details, Mapping):
        details = dict(raw_details)
    elif isinstance(raw_details, tuple):
        try:
            details = dict(raw_details)
        except (TypeError, ValueError):
            details = {}
    else:
        details = {}
    actual = details.get(
        "current",
        details.get("actual", details.get("actual_edition")),
    )
    expected = details.get("expected", expected_edition)
    def edition(value: object) -> int | None:
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
        if isinstance(value, str) and value.isdecimal():
            parsed = int(value)
            return parsed if parsed > 0 else None
        return None

    observe_validation_edition_conflict(
        operation=operation,
        subject_type=subject_type,
        subject_id=subject_id,
        expected_edition=edition(expected) or expected_edition,
        actual_edition=edition(actual),
        correlation_id=correlation_id,
        conflict_code=code,
    )
    return True


class _ObservedValidationUnitOfWorkFactory:
    def __init__(
        self,
        delegate: Any,
        *,
        assessment_kind: str,
        subject_type: str,
    ) -> None:
        self._delegate = delegate
        self._assessment_kind = assessment_kind
        self._subject_type = subject_type

    def __call__(self, *args: Any, **kwargs: Any):
        return self._scope(*args, **kwargs)

    @asynccontextmanager
    async def _scope(self, *args: Any, **kwargs: Any):
        started = perf_counter()
        outcome = "error"
        try:
            async with self._delegate(*args, **kwargs) as uow:
                yield uow
        except BaseException as exc:
            if _is_conflict_error(exc):
                outcome = "conflict"
            raise
        else:
            outcome = "success"
        finally:
            observe_validation_external_cognition_uow(
                assessment_kind=self._assessment_kind,
                subject_type=self._subject_type,
                outcome=outcome,
                duration_seconds=max(0.0, perf_counter() - started),
            )


def observe_validation_uow_factory(
    delegate: Any,
    *,
    assessment_kind: str,
    subject_type: str,
) -> Any:
    """Wrap only UoW entry/work/exit; external preflight stays unmeasured."""

    return _ObservedValidationUnitOfWorkFactory(
        delegate,
        assessment_kind=assessment_kind,
        subject_type=subject_type,
    )


def observe_validation_cycle_summary_selects(
    *,
    subject_type: str,
    query_mode: str,
    outcome: str,
    select_count: int,
) -> None:
    """Count bounded summary SQL selects without subject identifiers."""

    if (
        not isinstance(select_count, int)
        or isinstance(select_count, bool)
        or select_count < 0
    ):
        raise ValueError("validation_cycle_select_count_invalid")
    emit_ska_metric(
        SkaMetricEvent(
            metric_name=METRIC_VALIDATION_CYCLE_SUMMARY_SELECTS_TOTAL,
            value=select_count,
            labels={
                "subject_type": subject_type,
                "query_mode": query_mode,
                "outcome": outcome,
            },
        )
    )


def observe_validation_edition_migration_rows(
    *,
    subject_type: str,
    outcome: str,
    row_count: int,
) -> None:
    """Record migration row counts with only closed operational labels."""

    if (
        not isinstance(row_count, int)
        or isinstance(row_count, bool)
        or row_count < 0
    ):
        raise ValueError("validation_edition_migration_row_count_invalid")
    emit_ska_metric(
        SkaMetricEvent(
            metric_name=METRIC_VALIDATION_EDITION_MIGRATION_ROWS,
            value=row_count,
            labels={"subject_type": subject_type, "outcome": outcome},
        )
    )


def ska_metric_samples() -> tuple[dict[str, Any], ...]:
    return tuple(_SAMPLES.snapshot())


def ska_metric_counters() -> tuple[dict[str, Any], ...]:
    return tuple(_SAMPLES.counter_snapshot())


def validation_edition_conflict_events() -> tuple[dict[str, Any], ...]:
    """Return bounded, content-free conflict audit samples."""

    return tuple(
        dict(sample) for sample in _VALIDATION_EDITION_CONFLICT_EVENTS.snapshot()
    )


def reset_ska_metric_samples_for_tests() -> None:
    _SAMPLES.clear()
    _VALIDATION_EDITION_CONFLICT_EVENTS.clear()


def _non_negative_number(value: int | float, field: str) -> int | float:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(float(value))
        or value < 0
    ):
        raise ValueError(f"ska_metric_{field}_invalid")
    return value


def _optional_positive_int(value: object, field: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"validation_edition_conflict_{field}_invalid")
    return value


def _safe_validation_audit_id(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"validation_edition_conflict_{field}_invalid")
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > 255
        or _SAFE_AUDIT_ID.fullmatch(normalized) is None
    ):
        raise ValueError(f"validation_edition_conflict_{field}_invalid")
    return normalized


def _is_conflict_error(error: BaseException) -> bool:
    category = getattr(error, "category", None)
    category_value = str(getattr(category, "value", category) or "").lower()
    code = str(getattr(error, "code", "") or str(error)).strip().lower()
    return category_value == "conflict" or "conflict" in code


__all__ = [
    "METRIC_CONTRACT_PARITY_FAILURES_TOTAL",
    "METRIC_PROJECTION_QUERIES_TOTAL",
    "METRIC_RESOURCE_CATALOG_DRIFT_TOTAL",
    "METRIC_VALIDATION_EDITION_CONFLICT_TOTAL",
    "METRIC_VALIDATION_CYCLE_SUMMARY_SELECTS_TOTAL",
    "METRIC_VALIDATION_EDITION_MIGRATION_ROWS",
    "METRIC_VALIDATION_EXTERNAL_COGNITION_UOW_DURATION_SECONDS",
    "VALIDATION_EDITION_CONFLICT_CODES",
    "SkaMetricEvent",
    "emit_ska_metric",
    "observe_ska_contract_parity_failure",
    "observe_ska_projection_queries",
    "observe_ska_resource_catalog_drift",
    "observe_validation_edition_conflict",
    "observe_validation_edition_conflict_from_error",
    "observe_validation_cycle_summary_selects",
    "observe_validation_edition_migration_rows",
    "observe_validation_external_cognition_uow",
    "observe_validation_uow_factory",
    "reset_ska_metric_samples_for_tests",
    "sanitize_ska_metric",
    "ska_metric_counters",
    "ska_metric_samples",
    "validation_edition_conflict_events",
]
