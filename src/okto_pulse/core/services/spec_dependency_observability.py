"""Cardinality-safe observability for operational Spec precedence."""

from __future__ import annotations

import contextvars
import functools
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from time import perf_counter
from typing import Any, Awaitable, Callable, Mapping, ParamSpec, Protocol, TypeVar

from okto_pulse.core.observability.sample_buffer import runtime_sample_buffer
from okto_pulse.core.runtime_context import runtime_lock


logger = logging.getLogger(__name__)

METRIC_SPEC_DEPENDENCY_MUTATION_TOTAL = "spec_dependency_mutation_total"
METRIC_SPEC_DEPENDENCY_MUTATION_DURATION_MS = "spec_dependency_mutation_duration_ms"
METRIC_SPEC_DEPENDENCY_CRITICAL_SECTION_DURATION_MS = (
    "spec_dependency_critical_section_duration_ms"
)
METRIC_SPEC_DEPENDENCY_GATE_TOTAL = "spec_dependency_gate_total"
METRIC_SPEC_DEPENDENCY_PROJECTION_LAG_SECONDS = (
    "spec_dependency_projection_lag_seconds"
)

SPEC_DEPENDENCY_METRIC_NAMES = frozenset(
    {
        METRIC_SPEC_DEPENDENCY_MUTATION_TOTAL,
        METRIC_SPEC_DEPENDENCY_MUTATION_DURATION_MS,
        METRIC_SPEC_DEPENDENCY_CRITICAL_SECTION_DURATION_MS,
        METRIC_SPEC_DEPENDENCY_GATE_TOTAL,
        METRIC_SPEC_DEPENDENCY_PROJECTION_LAG_SECONDS,
    }
)


class SpecDependencyMutationOutcome(str, Enum):
    SUCCEEDED = "succeeded"
    REPLAYED = "replayed"
    INVALID_REQUEST = "invalid_request"
    NOT_FOUND = "not_found"
    VERSION_CONFLICT = "version_conflict"
    POLICY_CONFLICT = "policy_conflict"
    INTERNAL_ERROR = "internal_error"


class SpecDependencyGateOutcome(str, Enum):
    READY = "ready"
    BLOCKED = "blocked"
    STATE_CONFLICT = "state_conflict"
    INTERNAL_ERROR = "internal_error"


_ALLOWED_LABELS = frozenset({"operation", "outcome", "reason_code", "surface"})
_ALLOWED_OPERATIONS = frozenset({"add", "remove", "added", "removed"})
_ALLOWED_SURFACES = frozenset({"spec_start", "card_start", "execution"})
_ALLOWED_REASON_CODES = frozenset(
    {
        "none",
        "invalid_spec_dependency_request",
        "permission_denied",
        "spec_not_found",
        "dependency_target_unavailable",
        "spec_dependency_not_found",
        "spec_dependency_version_conflict",
        "spec_dependency_cycle",
        "spec_dependency_state_conflict",
        "spec_dependency_self_reference",
        "cross_board_dependency_forbidden",
        "invalid_cursor",
        "spec_dependencies_incomplete",
        "internal_error",
    }
)


@dataclass(frozen=True, slots=True)
class SpecDependencyMetricEvent:
    metric_name: str
    value: int | float = 1
    labels: Mapping[str, str] = field(default_factory=dict)


class SpecDependencyMetricsSink(Protocol):
    def emit(self, event: SpecDependencyMetricEvent) -> None: ...


_SAMPLES = runtime_sample_buffer("services.spec_dependency")
_SAMPLES_LOCK = runtime_lock("services.spec_dependency.samples")


class LoggingSpecDependencyMetricsSink:
    def emit(self, event: SpecDependencyMetricEvent) -> None:
        safe = sanitize_spec_dependency_metric_event(event)
        sample = {
            "metric_name": safe.metric_name,
            "value": safe.value,
            "labels": dict(safe.labels),
        }
        with _SAMPLES_LOCK:
            _SAMPLES.append(sample)
        logger.info(
            "spec_dependency.metric name=%s value=%s",
            safe.metric_name,
            safe.value,
            extra={"event": "spec_dependency.metric", **sample},
        )


_DEFAULT_SINK = LoggingSpecDependencyMetricsSink()


def sanitize_spec_dependency_metric_event(
    event: SpecDependencyMetricEvent,
) -> SpecDependencyMetricEvent:
    if event.metric_name not in SPEC_DEPENDENCY_METRIC_NAMES:
        raise ValueError("spec_dependency_metric_name_invalid")
    if isinstance(event.value, bool) or not isinstance(event.value, int | float):
        raise ValueError("spec_dependency_metric_value_invalid")
    if not math.isfinite(float(event.value)) or float(event.value) < 0:
        raise ValueError("spec_dependency_metric_value_invalid")
    if set(event.labels) - _ALLOWED_LABELS:
        raise ValueError("spec_dependency_metric_label_invalid")
    labels = {str(key): str(value) for key, value in event.labels.items()}
    if "operation" in labels and labels["operation"] not in _ALLOWED_OPERATIONS:
        raise ValueError("spec_dependency_metric_operation_invalid")
    if "surface" in labels and labels["surface"] not in _ALLOWED_SURFACES:
        raise ValueError("spec_dependency_metric_surface_invalid")
    if (
        "reason_code" in labels
        and labels["reason_code"] not in _ALLOWED_REASON_CODES
    ):
        raise ValueError("spec_dependency_metric_reason_code_invalid")
    allowed_outcomes = {item.value for item in SpecDependencyMutationOutcome} | {
        item.value for item in SpecDependencyGateOutcome
    } | {"projected"}
    if "outcome" in labels and labels["outcome"] not in allowed_outcomes:
        raise ValueError("spec_dependency_metric_outcome_invalid")
    return SpecDependencyMetricEvent(event.metric_name, event.value, labels)


def observe_spec_dependency_metric(
    metric_name: str,
    *,
    value: int | float = 1,
    labels: Mapping[str, str] | None = None,
    metrics_sink: SpecDependencyMetricsSink | None = None,
) -> None:
    (metrics_sink or _DEFAULT_SINK).emit(
        SpecDependencyMetricEvent(metric_name, value, dict(labels or {}))
    )


@dataclass(slots=True)
class _MutationTiming:
    operation: str
    started_at: float
    critical_started_at: float | None = None


_CURRENT_MUTATION: contextvars.ContextVar[_MutationTiming | None] = (
    contextvars.ContextVar("spec_dependency_current_mutation", default=None)
)


def mark_spec_dependency_critical_section_started() -> None:
    timing = _CURRENT_MUTATION.get()
    if timing is not None and timing.critical_started_at is None:
        timing.critical_started_at = perf_counter()


def _mutation_outcome(exc: BaseException | None, result: Any) -> tuple[str, str]:
    if exc is None:
        receipt = getattr(result, "receipt", result)
        return (
            SpecDependencyMutationOutcome.REPLAYED.value
            if bool(getattr(receipt, "replayed", False))
            else SpecDependencyMutationOutcome.SUCCEEDED.value,
            "none",
        )
    exception_name = type(exc).__name__
    if exception_name == "PermissionDeniedError":
        code = "permission_denied"
    elif exception_name == "EntityNotFoundError":
        # Target misses are normalized by the Add use case. Any raw entity miss
        # here is therefore the command's source Spec.
        code = "spec_not_found"
    else:
        code = str(getattr(exc, "code", "internal_error"))
    if code == "spec_dependency_version_conflict":
        outcome = SpecDependencyMutationOutcome.VERSION_CONFLICT
    elif code in {
        "invalid_spec_dependency_request",
        "invalid_cursor",
        "spec_dependency_self_reference",
    }:
        outcome = SpecDependencyMutationOutcome.INVALID_REQUEST
    elif code in {
        "dependency_target_unavailable",
        "spec_dependency_not_found",
        "spec_not_found",
    }:
        outcome = SpecDependencyMutationOutcome.NOT_FOUND
    elif code != "internal_error":
        outcome = SpecDependencyMutationOutcome.POLICY_CONFLICT
    else:
        outcome = SpecDependencyMutationOutcome.INTERNAL_ERROR
    return outcome.value, code if code in _ALLOWED_REASON_CODES else "internal_error"


def _call_context(
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
) -> tuple[Any | None, Any | None]:
    command = args[1] if len(args) > 1 else kwargs.get("command")
    return command, kwargs.get("actor")


def _safe_mutation_log_fields(
    *,
    operation: str,
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
    result: Any,
    outcome: str,
    reason_code: str,
    duration_ms: float,
    critical_section_duration_ms: float | None,
) -> dict[str, Any]:
    """Build the bounded OR-02 business event without target/idempotency data."""

    command, actor = _call_context(args, kwargs)
    receipt = getattr(result, "receipt", result)
    source = getattr(receipt, "source_spec", None)
    dependency = getattr(receipt, "dependency", None)
    version_new = getattr(source, "version", None)
    version_old = (
        int(version_new) - 1
        if isinstance(version_new, int) and not isinstance(version_new, bool)
        else None
    )
    return {
        "event": "spec_dependency.mutation",
        "board_id": (
            getattr(source, "board_id", None)
            or getattr(actor, "board_id", None)
        ),
        "spec_id": (
            getattr(source, "id", None)
            or getattr(command, "spec_id", None)
        ),
        "operation": operation,
        "outcome": outcome,
        "reason_code": reason_code,
        "duration_ms": duration_ms,
        "critical_section_duration_ms": critical_section_duration_ms,
        "replayed": bool(getattr(receipt, "replayed", False)),
        "dependency_id": getattr(dependency, "id", None),
        "version_old": version_old,
        "version_new": version_new,
    }


P = ParamSpec("P")
R = TypeVar("R")


def observe_spec_dependency_mutation(
    operation: str,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    if operation not in {"add", "remove"}:
        raise ValueError("spec_dependency_metric_operation_invalid")

    def decorator(function: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(function)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            timing = _MutationTiming(operation=operation, started_at=perf_counter())
            token = _CURRENT_MUTATION.set(timing)
            result: Any = None
            caught: BaseException | None = None
            try:
                result = await function(*args, **kwargs)
                return result
            except BaseException as exc:
                caught = exc
                raise
            finally:
                finished_at = perf_counter()
                outcome, reason_code = _mutation_outcome(caught, result)
                duration_ms = (finished_at - timing.started_at) * 1000
                critical_duration_ms = (
                    (finished_at - timing.critical_started_at) * 1000
                    if timing.critical_started_at is not None
                    else None
                )
                labels = {
                    "operation": operation,
                    "outcome": outcome,
                    "reason_code": reason_code,
                }
                observe_spec_dependency_metric(
                    METRIC_SPEC_DEPENDENCY_MUTATION_TOTAL,
                    labels=labels,
                )
                observe_spec_dependency_metric(
                    METRIC_SPEC_DEPENDENCY_MUTATION_DURATION_MS,
                    value=duration_ms,
                    labels=labels,
                )
                if timing.critical_started_at is not None:
                    observe_spec_dependency_metric(
                        METRIC_SPEC_DEPENDENCY_CRITICAL_SECTION_DURATION_MS,
                        value=critical_duration_ms or 0.0,
                        labels=labels,
                    )
                business_fields = _safe_mutation_log_fields(
                    operation=operation,
                    args=tuple(args),
                    kwargs=kwargs,
                    result=result,
                    outcome=outcome,
                    reason_code=reason_code,
                    duration_ms=duration_ms,
                    critical_section_duration_ms=critical_duration_ms,
                )
                logger.info(
                    "spec_dependency.mutation operation=%s outcome=%s reason_code=%s",
                    operation,
                    outcome,
                    reason_code,
                    extra=business_fields,
                )
                _CURRENT_MUTATION.reset(token)

        return wrapped

    return decorator


def observe_spec_dependency_gate(
    surface: str,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    if surface not in _ALLOWED_SURFACES:
        raise ValueError("spec_dependency_metric_surface_invalid")

    def decorator(function: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(function)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            started_at = perf_counter()
            result: Any = None
            caught_error: BaseException | None = None
            outcome = SpecDependencyGateOutcome.INTERNAL_ERROR.value
            reason_code = "internal_error"
            try:
                result = await function(*args, **kwargs)
            except BaseException as exc:
                caught_error = exc
                code = str(getattr(exc, "code", "internal_error"))
                outcome = (
                    SpecDependencyGateOutcome.BLOCKED.value
                    if code == "spec_dependencies_incomplete"
                    else SpecDependencyGateOutcome.STATE_CONFLICT.value
                    if code == "spec_dependency_state_conflict"
                    else SpecDependencyGateOutcome.INTERNAL_ERROR.value
                )
                reason_code = (
                    code if code in _ALLOWED_REASON_CODES else "internal_error"
                )
                raise
            else:
                outcome = SpecDependencyGateOutcome.READY.value
                reason_code = "none"
                return result
            finally:
                labels = {
                    "surface": surface,
                    "outcome": outcome,
                    "reason_code": reason_code,
                }
                observe_spec_dependency_metric(
                    METRIC_SPEC_DEPENDENCY_GATE_TOTAL,
                    labels=labels,
                )
                command, _actor = _call_context(tuple(args), kwargs)
                board_id = kwargs.get("board_id")
                spec_id = kwargs.get("spec_id")
                if board_id is None:
                    board_id = getattr(command, "board_id", None)
                if spec_id is None:
                    spec_id = getattr(command, "spec_id", None)
                blocking_count = getattr(result, "blocking_count", None)
                caught_facts = getattr(caught_error, "facts", {})
                if blocking_count is None and isinstance(caught_facts, Mapping):
                    blocking_count = caught_facts.get("blocking_count")
                logger.info(
                    "spec_dependency.gate surface=%s outcome=%s reason_code=%s",
                    surface,
                    outcome,
                    reason_code,
                    extra={
                        "event": "spec_dependency.gate",
                        "board_id": board_id,
                        "spec_id": spec_id,
                        "surface": surface,
                        "outcome": outcome,
                        "reason_code": reason_code,
                        "duration_ms": (perf_counter() - started_at) * 1000,
                        "blocking_count": blocking_count,
                    },
                )

        return wrapped

    return decorator


def observe_spec_dependency_projection_lag(
    *,
    event_type: str,
    triggered_at: datetime | None,
    projected_at: datetime | None = None,
) -> None:
    if event_type not in {"spec.dependency_added", "spec.dependency_removed"}:
        return
    if triggered_at is None:
        return
    projected = projected_at or datetime.now(timezone.utc)
    start = triggered_at
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    lag = max(0.0, (projected - start).total_seconds())
    observe_spec_dependency_metric(
        METRIC_SPEC_DEPENDENCY_PROJECTION_LAG_SECONDS,
        value=lag,
        labels={
            "operation": (
                "added" if event_type.endswith("added") else "removed"
            ),
            "outcome": "projected",
            "reason_code": "none",
        },
    )


def get_spec_dependency_metric_samples() -> list[dict[str, Any]]:
    with _SAMPLES_LOCK:
        return [
            {
                "metric_name": sample["metric_name"],
                "value": sample["value"],
                "labels": dict(sample["labels"]),
            }
            for sample in _SAMPLES.snapshot()
        ]


def reset_spec_dependency_observability_for_tests() -> None:
    with _SAMPLES_LOCK:
        _SAMPLES.clear()


__all__ = [
    "LoggingSpecDependencyMetricsSink",
    "METRIC_SPEC_DEPENDENCY_CRITICAL_SECTION_DURATION_MS",
    "METRIC_SPEC_DEPENDENCY_GATE_TOTAL",
    "METRIC_SPEC_DEPENDENCY_MUTATION_DURATION_MS",
    "METRIC_SPEC_DEPENDENCY_MUTATION_TOTAL",
    "METRIC_SPEC_DEPENDENCY_PROJECTION_LAG_SECONDS",
    "SPEC_DEPENDENCY_METRIC_NAMES",
    "SpecDependencyGateOutcome",
    "SpecDependencyMetricEvent",
    "SpecDependencyMetricsSink",
    "SpecDependencyMutationOutcome",
    "get_spec_dependency_metric_samples",
    "mark_spec_dependency_critical_section_started",
    "observe_spec_dependency_gate",
    "observe_spec_dependency_metric",
    "observe_spec_dependency_mutation",
    "observe_spec_dependency_projection_lag",
    "reset_spec_dependency_observability_for_tests",
    "sanitize_spec_dependency_metric_event",
]
