"""Bounded audit and observability for bug regression scenario reuse."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import threading
from typing import Any, Mapping


from okto_pulse.core.observability.sample_buffer import BoundedSampleBuffer
from okto_pulse.core.events import publish as event_publish
from okto_pulse.core.events.types import BugRegressionScenarioReuseDecision
from okto_pulse.core.services.bug_regression_scenarios import (
    PATH_B_REASON_CODES,
    BugRegressionCoverageState,
    BugRegressionScenarioEligibilityResult,
)
from okto_pulse.core.services.bug_workflow_remediation import (
    BugWorkflowRemediationMessage,
    bug_workflow_remediation_safe_labels,
)


logger = logging.getLogger(__name__)

BUG_REGRESSION_DECISION_EVENT_TYPE = "bug_regression_scenario_reuse_decision"

METRIC_RESOLVE_TOTAL = "bug_regression_scenario_resolve_total"
METRIC_UNRELATED_REJECTED_TOTAL = "bug_regression_unrelated_scenario_rejected_total"
METRIC_SEMANTIC_GAP_TOTAL = "bug_regression_semantic_gap_total"
METRIC_RESOLVE_LATENCY_MS = "bug_regression_scenario_resolve_latency_ms"
METRIC_NO_UNLOCK_INVARIANT = "bug_regression_no_unlock_invariant"
METRIC_WORKFLOW_REMEDIATION_TOTAL = "bug_workflow_remediation_build_total"
#: OR (card 966c7e7c): bounded Path B outcome counter. Born cardinality-safe —
#: labels are ONLY reason_code/coverage_state/outcome/surface (NO entity ids,
#: not even spec_id), enforced by ``_METRIC_LABEL_ALLOWLIST`` below.
METRIC_PATH_B_TOTAL = "pulse_bug_regression_path_b_total"

BUG_REGRESSION_METRIC_NAMES = frozenset(
    {
        METRIC_RESOLVE_TOTAL,
        METRIC_UNRELATED_REJECTED_TOTAL,
        METRIC_SEMANTIC_GAP_TOTAL,
        METRIC_RESOLVE_LATENCY_MS,
        METRIC_NO_UNLOCK_INVARIANT,
        METRIC_WORKFLOW_REMEDIATION_TOTAL,
        METRIC_PATH_B_TOTAL,
    }
)

_ALLOWED_LABEL_KEYS = frozenset(
    {
        "board_id",
        "spec_id",
        "outcome",
        "decision",
        "reason_code",
        "coverage_state",
        "next_action",
        "remediation_path",
        "hotfix_lane_status",
        "surface",
    }
)
#: Per-metric STRICTER allowlist. The new Path B OR is cardinality-safe by
#: construction: it accepts ONLY bounded codes, never an entity id (spec_id,
#: bug_id, amendment_revision_id, regression_*_id, ...). Legacy metrics keep the
#: global allowlist (board_id/spec_id) — not refactored, out of this card's scope.
_PATH_B_METRIC_LABEL_KEYS = frozenset(
    {"reason_code", "coverage_state", "outcome", "surface"}
)
_METRIC_LABEL_ALLOWLIST: dict[str, frozenset[str]] = {
    METRIC_PATH_B_TOTAL: _PATH_B_METRIC_LABEL_KEYS,
}
#: The 5 Path B REJECT codes (catalog minus the 2 coverage states) — derived from
#: PATH_B_REASON_CODES so it cannot drift from the resolver's source of truth.
_PATH_B_REJECT_CODES = PATH_B_REASON_CODES - {
    BugRegressionCoverageState.COVERAGE_PENDING.value,
    BugRegressionCoverageState.PATH_B_READY.value,
}
_FORBIDDEN_LABEL_FRAGMENTS = (
    "description",
    "expected",
    "observed",
    "payload",
    "scenario_body",
    "steps",
    "text",
    "then",
    "title",
    "when",
)
_MAX_LABEL_VALUE_CHARS = 128
_METRIC_SAMPLES_LOCK = threading.Lock()
_METRIC_SAMPLES = BoundedSampleBuffer()


@dataclass(frozen=True)
class BugRegressionMetricEvent:
    """Safe metric event with bounded labels only."""

    metric_name: str
    value: int | float
    labels: dict[str, Any] = field(default_factory=dict)


class BugRegressionMetricsSink:
    """Default sink backed by structured logs plus test-visible samples."""

    def emit(self, event: BugRegressionMetricEvent) -> None:
        safe = sanitize_bug_regression_metric_event(event)
        with _METRIC_SAMPLES_LOCK:
            _METRIC_SAMPLES.append(
                {
                    "metric_name": safe.metric_name,
                    "value": safe.value,
                    "labels": dict(safe.labels),
                }
            )
        logger.info(
            "bug_regression.metric name=%s value=%s",
            safe.metric_name,
            safe.value,
            extra={
                "event": "bug_regression.metric",
                "metric_name": safe.metric_name,
                "value": safe.value,
                **safe.labels,
            },
        )


_DEFAULT_METRICS_SINK = BugRegressionMetricsSink()


def sanitize_bug_regression_metric_event(
    event: BugRegressionMetricEvent,
) -> BugRegressionMetricEvent:
    """Reject unsafe metric names/labels and coerce values to bounded strings."""

    if event.metric_name not in BUG_REGRESSION_METRIC_NAMES:
        raise ValueError(f"Unsupported bug regression metric: {event.metric_name}")

    # Per-metric stricter allowlist wins (the Path B OR forbids entity ids); other
    # metrics fall back to the global bounded allowlist.
    allowed_keys = _METRIC_LABEL_ALLOWLIST.get(event.metric_name, _ALLOWED_LABEL_KEYS)
    safe_labels: dict[str, Any] = {}
    for key, value in event.labels.items():
        key_text = str(key)
        lowered = key_text.lower()
        if key_text not in allowed_keys:
            raise ValueError(f"Unsupported bug regression metric label: {key_text}")
        if any(fragment in lowered for fragment in _FORBIDDEN_LABEL_FRAGMENTS):
            raise ValueError(f"Forbidden bug regression metric label: {key_text}")
        safe_labels[key_text] = _safe_label_value(value)

    return BugRegressionMetricEvent(
        metric_name=event.metric_name,
        value=event.value,
        labels=safe_labels,
    )


def observe_bug_regression_resolution(
    *,
    board_id: str,
    result: BugRegressionScenarioEligibilityResult | None,
    duration_ms: int | float,
    spec_id: str | None = None,
    error_code: str | None = None,
    metrics_sink: BugRegressionMetricsSink | None = None,
) -> None:
    """Emit bounded resolver metrics for preview/gate calls."""

    sink = metrics_sink or _DEFAULT_METRICS_SINK
    if result is None:
        outcome = "error"
        reason_code = error_code or "error"
        next_action = "unknown"
        resolved_spec_id = spec_id or "unknown"
    else:
        outcome = _resolution_outcome(result)
        reason_code = _primary_reason_code(result) or "none"
        next_action = result.next_action.value
        resolved_spec_id = result.spec_id

    labels = {
        "board_id": board_id,
        "spec_id": resolved_spec_id,
        "outcome": outcome,
        "reason_code": reason_code,
        "next_action": next_action,
    }
    sink.emit(BugRegressionMetricEvent(METRIC_RESOLVE_TOTAL, 1, labels))
    sink.emit(BugRegressionMetricEvent(METRIC_RESOLVE_LATENCY_MS, round(float(duration_ms), 3), labels))

    if result is None:
        return

    if any(item.reason.value == "unrelated_scenario" for item in result.rejected_scenarios):
        sink.emit(
            BugRegressionMetricEvent(
                METRIC_UNRELATED_REJECTED_TOTAL,
                1,
                {
                    **labels,
                    "outcome": "rejected",
                    "reason_code": "unrelated_scenario",
                },
            )
        )
    if result.semantic_gap_required:
        sink.emit(
            BugRegressionMetricEvent(
                METRIC_SEMANTIC_GAP_TOTAL,
                1,
                {
                    **labels,
                    "outcome": "semantic_gap",
                    "reason_code": reason_code,
                },
            )
        )

    # OR (card 966c7e7c): Path B outcome counter. Emitted from this single point
    # so gate AND preview produce the SAME metric (parity). Cardinality-safe:
    # bounded codes only, NO entity ids (not even spec_id) — see
    # _PATH_B_METRIC_LABEL_KEYS. reason_code is always ∈ PATH_B_REASON_CODES.
    pb_reason = _path_b_reason_code(result)
    if pb_reason is not None:
        sink.emit(
            BugRegressionMetricEvent(
                METRIC_PATH_B_TOTAL,
                1,
                {
                    "outcome": outcome,
                    "reason_code": pb_reason,
                    "coverage_state": result.coverage_state.value,
                },
            )
        )


async def record_bug_regression_decision(
    *,
    board_id: str,
    bug_id: str,
    spec_id: str,
    decision: str,
    reason_code: str,
    scenario_count: int,
    test_task_count: int,
    coverage_state: str = "",
    actor_id: str | None = None,
    actor_type: str = "user",
    session: object | None = None,
) -> None:
    """Persist the bounded decision audit event.

    The event follows the EventBus outbox contract: callers publish it in
    their current unit of work, and the surrounding service owns
    commit/rollback. Opening a second write transaction here can deadlock
    SQLite and would be inconsistent with the rest of the event pipeline.
    """
    if session is None:
        raise ValueError("session is required to record bug regression decisions")

    event = BugRegressionScenarioReuseDecision(
        board_id=board_id,
        actor_id=actor_id,
        actor_type=actor_type,
        bug_id=bug_id,
        spec_id=spec_id,
        decision=decision,
        reason_code=reason_code,
        coverage_state=coverage_state,
        scenario_count=max(0, int(scenario_count)),
        test_task_count=max(0, int(test_task_count)),
    )
    await event_publish(event, session=session)


def emit_no_unlock_invariant(
    *,
    board_id: str,
    spec_id: str,
    outcome: str = "preserved",
    metrics_sink: BugRegressionMetricsSink | None = None,
) -> None:
    sink = metrics_sink or _DEFAULT_METRICS_SINK
    sink.emit(
        BugRegressionMetricEvent(
            METRIC_NO_UNLOCK_INVARIANT,
            1,
            {
                "board_id": board_id,
                "spec_id": spec_id,
                "outcome": outcome,
                "reason_code": "no_unlock_preserved",
            },
        )
    )


def observe_bug_workflow_remediation(
    *,
    board_id: str,
    spec_id: str,
    message: BugWorkflowRemediationMessage,
    surface: str,
    outcome: str = "blocked",
    metrics_sink: BugRegressionMetricsSink | None = None,
) -> None:
    """Emit bounded remediation guidance metrics with no free-form content."""

    sink = metrics_sink or _DEFAULT_METRICS_SINK
    sink.emit(
        BugRegressionMetricEvent(
            METRIC_WORKFLOW_REMEDIATION_TOTAL,
            1,
            {
                "board_id": board_id,
                "spec_id": spec_id,
                **bug_workflow_remediation_safe_labels(
                    message,
                    surface=surface,
                    outcome=outcome,
                ),
            },
        )
    )


def get_bug_regression_metric_samples() -> list[dict[str, Any]]:
    with _METRIC_SAMPLES_LOCK:
        return [
            {
                "metric_name": sample["metric_name"],
                "value": sample["value"],
                "labels": dict(sample["labels"]),
            }
            for sample in _METRIC_SAMPLES.snapshot()
        ]


def get_bug_regression_metric_labels() -> tuple[str, ...]:
    return tuple(sorted(_ALLOWED_LABEL_KEYS))


def reset_bug_regression_observability_for_tests() -> None:
    with _METRIC_SAMPLES_LOCK:
        _METRIC_SAMPLES.clear()


def _resolution_outcome(result: BugRegressionScenarioEligibilityResult) -> str:
    has_eligible = bool(result.eligible_scenarios)
    has_rejected = bool(result.rejected_scenarios)
    if has_eligible and has_rejected:
        return "mixed"
    if has_eligible:
        return "eligible"
    return "none_eligible"


def _primary_reason_code(result: BugRegressionScenarioEligibilityResult) -> str | None:
    if result.rejected_scenarios:
        return result.rejected_scenarios[0].reason.value
    if result.eligible_scenarios:
        return result.eligible_scenarios[0].reason.value
    if result.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING:
        return "coverage_pending"
    if result.semantic_gap_required:
        return "no_eligible_scenarios"
    return None


def _path_b_reason_code(result: BugRegressionScenarioEligibilityResult) -> str | None:
    """Bounded Path B reason code for the OR metric, or None when the resolution
    did not engage Path B. Always ∈ PATH_B_REASON_CODES (anti-drift by
    construction): coverage state wins, else the first Path B reject reason."""
    if result.coverage_state is BugRegressionCoverageState.PATH_B_READY:
        return BugRegressionCoverageState.PATH_B_READY.value
    if result.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING:
        return BugRegressionCoverageState.COVERAGE_PENDING.value
    for item in result.rejected_scenarios:
        if item.reason.value in _PATH_B_REJECT_CODES:
            return item.reason.value
    return None


def _safe_label_value(value: Any) -> str:
    if value is None:
        return "none"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        text = str(value)
    else:
        raise ValueError("Bug regression metric labels must be scalar")
    return text[:_MAX_LABEL_VALUE_CHARS]


def assert_bug_regression_payload_is_safe(payload: Mapping[str, Any]) -> None:
    """Test helper/invariant for event payloads and metric labels."""

    serialized = repr(dict(payload)).lower()
    forbidden = (
        "expected_behavior",
        "observed_behavior",
        "steps_to_reproduce",
        "scenario_body",
        "given",
        "when",
        "then",
        "title",
        "description",
        "payload",
        "password",
        "secret",
        "token",
    )
    leaked = [fragment for fragment in forbidden if fragment in serialized]
    if leaked:
        raise ValueError(f"Unsafe bug regression payload fragments: {leaked}")
