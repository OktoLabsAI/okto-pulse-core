"""SPEC f5a7cae7 / card 966c7e7c — Path B reason-code catalog + bounded OR metric.

`pulse_bug_regression_path_b_total` is born cardinality-safe (NO entity ids, not
even spec_id), the reason-code catalog is bounded + anti-drift, and the metric is
emitted from the SINGLE observe point so gate and preview cannot diverge (the
integration parity — confirm -> gate ALLOW + preview path_b_ready — is proven in
tests/test_bug_regression_locked_spec.py).

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_bug_regression_path_b_observability.py
"""

from __future__ import annotations

import pytest

from okto_pulse.core.services.bug_regression_observability import (
    METRIC_PATH_B_TOTAL,
    BugRegressionMetricEvent,
    _path_b_reason_code,
    get_bug_regression_metric_samples,
    observe_bug_regression_resolution,
    reset_bug_regression_observability_for_tests,
    sanitize_bug_regression_metric_event,
)
from okto_pulse.core.services.bug_regression_scenarios import (
    PATH_B_REASON_CODES,
    BugRegressionCoverageState,
    BugRegressionEligibilityReason,
    BugRegressionNextAction,
    BugRegressionRejectionReason,
    BugRegressionScenarioEligibilityResult,
    EligibleBugRegressionScenario,
    RejectedBugRegressionScenario,
)


def _result(**over) -> BugRegressionScenarioEligibilityResult:
    base = dict(
        bug_id="bug-1",
        spec_id="spec-1",
        eligible_scenarios=(),
        rejected_scenarios=(),
        semantic_gap_required=False,
        spec_mutation_required=False,
        next_action=BugRegressionNextAction.CREATE_REGRESSION_TEST_CARD,
    )
    base.update(over)
    return BugRegressionScenarioEligibilityResult(**base)


def _eligible(reason):
    return EligibleBugRegressionScenario("ts", None, reason, "task-1")


def _rejected(reason):
    return RejectedBugRegressionScenario("ts", reason)


# ---------------------------------------------------------------------------
# Bounded reason-code catalog (TR3) — exactly the 7, anti-drift.
# ---------------------------------------------------------------------------


def test_path_b_reason_code_catalog_is_the_bounded_seven():
    assert PATH_B_REASON_CODES == frozenset(
        {
            "missing_amendment_revision",
            "incomplete_amendment_lineage",
            "blocked_amendment_status",
            "unrelated_cross_spec_scenario",
            "missing_regression_artifact",
            "coverage_pending",
            "path_b_ready",
        }
    )


def test_path_b_reason_code_derivation_always_in_catalog():
    ready = _result(
        coverage_state=BugRegressionCoverageState.PATH_B_READY,
        eligible_scenarios=(_eligible(BugRegressionEligibilityReason.PATH_B_AMENDMENT_LINEAGE),),
    )
    assert _path_b_reason_code(ready) == "path_b_ready"

    pending = _result(
        coverage_state=BugRegressionCoverageState.COVERAGE_PENDING,
        coverage_pending_scenarios=("ts",),
    )
    assert _path_b_reason_code(pending) == "coverage_pending"

    reject = _result(
        rejected_scenarios=(_rejected(BugRegressionRejectionReason.MISSING_AMENDMENT_REVISION),),
        semantic_gap_required=True,
        spec_mutation_required=True,
        next_action=BugRegressionNextAction.ESCALATE_SEMANTIC_GAP,
    )
    assert _path_b_reason_code(reject) == "missing_amendment_revision"

    for result in (ready, pending, reject):
        assert _path_b_reason_code(result) in PATH_B_REASON_CODES


def test_path_a_resolution_yields_no_path_b_reason():
    # A same-spec UNRELATED_SCENARIO reject is Path A territory -> no Path B code
    # (the OR metric is not emitted for it).
    path_a = _result(
        rejected_scenarios=(_rejected(BugRegressionRejectionReason.UNRELATED_SCENARIO),),
        semantic_gap_required=True,
        spec_mutation_required=True,
        next_action=BugRegressionNextAction.ESCALATE_SEMANTIC_GAP,
    )
    assert _path_b_reason_code(path_a) is None


# ---------------------------------------------------------------------------
# OR metric pulse_bug_regression_path_b_total — bounded labels, no entity ids.
# ---------------------------------------------------------------------------


def test_path_b_metric_emitted_with_bounded_labels_only():
    reset_bug_regression_observability_for_tests()
    observe_bug_regression_resolution(
        board_id="board-1",
        result=_result(
            coverage_state=BugRegressionCoverageState.COVERAGE_PENDING,
            coverage_pending_scenarios=("ts",),
        ),
        duration_ms=1,
    )
    pb = [s for s in get_bug_regression_metric_samples() if s["metric_name"] == METRIC_PATH_B_TOTAL]
    reset_bug_regression_observability_for_tests()

    assert len(pb) == 1
    labels = pb[0]["labels"]
    assert labels["reason_code"] == "coverage_pending"
    assert labels["coverage_state"] == "coverage_pending"
    # cardinality-safe: ONLY bounded codes, NO entity ids (not even spec_id).
    assert set(labels).issubset({"reason_code", "coverage_state", "outcome", "surface"})
    for forbidden in ("spec_id", "board_id", "bug_id", "amendment_revision_id",
                      "regression_test_task_id", "regression_scenario_id"):
        assert forbidden not in labels


def test_path_a_resolution_emits_no_path_b_metric():
    reset_bug_regression_observability_for_tests()
    observe_bug_regression_resolution(
        board_id="board-1",
        result=_result(eligible_scenarios=(_eligible(BugRegressionEligibilityReason.ORIGIN_TASK_DIRECT),)),
        duration_ms=1,
    )
    pb = [s for s in get_bug_regression_metric_samples() if s["metric_name"] == METRIC_PATH_B_TOTAL]
    reset_bug_regression_observability_for_tests()
    assert pb == []


# ---------------------------------------------------------------------------
# Cardinality teeth: sanitize REJECTS any entity id label on the new metric,
# even spec_id (which the legacy global allowlist accepts for other metrics).
# ---------------------------------------------------------------------------


def test_path_b_metric_rejects_entity_id_labels():
    for forbidden in ("spec_id", "board_id", "amendment_revision_id",
                      "regression_test_task_id", "regression_scenario_id"):
        with pytest.raises(ValueError):
            sanitize_bug_regression_metric_event(
                BugRegressionMetricEvent(
                    METRIC_PATH_B_TOTAL,
                    1,
                    {"reason_code": "coverage_pending", forbidden: "x"},
                )
            )
    # the bounded labels ARE accepted on the same metric.
    ok = sanitize_bug_regression_metric_event(
        BugRegressionMetricEvent(
            METRIC_PATH_B_TOTAL,
            1,
            {"reason_code": "path_b_ready", "coverage_state": "path_b_ready", "outcome": "eligible"},
        )
    )
    assert ok.labels["reason_code"] == "path_b_ready"


def test_legacy_metric_still_accepts_spec_id_label():
    # The stricter allowlist is per-metric; legacy metrics keep board_id/spec_id
    # (not refactored — out of card scope).
    from okto_pulse.core.services.bug_regression_observability import METRIC_RESOLVE_TOTAL

    ok = sanitize_bug_regression_metric_event(
        BugRegressionMetricEvent(
            METRIC_RESOLVE_TOTAL, 1, {"board_id": "b", "spec_id": "s", "outcome": "eligible"}
        )
    )
    assert ok.labels["spec_id"] == "s"
