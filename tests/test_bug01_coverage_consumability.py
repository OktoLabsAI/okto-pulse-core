"""BUG-01 A1 (spec 87acd0e3 / card 96b8b73b) — shared gate-consumable predicate.

``evaluate_coverage_confirmation_consumability`` is the pure, DB-free preflight
that answers: would persisting THIS candidate coverage confirmation make the bug
regression gate actually SELECT the scenario? It REUSES the authoritative
resolver routing (TR2) rather than calling ``evaluate_path_b_for_scenario``
directly, so a same-spec candidate is routed through Path A.

The load-bearing case is ``test_same_spec_unrelated_with_intersecting_claim_*``:
the amendment's task claim intersects the bug's authoritative tasks (so a naive
Path-B-only preflight would reach rank 5 and WRONGLY accept), yet the scenario is
same-spec and not linked to the bug lineage, so Path A rejects it
``unrelated_scenario``. That single test distinguishes the correct routing from a
Path-B-only implementation.

Reproduce:
  uv run pytest tests/test_bug01_coverage_consumability.py -p no:cacheprovider -q
"""

from __future__ import annotations

from okto_pulse.core.models.db import Card, CardStatus, CardType, Spec
from okto_pulse.core.services.bug_regression_scenarios import (
    AmendmentLineageFact,
    BugRegressionCoverageState,
    BugRegressionEligibilityReason,
    BugRegressionRejectionReason,
    CoverageConfirmationFact,
    CoverageConsumabilityVerdict,
    evaluate_coverage_confirmation_consumability,
)

SAME_SPEC_ID = "spec-1"
CROSS_SPEC_ID = "other-spec"

SAME_SPEC_LINKED = "ts-origin-direct"       # same-spec, linked to the origin task
SAME_SPEC_UNRELATED = "ts-samespec-unrelated"  # same-spec, NOT linked to lineage
CROSS_SPEC_SCENARIO = "ts-foreign"          # lives on another spec (Path B only)


def _spec() -> Spec:
    return Spec(
        id=SAME_SPEC_ID,
        board_id="board-1",
        title="Bug spec",
        created_by="agent",
        test_scenarios=[
            {"id": SAME_SPEC_LINKED, "title": "Origin direct", "status": "passed"},
            {"id": SAME_SPEC_UNRELATED, "title": "AC happy-path", "status": "passed"},
        ],
    )


def _card(card_id: str, *, card_type: CardType = CardType.NORMAL,
          test_scenario_ids: list[str] | None = None) -> Card:
    return Card(
        id=card_id,
        board_id="board-1",
        spec_id=SAME_SPEC_ID,
        title=card_id,
        status=CardStatus.DONE,
        card_type=card_type,
        created_by="agent",
        test_scenario_ids=test_scenario_ids,
    )


def _fact(**over) -> AmendmentLineageFact:
    """Fully-valid amendment for bug-1 on spec-1, claiming origin-1 (a member of
    the bug authoritative set) and declaring the cross-spec artifact."""
    base = dict(
        amendment_revision_id="amd-1",
        board_id="board-1",
        original_spec_id=SAME_SPEC_ID,
        origin_bug_id="bug-1",
        status="done",
        lineage_state="complete",
        origin_task_ids=("origin-1",),
        affected_task_ids=(),
        regression_scenario_ids=(CROSS_SPEC_SCENARIO,),
        regression_test_task_ids=("tc-1",),
        automated_regression_refs=(),
    )
    base.update(over)
    return AmendmentLineageFact(**base)


def _confirmation(scenario_id: str, **over) -> CoverageConfirmationFact:
    base = dict(
        validator_id="claude-validator",
        amendment_revision_id="amd-1",
        regression_test_task_id="tc-1",
        regression_scenario_id=scenario_id,
        evidence_ref="tests/test_x.py::test_y",
        confirmed_at="2026-06-28T10:00:00Z",
    )
    base.update(over)
    return CoverageConfirmationFact(**base)


def _evaluate(scenario_id: str, scenario_spec_id: str, *,
              fact_over: dict | None = None) -> CoverageConsumabilityVerdict:
    return evaluate_coverage_confirmation_consumability(
        bug_card=_card("bug-1", card_type=CardType.BUG),
        original_spec=_spec(),
        origin_task=_card("origin-1", test_scenario_ids=[SAME_SPEC_LINKED]),
        affected_tasks=None,
        amendment_fact=_fact(**(fact_over or {})),
        candidate_confirmation=_confirmation(scenario_id),
        scenario_id=scenario_id,
        scenario_spec_id=scenario_spec_id,
    )


# ---------------------------------------------------------------------------
# Cross-spec (Path B) — the candidate attestation drives path_b_ready.
# ---------------------------------------------------------------------------


def test_cross_spec_valid_candidate_is_consumable() -> None:
    verdict = _evaluate(CROSS_SPEC_SCENARIO, CROSS_SPEC_ID)
    assert verdict.consumable is True
    assert verdict.routed_path == "path_b"
    assert verdict.coverage_state is BugRegressionCoverageState.PATH_B_READY
    assert verdict.eligibility_reason is BugRegressionEligibilityReason.PATH_B_AMENDMENT_LINEAGE
    assert verdict.reject_reason is None


def test_cross_spec_claim_without_bug_membership_not_consumable() -> None:
    # Amendment claims a task that is NOT in the bug authoritative set -> rank 4
    # ADJ-A reject. Changing exactly this field flips the positive above.
    verdict = _evaluate(
        CROSS_SPEC_SCENARIO, CROSS_SPEC_ID,
        fact_over={"origin_task_ids": ("foreign-task",)},
    )
    assert verdict.consumable is False
    assert verdict.routed_path == "path_b"
    assert verdict.reject_reason is BugRegressionRejectionReason.UNRELATED_CROSS_SPEC_SCENARIO


def test_cross_spec_incomplete_lineage_not_consumable() -> None:
    verdict = _evaluate(
        CROSS_SPEC_SCENARIO, CROSS_SPEC_ID,
        fact_over={"lineage_state": "incomplete"},
    )
    assert verdict.consumable is False
    assert verdict.reject_reason is BugRegressionRejectionReason.INCOMPLETE_AMENDMENT_LINEAGE


def test_cross_spec_no_linked_amendment_not_consumable() -> None:
    verdict = _evaluate(
        CROSS_SPEC_SCENARIO, CROSS_SPEC_ID,
        fact_over={"origin_bug_id": "another-bug"},
    )
    assert verdict.consumable is False
    assert verdict.reject_reason is BugRegressionRejectionReason.MISSING_AMENDMENT_REVISION


# ---------------------------------------------------------------------------
# Same-spec (Path A) — the amendment is irrelevant; lineage decides.
# ---------------------------------------------------------------------------


def test_same_spec_lineage_linked_is_consumable() -> None:
    verdict = _evaluate(SAME_SPEC_LINKED, SAME_SPEC_ID)
    assert verdict.consumable is True
    assert verdict.routed_path == "path_a"
    assert verdict.eligibility_reason is BugRegressionEligibilityReason.ORIGIN_TASK_DIRECT


def test_same_spec_unrelated_with_intersecting_claim_not_consumable() -> None:
    """THE TEETH (TS-BUG01-2 / TS-BUG02-1 at the unit level).

    The amendment declares the same-spec scenario AND claims origin-1, which IS
    in the bug authoritative set — so a Path-B-only preflight would pass ranks
    3+4 and WRONGLY accept. The scenario is same-spec and not linked to the bug
    lineage, so the resolver routes it through Path A and rejects it
    ``unrelated_scenario``. A Path-B-only implementation fails this test.
    """
    verdict = _evaluate(
        SAME_SPEC_UNRELATED, SAME_SPEC_ID,
        fact_over={"regression_scenario_ids": (SAME_SPEC_UNRELATED,)},
    )
    assert verdict.consumable is False
    assert verdict.routed_path == "path_a"
    assert verdict.reject_reason is BugRegressionRejectionReason.UNRELATED_SCENARIO
