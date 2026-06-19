"""SPEC f5a7cae7 / card ead17e4d — shared Path A/Path B bug regression predicate.

Path B is a NARROW ADDITIVE allow on top of the existing cross-spec reject and
is fail-closed. Every negative below ships with its positive neighbour (change
exactly one field -> it flips), so each reject is proven to exercise the real
condition (the teeth the validator asked for).

Covers FR1-FR4, TR1/TR2/TR4, BR exact membership + P1-P7 + ADJ-A..D.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_bug_regression_path_b.py
"""

from __future__ import annotations

from copy import deepcopy

from okto_pulse.core.models.db import Card, CardStatus, CardType, Spec
from okto_pulse.core.services.bug_regression_scenarios import (
    AmendmentLineageFact,
    BugRegressionCoverageState,
    BugRegressionEligibilityReason,
    BugRegressionGateDecision,
    BugRegressionGateValidator,
    BugRegressionNextAction,
    BugRegressionRejectionReason,
    BugRegressionScenarioEligibilityResolver,
    CoverageConfirmationFact,
)

FOREIGN = "ts-foreign"  # a scenario that lives on another spec (cross-spec)


def _spec() -> Spec:
    return Spec(
        id="spec-1",
        board_id="board-1",
        title="Locked bug spec",
        created_by="agent",
        test_scenarios=[
            {"id": "ts-origin-direct", "title": "Origin direct", "status": "passed"},
        ],
    )


def _card(
    card_id: str,
    *,
    spec_id: str = "spec-1",
    card_type: CardType = CardType.NORMAL,
    test_scenario_ids: list[str] | None = None,
) -> Card:
    return Card(
        id=card_id,
        board_id="board-1",
        spec_id=spec_id,
        title=card_id,
        status=CardStatus.DONE,
        card_type=card_type,
        created_by="agent",
        test_scenario_ids=test_scenario_ids,
    )


def _fact(**over) -> AmendmentLineageFact:
    """A fully-valid amendment lineage fact for bug-1 on spec-1 declaring FOREIGN
    and claiming a task that IS in the bug's authoritative set (origin-1)."""
    base = dict(
        amendment_revision_id="amd-1",
        board_id="board-1",
        original_spec_id="spec-1",   # == bug.spec_id
        origin_bug_id="bug-1",       # == bug.id
        status="done",               # non-blocking
        lineage_state="complete",
        origin_task_ids=("origin-1",),       # exact member of bug authoritative
        affected_task_ids=(),
        regression_scenario_ids=(FOREIGN,),  # declares the artifact
        regression_test_task_ids=("tc-1",),  # declares the regression test task
        automated_regression_refs=(),
    )
    base.update(over)
    return AmendmentLineageFact(**base)


def _confirmation(**over) -> CoverageConfirmationFact:
    """A valid, artifact-bound validator coverage attestation for _fact() (G2).
    Coverage is reached ONLY via this persisted bound signal — never a bool."""
    base = dict(
        validator_id="claude-validator",
        amendment_revision_id="amd-1",       # binds to THIS amendment
        regression_test_task_id="tc-1",      # ∈ amendment.regression_test_task_ids
        regression_scenario_id=FOREIGN,      # ∈ amendment.regression_scenario_ids
        evidence_ref="tests/test_x.py::test_y",
        confirmed_at="2026-06-19T10:00:00Z",
    )
    base.update(over)
    return CoverageConfirmationFact(**base)


def _resolve(*, facts, fact_overrides=None):
    spec = _spec()
    origin = _card("origin-1", test_scenario_ids=[])
    bug = _card("bug-1", card_type=CardType.BUG)
    amendment_facts = None
    if facts is not None:
        amendment_facts = [
            _fact(**(o or {})) for o in (fact_overrides or [{}])
        ] if facts == "valid" else facts
    return BugRegressionScenarioEligibilityResolver().resolve(
        bug_card=bug,
        spec=spec,
        origin_task=origin,
        candidate_scenario_ids=[FOREIGN],
        candidate_spec_ids_by_scenario_id={FOREIGN: "other-spec"},
        amendment_facts=amendment_facts,
    )


def _reject_reason(result):
    assert result.eligible_scenarios == ()
    assert result.rejected_scenarios, "expected a rejection"
    return result.rejected_scenarios[0].reason


# ---------------------------------------------------------------------------
# ADJ-C / backward compat: legacy Path-A-only mode is unchanged.
# ---------------------------------------------------------------------------


def test_legacy_mode_preserves_cross_spec_reject():
    # amendment_facts=None -> the predicate is NOT active; cross-spec is the
    # legacy CROSS_SPEC_SCENARIO reject (never silently relaxed).
    result = _resolve(facts=None)
    assert _reject_reason(result) is BugRegressionRejectionReason.CROSS_SPEC_SCENARIO
    assert result.coverage_state is BugRegressionCoverageState.NOT_APPLICABLE


# ---------------------------------------------------------------------------
# P2/P3 + ADJ-C: cross-spec without a formal amendment stays blocked.
# ---------------------------------------------------------------------------


def test_p2_p3_no_amendment_blocks_missing_amendment_revision():
    # Path B context active but NO amendment (hotfix lane/label/manual link can
    # never satisfy Path B). eligible=0, fail-closed.
    result = _resolve(facts=[])
    assert _reject_reason(result) is BugRegressionRejectionReason.MISSING_AMENDMENT_REVISION
    assert "amendment_revision" in result.missing_links
    # positive neighbour: add the valid amendment -> becomes lineage eligible.
    ok = _resolve(facts="valid")
    assert ok.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING


def test_amendment_must_formally_link_this_bug_and_spec():
    # wrong bug -> not linked -> missing_amendment_revision.
    wrong_bug = _resolve(facts=[_fact(origin_bug_id="other-bug")])
    assert _reject_reason(wrong_bug) is BugRegressionRejectionReason.MISSING_AMENDMENT_REVISION
    # wrong spec -> not linked.
    wrong_spec = _resolve(facts=[_fact(original_spec_id="other-spec")])
    assert _reject_reason(wrong_spec) is BugRegressionRejectionReason.MISSING_AMENDMENT_REVISION
    # wrong board -> not linked.
    wrong_board = _resolve(facts=[_fact(board_id="other-board")])
    assert _reject_reason(wrong_board) is BugRegressionRejectionReason.MISSING_AMENDMENT_REVISION
    # positive neighbour: correct linkage -> eligible lineage.
    ok = _resolve(facts="valid")
    assert ok.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING


# ---------------------------------------------------------------------------
# blocked_amendment_status (== TS1 / ts_cc824ace THEN) — P5/P7.
# ---------------------------------------------------------------------------


def test_blocked_amendment_status_for_each_blocking_state():
    for status in ("draft", "review", "cancelled", "superseded", "totally_made_up"):
        result = _resolve(facts=[_fact(status=status)])
        assert _reject_reason(result) is (
            BugRegressionRejectionReason.BLOCKED_AMENDMENT_STATUS
        ), status
        assert result.amendment_status == status
    # positive neighbour: done is non-blocking -> passes the status gate.
    ok = _resolve(facts=[_fact(status="done")])
    assert ok.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING


def test_path_b_draft_amendment_blocks_gate():
    # The exact THEN of TS1 (ts_cc824ace): a draft amendment with complete
    # lineage stays blocked and the gate is NOT allowed. (TS1 closes via the
    # 98e91b04 dependency once this resolver lands; this is its real test.)
    spec, origin, bug = _spec(), _card("origin-1"), _card("bug-1", card_type=CardType.BUG)
    test_task = _card("tc-1", spec_id="other-spec", test_scenario_ids=[FOREIGN])
    gate = BugRegressionGateValidator().validate_linked_test_tasks(
        bug_card=bug,
        linked_test_tasks=[test_task],
        spec=spec,
        origin_task=origin,
        candidate_spec_ids_by_scenario_id={FOREIGN: "other-spec"},
        amendment_facts=[_fact(status="draft", lineage_state="complete")],
    )
    assert gate.allowed is False
    assert gate.eligibility.rejected_scenarios[0].reason is (
        BugRegressionRejectionReason.BLOCKED_AMENDMENT_STATUS
    )


# ---------------------------------------------------------------------------
# incomplete_amendment_lineage — P5.
# ---------------------------------------------------------------------------


def test_incomplete_amendment_lineage_blocks():
    result = _resolve(facts=[_fact(lineage_state="incomplete")])
    assert _reject_reason(result) is (
        BugRegressionRejectionReason.INCOMPLETE_AMENDMENT_LINEAGE
    )
    # positive neighbour: complete lineage -> passes the lineage gate.
    ok = _resolve(facts=[_fact(lineage_state="complete")])
    assert ok.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING


# ---------------------------------------------------------------------------
# missing_regression_artifact — FR2 "artifact declared by the amendment".
# ---------------------------------------------------------------------------


def test_missing_regression_artifact_when_scenario_not_declared():
    result = _resolve(facts=[_fact(regression_scenario_ids=())])
    assert _reject_reason(result) is (
        BugRegressionRejectionReason.MISSING_REGRESSION_ARTIFACT
    )
    # positive neighbour: declare the scenario -> passes the artifact gate.
    ok = _resolve(facts=[_fact(regression_scenario_ids=(FOREIGN,))])
    assert ok.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING


# ---------------------------------------------------------------------------
# ADJ-A (CRITICAL) / P4 — membership is authoritative from the BUG, not the
# amendment. A fabricated/foreign task_id never grants eligibility.
# ---------------------------------------------------------------------------


def test_adj_a_fabricated_task_id_blocks_unrelated_cross_spec():
    # amendment CLAIMS a task that is NOT in the bug authoritative set {origin-1}.
    result = _resolve(
        facts=[_fact(origin_task_ids=("fabricated-task",), affected_task_ids=("also-fake",))]
    )
    assert _reject_reason(result) is (
        BugRegressionRejectionReason.UNRELATED_CROSS_SPEC_SCENARIO
    )
    assert "authoritative_task_membership" in result.missing_links
    # positive neighbour: claim the real authoritative task -> eligible lineage.
    ok = _resolve(facts=[_fact(origin_task_ids=("origin-1",))])
    assert ok.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING


def test_adj_a_partial_claim_keeps_only_compatible_membership():
    # amendment claims one real + one fabricated task: the intersection with the
    # bug authoritative set is non-empty (origin-1), so membership holds.
    ok = _resolve(facts=[_fact(origin_task_ids=("origin-1", "fabricated-task"))])
    assert ok.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING
    assert ok.rejected_scenarios == ()


# ---------------------------------------------------------------------------
# P6 + ADJ-B — lineage eligible without confirmed coverage is coverage_pending,
# never closure-ready.
# ---------------------------------------------------------------------------


def test_p6_full_lineage_without_coverage_is_pending_not_ready():
    result = _resolve(facts="valid")  # no attestation -> coverage_pending
    assert result.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING
    assert result.coverage_pending_scenarios == (FOREIGN,)
    assert result.eligible_scenarios == ()  # NOT closure-eligible
    assert result.rejected_scenarios == ()  # NOT a hard reject either
    assert result.semantic_gap_required is False  # lineage exists -> no spec mutation
    assert result.spec_mutation_required is False
    assert result.next_action is BugRegressionNextAction.CONFIRM_VALIDATOR_COVERAGE
    assert "validator_coverage" in result.missing_links
    assert result.amendment_revision_id == "amd-1"


def test_path_b_ready_only_with_bound_validator_attestation():
    # G2 (c9cf9781): path_b_ready is reached ONLY via a persisted, artifact-bound
    # validator attestation carried on the fact — never a caller-supplied bool.
    result = _resolve(
        facts="valid", fact_overrides=[{"coverage_confirmation": _confirmation()}]
    )
    assert result.coverage_state is BugRegressionCoverageState.PATH_B_READY
    assert [s.scenario_id for s in result.eligible_scenarios] == [FOREIGN]
    assert result.eligible_scenarios[0].reason is (
        BugRegressionEligibilityReason.PATH_B_AMENDMENT_LINEAGE
    )
    assert result.eligible_scenarios[0].source_task_id == "origin-1"
    assert result.rejected_scenarios == ()


# ---------------------------------------------------------------------------
# G2 (c9cf9781) — coverage attestation BINDING forge teeth. The positive
# neighbour above (valid bound attestation -> path_b_ready) flips to
# coverage_pending when ANY binding field is forged. Proves the gate honours a
# bound attestation, never a bare/foreign dict.
# ---------------------------------------------------------------------------


def test_coverage_attestation_foreign_amendment_does_not_confirm():
    # bound to ANOTHER amendment_revision_id -> not confirmed (coverage_pending).
    result = _resolve(
        facts="valid",
        fact_overrides=[{"coverage_confirmation": _confirmation(amendment_revision_id="other-amd")}],
    )
    assert result.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING
    assert result.eligible_scenarios == ()


def test_coverage_attestation_foreign_artifact_does_not_confirm():
    # test task not declared by THIS amendment -> not confirmed.
    foreign_task = _resolve(
        facts="valid",
        fact_overrides=[{"coverage_confirmation": _confirmation(regression_test_task_id="other-tc")}],
    )
    assert foreign_task.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING
    # scenario not declared by THIS amendment (and not the candidate) -> not confirmed.
    foreign_sc = _resolve(
        facts="valid",
        fact_overrides=[{"coverage_confirmation": _confirmation(regression_scenario_id="ts-other")}],
    )
    assert foreign_sc.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING


def test_coverage_attestation_missing_validator_or_evidence_does_not_confirm():
    no_validator = _resolve(
        facts="valid",
        fact_overrides=[{"coverage_confirmation": _confirmation(validator_id="")}],
    )
    assert no_validator.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING
    no_evidence = _resolve(
        facts="valid",
        fact_overrides=[{"coverage_confirmation": _confirmation(evidence_ref="")}],
    )
    assert no_evidence.coverage_state is BugRegressionCoverageState.COVERAGE_PENDING


# ---------------------------------------------------------------------------
# Gate decisions through BugRegressionGateValidator (the real gate surface).
# ---------------------------------------------------------------------------


def _gate(*, amendment_facts):
    spec, origin, bug = _spec(), _card("origin-1"), _card("bug-1", card_type=CardType.BUG)
    test_task = _card("tc-1", spec_id="other-spec", test_scenario_ids=[FOREIGN])
    return BugRegressionGateValidator().validate_linked_test_tasks(
        bug_card=bug,
        linked_test_tasks=[test_task],
        spec=spec,
        origin_task=origin,
        candidate_spec_ids_by_scenario_id={FOREIGN: "other-spec"},
        amendment_facts=amendment_facts,
    )


def test_gate_allows_only_path_b_ready():
    # only a bound validator attestation reaches ALLOW.
    allowed = _gate(amendment_facts=[_fact(coverage_confirmation=_confirmation())])
    assert allowed.allowed is True
    assert allowed.decision is BugRegressionGateDecision.ALLOW


def test_gate_blocks_coverage_pending():
    blocked = _gate(amendment_facts=[_fact()])  # lineage ok, no attestation
    assert blocked.allowed is False
    assert blocked.decision is BugRegressionGateDecision.BLOCK_COVERAGE_PENDING


def test_gate_blocks_when_no_amendment():
    blocked = _gate(amendment_facts=[])
    assert blocked.allowed is False
    # missing amendment -> no reusable lineage at all -> the remediation is to
    # create the formal amendment (the semantic-gap escalation family). The
    # precise, stable signal is the reason code.
    assert blocked.decision is BugRegressionGateDecision.BLOCK_SEMANTIC_GAP
    assert blocked.eligibility.semantic_gap_required is True
    assert blocked.eligibility.rejected_scenarios[0].reason is (
        BugRegressionRejectionReason.MISSING_AMENDMENT_REVISION
    )


# ---------------------------------------------------------------------------
# Purity — the predicate never mutates inputs (FR/AC1 family).
# ---------------------------------------------------------------------------


def test_predicate_does_not_mutate_inputs():
    spec = _spec()
    origin = _card("origin-1")
    bug = _card("bug-1", card_type=CardType.BUG)
    fact = _fact()
    before_spec = deepcopy(spec.test_scenarios)
    BugRegressionScenarioEligibilityResolver().resolve(
        bug_card=bug,
        spec=spec,
        origin_task=origin,
        candidate_scenario_ids=[FOREIGN],
        candidate_spec_ids_by_scenario_id={FOREIGN: "other-spec"},
        amendment_facts=[fact],
    )
    assert spec.test_scenarios == before_spec
    assert fact.regression_scenario_ids == (FOREIGN,)  # fact untouched
