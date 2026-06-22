"""SPEC e2598178 / card ddaa152b — KG connectivity guard ignores semantic
self-loops (scenarios ts_aaa12f68, ts_fbd5c684).

A semantic edge whose resolved endpoint is the guarded node itself can never
establish connectivity — a node cannot justify its own existence. These
regressions prove:

* ts_aaa12f68 — a Decision whose ONLY judgement edge is a self-loop is rejected
  with the dedicated, bounded ``self_loop_not_connectivity`` reason, and the
  self-loop grants no connectivity credit;
* ts_fbd5c684 — a Decision with one self-loop AND one judgement edge to a
  DISTINCT node passes only while the distinct edge exists; the regression
  variant (distinct edge removed) fails closed because the self-loop is ignored.

Plus guard rails from the dispatch: Learning/Bug provenance must not regress and
the rejection metric must stay closed-vocabulary (no free content).

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_kg_self_loop_connectivity.py
"""

from __future__ import annotations

from types import SimpleNamespace

from okto_pulse.core.kg.connectivity_guard import (
    SAFE_CONNECTIVITY_METRIC_LABELS,
    SELF_LOOP_NOT_CONNECTIVITY_REASON,
    InMemoryConnectivityMetricSink,
    KGConnectivityOutcome,
    KGNodeConnectivityGuard,
    KGNodeRef,
)


def _node(candidate_id: str, node_type: str, source_ref: str = ""):
    return SimpleNamespace(
        candidate_id=candidate_id,
        node_type=node_type,
        source_artifact_ref=source_ref,
    )


def _edge(candidate_id: str, edge_type: str, source: str, target: str):
    return SimpleNamespace(
        candidate_id=candidate_id,
        edge_type=edge_type,
        from_candidate_id=source,
        to_candidate_id=target,
    )


# Provenance (belongs_to -> Entity) keeps the FIRST required group satisfied so
# the validation reaches the decision_judgement group under test.
_PROVENANCE_EDGE = _edge("e-prov", "belongs_to", "decision-1", "kg:entity_spec_1")
_PROVENANCE_REF = KGNodeRef(ref_id="entity_spec_1", node_type="Entity")


def _validate_decision(edges, *, existing=(), sink=None):
    guard = KGNodeConnectivityGuard(metric_sink=sink)
    return guard.validate(
        board_id="board-1",
        writer_path="commit_consolidation",
        kg_health_state="healthy",
        generation_id="gen-self-loop",
        nodes=[_node("decision-1", "Decision", "spec:spec-1:decision:0")],
        edges=list(edges),
        existing_node_refs=[_PROVENANCE_REF, *existing],
    )


# ---------------------------------------------------------------------------
# ts_aaa12f68 — self-loop-only Decision fails connectivity
# ---------------------------------------------------------------------------


def test_self_loop_only_decision_fails_connectivity_guard():
    sink = InMemoryConnectivityMetricSink()
    # The Decision's only judgement edge points from the Decision to itself.
    result = _validate_decision(
        [
            _PROVENANCE_EDGE,
            _edge("e-self", "depends_on", "decision-1", "decision-1"),
        ],
        sink=sink,
    )

    assert result.passed is False
    assert result.outcome == KGConnectivityOutcome.REJECTED
    assert len(result.violations) == 1
    violation = result.violations[0]
    # Dedicated, explicit self-loop diagnostic — the self-loop grants no credit.
    assert violation.reason == SELF_LOOP_NOT_CONNECTIVITY_REASON
    assert violation.reason == "self_loop_not_connectivity"
    assert violation.node_type == "Decision"
    # Failing on the judgement group, not on provenance (which is satisfied):
    # the judgement group label carries the judgement edge types, never belongs_to.
    assert "depends_on" in violation.required_edge
    assert "belongs_to" not in violation.required_edge

    # Metric stays closed-vocabulary: bounded labels, dedicated reason, no
    # free-form content leaking through.
    assert len(sink.events) == 1
    labels = sink.events[0].labels()
    assert tuple(labels.keys()) == SAFE_CONNECTIVITY_METRIC_LABELS
    assert labels["outcome"] == "rejected"
    assert labels["reason"] == "self_loop_not_connectivity"
    assert labels["source_resolution_status"] == "unresolved_source_ref"
    assert "spec:spec-1:decision:0" not in " ".join(labels.values())


def test_self_loop_by_source_artifact_ref_is_also_rejected():
    # A self-loop expressed via the node's source_artifact_ref (not its
    # candidate_id) is just as invalid as a candidate_id self-loop.
    guard = KGNodeConnectivityGuard()
    result = guard.validate(
        board_id="board-1",
        writer_path="commit_consolidation",
        kg_health_state="healthy",
        nodes=[_node("decision-1", "Decision", "spec:spec-1:decision:0")],
        edges=[
            _PROVENANCE_EDGE,
            # endpoint references the node by its own source_artifact_ref.
            _edge("e-self", "depends_on", "decision-1", "spec:spec-1:decision:0"),
        ],
        existing_node_refs=[_PROVENANCE_REF],
    )

    assert result.passed is False
    assert result.violations[0].reason == SELF_LOOP_NOT_CONNECTIVITY_REASON


# ---------------------------------------------------------------------------
# ts_fbd5c684 — distinct judgement edge (not the self-loop) satisfies the guard
# ---------------------------------------------------------------------------


def test_distinct_judgement_edge_satisfies_and_self_loop_is_ignored():
    distinct_ref = KGNodeRef(ref_id="decision_2", node_type="Decision")
    self_loop = _edge("e-self", "depends_on", "decision-1", "decision-1")
    distinct = _edge("e-distinct", "depends_on", "decision-1", "kg:decision_2")

    # With BOTH the self-loop and the distinct judgement edge, connectivity is
    # satisfied — by the distinct edge alone.
    passing = _validate_decision(
        [_PROVENANCE_EDGE, self_loop, distinct],
        existing=(distinct_ref,),
    )
    assert passing.passed is True
    assert passing.violations == ()

    # Regression variant: remove the distinct edge, leaving ONLY the self-loop.
    # The candidate now fails closed — proving the self-loop is ignored and the
    # distinct edge was the sole source of connectivity.
    regression = _validate_decision(
        [_PROVENANCE_EDGE, self_loop],
        existing=(distinct_ref,),
    )
    assert regression.passed is False
    assert regression.violations[0].reason == SELF_LOOP_NOT_CONNECTIVITY_REASON


# ---------------------------------------------------------------------------
# Guard rails — the self-loop check must not regress other node types
# ---------------------------------------------------------------------------


def test_self_loop_check_does_not_regress_learning_and_bug_provenance():
    guard = KGNodeConnectivityGuard()

    # R7 bug-derived Learning -> validates -> canonical Bug still passes.
    learning = guard.validate(
        board_id="board-1",
        writer_path="commit_consolidation",
        kg_health_state="healthy",
        nodes=[_node("learning-1", "Learning", "card:bug:bug-1:learning:0")],
        edges=[_edge("e-1", "validates", "learning-1", "kg:bug_1")],
        existing_node_refs=[
            KGNodeRef(ref_id="bug_1", node_type="Bug", graph_layer="canonical"),
        ],
    )
    assert learning.passed is True

    # Bug node attached to the board root (distinct Entity) still passes.
    bug = guard.validate(
        board_id="board-1",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=[
            _node("board-root", "Entity", "board:board-1"),
            _node("bug-1", "Bug", "card:bug:bug-1"),
        ],
        edges=[_edge("e-1", "belongs_to", "bug-1", "board-root")],
    )
    assert bug.passed is True
    assert "bug-1" not in bug.allowlisted_roots


def test_self_loop_reason_is_distinct_from_missing_required_edge():
    # A Decision with NO judgement edge at all reports missing_required_edge —
    # NOT the self-loop reason. The two diagnostics must not be conflated.
    no_judgement = _validate_decision([_PROVENANCE_EDGE])
    assert no_judgement.passed is False
    assert no_judgement.violations[0].reason == "missing_required_edge"
    assert no_judgement.violations[0].reason != SELF_LOOP_NOT_CONNECTIVITY_REASON
