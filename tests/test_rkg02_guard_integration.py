"""RKG-02 — connectivity-guard integration: the type-aware resolver makes a
plain ``card:<uuid>`` Learning behave like ``bug:<uuid>`` when (and only when)
the card is backed by a canonical Bug. This is the original ideation bug
(``card:<id>`` failed while ``bug:<id>`` worked).
"""

from __future__ import annotations

from types import SimpleNamespace

from okto_pulse.core.kg.connectivity_guard import (
    KGNodeConnectivityGuard,
    KGNodeRef,
)

U = "11111111-1111-1111-1111-111111111111"


def _node(candidate_id, node_type, source_ref=""):
    return SimpleNamespace(
        candidate_id=candidate_id, node_type=node_type, source_artifact_ref=source_ref)


def _edge(candidate_id, edge_type, source, target):
    return SimpleNamespace(
        candidate_id=candidate_id, edge_type=edge_type,
        from_candidate_id=source, to_candidate_id=target)


def _validate(guard, learning_source_ref, *, canonical_bug):
    existing = []
    if canonical_bug:
        existing = [KGNodeRef(
            ref_id="bug-node", node_type="Bug", graph_layer="canonical",
            source_artifact_ref=f"bug:{U}")]
    return guard.validate(
        board_id="b1",
        writer_path="commit_consolidation",
        kg_health_state="healthy",
        nodes=[_node("learn-1", "Learning", learning_source_ref)],
        edges=[_edge("e1", "validates", "learn-1", "kg:bug-node")],
        existing_node_refs=existing,
    )


def test_card_uuid_of_canonical_bug_is_bug_derived_and_passes():
    """THE FIX (AC1 / ts_15a04817): a Learning with source_ref card:<uuid> whose
    card is a canonical Bug is treated as bug-derived, so its validates->Bug edge
    satisfies the guard — exactly like bug:<uuid> would."""
    guard = KGNodeConnectivityGuard()
    res = _validate(guard, f"card:{U}", canonical_bug=True)
    assert res.passed is True


def test_bug_uuid_form_still_passes():
    """Regression: the explicit bug:<uuid> form is unchanged."""
    guard = KGNodeConnectivityGuard()
    res = _validate(guard, f"bug:{U}", canonical_bug=True)
    assert res.passed is True


def test_card_uuid_without_canonical_bug_is_not_aliased_to_bug():
    """AC2 / ts_7be24883: when the card is NOT a canonical bug, card:<uuid> is
    NOT silently aliased to a Bug — the validates edge to a non-existent canonical
    Bug does not satisfy the guard, so the write is rejected (no graph mutation)."""
    guard = KGNodeConnectivityGuard()
    res = _validate(guard, f"card:{U}", canonical_bug=False)
    assert res.passed is False
