"""Spec f24c43f7 / card 4836a25b — the MCP cognitive consolidation path rejects
Criterion/Constraint candidates BEFORE any graph mutation, with a structured error and
actionable remediation (FR5, TR3/TR4/TR5, AC4/AC5/AC8), scenarios ts_9fae301e (Criterion)
+ ts_ab1dc76c (Constraint).

Criterion/Constraint are deterministic-only (connectivity_guard ownership table — NOT
changed by this card). A cognitive writer (writer_path 'commit_consolidation') that
proposes one is rejected with status=source_type_not_supported, reason=
writer_not_connectivity_owner, and a remediation enumerating the three actionable
options. The guard runs before the Kùzu write, so nothing is durably written. The
positive case (Alternative/Assumption still commits, TR7) is card 38be8c3a.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_kg_consolidation_rejects_criterion_constraint.py
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.connectivity_guard import (
    KGNodeConnectivityGuard,
    SourceResolutionStatus,
)
from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    add_node_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from okto_pulse.core.kg.schemas import (
    AddNodeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    KGNodeType,
    NodeCandidate,
    ProposeReconciliationRequest,
)

_REMEDIATION_OPTIONS = (
    "remove the invalid candidate",
    "abort and recreate",
    "connectivity owner",
)


def _node(candidate_id: str, node_type: str, source_ref: str = ""):
    return SimpleNamespace(
        candidate_id=candidate_id, node_type=node_type, source_artifact_ref=source_ref
    )


def _count_by_source_ref(board_id: str, node_type: str, source_ref: str) -> int:
    from okto_pulse.core.kg.schema import open_board_connection

    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            f"MATCH (n:{node_type}) WHERE n.source_artifact_ref = $ref RETURN count(n)",
            {"ref": source_ref},
        )
        try:
            if res.has_next():
                return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass
    return 0


# ---------------------------------------------------------------------------
# guard-unit (TR4) — Criterion AND Constraint rejected for a cognitive writer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("node_type", ["Criterion", "Constraint"])
def test_guard_rejects_deterministic_only_node_for_cognitive_writer(node_type):
    guard = KGNodeConnectivityGuard()
    result = guard.validate(
        board_id="board-1",
        writer_path="commit_consolidation",  # classify_writer_path -> COGNITIVE
        kg_health_state="healthy",
        nodes=[_node("c1", node_type, f"{node_type.lower()}:1")],
        edges=[],
        existing_node_refs=[],
    )
    assert result.passed is False
    v = result.violations[0]
    assert v.node_type == node_type
    assert v.reason == "writer_not_connectivity_owner"
    assert v.source_resolution_status == SourceResolutionStatus.SOURCE_TYPE_NOT_SUPPORTED
    # AC8: the remediation enumerates the three actionable options + the `remediation`
    # alias mirrors `remediation_hint` (contract-stable key).
    payload = v.to_safe_dict()
    for option in _REMEDIATION_OPTIONS:
        assert option in payload["remediation_hint"], option
    assert payload["remediation"] == payload["remediation_hint"]


def test_guard_distinguishes_wrong_writer_from_missing_connectivity():
    """TR5: node_type-not-permitted-by-writer_path is a DIFFERENT failure than a
    missing semantic connectivity edge."""
    guard = KGNodeConnectivityGuard()
    # Criterion via cognitive writer -> wrong-writer (node_type not permitted).
    wrong_writer = guard.validate(
        board_id="b", writer_path="commit_consolidation", kg_health_state="healthy",
        nodes=[_node("c1", "Criterion", "criterion:1")], edges=[], existing_node_refs=[],
    )
    # Decision IS cognitive-allowed but missing its required edges -> connectivity failure.
    missing_conn = guard.validate(
        board_id="b", writer_path="commit_consolidation", kg_health_state="healthy",
        nodes=[_node("d1", "Decision", "spec:x:decision:1")], edges=[], existing_node_refs=[],
    )
    assert wrong_writer.violations[0].reason == "writer_not_connectivity_owner"
    assert missing_conn.violations[0].reason != "writer_not_connectivity_owner"


# ---------------------------------------------------------------------------
# integration (ts_9fae301e + ts_ab1dc76c) — MCP commit path, no mutation
# ---------------------------------------------------------------------------


async def _begin_with_candidate(board_id, agent_id, db_factory, *, node_type, source_ref, cid):
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=source_ref.split(":", 1)[-1],
                raw_content=f"closeout ownership {source_ref}",
            ),
            agent_id=agent_id,
            db=db,
        )
    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id=cid,
                node_type=node_type,
                title=f"Cognitive {node_type.value}",
                source_artifact_ref=source_ref,
                source_confidence=0.95,
            ),
        ),
        agent_id=agent_id,
    )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=agent_id,
        db=None,
        force_reprocess=True,
    )
    return begin


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "node_type,label",
    [(KGNodeType.CRITERION, "Criterion"), (KGNodeType.CONSTRAINT, "Constraint")],
)
async def test_commit_rejects_cognitive_criterion_constraint_before_mutation(
    node_type, label, board_id, agent_id, db_factory, board_handle
):
    source_ref = f"spec:{label.lower()}:{uuid.uuid4()}"
    begin = await _begin_with_candidate(
        board_id, agent_id, db_factory,
        node_type=node_type, source_ref=source_ref, cid=f"{label.lower()}_cog",
    )

    with pytest.raises(KGPrimitiveError) as exc_info:
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=begin.session_id),
                agent_id=agent_id,
                db=db,
            )

    # rejected before graph mutation with the structured connectivity error.
    assert exc_info.value.code == "kg_node_connectivity_violation"
    connectivity = exc_info.value.details["connectivity"]
    assert connectivity["passed"] is False
    violation = next(
        v for v in connectivity["violations"] if v["source_artifact_ref"] == source_ref
    )
    assert violation["node_type"] == label
    assert violation["reason"] == "writer_not_connectivity_owner"
    assert violation["source_resolution_status"] == "source_type_not_supported"
    # AC8: actionable remediation present (both keys) with the three options.
    for option in _REMEDIATION_OPTIONS:
        assert option in violation["remediation_hint"], option
    assert violation["remediation"] == violation["remediation_hint"]
    # no content leak in the safe payload.
    assert "content" not in violation
    # AC4/AC5 + TR3: nothing was durably written to the graph.
    assert _count_by_source_ref(board_id, label, source_ref) == 0
