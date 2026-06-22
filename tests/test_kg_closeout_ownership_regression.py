"""Spec f24c43f7 / card 38be8c3a — consolidated KG closeout ownership regression
(FR6/FR7, AC4/AC5/AC6/AC7, TR4/TR7), scenarios ts_a43c9874 (positive) + ts_9806021d
(docs drift).

One suite that proves the whole contract end-to-end:
* NEGATIVE: a cognitive writer cannot create deterministic-only `Criterion`/`Constraint`
  — the commit fails before any graph mutation;
* POSITIVE: an allowed cognitive node type (`Alternative`) STILL commits, so the
  deterministic-only restriction does not disable the permitted cognitive closeout
  (ts_a43c9874 uses example language; one allowed type suffices). `Assumption` is a
  latent gap — see the test body note — and is out of this card's scope by decision;
* DOCS-DRIFT: the official KG closeout docs never reintroduce `Criterion`/`Constraint` as
  free cognitive candidates.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_kg_closeout_ownership_regression.py
"""

from __future__ import annotations

import re
import uuid

import pytest

from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    _apply_kuzu_node_create_with_timestamp,
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

# Reuse the card #1 docs guard helpers (single source of the semantic drift check).
from test_kg_closeout_docs_deterministic_only import (  # noqa: E402
    TOOLDOCS_KG,
    WORKFLOW_KG,
    _assert_only_deterministic,
    _section,
)


def _seed_entity_root(board_id: str, source_ref: str) -> str:
    from okto_pulse.core.kg.schema import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    root_id = f"entity_seed_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _apply_kuzu_node_create_with_timestamp(
            orch,
            "Entity",
            root_id,
            {
                "title": "Seed Entity",
                "content": "",
                "context": "",
                "justification": "",
                "source_artifact_ref": source_ref,
                "created_at": "2026-06-08T00:00:00+00:00",
                "created_by_agent": "test",
                "source_confidence": 1.0,
                "relevance_score": 0.5,
                "query_hits": 0,
                "last_queried_at": None,
                "priority_boost": 0.0,
                "human_curated": False,
                "embedding": [0.0] * 384,
            },
        )
    return root_id


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
# POSITIVE (ts_a43c9874 / TR7 / AC6) — Alternative + Assumption still commit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_allowed_cognitive_candidates_still_commit(
    board_id, agent_id, db_factory, board_handle
):
    # Positive case (ts_a43c9874 / TR7 / AC6): allowed cognitive node types still
    # commit while deterministic-only Criterion/Constraint remain blocked.
    spec_id = f"spec-{uuid.uuid4()}"
    spec_ref = f"spec:{spec_id}"
    _seed_entity_root(board_id, spec_ref)  # provenance root the cognitive node attaches to
    alt_ref = f"{spec_ref}:alternative:{uuid.uuid4().hex[:8]}"
    assumption_ref = f"{spec_ref}:assumption:{uuid.uuid4().hex[:8]}"

    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id, artifact_type="spec", artifact_id=spec_id,
                raw_content="allowed cognitive closeout",
            ),
            agent_id=agent_id, db=db,
        )
    for cid, node_type, ref, title in (
        ("alt_ok", KGNodeType.ALTERNATIVE, alt_ref, "Considered synchronous polling"),
        ("assumption_ok", KGNodeType.ASSUMPTION, assumption_ref, "Assume replay manifest remains stable"),
    ):
        await add_node_candidate(
            AddNodeCandidateRequest(
                session_id=begin.session_id,
                candidate=NodeCandidate(
                    candidate_id=cid, node_type=node_type, title=title,
                    source_artifact_ref=ref, source_confidence=0.9,
                ),
            ),
            agent_id=agent_id,
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=agent_id, db=None, force_reprocess=True,
    )

    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id, db=db,
        )

    assert commit.connectivity["passed"] is True
    assert commit.nodes_added >= 2
    assert _count_by_source_ref(board_id, "Alternative", alt_ref) == 1
    assert _count_by_source_ref(board_id, "Assumption", assumption_ref) == 1


# ---------------------------------------------------------------------------
# NEGATIVE (ts_9fae301e / ts_ab1dc76c / AC4 / AC5) — Criterion/Constraint fail
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "node_type,label",
    [(KGNodeType.CRITERION, "Criterion"), (KGNodeType.CONSTRAINT, "Constraint")],
)
async def test_deterministic_only_candidates_fail_without_mutation(
    node_type, label, board_id, agent_id, db_factory, board_handle
):
    source_ref = f"spec:{label.lower()}:{uuid.uuid4()}"
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id, artifact_type="spec",
                artifact_id=source_ref.split(":", 1)[-1], raw_content="deterministic-only",
            ),
            agent_id=agent_id, db=db,
        )
    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id=f"{label.lower()}_cog", node_type=node_type,
                title=f"Cognitive {label}", source_artifact_ref=source_ref, source_confidence=0.9,
            ),
        ),
        agent_id=agent_id,
    )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=agent_id, db=None, force_reprocess=True,
    )

    with pytest.raises(KGPrimitiveError) as exc_info:
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=begin.session_id),
                agent_id=agent_id, db=db,
            )
    assert exc_info.value.code == "kg_node_connectivity_violation"
    violation = next(
        v for v in exc_info.value.details["connectivity"]["violations"]
        if v["source_artifact_ref"] == source_ref
    )
    assert violation["reason"] == "writer_not_connectivity_owner"
    assert violation["source_resolution_status"] == "source_type_not_supported"
    assert _count_by_source_ref(board_id, label, source_ref) == 0


# ---------------------------------------------------------------------------
# DOCS-DRIFT (ts_9806021d / AC7) — official closeout docs never reintroduce them
# ---------------------------------------------------------------------------


def test_official_closeout_docs_have_no_cognitive_criterion_constraint_drift():
    workflow = WORKFLOW_KG.read_text(encoding="utf-8")
    _assert_only_deterministic(_section(workflow, "When and How to Consolidate"), "kg.md triggers")
    _assert_only_deterministic(_section(workflow, "Cognitive KG Closeout"), "kg.md closeout")
    tooldocs = TOOLDOCS_KG.read_text(encoding="utf-8")
    _assert_only_deterministic(
        _section(tooldocs, "okto_pulse_kg_add_node_candidate"), "tool-docs add_node_candidate"
    )
    # the allowed cognitive node types are named; the deterministic-only ones are not
    # presented as cognitive candidates.
    closeout = _section(workflow, "Cognitive KG Closeout")
    assert re.search(r"\bAlternative\b", closeout) and re.search(r"\bAssumption\b", closeout)
