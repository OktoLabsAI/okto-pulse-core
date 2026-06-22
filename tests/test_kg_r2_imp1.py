"""R2-IMP1 — stale canonical reconciler (deterministic/SDLC) tests.

Spec 9aedfe78 / card affd0444 (FR1/FR2/FR3/FR8, TR1/TR2/TR7/TR8, AC1/AC2/AC8/AC9).

Anti-test-theater: the STARTING canonical state is materialized by the REAL
deterministic worker + commit_consolidation pipeline (for deterministic nodes)
and the consolidation orchestrator (for cognitive nodes) — never by a raw
DecisionDigest/Kuzu seed of the demotion target. The source status regression is
driven through the SQL source-of-truth (the real maturity signal).
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2i1_"))

from okto_pulse.core.kg.canonical_stale_reconciler import (
    ACTION_ROUTED_DEBT,
    reconcile_stale_canonical,
)
from okto_pulse.core.kg.primitives import (
    _apply_kuzu_node_create_with_timestamp,
    add_edge_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from okto_pulse.core.kg.schema import bootstrap_board_graph, open_board_connection
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    ProposeReconciliationRequest,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    MATURITY_CANONICAL_ELIGIBLE,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator
from okto_pulse.core.kg.workers.consolidation import (
    _worker_edge_to_candidate,
    _worker_node_to_candidate,
)
from okto_pulse.core.kg.workers.deterministic_worker import DeterministicWorker
from okto_pulse.core.models.db import Board, Card, Spec

USER_ID = "user-r2-imp1"


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


# ---------------------------------------------------------------------------
# Helpers — real SQL source rows + real worker/orchestrator pipeline
# ---------------------------------------------------------------------------


async def _new_board(db_factory) -> str:
    board_id = f"r2i1-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="r2 imp1", owner_id=USER_ID))
            await db.commit()
    return board_id


def _spec_dict(spec_id, board_id, status):
    return {
        "id": spec_id,
        "title": "Stale demotion spec",
        "description": "spec producing deterministic canonical children",
        "status": status,
        "board_id": board_id,
        "functional_requirements": ["FR alpha requirement", "FR beta requirement"],
        "acceptance_criteria": ["AC alpha criterion"],
        "api_contracts": [{"name": "GET /x", "description": "an api"}],
        "test_scenarios": [
            {"id": "ts_x", "title": "Scenario", "given": "g", "when": "w",
             "then": "t", "linked_criteria": ["AC alpha criterion"]},
        ],
    }


async def _insert_spec(db_factory, board_id, spec_id, *, status):
    async with db_factory() as db:
        db.add(Spec(
            id=spec_id, board_id=board_id, title="Stale demotion spec",
            status=status, created_by=USER_ID,
            functional_requirements=["FR alpha requirement", "FR beta requirement"],
            acceptance_criteria=["AC alpha criterion"],
        ))
        await db.commit()


async def _set_spec_status(db_factory, spec_id, status):
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        spec.status = status
        await db.commit()


async def _insert_bug_card(db_factory, board_id, card_id, *, status):
    async with db_factory() as db:
        db.add(Card(
            id=card_id, board_id=board_id, title="bug card",
            status=status, card_type="bug", created_by=USER_ID,
        ))
        await db.commit()


async def _commit_worker_result(db_factory, board_id, agent_id, result):
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=f"spec-{uuid.uuid4().hex[:8]}",
                raw_content=result.raw_content or "deterministic spec",
                deterministic_candidates=[
                    _worker_node_to_candidate(n) for n in result.nodes
                ],
            ),
            agent_id=agent_id,
            db=db,
        )
    session_id = begin.session_id
    for edge in result.edges:
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=session_id,
                candidate=_worker_edge_to_candidate(edge),
            ),
            agent_id=agent_id,
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=session_id),
        agent_id=agent_id,
        db=None,
        force_reprocess=True,
    )
    async with db_factory() as db:
        return await commit_consolidation(
            CommitConsolidationRequest(session_id=session_id),
            agent_id=agent_id,
            db=db,
        )


def _count_canonical(board_id, node_type) -> int:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            f"MATCH (n:{node_type}) WHERE n.graph_layer = $c RETURN count(n)",
            {"c": GRAPH_LAYER_CANONICAL},
        )
        return int(res.get_next()[0]) if res.has_next() else 0


def _node_layer(board_id, node_type, node_id) -> str | None:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            f"MATCH (n:{node_type} {{id: $id}}) RETURN n.graph_layer",
            {"id": node_id},
        )
        return str(res.get_next()[0]) if res.has_next() else None


def _seed_canonical_cognitive(board_id, node_type, *, source_ref, title="cognitive node"):
    """Materialize a canonical cognitive node via the consolidation orchestrator
    (the R7-validated write primitive, not a raw digest seed)."""
    node_id = f"r2i1c_{uuid.uuid4().hex[:12]}"
    attrs = {
        "title": title, "content": "", "context": "", "justification": "",
        "source_artifact_ref": source_ref, "created_at": "2026-06-08T00:00:00+00:00",
        "created_by_agent": "agent:cognitive", "source_confidence": 1.0,
        "relevance_score": 0.5, "query_hits": 0, "last_queried_at": None,
        "priority_boost": 0.0, "human_curated": False, "embedding": [0.0] * 384,
        "graph_layer": GRAPH_LAYER_CANONICAL, "maturity_status": MATURITY_CANONICAL_ELIGIBLE,
    }
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn, sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _apply_kuzu_node_create_with_timestamp(orch, node_type, node_id, attrs)
    return node_id


async def _seed_done_spec_canonical(db_factory, board_id):
    """Real pipeline: SQL Spec(done) + DeterministicWorker + commit -> canonical
    deterministic nodes. Returns (spec_id, spec_ref)."""
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await _insert_spec(db_factory, board_id, spec_id, status="done")
    result = DeterministicWorker().process_spec(_spec_dict(spec_id, board_id, "done"))
    await _commit_worker_result(db_factory, board_id, "system:layer1_worker", result)
    return spec_id, f"spec:{spec_id}"


# ===========================================================================
# AC1 / AC8 — deterministic demotion on source regression + idempotency
# ===========================================================================


@pytest.mark.asyncio
async def test_reconcile_demotes_stale_deterministic_nodes_and_is_idempotent(db_factory):
    board_id = await _new_board(db_factory)
    spec_id, _ref = await _seed_done_spec_canonical(db_factory, board_id)
    assert _count_canonical(board_id, "Requirement") >= 1, "pipeline must produce canonical"

    # Source regresses done -> draft (the real maturity signal).
    await _set_spec_status(db_factory, spec_id, "draft")

    async with db_factory() as db:
        result = await reconcile_stale_canonical(db, board_id=board_id)
    assert result.demoted, result.to_dict()
    # Every demoted record preserves identity + audit fields.
    rec = result.demoted[0]
    assert rec["prev_layer"] == "canonical" and rec["expected_layer"] == "working"
    assert rec["source_artifact_ref"].startswith("spec:")
    assert rec["correlation_id"]
    # The deterministic canonical children are no longer canonical.
    assert _count_canonical(board_id, "Requirement") == 0

    # Idempotent: a second sweep finds nothing to demote (already working).
    async with db_factory() as db:
        result2 = await reconcile_stale_canonical(db, board_id=board_id)
    assert result2.demoted == []


@pytest.mark.asyncio
async def test_fast_path_and_sweep_demote_the_same_state(db_factory):
    board_id = await _new_board(db_factory)
    spec_id, spec_ref = await _seed_done_spec_canonical(db_factory, board_id)
    await _set_spec_status(db_factory, spec_id, "draft")

    # Fast-path scoped to the regressed spec demotes exactly its nodes.
    async with db_factory() as db:
        fast = await reconcile_stale_canonical(
            db, board_id=board_id, source_refs=[spec_ref],
        )
    assert fast.demoted, fast.to_dict()
    assert _count_canonical(board_id, "Requirement") == 0
    # A following full sweep is a NOOP (fast-path already converged) — AC8.
    async with db_factory() as db:
        sweep = await reconcile_stale_canonical(db, board_id=board_id)
    assert sweep.demoted == []


# ===========================================================================
# AC9 / TR8 — cognitive canonical never demoted (carve-out)
# ===========================================================================


@pytest.mark.asyncio
async def test_cognitive_nodes_are_never_demoted(db_factory):
    board_id = await _new_board(db_factory)
    # Stale deterministic spec (will be demoted) ...
    spec_id, _ref = await _seed_done_spec_canonical(db_factory, board_id)
    await _set_spec_status(db_factory, spec_id, "draft")
    # ... alongside a canonical cognitive Alternative whose (absent) source could
    # look stale. It must be PRESERVED, never demoted, and create NO debt.
    alt_id = _seed_canonical_cognitive(
        board_id, "Alternative", source_ref=f"spec:{spec_id}:alternative:1",
    )

    async with db_factory() as db:
        result = await reconcile_stale_canonical(db, board_id=board_id)

    assert _node_layer(board_id, "Alternative", alt_id) == GRAPH_LAYER_CANONICAL
    assert any(s["node_id"] == alt_id for s in result.skipped_cognitive)
    assert all(d["node_id"] != alt_id for d in result.demoted)
    assert result.routed_to_debt == []  # non-bug-derived -> no auto-debt (ressalva 2)


@pytest.mark.asyncio
async def test_bug_derived_learning_preserved_and_routed_to_debt(db_factory):
    board_id = await _new_board(db_factory)
    bug_id = uuid.uuid4().hex
    # Bug source is regressed (draft -> classifies working, not canonical).
    await _insert_bug_card(db_factory, board_id, bug_id, status="draft")
    learning_ref = f"card:bug:{bug_id}:learning:{uuid.uuid4().hex}"
    learning_id = _seed_canonical_cognitive(
        board_id, "Learning", source_ref=learning_ref,
    )

    async with db_factory() as db:
        result = await reconcile_stale_canonical(db, board_id=board_id)

    # The bug-derived canonical Learning is PRESERVED (never demoted) ...
    assert _node_layer(board_id, "Learning", learning_id) == GRAPH_LAYER_CANONICAL
    assert all(d["node_id"] != learning_id for d in result.demoted)
    # ... and the material irregularity is routed to R7 CanonicalDebt.
    routed = [r for r in result.routed_to_debt if r["node_id"] == learning_id]
    assert routed and routed[0]["action"] == ACTION_ROUTED_DEBT

    # Idempotent: second run still never demotes the Learning (debt upsert is
    # key-stable, so no churn).
    async with db_factory() as db:
        result2 = await reconcile_stale_canonical(db, board_id=board_id)
    assert all(d["node_id"] != learning_id for d in result2.demoted)
    assert _node_layer(board_id, "Learning", learning_id) == GRAPH_LAYER_CANONICAL
