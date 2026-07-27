"""R2-IMP5 — exercise digest parity after a deterministic stale demotion.

Spec 9aedfe78 / card fb2d683f (TR3/TR10/AC7).

Anti-test-theater: the canonical starting point is a REAL Spec + DeterministicWorker
+ commit, the DecisionDigest is created by the REAL Global Discovery outbox worker,
and the convergence is the EXISTING R1-IMP1 parity reconciler (the GD worker's
_apply_event). The reconciler is graph-only; Card 6 worker tests own the durable
ledger/outbox/queue transfer, while this module consumes R1 unchanged.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest
from sqlalchemy import delete

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2i5_"))

from okto_pulse.core.kg.canonical_stale_reconciler import reconcile_stale_canonical
from okto_pulse.core.kg.canonical_demotion_global_sync import (
    enqueue_digest_layer_reconciliation,
)
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.application.processors.global_outbox import GlobalOutboxProcessor
from global_graph_testing import (
    bootstrap_global_discovery,
    execute_global_read,
    reset_global_discovery_runtime_for_tests,
)
from okto_pulse.core.kg.primitives import (
    add_edge_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from kg_schema_testing import bootstrap_board_graph, open_board_connection
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    ProposeReconciliationRequest,
)
from okto_pulse.core.kg.source_maturity import GRAPH_LAYER_CANONICAL
from okto_pulse.core.application.processors.consolidation import (
    _worker_edge_to_candidate,
    _worker_node_to_candidate,
)
from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
from sqlalchemy_test_models import (
    Board,
    GlobalUpdateOutbox,
    KuzuNodeRef,
    Spec,
)
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    RealBoardGraphTransactionForTests,
    configure_test_kg_registry,
)

USER_ID = "user-r2-imp5"
QUERY_TEXT = "FR alpha parity sync requirement"


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_test_kg_registry(
        cypher_executor=RealBoardCypherExecutorForTests(),
        graph_transaction=RealBoardGraphTransactionForTests(),
    )


@pytest.fixture(scope="module", autouse=True)
def _bootstrap_global():
    reset_global_discovery_runtime_for_tests()
    bootstrap_global_discovery()
    yield
    reset_global_discovery_runtime_for_tests()


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


async def _new_board(db_factory) -> str:
    board_id = f"r2i5-{uuid.uuid4().hex[:10]}"
    await run_blocking_graph_io(
        lambda: bootstrap_board_graph(board_id),
        task_name="tests.r2_imp5.bootstrap_board_graph",
    )
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="r2 imp5", owner_id=USER_ID))
            await db.commit()
    return board_id


def _spec_dict(spec_id, board_id, status):
    return {
        "id": spec_id, "title": "sync spec",
        "description": "spec producing canonical children for GD sync",
        "status": status, "board_id": board_id,
        "functional_requirements": [QUERY_TEXT, "FR beta parity sync"],
        "acceptance_criteria": ["AC alpha parity sync"],
    }


async def _insert_spec(db_factory, board_id, spec_id, *, status):
    async with db_factory() as db:
        db.add(Spec(
            id=spec_id, board_id=board_id, title="sync spec", status=status,
            created_by=USER_ID,
            functional_requirements=[QUERY_TEXT, "FR beta parity sync"],
            acceptance_criteria=["AC alpha parity sync"],
        ))
        await db.commit()


async def _set_spec_status(db_factory, spec_id, status):
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        spec.status = status
        await db.commit()


async def _commit_worker_result(db_factory, board_id, result):
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id, artifact_type="spec",
                artifact_id=f"spec-{uuid.uuid4().hex[:8]}",
                raw_content=result.raw_content or "deterministic spec",
                deterministic_candidates=[
                    _worker_node_to_candidate(n) for n in result.nodes
                ],
            ),
            agent_id="system:layer1_worker", db=db,
        )
    session_id = begin.session_id
    for edge in result.edges:
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=session_id, candidate=_worker_edge_to_candidate(edge),
            ),
            agent_id="system:layer1_worker",
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=session_id),
        agent_id="system:layer1_worker", db=None, force_reprocess=True,
    )
    async with db_factory() as db:
        await commit_consolidation(
            CommitConsolidationRequest(session_id=session_id),
            agent_id="system:layer1_worker", db=db,
        )


def _first_canonical_requirement(board_id) -> tuple[str, str] | None:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            "MATCH (n:Requirement) WHERE n.graph_layer = $c "
            "RETURN n.id, n.title LIMIT 1",
            {"c": GRAPH_LAYER_CANONICAL},
        )
        if res.has_next():
            row = res.get_next()
            return str(row[0]), str(row[1] or "")
    return None


async def _first_canonical_requirement_async(
    board_id: str,
) -> tuple[str, str] | None:
    return await run_blocking_graph_io(
        lambda: _first_canonical_requirement(board_id),
        task_name="tests.r2_imp5.first_canonical_requirement",
    )


async def _ensure_outbox_audit_parent(db, board_id: str, session_id: str) -> None:
    """Persist the relational parents required by KuzuNodeRef fixtures."""
    from datetime import datetime, timezone

    from sqlalchemy_test_models import ConsolidationAudit

    if await db.get(Board, board_id) is None:
        db.add(Board(id=board_id, name=f"R2 IMP5 {board_id}", owner_id=USER_ID))
        await db.flush()
    if await db.get(ConsolidationAudit, session_id) is None:
        now = datetime.now(timezone.utc)
        db.add(ConsolidationAudit(
            session_id=session_id,
            board_id=board_id,
            artifact_id=f"outbox-{session_id[-16:]}",
            artifact_type="test_fixture",
            agent_id=USER_ID,
            started_at=now,
            committed_at=now,
        ))
        await db.flush()


async def _digest_node_via_gd_worker(db_factory, board_id, node_id, node_type):
    """Create the DecisionDigest by the REAL GD outbox pipeline (add-ref + event)."""
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        await _ensure_outbox_audit_parent(db, board_id, session_id)
        db.add(KuzuNodeRef(
            session_id=session_id, board_id=board_id,
            kuzu_node_id=node_id, kuzu_node_type=node_type, operation="add",
        ))
        db.add(GlobalUpdateOutbox(
            event_id=str(uuid.uuid4()), board_id=board_id, session_id=session_id,
            event_type="consolidation_committed",
            payload={"session_id": session_id, "nodes_added": 1},
        ))
        await db.commit()
    return await GlobalOutboxProcessor(db_factory, interval_seconds=5).process_once()


async def _drain_gd_worker(db_factory) -> int:
    return await GlobalOutboxProcessor(db_factory, interval_seconds=5).process_once()


def _digest_layer(board_id, node_id) -> str | None:
    res = execute_global_read(
        "MATCH (d:DecisionDigest) WHERE d.board_id = $b AND d.original_node_id = $n "
        "RETURN coalesce(d.graph_layer, 'legacy_unknown')",
        {"b": board_id, "n": node_id},
    )
    return str(res.rows[0][0]) if res.rows else None


def _query_ids(board_id, layer):
    """Deterministic canonical/all selection using the SAME fail-closed
    layer_filter_clause that query_global applies (avoids embedding-similarity
    flakiness; this is the exact filter that decides what query_global can return)."""
    from okto_pulse.core.kg.cypher_templates import layer_filter_clause

    cypher = (
        "MATCH (b:Board)-[:CONTAINS_DECISION]->(d:DecisionDigest) "
        "WHERE b.board_id = $bid AND " + layer_filter_clause("d") + " "
        "RETURN d.original_node_id"
    )
    res = execute_global_read(cypher, {"bid": board_id, "graph_layer": layer})
    return {str(row[0]) for row in res.rows}


# ===========================================================================
# AC7 — the full real cycle: demotion converges the GD digest via R1
# ===========================================================================


@pytest.mark.asyncio
async def test_demotion_syncs_global_discovery_canonical_disappears(db_factory):
    board_id = await _new_board(db_factory)
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await _insert_spec(db_factory, board_id, spec_id, status="done")
    result = DeterministicWorker().process_spec(_spec_dict(spec_id, board_id, "done"))
    await _commit_worker_result(db_factory, board_id, result)

    req = await _first_canonical_requirement_async(board_id)
    assert req is not None, "pipeline must produce a canonical Requirement"
    req_id, _title = req

    # Create the canonical DecisionDigest via the REAL GD pipeline.
    assert await _digest_node_via_gd_worker(db_factory, board_id, req_id, "Requirement") == 1
    assert _digest_layer(board_id, req_id) == "canonical"
    assert req_id in _query_ids(board_id, "canonical"), "sanity: canonical query returns it"

    # Regress the source + run the graph-only stale reconciler. Delivery is a
    # separate durable ownership boundary now; this lower-level R1 proof
    # explicitly supplies its parity trigger.
    await _set_spec_status(db_factory, spec_id, "draft")
    async with db_factory() as db:
        rec = await reconcile_stale_canonical(db, board_id=board_id)
        await enqueue_digest_layer_reconciliation(
            db,
            board_id=board_id,
            reason="stale_demotion_parity",
            idempotency_key=f"r2-imp5:{board_id}:{spec_id}",
        )
        await db.commit()
    assert rec.demoted, rec.to_dict()
    assert rec.global_sync_enqueued is False

    # The GD worker runs the EXISTING R1 parity reconciler -> digest converges.
    assert await _drain_gd_worker(db_factory) == 1
    assert _digest_layer(board_id, req_id) == "working"

    # AC7: obsolete canonical no longer in canonical query; all keeps it (diagnostic).
    assert req_id not in _query_ids(board_id, "canonical")
    assert req_id in _query_ids(board_id, "all")

    # Idempotent: a second graph sweep finds nothing to demote and never emits
    # delivery by itself.
    async with db_factory() as db:
        rec2 = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert rec2.demoted == []
    assert rec2.global_sync_enqueued is False
    assert await _drain_gd_worker(db_factory) == 0
    assert _digest_layer(board_id, req_id) == "working"


# ===========================================================================
# Ownership boundary: the bare graph reconciler never enqueues GD delivery
# ===========================================================================


@pytest.mark.asyncio
async def test_bare_reconciler_never_enqueues_global_sync_event(db_factory):
    board_id = await _new_board(db_factory)
    spec_id = f"spec-{uuid.uuid4().hex[:10]}"
    await _insert_spec(db_factory, board_id, spec_id, status="done")
    result = DeterministicWorker().process_spec(_spec_dict(spec_id, board_id, "done"))
    await _commit_worker_result(db_factory, board_id, result)
    await _set_spec_status(db_factory, spec_id, "draft")

    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        await db.commit()
        rec = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert rec.demoted and rec.global_sync_enqueued is False

    # A Global Discovery outbox event was enqueued for the board (the R1 trigger).
    async with db_factory() as db:
        from sqlalchemy import select
        rows = (await db.execute(
            select(GlobalUpdateOutbox).where(GlobalUpdateOutbox.board_id == board_id)
        )).scalars().all()
    assert rows == []
