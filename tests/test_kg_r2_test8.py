"""R2-TEST8 (card 29254584) — demotion synchronizes Global Discovery via R1.

Scenario ts_fb7f8da6 (AC7): a stale demotion in the board graph converges the
Global Discovery DecisionDigest THROUGH the R1 parity contract (R2-IMP5 enqueues;
the GD outbox worker runs the R1-IMP1 reconciler). After the cycle,
``query_global(graph_layer=canonical)`` no longer returns the obsolete digest while
``graph_layer=all`` still surfaces it (working / diagnostic).

Anti-test-theater: the canonical Requirement is materialized by the REAL worker
pipeline and the DecisionDigest is created by the REAL Global Discovery outbox
worker — never a raw digest seed of the proof target. The convergence is the
EXISTING R1 reconciler running for real.

Teeth: the test fails if ONLY the board graph is updated — without the R1 sync the
digest stays canonical and the canonical-query assertion fails.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest
from sqlalchemy import delete

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2t8_"))

from r2_scenario_helpers import (
    first_canonical_node,
    new_board,
    seed_done_spec_canonical,
    set_spec_status,
)

from okto_pulse.core.kg.canonical_stale_reconciler import reconcile_stale_canonical
from okto_pulse.core.kg.cypher_templates import layer_filter_clause
from okto_pulse.core.application.processors.global_outbox import GlobalOutboxProcessor
from global_graph_testing import (
    bootstrap_global_discovery,
    execute_global_read,
    reset_global_discovery_runtime_for_tests,
)
from sqlalchemy_test_models import GlobalUpdateOutbox, KuzuNodeRef
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    RealBoardGraphTransactionForTests,
    configure_test_kg_registry,
)


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


async def _ensure_outbox_audit_parent(db, board_id: str, session_id: str) -> None:
    """Persist the relational parents required by KuzuNodeRef fixtures."""
    from datetime import datetime, timezone

    from sqlalchemy_test_models import Board, ConsolidationAudit

    if await db.get(Board, board_id) is None:
        db.add(Board(id=board_id, name=f"R2 TEST8 {board_id}", owner_id="r2-test8"))
        await db.flush()
    if await db.get(ConsolidationAudit, session_id) is None:
        now = datetime.now(timezone.utc)
        db.add(ConsolidationAudit(
            session_id=session_id,
            board_id=board_id,
            artifact_id=f"outbox-{session_id[-16:]}",
            artifact_type="test_fixture",
            agent_id="r2-test8",
            started_at=now,
            committed_at=now,
        ))
        await db.flush()


async def _digest_node_via_gd_worker(db_factory, board_id, node_id, node_type) -> int:
    """Create the DecisionDigest through the REAL GD outbox pipeline."""
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
    """Deterministic canonical/all selection via the SAME fail-closed
    layer_filter_clause query_global applies (no embedding-similarity flakiness)."""
    cypher = (
        "MATCH (b:Board)-[:CONTAINS_DECISION]->(d:DecisionDigest) "
        "WHERE b.board_id = $bid AND " + layer_filter_clause("d") + " "
        "RETURN d.original_node_id"
    )
    res = execute_global_read(cypher, {"bid": board_id, "graph_layer": layer})
    return {str(row[0]) for row in res.rows}


# ===========================================================================
# ts_fb7f8da6 — AC7: demotion converges Global Discovery via R1
# ===========================================================================


@pytest.mark.asyncio
async def test_demotion_syncs_global_discovery_via_r1(db_factory):
    board_id = await new_board(db_factory, "r2t8")
    spec_id, _ref = await seed_done_spec_canonical(db_factory, board_id)
    req = await first_canonical_node(board_id, "Requirement")
    assert req is not None, "real pipeline must produce a canonical Requirement"
    req_id, _src = req

    # The canonical DecisionDigest is created by the REAL GD pipeline.
    assert await _digest_node_via_gd_worker(db_factory, board_id, req_id, "Requirement") == 1
    assert _digest_layer(board_id, req_id) == "canonical"
    assert req_id in _query_ids(board_id, "canonical"), "sanity: canonical query returns it"

    # Regress the source + run the R2 stale reconciler -> demote board node +
    # enqueue the R1 Global Discovery sync (R2-IMP5).
    await set_spec_status(db_factory, spec_id, "draft")
    async with db_factory() as db:
        rec = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert rec.demoted, rec.to_dict()
    # Coupling: the demotion enqueued the GD sync (not just a board-graph update).
    assert rec.global_sync_enqueued is True

    # The GD worker runs the EXISTING R1 parity reconciler -> the digest converges.
    assert await _drain_gd_worker(db_factory) == 1
    assert _digest_layer(board_id, req_id) == "working"

    # AC7 / TEETH: obsolete canonical no longer in the canonical query (would still
    # be there if only the board graph were updated), and `all` keeps the
    # diagnostic representation.
    assert req_id not in _query_ids(board_id, "canonical")
    assert req_id in _query_ids(board_id, "all")


@pytest.mark.asyncio
async def test_demotion_sync_is_idempotent(db_factory):
    """A second reconcile finds nothing to demote, does not re-enqueue, and the
    digest stays converged (no churn)."""
    board_id = await new_board(db_factory, "r2t8")
    spec_id, _ref = await seed_done_spec_canonical(db_factory, board_id)
    req = await first_canonical_node(board_id, "Requirement")
    assert req is not None
    req_id, _src = req
    assert await _digest_node_via_gd_worker(db_factory, board_id, req_id, "Requirement") == 1

    await set_spec_status(db_factory, spec_id, "draft")
    async with db_factory() as db:
        first = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert first.demoted and first.global_sync_enqueued is True
    await _drain_gd_worker(db_factory)
    assert _digest_layer(board_id, req_id) == "working"

    async with db_factory() as db:
        second = await reconcile_stale_canonical(db, board_id=board_id)
        await db.commit()
    assert second.demoted == []
    assert second.global_sync_enqueued is False
    await _drain_gd_worker(db_factory)
    assert _digest_layer(board_id, req_id) == "working"
    assert req_id not in _query_ids(board_id, "canonical")
