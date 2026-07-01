"""Behavioral tests for Global Discovery layer-aware vector types (spec 849d6292,
Batch 1 — impl card f09aa6f8).

One test per Batch-1 test card / acceptance criterion, all exercising the REAL
board graph + REAL outbox worker + REAL global discovery graph (no source
inspection — TR6/validator bar):

* ts_* (card e3bb09a1) AC1 — outbox digests the new vector types
  (Requirement/APIContract/TestScenario/Bug) preserving node_type /
  original_node_id / graph_layer.
* ts_* (card 53060c8a) AC2 — reprocessing the same original_node_id updates the
  existing digest (no duplicate); digest_id == dd_{board[:8]}_{original_node_id}
  is stable under rebuild.
* ts_* (card 5c061e8c) AC3 — a legacy canonical node missing its embedding is
  SKIPPED with the missing_embedding_skipped diagnostic/counter and the other
  nodes keep processing.
* ts_* (card d7a69104) AC4 — a Requirement/APIContract/TestScenario/Bug freshly
  produced by the deterministic worker MUST carry an embedding; the test fails
  if any vector-index-type node lands in the board graph without one.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest
from sqlalchemy import delete

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_gdvt_"))

from okto_pulse.core.kg.embedding import get_embedding_provider
from okto_pulse.core.kg.global_discovery import metrics as gdm
from okto_pulse.core.kg.global_discovery.outbox_worker import OutboxWorker
from okto_pulse.core.kg.global_discovery.schema import (
    bootstrap_global_discovery,
    open_global_connection,
    reset_global_discovery_runtime_for_tests,
)
from okto_pulse.core.kg.primitives import (
    add_edge_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from okto_pulse.core.kg.schema import (
    VECTOR_INDEX_TYPES,
    bootstrap_board_graph,
    open_board_connection,
)
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    ProposeReconciliationRequest,
)
from okto_pulse.core.kg.workers.deterministic_worker import DeterministicWorker
from okto_pulse.core.kg.workers.consolidation import (
    _worker_edge_to_candidate,
    _worker_node_to_candidate,
)
from okto_pulse.core.models.db import GlobalUpdateOutbox, KuzuNodeRef
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    RealBoardGraphPathResolverForTests,
    RealBoardGraphTransactionForTests,
    configure_test_kg_registry,
)


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_test_kg_registry(
        cypher_executor=RealBoardCypherExecutorForTests(),
        graph_transaction=RealBoardGraphTransactionForTests(),
        graph_path_resolver=RealBoardGraphPathResolverForTests(),
    )


@pytest.fixture(scope="module", autouse=True)
def _bootstrap_global():
    reset_global_discovery_runtime_for_tests()
    bootstrap_global_discovery()
    yield
    reset_global_discovery_runtime_for_tests()


@pytest.fixture(autouse=True)
def _reset_gd_metrics():
    gdm.reset_global_discovery_metrics()
    yield
    gdm.reset_global_discovery_metrics()


# ---------------------------------------------------------------------------
# Helpers — real board graph + real outbox flow
# ---------------------------------------------------------------------------


def _new_board() -> str:
    board_id = f"gdvt-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    return board_id


def _seed_node(board_id, node_type, node_id, *, with_embedding, layer="canonical"):
    """CREATE one board-graph node, optionally with an embedding."""
    with open_board_connection(board_id) as (_db, conn):
        if with_embedding:
            emb = get_embedding_provider().encode(f"{node_type} {node_id}")
            conn.execute(
                f"CREATE (n:{node_type} {{id: $id, title: $t, "
                f"embedding: $e, graph_layer: $l}})",
                {"id": node_id, "t": f"{node_type} {node_id}", "e": emb, "l": layer},
            )
        else:
            conn.execute(
                f"CREATE (n:{node_type} {{id: $id, title: $t, graph_layer: $l}})",
                {"id": node_id, "t": f"{node_type} {node_id}", "l": layer},
            )


def _delete_node(board_id, node_type, node_id):
    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            f"MATCH (n:{node_type} {{id: $id}}) DETACH DELETE n",
            {"id": node_id},
        )


async def _run_outbox(db_factory, board_id, refs):
    """Insert KuzuNodeRef add-rows + one outbox event, then process it.

    ``refs`` is a list of (node_type, node_id). Returns processed count."""
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        # Isolation: clear any outbox events left pending by other test modules
        # so process_once() handles exactly this test's event (the global
        # outbox/DB is a process singleton shared across modules).
        await db.execute(delete(GlobalUpdateOutbox))
        for node_type, node_id in refs:
            db.add(KuzuNodeRef(
                session_id=session_id,
                board_id=board_id,
                kuzu_node_id=node_id,
                kuzu_node_type=node_type,
                operation="add",
            ))
        db.add(GlobalUpdateOutbox(
            event_id=str(uuid.uuid4()),
            board_id=board_id,
            session_id=session_id,
            event_type="consolidation_committed",
            payload={"session_id": session_id, "nodes_added": len(refs)},
        ))
        await db.commit()
    worker = OutboxWorker(db_factory, interval_seconds=5)
    return await worker.process_once()


def _read_digests(board_id) -> list[dict]:
    _gdb, gconn = open_global_connection()
    try:
        res = gconn.execute(
            "MATCH (d:DecisionDigest) WHERE d.board_id = $b "
            "RETURN d.id, d.node_type, d.original_node_id, d.graph_layer "
            "ORDER BY d.node_type, d.original_node_id",
            {"b": board_id},
        )
        out = []
        while res.has_next():
            row = res.get_next()
            out.append({
                "id": row[0],
                "node_type": row[1],
                "original_node_id": row[2],
                "graph_layer": row[3],
            })
        return out
    finally:
        del gconn, _gdb


# ---------------------------------------------------------------------------
# AC1 (card e3bb09a1) — new vector types are digested
# ---------------------------------------------------------------------------


NEW_TYPES = ("Requirement", "APIContract", "TestScenario", "Bug")


@pytest.mark.asyncio
async def test_outbox_digests_new_vector_types(db_factory):
    board_id = _new_board()
    refs = [(nt, f"{nt.lower()}_1") for nt in NEW_TYPES]
    for nt, nid in refs:
        _seed_node(board_id, nt, nid, with_embedding=True)

    processed = await _run_outbox(db_factory, board_id, refs)
    assert processed == 1

    digests = _read_digests(board_id)
    by_type = {d["node_type"]: d for d in digests}
    # One digest per new vector type, with identity + layer preserved.
    for nt, nid in refs:
        assert nt in by_type, f"no global digest created for {nt}"
        d = by_type[nt]
        assert d["original_node_id"] == nid
        assert d["graph_layer"] == "canonical"
        assert d["id"] == f"dd_{board_id[:8]}_{nid}"
        # or_38b60fe1: a created upsert was counted for this type.
        assert gdm.get_digest_upsert_count(
            board_id=board_id, node_type=nt, outcome="created"
        ) == 1

    # New-fixture invariant: no missing-embedding skips at all.
    assert gdm.get_missing_embedding_skipped_count(board_id=board_id) == 0


# ---------------------------------------------------------------------------
# AC2 (card 53060c8a) — reprocess updates, no duplicate; id stable under rebuild
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reprocess_updates_digest_without_duplicate(db_factory):
    board_id = _new_board()
    node_id = "requirement_stable"
    _seed_node(board_id, "Requirement", node_id, with_embedding=True)

    # First pass creates the digest.
    assert await _run_outbox(db_factory, board_id, [("Requirement", node_id)]) == 1
    first = _read_digests(board_id)
    assert len(first) == 1
    digest_id = first[0]["id"]
    assert digest_id == f"dd_{board_id[:8]}_{node_id}"
    assert gdm.get_digest_upsert_count(
        board_id=board_id, outcome="created"
    ) == 1

    # Second pass (reprocess / rebuild keeps original_node_id) updates in place.
    assert await _run_outbox(db_factory, board_id, [("Requirement", node_id)]) == 1
    second = _read_digests(board_id)
    assert len(second) == 1  # NO duplicate
    assert second[0]["id"] == digest_id  # identity stable under rebuild
    assert second[0]["original_node_id"] == node_id
    assert gdm.get_digest_upsert_count(
        board_id=board_id, outcome="updated"
    ) == 1


@pytest.mark.asyncio
async def test_outbox_prunes_stale_digest_after_board_node_disappears(db_factory):
    board_id = _new_board()
    old_id = "decision_before_rebuild"
    new_id = "decision_after_rebuild"

    _seed_node(board_id, "Decision", old_id, with_embedding=True)
    assert await _run_outbox(db_factory, board_id, [("Decision", old_id)]) == 1
    assert {d["original_node_id"] for d in _read_digests(board_id)} == {old_id}

    _delete_node(board_id, "Decision", old_id)
    _seed_node(board_id, "Decision", new_id, with_embedding=True)
    assert await _run_outbox(db_factory, board_id, [("Decision", new_id)]) == 1

    digests = _read_digests(board_id)
    assert {d["original_node_id"] for d in digests} == {new_id}
    assert all(d["id"] != f"dd_{board_id[:8]}_{old_id}" for d in digests)


# ---------------------------------------------------------------------------
# AC3 (card 5c061e8c) — legacy node missing embedding is skipped + diagnosed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_legacy_node_missing_embedding_is_skipped_with_diagnostic(db_factory):
    board_id = _new_board()
    # A legacy canonical Bug WITHOUT embedding + a healthy Requirement WITH one.
    _seed_node(board_id, "Bug", "bug_legacy", with_embedding=False)
    _seed_node(board_id, "Requirement", "req_ok", with_embedding=True)

    processed = await _run_outbox(
        db_factory, board_id,
        [("Bug", "bug_legacy"), ("Requirement", "req_ok")],
    )
    # The event is NOT blocked — it processes, skipping only the bad node.
    assert processed == 1

    digests = _read_digests(board_id)
    types = {d["node_type"] for d in digests}
    # The healthy node was digested; the embedding-less one was NOT.
    assert "Requirement" in types
    assert "Bug" not in types

    # or_a921cc64: exactly one missing-embedding skip, attributed to Bug.
    assert gdm.get_missing_embedding_skipped_count(
        board_id=board_id, node_type="Bug"
    ) == 1
    assert gdm.get_missing_embedding_skipped_count(
        board_id=board_id, node_type="Requirement"
    ) == 0
    # No null-embedding digest was created (HNSW stays consistent).
    assert all(d["node_type"] != "Bug" for d in digests)


# ---------------------------------------------------------------------------
# AC4 (card d7a69104) — new deterministic vector-type nodes carry embeddings
# ---------------------------------------------------------------------------


def _full_done_spec() -> dict:
    return {
        "id": f"spec-{uuid.uuid4().hex[:8]}",
        "title": "Vector spec",
        "description": "spec producing vector-type children",
        "status": "done",
        "board_id": "ignored",
        "functional_requirements": ["FR alpha", "FR beta"],
        "acceptance_criteria": ["AC alpha"],
        "api_contracts": [{"name": "GET /x", "description": "an api"}],
        "test_scenarios": [
            {"id": "ts_x", "title": "Scenario", "given": "g", "when": "w",
             "then": "t", "linked_criteria": ["AC alpha"]},
        ],
    }


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


@pytest.mark.asyncio
async def test_deterministic_vector_nodes_have_embeddings(
    db_factory, board_handle, board_id,
):
    result = DeterministicWorker().process_spec(_full_done_spec())
    produced_types = {
        (n.node_type.value if hasattr(n.node_type, "value") else n.node_type)
        for n in result.nodes
    }
    # The worker produced at least the spec vector-index children.
    assert {"Requirement", "APIContract", "TestScenario"} <= produced_types

    # Commit as the real Layer 1 deterministic worker (deterministic edge types
    # like belongs_to are reserved for the system worker).
    await _commit_worker_result(
        db_factory, board_id, "system:layer1_worker", result
    )

    # Regression guard: EVERY vector-index-type node now in the board graph
    # must carry a non-null embedding (else it can never be globally digested).
    with open_board_connection(board_id) as (_db, conn):
        for node_type in VECTOR_INDEX_TYPES:
            total = conn.execute(
                f"MATCH (n:{node_type}) RETURN count(n)"
            )
            total_n = int(total.get_next()[0]) if total.has_next() else 0
            if total_n == 0:
                continue
            missing = conn.execute(
                f"MATCH (n:{node_type}) WHERE n.embedding IS NULL "
                f"RETURN count(n)"
            )
            missing_n = int(missing.get_next()[0]) if missing.has_next() else 0
            assert missing_n == 0, (
                f"REGRESSION: {missing_n}/{total_n} {node_type} nodes produced "
                f"by the deterministic worker have NULL embedding — they would "
                f"be silently undiscoverable in Global Discovery (FR4/AC4)."
            )

    # And the three spec vector children specifically materialized with embeddings.
    with open_board_connection(board_id) as (_db, conn):
        for node_type in ("Requirement", "APIContract", "TestScenario"):
            res = conn.execute(
                f"MATCH (n:{node_type}) WHERE n.embedding IS NOT NULL "
                f"RETURN count(n)"
            )
            count = int(res.get_next()[0]) if res.has_next() else 0
            assert count >= 1, f"no embedded {node_type} node materialized"
