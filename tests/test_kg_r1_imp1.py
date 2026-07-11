"""R1-IMP1 — expected_digest_layer reconciliation in Global Discovery.

Spec 29b35f60 / card 626b805f. The Global Discovery digest publication layer
(`DecisionDigest.graph_layer`) must stay in parity with the board graph node's
EFFECTIVE publication layer, corrected in EVERY outbox event — even one with NO
`KuzuNodeRef operation=add` (FR1/FR2) — preserving `original_node_id` identity.

All tests are pipeline-driven: board graph -> outbox worker -> reconcile ->
digest/query. NO direct `DecisionDigest` seeding is used as proof (AC5).
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest
from sqlalchemy import delete

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r1i1_"))

from okto_pulse.core.kg.embedding import get_embedding_provider
from okto_pulse.core.kg.global_discovery import metrics as gdm
from okto_pulse.core.kg.global_discovery.layer_parity import (
    resolve_expected_digest_layer,
)
from okto_pulse.core.application.processors.global_outbox import GlobalOutboxProcessor
from global_graph_testing import (
    bootstrap_global_discovery,
    open_global_connection,
    reset_global_discovery_runtime_for_tests,
)
from okto_pulse.core.kg.kg_service import get_kg_service
from okto_pulse.core.kg.primitives import _apply_kuzu_node_create_with_timestamp
from kg_schema_testing import bootstrap_board_graph, open_board_connection
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    MATURITY_CANONICAL_ELIGIBLE,
    MATURITY_WORKING_IMMATURE,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator
from sqlalchemy_test_models import Board, GlobalUpdateOutbox, KuzuNodeRef
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    configure_test_kg_registry,
)

USER_ID = "user-r1-imp1"
QUERY_TEXT = "gateway caching parity learning"
LEGACY_UNKNOWN = "legacy_unknown"


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_test_kg_registry(cypher_executor=RealBoardCypherExecutorForTests())


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


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


# ---------------------------------------------------------------------------
# Helpers — real board graph + real outbox worker
# ---------------------------------------------------------------------------


async def _new_board(db_factory) -> str:
    board_id = f"r1i1-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    async with db_factory() as db:
        if await db.get(Board, board_id) is None:
            db.add(Board(id=board_id, name="r1 imp1", owner_id=USER_ID))
            await db.commit()
    return board_id


def _seed_node(board_id, node_type, node_id, *, layer="canonical", title=QUERY_TEXT):
    """CREATE one digestable board node with a real embedding. ``layer=None``
    omits graph_layer entirely (legacy node — never implicit canonical)."""
    emb = get_embedding_provider().encode(title)
    with open_board_connection(board_id) as (_db, conn):
        if layer is None:
            conn.execute(
                f"CREATE (n:{node_type} {{id: $id, title: $t, embedding: $e}})",
                {"id": node_id, "t": title, "e": emb},
            )
        else:
            conn.execute(
                f"CREATE (n:{node_type} {{id: $id, title: $t, embedding: $e, "
                f"graph_layer: $l}})",
                {"id": node_id, "t": title, "e": emb, "l": layer},
            )


def _set_node_layer(board_id, node_type, node_id, layer):
    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            f"MATCH (n:{node_type} {{id: $id}}) SET n.graph_layer = $l",
            {"id": node_id, "l": layer},
        )


def _node_attrs(source_ref, graph_layer, maturity, *, title, embedding):
    return {
        "title": title, "content": "", "context": "", "justification": "",
        "source_artifact_ref": source_ref, "created_at": "2026-06-08T00:00:00+00:00",
        "created_by_agent": "test", "source_confidence": 1.0, "relevance_score": 0.5,
        "query_hits": 0, "last_queried_at": None, "priority_boost": 0.0,
        "human_curated": False, "embedding": embedding,
        "graph_layer": graph_layer, "maturity_status": maturity,
    }


def _seed_learning(board_id, *, source_ref, title=QUERY_TEXT, canonical=0, working=0):
    """Canonical Learning (embedded on ``title``) + validates->Bug evidence."""
    learning_id = f"r1i1l_{uuid.uuid4().hex[:12]}"
    emb = get_embedding_provider().encode(title)
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn, sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _apply_kuzu_node_create_with_timestamp(
            orch, "Learning", learning_id,
            _node_attrs(source_ref, GRAPH_LAYER_CANONICAL, MATURITY_CANONICAL_ELIGIBLE,
                        title=title, embedding=emb),
        )
        for _ in range(canonical):
            _add_bug(orch, board_id, learning_id, GRAPH_LAYER_CANONICAL)
        for _ in range(working):
            _add_bug(orch, board_id, learning_id, GRAPH_LAYER_WORKING)
    return learning_id


def _add_bug(orch, board_id, learning_id, layer):
    bug_id = f"r1i1b_{uuid.uuid4().hex[:10]}"
    _apply_kuzu_node_create_with_timestamp(
        orch, "Bug", bug_id,
        _node_attrs(f"bug:{bug_id}", layer,
                    MATURITY_CANONICAL_ELIGIBLE if layer == GRAPH_LAYER_CANONICAL
                    else MATURITY_WORKING_IMMATURE,
                    title=f"bug {bug_id}", embedding=[0.0] * 384),
    )
    orch.create_edge(edge_type="validates", from_id=learning_id, to_id=bug_id,
                     attrs={"confidence": 1.0}, from_type="Learning", to_type="Bug")
    return bug_id


def _mature_learning_with_canonical_bug(board_id, learning_id):
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn, sqlite_session=None,
            session_id=f"mat_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _add_bug(orch, board_id, learning_id, GRAPH_LAYER_CANONICAL)


async def _run_outbox(db_factory, board_id, refs) -> int:
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        for node_type, node_id in refs:
            db.add(KuzuNodeRef(
                session_id=session_id, board_id=board_id,
                kuzu_node_id=node_id, kuzu_node_type=node_type, operation="add",
            ))
        db.add(GlobalUpdateOutbox(
            event_id=str(uuid.uuid4()), board_id=board_id, session_id=session_id,
            event_type="consolidation_committed",
            payload={"session_id": session_id, "nodes_added": len(refs)},
        ))
        await db.commit()
    return await GlobalOutboxProcessor(db_factory, interval_seconds=5).process_once()


async def _run_outbox_no_refs(db_factory, board_id) -> int:
    """An outbox event with NO KuzuNodeRef add rows — exercises the reconcile
    path that must run before the empty-refs early return (FR1/FR2)."""
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        db.add(GlobalUpdateOutbox(
            event_id=str(uuid.uuid4()), board_id=board_id, session_id=session_id,
            event_type="consolidation_committed",
            payload={"session_id": session_id, "nodes_added": 0},
        ))
        await db.commit()
    return await GlobalOutboxProcessor(db_factory, interval_seconds=5).process_once()


def _digests_for(board_id, node_id) -> list[dict]:
    _gdb, gconn = open_global_connection()
    try:
        res = gconn.execute(
            "MATCH (d:DecisionDigest) WHERE d.board_id = $b AND d.original_node_id = $n "
            "RETURN d.id, coalesce(d.graph_layer, 'legacy_unknown')",
            {"b": board_id, "n": node_id},
        )
        out = []
        while res.has_next():
            row = res.get_next()
            out.append({"id": str(row[0]), "graph_layer": str(row[1])})
        return out
    finally:
        del gconn, _gdb


def _digest_layer(board_id, node_id) -> str | None:
    rows = _digests_for(board_id, node_id)
    return rows[0]["graph_layer"] if rows else None


def _source_layer(board_id, node_type, node_id) -> str | None:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            f"MATCH (n:{node_type} {{id: $id}}) RETURN n.graph_layer", {"id": node_id},
        )
        return str(res.get_next()[0]) if res.has_next() else None


# ===========================================================================
# Pure resolver
# ===========================================================================


def test_resolver_non_learning_mirrors_raw_layer():
    assert resolve_expected_digest_layer(
        node_type="Decision", raw_graph_layer="canonical") == ("canonical", None)
    assert resolve_expected_digest_layer(
        node_type="Decision", raw_graph_layer="working") == ("working", None)
    # Fail-closed: a legacy_unknown stays legacy_unknown, never canonical (FR5).
    assert resolve_expected_digest_layer(
        node_type="Requirement", raw_graph_layer=LEGACY_UNKNOWN) == (LEGACY_UNKNOWN, None)


def test_resolver_canonical_learning_uses_r7_rule():
    # Incomplete (working-only) canonical Learning -> working publication.
    layer, reason = resolve_expected_digest_layer(
        node_type="Learning", raw_graph_layer="canonical",
        source_artifact_ref="card:bug:x:learning:1", canonical_bug_count=0)
    assert layer == GRAPH_LAYER_WORKING and reason is not None
    # Complete (>=1 canonical bug) -> canonical.
    layer, reason = resolve_expected_digest_layer(
        node_type="Learning", raw_graph_layer="canonical",
        source_artifact_ref="card:bug:x:learning:1", canonical_bug_count=1)
    assert layer == GRAPH_LAYER_CANONICAL and reason is None


# ===========================================================================
# Reconciler — runs without a new add ref (FR1/FR2), preserves identity (AC1)
# ===========================================================================


@pytest.mark.asyncio
async def test_reconcile_promotes_working_to_canonical_without_new_add(db_factory):
    board_id = await _new_board(db_factory)
    nid = f"dec_{uuid.uuid4().hex[:10]}"
    _seed_node(board_id, "Decision", nid, layer="working")
    assert await _run_outbox(db_factory, board_id, [("Decision", nid)]) == 1
    before = _digests_for(board_id, nid)
    assert len(before) == 1 and before[0]["graph_layer"] == "working"
    digest_id = before[0]["id"]

    # Promote the source IN PLACE; NO new add ref is emitted.
    _set_node_layer(board_id, "Decision", nid, "canonical")
    assert await _run_outbox_no_refs(db_factory, board_id) == 1

    after = _digests_for(board_id, nid)
    assert len(after) == 1, "reconcile must not duplicate the digest"
    assert after[0]["id"] == digest_id, "original identity (digest id) preserved"
    assert after[0]["graph_layer"] == "canonical", "parity not reconciled"


@pytest.mark.asyncio
async def test_reconcile_promotes_matured_learning_via_r7_rule(db_factory):
    board_id = await _new_board(db_factory)
    ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    lid = _seed_learning(board_id, source_ref=ref, canonical=0, working=1)
    assert await _run_outbox(db_factory, board_id, [("Learning", lid)]) == 1
    assert _digest_layer(board_id, lid) == GRAPH_LAYER_WORKING  # incomplete -> working

    # Evidence matures: a canonical Bug now validates the learning. No new add.
    _mature_learning_with_canonical_bug(board_id, lid)
    assert await _run_outbox_no_refs(db_factory, board_id) == 1

    assert _digest_layer(board_id, lid) == GRAPH_LAYER_CANONICAL  # reconciled up
    assert _source_layer(board_id, "Learning", lid) == GRAPH_LAYER_CANONICAL


@pytest.mark.asyncio
async def test_reconcile_keeps_incomplete_learning_working_no_flip(db_factory):
    board_id = await _new_board(db_factory)
    ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    lid = _seed_learning(board_id, source_ref=ref, canonical=0, working=1)
    assert await _run_outbox(db_factory, board_id, [("Learning", lid)]) == 1
    assert _digest_layer(board_id, lid) == GRAPH_LAYER_WORKING

    # A bare reconcile event must NOT promote an still-incomplete Learning (AC2:
    # this is the expected state, not a parity violation).
    assert await _run_outbox_no_refs(db_factory, board_id) == 1
    assert _digest_layer(board_id, lid) == GRAPH_LAYER_WORKING


# ===========================================================================
# FR5 — missing graph_layer is legacy_unknown, never implicit canonical
# ===========================================================================


@pytest.mark.asyncio
async def test_legacy_missing_layer_stays_out_of_canonical(db_factory):
    board_id = await _new_board(db_factory)
    nid = f"req_{uuid.uuid4().hex[:10]}"
    _seed_node(board_id, "Requirement", nid, layer=None)  # no graph_layer at all
    assert await _run_outbox(db_factory, board_id, [("Requirement", nid)]) == 1
    # Published as legacy_unknown, NOT canonical.
    assert _digest_layer(board_id, nid) == LEGACY_UNKNOWN
    # A reconcile event must not "heal" it into canonical.
    assert await _run_outbox_no_refs(db_factory, board_id) == 1
    assert _digest_layer(board_id, nid) == LEGACY_UNKNOWN

    svc = get_kg_service()
    canon_ids = {r["id"] for r in svc.query_global(
        QUERY_TEXT, user_boards=[board_id], graph_layer="canonical", min_similarity=0.1)}
    all_ids = {r["id"] for r in svc.query_global(
        QUERY_TEXT, user_boards=[board_id], graph_layer="all", min_similarity=0.1)}
    assert nid not in canon_ids, "legacy_unknown digest leaked into canonical"
    assert nid in all_ids, "all query must still surface it diagnostically"


@pytest.mark.asyncio
async def test_promoted_node_becomes_queryable_in_canonical(db_factory):
    """AC1 end-to-end: after promotion + reconcile, canonical query returns it."""
    board_id = await _new_board(db_factory)
    nid = f"dec_{uuid.uuid4().hex[:10]}"
    _seed_node(board_id, "Decision", nid, layer="working")
    assert await _run_outbox(db_factory, board_id, [("Decision", nid)]) == 1

    svc = get_kg_service()
    canon_before = {r["id"] for r in svc.query_global(
        QUERY_TEXT, user_boards=[board_id], graph_layer="canonical", min_similarity=0.1)}
    assert nid not in canon_before  # working digest not in canonical

    _set_node_layer(board_id, "Decision", nid, "canonical")
    assert await _run_outbox_no_refs(db_factory, board_id) == 1

    canon_after = {r["id"] for r in svc.query_global(
        QUERY_TEXT, user_boards=[board_id], graph_layer="canonical", min_similarity=0.1)}
    assert nid in canon_after, "promoted node must be canonical-queryable after reconcile"
