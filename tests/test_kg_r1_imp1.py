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
from sqlalchemy import delete, select

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
    execute_global_read,
    execute_global_write,
    reset_global_discovery_runtime_for_tests,
)
from okto_pulse.core.kg.kg_service import get_kg_service
from okto_pulse.core.kg.primitives import _apply_graph_node_create
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


def _set_node_revoked(board_id, node_type, node_id, *, revoked: bool):
    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            f"MATCH (n:{node_type} {{id: $id}}) "
            "SET n.revocation_reason = $reason, n.superseded_by = $reason",
            {
                "id": node_id,
                "reason": "source_cancelled" if revoked else None,
            },
        )


def _node_attrs(source_ref, graph_layer, maturity, *, title, embedding):
    return {
        "title": title,
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
        "embedding": embedding,
        "graph_layer": graph_layer,
        "maturity_status": maturity,
    }


def _seed_learning(board_id, *, source_ref, title=QUERY_TEXT, canonical=0, working=0):
    """Canonical Learning (embedded on ``title``) + validates->Bug evidence."""
    learning_id = f"r1i1l_{uuid.uuid4().hex[:12]}"
    emb = get_embedding_provider().encode(title)
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _apply_graph_node_create(
            orch,
            "Learning",
            learning_id,
            _node_attrs(
                source_ref,
                GRAPH_LAYER_CANONICAL,
                MATURITY_CANONICAL_ELIGIBLE,
                title=title,
                embedding=emb,
            ),
        )
        for _ in range(canonical):
            _add_bug(orch, board_id, learning_id, GRAPH_LAYER_CANONICAL)
        for _ in range(working):
            _add_bug(orch, board_id, learning_id, GRAPH_LAYER_WORKING)
    return learning_id


def _add_bug(orch, board_id, learning_id, layer):
    bug_id = f"r1i1b_{uuid.uuid4().hex[:10]}"
    _apply_graph_node_create(
        orch,
        "Bug",
        bug_id,
        _node_attrs(
            f"bug:{bug_id}",
            layer,
            MATURITY_CANONICAL_ELIGIBLE
            if layer == GRAPH_LAYER_CANONICAL
            else MATURITY_WORKING_IMMATURE,
            title=f"bug {bug_id}",
            embedding=[0.0] * 384,
        ),
    )
    orch.create_edge(
        edge_type="validates",
        from_id=learning_id,
        to_id=bug_id,
        attrs={"confidence": 1.0},
        from_type="Learning",
        to_type="Bug",
    )
    return bug_id


def _mature_learning_with_canonical_bug(board_id, learning_id):
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,
            session_id=f"mat_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _add_bug(orch, board_id, learning_id, GRAPH_LAYER_CANONICAL)


async def _ensure_outbox_audit_parent(db, board_id: str, session_id: str) -> None:
    """Persist the relational parents required by KuzuNodeRef fixtures."""
    from datetime import datetime, timezone

    from sqlalchemy_test_models import ConsolidationAudit

    if await db.get(Board, board_id) is None:
        db.add(Board(id=board_id, name=f"R1 IMP1 {board_id}", owner_id=USER_ID))
        await db.flush()
    if await db.get(ConsolidationAudit, session_id) is None:
        now = datetime.now(timezone.utc)
        db.add(
            ConsolidationAudit(
                session_id=session_id,
                board_id=board_id,
                artifact_id=f"outbox-{session_id[-16:]}",
                artifact_type="test_fixture",
                agent_id=USER_ID,
                started_at=now,
                committed_at=now,
            )
        )
        await db.flush()


async def _run_outbox(db_factory, board_id, refs) -> int:
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        await _ensure_outbox_audit_parent(db, board_id, session_id)
        for node_type, node_id in refs:
            db.add(
                KuzuNodeRef(
                    session_id=session_id,
                    board_id=board_id,
                    kuzu_node_id=node_id,
                    kuzu_node_type=node_type,
                    operation="add",
                )
            )
        db.add(
            GlobalUpdateOutbox(
                event_id=str(uuid.uuid4()),
                board_id=board_id,
                session_id=session_id,
                event_type="consolidation_committed",
                payload={"session_id": session_id, "nodes_added": len(refs)},
            )
        )
        await db.commit()
    return await GlobalOutboxProcessor(db_factory, interval_seconds=5).process_once()


async def _run_outbox_no_refs(db_factory, board_id) -> int:
    """An outbox event with NO KuzuNodeRef add rows — exercises the reconcile
    path that must run before the empty-refs early return (FR1/FR2)."""
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        db.add(
            GlobalUpdateOutbox(
                event_id=str(uuid.uuid4()),
                board_id=board_id,
                session_id=session_id,
                event_type="consolidation_committed",
                payload={"session_id": session_id, "nodes_added": 0},
            )
        )
        await db.commit()
    return await GlobalOutboxProcessor(db_factory, interval_seconds=5).process_once()


def _digests_for(board_id, node_id) -> list[dict]:
    res = execute_global_read(
        "MATCH (d:DecisionDigest) WHERE d.board_id = $b AND d.original_node_id = $n "
        "RETURN d.id, coalesce(d.graph_layer, 'legacy_unknown'), "
        "coalesce(d.source_revoked, false)",
        {"b": board_id, "n": node_id},
    )
    return [
        {
            "id": str(row[0]),
            "graph_layer": str(row[1]),
            "source_revoked": bool(row[2]),
        }
        for row in res.rows
    ]


def _digest_layer(board_id, node_id) -> str | None:
    rows = _digests_for(board_id, node_id)
    return rows[0]["graph_layer"] if rows else None


def _source_layer(board_id, node_type, node_id) -> str | None:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            f"MATCH (n:{node_type} {{id: $id}}) RETURN n.graph_layer",
            {"id": node_id},
        )
        return str(res.get_next()[0]) if res.has_next() else None


# ===========================================================================
# Pure resolver
# ===========================================================================


def test_resolver_non_learning_mirrors_raw_layer():
    assert resolve_expected_digest_layer(
        node_type="Decision", raw_graph_layer="canonical"
    ) == ("canonical", None)
    assert resolve_expected_digest_layer(
        node_type="Decision", raw_graph_layer="working"
    ) == ("working", None)
    # Fail-closed: a legacy_unknown stays legacy_unknown, never canonical (FR5).
    assert resolve_expected_digest_layer(
        node_type="Requirement", raw_graph_layer=LEGACY_UNKNOWN
    ) == (LEGACY_UNKNOWN, None)


def test_resolver_canonical_learning_uses_r7_rule():
    # Incomplete (working-only) canonical Learning -> working publication.
    layer, reason = resolve_expected_digest_layer(
        node_type="Learning",
        raw_graph_layer="canonical",
        source_artifact_ref="card:bug:x:learning:1",
        canonical_bug_count=0,
    )
    assert layer == GRAPH_LAYER_WORKING and reason is not None
    # Complete (>=1 canonical bug) -> canonical.
    layer, reason = resolve_expected_digest_layer(
        node_type="Learning",
        raw_graph_layer="canonical",
        source_artifact_ref="card:bug:x:learning:1",
        canonical_bug_count=1,
    )
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
async def test_reconcile_backfills_missing_digest_from_authoritative_board(
    db_factory,
):
    board_id = await _new_board(db_factory)
    node_id = f"dec_{uuid.uuid4().hex[:10]}"
    _seed_node(board_id, "Decision", node_id, layer="canonical")
    assert _digests_for(board_id, node_id) == []

    assert await _run_outbox_no_refs(db_factory, board_id) == 1

    rows = _digests_for(board_id, node_id)
    assert len(rows) == 1
    assert rows[0]["graph_layer"] == "canonical"


@pytest.mark.asyncio
async def test_revoked_source_is_hidden_globally_and_restore_preserves_relations(
    db_factory,
):
    """Global lifecycle is reversible even when the digest has derived edges."""

    board_id = await _new_board(db_factory)
    node_id = f"dec_{uuid.uuid4().hex[:10]}"
    peer_id = f"dec_{uuid.uuid4().hex[:10]}"
    _seed_node(board_id, "Decision", node_id, layer="canonical")
    _seed_node(board_id, "Decision", peer_id, layer="canonical")
    assert (
        await _run_outbox(
            db_factory,
            board_id,
            [("Decision", node_id), ("Decision", peer_id)],
        )
        == 1
    )
    assert _digest_layer(board_id, node_id) == "canonical"
    digest_id = _digests_for(board_id, node_id)[0]["id"]
    peer_digest_id = _digests_for(board_id, peer_id)[0]["id"]
    execute_global_write(
        "MATCH (d:DecisionDigest {id: $digest_id}), "
        "(peer:DecisionDigest {id: $peer_digest_id}) "
        "CREATE (d)-[:DECISION_DERIVES_FROM]->(peer)",
        {"digest_id": digest_id, "peer_digest_id": peer_digest_id},
        operation="test_global_lifecycle_derived_edge",
    )

    _set_node_revoked(board_id, "Decision", node_id, revoked=True)
    assert await _run_outbox_no_refs(db_factory, board_id) == 1
    hidden = _digests_for(board_id, node_id)
    assert len(hidden) == 1
    assert hidden[0]["source_revoked"] is True
    visible = execute_global_read(
        "MATCH (b:Board)-[:CONTAINS_DECISION]->(d:DecisionDigest) "
        "WHERE b.board_id = $bid AND d.original_node_id = $oid "
        "AND (d.source_revoked IS NULL OR d.source_revoked = false) "
        "RETURN d.id",
        {"bid": board_id, "oid": node_id},
    )
    assert visible.rows == ()
    related = execute_global_read(
        "MATCH (d:DecisionDigest {id: $digest_id})"
        "-[r:DECISION_DERIVES_FROM]->"
        "(peer:DecisionDigest {id: $peer_digest_id}) RETURN count(r)",
        {"digest_id": digest_id, "peer_digest_id": peer_digest_id},
    )
    assert related.rows[0][0] == 1

    _set_node_revoked(board_id, "Decision", node_id, revoked=False)
    assert await _run_outbox_no_refs(db_factory, board_id) == 1
    assert _digest_layer(board_id, node_id) == "canonical"
    assert _digests_for(board_id, node_id)[0]["source_revoked"] is False
    related = execute_global_read(
        "MATCH (d:DecisionDigest {id: $digest_id})"
        "-[r:DECISION_DERIVES_FROM]->"
        "(peer:DecisionDigest {id: $peer_digest_id}) RETURN count(r)",
        {"digest_id": digest_id, "peer_digest_id": peer_digest_id},
    )
    assert related.rows[0][0] == 1


@pytest.mark.asyncio
async def test_source_without_embedding_is_not_required_for_set_parity(db_factory):
    board_id = await _new_board(db_factory)
    node_id = f"dec_{uuid.uuid4().hex[:10]}"
    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            "CREATE (n:Decision {id: $id, title: $title, graph_layer: $layer})",
            {"id": node_id, "title": "not embedded", "layer": "canonical"},
        )

    assert await _run_outbox_no_refs(db_factory, board_id) == 1
    assert _digests_for(board_id, node_id) == []


def test_source_inventory_paginates_at_row_cap_without_false_prune(monkeypatch):
    from okto_pulse.core.application.processors import global_outbox as module

    page_size = module.BOARD_SOURCE_INVENTORY_PAGE_SIZE
    calls: list[tuple[str, str]] = []

    class _Executor:
        def execute_read_only(self, _board_id, statement, params, max_rows):
            calls.append((statement, params["after_id"]))
            assert max_rows == page_size
            assert "n.embedding IS NOT NULL" in statement
            assert "n.revocation_reason IS NULL" in statement
            assert "n.superseded_by IS NULL" in statement
            assert "SKIP" not in statement
            if params["after_id"] == "":
                return {
                    "rows": [[f"node-{index:05d}", 1] for index in range(page_size)]
                }
            assert params["after_id"] == f"node-{page_size - 1:05d}"
            return {"rows": [[f"node-{page_size:05d}", 1]]}

    class _Registry:
        cypher_executor = _Executor()

    monkeypatch.setattr(module, "DIGESTED_NODE_TYPES", ("Decision",))
    monkeypatch.setattr(module, "get_kg_registry", lambda: _Registry())

    inventory = GlobalOutboxProcessor._read_board_digestable_node_types(
        "board-paginated"
    )

    assert inventory is not None
    assert len(inventory) == page_size + 1
    assert set(inventory.values()) == {"Decision"}
    assert len(calls) == 2
    assert [cursor for _statement, cursor in calls] == [
        "",
        f"node-{page_size - 1:05d}",
    ]


def test_source_inventory_fails_closed_when_pagination_does_not_advance(
    monkeypatch,
):
    from okto_pulse.core.application.processors import global_outbox as module

    page = [
        [f"node-{index:05d}", 1]
        for index in range(module.BOARD_SOURCE_INVENTORY_PAGE_SIZE)
    ]

    class _Executor:
        def execute_read_only(self, _board_id, _statement, _params, max_rows):
            assert max_rows == module.BOARD_SOURCE_INVENTORY_PAGE_SIZE
            return {"rows": page}

    class _Registry:
        cypher_executor = _Executor()

    monkeypatch.setattr(module, "DIGESTED_NODE_TYPES", ("Decision",))
    monkeypatch.setattr(module, "get_kg_registry", lambda: _Registry())

    with pytest.raises(RuntimeError, match="source_inventory_pagination_stalled"):
        GlobalOutboxProcessor._read_board_digestable_node_types("board-stalled")


def test_source_inventory_rejects_same_label_physical_duplicate(monkeypatch):
    from okto_pulse.core.application.processors import global_outbox as module

    class _Executor:
        def execute_read_only(self, _board_id, statement, params, max_rows):
            assert "n.embedding IS NOT NULL" in statement
            assert "n.revocation_reason IS NULL" in statement
            assert "n.superseded_by IS NULL" in statement
            assert params == {"after_id": ""}
            assert max_rows == module.BOARD_SOURCE_INVENTORY_PAGE_SIZE
            return {"rows": [["duplicate-decision", 2]]}

    class _Registry:
        cypher_executor = _Executor()

    monkeypatch.setattr(module, "DIGESTED_NODE_TYPES", ("Decision",))
    monkeypatch.setattr(module, "get_kg_registry", lambda: _Registry())

    with pytest.raises(RuntimeError, match="source_identity_duplicate"):
        GlobalOutboxProcessor._read_board_digestable_node_types(
            "board-duplicate-source"
        )


def test_source_inventory_final_recheck_detects_concurrent_remove_and_insert(
    monkeypatch,
):
    from okto_pulse.core.application.processors import global_outbox as module

    state = {"rows": [["decision-a", 1], ["decision-b", 1]]}

    class _Executor:
        def execute_read_only(self, _board_id, statement, params, max_rows):
            assert "SKIP" not in statement
            assert params == {"after_id": ""}
            assert max_rows == module.BOARD_SOURCE_INVENTORY_PAGE_SIZE
            return {"rows": list(state["rows"])}

    class _Registry:
        cypher_executor = _Executor()

    monkeypatch.setattr(module, "DIGESTED_NODE_TYPES", ("Decision",))
    monkeypatch.setattr(module, "get_kg_registry", lambda: _Registry())

    baseline = GlobalOutboxProcessor._read_board_digestable_node_types(
        "board-concurrent-inventory"
    )
    assert baseline == {"decision-a": "Decision", "decision-b": "Decision"}

    state["rows"] = [["decision-b", 1], ["decision-c", 1]]
    with pytest.raises(RuntimeError, match="source_inventory_changed"):
        GlobalOutboxProcessor._assert_source_inventory_unchanged(
            "board-concurrent-inventory",
            baseline,
        )


def test_learning_evidence_aggregates_above_cap_and_keyset_paginates_relations(
    monkeypatch,
):
    from okto_pulse.core.application.processors import global_outbox as module

    learning_id = "learning-many-edges"
    calls: list[tuple[str, tuple[str, str, str] | None]] = []

    class _Executor:
        def execute_read_only(self, _board_id, statement, params, max_rows):
            if ":validates" in statement:
                calls.append(("validates", None))
                assert "sum(CASE" in statement
                assert max_rows == 1
                return {"rows": [[learning_id, 10001, 10002]]}
            cursor = (
                params["after_learning_id"],
                params["after_node_type"],
                params["after_layer"],
            )
            calls.append(("relates_to", cursor))
            assert "count(t)" in statement
            assert "SKIP" not in statement
            assert max_rows == 2
            if cursor == ("", "", ""):
                return {
                    "rows": [
                        [learning_id, "Decision", "working", 10001],
                        [learning_id, "Entity", "working", 1],
                    ]
                }
            assert cursor == (learning_id, "Entity", "working")
            return {"rows": [[learning_id, "Requirement", "canonical", 1]]}

    class _Registry:
        cypher_executor = _Executor()

    monkeypatch.setattr(module, "LEARNING_RELATION_GROUP_PAGE_SIZE", 2)
    monkeypatch.setattr(module, "get_kg_registry", lambda: _Registry())

    evidence = GlobalOutboxProcessor._read_learning_completeness_evidence(
        "board-learning-cap",
        [learning_id],
    )

    assert evidence[learning_id]["canonical_bug_count"] == 10001
    assert evidence[learning_id]["working_bug_count"] == 1
    assert evidence[learning_id]["relates_to_endpoints"] == [
        ("Decision", "working"),
        ("Entity", "working"),
        ("Requirement", "canonical"),
    ]
    assert calls == [
        ("validates", None),
        ("relates_to", ("", "", "")),
        ("relates_to", (learning_id, "Entity", "working")),
    ]


def test_prune_delegates_complete_stale_set_to_guard_before_any_delete():
    from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult

    class _Runtime:
        def __init__(self) -> None:
            self.execute_calls = 0
            self.guarded_calls: list[dict] = []

        def execute(self, statement, params=None):
            self.execute_calls += 1
            assert "DETACH DELETE" not in statement
            assert params == {"bid": "board-prune-guard"}
            return GraphStatementResult.from_rows(
                [
                    ["digest-stale", "stale-source"],
                    ["digest-malformed", ""],
                ]
            )

        def delete_decision_digests_guarded(self, **values):
            self.guarded_calls.append(values)
            raise RuntimeError("global_discovery.digest_prune_relationships_present")

    runtime = _Runtime()
    with pytest.raises(RuntimeError, match="digest_prune_relationships_present"):
        GlobalOutboxProcessor._prune_stale_board_digests(
            runtime,
            "board-prune-guard",
            set(),
        )

    assert runtime.execute_calls == 1
    assert runtime.guarded_calls == [
        {
            "board_id": "board-prune-guard",
            "original_node_ids": ("stale-source",),
            "include_malformed": True,
        }
    ]


def test_digest_verifier_rejects_cross_board_or_unstable_contains_edge():
    from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult

    board_id = "board-verify-edge"
    node_id = "decision-edge"
    digest_id = f"dd_{board_id[:8]}_{node_id}"

    class _Runtime:
        def execute(self, statement, params=None):
            assert params == {"bid": board_id}
            if "RETURN d.id, d.original_node_id" in statement:
                return GraphStatementResult.from_rows(
                    [[digest_id, node_id, "Decision", "canonical"]]
                )
            if "RETURN b.board_id, d.board_id" in statement:
                return GraphStatementResult.from_rows(
                    [
                        [
                            board_id,
                            "foreign-board",
                            "dd_foreign_unstable",
                            node_id,
                            1,
                        ]
                    ]
                )
            assert "WHERE d.board_id = $bid RETURN d.id" in statement
            return GraphStatementResult.from_rows([])

    with pytest.raises(RuntimeError, match="invalid_contains_edge"):
        GlobalOutboxProcessor()._verify_reconciled_digest_layers(
            _Runtime(),
            board_id,
            {node_id: ("canonical", "Decision")},
        )


@pytest.mark.asyncio
async def test_reconcile_repairs_duplicate_digest_identity_and_verifies_readback(
    monkeypatch,
):
    """Model a legacy Ladybug file that returns two physical rows for one PK.

    Fresh Ladybug schemas reject a sequential duplicate primary key, so the
    corrupt historical shape is represented at the port boundary here.  The
    Community adapter's one-statement delete/recreate path is covered against a
    real Ladybug database in its edition test suite.
    """
    from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult

    board_id = "board-duplicate-reconcile"
    node_id = "decision-duplicate"
    digest_id = f"dd_{board_id[:8]}_{node_id}"

    class _DuplicateDigestRuntime:
        def __init__(self) -> None:
            self.rows = [
                [digest_id, node_id, "Decision", "canonical"],
                [digest_id, node_id, "Entity", "working"],
            ]
            self.replace_calls: list[dict] = []

        def execute(self, statement, params=None):
            assert params == {"bid": board_id}
            if "RETURN d.id, d.original_node_id" in statement:
                return GraphStatementResult.from_rows(self.rows)
            if "RETURN b.board_id, d.board_id" in statement:
                return GraphStatementResult.from_rows(
                    [[board_id, board_id, digest_id, node_id, 1]]
                )
            assert "WHERE d.board_id = $bid RETURN d.id" in statement
            return GraphStatementResult.from_rows([[digest_id, 1]])

        def delete_invalid_board_digest_links(self, **_values):
            return 0

        def replace_decision_digest_identity(self, **values):
            self.replace_calls.append(values)
            self.rows = [
                [
                    values["digest_id"],
                    values["original_node_id"],
                    values["node_type"],
                    values["graph_layer"],
                ]
            ]
            return 2

    runtime = _DuplicateDigestRuntime()
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_read_board_layer_meta",
        staticmethod(
            lambda _board_id, _types: {
                node_id: {
                    "node_type": "Decision",
                    "graph_layer": "canonical",
                    "source_artifact_ref": "",
                    "canonical_bug_count": 0,
                    "relates_to_endpoints": [],
                }
            }
        ),
    )
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_read_board_nodes_for_refs",
        staticmethod(
            lambda _board_id, _refs: [
                {
                    "id": node_id,
                    "title": "Canonical source",
                    "embedding": [0.0] * 384,
                    "graph_layer": "canonical",
                    "node_type": "Decision",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_assert_source_inventory_unchanged",
        staticmethod(lambda _board_id, _types: None),
    )

    corrected = await GlobalOutboxProcessor()._reconcile_board_digest_layers(
        runtime,
        board_id,
        object(),
        source_types_by_id={node_id: "Decision"},
    )

    assert corrected == 1
    assert len(runtime.replace_calls) == 1
    assert runtime.replace_calls[0]["digest_id"] == digest_id
    assert runtime.replace_calls[0]["graph_layer"] == "canonical"
    assert runtime.rows == [[digest_id, node_id, "Decision", "canonical"]]

    assert (
        await GlobalOutboxProcessor()._reconcile_board_digest_layers(
            runtime,
            board_id,
            object(),
            source_types_by_id={node_id: "Decision"},
        )
        == 0
    )
    assert len(runtime.replace_calls) == 1


@pytest.mark.asyncio
async def test_reconcile_restores_missing_board_digest_edge_without_republish(
    monkeypatch,
):
    from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult

    board_id = "board-missing-digest-link"
    node_id = "decision-missing-link"
    digest_id = f"dd_{board_id[:8]}_{node_id}"

    class _MissingLinkRuntime:
        def __init__(self) -> None:
            self.edge_count = 0
            self.link_calls = 0

        def execute(self, statement, params=None):
            assert params == {"bid": board_id}
            if "RETURN d.id, d.original_node_id" in statement:
                return GraphStatementResult.from_rows(
                    [[digest_id, node_id, "Decision", "canonical"]]
                )
            if "RETURN b.board_id, d.board_id" in statement:
                rows = (
                    [[board_id, board_id, digest_id, node_id, self.edge_count]]
                    if self.edge_count
                    else []
                )
                return GraphStatementResult.from_rows(rows)
            assert "WHERE d.board_id = $bid RETURN d.id" in statement
            rows = [[digest_id, self.edge_count]] if self.edge_count else []
            return GraphStatementResult.from_rows(rows)

        def delete_invalid_board_digest_links(self, **_values):
            return 0

        def link_board_digest(self, *, board_id, digest_id):
            assert board_id == "board-missing-digest-link"
            assert digest_id == "dd_board-mi_decision-missing-link"
            self.edge_count = 1
            self.link_calls += 1

    runtime = _MissingLinkRuntime()
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_read_board_layer_meta",
        staticmethod(
            lambda _board_id, _types: {
                node_id: {
                    "node_type": "Decision",
                    "graph_layer": "canonical",
                    "source_artifact_ref": "",
                    "canonical_bug_count": 0,
                    "relates_to_endpoints": [],
                }
            }
        ),
    )
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_assert_source_inventory_unchanged",
        staticmethod(lambda _board_id, _types: None),
    )

    corrected = await GlobalOutboxProcessor()._reconcile_board_digest_layers(
        runtime,
        board_id,
        object(),
        source_types_by_id={node_id: "Decision"},
    )

    assert corrected == 1
    assert runtime.link_calls == 1
    assert (
        await GlobalOutboxProcessor()._reconcile_board_digest_layers(
            runtime,
            board_id,
            object(),
            source_types_by_id={node_id: "Decision"},
        )
        == 0
    )
    assert runtime.link_calls == 1


@pytest.mark.asyncio
async def test_reconcile_removes_cross_board_outgoing_edge_then_links_correct_digest(
    monkeypatch,
):
    from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult

    board_id = "board-cross-edge"
    node_id = "decision-cross-edge"
    digest_id = f"dd_{board_id[:8]}_{node_id}"

    class _CrossBoardRuntime:
        def __init__(self) -> None:
            self.invalid_edge = True
            self.correct_edge = False
            self.invalid_removed = 0

        def execute(self, statement, params=None):
            assert params == {"bid": board_id}
            if "RETURN d.id, d.original_node_id" in statement:
                return GraphStatementResult.from_rows(
                    [[digest_id, node_id, "Decision", "canonical"]]
                )
            if "RETURN b.board_id, d.board_id" in statement:
                rows = []
                if self.invalid_edge:
                    rows.append(
                        [
                            board_id,
                            "foreign-board",
                            "dd_foreign_decision-cross-edge",
                            node_id,
                            1,
                        ]
                    )
                if self.correct_edge:
                    rows.append([board_id, board_id, digest_id, node_id, 1])
                return GraphStatementResult.from_rows(rows)
            assert "WHERE d.board_id = $bid RETURN d.id" in statement
            rows = [[digest_id, 1]] if self.correct_edge else []
            return GraphStatementResult.from_rows(rows)

        def delete_invalid_board_digest_links(self, **_values):
            if not self.invalid_edge:
                return 0
            self.invalid_edge = False
            self.invalid_removed += 1
            return 1

        def link_board_digest(self, **_values):
            self.correct_edge = True

        def normalize_board_digest_link(self, **_values):
            raise AssertionError("no inbound link existed; simple link is enough")

    runtime = _CrossBoardRuntime()
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_read_board_layer_meta",
        staticmethod(
            lambda _board_id, _types: {
                node_id: {
                    "node_type": "Decision",
                    "graph_layer": "canonical",
                    "source_artifact_ref": "",
                    "canonical_bug_count": 0,
                    "relates_to_endpoints": [],
                }
            }
        ),
    )
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_assert_source_inventory_unchanged",
        staticmethod(lambda _board_id, _types: None),
    )

    assert (
        await GlobalOutboxProcessor()._reconcile_board_digest_layers(
            runtime,
            board_id,
            object(),
            source_types_by_id={node_id: "Decision"},
        )
        == 2
    )
    assert runtime.invalid_removed == 1
    assert runtime.correct_edge is True
    assert (
        await GlobalOutboxProcessor()._reconcile_board_digest_layers(
            runtime,
            board_id,
            object(),
            source_types_by_id={node_id: "Decision"},
        )
        == 0
    )


@pytest.mark.asyncio
async def test_reconcile_collapses_duplicate_contains_without_replacing_digest(
    monkeypatch,
):
    from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult

    board_id = "board-duplicate-edge"
    node_id = "decision-duplicate-edge"
    digest_id = f"dd_{board_id[:8]}_{node_id}"

    class _DuplicateEdgeRuntime:
        def __init__(self) -> None:
            self.edge_count = 2
            self.normalize_calls = 0
            self.derived_relationships_preserved = True

        def execute(self, statement, params=None):
            assert params == {"bid": board_id}
            if "RETURN d.id, d.original_node_id" in statement:
                return GraphStatementResult.from_rows(
                    [[digest_id, node_id, "Decision", "canonical"]]
                )
            if "RETURN b.board_id, d.board_id" in statement:
                return GraphStatementResult.from_rows(
                    [
                        [
                            board_id,
                            board_id,
                            digest_id,
                            node_id,
                            self.edge_count,
                        ]
                    ]
                )
            assert "WHERE d.board_id = $bid RETURN d.id" in statement
            return GraphStatementResult.from_rows([[digest_id, self.edge_count]])

        def delete_invalid_board_digest_links(self, **_values):
            return 0

        def normalize_board_digest_link(self, **_values):
            removed = self.edge_count
            self.edge_count = 1
            self.normalize_calls += 1
            return removed

        def replace_decision_digest_identity(self, **_values):
            raise AssertionError("edge-only corruption must not replace digest")

    runtime = _DuplicateEdgeRuntime()
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_read_board_layer_meta",
        staticmethod(
            lambda _board_id, _types: {
                node_id: {
                    "node_type": "Decision",
                    "graph_layer": "canonical",
                    "source_artifact_ref": "",
                    "canonical_bug_count": 0,
                    "relates_to_endpoints": [],
                }
            }
        ),
    )
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_assert_source_inventory_unchanged",
        staticmethod(lambda _board_id, _types: None),
    )

    assert (
        await GlobalOutboxProcessor()._reconcile_board_digest_layers(
            runtime,
            board_id,
            object(),
            source_types_by_id={node_id: "Decision"},
        )
        == 1
    )
    assert runtime.normalize_calls == 1
    assert runtime.derived_relationships_preserved is True
    assert (
        await GlobalOutboxProcessor()._reconcile_board_digest_layers(
            runtime,
            board_id,
            object(),
            source_types_by_id={node_id: "Decision"},
        )
        == 0
    )


@pytest.mark.asyncio
async def test_reconcile_readback_failure_keeps_outbox_pending_for_retry(
    db_factory,
    monkeypatch,
):
    board_id = await _new_board(db_factory)
    node_id = f"dec_{uuid.uuid4().hex[:10]}"
    _seed_node(board_id, "Decision", node_id, layer="working")
    assert await _run_outbox(db_factory, board_id, [("Decision", node_id)]) == 1
    _set_node_layer(board_id, "Decision", node_id, "canonical")

    event_id = str(uuid.uuid4())
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        db.add(
            GlobalUpdateOutbox(
                event_id=event_id,
                board_id=board_id,
                session_id=session_id,
                event_type="consolidation_committed",
                payload={"session_id": session_id, "nodes_added": 0},
            )
        )
        await db.commit()

    original_readback = GlobalOutboxProcessor._read_global_digest_rows
    reads = 0

    def _stale_fresh_read(gconn, selected_board_id):
        nonlocal reads
        reads += 1
        rows = original_readback(gconn, selected_board_id)
        if reads == 3:
            return [
                {**row, "current_layer": "working"}
                if row["original_node_id"] == node_id
                else row
                for row in rows
            ]
        return rows

    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_read_global_digest_rows",
        staticmethod(_stale_fresh_read),
    )

    assert (
        await GlobalOutboxProcessor(
            db_factory,
            interval_seconds=5,
        ).process_once()
        == 0
    )
    assert reads == 3  # initial scan, same-handle check, fresh post-flush check

    async with db_factory() as db:
        row = (
            await db.execute(
                select(GlobalUpdateOutbox).where(
                    GlobalUpdateOutbox.event_id == event_id
                )
            )
        ).scalar_one()
        assert row.processed_at is None
        assert row.retry_count == 1
        assert "outbox.digest_reconcile_verification_failed" in (row.last_error or "")


@pytest.mark.asyncio
async def test_reconcile_does_not_skip_existing_digest_present_in_add_refs(
    db_factory,
    monkeypatch,
):
    """A same-session add ref is not proof that the later add-path will upsert.

    Reproduce the production failure mode: a working digest already exists, its
    source is now canonical, the same id occurs in this event's add refs, and
    source-node materialization omits it.  Parity must still converge before the
    add-path returns with no rows.
    """
    board_id = await _new_board(db_factory)
    nid = f"dec_{uuid.uuid4().hex[:10]}"
    _seed_node(board_id, "Decision", nid, layer="working")
    assert await _run_outbox(db_factory, board_id, [("Decision", nid)]) == 1
    assert _digest_layer(board_id, nid) == "working"

    _set_node_layer(board_id, "Decision", nid, "canonical")
    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_read_board_nodes_for_refs",
        staticmethod(lambda _board_id, _refs: []),
    )

    assert await _run_outbox(db_factory, board_id, [("Decision", nid)]) == 1
    assert _digest_layer(board_id, nid) == "canonical"


@pytest.mark.asyncio
async def test_board_meta_read_failure_keeps_outbox_event_pending_for_retry(
    db_factory,
    monkeypatch,
):
    board_id = await _new_board(db_factory)
    nid = f"dec_{uuid.uuid4().hex[:10]}"
    _seed_node(board_id, "Decision", nid, layer="working")
    assert await _run_outbox(db_factory, board_id, [("Decision", nid)]) == 1
    _set_node_layer(board_id, "Decision", nid, "canonical")

    event_id = str(uuid.uuid4())
    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    async with db_factory() as db:
        await db.execute(delete(GlobalUpdateOutbox))
        db.add(
            GlobalUpdateOutbox(
                event_id=event_id,
                board_id=board_id,
                session_id=session_id,
                event_type="consolidation_committed",
                payload={"session_id": session_id, "nodes_added": 0},
            )
        )
        await db.commit()

    monkeypatch.setattr(
        GlobalOutboxProcessor,
        "_read_board_layer_meta",
        staticmethod(lambda _board_id, _node_types_by_id: None),
    )
    assert (
        await GlobalOutboxProcessor(db_factory, interval_seconds=5).process_once() == 0
    )

    async with db_factory() as db:
        row = (
            await db.execute(
                select(GlobalUpdateOutbox).where(
                    GlobalUpdateOutbox.event_id == event_id
                )
            )
        ).scalar_one()
        assert row.processed_at is None
        assert row.retry_count == 1
        assert "outbox.read_board_failed" in (row.last_error or "")


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
    canon_ids = {
        r["id"]
        for r in svc.query_global(
            QUERY_TEXT,
            user_boards=[board_id],
            graph_layer="canonical",
            min_similarity=0.1,
        )
    }
    all_ids = {
        r["id"]
        for r in svc.query_global(
            QUERY_TEXT, user_boards=[board_id], graph_layer="all", min_similarity=0.1
        )
    }
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
    canon_before = {
        r["id"]
        for r in svc.query_global(
            QUERY_TEXT,
            user_boards=[board_id],
            graph_layer="canonical",
            min_similarity=0.1,
        )
    }
    assert nid not in canon_before  # working digest not in canonical

    _set_node_layer(board_id, "Decision", nid, "canonical")
    assert await _run_outbox_no_refs(db_factory, board_id) == 1

    canon_after = {
        r["id"]
        for r in svc.query_global(
            QUERY_TEXT,
            user_boards=[board_id],
            graph_layer="canonical",
            min_similarity=0.1,
        )
    }
    assert nid in canon_after, (
        "promoted node must be canonical-queryable after reconcile"
    )
