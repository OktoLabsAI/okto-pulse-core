"""R2-IMP2 — preserve canonical cognitive knowledge across a board rebuild.

Spec 9aedfe78 / card 91410196 (FR4/FR9, TR5/TR9, AC3/AC10, ts_c6434c50/ts_6d7eaa59).

Anti-test-theater: the canonical cognitive starting state is materialized via the
consolidation orchestrator (the R7-validated write primitive), and the rebuild
purge is the REAL ``purge_board_graph_storage``. The negative/teeth test proves
that WITHOUT the restore the cognitive node is genuinely lost.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2i2_"))

from okto_pulse.core.kg import canonical_cognitive_preservation as ccp
from okto_pulse.core.kg.canonical_cognitive_preservation import (
    CognitiveSnapshot,
    RestoreResult,
    preservation_summary,
    restore_canonical_cognitive,
    snapshot_canonical_cognitive,
)
from okto_pulse.core.kg.primitives import _apply_kuzu_node_create_with_timestamp
from kg_schema_testing import (
    bootstrap_board_graph,
    open_board_connection,
    purge_board_graph_storage,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    MATURITY_CANONICAL_ELIGIBLE,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    RealBoardGraphLifecycleForTests,
    RealBoardGraphTransactionForTests,
    configure_test_kg_registry,
)


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_test_kg_registry(
        cypher_executor=RealBoardCypherExecutorForTests(),
        graph_transaction=RealBoardGraphTransactionForTests(),
        graph_lifecycle=RealBoardGraphLifecycleForTests(),
    )


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


def _new_board() -> str:
    board_id = f"r2i2-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    return board_id


def _attrs(source_ref, layer, maturity, *, title="node"):
    return {
        "title": title, "content": "", "context": "", "justification": "",
        "source_artifact_ref": source_ref, "created_at": "2026-06-08T00:00:00+00:00",
        "created_by_agent": "agent:cognitive", "source_confidence": 1.0,
        "relevance_score": 0.5, "query_hits": 0, "last_queried_at": None,
        "priority_boost": 0.0, "human_curated": False, "embedding": [0.0] * 384,
        "graph_layer": layer, "maturity_status": maturity,
    }


def _seed_learning_with_canonical_bug(board_id, *, learning_ref):
    """Canonical bug-derived Learning + canonical Bug + validates edge (orchestrator
    consolidation primitive). Returns (learning_id, bug_id, bug_attrs)."""
    learning_id = f"r2i2l_{uuid.uuid4().hex[:12]}"
    bug_id = f"r2i2b_{uuid.uuid4().hex[:12]}"
    bug_attrs = _attrs(f"bug:{bug_id}", GRAPH_LAYER_CANONICAL, MATURITY_CANONICAL_ELIGIBLE,
                       title="bug node")
    with open_board_connection(board_id) as (_db, conn):
        orch = TransactionOrchestrator(
            kuzu_conn=conn, sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _apply_kuzu_node_create_with_timestamp(
            orch, "Learning", learning_id,
            _attrs(learning_ref, GRAPH_LAYER_CANONICAL, MATURITY_CANONICAL_ELIGIBLE,
                   title="learning node"),
        )
        _apply_kuzu_node_create_with_timestamp(orch, "Bug", bug_id, bug_attrs)
        orch.create_edge(edge_type="validates", from_id=learning_id, to_id=bug_id,
                         attrs={"confidence": 1.0}, from_type="Learning", to_type="Bug")
    return learning_id, bug_id, bug_attrs


def _recreate_bug(board_id, bug_id, bug_attrs):
    """Simulate the deterministic rebuild re-materializing the Bug endpoint."""
    with open_board_connection(board_id) as (_db, conn):
        orch = TransactionOrchestrator(
            kuzu_conn=conn, sqlite_session=None,
            session_id=f"redet_{uuid.uuid4().hex[:8]}", board_id=board_id,
        )
        _apply_kuzu_node_create_with_timestamp(orch, "Bug", bug_id, bug_attrs)


def _purge_and_rebootstrap(board_id):
    purge_board_graph_storage(board_id, reason="r2i2_test_rebuild")
    bootstrap_board_graph(board_id)


def _count(board_id, node_type, *, layer=None) -> int:
    with open_board_connection(board_id) as (_db, conn):
        if layer is None:
            res = conn.execute(f"MATCH (n:{node_type}) RETURN count(n)")
        else:
            res = conn.execute(
                f"MATCH (n:{node_type}) WHERE n.graph_layer = $l RETURN count(n)",
                {"l": layer},
            )
        return int(res.get_next()[0]) if res.has_next() else 0


def _edge_count(board_id, etype) -> int:
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(f"MATCH ()-[r:{etype}]->() RETURN count(r)")
        return int(res.get_next()[0]) if res.has_next() else 0


# ===========================================================================
# preservation_summary status logic (clean / degraded / unreadable / integrity)
# ===========================================================================


def test_preservation_summary_status_matrix():
    snap = CognitiveSnapshot(board_id="b", readable=True,
                             nodes=[{"node_type": "Learning", "id": "x", "attrs": {}}], edges=[])
    assert preservation_summary(snap, RestoreResult(restored_nodes=1))["status"] == ccp.STATUS_CLEAN
    degraded = preservation_summary(
        snap, RestoreResult(restored_nodes=1, unrestorable=[{"kind": "edge"}]))
    assert degraded["status"] == ccp.STATUS_DEGRADED
    assert preservation_summary(
        CognitiveSnapshot(board_id="b", readable=False), RestoreResult(unreadable=True),
    )["status"] == ccp.STATUS_UNREADABLE
    assert preservation_summary(
        snap, RestoreResult(error="restore_failed:RuntimeError"),
    )["status"] == ccp.STATUS_INTEGRITY_ERROR


# ===========================================================================
# Clean survival across a REAL purge+restore (FR9/AC, node + edge — #2)
# ===========================================================================


def test_cognitive_canonical_node_and_edge_survive_rebuild():
    board_id = _new_board()
    ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    learning_id, bug_id, bug_attrs = _seed_learning_with_canonical_bug(board_id, learning_ref=ref)

    snap = snapshot_canonical_cognitive(board_id)
    assert snap.readable
    assert any(n["id"] == learning_id and n["node_type"] == "Learning" for n in snap.nodes)
    assert any(e["edge_type"] == "validates" and e["from_id"] == learning_id
               and e["to_id"] == bug_id for e in snap.edges)

    # REAL purge + deterministic re-materialization of the Bug endpoint.
    _purge_and_rebootstrap(board_id)
    assert _count(board_id, "Learning") == 0, "purge must drop the cognitive node"
    _recreate_bug(board_id, bug_id, bug_attrs)

    restore = restore_canonical_cognitive(board_id, snap)
    summary = preservation_summary(snap, restore)
    assert summary["status"] == ccp.STATUS_CLEAN, summary
    assert restore.restored_nodes >= 1 and restore.restored_edges >= 1
    # The canonical Learning + its validates edge are back.
    assert _count(board_id, "Learning", layer=GRAPH_LAYER_CANONICAL) == 1
    assert _edge_count(board_id, "validates") == 1


def test_restore_is_idempotent():
    board_id = _new_board()
    ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    _lid, bug_id, bug_attrs = _seed_learning_with_canonical_bug(board_id, learning_ref=ref)
    snap = snapshot_canonical_cognitive(board_id)
    _purge_and_rebootstrap(board_id)
    _recreate_bug(board_id, bug_id, bug_attrs)
    restore_canonical_cognitive(board_id, snap)
    # Second restore over the already-restored graph adds nothing (idempotent).
    second = restore_canonical_cognitive(board_id, snap)
    assert second.restored_nodes == 0
    assert _count(board_id, "Learning") == 1


# ===========================================================================
# Degraded: endpoint not re-materialized -> edge unrestorable + traced + fallback
# ===========================================================================


def test_unrestorable_edge_is_degraded_and_fallback_recorded(_tmp_rebuild_dir):
    board_id = _new_board()
    ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    learning_id, bug_id, _bug_attrs = _seed_learning_with_canonical_bug(board_id, learning_ref=ref)
    snap = snapshot_canonical_cognitive(board_id)

    # Purge WITHOUT re-materializing the Bug endpoint -> the validates edge cannot
    # be restored. The Learning node is restored; the edge is unrestorable.
    _purge_and_rebootstrap(board_id)
    restore = restore_canonical_cognitive(board_id, snap)
    summary = preservation_summary(snap, restore)

    assert summary["status"] == ccp.STATUS_DEGRADED, summary
    assert _count(board_id, "Learning", layer=GRAPH_LAYER_CANONICAL) == 1  # node restored
    assert any(u["kind"] == "edge" for u in restore.unrestorable)

    # #2 visible fallback: the bug-derived Learning loss is recorded as an R7 hold...
    holds = ccp.record_cognitive_loss_fallback(board_id, summary, base_dir=_tmp_rebuild_dir)
    assert holds >= 1
    # ...and that hold is operator-visible via the cognitive readiness store (the
    # same source the R7 canonical_partition_integrity drilldown reads).
    from okto_pulse.core.kg.connectivity_guard import (
        CANONICAL_LEARNING_WORKING_ONLY_REASON,
    )
    from okto_pulse.core.kg.rebuild_audit import CognitiveConsolidationItemStore
    store = CognitiveConsolidationItemStore(base_dir=_tmp_rebuild_dir)
    gen = store.latest_generation(board_id)
    assert gen is not None
    items = store.list_items(board_id, gen)
    assert any(
        str(getattr(it, "reason_code", "") or "") == CANONICAL_LEARNING_WORKING_ONLY_REASON
        for it in items
    ), "fallback hold must be visible in the cognitive readiness store"


def test_report_field_exposes_non_learning_loss_fallback_is_learning_only(_tmp_rebuild_dir):
    """#1: the cognitive_preservation report field is TYPE-AGNOSTIC — it surfaces
    unrestorable Alternative/Assumption losses for operator visibility. The richer
    R7 sync hold path exists only for bug-derived Learning (a flagged gap), so
    Alt/Assumption stay visible via the degraded report field and are NOT silently
    held nor silently dropped."""
    snap = CognitiveSnapshot(
        board_id="b", readable=True,
        nodes=[{"node_type": "Alternative", "id": "a1", "attrs": {}}], edges=[],
    )
    restore = RestoreResult(unrestorable=[
        {"kind": "node", "node_type": "Alternative", "node_id": "a1",
         "source_artifact_ref": "spec:s:alternative:1",
         "reason": "node_create_failed:X"},
        {"kind": "edge", "edge_type": "derives_from", "from_id": "as1",
         "to_id": "gone", "from_node_type": "Assumption",
         "from_source_artifact_ref": "spec:s:assumption:1",
         "reason": "endpoint_absent_after_rebuild"},
    ])
    summary = preservation_summary(snap, restore)
    assert summary["status"] == ccp.STATUS_DEGRADED
    types = {u.get("node_type") or u.get("from_node_type") for u in summary["unrestorable"]}
    assert {"Alternative", "Assumption"} <= types  # both visible in the report field
    # The R7 hold fallback is Learning-only (documented gap) -> no silent holds here.
    assert ccp.record_cognitive_loss_fallback("b", summary, base_dir=_tmp_rebuild_dir) == 0


# ===========================================================================
# Unreadable pre-purge graph -> readable=False, never a silent snapshotted=0 pass
# ===========================================================================


def test_unreadable_graph_is_recorded_not_silently_passed(monkeypatch):
    board_id = _new_board()

    class _BrokenCypherExecutor:
        def execute_read_only(self, *_args, **_kwargs):
            raise RuntimeError("simulated corrupted graph")

    from okto_pulse.core.kg.interfaces import get_kg_registry

    monkeypatch.setattr(
        get_kg_registry(),
        "cypher_executor",
        _BrokenCypherExecutor(),
    )

    snap = snapshot_canonical_cognitive(board_id)
    assert snap.readable is False
    restore = restore_canonical_cognitive(board_id, snap)
    assert restore.unreadable is True
    assert preservation_summary(snap, restore)["status"] == ccp.STATUS_UNREADABLE


# ===========================================================================
# NEGATIVE / teeth (#5) — without restore, the cognitive node is genuinely lost
# ===========================================================================


def test_without_restore_cognitive_node_is_lost():
    board_id = _new_board()
    ref = f"card:bug:{uuid.uuid4().hex}:learning:{uuid.uuid4().hex}"
    _seed_learning_with_canonical_bug(board_id, learning_ref=ref)
    assert _count(board_id, "Learning", layer=GRAPH_LAYER_CANONICAL) == 1

    # A rebuild WITHOUT the preservation mechanism (no snapshot/restore) loses it.
    _purge_and_rebootstrap(board_id)
    assert _count(board_id, "Learning") == 0, (
        "teeth: without restore the canonical cognitive node must be GONE — if this "
        "ever shows 1, the preservation test would be theater"
    )
