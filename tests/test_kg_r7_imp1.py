"""R7 IMP1 — layer-aware canonical Learning connectivity guard + working-only hold.

Behavioral coverage for spec 7e0a5a28 / card fd7f112a:

- TS1  working-only Bug evidence HOLDS the canonical Learning before any graph
       mutation, with a structured ``r7_cognitive_hold_candidate`` payload.
- TS2  mixed evidence ACCEPTS the canonical Bug and DEFERS the working Bug edge
       (the working endpoint never counts toward canonical completeness).
- TS4  provenance-only Learning (NOT bug-derived) is unaffected by R7.
- TS10 the guard never FABRICATES a canonical Bug nor a ``validates`` edge to
       satisfy completeness on the working-only path.
- persistence the working-only go-forward hold lands in the existing
       CognitiveConsolidationItemStore (never CanonicalDebt / DLQ), both via the
       shared helper and via the MCP commit-tool dispatch hook.

The guard is the single layer-aware policy classifier; the async/MCP layer
persists the hold. Tests drive the real ``commit_consolidation`` path so the
verdict + diagnostics are exercised end to end (no source-inspection-only tests).
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.connectivity_guard import (
    CANONICAL_LEARNING_MIXED_DEFERRED_REASON,
    CANONICAL_LEARNING_WORKING_ONLY_REASON,
)
from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    _apply_graph_node_create,
    add_edge_candidate,
    add_node_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    record_cognitive_working_only_hold,
)
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    AddNodeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    EdgeCandidate,
    KGEdgeType,
    KGNodeType,
    NodeCandidate,
    ProposeReconciliationRequest,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    MATURITY_CANONICAL_ELIGIBLE,
    MATURITY_WORKING_IMMATURE,
)


@pytest.fixture
def board_id():
    """Per-test isolated board graph (overrides the shared conftest board) so
    accumulated nodes from one R7 test never leak the layer verdict of another."""
    from kg_schema_testing import bootstrap_board_graph

    bid = f"r7board-{uuid.uuid4().hex[:12]}"
    bootstrap_board_graph(bid)
    return bid


# ---------------------------------------------------------------------------
# Seed + drive helpers (self-contained; R7 stamps explicit graph_layer)
# ---------------------------------------------------------------------------


def _seed_node(
    kconn,
    orch,
    node_type: str,
    node_id: str,
    source_ref: str,
    *,
    graph_layer: str | None = None,
    maturity_status: str | None = None,
) -> None:
    attrs = {
        "title": f"R7 seed {node_type}",
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
    }
    if graph_layer is not None:
        attrs["graph_layer"] = graph_layer
    if maturity_status is not None:
        attrs["maturity_status"] = maturity_status
    _apply_graph_node_create(orch, node_type, node_id, attrs)


def _seed_bug(board_id: str, *, graph_layer: str) -> str:
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    bug_id = f"r7bug_{uuid.uuid4().hex[:12]}"
    maturity = (
        MATURITY_CANONICAL_ELIGIBLE
        if graph_layer == GRAPH_LAYER_CANONICAL
        else MATURITY_WORKING_IMMATURE
    )
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,

            session_id=f"r7seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(
            kconn,
            orch,
            "Bug",
            bug_id,
            f"bug:{bug_id}",
            graph_layer=graph_layer,
            maturity_status=maturity,
        )
    return bug_id


def _seed_connected_learning(board_id: str, source_ref: str) -> None:
    """Seed an EXISTING canonical Learning already connected to a root Entity by
    a belongs_to provenance edge.

    belongs_to is a deterministic-owned edge, so a cognitive agent cannot emit
    it via add_edge_candidate (Layer Ownership Isolation). The realistic shape
    for a provenance-only Learning is therefore an existing connected node that
    a later consolidation dedups onto."""
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    learning_id = f"r7learn_{uuid.uuid4().hex[:12]}"
    entity_id = f"r7ent_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,

            session_id=f"r7seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(
            kconn,
            orch,
            "Learning",
            learning_id,
            source_ref,
            graph_layer=GRAPH_LAYER_CANONICAL,
            maturity_status=MATURITY_CANONICAL_ELIGIBLE,
        )
        _seed_node(
            kconn,
            orch,
            "Entity",
            entity_id,
            f"entity:{entity_id}",
            graph_layer=GRAPH_LAYER_CANONICAL,
            maturity_status=MATURITY_CANONICAL_ELIGIBLE,
        )
        orch.create_edge(
            edge_type="belongs_to",
            from_id=learning_id,
            to_id=entity_id,
            attrs={"confidence": 1.0},
            from_type="Learning",
            to_type="Entity",
        )


def _count_nodes(board_id: str, node_type: str, source_ref: str) -> int:
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            f"MATCH (n:{node_type}) WHERE n.source_artifact_ref = $ref "
            "RETURN count(n)",
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


def _count_canonical_bugs(board_id: str) -> int:
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            "MATCH (b:Bug) WHERE b.graph_layer = $layer RETURN count(b)",
            {"layer": GRAPH_LAYER_CANONICAL},
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


def _count_validates(
    board_id: str, learning_source_ref: str, *, bug_id: str | None = None
) -> int:
    from kg_schema_testing import open_board_connection

    cypher = (
        "MATCH (n:Learning)-[r:validates]->(b:Bug) "
        "WHERE n.source_artifact_ref = $ref"
    )
    params = {"ref": learning_source_ref}
    if bug_id is not None:
        cypher += " AND b.id = $bug_id"
        params["bug_id"] = bug_id
    cypher += " RETURN count(r)"
    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(cypher, params)
        try:
            if res.has_next():
                return int(res.get_next()[0])
        finally:
            try:
                res.close()
            except Exception:
                pass
    return 0


async def _run_test_graph_io(operation, *, task_name: str):
    return await run_blocking_graph_io(
        operation,
        task_name=f"tests.r7_imp1.{task_name}",
    )


async def _begin_bug_derived_learning(
    board_id: str,
    agent_id: str,
    db_factory,
    *,
    source_ref: str,
    candidate_id: str,
    validates_bug_ids: list[str],
):
    """Open a session with a bug-derived canonical Learning candidate that
    validates each given Bug (explicit candidate edges)."""
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="bug",
                artifact_id=source_ref.split(":")[-1],
                raw_content=f"r7 imp1 {source_ref}",
            ),
            agent_id=agent_id,
            db=db,
        )
    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id=candidate_id,
                node_type=KGNodeType.LEARNING,
                title="R7 canonical learning",
                source_artifact_ref=source_ref,
                source_confidence=0.95,
            ),
        ),
        agent_id=agent_id,
    )
    for idx, bug_id in enumerate(validates_bug_ids):
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=EdgeCandidate(
                    candidate_id=f"{candidate_id}__validates_{idx}",
                    edge_type=KGEdgeType.VALIDATES,
                    from_candidate_id=candidate_id,
                    to_candidate_id=f"kg:{bug_id}",
                    confidence=0.92,
                    layer="cognitive",
                    rule_id="R7-IMP1.test.validates",
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


async def _attempt_working_only_commit(
    board_id: str, agent_id: str, db_factory, *, candidate_id: str
):
    """Seed a WORKING Bug, build a bug-derived canonical Learning that validates
    it, attempt commit, and return ``(error, source_ref, bug_id)``."""
    bug_id = await _run_test_graph_io(
        lambda: _seed_bug(board_id, graph_layer=GRAPH_LAYER_WORKING),
        task_name="seed-working-bug",
    )
    source_ref = f"card:bug:{bug_id}:learning:{uuid.uuid4()}"
    begin = await _begin_bug_derived_learning(
        board_id,
        agent_id,
        db_factory,
        source_ref=source_ref,
        candidate_id=candidate_id,
        validates_bug_ids=[bug_id],
    )
    with pytest.raises(KGPrimitiveError) as exc_info:
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=begin.session_id),
                agent_id=agent_id,
                db=db,
            )
    return exc_info.value, source_ref, bug_id


# ---------------------------------------------------------------------------
# TS1 — working-only Bug holds the canonical Learning before mutation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts1_working_only_bug_holds_learning_before_mutation(
    board_id, agent_id, db_factory
):
    error, source_ref, bug_id = await _attempt_working_only_commit(
        board_id, agent_id, db_factory, candidate_id="r7_ts1_learning"
    )

    # Structured verdict — rejected before any graph mutation.
    connectivity = error.details["connectivity"]
    assert connectivity["passed"] is False
    violation = connectivity["violations"][0]
    assert violation["reason"] == CANONICAL_LEARNING_WORKING_ONLY_REASON
    # The working Bug endpoint is reported (bounded "<ref>@<layer>") and the
    # raw learning/bug content never leaks into the diagnostic.
    assert violation["observed_endpoints"]
    assert "content" not in violation

    # Structured hold record with reason_code (the worker/MCP layer persists it).
    hold = error.details["r7_cognitive_hold_candidate"]
    assert hold["reason_code"] == CANONICAL_LEARNING_WORKING_ONLY_REASON
    assert hold["source_ref"] == source_ref
    assert hold["artifact_type"] == "bug"
    assert hold["node_type"] == "Learning"
    assert any(GRAPH_LAYER_WORKING in ep for ep in hold["observed_endpoints"])

    # No mutation: no canonical Learning node materialized.
    assert await _run_test_graph_io(
        lambda: _count_nodes(board_id, "Learning", source_ref),
        task_name="count-held-learning",
    ) == 0


# ---------------------------------------------------------------------------
# TS2 — mixed evidence accepts canonical Bug, defers working Bug edge
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts2_mixed_evidence_accepts_canonical_defers_working(
    board_id, agent_id, db_factory
):
    canonical_bug = await _run_test_graph_io(
        lambda: _seed_bug(board_id, graph_layer=GRAPH_LAYER_CANONICAL),
        task_name="seed-canonical-bug",
    )
    working_bug = await _run_test_graph_io(
        lambda: _seed_bug(board_id, graph_layer=GRAPH_LAYER_WORKING),
        task_name="seed-mixed-working-bug",
    )
    source_ref = f"card:bug:{canonical_bug}:learning:{uuid.uuid4()}"
    begin = await _begin_bug_derived_learning(
        board_id,
        agent_id,
        db_factory,
        source_ref=source_ref,
        candidate_id="r7_ts2_learning",
        validates_bug_ids=[canonical_bug, working_bug],
    )

    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id,
            db=db,
        )

    # Accepted because >=1 canonical Bug validates edge exists.
    assert commit.connectivity["passed"] is True
    assert await _run_test_graph_io(
        lambda: _count_nodes(board_id, "Learning", source_ref),
        task_name="count-mixed-learning",
    ) == 1
    # Completeness is satisfied by the CANONICAL Bug.
    assert await _run_test_graph_io(
        lambda: _count_validates(board_id, source_ref, bug_id=canonical_bug),
        task_name="count-canonical-validates",
    ) == 1
    # The working Bug edge is DEFERRED (surfaced as a non-blocking advisory),
    # never counted as canonical completeness.
    advisories = commit.connectivity.get("advisories", [])
    deferred = [
        a for a in advisories
        if a.get("reason") == CANONICAL_LEARNING_MIXED_DEFERRED_REASON
    ]
    assert deferred, f"expected mixed-deferred advisory, got {advisories!r}"
    assert any(
        working_bug in ep
        for a in deferred
        for ep in a.get("deferred_endpoints", [])
    )


# ---------------------------------------------------------------------------
# TS4 — provenance-only Learning (not bug-derived) is unaffected by R7
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts4_provenance_only_learning_not_bug_derived_passes(
    board_id, agent_id, db_factory
):
    source_ref = f"learning:provenance:{uuid.uuid4()}"  # NOT bug-derived
    await _run_test_graph_io(
        lambda: _seed_connected_learning(board_id, source_ref),
        task_name="seed-connected-learning",
    )

    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="bug",
                artifact_id=source_ref.split(":")[-1],
                raw_content=f"r7 imp1 provenance {source_ref}",
            ),
            agent_id=agent_id,
            db=db,
        )
    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="r7_ts4_learning",
                node_type=KGNodeType.LEARNING,
                # Keep identity stable so this R7 test does not also trigger the
                # MKG-D identity-change supersedence trail.
                title="R7 seed Learning",
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

    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id,
            db=db,
        )

    # R7 left the non-bug-derived provenance path untouched: the existing
    # belongs_to -> Entity satisfies completeness; no canonical Bug required.
    assert commit.connectivity["passed"] is True
    assert await _run_test_graph_io(
        lambda: _count_nodes(board_id, "Learning", source_ref),
        task_name="count-provenance-learning",
    ) == 1


# ---------------------------------------------------------------------------
# TS10 — the guard never fabricates a canonical Bug nor a validates edge
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ts10_working_only_path_fabricates_nothing(
    board_id, agent_id, db_factory
):
    canonical_bugs_before = await _run_test_graph_io(
        lambda: _count_canonical_bugs(board_id),
        task_name="count-canonical-bugs-before",
    )

    error, source_ref, bug_id = await _attempt_working_only_commit(
        board_id, agent_id, db_factory, candidate_id="r7_ts10_learning"
    )
    assert (
        error.details["r7_cognitive_hold_candidate"]["reason_code"]
        == CANONICAL_LEARNING_WORKING_ONLY_REASON
    )

    # No canonical Bug fabricated to satisfy the guard.
    assert await _run_test_graph_io(
        lambda: _count_canonical_bugs(board_id),
        task_name="count-canonical-bugs-after",
    ) == canonical_bugs_before
    # No validates edge fabricated from the (uncommitted) Learning.
    assert await _run_test_graph_io(
        lambda: _count_validates(board_id, source_ref),
        task_name="count-fabricated-validates",
    ) == 0
    # The seeded Bug stayed working — never promoted to canonical.
    assert await _run_test_graph_io(
        lambda: _count_canonical_bugs(board_id),
        task_name="count-canonical-bugs-final",
    ) == canonical_bugs_before


# ---------------------------------------------------------------------------
# Persistence — working-only hold lands in CognitiveConsolidationItemStore
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_working_only_hold_persists_real_payload_to_store(
    board_id, agent_id, db_factory, tmp_path
):
    error, source_ref, bug_id = await _attempt_working_only_commit(
        board_id, agent_id, db_factory, candidate_id="r7_persist_learning"
    )
    payload = error.details["r7_cognitive_hold_candidate"]

    result = record_cognitive_working_only_hold(
        board_id=board_id,
        hold_payload=payload,
        actor_id="claude-coder",
        base_dir=tmp_path,
    )
    assert result is not None

    store = CognitiveConsolidationItemStore(base_dir=tmp_path)
    items = store.list_items(board_id, result["generation_id"])
    assert len(items) == 1
    item = items[0]
    assert item.source_ref == source_ref
    assert item.status == "pending"
    assert item.reason_code == CANONICAL_LEARNING_WORKING_ONLY_REASON
    assert item.artifact_type == "bug"


def test_mcp_commit_dispatch_persists_hold(tmp_path):
    """The MCP commit tool's catch hook routes an R7 hold KGPrimitiveError into
    the cognitive pending ledger (never CanonicalDebt/DLQ)."""
    from okto_pulse.core.mcp import kg_tools
    from kg_registry_testing import configure_test_kg_registry
    from okto_pulse.community.adapters.rebuild_audit_storage import (
        CommunityFileSystemRebuildAuditArtifactStore,
    )

    artifact_store = CommunityFileSystemRebuildAuditArtifactStore(tmp_path)
    configure_test_kg_registry(rebuild_audit_artifact_store=artifact_store)
    source_ref = f"card:bug:{uuid.uuid4().hex[:10]}:learning:{uuid.uuid4()}"
    error = KGPrimitiveError(
        "kg_node_connectivity_violation",
        "rejected",
        session_id="sess-r7",
        details={
            "connectivity": {"passed": False},
            "r7_cognitive_hold_candidate": {
                "reason_code": CANONICAL_LEARNING_WORKING_ONLY_REASON,
                "node_type": "Learning",
                "candidate_id": "c1",
                "source_ref": source_ref,
                "artifact_type": "bug",
                "observed_endpoints": [f"kg:somebug@{GRAPH_LAYER_WORKING}"],
                "session_id": "sess-r7",
            },
        },
    )

    kg_tools._maybe_record_r7_cognitive_hold(
        board_id="board-r7", error=error, actor_id="claude-coder"
    )

    store = CognitiveConsolidationItemStore(artifact_store=artifact_store)
    gen = store.latest_generation("board-r7")
    assert gen is not None
    items = store.list_items("board-r7", gen)
    assert len(items) == 1
    assert items[0].reason_code == CANONICAL_LEARNING_WORKING_ONLY_REASON
    assert items[0].source_ref == source_ref
