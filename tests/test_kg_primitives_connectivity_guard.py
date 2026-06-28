from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    _apply_kuzu_node_create_with_timestamp,
    add_edge_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    EdgeCandidate,
    KGEdgeType,
    KGNodeType,
    NodeCandidate,
    ProposeReconciliationRequest,
)


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    from kg_registry_testing import (
        RealBoardCypherExecutorForTests,
        RealBoardGraphPathResolverForTests,
        RealBoardGraphTransactionForTests,
        configure_test_kg_registry,
    )

    configure_test_kg_registry(
        cypher_executor=RealBoardCypherExecutorForTests(),
        graph_transaction=RealBoardGraphTransactionForTests(),
        graph_path_resolver=RealBoardGraphPathResolverForTests(),
    )


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
        "title": f"Seed {node_type}",
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
    # R7: tests that exercise the layer-aware guard stamp an explicit
    # graph_layer/maturity_status; legacy seeds leave them unset (NULL).
    if graph_layer is not None:
        attrs["graph_layer"] = graph_layer
    if maturity_status is not None:
        attrs["maturity_status"] = maturity_status
    _apply_kuzu_node_create_with_timestamp(orch, node_type, node_id, attrs)


def _seed_learning_with_optional_parent(
    board_id: str,
    *,
    source_ref: str,
    connected: bool,
) -> str:
    from okto_pulse.core.kg.schema import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    learning_id = f"learning_seed_{uuid.uuid4().hex[:12]}"
    entity_id = f"entity_seed_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(kconn, orch, "Learning", learning_id, source_ref)
        if connected:
            _seed_node(kconn, orch, "Entity", entity_id, f"entity:{entity_id}")
            orch.create_edge(
                edge_type="belongs_to",
                from_id=learning_id,
                to_id=entity_id,
                attrs={"confidence": 1.0},
                from_type="Learning",
                to_type="Entity",
            )
    return learning_id


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


def _count_learning_belongs_to(board_id: str, source_ref: str) -> int:
    from okto_pulse.core.kg.schema import open_board_connection

    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            "MATCH (n:Learning)-[r:belongs_to]->(m:Entity) "
            "WHERE n.source_artifact_ref = $ref RETURN count(r)",
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


def _seed_bug_with_parent(board_id: str, *, graph_layer: str = "canonical") -> str:
    from okto_pulse.core.kg.schema import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    bug_id = f"bug_seed_{uuid.uuid4().hex[:12]}"
    entity_id = f"entity_seed_{uuid.uuid4().hex[:12]}"
    maturity = (
        "canonical_eligible" if graph_layer == "canonical" else "working_immature"
    )
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        # R7: a bug-derived canonical Learning only reaches completeness through
        # a CANONICAL Bug, so the seeded bug is canonical by default.
        _seed_node(
            kconn,
            orch,
            "Bug",
            bug_id,
            f"bug:{bug_id}",
            graph_layer=graph_layer,
            maturity_status=maturity,
        )
        _seed_node(kconn, orch, "Entity", entity_id, f"entity:{entity_id}")
        orch.create_edge(
            edge_type="belongs_to",
            from_id=bug_id,
            to_id=entity_id,
            attrs={"confidence": 1.0},
            from_type="Bug",
            to_type="Entity",
        )
    return bug_id


def _seed_spec_root_and_decision(board_id: str, spec_ref: str) -> tuple[str, str]:
    from okto_pulse.core.kg.schema import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    root_id = f"entity_seed_{uuid.uuid4().hex[:12]}"
    decision_id = f"decision_seed_{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(kconn, orch, "Entity", root_id, spec_ref)
        _seed_node(kconn, orch, "Decision", decision_id, f"{spec_ref}:decision:seed")
        orch.create_edge(
            edge_type="belongs_to",
            from_id=decision_id,
            to_id=root_id,
            attrs={"confidence": 1.0},
            from_type="Decision",
            to_type="Entity",
        )
    return root_id, decision_id


def _count_decision_belongs_to_root(
    board_id: str,
    *,
    decision_source_ref: str,
    root_id: str,
) -> int:
    from okto_pulse.core.kg.schema import open_board_connection

    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            "MATCH (n:Decision)-[r:belongs_to]->(m:Entity) "
            "WHERE n.source_artifact_ref = $ref AND m.id = $root_id "
            "RETURN count(r)",
            {"ref": decision_source_ref, "root_id": root_id},
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


def _count_assumption_belongs_to_root(
    board_id: str,
    *,
    assumption_source_ref: str,
    root_id: str,
) -> int:
    from okto_pulse.core.kg.schema import open_board_connection

    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            "MATCH (n:Assumption)-[r:belongs_to]->(m:Entity) "
            "WHERE n.source_artifact_ref = $ref AND m.id = $root_id "
            "RETURN count(r)",
            {"ref": assumption_source_ref, "root_id": root_id},
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


def _count_learning_validates_bug(
    board_id: str,
    *,
    learning_source_ref: str,
    bug_id: str,
) -> int:
    from okto_pulse.core.kg.schema import open_board_connection

    with open_board_connection(board_id) as (_db, kconn):
        res = kconn.execute(
            "MATCH (n:Learning)-[r:validates]->(b:Bug) "
            "WHERE n.source_artifact_ref = $ref AND b.id = $bug_id "
            "RETURN count(r)",
            {"ref": learning_source_ref, "bug_id": bug_id},
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


async def _begin_with_learning(
    board_id: str,
    agent_id: str,
    db_factory,
    *,
    source_ref: str,
    candidate_id: str,
):
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="bug",
                artifact_id=source_ref.split(":", 1)[-1],
                raw_content=f"connectivity guard {source_ref}",
            ),
            agent_id=agent_id,
            db=db,
        )
    from okto_pulse.core.kg.primitives import add_node_candidate
    from okto_pulse.core.kg.schemas import AddNodeCandidateRequest

    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id=candidate_id,
                node_type=KGNodeType.LEARNING,
                title="Connectivity learning",
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
async def test_commit_rejects_isolated_learning_before_graph_mutation(
    board_id,
    agent_id,
    db_factory,
    board_handle,
):
    source_ref = f"learning:isolated:{uuid.uuid4()}"
    begin = await _begin_with_learning(
        board_id,
        agent_id,
        db_factory,
        source_ref=source_ref,
        candidate_id="learning_isolated",
    )

    with pytest.raises(KGPrimitiveError) as exc_info:
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=begin.session_id),
                agent_id=agent_id,
                db=db,
            )

    assert exc_info.value.code == "kg_node_connectivity_violation"
    details = exc_info.value.details["connectivity"]
    assert details["passed"] is False
    assert details["violations"][0]["source_artifact_ref"] == source_ref
    assert "content" not in details["violations"][0]
    assert _count_by_source_ref(board_id, "Learning", source_ref) == 0


@pytest.mark.asyncio
async def test_source_artifact_ref_dedup_hit_without_edge_is_not_successful_merge(
    board_id,
    agent_id,
    db_factory,
    board_handle,
):
    source_ref = f"learning:orphan-dedup:{uuid.uuid4()}"
    _seed_learning_with_optional_parent(board_id, source_ref=source_ref, connected=False)
    assert _count_by_source_ref(board_id, "Learning", source_ref) == 1

    begin = await _begin_with_learning(
        board_id,
        agent_id,
        db_factory,
        source_ref=source_ref,
        candidate_id="learning_dedup_orphan",
    )

    with pytest.raises(KGPrimitiveError) as exc_info:
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=begin.session_id),
                agent_id=agent_id,
                db=db,
            )

    assert exc_info.value.code == "kg_node_connectivity_violation"
    assert exc_info.value.details["connectivity"]["checked_nodes"] == 1
    assert _count_by_source_ref(board_id, "Learning", source_ref) == 1


@pytest.mark.asyncio
async def test_source_artifact_ref_dedup_hit_with_existing_edge_counts_merge(
    board_id,
    agent_id,
    db_factory,
    board_handle,
):
    source_ref = f"learning:connected-dedup:{uuid.uuid4()}"
    _seed_learning_with_optional_parent(board_id, source_ref=source_ref, connected=True)
    assert _count_learning_belongs_to(board_id, source_ref) == 1

    begin = await _begin_with_learning(
        board_id,
        agent_id,
        db_factory,
        source_ref=source_ref,
        candidate_id="learning_dedup_connected",
    )

    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id,
            db=db,
        )

    assert commit.nodes_added == 0
    assert commit.nodes_merged == 1
    assert commit.connectivity["passed"] is True
    assert commit.processed_candidates == 1
    assert _count_by_source_ref(board_id, "Learning", source_ref) == 1


@pytest.mark.asyncio
async def test_degraded_graph_returns_contextual_error_without_opening_kuzu(
    board_id,
    agent_id,
    db_factory,
    board_handle,
    monkeypatch,
):
    source_ref = f"learning:degraded:{uuid.uuid4()}"
    begin = await _begin_with_learning(
        board_id,
        agent_id,
        db_factory,
        source_ref=source_ref,
        candidate_id="learning_degraded",
    )

    async def fake_get_kg_health(_board_id, _db):
        return {"graph_state": "recovery_needed", "overall_state": "recovery_needed"}

    def forbidden_open(_board_id):
        raise AssertionError("degraded commit must not open LadybugDB")

    import okto_pulse.core.services.kg_health_service as health_service
    import okto_pulse.core.kg.schema as kg_schema

    original_open = kg_schema.open_board_connection
    monkeypatch.setattr(health_service, "get_kg_health", fake_get_kg_health)
    monkeypatch.setattr(kg_schema, "open_board_connection", forbidden_open)

    with pytest.raises(KGPrimitiveError) as exc_info:
        async with db_factory() as db:
            await commit_consolidation(
                CommitConsolidationRequest(session_id=begin.session_id),
                agent_id=agent_id,
                db=db,
            )

    assert exc_info.value.code == "kg_graph_degraded"
    details = exc_info.value.details
    assert details["kg_health_state"] == "recovery_needed"
    connectivity = details["connectivity"]
    assert connectivity["outcome"] == "deferred"
    assert connectivity["checked_nodes"] == 1
    assert connectivity["violations"][0]["source_resolution_status"] == (
        "deferred_degraded_graph"
    )
    monkeypatch.setattr(kg_schema, "open_board_connection", original_open)
    assert _count_by_source_ref(board_id, "Learning", source_ref) == 0


@pytest.mark.asyncio
async def test_bug_derived_learning_validates_existing_bug_is_connected(
    board_id,
    agent_id,
    db_factory,
    board_handle,
):
    bug_id = _seed_bug_with_parent(board_id)
    source_ref = f"card:bug:{bug_id}:learning:{uuid.uuid4()}"
    begin = await _begin_with_learning(
        board_id,
        agent_id,
        db_factory,
        source_ref=source_ref,
        candidate_id="learning_bug_validates",
    )

    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id="learning_validates_bug",
                edge_type=KGEdgeType.VALIDATES,
                from_candidate_id="learning_bug_validates",
                to_candidate_id=f"kg:{bug_id}",
                confidence=0.92,
                layer="cognitive",
                rule_id="KG-ZO-01.4.learning_bug_validates",
            ),
        ),
        agent_id=agent_id,
    )

    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id,
            db=db,
        )

    assert commit.connectivity["passed"] is True
    assert commit.nodes_added == 1
    assert commit.edges_added == 1
    assert _count_learning_validates_bug(
        board_id,
        learning_source_ref=source_ref,
        bug_id=bug_id,
    ) == 1


@pytest.mark.asyncio
async def test_commit_auto_attaches_cognitive_decision_to_source_root(
    board_id,
    agent_id,
    db_factory,
    board_handle,
):
    spec_id = f"spec-{uuid.uuid4()}"
    spec_ref = f"spec:{spec_id}"
    root_id, existing_decision_id = _seed_spec_root_and_decision(board_id, spec_ref)
    decision_source_ref = f"{spec_ref}:decision:e2e"

    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec_id,
                raw_content="decision auto provenance",
            ),
            agent_id=agent_id,
            db=db,
        )

    from okto_pulse.core.kg.primitives import add_node_candidate
    from okto_pulse.core.kg.schemas import AddNodeCandidateRequest

    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="decision_auto_root",
                node_type=KGNodeType.DECISION,
                title="Auto root decision",
                source_artifact_ref=decision_source_ref,
                source_confidence=0.9,
            ),
        ),
        agent_id=agent_id,
    )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id="decision_depends_on_existing",
                edge_type=KGEdgeType.DEPENDS_ON,
                from_candidate_id="decision_auto_root",
                to_candidate_id=f"kg:{existing_decision_id}",
                confidence=0.8,
                layer="cognitive",
                rule_id="test/depends_on_existing",
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

    assert commit.connectivity["passed"] is True
    assert commit.nodes_added == 1
    assert commit.edges_added == 2
    assert _count_decision_belongs_to_root(
        board_id,
        decision_source_ref=decision_source_ref,
        root_id=root_id,
    ) == 1


@pytest.mark.asyncio
async def test_commit_auto_attaches_cognitive_assumption_to_source_root(
    board_id,
    agent_id,
    db_factory,
    board_handle,
):
    spec_id = f"spec-{uuid.uuid4()}"
    spec_ref = f"spec:{spec_id}"
    root_id, _existing_decision_id = _seed_spec_root_and_decision(board_id, spec_ref)
    assumption_source_ref = f"{spec_ref}:assumption:e2e"

    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id=spec_id,
                raw_content="assumption auto provenance",
            ),
            agent_id=agent_id,
            db=db,
        )

    from okto_pulse.core.kg.primitives import add_node_candidate
    from okto_pulse.core.kg.schemas import AddNodeCandidateRequest

    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="assumption_auto_root",
                node_type=KGNodeType.ASSUMPTION,
                title="Auto root assumption",
                source_artifact_ref=assumption_source_ref,
                source_confidence=0.9,
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

    assert commit.connectivity["passed"] is True
    assert commit.nodes_added == 1
    assert commit.edges_added == 1
    assert _count_assumption_belongs_to_root(
        board_id,
        assumption_source_ref=assumption_source_ref,
        root_id=root_id,
    ) == 1
