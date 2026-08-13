from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.application.processors.consolidation import (
    ConsolidationProcessor,
    _commit_consolidation_with_board_graph_lifecycle,
    _worker_edge_to_candidate,
    _worker_node_to_candidate,
)
from okto_pulse.core.application.processors.deterministic_kg import (
    DeterministicWorker,
)
from okto_pulse.core.kg import primitives
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
from okto_pulse.core.kg.interfaces.graph_transaction import (
    ProjectionActiveSetIntent,
    ProjectionEdgeRef,
)
from okto_pulse.core.kg.providers.testing.memory_graph_store import (
    InMemoryGraphLifecycle,
    InMemoryGraphRuntimeStore,
    InMemoryGraphSchemaManager,
    InMemoryGraphStore,
    _InMemoryGraphTransactionScope,
)
from okto_pulse.core.kg.providers.testing.embedding import (
    TestingStubEmbeddingProvider,
)
from okto_pulse.core.kg.schema_contract import relationship_endpoint_pairs
from okto_pulse.core.kg.schemas import (
    KGNodeType,
    NodeCandidate,
    ReconciliationHint,
    ReconciliationOperation,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator
from okto_pulse.core.ports.consolidation import ConsolidationQueueRecord


BOARD_ID = "board-skm"
DEPENDENT_ID = "aaaaaaaa-0000-0000-0000-000000000000"
PREREQUISITE_ID = "bbbbbbbb-0000-0000-0000-000000000000"
DEPENDENCY_ID = "dep-skm-1"


class _QueryableMemoryScope(_InMemoryGraphTransactionScope):
    """Tiny source-ref query surface for exercising the real commit primitive."""

    def execute(
        self,
        cypher: str,
        params: dict[str, object] | None = None,
    ) -> GraphStatementResult:
        self.statements.append((cypher, params))
        params = params or {}
        match = re.search(r"MATCH \(n:([A-Za-z_][A-Za-z0-9_]*)", cypher)
        if match is None:
            return GraphStatementResult()
        node_type = match.group(1)
        nodes = [
            node
            for node in self.store._board_nodes(self.board_id).values()
            if node.get("_type") == node_type
        ]
        node_id = params.get("id") or params.get("node_id")
        if node_id is not None:
            nodes = [node for node in nodes if node.get("id") == node_id]
        source_ref = params.get("ref")
        if source_ref is not None:
            nodes = [
                node for node in nodes if node.get("source_artifact_ref") == source_ref
            ]
        if "n.superseded_by IS NULL" in cypher:
            nodes = [node for node in nodes if not node.get("superseded_by")]
        nodes.sort(
            key=lambda node: (int(node.get("generation") or 0), str(node["id"])),
            reverse=True,
        )
        if "RETURN n.id, n.source_artifact_ref" in cypher:
            rows = [(node["id"], node.get("source_artifact_ref")) for node in nodes[:1]]
        elif "RETURN n.title, n.content, n.context, n.justification" in cypher:
            rows = [
                (
                    node.get("title"),
                    node.get("content"),
                    node.get("context"),
                    node.get("justification"),
                )
                for node in nodes[:1]
            ]
        elif "RETURN n.created_by_agent" in cypher:
            rows = [(node.get("created_by_agent"),) for node in nodes[:1]]
        elif "RETURN n.title" in cypher:
            rows = [(node.get("title"),) for node in nodes[:1]]
        elif "RETURN n.generation" in cypher:
            rows = [(node.get("generation"),) for node in nodes[:1]]
        elif "RETURN n.superseded_by" in cypher:
            rows = [(node.get("superseded_by"),) for node in nodes[:1]]
        elif "RETURN n.id" in cypher:
            limit = 2 if "LIMIT 2" in cypher else 1
            rows = [(node["id"],) for node in nodes[:limit]]
        else:
            rows = []
        return GraphStatementResult.from_rows(rows)


class _QueryableMemoryTransaction:
    def __init__(self, store: InMemoryGraphStore) -> None:
        self.store = store

    async def begin(self, board_id: str) -> _QueryableMemoryScope:
        return _QueryableMemoryScope(board_id, self.store)


class _FailingIdentityLookupScope(_QueryableMemoryScope):
    def execute(
        self,
        cypher: str,
        params: dict[str, object] | None = None,
    ) -> GraphStatementResult:
        if "RETURN n.id, n.source_artifact_ref" in cypher:
            raise RuntimeError("identity lookup unavailable")
        return super().execute(cypher, params)


class _FailingIdentityLookupTransaction:
    def __init__(self, store: InMemoryGraphStore) -> None:
        self.store = store

    async def begin(self, board_id: str) -> _FailingIdentityLookupScope:
        return _FailingIdentityLookupScope(board_id, self.store)


def _spec(*, dependencies: list[dict[str, object]]) -> dict[str, object]:
    return {
        "id": DEPENDENT_ID,
        "board_id": BOARD_ID,
        "title": "Dependent Spec",
        "description": "",
        "context": "",
        "spec_dependencies": dependencies,
    }


def _commit_worker_result(
    monkeypatch: pytest.MonkeyPatch,
    store: InMemoryGraphStore,
    result: object,
    *,
    artifact_id: str,
    session_id: str,
) -> tuple[dict[str, str], object, list[object], object, dict, list[dict]]:
    transaction = _QueryableMemoryTransaction(store)
    monkeypatch.setattr(
        primitives,
        "get_kg_registry",
        lambda: SimpleNamespace(graph_transaction=transaction),
    )
    node_candidates = {
        node.candidate_id: node
        for node in (_worker_node_to_candidate(item) for item in result.nodes)
    }
    edge_candidates = {
        edge.candidate_id: edge
        for edge in (_worker_edge_to_candidate(item) for item in result.edges)
    }
    return primitives._do_graph_commit(
        BOARD_ID,
        session_id,
        node_candidates,
        edge_candidates,
        {},
        "system:historical_consolidation",
        TestingStubEmbeddingProvider(dim=8),
        "healthy",
        result.content_hash,
        artifact_id,
        frozenset(),
        "spec",
        result.spec_lineage_parent_intent,
        frozenset(result.relational_projection_candidate_ids),
        result.relational_projection_active_set_intent,
    )


def test_worker_projects_distinct_operational_precedence_relation() -> None:
    result = DeterministicWorker().process_spec(
        _spec(
            dependencies=[
                {
                    "dependency_id": DEPENDENCY_ID,
                    "dependent_spec_id": DEPENDENT_ID,
                    "prerequisite_spec_id": PREREQUISITE_ID,
                    "prerequisite_title": "Prerequisite Spec",
                    "prerequisite_status": "done",
                    "prerequisite_version": 7,
                }
            ]
        )
    )

    precedence = [edge for edge in result.edges if edge.edge_type == "precedes"]
    assert len(precedence) == 1
    assert precedence[0].from_candidate_id == (f"kgref:Entity:spec:{PREREQUISITE_ID}")
    assert precedence[0].to_candidate_id == "spec_aaaaaaaa_entity"
    assert precedence[0].rule_id.startswith("precedes/spec_dependency/")
    assert relationship_endpoint_pairs("precedes") == (("Entity", "Entity"),)
    assert relationship_endpoint_pairs("depends_on") == (("Decision", "Decision"),)

    intent = result.relational_projection_active_set_intent
    assert intent is not None
    assert intent.owner_type == "spec"
    assert intent.owner_id == DEPENDENT_ID
    assert intent.namespace == "dependencies"
    assert intent.active_refs == ()
    assert len(intent.active_edges) == 1
    assert not any(
        node.source_artifact_ref == f"spec:{PREREQUISITE_ID}" for node in result.nodes
    )


def test_dependent_projection_never_overwrites_canonical_prerequisite_root() -> None:
    store = InMemoryGraphStore()
    prerequisite_before = {
        "source_artifact_ref": f"spec:{PREREQUISITE_ID}",
        "title": "Canonical prerequisite title",
        "content": "Canonical prerequisite content",
        "context": "Canonical prerequisite context",
        "generation": 4,
        "created_by_agent": "canonical-worker",
    }
    store.create_node(
        BOARD_ID,
        "Entity",
        "canonical-prerequisite-node",
        prerequisite_before,
    )
    result = DeterministicWorker().process_spec(
        _spec(
            dependencies=[
                {
                    "dependency_id": DEPENDENCY_ID,
                    "dependent_spec_id": DEPENDENT_ID,
                    "prerequisite_spec_id": PREREQUISITE_ID,
                    # A stale dependent snapshot must never be emitted as the
                    # prerequisite's canonical root payload.
                    "prerequisite_title": "Stale snapshot title",
                    "prerequisite_status": "done",
                    "prerequisite_version": 1,
                }
            ]
        )
    )
    for node in result.nodes:
        store.create_node(
            BOARD_ID,
            str(getattr(node.node_type, "value", node.node_type)),
            node.candidate_id,
            {
                "source_artifact_ref": node.source_artifact_ref,
                "title": node.title,
                "content": node.content,
                "context": node.context,
            },
        )

    prerequisite_after = store._board_nodes(BOARD_ID)["canonical-prerequisite-node"]
    assert {
        key: prerequisite_after[key] for key in prerequisite_before
    } == prerequisite_before
    assert not any(
        node.source_artifact_ref == f"spec:{PREREQUISITE_ID}" for node in result.nodes
    )


def test_active_set_removes_tombstoned_edge_and_compensates_exactly() -> None:
    store = InMemoryGraphStore()
    dependent_node_id = "dependent-node"
    prerequisite_node_id = "prerequisite-node"
    store.create_node(
        BOARD_ID,
        "Entity",
        dependent_node_id,
        {"source_artifact_ref": f"spec:{DEPENDENT_ID}"},
    )
    store.create_node(
        BOARD_ID,
        "Entity",
        prerequisite_node_id,
        {"source_artifact_ref": f"spec:{PREREQUISITE_ID}"},
    )
    attrs = {
        "confidence": 1.0,
        "created_by_session_id": "prior-session",
        "layer": "deterministic",
        "rule_id": f"precedes/spec_dependency/{DEPENDENCY_ID}@v2.0",
        "created_by": "worker_layer1",
        "fallback_reason": "",
    }
    store.create_edge(
        BOARD_ID,
        "precedes",
        prerequisite_node_id,
        dependent_node_id,
        attrs,
        from_type="Entity",
        to_type="Entity",
    )
    scope = _InMemoryGraphTransactionScope(BOARD_ID, store)

    active = ProjectionActiveSetIntent(
        owner_type="spec",
        owner_id=DEPENDENT_ID,
        namespace="dependencies",
        owner_node_id=dependent_node_id,
        active_edges=(
            ProjectionEdgeRef(
                edge_type="precedes",
                from_type="Entity",
                to_type="Entity",
                from_id=prerequisite_node_id,
                to_id=dependent_node_id,
                rule_id=attrs["rule_id"],
            ),
        ),
    )
    assert scope.reconcile_projection_active_set(active).edge_before_images == ()
    assert len(store._board_edges(BOARD_ID)) == 1

    removed = scope.reconcile_projection_active_set(
        ProjectionActiveSetIntent(
            owner_type="spec",
            owner_id=DEPENDENT_ID,
            namespace="dependencies",
            owner_node_id=dependent_node_id,
        )
    )
    assert len(removed.edge_before_images) == 1
    assert store._board_edges(BOARD_ID) == []

    scope.compensate_projection_active_set(removed)
    assert len(store._board_edges(BOARD_ID)) == 1
    assert store._board_edges(BOARD_ID)[0]["rule_id"] == attrs["rule_id"]


def test_empty_authoritative_set_emits_cleanup_intent() -> None:
    result = DeterministicWorker().process_spec(_spec(dependencies=[]))
    intent = result.relational_projection_active_set_intent
    assert intent is not None
    assert intent.active_edges == ()
    assert not [edge for edge in result.edges if edge.edge_type == "precedes"]


@pytest.mark.asyncio
async def test_remove_readd_same_endpoints_replaces_stale_rule_exactly() -> None:
    store = InMemoryGraphStore()
    store.create_node(
        BOARD_ID,
        "Entity",
        "dependent-node",
        {"source_artifact_ref": f"spec:{DEPENDENT_ID}"},
    )
    store.create_node(
        BOARD_ID,
        "Entity",
        "prerequisite-node",
        {"source_artifact_ref": f"spec:{PREREQUISITE_ID}"},
    )
    old_rule = "precedes/spec_dependency/dep-old@v2.0"
    new_rule = "precedes/spec_dependency/dep-new@v2.0"
    store.create_edge(
        BOARD_ID,
        "precedes",
        "prerequisite-node",
        "dependent-node",
        {
            "confidence": 1.0,
            "created_by_session_id": "old-session",
            "layer": "deterministic",
            "rule_id": old_rule,
            "created_by": "worker_layer1",
            "fallback_reason": "",
        },
        from_type="Entity",
        to_type="Entity",
    )
    orchestrator = TransactionOrchestrator(
        _InMemoryGraphTransactionScope(BOARD_ID, store),
        "new-session",
        BOARD_ID,
    )
    orchestrator.create_edge(
        "precedes",
        "prerequisite-node",
        "dependent-node",
        attrs={
            "confidence": 1.0,
            "layer": "deterministic",
            "rule_id": new_rule,
            "created_by": "worker_layer1",
            "fallback_reason": "",
        },
        from_type="Entity",
        to_type="Entity",
    )
    orchestrator.reconcile_projection_active_set(
        ProjectionActiveSetIntent(
            owner_type="spec",
            owner_id=DEPENDENT_ID,
            namespace="dependencies",
            owner_node_id="dependent-node",
            active_edges=(
                ProjectionEdgeRef(
                    edge_type="precedes",
                    from_type="Entity",
                    to_type="Entity",
                    from_id="prerequisite-node",
                    to_id="dependent-node",
                    rule_id=new_rule,
                ),
            ),
        )
    )

    edges = store._board_edges(BOARD_ID)
    assert len(edges) == 1
    assert edges[0]["rule_id"] == new_rule

    # Compensation is also exact: restore the stale before-image and remove
    # only the edge created by this consolidation session.
    await orchestrator.compensate()
    edges = store._board_edges(BOARD_ID)
    assert len(edges) == 1
    assert edges[0]["rule_id"] == old_rule


def test_commit_defers_before_writes_until_prerequisite_root_is_consolidated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryGraphStore()
    dependent_result = DeterministicWorker().process_spec(
        {
            **_spec(
                dependencies=[
                    {
                        "dependency_id": DEPENDENCY_ID,
                        "dependent_spec_id": DEPENDENT_ID,
                        "prerequisite_spec_id": PREREQUISITE_ID,
                        "prerequisite_title": "Prerequisite Spec",
                        "prerequisite_status": "draft",
                        "prerequisite_version": 1,
                    }
                ]
            ),
            "status": "draft",
        }
    )

    prerequisite_ref = f"spec:{PREREQUISITE_ID}"
    prerequisite_endpoint = f"kgref:Entity:{prerequisite_ref}"

    def forbid_compensation(*_: object, **__: object) -> None:
        raise AssertionError("read-only endpoint preflight must not compensate")

    monkeypatch.setattr(primitives, "_compensate_graph_writes", forbid_compensation)
    with pytest.raises(primitives.KGPrimitiveError) as caught:
        _commit_worker_result(
            monkeypatch,
            store,
            dependent_result,
            artifact_id=DEPENDENT_ID,
            session_id="session-dependent-first",
        )
    assert caught.value.code == "relational_projection_endpoint_pending"
    assert caught.value.details == {
        "edge_candidate_id": f"spec_aaaaaaaa_precedes_{DEPENDENCY_ID}",
        "source_artifact_ref": prerequisite_ref,
    }
    # The owner, board root and PRECEDES relation all remain absent: the retry
    # barrier fired before the first graph mutation, so no graph-ahead residue
    # or second writer scope was needed for compensation.
    assert store._board_nodes(BOARD_ID) == {}
    assert store._board_edges(BOARD_ID) == []

    # Materialize the prerequisite through its own ordinary consolidation.
    prerequisite_result = DeterministicWorker().process_spec(
        {
            "id": PREREQUISITE_ID,
            "board_id": BOARD_ID,
            "title": "Prerequisite Spec",
            "description": "Prerequisite content",
            "context": "Prerequisite context",
            "status": "draft",
            "spec_dependencies": [],
        }
    )
    prerequisite_map, *_ = _commit_worker_result(
        monkeypatch,
        store,
        prerequisite_result,
        artifact_id=PREREQUISITE_ID,
        session_id="session-prerequisite",
    )

    prerequisite_nodes = [
        node
        for node in store._board_nodes(BOARD_ID).values()
        if node.get("source_artifact_ref") == prerequisite_ref
        and not node.get("superseded_by")
    ]
    assert len(prerequisite_nodes) == 1
    prerequisite_node_id = prerequisite_map["spec_bbbbbbbb_entity"]
    assert prerequisite_nodes[0]["id"] == prerequisite_node_id

    # Retry the unchanged dependent projection. The exact active set now
    # resolves and commits one relation to the ordinary prerequisite root.
    dependent_map, *_ = _commit_worker_result(
        monkeypatch,
        store,
        dependent_result,
        artifact_id=DEPENDENT_ID,
        session_id="session-dependent-retry",
    )
    assert dependent_map[prerequisite_endpoint] == prerequisite_node_id
    precedence = [
        edge
        for edge in store._board_edges(BOARD_ID)
        if edge.get("_type") == "precedes"
        and edge.get("rule_id", "").startswith(
            f"precedes/spec_dependency/{DEPENDENCY_ID}@"
        )
    ]
    assert len(precedence) == 1
    assert precedence[0]["_from"] == prerequisite_node_id
    assert precedence[0]["_to"] == dependent_map["spec_aaaaaaaa_entity"]


@pytest.mark.asyncio
async def test_ac20_core_rebuild_replays_precedence_and_erasure_removes_board(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove the Core half of rebuild + board-erasure parity.

    Hydrating the authoritative relational Spec payload is an edition adapter
    responsibility. Starting at that port boundary, Core must be able to
    rebuild the derived PRECEDES edge and its strict erasure workflow must
    remove the complete board graph without affecting a surviving board.
    """

    from okto_pulse.core.infra import storage as storage_module
    from okto_pulse.core.kg import governance, interfaces as kg_interfaces
    from okto_pulse.core.kg.global_discovery import clustering

    store = InMemoryGraphStore()
    schema = InMemoryGraphSchemaManager(store)
    lifecycle = InMemoryGraphLifecycle(schema)
    runtime = InMemoryGraphRuntimeStore(store, schema)

    # A rebuild starts from relational truth, not from the previous derived
    # graph. Prove the old graph is gone before replaying the source payload.
    store.bootstrap(BOARD_ID)
    store.create_node(
        BOARD_ID,
        "Entity",
        "stale-before-rebuild",
        {"source_artifact_ref": "spec:stale"},
    )
    reset = runtime.purge_board_graph(BOARD_ID, reason="explicit_rebuild:test")
    assert reset.status == "purged"
    rebuild = await lifecycle.rebuild(BOARD_ID)
    assert rebuild.status == "rebuilt"
    assert "stale-before-rebuild" not in store._board_nodes(BOARD_ID)

    prerequisite_result = DeterministicWorker().process_spec(
        {
            "id": PREREQUISITE_ID,
            "board_id": BOARD_ID,
            "title": "Prerequisite Spec",
            "description": "Prerequisite content",
            "context": "Prerequisite context",
            "status": "done",
            "spec_dependencies": [],
        }
    )
    prerequisite_map, *_ = _commit_worker_result(
        monkeypatch,
        store,
        prerequisite_result,
        artifact_id=PREREQUISITE_ID,
        session_id="ac20-rebuild-prerequisite",
    )
    dependent_result = DeterministicWorker().process_spec(
        _spec(
            dependencies=[
                {
                    "dependency_id": DEPENDENCY_ID,
                    "dependent_spec_id": DEPENDENT_ID,
                    "prerequisite_spec_id": PREREQUISITE_ID,
                    "prerequisite_title": "Prerequisite Spec",
                    "prerequisite_status": "done",
                    "prerequisite_version": 4,
                }
            ]
        )
    )
    dependent_map, *_ = _commit_worker_result(
        monkeypatch,
        store,
        dependent_result,
        artifact_id=DEPENDENT_ID,
        session_id="ac20-rebuild-dependent",
    )

    precedence = [
        edge
        for edge in store._board_edges(BOARD_ID)
        if edge.get("_type") == "precedes"
        and edge.get("rule_id", "").startswith(
            f"precedes/spec_dependency/{DEPENDENCY_ID}@"
        )
    ]
    assert len(precedence) == 1
    assert precedence[0]["_from"] == prerequisite_map["spec_bbbbbbbb_entity"]
    assert precedence[0]["_to"] == dependent_map["spec_aaaaaaaa_entity"]

    survivor_board_id = "board-skm-survivor"
    store.bootstrap(survivor_board_id)
    store.create_node(
        survivor_board_id,
        "Entity",
        "survivor-node",
        {"source_artifact_ref": "spec:survivor"},
    )
    erased_capabilities: list[str] = []

    class _GlobalRuntime:
        def erase_storage_for_privacy(
            self,
            *,
            board_id: str,
            reason: str,
            survivor_board_ids: tuple[str, ...],
        ) -> dict[str, object]:
            assert reason == "board_right_to_erasure"
            assert survivor_board_ids == (survivor_board_id,)
            erased_capabilities.append("global")
            return {
                "board_id": board_id,
                "status": "purged",
                "verified_absent": True,
            }

    class _Artifacts:
        def purge_board_artifacts(self, board_id: str) -> dict[str, object]:
            erased_capabilities.append("artifacts")
            return {
                "board_id": board_id,
                "status": "purged",
                "verified_absent": True,
            }

    class _Attachments:
        async def purge_board(self, board_id: str) -> dict[str, object]:
            erased_capabilities.append("attachments")
            return {
                "board_id": board_id,
                "status": "purged",
                "verified_absent": True,
            }

    async def _survivors(
        _db: object,
        *,
        erased_board_id: str,
    ) -> tuple[str, ...]:
        assert erased_board_id == BOARD_ID
        return (survivor_board_id,)

    def _cascade(board_id: str, **kwargs: object) -> dict[str, object]:
        assert board_id == BOARD_ID
        assert kwargs == {
            "strict": True,
            "purge_board_graph": False,
            "purge_relational_runtime": False,
            "global_writer_guarded": True,
        }
        erased_capabilities.append("cascade")
        return {"board_id": board_id, "verified_absent": True}

    registry = SimpleNamespace(
        graph_runtime_store=runtime,
        require_global_discovery_runtime=lambda: _GlobalRuntime(),
        require_rebuild_audit_artifact_store=lambda: _Artifacts(),
    )
    monkeypatch.setattr(clustering, "board_delete_cascade", _cascade)
    monkeypatch.setattr(governance, "_authoritative_survivor_board_ids", _survivors)
    monkeypatch.setattr(kg_interfaces, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        storage_module,
        "get_storage_provider",
        lambda: _Attachments(),
    )

    erased = await governance.right_to_erasure(
        object(),
        BOARD_ID,
        strict=True,
        commit=False,
        global_writer_guarded=True,
        purge_relational=False,
    )

    assert erased["graph_purge"]["status"] == "purged"
    assert erased["graph_verified_absent"] is True
    assert set(erased_capabilities) == {
        "cascade",
        "global",
        "attachments",
        "artifacts",
    }
    assert runtime.exists(BOARD_ID) is False
    assert BOARD_ID not in store._nodes
    assert BOARD_ID not in store._edges
    assert runtime.exists(survivor_board_id) is True
    assert "survivor-node" in store._board_nodes(survivor_board_id)


def test_commit_rejects_forced_cross_source_entity_supersedence_before_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryGraphStore()
    store.create_node(
        BOARD_ID,
        "Entity",
        "ideation-root",
        {
            "source_artifact_ref": "ideation:foreign",
            "title": "Same lifecycle title",
        },
    )
    transaction = _QueryableMemoryTransaction(store)
    monkeypatch.setattr(
        primitives,
        "get_kg_registry",
        lambda: SimpleNamespace(graph_transaction=transaction),
    )
    candidate = NodeCandidate(
        candidate_id="spec-root",
        node_type=KGNodeType.ENTITY,
        title="Same lifecycle title",
        source_artifact_ref=f"spec:{DEPENDENT_ID}",
        source_confidence=1.0,
    )
    hint = ReconciliationHint(
        candidate_id=candidate.candidate_id,
        operation=ReconciliationOperation.SUPERSEDE,
        target_node_id="ideation-root",
        confidence=1.0,
        reason="forced stale override",
    )

    with pytest.raises(primitives.KGPrimitiveError) as caught:
        primitives._do_graph_commit(
            BOARD_ID,
            "cross-source-session",
            {candidate.candidate_id: candidate},
            {},
            {candidate.candidate_id: hint},
            "system:historical_consolidation",
            TestingStubEmbeddingProvider(dim=8),
            "healthy",
            "content-hash",
            DEPENDENT_ID,
            frozenset(),
            "spec",
            primitives.SpecLineageParentIntent.PRESERVE,
            frozenset(),
            None,
        )

    assert caught.value.code == "entity_source_identity_mismatch"
    assert caught.value.details == {
        "candidate_id": "spec-root",
        "candidate_source_artifact_ref": f"spec:{DEPENDENT_ID}",
        "target_node_id": "ideation-root",
        "target_source_artifact_ref": "ideation:foreign",
    }
    assert set(store._board_nodes(BOARD_ID)) == {"ideation-root"}
    assert store._board_nodes(BOARD_ID)["ideation-root"].get("superseded_by") is None
    assert store._board_edges(BOARD_ID) == []


@pytest.mark.parametrize(
    ("identity_state", "expected_reason"),
    [
        ("lookup_failed", "target_lookup_failed"),
        ("target_missing", "target_not_found"),
        ("source_ref_null", "target_source_artifact_ref_missing"),
        ("source_ref_empty", "target_source_artifact_ref_missing"),
    ],
)
def test_entity_identity_guard_fails_closed_before_writes_or_compensation(
    monkeypatch: pytest.MonkeyPatch,
    identity_state: str,
    expected_reason: str,
) -> None:
    store = InMemoryGraphStore()
    if identity_state != "target_missing":
        store.create_node(
            BOARD_ID,
            "Entity",
            "target-root",
            {
                "source_artifact_ref": {
                    "lookup_failed": f"spec:{DEPENDENT_ID}",
                    "source_ref_null": None,
                    "source_ref_empty": "",
                }[identity_state],
                "title": "Existing target",
            },
        )
    transaction = (
        _FailingIdentityLookupTransaction(store)
        if identity_state == "lookup_failed"
        else _QueryableMemoryTransaction(store)
    )
    monkeypatch.setattr(
        primitives,
        "get_kg_registry",
        lambda: SimpleNamespace(graph_transaction=transaction),
    )

    compensation_calls: list[tuple[object, ...]] = []

    def record_compensation(*args: object, **_kwargs: object) -> None:
        compensation_calls.append(args)

    monkeypatch.setattr(primitives, "_compensate_graph_writes", record_compensation)
    candidate = NodeCandidate(
        candidate_id="spec-root",
        node_type=KGNodeType.ENTITY,
        title="Dependent Spec",
        source_artifact_ref=f"spec:{DEPENDENT_ID}",
        source_confidence=1.0,
    )
    hint = ReconciliationHint(
        candidate_id=candidate.candidate_id,
        operation=ReconciliationOperation.UPDATE,
        target_node_id="target-root",
        confidence=1.0,
        reason="forced override",
    )
    before_nodes = {
        node_id: dict(attrs) for node_id, attrs in store._board_nodes(BOARD_ID).items()
    }

    with pytest.raises(primitives.KGPrimitiveError) as caught:
        primitives._do_graph_commit(
            BOARD_ID,
            f"identity-{identity_state}",
            {candidate.candidate_id: candidate},
            {},
            {candidate.candidate_id: hint},
            "system:historical_consolidation",
            TestingStubEmbeddingProvider(dim=8),
            "healthy",
            "content-hash",
            DEPENDENT_ID,
            frozenset(),
            "spec",
            primitives.SpecLineageParentIntent.PRESERVE,
            frozenset(),
            None,
        )

    assert caught.value.code == "entity_source_identity_unverifiable"
    assert caught.value.details["reason"] == expected_reason
    assert store._board_nodes(BOARD_ID) == before_nodes
    assert store._board_edges(BOARD_ID) == []
    assert compensation_calls == []


def test_dependency_endpoint_rejects_spec_child_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = DeterministicWorker().process_spec(
        _spec(
            dependencies=[
                {
                    "dependency_id": DEPENDENCY_ID,
                    "dependent_spec_id": DEPENDENT_ID,
                    "prerequisite_spec_id": PREREQUISITE_ID,
                    "prerequisite_title": "Prerequisite Spec",
                    "prerequisite_status": "done",
                    "prerequisite_version": 1,
                }
            ]
        )
    )
    edge = next(item for item in result.edges if item.edge_type == "precedes")
    edge.from_candidate_id = f"kgref:Entity:spec:{PREREQUISITE_ID}:child"
    intent_edge = result.relational_projection_active_set_intent.active_edges[0]
    result.relational_projection_active_set_intent = SimpleNamespace(
        owner_type="spec",
        owner_id=DEPENDENT_ID,
        namespace="dependencies",
        active_refs=(),
        active_edges=(
            intent_edge.__class__(
                candidate_id=intent_edge.candidate_id,
                edge_type=intent_edge.edge_type,
                from_candidate_id=edge.from_candidate_id,
                to_candidate_id=intent_edge.to_candidate_id,
                rule_id=intent_edge.rule_id,
            ),
        ),
    )

    with pytest.raises(primitives.KGPrimitiveError) as caught:
        _commit_worker_result(
            monkeypatch,
            InMemoryGraphStore(),
            result,
            artifact_id=DEPENDENT_ID,
            session_id="child-ref-session",
        )

    assert caught.value.code == "relational_projection_endpoint_invalid"


@pytest.mark.asyncio
async def test_endpoint_pending_skips_durability_for_read_only_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    async def pending_commit(*_args: object, **_kwargs: object) -> None:
        events.append("commit")
        raise primitives.KGPrimitiveError(
            "relational_projection_endpoint_pending",
            "prerequisite root pending",
            session_id="session-pending",
        )

    async def forbidden_durability(**_kwargs: object) -> None:
        events.append("durability")
        raise AssertionError("read-only retry must not touch graph lifecycle")

    monkeypatch.setattr(consolidation, "commit_consolidation", pending_commit)
    monkeypatch.setattr(
        consolidation,
        "_ensure_board_graph_durable",
        forbidden_durability,
    )
    entry = SimpleNamespace(
        id="queue-pending",
        board_id=BOARD_ID,
        artifact_type="spec",
        artifact_id=DEPENDENT_ID,
    )

    with pytest.raises(primitives.KGPrimitiveError) as caught:
        await _commit_consolidation_with_board_graph_lifecycle(
            entry=entry,
            session_id="session-pending",
            summary_text="dependent projection",
            db=object(),
            enter_graph_write=lambda _mutation_ref: object(),
        )

    assert caught.value.code == "relational_projection_endpoint_pending"
    assert events == ["commit"]


@pytest.mark.asyncio
async def test_endpoint_pending_requeues_without_spending_delivery_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 12, tzinfo=timezone.utc)
    saved: list[ConsolidationQueueRecord] = []

    class _Store:
        async def save_queue_entries(
            self,
            _db: object,
            entries: tuple[ConsolidationQueueRecord, ...],
        ) -> None:
            saved.extend(entries)

    entry = ConsolidationQueueRecord(
        id="queue-pending",
        board_id=BOARD_ID,
        artifact_type="spec",
        artifact_id=DEPENDENT_ID,
        status="claimed",
        attempts=4,
        last_error=None,
        next_retry_at=None,
        claimed_at=now,
        claim_timeout_at=now,
        worker_id="worker-1",
        claimed_by_session_id="worker-1",
        triggered_at=now,
        priority="high",
        claim_token="claim-1",
    )
    monkeypatch.setattr(
        consolidation,
        "get_consolidation_persistence_port",
        lambda: _Store(),
    )
    processor = ConsolidationProcessor(
        relational_scope_factory=lambda: None,
        clock=SimpleNamespace(now=lambda: now),
    )

    await processor._defer_relational_projection_endpoint(
        object(),
        entry,
        error_text="relational_projection_endpoint_pending:pending",
    )

    assert entry.attempts == 4
    assert entry.status == "pending"
    assert entry.next_retry_at == now + timedelta(seconds=1)
    assert entry.claimed_at is None
    assert entry.claim_timeout_at is None
    assert entry.worker_id is None
    assert entry.claimed_by_session_id is None
    assert entry.claim_token is None
    assert saved == [entry]
