"""Authoritative two-phase overlap materialization for empty KG rebuilds."""

from __future__ import annotations

from datetime import datetime, timezone
import re

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.application.processors.consolidation import (
    _materialize_authoritative_overlap_endpoint_nodes,
    _worker_edge_to_candidate,
    _worker_node_to_candidate,
)
from okto_pulse.core.application.processors.deterministic_kg import (
    DeterministicWorker,
)
from okto_pulse.core.kg.interfaces.registry import reset_registry_for_tests
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
from okto_pulse.core.kg.primitives import (
    add_edge_candidate,
    begin_consolidation,
    commit_consolidation,
    propose_reconciliation,
)
from okto_pulse.core.kg.providers.testing.memory_graph_store import (
    InMemoryGraphStore,
    InMemoryGraphTransaction,
    _InMemoryGraphTransactionScope,
)
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    ProposeReconciliationRequest,
)
from okto_pulse.core.ports.consolidation import ConsolidationQueueRecord


SHA_A = "a" * 64
SHA_B = "b" * 64
BOARD_ID = "board-overlap-rebuild"
AGENT_ID = "system:layer1_worker"


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_registry_for_tests()
    yield
    reset_registry_for_tests()


def _target(target_id: str, peer_ids: list[str]) -> dict[str, object]:
    suffix = target_id.rsplit("-", 1)[-1]
    return {
        "id": target_id,
        "board_id": BOARD_ID,
        "card_id": f"card-{suffix}",
        "card_node_type": "Entity",
        "investigation_source_ref": f"source:{suffix}",
        "selector_kind": "file",
        "relative_path_hint": f"src/{suffix}.py",
        "role": "modify",
        "intent": f"Modify authoritative target {suffix}.",
        "lifecycle_status": "active",
        "revision": 1,
        "resolution_state": "resolved",
        "payload_sha256": SHA_A,
        "content_hash": SHA_B,
        "evidence_links": [],
        "overlap_target_ids": peer_ids,
    }


def _entry(target_id: str) -> ConsolidationQueueRecord:
    return ConsolidationQueueRecord(
        id=f"queue-{target_id}",
        board_id=BOARD_ID,
        artifact_type="implementation_target",
        artifact_id=target_id,
        status="claimed",
        attempts=0,
        last_error=None,
        next_retry_at=None,
        claimed_at=datetime.now(timezone.utc),
        claim_timeout_at=None,
        worker_id="worker-1",
        claimed_by_session_id="session-1",
        triggered_at=datetime.now(timezone.utc),
        priority="high",
        source="rebuild:manifest-overlap",
    )


class _TargetPersistence:
    def __init__(self, targets: dict[str, dict[str, object]]) -> None:
        self.targets = targets

    async def load_artifact(
        self,
        _db,
        *,
        artifact_type: str,
        artifact_id: str,
    ):
        if artifact_type != "implementation_target":
            return None
        target = self.targets.get(artifact_id)
        return dict(target) if target is not None else None


class _ResolvingScope(_InMemoryGraphTransactionScope):
    """Memory transaction with the exact read probes used by primitives."""

    def execute(self, cypher: str, params=None) -> GraphStatementResult:
        self.statements.append((cypher, params))
        values = dict(params or {})
        match = re.search(r"MATCH \(n:([A-Za-z]+)\)", cypher)
        node_type = match.group(1) if match is not None else None
        nodes = self.store._board_nodes(self.board_id)
        if node_type is not None and "source_artifact_ref = $ref" in cypher:
            matches = [
                node
                for node in nodes.values()
                if node.get("_type") == node_type
                and node.get("source_artifact_ref") == values.get("ref")
                and not node.get("superseded_by")
            ]
            return GraphStatementResult.from_rows(
                ((node["id"],) for node in matches[:2]),
                columns=("id",),
            )
        if node_type is not None and "n.id = $id" in cypher:
            node = nodes.get(str(values.get("id") or ""))
            if node is None or node.get("_type") != node_type:
                return GraphStatementResult()
            return_clause = cypher.partition("RETURN ")[2].partition(" LIMIT")[0]
            fields = [field.strip() for field in return_clause.split(",")]
            row = []
            for field in fields:
                name = field.removeprefix("n.").split()[0]
                row.append(node.get(name))
            return GraphStatementResult.from_rows((row,), columns=fields)
        return GraphStatementResult()


class _ResolvingTransaction(InMemoryGraphTransaction):
    async def begin(self, board_id: str) -> _ResolvingScope:
        return _ResolvingScope(board_id, self.store)


async def _commit_target_result(target_id: str, result) -> None:
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=BOARD_ID,
            artifact_type="implementation_target",
            artifact_id=target_id,
            raw_content=result.raw_content,
            deterministic_candidates=[
                _worker_node_to_candidate(node) for node in result.nodes
            ],
        ),
        agent_id=AGENT_ID,
        db=None,
    )
    for edge in result.edges:
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=_worker_edge_to_candidate(edge),
            ),
            agent_id=AGENT_ID,
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=AGENT_ID,
        db=None,
        force_reprocess=True,
    )
    await commit_consolidation(
        CommitConsolidationRequest(session_id=begin.session_id),
        agent_id=AGENT_ID,
        db=None,
    )


def _nodes_for_source(store: InMemoryGraphStore, source_ref: str) -> list[dict]:
    return [
        node
        for node in store._board_nodes(BOARD_ID).values()  # noqa: SLF001
        if node.get("source_artifact_ref") == source_ref
    ]


@pytest.mark.asyncio
async def test_symmetric_overlap_rebuild_materializes_authoritative_roots_and_preserves_peer_ownership(
    monkeypatch,
) -> None:
    store = InMemoryGraphStore()
    configure_test_kg_registry(
        graph_provider="inmemory",
        graph_store=store,
        graph_transaction=_ResolvingTransaction(store),
    )
    store.bootstrap(BOARD_ID)
    for suffix in ("a", "b"):
        store.create_node(
            BOARD_ID,
            "Entity",
            f"card-node-{suffix}",
            {
                "title": f"Card {suffix}",
                "content": "",
                "source_artifact_ref": f"card:card-{suffix}",
                "kind_of": "card",
                "graph_layer": "canonical",
                "maturity_status": "canonical_eligible",
            },
        )

    target_a = _target("target-a", ["target-b"])
    target_b = _target("target-b", ["target-a"])
    persistence = _TargetPersistence({"target-a": target_a, "target-b": target_b})
    monkeypatch.setattr(
        consolidation,
        "get_consolidation_persistence_port",
        lambda: persistence,
    )

    result_a = DeterministicWorker().process_implementation_target(target_a)
    result_a = await _materialize_authoritative_overlap_endpoint_nodes(
        object(),
        _entry("target-a"),
        target_a,
        result_a,
    )
    assert {
        node.source_artifact_ref
        for node in result_a.nodes
        if node.kind_of == "implementation_target"
    } == {
        "implementation_target:target-a",
        "implementation_target:target-b",
    }
    authoritative_b = (
        DeterministicWorker().process_implementation_target(target_b).nodes[0]
    )
    staged_b = next(
        node
        for node in result_a.nodes
        if node.source_artifact_ref == "implementation_target:target-b"
    )
    assert staged_b == authoritative_b

    await _commit_target_result("target-a", result_a)
    assert len(_nodes_for_source(store, "implementation_target:target-a")) == 1
    assert len(_nodes_for_source(store, "implementation_target:target-b")) == 1
    assert (
        sum(
            edge.get("_type") == "overlaps"
            for edge in store._board_edges(BOARD_ID)  # noqa: SLF001
        )
        == 1
    )

    # A later authoritative projection no longer declares the relation.  Its
    # active-set cleanup may retire the edge it owned, but must not retire B's
    # root merely because B was preloaded into A's earlier two-phase session.
    target_a_without_overlap = _target("target-a", [])
    await _commit_target_result(
        "target-a",
        DeterministicWorker().process_implementation_target(target_a_without_overlap),
    )
    assert len(_nodes_for_source(store, "implementation_target:target-b")) == 1

    target_b_without_overlap = _target("target-b", [])
    result_b = DeterministicWorker().process_implementation_target(
        target_b_without_overlap
    )
    await _commit_target_result("target-b", result_b)
    before_replay = (
        len(store._board_nodes(BOARD_ID)),  # noqa: SLF001
        len(store._board_edges(BOARD_ID)),  # noqa: SLF001
    )
    await _commit_target_result("target-b", result_b)
    assert (
        len(store._board_nodes(BOARD_ID)),  # noqa: SLF001
        len(store._board_edges(BOARD_ID)),  # noqa: SLF001
    ) == before_replay
    assert len(_nodes_for_source(store, "implementation_target:target-b")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("peer", "expected"),
    (
        (None, "code_traceability_overlap_peer_unresolved"),
        (
            {**_target("target-b", []), "board_id": "board-other"},
            "code_traceability_overlap_peer_scope_mismatch",
        ),
        (
            {**_target("target-b", []), "lifecycle_status": "revoked"},
            "code_traceability_overlap_peer_not_materializable",
        ),
    ),
)
async def test_overlap_peer_closure_fails_before_any_graph_session_or_placeholder(
    monkeypatch,
    peer,
    expected: str,
) -> None:
    store = InMemoryGraphStore()
    configure_test_kg_registry(
        graph_provider="inmemory",
        graph_store=store,
        graph_transaction=_ResolvingTransaction(store),
    )
    store.bootstrap(BOARD_ID)
    target_a = _target("target-a", ["target-b"])
    targets = {"target-a": target_a}
    if peer is not None:
        targets["target-b"] = peer
    monkeypatch.setattr(
        consolidation,
        "get_consolidation_persistence_port",
        lambda: _TargetPersistence(targets),
    )
    result = DeterministicWorker().process_implementation_target(target_a)

    with pytest.raises(ValueError, match=expected):
        await _materialize_authoritative_overlap_endpoint_nodes(
            object(),
            _entry("target-a"),
            target_a,
            result,
        )

    assert _nodes_for_source(store, "implementation_target:target-b") == []
    assert store._board_edges(BOARD_ID) == []  # noqa: SLF001
