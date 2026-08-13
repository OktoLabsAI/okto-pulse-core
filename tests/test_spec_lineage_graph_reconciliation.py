from __future__ import annotations

import pytest

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.interfaces.graph_transaction import (
    SpecLineageEdgeSnapshot,
    SpecLineageParentIntent,
    SpecLineageReconciliationError,
    SpecLineageReconciliationReceipt,
)
from okto_pulse.core.kg.providers.testing.memory_graph_store import (
    InMemoryGraphStore,
    _InMemoryGraphTransactionScope,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator


BOARD_ID = "board-lineage"
SPEC_ID = "spec-source"
IDEATION_ID = "ideation-parent"
REFINEMENT_ID = "refinement-parent"
BOARD_ROOT_ID = "board-root"
CHILD_ID = "spec-requirement"

IDEATION_RULE = "belongs_to/spec_to_ideation@1.0"
REFINEMENT_RULE = "belongs_to/spec_to_refinement@1.0"
BOARD_RULE = "belongs_to/spec_to_board@1.0"
CHILD_RULE = "belongs_to/requirement@1.0"


def _edge_attrs(rule_id: str, session_id: str) -> dict[str, object]:
    return {
        "confidence": 1.0,
        "created_by_session_id": session_id,
        "created_at": "2026-07-25T12:00:00.000000",
        "layer": "deterministic",
        "rule_id": rule_id,
        "created_by": "worker_deterministic_v1",
        "fallback_reason": "",
    }


def _seed_graph() -> InMemoryGraphStore:
    store = InMemoryGraphStore()
    for node_id in (SPEC_ID, IDEATION_ID, REFINEMENT_ID, BOARD_ROOT_ID):
        store.create_node(BOARD_ID, "Entity", node_id, {})
    store.create_node(BOARD_ID, "Requirement", CHILD_ID, {})
    store.create_edge(
        BOARD_ID,
        "belongs_to",
        SPEC_ID,
        IDEATION_ID,
        _edge_attrs(IDEATION_RULE, "session-old"),
        from_type="Entity",
        to_type="Entity",
    )
    store.create_edge(
        BOARD_ID,
        "belongs_to",
        SPEC_ID,
        BOARD_ROOT_ID,
        _edge_attrs(BOARD_RULE, "session-board"),
        from_type="Entity",
        to_type="Entity",
    )
    store.create_edge(
        BOARD_ID,
        "belongs_to",
        CHILD_ID,
        SPEC_ID,
        _edge_attrs(CHILD_RULE, "session-child"),
        from_type="Requirement",
        to_type="Entity",
    )
    return store


def _lineage_targets(store: InMemoryGraphStore) -> list[str]:
    return sorted(
        str(edge["_to"])
        for edge in store._board_edges(BOARD_ID)
        if edge.get("_from") == SPEC_ID
        and str(edge.get("rule_id") or "") in {IDEATION_RULE, REFINEMENT_RULE}
    )


def _has_edge(
    store: InMemoryGraphStore,
    *,
    source_id: str,
    target_id: str,
    rule_id: str,
) -> bool:
    return any(
        edge.get("_from") == source_id
        and edge.get("_to") == target_id
        and str(edge.get("rule_id") or "") == rule_id
        for edge in store._board_edges(BOARD_ID)
    )


def test_spec_worker_emits_explicit_clear_intent_only_for_persisted_unlink() -> None:
    from okto_pulse.core.application.processors.deterministic_kg import (
        DeterministicWorker,
    )

    worker = DeterministicWorker()
    unlinked = worker.process_spec({"id": SPEC_ID, "board_id": BOARD_ID})
    linked = worker.process_spec(
        {
            "id": SPEC_ID,
            "board_id": BOARD_ID,
            "ideation_id": IDEATION_ID,
        }
    )

    assert (
        unlinked.spec_lineage_parent_intent
        is SpecLineageParentIntent.CLEAR
    )
    assert (
        linked.spec_lineage_parent_intent
        is SpecLineageParentIntent.PRESERVE
    )


@pytest.mark.asyncio
async def test_relink_creates_new_before_delete_and_compensation_restores_old() -> None:
    store = _seed_graph()
    scope = _InMemoryGraphTransactionScope(BOARD_ID, store)
    operations: list[str] = []
    original_create = scope.create_edge
    original_delete = scope._delete_spec_lineage_edge

    def _record_create(*args, **kwargs):
        operations.append(f"create:{args[4]}")
        return original_create(*args, **kwargs)

    def _record_delete(snapshot: SpecLineageEdgeSnapshot) -> None:
        operations.append(f"delete:{snapshot.target_id}")
        original_delete(snapshot)

    scope.create_edge = _record_create  # type: ignore[method-assign]
    scope._delete_spec_lineage_edge = _record_delete  # type: ignore[method-assign]
    orchestrator = TransactionOrchestrator(
        scope,
        session_id="session-new",
        board_id=BOARD_ID,
    )

    orchestrator.create_edge(
        "belongs_to",
        SPEC_ID,
        REFINEMENT_ID,
        attrs=_edge_attrs(REFINEMENT_RULE, "ignored-by-orchestrator"),
        from_type="Entity",
        to_type="Entity",
    )

    assert operations[:2] == [
        f"create:{REFINEMENT_ID}",
        f"delete:{IDEATION_ID}",
    ]
    assert _lineage_targets(store) == [REFINEMENT_ID]
    assert _has_edge(
        store,
        source_id=SPEC_ID,
        target_id=BOARD_ROOT_ID,
        rule_id=BOARD_RULE,
    )
    assert _has_edge(
        store,
        source_id=CHILD_ID,
        target_id=SPEC_ID,
        rule_id=CHILD_RULE,
    )

    await orchestrator.compensate()

    assert _lineage_targets(store) == [IDEATION_ID]
    assert _has_edge(
        store,
        source_id=SPEC_ID,
        target_id=BOARD_ROOT_ID,
        rule_id=BOARD_RULE,
    )
    assert _has_edge(
        store,
        source_id=CHILD_ID,
        target_id=SPEC_ID,
        rule_id=CHILD_RULE,
    )


def test_missing_new_parent_fails_before_deleting_old() -> None:
    store = _seed_graph()
    scope = _InMemoryGraphTransactionScope(BOARD_ID, store)

    with pytest.raises(SpecLineageReconciliationError) as excinfo:
        scope.reconcile_spec_lineage_parent(
            SPEC_ID,
            "missing-parent",
            _edge_attrs(REFINEMENT_RULE, "session-new"),
        )

    assert excinfo.value.code == "spec_lineage_endpoint_not_found"
    assert _lineage_targets(store) == [IDEATION_ID]


def test_retry_is_idempotent_and_keeps_unrelated_belongs_to_edges() -> None:
    store = _seed_graph()
    scope = _InMemoryGraphTransactionScope(BOARD_ID, store)

    first = scope.reconcile_spec_lineage_parent(
        SPEC_ID,
        REFINEMENT_ID,
        _edge_attrs(REFINEMENT_RULE, "session-new"),
    )
    second = scope.reconcile_spec_lineage_parent(
        SPEC_ID,
        REFINEMENT_ID,
        _edge_attrs(REFINEMENT_RULE, "session-retry"),
    )

    assert first.new_edge_created is True
    assert len(first.removed_edges) == 1
    assert second.new_edge_created is False
    assert second.removed_edges == ()
    assert _lineage_targets(store) == [REFINEMENT_ID]
    assert _has_edge(
        store,
        source_id=SPEC_ID,
        target_id=BOARD_ROOT_ID,
        rule_id=BOARD_RULE,
    )
    assert _has_edge(
        store,
        source_id=CHILD_ID,
        target_id=SPEC_ID,
        rule_id=CHILD_RULE,
    )


@pytest.mark.asyncio
async def test_explicit_clear_is_compensable_idempotent_and_preserves_ambiguity() -> None:
    store = _seed_graph()
    store.create_node(BOARD_ID, "Entity", "legacy-parent", {})
    store.create_edge(
        BOARD_ID,
        "belongs_to",
        SPEC_ID,
        "legacy-parent",
        {
            **_edge_attrs("legacy_pre_v2", "session-legacy"),
            "layer": "legacy",
        },
        from_type="Entity",
        to_type="Entity",
    )
    scope = _InMemoryGraphTransactionScope(BOARD_ID, store)
    orchestrator = TransactionOrchestrator(
        scope,
        session_id="session-clear",
        board_id=BOARD_ID,
    )

    orchestrator.clear_spec_lineage_parent(SPEC_ID)

    receipt = orchestrator.records[0].lineage_receipt
    assert receipt is not None
    assert receipt.target_id is None
    assert receipt.new_edge_created is False
    assert len(receipt.removed_edges) == 1
    assert receipt.ambiguous_legacy_edges == 1
    assert _lineage_targets(store) == []
    assert _has_edge(
        store,
        source_id=SPEC_ID,
        target_id=BOARD_ROOT_ID,
        rule_id=BOARD_RULE,
    )
    assert _has_edge(
        store,
        source_id=CHILD_ID,
        target_id=SPEC_ID,
        rule_id=CHILD_RULE,
    )
    assert _has_edge(
        store,
        source_id=SPEC_ID,
        target_id="legacy-parent",
        rule_id="legacy_pre_v2",
    )

    retry = scope.clear_spec_lineage_parent(SPEC_ID)
    assert retry.removed_edges == ()
    assert retry.ambiguous_legacy_edges == 1

    await orchestrator.compensate()
    assert _lineage_targets(store) == [IDEATION_ID]
    assert _has_edge(
        store,
        source_id=SPEC_ID,
        target_id="legacy-parent",
        rule_id="legacy_pre_v2",
    )


class _DeleteAndRestoreFailingScope(_InMemoryGraphTransactionScope):
    def __init__(self, board_id: str, store: InMemoryGraphStore) -> None:
        super().__init__(board_id, store)
        self.new_created = False
        self.delete_attempted = False

    def create_edge(
        self,
        edge_type: str,
        from_type: str,
        to_type: str,
        from_id: str,
        to_id: str,
        attrs: dict,
    ) -> bool:
        if to_id == IDEATION_ID and self.delete_attempted:
            raise RuntimeError("injected restore failure")
        created = super().create_edge(
            edge_type,
            from_type,
            to_type,
            from_id,
            to_id,
            attrs,
        )
        if to_id == REFINEMENT_ID:
            self.new_created = created
        return created

    def _delete_spec_lineage_edge(
        self,
        snapshot: SpecLineageEdgeSnapshot,
    ) -> None:
        assert self.new_created is True
        super()._delete_spec_lineage_edge(snapshot)
        self.delete_attempted = True
        raise RuntimeError("injected delete failure after auto-commit")


class _AlreadyCompensatedCleanupScope(_InMemoryGraphTransactionScope):
    """Emulate the embedded adapter's restore-first cleanup failure."""

    def __init__(self, board_id: str, store: InMemoryGraphStore) -> None:
        super().__init__(board_id, store)
        self.cleanup_failure_injected = False
        self.compensation_calls = 0

    def _delete_spec_lineage_edge(
        self,
        snapshot: SpecLineageEdgeSnapshot,
    ) -> None:
        super()._delete_spec_lineage_edge(snapshot)
        if (
            snapshot.target_id == IDEATION_ID
            and not self.cleanup_failure_injected
        ):
            self.cleanup_failure_injected = True
            raise RuntimeError("injected old-parent cleanup failure")

    def compensate_spec_lineage_parent(self, receipt) -> None:
        self.compensation_calls += 1
        super().compensate_spec_lineage_parent(receipt)


class _ProgressPreservedScope(_InMemoryGraphTransactionScope):
    def __init__(self, board_id: str, store: InMemoryGraphStore) -> None:
        super().__init__(board_id, store)
        self.compensation_calls = 0

    def reconcile_spec_lineage_parent(
        self,
        source_id: str,
        target_id: str,
        attrs: dict,
    ) -> SpecLineageReconciliationReceipt:
        old_edges = tuple(
            self._spec_lineage_snapshot(edge)
            for edge in self._spec_lineage_edges(source_id)
            if str(edge.get("rule_id") or "") == IDEATION_RULE
        )
        created = self.create_edge(
            "belongs_to",
            "Entity",
            "Entity",
            source_id,
            target_id,
            dict(attrs),
        )
        receipt = SpecLineageReconciliationReceipt(
            source_id=source_id,
            target_id=target_id,
            target_rule_id=str(attrs["rule_id"]),
            target_attrs=dict(attrs),
            new_edge_created=created,
            removed_edges=old_edges,
        )
        raise SpecLineageReconciliationError(
            "spec_lineage_old_parent_cleanup_incomplete",
            "replacement exists while the old parent remains",
            receipt=receipt,
        )

    def compensate_spec_lineage_parent(self, receipt) -> None:
        self.compensation_calls += 1
        super().compensate_spec_lineage_parent(receipt)


class _SpecLineageFailureTransaction:
    def __init__(self, store: InMemoryGraphStore) -> None:
        self.store = store
        self.begin_calls = 0
        self.inline_scope: _AlreadyCompensatedCleanupScope | None = None
        self.outer_compensation_calls = 0

    async def begin(self, board_id: str):
        self.begin_calls += 1
        if self.begin_calls == 1:
            self.inline_scope = _AlreadyCompensatedCleanupScope(
                board_id,
                self.store,
            )
            return self.inline_scope

        scope = _InMemoryGraphTransactionScope(board_id, self.store)
        original = scope.compensate_spec_lineage_parent

        def _probe(receipt) -> None:
            self.outer_compensation_calls += 1
            original(receipt)

        scope.compensate_spec_lineage_parent = _probe  # type: ignore[method-assign]
        return scope


def test_partial_failure_carries_receipt_and_never_leaves_zero_parent() -> None:
    store = _seed_graph()
    scope = _DeleteAndRestoreFailingScope(BOARD_ID, store)
    orchestrator = TransactionOrchestrator(
        scope,
        session_id="session-new",
        board_id=BOARD_ID,
    )

    with pytest.raises(SpecLineageReconciliationError) as excinfo:
        orchestrator.create_edge(
            "belongs_to",
            SPEC_ID,
            REFINEMENT_ID,
            attrs=_edge_attrs(REFINEMENT_RULE, "ignored-by-orchestrator"),
            from_type="Entity",
            to_type="Entity",
        )

    assert excinfo.value.code == "spec_lineage_partial_cleanup_restore_failed"
    assert excinfo.value.receipt is not None
    assert len(orchestrator.records) == 1
    assert orchestrator.records[0].lineage_receipt is excinfo.value.receipt
    assert _lineage_targets(store) == [REFINEMENT_ID]


@pytest.mark.asyncio
async def test_outer_compensation_preserves_bounded_reconciliation_progress() -> None:
    store = _seed_graph()
    scope = _ProgressPreservedScope(BOARD_ID, store)
    orchestrator = TransactionOrchestrator(
        scope,
        session_id="session-new",
        board_id=BOARD_ID,
    )

    with pytest.raises(SpecLineageReconciliationError) as excinfo:
        orchestrator.create_edge(
            "belongs_to",
            SPEC_ID,
            REFINEMENT_ID,
            attrs=_edge_attrs(REFINEMENT_RULE, "ignored-by-orchestrator"),
            from_type="Entity",
            to_type="Entity",
        )

    assert excinfo.value.retryable is True
    assert excinfo.value.preserve_progress is True
    assert orchestrator.records[0].lineage_progress_preserved is True

    await orchestrator.compensate()

    assert scope.compensation_calls == 0
    assert _lineage_targets(store) == [IDEATION_ID, REFINEMENT_ID]


def test_graph_commit_preserves_lineage_error_and_avoids_double_compensation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from okto_pulse.core.kg import primitives
    from okto_pulse.core.kg.providers.testing.embedding import (
        TestingStubEmbeddingProvider,
    )
    from okto_pulse.core.kg.query_contract import KGEdgeType, KGNodeType
    from okto_pulse.core.kg.schemas import (
        EdgeCandidate,
        NodeCandidate,
        ReconciliationHint,
        ReconciliationOperation,
    )

    store = _seed_graph()
    transaction = _SpecLineageFailureTransaction(store)
    monkeypatch.setattr(
        primitives,
        "get_kg_registry",
        lambda: SimpleNamespace(graph_transaction=transaction),
    )
    monkeypatch.setattr(
        primitives,
        "_validate_graph_connectivity_before_commit",
        lambda *args, **kwargs: primitives._connectivity_empty_result(),
    )

    spec_candidate = NodeCandidate(
        candidate_id="spec-candidate",
        node_type=KGNodeType.ENTITY,
        title="Spec",
    )
    refinement_candidate = NodeCandidate(
        candidate_id="refinement-candidate",
        node_type=KGNodeType.ENTITY,
        title="Refinement",
    )
    edge = EdgeCandidate(
        candidate_id="spec-parent",
        edge_type=KGEdgeType.BELONGS_TO,
        from_candidate_id=spec_candidate.candidate_id,
        to_candidate_id=refinement_candidate.candidate_id,
        confidence=1.0,
        layer="deterministic",
        rule_id=REFINEMENT_RULE,
        created_by="worker_deterministic_v1",
    )
    hints = {
        spec_candidate.candidate_id: ReconciliationHint(
            candidate_id=spec_candidate.candidate_id,
            operation=ReconciliationOperation.UPDATE,
            target_node_id=SPEC_ID,
            confidence=1.0,
            reason="test existing root",
        ),
        refinement_candidate.candidate_id: ReconciliationHint(
            candidate_id=refinement_candidate.candidate_id,
            operation=ReconciliationOperation.UPDATE,
            target_node_id=REFINEMENT_ID,
            confidence=1.0,
            reason="test existing root",
        ),
    }

    with pytest.raises(primitives.KGPrimitiveError) as excinfo:
        primitives._do_graph_commit(
            BOARD_ID,
            "session-new",
            {
                spec_candidate.candidate_id: spec_candidate,
                refinement_candidate.candidate_id: refinement_candidate,
            },
            {edge.candidate_id: edge},
            hints,
            "system:historical_consolidation",
            TestingStubEmbeddingProvider(dim=8),
            "healthy",
            "content-hash",
            SPEC_ID,
            frozenset(),
            "spec",
            SpecLineageParentIntent.PRESERVE,
            frozenset(),
            None,
        )

    failure = excinfo.value
    assert failure.code == "spec_lineage_old_parent_cleanup_failed"
    assert failure.retryable is True
    assert failure.details == {
        "failure_type": "SpecLineageReconciliationError",
        "spec_lineage_error_code": "spec_lineage_old_parent_cleanup_failed",
        "retryable": True,
        "compensation_applied": True,
        "progress_preserved": False,
        "cause_type": "RuntimeError",
        "cause_code": None,
        "source_id": SPEC_ID,
        "target_id": REFINEMENT_ID,
        "target_rule_id": REFINEMENT_RULE,
        "new_edge_created": True,
        "removed_edge_count": 1,
        "ambiguous_legacy_edges": 0,
    }
    assert transaction.inline_scope is not None
    assert transaction.inline_scope.compensation_calls == 1
    assert transaction.outer_compensation_calls == 0
    assert _lineage_targets(store) == [IDEATION_ID]


def test_legacy_ambiguous_edge_is_signaled_and_preserved() -> None:
    store = _seed_graph()
    store.create_edge(
        BOARD_ID,
        "belongs_to",
        SPEC_ID,
        "legacy-parent",
        {
            **_edge_attrs("legacy_pre_v2", "session-legacy"),
            "layer": "legacy",
        },
        from_type="Entity",
        to_type="Entity",
    )
    store.create_node(BOARD_ID, "Entity", "legacy-parent", {})
    scope = _InMemoryGraphTransactionScope(BOARD_ID, store)

    receipt = scope.reconcile_spec_lineage_parent(
        SPEC_ID,
        REFINEMENT_ID,
        _edge_attrs(REFINEMENT_RULE, "session-new"),
    )

    assert receipt.ambiguous_legacy_edges == 1
    assert _lineage_targets(store) == [REFINEMENT_ID]
    assert _has_edge(
        store,
        source_id=SPEC_ID,
        target_id="legacy-parent",
        rule_id="legacy_pre_v2",
    )


class _ToggleAuditRepository:
    def __init__(self) -> None:
        from okto_pulse.core.kg.providers.testing.memory_audit_repo import (
            InMemoryAuditRepository,
        )

        self._delegate = InMemoryAuditRepository()
        self.fail_staging = False

    async def get_latest_for_artifact(self, *args, **kwargs):
        return await self._delegate.get_latest_for_artifact(*args, **kwargs)

    async def get_audit_by_session(self, *args, **kwargs):
        return await self._delegate.get_audit_by_session(*args, **kwargs)

    async def get_node_refs_by_session(self, *args, **kwargs):
        return await self._delegate.get_node_refs_by_session(*args, **kwargs)

    async def stage_consolidation_records(self, *args, **kwargs) -> None:
        if self.fail_staging:
            raise RuntimeError("injected audit/UOW staging failure")
        await self._delegate.stage_consolidation_records(*args, **kwargs)

    async def mark_audit_undone(self, *args, **kwargs) -> None:
        await self._delegate.mark_audit_undone(*args, **kwargs)

    async def purge_by_board(self, *args, **kwargs) -> int:
        return await self._delegate.purge_by_board(*args, **kwargs)


def _lineage_commit_candidates(
    *,
    board_id: str,
    parent_kind: str,
    parent_id: str,
):
    from okto_pulse.core.kg.query_contract import KGEdgeType, KGNodeType
    from okto_pulse.core.kg.schemas import EdgeCandidate, NodeCandidate

    spec_candidate = "spec_candidate"
    parent_candidate = f"{parent_kind}_candidate"
    board_candidate = "board_candidate"
    rule_id = f"belongs_to/spec_to_{parent_kind}@1.0"
    nodes = [
        NodeCandidate(
            candidate_id=spec_candidate,
            node_type=KGNodeType.ENTITY,
            title="Lineage Spec",
            source_artifact_ref=f"spec:{SPEC_ID}",
        ),
        NodeCandidate(
            candidate_id=parent_candidate,
            node_type=KGNodeType.ENTITY,
            title=f"Lineage {parent_kind}",
            source_artifact_ref=f"{parent_kind}:{parent_id}",
        ),
        NodeCandidate(
            candidate_id=board_candidate,
            node_type=KGNodeType.ENTITY,
            title="Lineage Board",
            source_artifact_ref=f"board:{board_id}",
        ),
    ]
    edges = [
        EdgeCandidate(
            candidate_id=f"spec_parent_{parent_kind}",
            edge_type=KGEdgeType.BELONGS_TO,
            from_candidate_id=spec_candidate,
            to_candidate_id=parent_candidate,
            confidence=1.0,
            layer="deterministic",
            rule_id=rule_id,
            created_by="worker_deterministic_v1",
        ),
        EdgeCandidate(
            candidate_id="spec_board",
            edge_type=KGEdgeType.BELONGS_TO,
            from_candidate_id=spec_candidate,
            to_candidate_id=board_candidate,
            confidence=1.0,
            layer="deterministic",
            rule_id=BOARD_RULE,
            created_by="worker_deterministic_v1",
        ),
        EdgeCandidate(
            candidate_id=f"{parent_kind}_board",
            edge_type=KGEdgeType.BELONGS_TO,
            from_candidate_id=parent_candidate,
            to_candidate_id=board_candidate,
            confidence=1.0,
            layer="deterministic",
            rule_id=f"belongs_to/{parent_kind}_to_board@1.0",
            created_by="worker_deterministic_v1",
        ),
    ]
    return nodes, edges


async def _commit_lineage_candidates(
    *,
    board_id: str,
    parent_kind: str,
    parent_id: str,
):
    from okto_pulse.core.kg.primitives import (
        add_edge_candidate,
        begin_consolidation,
        commit_consolidation,
        propose_reconciliation,
    )
    from okto_pulse.core.kg.schemas import (
        AddEdgeCandidateRequest,
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        ProposeReconciliationRequest,
    )

    nodes, edges = _lineage_commit_candidates(
        board_id=board_id,
        parent_kind=parent_kind,
        parent_id=parent_id,
    )
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=SPEC_ID,
            raw_content=f"Spec parent is {parent_kind}:{parent_id}",
            deterministic_candidates=nodes,
        ),
        agent_id="system:historical_consolidation",
        db=None,
        force_reprocess=True,
    )
    for edge in edges:
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=edge,
            ),
            agent_id="system:historical_consolidation",
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id="system:historical_consolidation",
        db=None,
        force_reprocess=True,
    )
    return await commit_consolidation(
        CommitConsolidationRequest(session_id=begin.session_id),
        agent_id="system:historical_consolidation",
        db=None,
    )


async def _commit_lineage_clear(*, board_id: str):
    from okto_pulse.core.kg.interfaces.graph_transaction import (
        SpecLineageParentIntent,
    )
    from okto_pulse.core.kg.primitives import (
        add_edge_candidate,
        begin_consolidation,
        commit_consolidation,
        propose_reconciliation,
    )
    from okto_pulse.core.kg.schemas import (
        AddEdgeCandidateRequest,
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        ProposeReconciliationRequest,
    )

    nodes, edges = _lineage_commit_candidates(
        board_id=board_id,
        parent_kind="ideation",
        parent_id=IDEATION_ID,
    )
    nodes = [candidate for candidate in nodes if candidate.candidate_id != "ideation_candidate"]
    edges = [candidate for candidate in edges if candidate.candidate_id == "spec_board"]
    begin = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=board_id,
            artifact_type="spec",
            artifact_id=SPEC_ID,
            raw_content="Spec has no lifecycle parent.",
            deterministic_candidates=nodes,
        ),
        agent_id="system:historical_consolidation",
        db=None,
        force_reprocess=True,
        spec_lineage_parent_intent=SpecLineageParentIntent.CLEAR,
    )
    for edge in edges:
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=edge,
            ),
            agent_id="system:historical_consolidation",
        )
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id="system:historical_consolidation",
        db=None,
        force_reprocess=True,
    )
    return await commit_consolidation(
        CommitConsolidationRequest(session_id=begin.session_id),
        agent_id="system:historical_consolidation",
        db=None,
    )


def _real_lineage_rows(
    kg_runtime,
    board_id: str,
    *,
    spec_id: str = SPEC_ID,
) -> list[tuple[str, str]]:
    with kg_runtime.open_board_connection(board_id) as (_db, conn):
        result = conn.execute(
            "MATCH (source:Entity)-[r:belongs_to]->(target:Entity) "
            "WHERE source.source_artifact_ref = $source_ref "
            "RETURN target.source_artifact_ref, r.rule_id "
            "ORDER BY target.source_artifact_ref, r.rule_id",
            {"source_ref": f"spec:{spec_id}"},
        )
        try:
            rows: list[tuple[str, str]] = []
            while result.has_next():
                row = result.get_next()
                rows.append((str(row[0]), str(row[1] or "")))
            return rows
        finally:
            result.close()


async def _real_lineage_rows_async(
    kg_runtime,
    board_id: str,
    *,
    spec_id: str = SPEC_ID,
) -> list[tuple[str, str]]:
    return await run_blocking_graph_io(
        lambda: _real_lineage_rows(kg_runtime, board_id, spec_id=spec_id),
        task_name="tests.spec_lineage_graph_reconciliation.lineage_rows",
    )


def _real_edge_exists(
    kg_runtime,
    board_id: str,
    *,
    from_type: str,
    from_id: str,
    to_type: str,
    to_id: str,
    rule_id: str,
) -> bool:
    with kg_runtime.open_board_connection(board_id) as (_db, conn):
        result = conn.execute(
            f"MATCH (source:{from_type} {{id: $from_id}})"
            f"-[r:belongs_to]->(target:{to_type} {{id: $to_id}}) "
            "WHERE r.rule_id = $rule_id RETURN count(r)",
            {
                "from_id": from_id,
                "to_id": to_id,
                "rule_id": rule_id,
            },
        )
        try:
            return bool(result.has_next() and int(result.get_next()[0]) == 1)
        finally:
            result.close()


def _real_node_id_by_source_ref(
    kg_runtime,
    board_id: str,
    *,
    node_type: str,
    source_ref: str,
) -> str:
    with kg_runtime.open_board_connection(board_id) as (_db, conn):
        result = conn.execute(
            f"MATCH (node:{node_type}) "
            "WHERE node.source_artifact_ref = $source_ref "
            "RETURN node.id LIMIT 1",
            {"source_ref": source_ref},
        )
        try:
            if not result.has_next():
                raise AssertionError(f"node not found for {source_ref}")
            return str(result.get_next()[0])
        finally:
            result.close()


@pytest.mark.asyncio
async def test_commit_audit_failure_restores_old_parent_before_removing_new(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kg_registry_testing import configure_real_graph_test_kg_registry
    from okto_pulse.community.adapters import kg_runtime
    from okto_pulse.core.kg import primitives

    board_id = "lineage-uow-compensation"
    audit = _ToggleAuditRepository()
    monkeypatch.setattr(kg_runtime, "_kg_base_dir", lambda: tmp_path / "kg")
    kg_runtime.reset_bootstrap_cache_for_tests()
    configure_real_graph_test_kg_registry(audit_repo=audit)

    async def _healthy(*_args, **_kwargs) -> str:
        return "healthy"

    monkeypatch.setattr(primitives, "_resolve_commit_kg_health_state", _healthy)

    try:
        kg_runtime.bootstrap_board_graph(board_id)
        await _commit_lineage_candidates(
            board_id=board_id,
            parent_kind="ideation",
            parent_id=IDEATION_ID,
        )
        assert await _real_lineage_rows_async(kg_runtime, board_id) == [
            (f"board:{board_id}", BOARD_RULE),
            (f"ideation:{IDEATION_ID}", IDEATION_RULE),
        ]

        audit.fail_staging = True
        with pytest.raises(
            RuntimeError,
            match="injected audit/UOW staging failure",
        ):
            await _commit_lineage_candidates(
                board_id=board_id,
                parent_kind="refinement",
                parent_id=REFINEMENT_ID,
            )

        assert await _real_lineage_rows_async(kg_runtime, board_id) == [
            (f"board:{board_id}", BOARD_RULE),
            (f"ideation:{IDEATION_ID}", IDEATION_RULE),
        ]

        audit.fail_staging = False
        await _commit_lineage_candidates(
            board_id=board_id,
            parent_kind="refinement",
            parent_id=REFINEMENT_ID,
        )
        assert await _real_lineage_rows_async(kg_runtime, board_id) == [
            (f"board:{board_id}", BOARD_RULE),
            (f"refinement:{REFINEMENT_ID}", REFINEMENT_RULE),
        ]
    finally:
        kg_runtime.close_all_connections(board_id)


@pytest.mark.asyncio
async def test_clear_audit_failure_restores_old_parent_and_retry_converges(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kg_registry_testing import configure_real_graph_test_kg_registry
    from okto_pulse.community.adapters import kg_runtime
    from okto_pulse.core.kg import primitives

    board_id = "lineage-clear-uow-compensation"
    audit = _ToggleAuditRepository()
    monkeypatch.setattr(kg_runtime, "_kg_base_dir", lambda: tmp_path / "kg")
    kg_runtime.reset_bootstrap_cache_for_tests()
    configure_real_graph_test_kg_registry(audit_repo=audit)

    async def _healthy(*_args, **_kwargs) -> str:
        return "healthy"

    monkeypatch.setattr(primitives, "_resolve_commit_kg_health_state", _healthy)

    try:
        kg_runtime.bootstrap_board_graph(board_id)
        await _commit_lineage_candidates(
            board_id=board_id,
            parent_kind="ideation",
            parent_id=IDEATION_ID,
        )
        expected_with_parent = [
            (f"board:{board_id}", BOARD_RULE),
            (f"ideation:{IDEATION_ID}", IDEATION_RULE),
        ]
        assert (
            await _real_lineage_rows_async(kg_runtime, board_id)
            == expected_with_parent
        )

        audit.fail_staging = True
        with pytest.raises(
            RuntimeError,
            match="injected audit/UOW staging failure",
        ):
            await _commit_lineage_clear(board_id=board_id)
        assert (
            await _real_lineage_rows_async(kg_runtime, board_id)
            == expected_with_parent
        )

        audit.fail_staging = False
        await _commit_lineage_clear(board_id=board_id)
        expected_without_parent = [(f"board:{board_id}", BOARD_RULE)]
        assert (
            await _real_lineage_rows_async(kg_runtime, board_id)
            == expected_without_parent
        )

        await _commit_lineage_clear(board_id=board_id)
        assert (
            await _real_lineage_rows_async(kg_runtime, board_id)
            == expected_without_parent
        )
    finally:
        kg_runtime.close_all_connections(board_id)


@pytest.mark.asyncio
async def test_commit_restore_failure_aborts_generic_cleanup_and_keeps_new_parent(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kg_registry_testing import configure_real_graph_test_kg_registry
    from okto_pulse.community.adapters import kg_runtime
    from okto_pulse.core.kg import primitives
    from okto_pulse.core.kg.interfaces import get_kg_registry

    board_id = "lineage-restore-failure"
    audit = _ToggleAuditRepository()
    monkeypatch.setattr(kg_runtime, "_kg_base_dir", lambda: tmp_path / "kg")
    kg_runtime.reset_bootstrap_cache_for_tests()
    configure_real_graph_test_kg_registry(audit_repo=audit)

    async def _healthy(*_args, **_kwargs) -> str:
        return "healthy"

    monkeypatch.setattr(primitives, "_resolve_commit_kg_health_state", _healthy)

    try:
        kg_runtime.bootstrap_board_graph(board_id)
        await _commit_lineage_candidates(
            board_id=board_id,
            parent_kind="ideation",
            parent_id=IDEATION_ID,
        )

        registry = get_kg_registry()
        original_transaction = registry.graph_transaction
        assert original_transaction is not None

        class _FailSecondBeginCompensation:
            def __init__(self) -> None:
                self.begin_calls = 0

            async def begin(self, requested_board_id: str):
                scope = await original_transaction.begin(requested_board_id)
                self.begin_calls += 1
                if self.begin_calls == 2:

                    def _fail_restore(_receipt) -> None:
                        raise RuntimeError("injected restore-old failure")

                    scope.compensate_spec_lineage_parent = (  # type: ignore[method-assign]
                        _fail_restore
                    )
                return scope

        failing_transaction = _FailSecondBeginCompensation()
        registry.graph_transaction = failing_transaction
        audit.fail_staging = True

        with pytest.raises(primitives.KGPrimitiveError) as excinfo:
            await _commit_lineage_candidates(
                board_id=board_id,
                parent_kind="refinement",
                parent_id=REFINEMENT_ID,
            )
        assert excinfo.value.code == "graph_compensation_failed"
        assert excinfo.value.details == {
            "failure_stage": "audit_outbox_stage",
            "staging_failure_type": "RuntimeError",
            "compensation_failure_type": "CompensationError",
        }

        assert failing_transaction.begin_calls == 2
        # The old edge was already deleted by the successful graph phase.
        # Restore then failed, so compensation must retain the replacement and
        # abort generic session cleanup instead of producing zero parents.
        assert await _real_lineage_rows_async(kg_runtime, board_id) == [
            (f"board:{board_id}", BOARD_RULE),
            (f"refinement:{REFINEMENT_ID}", REFINEMENT_RULE),
        ]

        registry.graph_transaction = original_transaction
        audit.fail_staging = False
        await _commit_lineage_candidates(
            board_id=board_id,
            parent_kind="refinement",
            parent_id=REFINEMENT_ID,
        )
        assert await _real_lineage_rows_async(kg_runtime, board_id) == [
            (f"board:{board_id}", BOARD_RULE),
            (f"refinement:{REFINEMENT_ID}", REFINEMENT_RULE),
        ]
    finally:
        kg_runtime.close_all_connections(board_id)


@pytest.mark.asyncio
async def test_real_worker_lifecycle_explicit_clear_removes_only_spec_parent(
    db_factory,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import uuid

    from kg_registry_testing import (
        configure_real_graph_and_data_test_kg_registry,
    )
    from okto_pulse.community.adapters import kg_runtime
    from okto_pulse.core.application.processors import consolidation
    from okto_pulse.core.application.processors.consolidation import (
        ConsolidationProcessor,
    )
    from okto_pulse.core.domain.enums import (
        IdeationStatus,
        RefinementStatus,
        SpecStatus,
    )
    from okto_pulse.core.kg.interfaces import get_kg_registry
    from sqlalchemy_test_models import (
        Board,
        ConsolidationQueue,
        Ideation,
        Refinement,
        Spec,
    )

    board_id = str(uuid.uuid4())
    ideation_id = str(uuid.uuid4())
    refinement_id = str(uuid.uuid4())
    spec_id = str(uuid.uuid4())
    monkeypatch.setattr(kg_runtime, "_kg_base_dir", lambda: tmp_path / "kg")
    kg_runtime.reset_bootstrap_cache_for_tests()
    configure_real_graph_and_data_test_kg_registry(db_factory)
    lifecycle_mutations: list[str] = []
    original_lifecycle = consolidation._apply_board_graph_lifecycle_after_commit

    def _record_lifecycle(**kwargs):
        lifecycle_mutations.append(str(kwargs["mutation_ref"]))
        return original_lifecycle(**kwargs)

    monkeypatch.setattr(
        consolidation,
        "_apply_board_graph_lifecycle_after_commit",
        _record_lifecycle,
    )

    try:
        kg_runtime.bootstrap_board_graph(board_id)
        async with db_factory() as db:
            await db.execute(ConsolidationQueue.__table__.delete())
            db.add(Board(id=board_id, name="Lineage worker", owner_id="owner"))
            db.add(
                Ideation(
                    id=ideation_id,
                    board_id=board_id,
                    title="Parent ideation",
                    description="Initial lineage parent.",
                    status=IdeationStatus.DONE,
                    created_by="owner",
                )
            )
            db.add(
                Refinement(
                    id=refinement_id,
                    ideation_id=ideation_id,
                    board_id=board_id,
                    title="Parent refinement",
                    description="Replacement lineage parent.",
                    decisions=["Use the refined lineage."],
                    status=RefinementStatus.DONE,
                    created_by="owner",
                )
            )
            db.add(
                Spec(
                    id=spec_id,
                    board_id=board_id,
                    ideation_id=ideation_id,
                    refinement_id=None,
                    title="Worker relink spec",
                    description="Exercises persisted exclusive parent swaps.",
                    context="Initial ideation lineage.",
                    status=SpecStatus.DRAFT,
                    created_by="owner",
                )
            )
            db.add(
                ConsolidationQueue(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id=spec_id,
                    priority="high",
                    source="test:initial-lineage",
                    status="pending",
                    attempts=0,
                )
            )
            await db.commit()

        worker = ConsolidationProcessor(db_factory, batch_size=1)
        assert await worker.process_batch() == 1
        initial_rows = _real_lineage_rows(
            kg_runtime,
            board_id,
            spec_id=spec_id,
        )
        assert len(lifecycle_mutations) == 1
        assert any(
            target == f"ideation:{ideation_id}"
            and rule.startswith("belongs_to/spec_to_ideation@")
            for target, rule in initial_rows
        )
        assert any(target == f"board:{board_id}" for target, _rule in initial_rows)

        spec_graph_id = _real_node_id_by_source_ref(
            kg_runtime,
            board_id,
            node_type="Entity",
            source_ref=f"spec:{spec_id}",
        )
        legacy_parent_id = f"legacy_{spec_id[:8]}_parent"
        child_id = f"requirement_{spec_id[:8]}_child"
        registry = get_kg_registry()
        scope = await registry.graph_transaction.begin(board_id)
        scope.create_node(
            "Entity",
            legacy_parent_id,
            {"source_artifact_ref": "legacy:ambiguous-parent"},
            source_session_id="seed-legacy",
        )
        scope.create_node(
            "Requirement",
            child_id,
            {"source_artifact_ref": "requirement:child"},
            source_session_id="seed-child",
        )
        assert scope.create_edge(
            "belongs_to",
            "Entity",
            "Entity",
            spec_graph_id,
            legacy_parent_id,
            {
                **_edge_attrs("legacy_pre_v2", "seed-legacy"),
                "layer": "legacy",
            },
        )
        assert scope.create_edge(
            "belongs_to",
            "Requirement",
            "Entity",
            child_id,
            spec_graph_id,
            _edge_attrs(CHILD_RULE, "seed-child"),
        )
        await scope.commit()

        async with db_factory() as db:
            spec = await db.get(Spec, spec_id)
            assert spec is not None
            spec.ideation_id = None
            spec.refinement_id = None
            db.add(
                ConsolidationQueue(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id=spec_id,
                    priority="high",
                    source="event:spec.lineage_cleared",
                    triggered_by_event="spec.semantic_changed",
                    status="pending",
                    attempts=0,
                )
            )
            await db.commit()

        assert await worker.process_batch() == 1
        cleared_rows = _real_lineage_rows(
            kg_runtime,
            board_id,
            spec_id=spec_id,
        )
        assert len(lifecycle_mutations) == 2
        assert not any(
            rule.startswith("belongs_to/spec_to_ideation@")
            or rule.startswith("belongs_to/spec_to_refinement@")
            for _target, rule in cleared_rows
        )
        assert any(target == f"board:{board_id}" for target, _rule in cleared_rows)
        assert (
            "legacy:ambiguous-parent",
            "legacy_pre_v2",
        ) in cleared_rows
        assert _real_edge_exists(
            kg_runtime,
            board_id,
            from_type="Requirement",
            from_id=child_id,
            to_type="Entity",
            to_id=spec_graph_id,
            rule_id=CHILD_RULE,
        )

        async with db_factory() as db:
            db.add(
                ConsolidationQueue(
                    board_id=board_id,
                    artifact_type="spec",
                    artifact_id=spec_id,
                    priority="high",
                    source="test:idempotent-retry",
                    status="pending",
                    attempts=0,
                )
            )
            await db.commit()

        assert await worker.process_batch() == 1
        assert len(lifecycle_mutations) == 3
        assert (
            _real_lineage_rows(kg_runtime, board_id, spec_id=spec_id)
            == cleared_rows
        )
        assert _real_edge_exists(
            kg_runtime,
            board_id,
            from_type="Requirement",
            from_id=child_id,
            to_type="Entity",
            to_id=spec_graph_id,
            rule_id=CHILD_RULE,
        )
    finally:
        kg_runtime.close_all_connections(board_id)
