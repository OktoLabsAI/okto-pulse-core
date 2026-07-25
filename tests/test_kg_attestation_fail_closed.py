"""Fail-closed ordering and lifecycle probes for unchanged re-attestation."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
import threading
from types import SimpleNamespace

import pytest

import okto_pulse.core.kg.primitives as primitives
import okto_pulse.core.services.kg_health_service as health_service
from okto_pulse.core.kg.guarded_write import GuardedWriteError
from okto_pulse.core.kg.providers.testing.memory import InMemorySessionStore
from okto_pulse.core.kg.schemas import (
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    NodeCandidate,
    ProposeReconciliationRequest,
)
from okto_pulse.core.kg.session_manager import compute_content_hash
from okto_pulse.core.kg.write_barrier import (
    BarrierMode,
    get_barrier_mode,
    require_write_token,
    set_barrier_mode,
    under_safe_write,
)


BOARD_ID = "board-attestation-fail-closed"
ARTIFACT_ID = "artifact-1"
AGENT_ID = "system:attestation-test"
RAW_CONTENT = "unchanged assertion"


def _candidate(candidate_id: str = "candidate-1") -> NodeCandidate:
    return NodeCandidate(
        candidate_id=candidate_id,
        node_type="Entity",
        title="Stable entity",
        content=RAW_CONTENT,
        source_artifact_ref=f"spec:{ARTIFACT_ID}",
    )


class _AuditRepository:
    def __init__(self, *, latest=None, refs=()):
        self.latest = latest
        self.refs = list(refs)
        self.latest_calls = 0
        self.ref_calls = 0

    async def get_latest_for_artifact(
        self,
        board_id: str,
        artifact_id: str,
        *,
        artifact_type: str,
    ):
        assert board_id == BOARD_ID
        assert artifact_id == ARTIFACT_ID
        assert artifact_type == "spec"
        self.latest_calls += 1
        return self.latest

    async def get_node_refs_by_session(self, session_id: str):
        assert session_id == "origin-session"
        self.ref_calls += 1
        return list(self.refs)


class _CountingSessionStore(InMemorySessionStore):
    def __init__(self, *, fail_create: bool = False):
        super().__init__()
        self.create_calls = 0
        self.fail_create = fail_create

    async def create(self, **kwargs):
        self.create_calls += 1
        if self.fail_create:
            raise RuntimeError("session store unavailable")
        return await super().create(**kwargs)


class _GraphScope:
    def __init__(self):
        self.attestation_count = 1
        self.increment_calls = 0
        self.commit_calls = 0

    def find_node_types(self, node_id: str):
        assert node_id == "graph-node-1"
        return ("Entity",)

    def increment_attestation(
        self,
        node_type: str,
        node_id: str,
        *,
        attested_at: str,
    ):
        assert node_type == "Entity"
        assert node_id == "graph-node-1"
        assert attested_at
        self.increment_calls += 1
        self.attestation_count += 1

    async def commit(self):
        self.commit_calls += 1

    async def rollback(self):
        return None


class _GraphTransaction:
    def __init__(self, scope: _GraphScope):
        self.scope = scope
        self.begin_calls = 0

    async def begin(self, board_id: str):
        assert board_id == BOARD_ID
        self.begin_calls += 1
        return self.scope


def _latest_audit():
    return SimpleNamespace(
        session_id="origin-session",
        content_hash=compute_content_hash(RAW_CONTENT, ARTIFACT_ID, BOARD_ID),
    )


def _registry(store, audit_repo, *, graph_transaction=None):
    return SimpleNamespace(
        audit_repo=audit_repo,
        graph_transaction=graph_transaction,
        require_session_store=lambda: store,
    )


def _healthy_payload(state: str):
    async def _health(_board_id, _db, scheduler_control=None):
        return {
            "overall_state": state,
            "graph_state": state,
            "discovery_state": "healthy",
        }

    return _health


def _fixed_health_payload(payload: dict):
    async def _health(_board_id, _db, scheduler_control=None):
        return dict(payload)

    return _health


@pytest.fixture(autouse=True)
def _reset_health_cache():
    primitives.reset_commit_health_cache_for_tests()
    yield
    primitives.reset_commit_health_cache_for_tests()


@pytest.fixture
def strict_barrier_mode():
    previous_mode = get_barrier_mode()
    set_barrier_mode(BarrierMode.STRICT)
    try:
        yield
    finally:
        set_barrier_mode(previous_mode)


@pytest.mark.asyncio
async def test_duplicate_deterministic_candidate_precedes_audit_health_and_create(
    monkeypatch,
):
    store = _CountingSessionStore()

    class _ForbiddenAudit:
        async def get_latest_for_artifact(self, *args, **kwargs):
            raise AssertionError("audit lookup must not run")

    registry = _registry(store, _ForbiddenAudit())
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)

    async def _forbidden_health(*args, **kwargs):
        raise AssertionError("health lookup must not run")

    monkeypatch.setattr(health_service, "get_kg_health", _forbidden_health)
    duplicate = _candidate("duplicate")

    with pytest.raises(primitives.KGPrimitiveError) as exc_info:
        await primitives.begin_consolidation(
            BeginConsolidationRequest(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id=ARTIFACT_ID,
                raw_content=RAW_CONTENT,
                deterministic_candidates=[duplicate, duplicate],
            ),
            agent_id=AGENT_ID,
            db=object(),
        )

    assert exc_info.value.code == "duplicate_candidate_id"
    assert store.create_calls == 0
    assert await store.active_count() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("health_state", ["quarantined", "recovery_needed"])
async def test_degraded_begin_leaves_session_and_attestation_unchanged(
    monkeypatch,
    health_state,
):
    store = _CountingSessionStore()
    audit_repo = _AuditRepository(latest=_latest_audit())
    scope = _GraphScope()
    graph_transaction = _GraphTransaction(scope)
    registry = _registry(
        store,
        audit_repo,
        graph_transaction=graph_transaction,
    )
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        health_service,
        "get_kg_health",
        _healthy_payload(health_state),
    )

    with pytest.raises(primitives.KGPrimitiveError) as exc_info:
        await primitives.begin_consolidation(
            BeginConsolidationRequest(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id=ARTIFACT_ID,
                raw_content=RAW_CONTENT,
                deterministic_candidates=[_candidate()],
            ),
            agent_id=AGENT_ID,
            db=object(),
        )

    assert exc_info.value.code == "kg_graph_degraded"
    assert store.create_calls == 0
    assert await store.active_count() == 0
    assert graph_transaction.begin_calls == 0
    assert scope.attestation_count == 1


@pytest.mark.asyncio
async def test_hidden_graph_quarantine_blocks_begin_before_session_or_attestation(
    monkeypatch,
):
    store = _CountingSessionStore()
    audit_repo = _AuditRepository(latest=_latest_audit())
    scope = _GraphScope()
    graph_transaction = _GraphTransaction(scope)
    registry = _registry(
        store,
        audit_repo,
        graph_transaction=graph_transaction,
    )
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        health_service,
        "get_kg_health",
        _fixed_health_payload(
            {
                "overall_state": "recovery_needed",
                "graph_state": "quarantined",
                "discovery_state": "healthy",
                "total_nodes": 0,
            }
        ),
    )

    with pytest.raises(primitives.KGPrimitiveError) as exc_info:
        await primitives.begin_consolidation(
            BeginConsolidationRequest(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id=ARTIFACT_ID,
                raw_content=RAW_CONTENT,
                deterministic_candidates=[_candidate()],
            ),
            agent_id=AGENT_ID,
            db=object(),
        )

    assert exc_info.value.code == "kg_graph_degraded"
    assert exc_info.value.details["kg_health_state"] == "quarantined"
    assert store.create_calls == 0
    assert await store.active_count() == 0
    assert graph_transaction.begin_calls == 0
    assert scope.attestation_count == 1


@pytest.mark.asyncio
async def test_store_create_failure_cannot_publish_count_only_attestation(
    monkeypatch,
):
    store = _CountingSessionStore(fail_create=True)
    audit_repo = _AuditRepository(latest=_latest_audit())
    scope = _GraphScope()
    graph_transaction = _GraphTransaction(scope)
    registry = _registry(
        store,
        audit_repo,
        graph_transaction=graph_transaction,
    )
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        health_service,
        "get_kg_health",
        _healthy_payload("healthy"),
    )

    with pytest.raises(RuntimeError, match="session store unavailable"):
        await primitives.begin_consolidation(
            BeginConsolidationRequest(
                board_id=BOARD_ID,
                artifact_type="spec",
                artifact_id=ARTIFACT_ID,
                raw_content=RAW_CONTENT,
                deterministic_candidates=[_candidate()],
            ),
            agent_id=AGENT_ID,
            db=object(),
        )

    assert store.create_calls == 1
    assert await store.active_count() == 0
    assert graph_transaction.begin_calls == 0
    assert scope.attestation_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("health_state", ["quarantined", "recovery_needed"])
async def test_degraded_propose_leaves_existing_session_and_graph_unchanged(
    monkeypatch,
    health_state,
):
    store = _CountingSessionStore()
    audit_repo = _AuditRepository(
        latest=_latest_audit(),
        refs=[
            SimpleNamespace(
                graph_node_type="Entity",
                graph_node_id="graph-node-1",
            )
        ],
    )
    scope = _GraphScope()
    graph_transaction = _GraphTransaction(scope)
    registry = _registry(
        store,
        audit_repo,
        graph_transaction=graph_transaction,
    )
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)

    begin = await primitives.begin_consolidation(
        BeginConsolidationRequest(
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id=ARTIFACT_ID,
            raw_content=RAW_CONTENT,
            deterministic_candidates=[_candidate()],
        ),
        agent_id=AGENT_ID,
        db=None,
    )
    session = await store.get(begin.session_id)
    assert session is not None
    expires_at = session.expires_at
    monkeypatch.setattr(
        health_service,
        "get_kg_health",
        _healthy_payload(health_state),
    )

    def _forbidden_guard(*args, **kwargs):
        raise AssertionError("write boundary must not be entered")

    monkeypatch.setattr(primitives, "guarded_board_write", _forbidden_guard)

    with pytest.raises(primitives.KGPrimitiveError) as exc_info:
        await primitives.propose_reconciliation(
            ProposeReconciliationRequest(session_id=begin.session_id),
            agent_id=AGENT_ID,
            db=object(),
        )

    assert exc_info.value.code == "kg_graph_degraded"
    assert session.count_only_attested is False
    assert session.reconciliation_hints == {}
    assert session.expires_at == expires_at
    assert not hasattr(session, "_count_only_attestation_progress")
    assert graph_transaction.begin_calls == 0
    assert scope.attestation_count == 1


@pytest.mark.asyncio
async def test_hidden_discovery_quarantine_blocks_propose_before_graph_mutation(
    monkeypatch,
):
    store = _CountingSessionStore()
    audit_repo = _AuditRepository(
        latest=_latest_audit(),
        refs=[
            SimpleNamespace(
                graph_node_type="Entity",
                graph_node_id="graph-node-1",
            )
        ],
    )
    scope = _GraphScope()
    graph_transaction = _GraphTransaction(scope)
    registry = _registry(
        store,
        audit_repo,
        graph_transaction=graph_transaction,
    )
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)

    begin = await primitives.begin_consolidation(
        BeginConsolidationRequest(
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id=ARTIFACT_ID,
            raw_content=RAW_CONTENT,
            deterministic_candidates=[_candidate()],
        ),
        agent_id=AGENT_ID,
        db=None,
    )
    session = await store.get(begin.session_id)
    assert session is not None
    expires_at = session.expires_at
    monkeypatch.setattr(
        health_service,
        "get_kg_health",
        _fixed_health_payload(
            {
                "overall_state": "recovery_needed",
                "graph_state": "recovery_needed",
                "discovery_state": "quarantined",
                "total_nodes": 0,
            }
        ),
    )

    def _forbidden_guard(*args, **kwargs):
        raise AssertionError("write boundary must not be entered")

    monkeypatch.setattr(primitives, "guarded_board_write", _forbidden_guard)

    with pytest.raises(primitives.KGPrimitiveError) as exc_info:
        await primitives.propose_reconciliation(
            ProposeReconciliationRequest(session_id=begin.session_id),
            agent_id=AGENT_ID,
            db=object(),
        )

    assert exc_info.value.code == "kg_graph_degraded"
    assert exc_info.value.details["kg_health_state"] == "quarantined"
    assert session.count_only_attested is False
    assert session.reconciliation_hints == {}
    assert session.expires_at == expires_at
    assert not hasattr(session, "_count_only_attestation_progress")
    assert graph_transaction.begin_calls == 0
    assert scope.attestation_count == 1


@pytest.mark.asyncio
async def test_propose_retries_lifecycle_failure_without_double_attestation(
    monkeypatch,
    strict_barrier_mode,
):
    del strict_barrier_mode
    event_loop_thread = threading.get_ident()
    store = _CountingSessionStore()
    audit_repo = _AuditRepository(
        latest=_latest_audit(),
        refs=[
            SimpleNamespace(
                graph_node_type="Entity",
                graph_node_id="graph-node-1",
            )
        ],
    )
    scope = _GraphScope()
    graph_transaction = _GraphTransaction(scope)
    registry = _registry(
        store,
        audit_repo,
        graph_transaction=graph_transaction,
    )
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        health_service,
        "get_kg_health",
        _healthy_payload("healthy"),
    )

    ensure_calls = {"count": 0}
    ensure_threads: list[int] = []
    boundary_calls = {"count": 0}

    class _Lease:
        def ensure_durable(self):
            ensure_threads.append(threading.get_ident())
            assert require_write_token(BOARD_ID) is not None
            ensure_calls["count"] += 1
            if ensure_calls["count"] == 1:
                raise GuardedWriteError(
                    "fsync_failed",
                    "injected lifecycle failure",
                    retryable=True,
                )

    @contextmanager
    def _guarded_write(
        board_id: str,
        *,
        operation: str,
        owner_id: str,
        mutation_ref: str,
    ):
        assert board_id == BOARD_ID
        assert operation == "count_only_attestation"
        assert owner_id == AGENT_ID
        assert mutation_ref.endswith(":count_only_attestation")
        boundary_calls["count"] += 1
        with under_safe_write(board_id, "count-only-test-token", operation):
            yield _Lease()

    monkeypatch.setattr(primitives, "guarded_board_write", _guarded_write)

    begin = await primitives.begin_consolidation(
        BeginConsolidationRequest(
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id=ARTIFACT_ID,
            raw_content=RAW_CONTENT,
            deterministic_candidates=[_candidate()],
        ),
        agent_id=AGENT_ID,
        db=None,
    )
    session = await store.get(begin.session_id)
    assert session is not None
    assert session.count_only_attested is False

    with pytest.raises(primitives.KGPrimitiveError) as exc_info:
        await primitives.propose_reconciliation(
            ProposeReconciliationRequest(session_id=begin.session_id),
            agent_id=AGENT_ID,
            db=object(),
        )

    assert exc_info.value.code == "count_only_attestation_failed"
    assert exc_info.value.details["failure_code"] == "fsync_failed"
    assert session.count_only_attested is False
    assert session.reconciliation_hints == {}
    assert scope.attestation_count == 2
    assert graph_transaction.begin_calls == 1

    response = await primitives.propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=AGENT_ID,
        db=object(),
    )

    assert response.hints[0].operation == "NOOP"
    assert session.count_only_attested is True
    assert scope.attestation_count == 2
    assert graph_transaction.begin_calls == 1
    assert boundary_calls["count"] == 2
    assert ensure_calls["count"] == 2
    assert ensure_threads
    assert all(thread_id != event_loop_thread for thread_id in ensure_threads)


@pytest.mark.asyncio
async def test_cancelled_propose_drains_attestation_and_does_not_double_bump(
    monkeypatch,
    strict_barrier_mode,
):
    del strict_barrier_mode
    event_loop_thread = threading.get_ident()
    store = _CountingSessionStore()
    audit_repo = _AuditRepository(
        latest=_latest_audit(),
        refs=[
            SimpleNamespace(
                graph_node_type="Entity",
                graph_node_id="graph-node-1",
            )
        ],
    )
    increment_entered = threading.Event()
    release_increment = threading.Event()

    class _BlockingGraphScope(_GraphScope):
        def increment_attestation(
            self,
            node_type: str,
            node_id: str,
            *,
            attested_at: str,
        ):
            increment_entered.set()
            assert release_increment.wait(timeout=5)
            return super().increment_attestation(
                node_type,
                node_id,
                attested_at=attested_at,
            )

    scope = _BlockingGraphScope()
    graph_transaction = _GraphTransaction(scope)
    registry = _registry(
        store,
        audit_repo,
        graph_transaction=graph_transaction,
    )
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        health_service,
        "get_kg_health",
        _healthy_payload("healthy"),
    )

    boundary_calls = {"entered": 0, "exited": 0}
    ensure_calls = {"count": 0}
    ensure_threads: list[int] = []

    class _Lease:
        def ensure_durable(self):
            ensure_threads.append(threading.get_ident())
            assert require_write_token(BOARD_ID) is not None
            ensure_calls["count"] += 1

    @contextmanager
    def _guarded_write(
        board_id: str,
        *,
        operation: str,
        owner_id: str,
        mutation_ref: str,
    ):
        assert board_id == BOARD_ID
        assert operation == "count_only_attestation"
        assert owner_id == AGENT_ID
        assert mutation_ref.endswith(":count_only_attestation")
        boundary_calls["entered"] += 1
        try:
            with under_safe_write(
                board_id,
                "cancel-test-token",
                operation,
            ):
                yield _Lease()
        finally:
            boundary_calls["exited"] += 1

    monkeypatch.setattr(primitives, "guarded_board_write", _guarded_write)

    begin = await primitives.begin_consolidation(
        BeginConsolidationRequest(
            board_id=BOARD_ID,
            artifact_type="spec",
            artifact_id=ARTIFACT_ID,
            raw_content=RAW_CONTENT,
            deterministic_candidates=[_candidate()],
        ),
        agent_id=AGENT_ID,
        db=None,
    )
    session = await store.get(begin.session_id)
    assert session is not None

    propose_task = asyncio.create_task(
        primitives.propose_reconciliation(
            ProposeReconciliationRequest(session_id=begin.session_id),
            agent_id=AGENT_ID,
            db=object(),
        )
    )
    assert await asyncio.to_thread(increment_entered.wait, 2)

    propose_task.cancel()
    await asyncio.sleep(0.02)
    assert not propose_task.done()

    release_increment.set()
    with pytest.raises(asyncio.CancelledError):
        await propose_task

    assert scope.attestation_count == 2
    assert scope.increment_calls == 1
    assert scope.commit_calls == 1
    assert graph_transaction.begin_calls == 1
    assert ensure_calls["count"] == 1
    assert ensure_threads
    assert all(thread_id != event_loop_thread for thread_id in ensure_threads)
    assert boundary_calls == {"entered": 1, "exited": 1}
    assert session.count_only_attested is True
    assert not hasattr(session, "_count_only_attestation_progress")
    assert session.reconciliation_hints == {}

    response = await primitives.propose_reconciliation(
        ProposeReconciliationRequest(session_id=begin.session_id),
        agent_id=AGENT_ID,
        db=object(),
    )
    assert response.hints[0].operation == "NOOP"
    assert session.reconciliation_hints
    assert scope.attestation_count == 2
    assert scope.increment_calls == 1
    assert graph_transaction.begin_calls == 1
    assert ensure_calls["count"] == 1
    assert boundary_calls == {"entered": 1, "exited": 1}


@pytest.mark.asyncio
async def test_db_none_lane_does_not_mask_board_not_found_at_real_commit(
    monkeypatch,
):
    store = _CountingSessionStore()
    session = await store.create(
        session_id="commit-session",
        board_id=BOARD_ID,
        artifact_id=ARTIFACT_ID,
        artifact_type="spec",
        agent_id=AGENT_ID,
        raw_content=RAW_CONTENT,
    )
    session.node_candidates["candidate-1"] = _candidate()
    registry = _registry(store, audit_repo=None)
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)

    health_calls = {"count": 0}

    async def _healthy_then_fail(_board_id, _db, scheduler_control=None):
        health_calls["count"] += 1
        if health_calls["count"] == 1:
            return {
                "overall_state": "healthy",
                "graph_state": "healthy",
                "discovery_state": "healthy",
                "total_nodes": 1,
            }
        raise health_service.BoardNotFoundError("board not found")

    monkeypatch.setattr(
        health_service,
        "get_kg_health",
        _healthy_then_fail,
    )
    assert (
        await primitives._resolve_commit_kg_health_state(BOARD_ID, None)
        == "healthy"
    )
    assert health_calls["count"] == 0
    assert (
        await primitives._resolve_commit_kg_health_state(BOARD_ID, object())
        == "healthy"
    )

    callback_calls = {"count": 0}

    def _forbidden_graph_callback(*args, **kwargs):
        callback_calls["count"] += 1
        raise AssertionError("graph callback must not run")

    monkeypatch.setattr(primitives, "_do_graph_commit", _forbidden_graph_callback)

    previous_mode = get_barrier_mode()
    set_barrier_mode(BarrierMode.STRICT)
    try:
        with under_safe_write(BOARD_ID, "strict-token", "test"):
            with pytest.raises(primitives.KGPrimitiveError) as exc_info:
                await primitives.commit_consolidation(
                    CommitConsolidationRequest(session_id=session.session_id),
                    agent_id=AGENT_ID,
                    db=object(),
                )
    finally:
        set_barrier_mode(previous_mode)

    assert exc_info.value.code == "kg_graph_degraded"
    assert health_calls["count"] == 2
    assert callback_calls["count"] == 0
    assert session.count_only_attested is False
    assert session.reconciliation_hints == {}
    assert await store.active_count() == 1


@pytest.mark.asyncio
async def test_less_severe_overall_blocks_commit_before_graph_callback(
    monkeypatch,
):
    store = _CountingSessionStore()
    session = await store.create(
        session_id="contradictory-health-commit",
        board_id=BOARD_ID,
        artifact_id=ARTIFACT_ID,
        artifact_type="spec",
        agent_id=AGENT_ID,
        raw_content=RAW_CONTENT,
    )
    session.node_candidates["candidate-1"] = _candidate()
    registry = _registry(store, audit_repo=None)
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        health_service,
        "get_kg_health",
        _fixed_health_payload(
            {
                "overall_state": "healthy",
                "graph_state": "recovery_needed",
                "discovery_state": "healthy",
                "total_nodes": 0,
            }
        ),
    )

    callback_calls = {"count": 0}

    def _forbidden_graph_callback(*args, **kwargs):
        callback_calls["count"] += 1
        raise AssertionError("graph callback must not run")

    monkeypatch.setattr(primitives, "_do_graph_commit", _forbidden_graph_callback)

    with pytest.raises(primitives.KGPrimitiveError) as exc_info:
        await primitives.commit_consolidation(
            CommitConsolidationRequest(session_id=session.session_id),
            agent_id=AGENT_ID,
            db=object(),
        )

    assert exc_info.value.code == "kg_graph_degraded"
    assert exc_info.value.details["kg_health_state"] == "recovery_needed"
    assert callback_calls["count"] == 0
    assert session.count_only_attested is False
    assert session.reconciliation_hints == {}
    assert await store.active_count() == 1
