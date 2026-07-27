"""Cancellation ownership for native graph IO."""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import primitives
from okto_pulse.core.kg.interfaces.graph_transaction import (
    SpecLineageParentIntent,
)
from okto_pulse.core.kg.primitives import (
    _run_cancellation_atomic,
    _run_graph_io,
)
from okto_pulse.core.kg.schemas import (
    CommitConsolidationRequest,
    SessionStatus,
)


@pytest.mark.asyncio
async def test_untracked_fallback_drains_native_thread_before_cancellation() -> None:
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def native_operation() -> None:
        entered.set()
        assert release.wait(timeout=5)
        finished.set()

    parent = asyncio.create_task(_run_graph_io(native_operation))
    assert await asyncio.to_thread(entered.wait, 1)
    parent.cancel()
    await asyncio.sleep(0.02)
    assert not parent.done()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await parent
    assert finished.is_set()


@pytest.mark.asyncio
async def test_commit_critical_section_finishes_before_repeated_cancellation_returns(
) -> None:
    graph_entered = asyncio.Event()
    release_graph = asyncio.Event()
    stages: list[str] = []

    async def graph_audit_and_finalize() -> None:
        stages.append("graph_started")
        graph_entered.set()
        await release_graph.wait()
        stages.append("graph_committed")
        await asyncio.sleep(0)
        stages.append("audit_outbox_committed")
        await asyncio.sleep(0)
        stages.append("session_finalized")

    parent = asyncio.create_task(
        _run_cancellation_atomic(
            graph_audit_and_finalize(),
            task_name="test.commit_consolidation",
        )
    )
    await asyncio.wait_for(graph_entered.wait(), timeout=1)

    parent.cancel()
    await asyncio.sleep(0)
    parent.cancel()
    await asyncio.sleep(0)
    assert not parent.done()

    release_graph.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(parent, timeout=1)

    assert stages == [
        "graph_started",
        "graph_committed",
        "audit_outbox_committed",
        "session_finalized",
    ]


@pytest.mark.asyncio
async def test_commit_consolidation_drains_graph_audit_and_session_finalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph_entered = asyncio.Event()
    release_graph = asyncio.Event()
    stages: list[str] = []
    session = SimpleNamespace(
        session_id="session-cancel-atomic",
        board_id="board-cancel-atomic",
        artifact_id="artifact-cancel-atomic",
        artifact_type="spec",
        content_hash="hash-cancel-atomic",
        spec_lineage_parent_intent=SpecLineageParentIntent.PRESERVE,
        node_candidates={},
        edge_candidates={},
        reconciliation_hints={},
        lock=asyncio.Lock(),
        status=SessionStatus.OPEN,
        committed_graph_node_refs=[],
    )

    class SessionStore:
        async def remove(self, session_id: str) -> None:
            assert session_id == "session-cancel-atomic"
            assert session.status == SessionStatus.COMMITTED
            stages.append("session_finalized")

    class CacheBackend:
        def invalidate_board(self, board_id: str) -> None:
            assert board_id == session.board_id
            stages.append("cache_invalidated")

    registry = SimpleNamespace(
        require_embedding_provider=lambda: object(),
        require_session_store=lambda: SessionStore(),
        require_cache_backend=lambda: CacheBackend(),
    )

    async def require_open_session(
        session_id: str, agent_id: str, *, allow_pending_commit: bool = False
    ):
        assert allow_pending_commit is True
        assert session_id == "session-cancel-atomic"
        assert agent_id == "agent-cancel-atomic"
        return session

    async def validate_subtypes(_candidates: dict) -> None:
        return None

    async def resolve_health(board_id: str, _db) -> str:
        assert board_id == session.board_id
        return "healthy"

    async def graph_io(_func, *_args, **_kwargs):
        stages.append("graph_started")
        graph_entered.set()
        await release_graph.wait()
        stages.append("graph_committed")
        counters = SimpleNamespace(
            nodes_added=1,
            nodes_updated=0,
            nodes_superseded=0,
            edges_added=0,
            nodes_merged=0,
            nodes_noop=0,
            merge_audit_items=[],
        )
        record = SimpleNamespace(
            entity_id="node-cancel-atomic",
            entity_type="Learning",
            kind="node",
        )
        return {}, counters, [record], datetime.now(timezone.utc), {}, []

    async def commit_audit(*_args, **_kwargs) -> None:
        await asyncio.sleep(0)
        stages.append("audit_outbox_committed")

    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(primitives, "_require_open_session", require_open_session)
    monkeypatch.setattr(
        primitives,
        "_validate_subtype_declarations",
        validate_subtypes,
    )
    monkeypatch.setattr(
        primitives,
        "_resolve_commit_kg_health_state",
        resolve_health,
    )
    monkeypatch.setattr(primitives, "_run_graph_io", graph_io)
    monkeypatch.setattr(primitives, "_commit_audit_records", commit_audit)

    from okto_pulse.core.kg import write_barrier

    monkeypatch.setattr(write_barrier, "require_write_token", lambda _board: None)

    parent = asyncio.create_task(
        primitives.commit_consolidation(
            CommitConsolidationRequest(session_id="session-cancel-atomic"),
            agent_id="agent-cancel-atomic",
        )
    )
    await asyncio.wait_for(graph_entered.wait(), timeout=1)

    parent.cancel()
    await asyncio.sleep(0)
    parent.cancel()
    await asyncio.sleep(0)
    assert not parent.done()

    release_graph.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(parent, timeout=1)

    assert stages == [
        "graph_started",
        "graph_committed",
        "audit_outbox_committed",
        "session_finalized",
        "cache_invalidated",
    ]
    assert session.status == SessionStatus.COMMITTED


@pytest.mark.asyncio
async def test_audit_staging_failure_requests_graph_compensation_and_keeps_session_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stages: list[str] = []
    session = SimpleNamespace(
        session_id="session-audit-stage-failure",
        board_id="board-audit-stage-failure",
        artifact_id="artifact-audit-stage-failure",
        artifact_type="spec",
        content_hash="hash-audit-stage-failure",
        spec_lineage_parent_intent=SpecLineageParentIntent.PRESERVE,
        node_candidates={},
        edge_candidates={},
        reconciliation_hints={},
        lock=asyncio.Lock(),
        status=SessionStatus.OPEN,
        committed_graph_node_refs=[],
    )

    class AuditStagingFailure(RuntimeError):
        pass

    original_error = AuditStagingFailure("test-injected audit staging failure")

    class SessionStore:
        async def remove(self, _session_id: str) -> None:
            stages.append("session_finalized")

    class CacheBackend:
        def invalidate_board(self, _board_id: str) -> None:
            stages.append("cache_invalidated")

    registry = SimpleNamespace(
        require_embedding_provider=lambda: object(),
        require_session_store=lambda: SessionStore(),
        require_cache_backend=lambda: CacheBackend(),
    )

    async def require_open_session(
        _session_id: str,
        _agent_id: str,
        *,
        allow_pending_commit: bool = False,
    ):
        assert allow_pending_commit is True
        return session

    async def resolve_health(_board_id: str, _db) -> str:
        return "healthy"

    async def graph_io(operation, *_args, **_kwargs):
        if operation is primitives._do_graph_commit:
            stages.append("graph_committed")
            counters = SimpleNamespace(
                nodes_added=1,
                nodes_updated=0,
                nodes_superseded=0,
                edges_added=0,
                nodes_merged=0,
                nodes_noop=0,
                merge_audit_items=[],
            )
            record = SimpleNamespace(
                entity_id="node-audit-stage-failure",
                entity_type="Learning",
                kind="node",
            )
            return (
                {},
                counters,
                [record],
                datetime.now(timezone.utc),
                {},
                [],
            )
        assert operation is primitives._compensate_graph_writes
        stages.append("graph_compensation_requested")

    async def append_cognitive(*_args, **_kwargs) -> None:
        stages.append("cognitive_source_staged")

    async def stage_audit(*_args, **_kwargs) -> None:
        stages.append("audit_outbox_stage_failed")
        raise original_error

    monkeypatch.setattr(primitives, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(primitives, "_require_open_session", require_open_session)
    monkeypatch.setattr(
        primitives,
        "_validate_subtype_declarations",
        lambda _candidates: asyncio.sleep(0),
    )
    monkeypatch.setattr(
        primitives,
        "_resolve_commit_kg_health_state",
        resolve_health,
    )
    monkeypatch.setattr(primitives, "_run_graph_io", graph_io)
    monkeypatch.setattr(
        primitives,
        "_append_cognitive_source_records",
        append_cognitive,
    )
    monkeypatch.setattr(primitives, "_commit_audit_records", stage_audit)

    from okto_pulse.core.kg import write_barrier

    monkeypatch.setattr(write_barrier, "require_write_token", lambda _board: None)

    with pytest.raises(AuditStagingFailure) as excinfo:
        await primitives.commit_consolidation(
            CommitConsolidationRequest(session_id="session-audit-stage-failure"),
            agent_id="agent-audit-stage-failure",
        )

    assert excinfo.value is original_error
    assert stages == [
        "graph_committed",
        "cognitive_source_staged",
        "audit_outbox_stage_failed",
        "graph_compensation_requested",
    ]
    assert session.status == SessionStatus.OPEN
    assert session.committed_graph_node_refs == []


def test_compensation_clears_only_dangling_fresh_supersede_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[tuple[str, dict | None]] = []

    class Scope:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def execute(self, statement: str, params: dict | None = None):
            statements.append((statement, params))
            return SimpleNamespace()

    class GraphTransaction:
        async def begin(self, board_id: str):
            assert board_id == "board-supersede-compensation"
            return Scope()

    monkeypatch.setattr(
        primitives,
        "get_kg_registry",
        lambda: SimpleNamespace(graph_transaction=GraphTransaction()),
    )

    records = [
        SimpleNamespace(
            kind="node",
            entity_type="Decision",
            entity_id="decision-successor",
        ),
        SimpleNamespace(
            kind="edge",
            entity_type="supersedes",
            entity_id="decision-successor->decision-predecessor",
            from_id="decision-successor",
            to_id="decision-predecessor",
        ),
    ]

    primitives._compensate_graph_writes(
        "board-supersede-compensation",
        "session-supersede-compensation",
        records,
    )

    clear_statements = [
        (statement, params)
        for statement, params in statements
        if "SET n.superseded_by = NULL" in statement
    ]
    assert len(clear_statements) == 1
    statement, params = clear_statements[0]
    assert "n.superseded_by = $successor_id" in statement
    assert params == {
        "predecessor_id": "decision-predecessor",
        "successor_id": "decision-successor",
    }


@pytest.mark.asyncio
async def test_commit_consolidation_remains_cancellable_while_waiting_for_session_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        board_id="board-lock-wait",
        node_candidates={},
        lock=asyncio.Lock(),
    )
    graph_started = False

    async def require_open_session(
        session_id: str, agent_id: str, *, allow_pending_commit: bool = False
    ):
        assert allow_pending_commit is True
        assert session_id == "session-lock-wait"
        assert agent_id == "agent-lock-wait"
        return session

    async def validate_subtypes(_candidates: dict) -> None:
        return None

    async def graph_io(*_args, **_kwargs):
        nonlocal graph_started
        graph_started = True
        raise AssertionError("graph IO must not start while the session lock is held")

    monkeypatch.setattr(
        primitives,
        "get_kg_registry",
        lambda: SimpleNamespace(),
    )
    monkeypatch.setattr(primitives, "_require_open_session", require_open_session)
    monkeypatch.setattr(
        primitives,
        "_validate_subtype_declarations",
        validate_subtypes,
    )
    monkeypatch.setattr(primitives, "_run_graph_io", graph_io)

    from okto_pulse.core.kg import write_barrier

    monkeypatch.setattr(write_barrier, "require_write_token", lambda _board: None)

    await session.lock.acquire()
    parent = asyncio.create_task(
        primitives.commit_consolidation(
            CommitConsolidationRequest(session_id="session-lock-wait"),
            agent_id="agent-lock-wait",
        )
    )
    await asyncio.sleep(0)
    parent.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(parent, timeout=1)

    assert not graph_started
    assert session.lock.locked()
    session.lock.release()


@pytest.mark.asyncio
async def test_commit_does_not_inspect_orm_pending_change_collections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        session_id="session-pending-uow",
        board_id="board-pending-uow",
        node_candidates={},
        edge_candidates={},
        reconciliation_hints={},
        content_hash="hash-pending-uow",
        artifact_id="artifact-pending-uow",
        artifact_type="spec",
        spec_lineage_parent_intent=SpecLineageParentIntent.PRESERVE,
        lock=asyncio.Lock(),
        status=SessionStatus.OPEN,
    )
    db = SimpleNamespace(new=(object(),), dirty=(), deleted=())
    graph_started = False
    health_started = False

    async def require_open_session(
        _session_id: str,
        _agent_id: str,
        *,
        allow_pending_commit: bool = False,
    ):
        assert allow_pending_commit is True
        return session

    async def validate_subtypes(_candidates: dict) -> None:
        return None

    async def resolve_health(*_args, **_kwargs) -> str:
        nonlocal health_started
        health_started = True
        return "healthy"

    async def graph_io(*_args, **_kwargs):
        nonlocal graph_started
        graph_started = True
        raise AssertionError("stop after proving graph IO was reached")

    monkeypatch.setattr(
        primitives,
        "get_kg_registry",
        lambda: SimpleNamespace(require_embedding_provider=lambda: object()),
    )
    monkeypatch.setattr(primitives, "_require_open_session", require_open_session)
    monkeypatch.setattr(
        primitives,
        "_validate_subtype_declarations",
        validate_subtypes,
    )
    monkeypatch.setattr(
        primitives,
        "_resolve_commit_kg_health_state",
        resolve_health,
    )
    monkeypatch.setattr(primitives, "_run_graph_io", graph_io)

    from okto_pulse.core.kg import write_barrier

    monkeypatch.setattr(write_barrier, "require_write_token", lambda _board: None)

    with pytest.raises(primitives.KGPrimitiveError) as excinfo:
        await primitives.commit_consolidation(
            CommitConsolidationRequest(session_id="session-pending-uow"),
            agent_id="agent-pending-uow",
            db=db,
        )

    assert excinfo.value.code == "commit_failed"
    assert health_started
    assert graph_started
    assert db.new
