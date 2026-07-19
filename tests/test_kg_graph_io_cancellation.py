"""Cancellation ownership for native graph IO."""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import primitives
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
        board_id="board-cancel-atomic",
        artifact_id="artifact-cancel-atomic",
        content_hash="hash-cancel-atomic",
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

    async def require_open_session(session_id: str, agent_id: str):
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
        return {}, counters, [record], datetime.now(timezone.utc), {}

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
async def test_commit_consolidation_remains_cancellable_while_waiting_for_session_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = SimpleNamespace(
        board_id="board-lock-wait",
        node_candidates={},
        lock=asyncio.Lock(),
    )
    graph_started = False

    async def require_open_session(session_id: str, agent_id: str):
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
