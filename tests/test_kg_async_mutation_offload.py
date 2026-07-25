"""Event-loop and lease-context proofs for asynchronous KG mutations."""

from __future__ import annotations

import asyncio
from contextlib import AbstractContextManager, ExitStack
import threading
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.rebuild_ports import BoardSourceSnapshot
from okto_pulse.core.kg.guarded_write import (
    guarded_board_write as _real_guarded_board_write,
    revalidate_active_board_write_lease,
)
from okto_pulse.core.kg.interfaces.graph_lifecycle import (
    GraphLifecycleStepResult,
)
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult
from okto_pulse.core.kg.safe_write_lifecycle import (
    KGSafeWriteLifecycle,
    LockOwnerProbe,
)
from okto_pulse.core.kg.single_writer_lock import LockAcquisition


class _WriterLock:
    def __init__(self) -> None:
        self.active = False
        self.token = "async-offload-owner"

    def acquire(self, **_kwargs: Any) -> LockAcquisition:
        self.active = True
        return LockAcquisition(
            acquired=True,
            owner_token=self.token,
            expires_at=None,
            current_owner=None,
        )

    def is_owner(self, _board_id: str, owner_token: str) -> bool:
        return self.active and owner_token == self.token

    def renew(self, **_kwargs: Any) -> bool:
        return self.active

    def release(self, **_kwargs: Any) -> bool:
        self.active = False
        return True


def _guard_factory(
    writer_lock: _WriterLock,
) -> Any:
    lifecycle = KGSafeWriteLifecycle(
        step_adapter=lambda _board, _graph, _step: GraphLifecycleStepResult(
            ok=True
        ),
        owner_probe=LockOwnerProbe(is_active_owner=writer_lock.is_owner),
    )

    def _guard(board_id: str, **kwargs: Any) -> AbstractContextManager[Any]:
        return _real_guarded_board_write(
            board_id,
            **kwargs,
            writer_lock=writer_lock,
            lifecycle=lifecycle,
        )

    return _guard


class _ScopeContext:
    def __init__(self, scope: object) -> None:
        self.scope = scope

    async def __aenter__(self) -> object:
        return self.scope

    async def __aexit__(self, *_args: Any) -> None:
        return None


class _GraphTransaction:
    def __init__(self, scope: object) -> None:
        self.scope = scope

    async def begin(self, _board_id: str) -> _ScopeContext:
        return _ScopeContext(self.scope)


async def _run_with_responsiveness_probe(
    operation: Any,
    *,
    started: threading.Event,
    release: threading.Event,
) -> tuple[Any, float]:
    loop = asyncio.get_running_loop()
    began_at = loop.time()
    ticker = asyncio.create_task(asyncio.sleep(0.01))
    task = asyncio.create_task(operation)
    started_ok = await asyncio.to_thread(started.wait, 1.0)
    ticker_elapsed = loop.time() - began_at
    release.set()
    result = await task
    await ticker
    assert started_ok is True
    return result, ticker_elapsed


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ("apply", "revert"))
async def test_cancellation_decay_keeps_loop_responsive_and_lease_visible(
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    from okto_pulse.core.events.handlers import cancellation_decay as handler

    board_id = "board-decay-offload"
    event_loop_thread = threading.get_ident()
    graph_threads: list[int] = []
    started = threading.Event()
    release = threading.Event()
    writer_lock = _WriterLock()

    class _Scope:
        def execute(self, _statement: str, _params: object) -> GraphStatementResult:
            lease = revalidate_active_board_write_lease(board_id)
            assert lease is not None
            graph_threads.append(threading.get_ident())
            started.set()
            release.wait(timeout=2.0)
            return GraphStatementResult.from_rows([["node-1"]])

    registry = SimpleNamespace(
        graph_runtime_store=SimpleNamespace(exists=lambda _board_id: True),
        graph_transaction=_GraphTransaction(_Scope()),
    )
    monkeypatch.setattr(handler, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(handler, "NODE_TYPES", ("Entity",))
    monkeypatch.setattr(
        handler,
        "guarded_board_write",
        _guard_factory(writer_lock),
    )

    operation = (
        handler._apply_source_decay
        if action == "apply"
        else handler._revert_source_decay
    )
    affected, ticker_elapsed = await _run_with_responsiveness_probe(
        operation(board_id, "card:card-1"),
        started=started,
        release=release,
    )

    assert affected == 1
    assert ticker_elapsed < 0.5
    assert graph_threads and graph_threads[0] != event_loop_thread
    assert writer_lock.active is False


@pytest.mark.asyncio
async def test_card_boost_keeps_loop_responsive_and_lease_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.events.handlers import card_boost_recompute as handler

    board_id = "board-card-boost-offload"
    event_loop_thread = threading.get_ident()
    graph_threads: list[int] = []
    started = threading.Event()
    release = threading.Event()
    writer_lock = _WriterLock()
    scope = object()
    registry = SimpleNamespace(
        graph_runtime_store=SimpleNamespace(exists=lambda _board_id: True),
        graph_transaction=_GraphTransaction(scope),
    )

    def _resolve_root(*_args: Any, **_kwargs: Any):
        lease = revalidate_active_board_write_lease(board_id)
        assert lease is not None
        graph_threads.append(threading.get_ident())
        started.set()
        release.wait(timeout=2.0)
        return ("node-1", "working", "working_immature")

    monkeypatch.setattr(handler, "get_kg_registry", lambda: registry)
    monkeypatch.setattr(
        handler,
        "guarded_board_write",
        _guard_factory(writer_lock),
    )
    monkeypatch.setattr(handler, "_resolve_root_node", _resolve_root)
    monkeypatch.setattr(
        handler,
        "_fetch_priority_boost",
        lambda *_args, **_kwargs: 0.0,
    )
    monkeypatch.setattr(
        handler,
        "_persist_priority_boost",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        handler,
        "_recompute_relevance",
        lambda *_args, **_kwargs: 0.3,
    )
    monkeypatch.setattr(
        handler,
        "_emit_boost_decision_node",
        lambda *_args, **_kwargs: None,
    )

    result, ticker_elapsed = await _run_with_responsiveness_probe(
        handler._recompute_boost(
            board_id=board_id,
            card_id="card-1",
            spec_id=None,
            card_type_value="task",
            new_priority_value="high",
            new_severity_value=None,
            trigger_event_type="card.priority_changed",
            changed_by="actor-1",
        ),
        started=started,
        release=release,
    )

    assert result == (0.0, 0.1)
    assert ticker_elapsed < 0.5
    assert graph_threads and graph_threads[0] != event_loop_thread
    assert writer_lock.active is False


@pytest.mark.asyncio
async def test_boost_graph_and_fence_are_off_loop_but_uow_stays_on_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okto_pulse.core.application.use_cases.kg_routes_crud as crud
    import okto_pulse.core.kg.guarded_write as guarded

    board_id = "board-route-boost-offload"
    event_loop_thread = threading.get_ident()
    graph_threads: list[int] = []
    uow_threads: list[int] = []
    started = threading.Event()
    release = threading.Event()
    writer_lock = _WriterLock()

    async def _allow_access(*_args: Any, **_kwargs: Any) -> None:
        uow_threads.append(threading.get_ident())

    class _KG:
        async def mutate_boost_node_graph(self, *_args: Any, **_kwargs: Any):
            lease = revalidate_active_board_write_lease(board_id)
            assert lease is not None
            graph_threads.append(threading.get_ident())
            started.set()
            release.wait(timeout=2.0)
            return object()

        def stage_boost_node_audit(self, _mutation: object) -> dict[str, object]:
            uow_threads.append(threading.get_ident())
            assert writer_lock.active is True
            return {"node_id": "node-1"}

    async def _commit(_uow: object) -> None:
        uow_threads.append(threading.get_ident())
        assert writer_lock.active is True

    async def _rollback() -> None:
        raise AssertionError("successful audit must not roll back")

    monkeypatch.setattr(crud, "_require_board_access", _allow_access)
    monkeypatch.setattr(
        guarded,
        "guarded_board_write",
        _guard_factory(writer_lock),
    )
    monkeypatch.setattr(crud, "commit", _commit)
    uow = SimpleNamespace(
        services=SimpleNamespace(kg=_KG()),
        rollback=_rollback,
    )

    result, ticker_elapsed = await _run_with_responsiveness_probe(
        crud.BoostNodeUseCase().execute(
            crud.BoostNodeCommand(board_id, "node-1"),
            actor=crud.ActorContext("actor-1", "rest"),
            uow=uow,
        ),
        started=started,
        release=release,
    )

    assert result.payload == {"node_id": "node-1"}
    assert ticker_elapsed < 0.5
    assert graph_threads and graph_threads[0] != event_loop_thread
    assert uow_threads == [event_loop_thread, event_loop_thread, event_loop_thread]
    assert writer_lock.active is False


@pytest.mark.asyncio
async def test_stale_reconcile_offloads_snapshot_and_graph_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import canonical_stale_reconciler as reconciler

    board_id = "board-stale-reconcile-offload"
    event_loop_thread = threading.get_ident()
    events: list[str] = []
    threads: dict[str, int] = {}
    writer_lock = _WriterLock()

    class _SourceReader:
        def fetch(self, requested_board_id: str) -> BoardSourceSnapshot:
            assert requested_board_id == board_id
            threads["snapshot"] = threading.get_ident()
            events.append("snapshot")
            return BoardSourceSnapshot()

    class _Scope:
        def execute(
            self,
            statement: str,
            _params: object,
        ) -> GraphStatementResult:
            assert "MATCH (n:Learning)" in statement
            lease = revalidate_active_board_write_lease(board_id)
            assert lease is not None
            threads["graph_execute"] = threading.get_ident()
            events.append("graph_execute")
            return GraphStatementResult.from_rows(
                [
                    [
                        "learning-1",
                        "bug:deleted-bug",
                        "cognitive:analyst",
                        "canonical_eligible",
                        "canonical",
                        None,
                        1.0,
                        "title",
                        "content",
                        "context",
                        "justification",
                        "quote",
                        False,
                        "hash",
                    ]
                ]
            )

    class _Transaction:
        async def begin(self, requested_board_id: str) -> _ScopeContext:
            assert requested_board_id == board_id
            lease = revalidate_active_board_write_lease(board_id)
            assert lease is not None
            threads["graph_begin"] = threading.get_ident()
            events.append("graph_begin")
            return _ScopeContext(_Scope())

    class _Registry:
        graph_transaction = _Transaction()

        def require_board_source_reader(self) -> _SourceReader:
            return _SourceReader()

    class _ThreadedBlockingExecution:
        def __init__(self) -> None:
            self.submission_threads: list[int] = []

        async def run(self, operation: Any) -> Any:
            self.submission_threads.append(threading.get_ident())
            return await asyncio.to_thread(operation)

        async def join(self, _timeout: float) -> int:
            return 0

    async def _route_debt(
        _db: object,
        requested_board_id: str,
        _intent: dict[str, Any],
        _correlation_id: str,
    ) -> None:
        assert requested_board_id == board_id
        threads["debt"] = threading.get_ident()
        events.append("debt")

    executor = _ThreadedBlockingExecution()
    guard = _guard_factory(writer_lock)
    leases: list[Any] = []

    monkeypatch.setattr(reconciler, "ALL_NODE_TYPES", ("Learning",))
    monkeypatch.setattr(reconciler, "get_kg_registry", lambda: _Registry())
    monkeypatch.setattr(reconciler, "_route_cognitive_to_debt", _route_debt)

    with ExitStack() as write_stack:

        def _before_graph_write() -> None:
            threads["callback"] = threading.get_ident()
            events.append("callback")
            leases.append(
                write_stack.enter_context(
                    guard(
                        board_id,
                        operation="stale_reconcile",
                        owner_id="test-worker",
                        mutation_ref="delete-event",
                    )
                )
            )

        result = await reconciler.reconcile_stale_canonical(
            object(),
            board_id=board_id,
            correlation_id="delete-event",
            before_graph_write=_before_graph_write,
            blocking_execution=executor,
        )
        leases[0].ensure_durable()

    assert events == [
        "snapshot",
        "callback",
        "graph_begin",
        "graph_execute",
        "debt",
    ]
    assert threads["snapshot"] != event_loop_thread
    assert threads["graph_begin"] != event_loop_thread
    assert threads["graph_execute"] != event_loop_thread
    assert threads["callback"] == event_loop_thread
    assert threads["debt"] == event_loop_thread
    assert executor.submission_threads == [event_loop_thread, event_loop_thread]
    assert len(result.routed_to_debt) == 1
    assert writer_lock.active is False
