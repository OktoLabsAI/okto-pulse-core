"""Regression proofs for board-writer fencing on asynchronous KG mutations."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.guarded_write import GuardedWriteError
from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult


class _Scope:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    def execute(self, _query, _params=None):
        self.trace.append("graph_mutation")
        return GraphStatementResult(rows=[])


class _ScopeContext:
    def __init__(self, trace: list[str]) -> None:
        self.scope = _Scope(trace)

    async def __aenter__(self):
        return self.scope

    async def __aexit__(self, *_args):
        return None


class _GraphTransaction:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    async def begin(self, _board_id: str):
        return _ScopeContext(self.trace)


def _registry(trace: list[str]):
    return SimpleNamespace(
        graph_runtime_store=SimpleNamespace(exists=lambda _board_id: True),
        graph_transaction=_GraphTransaction(trace),
    )


def _guard(trace: list[str], *, fail_lifecycle: bool = False):
    class _Lease:
        def ensure_durable(self) -> None:
            trace.append("durability")
            if fail_lifecycle:
                raise GuardedWriteError(
                    "safe_lifecycle_failed",
                    "injected lifecycle failure",
                    retryable=True,
                )

    @contextmanager
    def _factory(*_args, **_kwargs):
        trace.append("fence_enter")
        try:
            yield _Lease()
        finally:
            trace.append("fence_exit")

    return _factory


@pytest.mark.asyncio
async def test_cancellation_decay_mutation_is_durable_inside_one_fence(
    monkeypatch,
) -> None:
    from okto_pulse.core.events.handlers import cancellation_decay as handler

    trace: list[str] = []
    monkeypatch.setattr(handler, "get_kg_registry", lambda: _registry(trace))
    monkeypatch.setattr(handler, "NODE_TYPES", ("Entity",))
    monkeypatch.setattr(handler, "guarded_board_write", _guard(trace))

    await handler._apply_source_decay("board-1", "card:card-1")

    assert trace == [
        "fence_enter",
        "graph_mutation",
        "durability",
        "fence_exit",
    ]


@pytest.mark.asyncio
async def test_card_boost_recompute_keeps_mutation_and_lifecycle_fenced(
    monkeypatch,
) -> None:
    from okto_pulse.core.events.handlers import card_boost_recompute as handler

    trace: list[str] = []
    monkeypatch.setattr(handler, "get_kg_registry", lambda: _registry(trace))
    monkeypatch.setattr(handler, "guarded_board_write", _guard(trace))
    monkeypatch.setattr(
        handler,
        "_resolve_root_node",
        lambda *_args, **_kwargs: ("node-1", "working", "working_immature"),
    )
    monkeypatch.setattr(
        handler,
        "_fetch_priority_boost",
        lambda *_args, **_kwargs: 0.0,
    )
    monkeypatch.setattr(
        handler,
        "_persist_priority_boost",
        lambda *_args, **_kwargs: trace.append("graph_mutation"),
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

    await handler._recompute_boost(
        board_id="board-1",
        card_id="card-1",
        spec_id=None,
        card_type_value="task",
        new_priority_value="high",
        new_severity_value=None,
        trigger_event_type="card.priority_changed",
        changed_by="actor-1",
    )

    assert trace == [
        "fence_enter",
        "graph_mutation",
        "durability",
        "fence_exit",
    ]


def test_hit_recompute_lifecycle_failure_refuses_success(monkeypatch) -> None:
    from okto_pulse.core.events.handlers import kg_hit_recompute as handler

    trace: list[str] = []
    monkeypatch.setattr(handler, "get_kg_registry", lambda: _registry(trace))
    monkeypatch.setattr(
        handler,
        "guarded_board_write",
        _guard(trace, fail_lifecycle=True),
    )
    monkeypatch.setattr(
        handler,
        "_recompute_relevance",
        lambda *_args, **_kwargs: trace.append("graph_mutation") or 0.7,
    )

    with pytest.raises(GuardedWriteError, match="injected lifecycle failure"):
        handler._recompute_sync("board-1", "Entity", "node-1")

    assert trace == [
        "fence_enter",
        "graph_mutation",
        "durability",
        "fence_exit",
    ]


def test_decay_tick_batch_runs_under_shared_guard(monkeypatch) -> None:
    from okto_pulse.core.events.handlers import kg_decay_tick as handler
    import okto_pulse.core.kg.guarded_write as guarded

    trace: list[str] = []
    fetch_count = 0

    def _fetch(*_args, **_kwargs):
        nonlocal fetch_count
        fetch_count += 1
        return [("Entity", "node-1")] if fetch_count == 1 else []

    monkeypatch.setattr(handler, "get_kg_registry", lambda: _registry(trace))
    monkeypatch.setattr(handler, "NODE_TYPES", ("Entity",))
    monkeypatch.setattr(handler, "_count_stale_nodes_pre_tick", lambda *_args: 1)
    monkeypatch.setattr(handler, "_fetch_stale_nodes", _fetch)
    monkeypatch.setattr(
        handler,
        "_recompute_relevance_batch",
        lambda *_args, **_kwargs: trace.append("graph_mutation") or 1,
    )
    monkeypatch.setattr(guarded, "guarded_board_write", _guard(trace))

    result = handler._process_board_sync(
        "board-1",
        "2026-07-01T00:00:00+00:00",
        batch_size=10,
    )

    assert result == (1, 1)
    assert trace == [
        "fence_enter",
        "graph_mutation",
        "durability",
        "fence_exit",
    ]


@pytest.mark.asyncio
async def test_decay_tick_guard_failure_is_not_folded_into_board_failure(
    monkeypatch,
) -> None:
    from okto_pulse.core.events.handlers import kg_decay_tick as handler

    def _fail_guarded_write(*_args, **_kwargs):
        raise GuardedWriteError(
            "safe_lifecycle_failed",
            "injected lifecycle failure",
            retryable=True,
        )

    persisted: list[object] = []

    async def _persist(*_args, **kwargs):
        persisted.append(kwargs)

    monkeypatch.setattr(handler, "_process_board_sync", _fail_guarded_write)
    monkeypatch.setattr(handler, "_optional_delivery_ledger_port", lambda: None)
    monkeypatch.setattr(handler, "_optional_stale_sweep_port", lambda: None)
    monkeypatch.setattr(handler, "_optional_takedown_telemetry_port", lambda: None)
    monkeypatch.setattr(handler, "_persist_tick_run", _persist)

    with pytest.raises(GuardedWriteError, match="injected lifecycle failure"):
        await handler._run_daily_tick(
            tick_id="tick-guard-failure",
            session=object(),
            board_id="board-1",
            batch_size=10,
            staleness_days=7,
            delivery_watchdog_limit=10,
            delivery_redrive_limit=10,
            stale_sweep_budget=10,
        )

    assert persisted == []


@pytest.mark.asyncio
async def test_decay_tick_handler_propagates_guard_failure_to_dispatcher(
    monkeypatch,
) -> None:
    from okto_pulse.core.application import kg_tick
    from okto_pulse.core.events.handlers import kg_decay_tick as handler

    async def _admit(*_args, **_kwargs):
        return None

    async def _fail_tick(*_args, **_kwargs):
        raise GuardedWriteError(
            "safe_lifecycle_failed",
            "injected lifecycle failure",
            retryable=True,
        )

    persisted: list[object] = []

    async def _persist(*_args, **kwargs):
        persisted.append(kwargs)

    monkeypatch.setattr(kg_tick, "require_kg_tick_admission", _admit)
    monkeypatch.setattr(handler, "_run_daily_tick", _fail_tick)
    monkeypatch.setattr(handler, "_persist_tick_run", _persist)

    event = SimpleNamespace(
        tick_id="tick-handler-guard-failure",
        board_id="board-1",
        force_full_rebuild=False,
    )
    with pytest.raises(GuardedWriteError, match="injected lifecycle failure"):
        await handler.KGDailyTickHandler().handle(event, object())

    assert persisted == []
