"""Tests do broadcast hub SSE — root-cause fix do vazamento de pool.

Cobrem o contrato central do fix de 2026-06-09:
- fan-out de eventos do outbox para múltiplos assinantes sem DB no generator
- lifecycle do poller (inicia no primeiro subscribe, para no último unsubscribe)
- hard-cancel de um consumidor SSE NÃO vaza conexões do pool (o cenário que
  exauria o pool e "travava" o servidor em produção)
- backpressure da queue por assinante
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.api.kg_events_hub import (
    SUBSCRIBER_QUEUE_MAXSIZE,
    KgEventsHub,
    _BoardStream,
    configure_kg_events_hub_session_factory,
    get_kg_events_hub,
    shutdown_kg_events_hub,
)
from okto_pulse.core.infra.database import get_engine, get_session_factory
from okto_pulse.core.models.db import GlobalUpdateOutbox

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def _insert_outbox_event(board_id: str, event_type: str = "kg.session.committed") -> str:
    """Insere uma linha no outbox com created_at no futuro próximo, garantindo
    created_at > last_seen do stream (cursor é aberto com now())."""
    event_id = str(uuid.uuid4())
    factory = get_session_factory()
    async with factory() as session:
        session.add(GlobalUpdateOutbox(
            event_id=event_id,
            board_id=board_id,
            session_id=str(uuid.uuid4()),
            event_type=event_type,
            payload={"test": True},
            created_at=datetime.now(timezone.utc) + timedelta(seconds=1),
        ))
        await session.commit()
    return event_id


async def _drain_until(queue: asyncio.Queue, predicate, timeout: float = 6.0) -> str:
    """Consome a queue até um chunk satisfazer o predicate (ou estoura)."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        assert remaining > 0, "timed out waiting for matching SSE chunk"
        chunk = await asyncio.wait_for(queue.get(), timeout=remaining)
        if predicate(chunk):
            return chunk


async def test_fanout_outbox_event_to_multiple_subscribers():
    hub = KgEventsHub(
        poll_interval=0.1,
        session_scope_factory=get_session_factory(),
    )
    board_id = f"board-hub-{uuid.uuid4().hex[:8]}"
    sub_a = hub.subscribe(board_id)
    sub_b = hub.subscribe(board_id)
    try:
        event_id = await _insert_outbox_event(board_id)
        chunk_a = await _drain_until(sub_a.queue, lambda c: event_id in c)
        chunk_b = await _drain_until(sub_b.queue, lambda c: event_id in c)
        assert "event: kg.session.committed" in chunk_a
        assert chunk_a == chunk_b
    finally:
        hub.unsubscribe(sub_a)
        hub.unsubscribe(sub_b)
        await hub.aclose()


async def test_progress_snapshot_broadcast_on_first_cycle():
    hub = KgEventsHub(
        poll_interval=0.1,
        session_scope_factory=get_session_factory(),
    )
    board_id = f"board-hub-{uuid.uuid4().hex[:8]}"
    sub = hub.subscribe(board_id)
    try:
        chunk = await _drain_until(sub.queue, lambda c: "kg.queue.progress" in c)
        # Board sem ConsolidationQueue rows → snapshot zerado.
        assert '"pending": 0' in chunk
        assert '"total": 0' in chunk
    finally:
        hub.unsubscribe(sub)
        await hub.aclose()


async def test_poller_stops_when_last_subscriber_leaves():
    hub = KgEventsHub(
        poll_interval=0.1,
        session_scope_factory=get_session_factory(),
    )
    board_id = f"board-hub-{uuid.uuid4().hex[:8]}"
    sub = hub.subscribe(board_id)
    stream_task = hub._streams[board_id].task
    assert stream_task is not None and not stream_task.done()

    hub.unsubscribe(sub)
    assert board_id not in hub._streams
    with pytest.raises((asyncio.CancelledError, Exception)):
        await asyncio.wait_for(stream_task, timeout=2.0)
    assert stream_task.done()
    await hub.aclose()


async def test_subscriber_queue_is_bounded_and_keeps_newest():
    stream = _BoardStream("board-x")
    queue: asyncio.Queue = asyncio.Queue(maxsize=SUBSCRIBER_QUEUE_MAXSIZE)
    stream.subscribers.add(queue)
    for i in range(SUBSCRIBER_QUEUE_MAXSIZE + 100):
        stream.broadcast(f"chunk-{i}")
    assert queue.qsize() == SUBSCRIBER_QUEUE_MAXSIZE
    # Os mais antigos foram descartados; o último broadcast está presente.
    items = [queue.get_nowait() for _ in range(queue.qsize())]
    assert items[-1] == f"chunk-{SUBSCRIBER_QUEUE_MAXSIZE + 99}"
    assert items[0] == "chunk-100"


async def test_hub_poller_uses_injected_session_scope(monkeypatch):
    from okto_pulse.core.api import kg_events_hub
    from okto_pulse.core.infra import database as database_module

    class _SessionScope:
        def __init__(self) -> None:
            self.entered = False
            self.exited = False

        async def __aenter__(self):
            self.entered = True
            return object()

        async def __aexit__(self, *_args) -> None:
            self.exited = True

    scopes: list[_SessionScope] = []

    def _factory() -> _SessionScope:
        scope = _SessionScope()
        scopes.append(scope)
        return scope

    async def _fake_rows(_session, _board_id, _after, limit=50):
        return []

    async def _fake_snapshot(_session, _board_id):
        return {"pending": 0, "claimed": 0, "done": 0, "failed": 0, "paused": 0}

    monkeypatch.setattr(kg_events_hub, "query_outbox_rows", _fake_rows)
    monkeypatch.setattr(kg_events_hub, "query_queue_snapshot", _fake_snapshot)
    monkeypatch.setattr(
        database_module,
        "get_session_factory",
        lambda: (_ for _ in ()).throw(
            AssertionError("concrete get_session_factory must not be called")
        ),
    )

    hub = KgEventsHub(poll_interval=0.01, session_scope_factory=_factory)
    sub = hub.subscribe("board-injected")
    try:
        await _drain_until(sub.queue, lambda c: "kg.queue.progress" in c, timeout=2.0)
    finally:
        hub.unsubscribe(sub)
        await hub.aclose()

    assert scopes
    assert scopes[0].entered is True
    assert scopes[0].exited is True


async def test_sse_route_hard_cancel_does_not_leak_pool_connections():
    """O cenário de produção: cliente SSE desconecta → task da request é
    hard-cancelada. O contrato do fix: nenhuma conexão do pool fica
    checked-out e o hub remove o assinante."""
    from okto_pulse.core.api.kg_routes import stream_kg_events

    configure_kg_events_hub_session_factory(get_session_factory())
    board_id = f"board-hub-{uuid.uuid4().hex[:8]}"
    response = await stream_kg_events(board_id, since=None)
    hub = get_kg_events_hub()
    assert board_id in hub._streams

    consumed: list[str] = []

    async def _consume():
        async for chunk in response.body_iterator:
            consumed.append(chunk)

    task = asyncio.create_task(_consume())
    # Espera o stream entregar o hello + primeiro snapshot do poller, ou
    # seja, o generator está parado no queue.get() quando o cancel chega.
    deadline = asyncio.get_running_loop().time() + 6.0
    while len(consumed) < 2 and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.05)
    assert consumed, "stream nunca emitiu o hello"

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # finally do generator rodou: assinante removido, poller do board parado.
    assert board_id not in hub._streams
    # E nada ficou preso no pool de conexões do SQLAlchemy.
    await asyncio.sleep(0.2)
    assert get_engine().sync_engine.pool.checkedout() == 0
    await shutdown_kg_events_hub()


async def test_sse_route_replays_backlog_since_cursor():
    """Reconexão com `since` re-entrega eventos perdidos via cancel_safe_session."""
    from okto_pulse.core.api.kg_routes import stream_kg_events

    configure_kg_events_hub_session_factory(get_session_factory())
    board_id = f"board-hub-{uuid.uuid4().hex[:8]}"
    event_id = await _insert_outbox_event(board_id)
    # O evento foi inserido com created_at ~1s no futuro; `since` bem no
    # passado garante que ele caia na janela de replay (since, cursor-do-hub]
    # OU chegue pela queue do hub — o cliente recebe exatamente uma vez.
    since = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    response = await stream_kg_events(board_id, since=since)

    seen: list[str] = []

    async def _consume():
        async for chunk in response.body_iterator:
            seen.append(chunk)
            if any(event_id in c for c in seen):
                break

    await asyncio.wait_for(_consume(), timeout=8.0)
    matching = [c for c in seen if event_id in c]
    assert len(matching) == 1, f"evento deve chegar exatamente 1x, veio {len(matching)}"
    await shutdown_kg_events_hub()
    await asyncio.sleep(0.1)
    assert get_engine().sync_engine.pool.checkedout() == 0
