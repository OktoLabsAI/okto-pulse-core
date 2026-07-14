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

from okto_pulse.community.api.kg_events_hub import (
    SUBSCRIBER_QUEUE_MAXSIZE,
    KgEventsHub,
    _BoardStream,
    get_kg_events_hub,
    shutdown_kg_events_hub,
)
from okto_pulse.core.infra.database import get_engine, get_session_factory
from sqlalchemy_test_models import GlobalUpdateOutbox
from okto_pulse.core.ports.kg_events import KGEventsPoll

pytestmark = pytest.mark.asyncio(loop_scope="function")


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


async def test_fanout_outbox_event_to_multiple_subscribers(kg_events_reader):
    hub = KgEventsHub(
        kg_events_reader,
        poll_interval=0.1,
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


async def test_progress_snapshot_broadcast_on_first_cycle(kg_events_reader):
    hub = KgEventsHub(
        kg_events_reader,
        poll_interval=0.1,
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


async def test_poller_stops_when_last_subscriber_leaves(kg_events_reader):
    hub = KgEventsHub(
        kg_events_reader,
        poll_interval=0.1,
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


async def test_hub_poller_uses_injected_reader():
    class _Reader:
        def __init__(self) -> None:
            self.calls = 0

        async def poll(self, *, board_id, after, limit):
            self.calls += 1
            return KGEventsPoll(
                events=[],
                progress={"pending": 0, "claimed": 0, "done": 0, "failed": 0, "paused": 0},
            )

        async def replay(self, *, board_id, after, limit):
            return []

    reader = _Reader()
    hub = KgEventsHub(reader, poll_interval=0.01)
    sub = hub.subscribe("board-injected")
    try:
        await _drain_until(sub.queue, lambda c: "kg.queue.progress" in c, timeout=2.0)
    finally:
        hub.unsubscribe(sub)
        await hub.aclose()

    assert reader.calls >= 1


async def test_hub_unsubscribe_cancels_an_inflight_reader_poll():
    poll_started = asyncio.Event()
    poll_cancelled = asyncio.Event()

    class _Reader:
        async def poll(self, *, board_id, after, limit):
            poll_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                poll_cancelled.set()
                raise

        async def replay(self, *, board_id, after, limit):
            return []

    hub = KgEventsHub(_Reader(), poll_interval=30.0)
    sub = hub.subscribe("board-cancel-exit")
    stream_task = hub._streams["board-cancel-exit"].task
    assert stream_task is not None

    await asyncio.wait_for(poll_started.wait(), timeout=2.0)
    hub.unsubscribe(sub)

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(stream_task, timeout=2.0)
    assert poll_cancelled.is_set()
    await hub.aclose()


async def test_sse_route_hard_cancel_does_not_leak_pool_connections():
    """O cenário de produção: cliente SSE desconecta → task da request é
    hard-cancelada. O contrato do fix: nenhuma conexão do pool fica
    checked-out e o hub remove o assinante."""
    from okto_pulse.community.api.kg_routes import stream_kg_events

    board_id = f"board-hub-{uuid.uuid4().hex[:8]}"
    response = await stream_kg_events(board_id, since=None)
    hub = get_kg_events_hub()

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
    assert board_id in hub._streams

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
    from okto_pulse.community.api.kg_routes import stream_kg_events

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
