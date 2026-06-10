"""Broadcast hub para o stream SSE de eventos do KG.

Root-cause fix (travamento/corrupção 2026-06-09): a rota
``GET /api/v1/kg/boards/{board_id}/events`` fazia polling de DB (outbox +
queue snapshot, 2 queries/s) DENTRO do generator de cada cliente SSE.
Quando o cliente desconectava, o hard-cancel da request aterrissava no
meio do ``async with session_factory()`` e o close da conexão era
cancelado — a conexão nunca voltava ao pool. Vazamentos acumulavam até a
exaustão do pool (servidor "travado") e o GC terminava conexões aiosqlite
abruptamente (SAWarnings + risco de inconsistência).

Este hub move TODO o acesso a DB para uma única task em background por
board: o poller lê o ``global_update_outbox`` e o snapshot da
``consolidation_queue`` uma vez por segundo e faz fan-out de chunks SSE
pré-formatados para uma ``asyncio.Queue`` por assinante. O generator da
request só faz ``queue.get()`` — cancelá-lo não toca em nenhum recurso de
pool. Carga de DB cai de O(clientes × 2/s) para O(boards_ativos × 2/s).

Lifecycle: o poller de um board inicia no primeiro subscribe e para quando
o último assinante sai. ``shutdown_kg_events_hub()`` é chamado no lifespan
do app.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid as _uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("okto_pulse.api.kg_events_hub")

POLL_INTERVAL_SECONDS = 1.0
# Backpressure por assinante: cliente parado >512 eventos perde os mais
# antigos (o snapshot de progresso é autoritativo, então perda é benigna).
SUBSCRIBER_QUEUE_MAXSIZE = 512
OUTBOX_BATCH_LIMIT = 50


def format_sse(event_type: str, payload: dict[str, Any]) -> str:
    """Serializa um evento no wire format SSE usado pelo hook useKgLiveEvents."""
    return f"event: {event_type}\ndata: {json.dumps(payload, default=str)}\n\n"


def format_progress_sse(snapshot: dict[str, int]) -> str:
    return format_sse(
        "kg.queue.progress",
        {
            "event_id": f"progress:{_uuid.uuid4().hex}",
            "event_type": "kg.queue.progress",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "payload": snapshot,
        },
    )


def format_outbox_row_sse(row: dict[str, Any]) -> str:
    return format_sse(row["event_type"], row)


async def query_outbox_rows(
    session, board_id: str, after: datetime, limit: int = OUTBOX_BATCH_LIMIT,
) -> list[dict[str, Any]]:
    """Lê linhas do outbox após ``after`` já serializadas para SSE.

    Retorna dicts com a chave extra ``_created_at_raw`` (datetime) para o
    chamador avançar o cursor; a chave não é emitida no wire.
    """
    from sqlalchemy import and_, asc, select

    from okto_pulse.core.models.db import GlobalUpdateOutbox

    rows = (await session.execute(
        select(GlobalUpdateOutbox).where(and_(
            GlobalUpdateOutbox.board_id == board_id,
            GlobalUpdateOutbox.created_at > after,
        )).order_by(asc(GlobalUpdateOutbox.created_at)).limit(limit)
    )).scalars().all()
    return [
        {
            "event_id": row.event_id,
            "session_id": row.session_id,
            "event_type": row.event_type,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "payload": row.payload,
            "_created_at_raw": row.created_at,
        }
        for row in rows
    ]


async def query_queue_snapshot(session, board_id: str) -> dict[str, int]:
    """Contadores de ConsolidationQueue do board (mesma semântica da rota antiga)."""
    from sqlalchemy import func, select

    from okto_pulse.core.kg.governance import HISTORICAL_PROGRESS_SETTINGS_KEY
    from okto_pulse.core.models.db import Board, ConsolidationQueue

    rows = (await session.execute(
        select(ConsolidationQueue.status, ConsolidationQueue.source, func.count())
        .where(ConsolidationQueue.board_id == board_id)
        .group_by(ConsolidationQueue.status, ConsolidationQueue.source)
    )).all()
    snap = {"pending": 0, "claimed": 0, "done": 0, "failed": 0, "paused": 0}
    historical = {"pending": 0, "claimed": 0, "done": 0, "failed": 0, "paused": 0}
    for status, source, count in rows:
        if status in snap:
            snap[status] += int(count)
            if source == "historical_backfill":
                historical[status] += int(count)

    live_total = sum(snap.values())
    historical_active = (
        historical["pending"] + historical["claimed"] + historical["paused"]
    )
    historical_total = 0
    if historical_active > 0:
        board = await session.get(Board, board_id)
        if board is not None and isinstance(board.settings, dict):
            state = board.settings.get(HISTORICAL_PROGRESS_SETTINGS_KEY)
            if isinstance(state, dict):
                try:
                    historical_total = int(state.get("total") or 0)
                except (TypeError, ValueError):
                    historical_total = 0

    non_historical_total = live_total - sum(historical.values())
    if historical_active > 0 and historical_total > 0:
        snap["total"] = max(live_total, historical_total + non_historical_total)
        snap["processed"] = max(0, snap["total"] - (
            snap["pending"] + snap["claimed"] + snap["paused"]
        ))
    else:
        snap["total"] = live_total
        snap["processed"] = snap["done"]
    return snap


@dataclass
class KgEventsSubscription:
    """Handle devolvido por :meth:`KgEventsHub.subscribe`."""

    board_id: str
    queue: asyncio.Queue
    # Cursor do poller no momento do subscribe — eventos com created_at
    # > cursor chegam pela queue; backlog (since, cursor] é responsabilidade
    # do chamador (replay de reconexão).
    cursor: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    initial_progress: dict[str, int] | None = None


class _BoardStream:
    def __init__(self, board_id: str) -> None:
        self.board_id = board_id
        self.subscribers: set[asyncio.Queue] = set()
        self.task: asyncio.Task | None = None
        self.last_seen: datetime = datetime.now(timezone.utc)
        self.last_progress: dict[str, int] | None = None

    def broadcast(self, chunk: str) -> None:
        """Fan-out síncrono (sem awaits) — atômico perante novos subscribes."""
        for q in self.subscribers:
            try:
                q.put_nowait(chunk)
            except asyncio.QueueFull:
                # Cliente lento: descarta o mais antigo, fica com o mais novo.
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    q.put_nowait(chunk)
                except asyncio.QueueFull:
                    pass


class KgEventsHub:
    """Singleton de processo — um poller por board com assinantes ativos."""

    def __init__(self, poll_interval: float = POLL_INTERVAL_SECONDS) -> None:
        self._poll_interval = poll_interval
        self._streams: dict[str, _BoardStream] = {}
        self._closed = False

    def subscribe(self, board_id: str) -> KgEventsSubscription:
        if self._closed:
            raise RuntimeError("KgEventsHub is shut down")
        stream = self._streams.get(board_id)
        if stream is None:
            stream = _BoardStream(board_id)
            self._streams[board_id] = stream
        queue: asyncio.Queue = asyncio.Queue(maxsize=SUBSCRIBER_QUEUE_MAXSIZE)
        stream.subscribers.add(queue)
        if stream.task is None or stream.task.done():
            stream.task = asyncio.get_running_loop().create_task(
                self._poll_board(stream), name=f"kg_events_poller:{board_id}",
            )
        return KgEventsSubscription(
            board_id=board_id,
            queue=queue,
            cursor=stream.last_seen,
            initial_progress=stream.last_progress,
        )

    def unsubscribe(self, subscription: KgEventsSubscription) -> None:
        stream = self._streams.get(subscription.board_id)
        if stream is None:
            return
        stream.subscribers.discard(subscription.queue)
        if not stream.subscribers:
            if stream.task is not None and not stream.task.done():
                stream.task.cancel()
            self._streams.pop(subscription.board_id, None)

    async def aclose(self) -> None:
        self._closed = True
        streams = list(self._streams.values())
        self._streams.clear()
        for stream in streams:
            if stream.task is not None and not stream.task.done():
                stream.task.cancel()
        for stream in streams:
            if stream.task is not None:
                try:
                    await stream.task
                except (asyncio.CancelledError, Exception):
                    pass

    async def _poll_board(self, stream: _BoardStream) -> None:
        """Loop único de DB por board — nunca roda dentro de uma request."""
        from okto_pulse.core.infra.database import get_session_factory

        session_factory = get_session_factory()
        while stream.subscribers and not self._closed:
            try:
                async with session_factory() as session:
                    rows = await query_outbox_rows(
                        session, stream.board_id, stream.last_seen,
                    )
                    snap = await query_queue_snapshot(session, stream.board_id)
                # Bloco síncrono: broadcast + avanço de cursor sem awaits no
                # meio, para que um subscribe concorrente não perca eventos
                # (contrato do KgEventsSubscription.cursor).
                for row in rows:
                    stream.broadcast(format_outbox_row_sse(
                        {k: v for k, v in row.items() if k != "_created_at_raw"}
                    ))
                    if row["_created_at_raw"] is not None:
                        stream.last_seen = row["_created_at_raw"]
                if snap != stream.last_progress:
                    stream.last_progress = snap
                    stream.broadcast(format_progress_sse(snap))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "kg_events_hub.poll_failed board=%s err=%s",
                    stream.board_id, exc,
                    extra={
                        "event": "kg_events_hub.poll_failed",
                        "board_id": stream.board_id,
                    },
                )
            await asyncio.sleep(self._poll_interval)


_hub: KgEventsHub | None = None


def get_kg_events_hub() -> KgEventsHub:
    global _hub
    if _hub is None or _hub._closed:
        _hub = KgEventsHub()
    return _hub


async def shutdown_kg_events_hub() -> None:
    """Encerra o hub no shutdown do app (chamado pelo lifespan)."""
    global _hub
    if _hub is not None:
        await _hub.aclose()
        _hub = None
