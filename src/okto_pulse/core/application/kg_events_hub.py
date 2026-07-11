"""Generic fan-out coordination for the KG live-event stream.

The hub intentionally owns no SQLAlchemy session, table or database factory.
It coordinates subscribers over the Core event port while a composed edition
provides the concrete Local First or SaaS event source.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

import asyncio
import json
import logging
import uuid as _uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Sequence

from okto_pulse.core.ports.kg_events import (
    KGEventsReaderPort,
    KGOutboxEvent,
    get_kg_events_reader_port,
    register_kg_events_reader_port,
)

logger = logging.getLogger("okto_pulse.application.kg_events_hub")

POLL_INTERVAL_SECONDS = 1.0
SUBSCRIBER_QUEUE_MAXSIZE = 512
OUTBOX_BATCH_LIMIT = 50


def format_sse(event_type: str, payload: Mapping[str, Any]) -> str:
    """Serialize one event in the wire format consumed by the web client."""

    return f"event: {event_type}\ndata: {json.dumps(dict(payload), default=str)}\n\n"


def format_progress_sse(snapshot: Mapping[str, int]) -> str:
    return format_sse(
        "kg.queue.progress",
        {
            "event_id": f"progress:{_uuid.uuid4().hex}",
            "event_type": "kg.queue.progress",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "payload": dict(snapshot),
        },
    )


def format_outbox_row_sse(event: KGOutboxEvent | Mapping[str, Any]) -> str:
    """Serialize an outbox event while preserving the established SSE schema."""

    if isinstance(event, KGOutboxEvent):
        payload = {
            "event_id": event.event_id,
            "session_id": event.session_id,
            "event_type": event.event_type,
            "created_at": event.created_at.isoformat() if event.created_at else None,
            "payload": dict(event.payload),
        }
        return format_sse(event.event_type, payload)

    payload = dict(event)
    payload.pop("_created_at_raw", None)
    return format_sse(str(payload["event_type"]), payload)


@dataclass
class KgEventsSubscription:
    """Handle returned by :meth:`KgEventsHub.subscribe`."""

    board_id: str
    queue: asyncio.Queue[str]
    cursor: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    initial_progress: dict[str, int] | None = None


class _BoardStream:
    def __init__(self, board_id: str) -> None:
        self.board_id = board_id
        self.subscribers: set[asyncio.Queue[str]] = set()
        self.task: asyncio.Task[None] | None = None
        self.last_seen: datetime = datetime.now(timezone.utc)
        self.last_progress: dict[str, int] | None = None

    def broadcast(self, chunk: str) -> None:
        """Fan out without awaits so a concurrent subscription sees no gap."""

        for queue in self.subscribers:
            try:
                queue.put_nowait(chunk)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    queue.put_nowait(chunk)
                except asyncio.QueueFull:
                    pass


class KgEventsHub:
    """Process-local coordination with one poller for each active board."""

    def __init__(
        self,
        reader: KGEventsReaderPort | None = None,
        *,
        poll_interval: float = POLL_INTERVAL_SECONDS,
    ) -> None:
        self._reader = reader or get_kg_events_reader_port()
        self._poll_interval = poll_interval
        self._streams: dict[str, _BoardStream] = {}
        self._closed = False

    def configure_reader(self, reader: KGEventsReaderPort) -> None:
        """Replace the provider before serving a new composed runtime."""

        self._reader = reader

    def subscribe(self, board_id: str) -> KgEventsSubscription:
        if self._closed:
            raise RuntimeError("KgEventsHub is shut down")
        stream = self._streams.get(board_id)
        if stream is None:
            stream = _BoardStream(board_id)
            self._streams[board_id] = stream
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=SUBSCRIBER_QUEUE_MAXSIZE)
        stream.subscribers.add(queue)
        if stream.task is None or stream.task.done():
            stream.task = asyncio.get_running_loop().create_task(
                self._poll_board(stream), name=f"kg_events_poller:{board_id}"
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

    async def replay(
        self,
        *,
        board_id: str,
        after: datetime,
        limit: int,
    ) -> Sequence[KGOutboxEvent]:
        return await self._reader.replay(
            board_id=board_id,
            after=after,
            limit=limit,
        )

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
        """Read once per board through the composed provider and fan out."""

        while stream.subscribers and not self._closed:
            try:
                result = await self._reader.poll(
                    board_id=stream.board_id,
                    after=stream.last_seen,
                    limit=OUTBOX_BATCH_LIMIT,
                )
                for event in result.events:
                    stream.broadcast(format_outbox_row_sse(event))
                    if event.created_at is not None:
                        stream.last_seen = event.created_at
                progress = dict(result.progress)
                if progress != stream.last_progress:
                    stream.last_progress = progress
                    stream.broadcast(format_progress_sse(progress))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "kg_events_hub.poll_failed board=%s err=%s",
                    stream.board_id,
                    exc,
                    extra={
                        "event": "kg_events_hub.poll_failed",
                        "board_id": stream.board_id,
                    },
                )
            await asyncio.sleep(self._poll_interval)


_RUNTIME_KEY = "application.kg_events_hub"


def configure_kg_events_hub(reader: KGEventsReaderPort) -> KgEventsHub:
    """Compose the generic hub with the edition-owned event reader."""

    register_kg_events_reader_port(reader)
    hub = resolve_runtime_value(_RUNTIME_KEY)
    if hub is None or hub._closed:
        hub = KgEventsHub(reader)
        register_runtime_value(_RUNTIME_KEY, hub)
    else:
        hub.configure_reader(reader)
    return hub


def get_kg_events_hub() -> KgEventsHub:
    """Resolve the composed hub, never creating a concrete data provider."""

    hub = resolve_runtime_value(_RUNTIME_KEY)
    if hub is None or hub._closed:
        hub = KgEventsHub()
        register_runtime_value(_RUNTIME_KEY, hub)
    return hub


async def shutdown_kg_events_hub() -> None:
    """Stop active pollers before the edition closes its data runtime."""

    hub = resolve_runtime_value(_RUNTIME_KEY)
    if hub is not None:
        await hub.aclose()
        reset_runtime_values(_RUNTIME_KEY)


BoardStream = _BoardStream


__all__ = [
    "OUTBOX_BATCH_LIMIT",
    "POLL_INTERVAL_SECONDS",
    "SUBSCRIBER_QUEUE_MAXSIZE",
    "KgEventsHub",
    "KgEventsSubscription",
    "BoardStream",
    "configure_kg_events_hub",
    "format_outbox_row_sse",
    "format_progress_sse",
    "format_sse",
    "get_kg_events_hub",
    "shutdown_kg_events_hub",
]
