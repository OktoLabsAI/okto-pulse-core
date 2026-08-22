"""Generic fan-out coordination for the KG live-event stream.

The hub intentionally owns no SQLAlchemy session, table or database factory.
It coordinates subscribers over the Core event port while a composed edition
provides the concrete Local First or SaaS event source.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

import asyncio
import inspect
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


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _event_order_key(event: KGOutboxEvent) -> tuple[datetime, str]:
    # Outbox rows are expected to carry a timestamp.  Keep a deterministic
    # position even for a defensive transport event that does not.
    created_at = (
        _as_utc(event.created_at)
        if event.created_at is not None
        else datetime.max.replace(tzinfo=timezone.utc)
    )
    return created_at, event.event_id


def _event_is_after_cursor(
    event: KGOutboxEvent,
    *,
    after: datetime,
    after_event_id: str | None,
) -> bool:
    """Apply the same composite boundary required from reader providers."""

    if event.created_at is None:
        return False
    event_created_at = _as_utc(event.created_at)
    cursor_created_at = _as_utc(after)
    if event_created_at != cursor_created_at:
        return event_created_at > cursor_created_at
    return after_event_id is not None and event.event_id > after_event_id


def _supports_composite_cursor(method: Any) -> bool:
    """Detect readers that implement the optional composite-cursor keyword."""

    try:
        parameters = inspect.signature(method).parameters.values()
    except (TypeError, ValueError):
        # Opaque callables are expected to follow the current public protocol.
        return True
    return any(
        parameter.name == "after_event_id"
        or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _supports_visibility_scope(method: Any) -> bool:
    """Whether the reader can exclude CT rows before polling its provider."""

    try:
        parameters = inspect.signature(method).parameters.values()
    except (TypeError, ValueError):
        return True
    return any(
        parameter.name == "include_code_traceability"
        or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


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
            "created_at": _as_utc(event.created_at).isoformat()
            if event.created_at
            else None,
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
    cursor_event_id: str | None = None
    include_code_traceability: bool = True


class _BoardStream:
    def __init__(
        self,
        board_id: str,
        *,
        include_code_traceability: bool = True,
    ) -> None:
        self.board_id = board_id
        self.include_code_traceability = include_code_traceability
        self.subscribers: set[asyncio.Queue[str]] = set()
        self.task: asyncio.Task[None] | None = None
        self.last_seen: datetime = datetime.now(timezone.utc)
        self.last_seen_event_id: str | None = None
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
        self._reader_poll_supports_composite_cursor = _supports_composite_cursor(
            self._reader.poll
        )
        self._reader_replay_supports_composite_cursor = _supports_composite_cursor(
            self._reader.replay
        )
        self._reader_poll_supports_visibility_scope = _supports_visibility_scope(
            self._reader.poll
        )
        self._reader_replay_supports_visibility_scope = _supports_visibility_scope(
            self._reader.replay
        )
        self._poll_interval = poll_interval
        # Preserve the historical board-id key for the fully authorized stream
        # while using a distinct tuple key for the restricted stream.
        self._streams: dict[str | tuple[str, bool], _BoardStream] = {}
        self._closed = False

    def configure_reader(self, reader: KGEventsReaderPort) -> None:
        """Replace the provider before serving a new composed runtime."""

        self._reader = reader
        self._reader_poll_supports_composite_cursor = _supports_composite_cursor(
            reader.poll
        )
        self._reader_replay_supports_composite_cursor = _supports_composite_cursor(
            reader.replay
        )
        self._reader_poll_supports_visibility_scope = _supports_visibility_scope(
            reader.poll
        )
        self._reader_replay_supports_visibility_scope = _supports_visibility_scope(
            reader.replay
        )

    def subscribe(
        self,
        board_id: str,
        *,
        include_code_traceability: bool = True,
    ) -> KgEventsSubscription:
        if self._closed:
            raise RuntimeError("KgEventsHub is shut down")
        stream_key: str | tuple[str, bool] = (
            board_id
            if include_code_traceability
            else (board_id, False)
        )
        stream = self._streams.get(stream_key)
        if stream is None:
            stream = _BoardStream(
                board_id,
                include_code_traceability=include_code_traceability,
            )
            self._streams[stream_key] = stream
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
            cursor_event_id=stream.last_seen_event_id,
            initial_progress=stream.last_progress,
            include_code_traceability=include_code_traceability,
        )

    def unsubscribe(self, subscription: KgEventsSubscription) -> None:
        stream_key: str | tuple[str, bool] = (
            subscription.board_id
            if subscription.include_code_traceability
            else (subscription.board_id, False)
        )
        stream = self._streams.get(stream_key)
        if stream is None:
            return
        stream.subscribers.discard(subscription.queue)
        if not stream.subscribers:
            if stream.task is not None and not stream.task.done():
                stream.task.cancel()
            self._streams.pop(stream_key, None)

    async def replay(
        self,
        *,
        board_id: str,
        after: datetime,
        limit: int,
        after_event_id: str | None = None,
        include_code_traceability: bool = True,
    ) -> Sequence[KGOutboxEvent]:
        kwargs: dict[str, Any] = {
            "board_id": board_id,
            "after": after,
            "limit": limit,
        }
        if self._reader_replay_supports_composite_cursor:
            kwargs["after_event_id"] = after_event_id
        if self._reader_replay_supports_visibility_scope:
            kwargs["include_code_traceability"] = include_code_traceability
        elif not include_code_traceability:
            # A legacy provider cannot prove pre-provider filtering. Denied
            # subscriptions therefore see an empty replay, never a fallback
            # unscoped read.
            return ()
        return await self._reader.replay(
            **kwargs,
        )

    async def aclose(self) -> None:
        self._closed = True
        streams = list(self._streams.values())
        self._streams.clear()
        for stream in streams:
            if stream.task is not None and not stream.task.done():
                try:
                    stream.task.cancel()
                except RuntimeError:
                    # The task belongs to an already-closed loop (e.g. an
                    # asyncio.run() that finished in a previous test); the
                    # loop is gone along with its callbacks — nothing to
                    # cancel, and raising here would poison teardown.
                    continue
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
                kwargs: dict[str, Any] = {
                    "board_id": stream.board_id,
                    "after": stream.last_seen,
                    "limit": OUTBOX_BATCH_LIMIT,
                }
                if self._reader_poll_supports_composite_cursor:
                    kwargs["after_event_id"] = stream.last_seen_event_id
                if self._reader_poll_supports_visibility_scope:
                    kwargs["include_code_traceability"] = (
                        stream.include_code_traceability
                    )
                elif not stream.include_code_traceability:
                    # No unsafe provider fallback for a restricted stream.
                    await asyncio.sleep(self._poll_interval)
                    continue
                result = await self._reader.poll(**kwargs)
                ordered_events = sorted(
                    result.events,
                    key=_event_order_key,
                )
                for event in ordered_events:
                    if event.created_at is None:
                        logger.warning(
                            "kg_events_hub.event_missing_created_at "
                            "board=%s event_id=%s",
                            stream.board_id,
                            event.event_id,
                            extra={
                                "event": "kg_events_hub.event_missing_created_at",
                                "board_id": stream.board_id,
                                "event_id": event.event_id,
                            },
                        )
                        continue
                    if not _event_is_after_cursor(
                        event,
                        after=stream.last_seen,
                        after_event_id=stream.last_seen_event_id,
                    ):
                        continue
                    stream.broadcast(format_outbox_row_sse(event))
                    if event.created_at is not None:
                        stream.last_seen = _as_utc(event.created_at)
                        stream.last_seen_event_id = event.event_id
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
