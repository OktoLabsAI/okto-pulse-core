"""Composite cursor contract for the transport-neutral KG events hub."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.application import kg_events_hub as hub_module
from okto_pulse.core.application.kg_events_hub import (
    KgEventsHub,
    KgEventsSubscription,
    format_outbox_row_sse,
)
from okto_pulse.core.ports.kg_events import KGEventsPoll, KGOutboxEvent


def _event(event_id: str, created_at: datetime) -> KGOutboxEvent:
    return KGOutboxEvent(
        event_id=event_id,
        session_id=None,
        event_type="kg.session.committed",
        created_at=created_at,
        payload={},
    )


def _is_after(
    event: KGOutboxEvent,
    *,
    after: datetime,
    after_event_id: str | None,
) -> bool:
    assert event.created_at is not None
    if event.created_at != after:
        return event.created_at > after
    return after_event_id is not None and event.event_id > after_event_id


def _event_id_from_sse(chunk: str) -> str:
    data_line = next(line for line in chunk.splitlines() if line.startswith("data: "))
    return str(json.loads(data_line.removeprefix("data: "))["event_id"])


def test_outbox_sse_normalizes_naive_timestamp_to_utc() -> None:
    chunk = format_outbox_row_sse(
        _event("event-naive", datetime(2026, 7, 27, 9, 42, 1, 123456))
    )
    data_line = next(line for line in chunk.splitlines() if line.startswith("data: "))
    payload = json.loads(data_line.removeprefix("data: "))

    assert payload["created_at"] == "2026-07-27T09:42:01.123456+00:00"


class _CompositeReader:
    def __init__(self, events: list[KGOutboxEvent]) -> None:
        self.events = events
        self.poll_calls: list[tuple[datetime, str | None, int]] = []
        self.replay_calls: list[tuple[datetime, str | None, int]] = []

    def _page(
        self,
        *,
        after: datetime,
        after_event_id: str | None,
        limit: int,
    ) -> list[KGOutboxEvent]:
        ordered = sorted(
            self.events,
            key=lambda event: (event.created_at, event.event_id),
        )
        return [
            event
            for event in ordered
            if _is_after(
                event,
                after=after,
                after_event_id=after_event_id,
            )
        ][:limit]

    async def poll(
        self,
        *,
        board_id: str,
        after: datetime,
        limit: int,
        after_event_id: str | None = None,
    ) -> KGEventsPoll:
        del board_id
        self.poll_calls.append((after, after_event_id, limit))
        return KGEventsPoll(
            events=self._page(
                after=after,
                after_event_id=after_event_id,
                limit=limit,
            ),
            progress={"pending": 0, "total": 0},
        )

    async def replay(
        self,
        *,
        board_id: str,
        after: datetime,
        limit: int,
        after_event_id: str | None = None,
    ) -> list[KGOutboxEvent]:
        del board_id
        self.replay_calls.append((after, after_event_id, limit))
        return self._page(
            after=after,
            after_event_id=after_event_id,
            limit=limit,
        )


@pytest.mark.asyncio
async def test_hub_pages_all_events_tied_at_timestamp_without_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(hub_module, "OUTBOX_BATCH_LIMIT", 2)
    created_at = datetime.now(timezone.utc) + timedelta(minutes=1)
    reader = _CompositeReader(
        [_event(f"event-{index:02d}", created_at) for index in reversed(range(5))]
    )
    hub = KgEventsHub(reader, poll_interval=0.001)
    subscription = hub.subscribe("board-tied-events")

    try:
        received: list[str] = []
        while len(received) < 5:
            chunk = await asyncio.wait_for(subscription.queue.get(), timeout=2)
            if "event: kg.session.committed" in chunk:
                received.append(_event_id_from_sse(chunk))

        assert received == [f"event-{index:02d}" for index in range(5)]
        assert [call[1] for call in reader.poll_calls[:3]] == [
            None,
            "event-01",
            "event-03",
        ]
        assert all(call[2] == 2 for call in reader.poll_calls[:3])

        resumed = hub.subscribe("board-tied-events")
        try:
            assert resumed.cursor == created_at
            assert resumed.cursor_event_id == "event-04"
        finally:
            hub.unsubscribe(resumed)
    finally:
        hub.unsubscribe(subscription)
        await hub.aclose()


@pytest.mark.asyncio
async def test_hub_broadcasts_one_provider_batch_in_composite_order() -> None:
    created_at = datetime.now(timezone.utc) + timedelta(minutes=1)

    class _UnorderedReader(_CompositeReader):
        async def poll(self, **kwargs) -> KGEventsPoll:
            self.poll_calls.append(
                (
                    kwargs["after"],
                    kwargs.get("after_event_id"),
                    kwargs["limit"],
                )
            )
            if len(self.poll_calls) == 1:
                return KGEventsPoll(
                    events=[
                        _event("event-c", created_at),
                        _event("event-a", created_at),
                        _event("event-b", created_at),
                    ],
                    progress={},
                )
            return KGEventsPoll(events=[], progress={})

    hub = KgEventsHub(_UnorderedReader([]), poll_interval=0.001)
    subscription = hub.subscribe("board-unordered-provider")
    try:
        received: list[str] = []
        while len(received) < 3:
            chunk = await asyncio.wait_for(subscription.queue.get(), timeout=2)
            if "event: kg.session.committed" in chunk:
                received.append(_event_id_from_sse(chunk))
        assert received == ["event-a", "event-b", "event-c"]
    finally:
        hub.unsubscribe(subscription)
        await hub.aclose()


@pytest.mark.asyncio
async def test_replay_forwards_composite_boundary_and_keeps_legacy_default() -> None:
    created_at = datetime(2026, 7, 27, tzinfo=timezone.utc)
    reader = _CompositeReader(
        [
            _event("event-a", created_at),
            _event("event-b", created_at),
            _event("event-c", created_at),
        ]
    )
    hub = KgEventsHub(reader)

    resumed = await hub.replay(
        board_id="board-replay",
        after=created_at,
        after_event_id="event-a",
        limit=10,
    )
    legacy = await hub.replay(
        board_id="board-replay",
        after=created_at - timedelta(seconds=1),
        limit=10,
    )

    assert [event.event_id for event in resumed] == ["event-b", "event-c"]
    assert [event.event_id for event in legacy] == [
        "event-a",
        "event-b",
        "event-c",
    ]
    assert reader.replay_calls == [
        (created_at, "event-a", 10),
        (created_at - timedelta(seconds=1), None, 10),
    ]


@pytest.mark.asyncio
async def test_hub_keeps_legacy_reader_without_composite_keyword_compatible() -> None:
    created_at = datetime.now(timezone.utc) + timedelta(minutes=1)

    class _LegacyReader:
        def __init__(self) -> None:
            self.poll_calls = 0
            self.replay_calls = 0

        async def poll(
            self,
            *,
            board_id: str,
            after: datetime,
            limit: int,
        ) -> KGEventsPoll:
            del board_id, after, limit
            self.poll_calls += 1
            events = [_event("event-legacy", created_at)] if self.poll_calls == 1 else []
            return KGEventsPoll(events=events, progress={"pending": 0, "total": 1})

        async def replay(
            self,
            *,
            board_id: str,
            after: datetime,
            limit: int,
        ) -> list[KGOutboxEvent]:
            del board_id, after, limit
            self.replay_calls += 1
            return []

    reader = _LegacyReader()
    hub = KgEventsHub(reader, poll_interval=0.001)
    subscription = hub.subscribe("board-legacy-reader")
    try:
        chunk = await asyncio.wait_for(subscription.queue.get(), timeout=2)
        while "event-legacy" not in chunk:
            chunk = await asyncio.wait_for(subscription.queue.get(), timeout=2)
        for _ in range(100):
            if reader.poll_calls >= 2:
                break
            await asyncio.sleep(0.001)

        assert reader.poll_calls >= 2
        await hub.replay(
            board_id="board-legacy-reader",
            after=created_at,
            after_event_id="event-legacy",
            limit=10,
        )
        assert reader.replay_calls == 1
    finally:
        hub.unsubscribe(subscription)
        await hub.aclose()


def test_subscription_preserves_legacy_positional_progress_argument() -> None:
    queue: asyncio.Queue[str] = asyncio.Queue()
    cursor = datetime(2026, 7, 27, tzinfo=timezone.utc)
    progress = {"pending": 1, "total": 1}

    subscription = KgEventsSubscription(
        "board-legacy-constructor",
        queue,
        cursor,
        progress,
    )

    assert subscription.initial_progress is progress
    assert subscription.cursor_event_id is None


@pytest.mark.asyncio
async def test_hub_drops_missing_timestamp_event_without_rebroadcast(
    caplog: pytest.LogCaptureFixture,
) -> None:
    invalid = _event("event-without-time", datetime.now(timezone.utc))
    invalid = KGOutboxEvent(
        event_id=invalid.event_id,
        session_id=invalid.session_id,
        event_type=invalid.event_type,
        created_at=None,
        payload=invalid.payload,
    )

    class _RepeatingInvalidReader(_CompositeReader):
        async def poll(self, **kwargs) -> KGEventsPoll:
            self.poll_calls.append(
                (
                    kwargs["after"],
                    kwargs.get("after_event_id"),
                    kwargs["limit"],
                )
            )
            return KGEventsPoll(events=[invalid], progress={})

    reader = _RepeatingInvalidReader([])
    hub = KgEventsHub(reader, poll_interval=0.001)
    subscription = hub.subscribe("board-invalid-timestamp")
    try:
        for _ in range(100):
            if len(reader.poll_calls) >= 3:
                break
            await asyncio.sleep(0.001)
        assert len(reader.poll_calls) >= 2
        queued_chunks = []
        while not subscription.queue.empty():
            queued_chunks.append(subscription.queue.get_nowait())
        assert all("event-without-time" not in chunk for chunk in queued_chunks)
        assert subscription.cursor_event_id is None
        assert "kg_events_hub.event_missing_created_at" in caplog.text
    finally:
        hub.unsubscribe(subscription)
        await hub.aclose()
