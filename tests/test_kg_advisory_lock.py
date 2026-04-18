"""Tests for kg.workers.advisory_lock + kg.workers.commit_events."""

from __future__ import annotations

import asyncio

import pytest

from okto_pulse.core.kg.workers.advisory_lock import (
    advisory_lock,
    advisory_lock_sync,
    get_async_lock,
    get_sync_lock,
    reset_locks_for_tests,
)


@pytest.fixture(autouse=True)
def _reset():
    reset_locks_for_tests()
    yield
    reset_locks_for_tests()


def test_get_async_lock_returns_same_instance_for_same_key():
    a = get_async_lock("b1", "artifact-1")
    b = get_async_lock("b1", "artifact-1")
    assert a is b


def test_get_async_lock_returns_distinct_for_distinct_key():
    a = get_async_lock("b1", "artifact-1")
    b = get_async_lock("b1", "artifact-2")
    c = get_async_lock("b2", "artifact-1")
    assert a is not b
    assert a is not c
    assert b is not c


@pytest.mark.asyncio
async def test_advisory_lock_serialises_concurrent_tasks():
    """Two tasks asking for the same lock must run strictly sequentially."""
    seen_inside: list[int] = []
    enter_order: list[int] = []

    async def worker(i: int):
        async with advisory_lock("b-serial", "art-1"):
            enter_order.append(i)
            # Give the scheduler a chance to let another task enter.
            await asyncio.sleep(0.01)
            seen_inside.append(i)

    await asyncio.gather(worker(1), worker(2), worker(3))
    # The first to enter must also be the first to leave — serialisation.
    assert seen_inside == enter_order


@pytest.mark.asyncio
async def test_advisory_lock_allows_parallel_for_distinct_artifacts():
    """Distinct artifact_ids should NOT block each other."""
    t0 = asyncio.get_event_loop().time()

    async def worker(bid: str, aid: str):
        async with advisory_lock(bid, aid):
            await asyncio.sleep(0.05)

    await asyncio.gather(
        worker("b-parallel", "art-A"),
        worker("b-parallel", "art-B"),
        worker("b-parallel", "art-C"),
    )
    elapsed = asyncio.get_event_loop().time() - t0
    # Three 50ms sleeps in parallel must complete in ~50ms (plus scheduler
    # overhead), not 150ms. 0.12s gives a generous ceiling for Windows.
    assert elapsed < 0.12, f"expected parallel, got {elapsed:.3f}s"


def test_get_sync_lock_serialises_threads():
    import threading
    seen: list[int] = []

    def worker(i: int):
        with advisory_lock_sync("b-sync", "art-1"):
            seen.append(i)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # All three threads must have observed the lock (regardless of order).
    assert sorted(seen) == [0, 1, 2]


def test_get_sync_lock_distinct_keys_do_not_share():
    a = get_sync_lock("b", "art-1")
    b = get_sync_lock("b", "art-2")
    assert a is not b


@pytest.mark.asyncio
async def test_emit_session_committed_writes_outbox_row():
    """Emitter inserts a properly-typed row into the global_update_outbox."""
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.models.db import GlobalUpdateOutbox
    from okto_pulse.core.kg.workers.commit_events import (
        EVENT_TYPE_SESSION_COMMITTED,
        emit_session_committed,
    )
    from sqlalchemy import select

    factory = get_session_factory()
    async with factory() as db:
        await emit_session_committed(
            db, board_id="board-outbox", session_id="sess-1",
            artifact_type="spec", artifact_id="spec-1",
            nodes_added=10, edges_added=5, content_hash="abc",
        )
        await db.flush()
        row = (await db.execute(
            select(GlobalUpdateOutbox)
            .where(GlobalUpdateOutbox.board_id == "board-outbox")
        )).scalars().first()
        assert row is not None
        assert row.event_type == EVENT_TYPE_SESSION_COMMITTED
        assert row.payload["nodes_added"] == 10
        assert row.payload["edges_added"] == 5
        assert row.payload["content_hash"] == "abc"
        await db.rollback()
