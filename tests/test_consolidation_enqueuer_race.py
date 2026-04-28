"""Regression tests for bug card 4a430c6d — ConsolidationEnqueuer race
condition fix (SELECT-then-INSERT replaced by dialect-aware UPSERT).

The handler at events/handlers/consolidation_enqueuer.py:_enqueue_one was
vulnerable to a TOCTOU race: when two domain events for the same
(board_id, artifact_type, artifact_id) were processed concurrently by the
dispatcher, both sessions did SELECT (neither saw the other's uncommitted
INSERT), both called session.add, and the second commit raised
``sqlite3.IntegrityError: UNIQUE constraint failed`` — only self-corrected
by the dispatcher's retry+backoff at the cost of log noise + latency.

These tests verify the post-fix behaviour:
  - concurrent enqueues for the same artifact merge atomically into 1 row,
  - terminal-state rows reset to pending under a new event,
  - in-flight rows (pending/claimed) are left untouched.

The structural assertion at the end guarantees the SELECT-then-INSERT
idiom is gone so a future regression that reintroduces it gets flagged.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from okto_pulse.core.events.handlers.consolidation_enqueuer import (
    ConsolidationEnqueuer,
)
from okto_pulse.core.events.types import CardMoved
from okto_pulse.core.infra.database import Base
from okto_pulse.core.models.db import Board, ConsolidationQueue


@pytest_asyncio.fixture
async def race_db_factory(tmp_path):
    """Per-test SQLite file (NOT in-memory) so multiple AsyncSessions can
    open distinct connections to the same database — required to exercise
    the cross-session race that the in-memory engine cannot reproduce
    (in-memory uses StaticPool which serialises everything)."""
    db_path = tmp_path / "race.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}",
        future=True,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    # Seed a board so FK constraints don't fire on the queue inserts.
    board_id = str(uuid.uuid4())
    async with factory() as session:
        session.add(
            Board(
                id=board_id,
                name="race-test",
                description="bug 4a430c6d regression",
                owner_id="test-owner",
            )
        )
        await session.commit()

    try:
        yield factory, board_id
    finally:
        await engine.dispose()


def _card_moved_event(board_id: str, card_id: str) -> CardMoved:
    return CardMoved(
        event_id=str(uuid.uuid4()),
        board_id=board_id,
        actor_id="test-actor",
        actor_type="user",
        occurred_at=datetime.now(UTC),
        card_id=card_id,
        from_status="started",
        to_status="in_progress",
        spec_id=None,
    )


# ---------------------------------------------------------------------------
# Behavioural — race condition reproducer + fix verification
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_enqueues_for_same_artifact_atomically_merge(race_db_factory):
    """The exact race from the bug log: 2 events for the same card processed
    by 2 distinct AsyncSessions in parallel. Pre-fix: sqlite3.IntegrityError
    on the second commit. Post-fix: both succeed atomically; exactly 1 row
    in consolidation_queue."""
    factory, board_id = race_db_factory
    card_id = str(uuid.uuid4())

    handler = ConsolidationEnqueuer()

    async def enqueue_in_own_session():
        async with factory() as session:
            event = _card_moved_event(board_id, card_id)
            await handler.handle(event, session)
            await session.commit()

    # Launch two enqueues concurrently. asyncio.gather waits for both.
    # If the bug were still present, one of the two would raise
    # IntegrityError; gather propagates the first exception.
    results = await asyncio.gather(
        enqueue_in_own_session(),
        enqueue_in_own_session(),
        return_exceptions=True,
    )

    # No IntegrityError surfaced.
    for r in results:
        assert not isinstance(r, IntegrityError), (
            f"UPSERT race fix regressed: got IntegrityError {r}"
        )
        assert not isinstance(r, Exception), (
            f"Unexpected exception during concurrent enqueue: {r!r}"
        )

    # Exactly 1 row in the queue for this artifact.
    async with factory() as session:
        from sqlalchemy import select
        rows = (await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.artifact_type == "card",
                ConsolidationQueue.artifact_id == card_id,
            )
        )).scalars().all()
    assert len(rows) == 1, f"expected 1 merged row; got {len(rows)}"
    assert rows[0].status == "pending"
    assert rows[0].attempts == 0


@pytest.mark.asyncio
async def test_terminal_state_row_reset_to_pending_under_new_event(race_db_factory):
    """Pre-existing row in `done` state must be reset to `pending` (with
    attempts=0, last_error=None) when a new event arrives for the same
    artifact. Preserves the v1 NC-11 reset semantics across the upsert
    path."""
    factory, board_id = race_db_factory
    card_id = str(uuid.uuid4())

    # Seed the queue with a terminal-state row for the artifact.
    async with factory() as session:
        session.add(
            ConsolidationQueue(
                board_id=board_id,
                artifact_type="card",
                artifact_id=card_id,
                priority="normal",
                source="event:card.cancelled",
                triggered_by_event="card.cancelled",
                status="done",
                attempts=3,
                last_error="prev failure",
            )
        )
        await session.commit()

    handler = ConsolidationEnqueuer()
    async with factory() as session:
        event = _card_moved_event(board_id, card_id)
        await handler.handle(event, session)
        await session.commit()

    async with factory() as session:
        from sqlalchemy import select
        row = (await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.artifact_type == "card",
                ConsolidationQueue.artifact_id == card_id,
            )
        )).scalar_one()
    assert row.status == "pending"
    assert row.attempts == 0
    assert row.last_error is None
    assert row.triggered_by_event == "card.moved"


@pytest.mark.asyncio
async def test_in_flight_pending_row_left_untouched(race_db_factory):
    """Pre-existing row in `pending` state (worker hasn't claimed yet) must
    be a no-op — the WHERE filter on the conflict_update branch keeps the
    row's identity (attempts, claimed_by, etc.) intact. Worker will pick up
    the in-flight row; the new event is silently merged."""
    factory, board_id = race_db_factory
    card_id = str(uuid.uuid4())

    async with factory() as session:
        session.add(
            ConsolidationQueue(
                board_id=board_id,
                artifact_type="card",
                artifact_id=card_id,
                priority="normal",
                source="event:card.created",
                triggered_by_event="card.created",
                status="pending",
                attempts=1,  # already retried once
            )
        )
        await session.commit()

    handler = ConsolidationEnqueuer()
    async with factory() as session:
        event = _card_moved_event(board_id, card_id)
        await handler.handle(event, session)
        await session.commit()

    async with factory() as session:
        from sqlalchemy import select
        row = (await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.artifact_type == "card",
                ConsolidationQueue.artifact_id == card_id,
            )
        )).scalar_one()
    # WHERE filter blocked the update — original triggered_by_event is preserved.
    assert row.triggered_by_event == "card.created", (
        "in-flight row was overwritten — WHERE filter regressed"
    )
    assert row.attempts == 1
    assert row.status == "pending"


@pytest.mark.asyncio
async def test_serial_events_on_same_artifact_produce_one_row(race_db_factory):
    """Backward-compat: rapid serial events on the same artifact (typical
    of card transitions started → in_progress → validation → done) still
    converge on a single row, regardless of concurrency."""
    factory, board_id = race_db_factory
    card_id = str(uuid.uuid4())

    handler = ConsolidationEnqueuer()
    for _ in range(5):
        async with factory() as session:
            event = _card_moved_event(board_id, card_id)
            await handler.handle(event, session)
            await session.commit()

    async with factory() as session:
        from sqlalchemy import select
        rows = (await session.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.artifact_id == card_id,
            )
        )).scalars().all()
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# Structural — verify the legacy SELECT-then-INSERT idiom is gone
# ---------------------------------------------------------------------------

def test_legacy_select_then_insert_idiom_is_gone():
    """The previous v1 path used `session.add(ConsolidationQueue(...))`
    after a SELECT lookup. Post-fix uses `session.execute(stmt)` with a
    dialect-specific upsert. A future regression that copy-pastes the old
    idiom back into _enqueue_one gets caught by this grep-style assertion.
    """
    src = (
        Path(__file__).resolve().parent.parent
        / "src"
        / "okto_pulse"
        / "core"
        / "events"
        / "handlers"
        / "consolidation_enqueuer.py"
    ).read_text(encoding="utf-8")
    # session.add(ConsolidationQueue(...)) was the v1 INSERT path.
    assert "session.add(\n            ConsolidationQueue(" not in src, (
        "v1 SELECT-then-INSERT idiom returned to _enqueue_one — race fix regressed"
    )
    assert "session.add(ConsolidationQueue(" not in src, (
        "v1 SELECT-then-INSERT idiom returned to _enqueue_one — race fix regressed"
    )
    # Required post-fix idiom present.
    assert "on_conflict_do_update" in src, (
        "ConflictDoUpdate UPSERT call missing — race fix not in place"
    )
    assert "from sqlalchemy.dialects.sqlite import insert" in src
    assert "from sqlalchemy.dialects.postgresql import insert" in src
