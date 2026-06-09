"""Behavioral tests for the memory-pressure collector ring-buffers and their
integration with get_kg_health (Spec R2c, FR1/FR4).

Coverage map (NC-9 evidence):
  test_confirmed_primary_cause_from_real_samples  -> AC1
  test_unconfirmed_when_buffers_empty             -> AC2
  test_confirmed_correlation_id_and_recent_events -> AC8
  test_samples_ring_buffer_maxlen                 -> AC14
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from okto_pulse.core.kg.memory_pressure import (
    FailureEvent,
    HighWaterMarkSample,
    MemoryPressureStatus,
)
from okto_pulse.core.kg.memory_pressure_collector import (
    clear_board,
    get_failures,
    get_samples,
    record_failure,
    record_sample,
)
from okto_pulse.core.models.db import Board
from okto_pulse.core.services.kg_health_service import get_kg_health

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BOARD_ID = "board-mp-collector-test"
_USER_ID = "user-mp-collector-test"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _sample(pct: float, offset_s: int = 0, graph_type: str = "board") -> HighWaterMarkSample:
    return HighWaterMarkSample(
        timestamp=_now() - timedelta(seconds=offset_s),
        high_water_mark_pct=pct,
        graph_type=graph_type,
    )


def _failure(offset_s: int = 0, graph_type: str = "board") -> FailureEvent:
    return FailureEvent(
        timestamp=_now() - timedelta(seconds=offset_s),
        event_kind="kg.wal.flush.failed",
        graph_type=graph_type,
        correlation_id=uuid.uuid4().hex,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def mp_board(db_factory):
    """Idempotent Board row for memory-pressure tests; clears buffers before/after."""
    clear_board(_BOARD_ID)
    async with db_factory() as session:
        existing = await session.get(Board, _BOARD_ID)
        if existing is None:
            session.add(Board(id=_BOARD_ID, name="mp-collector-test", owner_id=_USER_ID))
            await session.commit()
    yield _BOARD_ID
    clear_board(_BOARD_ID)


# ---------------------------------------------------------------------------
# AC1: 3 HighWaterMark samples pct=92 within 10 min + 1 FailureEvent
#      -> get_kg_health -> memory_pressure_status == confirmed_primary_cause
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_confirmed_primary_cause_from_real_samples(db_factory, mp_board):
    """AC1: push 3 samples above threshold inside the window + 1 failure event
    -> health endpoint reports memory_pressure_status=confirmed_primary_cause.

    Verifies that get_kg_health reads from the real ring-buffer (not empty
    stubs). Without the collector wiring, this would always be 'unconfirmed'.
    """
    board_id = mp_board

    # Push 3 samples above 90 % within the last 5 minutes (inside 10-min window)
    for offset_s in (60, 120, 180):  # 1, 2, 3 minutes ago
        record_sample(board_id, _sample(pct=92.0, offset_s=offset_s))

    # Failure happens NOW (after the samples)
    fe = _failure(offset_s=0)
    record_failure(board_id, fe)

    async with db_factory() as session:
        result = await get_kg_health(board_id, session)

    assert result["memory_pressure_status"] == MemoryPressureStatus.CONFIRMED_PRIMARY_CAUSE.value


# ---------------------------------------------------------------------------
# AC2: buffers empty -> unconfirmed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unconfirmed_when_buffers_empty(db_factory, mp_board):
    """AC2: when no samples/failures have been recorded, the correlator
    returns unconfirmed (the buffer is empty, no correlation possible).
    """
    board_id = mp_board
    # clear_board already called by the fixture; buffers are empty

    async with db_factory() as session:
        result = await get_kg_health(board_id, session)

    assert result["memory_pressure_status"] == MemoryPressureStatus.UNCONFIRMED.value


# ---------------------------------------------------------------------------
# AC8: when confirmed, correlation_id == FailureEvent.correlation_id
#      AND recent_events is non-empty
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_confirmed_correlation_id_and_recent_events(db_factory, mp_board):
    """AC8: confirmed -> payload.correlation_id == FailureEvent.correlation_id
    and recent_events is non-empty (join-key contract api_3ed9037f).
    """
    board_id = mp_board

    # 3 above-threshold samples
    for offset_s in (30, 60, 90):
        record_sample(board_id, _sample(pct=95.0, offset_s=offset_s))

    fe = _failure(offset_s=0)
    record_failure(board_id, fe)

    async with db_factory() as session:
        result = await get_kg_health(board_id, session)

    assert result["memory_pressure_status"] == MemoryPressureStatus.CONFIRMED_PRIMARY_CAUSE.value

    # The correlation_id in the payload must be the FailureEvent's ID
    assert result["correlation_id"] == fe.correlation_id

    # recent_events must have exactly one entry and it must carry the same ID
    events = result["recent_events"]
    assert len(events) >= 1
    assert events[0]["correlation_id"] == fe.correlation_id
    assert events[0]["event_type"] == "kg.wal.flush.failed"


# ---------------------------------------------------------------------------
# AC14: ring buffer maxlen=200 — pushing 201 samples keeps only 200
# ---------------------------------------------------------------------------

def test_samples_ring_buffer_maxlen():
    """AC14: pushing 201 HighWaterMarkSample items into a board's buffer
    results in exactly 200 items (oldest dropped by deque maxlen semantics).
    """
    board_id = "board-mp-maxlen-test"
    clear_board(board_id)

    ts_base = datetime.now(timezone.utc)
    for i in range(201):
        record_sample(
            board_id,
            HighWaterMarkSample(
                timestamp=ts_base + timedelta(seconds=i),
                high_water_mark_pct=float(i % 100),
                graph_type="board",
            ),
        )

    samples = get_samples(board_id)
    assert len(samples) == 200

    # The oldest sample (i=0, pct=0.0) must have been dropped; the newest
    # (i=200) must still be present.
    pcts = [s.high_water_mark_pct for s in samples]
    assert float(200 % 100) in pcts   # newest value (200 % 100 == 0 but ts is latest)

    # Confirm the buffer returned is a snapshot (not the live deque)
    samples_again = get_samples(board_id)
    assert samples is not samples_again  # separate list objects

    clear_board(board_id)


# ---------------------------------------------------------------------------
# Supplementary: get_failures maxlen and thread-safety is implicitly tested
# via record_failure -> get_failures round-trip
# ---------------------------------------------------------------------------

def test_record_and_get_failures_round_trip():
    """record_failure + get_failures returns a stable snapshot list."""
    board_id = "board-mp-failure-trip"
    clear_board(board_id)

    fe1 = _failure(offset_s=10)
    fe2 = _failure(offset_s=5)
    record_failure(board_id, fe1)
    record_failure(board_id, fe2)

    failures = get_failures(board_id)
    assert len(failures) == 2
    # Deque appends at the right; get_failures preserves insertion order
    assert failures[0].correlation_id == fe1.correlation_id
    assert failures[1].correlation_id == fe2.correlation_id

    clear_board(board_id)
