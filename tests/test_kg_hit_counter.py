"""Tests for the R2 hit counter cache (kg_service.increment_hit).

The integration path (flush → Kùzu UPDATE) is covered by mocking
``open_board_connection`` so the test doesn't run afoul of Kùzu's
per-process exclusive file lock on Windows. The lock behaviour itself
is not under test — the cache/flush logic is.

Covers:
    * TS4/AC5 — cache respects threshold of 10 before flush
    * TS5/AC6 — age-based flush (>24h)
    * TS10/AC11 — concurrent hits against the same node
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from okto_pulse.core.kg.kg_service import (
    HIT_FLUSH_THRESHOLD,
    KGService,
    _LAST_FLUSH,
    _PENDING_HITS,
    _hits_snapshot,
    _reset_hit_state_for_tests,
)


class _StubBoardConnection:
    """Context-manager-compatible stub that records every execute() call.

    Exposes `.calls` as a list of (cypher, params) so the test can assert
    what the flush would have sent to Kùzu without opening a real DB.
    """

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def __enter__(self):
        return (MagicMock(), self)

    def __exit__(self, *args):
        return False

    def execute(self, cypher, params=None):
        self.calls.append((cypher, params or {}))


@pytest.fixture
def stub_conn():
    _reset_hit_state_for_tests()
    conn = _StubBoardConnection()
    with patch(
        "okto_pulse.core.kg.schema.open_board_connection",
        return_value=conn,
    ):
        yield conn
    _reset_hit_state_for_tests()


@pytest.mark.asyncio
async def test_ts4_hit_cache_respects_threshold(stub_conn):
    """TS4/AC5: 9 hits stay in cache; 10th flushes."""
    svc = KGService()

    for _ in range(9):
        await svc.increment_hit("b1", "Decision", "node_x")

    # Nothing flushed yet.
    assert stub_conn.calls == []
    assert _hits_snapshot()[("b1", "node_x")] == 9

    # 10th increment crosses the threshold.
    await svc.increment_hit("b1", "Decision", "node_x")

    # One UPDATE call with delta=10.
    assert len(stub_conn.calls) == 1
    cypher, params = stub_conn.calls[0]
    assert "SET n.query_hits = COALESCE(n.query_hits, 0) + $delta" in cypher
    assert params["delta"] == 10
    assert params["nid"] == "node_x"
    # Cache drained after flush.
    assert _hits_snapshot()[("b1", "node_x")] == 0


@pytest.mark.asyncio
async def test_ts5_age_triggered_flush(stub_conn):
    """TS5/AC6: pending count <10 but last_flush >24h → force flush."""
    svc = KGService()
    key = ("b1", "node_y")

    # Seed cache with 3 hits and last flush 25h ago.
    _PENDING_HITS[key] = 3
    _LAST_FLUSH[key] = datetime.now(timezone.utc) - timedelta(hours=25)

    await svc.increment_hit("b1", "Decision", "node_y")

    # The age-based flush should fire — count=4 is still below the
    # 10-threshold, yet the UPDATE lands.
    assert len(stub_conn.calls) == 1
    _, params = stub_conn.calls[0]
    assert params["delta"] == 4


@pytest.mark.asyncio
async def test_ts10_concurrent_hits_no_lost_updates(stub_conn):
    """TS10/AC11: 100 concurrent increments lose nothing."""
    svc = KGService()

    await asyncio.gather(*[
        svc.increment_hit("b1", "Decision", "node_z") for _ in range(100)
    ])

    # The flush could have fired multiple times along the way (every 10th
    # hit). Summing every observed delta must equal exactly 100 — i.e. no
    # hit was silently dropped by a race.
    deltas_seen = sum(call[1]["delta"] for call in stub_conn.calls)
    remainder = _hits_snapshot().get(("b1", "node_z"), 0)
    assert deltas_seen + remainder == 100


def test_threshold_constant_matches_spec():
    assert HIT_FLUSH_THRESHOLD == 10
