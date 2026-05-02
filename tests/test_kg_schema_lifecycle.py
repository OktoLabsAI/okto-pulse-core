"""Lifecycle tests for BoardConnection, ConnectionPool, and close_all_connections.

Covers card 1.7 of the KG lifecycle spec:
  (a) context manager happy path
  (b) __enter__/__exit__ invokes close() + gc
  (c) ConnectionPool cap=2 eviction on third board insert
  (d) KG_CONNECTION_POOL_SIZE=0 disables caching
  (e) concurrent acquire/release from 4 threads
  (f) close_all releases every pooled connection
"""

from __future__ import annotations

import gc
import os
import threading

import pytest

from okto_pulse.core.kg.connection_pool import (
    ConnectionPool,
    _read_cap_from_env,
    reset_connection_pool_for_tests,
)
from okto_pulse.core.kg.schema import (
    BoardConnection,
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


@pytest.fixture
def fresh_boards():
    """Bootstrap a fresh set of boards per test to avoid cross-test state."""
    ids = [f"board-lifecycle-{i}-{os.urandom(3).hex()}" for i in range(4)]
    for bid in ids:
        bootstrap_board_graph(bid)
    yield ids
    close_all_connections()
    reset_connection_pool_for_tests()


# ----------------------------------------------------------------------
# (a) context manager happy path
# ----------------------------------------------------------------------

def test_context_manager_yields_db_and_conn(fresh_boards):
    bid = fresh_boards[0]
    with open_board_connection(bid) as (db, conn):
        assert db is not None
        assert conn is not None
        result = conn.execute("MATCH (m:BoardMeta) RETURN count(m)")
        assert result.has_next()


def test_context_manager_releases_lock_for_reopen(fresh_boards):
    bid = fresh_boards[0]

    # Each open/close is isolated in its own function scope so the
    # `as (_db, conn)` locals get released at return. If we used a single
    # function scope here, those locals would outlive __exit__ and hold the
    # Windows file lock past the next open.
    def _use_once():
        with open_board_connection(bid) as (_db, conn):
            conn.execute("MATCH (m:BoardMeta) RETURN count(m)")

    _use_once()
    gc.collect()
    _use_once()  # must not raise "Could not set lock on file"


# ----------------------------------------------------------------------
# (b) __enter__/__exit__ invokes close() + gc
# ----------------------------------------------------------------------

def test_exit_marks_closed(fresh_boards):
    bid = fresh_boards[0]
    bc = BoardConnection(bid)
    assert bc._closed is False
    with bc as (_db, _conn):
        pass
    assert bc._closed is True


def test_close_is_idempotent(fresh_boards):
    bid = fresh_boards[0]
    bc = BoardConnection(bid)
    bc.close()
    bc.close()  # must not raise


# ----------------------------------------------------------------------
# (c) ConnectionPool cap=2 eviction
# ----------------------------------------------------------------------

def test_pool_evicts_lru_when_at_cap(fresh_boards):
    pool = ConnectionPool(cap=2)
    bid_a, bid_b, bid_c, _ = fresh_boards

    bc_a = pool.acquire(bid_a)
    _bc_b = pool.acquire(bid_b)
    assert len(pool) == 2

    _bc_c = pool.acquire(bid_c)  # evicts A (LRU)
    assert len(pool) == 2
    assert bid_a not in pool
    assert bid_b in pool
    assert bid_c in pool

    # Re-acquiring A produces a fresh BoardConnection
    bc_a2 = pool.acquire(bid_a)
    assert bc_a2 is not bc_a

    pool.close_all()


def test_pool_lru_order_updates_on_hit(fresh_boards):
    pool = ConnectionPool(cap=2)
    bid_a, bid_b, bid_c, _ = fresh_boards

    pool.acquire(bid_a)
    pool.acquire(bid_b)
    pool.acquire(bid_a)  # touch A -> B becomes LRU
    pool.acquire(bid_c)  # evicts B

    assert bid_a in pool
    assert bid_c in pool
    assert bid_b not in pool

    pool.close_all()


# ----------------------------------------------------------------------
# (d) KG_CONNECTION_POOL_SIZE=0 disables caching
# ----------------------------------------------------------------------

def test_pool_cap_zero_returns_fresh_each_time(fresh_boards):
    pool = ConnectionPool(cap=0)
    bid = fresh_boards[0]

    assert pool.enabled is False

    bc1 = pool.acquire(bid)
    bc1.close()  # Release the lock before acquiring again (Kùzu single-owner).

    bc2 = pool.acquire(bid)
    assert bc1 is not bc2
    assert len(pool) == 0

    bc2.close()


def test_env_reader_parses_values(monkeypatch):
    monkeypatch.setenv("KG_CONNECTION_POOL_SIZE", "3")
    assert _read_cap_from_env() == 3

    monkeypatch.setenv("KG_CONNECTION_POOL_SIZE", "0")
    assert _read_cap_from_env() == 0

    monkeypatch.setenv("KG_CONNECTION_POOL_SIZE", "-1")
    assert _read_cap_from_env() == 0

    monkeypatch.setenv("KG_CONNECTION_POOL_SIZE", "abc")
    # Falls back to default (8) on invalid input
    assert _read_cap_from_env() == 8

    monkeypatch.delenv("KG_CONNECTION_POOL_SIZE", raising=False)
    assert _read_cap_from_env() == 8


# ----------------------------------------------------------------------
# (e) concurrent acquire/release from 4 threads
# ----------------------------------------------------------------------

def test_pool_concurrent_acquire_is_safe(fresh_boards):
    pool = ConnectionPool(cap=4)
    errors: list[Exception] = []
    barrier = threading.Barrier(4)

    def worker(bid: str) -> None:
        try:
            barrier.wait(timeout=5)
            for _ in range(3):
                bc = pool.acquire(bid)
                assert bc is not None
                # Exercise the connection inside the pool-held handle.
                bc.conn.execute("MATCH (m:BoardMeta) RETURN count(m)")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(fresh_boards[i],))
        for i in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not errors, f"concurrent failures: {errors!r}"
    assert len(pool) == 4
    pool.close_all()


# ----------------------------------------------------------------------
# (f) close_all closes everything
# ----------------------------------------------------------------------

def test_pool_close_all_empties_and_allows_reopen(fresh_boards):
    pool = ConnectionPool(cap=4)
    for bid in fresh_boards[:3]:
        pool.acquire(bid)
    assert len(pool) == 3

    pool.close_all()
    assert len(pool) == 0

    # File locks released — re-opening must succeed on Windows.
    with open_board_connection(fresh_boards[0]) as (_db, conn):
        conn.execute("MATCH (m:BoardMeta) RETURN count(m)")


def test_close_all_connections_accepts_specific_board(fresh_boards):
    """close_all_connections(board_id=<id>) must not explode on missing pool."""
    bid = fresh_boards[0]
    with open_board_connection(bid) as (_db, _conn):
        pass
    close_all_connections(bid)
    close_all_connections()  # also the no-arg variant
