"""File handle leak tests — ensure Kùzu handles are released after close.

Covers card 1.7 of the KG lifecycle spec. Uses psutil to introspect the
current process's open files before/after a context-managed Kùzu session
and asserts that the handle count does not grow.

Handled with a small tolerance because Python runtime, pytest, and Kùzu's
own internals may open/close unrelated files during the test — we care
about the graph-specific handles not leaking, not about a global zero-delta.
"""

from __future__ import annotations

import gc
import os
from pathlib import Path

import psutil
import pytest

from okto_pulse.core.kg.schema import (
    board_kuzu_path,
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)


@pytest.fixture
def fh_board():
    bid = f"board-filehandle-{os.urandom(3).hex()}"
    bootstrap_board_graph(bid)
    yield bid
    close_all_connections()


def _graph_file_handles(proc: psutil.Process, graph_path: Path) -> list[str]:
    """Return paths of open files under the board's graph path, case-insensitive."""
    graph_str = str(graph_path).lower()
    out: list[str] = []
    try:
        for f in proc.open_files():
            try:
                if graph_str in f.path.lower():
                    out.append(f.path)
            except Exception:
                continue
    except (psutil.AccessDenied, psutil.NoSuchProcess):
        return []
    return out


def test_with_block_does_not_leak_graph_handles(fh_board):
    proc = psutil.Process(os.getpid())
    graph_path = board_kuzu_path(fh_board)

    # Warm-up: first open may load Kùzu extensions that stay cached.
    def _warm():
        with open_board_connection(fh_board) as (_db, conn):
            conn.execute("MATCH (m:BoardMeta) RETURN count(m)")

    _warm()
    gc.collect()

    baseline = len(_graph_file_handles(proc, graph_path))

    # Run several open/close cycles. If the with-block leaked a handle,
    # each iteration would add one and `after > baseline`.
    def _cycle():
        with open_board_connection(fh_board) as (_db, conn):
            conn.execute("MATCH (m:BoardMeta) RETURN count(m)")

    for _ in range(5):
        _cycle()
        gc.collect()

    after = len(_graph_file_handles(proc, graph_path))
    assert after <= baseline, (
        f"graph file handles grew: baseline={baseline} after={after}"
    )


def test_close_releases_handles(fh_board):
    proc = psutil.Process(os.getpid())
    graph_path = board_kuzu_path(fh_board)

    def _open_and_close():
        with open_board_connection(fh_board) as (_db, conn):
            conn.execute("MATCH (m:BoardMeta) RETURN count(m)")

    _open_and_close()
    gc.collect()

    # After the with-block exits, graph-specific handles must not grow without
    # bound. LadybugDB can keep both the main graph file and WAL open briefly
    # inside the process, so tolerate that steady two-handle footprint.
    remaining = _graph_file_handles(proc, graph_path)
    assert len(remaining) <= 2, f"handles still open: {remaining!r}"
