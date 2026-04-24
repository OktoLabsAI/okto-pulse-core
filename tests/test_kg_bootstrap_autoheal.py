"""Tests for the Bug #2 auto-bootstrap fix.

BoardConnection now self-heals missing or partial Kùzu graphs via
ensure_board_graph_bootstrapped(). This closes the gap where boards
created through the UI/API path never had their per-board graph
directory initialised and the consolidation worker crashed with
`Binder exception: Table Entity does not exist` on first commit.

Covers:
  1. ensure_board_graph_bootstrapped creates a graph when the
     directory is missing.
  2. Idempotency after an explicit bootstrap_board_graph call.
  3. open_board_connection lazily triggers the bootstrap.
  4. Concurrent opens serialize on the per-board lock without racing.
  5. Worker commit path works on a cold board (end-to-end Bug #2 regression).
  6. Event-triggered consolidation works on a cold board.
"""

from __future__ import annotations

import os
import shutil
import threading
import uuid

import pytest

from okto_pulse.core.kg.schema import (
    bootstrap_board_graph,
    board_kuzu_path,
    close_all_connections,
    ensure_board_graph_bootstrapped,
    open_board_connection,
    reset_bootstrap_cache_for_tests,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fresh_board_id(prefix: str = "bootstrap-autoheal") -> str:
    """Return a unique board id, guaranteeing no pre-existing graph."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _purge_board_graph(board_id: str) -> None:
    """Delete the graph directory and drop every cache so the next open
    triggers a clean bootstrap."""
    path = board_kuzu_path(board_id)
    close_all_connections(board_id)
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    parent = path.parent
    if parent.exists() and not any(parent.iterdir()):
        shutil.rmtree(parent, ignore_errors=True)
    reset_bootstrap_cache_for_tests()


@pytest.fixture
def fresh_board():
    bid = _fresh_board_id()
    _purge_board_graph(bid)
    yield bid
    # Teardown — release connections and cleanup.
    _purge_board_graph(bid)


# ---------------------------------------------------------------------------
# 1 + 2: ensure_* behaviour
# ---------------------------------------------------------------------------


def test_ensure_on_missing_directory_bootstraps(fresh_board):
    """ensure_board_graph_bootstrapped must create the .kuzu path when
    nothing exists on disk."""
    path = board_kuzu_path(fresh_board)
    assert not path.exists()

    ensure_board_graph_bootstrapped(fresh_board)

    assert path.exists(), "graph directory should exist after ensure_*"

    # BoardMeta table is the canonical proof of a full bootstrap.
    with open_board_connection(fresh_board) as (_db, conn):
        res = conn.execute(
            "CALL SHOW_TABLES() WHERE name = 'BoardMeta' RETURN name"
        )
        assert res.has_next()
        res.close()


def test_ensure_is_idempotent_after_explicit_bootstrap(fresh_board):
    """Calling ensure_* after bootstrap_board_graph must be a cheap no-op
    (no exception, no schema rewrite)."""
    bootstrap_board_graph(fresh_board)

    # Drop the cache so ensure_* reaches the probe path — this is what
    # the API/worker flow looks like on a fresh process.
    reset_bootstrap_cache_for_tests()

    ensure_board_graph_bootstrapped(fresh_board)

    with open_board_connection(fresh_board) as (_db, conn):
        res = conn.execute("MATCH (m:BoardMeta) RETURN count(m) AS c")
        assert res.has_next()
        row = res.get_next()
        res.close()
        # Exactly one BoardMeta row — ensure_* didn't re-run the CREATE.
        assert row[0] == 1


# ---------------------------------------------------------------------------
# 3: open_board_connection is self-healing
# ---------------------------------------------------------------------------


def test_open_board_connection_autobootstraps(fresh_board):
    """BoardConnection.__init__ must invoke ensure_board_graph_bootstrapped
    so API/worker paths that never called bootstrap_board_graph still get
    a working graph."""
    path = board_kuzu_path(fresh_board)
    assert not path.exists()

    with open_board_connection(fresh_board) as (_db, conn):
        # If the ensure hook is missing, Kùzu would raise at the first
        # query because BoardMeta wouldn't exist. Reaching this assert
        # proves the auto-bootstrap happened.
        res = conn.execute(
            "CALL SHOW_TABLES() WHERE name = 'BoardMeta' RETURN name"
        )
        assert res.has_next()
        res.close()


# ---------------------------------------------------------------------------
# 4: concurrent open — lock serialisation
# ---------------------------------------------------------------------------


def test_concurrent_opens_serialize_bootstrap(fresh_board):
    """Two threads opening a cold board simultaneously must both succeed.
    The per-board lock in ensure_board_graph_bootstrapped serialises them
    so we never see a partially-bootstrapped graph or a Kùzu file-lock
    contention crash."""
    path = board_kuzu_path(fresh_board)
    assert not path.exists()

    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def _worker() -> None:
        try:
            barrier.wait(timeout=5)
            with open_board_connection(fresh_board) as (_db, conn):
                res = conn.execute(
                    "CALL SHOW_TABLES() WHERE name = 'BoardMeta' RETURN name"
                )
                assert res.has_next()
                res.close()
        except BaseException as exc:  # pragma: no cover — recorded for assert
            errors.append(exc)

    t1 = threading.Thread(target=_worker)
    t2 = threading.Thread(target=_worker)
    t1.start()
    t2.start()
    t1.join(timeout=15)
    t2.join(timeout=15)

    assert not t1.is_alive() and not t2.is_alive(), "threads must finish"
    assert not errors, f"concurrent open failed: {errors!r}"
    assert path.exists()


# ---------------------------------------------------------------------------
# 5 + 6: End-to-end Bug #2 regression
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_commits_on_cold_board(fresh_board, db_factory):
    """The worker uses the primitives commit_consolidation pipeline,
    which ultimately calls open_board_connection. Before Bug #2 was
    fixed, a cold board would crash here with `Table Entity does not
    exist`. After the fix, the first commit self-heals the graph."""
    from okto_pulse.core.kg.primitives import (
        add_node_candidate,
        begin_consolidation,
        commit_consolidation,
    )
    from okto_pulse.core.kg.schemas import (
        AddNodeCandidateRequest,
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        KGNodeType,
        NodeCandidate,
    )

    # Confirm we truly start cold.
    path = board_kuzu_path(fresh_board)
    assert not path.exists()

    agent_id = "test-agent"
    artifact_id = f"spec-{uuid.uuid4().hex[:8]}"

    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=fresh_board,
                artifact_type="spec",
                artifact_id=artifact_id,
                raw_content="bug2 regression content",
            ),
            agent_id=agent_id,
            db=db,
        )

    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="cand-1",
                node_type=KGNodeType.DECISION,
                title="Use per-board autoheal for cold graphs",
                source_confidence=0.9,
            ),
        ),
        agent_id=agent_id,
    )

    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id,
            db=db,
        )

    assert commit.status == "committed"
    assert commit.nodes_added == 1
    assert path.exists(), "commit must have triggered the autoheal bootstrap"


@pytest.mark.asyncio
async def test_event_triggered_consolidation_on_cold_board(fresh_board, db_factory):
    """Simulate the API/worker path that Bug #2 uncovered: a board created
    through the service layer never had bootstrap_board_graph called, and
    the first event-driven consolidation blows up. The autoheal in
    BoardConnection must make that scenario succeed."""
    from okto_pulse.core.kg.primitives import (
        add_node_candidate,
        begin_consolidation,
        commit_consolidation,
    )
    from okto_pulse.core.kg.schemas import (
        AddNodeCandidateRequest,
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        KGNodeType,
        NodeCandidate,
    )

    # Double-check we did not accidentally seed the graph.
    path = board_kuzu_path(fresh_board)
    assert not path.exists()

    agent_id = "test-agent-event"
    artifact_id = f"spec-{uuid.uuid4().hex[:8]}"

    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=fresh_board,
                artifact_type="spec",
                artifact_id=artifact_id,
                raw_content="event-triggered cold-board content",
            ),
            agent_id=agent_id,
            db=db,
        )

    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="cand-event-1",
                node_type=KGNodeType.DECISION,
                title="Event-triggered autoheal decision",
                source_confidence=0.85,
            ),
        ),
        agent_id=agent_id,
    )

    async with db_factory() as db:
        commit = await commit_consolidation(
            CommitConsolidationRequest(session_id=begin.session_id),
            agent_id=agent_id,
            db=db,
        )

    assert commit.status == "committed"
    assert commit.nodes_added == 1
    assert path.exists()
