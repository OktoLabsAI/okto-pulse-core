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

import shutil
import threading
import uuid

import pytest

from kg_schema_testing import (
    bootstrap_board_graph,
    board_kuzu_path,
    close_all_connections,
    ensure_board_graph_bootstrapped,
    open_board_connection,
    reset_bootstrap_cache_for_tests,
)


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    from kg_registry_testing import (
        RealBoardCypherExecutorForTests,
        RealBoardGraphTransactionForTests,
        configure_test_kg_registry,
    )

    configure_test_kg_registry(
        cypher_executor=RealBoardCypherExecutorForTests(),
        graph_transaction=RealBoardGraphTransactionForTests(),
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
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    elif path.is_file():
        path.unlink(missing_ok=True)
        for sibling in path.parent.glob(path.name + ".*"):
            if sibling.is_dir():
                shutil.rmtree(sibling, ignore_errors=True)
            else:
                sibling.unlink(missing_ok=True)
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


def test_corrupt_ladybug_wal_is_preserved_and_blocks_rebootstrap(fresh_board, monkeypatch):
    """A crash during LadybugDB commit can leave graph.lbug.wal corrupt.

    The next bootstrap probe must not quarantine or replace the existing
    graph automatically. A probe is not an operator-approved recovery action;
    preserving graph.lbug + graph.lbug.wal is safer than silently creating an
    empty graph.
    """
    from okto_pulse.community.adapters import kg_runtime

    path = board_kuzu_path(fresh_board)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"partial-lbug")
    wal_path = path.parent / f"{path.name}.wal"
    wal_path.write_bytes(b"corrupt-wal")

    def _raise_corrupt(_path):
        raise RuntimeError(
            "Runtime exception: Corrupted wal file. "
            "Read out invalid WAL record type."
        )

    monkeypatch.setattr(kg_runtime, "_open_kuzu_db_path_cached", _raise_corrupt)
    reset_bootstrap_cache_for_tests()

    with pytest.raises(RuntimeError, match="refusing to auto-bootstrap"):
        kg_runtime._graph_needs_bootstrap(fresh_board)
    assert path.exists()
    assert wal_path.exists()


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
        add_edge_candidate,
        add_node_candidate,
        begin_consolidation,
        commit_consolidation,
    )
    from okto_pulse.core.kg.schemas import (
        AddEdgeCandidateRequest,
        AddNodeCandidateRequest,
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        EdgeCandidate,
        KGEdgeType,
        KGNodeType,
        NodeCandidate,
    )

    # Confirm we truly start cold.
    path = board_kuzu_path(fresh_board)
    assert not path.exists()

    agent_id = "system:layer1_worker"
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
                candidate_id="board-root",
                node_type=KGNodeType.ENTITY,
                title="Cold board root",
                source_artifact_ref=f"board:{fresh_board}",
                source_confidence=1.0,
            ),
        ),
        agent_id=agent_id,
    )
    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="learning-1",
                node_type=KGNodeType.LEARNING,
                title="Use per-board autoheal for cold graphs",
                source_artifact_ref=f"spec:{artifact_id}:learning:0",
                source_confidence=0.9,
            ),
        ),
        agent_id=agent_id,
    )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id="learning-1-belongs-to-board",
                edge_type=KGEdgeType.BELONGS_TO,
                from_candidate_id="learning-1",
                to_candidate_id="board-root",
                confidence=1.0,
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
    assert commit.nodes_added == 2
    assert commit.edges_added == 1
    assert commit.connectivity["passed"] is True
    assert path.exists(), "commit must have triggered the autoheal bootstrap"


@pytest.mark.asyncio
async def test_event_triggered_consolidation_on_cold_board(fresh_board, db_factory):
    """Simulate the API/worker path that Bug #2 uncovered: a board created
    through the service layer never had bootstrap_board_graph called, and
    the first event-driven consolidation blows up. The autoheal in
    BoardConnection must make that scenario succeed."""
    from okto_pulse.core.kg.primitives import (
        add_edge_candidate,
        add_node_candidate,
        begin_consolidation,
        commit_consolidation,
    )
    from okto_pulse.core.kg.schemas import (
        AddEdgeCandidateRequest,
        AddNodeCandidateRequest,
        BeginConsolidationRequest,
        CommitConsolidationRequest,
        EdgeCandidate,
        KGEdgeType,
        KGNodeType,
        NodeCandidate,
    )

    # Double-check we did not accidentally seed the graph.
    path = board_kuzu_path(fresh_board)
    assert not path.exists()

    agent_id = "system:layer1_worker"
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
                candidate_id="board-root",
                node_type=KGNodeType.ENTITY,
                title="Cold event board root",
                source_artifact_ref=f"board:{fresh_board}",
                source_confidence=1.0,
            ),
        ),
        agent_id=agent_id,
    )
    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="learning-event-1",
                node_type=KGNodeType.LEARNING,
                title="Event-triggered autoheal learning",
                source_artifact_ref=f"spec:{artifact_id}:learning:0",
                source_confidence=0.85,
            ),
        ),
        agent_id=agent_id,
    )
    await add_edge_candidate(
        AddEdgeCandidateRequest(
            session_id=begin.session_id,
            candidate=EdgeCandidate(
                candidate_id="learning-event-1-belongs-to-board",
                edge_type=KGEdgeType.BELONGS_TO,
                from_candidate_id="learning-event-1",
                to_candidate_id="board-root",
                confidence=1.0,
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
    assert commit.nodes_added == 2
    assert commit.edges_added == 1
    assert commit.connectivity["passed"] is True
    assert path.exists()
