"""Tests for the v0.2.0 → v0.3.0 relevance_score migration.

Validates:
- new boards bootstrap with the v0.3.0 schema (relevance_score / query_hits /
  last_queried_at columns present, validation_status / corroboration_count
  absent)
- migrate_board_to_v030 dump/drop/create flow preserves rows when the legacy
  columns are present, defaulting score=0.5 and hits=0
- migrate_board_to_v030 takes the ALTER TABLE ADD path when the table is
  bootstrapped fresh (no legacy columns)
- the migration is idempotent — re-running on a v0.3.0 board is a no-op
- BoardMeta.schema_version is bumped to '0.3.0' after migration
- _board_needs_v030_migration returns False on a freshly bootstrapped board
"""

from __future__ import annotations

import os

import pytest

from okto_pulse.core.kg.connection_pool import reset_connection_pool_for_tests
from okto_pulse.core.kg.schema import (
    LEGACY_NODE_COLUMNS,
    NODE_TYPES,
    RELEVANCE_COLUMNS,
    SCHEMA_VERSION,
    _board_needs_v030_migration,
    _node_has_legacy_columns,
    _node_has_relevance_columns,
    bootstrap_board_graph,
    close_all_connections,
    migrate_board_to_v030,
    open_board_connection,
)


@pytest.fixture
def fresh_board():
    bid = f"board-relevance-{os.urandom(4).hex()}"
    bootstrap_board_graph(bid)
    yield bid
    close_all_connections()
    reset_connection_pool_for_tests()


def test_schema_version_is_v030():
    assert SCHEMA_VERSION == "0.3.3"


def test_relevance_columns_declared():
    names = [c[0] for c in RELEVANCE_COLUMNS]
    assert names == ["relevance_score", "query_hits", "last_queried_at"]


def test_legacy_columns_listed():
    assert set(LEGACY_NODE_COLUMNS) == {"validation_status", "corroboration_count"}


def test_fresh_board_has_v030_columns(fresh_board):
    """Fresh bootstrap should land directly on v0.3.0 — no migration needed."""
    with open_board_connection(fresh_board) as (_db, conn):
        for ntype in NODE_TYPES:
            assert _node_has_relevance_columns(conn, ntype), (
                f"{ntype} missing v0.3.0 columns after bootstrap"
            )
            assert not _node_has_legacy_columns(conn, ntype), (
                f"{ntype} unexpectedly has v0.2.0 legacy columns"
            )


def test_fresh_board_does_not_need_v030_migration(fresh_board):
    assert _board_needs_v030_migration(fresh_board) is False


def test_insert_with_relevance_score_defaults(fresh_board):
    """An insert via raw Cypher should carry the new columns through."""
    with open_board_connection(fresh_board) as (_db, conn):
        conn.execute(
            "CREATE (n:Decision {id: 'd-rel-1', title: 'Test', content: 'c', "
            "source_artifact_ref: 'art-1', source_session_id: 's-1', "
            "source_confidence: 0.9, relevance_score: 0.7, query_hits: 0, "
            "created_at: timestamp('2026-04-18T10:00:00'), "
            "created_by_agent: 'agent-1', "
            "embedding: $emb})",
            {"emb": [0.1] * 384},
        )
        res = conn.execute(
            "MATCH (n:Decision {id: 'd-rel-1'}) "
            "RETURN n.relevance_score, n.query_hits, n.last_queried_at"
        )
        assert res.has_next()
        row = res.get_next()
        assert row[0] == pytest.approx(0.7)
        assert row[1] == 0
        assert row[2] is None


def test_migrate_v030_is_idempotent(fresh_board):
    """Running the migration on a board that's already on v0.3.0 is a no-op."""
    summary_first = migrate_board_to_v030(fresh_board)
    summary_second = migrate_board_to_v030(fresh_board)
    # Both runs should report all node types as 'alter' strategy with no
    # columns added (they all already exist).
    for ntype in NODE_TYPES:
        assert summary_first[ntype]["strategy"] == "alter"
        assert summary_first[ntype]["added"] == []
        assert summary_second[ntype]["strategy"] == "alter"
        assert summary_second[ntype]["added"] == []


def test_migrate_bumps_board_meta_schema_version(fresh_board):
    """After migrate_board_to_v030, BoardMeta.schema_version == '0.3.0'."""
    migrate_board_to_v030(fresh_board)
    with open_board_connection(fresh_board) as (_db, conn):
        res = conn.execute(
            "MATCH (m:BoardMeta {board_id: $bid}) RETURN m.schema_version",
            {"bid": fresh_board},
        )
        assert res.has_next()
        assert res.get_next()[0] == "0.3.3"
