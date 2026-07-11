"""Tests for the v0.1.0 → v0.2.0 edge metadata migration.

Validates:
- bootstrap adds {layer, rule_id, created_by, fallback_reason} columns
- migrate_edge_metadata is idempotent on already-migrated boards
- backfill tags NULL layer as 'legacy' on pre-existing edges
- create_edge defaults layer=cognitive when caller omits it
"""

from __future__ import annotations

import gc
import os

import pytest

import kg_schema_testing as kg_schema
from okto_pulse.community.adapters.graph_connection_pool import reset_connection_pool_for_tests
from kg_schema_testing import (
    EDGE_LAYERS,
    EDGE_METADATA_COLUMNS,
    REL_TYPES,
    SCHEMA_VERSION,
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
)

kg_runtime = pytest.importorskip("okto_pulse.community.adapters.kg_runtime")
_ensure_multi_rel_pairs = kg_runtime._ensure_multi_rel_pairs
_show_rel_connection_pairs = kg_runtime._show_rel_connection_pairs
migrate_edge_metadata = kg_runtime.migrate_edge_metadata


@pytest.fixture
def board():
    bid = f"board-migration-{os.urandom(4).hex()}"
    bootstrap_board_graph(bid)
    yield bid
    close_all_connections()
    reset_connection_pool_for_tests()


def test_schema_version_is_current():
    # Monotonic additive bumps (0.3.6 = KG working/canonical partitioning,
    # commit dcf6130) preserve the floor — assert known-version membership
    # rather than a single fixed string (pattern from test_events.py).
    assert SCHEMA_VERSION in {"0.3.2", "0.3.3", "0.3.4", "0.3.5", "0.3.6", "0.3.7"}


def test_edge_metadata_columns_declared():
    col_names = [c[0] for c in EDGE_METADATA_COLUMNS]
    assert col_names == ["layer", "rule_id", "created_by", "fallback_reason"]


def test_edge_layers_catalog_is_closed():
    assert set(EDGE_LAYERS) == {"deterministic", "cognitive", "fallback", "legacy"}


def test_multi_rel_pair_migration_adds_architecture_to_bug_pair(tmp_path):
    import ladybug as kuzu  # type: ignore

    db = kuzu.Database(str(tmp_path / "graph.lbug"))
    conn = kuzu.Connection(db)
    try:
        conn.execute("CREATE NODE TABLE Entity(id STRING PRIMARY KEY)")
        conn.execute("CREATE NODE TABLE Bug(id STRING PRIMARY KEY)")
        conn.execute(
            "CREATE REL TABLE belongs_to(FROM Bug TO Entity, confidence DOUBLE)"
        )

        added = _ensure_multi_rel_pairs(
            conn,
            "belongs_to",
            (("Bug", "Entity"), ("Entity", "Bug")),
        )

        assert added == [("Entity", "Bug")]
        assert ("Entity", "Bug") in _show_rel_connection_pairs(conn, "belongs_to")
    finally:
        conn.close()
        del conn, db


def test_board_migration_probe_detects_missing_multi_rel_pair(tmp_path, monkeypatch):
    import ladybug as kuzu  # type: ignore

    board = "legacy-missing-rel-pair"
    graph_path = tmp_path / "graph.lbug"
    db = kuzu.Database(str(graph_path))
    conn = kuzu.Connection(db)
    try:
        for node_type in kg_schema.NODE_TYPES:
            conn.execute(kg_schema._build_node_ddl(node_type))
        for rel_name, from_type, to_type in kg_schema.REL_TYPES:
            conn.execute(kg_schema._build_rel_ddl(rel_name, from_type, to_type))
        for rel_name, pairs in kg_schema.MULTI_REL_TYPES:
            legacy_pairs = tuple(
                pair
                for pair in pairs
                if not (rel_name == "belongs_to" and pair == ("Entity", "Bug"))
            )
            conn.execute(kg_schema._build_multi_rel_ddl(rel_name, legacy_pairs))
    finally:
        conn.close()
        del conn, db

    monkeypatch.setattr(kg_schema, "board_kuzu_path", lambda _board_id: graph_path)
    kg_runtime._MIGRATED_BOARDS.discard(board)
    try:
        assert kg_runtime._board_needs_migration(board) is True
    finally:
        kg_schema.close_board_db_cache(board)
        kg_runtime._MIGRATED_BOARDS.discard(board)


def test_bootstrap_creates_rels_with_metadata_columns(board):
    """Every REL_TYPES table must accept writes to the new columns."""
    with open_board_connection(board) as (_db, conn):
        # Seed two Decisions so a supersedes edge has anchors.
        conn.execute(
            "CREATE (n:Decision {id: 'd1', title: 't1', content: 'c1'})"
        )
        conn.execute(
            "CREATE (n:Decision {id: 'd2', title: 't2', content: 'c2'})"
        )
        conn.execute(
            "MATCH (a:Decision {id: 'd1'}), (b:Decision {id: 'd2'}) "
            "CREATE (a)-[:supersedes {confidence: 1.0, "
            "created_by_session_id: 'sess_test', "
            "created_at: timestamp('2026-04-17T12:00:00'), "
            "layer: 'deterministic', rule_id: 'test_rule', "
            "created_by: 'worker_unit', fallback_reason: ''}]->(b)"
        )
        result = conn.execute(
            "MATCH ()-[r:supersedes]->() "
            "RETURN r.layer, r.rule_id, r.created_by, r.fallback_reason"
        )
        assert result.has_next()
        row = result.get_next()
        assert row[0] == "deterministic"
        assert row[1] == "test_rule"
        assert row[2] == "worker_unit"
        assert row[3] == ""


def test_migrate_is_idempotent(board):
    """Running migrate_edge_metadata twice must not raise or duplicate columns."""
    first = migrate_edge_metadata(board)
    second = migrate_edge_metadata(board)
    # Bootstrap already applied the migration — both runs should be no-ops.
    assert isinstance(first, dict)
    assert isinstance(second, dict)
    # Every REL_TYPES rel must be present in the summary (even with empty list).
    for rel_name, _from, _to in REL_TYPES:
        assert rel_name in first
        assert rel_name in second


def test_migrate_on_nonexistent_board_is_noop():
    """migrate_edge_metadata must not create a DB — only mutate existing ones."""
    summary = migrate_edge_metadata("nonexistent-board-xxx")
    assert summary == {}


def test_legacy_backfill_tags_untagged_edges(board):
    """Edges inserted with NULL layer get backfilled to layer='legacy'.

    Simulates a pre-v0.2.0 edge (no layer field) by inserting the legacy
    shape, then running migrate_edge_metadata and asserting the update.
    """
    def seed_legacy_edge():
        with open_board_connection(board) as (_db, conn):
            conn.execute(
                "CREATE (n:Decision {id: 'd3', title: 't3', content: 'c3'})"
            )
            conn.execute(
                "CREATE (n:Decision {id: 'd4', title: 't4', content: 'c4'})"
            )
            # Insert WITHOUT any of the v0.2.0 columns — they default to NULL.
            conn.execute(
                "MATCH (a:Decision {id: 'd3'}), (b:Decision {id: 'd4'}) "
                "CREATE (a)-[:supersedes {confidence: 0.8, "
                "created_by_session_id: 'sess_legacy', "
                "created_at: timestamp('2026-01-01T00:00:00')}]->(b)"
            )

    seed_legacy_edge()
    # Windows holds the Kùzu file lock until every reference to the Database/
    # Connection objects is gone — delegating to a helper scope + gc.collect
    # drops the locals that `with ... as (_db, conn)` leaked into this frame.
    gc.collect()

    migrate_edge_metadata(board)

    def assert_backfilled():
        with open_board_connection(board) as (_db, conn):
            result = conn.execute(
                "MATCH (a:Decision {id: 'd3'})-[r:supersedes]->(b:Decision {id: 'd4'}) "
                "RETURN r.layer, r.rule_id, r.created_by"
            )
            assert result.has_next()
            row = result.get_next()
            assert row[0] == "legacy"
            assert row[1] == "legacy_pre_v2"
            assert row[2] == "worker_legacy"

    assert_backfilled()
    gc.collect()
