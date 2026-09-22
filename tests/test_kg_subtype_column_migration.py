"""MKG-E C1 — kind_of column migration (S1) + physical-enforcement
non-regression assertions (S8, static part)."""

from __future__ import annotations

import gc
import uuid

import pytest

from okto_pulse.core.kg.schema_contract import (
    LEGACY_NODE_COLUMNS,
    NODE_TYPES,
    SCHEMA_VERSION,
    STABLE_NODE_PROPERTIES,
    SUBTYPE_COLUMNS,
    VECTOR_INDEX_TYPES,
)

from kg_schema_testing import (
    board_graph_columns,
    bootstrap_board_graph,
    close_all_connections,
)


@pytest.fixture
def kg_tempdir(monkeypatch):
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    yield
    try:
        close_all_connections()
    except Exception:
        pass
    gc.collect()


def test_s1_fresh_bootstrap_has_kind_of_and_version(kg_tempdir):
    assert SCHEMA_VERSION == "0.6.0"
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    for node_type in NODE_TYPES:
        assert "kind_of" in board_graph_columns(board_id, node_type), node_type


def test_s8_physical_enforcement_untouched(kg_tempdir):
    """Static half of S8: the closed physical taxonomy is unchanged and
    kind_of never enters the vector/digest surface."""
    from okto_pulse.core.application.processors.global_outbox import (
        DIGESTED_NODE_TYPES,
    )

    assert len(NODE_TYPES) == 11
    assert tuple(DIGESTED_NODE_TYPES) == tuple(VECTOR_INDEX_TYPES)
    assert "kind_of" not in VECTOR_INDEX_TYPES
    assert "kind_of" in STABLE_NODE_PROPERTIES
    assert LEGACY_NODE_COLUMNS == ("validation_status", "corroboration_count")
    for name, _ in SUBTYPE_COLUMNS:
        assert name not in LEGACY_NODE_COLUMNS

    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    for node_type in NODE_TYPES:
        cols = board_graph_columns(board_id, node_type)
        assert not cols & set(LEGACY_NODE_COLUMNS), node_type
