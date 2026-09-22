"""v0.3.11 reversible-cancellation schema coverage."""

from __future__ import annotations

import gc
import uuid

import pytest

from kg_schema_testing import (
    board_graph_columns,
    bootstrap_board_graph,
    close_all_connections,
)
from okto_pulse.core.kg.schema_contract import (
    CANCELLATION_COLUMNS,
    NODE_TYPES,
    SCHEMA_VERSION,
    STABLE_NODE_PROPERTIES,
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


def test_fresh_bootstrap_has_reversible_cancellation_snapshot(kg_tempdir):
    assert SCHEMA_VERSION == "0.6.0"
    assert CANCELLATION_COLUMNS == (("pre_cancellation_relevance_score", "DOUBLE"),)
    assert "pre_cancellation_relevance_score" in STABLE_NODE_PROPERTIES

    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    for node_type in NODE_TYPES:
        assert "pre_cancellation_relevance_score" in board_graph_columns(
            board_id, node_type
        ), node_type
