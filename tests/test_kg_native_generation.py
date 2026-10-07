"""Native graph generations are explicit and never inferred from old rows."""

from __future__ import annotations

import gc
import uuid

import pytest

from kg_schema_testing import (
    NODE_TYPES,
    board_graph_columns,
    bootstrap_board_graph,
    close_all_connections,
    open_board_connection,
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


def test_fresh_bootstrap_has_generation_on_all_node_types(kg_tempdir):
    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    for node_type in NODE_TYPES:
        assert "generation" in board_graph_columns(board_id, node_type), node_type


def test_native_generation_is_read_without_converting_incomplete_nodes(kg_tempdir):
    from okto_pulse.core.kg.primitives import KGPrimitiveError, _node_generation

    board_id = str(uuid.uuid4())
    bootstrap_board_graph(board_id)
    with open_board_connection(board_id) as (_kdb, scope):
        scope.execute("CREATE (n:Entity {id: 'native', title: 'native', generation: 4})")
        scope.execute("CREATE (n:Entity {id: 'incomplete', title: 'incomplete'})")
        assert _node_generation(scope, "Entity", "native") == 4
        for node_id in ("incomplete", "missing", ""):
            with pytest.raises(KGPrimitiveError) as failure:
                _node_generation(scope, "Entity", node_id)
            assert failure.value.code == "kg_node_generation_invalid"
        rows = scope.execute(
            "MATCH (n:Entity {id: 'incomplete'}) RETURN n.generation"
        ).rows
        assert len(rows) == 1 and rows[0][0] is None


@pytest.mark.parametrize("rows", [[], [[]], [[None]], [[-1]], [[True]], [["0"]], [[0.5]]])
def test_incomplete_generation_never_becomes_a_successor_identity(rows):
    from types import SimpleNamespace
    from okto_pulse.core.kg.primitives import KGPrimitiveError, _node_generation

    class ReadOnlyScope:
        def execute(self, statement, parameters):
            assert statement.startswith("MATCH ")
            assert parameters == {"id": "node"}
            return SimpleNamespace(rows=rows)

    with pytest.raises(KGPrimitiveError) as failure:
        _node_generation(ReadOnlyScope(), "Entity", "node")
    assert failure.value.code == "kg_node_generation_invalid"


def test_generation_read_failure_is_not_interpreted_as_zero():
    from okto_pulse.core.kg.primitives import KGPrimitiveError, _node_generation

    class UnavailableScope:
        def execute(self, _statement, _parameters):
            raise RuntimeError("graph temporarily unavailable")

    with pytest.raises(KGPrimitiveError) as failure:
        _node_generation(UnavailableScope(), "Entity", "node")
    assert failure.value.code == "kg_node_generation_unavailable"
    assert failure.value.retryable is True
