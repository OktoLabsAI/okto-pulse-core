"""KG6.5: per-cell and row limits do not bound the serialized response."""
from types import SimpleNamespace

import pytest
from okto_pulse.core.kg.interfaces import registry
from okto_pulse.core.kg.interfaces.graph_errors import GraphQueryResourceLimit
from okto_pulse.core.kg.tier_power import execute_cypher_read_only


def test_many_individually_bounded_cells_cannot_exceed_response_budget(monkeypatch):
    # Every scalar is below the existing native character limit, and 150 rows
    # are below the default 200. Their combined serialized payload is still large.
    cell = 'á' * 60000
    executor = SimpleNamespace(execute_read_only=lambda *args, **kwargs: dict(
        columns=['text'], rows=[[cell] for _ in range(150)], row_count=150, truncated=False))
    monkeypatch.setattr(registry, 'get_kg_registry', lambda: SimpleNamespace(cypher_executor=executor))
    with pytest.raises(GraphQueryResourceLimit) as failure:
        execute_cypher_read_only('b', 'MATCH (n:Decision) RETURN n.content', include_working=True)
    assert failure.value.details['resource'] == 'serialized_payload_bytes'
    assert failure.value.details['limit'] == 4 * 1024 * 1024


def test_small_aggregate_and_server_counts_are_not_rewritten():
    from okto_pulse.core.kg.query_response_budget import enforce_query_response_budget

    payload = dict(rows=[[1000000, ['a', 'b']]], columns=['total', 'page'],
                   row_count=1, truncated=True, total_count=1000000)
    assert enforce_query_response_budget(payload) is payload
    assert payload['rows'][0][0] == payload['total_count'] == 1000000


def test_payload_limit_counts_metadata_and_json_escaping():
    from okto_pulse.core.kg.query_response_budget import enforce_query_response_budget

    # A compact Unicode byte count underestimates the escaped MCP data form.
    with pytest.raises(GraphQueryResourceLimit) as failure:
        enforce_query_response_budget(dict(rows=[], trace=['á' * 60000] * 12))
    assert failure.value.details['resource'] == 'serialized_payload_bytes'


@pytest.mark.asyncio
async def test_compound_query_envelope_uses_the_same_budget(monkeypatch):
    from contextlib import contextmanager
    from okto_pulse.core.mcp.kg_power_tools import _run_query_with_deadline

    exited = []
    @contextmanager
    def scope(*args, **kwargs):
        try:
            yield
        finally:
            exited.append(True)

    monkeypatch.setattr(registry.get_kg_registry(), 'graph_query_execution', SimpleNamespace(scope=scope))
    with pytest.raises(GraphQueryResourceLimit):
        await _run_query_with_deadline('b', 1000, lambda: dict(rows=[['x' * 60000]] * 80))
    assert exited == [True]
