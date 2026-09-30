"""KG-45: successive authorized queries cannot exhaust a question-count quota."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.mcp import kg_power_tools as api
from okto_pulse.core.mcp.catalog import CoreMcpCatalog
from okto_pulse.core.ports.kg_query_policy import KGQueryPolicy


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['cypher', 'natural', 'reflective'])
async def test_more_than_old_quota_reaches_query_without_counting(monkeypatch, kind):
    registry = get_kg_registry()
    counts = []

    def legacy_quota(agent):
        counts.append(agent)
        return len(counts) <= 30, 60

    monkeypatch.setattr(registry, 'rate_limiter', SimpleNamespace(allow=legacy_quota))
    monkeypatch.setattr(api, '_read_query_policy', AsyncMock(return_value=KGQueryPolicy()))
    calls = []

    def query(*args, **kwargs):
        calls.append((args, kwargs))
        return dict(rows=[], columns=[], row_count=0, truncated=False,
                    nodes=[], total_matches=0, accepted=True)

    monkeypatch.setattr(api, 'execute_cypher_read_only', query)
    monkeypatch.setattr(api, 'execute_natural_query', query)
    from okto_pulse.core.kg import retrieve_critic
    monkeypatch.setattr(retrieve_critic, 'run_reflective_query', query)
    monkeypatch.setattr(registry, 'auth_context_factory', lambda: SimpleNamespace(
        get_agent_id=AsyncMock(return_value='a'), get_accessible_boards=AsyncMock(return_value=['b'])))
    monkeypatch.setattr(registry, 'reflective_retrieval', object())
    monkeypatch.setattr(registry, 'reflective_critic', object())
    auth = AsyncMock(return_value=SimpleNamespace(id='a'))
    scoped = AsyncMock(return_value=SimpleNamespace(agent_id='a', permissions=None))
    catalog = CoreMcpCatalog(name='query-count', version='test')
    api.register_kg_power_tools(catalog, get_agent=auth, get_board_agent=scoped)
    tool = await catalog.get_tool('okto_pulse_kg_query_' + kind)
    arguments = dict(board_id='b')
    arguments.update(dict(cypher='MATCH (n) RETURN n.id') if kind == 'cypher' else dict(nl_query='decision'))
    for index in range(35):
        result = json.loads(await tool.fn(**arguments))
        assert 'error' not in result, (index, result)
    assert len(calls) == auth.await_count == scoped.await_count == 35
    assert counts == [], 'Question count is not a resource-concurrency control'


@pytest.mark.asyncio
async def test_native_cypher_reads_remain_available_after_old_quota(monkeypatch):
    from kg_schema_testing import bootstrap_board_graph

    board = 'queries-after-old-quota'
    bootstrap_board_graph(board)
    monkeypatch.setattr(api, '_read_query_policy', AsyncMock(return_value=KGQueryPolicy()))
    catalog = CoreMcpCatalog(name='native-query-count', version='test')
    api.register_kg_power_tools(catalog,
        get_agent=AsyncMock(return_value=SimpleNamespace(id='native-reader')),
        get_board_agent=AsyncMock(return_value=SimpleNamespace(agent_id='native-reader', permissions=None)))
    tool = await catalog.get_tool('okto_pulse_kg_query_cypher')
    for index in range(35):
        result = json.loads(await tool.fn(board_id=board, cypher='MATCH (n:Decision) RETURN n.id'))
        assert 'error' not in result, (index, result)
        assert result['row_count'] == 0
