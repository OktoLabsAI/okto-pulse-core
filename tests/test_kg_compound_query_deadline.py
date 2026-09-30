"""Board policy and resource ownership span complete compound MCP queries."""
import asyncio
from contextlib import asynccontextmanager, contextmanager
import json
from threading import Event
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from okto_pulse.core import runtime_registry
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.kg.interfaces.graph_errors import GraphQueryTimeout
from okto_pulse.core.mcp import kg_power_tools as api
from okto_pulse.core.mcp.catalog import CoreMcpCatalog


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['natural', 'reflective'])
@pytest.mark.parametrize('requested,expected', [(None, 800), (30000, 800), (100, 100)])
async def test_compound_query_reads_current_board_policy(monkeypatch, kind, requested, expected):
    board = SimpleNamespace(id='b', owner_id='owner', realm_id=LOCAL_REALM_ID,
                            settings={'kg_query_timeout_ms': 800})
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)))

    @asynccontextmanager
    async def factory(*, actor):
        assert actor.board_id == 'b' and actor.actor_id == 'a'
        yield uow

    monkeypatch.setattr(runtime_registry, 'resolve_unit_of_work_factory', lambda: factory)
    registry = get_kg_registry()
    scopes = []

    @contextmanager
    def scope(board_id, *, timeout_ms):
        scopes.append(('enter', board_id, timeout_ms))
        try:
            yield
        finally:
            scopes.append(('exit', board_id, timeout_ms))

    monkeypatch.setattr(registry, 'graph_query_execution', SimpleNamespace(scope=scope))
    monkeypatch.setattr(registry, 'auth_context_factory', lambda: SimpleNamespace(
        get_agent_id=AsyncMock(return_value='a'), get_accessible_boards=AsyncMock(return_value=['b'])))
    monkeypatch.setattr(registry, 'reflective_retrieval', object())
    monkeypatch.setattr(registry, 'reflective_critic', object())

    def query(*args, **kwargs):
        assert scopes == [('enter', 'b', expected)]
        if kind == 'reflective':
            assert kwargs['deadline_ms'] == expected
        return dict(nodes=[], total_matches=0, accepted=True)

    monkeypatch.setattr(api, 'execute_natural_query', query)
    from okto_pulse.core.kg import retrieve_critic
    monkeypatch.setattr(retrieve_critic, 'run_reflective_query', query)
    catalog = CoreMcpCatalog(name='compound-budget', version='test')
    api.register_kg_power_tools(catalog,
        get_agent=AsyncMock(return_value=SimpleNamespace(id='a')),
        get_board_agent=AsyncMock(return_value=SimpleNamespace(agent_id='a', permissions=None,
                                                               realm_id=LOCAL_REALM_ID)))
    tool = await catalog.get_tool('okto_pulse_kg_query_' + kind)
    kwargs = {'timeout_ms' if kind == 'natural' else 'deadline_ms': requested}
    result = json.loads(await tool.fn(board_id='b', nl_query='decision', **kwargs))
    assert 'error' not in result, result
    assert scopes == [('enter', 'b', expected), ('exit', 'b', expected)]
    uow.boards.get.assert_awaited_once_with('b')


@pytest.mark.asyncio
async def test_compound_worker_is_drained_on_cancellation(monkeypatch):
    entered, release, closed = Event(), Event(), Event()

    @contextmanager
    def scope(*args, **kwargs):
        try:
            yield
        finally:
            closed.set()

    monkeypatch.setattr(get_kg_registry(), 'graph_query_execution', SimpleNamespace(scope=scope))

    def operation():
        entered.set()
        assert release.wait(3)

    task = asyncio.create_task(api._run_query_with_deadline('b', 1000, operation))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not closed.is_set()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()


def test_natural_confidence_never_converts_native_timeout_into_fallback(monkeypatch):
    from okto_pulse.core.kg.tier_power import _filter_natural_source_confidence

    def expired(*args, **kwargs):
        raise GraphQueryTimeout('expired')

    monkeypatch.setattr(get_kg_registry(), 'cypher_executor', SimpleNamespace(execute_read_only=expired))
    with pytest.raises(GraphQueryTimeout):
        _filter_natural_source_confidence('b', [dict(node_type='Decision', node_id='n')], 0.5)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['cypher', 'natural', 'reflective'])
async def test_mcp_exposes_query_resource_limit(monkeypatch, kind):
    from okto_pulse.core.kg.interfaces.graph_errors import GraphQueryResourceLimit
    from okto_pulse.core.ports.kg_query_policy import KGQueryPolicy

    monkeypatch.setattr(api, '_read_query_policy', AsyncMock(return_value=KGQueryPolicy()))
    registry = get_kg_registry()
    monkeypatch.setattr(registry, 'auth_context_factory', lambda: SimpleNamespace(
        get_agent_id=AsyncMock(return_value='a'), get_accessible_boards=AsyncMock(return_value=['b'])))
    monkeypatch.setattr(registry, 'reflective_retrieval', object())
    monkeypatch.setattr(registry, 'reflective_critic', object())

    def limited(*args, **kwargs):
        raise GraphQueryResourceLimit('Query exceeds value limit.',
                                      details=dict(resource='result_value', limit=1024, observed=1025))

    monkeypatch.setattr(api, 'execute_cypher_read_only', limited)
    monkeypatch.setattr(api, 'execute_natural_query', limited)
    from okto_pulse.core.kg import retrieve_critic
    monkeypatch.setattr(retrieve_critic, 'run_reflective_query', limited)
    catalog = CoreMcpCatalog(name='query-limit', version='test')
    api.register_kg_power_tools(catalog,
        get_agent=AsyncMock(return_value=SimpleNamespace(id='a')),
        get_board_agent=AsyncMock(return_value=SimpleNamespace(agent_id='a', permissions=None)))
    tool = await catalog.get_tool('okto_pulse_kg_query_' + kind)
    args = dict(cypher='RETURN 1') if kind == 'cypher' else dict(nl_query='decision')
    result = json.loads(await tool.fn(board_id='b', **args))
    assert result['error']['code'] == 'graph_query_resource_limit'
    assert result['error']['details'] == dict(resource='result_value', limit=1024, observed=1025)
