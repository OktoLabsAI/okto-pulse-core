"""MCP calls resolve scoped persisted policy before invoking the graph."""

from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.mcp import kg_power_tools as api
from okto_pulse.core.mcp.catalog import CoreMcpCatalog
from okto_pulse.core import runtime_registry


@pytest.mark.asyncio
@pytest.mark.parametrize('requested,expected', [(None, 800), (30000, 800), (12, 12)])
async def test_mcp_uses_authorized_board_policy(monkeypatch, requested, expected):
    board = SimpleNamespace(id='b', owner_id='owner', realm_id=LOCAL_REALM_ID,
                            settings={'kg_query_timeout_ms': 800})
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)))

    @asynccontextmanager
    async def factory(*, actor):
        assert actor.board_id == 'b' and actor.actor_id == 'a'
        yield uow

    monkeypatch.setattr(runtime_registry, 'resolve_unit_of_work_factory', lambda: factory)
    calls = []

    def query(*args, **kwargs):
        calls.append(kwargs)
        return dict(rows=[], columns=[], row_count=0, truncated=False)

    monkeypatch.setattr(api, 'execute_cypher_read_only', query)
    catalog = CoreMcpCatalog(name='query-policy', version='test')
    api.register_kg_power_tools(catalog,
        get_agent=AsyncMock(return_value=SimpleNamespace(id='a')),
        get_board_agent=AsyncMock(return_value=SimpleNamespace(agent_id='a', permissions=None,
                                                               realm_id=LOCAL_REALM_ID)))
    tool = await catalog.get_tool('okto_pulse_kg_query_cypher')
    result = json.loads(await tool.fn(board_id='b', cypher='MATCH (n) RETURN n.id', timeout_ms=requested))
    assert 'error' not in result, result
    assert calls[0]['timeout_ms'] == expected
    assert calls[0]['max_rows'] == 200
    uow.boards.get.assert_awaited_once_with('b')


@pytest.mark.asyncio
@pytest.mark.parametrize("unavailable", [False, True])
async def test_mcp_preserves_source_window_contract(monkeypatch, unavailable):
    board = SimpleNamespace(id="b", owner_id="owner", realm_id=LOCAL_REALM_ID,
                            settings={"kg_query_timeout_ms": 800})
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)))

    @asynccontextmanager
    async def factory(*, actor):
        yield uow

    monkeypatch.setattr(runtime_registry, "resolve_unit_of_work_factory", lambda: factory)
    temporal = dict(field="source_updated_at", interpretation="latest_source_update_not_history",
                    history_complete=False, as_of_supported=False, complete_for_window=False)
    calls = []

    def query(*args, **kwargs):
        calls.append(kwargs)
        if unavailable:
            raise api.TierPowerError("query_temporal_unavailable",
                                     "Source update times could not be resolved.")
        return dict(nodes=[], total_matches=0, temporal_filter=temporal)

    monkeypatch.setattr(api, "execute_natural_query", query)
    catalog = CoreMcpCatalog(name="temporal-policy", version="test")
    api.register_kg_power_tools(catalog,
        get_agent=AsyncMock(return_value=SimpleNamespace(id="a")),
        get_board_agent=AsyncMock(return_value=SimpleNamespace(agent_id="a", permissions=None,
                                                               realm_id=LOCAL_REALM_ID)))
    tool = await catalog.get_tool("okto_pulse_kg_query_natural")
    result = json.loads(await tool.fn(board_id="b", nl_query="needle",
                                     since="2026-10-04", until="2026-10-06"))
    assert calls[0]["since"] == "2026-10-04"
    assert calls[0]["until"] == "2026-10-06"
    if unavailable:
        assert result["error"]["code"] == "query_temporal_unavailable"
        assert "nodes" not in result
    else:
        assert result["temporal_filter"] == temporal
