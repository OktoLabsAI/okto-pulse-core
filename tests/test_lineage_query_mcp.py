from contextlib import asynccontextmanager
from dataclasses import replace
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from pydantic import ValidationError
from okto_pulse.core.mcp import server
from okto_pulse.core.models.lineage_query import LineageRequest, LineageResponse
from okto_pulse.core.services.lineage_query import project_lineage
from test_lineage_query import chain
from test_lineage_query_authorization import FLAGS


@pytest.fixture
def facade(monkeypatch):
    context = SimpleNamespace(agent_id='agent', agent_name='Agent', realm_id='local', board_id='board',
        permissions=['board:read', *FLAGS])
    board = SimpleNamespace(id='board', realm_id='local', owner_id='owner', settings={})
    async def aggregate(query, **options):
        return project_lineage(query,replace(chain(),actor_scope_ref=query.actor_scope_ref))
    operation = AsyncMock(side_effect=aggregate)
    legacy = AsyncMock()
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=SimpleNamespace(analytics=SimpleNamespace(lineage=operation),build_traceability_report=legacy))
    @asynccontextmanager
    async def factory(**options):
        yield uow
    monkeypatch.setattr(server,'_get_agent_ctx',AsyncMock(return_value=context))
    monkeypatch.setattr(server,'get_unit_of_work_factory_for_mcp',lambda:factory)
    return SimpleNamespace(context=context,operation=operation,legacy=legacy)


async def call(**kwargs):
    tool = await server.mcp.get_tool('okto_pulse_get_traceability_report')
    return json.loads(await tool.fn(board_id='board',**kwargs))


@pytest.mark.asyncio
async def test_lineage_variant_exposes_partial_frontier_and_real_direction(facade):
    result = await call(query=LineageRequest(view='lineage',subject_ref='spec:0'))
    LineageResponse.model_validate(result)
    assert result['frontier_refs'] == ['spec:4']
    assert not result['completeness']['complete_for_scope']
    assert result['items'][-1]['path'][-1]['relation'] == 'precedes'
    assert result['projection_freshness']['state'] == 'unknown'
    facade.legacy.assert_not_called()


@pytest.mark.asyncio
async def test_cursor_does_not_bypass_revoked_source_permission(facade):
    query = LineageRequest(view='lineage',subject_ref='spec:0',limit=1)
    first = await call(query=query)
    facade.context.permissions.remove('amendment.revision.read')
    facade.operation.reset_mock()
    result = await call(query=query.model_copy(update={'cursor':first['next_cursor']}))
    assert result['code'] == 'permission_denied'
    facade.operation.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('extra',[{'spec_id':'spec'},{'ideation_id':'idea'},{'include_artifacts':True}])
async def test_lineage_cannot_mix_with_legacy_scope(facade,extra):
    result = await call(query=LineageRequest(view='lineage',subject_ref='spec:0'),**extra)
    assert result['code'] == 'traceability_query_scope_ambiguous'
    facade.operation.assert_not_called()


@pytest.mark.parametrize('fields',[{'subject_ref':'spec:s:fr:x'}, {'subject_ref':'amendment:a'},
    {'read_delivery':True},{'max_depth':33},{'limit':1001}])
def test_request_is_closed_and_uses_canonical_domain_refs(fields):
    with pytest.raises(ValidationError):
        LineageRequest.model_validate({'view':'lineage','subject_ref':'spec:0',**fields})


@pytest.mark.asyncio
async def test_live_facade_uses_closed_lineage_discriminator(facade):
    tool = await server.mcp.get_tool('okto_pulse_get_traceability_report')
    schema = json.dumps(tool.parameters)
    assert 'LineageRequest' in schema and 'discriminator' in schema
    assert LineageRequest.model_json_schema()['additionalProperties'] is False
