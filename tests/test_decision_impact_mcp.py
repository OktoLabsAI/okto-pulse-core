from contextlib import asynccontextmanager
from dataclasses import replace
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from pydantic import ValidationError
from okto_pulse.core.mcp import server
from okto_pulse.core.models.decision_impact import DecisionImpactRequest, DecisionImpactResponse
from test_decision_impact import source, project
from test_decision_impact_authorization import FLAGS


@pytest.fixture
def facade(monkeypatch):
    context = SimpleNamespace(agent_id='agent', agent_name='Agent', realm_id='local', board_id='board',
        permissions=['board:read', *FLAGS])
    board = SimpleNamespace(id='board', realm_id='local', owner_id='owner', settings={})
    async def aggregate(query, **options):
        return project(replace(source(), actor_scope_ref=query.actor_scope_ref), query)
    operation = AsyncMock(side_effect=aggregate)
    legacy = AsyncMock()
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=SimpleNamespace(analytics=SimpleNamespace(decision_impact=operation), build_traceability_report=legacy))
    @asynccontextmanager
    async def factory(**options):
        yield uow
    monkeypatch.setattr(server, '_get_agent_ctx', AsyncMock(return_value=context))
    monkeypatch.setattr(server, 'get_unit_of_work_factory_for_mcp', lambda: factory)
    return SimpleNamespace(context=context, operation=operation, legacy=legacy)


async def call(**kwargs):
    tool = await server.mcp.get_tool('okto_pulse_get_traceability_report')
    return json.loads(await tool.fn(board_id='board', **kwargs))


@pytest.mark.asyncio
async def test_closed_variant_preserves_kg57_and_uses_domain_refs(facade):
    result = await call(query=DecisionImpactRequest(view='impact', subject_ref='spec:spec:decision:decision'))
    DecisionImpactResponse.model_validate(result)
    scenario = next(item for item in result['items'] if item['target_type'] == 'TestScenario')
    assert scenario['certainty'] == 'potential'
    assert len(scenario['path']) == 3
    assert result['authority'] == 'informational'
    facade.legacy.assert_not_called()


@pytest.mark.asyncio
async def test_cursor_does_not_bypass_revoked_graph_permission(facade):
    query = DecisionImpactRequest(view='impact', subject_ref='spec:spec:decision:decision', limit=1)
    first = await call(query=query)
    facade.context.permissions.remove('kg.query.related_context')
    facade.operation.reset_mock()
    result = await call(query=query.model_copy(update={'cursor': first['next_cursor']}))
    assert result['code'] == 'permission_denied'
    facade.operation.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('extra', [{'spec_id': 'spec'}, {'ideation_id': 'idea'}, {'include_artifacts': True}])
async def test_impact_cannot_mix_with_legacy_scope(facade, extra):
    result = await call(query=DecisionImpactRequest(view='impact', subject_ref='spec:spec:decision:decision'), **extra)
    assert result['code'] == 'traceability_query_scope_ambiguous'
    facade.operation.assert_not_called()


@pytest.mark.parametrize('fields', [{'subject_ref': 'spec:s'}, {'subject_ref': 'decision:s:d'},
    {'subject_ref': 'spec:s:decision:d:extra'}, {'read_delivery': True}, {'max_depth': 9}, {'limit': 1001}])
def test_request_has_closed_scope_and_limits(fields):
    with pytest.raises(ValidationError):
        DecisionImpactRequest.model_validate({'view': 'impact', 'subject_ref': 'spec:s:decision:d', **fields})


@pytest.mark.asyncio
async def test_live_facade_uses_discriminator(facade):
    tool = await server.mcp.get_tool('okto_pulse_get_traceability_report')
    schema = json.dumps(tool.parameters)
    assert 'DecisionImpactRequest' in schema and 'discriminator' in schema
    assert DecisionImpactRequest.model_json_schema()['additionalProperties'] is False
