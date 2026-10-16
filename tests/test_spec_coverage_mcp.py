from contextlib import asynccontextmanager
from dataclasses import replace
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from pydantic import ValidationError
from okto_pulse.core.mcp import server
from okto_pulse.core.models.spec_coverage_query import SpecCoverageRequest, SpecCoverageResponse
from okto_pulse.core.services.spec_coverage_query import project_spec_coverage
from test_spec_coverage_query import paged
from test_spec_coverage_authorization import FLAGS


@pytest.fixture
def facade(monkeypatch):
    context = SimpleNamespace(agent_id='agent', agent_name='Agent', realm_id='local', board_id='board',
        permissions=['board:read', *FLAGS, 'code_traceability.evidence.read'])
    board = SimpleNamespace(id='board', realm_id='local', owner_id='owner', settings={})
    async def aggregate(query, **options):
        facts = replace(paged(), actor_scope_ref=query.actor_scope_ref)
        if not query.read_delivery:
            facts = replace(facts, delivery=None, delivery_state='restricted')
        return project_spec_coverage(query, facts)
    operation = AsyncMock(side_effect=aggregate)
    legacy = AsyncMock(return_value={'legacy': True})
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=SimpleNamespace(analytics=SimpleNamespace(spec_coverage=operation), build_traceability_report=legacy), commit=AsyncMock())
    @asynccontextmanager
    async def factory(**options):
        yield uow
    monkeypatch.setattr(server, '_get_agent_ctx', AsyncMock(return_value=context))
    monkeypatch.setattr(server, 'get_unit_of_work_factory_for_mcp', lambda: factory)
    return SimpleNamespace(context=context, operation=operation, legacy=legacy, board=board)


async def call(**kwargs):
    tool = await server.mcp.get_tool('okto_pulse_get_traceability_report')
    return json.loads(await tool.fn(board_id='board', **kwargs))


@pytest.mark.asyncio
async def test_closed_coverage_variant_uses_domain_ref_and_shared_resolver(facade):
    result = await call(query=SpecCoverageRequest(view='coverage', subject_ref='spec:spec', limit=1))
    SpecCoverageResponse.model_validate(result)
    assert result['items'][0]['kind'] == 'delivery'
    assert result['items'][0]['verification'] == 'missing'
    assert result['delivery']['counts']['obligations'] == 3
    assert result['next_cursor']
    facade.legacy.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('flag', ['spec.integration_requirements.read', 'spec.observability_requirements.read'])
async def test_missing_section_grant_denies_before_read(facade, flag):
    facade.context.permissions.remove(flag)
    result = await call(query=SpecCoverageRequest(view='coverage', subject_ref='spec:spec'))
    assert result['code'] == 'permission_denied'
    facade.operation.assert_not_called()


@pytest.mark.asyncio
async def test_optional_proof_grant_does_not_expose_counts(facade):
    facade.context.permissions.remove('code_traceability.evidence.read')
    result = await call(query=SpecCoverageRequest(view='coverage', subject_ref='spec:spec'))
    assert result['delivery']['state'] == 'restricted'
    assert result['delivery']['counts']['obligations'] is None


@pytest.mark.asyncio
async def test_cursor_is_not_reused_after_permission_revocation(facade):
    first = await call(query=SpecCoverageRequest(view='coverage', subject_ref='spec:spec', limit=1))
    facade.context.permissions.remove('code_traceability.evidence.read')
    result = await call(query=SpecCoverageRequest(view='coverage', subject_ref='spec:spec', limit=1, cursor=first['next_cursor']))
    assert result['code'] == 'spec_coverage_cursor_stale'


@pytest.mark.asyncio
@pytest.mark.parametrize('extra', [{'spec_id': 'spec'}, {'ideation_id': 'idea'}, {'include_artifacts': True}])
async def test_coverage_cannot_mix_with_legacy_scope(facade, extra):
    result = await call(query=SpecCoverageRequest(view='coverage', subject_ref='spec:spec'), **extra)
    assert result['code'] == 'traceability_query_scope_ambiguous'
    facade.operation.assert_not_called()


@pytest.mark.parametrize('fields', [{'subject_ref': 'card:card'}, {'subject_ref': 'spec:s:fr:x'},
    {'subject_ref': 'spec:s', 'read_delivery': True}, {'subject_ref': 'spec:s', 'limit': 1001}])
def test_request_cannot_choose_authority_or_ambiguous_scope(fields):
    with pytest.raises(ValidationError):
        SpecCoverageRequest.model_validate({'view': 'coverage', **fields})


@pytest.mark.asyncio
async def test_live_facade_has_closed_discriminator(facade):
    tool = await server.mcp.get_tool('okto_pulse_get_traceability_report')
    schema = json.dumps(tool.parameters)
    assert 'SpecCoverageRequest' in schema and 'discriminator' in schema
    assert SpecCoverageRequest.model_json_schema()['additionalProperties'] is False
