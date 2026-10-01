from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.mcp import server
from okto_pulse.core.models.bug_clusters import BugClustersRequest
from okto_pulse.core.ports.bug_clusters import BugClustersSnapshot
from okto_pulse.core.services.bug_clusters import project_bug_clusters


@pytest.fixture
def facade(monkeypatch):
    context = SimpleNamespace(agent_id='agent', agent_name='Agent', realm_id=LOCAL_REALM_ID,
        board_id='board', permissions=['board:read', 'board.read', 'card.entity.read',
            'spec.entity.read', 'kg.query.related_context', 'kg.query.learning_from_bugs'])
    board = SimpleNamespace(id='board', realm_id=LOCAL_REALM_ID, owner_id='owner', settings={})
    calls = []

    async def aggregate(query, **options):
        calls.append(query)
        return project_bug_clusters(query, BugClustersSnapshot(query.board_id, query.actor_scope_ref,
            datetime.now(timezone.utc), (), 0, True))

    operation = AsyncMock(side_effect=aggregate)
    legacy = AsyncMock(return_value={'legacy': True})
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=SimpleNamespace(analytics=SimpleNamespace(bug_clusters=operation), build_traceability_report=legacy),
        commit=AsyncMock())

    @asynccontextmanager
    async def factory(**options):
        yield uow

    monkeypatch.setattr(server, '_get_agent_ctx', AsyncMock(return_value=context))
    monkeypatch.setattr(server, 'get_unit_of_work_factory_for_mcp', lambda: factory)
    return SimpleNamespace(context=context, calls=calls, operation=operation, legacy=legacy)


async def call(**kwargs):
    tool = await server.mcp.get_tool('okto_pulse_get_traceability_report')
    return json.loads(await tool.fn(board_id='board', **kwargs))


@pytest.mark.asyncio
async def test_existing_facade_accepts_closed_bug_variant_without_a_new_tool(facade):
    result = await call(query=BugClustersRequest(view='bugs', group_by='severity'))
    assert result.get('view') == 'bugs', result
    assert result['distinct_bug_count'] == 0
    assert result['authority'] == 'informational'
    assert facade.calls[0].actor_scope_ref == f'{LOCAL_REALM_ID}:agent:mcp:agent'
    facade.legacy.assert_not_called()


@pytest.mark.asyncio
async def test_existing_sdlc_call_remains_unchanged(facade):
    assert await call(spec_id='spec') == {'legacy': True}
    facade.legacy.assert_awaited_once_with('board', ideation_id='', spec_id='spec', include_artifacts=False)
    facade.operation.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('extra', [{'spec_id': 'spec'}, {'ideation_id': 'idea'}, {'include_artifacts': True}])
async def test_ambiguous_sdlc_and_bug_scopes_are_rejected(facade, extra):
    result = await call(query=BugClustersRequest(view='bugs'), **extra)
    assert result['code'] == 'traceability_query_scope_ambiguous'
    facade.operation.assert_not_called()
    facade.legacy.assert_not_called()


@pytest.mark.asyncio
async def test_mcp_rechecks_grouping_permission(facade):
    facade.context.permissions = ['board:read', 'board.read', 'card.entity.read']
    result = await call(query=BugClustersRequest(view='bugs', group_by='learning'))
    assert result['code'] == 'permission_denied'
    facade.operation.assert_not_called()


@pytest.mark.asyncio
async def test_mcp_cursor_requires_original_window(facade):
    result = await call(query=BugClustersRequest(view='bugs', cursor='previous'))
    assert result['code'] == 'bug_clusters_cursor_window_required'
    facade.operation.assert_not_called()


@pytest.mark.asyncio
async def test_live_schema_exposes_a_closed_variant(facade):
    tool = await server.mcp.get_tool('okto_pulse_get_traceability_report')
    schema = tool.parameters
    serialized = json.dumps(schema)
    assert 'BugClustersRequest' in serialized
    assert 'additionalProperties' in serialized
    assert 'payload_json' not in serialized
    assert BugClustersRequest.model_json_schema()['additionalProperties'] is False
