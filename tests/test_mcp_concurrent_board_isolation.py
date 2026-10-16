"""BASE T14: real HTTP MCP requests keep identity, Board data and discovery separate."""
import asyncio
import json

import httpx
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from sqlalchemy import update

from okto_pulse.community.adapters.mcp_host import register_community_mcp_host
from okto_pulse.community.adapters.sqlalchemy_models import Agent, Board
from okto_pulse.community.adapters.sqlalchemy_unit_of_work import CommunityUnitOfWorkFactory
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.mcp import server
from okto_pulse.core.ports.mcp_resources import freeze_mcp_resource_catalog
from okto_pulse.core.runtime_registry import register_unit_of_work_factory
from test_r08d_auth_replay_gates import _harness_env, _seed, _db_mod

__all__ = ['_harness_env']


@pytest.mark.asyncio
@pytest.mark.timeout(120)
async def test_overlapping_http_mcp_sessions_do_not_leak_board_data_or_discovery(_harness_env):
    prior_tasks = asyncio.all_tasks()
    await _seed(_harness_env)
    factory = _db_mod.get_session_factory()
    register_unit_of_work_factory(CommunityUnitOfWorkFactory(factory))
    async with factory() as db:
        await db.execute(update(Board).values(realm_id=LOCAL_REALM_ID))
        await db.execute(update(Agent).where(Agent.id.in_(['A1', 'A2'])).values(permission_flags=None))
        await db.commit()

    provider = register_community_mcp_host()
    frozen = freeze_mcp_resource_catalog(server.effective_resource_catalog())
    host = provider.materialize_catalog(server.mcp, resource_catalog=frozen, projection_identity=frozen.identity)
    app = host.http_app(transport='streamable-http')
    wrapped = provider.wrap_session_middleware(app)
    rendezvous = asyncio.Barrier(2)

    async def agent_session(key, own, foreign):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=wrapped),
                                    base_url='http://test', headers={'X-API-Key': key}) as http:
            async with streamable_http_client('http://test/mcp', http_client=http) as (read, write, _session):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    listed = await session.list_tools()
                    discovery = [tool.model_dump(mode='json') for tool in listed.tools]
                    assert 'okto_pulse_get_board' in {tool.name for tool in listed.tools}
                    await rendezvous.wait()
                    for _ in range(3):
                        accepted, denied = await asyncio.gather(
                            session.call_tool('okto_pulse_get_board', {'board_id': own}),
                            session.call_tool('okto_pulse_get_board', {'board_id': foreign}))
                        assert not accepted.isError, accepted
                        assert denied.isError, denied
                        assert accepted.structuredContent['data']['id'] == own
                        assert accepted.structuredContent['data']['owner_id'] == f'A{own[-1]}'
                        actual = json.dumps(accepted.model_dump(mode='json'))
                        refusal = json.dumps(denied.model_dump(mode='json'))
                        assert f'Board {own[-1]}' in actual
                        assert f'Board {foreign[-1]}' not in actual + refusal
                        assert key not in actual + refusal
                    return discovery

    try:
        async with app.router.lifespan_context(app):
            first, second = await asyncio.gather(
                agent_session('kA1', 'B1', 'B2'), agent_session('kA2', 'B2', 'B1'))
    finally:
        # sse-starlette installs a loop-scoped shutdown watcher even for the
        # disposable ASGI host. Drain only watchers created by this fixture.
        watchers = [task for task in asyncio.all_tasks() - prior_tasks
                    if task.get_coro().__qualname__ == '_shutdown_watcher']
        for task in watchers:
            task.cancel()
        await asyncio.gather(*watchers, return_exceptions=True)
    # Tool schemas are shared capability descriptions, never actor/Board snapshots.
    assert first == second
    serialized = json.dumps(first)
    for forbidden in ('kA1', 'kA2', 'Board 1', 'Board 2'):
        assert forbidden not in serialized
