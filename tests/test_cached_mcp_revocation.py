"""BASE T13: cached MCP discovery never preserves revoked authority."""
import pytest
from fastmcp import Client
from sqlalchemy import update

from okto_pulse.community.adapters.mcp_host import CommunityMcpHostProvider
from okto_pulse.community.adapters.sqlalchemy_unit_of_work import CommunityUnitOfWorkFactory
from okto_pulse.core.mcp import server
from okto_pulse.core.ports import McpCredential
from okto_pulse.core.ports.mcp_resources import freeze_mcp_resource_catalog
from okto_pulse.core.runtime_registry import register_unit_of_work_factory
from okto_pulse.community.adapters.sqlalchemy_models import Agent, AgentBoard, Board
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from test_r08d_auth_replay_gates import _harness_env, _seed, _db_mod, _agent_full_snapshot

__all__ = ['_harness_env']


@pytest.mark.asyncio
@pytest.mark.parametrize('revocation', ['inactive', 'rotated_key', 'board_grant'])
async def test_cached_catalog_cannot_authorize_revoked_access(_harness_env, monkeypatch, revocation):
    await _seed(_harness_env)
    factory = _db_mod.get_session_factory()
    register_unit_of_work_factory(CommunityUnitOfWorkFactory(factory))
    async with factory() as db:
        # The older auth-only fixture omits realm; actual Board reads are scoped.
        await db.execute(update(Board).values(realm_id=LOCAL_REALM_ID))
        agent = await db.get(Agent, 'A1')
        agent.permission_flags = None
        await db.commit()

    # In-process FastMCP has no HTTP request; supply only the extracted credential.
    # Authentication, database lookup, Board access and tool execution stay real.
    monkeypatch.setattr(server, 'active_api_key_credential',
                        lambda: McpCredential(source='x_api_key_header', value='kA1'))
    frozen = freeze_mcp_resource_catalog(server.effective_resource_catalog())
    host = CommunityMcpHostProvider().materialize_catalog(server.mcp,
        resource_catalog=frozen, projection_identity=frozen.identity)
    async with Client(host) as client:
        cached_tools = {tool.name: tool for tool in await client.list_tools()}
        cached_tool = cached_tools['okto_pulse_get_board']
        first = await client.call_tool(cached_tool.name, {'board_id': 'B1'}, raise_on_error=False)
        assert not first.is_error, first
        assert 'Board 1' in str(first)

        async with factory() as db:
            agent = await db.get(Agent, 'A1')
            if revocation == 'inactive':
                agent.is_active = False
            elif revocation == 'rotated_key':
                agent.api_key_hash = '0' * 64
            else:
                await db.delete(await db.get(AgentBoard, 'AB1'))
            await db.commit()
        before = await _agent_full_snapshot()
        # Reuse the same client session and cached tool, without tools/list refresh.
        refused = await client.call_tool(cached_tool.name, {'board_id': 'B1'}, raise_on_error=False)
        assert refused.is_error, refused
        assert 'Board 1' not in str(refused)
        assert await _agent_full_snapshot() == before
