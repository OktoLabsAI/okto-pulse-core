"""Removed compatibility writers cannot reach the current mutation runtime."""
import pytest

from okto_pulse.core.application import use_cases
from okto_pulse.core.domain.mcp_permission_registry import MCP_TOOL_PERMISSION_POLICIES
from okto_pulse.core.mcp import server


@pytest.mark.parametrize('suffix,command', [
    ('update_business_rule', 'McpUpdateBusinessRule'),
    ('update_decision', 'McpUpdateDecision'),
    ('update_api_contract', 'McpUpdateApiContract'),
    ('migrate_spec_decisions', 'McpMigrateSpecDecisions'),
])
async def test_retired_writer_has_no_handler_policy_or_use_case(monkeypatch, suffix, command):
    def forbidden(*args, **kwargs):
        pytest.fail('Retired writer reached mutation runtime')

    monkeypatch.setattr(server, '_get_agent_ctx', forbidden)
    name = f'okto_pulse_{suffix}'
    assert not hasattr(server, name)
    with pytest.raises(KeyError):
        await server.mcp.get_tool(name)
    assert name not in {policy.tool_name for policy in MCP_TOOL_PERMISSION_POLICIES}
    for ending in ('Command', 'Result', 'UseCase'):
        assert not hasattr(use_cases, command + ending)
