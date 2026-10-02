"""Retired guideline writers cannot dispatch or author policy through old payloads."""
import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.mcp_permission_registry import MCP_TOOL_PERMISSION_POLICIES
from okto_pulse.core.mcp import server
from okto_pulse.core.models.schemas import BoardGuidelineCreate


@pytest.mark.parametrize('name', [
    'okto_pulse_update_guideline', 'okto_pulse_delete_guideline',
    'okto_pulse_link_guideline_to_board', 'okto_pulse_update_board_guideline_priority',
])
async def test_removed_guideline_writer_cannot_reach_uow(monkeypatch, name):
    def forbidden(*args, **kwargs):
        pytest.fail('Removed writer reached runtime')
    monkeypatch.setattr(server, '_get_agent_ctx', forbidden)
    assert not hasattr(server, name)
    with pytest.raises(KeyError):
        await server.mcp.get_tool(name)
    assert name not in {policy.tool_name for policy in MCP_TOOL_PERMISSION_POLICIES}


def test_inline_creation_cannot_accept_old_adoption_payload():
    with pytest.raises(ValidationError) as error:
        BoardGuidelineCreate.model_validate({
            'title': 'Context', 'content': 'Text', 'guideline_id': 'old-link',
        })
    assert [(e['loc'], e['type']) for e in error.value.errors()] == [
        (('guideline_id',), 'extra_forbidden'),
    ]
