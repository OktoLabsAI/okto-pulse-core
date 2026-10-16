"""Retired polymorphic MCP reads fail before persistence or topic fallback."""

import json
from typing import get_args

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext, CommandValidationError
from okto_pulse.core.application.use_cases.entity_pagination import list_entities_page
from okto_pulse.core.application.use_cases.mcp_board_crud import McpListByBoardCommand, McpListByBoardUseCase
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.filters import BoardEntityType, supported_filter_keys
from okto_pulse.core.ports.application_persistence import PageRequest


def no_read(*args, **kwargs):
    pytest.fail("retired entity reached authentication or persistence")


@pytest.mark.asyncio
@pytest.mark.parametrize("filters", (None, {"spec_id": "old-spec"}, {"include_archived": True}))
async def test_board_list_rejects_retired_entity_before_any_lookup(monkeypatch, filters):
    monkeypatch.setattr(server, "_get_agent_ctx", no_read)
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", no_read)
    result = json.loads(await server.okto_pulse_list_by_board.fn(
        board_id="board", entity_type="sprint", filters=filters))
    assert result["error_code"] == "unsupported_entity"
    assert result["supported"] == ["spec", "ideation", "refinement", "story", "topic"]


@pytest.mark.asyncio
@pytest.mark.parametrize("arguments", ({"entity_id": "old-id"}, {"current_status": "draft"}))
async def test_lifecycle_discovery_cannot_reintroduce_retired_operations(monkeypatch, arguments):
    monkeypatch.setattr(server, "_get_agent_ctx", no_read)
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", no_read)
    result = json.loads(await server.okto_pulse_get_allowed_transitions.fn(
        board_id="board", entity_type="sprint", **arguments))
    assert result["error_code"] == "unsupported_entity"
    assert "sprint" not in result["supported"]


@pytest.mark.asyncio
@pytest.mark.parametrize("entity_type", ("sprint", "unknown", ""))
async def test_application_list_rejects_retired_and_unknown_targets(entity_type):
    class NoUnitOfWork:
        def __getattr__(self, name):
            pytest.fail(f"unknown target reached {name}")

    with pytest.raises(CommandValidationError, match="unsupported_entity"):
        await McpListByBoardUseCase().execute(
            McpListByBoardCommand("board", entity_type, {"spec_id": "old-spec"}),
            actor=ActorContext("owner", "mcp", board_id="board", permissions=["*"]),
            uow=NoUnitOfWork())


@pytest.mark.asyncio
async def test_retired_page_surface_and_filters_are_unavailable():
    with pytest.raises(ValueError, match="page_request_unknown_surface"):
        await list_entities_page(None, PageRequest(surface="mcp_sprint_list", scope=(), offset=0, limit=10))
    assert "sprint" not in get_args(BoardEntityType)
    assert supported_filter_keys("sprint", scope="by_board") == []
    assert supported_filter_keys("sprint", scope="qa") == []
