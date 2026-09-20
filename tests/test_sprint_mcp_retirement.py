"""F3: removed Sprint commands cannot reach a live writer or an alias."""

import json
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.mcp_collaboration import (
    McpAskQuestionCommand,
    McpAskQuestionUseCase,
)
from okto_pulse.core.domain.mcp_permission_registry import (
    MCP_TOOL_PERMISSION_POLICIES,
)
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.tool_family_registry import REGISTRY


RETIRED = (
    "create_sprint", "update_sprint", "move_sprint", "get_sprint",
    "get_sprint_context", "assign_tasks_to_sprint", "submit_sprint_evaluation",
    "list_sprint_evaluations", "get_sprint_evaluation", "delete_sprint_evaluation",
    "ask_sprint_question", "answer_sprint_question", "delete_sprint_question",
    "suggest_sprints",
)


def _no_runtime(*args, **kwargs):
    pytest.fail("removed Sprint command reached the runtime")


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", RETIRED)
async def test_removed_names_have_no_handler_registry_policy_or_alias(suffix, monkeypatch):
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", _no_runtime)
    monkeypatch.setattr(server, "_get_agent_ctx", _no_runtime)
    name = f"okto_pulse_{suffix}"
    assert not hasattr(server, name)
    with pytest.raises(KeyError):
        await server.mcp.get_tool(name)
    assert name not in {policy.tool_name for policy in MCP_TOOL_PERMISSION_POLICIES}
    for family in (*REGISTRY.eligible(), *REGISTRY.excluded()):
        assert name not in family.legacy_aliases
        assert "sprint" not in family.target_types


@pytest.mark.asyncio
@pytest.mark.parametrize("permissions", (["*"], ["board:read"], ["sprint.qa.ask"], []))
async def test_consolidated_ask_cannot_restore_sprint_for_any_actor(permissions, monkeypatch):
    async def context(_board_id):
        return SimpleNamespace(agent_id="a", agent_name="a", permissions=permissions)

    monkeypatch.setattr(server, "_get_agent_ctx", context)
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", _no_runtime)
    result = json.loads(await server.okto_pulse_ask.fn(
        board_id="board", target_type="sprint", parent_id="old-id", question="Q"))
    assert result["error"] == "unsupported_target_type"
    assert result["allowed"] == ["card", "ideation", "refinement", "spec"]


@pytest.mark.asyncio
@pytest.mark.parametrize("target_type", ("sprint", "unknown", "Sprint", ""))
async def test_application_ask_has_no_unknown_target_fallback(target_type):
    class NoUnitOfWork:
        def __getattr__(self, name):
            pytest.fail(f"unsupported target reached unit of work: {name}")

    result = await McpAskQuestionUseCase().execute(
        McpAskQuestionCommand("board", target_type, "old-id", "Q"),
        actor=ActorContext("agent", "mcp", board_id="board", permissions=["*"]),
        uow=NoUnitOfWork(),
    )
    assert result.payload == {"error": "unsupported_target_type",
        "allowed": ["card", "ideation", "refinement", "spec"]}


@pytest.mark.asyncio
async def test_retired_resources_absent_and_surviving_work_remains_discoverable():
    uris = {spec.uri for spec in server.effective_resource_catalog().specs()}
    assert "okto-pulse://workflows/sprints" not in uris
    assert "okto-pulse://reference/tool-docs/sprint" not in uris
    tools = await server.mcp.get_tools()
    assert {"okto_pulse_get_historical_context", "okto_pulse_move_card",
        "okto_pulse_submit_task_validation", "okto_pulse_submit_spec_evaluation",
        "okto_pulse_delete_spec_question", "okto_pulse_delete_ideation_question",
        "okto_pulse_delete_refinement_question"} <= tools.keys()


def test_dedicated_sprint_mcp_use_cases_are_not_shipped_or_exported():
    from importlib.util import find_spec
    from okto_pulse.core.application import use_cases

    assert find_spec("okto_pulse.core.application.use_cases.mcp_sprint_crud") is None
    assert not [name for name in dir(use_cases) if name.startswith("Mcp") and "Sprint" in name]
    assert not [name for name in use_cases.__all__ if name.startswith("Mcp") and "Sprint" in name]
