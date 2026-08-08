"""MCP admin reads delegate exact decisions to Core without legacy prechecks."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from okto_pulse.core.application.use_cases.mcp_board_crud import (
    McpGetActiveDefaultBoardConfigUseCase,
    McpGetBoardDefaultConfigDiffUseCase,
    McpGetBoardDesignSystemUseCase,
    McpListBoardMembersUseCase,
    McpListDefaultBoardConfigVersionsUseCase,
)
from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
    McpGetDesignSystemUseCase,
    McpListDesignSystemsUseCase,
)
from okto_pulse.core.application.use_cases.mcp_profile_activity import (
    McpListAgentsUseCase,
)
from okto_pulse.core.mcp import server as mcp_server


class _UowContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _UowFactory:
    def __call__(self, *, actor):
        del actor
        return _UowContext()


def _ctx(*permissions: str) -> SimpleNamespace:
    return SimpleNamespace(
        agent_id="agent-1",
        agent_name="Agent",
        board_id="board-1",
        permissions=list(permissions),
    )


@pytest.mark.asyncio
async def test_list_agents_has_no_board_read_edge_precheck() -> None:
    execute = AsyncMock(return_value=SimpleNamespace(agents=[]))
    with (
        patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_ctx("agent.entity.read"))),
        patch.object(mcp_server, "check_permission", return_value="legacy precheck denied"),
        patch.object(mcp_server, "get_unit_of_work_factory_for_mcp", return_value=_UowFactory()),
        patch.object(McpListAgentsUseCase, "execute", execute),
    ):
        payload = json.loads(
            await mcp_server.okto_pulse_list_agents.fn(board_id="board-1")
        )

    assert payload == []
    execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_list_board_members_has_no_board_read_edge_precheck() -> None:
    execute = AsyncMock(
        return_value=SimpleNamespace(
            board=SimpleNamespace(owner_id="owner-1"),
            agents=[],
        )
    )
    with (
        patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_ctx("board.share.read"))),
        patch.object(mcp_server, "check_permission", return_value="legacy precheck denied"),
        patch.object(mcp_server, "get_unit_of_work_factory_for_mcp", return_value=_UowFactory()),
        patch.object(McpListBoardMembersUseCase, "execute", execute),
    ):
        payload = json.loads(
            await mcp_server.okto_pulse_list_board_members.fn(board_id="board-1")
        )

    assert payload == {"owner": {"id": "owner-1", "type": "user"}, "agents": []}
    execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_publish_health_requires_exact_core_permission_before_port_read() -> None:
    agent = SimpleNamespace(id="agent-1", name="Agent", permissions=[])
    port_getter = Mock()
    with (
        patch.object(mcp_server, "_get_authenticated_agent", AsyncMock(return_value=agent)),
        patch(
            "okto_pulse.core.telemetry.telemetry_port_registry.get_telemetry_port",
            port_getter,
        ),
    ):
        denied = json.loads(await mcp_server.okto_pulse_get_publish_health.fn())

    denial_detail = json.loads(denied["error"])
    assert denial_detail["required_permission"] == "metrics.publish_health.read"
    port_getter.assert_not_called()

    agent.permissions = ["metrics.publish_health.read"]
    telemetry = SimpleNamespace(publish_health=Mock(return_value={"status": "healthy"}))
    with (
        patch.object(mcp_server, "_get_authenticated_agent", AsyncMock(return_value=agent)),
        patch(
            "okto_pulse.core.telemetry.telemetry_port_registry.get_telemetry_port",
            return_value=telemetry,
        ) as allowed_getter,
    ):
        allowed = json.loads(await mcp_server.okto_pulse_get_publish_health.fn())

    assert allowed == {"status": "healthy"}
    allowed_getter.assert_called_once()
    telemetry.publish_health.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("permission", "use_case", "invoke", "data"),
    (
        (
            "default_board_config.read",
            McpGetActiveDefaultBoardConfigUseCase,
            lambda: mcp_server.okto_pulse_get_active_default_board_config.fn(
                board_id="board-1"
            ),
            {"active": None},
        ),
        (
            "default_board_config.read",
            McpListDefaultBoardConfigVersionsUseCase,
            lambda: mcp_server.okto_pulse_list_default_board_config_versions.fn(
                board_id="board-1"
            ),
            {"versions": []},
        ),
        (
            "default_board_config.diff_read",
            McpGetBoardDefaultConfigDiffUseCase,
            lambda: mcp_server.okto_pulse_get_board_default_config_diff.fn(
                board_id="board-1"
            ),
            {"fields": []},
        ),
        (
            "design_system.entity.read",
            McpListDesignSystemsUseCase,
            lambda: mcp_server.okto_pulse_list_design_systems.fn(
                board_id="board-1"
            ),
            {"items": []},
        ),
        (
            "design_system.entity.read",
            McpGetDesignSystemUseCase,
            lambda: mcp_server.okto_pulse_get_design_system.fn(
                board_id="board-1",
                design_system_id="design-1",
            ),
            {"id": "design-1"},
        ),
        (
            "design_system.board_link.read",
            McpGetBoardDesignSystemUseCase,
            lambda: mcp_server.okto_pulse_get_board_design_system.fn(
                board_id="board-1"
            ),
            {"effective": None},
        ),
    ),
    ids=(
        "default-active",
        "default-versions",
        "default-diff",
        "design-list",
        "design-get",
        "design-board-effective",
    ),
)
async def test_admin_reads_accept_exact_flag_without_board_read_edge_gate(
    permission,
    use_case,
    invoke,
    data,
) -> None:
    execute = AsyncMock(return_value=SimpleNamespace(data=data))
    with (
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=_ctx(permission)),
        ),
        patch.object(mcp_server, "check_permission", return_value="legacy denied"),
        patch.object(
            mcp_server,
            "get_unit_of_work_factory_for_mcp",
            return_value=_UowFactory(),
        ),
        patch.object(use_case, "execute", execute),
    ):
        payload = json.loads(await invoke())

    assert "error" not in payload
    execute.assert_awaited_once()
