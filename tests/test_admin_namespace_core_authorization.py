"""Central authorization contracts for administrative namespace writes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Awaitable, Callable

import pytest

from okto_pulse.core.application.use_cases import agent_crud, boards_crud
from okto_pulse.core.application.use_cases import (
    mcp_admin_validation_analytics,
    mcp_board_crud,
)
from okto_pulse.core.application.use_cases.admin_catalog import (
    CreateDesignSystemUseCase,
    DefaultBoardConfigCommand,
    DesignSystemCommand,
    SetDefaultDesignSystemUseCase,
)
from okto_pulse.core.application.use_cases.agent_crud import (
    CreateAgentCommand,
    CreateAgentUseCase,
    GetAgentCommand,
    GetAgentUseCase,
    ListAgentsForBoardCommand,
    ListAgentsForBoardUseCase,
    ListAgentsForUserCommand,
    ListAgentsForUserUseCase,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    decide_authorization,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.boards_crud import (
    RevokeBoardShareCommand,
    RevokeBoardShareUseCase,
    ShareBoardCommand,
    ShareBoardUseCase,
    UpdateBoardCommand,
    UpdateBoardUseCase,
)
from okto_pulse.core.application.use_cases.import_export import (
    ImportBoardConfigCommand,
    ImportBoardConfigUseCase,
    ImportDesignSystemsCommand,
    ImportDesignSystemsUseCase,
    ImportPresetsCommand,
    ImportPresetsUseCase,
)
from okto_pulse.core.application.use_cases.list_boards_for_agent import (
    ListBoardsForAgentCommand,
    ListBoardsForAgentUseCase,
)
from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
    McpCreateDesignSystemCommand,
    McpCreateDesignSystemUseCase,
    McpSetDefaultDesignSystemCommand,
    McpSetDefaultDesignSystemUseCase,
)
from okto_pulse.core.application.use_cases.mcp_board_crud import (
    McpCreateDefaultBoardConfigVersionCommand,
    McpCreateDefaultBoardConfigVersionUseCase,
    McpLinkBoardDesignSystemCommand,
    McpLinkBoardDesignSystemUseCase,
    McpListBoardMembersCommand,
    McpListBoardMembersUseCase,
)
from okto_pulse.core.application.use_cases.mcp_profile_activity import (
    McpListAgentsCommand,
    McpListAgentsUseCase,
)
from okto_pulse.core.application.use_cases.permission_presets import (
    CreatePermissionPresetCommand,
    CreatePermissionPresetUseCase,
)
from okto_pulse.core.domain.realm import RealmScope


class _ServiceSpy:
    def __init__(self, **results: Any) -> None:
        self.results = results
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Callable[..., Awaitable[Any]]:
        async def _call(*args: Any, **kwargs: Any) -> Any:
            self.calls.append(name)
            result = self.results.get(name)
            return result(*args, **kwargs) if callable(result) else result

        return _call


class _Uow:
    def __init__(self, services: Any) -> None:
        self.services = services
        self.commit_calls = 0
        self.rollback_calls = 0

    async def commit(self) -> None:
        self.commit_calls += 1

    async def rollback(self) -> None:
        self.rollback_calls += 1


def _actor(permissions: list[str]) -> ActorContext:
    return ActorContext(
        "owner-1",
        "mcp",
        board_id="board-1",
        realm_id="local",
        realm_scope=RealmScope.local(),
        permissions=permissions,
    )


@dataclass(frozen=True)
class _WriteCase:
    operation: str
    legacy: str
    writer: str
    build: Callable[[], tuple[_Uow, _ServiceSpy]]
    invoke: Callable[[_Uow, ActorContext], Awaitable[Any]]


def _agent_case() -> tuple[_Uow, _ServiceSpy]:
    agent = SimpleNamespace(id="agent-1")
    service = _ServiceSpy(
        create_agent=(agent, "secret"),
        get_agent=agent,
    )
    return _Uow(SimpleNamespace(agents=service)), service


async def _invoke_agent(uow: _Uow, actor: ActorContext) -> Any:
    return await CreateAgentUseCase().execute(
        CreateAgentCommand(SimpleNamespace()),
        actor=actor,
        uow=uow,
    )


def _board_case() -> tuple[_Uow, _ServiceSpy]:
    board = SimpleNamespace(id="board-1", owner_id="owner-1", settings={})
    service = _ServiceSpy(update_board=board, get_board=board)
    agents = _ServiceSpy(list_agents_for_board=[])
    return _Uow(SimpleNamespace(boards=service, agents=agents)), service


async def _invoke_board(uow: _Uow, actor: ActorContext) -> Any:
    return await UpdateBoardUseCase().execute(
        UpdateBoardCommand("board-1", SimpleNamespace()),
        actor=actor,
        uow=uow,
    )


def _share_case() -> tuple[_Uow, _ServiceSpy]:
    service = _ServiceSpy(share_board=SimpleNamespace(id="share-1"))
    return _Uow(SimpleNamespace(shares=service)), service


async def _invoke_share(uow: _Uow, actor: ActorContext) -> Any:
    return await ShareBoardUseCase().execute(
        ShareBoardCommand("board-1", SimpleNamespace()),
        actor=actor,
        uow=uow,
    )


def _preset_case() -> tuple[_Uow, _ServiceSpy]:
    service = _ServiceSpy(create_preset=SimpleNamespace(id="preset-1"))
    return _Uow(SimpleNamespace(permission_presets=service)), service


async def _invoke_preset(uow: _Uow, actor: ActorContext) -> Any:
    return await CreatePermissionPresetUseCase().execute(
        CreatePermissionPresetCommand(name="Preset", description="", flags={}),
        actor=actor,
        uow=uow,
    )


def _preset_import_case() -> tuple[_Uow, _ServiceSpy]:
    service = _ServiceSpy(
        list_presets=[],
        get_preset=None,
        create_preset=SimpleNamespace(id="preset-1", name="Preset"),
    )
    return _Uow(SimpleNamespace(permission_presets=service)), service


async def _invoke_preset_import(uow: _Uow, actor: ActorContext) -> Any:
    return await ImportPresetsUseCase().execute(
        ImportPresetsCommand(items=[{"name": "Preset", "flags": {}}]),
        actor=actor,
        uow=uow,
    )


def _default_config_case() -> tuple[_Uow, _ServiceSpy]:
    service = _ServiceSpy(set_template_design_system={"id": "template-1"})
    return _Uow(SimpleNamespace(default_board_config=service)), service


async def _invoke_default_config(uow: _Uow, actor: ActorContext) -> Any:
    return await SetDefaultDesignSystemUseCase().execute(
        DefaultBoardConfigCommand(
            template_id="template-1",
            payload={"design_system_id": "design-1"},
        ),
        actor=actor,
        uow=uow,
    )


async def _invoke_mcp_default_config(uow: _Uow, actor: ActorContext) -> Any:
    return await McpSetDefaultDesignSystemUseCase().execute(
        McpSetDefaultDesignSystemCommand(
            template_id="template-1",
            design_system_id="design-1",
        ),
        actor=actor,
        uow=uow,
    )


def _mcp_default_create_case() -> tuple[_Uow, _ServiceSpy]:
    service = _ServiceSpy(
        preview_create_guideline_ref_diff={
            "added": [],
            "removed": [],
            "reordered": [],
        },
        create_version={"id": "template-1"},
    )
    return _Uow(SimpleNamespace(default_board_config=service)), service


async def _invoke_mcp_default_create(uow: _Uow, actor: ActorContext) -> Any:
    return await McpCreateDefaultBoardConfigVersionUseCase().execute(
        McpCreateDefaultBoardConfigVersionCommand(
            settings_payload=None,
            scope="global",
            guideline_default_refs=None,
            design_system_default_ref=None,
            activate=False,
        ),
        actor=actor,
        uow=uow,
    )


def _default_config_import_case() -> tuple[_Uow, _ServiceSpy]:
    service = _ServiceSpy(
        preview_create_guideline_ref_diff={
            "added": [],
            "removed": [],
            "reordered": [],
        },
        create_version={"id": "template-1"},
    )
    return _Uow(SimpleNamespace(default_board_config=service)), service


async def _invoke_default_config_import(uow: _Uow, actor: ActorContext) -> Any:
    return await ImportBoardConfigUseCase().execute(
        ImportBoardConfigCommand(
            items=[{"scope": "global", "settings_payload": {}}]
        ),
        actor=actor,
        uow=uow,
    )


def _design_system_record() -> SimpleNamespace:
    return SimpleNamespace(
        id="design-1",
        scope="global",
        board_id=None,
        title="Design",
        version=1,
        status="active",
        owner_id="owner-1",
        payload={},
        created_at=None,
        updated_at=None,
    )


def _design_system_case() -> tuple[_Uow, _ServiceSpy]:
    service = _ServiceSpy(create_design_system=_design_system_record())
    return _Uow(SimpleNamespace(design_systems=service)), service


async def _invoke_design_system(uow: _Uow, actor: ActorContext) -> Any:
    return await CreateDesignSystemUseCase().execute(
        DesignSystemCommand(payload={"title": "Design", "scope": "global"}),
        actor=actor,
        uow=uow,
    )


async def _invoke_mcp_design_system(uow: _Uow, actor: ActorContext) -> Any:
    return await McpCreateDesignSystemUseCase().execute(
        McpCreateDesignSystemCommand(
            "board-1",
            title="Design",
            scope="global",
        ),
        actor=actor,
        uow=uow,
    )


def _design_link_case() -> tuple[_Uow, _ServiceSpy]:
    link = SimpleNamespace(
        board_id="board-1",
        design_system_id="design-1",
        design_system_version=1,
    )
    service = _ServiceSpy(
        require_design_system=_design_system_record(),
        link_design_system_to_board=link,
    )
    return _Uow(SimpleNamespace(design_systems=service)), service


async def _invoke_design_link(uow: _Uow, actor: ActorContext) -> Any:
    return await McpLinkBoardDesignSystemUseCase().execute(
        McpLinkBoardDesignSystemCommand("board-1", "design-1"),
        actor=actor,
        uow=uow,
    )


def _design_system_import_case() -> tuple[_Uow, _ServiceSpy]:
    service = _ServiceSpy(
        get_design_system=None,
        create_design_system=_design_system_record(),
    )
    return _Uow(SimpleNamespace(design_systems=service)), service


async def _invoke_design_system_import(uow: _Uow, actor: ActorContext) -> Any:
    return await ImportDesignSystemsUseCase().execute(
        ImportDesignSystemsCommand(
            items=[{"id": "design-1", "title": "Design", "version": 1}]
        ),
        actor=actor,
        uow=uow,
    )


_WRITE_CASES = (
    _WriteCase(
        "agent.entity.create",
        "profile.update",
        "create_agent",
        _agent_case,
        _invoke_agent,
    ),
    _WriteCase(
        "board.admin.edit",
        "board.read",
        "update_board",
        _board_case,
        _invoke_board,
    ),
    _WriteCase(
        "board.share.create",
        "board.read",
        "share_board",
        _share_case,
        _invoke_share,
    ),
    _WriteCase(
        "permission_preset.entity.create",
        "profile.update",
        "create_preset",
        _preset_case,
        _invoke_preset,
    ),
    _WriteCase(
        "permission_preset.import",
        "profile.update",
        "create_preset",
        _preset_import_case,
        _invoke_preset_import,
    ),
    _WriteCase(
        "default_board_config.set_design_system",
        "spec.entity.edit_fields",
        "set_template_design_system",
        _default_config_case,
        _invoke_default_config,
    ),
    _WriteCase(
        "default_board_config.set_design_system",
        "spec.entity.edit_fields",
        "set_template_design_system",
        _default_config_case,
        _invoke_mcp_default_config,
    ),
    _WriteCase(
        "default_board_config.create",
        "spec.entity.edit_fields",
        "create_version",
        _mcp_default_create_case,
        _invoke_mcp_default_create,
    ),
    _WriteCase(
        "default_board_config.import",
        "spec.entity.edit_fields",
        "create_version",
        _default_config_import_case,
        _invoke_default_config_import,
    ),
    _WriteCase(
        "design_system.entity.create",
        "spec.architecture.create",
        "create_design_system",
        _design_system_case,
        _invoke_design_system,
    ),
    _WriteCase(
        "design_system.entity.create",
        "spec.architecture.create",
        "create_design_system",
        _design_system_case,
        _invoke_mcp_design_system,
    ),
    _WriteCase(
        "design_system.board_link.create",
        "spec.architecture.edit",
        "link_design_system_to_board",
        _design_link_case,
        _invoke_design_link,
    ),
    _WriteCase(
        "design_system.import",
        "spec.architecture.import",
        "create_design_system",
        _design_system_import_case,
        _invoke_design_system_import,
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    _WRITE_CASES,
    ids=lambda case: f"{case.operation}-{case.invoke.__name__}",
)
async def test_namespace_write_denial_precedes_writer_and_commit_and_legacy_allows(
    case: _WriteCase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _owned_board(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(id="board-1", owner_id="owner-1")

    monkeypatch.setattr(boards_crud, "_require_owned_board", _owned_board)
    monkeypatch.setattr(boards_crud, "_require_readable_board", _owned_board)
    monkeypatch.setattr(
        mcp_admin_validation_analytics,
        "_require_mcp_design_system_board",
        _owned_board,
    )
    monkeypatch.setattr(
        mcp_board_crud,
        "_require_design_system_board",
        _owned_board,
    )

    denied_uow, denied_service = case.build()
    with pytest.raises(PermissionDeniedError) as exc_info:
        await case.invoke(denied_uow, _actor([]))
    assert case.operation in str(exc_info.value)
    assert case.writer not in denied_service.calls
    assert denied_uow.commit_calls == 0

    allowed_uow, allowed_service = case.build()
    await case.invoke(allowed_uow, _actor([case.legacy]))
    assert allowed_service.calls.count(case.writer) == 1
    assert allowed_uow.commit_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "invoke"),
    (
        (
            "agent.entity.read",
            lambda uow, actor: McpListAgentsUseCase().execute(
                McpListAgentsCommand("board-1"), actor=actor, uow=uow
            ),
        ),
        (
            "board.share.read",
            lambda uow, actor: McpListBoardMembersUseCase().execute(
                McpListBoardMembersCommand("board-1"), actor=actor, uow=uow
            ),
        ),
    ),
    ids=("list-agents", "list-board-members"),
)
async def test_mcp_admin_reads_require_exact_core_namespace_before_services(
    operation: str,
    invoke,
) -> None:
    board_service = _ServiceSpy(
        get_board=SimpleNamespace(id="board-1", owner_id="owner-1")
    )
    agent_service = _ServiceSpy(list_agents=[], list_agents_for_board=[])
    denied_uow = _Uow(SimpleNamespace(boards=board_service, agents=agent_service))

    with pytest.raises(PermissionDeniedError):
        await invoke(denied_uow, _actor([]))
    assert board_service.calls == []
    assert agent_service.calls == []

    allowed_board_service = _ServiceSpy(
        get_board=SimpleNamespace(id="board-1", owner_id="owner-1")
    )
    allowed_agent_service = _ServiceSpy(
        list_agents=[],
        list_agents_for_board=[],
    )
    allowed_uow = _Uow(
        SimpleNamespace(
            boards=allowed_board_service,
            agents=allowed_agent_service,
        )
    )
    await invoke(allowed_uow, _actor([operation]))
    assert allowed_agent_service.calls


@pytest.mark.parametrize(
    ("operation", "legacy"),
    (
        ("agent.entity.create", "profile.update"),
        ("agent.entity.edit", "profile.update"),
        ("agent.entity.delete", "profile.update"),
        ("agent.api_key.rotate", "profile.update"),
        ("agent.board_access.grant", "board.read"),
        ("agent.board_access.edit", "board.read"),
        ("agent.board_access.revoke", "board.read"),
        ("board.admin.create", "board.read"),
        ("board.admin.edit", "board.read"),
        ("board.admin.delete", "board.read"),
        ("board.share.create", "board.read"),
        ("board.share.edit", "board.read"),
        ("board.share.revoke", "board.read"),
        ("board.share.leave", "board.read"),
        ("permission_preset.entity.create", "profile.update"),
        ("permission_preset.entity.edit", "profile.update"),
        ("permission_preset.entity.delete", "profile.update"),
        ("permission_preset.clone", "profile.update"),
        ("permission_preset.import", "profile.update"),
        ("default_board_config.create", "spec.entity.edit_fields"),
        ("default_board_config.activate", "spec.entity.edit_fields"),
        ("default_board_config.deactivate", "spec.entity.edit_fields"),
        ("default_board_config.import", "spec.entity.edit_fields"),
        ("default_board_config.set_design_system", "spec.entity.edit_fields"),
        (
            "default_board_config.guidelines.edit",
            "guidelines.adoption.manage",
        ),
        ("design_system.entity.create", "spec.architecture.create"),
        ("design_system.entity.edit", "spec.architecture.edit"),
        ("design_system.entity.delete", "spec.architecture.delete"),
        ("design_system.import", "spec.architecture.import"),
        ("design_system.board_link.create", "spec.architecture.edit"),
        ("design_system.board_link.delete", "spec.architecture.edit"),
    ),
)
def test_each_namespace_write_declares_explicit_legacy_compatibility(
    operation: str,
    legacy: str,
) -> None:
    decision = decide_authorization(
        _actor([legacy]),
        PermissionRequirement(operation, legacy_operation=legacy),
    )
    assert decision.allowed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("board_owner", "share_user", "operation"),
    (
        ("owner-1", "target-1", "board.share.revoke"),
        ("different-owner", "owner-1", "board.share.leave"),
    ),
)
async def test_revoke_share_selects_revoke_or_leave_before_writer(
    board_owner: str,
    share_user: str,
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _readable_board(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(id="board-1", owner_id=board_owner)

    monkeypatch.setattr(boards_crud, "_require_readable_board", _readable_board)
    service = _ServiceSpy(
        list_shares=[SimpleNamespace(id="share-1", user_id=share_user)],
        revoke_share=True,
    )
    uow = _Uow(SimpleNamespace(shares=service))

    with pytest.raises(PermissionDeniedError) as exc_info:
        await RevokeBoardShareUseCase().execute(
            RevokeBoardShareCommand("board-1", "share-1"),
            actor=_actor([]),
            uow=uow,
        )

    assert operation in str(exc_info.value)
    assert "revoke_share" not in service.calls
    assert uow.commit_calls == 0


@pytest.mark.asyncio
async def test_mcp_adapter_projects_core_denial_without_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server

    async def _agent_context(_board_id: str) -> SimpleNamespace:
        return SimpleNamespace(
            agent_id="owner-1",
            agent_name="Agent",
            realm_id="local",
            permissions=[],
        )

    async def _owned_board(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(id="board-1", owner_id="owner-1")

    service = _ServiceSpy(create_design_system=_design_system_record())
    uow = _Uow(SimpleNamespace(design_systems=service))

    class _Manager:
        async def __aenter__(self) -> _Uow:
            return uow

        async def __aexit__(self, *_exc: object) -> None:
            return None

    monkeypatch.setattr(server, "_get_agent_ctx", _agent_context)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: (lambda *, actor: _Manager()),
    )
    monkeypatch.setattr(
        mcp_admin_validation_analytics,
        "_require_mcp_design_system_board",
        _owned_board,
    )

    raw = await server.mcp._tool_manager._tools[
        "okto_pulse_create_design_system"
    ].fn(board_id="board-1", title="Design")

    payload = json.loads(raw)
    assert "design_system.entity.create" in payload["error"]
    assert "create_design_system" not in service.calls
    assert uow.commit_calls == 0


@pytest.mark.asyncio
async def test_agent_entity_list_read_is_guarded_before_catalog_access() -> None:
    service = _ServiceSpy(list_agents_for_user=[])
    uow = _Uow(SimpleNamespace(agents=service))

    with pytest.raises(PermissionDeniedError, match="agent.entity.read"):
        await ListAgentsForUserUseCase().execute(
            ListAgentsForUserCommand(),
            actor=_actor([]),
            uow=uow,
        )

    assert "list_agents_for_user" not in service.calls


@pytest.mark.asyncio
async def test_agent_entity_get_resolves_ownership_then_guards_read() -> None:
    service = _ServiceSpy(
        get_agent=SimpleNamespace(id="agent-1", created_by="owner-1")
    )
    uow = _Uow(SimpleNamespace(agents=service))

    with pytest.raises(PermissionDeniedError, match="agent.entity.read"):
        await GetAgentUseCase().execute(
            GetAgentCommand("agent-1"),
            actor=_actor([]),
            uow=uow,
        )

    assert service.calls == ["get_agent"]


@pytest.mark.asyncio
async def test_agent_board_access_list_is_guarded_after_board_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _owned_board(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(id="board-1", owner_id="owner-1")

    monkeypatch.setattr(agent_crud, "_require_owned_board", _owned_board)
    service = _ServiceSpy(list_agents_for_board=[])
    uow = _Uow(SimpleNamespace(agents=service))

    with pytest.raises(PermissionDeniedError, match="agent.board_access.read"):
        await ListAgentsForBoardUseCase().execute(
            ListAgentsForBoardCommand("board-1"),
            actor=_actor([]),
            uow=uow,
        )

    assert "list_agents_for_board" not in service.calls


@pytest.mark.asyncio
async def test_agent_accessible_boards_read_is_guarded_before_reader() -> None:
    service = _ServiceSpy(list_boards_for_agent=[])
    uow = _Uow(SimpleNamespace(agents=service))

    with pytest.raises(PermissionDeniedError, match="agent.board_access.read"):
        await ListBoardsForAgentUseCase().execute(
            ListBoardsForAgentCommand("agent-1"),
            actor=_actor([]),
            uow=uow,
        )

    assert "list_boards_for_agent" not in service.calls
    assert uow.commit_calls == 0
