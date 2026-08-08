"""SK-B/B22 authorization for guideline refs in default board configuration."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
import json

import pytest

from okto_pulse.core.application.use_cases.admin_catalog import (
    ActivateDefaultBoardConfigVersionUseCase,
    CreateDefaultBoardConfigVersionUseCase,
    DeactivateDefaultBoardConfigVersionUseCase,
    DefaultBoardConfigCommand,
    UpdateDefaultGuidelineRefsUseCase,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.import_export import (
    ImportBoardConfigCommand,
    ImportBoardConfigUseCase,
)
from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
    McpUpdateDefaultGuidelineRefsCommand,
    McpUpdateDefaultGuidelineRefsUseCase,
)
from okto_pulse.core.application.use_cases.mcp_board_crud import (
    McpActivateDefaultBoardConfigVersionCommand,
    McpActivateDefaultBoardConfigVersionUseCase,
    McpCreateDefaultBoardConfigVersionCommand,
    McpCreateDefaultBoardConfigVersionUseCase,
    McpDeactivateDefaultBoardConfigVersionCommand,
    McpDeactivateDefaultBoardConfigVersionUseCase,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID

pytestmark = pytest.mark.asyncio

_UNCHANGED = {"added": [], "removed": [], "reordered": []}
_CHANGED = {"added": ["guideline-1"], "removed": [], "reordered": []}


def _actor(*, granted: bool = False, source: str = "rest") -> ActorContext:
    permissions: dict[str, Any] = {
        "default_board_config": {
            "create": True,
            "activate": True,
            "deactivate": True,
        },
        "spec": {
            "entity": {
                "edit_fields": True,
            }
        },
    }
    if granted:
        permissions["default_board_config"]["guidelines"] = {"edit": True}
        permissions["guidelines"] = {
            "adoption": {
                "manage": True,
            }
        }
    return ActorContext(
        "actor-1",
        source,  # type: ignore[arg-type]
        board_id="board-1" if source == "mcp" else None,
        realm_id=LOCAL_REALM_ID,
        permissions=permissions,
        roles=("admin",),
    )


class _DefaultConfigSpy:
    def __init__(
        self,
        *,
        create_diff: dict[str, list[str]] = _UNCHANGED,
        activate_diff: dict[str, list[str]] = _UNCHANGED,
        deactivate_diff: dict[str, list[str]] = _UNCHANGED,
    ) -> None:
        self.create_diff = create_diff
        self.activate_diff = activate_diff
        self.deactivate_diff = deactivate_diff
        self.mutations: list[str] = []
        self.preview_calls: list[str] = []

    async def get_active(self, *, scope: str) -> dict[str, Any]:
        del scope
        return {"active": None}

    async def preview_create_guideline_ref_diff(self, **_kwargs: Any) -> dict[str, list[str]]:
        self.preview_calls.append("create")
        return self.create_diff

    async def preview_activate_guideline_ref_diff(
        self, **_kwargs: Any
    ) -> dict[str, list[str]]:
        self.preview_calls.append("activate")
        return self.activate_diff

    async def preview_deactivate_guideline_ref_diff(
        self, **_kwargs: Any
    ) -> dict[str, list[str]]:
        self.preview_calls.append("deactivate")
        return self.deactivate_diff

    async def create_version(self, **_kwargs: Any) -> dict[str, Any]:
        self.mutations.append("create")
        return {"id": "template-created"}

    async def activate_version(self, **_kwargs: Any) -> dict[str, Any]:
        self.mutations.append("activate")
        return {"id": "template-activated"}

    async def deactivate_version(self, **_kwargs: Any) -> dict[str, Any]:
        self.mutations.append("deactivate")
        return {"id": "template-deactivated"}

    async def update_template_guidelines(self, **_kwargs: Any) -> dict[str, Any]:
        self.mutations.append("update")
        return {"id": "template-updated"}


class _Uow:
    def __init__(self, service: _DefaultConfigSpy) -> None:
        self.services = SimpleNamespace(default_board_config=service)
        self.commit_calls = 0
        self.rollback_calls = 0

    async def commit(self) -> None:
        self.commit_calls += 1

    async def rollback(self) -> None:
        self.rollback_calls += 1


@pytest.mark.parametrize(
    ("use_case", "command", "preview_name"),
    (
        (
            CreateDefaultBoardConfigVersionUseCase(),
            DefaultBoardConfigCommand(
                payload={
                    "scope": "global",
                    "settings_payload": {},
                    "guideline_default_refs": [{"guideline_id": "guideline-1"}],
                }
            ),
            "create",
        ),
        (
            ActivateDefaultBoardConfigVersionUseCase(),
            DefaultBoardConfigCommand(template_id="template-1"),
            "activate",
        ),
        (
            DeactivateDefaultBoardConfigVersionUseCase(),
            DefaultBoardConfigCommand(template_id="template-1"),
            "deactivate",
        ),
    ),
)
async def test_rest_default_config_diff_denies_before_mutation(
    use_case: Any,
    command: Any,
    preview_name: str,
) -> None:
    service = _DefaultConfigSpy(
        create_diff=_CHANGED,
        activate_diff=_CHANGED,
        deactivate_diff=_CHANGED,
    )
    uow = _Uow(service)

    with pytest.raises(
        PermissionDeniedError,
        match="default_board_config.guidelines.edit",
    ):
        await use_case.execute(command, actor=_actor(), uow=uow)

    assert service.preview_calls == [preview_name]
    assert service.mutations == []
    assert uow.commit_calls == 0


@pytest.mark.parametrize(
    ("use_case", "command", "preview_name"),
    (
        (
            McpCreateDefaultBoardConfigVersionUseCase(),
            McpCreateDefaultBoardConfigVersionCommand(
                settings_payload={},
                scope="global",
                guideline_default_refs=[{"guideline_id": "guideline-1"}],
                design_system_default_ref=None,
                activate=False,
            ),
            "create",
        ),
        (
            McpActivateDefaultBoardConfigVersionUseCase(),
            McpActivateDefaultBoardConfigVersionCommand("template-1"),
            "activate",
        ),
        (
            McpDeactivateDefaultBoardConfigVersionUseCase(),
            McpDeactivateDefaultBoardConfigVersionCommand("template-1"),
            "deactivate",
        ),
    ),
)
async def test_mcp_default_config_diff_denies_before_mutation(
    use_case: Any,
    command: Any,
    preview_name: str,
) -> None:
    service = _DefaultConfigSpy(
        create_diff=_CHANGED,
        activate_diff=_CHANGED,
        deactivate_diff=_CHANGED,
    )
    uow = _Uow(service)

    with pytest.raises(
        PermissionDeniedError,
        match="default_board_config.guidelines.edit",
    ):
        await use_case.execute(command, actor=_actor(source="mcp"), uow=uow)

    assert service.preview_calls == [preview_name]
    assert service.mutations == []
    assert uow.commit_calls == 0


async def test_settings_only_equivalent_refs_do_not_require_adoption_manage() -> None:
    service = _DefaultConfigSpy(create_diff=_UNCHANGED)
    uow = _Uow(service)

    await CreateDefaultBoardConfigVersionUseCase().execute(
        DefaultBoardConfigCommand(
            payload={
                "scope": "global",
                "settings_payload": {"name": "settings-only"},
                "guideline_default_refs": [],
            }
        ),
        actor=_actor(),
        uow=uow,
    )

    assert service.mutations == ["create"]
    assert uow.commit_calls == 1


@pytest.mark.parametrize(
    ("use_case", "command", "source"),
    (
        (
            UpdateDefaultGuidelineRefsUseCase(),
            DefaultBoardConfigCommand(
                template_id="template-1",
                payload={"guideline_default_refs": []},
            ),
            "rest",
        ),
        (
            McpUpdateDefaultGuidelineRefsUseCase(),
            McpUpdateDefaultGuidelineRefsCommand(
                "board-1",
                template_id="template-1",
                guideline_default_refs=[],
            ),
            "mcp",
        ),
    ),
)
async def test_explicit_default_guideline_update_always_requires_adoption_manage(
    use_case: Any,
    command: Any,
    source: str,
) -> None:
    service = _DefaultConfigSpy()
    uow = _Uow(service)

    with pytest.raises(
        PermissionDeniedError,
        match="default_board_config.guidelines.edit",
    ):
        await use_case.execute(command, actor=_actor(source=source), uow=uow)

    assert service.preview_calls == []
    assert service.mutations == []


async def test_import_preflights_every_ref_delta_before_first_staged_create() -> None:
    service = _DefaultConfigSpy(create_diff=_CHANGED)
    uow = _Uow(service)

    with pytest.raises(
        PermissionDeniedError,
        match="default_board_config.guidelines.edit",
    ):
        await ImportBoardConfigUseCase().execute(
            ImportBoardConfigCommand(
                items=[
                    {
                        "scope": "global",
                        "settings_payload": {},
                        "guideline_default_refs": [{"guideline_id": "guideline-1"}],
                    }
                ]
            ),
            actor=_actor(),
            uow=uow,
        )

    assert service.preview_calls == ["create"]
    assert service.mutations == []
    assert uow.commit_calls == 0


async def test_adoption_grant_allows_changed_refs_and_commits() -> None:
    service = _DefaultConfigSpy(create_diff=_CHANGED)
    uow = _Uow(service)

    await CreateDefaultBoardConfigVersionUseCase().execute(
        DefaultBoardConfigCommand(
            payload={
                "scope": "global",
                "settings_payload": {},
                "guideline_default_refs": [{"guideline_id": "guideline-1"}],
            }
        ),
        actor=_actor(granted=True),
        uow=uow,
    )

    assert service.mutations == ["create"]
    assert uow.commit_calls == 1


async def test_mcp_explicit_ref_update_denial_is_json_and_precedes_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server

    calls = {"provider": 0}

    async def get_agent(_board_id: str) -> object:
        return SimpleNamespace(
            agent_id="agent-1",
            agent_name="Agent",
            realm_id=LOCAL_REALM_ID,
            permissions=["spec.entity.edit_fields"],
        )

    service = _DefaultConfigSpy()
    uow = _Uow(service)

    class _Manager:
        async def __aenter__(self) -> _Uow:
            return uow

        async def __aexit__(self, *_exc: object) -> None:
            return None

    def provider() -> object:
        calls["provider"] += 1
        return lambda *, actor: _Manager()

    monkeypatch.setattr(server, "_get_agent_ctx", get_agent)
    monkeypatch.setattr(server, "check_permission", lambda *_args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        provider,
    )

    raw = await server.mcp._tool_manager._tools[
        "okto_pulse_update_default_guideline_refs"
    ].fn(
        board_id="board-1",
        template_id="template-1",
        guideline_default_refs=[],
    )

    payload = json.loads(raw)
    assert "default_board_config.guidelines.edit" in payload["error"]
    assert calls == {"provider": 1}
    assert service.mutations == []
    assert uow.commit_calls == 0
