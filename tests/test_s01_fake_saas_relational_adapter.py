"""S01: Core application flows run against a SaaS-shaped port adapter."""

from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.permission_presets import (
    ClonePermissionPresetCommand,
    ClonePermissionPresetUseCase,
    CreatePermissionPresetCommand,
    CreatePermissionPresetUseCase,
    GetMyPermissionsCommand,
    GetMyPermissionsUseCase,
    ListPermissionPresetsCommand,
    ListPermissionPresetsUseCase,
)
from okto_pulse.core.ports.relational_application import (
    RelationalApplicationAdapter,
    register_relational_application_adapter,
    reset_relational_application_adapter_for_tests,
)
from okto_pulse.core.ports.quality_assessment import (
    QualityAssessmentAdapterMissing,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyAdapterMissing,
)
from okto_pulse.core.services.application_agents import (
    agent_has_board_access,
    authenticate_agent_by_api_key,
    list_accessible_board_ids_for_agent,
    resolve_agent_permission_context,
)
from okto_pulse.core.testing import FakeSaaSRelationalApplicationAdapter


class _OpaqueSaaSUow:
    def __init__(self, adapter: FakeSaaSRelationalApplicationAdapter) -> None:
        async def _get_board(board_id: str):
            return SimpleNamespace(id=board_id, owner_id="saas-agent")

        self.boards = SimpleNamespace(get=_get_board)
        self.services = type(
            "FakeServices",
            (),
            {"permission_presets": adapter.permission_presets(None)},
        )()
        self.commit_calls = 0

    async def commit(self) -> None:
        self.commit_calls += 1


def test_s01_saas_adapter_exposes_quality_assessment_seam_fail_closed() -> None:
    adapter = FakeSaaSRelationalApplicationAdapter()
    assert isinstance(adapter, RelationalApplicationAdapter)
    with pytest.raises(QualityAssessmentAdapterMissing):
        adapter.quality_assessments(object())
    with pytest.raises(GuidelinePolicyAdapterMissing):
        adapter.guideline_policy(object())


@pytest.mark.asyncio
async def test_s01_core_use_cases_run_unchanged_against_a_saas_adapter() -> None:
    adapter = FakeSaaSRelationalApplicationAdapter()
    adapter.add_agent(
        agent_id="saas-agent",
        name="SaaS Agent",
        api_key="saas-secret",
        board_ids={"tenant-board"},
        permissions={"boards": {"read": True}},
    )
    uow = _OpaqueSaaSUow(adapter)
    actor = ActorContext("saas-agent", "system", board_id="tenant-board")

    reset_relational_application_adapter_for_tests()
    register_relational_application_adapter(adapter)
    try:
        created = await CreatePermissionPresetUseCase().execute(
            CreatePermissionPresetCommand(
                name="Tenant Reader",
                description="SaaS tenant preset",
                flags={"boards": {"read": True}},
            ),
            actor=actor,
            uow=uow,
        )
        cloned = await ClonePermissionPresetUseCase().execute(
            ClonePermissionPresetCommand(
                preset_id=created.preset.id,
                name="Tenant Reader Clone",
                description="",
                flags={"boards": {"write": False}},
            ),
            actor=actor,
            uow=uow,
        )
        listed = await ListPermissionPresetsUseCase().execute(
            ListPermissionPresetsCommand(), actor=actor, uow=uow
        )
        effective = await GetMyPermissionsUseCase().execute(
            GetMyPermissionsCommand(board_id="tenant-board"), actor=actor, uow=uow
        )

        adapter_context = object()
        authenticated = await authenticate_agent_by_api_key(
            adapter_context, "saas-secret", credential_source="saas_gateway"
        )
        context = await resolve_agent_permission_context(
            adapter_context, "saas-agent", board_id="tenant-board"
        )
        board_ids = await list_accessible_board_ids_for_agent(
            adapter_context, "saas-agent"
        )
        has_access = await agent_has_board_access(
            adapter_context, "saas-agent", "tenant-board"
        )
    finally:
        reset_relational_application_adapter_for_tests()

    assert created.preset.owner_id == "saas-agent"
    assert cloned.preset.base_preset_id == created.preset.id
    assert {preset.id for preset in listed.presets} >= {
        created.preset.id,
        cloned.preset.id,
    }
    assert effective.permissions.board_id == "tenant-board"
    assert uow.commit_calls == 2
    assert authenticated is not None and authenticated.agent_id == "saas-agent"
    assert context is not None and context.agent_name == "SaaS Agent"
    assert board_ids == ["tenant-board"]
    assert has_access is True


def test_s01_saas_fake_has_no_local_first_or_orm_dependency() -> None:
    source_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "testing"
        / "fake_saas_relational.py"
    )
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    modules.update(
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    )

    assert not any(module.startswith("sqlalchemy") for module in modules)
    assert not any(module.startswith("okto_pulse.community") for module in modules)
    assert "okto_pulse.core.models" not in modules
