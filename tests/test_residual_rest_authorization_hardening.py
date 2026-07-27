"""Residual authorization oracles for Resource Gate, /me and agent grants."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.agents import router as agents_router
from okto_pulse.community.api.auth_deps import get_realm_id, require_user
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.me import router as me_router
from okto_pulse.community.api.resource_gate import router as resource_gate_router
from okto_pulse.core.application.use_cases.agent_crud import (
    GrantBoardAccessCommand,
    GrantBoardAccessUseCase,
    RevokeBoardAccessCommand,
    RevokeBoardAccessUseCase,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.operational_rest import (
    BoardNotFoundError,
    ClearResourceNotApplicableCommand,
    ClearResourceNotApplicableUseCase,
    GetResourceGateSummaryUseCase,
    GetSpecResourceTaskCoverageUseCase,
    MarkResourceNotApplicableCommand,
    MarkResourceNotApplicableUseCase,
    ResourceGateEntityCommand,
    ResourceGateTaskCoverageCommand,
    UpdateResourceGateBoardSettingsCommand,
    UpdateResourceGateBoardSettingsUseCase,
)
from okto_pulse.core.application.use_cases.permission_presets import (
    GetMyPermissionsCommand,
    GetMyPermissionsUseCase,
)
from okto_pulse.core.application.use_cases.update_board_overrides import (
    UpdateBoardOverridesCommand,
    UpdateBoardOverridesUseCase,
)


BOARD_A = "board-a"
BOARD_B = "board-b"
USER_A = "user-a"
USER_B = "user-b"
REALM_A = "realm-a"
REALM_B = "realm-b"


def _board(
    *,
    board_id: str = BOARD_A,
    owner_id: str = USER_A,
    realm_id: str = REALM_A,
):
    return SimpleNamespace(
        id=board_id,
        owner_id=owner_id,
        realm_id=realm_id,
        settings={},
    )


def _resource_uow(
    *,
    board=None,
    permission: str | None = None,
    entity_board_id: str = BOARD_A,
):
    entity = SimpleNamespace(id="child-a", board_id=entity_board_id)
    resource_gate = SimpleNamespace(
        is_spec_resource_task_coverage_required=MagicMock(return_value=True),
        validate_spec_resource_task_coverage=AsyncMock(
            return_value={"enabled": True}
        ),
        get_summary=AsyncMock(return_value={"ok": True}),
        get_effective_resources=AsyncMock(return_value={"ok": True}),
        mark_not_applicable=AsyncMock(return_value={"ok": True}),
        clear_not_applicable=AsyncMock(return_value={"ok": True}),
    )
    permission_gateway = SimpleNamespace(
        get_effective_permissions=AsyncMock(
            return_value=SimpleNamespace(
                board_id=BOARD_A,
                preset_name=None,
                flags={"board": {"read": True}},
            )
        )
    )
    agent_service = SimpleNamespace(
        get_agent=AsyncMock(
            return_value=SimpleNamespace(id="agent-a", created_by=USER_A)
        ),
        agent_has_board_access=AsyncMock(return_value=False),
        grant_board_access=AsyncMock(
            return_value=SimpleNamespace(
                id="grant-a",
                agent_id="agent-a",
                board_id=BOARD_A,
                granted_by=USER_A,
                granted_at=datetime.now(timezone.utc),
                permission_overrides=None,
            )
        ),
        revoke_board_access=AsyncMock(return_value=True),
        update_board_overrides=AsyncMock(
            return_value=SimpleNamespace(
                id="grant-a",
                agent_id="agent-a",
                board_id=BOARD_A,
                granted_by=USER_A,
                granted_at=datetime.now(timezone.utc),
                permission_overrides={},
            )
        ),
    )
    services = SimpleNamespace(
        shares=SimpleNamespace(
            get_user_permission=AsyncMock(return_value=permission)
        ),
        ideations=SimpleNamespace(get_ideation=AsyncMock(return_value=entity)),
        refinements=SimpleNamespace(get_refinement=AsyncMock(return_value=entity)),
        specs=SimpleNamespace(get_spec=AsyncMock(return_value=entity)),
        cards=SimpleNamespace(get_card=AsyncMock(return_value=entity)),
        resource_gate=resource_gate,
        permission_presets=permission_gateway,
        agents=agent_service,
        update_resource_gate_board_settings=AsyncMock(
            return_value={"require_spec_resource_task_coverage": False}
        ),
    )
    return SimpleNamespace(
        boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=services,
        commit=AsyncMock(),
    )


def _actor(
    actor_id: str = USER_A,
    *,
    realm_id: str = REALM_A,
) -> ActorContext:
    return ActorContext(
        actor_id,
        "rest",
        board_id=BOARD_A,
        realm_id=realm_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owner_id", "permission"),
    [
        (USER_A, None),
        (USER_B, "viewer"),
        (USER_B, "editor"),
        (USER_B, "admin"),
    ],
    ids=["owner", "viewer", "editor", "board-admin"],
)
async def test_resource_gate_read_allows_owner_and_every_share(
    owner_id,
    permission,
) -> None:
    uow = _resource_uow(
        board=_board(owner_id=owner_id),
        permission=permission,
    )

    result = await GetResourceGateSummaryUseCase().execute(
        ResourceGateEntityCommand(BOARD_A, "spec", "child-a"),
        actor=_actor(),
        uow=uow,
    )

    assert result.data == {"ok": True}
    uow.services.specs.get_spec.assert_awaited_once_with("child-a")
    uow.services.resource_gate.get_summary.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("entity_type", "service_name", "method_name"),
    [
        ("ideation", "ideations", "get_ideation"),
        ("refinement", "refinements", "get_refinement"),
        ("spec", "specs", "get_spec"),
        ("card", "cards", "get_card"),
    ],
)
async def test_resource_gate_rejects_cross_board_child_before_gate_service(
    entity_type,
    service_name,
    method_name,
) -> None:
    uow = _resource_uow(
        board=_board(),
        entity_board_id=BOARD_B,
    )

    with pytest.raises(BoardNotFoundError):
        await GetResourceGateSummaryUseCase().execute(
            ResourceGateEntityCommand(BOARD_A, entity_type, "child-b"),
            actor=_actor(),
            uow=uow,
        )

    getattr(getattr(uow.services, service_name), method_name).assert_awaited_once()
    uow.services.resource_gate.get_summary.assert_not_awaited()


@pytest.mark.asyncio
async def test_spec_coverage_rejects_cross_board_spec_before_gate_service() -> None:
    uow = _resource_uow(
        board=_board(),
        entity_board_id=BOARD_B,
    )

    with pytest.raises(BoardNotFoundError):
        await GetSpecResourceTaskCoverageUseCase().execute(
            ResourceGateTaskCoverageCommand(BOARD_A, "child-b"),
            actor=_actor(),
            uow=uow,
        )

    uow.services.resource_gate.validate_spec_resource_task_coverage.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("permission", ["viewer", None], ids=["viewer", "no-share"])
async def test_resource_gate_write_denial_has_no_entity_call_mutation_or_commit(
    permission,
) -> None:
    uow = _resource_uow(
        board=_board(owner_id=USER_B),
        permission=permission,
    )

    with pytest.raises(BoardNotFoundError):
        await MarkResourceNotApplicableUseCase().execute(
            MarkResourceNotApplicableCommand(
                BOARD_A,
                "spec",
                "child-a",
                "architecture",
                "not applicable",
                "api",
            ),
            actor=_actor(),
            uow=uow,
        )

    uow.services.specs.get_spec.assert_not_awaited()
    uow.services.resource_gate.mark_not_applicable.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("permission", ["editor", "admin"])
async def test_resource_gate_editor_and_admin_share_can_write(permission) -> None:
    uow = _resource_uow(
        board=_board(owner_id=USER_B),
        permission=permission,
    )

    await ClearResourceNotApplicableUseCase().execute(
        ClearResourceNotApplicableCommand(
            BOARD_A,
            "spec",
            "child-a",
            "architecture",
            "now applicable",
        ),
        actor=_actor(),
        uow=uow,
    )

    uow.services.resource_gate.clear_not_applicable.assert_awaited_once()
    uow.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_resource_gate_settings_viewer_is_denied_without_mutation() -> None:
    uow = _resource_uow(
        board=_board(owner_id=USER_B),
        permission="viewer",
    )

    with pytest.raises(BoardNotFoundError):
        await UpdateResourceGateBoardSettingsUseCase().execute(
            UpdateResourceGateBoardSettingsCommand(BOARD_A, False),
            actor=_actor(),
            uow=uow,
        )

    uow.services.update_resource_gate_board_settings.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("permission", [None, "viewer", "editor"])
async def test_my_permissions_preflights_owner_or_share_before_gateway(
    permission,
) -> None:
    owner_id = USER_A if permission is None else USER_B
    uow = _resource_uow(
        board=_board(owner_id=owner_id),
        permission=permission,
    )

    await GetMyPermissionsUseCase().execute(
        GetMyPermissionsCommand(board_id=BOARD_A),
        actor=_actor(),
        uow=uow,
    )

    uow.services.permission_presets.get_effective_permissions.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "board",
    [None, _board(owner_id=USER_A, realm_id=REALM_B)],
    ids=["missing", "wrong-realm"],
)
async def test_my_permissions_denied_board_never_reaches_gateway(board) -> None:
    uow = _resource_uow(board=board)

    with pytest.raises(EntityNotFoundError):
        await GetMyPermissionsUseCase().execute(
            GetMyPermissionsCommand(board_id=BOARD_A),
            actor=_actor(),
            uow=uow,
        )

    uow.services.permission_presets.get_effective_permissions.assert_not_awaited()


async def _run_agent_board_operation(name: str, uow, *, actor=None):
    actor = actor or _actor()
    if name == "grant":
        return await GrantBoardAccessUseCase().execute(
            GrantBoardAccessCommand("agent-a", BOARD_A),
            actor=actor,
            uow=uow,
        )
    if name == "revoke":
        return await RevokeBoardAccessUseCase().execute(
            RevokeBoardAccessCommand("agent-a", BOARD_A),
            actor=actor,
            uow=uow,
        )
    return await UpdateBoardOverridesUseCase().execute(
        UpdateBoardOverridesCommand("agent-a", BOARD_A, {}),
        actor=actor,
        uow=uow,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["grant", "revoke", "overrides"])
@pytest.mark.parametrize("permission", ["viewer", "editor", "admin"])
async def test_agent_board_admin_rejects_all_shares_without_mutation(
    operation,
    permission,
) -> None:
    uow = _resource_uow(
        board=_board(owner_id=USER_B),
        permission=permission,
    )

    with pytest.raises(EntityNotFoundError) as exc_info:
        await _run_agent_board_operation(operation, uow)

    assert exc_info.value.entity_type == "board"
    uow.services.agents.agent_has_board_access.assert_not_awaited()
    uow.services.agents.grant_board_access.assert_not_awaited()
    uow.services.agents.revoke_board_access.assert_not_awaited()
    uow.services.agents.update_board_overrides.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["grant", "revoke", "overrides"])
async def test_agent_board_admin_owner_can_mutate(operation) -> None:
    uow = _resource_uow(board=_board())

    await _run_agent_board_operation(operation, uow)

    getattr(
        uow.services.agents,
        {
            "grant": "grant_board_access",
            "revoke": "revoke_board_access",
            "overrides": "update_board_overrides",
        }[operation],
    ).assert_awaited_once()
    uow.commit.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["grant", "revoke", "overrides"])
async def test_agent_board_admin_rejects_wrong_realm_without_mutation(operation) -> None:
    uow = _resource_uow(board=_board(realm_id=REALM_B))

    with pytest.raises(EntityNotFoundError) as exc_info:
        await _run_agent_board_operation(operation, uow)

    assert exc_info.value.entity_type == "board"
    uow.services.agents.grant_board_access.assert_not_awaited()
    uow.services.agents.revoke_board_access.assert_not_awaited()
    uow.services.agents.update_board_overrides.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_mcp_board_binding_does_not_impersonate_board_owner_for_grants() -> None:
    uow = _resource_uow(board=_board(owner_id=USER_B))
    mcp_actor = ActorContext(
        USER_A,
        "mcp",
        board_id=BOARD_A,
        realm_id=REALM_A,
    )

    with pytest.raises(EntityNotFoundError) as exc_info:
        await _run_agent_board_operation("grant", uow, actor=mcp_actor)

    assert exc_info.value.entity_type == "board"
    uow.services.agents.grant_board_access.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_foreign_agent_is_rejected_after_board_preflight_without_mutation() -> None:
    uow = _resource_uow(board=_board())
    uow.services.agents.get_agent.return_value = SimpleNamespace(
        id="agent-a",
        created_by=USER_B,
    )

    with pytest.raises(EntityNotFoundError) as exc_info:
        await _run_agent_board_operation("grant", uow)

    assert exc_info.value.entity_type == "agent"
    uow.boards.get.assert_awaited_once_with(BOARD_A)
    uow.services.agents.grant_board_access.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_inaccessible_board_is_rejected_before_agent_lookup() -> None:
    uow = _resource_uow(board=None)

    with pytest.raises(EntityNotFoundError) as exc_info:
        await _run_agent_board_operation("grant", uow)

    assert exc_info.value.entity_type == "board"
    uow.services.agents.get_agent.assert_not_awaited()
    uow.services.agents.grant_board_access.assert_not_awaited()
    uow.commit.assert_not_awaited()


def _client(uow, *, realm_id: str = REALM_A) -> TestClient:
    app = FastAPI()
    app.include_router(resource_gate_router, prefix="/api/v1")
    app.include_router(me_router, prefix="/api/v1")
    app.include_router(agents_router, prefix="/api/v1/agents")

    async def _override_uow():
        yield uow

    app.dependency_overrides[get_unit_of_work] = _override_uow
    app.dependency_overrides[require_user] = lambda: USER_A
    app.dependency_overrides[get_realm_id] = lambda: realm_id
    return TestClient(app)


@pytest.mark.parametrize("board", [None, _board(owner_id=USER_B)])
def test_rest_me_missing_and_foreign_board_have_identical_404(board) -> None:
    uow = _resource_uow(board=board)

    response = _client(uow).get(
        "/api/v1/me/permissions",
        params={"board_id": BOARD_A},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Board not found"}
    uow.services.permission_presets.get_effective_permissions.assert_not_awaited()


def test_rest_resource_gate_cross_board_child_is_non_enumerable() -> None:
    uow = _resource_uow(
        board=_board(),
        entity_board_id=BOARD_B,
    )

    response = _client(uow).get(
        "/api/v1/resource-gate/spec/child-b",
        params={"board_id": BOARD_A},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Board not found"}
    uow.services.resource_gate.get_summary.assert_not_awaited()


@pytest.mark.parametrize("board", [None, _board(owner_id=USER_B)])
def test_rest_resource_gate_write_missing_and_foreign_have_identical_404(
    board,
) -> None:
    uow = _resource_uow(board=board)

    response = _client(uow).post(
        "/api/v1/resource-gate/spec/child-a/not-applicable",
        params={"board_id": BOARD_A},
        json={
            "resource_type": "architecture",
            "source_channel": "api",
            "justification": "not applicable",
        },
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Board not found"}
    uow.services.specs.get_spec.assert_not_awaited()
    uow.services.resource_gate.mark_not_applicable.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.parametrize("operation", ["grant", "revoke", "overrides"])
@pytest.mark.parametrize("permission", ["viewer", "editor", "admin"])
def test_rest_agent_board_shares_cannot_admin_grants(operation, permission) -> None:
    uow = _resource_uow(
        board=_board(owner_id=USER_B),
        permission=permission,
    )
    method, payload = {
        "grant": ("POST", None),
        "revoke": ("DELETE", None),
        "overrides": ("PATCH", {"permission_overrides": {}}),
    }[operation]

    response = _client(uow).request(
        method,
        f"/api/v1/agents/agent-a/boards/{BOARD_A}",
        json=payload,
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Board not found"}
    uow.services.agents.grant_board_access.assert_not_awaited()
    uow.services.agents.revoke_board_access.assert_not_awaited()
    uow.services.agents.update_board_overrides.assert_not_awaited()
    uow.commit.assert_not_awaited()


def test_rest_update_overrides_owned_board_missing_grant_preserves_404() -> None:
    uow = _resource_uow(board=_board())
    uow.services.agents.update_board_overrides.return_value = None

    with patch("okto_pulse.core.mcp.invalidate_agent_cache") as invalidate:
        response = _client(uow).patch(
            f"/api/v1/agents/agent-a/boards/{BOARD_A}",
            json={"permission_overrides": {}},
        )

    assert response.status_code == 404
    assert response.json() == {"detail": "Board access not found"}
    uow.services.agents.update_board_overrides.assert_awaited_once()
    uow.commit.assert_not_awaited()
    invalidate.assert_not_called()


def test_rest_update_overrides_owned_agent_and_board_succeeds() -> None:
    uow = _resource_uow(board=_board())

    with patch("okto_pulse.core.mcp.invalidate_agent_cache") as invalidate:
        response = _client(uow).patch(
            f"/api/v1/agents/agent-a/boards/{BOARD_A}",
            json={"permission_overrides": {}},
        )

    assert response.status_code == 200, response.text
    assert response.json()["board_id"] == BOARD_A
    uow.services.agents.update_board_overrides.assert_awaited_once()
    uow.commit.assert_awaited_once()
    invalidate.assert_called_once_with("agent-a")
