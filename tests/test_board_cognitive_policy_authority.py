"""D17: an authenticated executor cannot disable its own cognitive policy."""
# ruff: noqa: F811 -- imported pytest fixture is injected by argument name.
import pytest

from test_r01a_boards_uow import PREFIX, USER, _seed_board, client  # noqa: F401
from test_default_board_config_api import _isolate_committed_global_templates  # noqa: F401
from okto_pulse.community.api.auth_deps import require_principal
from okto_pulse.core.domain.permissions import PERMISSION_REGISTRY
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.ports.authentication import Principal
from sqlalchemy_test_models import Board
from sqlalchemy import select


@pytest.mark.asyncio
@pytest.mark.parametrize("actor_kind", ["agent", "unknown"])
@pytest.mark.parametrize("previous,patch", [
    ({}, {"skip_cognitive_consolidation": True}),
    ({"skip_cognitive_consolidation": True}, {"skip_cognitive_consolidation": False}),
    ({"skip_cognitive_consolidation": True}, None),
    ({"cognitive_readiness_policy": "blocking"}, None),
])
async def test_executor_cannot_change_board_cognitive_policy(client, actor_kind, previous, patch):
    board_id = await _seed_board()
    async with get_session_factory()() as db:
        board = await db.get(Board, board_id)
        board.settings = previous
        await db.commit()
    client.app.dependency_overrides[require_principal] = lambda: Principal(
        subject=USER, realm_id=LOCAL_REALM_ID, actor_kind=actor_kind,
        claims={"permissions": PERMISSION_REGISTRY},
    )
    response = client.patch(
        f"{PREFIX}/{board_id}",
        json={"settings": patch},
    )
    assert response.status_code == 403, response.text
    async with get_session_factory()() as db:
        board = await db.get(Board, board_id)
        assert board.settings == previous


@pytest.mark.asyncio
@pytest.mark.parametrize("actor_kind,patch", [
    ("human", {"skip_cognitive_consolidation": True}),
    ("agent", {"max_scenarios_per_card": 4}),
    ("agent", {"skip_cognitive_consolidation": False}),
])
async def test_human_authoring_and_executor_non_policy_edits_remain_available(client, actor_kind, patch):
    board_id = await _seed_board()
    client.app.dependency_overrides[require_principal] = lambda: Principal(
        subject=USER, realm_id=LOCAL_REALM_ID, actor_kind=actor_kind,
        claims={"permissions": PERMISSION_REGISTRY},
    )
    response = client.patch(f"{PREFIX}/{board_id}", json={"settings": patch})
    assert response.status_code == 200, response.text
    async with get_session_factory()() as db:
        board = await db.get(Board, board_id)
        for key, value in patch.items():
            assert board.settings[key] == value


@pytest.mark.asyncio
@pytest.mark.parametrize("skip", [True, False])
async def test_executor_cannot_author_cognitive_policy_during_board_creation(client, skip):
    client.app.dependency_overrides[require_principal] = lambda: Principal(
        subject=USER, realm_id=LOCAL_REALM_ID, actor_kind="agent",
        claims={"permissions": PERMISSION_REGISTRY},
    )
    name = f"denied-agent-cognitive-policy-{skip}"
    response = client.post(PREFIX, json={
        "name": name, "settings": {"skip_cognitive_consolidation": skip},
    })
    assert response.status_code == 403, response.text
    async with get_session_factory()() as db:
        assert (await db.execute(select(Board.id).where(Board.name == name))).first() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("actor_kind,settings,expected_skip", [
    ("human", {"skip_cognitive_consolidation": True}, True),
    ("agent", {"max_scenarios_per_card": 4}, False),
])
async def test_board_creation_retains_human_policy_and_executor_other_settings(client, actor_kind, settings, expected_skip):
    client.app.dependency_overrides[require_principal] = lambda: Principal(
        subject=USER, realm_id=LOCAL_REALM_ID, actor_kind=actor_kind,
        claims={"permissions": PERMISSION_REGISTRY},
    )
    response = client.post(PREFIX, json={"name": "allowed-policy-creation", "settings": settings})
    assert response.status_code == 201, response.text
    async with get_session_factory()() as db:
        board = await db.get(Board, response.json()["id"])
        assert board.settings["skip_cognitive_consolidation"] is expected_skip


@pytest.mark.asyncio
async def test_executor_inherits_existing_template_cognitive_policy_without_override(client):
    from okto_pulse.core.services.default_board_configuration import DefaultBoardConfigurationService

    async with get_session_factory()() as db:
        template = await DefaultBoardConfigurationService(db).create_version(
            actor=USER, actor_kind="human", settings_payload={"skip_cognitive_consolidation": True},
            activate=True,
        )
        template_id = template.id
        await db.commit()
    client.app.dependency_overrides[require_principal] = lambda: Principal(
        subject=USER, realm_id=LOCAL_REALM_ID, actor_kind="agent",
        claims={"permissions": PERMISSION_REGISTRY},
    )
    response = client.post(PREFIX, json={"name": "inherited-human-policy"})
    assert response.status_code == 201, response.text
    async with get_session_factory()() as db:
        board = await db.get(Board, response.json()["id"])
        assert board.settings["skip_cognitive_consolidation"] is True
        assert board.default_config_snapshot["template_id"] == template_id
