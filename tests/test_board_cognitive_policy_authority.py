"""D17: an authenticated executor cannot disable its own cognitive policy."""
# ruff: noqa: F811 -- imported pytest fixture is injected by argument name.
import pytest

from test_r01a_boards_uow import PREFIX, USER, _seed_board, client  # noqa: F401
from okto_pulse.community.api.auth_deps import require_principal
from okto_pulse.core.domain.permissions import PERMISSION_REGISTRY
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.ports.authentication import Principal
from sqlalchemy_test_models import Board


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
