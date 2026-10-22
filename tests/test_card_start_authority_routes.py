"""Starting a card preserves each type's effective pre-unification authority."""

import pytest

from okto_pulse.core.application.use_cases.authorization import (
    require_card_return_to_planning_authorization,
    require_card_start_authorization,
)
from okto_pulse.core.application.use_cases.base import ActorContext, PermissionDeniedError
from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.domain.sdlc_registry import is_transition_allowed


@pytest.mark.asyncio
@pytest.mark.parametrize("card_type", ["normal", "test", "bug"])
@pytest.mark.parametrize("preparation", [False, True])
@pytest.mark.parametrize("execution", [False, True])
@pytest.mark.parametrize("direct", [False, True])
async def test_start_requires_an_entire_authorized_route(
    card_type: str, preparation: bool, execution: bool, direct: bool
) -> None:
    permissions = PermissionSet(
        {
            "card": {
                "entity": {"read": True},
                "interact_in": {"not_started": True, "started": True},
                "move": {
                    "not_started_to_started": preparation,
                    "started_to_in_progress": execution,
                    "not_started_to_in_progress": direct,
                },
            }
        }
    )
    actor = ActorContext("actor", "mcp", board_id="board", permissions=permissions)
    expected = preparation and execution
    if card_type in {"test", "bug"}:
        expected = expected or direct

    if expected:
        decision = await require_card_start_authorization(
            actor, card_type=card_type, board_id="board"
        )
        assert decision.allowed
    else:
        with pytest.raises(PermissionDeniedError):
            await require_card_start_authorization(
                actor, card_type=card_type, board_id="board"
            )


@pytest.mark.asyncio
async def test_start_respects_state_denial_on_legacy_route() -> None:
    permissions = PermissionSet(
        {
            "card": {
                "entity": {"read": True},
                "interact_in": {"not_started": True, "started": False},
                "move": {
                    "not_started_to_started": True,
                    "started_to_in_progress": True,
                    "not_started_to_in_progress": False,
                },
            }
        }
    )
    actor = ActorContext("actor", "mcp", board_id="board", permissions=permissions)
    with pytest.raises(PermissionDeniedError):
        await require_card_start_authorization(
            actor, card_type="normal", board_id="board"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("pause_right", [False, True])
@pytest.mark.parametrize("planning_right", [False, True])
async def test_return_to_planning_needs_both_former_edges(
    pause_right: bool, planning_right: bool
) -> None:
    permissions = PermissionSet(
        {
            "card": {
                "entity": {"read": True},
                "interact_in": {"in_progress": True, "started": True},
                "move": {
                    "in_progress_to_not_started": True,
                    "in_progress_to_started": pause_right,
                    "started_to_not_started": planning_right,
                },
            }
        }
    )
    actor = ActorContext("actor", "mcp", board_id="board", permissions=permissions)
    if pause_right and planning_right:
        decision = await require_card_return_to_planning_authorization(
            actor, board_id="board"
        )
        assert decision.allowed
    else:
        with pytest.raises(PermissionDeniedError):
            await require_card_return_to_planning_authorization(
                actor, board_id="board"
            )


@pytest.mark.asyncio
async def test_return_to_planning_requires_materialized_new_capability() -> None:
    permissions = PermissionSet({
        "card": {
            "interact_in": {"in_progress": True, "started": True},
            "move": {
                "in_progress_to_started": True,
                "started_to_not_started": True,
                "in_progress_to_not_started": False,
            },
        },
    })
    actor = ActorContext("actor", "mcp", board_id="board", permissions=permissions)
    with pytest.raises(PermissionDeniedError):
        await require_card_return_to_planning_authorization(actor, board_id="board")


@pytest.mark.asyncio
async def test_migrated_normal_retains_its_former_started_exit() -> None:
    actor = ActorContext("actor", "mcp", board_id="board", permissions=PermissionSet({
        "card": {
            "entity": {"read": True},
            "interact_in": {"started": True},
            "move": {"started_to_not_started": True},
        },
    }))
    assert (await require_card_return_to_planning_authorization(
        actor, card_type="normal", legacy_started_eligible=True, board_id="board"
    )).allowed
    with pytest.raises(PermissionDeniedError):
        await require_card_return_to_planning_authorization(
            actor, card_type="normal", board_id="board"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("card_type", ["test", "bug"])
async def test_migrated_test_bug_retains_only_its_former_return_right(card_type: str) -> None:
    assert is_transition_allowed(
        "card", "in_progress", "not_started", card_type=card_type
    )
    permissions = PermissionSet({
        "card": {
            "entity": {"read": True},
            "interact_in": {"started": True},
            "move": {"started_to_not_started": True},
        },
    })
    actor = ActorContext("actor", "mcp", board_id="board", permissions=permissions)
    with pytest.raises(PermissionDeniedError, match="card_legacy_return_not_eligible"):
        await require_card_return_to_planning_authorization(
            actor, card_type=card_type, board_id="board"
        )
    assert (await require_card_return_to_planning_authorization(
        actor, card_type=card_type, legacy_started_eligible=True, board_id="board"
    )).allowed
    denied = ActorContext("actor", "mcp", board_id="board", permissions=PermissionSet({
        "card": {"entity": {"read": True}, "interact_in": {"started": True},
                 "move": {"started_to_not_started": False}},
    }))
    with pytest.raises(PermissionDeniedError):
        await require_card_return_to_planning_authorization(
            denied, card_type=card_type, legacy_started_eligible=True,
            board_id="board",
        )
