"""Existing policy authority survives unrelated public settings edits."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from okto_pulse.core.models.schemas import BoardSettings, BoardUpdate
from okto_pulse.core.services.board_governance import BoardGovernanceService
from okto_pulse.core.services.main import _board_cognitive_readiness_policy


@pytest.mark.parametrize("policy", ["blocking", "advisory", "BLOCKING", "unknown", None])
def test_unrelated_typed_patch_preserves_exact_policy_and_effective_gate(policy):
    current = {"cognitive_readiness_policy": policy, "skip_cognitive_consolidation": False}
    before = deepcopy(current)
    patch = BoardUpdate(settings=BoardSettings(max_scenarios_per_card=4)).settings
    merged = BoardGovernanceService.merge_settings_patch(current, patch)
    assert merged["cognitive_readiness_policy"] == policy
    assert _board_cognitive_readiness_policy(SimpleNamespace(settings=merged)) == (
        _board_cognitive_readiness_policy(SimpleNamespace(settings=current))
    )
    assert merged["skip_cognitive_consolidation"] is False
    assert merged["max_scenarios_per_card"] == 4
    assert current == before


@pytest.mark.parametrize("attempt", ["advisory", "blocking", None])
def test_unsupported_patch_does_not_author_policy(attempt):
    patch = {"cognitive_readiness_policy": attempt, "max_scenarios_per_card": 4}
    existing = BoardGovernanceService.merge_settings_patch(
        {"cognitive_readiness_policy": "blocking"}, patch,
    )
    assert existing["cognitive_readiness_policy"] == "blocking"
    fresh = BoardGovernanceService.merge_settings_patch({}, patch)
    assert "cognitive_readiness_policy" not in fresh
    assert "cognitive_readiness_policy" not in BoardSettings.model_fields


@pytest.mark.asyncio
async def test_board_service_roundtrip_keeps_existing_blocking_policy(db_factory):
    from sqlalchemy_test_models import Board
    from okto_pulse.core.services.main import BoardService
    from test_board_governance_settings import USER_ID, _create_board

    async with db_factory() as db:
        board = await _create_board(db, settings={
            "cognitive_readiness_policy": "blocking",
            "skip_cognitive_consolidation": False,
        })
        board_id = board.id
        await db.commit()
    async with db_factory() as db:
        updated = await BoardService(db).update_board(
            board_id, USER_ID, BoardUpdate.model_validate({
                "settings": {"max_scenarios_per_card": 4},
            }),
        )
        assert updated is not None
        await db.commit()
    async with db_factory() as db:
        stored = await db.get(Board, board_id)
        assert stored.settings["cognitive_readiness_policy"] == "blocking"
        assert stored.settings["skip_cognitive_consolidation"] is False
        assert stored.settings["max_scenarios_per_card"] == 4
        assert _board_cognitive_readiness_policy(stored) == "blocking"
