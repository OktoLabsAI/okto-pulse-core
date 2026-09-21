"""Retired workflow actions cannot return through generic guard/write helpers."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.mutation_permissions import card_update_permission_requirements
from okto_pulse.core.services.board_governance import BoardGovernanceService
from okto_pulse.core.services.critical_context_guard import (
    FullContextCriticalActionGuard, build_default_full_context_resolvers,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["move_status", "submit_evaluation", "closeout", "cancel", "archive"])
async def test_retired_action_is_refused_before_board_policy_or_custom_resolver(monkeypatch, action):
    policy = AsyncMock(side_effect=AssertionError("Retired action reached policy"))
    resolver = SimpleNamespace(resolve_full_context=AsyncMock(side_effect=AssertionError("Retired action resolved")))
    monkeypatch.setattr(BoardGovernanceService, "resolve", policy)
    assert "sprint" not in build_default_full_context_resolvers(object())
    guard = FullContextCriticalActionGuard(object(), resolvers={"sprint": resolver})
    with pytest.raises(ValueError, match="unsupported_critical_action"):
        await guard.authorize_and_resolve(board_id="board", actor_id="actor", entity_type="sprint",
                                         entity_id="historical", critical_action=f"sprint.{action}")
    policy.assert_not_awaited()
    resolver.resolve_full_context.assert_not_awaited()


@pytest.mark.parametrize("value", [None, "historical"])
@pytest.mark.parametrize("typed", [False, True])
def test_retired_link_is_not_reclassified_as_assignment_or_content(value, typed):
    data = {"sprint_id": value, "assignee_id": "executor"}
    if typed:
        data = SimpleNamespace(**data, model_fields_set=set(data))
    with pytest.raises(ValueError, match="sprint_id_retired"):
        card_update_permission_requirements(data)


def test_real_assignment_still_requires_its_own_permission():
    requirements = card_update_permission_requirements({"assignee_id": "executor"})
    assert [item.operation for item in requirements] == ["card.entity.assign"]
