"""Direct authorization regressions for card validations and bug/test links."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.card_crud import (
    DeleteTaskValidationCommand,
    DeleteTaskValidationUseCase,
    LinkTestTaskToBugCommand,
    LinkTestTaskToBugUseCase,
    SubmitTaskValidationCommand,
    SubmitTaskValidationUseCase,
    UnlinkTestTaskFromBugCommand,
    UnlinkTestTaskFromBugUseCase,
)
from okto_pulse.core.domain.permissions import PermissionSet


BOARD_ID = "board-card-auth"
ACTOR_ID = "actor-card-auth"
CARD_ID = "card-validation"
BUG_ID = "bug-card"
TEST_TASK_ID = "test-task"
SPEC_ID = "spec-card-auth"
VALIDATION_ID = "validation-card-auth"

VALIDATION_DATA = {
    "confidence": 90,
    "confidence_justification": "high confidence",
    "estimated_completeness": 95,
    "completeness_justification": "nearly complete",
    "estimated_drift": 5,
    "drift_justification": "minimal drift",
    "general_justification": "validation evidence is complete",
    "recommendation": "approve",
}


class _Boards:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def get(self, board_id: str):
        self._events.append(f"boards:get:{board_id}")
        return SimpleNamespace(
            id=board_id,
            owner_id=ACTOR_ID,
            realm_id=None,
        )


class _Cards:
    def __init__(self, events: list[str], *, linked: bool) -> None:
        now = datetime(2026, 8, 7, tzinfo=timezone.utc)
        self.validation_card = SimpleNamespace(
            id=CARD_ID,
            board_id=BOARD_ID,
            status="validation",
        )
        self.bug = SimpleNamespace(
            id=BUG_ID,
            board_id=BOARD_ID,
            status="in_progress",
            card_type="bug",
            spec_id=SPEC_ID,
            created_at=now,
            test_scenario_ids=[],
            linked_test_task_ids=[TEST_TASK_ID] if linked else [],
        )
        self.test_task = SimpleNamespace(
            id=TEST_TASK_ID,
            board_id=BOARD_ID,
            status="done",
            card_type="test",
            spec_id=SPEC_ID,
            created_at=now + timedelta(minutes=1),
            test_scenario_ids=[],
        )
        self._cards = {
            CARD_ID: self.validation_card,
            BUG_ID: self.bug,
            TEST_TASK_ID: self.test_task,
        }
        self._events = events
        self.submit_calls = 0
        self.delete_calls = 0

    async def get_card(self, card_id: str):
        self._events.append(f"cards:get:{card_id}")
        return self._cards.get(card_id)

    async def get_task_validation(self, card_id: str, validation_id: str):
        self._events.append(f"validations:get:{card_id}:{validation_id}")
        if card_id == CARD_ID and validation_id == VALIDATION_ID:
            return {"id": VALIDATION_ID}
        return None

    async def submit_task_validation(self, **_kwargs):
        self._events.append("writer:submit_validation")
        self.submit_calls += 1
        return {"id": VALIDATION_ID}

    async def delete_task_validation(self, *_args):
        self._events.append("writer:delete_validation")
        self.delete_calls += 1
        return True


class _Specs:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def get_spec(self, spec_id: str):
        self._events.append(f"specs:get:{spec_id}")
        return SimpleNamespace(
            id=spec_id,
            board_id=BOARD_ID,
            test_scenarios=[],
        )


class _Services:
    def __init__(
        self,
        events: list[str],
        permissions,
        *,
        linked: bool,
    ) -> None:
        self._events = events
        self._permissions = permissions
        self.cards = _Cards(events, linked=linked)
        self.specs = _Specs(events)
        self.shares = SimpleNamespace()
        self.permission_calls: list[tuple[str, str]] = []
        self.actor_name_calls = 0

    async def resolve_user_permissions(self, actor_id: str, board_id: str):
        self._events.append(f"permissions:{actor_id}:{board_id}")
        self.permission_calls.append((actor_id, board_id))
        return self._permissions

    async def resolve_actor_name(self, actor_id: str, board_id: str):
        self._events.append(f"actor-name:{actor_id}:{board_id}")
        self.actor_name_calls += 1
        return "Card Auth Actor"


class _Uow:
    def __init__(self, permissions, *, linked: bool = False) -> None:
        self.events: list[str] = []
        self.services = _Services(
            self.events,
            permissions,
            linked=linked,
        )
        self.boards = _Boards(self.events)
        self.commits = 0

    async def commit(self) -> None:
        self.events.append("writer:commit")
        self.commits += 1


def _permission_set(
    operation: str,
    *,
    action_allowed: bool,
    state: str,
    state_allowed: bool = True,
) -> PermissionSet:
    flags: dict[str, object] = {
        "card": {"interact_in": {state: state_allowed}},
    }
    current = flags
    for part in operation.split(".")[:-1]:
        child = current.setdefault(part, {})
        assert isinstance(child, dict)
        current = child
    current[operation.rsplit(".", 1)[-1]] = action_allowed
    return PermissionSet(flags)


async def _execute(operation: str, *, actor: ActorContext, uow: _Uow) -> None:
    if operation == "submit":
        await SubmitTaskValidationUseCase().execute(
            SubmitTaskValidationCommand(CARD_ID, VALIDATION_DATA),
            actor=actor,
            uow=uow,
        )
    elif operation == "delete":
        await DeleteTaskValidationUseCase().execute(
            DeleteTaskValidationCommand(CARD_ID, VALIDATION_ID),
            actor=actor,
            uow=uow,
        )
    elif operation == "link":
        await LinkTestTaskToBugUseCase().execute(
            LinkTestTaskToBugCommand(BUG_ID, TEST_TASK_ID),
            actor=actor,
            uow=uow,
        )
    else:
        await UnlinkTestTaskFromBugUseCase().execute(
            UnlinkTestTaskFromBugCommand(BUG_ID, TEST_TASK_ID),
            actor=actor,
            uow=uow,
        )


_CASES = (
    ("submit", "card.validation.submit", "validation", False),
    ("delete", "card.validation.delete", "validation", True),
    ("link", "card.entity.link_tests", "in_progress", False),
    ("unlink", "card.entity.link_tests", "in_progress", True),
)

_PREFLIGHT_EVENTS = {
    "submit": (f"cards:get:{CARD_ID}", f"boards:get:{BOARD_ID}"),
    "delete": (
        f"cards:get:{CARD_ID}",
        f"boards:get:{BOARD_ID}",
        f"validations:get:{CARD_ID}:{VALIDATION_ID}",
    ),
    "link": (
        f"cards:get:{BUG_ID}",
        f"cards:get:{TEST_TASK_ID}",
        f"specs:get:{SPEC_ID}",
    ),
    "unlink": (f"cards:get:{BUG_ID}", f"cards:get:{TEST_TASK_ID}"),
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "permission", "state", "linked"),
    _CASES,
)
async def test_rest_denial_resolves_board_after_preflight_and_before_writer(
    case: str,
    permission: str,
    state: str,
    linked: bool,
) -> None:
    permissions = _permission_set(
        permission,
        action_allowed=False,
        state=state,
    )
    uow = _Uow(permissions, linked=linked)
    actor = ActorContext(ACTOR_ID, "rest")
    before_links = list(uow.services.cards.bug.linked_test_task_ids)

    with pytest.raises(PermissionDeniedError):
        await _execute(case, actor=actor, uow=uow)

    assert uow.services.permission_calls == [(ACTOR_ID, BOARD_ID)]
    permission_event = f"permissions:{ACTOR_ID}:{BOARD_ID}"
    permission_index = uow.events.index(permission_event)
    assert all(
        uow.events.index(event) < permission_index
        for event in _PREFLIGHT_EVENTS[case]
    )
    assert uow.services.cards.submit_calls == 0
    assert uow.services.cards.delete_calls == 0
    assert uow.services.cards.bug.linked_test_task_ids == before_links
    assert uow.services.actor_name_calls == 0
    assert uow.commits == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "permission", "state", "linked"),
    _CASES,
)
async def test_card_authorization_uses_the_entity_state(
    case: str,
    permission: str,
    state: str,
    linked: bool,
) -> None:
    permissions = _permission_set(
        permission,
        action_allowed=True,
        state=state,
        state_allowed=False,
    )
    uow = _Uow(permissions, linked=linked)
    actor = ActorContext(
        ACTOR_ID,
        "mcp",
        board_id=BOARD_ID,
        permissions=permissions,
    )

    with pytest.raises(PermissionDeniedError, match="interact_in_blocked"):
        await _execute(case, actor=actor, uow=uow)

    assert uow.services.cards.submit_calls == 0
    assert uow.services.cards.delete_calls == 0
    assert uow.commits == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("submit", "delete"))
async def test_validation_permissions_do_not_fall_back_to_cards_update(
    case: str,
) -> None:
    uow = _Uow(None)
    actor = ActorContext(
        ACTOR_ID,
        "mcp",
        board_id=BOARD_ID,
        permissions=["cards:update"],
    )

    with pytest.raises(PermissionDeniedError):
        await _execute(case, actor=actor, uow=uow)

    assert uow.services.cards.submit_calls == 0
    assert uow.services.cards.delete_calls == 0
    assert uow.commits == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(("case", "linked"), (("link", False), ("unlink", True)))
async def test_bug_test_links_keep_cards_update_legacy_compatibility(
    case: str,
    linked: bool,
) -> None:
    uow = _Uow(None, linked=linked)
    actor = ActorContext(
        ACTOR_ID,
        "mcp",
        board_id=BOARD_ID,
        permissions=["cards:update"],
    )

    await _execute(case, actor=actor, uow=uow)

    expected = [TEST_TASK_ID] if case == "link" else []
    assert uow.services.cards.bug.linked_test_task_ids == expected
    assert uow.commits == 1
