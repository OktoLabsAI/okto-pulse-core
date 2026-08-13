"""Direct use-case regressions for Card/Sprint central authorization."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.card_crud import (
    UpdateCardCommand,
    UpdateCardUseCase,
)
from okto_pulse.core.application.use_cases.mcp_card_crud import (
    McpUpdateCardCommand,
    McpUpdateCardUseCase,
)
from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
    McpCopyMockupsToCardCommand,
    McpCopyMockupsToCardUseCase,
)
from okto_pulse.core.application.use_cases.mutation_permissions import (
    card_create_permission_requirement,
    card_update_permission_requirements,
    sprint_update_permission_requirements,
)
from okto_pulse.core.application.use_cases.sprints_crud import (
    AssignSprintTasksCommand,
    AssignSprintTasksUseCase,
    SubmitSprintEvaluationCommand,
    SubmitSprintEvaluationUseCase,
)
from okto_pulse.core.application.use_cases.submit_spec_validation import (
    SubmitSpecValidationCommand,
    SubmitSpecValidationUseCase,
)
from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.domain.enums import CardType


BOARD_ID = "board-1"


def test_test_card_enum_uses_specific_create_permission():
    requirement = card_create_permission_requirement(
        SimpleNamespace(card_type=CardType.TEST)
    )

    assert requirement.operation == "card.entity.create_test"


def test_test_card_mapping_uses_specific_create_permission():
    requirement = card_create_permission_requirement({"card_type": "test"})

    assert requirement.operation == "card.entity.create_test"


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"title": "New"}, {"card.entity.edit_fields"}),
        ({"assignee_id": "agent-2"}, {"card.entity.assign"}),
        ({"sprint_id": "sprint-2"}, {"card.entity.assign"}),
        ({"labels": ["urgent"]}, {"card.entity.label"}),
        ({"spec_id": "spec-2"}, {"card.entity.link_spec"}),
        ({"test_scenario_ids": ["ts-1"]}, {"card.entity.link_tests"}),
        ({"linked_test_task_ids": ["card-2"]}, {"card.entity.link_tests"}),
        ({"severity": "major"}, {"card.entity.edit_bug_fields"}),
        (
            {"title": "New", "labels": ["urgent"]},
            {"card.entity.edit_fields", "card.entity.label"},
        ),
    ],
)
def test_card_update_fields_map_to_canonical_permissions(payload, expected):
    requirements = card_update_permission_requirements(payload, state="not_started")

    assert {requirement.operation for requirement in requirements} == expected


@pytest.mark.parametrize(
    "field",
    [
        "skip_test_coverage",
        "skip_rules_coverage",
        "skip_qualitative_validation",
        "validation_threshold",
        "require_task_validation",
        "validation_min_confidence",
        "validation_min_completeness",
        "validation_max_drift",
    ],
)
def test_sprint_validation_configuration_uses_coverage_permission(field: str):
    requirements = sprint_update_permission_requirements(
        {field: 1, "expected_version": 4},
        state="active",
    )

    assert [requirement.operation for requirement in requirements] == [
        "sprint.entity.edit_coverage_flags"
    ]


def test_sprint_optimistic_lock_metadata_does_not_add_a_permission():
    assert (
        sprint_update_permission_requirements(
            {"expected_version": 4},
            state="active",
        )
        == ()
    )


class _Boards:
    async def get(self, board_id: str):
        return SimpleNamespace(id=board_id, owner_id="actor-1", realm_id=None)


class _Services:
    def __init__(self, permissions, **services):
        self.permissions = permissions
        self.permission_calls: list[tuple[str, str]] = []
        for name, service in services.items():
            setattr(self, name, service)
        self.shares = SimpleNamespace()

    async def resolve_user_permissions(self, actor_id: str, board_id: str):
        self.permission_calls.append((actor_id, board_id))
        return self.permissions


class _Uow:
    def __init__(self, services: _Services):
        self.services = services
        self.boards = _Boards()
        self.commits = 0

    async def commit(self) -> None:
        self.commits += 1


class _Cards:
    def __init__(self):
        self.card = SimpleNamespace(
            id="card-1",
            board_id=BOARD_ID,
            status="not_started",
            screen_mockups=[],
        )
        self.update_calls = 0

    async def get_card(self, _card_id: str):
        return self.card

    async def update_card(self, *_args, **_kwargs):
        self.update_calls += 1
        return self.card


class _Sprints:
    def __init__(self):
        self.sprint = SimpleNamespace(
            id="sprint-1",
            board_id=BOARD_ID,
            status="active",
        )
        self.assign_calls = 0
        self.evaluation_calls = 0

    async def get_sprint(self, _sprint_id: str):
        return self.sprint

    async def assign_tasks(self, *_args):
        self.assign_calls += 1
        return 1

    async def submit_evaluation(self, *_args):
        self.evaluation_calls += 1
        return self.sprint


def _permission_set(entity: str, operation: tuple[str, ...], *, allowed: bool):
    leaf: dict[str, object] = {}
    current = leaf
    for part in operation[:-1]:
        child: dict[str, object] = {}
        current[part] = child
        current = child
    current[operation[-1]] = allowed
    entity_permissions = leaf.setdefault(entity, {})
    assert isinstance(entity_permissions, dict)
    entity_permissions["interact_in"] = {
        "not_started": True,
        "active": True,
        "approved": True,
    }
    return PermissionSet(leaf)


@pytest.mark.asyncio
async def test_rest_card_update_resolves_board_permissions_and_denies_before_writer():
    cards = _Cards()
    services = _Services(
        _permission_set("card", ("card", "entity", "edit_fields"), allowed=False),
        cards=cards,
    )
    uow = _Uow(services)
    actor = ActorContext("actor-1", "rest")

    with pytest.raises(PermissionDeniedError):
        await UpdateCardUseCase().execute(
            UpdateCardCommand("card-1", {"title": "blocked"}),
            actor=actor,
            uow=uow,
        )

    assert services.permission_calls == [("actor-1", BOARD_ID)]
    assert cards.update_calls == 0
    assert uow.commits == 0


@pytest.mark.asyncio
async def test_mcp_card_update_granular_false_denies_before_writer():
    cards = _Cards()
    uow = _Uow(
        _Services(
            _permission_set("card", ("card", "entity", "edit_fields"), allowed=False),
            cards=cards,
        )
    )
    actor = ActorContext(
        "actor-1",
        "mcp",
        board_id=BOARD_ID,
        permissions=uow.services.permissions,
    )

    with pytest.raises(PermissionDeniedError):
        await McpUpdateCardUseCase().execute(
            McpUpdateCardCommand("card-1", BOARD_ID, {"title": "blocked"}, {}),
            actor=actor,
            uow=uow,
        )

    assert cards.update_calls == 0
    assert uow.commits == 0


@pytest.mark.asyncio
async def test_mcp_card_update_legacy_list_still_authorizes():
    cards = _Cards()
    uow = _Uow(_Services(None, cards=cards))
    actor = ActorContext(
        "actor-1",
        "mcp",
        board_id=BOARD_ID,
        permissions=["cards:update"],
    )

    await McpUpdateCardUseCase().execute(
        McpUpdateCardCommand("card-1", BOARD_ID, {"title": "allowed"}, {}),
        actor=actor,
        uow=uow,
    )

    assert cards.update_calls == 1
    assert uow.commits == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["assign", "evaluation"])
async def test_sprint_writes_deny_before_writer(operation: str):
    sprints = _Sprints()
    permission = (
        ("sprint", "entity", "assign")
        if operation == "assign"
        else ("sprint", "evaluations", "submit")
    )
    permissions = _permission_set("sprint", permission, allowed=False)
    uow = _Uow(_Services(permissions, sprints=sprints))
    actor = ActorContext("actor-1", "mcp", board_id=BOARD_ID, permissions=permissions)

    with pytest.raises(PermissionDeniedError):
        if operation == "assign":
            await AssignSprintTasksUseCase().execute(
                AssignSprintTasksCommand("sprint-1", ["card-1"]),
                actor=actor,
                uow=uow,
            )
        else:
            await SubmitSprintEvaluationUseCase().execute(
                SubmitSprintEvaluationCommand("sprint-1", {"score": 10}),
                actor=actor,
                uow=uow,
            )

    assert sprints.assign_calls == 0
    assert sprints.evaluation_calls == 0
    assert uow.commits == 0


@pytest.mark.asyncio
async def test_copy_permission_denies_before_card_writer():
    cards = _Cards()
    permissions = _permission_set(
        "card", ("card", "copy_from_spec", "mockups"), allowed=False
    )
    specs = SimpleNamespace(
        get_spec=lambda _spec_id: None,
    )

    async def get_spec(_spec_id: str):
        return SimpleNamespace(id="spec-1", board_id=BOARD_ID, screen_mockups=[])

    specs.get_spec = get_spec
    uow = _Uow(_Services(permissions, cards=cards, specs=specs))
    actor = ActorContext("actor-1", "mcp", board_id=BOARD_ID, permissions=permissions)

    with pytest.raises(PermissionDeniedError):
        await McpCopyMockupsToCardUseCase().execute(
            McpCopyMockupsToCardCommand(BOARD_ID, "spec-1", "card-1", None),
            actor=actor,
            uow=uow,
        )

    assert cards.update_calls == 0
    assert uow.commits == 0


@pytest.mark.asyncio
async def test_spec_validation_permission_denies_before_writer():
    class Specs:
        submit_calls = 0

        async def get_spec(self, _spec_id: str):
            return SimpleNamespace(id="spec-1", board_id=BOARD_ID, status="approved")

        async def submit_spec_validation(self, **_kwargs):
            self.submit_calls += 1
            return {}

    specs = Specs()
    permissions = _permission_set(
        "spec", ("spec", "validation", "submit"), allowed=False
    )
    uow = _Uow(_Services(permissions, specs=specs))
    actor = ActorContext("actor-1", "mcp", board_id=BOARD_ID, permissions=permissions)
    data = {
        "expected_validation_edition": 1,
        "expected_spec_version": 1,
        "expected_head_revision": 0,
        "confidence": 90,
        "confidence_justification": "assessment evidence is comprehensive",
        "clarity": 90,
        "clarity_justification": "problem and solution are explicit",
        "assertiveness": 90,
        "assertiveness_justification": "assertive enough",
        "decidability": 90,
        "decidability_justification": "requirements direct concrete choices",
        "ambiguity": 10,
        "ambiguity_justification": "ambiguity is low",
        "recommendation": "approve",
    }

    with pytest.raises(PermissionDeniedError):
        await SubmitSpecValidationUseCase().execute(
            SubmitSpecValidationCommand("spec-1", data),
            actor=actor,
            uow=uow,
        )

    assert specs.submit_calls == 0
    assert uow.commits == 0
