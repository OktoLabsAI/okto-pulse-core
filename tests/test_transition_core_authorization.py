"""Direct regressions for canonical authorization of lifecycle mutations."""

from __future__ import annotations

from collections import Counter
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.use_cases import (
    card_crud,
    mcp_card_crud,
    mcp_refinement_crud,
    mcp_resource_stories,
    mcp_spec_crud,
    move_ideation,
    refinements_crud,
    spec_crud,
    sprints_crud,
    stories_crud,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.domain.enums import (
    CardStatus,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
    SprintStatus,
    StoryStatus,
)


BOARD_ID = "board-1"


class _Boards:
    async def get(self, board_id: str) -> Any:
        return SimpleNamespace(
            id=board_id,
            owner_id="actor-1",
            realm_id=None,
            settings={},
        )


class _TransitionServices:
    def __init__(self) -> None:
        self.calls: Counter[str] = Counter()
        self.card = SimpleNamespace(
            id="card-1", board_id=BOARD_ID, status=CardStatus.NOT_STARTED
        )
        self.sprint = SimpleNamespace(
            id="sprint-1", board_id=BOARD_ID, status=SprintStatus.DRAFT
        )
        self.story = SimpleNamespace(
            id="story-1",
            board_id=BOARD_ID,
            status=StoryStatus.DRAFT,
            archived=False,
        )
        self.ideation = SimpleNamespace(
            id="ideation-1", board_id=BOARD_ID, status=IdeationStatus.DRAFT
        )
        self.refinement = SimpleNamespace(
            id="refinement-1",
            board_id=BOARD_ID,
            status=RefinementStatus.DRAFT,
        )
        self.spec = SimpleNamespace(
            id="spec-1",
            board_id=BOARD_ID,
            status=SpecStatus.DRAFT,
            test_scenarios=[{"id": "scenario-1", "status": "draft"}],
        )
        self.cards = self
        self.sprints = self
        self.stories = self
        self.ideations = self
        self.refinements = self
        self.specs = self
        self.shares = SimpleNamespace()

    async def get_card(self, _entity_id: str) -> Any:
        return self.card

    async def get_sprint(self, _entity_id: str) -> Any:
        return self.sprint

    async def get_story(self, _entity_id: str) -> Any:
        return self.story

    async def get_ideation(self, _entity_id: str) -> Any:
        return self.ideation

    async def get_refinement(self, _entity_id: str) -> Any:
        return self.refinement

    async def get_spec(self, _entity_id: str) -> Any:
        return self.spec

    async def move_card(self, *_args: Any, **_kwargs: Any) -> Any:
        self.calls["move_card"] += 1
        return self.card

    async def move_sprint(self, *_args: Any, **_kwargs: Any) -> Any:
        self.calls["move_sprint"] += 1
        return self.sprint

    async def move_story(self, *_args: Any, **_kwargs: Any) -> Any:
        self.calls["move_story"] += 1
        return self.story

    async def move_ideation(self, *_args: Any, **_kwargs: Any) -> Any:
        self.calls["move_ideation"] += 1
        return self.ideation

    async def move_refinement(self, *_args: Any, **_kwargs: Any) -> Any:
        self.calls["move_refinement"] += 1
        return self.refinement

    async def move_spec(self, *_args: Any, **_kwargs: Any) -> Any:
        self.calls["move_spec"] += 1
        return self.spec

    async def set_test_scenario_status(self, *_args: Any, **_kwargs: Any) -> dict:
        self.calls["set_test_scenario_status"] += 1
        return {
            "scenario_id": "scenario-1",
            "old_status": "draft",
            "new_status": "ready",
        }


class _Uow:
    def __init__(self) -> None:
        self.services = _TransitionServices()
        self.boards = _Boards()
        self.commits = 0
        self.synchronizes = 0
        self.reloads = 0

    async def commit(self) -> None:
        self.commits += 1

    async def synchronize(self) -> None:
        self.synchronizes += 1

    async def reload(self, *_args: Any, **_kwargs: Any) -> None:
        self.reloads += 1


def _actor(permissions: Any = ("*",)) -> ActorContext:
    return ActorContext(
        "actor-1",
        "mcp",
        board_id=BOARD_ID,
        permissions=permissions,
    )


def _transition_case(name: str) -> tuple[Any, Any, Any, str, str, str, str]:
    data: Any
    if name == "rest_card":
        data = SimpleNamespace(status=CardStatus.STARTED)
        return (
            card_crud,
            card_crud.MoveCardUseCase(),
            card_crud.MoveCardCommand("card-1", data),
            "move_card",
            "card.move.not_started_to_started",
            "cards:move",
            "card",
        )
    if name == "mcp_card":
        data = SimpleNamespace(status=CardStatus.STARTED)
        return (
            mcp_card_crud,
            mcp_card_crud.McpMoveCardUseCase(),
            mcp_card_crud.McpMoveCardCommand("card-1", BOARD_ID, data),
            "move_card",
            "card.move.not_started_to_started",
            "cards:move",
            "card",
        )
    if name == "sprint":
        data = SimpleNamespace(status=SprintStatus.ACTIVE)
        return (
            sprints_crud,
            sprints_crud.MoveSprintUseCase(),
            sprints_crud.MoveSprintCommand("sprint-1", data),
            "move_sprint",
            "sprint.move.draft_to_active",
            "specs:move",
            "sprint",
        )
    if name == "rest_story":
        data = SimpleNamespace(status=StoryStatus.TRIAGE)
        return (
            stories_crud,
            stories_crud.MoveStoryUseCase(),
            stories_crud.MoveStoryCommand("story-1", data),
            "move_story",
            "story.move.draft_to_triage",
            "specs:move",
            "story",
        )
    if name == "mcp_story":
        data = SimpleNamespace(status=StoryStatus.TRIAGE)
        return (
            mcp_resource_stories,
            mcp_resource_stories.McpMoveStoryUseCase(),
            mcp_resource_stories.McpMoveStoryCommand(
                BOARD_ID, "story-1", StoryStatus.TRIAGE, data
            ),
            "move_story",
            "story.move.draft_to_triage",
            "specs:move",
            "story",
        )
    if name == "ideation":
        data = SimpleNamespace(status=IdeationStatus.REVIEW)
        return (
            move_ideation,
            move_ideation.MoveIdeationUseCase(),
            move_ideation.MoveIdeationCommand("ideation-1", data),
            "move_ideation",
            "ideation.move.draft_to_review",
            "specs:move",
            "ideation",
        )
    if name == "rest_refinement":
        data = SimpleNamespace(status=RefinementStatus.REVIEW)
        return (
            refinements_crud,
            refinements_crud.MoveRefinementUseCase(),
            refinements_crud.MoveRefinementCommand("refinement-1", data),
            "move_refinement",
            "refinement.move.draft_to_review",
            "specs:move",
            "refinement",
        )
    if name == "mcp_refinement":
        data = SimpleNamespace(status=RefinementStatus.REVIEW)
        return (
            mcp_refinement_crud,
            mcp_refinement_crud.McpMoveRefinementUseCase(),
            mcp_refinement_crud.McpMoveRefinementCommand(
                "refinement-1", BOARD_ID, data
            ),
            "move_refinement",
            "refinement.move.draft_to_review",
            "specs:move",
            "refinement",
        )
    if name == "rest_spec":
        data = SimpleNamespace(status=SpecStatus.REVIEW)
        return (
            spec_crud,
            spec_crud.MoveSpecUseCase(),
            spec_crud.MoveSpecCommand("spec-1", data),
            "move_spec",
            "spec.move.draft_to_review",
            "specs:move",
            "spec",
        )
    if name == "mcp_spec":
        data = SimpleNamespace(status=SpecStatus.REVIEW)
        return (
            mcp_spec_crud,
            mcp_spec_crud.McpMoveSpecUseCase(),
            mcp_spec_crud.McpMoveSpecCommand("spec-1", BOARD_ID, data),
            "move_spec",
            "spec.move.draft_to_review",
            "specs:move",
            "spec",
        )
    if name == "test_scenario":
        return (
            spec_crud,
            spec_crud.SetTestScenarioStatusUseCase(),
            spec_crud.SetTestScenarioStatusCommand(
                "spec-1", "scenario-1", "ready"
            ),
            "set_test_scenario_status",
            "test_scenario.move.draft_to_ready",
            "specs:update",
            "test_scenario",
        )
    raise AssertionError(f"unknown transition case: {name}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case_name",
    [
        "rest_card",
        "mcp_card",
        "sprint",
        "rest_story",
        "mcp_story",
        "ideation",
        "rest_refinement",
        "mcp_refinement",
        "rest_spec",
        "mcp_spec",
        "test_scenario",
    ],
)
async def test_transition_denies_before_writer_with_exact_requirement(
    monkeypatch: pytest.MonkeyPatch, case_name: str
) -> None:
    (
        module,
        use_case,
        command,
        writer,
        expected_operation,
        expected_legacy,
        expected_entity,
    ) = _transition_case(case_name)
    captured: list[tuple[Any, dict[str, Any]]] = []

    async def deny(_actor: Any, requirement: Any, **kwargs: Any) -> None:
        captured.append((requirement, kwargs))
        raise PermissionDeniedError("denied")

    monkeypatch.setattr(module, "require_authorization", deny)
    uow = _Uow()

    with pytest.raises(PermissionDeniedError, match="denied"):
        await use_case.execute(command, actor=_actor(), uow=uow)

    assert uow.services.calls[writer] == 0
    assert uow.commits == 0
    assert len(captured) == 1
    requirement, kwargs = captured[0]
    assert requirement.operation == expected_operation
    assert requirement.legacy_operation == expected_legacy
    assert requirement.entity == expected_entity
    expected_state = "not_started" if case_name.endswith("card") else "draft"
    assert requirement.state == expected_state
    assert kwargs["board_id"] == BOARD_ID


@pytest.mark.asyncio
@pytest.mark.parametrize("case_name", ["rest_card", "mcp_card"])
async def test_card_same_status_reorder_uses_edit_fields_permission(
    monkeypatch: pytest.MonkeyPatch, case_name: str
) -> None:
    module, use_case, command, writer, *_rest = _transition_case(case_name)
    command.data.status = CardStatus.NOT_STARTED
    captured: list[Any] = []

    async def deny(_actor: Any, requirement: Any, **_kwargs: Any) -> None:
        captured.append(requirement)
        raise PermissionDeniedError("denied")

    monkeypatch.setattr(module, "require_authorization", deny)
    uow = _Uow()

    with pytest.raises(PermissionDeniedError):
        await use_case.execute(command, actor=_actor(), uow=uow)

    assert uow.services.calls[writer] == 0
    assert captured[0].operation == "card.entity.edit_fields"
    assert captured[0].legacy_operation == "cards:move"
    assert captured[0].entity == "card"
    assert captured[0].state == "not_started"


@pytest.mark.asyncio
@pytest.mark.parametrize("case_name", ["rest_story", "mcp_story"])
async def test_story_same_status_is_a_read_only_noop(
    monkeypatch: pytest.MonkeyPatch, case_name: str
) -> None:
    module, use_case, command, writer, *_rest = _transition_case(case_name)
    command.data.status = StoryStatus.DRAFT
    if case_name == "mcp_story":
        command = mcp_resource_stories.McpMoveStoryCommand(
            BOARD_ID, "story-1", StoryStatus.DRAFT, command.data
        )
    authorization_calls = 0

    async def unexpected_authorization(*_args: Any, **_kwargs: Any) -> None:
        nonlocal authorization_calls
        authorization_calls += 1

    monkeypatch.setattr(module, "require_authorization", unexpected_authorization)
    uow = _Uow()

    result = await use_case.execute(command, actor=_actor(), uow=uow)

    assert result.story is uow.services.story
    assert authorization_calls == 0
    assert uow.services.calls[writer] == 0
    assert uow.commits == 0


@pytest.mark.asyncio
async def test_test_scenario_same_status_evidence_uses_execute_permission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[Any] = []

    async def deny(_actor: Any, requirement: Any, **_kwargs: Any) -> None:
        captured.append(requirement)
        raise PermissionDeniedError("denied")

    monkeypatch.setattr(spec_crud, "require_authorization", deny)
    uow = _Uow()

    with pytest.raises(PermissionDeniedError):
        await spec_crud.SetTestScenarioStatusUseCase().execute(
            spec_crud.SetTestScenarioStatusCommand(
                "spec-1",
                "scenario-1",
                "draft",
                {"run_id": "run-1"},
            ),
            actor=_actor(),
            uow=uow,
        )

    assert uow.services.calls["set_test_scenario_status"] == 0
    assert captured[0].operation == "spec.tests.execute"
    assert captured[0].legacy_operation == "specs:update"
    assert captured[0].entity == "spec"
    assert captured[0].state == "draft"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case_name", "legacy_permission"),
    [
        ("mcp_card", "cards:move"),
        ("mcp_spec", "specs:move"),
        ("test_scenario", "specs:update"),
    ],
)
async def test_legacy_transition_permission_still_authorizes_writer(
    case_name: str, legacy_permission: str
) -> None:
    _module, use_case, command, writer, *_rest = _transition_case(case_name)
    uow = _Uow()

    await use_case.execute(
        command,
        actor=_actor([legacy_permission]),
        uow=uow,
    )

    assert uow.services.calls[writer] == 1


@pytest.mark.asyncio
async def test_legacy_card_move_authorizes_same_status_reorder() -> None:
    uow = _Uow()

    await mcp_card_crud.McpMoveCardUseCase().execute(
        mcp_card_crud.McpMoveCardCommand(
            "card-1",
            BOARD_ID,
            SimpleNamespace(status=CardStatus.NOT_STARTED),
        ),
        actor=_actor(["cards:move"]),
        uow=uow,
    )

    assert uow.services.calls["move_card"] == 1


@pytest.mark.asyncio
async def test_legacy_specs_update_authorizes_same_status_scenario_evidence() -> None:
    uow = _Uow()

    await spec_crud.SetTestScenarioStatusUseCase().execute(
        spec_crud.SetTestScenarioStatusCommand(
            "spec-1",
            "scenario-1",
            "draft",
            {"run_id": "run-1"},
        ),
        actor=_actor(["specs:update"]),
        uow=uow,
    )

    assert uow.services.calls["set_test_scenario_status"] == 1
