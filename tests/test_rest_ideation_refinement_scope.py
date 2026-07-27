"""REST ownership and parent/child containment for ideation/refinement families."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.ideations import router as ideations_router
from okto_pulse.community.api.refinements import router as refinements_router
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.ideations_crud import (
    AnswerIdeationQuestionCommand,
    AnswerIdeationQuestionUseCase,
    CreateIdeationCommand,
    CreateIdeationUseCase,
    CreateIdeationKnowledgeCommand,
    CreateIdeationKnowledgeUseCase,
    CreateIdeationQuestionCommand,
    CreateIdeationQuestionUseCase,
    DeleteIdeationCommand,
    DeleteIdeationKnowledgeCommand,
    DeleteIdeationKnowledgeUseCase,
    DeleteIdeationQuestionCommand,
    DeleteIdeationQuestionUseCase,
    DeleteIdeationUseCase,
    DeriveSpecCommand,
    DeriveSpecUseCase,
    EvaluateComplexityCommand,
    EvaluateComplexityUseCase,
    GetIdeationCommand,
    GetIdeationKnowledgeCommand,
    GetIdeationKnowledgeUseCase,
    GetIdeationSnapshotCommand,
    GetIdeationSnapshotUseCase,
    GetIdeationUseCase,
    ListIdeationHistoryCommand,
    ListIdeationHistoryUseCase,
    ListIdeationsCommand,
    ListIdeationsUseCase,
    ListIdeationKnowledgeCommand,
    ListIdeationKnowledgeUseCase,
    ListIdeationQACommand,
    ListIdeationQAUseCase,
    ListIdeationSnapshotsCommand,
    ListIdeationSnapshotsUseCase,
    SetIdeationAmbiguityGateSkipCommand,
    SetIdeationAmbiguityGateSkipUseCase,
    UpdateIdeationCommand,
    UpdateIdeationUseCase,
)
from okto_pulse.core.application.use_cases.move_ideation import (
    MoveIdeationCommand,
    MoveIdeationUseCase,
)
from okto_pulse.core.application.use_cases.refinements_crud import (
    AnswerRefinementQuestionCommand,
    AnswerRefinementQuestionUseCase,
    CreateRefinementKnowledgeCommand,
    CreateRefinementKnowledgeUseCase,
    CreateRefinementQuestionCommand,
    CreateRefinementQuestionUseCase,
    CreateRefinementCommand,
    CreateRefinementUseCase,
    DeleteRefinementCommand,
    DeleteRefinementKnowledgeCommand,
    DeleteRefinementKnowledgeUseCase,
    DeleteRefinementQuestionCommand,
    DeleteRefinementQuestionUseCase,
    DeleteRefinementUseCase,
    DeriveSpecFromRefinementCommand,
    DeriveSpecFromRefinementUseCase,
    GetRefinementCommand,
    GetRefinementKnowledgeCommand,
    GetRefinementKnowledgeUseCase,
    GetRefinementSnapshotCommand,
    GetRefinementSnapshotUseCase,
    GetRefinementUseCase,
    ListRefinementHistoryCommand,
    ListRefinementHistoryUseCase,
    ListRefinementKnowledgeCommand,
    ListRefinementKnowledgeUseCase,
    ListRefinementQACommand,
    ListRefinementQAUseCase,
    ListRefinementSnapshotsCommand,
    ListRefinementSnapshotsUseCase,
    ListRefinementsCommand,
    ListRefinementsUseCase,
    MoveRefinementCommand,
    MoveRefinementUseCase,
    UpdateRefinementCommand,
    UpdateRefinementUseCase,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID


ACTOR = ActorContext("user-a", "rest", realm_id=LOCAL_REALM_ID)


def _service(**methods):
    return SimpleNamespace(
        **{name: AsyncMock(return_value=value) for name, value in methods.items()}
    )


def _foreign_uow():
    board = SimpleNamespace(id="board-b", owner_id="user-b")
    ideation = SimpleNamespace(id="ideation-b", board_id=board.id)
    refinement = SimpleNamespace(id="refinement-b", board_id=board.id)
    services = SimpleNamespace(
        shares=_service(get_user_permission=None),
        ideations=_service(
            get_ideation=ideation,
            create_ideation=None,
            list_ideations=[],
            update_ideation=None,
            set_ambiguity_gate_skip=None,
            delete_ideation=False,
            evaluate_complexity=None,
            derive_spec=None,
            list_snapshots=[],
            get_snapshot=None,
            list_history=[],
            move_ideation=None,
        ),
        ideation_knowledge=_service(
            list_knowledge=[],
            get_knowledge=None,
            create_knowledge=None,
            delete_knowledge=False,
        ),
        ideation_qa=_service(
            list_qa=[],
            get_question=None,
            create_question=None,
            answer_question=None,
            delete_question=False,
        ),
        refinements=_service(
            get_refinement=refinement,
            create_refinement=None,
            list_refinements=[],
            update_refinement=None,
            move_refinement=None,
            delete_refinement=False,
            derive_spec=None,
            list_history=[],
            list_snapshots=[],
            get_snapshot=None,
        ),
        refinement_knowledge=_service(
            list_knowledge=[],
            get_knowledge=None,
            create_knowledge=None,
            delete_knowledge=False,
        ),
        refinement_qa=_service(
            list_qa=[],
            get_question=None,
            create_question=None,
            answer_question=None,
            delete_question=False,
        ),
        specs=_service(get_spec=None),
    )
    return SimpleNamespace(
        boards=_service(get=board),
        services=services,
        commit=AsyncMock(),
        rollback=AsyncMock(),
        synchronize=AsyncMock(),
    )


def _ideation_matrix():
    data = SimpleNamespace()
    return (
        (GetIdeationUseCase(), GetIdeationCommand("ideation-b")),
        (UpdateIdeationUseCase(), UpdateIdeationCommand("ideation-b", data)),
        (
            SetIdeationAmbiguityGateSkipUseCase(),
            SetIdeationAmbiguityGateSkipCommand("ideation-b", True),
        ),
        (MoveIdeationUseCase(), MoveIdeationCommand("ideation-b", data)),
        (DeleteIdeationUseCase(), DeleteIdeationCommand("ideation-b")),
        (
            EvaluateComplexityUseCase(),
            EvaluateComplexityCommand("ideation-b", {}),
        ),
        (DeriveSpecUseCase(), DeriveSpecCommand("ideation-b")),
        (
            ListIdeationSnapshotsUseCase(),
            ListIdeationSnapshotsCommand("ideation-b"),
        ),
        (
            GetIdeationSnapshotUseCase(),
            GetIdeationSnapshotCommand("ideation-b", 1),
        ),
        (
            ListIdeationHistoryUseCase(),
            ListIdeationHistoryCommand("ideation-b"),
        ),
        (
            ListIdeationKnowledgeUseCase(),
            ListIdeationKnowledgeCommand("ideation-b"),
        ),
        (
            GetIdeationKnowledgeUseCase(),
            GetIdeationKnowledgeCommand("ideation-b", "knowledge-b"),
        ),
        (
            CreateIdeationKnowledgeUseCase(),
            CreateIdeationKnowledgeCommand("ideation-b", data),
        ),
        (
            DeleteIdeationKnowledgeUseCase(),
            DeleteIdeationKnowledgeCommand("ideation-b", "knowledge-b"),
        ),
        (ListIdeationQAUseCase(), ListIdeationQACommand("ideation-b")),
        (
            CreateIdeationQuestionUseCase(),
            CreateIdeationQuestionCommand("ideation-b", data),
        ),
        (
            AnswerIdeationQuestionUseCase(),
            AnswerIdeationQuestionCommand("ideation-b", "qa-b", data),
        ),
        (
            DeleteIdeationQuestionUseCase(),
            DeleteIdeationQuestionCommand("ideation-b", "qa-b"),
        ),
    )


def _refinement_matrix():
    data = SimpleNamespace()
    return (
        (GetRefinementUseCase(), GetRefinementCommand("refinement-b")),
        (
            UpdateRefinementUseCase(),
            UpdateRefinementCommand("refinement-b", data),
        ),
        (MoveRefinementUseCase(), MoveRefinementCommand("refinement-b", data)),
        (DeleteRefinementUseCase(), DeleteRefinementCommand("refinement-b")),
        (
            DeriveSpecFromRefinementUseCase(),
            DeriveSpecFromRefinementCommand("refinement-b"),
        ),
        (
            ListRefinementHistoryUseCase(),
            ListRefinementHistoryCommand("refinement-b"),
        ),
        (ListRefinementQAUseCase(), ListRefinementQACommand("refinement-b")),
        (
            CreateRefinementQuestionUseCase(),
            CreateRefinementQuestionCommand("refinement-b", data),
        ),
        (
            AnswerRefinementQuestionUseCase(),
            AnswerRefinementQuestionCommand("refinement-b", "qa-b", data),
        ),
        (
            DeleteRefinementQuestionUseCase(),
            DeleteRefinementQuestionCommand("refinement-b", "qa-b"),
        ),
        (
            ListRefinementSnapshotsUseCase(),
            ListRefinementSnapshotsCommand("refinement-b"),
        ),
        (
            GetRefinementSnapshotUseCase(),
            GetRefinementSnapshotCommand("refinement-b", 1),
        ),
        (
            ListRefinementKnowledgeUseCase(),
            ListRefinementKnowledgeCommand("refinement-b"),
        ),
        (
            GetRefinementKnowledgeUseCase(),
            GetRefinementKnowledgeCommand("refinement-b", "knowledge-b"),
        ),
        (
            CreateRefinementKnowledgeUseCase(),
            CreateRefinementKnowledgeCommand("refinement-b", data),
        ),
        (
            DeleteRefinementKnowledgeUseCase(),
            DeleteRefinementKnowledgeCommand("refinement-b", "knowledge-b"),
        ),
    )


def _ideation_write_matrix():
    data = SimpleNamespace()
    return (
        (UpdateIdeationUseCase(), UpdateIdeationCommand("ideation-b", data)),
        (
            SetIdeationAmbiguityGateSkipUseCase(),
            SetIdeationAmbiguityGateSkipCommand("ideation-b", True),
        ),
        (MoveIdeationUseCase(), MoveIdeationCommand("ideation-b", data)),
        (DeleteIdeationUseCase(), DeleteIdeationCommand("ideation-b")),
        (EvaluateComplexityUseCase(), EvaluateComplexityCommand("ideation-b", {})),
        (DeriveSpecUseCase(), DeriveSpecCommand("ideation-b")),
        (
            CreateIdeationKnowledgeUseCase(),
            CreateIdeationKnowledgeCommand("ideation-b", data),
        ),
        (
            DeleteIdeationKnowledgeUseCase(),
            DeleteIdeationKnowledgeCommand("ideation-b", "knowledge-b"),
        ),
        (
            CreateIdeationQuestionUseCase(),
            CreateIdeationQuestionCommand("ideation-b", data),
        ),
        (
            AnswerIdeationQuestionUseCase(),
            AnswerIdeationQuestionCommand("ideation-b", "qa-b", data),
        ),
        (
            DeleteIdeationQuestionUseCase(),
            DeleteIdeationQuestionCommand("ideation-b", "qa-b"),
        ),
    )


def _refinement_write_matrix():
    data = SimpleNamespace()
    return (
        (UpdateRefinementUseCase(), UpdateRefinementCommand("refinement-b", data)),
        (MoveRefinementUseCase(), MoveRefinementCommand("refinement-b", data)),
        (DeleteRefinementUseCase(), DeleteRefinementCommand("refinement-b")),
        (
            DeriveSpecFromRefinementUseCase(),
            DeriveSpecFromRefinementCommand("refinement-b"),
        ),
        (
            CreateRefinementQuestionUseCase(),
            CreateRefinementQuestionCommand("refinement-b", data),
        ),
        (
            AnswerRefinementQuestionUseCase(),
            AnswerRefinementQuestionCommand("refinement-b", "qa-b", data),
        ),
        (
            DeleteRefinementQuestionUseCase(),
            DeleteRefinementQuestionCommand("refinement-b", "qa-b"),
        ),
        (
            CreateRefinementKnowledgeUseCase(),
            CreateRefinementKnowledgeCommand("refinement-b", data),
        ),
        (
            DeleteRefinementKnowledgeUseCase(),
            DeleteRefinementKnowledgeCommand("refinement-b", "knowledge-b"),
        ),
    )


def _assert_no_ideation_child_or_writer_call(uow) -> None:
    for name in (
        "update_ideation",
        "set_ambiguity_gate_skip",
        "delete_ideation",
        "evaluate_complexity",
        "derive_spec",
        "list_snapshots",
        "get_snapshot",
        "list_history",
        "move_ideation",
    ):
        getattr(uow.services.ideations, name).assert_not_awaited()
    for service in (uow.services.ideation_knowledge, uow.services.ideation_qa):
        for value in vars(service).values():
            if isinstance(value, AsyncMock):
                value.assert_not_awaited()


def _assert_no_refinement_child_or_writer_call(uow) -> None:
    for name in (
        "update_refinement",
        "move_refinement",
        "delete_refinement",
        "derive_spec",
        "list_history",
        "list_snapshots",
        "get_snapshot",
    ):
        getattr(uow.services.refinements, name).assert_not_awaited()
    for service in (uow.services.refinement_knowledge, uow.services.refinement_qa):
        for value in vars(service).values():
            if isinstance(value, AsyncMock):
                value.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("use_case", "command"), _ideation_matrix())
async def test_ideation_family_denies_foreign_owner_before_child_or_writer(
    use_case, command
):
    uow = _foreign_uow()
    with pytest.raises(EntityNotFoundError) as exc_info:
        await use_case.execute(command, actor=ACTOR, uow=uow)
    assert exc_info.value.entity_type == "ideation"
    _assert_no_ideation_child_or_writer_call(uow)
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("use_case", "command"), _refinement_matrix())
async def test_refinement_family_denies_foreign_owner_before_child_or_writer(
    use_case, command
):
    uow = _foreign_uow()
    with pytest.raises(EntityNotFoundError) as exc_info:
        await use_case.execute(command, actor=ACTOR, uow=uow)
    assert exc_info.value.entity_type == "refinement"
    _assert_no_refinement_child_or_writer_call(uow)
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command", "entity_type"),
    [
        (
            CreateRefinementUseCase(),
            CreateRefinementCommand("ideation-b", SimpleNamespace()),
            "refinement_ideation_owner",
        ),
        (
            ListRefinementsUseCase(),
            ListRefinementsCommand("ideation-b"),
            "ideation",
        ),
    ],
)
async def test_refinement_parent_routes_deny_foreign_ideation_before_service(
    use_case, command, entity_type
):
    uow = _foreign_uow()
    with pytest.raises(EntityNotFoundError) as exc_info:
        await use_case.execute(command, actor=ACTOR, uow=uow)
    assert exc_info.value.entity_type == entity_type
    uow.services.refinements.create_refinement.assert_not_awaited()
    uow.services.refinements.list_refinements.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command", "attribute"),
    [
        (GetIdeationUseCase(), GetIdeationCommand("ideation-b"), "ideation"),
        (
            GetRefinementUseCase(),
            GetRefinementCommand("refinement-b"),
            "refinement",
        ),
    ],
)
async def test_shared_board_member_can_read_ideation_and_refinement(
    use_case, command, attribute
):
    uow = _foreign_uow()
    uow.services.shares.get_user_permission.return_value = "viewer"
    result = await use_case.execute(command, actor=ACTOR, uow=uow)
    assert getattr(result, attribute).id == getattr(command, f"{attribute}_id")


@pytest.mark.asyncio
@pytest.mark.parametrize(("use_case", "command"), _ideation_write_matrix())
async def test_viewer_share_cannot_mutate_ideation_family(use_case, command):
    uow = _foreign_uow()
    uow.services.shares.get_user_permission.return_value = "viewer"

    with pytest.raises(EntityNotFoundError) as exc_info:
        await use_case.execute(command, actor=ACTOR, uow=uow)

    assert exc_info.value.entity_type == "ideation"
    _assert_no_ideation_child_or_writer_call(uow)
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("use_case", "command"), _refinement_write_matrix())
async def test_viewer_share_cannot_mutate_refinement_family(use_case, command):
    uow = _foreign_uow()
    uow.services.shares.get_user_permission.return_value = "viewer"

    with pytest.raises(EntityNotFoundError) as exc_info:
        await use_case.execute(command, actor=ACTOR, uow=uow)

    assert exc_info.value.entity_type == "refinement"
    _assert_no_refinement_child_or_writer_call(uow)
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_editor_share_can_update_ideation_and_refinement():
    uow = _foreign_uow()
    uow.services.shares.get_user_permission.return_value = "editor"
    ideation = uow.services.ideations.get_ideation.return_value
    refinement = uow.services.refinements.get_refinement.return_value
    uow.services.ideations.update_ideation.return_value = ideation
    uow.services.refinements.update_refinement.return_value = refinement

    ideation_result = await UpdateIdeationUseCase().execute(
        UpdateIdeationCommand("ideation-b", SimpleNamespace()),
        actor=ACTOR,
        uow=uow,
    )
    refinement_result = await UpdateRefinementUseCase().execute(
        UpdateRefinementCommand("refinement-b", SimpleNamespace()),
        actor=ACTOR,
        uow=uow,
    )

    assert ideation_result.ideation is ideation
    assert refinement_result.refinement is refinement
    assert uow.commit.await_count == 2


@pytest.mark.asyncio
async def test_board_share_permissions_apply_to_create_and_list():
    uow = _foreign_uow()
    ideation = uow.services.ideations.get_ideation.return_value
    refinement = uow.services.refinements.get_refinement.return_value
    uow.services.ideations.create_ideation.return_value = ideation
    uow.services.ideations.list_ideations.return_value = [ideation]
    uow.services.refinements.create_refinement.return_value = refinement

    uow.services.shares.get_user_permission.return_value = "viewer"
    listed = await ListIdeationsUseCase().execute(
        ListIdeationsCommand("board-b"), actor=ACTOR, uow=uow
    )
    assert listed.ideations == [ideation]
    with pytest.raises(EntityNotFoundError):
        await CreateIdeationUseCase().execute(
            CreateIdeationCommand("board-b", SimpleNamespace()),
            actor=ACTOR,
            uow=uow,
        )
    with pytest.raises(EntityNotFoundError):
        await CreateRefinementUseCase().execute(
            CreateRefinementCommand("ideation-b", SimpleNamespace()),
            actor=ACTOR,
            uow=uow,
        )

    uow.services.shares.get_user_permission.return_value = "editor"
    created_ideation = await CreateIdeationUseCase().execute(
        CreateIdeationCommand("board-b", SimpleNamespace()),
        actor=ACTOR,
        uow=uow,
    )
    created_refinement = await CreateRefinementUseCase().execute(
        CreateRefinementCommand("ideation-b", SimpleNamespace()),
        actor=ACTOR,
        uow=uow,
    )
    assert created_ideation.ideation is ideation
    assert created_refinement.refinement is refinement


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["ideation", "refinement"])
async def test_qa_wrong_parent_is_not_found_before_answer_or_delete(family):
    uow = _foreign_uow()
    uow.boards.get.return_value = SimpleNamespace(id="board-a", owner_id="user-a")
    data = SimpleNamespace()
    if family == "ideation":
        uow.services.ideations.get_ideation.return_value = SimpleNamespace(
            id="ideation-a", board_id="board-a"
        )
        qa_service = uow.services.ideation_qa
        qa_service.get_question.return_value = SimpleNamespace(
            id="qa-b", ideation_id="ideation-b"
        )
        operations = (
            (
                AnswerIdeationQuestionUseCase(),
                AnswerIdeationQuestionCommand("ideation-a", "qa-b", data),
            ),
            (
                DeleteIdeationQuestionUseCase(),
                DeleteIdeationQuestionCommand("ideation-a", "qa-b"),
            ),
        )
    else:
        uow.services.refinements.get_refinement.return_value = SimpleNamespace(
            id="refinement-a", board_id="board-a"
        )
        qa_service = uow.services.refinement_qa
        qa_service.get_question.return_value = SimpleNamespace(
            id="qa-b", refinement_id="refinement-b"
        )
        operations = (
            (
                AnswerRefinementQuestionUseCase(),
                AnswerRefinementQuestionCommand("refinement-a", "qa-b", data),
            ),
            (
                DeleteRefinementQuestionUseCase(),
                DeleteRefinementQuestionCommand("refinement-a", "qa-b"),
            ),
        )

    for use_case, command in operations:
        with pytest.raises(EntityNotFoundError):
            await use_case.execute(command, actor=ACTOR, uow=uow)
    qa_service.answer_question.assert_not_awaited()
    qa_service.delete_question.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["ideation", "refinement"])
async def test_knowledge_wrong_parent_is_not_found_before_delete(family):
    uow = _foreign_uow()
    uow.boards.get.return_value = SimpleNamespace(id="board-a", owner_id="user-a")
    if family == "ideation":
        uow.services.ideations.get_ideation.return_value = SimpleNamespace(
            id="ideation-a", board_id="board-a"
        )
        knowledge_service = uow.services.ideation_knowledge
        knowledge_service.get_knowledge.return_value = SimpleNamespace(
            id="knowledge-b", ideation_id="ideation-b"
        )
        operations = (
            (
                GetIdeationKnowledgeUseCase(),
                GetIdeationKnowledgeCommand("ideation-a", "knowledge-b"),
            ),
            (
                DeleteIdeationKnowledgeUseCase(),
                DeleteIdeationKnowledgeCommand("ideation-a", "knowledge-b"),
            ),
        )
    else:
        uow.services.refinements.get_refinement.return_value = SimpleNamespace(
            id="refinement-a", board_id="board-a"
        )
        knowledge_service = uow.services.refinement_knowledge
        knowledge_service.get_knowledge.return_value = SimpleNamespace(
            id="knowledge-b", refinement_id="refinement-b"
        )
        operations = (
            (
                GetRefinementKnowledgeUseCase(),
                GetRefinementKnowledgeCommand("refinement-a", "knowledge-b"),
            ),
            (
                DeleteRefinementKnowledgeUseCase(),
                DeleteRefinementKnowledgeCommand("refinement-a", "knowledge-b"),
            ),
        )

    for use_case, command in operations:
        with pytest.raises(EntityNotFoundError):
            await use_case.execute(command, actor=ACTOR, uow=uow)
    knowledge_service.delete_knowledge.assert_not_awaited()
    uow.commit.assert_not_awaited()


def test_rest_list_readers_map_foreign_parents_to_404():
    uow = _foreign_uow()
    app = FastAPI()
    app.include_router(ideations_router, prefix="/api/v1")
    app.include_router(refinements_router, prefix="/api/v1")
    app.dependency_overrides[require_user] = lambda: "user-a"
    app.dependency_overrides[get_unit_of_work] = lambda: uow
    client = TestClient(app)

    paths = (
        "/api/v1/ideations/ideation-b",
        "/api/v1/ideations/ideation-b/snapshots",
        "/api/v1/ideations/ideation-b/history",
        "/api/v1/ideations/ideation-b/knowledge",
        "/api/v1/ideations/ideation-b/qa",
        "/api/v1/refinements/refinement-b",
        "/api/v1/refinements/refinement-b/history",
        "/api/v1/refinements/refinement-b/snapshots",
        "/api/v1/refinements/refinement-b/knowledge",
        "/api/v1/refinements/refinement-b/qa",
        "/api/v1/ideations/ideation-b/refinements",
    )
    for path in paths:
        response = client.get(path)
        assert response.status_code == 404, (path, response.text)
