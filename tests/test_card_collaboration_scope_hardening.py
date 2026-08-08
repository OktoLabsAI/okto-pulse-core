"""Fail-closed card -> board -> actor preflights for REST collaboration."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.card_collaboration import (
    AnswerCardQuestionCommand,
    AnswerCardQuestionUseCase,
    CreateCardCommentCommand,
    CreateCardCommentUseCase,
    CreateCardQuestionCommand,
    CreateCardQuestionUseCase,
    DeleteCardAttachmentCommand,
    DeleteCardAttachmentUseCase,
    DeleteCardCommentCommand,
    DeleteCardCommentUseCase,
    DeleteCardQuestionCommand,
    DeleteCardQuestionUseCase,
    GetCardAttachmentCommand,
    GetCardAttachmentUseCase,
    RespondToChoiceCommentCommand,
    RespondToChoiceCommentUseCase,
    UpdateCardCommentCommand,
    UpdateCardCommentUseCase,
    UploadCardAttachmentCommand,
    UploadCardAttachmentUseCase,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID


ACTOR = ActorContext(
    "actor",
    "rest",
    board_id="foreign-board",
    realm_id=LOCAL_REALM_ID,
    permissions=(
        "qa:create",
        "qa:answer",
        "qa:delete",
        "comments:create",
        "comments:update",
        "comments:delete",
        "attachments:upload",
        "attachments:delete",
    ),
)
CARD_ID = "foreign-card"
BOARD_ID = "foreign-board"
COMMENT_ID = "foreign-comment"
QA_ID = "foreign-qa"
ATTACHMENT_ID = "foreign-attachment"


def _uow(
    *,
    child_exists: bool = True,
    card_exists: bool = True,
    share_permission: str | None = None,
):
    card = (
        SimpleNamespace(id=CARD_ID, board_id=BOARD_ID) if card_exists else None
    )
    board = SimpleNamespace(
        id=BOARD_ID,
        owner_id="different-owner",
        realm_id=LOCAL_REALM_ID,
    )
    services = SimpleNamespace(
        cards=SimpleNamespace(get_card=AsyncMock(return_value=card)),
        shares=SimpleNamespace(
            get_user_permission=AsyncMock(return_value=share_permission)
        ),
        comments=SimpleNamespace(
            create_comment=AsyncMock(),
            update_comment=AsyncMock(),
            respond_to_choice=AsyncMock(),
            delete_comment=AsyncMock(),
        ),
        qa=SimpleNamespace(
            create_question=AsyncMock(),
            answer_question=AsyncMock(),
            delete_question=AsyncMock(),
        ),
        attachments=SimpleNamespace(
            upload_attachment=AsyncMock(),
            get_attachment=AsyncMock(),
            delete_attachment=AsyncMock(),
        ),
        comment_card_id=AsyncMock(
            return_value=CARD_ID if child_exists else None
        ),
        qa_card_id=AsyncMock(return_value=CARD_ID if child_exists else None),
        resolve_choice_comment_actor_name=AsyncMock(),
        log_card_collaboration_activity=AsyncMock(),
    )
    return SimpleNamespace(
        services=services,
        boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        commit=AsyncMock(),
        reload=AsyncMock(),
    )


def _error_contract(exc: EntityNotFoundError) -> tuple[type[Exception], str]:
    return type(exc), exc.entity_type


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command", "writer"),
    [
        (
            CreateCardCommentUseCase(),
            CreateCardCommentCommand(CARD_ID, SimpleNamespace(content="comment")),
            ("comments", "create_comment"),
        ),
        (
            CreateCardQuestionUseCase(),
            CreateCardQuestionCommand(CARD_ID, SimpleNamespace(question="question")),
            ("qa", "create_question"),
        ),
        (
            UploadCardAttachmentUseCase(),
            UploadCardAttachmentCommand(
                BOARD_ID, CARD_ID, "evidence.txt", b"evidence", "text/plain"
            ),
            ("attachments", "upload_attachment"),
        ),
    ],
)
async def test_foreign_card_create_paths_match_missing_before_writer(
    use_case, command, writer
) -> None:
    foreign_uow = _uow()
    missing_uow = _uow(card_exists=False)

    with pytest.raises(EntityNotFoundError) as foreign_error:
        await use_case.execute(command, actor=ACTOR, uow=foreign_uow)
    with pytest.raises(EntityNotFoundError) as missing_error:
        await use_case.execute(command, actor=ACTOR, uow=missing_uow)

    assert _error_contract(foreign_error.value) == _error_contract(missing_error.value)
    getattr(getattr(foreign_uow.services, writer[0]), writer[1]).assert_not_awaited()
    foreign_uow.services.log_card_collaboration_activity.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command", "writer", "resolver"),
    [
        (
            UpdateCardCommentUseCase(),
            UpdateCardCommentCommand(COMMENT_ID, SimpleNamespace(content="updated")),
            ("comments", "update_comment"),
            "comment_card_id",
        ),
        (
            RespondToChoiceCommentUseCase(),
            RespondToChoiceCommentCommand(COMMENT_ID, ["a"], None),
            ("comments", "respond_to_choice"),
            "comment_card_id",
        ),
        (
            DeleteCardCommentUseCase(),
            DeleteCardCommentCommand(COMMENT_ID),
            ("comments", "delete_comment"),
            "comment_card_id",
        ),
        (
            AnswerCardQuestionUseCase(),
            AnswerCardQuestionCommand(QA_ID, SimpleNamespace(answer="answer")),
            ("qa", "answer_question"),
            "qa_card_id",
        ),
        (
            DeleteCardQuestionUseCase(),
            DeleteCardQuestionCommand(QA_ID),
            ("qa", "delete_question"),
            "qa_card_id",
        ),
    ],
)
async def test_foreign_child_mutations_match_missing_before_writer(
    use_case, command, writer, resolver
) -> None:
    foreign_uow = _uow()
    missing_uow = _uow(child_exists=False)

    with pytest.raises(EntityNotFoundError) as foreign_error:
        await use_case.execute(command, actor=ACTOR, uow=foreign_uow)
    with pytest.raises(EntityNotFoundError) as missing_error:
        await use_case.execute(command, actor=ACTOR, uow=missing_uow)

    assert _error_contract(foreign_error.value) == _error_contract(missing_error.value)
    getattr(getattr(foreign_uow.services, writer[0]), writer[1]).assert_not_awaited()
    foreign_uow.services.log_card_collaboration_activity.assert_not_awaited()
    getattr(foreign_uow.services, resolver).assert_awaited_once()


@pytest.mark.asyncio
async def test_foreign_attachment_read_and_delete_stop_before_attachment_lookup() -> None:
    for use_case, command in (
        (
            GetCardAttachmentUseCase(),
            GetCardAttachmentCommand(BOARD_ID, CARD_ID, ATTACHMENT_ID),
        ),
        (
            DeleteCardAttachmentUseCase(),
            DeleteCardAttachmentCommand(BOARD_ID, CARD_ID, ATTACHMENT_ID),
        ),
    ):
        uow = _uow()
        with pytest.raises(EntityNotFoundError):
            await use_case.execute(command, actor=ACTOR, uow=uow)
        uow.services.attachments.get_attachment.assert_not_awaited()
        uow.services.attachments.delete_attachment.assert_not_awaited()
        uow.services.log_card_collaboration_activity.assert_not_awaited()


@pytest.mark.asyncio
async def test_viewer_share_is_read_only_while_editor_can_collaborate() -> None:
    command = CreateCardCommentCommand(
        CARD_ID,
        SimpleNamespace(content="scoped comment"),
    )
    viewer_uow = _uow(share_permission="viewer")
    with pytest.raises(EntityNotFoundError):
        await CreateCardCommentUseCase().execute(
            command,
            actor=ACTOR,
            uow=viewer_uow,
        )
    viewer_uow.services.comments.create_comment.assert_not_awaited()

    editor_uow = _uow(share_permission="editor")
    comment = SimpleNamespace(id="comment", card_id=CARD_ID)
    editor_uow.services.comments.create_comment.return_value = comment
    result = await CreateCardCommentUseCase().execute(
        command,
        actor=ACTOR,
        uow=editor_uow,
    )

    assert result.comment is comment
    editor_uow.services.comments.create_comment.assert_awaited_once()
    editor_uow.services.log_card_collaboration_activity.assert_awaited_once()
    editor_uow.commit.assert_awaited_once()
