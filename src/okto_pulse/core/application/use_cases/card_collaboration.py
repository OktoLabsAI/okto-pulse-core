"""Card collaboration use cases for AF35-S3 C2.

The REST adapter owns HTTP/auth/file envelopes; these use cases own the
transactional collaboration work for card comments, Q&A and attachments. They
delegate to the existing services while the strangler is still in progress, but
the REST handlers no longer depend on raw request sessions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.use_cases.board_access import load_accessible_card
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
    UseCaseError,
    commit,
)
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


class CardNotFoundError(EntityNotFoundError):
    def __init__(self, card_id: str) -> None:
        super().__init__("card", card_id)


class CardNotFoundInBoardError(EntityNotFoundError):
    def __init__(self, board_id: str, card_id: str) -> None:
        self.board_id = board_id
        super().__init__("card_in_board", card_id)


class CommentMutationNotFoundError(EntityNotFoundError):
    def __init__(self, comment_id: str) -> None:
        super().__init__("comment_mutation", comment_id)


class ChoiceCommentNotFoundError(EntityNotFoundError):
    def __init__(self, comment_id: str) -> None:
        super().__init__("choice_comment", comment_id)


class InvalidChoiceResponseError(CommandValidationError):
    def __init__(self) -> None:
        super().__init__("Invalid choice or comment not found")


class QuestionNotFoundError(EntityNotFoundError):
    def __init__(self, qa_id: str) -> None:
        super().__init__("qa_item", qa_id)


class AttachmentNotFoundError(EntityNotFoundError):
    def __init__(self, attachment_id: str) -> None:
        super().__init__("attachment", attachment_id)


MAX_ATTACHMENT_FILENAME_BYTES = 200
_INVALID_ATTACHMENT_FILENAME_CHARS = frozenset('<>:"/\\|?*')
_RESERVED_ATTACHMENT_FILENAMES = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{index}" for index in range(1, 10)),
        *(f"LPT{index}" for index in range(1, 10)),
    }
)


class InvalidAttachmentFilenameError(CommandValidationError):
    """A filename that cannot be represented safely by every storage adapter."""

    code = "invalid_attachment_filename"

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"Invalid attachment filename: {reason}")


class AttachmentStorageError(UseCaseError):
    """Safe storage failure that never exposes an adapter's local path."""

    code = "attachment_storage_error"

    def __init__(self, operation: str) -> None:
        self.operation = operation
        super().__init__("Attachment storage operation failed")


def validate_attachment_filename(filename: str) -> str:
    """Validate one portable storage-object name before any provider write.

    Storage providers may prepend collision-resistant tokens and temporary-file
    suffixes.  The UTF-8 byte cap leaves room for those additions below the
    common 255-byte filesystem component limit.  Invalid names are rejected
    instead of being silently reduced to ``Path(filename).name``.
    """

    if not isinstance(filename, str) or not filename:
        raise InvalidAttachmentFilenameError("a non-empty name is required")
    if filename != filename.strip():
        raise InvalidAttachmentFilenameError(
            "leading or trailing whitespace is not allowed"
        )
    if filename in {".", ".."}:
        raise InvalidAttachmentFilenameError("relative path names are not allowed")
    if filename.endswith("."):
        raise InvalidAttachmentFilenameError("a trailing dot is not allowed")
    if any(
        char in _INVALID_ATTACHMENT_FILENAME_CHARS or ord(char) < 32 or ord(char) == 127
        for char in filename
    ):
        raise InvalidAttachmentFilenameError(
            "path separators, control characters, or reserved characters "
            "are not allowed"
        )
    device_name = filename.split(".", 1)[0].upper()
    if device_name in _RESERVED_ATTACHMENT_FILENAMES:
        raise InvalidAttachmentFilenameError("a reserved device name is not allowed")
    try:
        encoded = filename.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise InvalidAttachmentFilenameError("the name must be valid Unicode") from exc
    if len(encoded) > MAX_ATTACHMENT_FILENAME_BYTES:
        raise InvalidAttachmentFilenameError(
            f"the name must not exceed {MAX_ATTACHMENT_FILENAME_BYTES} UTF-8 bytes"
        )
    return filename


@dataclass(frozen=True)
class CreateCardCommentCommand:
    card_id: str
    data: Any


@dataclass(frozen=True)
class UpdateCardCommentCommand:
    comment_id: str
    data: Any


@dataclass(frozen=True)
class RespondToChoiceCommentCommand:
    comment_id: str
    selected: list[str]
    free_text: str | None


@dataclass(frozen=True)
class DeleteCardCommentCommand:
    comment_id: str


@dataclass(frozen=True)
class CreateCardQuestionCommand:
    card_id: str
    data: Any


@dataclass(frozen=True)
class AnswerCardQuestionCommand:
    qa_id: str
    data: Any


@dataclass(frozen=True)
class DeleteCardQuestionCommand:
    qa_id: str


@dataclass(frozen=True)
class UploadCardAttachmentCommand:
    board_id: str
    card_id: str
    filename: str
    content: bytes
    mime_type: str


@dataclass(frozen=True)
class GetCardAttachmentCommand:
    board_id: str
    card_id: str
    attachment_id: str


@dataclass(frozen=True)
class DeleteCardAttachmentCommand:
    board_id: str
    card_id: str
    attachment_id: str


@dataclass(frozen=True)
class CommentResult:
    comment: Any


@dataclass(frozen=True)
class QAResult:
    qa: Any


@dataclass(frozen=True)
class AttachmentResult:
    attachment: Any


_CARD_COLLABORATION_WRITE_SHARES = {"editor", "admin"}


async def _log_card_activity(
    services: ApplicationServiceCatalog,
    card_id: str,
    action: str,
    actor: ActorContext,
    details: dict[str, Any] | None = None,
) -> None:
    await services.log_card_collaboration_activity(
        card_id,
        action=action,
        actor_type="user" if actor.source == "rest" else actor.source,
        actor_id=actor.actor_id,
        actor_name=actor.actor_name,
        details=details,
    )


async def _require_card_access(
    uow: PulseUnitOfWork,
    card_id: str,
    actor: ActorContext,
    *,
    board_id: str | None = None,
    write: bool = False,
    denied_error: EntityNotFoundError | None = None,
) -> Any:
    card = await load_accessible_card(
        uow,
        card_id,
        actor,
        expected_board_id=board_id,
        allowed_share_permissions=(_CARD_COLLABORATION_WRITE_SHARES if write else None),
    )
    if card is None:
        if denied_error is not None:
            raise denied_error
        if board_id is not None:
            raise CardNotFoundInBoardError(board_id, card_id)
        raise CardNotFoundError(card_id)
    return card


async def _refresh(
    uow: PulseUnitOfWork,
    entity: object,
    attribute_names: list[str] | None = None,
) -> None:
    await uow.reload(entity, fields=tuple(attribute_names or ()))


class CreateCardCommentUseCase:
    async def execute(
        self,
        command: CreateCardCommentCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CommentResult:

        await _require_card_access(uow, command.card_id, actor, write=True)
        comment = await uow.services.comments.create_comment(
            command.card_id, actor.actor_id, command.data
        )
        if not comment:
            raise CardNotFoundError(command.card_id)
        await _log_card_activity(
            uow.services,
            command.card_id,
            "comment_added",
            actor,
            {"content": command.data.content[:100]},
        )
        await commit(uow)
        await _refresh(
            uow,
            comment,
            ["id", "card_id", "content", "author_id", "created_at", "updated_at"],
        )
        return CommentResult(comment)


class UpdateCardCommentUseCase:
    async def execute(
        self,
        command: UpdateCardCommentCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CommentResult:

        card_id = await uow.services.comment_card_id(command.comment_id)
        if card_id is None:
            raise CommentMutationNotFoundError(command.comment_id)
        await _require_card_access(
            uow,
            card_id,
            actor,
            write=True,
            denied_error=CommentMutationNotFoundError(command.comment_id),
        )
        comment = await uow.services.comments.update_comment(
            command.comment_id, actor.actor_id, command.data
        )
        if not comment:
            raise CommentMutationNotFoundError(command.comment_id)
        await _log_card_activity(
            uow.services,
            comment.card_id,
            "comment_updated",
            actor,
            {"comment_id": command.comment_id},
        )
        await commit(uow)
        await _refresh(
            uow,
            comment,
            ["id", "card_id", "content", "author_id", "created_at", "updated_at"],
        )
        return CommentResult(comment)


class RespondToChoiceCommentUseCase:
    async def execute(
        self,
        command: RespondToChoiceCommentCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> CommentResult:
        card_id = await uow.services.comment_card_id(command.comment_id)
        if card_id is None:
            raise ChoiceCommentNotFoundError(command.comment_id)
        await _require_card_access(
            uow,
            card_id,
            actor,
            write=True,
            denied_error=ChoiceCommentNotFoundError(command.comment_id),
        )
        actor_name = await uow.services.resolve_choice_comment_actor_name(
            command.comment_id, actor.actor_id
        )
        if actor_name is None:
            raise ChoiceCommentNotFoundError(command.comment_id)

        comment = await uow.services.comments.respond_to_choice(
            comment_id=command.comment_id,
            responder_id=actor.actor_id,
            responder_name=actor_name or actor.actor_id,
            selected=command.selected,
            free_text=command.free_text,
        )
        if not comment:
            raise InvalidChoiceResponseError()
        await commit(uow)
        await _refresh(uow, comment)
        return CommentResult(comment)


class DeleteCardCommentUseCase:
    async def execute(
        self,
        command: DeleteCardCommentCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> None:
        card_id = await uow.services.comment_card_id(command.comment_id)
        if card_id is None:
            raise CommentMutationNotFoundError(command.comment_id)
        await _require_card_access(
            uow,
            card_id,
            actor,
            write=True,
            denied_error=CommentMutationNotFoundError(command.comment_id),
        )

        deleted = await uow.services.comments.delete_comment(
            command.comment_id, actor.actor_id
        )
        if not deleted:
            raise CommentMutationNotFoundError(command.comment_id)
        if card_id:
            await _log_card_activity(
                uow.services,
                card_id,
                "comment_deleted",
                actor,
                {"comment_id": command.comment_id},
            )
        await commit(uow)


class CreateCardQuestionUseCase:
    async def execute(
        self,
        command: CreateCardQuestionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> QAResult:

        await _require_card_access(uow, command.card_id, actor, write=True)
        qa = await uow.services.qa.create_question(
            command.card_id, actor.actor_id, command.data
        )
        if not qa:
            raise CardNotFoundError(command.card_id)
        await _log_card_activity(
            uow.services,
            command.card_id,
            "question_added",
            actor,
            {"question": command.data.question[:100]},
        )
        await commit(uow)
        await _refresh(
            uow,
            qa,
            [
                "id",
                "card_id",
                "question",
                "answer",
                "asked_by",
                "answered_by",
                "created_at",
                "answered_at",
            ],
        )
        return QAResult(qa)


class AnswerCardQuestionUseCase:
    async def execute(
        self,
        command: AnswerCardQuestionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> QAResult:
        from okto_pulse.core.services import QASelfAnsweringNotAllowedError

        card_id = await uow.services.qa_card_id(command.qa_id)
        if card_id is None:
            raise QuestionNotFoundError(command.qa_id)
        await _require_card_access(
            uow,
            card_id,
            actor,
            write=True,
            denied_error=QuestionNotFoundError(command.qa_id),
        )
        try:
            qa = await uow.services.qa.answer_question(
                command.qa_id,
                actor.actor_id,
                command.data,
                actor_type="user",
                surface="rest",
            )
        except QASelfAnsweringNotAllowedError:
            await commit(uow)
            raise
        if not qa:
            raise QuestionNotFoundError(command.qa_id)
        await _log_card_activity(
            uow.services,
            qa.card_id,
            "question_answered",
            actor,
            {"qa_id": command.qa_id, "answer": command.data.answer[:100]},
        )
        await commit(uow)
        await _refresh(
            uow,
            qa,
            [
                "id",
                "card_id",
                "question",
                "answer",
                "asked_by",
                "answered_by",
                "created_at",
                "answered_at",
            ],
        )
        return QAResult(qa)


class DeleteCardQuestionUseCase:
    async def execute(
        self,
        command: DeleteCardQuestionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> None:
        card_id = await uow.services.qa_card_id(command.qa_id)
        if card_id is None:
            raise QuestionNotFoundError(command.qa_id)
        await _require_card_access(
            uow,
            card_id,
            actor,
            write=True,
            denied_error=QuestionNotFoundError(command.qa_id),
        )

        deleted = await uow.services.qa.delete_question(command.qa_id)
        if not deleted:
            raise QuestionNotFoundError(command.qa_id)
        if card_id:
            await _log_card_activity(
                uow.services,
                card_id,
                "question_deleted",
                actor,
                {"qa_id": command.qa_id},
            )
        await commit(uow)


class UploadCardAttachmentUseCase:
    async def execute(
        self,
        command: UploadCardAttachmentCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> AttachmentResult:

        await _require_card_access(
            uow,
            command.card_id,
            actor,
            board_id=command.board_id,
            write=True,
        )
        filename = validate_attachment_filename(command.filename)
        service = uow.services.attachments
        try:
            attachment = await service.upload_attachment(
                card_id=command.card_id,
                user_id=actor.actor_id,
                filename=filename,
                content=command.content,
                mime_type=command.mime_type,
            )
        except OSError as exc:
            raise AttachmentStorageError("upload") from exc
        if not attachment:
            raise CardNotFoundError(command.card_id)
        try:
            await _log_card_activity(
                uow.services,
                command.card_id,
                "attachment_uploaded",
                actor,
                {"filename": filename, "size": len(command.content)},
            )
            await commit(uow)
        except BaseException as exc:
            try:
                await service.discard_uploaded_attachment(attachment)
            except OSError as compensation_error:
                raise AttachmentStorageError("upload") from compensation_error
            if isinstance(exc, OSError):
                raise AttachmentStorageError("upload") from exc
            raise
        await _refresh(
            uow,
            attachment,
            [
                "id",
                "card_id",
                "filename",
                "original_filename",
                "mime_type",
                "size",
                "uploaded_by",
                "created_at",
            ],
        )
        return AttachmentResult(attachment)


class GetCardAttachmentUseCase:
    async def execute(
        self,
        command: GetCardAttachmentCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> AttachmentResult:

        await _require_card_access(
            uow,
            command.card_id,
            actor,
            board_id=command.board_id,
        )
        attachment = await uow.services.attachments.get_attachment(
            command.attachment_id
        )
        if not attachment or attachment.card_id != command.card_id:
            raise AttachmentNotFoundError(command.attachment_id)
        return AttachmentResult(attachment)


class DeleteCardAttachmentUseCase:
    async def execute(
        self,
        command: DeleteCardAttachmentCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> None:

        await _require_card_access(
            uow,
            command.card_id,
            actor,
            board_id=command.board_id,
            write=True,
        )
        service = uow.services.attachments
        attachment = await service.get_attachment(command.attachment_id)
        if not attachment or attachment.card_id != command.card_id:
            raise AttachmentNotFoundError(command.attachment_id)

        try:
            receipt = await service.delete_attachment(command.attachment_id)
        except OSError as exc:
            raise AttachmentStorageError("delete") from exc
        if not receipt:
            raise AttachmentNotFoundError(command.attachment_id)
        try:
            await _log_card_activity(
                uow.services,
                command.card_id,
                "attachment_deleted",
                actor,
                {"attachment_id": command.attachment_id},
            )
            await commit(uow)
        except BaseException as exc:
            try:
                await service.restore_deleted_attachment(receipt)
            except OSError as compensation_error:
                raise AttachmentStorageError("delete") from compensation_error
            if isinstance(exc, OSError):
                raise AttachmentStorageError("delete") from exc
            raise
