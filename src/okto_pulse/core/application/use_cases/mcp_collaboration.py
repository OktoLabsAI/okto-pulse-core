"""MCP collaboration-content use cases for AF35-S4.

The MCP layer owns authentication, permission checks, parameter coercion and
JSON serialization. These use cases own the relational work over the MCP Unit of
Work path so wrappers do not open ``get_db_for_mcp`` directly.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    commit,
)
from okto_pulse.core.application.use_cases._service_payload import (
    payload,
    payload_choices,
)
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog


@dataclass(frozen=True)
class McpPayloadResult:
    payload: Any


async def _log_card_activity(
    services: ApplicationServiceCatalog,
    board_id: str,
    card_id: str,
    action: str,
    actor: ActorContext,
    details: dict[str, Any] | None = None,
) -> None:
    await services.boards._log_activity(
        board_id=board_id,
        card_id=card_id,
        action=action,
        actor_type="agent",
        actor_id=actor.actor_id,
        actor_name=actor.actor_name,
        details=details,
    )


def _qa_payload(qa: Any) -> dict[str, Any]:
    return {
        "id": qa.id,
        "question": qa.question,
        "answer": getattr(qa, "answer", None),
        "asked_by": getattr(qa, "asked_by", None),
        "answered_by": getattr(qa, "answered_by", None),
    }


def _topic_payload(topic: Any) -> dict[str, Any]:
    return {
        "id": topic.id,
        "board_id": topic.board_id,
        "name": topic.name,
        "description": topic.description,
        "archived": bool(topic.archived),
        "story_count": getattr(topic, "story_count", 0),
        "active_count": getattr(
            topic, "active_count", getattr(topic, "story_count", 0)
        ),
        "archived_count": getattr(topic, "archived_count", 0),
        "total_associated_count": getattr(
            topic,
            "total_associated_count",
            getattr(topic, "story_count", 0),
        ),
        "created_by": topic.created_by,
        "created_at": topic.created_at.isoformat(),
        "updated_at": topic.updated_at.isoformat(),
    }


def _topic_impact(topic: Any) -> dict[str, Any]:
    return {
        "topic_id": topic.id,
        "story_count": getattr(topic, "story_count", 0),
        "active_count": getattr(
            topic, "active_count", getattr(topic, "story_count", 0)
        ),
        "archived_count": getattr(topic, "archived_count", 0),
        "total_associated_count": getattr(
            topic,
            "total_associated_count",
            getattr(topic, "story_count", 0),
        ),
    }


def _topic_operation_error_payload(exc: Exception) -> dict[str, Any]:
    return {
        "success": False,
        "error": str(exc),
        "code": getattr(exc, "code", "topic_operation_error"),
        **getattr(exc, "details", {}),
    }


@dataclass(frozen=True)
class McpAskQuestionCommand:
    board_id: str
    target_type: str
    parent_id: str
    question: str


class McpAskQuestionUseCase:
    async def execute(
        self, command: McpAskQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        if command.target_type == "card":

            qa = await uow.services.qa.create_question(
                command.parent_id,
                actor.actor_id,
                payload(question=command.question),
            )
            if not qa:
                return McpPayloadResult(
                    {"error": "Failed to create question (card not found)"}
                )
            await _log_card_activity(
                uow.services,
                command.board_id,
                command.parent_id,
                "question_added",
                actor,
                {"question": command.question[:100]},
            )
            await commit(uow)
            return McpPayloadResult(
                {
                    "success": True,
                    "qa": {
                        "id": qa.id,
                        "question": qa.question,
                        "asked_by": qa.asked_by,
                    },
                }
            )

        if command.target_type in ("ideation", "refinement", "spec"):

            if command.target_type == "ideation":

                action = "ideation_question_added"
                not_found = "Ideation not found"
                key = "ideation_id"
            elif command.target_type == "refinement":

                action = "refinement_question_added"
                not_found = "Refinement not found"
                key = "refinement_id"
            else:

                action = "spec_question_added"
                not_found = "Spec not found"
                key = "spec_id"

            qa = await uow.services.qa.create_question(
                command.parent_id,
                actor.actor_id,
                payload(question=command.question),
            )
            if not qa:
                return McpPayloadResult({"error": not_found})
            await uow.services.boards._log_activity(
                board_id=command.board_id,
                action=action,
                actor_type="agent",
                actor_id=actor.actor_id,
                actor_name=actor.actor_name,
                details={key: command.parent_id, "question": command.question[:100]},
            )
            await commit(uow)
            return McpPayloadResult(
                {
                    "success": True,
                    "qa": {
                        "id": qa.id,
                        "question": qa.question,
                        "asked_by": qa.asked_by,
                    },
                }
            )


        qa = await uow.services.sprint_qa.create_question(
            command.parent_id,
            actor.actor_id,
            command.question,
        )
        await commit(uow)
        if not qa:
            return McpPayloadResult({"error": "Sprint not found"})
        return McpPayloadResult(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "asked_by": qa.asked_by,
                },
            }
        )


@dataclass(frozen=True)
class McpAnswerQuestionCommand:
    board_id: str
    qa_id: str
    answer: str


class McpAnswerQuestionUseCase:
    async def execute(
        self, command: McpAnswerQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:
        from okto_pulse.core.services import QASelfAnsweringNotAllowedError

        try:
            qa = await uow.services.qa.answer_question(
                command.qa_id,
                actor.actor_id,
                payload(answer=command.answer),
                actor_type="agent",
                surface="mcp",
            )
        except QASelfAnsweringNotAllowedError as exc:
            await commit(uow)
            return McpPayloadResult({"error": exc.reason, "detail": str(exc)})
        if not qa:
            return McpPayloadResult({"error": "Failed to answer question (not found)"})
        await _log_card_activity(
            uow.services,
            command.board_id,
            qa.card_id,
            "question_answered",
            actor,
            {"qa_id": command.qa_id, "answer": command.answer[:100]},
        )
        await commit(uow)
        return McpPayloadResult(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "answer": qa.answer,
                    "answered_by": qa.answered_by,
                },
            }
        )


@dataclass(frozen=True)
class McpDeleteQuestionCommand:
    qa_id: str


class McpDeleteQuestionUseCase:
    async def execute(
        self, command: McpDeleteQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        deleted = await uow.services.qa.delete_question(command.qa_id)
        await commit(uow)
        if not deleted:
            return McpPayloadResult({"error": "Q&A item not found"})
        return McpPayloadResult({"success": True})


@dataclass(frozen=True)
class McpAddCommentCommand:
    board_id: str
    card_id: str
    content: str


class McpAddCommentUseCase:
    async def execute(
        self, command: McpAddCommentCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        comment = await uow.services.comments.create_comment(
            command.card_id,
            actor.actor_id,
            payload(content=command.content),
        )
        if not comment:
            return McpPayloadResult({"error": "Failed to create comment (card not found)"})
        await _log_card_activity(
            uow.services,
            command.board_id,
            command.card_id,
            "comment_added",
            actor,
            {"content": command.content[:100]},
        )
        await commit(uow)
        return McpPayloadResult(
            {
                "success": True,
                "comment": {
                    "id": comment.id,
                    "content": comment.content,
                    "author_id": comment.author_id,
                    "created_at": comment.created_at.isoformat(),
                },
            }
        )


@dataclass(frozen=True)
class McpAddChoiceCommentCommand:
    board_id: str
    card_id: str
    question: str
    comment_type: str
    choices: list[Any]
    allow_free_text: bool


class McpAddChoiceCommentUseCase:
    async def execute(
        self, command: McpAddChoiceCommentCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        comment_type = (
            command.comment_type
            if command.comment_type in ("choice", "multi_choice")
            else "choice"
        )
        comment = await uow.services.comments.create_comment(
            command.card_id,
            actor.actor_id,
            payload(
                content=command.question,
                comment_type=comment_type,
                choices=payload_choices(command.choices),
                allow_free_text=command.allow_free_text,
            ),
        )
        if not comment:
            return McpPayloadResult(
                {"error": "Failed to create choice comment (card not found)"}
            )
        await _log_card_activity(
            uow.services,
            command.board_id,
            command.card_id,
            "choice_comment_added",
            actor,
            {
                "question": command.question[:100],
                "option_count": len(command.choices),
                "type": command.comment_type,
            },
        )
        await commit(uow)
        return McpPayloadResult(
            {
                "success": True,
                "comment": {
                    "id": comment.id,
                    "comment_type": comment.comment_type,
                    "content": comment.content,
                    "choices": comment.choices,
                    "allow_free_text": comment.allow_free_text,
                    "responses": [],
                },
            }
        )


@dataclass(frozen=True)
class McpRespondToChoiceCommand:
    comment_id: str
    selected_ids: list[str]
    free_text: str


class McpRespondToChoiceUseCase:
    async def execute(
        self, command: McpRespondToChoiceCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        comment = await uow.services.comments.respond_to_choice(
            comment_id=command.comment_id,
            responder_id=actor.actor_id,
            responder_name=actor.actor_name or "",
            selected=command.selected_ids,
            free_text=command.free_text or None,
        )
        if not comment:
            return McpPayloadResult(
                {"error": "Choice comment not found or invalid selection"}
            )
        await commit(uow)
        return McpPayloadResult(
            {
                "success": True,
                "comment": {
                    "id": comment.id,
                    "comment_type": comment.comment_type,
                    "content": comment.content,
                    "choices": comment.choices,
                    "responses": comment.responses,
                },
            }
        )


@dataclass(frozen=True)
class McpGetChoiceResponsesCommand:
    comment_id: str


class McpGetChoiceResponsesUseCase:
    async def execute(
        self, command: McpGetChoiceResponsesCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        comment = await uow.services.comments.get_comment(command.comment_id)
        await commit(uow)
        if not comment or comment.comment_type == "text":
            return McpPayloadResult({"error": "Choice comment not found"})
        return McpPayloadResult(
            {
                "id": comment.id,
                "comment_type": comment.comment_type,
                "question": comment.content,
                "choices": comment.choices,
                "allow_free_text": comment.allow_free_text,
                "responses": comment.responses or [],
                "response_count": len(comment.responses or []),
            }
        )


@dataclass(frozen=True)
class McpListCommentsCommand:
    board_id: str
    card_id: str


class McpListCommentsUseCase:
    async def execute(
        self, command: McpListCommentsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        card = await uow.services.cards.get_card(command.card_id)
        await commit(uow)
        if not card or card.board_id != command.board_id:
            return McpPayloadResult({"error": "Card not found"})

        rows: list[dict[str, Any]] = []
        for comment in card.comments:
            item: dict[str, Any] = {
                "id": comment.id,
                "content": comment.content,
                "author_id": comment.author_id,
                "comment_type": getattr(comment, "comment_type", "text") or "text",
                "created_at": comment.created_at.isoformat(),
                "updated_at": comment.updated_at.isoformat(),
            }
            if item["comment_type"] != "text":
                item["choices"] = getattr(comment, "choices", None)
                item["responses"] = getattr(comment, "responses", None) or []
                item["allow_free_text"] = getattr(comment, "allow_free_text", False)
            rows.append(item)
        return McpPayloadResult(rows)


@dataclass(frozen=True)
class McpUpdateCommentCommand:
    board_id: str
    comment_id: str
    content: str


class McpUpdateCommentUseCase:
    async def execute(
        self, command: McpUpdateCommentCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        comment = await uow.services.comments.update_comment(
            command.comment_id,
            actor.actor_id,
            payload(content=command.content),
        )
        if not comment:
            return McpPayloadResult(
                {"error": "Comment not found or not owned by this agent"}
            )
        await _log_card_activity(
            uow.services,
            command.board_id,
            comment.card_id,
            "comment_updated",
            actor,
            {"content": command.content[:100]},
        )
        await commit(uow)
        await uow.reload(comment)
        return McpPayloadResult(
            {
                "success": True,
                "comment": {
                    "id": comment.id,
                    "content": comment.content,
                    "updated_at": (
                        comment.updated_at.isoformat()
                        if comment.updated_at
                        else None
                    ),
                },
            }
        )


@dataclass(frozen=True)
class McpDeleteCommentCommand:
    board_id: str
    comment_id: str


class McpDeleteCommentUseCase:
    async def execute(
        self, command: McpDeleteCommentCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        comment_service = uow.services.comments
        comment = await comment_service.get_comment(command.comment_id)
        card_id = comment.card_id if comment else None
        deleted = await comment_service.delete_comment(
            command.comment_id,
            actor.actor_id,
        )
        if not deleted:
            return McpPayloadResult(
                {"error": "Comment not found or not owned by this agent"}
            )
        if card_id:
            await _log_card_activity(
                uow.services,
                command.board_id,
                card_id,
                "comment_deleted",
                actor,
            )
        await commit(uow)
        return McpPayloadResult({"success": True})


@dataclass(frozen=True)
class McpUploadAttachmentCommand:
    card_id: str
    filename: str
    content: bytes
    mime_type: str


class McpUploadAttachmentUseCase:
    async def execute(
        self, command: McpUploadAttachmentCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        attachment = await uow.services.attachments.upload_attachment(
            card_id=command.card_id,
            user_id=actor.actor_id,
            filename=command.filename,
            content=command.content,
            mime_type=command.mime_type,
        )
        await commit(uow)
        if not attachment:
            return McpPayloadResult(
                {"error": "Failed to upload attachment (card not found)"}
            )
        return McpPayloadResult(
            {
                "success": True,
                "attachment": {
                    "id": attachment.id,
                    "filename": attachment.original_filename,
                    "mime_type": attachment.mime_type,
                    "size": attachment.size,
                },
            }
        )


@dataclass(frozen=True)
class McpListAttachmentsCommand:
    board_id: str
    card_id: str


class McpListAttachmentsUseCase:
    async def execute(
        self, command: McpListAttachmentsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        card = await uow.services.cards.get_card(command.card_id)
        await commit(uow)
        if not card or card.board_id != command.board_id:
            return McpPayloadResult({"error": "Card not found"})
        return McpPayloadResult(
            [
                {
                    "id": attachment.id,
                    "filename": attachment.original_filename,
                    "mime_type": attachment.mime_type,
                    "size": attachment.size,
                    "uploaded_by": attachment.uploaded_by,
                    "created_at": attachment.created_at.isoformat(),
                }
                for attachment in card.attachments
            ]
        )


@dataclass(frozen=True)
class McpDeleteAttachmentCommand:
    attachment_id: str


class McpDeleteAttachmentUseCase:
    async def execute(
        self, command: McpDeleteAttachmentCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:

        deleted = await uow.services.attachments.delete_attachment(
            command.attachment_id
        )
        await commit(uow)
        if not deleted:
            return McpPayloadResult({"error": "Attachment not found"})
        return McpPayloadResult({"success": True})


@dataclass(frozen=True)
class McpCreateTopicCommand:
    board_id: str
    name: str
    description: str


class McpCreateTopicUseCase:
    async def execute(
        self, command: McpCreateTopicCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:
        from okto_pulse.core.services import TopicOperationError

        try:
            topic = await uow.services.stories.create_topic(
                command.board_id,
                actor.actor_id,
                payload(
                    name=command.name,
                    description=command.description or None,
                ),
                skip_ownership_check=True,
            )
        except TopicOperationError as exc:
            return McpPayloadResult(_topic_operation_error_payload(exc))
        await commit(uow)
        if not topic:
            return McpPayloadResult({"error": "Board not found"})
        return McpPayloadResult(
            {
                "success": True,
                "message": f"Topic '{topic.name}' created.",
                "topic": _topic_payload(topic),
                "impact": _topic_impact(topic),
            }
        )


@dataclass(frozen=True)
class McpUpdateTopicCommand:
    board_id: str
    topic_id: str
    update_data: dict[str, Any]


class McpUpdateTopicUseCase:
    async def execute(
        self, command: McpUpdateTopicCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:
        from okto_pulse.core.services import TopicOperationError

        story_service = uow.services.stories
        topic = await story_service.get_topic(command.topic_id)
        if not topic or topic.board_id != command.board_id:
            return McpPayloadResult(
                {
                    "success": False,
                    "error": "Topic not found",
                    "code": "topic_not_found",
                }
            )
        try:
            updated = await story_service.update_topic(
                command.topic_id,
                actor.actor_id,
                payload(**command.update_data),
            )
        except TopicOperationError as exc:
            return McpPayloadResult(_topic_operation_error_payload(exc))
        await commit(uow)
        if not updated:
            return McpPayloadResult(
                {
                    "success": False,
                    "error": "Topic not found",
                    "code": "topic_not_found",
                }
            )
        return McpPayloadResult(
            {
                "success": True,
                "message": f"Topic '{updated.name}' updated.",
                "topic": _topic_payload(updated),
                "impact": _topic_impact(updated),
            }
        )


@dataclass(frozen=True)
class McpSetTopicArchivedCommand:
    board_id: str
    topic_id: str
    archived: bool


class McpSetTopicArchivedUseCase:
    async def execute(
        self, command: McpSetTopicArchivedCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:
        from okto_pulse.core.services import TopicOperationError

        story_service = uow.services.stories
        topic = await story_service.get_topic(command.topic_id)
        if not topic or topic.board_id != command.board_id:
            return McpPayloadResult(
                {
                    "success": False,
                    "error": "Topic not found",
                    "code": "topic_not_found",
                }
            )
        try:
            updated = await story_service.update_topic(
                command.topic_id,
                actor.actor_id,
                payload(archived=command.archived),
            )
        except TopicOperationError as exc:
            return McpPayloadResult(_topic_operation_error_payload(exc))
        await commit(uow)
        if not updated:
            return McpPayloadResult(
                {
                    "success": False,
                    "error": "Topic not found",
                    "code": "topic_not_found",
                }
            )
        if command.archived:
            message = (
                "Topic archived. Stories remain unchanged and visible through "
                "All topics/search unless the Story itself is archived."
            )
        else:
            message = f"Topic '{updated.name}' restored."
        return McpPayloadResult(
            {
                "success": True,
                "message": message,
                "topic": _topic_payload(updated),
                "impact": _topic_impact(updated),
            }
        )


@dataclass(frozen=True)
class McpDeleteTopicCommand:
    board_id: str
    topic_id: str


class McpDeleteTopicUseCase:
    async def execute(
        self, command: McpDeleteTopicCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:
        from okto_pulse.core.services import TopicOperationError

        story_service = uow.services.stories
        topic = await story_service.get_topic(command.topic_id)
        if not topic or topic.board_id != command.board_id:
            return McpPayloadResult(
                {
                    "success": False,
                    "error": "Topic not found",
                    "code": "topic_not_found",
                }
            )
        try:
            deleted = await story_service.delete_topic(
                command.topic_id,
                actor.actor_id,
            )
        except TopicOperationError as exc:
            return McpPayloadResult(_topic_operation_error_payload(exc))
        await commit(uow)
        if not deleted:
            return McpPayloadResult(
                {
                    "success": False,
                    "error": "Topic not found",
                    "code": "topic_not_found",
                }
            )
        return McpPayloadResult(
            {
                "success": True,
                "message": f"Topic '{deleted.name}' deleted.",
                "deleted_topic_id": command.topic_id,
                "impact": {
                    "topic_id": command.topic_id,
                    "active_count": 0,
                    "archived_count": 0,
                    "total_associated_count": 0,
                },
            }
        )


@dataclass(frozen=True)
class McpMergeTopicsCommand:
    board_id: str
    source_topic_id: str
    target_topic_id: str


class McpMergeTopicsUseCase:
    async def execute(
        self, command: McpMergeTopicsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:
        from okto_pulse.core.services import TopicOperationError

        story_service = uow.services.stories
        source = await story_service.get_topic(command.source_topic_id)
        if not source or source.board_id != command.board_id:
            return McpPayloadResult(
                {
                    "success": False,
                    "error": "Topic not found",
                    "code": "topic_not_found",
                }
            )
        try:
            result = await story_service.merge_topics(
                command.source_topic_id,
                command.target_topic_id,
                actor.actor_id,
            )
        except TopicOperationError as exc:
            return McpPayloadResult(_topic_operation_error_payload(exc))
        await commit(uow)
        if not result:
            return McpPayloadResult(
                {
                    "success": False,
                    "error": "Topic not found",
                    "code": "topic_not_found",
                }
            )
        return McpPayloadResult(
            {
                "success": True,
                "message": (
                    f"Merged Topic '{result['source'].name}' into "
                    f"'{result['target'].name}'. Story-Ideation links were "
                    "preserved and the source Topic was archived."
                ),
                "source": _topic_payload(result["source"]),
                "target": _topic_payload(result["target"]),
                "impact": {
                    "source_topic_id": command.source_topic_id,
                    "target_topic_id": command.target_topic_id,
                    "moved_count": result["moved_count"],
                    "active_count": result["active_count"],
                    "archived_count": result["archived_count"],
                    "target_total_before": result["target_total_before"],
                    "target_total_after": result["target_total_after"],
                },
            }
        )
