"""MCP-scoped refinement CRUD use cases (SaaS Refactor spec R01A MCP-FU6, family:
refinement).

PURITY GUARDRAIL (Codex-mandated, enforced by test_r01a_mcp_refinement_uow.py): this
module is transport-free. It MUST NOT import the MCP server/transport package nor any
server-side transport helper. The MCP adapter (server.py) keeps JSON parsing /
coercion, the board-scoping envelopes, and the MCP aggregation projections
(get_refinement/get_refinement_context refinements/specs/qa_items in the adapter —
the get_spec_context precedent).

Refinement mirrors the ideation family (CRUD + reads + knowledge + Q&A) WITHOUT the
evaluate/stories tools. Every by-id path resolves the canonical refinement before
reading or writing and requires actor, command, and parent to share one board.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import require_authorization
from okto_pulse.core.application.use_cases.mutation_permissions import (
    transition_permission_requirement,
)
from okto_pulse.core.application.history_pagination import (
    history_page_metadata,
    validate_history_window,
    validate_snapshot_version,
)
from okto_pulse.core.domain.enums import RefinementStatus


def _in_board_scope(record: Any, board_id: str, actor: ActorContext) -> bool:
    """Require the MCP actor, command, and canonical parent to share a board."""
    return bool(
        record
        and actor.board_id == board_id
        and record.board_id == board_id
    )


# --- create (skip_ownership; ValueError propagates) --------------------------


class McpCreateRefinementCommand:
    __slots__ = ("ideation_id", "refinement_data")

    def __init__(self, ideation_id: str, refinement_data: Any) -> None:
        self.ideation_id = ideation_id
        self.refinement_data = refinement_data


class McpCreateRefinementResult:
    __slots__ = ("refinement", "resource_propagation")

    def __init__(self, refinement: Any, resource_propagation: Any = None) -> None:
        self.refinement = refinement
        self.resource_propagation = (
            resource_propagation
            if resource_propagation is not None
            else getattr(refinement, "resource_propagation", None)
        )


class McpCreateRefinementUseCase:
    """Create a refinement for a board-scoped DONE ideation, with one commit.

    The parent is resolved before the ``skip_ownership_check=True`` write and must
    belong to the board already authorized by the MCP adapter. Missing/cross-board
    parents return ``None`` for the adapter's legacy "Failed to create refinement
    (ideation not found)" envelope. ``ValueError`` (ideation not done / artifact
    propagation) still propagates for the adapter's ``{"error": str}`` envelope.
    """

    async def execute(
        self, command: McpCreateRefinementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpCreateRefinementResult:
        ideation = await uow.services.ideations.get_ideation(command.ideation_id)
        if not ideation or ideation.board_id != actor.board_id:
            return McpCreateRefinementResult(None)

        refinement = await uow.services.refinements.create_refinement(
            command.ideation_id,
            actor.actor_id,
            command.refinement_data,
            skip_ownership_check=True,
        )
        await commit(uow)
        return McpCreateRefinementResult(refinement)


# --- get (board-scoped; adapter owns presentation aggregation) ---------------


class McpGetRefinementCommand:
    __slots__ = ("refinement_id", "board_id")

    def __init__(self, refinement_id: str, board_id: str) -> None:
        self.refinement_id = refinement_id
        self.board_id = board_id


class McpGetRefinementResult:
    __slots__ = ("refinement",)

    def __init__(self, refinement: Any) -> None:
        self.refinement = refinement


class McpGetRefinementUseCase:
    """Board-scoped refinement fetch (read, no commit). A missing OR cross-board
    refinement is ``EntityNotFoundError`` -> the adapter's "Refinement not found".
    The specs/qa_items aggregation is built by the adapter through its presentation
    relationships) — reused by get_refinement_context."""

    async def execute(
        self, command: McpGetRefinementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpGetRefinementResult:
        refinement = await uow.services.refinements.get_refinement(
            command.refinement_id
        )
        if not _in_board_scope(refinement, command.board_id, actor):
            raise EntityNotFoundError("refinement", command.refinement_id)
        return McpGetRefinementResult(refinement)


# --- update -----------------------------------------------------------------


class McpUpdateRefinementCommand:
    __slots__ = ("refinement_id", "board_id", "payload")

    def __init__(self, refinement_id: str, board_id: str, payload: Any) -> None:
        self.refinement_id = refinement_id
        self.board_id = board_id
        self.payload = payload


class McpUpdateRefinementResult:
    __slots__ = ("refinement",)

    def __init__(self, refinement: Any) -> None:
        self.refinement = refinement


class McpUpdateRefinementUseCase:
    """Update a board-scoped refinement after a canonical-parent preflight."""

    async def execute(
        self, command: McpUpdateRefinementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpUpdateRefinementResult:
        service = uow.services.refinements
        existing = await service.get_refinement(command.refinement_id)
        if not _in_board_scope(existing, command.board_id, actor):
            raise EntityNotFoundError("refinement", command.refinement_id)

        refinement = await service.update_refinement(
            command.refinement_id, actor.actor_id, command.payload
        )
        if not refinement:
            raise EntityNotFoundError("refinement", command.refinement_id)
        await commit(uow)
        return McpUpdateRefinementResult(refinement)


# --- move (board-scope + old_status capture; like move_spec) -----------------


class McpMoveRefinementCommand:
    __slots__ = ("refinement_id", "board_id", "data")

    def __init__(self, refinement_id: str, board_id: str, data: Any) -> None:
        self.refinement_id = refinement_id
        self.board_id = board_id
        self.data = data


class McpMoveRefinementResult:
    __slots__ = ("refinement", "old_status")

    def __init__(self, refinement: Any, old_status: str) -> None:
        self.refinement = refinement
        self.old_status = old_status


class McpMoveRefinementUseCase:
    """Move a board-scoped refinement, capturing ``old_status`` BEFORE the move (the
    legacy envelope returns ``from_status``/``to_status``). A missing or cross-board
    refinement — and a ``None`` move result — both map to "Refinement not found" via
    ``EntityNotFoundError``. ``ValueError`` from ``move_refinement`` propagates for the
    adapter's ``{"error": str}``."""

    async def execute(
        self, command: McpMoveRefinementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpMoveRefinementResult:
        service = uow.services.refinements
        existing = await service.get_refinement(command.refinement_id)
        if not _in_board_scope(existing, command.board_id, actor):
            raise EntityNotFoundError("refinement", command.refinement_id)
        old_status = existing.status.value
        await require_authorization(
            actor,
            transition_permission_requirement(
                "refinement",
                existing.status,
                command.data.status,
                legacy_operation="specs:move",
            ),
            uow=uow,
            board_id=existing.board_id,
        )
        refinement = await service.move_refinement(
            command.refinement_id,
            actor.actor_id,
            command.data,
            actor_name=actor.actor_name,
        )
        if not refinement:
            raise EntityNotFoundError("refinement", command.refinement_id)
        if refinement.status == RefinementStatus.DONE:
            from okto_pulse.core.application.use_cases.research_decision_ledger import (
                ensure_research_decision_snapshot,
            )

            await ensure_research_decision_snapshot(
                refinement=refinement,
                uow=uow,
            )
        await commit(uow)
        return McpMoveRefinementResult(refinement, old_status)


# --- delete (cascade Q&A) ---------------------------------------------------


class McpDeleteRefinementCommand:
    __slots__ = ("refinement_id", "board_id")

    def __init__(self, refinement_id: str, board_id: str) -> None:
        self.refinement_id = refinement_id
        self.board_id = board_id


class McpDeleteRefinementResult:
    __slots__ = ("deleted", "takedown")

    def __init__(
        self,
        deleted: bool,
        takedown: dict[str, object] | None = None,
    ) -> None:
        self.deleted = deleted
        self.takedown = takedown


class McpDeleteRefinementUseCase:
    """Delete a board-scoped refinement and CASCADE its Q&A."""

    async def execute(
        self, command: McpDeleteRefinementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpDeleteRefinementResult:
        service = uow.services.refinements
        existing = await service.get_refinement(command.refinement_id)
        if not _in_board_scope(existing, command.board_id, actor):
            return McpDeleteRefinementResult(False)

        delete_result = await service.delete_refinement(
            command.refinement_id,
            actor.actor_id,
            return_receipt=True,
        )
        if not delete_result:
            return McpDeleteRefinementResult(False)
        receipt_serializer = getattr(delete_result, "to_dict", None)
        if not callable(receipt_serializer):
            raise RuntimeError("governed_delete_receipt_missing")
        takedown = receipt_serializer()
        if not isinstance(takedown, dict):
            raise RuntimeError("governed_delete_receipt_invalid")
        await commit(uow)
        return McpDeleteRefinementResult(True, takedown)


# --- snapshot / history -----------------------------------------------------


class McpGetRefinementSnapshotCommand:
    __slots__ = ("refinement_id", "board_id", "version")

    def __init__(self, refinement_id: str, board_id: str, version: object) -> None:
        self.refinement_id = refinement_id
        self.board_id = board_id
        self.version = validate_snapshot_version(version)


class McpGetRefinementSnapshotResult:
    __slots__ = ("snapshot",)

    def __init__(self, snapshot: Any) -> None:
        self.snapshot = snapshot


class McpGetRefinementSnapshotUseCase:
    """Fetch an immutable snapshot after validating its refinement parent."""

    async def execute(
        self, command: McpGetRefinementSnapshotCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpGetRefinementSnapshotResult:
        service = uow.services.refinements
        refinement = await service.get_refinement(command.refinement_id)
        if not _in_board_scope(refinement, command.board_id, actor):
            return McpGetRefinementSnapshotResult(None)

        snapshot = await service.get_snapshot(
            command.refinement_id, command.version
        )
        return McpGetRefinementSnapshotResult(snapshot)


class McpGetRefinementHistoryCommand:
    __slots__ = ("refinement_id", "board_id", "limit", "offset")

    def __init__(
        self,
        refinement_id: str,
        board_id: str,
        limit: object = 30,
        offset: object = 0,
    ) -> None:
        window = validate_history_window(limit, offset)
        self.refinement_id = refinement_id
        self.board_id = board_id
        self.limit = window.limit
        self.offset = window.offset


class McpGetRefinementHistoryResult:
    __slots__ = (
        "entries",
        "total",
        "has_more",
        "next_offset",
        "truncated",
    )

    def __init__(
        self,
        entries: Any,
        *,
        total: int | None = None,
        limit: int = 30,
        offset: int = 0,
    ) -> None:
        self.entries = entries
        exact_total = len(entries) if total is None else total
        metadata = history_page_metadata(
            total=exact_total,
            returned=len(entries),
            window=validate_history_window(limit, offset),
        )
        self.total = metadata.total
        self.has_more = metadata.has_more
        self.next_offset = metadata.next_offset
        self.truncated = metadata.truncated


class McpGetRefinementHistoryUseCase:
    """List a refinement's history after validating its canonical parent."""

    async def execute(
        self, command: McpGetRefinementHistoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpGetRefinementHistoryResult:
        service = uow.services.refinements
        refinement = await service.get_refinement(command.refinement_id)
        if not _in_board_scope(refinement, command.board_id, actor):
            raise EntityNotFoundError("refinement", command.refinement_id)

        entries = await service.list_history(
            command.refinement_id,
            limit=command.limit,
            offset=command.offset,
        )
        total = await service.count_history(command.refinement_id)
        return McpGetRefinementHistoryResult(
            entries,
            total=total,
            limit=command.limit,
            offset=command.offset,
        )


# --- knowledge (single RefinementKnowledgeService + KB membership) -----------
# Simpler than the ideation family: a single not-found case (KB missing OR KB not on
# this refinement -> kb_not_found -> "Knowledge base item not found"). The adapter
# keeps _resolve_text_content, the RefinementKnowledgeCreate build and the envelopes.


class McpGetRefinementKnowledgeCommand:
    __slots__ = ("refinement_id", "board_id", "knowledge_id")

    def __init__(self, refinement_id: str, board_id: str, knowledge_id: str) -> None:
        self.refinement_id = refinement_id
        self.board_id = board_id
        self.knowledge_id = knowledge_id


class McpGetRefinementKnowledgeResult:
    __slots__ = ("kb", "kb_not_found")

    def __init__(self, kb: Any, *, kb_not_found: bool = False) -> None:
        self.kb = kb
        self.kb_not_found = kb_not_found


class McpGetRefinementKnowledgeUseCase:
    """Refinement KB fetch by id with membership (read, no commit). The adapter builds
    the id/title/description/content/mime_type/created_at envelope."""

    async def execute(
        self, command: McpGetRefinementKnowledgeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpGetRefinementKnowledgeResult:
        refinement = await uow.services.refinements.get_refinement(
            command.refinement_id
        )
        if not _in_board_scope(refinement, command.board_id, actor):
            raise EntityNotFoundError("refinement", command.refinement_id)
        kb = await uow.services.refinement_knowledge.get_knowledge(
            command.knowledge_id
        )
        if not kb or kb.refinement_id != command.refinement_id:
            return McpGetRefinementKnowledgeResult(None, kb_not_found=True)
        return McpGetRefinementKnowledgeResult(kb)


class McpAddRefinementKnowledgeCommand:
    __slots__ = ("refinement_id", "board_id", "kb_data")

    def __init__(self, refinement_id: str, board_id: str, kb_data: Any) -> None:
        self.refinement_id = refinement_id
        self.board_id = board_id
        self.kb_data = kb_data


class McpAddRefinementKnowledgeResult:
    __slots__ = ("kb",)

    def __init__(self, kb: Any) -> None:
        self.kb = kb


class McpAddRefinementKnowledgeUseCase:
    """Refinement KB create (write). A ``None`` result -> the adapter's "Failed to
    create knowledge base item — refinement not found". The adapter resolves the
    content (``_resolve_text_content``), builds the ``RefinementKnowledgeCreate`` and
    the envelope."""

    async def execute(
        self, command: McpAddRefinementKnowledgeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpAddRefinementKnowledgeResult:
        refinement = await uow.services.refinements.get_refinement(
            command.refinement_id
        )
        if not _in_board_scope(refinement, command.board_id, actor):
            return McpAddRefinementKnowledgeResult(None)
        kb = await uow.services.refinement_knowledge.create_knowledge(
            command.refinement_id, actor.actor_id, command.kb_data
        )
        await commit(uow)
        return McpAddRefinementKnowledgeResult(kb)


class McpDeleteRefinementKnowledgeCommand:
    __slots__ = ("refinement_id", "board_id", "knowledge_id")

    def __init__(self, refinement_id: str, board_id: str, knowledge_id: str) -> None:
        self.refinement_id = refinement_id
        self.board_id = board_id
        self.knowledge_id = knowledge_id


class McpDeleteRefinementKnowledgeResult:
    __slots__ = ("kb_not_found",)

    def __init__(self, *, kb_not_found: bool = False) -> None:
        self.kb_not_found = kb_not_found


class McpDeleteRefinementKnowledgeUseCase:
    """Refinement KB delete (write). Missing KB / KB not on this refinement ->
    ``kb_not_found`` (no delete). Commit only after a real delete."""

    async def execute(
        self, command: McpDeleteRefinementKnowledgeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpDeleteRefinementKnowledgeResult:
        refinement = await uow.services.refinements.get_refinement(
            command.refinement_id
        )
        if not _in_board_scope(refinement, command.board_id, actor):
            raise EntityNotFoundError("refinement", command.refinement_id)
        service = uow.services.refinement_knowledge
        kb = await service.get_knowledge(command.knowledge_id)
        if not kb or kb.refinement_id != command.refinement_id:
            return McpDeleteRefinementKnowledgeResult(kb_not_found=True)
        await service.delete_knowledge(command.knowledge_id)
        await commit(uow)
        return McpDeleteRefinementKnowledgeResult(kb_not_found=False)


# --- Q&A (ATOMIC activity-log, like the card/ideation families) --------------


class McpAskRefinementChoiceQuestionCommand:
    __slots__ = ("board_id", "refinement_id", "data")

    def __init__(self, board_id: str, refinement_id: str, data: Any) -> None:
        self.board_id = board_id
        self.refinement_id = refinement_id
        self.data = data


class McpAskRefinementChoiceQuestionResult:
    __slots__ = ("qa", "refinement_not_found")

    def __init__(self, qa: Any, *, refinement_not_found: bool = False) -> None:
        self.qa = qa
        self.refinement_not_found = refinement_not_found


class McpAskRefinementChoiceQuestionUseCase:
    """Create a choice question + atomic ``refinement_choice_question_added`` activity
    log. A ``None`` create -> ``refinement_not_found`` -> "Refinement not found". The
    adapter parses options_json / coerces options into the ``RefinementQACreate``."""

    async def execute(
        self, command: McpAskRefinementChoiceQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpAskRefinementChoiceQuestionResult:
        refinement = await uow.services.refinements.get_refinement(
            command.refinement_id
        )
        if not _in_board_scope(refinement, command.board_id, actor):
            return McpAskRefinementChoiceQuestionResult(
                None,
                refinement_not_found=True,
            )
        qa = await uow.services.refinement_qa.create_question(
            command.refinement_id, actor.actor_id, command.data
        )
        if not qa:
            return McpAskRefinementChoiceQuestionResult(None, refinement_not_found=True)
        await uow.services.boards._log_activity(
            board_id=command.board_id,
            action="refinement_choice_question_added",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            details={
                "refinement_id": command.refinement_id,
                "question": command.data.question[:100],
                "option_count": len(command.data.choices),
            },
        )
        await commit(uow)
        return McpAskRefinementChoiceQuestionResult(qa)


class McpAnswerRefinementQuestionCommand:
    __slots__ = (
        "board_id", "refinement_id", "qa_id", "answer_payload", "answer_text",
        "selected_list",
    )

    def __init__(
        self,
        board_id: str,
        refinement_id: str,
        qa_id: str,
        *,
        answer_payload: Any,
        answer_text: str,
        selected_list: Any,
    ) -> None:
        self.board_id = board_id
        self.refinement_id = refinement_id
        self.qa_id = qa_id
        self.answer_payload = answer_payload
        self.answer_text = answer_text
        self.selected_list = selected_list


class McpAnswerRefinementQuestionResult:
    __slots__ = ("qa", "qa_not_found", "self_answer_error", "selection_error")

    def __init__(
        self,
        qa: Any,
        *,
        qa_not_found: bool = False,
        self_answer_error: Any = None,
        selection_error: Any = None,
    ) -> None:
        self.qa = qa
        self.qa_not_found = qa_not_found
        self.self_answer_error = self_answer_error
        self.selection_error = selection_error


class McpAnswerRefinementQuestionUseCase:
    """Answer a Q&A item + atomic ``refinement_question_answered`` activity log.
    ``QASelfAnsweringNotAllowedError`` is caught + COMMITTED (legacy parity) and
    returned for the adapter's ``{error, detail}``; a ``None`` result ->
    ``qa_not_found`` -> "Q&A item not found or invalid selection"."""

    async def execute(
        self, command: McpAnswerRefinementQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpAnswerRefinementQuestionResult:
        from okto_pulse.core.services import (
            QASelectionError,
            QASelfAnsweringNotAllowedError,
        )

        service = uow.services.refinement_qa
        qa_item = await service.get_question(command.qa_id)
        if not qa_item or qa_item.refinement_id != command.refinement_id:
            return McpAnswerRefinementQuestionResult(None, qa_not_found=True)
        refinement = await uow.services.refinements.get_refinement(
            qa_item.refinement_id
        )
        if not _in_board_scope(refinement, command.board_id, actor):
            return McpAnswerRefinementQuestionResult(None, qa_not_found=True)

        try:
            qa = await service.answer_question(
                command.qa_id,
                actor.actor_id,
                command.answer_payload,
                actor_type="agent",
                surface="mcp",
            )
        except QASelfAnsweringNotAllowedError as exc:
            await commit(uow)
            return McpAnswerRefinementQuestionResult(None, self_answer_error=exc)
        except QASelectionError as exc:
            return McpAnswerRefinementQuestionResult(None, selection_error=exc)
        if not qa:
            return McpAnswerRefinementQuestionResult(None, qa_not_found=True)
        await uow.services.boards._log_activity(
            board_id=refinement.board_id,
            action="refinement_question_answered",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            details={
                "refinement_id": qa_item.refinement_id,
                "qa_id": command.qa_id,
                "answer": (command.answer_text or "")[:100],
                "selected": command.selected_list,
            },
        )
        await commit(uow)
        return McpAnswerRefinementQuestionResult(qa)


class McpDeleteRefinementQuestionCommand:
    __slots__ = ("board_id", "refinement_id", "qa_id")

    def __init__(self, board_id: str, refinement_id: str, qa_id: str) -> None:
        self.board_id = board_id
        self.refinement_id = refinement_id
        self.qa_id = qa_id


class McpDeleteRefinementQuestionResult:
    __slots__ = ("qa_not_found",)

    def __init__(self, *, qa_not_found: bool = False) -> None:
        self.qa_not_found = qa_not_found


class McpDeleteRefinementQuestionUseCase:
    """Delete a Q&A item + atomic ``refinement_question_deleted`` activity log. A falsy
    delete -> ``qa_not_found`` -> "Q&A item not found" (no log, no commit)."""

    async def execute(
        self, command: McpDeleteRefinementQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpDeleteRefinementQuestionResult:
        service = uow.services.refinement_qa
        qa_item = await service.get_question(command.qa_id)
        if not qa_item or qa_item.refinement_id != command.refinement_id:
            return McpDeleteRefinementQuestionResult(qa_not_found=True)
        refinement = await uow.services.refinements.get_refinement(
            qa_item.refinement_id
        )
        if not _in_board_scope(refinement, command.board_id, actor):
            return McpDeleteRefinementQuestionResult(qa_not_found=True)

        deleted = await service.delete_question(command.qa_id)
        if not deleted:
            return McpDeleteRefinementQuestionResult(qa_not_found=True)
        await uow.services.boards._log_activity(
            board_id=refinement.board_id,
            action="refinement_question_deleted",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            details={"refinement_id": qa_item.refinement_id, "qa_id": command.qa_id},
        )
        await commit(uow)
        return McpDeleteRefinementQuestionResult()
