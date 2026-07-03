"""MCP-scoped ideation CRUD use cases (SaaS Refactor spec R01A MCP-FU6, family:
ideation).

PURITY GUARDRAIL (Codex-mandated, enforced by test_r01a_mcp_ideation_uow.py): this
module is transport-free. It MUST NOT import the MCP server/transport package nor any
server-side transport helper. The MCP adapter (server.py) keeps JSON parsing /
coercion, the board-scoping envelopes, and the MCP aggregation projections (the
``get_ideation`` refinements/specs/qa_items shape is built over ``uow.session`` while
the lazy ORM relationships are live — the Codex-approved get_spec_context precedent).

Ideation family traits (from the inventory): board-scope is ASYMMETRIC — present on
the read/aggregation + mutate-by-id tools (get/update/delete/get_context/...), absent
on create; create uses ``skip_ownership_check=True`` like the MCP spec create.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
    session_of,
)
from okto_pulse.core.services import IdeationService


# --- create (skip_ownership; adapter keeps the IdeationCreate build + envelope) -


class McpCreateIdeationCommand:
    __slots__ = ("board_id", "ideation_data")

    def __init__(self, board_id: str, ideation_data: Any) -> None:
        self.board_id = board_id
        self.ideation_data = ideation_data


class McpCreateIdeationResult:
    __slots__ = ("ideation",)

    def __init__(self, ideation: Any) -> None:
        self.ideation = ideation


class McpCreateIdeationUseCase:
    """Create an ideation (``skip_ownership_check=True``), single commit. A ``None``
    result maps to the adapter's ``"Failed to create ideation"``. The adapter keeps
    the ``IdeationCreate`` build (``\\n`` unescape + ``coerce_to_list_str`` labels)
    and the id/title/status/version envelope."""

    async def execute(
        self, command: McpCreateIdeationCommand, *, actor: ActorContext, uow: Any
    ) -> McpCreateIdeationResult:
        ideation = await IdeationService(session_of(uow)).create_ideation(
            command.board_id,
            actor.actor_id,
            command.ideation_data,
            skip_ownership_check=True,
        )
        await commit(uow)
        return McpCreateIdeationResult(ideation)


# --- get (board-scoped; adapter builds the lazy aggregation over uow.session) -


class McpGetIdeationCommand:
    __slots__ = ("ideation_id", "board_id")

    def __init__(self, ideation_id: str, board_id: str) -> None:
        self.ideation_id = ideation_id
        self.board_id = board_id


class McpGetIdeationResult:
    __slots__ = ("ideation",)

    def __init__(self, ideation: Any) -> None:
        self.ideation = ideation


class McpGetIdeationUseCase:
    """Board-scoped ideation fetch (read, no commit). A missing OR cross-board ideation
    is ``EntityNotFoundError`` -> the adapter's ``"Ideation not found"``. The
    refinements/specs/qa_items aggregation envelope is built by the adapter over
    ``uow.session`` (lazy ORM relationships) — the get_spec_context precedent."""

    async def execute(
        self, command: McpGetIdeationCommand, *, actor: ActorContext, uow: Any
    ) -> McpGetIdeationResult:
        ideation = await IdeationService(session_of(uow)).get_ideation(
            command.ideation_id
        )
        if not ideation or ideation.board_id != command.board_id:
            raise EntityNotFoundError("ideation", command.ideation_id)
        return McpGetIdeationResult(ideation)


# --- update (no board-scope, like update_spec) -------------------------------


class McpUpdateIdeationCommand:
    __slots__ = ("ideation_id", "payload")

    def __init__(self, ideation_id: str, payload: Any) -> None:
        self.ideation_id = ideation_id
        self.payload = payload


class McpUpdateIdeationResult:
    __slots__ = ("ideation",)

    def __init__(self, ideation: Any) -> None:
        self.ideation = ideation


class McpUpdateIdeationUseCase:
    """Update an ideation (write). A ``None`` result (missing) is
    ``EntityNotFoundError`` -> the adapter's ``"Ideation not found"``. NO board-scope
    (matches the legacy ``update_ideation``, which scopes only by id). The adapter
    builds the ``IdeationUpdate`` (non-empty fields + ``"No fields to update"``) and
    the id/title/status/version/complexity envelope."""

    async def execute(
        self, command: McpUpdateIdeationCommand, *, actor: ActorContext, uow: Any
    ) -> McpUpdateIdeationResult:
        ideation = await IdeationService(session_of(uow)).update_ideation(
            command.ideation_id, actor.actor_id, command.payload
        )
        if not ideation:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        return McpUpdateIdeationResult(ideation)


# --- delete (cascade refinements/Q&A; commit always for legacy parity) -------


class McpDeleteIdeationCommand:
    __slots__ = ("ideation_id",)

    def __init__(self, ideation_id: str) -> None:
        self.ideation_id = ideation_id


class McpDeleteIdeationResult:
    __slots__ = ("deleted",)

    def __init__(self, deleted: Any) -> None:
        self.deleted = deleted


class McpDeleteIdeationUseCase:
    """Delete an ideation and CASCADE its refinements/Q&A. Commits regardless (legacy
    parity — a missing ideation is a no-op commit); the adapter maps
    ``deleted=False`` -> ``"Ideation not found"``."""

    async def execute(
        self, command: McpDeleteIdeationCommand, *, actor: ActorContext, uow: Any
    ) -> McpDeleteIdeationResult:
        deleted = await IdeationService(session_of(uow)).delete_ideation(
            command.ideation_id, actor.actor_id
        )
        await commit(uow)
        return McpDeleteIdeationResult(deleted)


# --- snapshot / history (reads, no board-scope, no commit) -------------------


class McpGetIdeationSnapshotCommand:
    __slots__ = ("ideation_id", "version")

    def __init__(self, ideation_id: str, version: int) -> None:
        self.ideation_id = ideation_id
        self.version = version


class McpGetIdeationSnapshotResult:
    __slots__ = ("snapshot",)

    def __init__(self, snapshot: Any) -> None:
        self.snapshot = snapshot


class McpGetIdeationSnapshotUseCase:
    """Fetch the immutable ideation snapshot at a version (read, no commit). A ``None``
    result maps to the adapter's ``"Snapshot v<version> not found"``. No board-scope
    (legacy parity — the snapshot is keyed by ideation_id + version). The adapter
    coerces ``version`` to int and builds the snapshot envelope."""

    async def execute(
        self, command: McpGetIdeationSnapshotCommand, *, actor: ActorContext, uow: Any
    ) -> McpGetIdeationSnapshotResult:
        snapshot = await IdeationService(session_of(uow)).get_snapshot(
            command.ideation_id, command.version
        )
        return McpGetIdeationSnapshotResult(snapshot)


class McpGetIdeationHistoryCommand:
    __slots__ = ("ideation_id", "limit")

    def __init__(self, ideation_id: str, limit: int) -> None:
        self.ideation_id = ideation_id
        self.limit = limit


class McpGetIdeationHistoryResult:
    __slots__ = ("entries",)

    def __init__(self, entries: Any) -> None:
        self.entries = entries


class McpGetIdeationHistoryUseCase:
    """List an ideation's change history (read, no commit). No board-scope (legacy
    parity). The adapter coerces ``limit`` to int and builds the history envelope."""

    async def execute(
        self, command: McpGetIdeationHistoryCommand, *, actor: ActorContext, uow: Any
    ) -> McpGetIdeationHistoryResult:
        entries = await IdeationService(session_of(uow)).list_history(
            command.ideation_id, command.limit
        )
        return McpGetIdeationHistoryResult(entries)


# --- knowledge (board-scope via IdeationService + KB membership check) --------
# Two not-found cases: missing/cross-board ideation -> EntityNotFoundError ->
# "Ideation not found"; missing KB or KB not on this ideation -> kb_not_found ->
# "Knowledge base item not found". The adapter keeps _resolve_text_content, the
# IdeationKnowledgeCreate build and _serialize_knowledge_base.


class McpGetIdeationKnowledgeCommand:
    __slots__ = ("ideation_id", "board_id", "knowledge_id")

    def __init__(self, ideation_id: str, board_id: str, knowledge_id: str) -> None:
        self.ideation_id = ideation_id
        self.board_id = board_id
        self.knowledge_id = knowledge_id


class McpGetIdeationKnowledgeResult:
    __slots__ = ("kb", "kb_not_found")

    def __init__(self, kb: Any, *, kb_not_found: bool = False) -> None:
        self.kb = kb
        self.kb_not_found = kb_not_found


class McpGetIdeationKnowledgeUseCase:
    """Board-scoped ideation KB fetch (read, no commit). The adapter serializes the kb
    via ``_serialize_knowledge_base``."""

    async def execute(
        self, command: McpGetIdeationKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> McpGetIdeationKnowledgeResult:
        from okto_pulse.core.services import IdeationKnowledgeService

        session = session_of(uow)
        ideation = await IdeationService(session).get_ideation(command.ideation_id)
        if not ideation or ideation.board_id != command.board_id:
            raise EntityNotFoundError("ideation", command.ideation_id)
        kb = await IdeationKnowledgeService(session).get_knowledge(command.knowledge_id)
        if not kb or kb.ideation_id != command.ideation_id:
            return McpGetIdeationKnowledgeResult(None, kb_not_found=True)
        return McpGetIdeationKnowledgeResult(kb)


class McpAddIdeationKnowledgeCommand:
    __slots__ = ("ideation_id", "board_id", "kb_data")

    def __init__(self, ideation_id: str, board_id: str, kb_data: Any) -> None:
        self.ideation_id = ideation_id
        self.board_id = board_id
        self.kb_data = kb_data


class McpAddIdeationKnowledgeResult:
    __slots__ = ("kb",)

    def __init__(self, kb: Any) -> None:
        self.kb = kb


class McpAddIdeationKnowledgeUseCase:
    """Board-scoped ideation KB create (write). A ``None`` result -> the adapter's
    "Failed to create knowledge base item". The adapter resolves the content
    (``_resolve_text_content``), builds the ``IdeationKnowledgeCreate`` and serializes."""

    async def execute(
        self, command: McpAddIdeationKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> McpAddIdeationKnowledgeResult:
        from okto_pulse.core.services import IdeationKnowledgeService

        session = session_of(uow)
        ideation = await IdeationService(session).get_ideation(command.ideation_id)
        if not ideation or ideation.board_id != command.board_id:
            raise EntityNotFoundError("ideation", command.ideation_id)
        kb = await IdeationKnowledgeService(session).create_knowledge(
            command.ideation_id, actor.actor_id, command.kb_data
        )
        await commit(uow)
        return McpAddIdeationKnowledgeResult(kb)


class McpDeleteIdeationKnowledgeCommand:
    __slots__ = ("ideation_id", "board_id", "knowledge_id")

    def __init__(self, ideation_id: str, board_id: str, knowledge_id: str) -> None:
        self.ideation_id = ideation_id
        self.board_id = board_id
        self.knowledge_id = knowledge_id


class McpDeleteIdeationKnowledgeResult:
    __slots__ = ("kb_not_found",)

    def __init__(self, *, kb_not_found: bool = False) -> None:
        self.kb_not_found = kb_not_found


class McpDeleteIdeationKnowledgeUseCase:
    """Board-scoped ideation KB delete (write). Missing KB / KB not on this ideation ->
    ``kb_not_found`` (no delete). Commit only after a real delete."""

    async def execute(
        self, command: McpDeleteIdeationKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> McpDeleteIdeationKnowledgeResult:
        from okto_pulse.core.services import IdeationKnowledgeService

        session = session_of(uow)
        ideation = await IdeationService(session).get_ideation(command.ideation_id)
        if not ideation or ideation.board_id != command.board_id:
            raise EntityNotFoundError("ideation", command.ideation_id)
        kservice = IdeationKnowledgeService(session)
        kb = await kservice.get_knowledge(command.knowledge_id)
        if not kb or kb.ideation_id != command.ideation_id:
            return McpDeleteIdeationKnowledgeResult(kb_not_found=True)
        await kservice.delete_knowledge(command.knowledge_id)
        await commit(uow)
        return McpDeleteIdeationKnowledgeResult(kb_not_found=False)


# --- Q&A (ATOMIC activity-log, like the card family) -------------------------
# Each mutation logs a board-activity row via BoardService._log_activity in the
# SAME transaction as the IdeationQAService mutation, then commits atomically.


class McpAskIdeationChoiceQuestionCommand:
    __slots__ = ("board_id", "ideation_id", "data")

    def __init__(self, board_id: str, ideation_id: str, data: Any) -> None:
        self.board_id = board_id
        self.ideation_id = ideation_id
        self.data = data


class McpAskIdeationChoiceQuestionResult:
    __slots__ = ("qa", "ideation_not_found")

    def __init__(self, qa: Any, *, ideation_not_found: bool = False) -> None:
        self.qa = qa
        self.ideation_not_found = ideation_not_found


class McpAskIdeationChoiceQuestionUseCase:
    """Create a choice question + atomic ``ideation_choice_question_added`` activity
    log. A ``None`` create result -> ``ideation_not_found`` -> "Ideation not found".
    The adapter parses options_json / coerces options into the ``IdeationQACreate``."""

    async def execute(
        self, command: McpAskIdeationChoiceQuestionCommand, *, actor: ActorContext, uow: Any
    ) -> McpAskIdeationChoiceQuestionResult:
        from okto_pulse.core.services import BoardService
        from okto_pulse.core.services.main import IdeationQAService

        session = session_of(uow)
        qa = await IdeationQAService(session).create_question(
            command.ideation_id, actor.actor_id, command.data
        )
        if not qa:
            return McpAskIdeationChoiceQuestionResult(None, ideation_not_found=True)
        await BoardService(session)._log_activity(
            board_id=command.board_id,
            action="ideation_choice_question_added",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            details={
                "ideation_id": command.ideation_id,
                "question": command.data.question[:100],
                "option_count": len(command.data.choices),
            },
        )
        await commit(uow)
        return McpAskIdeationChoiceQuestionResult(qa)


class McpAnswerIdeationQuestionCommand:
    __slots__ = (
        "board_id", "ideation_id", "qa_id", "answer_payload", "answer_text",
        "selected_list",
    )

    def __init__(
        self,
        board_id: str,
        ideation_id: str,
        qa_id: str,
        *,
        answer_payload: Any,
        answer_text: str,
        selected_list: Any,
    ) -> None:
        self.board_id = board_id
        self.ideation_id = ideation_id
        self.qa_id = qa_id
        self.answer_payload = answer_payload
        self.answer_text = answer_text
        self.selected_list = selected_list


class McpAnswerIdeationQuestionResult:
    __slots__ = ("qa", "qa_not_found", "self_answer_error")

    def __init__(
        self,
        qa: Any,
        *,
        qa_not_found: bool = False,
        self_answer_error: Any = None,
    ) -> None:
        self.qa = qa
        self.qa_not_found = qa_not_found
        self.self_answer_error = self_answer_error


class McpAnswerIdeationQuestionUseCase:
    """Answer a Q&A item + atomic ``ideation_question_answered`` activity log.
    ``QASelfAnsweringNotAllowedError`` is caught + COMMITTED (legacy parity — the
    attempt is persisted) and returned for the adapter's ``{error, detail}``; a
    ``None`` result -> ``qa_not_found`` -> "Q&A item not found or invalid selection"."""

    async def execute(
        self, command: McpAnswerIdeationQuestionCommand, *, actor: ActorContext, uow: Any
    ) -> McpAnswerIdeationQuestionResult:
        from okto_pulse.core.services import BoardService, QASelfAnsweringNotAllowedError
        from okto_pulse.core.services.main import IdeationQAService

        session = session_of(uow)
        service = IdeationQAService(session)
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
            return McpAnswerIdeationQuestionResult(None, self_answer_error=exc)
        if not qa:
            return McpAnswerIdeationQuestionResult(None, qa_not_found=True)
        await BoardService(session)._log_activity(
            board_id=command.board_id,
            action="ideation_question_answered",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            details={
                "ideation_id": command.ideation_id,
                "qa_id": command.qa_id,
                "answer": (command.answer_text or "")[:100],
                "selected": command.selected_list,
            },
        )
        await commit(uow)
        return McpAnswerIdeationQuestionResult(qa)


class McpDeleteIdeationQuestionCommand:
    __slots__ = ("board_id", "ideation_id", "qa_id")

    def __init__(self, board_id: str, ideation_id: str, qa_id: str) -> None:
        self.board_id = board_id
        self.ideation_id = ideation_id
        self.qa_id = qa_id


class McpDeleteIdeationQuestionResult:
    __slots__ = ("qa_not_found",)

    def __init__(self, *, qa_not_found: bool = False) -> None:
        self.qa_not_found = qa_not_found


class McpDeleteIdeationQuestionUseCase:
    """Delete a Q&A item + atomic ``ideation_question_deleted`` activity log. A falsy
    delete -> ``qa_not_found`` -> "Q&A item not found" (no log, no commit)."""

    async def execute(
        self, command: McpDeleteIdeationQuestionCommand, *, actor: ActorContext, uow: Any
    ) -> McpDeleteIdeationQuestionResult:
        from okto_pulse.core.services import BoardService
        from okto_pulse.core.services.main import IdeationQAService

        session = session_of(uow)
        deleted = await IdeationQAService(session).delete_question(command.qa_id)
        if not deleted:
            return McpDeleteIdeationQuestionResult(qa_not_found=True)
        await BoardService(session)._log_activity(
            board_id=command.board_id,
            action="ideation_question_deleted",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            details={"ideation_id": command.ideation_id, "qa_id": command.qa_id},
        )
        await commit(uow)
        return McpDeleteIdeationQuestionResult()


# --- evaluate (scope write + complexity, board-scoped) -----------------------


class McpEvaluateIdeationCommand:
    __slots__ = ("ideation_id", "board_id", "scope")

    def __init__(self, ideation_id: str, board_id: str, scope: dict) -> None:
        self.ideation_id = ideation_id
        self.board_id = board_id
        self.scope = scope


class McpEvaluateIdeationResult:
    __slots__ = ("ideation",)

    def __init__(self, ideation: Any) -> None:
        self.ideation = ideation


class McpEvaluateIdeationUseCase:
    """Evaluate an ideation's complexity (write). When scope scores are provided,
    merge them into ``scope_assessment`` first (board-scoped, ``flag_modified`` —
    bypasses the draft-only edit guard since evaluation writes scores in
    'evaluating' status), then ``evaluate_complexity``, then commit. A missing /
    cross-board ideation -> ``EntityNotFoundError`` -> "Ideation not found". The
    adapter builds the scope dict (int + ``\\n``) and the envelope."""

    async def execute(
        self, command: McpEvaluateIdeationCommand, *, actor: ActorContext, uow: Any
    ) -> McpEvaluateIdeationResult:
        from okto_pulse.core.services.persistence_mutation import (
            mark_mutable_field_modified,
        )

        session = session_of(uow)
        service = IdeationService(session)

        if command.scope:
            ideation = await service.get_ideation(command.ideation_id)
            if not ideation or ideation.board_id != command.board_id:
                raise EntityNotFoundError("ideation", command.ideation_id)
            existing_scope = ideation.scope_assessment or {}
            existing_scope.update(command.scope)
            ideation.scope_assessment = existing_scope
            mark_mutable_field_modified(ideation, "scope_assessment")

        ideation = await service.evaluate_complexity(
            command.ideation_id, actor.actor_id
        )
        if not ideation:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        return McpEvaluateIdeationResult(ideation)


# --- stories (MCP VARIANT: board-scope + stateful permission via actor.permissions) -
# Codex opt-C: the story-state permission is evaluated against the MCP context's
# PermissionSet (actor.permissions), NOT the DB-loaded core _require_permissions
# (which would drift the authorization source). This is the transport-free twin of
# the server's _mcp_check_story_state_permission — PermissionSet.check_with_state,
# else the flat legacy-permission fallback (mirrors _mcp_check_permission). The
# adapter maps a non-None perm_err to _mcp_permission_error_response.


def _mcp_story_state_perm(
    permissions: Any, granular: str, legacy: str | None, story: Any
) -> str | None:
    from okto_pulse.core.services.permission_policy import check_story_state_permission
    from okto_pulse.core.services.story_permissions import story_state

    return check_story_state_permission(
        permissions,
        granular,
        legacy,
        story,
        story_state=story_state(
            story.status, archived=bool(getattr(story, "archived", False))
        ),
    )


class McpLinkStoryToIdeationCommand:
    __slots__ = ("board_id", "story_id", "ideation_id")

    def __init__(self, board_id: str, story_id: str, ideation_id: str) -> None:
        self.board_id = board_id
        self.story_id = story_id
        self.ideation_id = ideation_id


class McpLinkStoryToIdeationResult:
    __slots__ = ("link", "story", "not_found", "perm_err")

    def __init__(
        self,
        *,
        link: Any = None,
        story: Any = None,
        not_found: bool = False,
        perm_err: Any = None,
    ) -> None:
        self.link = link
        self.story = story
        self.not_found = not_found
        self.perm_err = perm_err


class McpLinkStoryToIdeationUseCase:
    """Link a Story to an Ideation (write, MCP VARIANT). Board-scope ``story.board_id
    == board_id`` (else ``not_found`` -> "Story or Ideation not found"); the
    ``story.links.ideation`` state permission via ``actor.permissions`` (perm_err ->
    adapter ``_mcp_permission_error_response``); ``link_story_to_ideation`` with
    ``mark_converted=True`` (legacy); ``ValueError`` propagates ({error}); a ``None``
    or cross-board link is ``not_found``. Re-fetches the story after commit."""

    async def execute(
        self, command: McpLinkStoryToIdeationCommand, *, actor: ActorContext, uow: Any
    ) -> McpLinkStoryToIdeationResult:
        from okto_pulse.core.services.permission_policy import Permissions
        from okto_pulse.core.services import StoryService

        session = session_of(uow)
        service = StoryService(session)
        story = await service.get_story(command.story_id)
        if not story or story.board_id != command.board_id:
            return McpLinkStoryToIdeationResult(not_found=True)
        perm_err = _mcp_story_state_perm(
            actor.permissions, "story.links.ideation", Permissions.SPECS_CREATE, story
        )
        if perm_err:
            return McpLinkStoryToIdeationResult(perm_err=perm_err)
        link = await service.link_story_to_ideation(
            command.story_id, command.ideation_id, actor.actor_id, mark_converted=True
        )
        if not link or link.board_id != command.board_id:
            return McpLinkStoryToIdeationResult(not_found=True)
        story = await service.get_story(command.story_id)
        await commit(uow)
        return McpLinkStoryToIdeationResult(link=link, story=story)


class McpConvertStoriesCommand:
    __slots__ = ("board_id", "story_ids", "data")

    def __init__(self, board_id: str, story_ids: list, data: Any) -> None:
        self.board_id = board_id
        self.story_ids = story_ids
        self.data = data


class McpConvertStoriesResult:
    __slots__ = (
        "ideation", "links", "propagated", "out_of_board", "perm_err",
        "board_not_found",
    )

    def __init__(
        self,
        *,
        ideation: Any = None,
        links: Any = None,
        propagated: Any = None,
        out_of_board: bool = False,
        perm_err: Any = None,
        board_not_found: bool = False,
    ) -> None:
        self.ideation = ideation
        self.links = links
        self.propagated = propagated
        self.out_of_board = out_of_board
        self.perm_err = perm_err
        self.board_not_found = board_not_found


class McpConvertStoriesUseCase:
    """Create or link an Ideation from selected Stories (write, MCP VARIANT). Per-story
    board-scope (``story.board_id == board_id``; else ``out_of_board`` -> "One or more
    Stories were not found in this board") + the per-story ``story.conversion.to_ideation``
    state permission via ``actor.permissions`` (perm_err -> adapter), BEFORE the
    conversion; ``convert_stories(skip_ownership_check=True)``; a ``None`` result is
    ``board_not_found`` -> "Board not found"; ``ValueError`` propagates. The adapter
    keeps the board-level permission, the coercion + the StoryConversionRequest build."""

    async def execute(
        self, command: McpConvertStoriesCommand, *, actor: ActorContext, uow: Any
    ) -> McpConvertStoriesResult:
        from okto_pulse.core.services.permission_policy import Permissions
        from okto_pulse.core.services import StoryService

        session = session_of(uow)
        service = StoryService(session)
        for story_id in command.story_ids:
            story = await service.get_story(story_id)
            if not story or story.board_id != command.board_id:
                return McpConvertStoriesResult(out_of_board=True)
            perm_err = _mcp_story_state_perm(
                actor.permissions,
                "story.conversion.to_ideation",
                Permissions.SPECS_CREATE,
                story,
            )
            if perm_err:
                return McpConvertStoriesResult(perm_err=perm_err)
        result = await service.convert_stories(
            command.board_id, actor.actor_id, command.data, skip_ownership_check=True
        )
        if not result:
            return McpConvertStoriesResult(board_not_found=True)
        ideation, links, propagated = result
        await commit(uow)
        return McpConvertStoriesResult(
            ideation=ideation, links=links, propagated=propagated
        )
