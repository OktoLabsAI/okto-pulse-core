"""Ideation CRUD + lifecycle + knowledge / Q&A use cases (SaaS Refactor spec R01A
REST-FU6-S1).

Transport-free reimplementations of every ``api/ideations.py`` endpoint that
still took a raw ``AsyncSession`` — create / list / get / update / ambiguity-gate
skip / delete / evaluate-complexity / derive-spec / snapshots / history, plus the
ideation knowledge base and Q&A surfaces. ``move_ideation`` already routes through
:class:`MoveIdeationUseCase` (``move_ideation.py``) and is NOT duplicated here; the
adapter only swaps its dependency to the UnitOfWork.

Each use case delegates to the EXISTING ``IdeationService`` /
``IdeationKnowledgeService`` / ``IdeationQAService`` / ``BoardService`` /
``SpecService`` method so payload, the status/ownership gates and the audit trail
are unchanged — the SQL inline in those services stays there. Not-found becomes a
typed :class:`EntityNotFoundError` the adapter maps to the legacy 404 detail; the
gate ``ValueError`` raised by ``set_ambiguity_gate_skip`` / ``update_ideation`` /
``evaluate_complexity`` / ``derive_spec`` propagates unchanged for the adapter to
map (or, where the legacy endpoint did not catch it, to surface as before). Reads
do NOT commit; writes ``commit(uow)`` after the service mutation.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
    session_of,
)
from okto_pulse.core.services import (
    BoardService,
    IdeationKnowledgeService,
    IdeationQAService,
    IdeationService,
    SpecService,
)


# --- create -----------------------------------------------------------------


class CreateIdeationCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


class CreateIdeationResult:
    __slots__ = ("ideation",)

    def __init__(self, ideation: Any) -> None:
        self.ideation = ideation


class CreateIdeationUseCase:
    """Create an ideation in a board (write). ``create_ideation`` returns ``None``
    when the board is missing/not owned → ``EntityNotFoundError("board")`` (adapter
    → 404 "Board not found or not owned by user"); commits and re-fetches, exactly
    as the legacy endpoint."""

    async def execute(
        self, command: CreateIdeationCommand, *, actor: ActorContext, uow: Any
    ) -> CreateIdeationResult:
        service = IdeationService(session_of(uow))
        ideation = await service.create_ideation(command.board_id, actor.actor_id, command.data)
        if not ideation:
            raise EntityNotFoundError("board", command.board_id)
        await commit(uow)
        return CreateIdeationResult(await service.get_ideation(ideation.id))


# --- list -------------------------------------------------------------------


class ListIdeationsCommand:
    __slots__ = ("board_id", "status_filter", "include_archived")

    def __init__(
        self, board_id: str, *, status_filter: str | None = None, include_archived: bool = False
    ) -> None:
        self.board_id = board_id
        self.status_filter = status_filter
        self.include_archived = include_archived


class ListIdeationsResult:
    __slots__ = ("ideations",)

    def __init__(self, ideations: list[Any]) -> None:
        self.ideations = ideations


class ListIdeationsUseCase:
    """List a board's ideations (read). 404 when the board is missing/not owned —
    the legacy endpoint resolves ``BoardService.get_board`` first, then lists."""

    async def execute(
        self, command: ListIdeationsCommand, *, actor: ActorContext, uow: Any
    ) -> ListIdeationsResult:
        session = session_of(uow)
        board = await BoardService(session).get_board(command.board_id, actor.actor_id)
        if not board:
            raise EntityNotFoundError("board", command.board_id)
        ideations = await IdeationService(session).list_ideations(
            command.board_id, command.status_filter, include_archived=command.include_archived
        )
        return ListIdeationsResult(ideations)


# --- get --------------------------------------------------------------------


class GetIdeationCommand:
    __slots__ = ("ideation_id",)

    def __init__(self, ideation_id: str) -> None:
        self.ideation_id = ideation_id


class GetIdeationResult:
    __slots__ = ("ideation",)

    def __init__(self, ideation: Any) -> None:
        self.ideation = ideation


class GetIdeationUseCase:
    """Fetch an ideation with nested data (read). 404 when missing."""

    async def execute(
        self, command: GetIdeationCommand, *, actor: ActorContext, uow: Any
    ) -> GetIdeationResult:
        ideation = await IdeationService(session_of(uow)).get_ideation(command.ideation_id)
        if not ideation:
            raise EntityNotFoundError("ideation", command.ideation_id)
        return GetIdeationResult(ideation)


# --- update -----------------------------------------------------------------


class UpdateIdeationCommand:
    __slots__ = ("ideation_id", "data")

    def __init__(self, ideation_id: str, data: Any) -> None:
        self.ideation_id = ideation_id
        self.data = data


class UpdateIdeationResult:
    __slots__ = ("ideation",)

    def __init__(self, ideation: Any) -> None:
        self.ideation = ideation


class UpdateIdeationUseCase:
    """Update an ideation, bumping version on content changes (write). ``None`` →
    ``EntityNotFoundError("ideation")`` (404); the draft-only / archived guard
    ``ValueError`` from ``update_ideation`` propagates unchanged — the legacy
    endpoint did NOT catch it, so it surfaces exactly as before."""

    async def execute(
        self, command: UpdateIdeationCommand, *, actor: ActorContext, uow: Any
    ) -> UpdateIdeationResult:
        service = IdeationService(session_of(uow))
        ideation = await service.update_ideation(
            command.ideation_id, actor.actor_id, command.data
        )
        if not ideation:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        return UpdateIdeationResult(await service.get_ideation(command.ideation_id))


# --- ambiguity-gate skip override (spec 2485780b) ---------------------------


class SetIdeationAmbiguityGateSkipCommand:
    __slots__ = ("ideation_id", "skip")

    def __init__(self, ideation_id: str, skip: bool) -> None:
        self.ideation_id = ideation_id
        self.skip = skip


class SetIdeationAmbiguityGateSkipResult:
    __slots__ = ("ideation",)

    def __init__(self, ideation: Any) -> None:
        self.ideation = ideation


class SetIdeationAmbiguityGateSkipUseCase:
    """Persist the per-ideation Max ambiguity gate skip override (write). Delegates
    to the dedicated ``set_ambiguity_gate_skip`` service path (source ``"rest"``)
    which works while the ideation is in evaluating status WITHOUT relaxing the
    draft-only guard and emits the auditable activity entry. The archived-ideation
    ``ValueError`` propagates for the adapter to map (400); ``None`` →
    ``EntityNotFoundError("ideation")`` (404); commits on success."""

    async def execute(
        self, command: SetIdeationAmbiguityGateSkipCommand, *, actor: ActorContext, uow: Any
    ) -> SetIdeationAmbiguityGateSkipResult:
        service = IdeationService(session_of(uow))
        ideation = await service.set_ambiguity_gate_skip(
            command.ideation_id, actor.actor_id, command.skip, source="rest"
        )
        if not ideation:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        return SetIdeationAmbiguityGateSkipResult(await service.get_ideation(command.ideation_id))


# --- delete -----------------------------------------------------------------


class DeleteIdeationCommand:
    __slots__ = ("ideation_id",)

    def __init__(self, ideation_id: str) -> None:
        self.ideation_id = ideation_id


class DeleteIdeationResult:
    __slots__ = ()


class DeleteIdeationUseCase:
    """Delete an ideation (write). ``delete_ideation`` returns ``False`` when the
    ideation is missing → ``EntityNotFoundError("ideation")`` (404); commits after
    the delete."""

    async def execute(
        self, command: DeleteIdeationCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteIdeationResult:
        deleted = await IdeationService(session_of(uow)).delete_ideation(
            command.ideation_id, actor.actor_id
        )
        if not deleted:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        return DeleteIdeationResult()


# --- evaluate complexity ----------------------------------------------------


class EvaluateComplexityCommand:
    __slots__ = ("ideation_id", "body")

    def __init__(self, ideation_id: str, body: dict[str, Any]) -> None:
        self.ideation_id = ideation_id
        self.body = body


class EvaluateComplexityResult:
    __slots__ = ("ideation",)

    def __init__(self, ideation: Any) -> None:
        self.ideation = ideation


class EvaluateComplexityUseCase:
    """Evaluate ideation complexity from scope scores (write). Reproduces the legacy
    endpoint EXACTLY: looks the ideation up (missing → ``EntityNotFoundError`` →
    404), folds the request ``body`` scores/justifications into ``scope_assessment``
    and marks the JSON column dirty (so evaluation can write scores while in
    'evaluating' status, bypassing the draft-only edit guard), then runs
    ``IdeationService.evaluate_complexity``. That method's status ``ValueError``
    (not 'evaluating') propagates unchanged — the legacy endpoint did NOT catch it.
    Commits and re-fetches."""

    async def execute(
        self, command: EvaluateComplexityCommand, *, actor: ActorContext, uow: Any
    ) -> EvaluateComplexityResult:
        from okto_pulse.core.services.persistence_mutation import (
            mark_mutable_field_modified,
        )

        service = IdeationService(session_of(uow))
        ideation = await service.get_ideation(command.ideation_id)
        if not ideation:
            raise EntityNotFoundError("ideation", command.ideation_id)

        body = command.body
        scope = ideation.scope_assessment or {}
        for dim in ("domains", "ambiguity", "dependencies"):
            if dim in body:
                scope[dim] = int(body[dim])
            if f"{dim}_justification" in body:
                scope[f"{dim}_justification"] = body[f"{dim}_justification"]

        ideation.scope_assessment = scope
        mark_mutable_field_modified(ideation, "scope_assessment")

        ideation = await service.evaluate_complexity(command.ideation_id, actor.actor_id)
        if not ideation:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        return EvaluateComplexityResult(await service.get_ideation(command.ideation_id))


# --- derive spec ------------------------------------------------------------


class DeriveSpecCommand:
    __slots__ = ("ideation_id",)

    def __init__(self, ideation_id: str) -> None:
        self.ideation_id = ideation_id


class DeriveSpecResult:
    __slots__ = ("spec",)

    def __init__(self, spec: Any) -> None:
        self.spec = spec


class DeriveSpecUseCase:
    """Create a spec draft from a done ideation (write). The
    ``derive_spec`` ``ValueError`` (ideation not 'done' / non-small complexity)
    propagates for the adapter to map (400); ``None`` →
    ``EntityNotFoundError("ideation")`` (404); commits, then re-fetches the spec via
    ``SpecService.get_spec`` exactly as the legacy endpoint."""

    async def execute(
        self, command: DeriveSpecCommand, *, actor: ActorContext, uow: Any
    ) -> DeriveSpecResult:
        session = session_of(uow)
        spec = await IdeationService(session).derive_spec(command.ideation_id, actor.actor_id)
        if not spec:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        return DeriveSpecResult(await SpecService(session).get_spec(spec.id))


# --- snapshots --------------------------------------------------------------


class ListIdeationSnapshotsCommand:
    __slots__ = ("ideation_id",)

    def __init__(self, ideation_id: str) -> None:
        self.ideation_id = ideation_id


class ListIdeationSnapshotsResult:
    __slots__ = ("snapshots",)

    def __init__(self, snapshots: list[Any]) -> None:
        self.snapshots = snapshots


class ListIdeationSnapshotsUseCase:
    """List an ideation's version snapshots (read, no commit). Mirrors the legacy
    endpoint: no existence check — an unknown ideation simply yields an empty list."""

    async def execute(
        self, command: ListIdeationSnapshotsCommand, *, actor: ActorContext, uow: Any
    ) -> ListIdeationSnapshotsResult:
        snapshots = await IdeationService(session_of(uow)).list_snapshots(command.ideation_id)
        return ListIdeationSnapshotsResult(snapshots)


class GetIdeationSnapshotCommand:
    __slots__ = ("ideation_id", "version")

    def __init__(self, ideation_id: str, version: int) -> None:
        self.ideation_id = ideation_id
        self.version = version


class GetIdeationSnapshotResult:
    __slots__ = ("snapshot",)

    def __init__(self, snapshot: Any) -> None:
        self.snapshot = snapshot


class GetIdeationSnapshotUseCase:
    """Fetch one version snapshot of an ideation (read, no commit). ``None`` →
    ``EntityNotFoundError("ideation_snapshot", version)`` so the adapter reproduces
    the legacy ``Snapshot v{version} not found`` 404."""

    async def execute(
        self, command: GetIdeationSnapshotCommand, *, actor: ActorContext, uow: Any
    ) -> GetIdeationSnapshotResult:
        snapshot = await IdeationService(session_of(uow)).get_snapshot(
            command.ideation_id, command.version
        )
        if not snapshot:
            raise EntityNotFoundError("ideation_snapshot", str(command.version))
        return GetIdeationSnapshotResult(snapshot)


# --- history ----------------------------------------------------------------


class ListIdeationHistoryCommand:
    __slots__ = ("ideation_id", "limit")

    def __init__(self, ideation_id: str, *, limit: int = 50) -> None:
        self.ideation_id = ideation_id
        self.limit = limit


class ListIdeationHistoryResult:
    __slots__ = ("history",)

    def __init__(self, history: list[Any]) -> None:
        self.history = history


class ListIdeationHistoryUseCase:
    """Read an ideation's change history (read, no commit)."""

    async def execute(
        self, command: ListIdeationHistoryCommand, *, actor: ActorContext, uow: Any
    ) -> ListIdeationHistoryResult:
        history = await IdeationService(session_of(uow)).list_history(
            command.ideation_id, command.limit
        )
        return ListIdeationHistoryResult(history)


# ===========================================================================
# Ideation knowledge base. Each use case wraps the EXISTING
# IdeationKnowledgeService method; the per-endpoint 404 detail lives in the
# adapter, keyed off the typed EntityNotFoundError entity_type (``ideation`` →
# "Ideation not found", ``ideation_knowledge`` → "Knowledge base item not found").
# ===========================================================================


class ListIdeationKnowledgeCommand:
    __slots__ = ("ideation_id",)

    def __init__(self, ideation_id: str) -> None:
        self.ideation_id = ideation_id


class ListIdeationKnowledgeResult:
    __slots__ = ("items",)

    def __init__(self, items: list[Any]) -> None:
        self.items = items


class ListIdeationKnowledgeUseCase:
    """List an ideation's knowledge base items without content (read, no commit).
    Mirrors the legacy endpoint: no existence check — an unknown ideation yields an
    empty list."""

    async def execute(
        self, command: ListIdeationKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> ListIdeationKnowledgeResult:
        items = await IdeationKnowledgeService(session_of(uow)).list_knowledge(
            command.ideation_id
        )
        return ListIdeationKnowledgeResult(items)


class GetIdeationKnowledgeCommand:
    __slots__ = ("ideation_id", "knowledge_id")

    def __init__(self, ideation_id: str, knowledge_id: str) -> None:
        self.ideation_id = ideation_id
        self.knowledge_id = knowledge_id


class GetIdeationKnowledgeResult:
    __slots__ = ("knowledge",)

    def __init__(self, knowledge: Any) -> None:
        self.knowledge = knowledge


class GetIdeationKnowledgeUseCase:
    """Fetch one knowledge base item with full content (read, no commit).
    ``EntityNotFoundError("ideation_knowledge")`` when the item is missing OR
    belongs to a different ideation — the adapter maps it to the legacy 404."""

    async def execute(
        self, command: GetIdeationKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> GetIdeationKnowledgeResult:
        kb = await IdeationKnowledgeService(session_of(uow)).get_knowledge(
            command.knowledge_id
        )
        if not kb or kb.ideation_id != command.ideation_id:
            raise EntityNotFoundError("ideation_knowledge", command.knowledge_id)
        return GetIdeationKnowledgeResult(kb)


class CreateIdeationKnowledgeCommand:
    __slots__ = ("ideation_id", "data")

    def __init__(self, ideation_id: str, data: Any) -> None:
        self.ideation_id = ideation_id
        self.data = data


class CreateIdeationKnowledgeResult:
    __slots__ = ("knowledge",)

    def __init__(self, knowledge: Any) -> None:
        self.knowledge = knowledge


class CreateIdeationKnowledgeUseCase:
    """Add a knowledge base item to an ideation (write). ``create_knowledge``
    returns ``None`` when the ideation is missing → ``EntityNotFoundError("ideation")``
    (404 "Ideation not found"); commits after the service mutation."""

    async def execute(
        self, command: CreateIdeationKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> CreateIdeationKnowledgeResult:
        kb = await IdeationKnowledgeService(session_of(uow)).create_knowledge(
            command.ideation_id, actor.actor_id, command.data
        )
        if not kb:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        return CreateIdeationKnowledgeResult(kb)


class DeleteIdeationKnowledgeCommand:
    __slots__ = ("ideation_id", "knowledge_id")

    def __init__(self, ideation_id: str, knowledge_id: str) -> None:
        self.ideation_id = ideation_id
        self.knowledge_id = knowledge_id


class DeleteIdeationKnowledgeResult:
    __slots__ = ()


class DeleteIdeationKnowledgeUseCase:
    """Delete a knowledge base item from an ideation (write). The same upfront
    ownership check as the legacy endpoint: missing item OR belonging to a different
    ideation → ``EntityNotFoundError("ideation_knowledge")`` (404 "Knowledge base
    item not found"); a ``delete_knowledge`` ``False`` (raced delete) maps to the
    same 404; commits after the delete."""

    async def execute(
        self, command: DeleteIdeationKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteIdeationKnowledgeResult:
        service = IdeationKnowledgeService(session_of(uow))
        kb = await service.get_knowledge(command.knowledge_id)
        if not kb or kb.ideation_id != command.ideation_id:
            raise EntityNotFoundError("ideation_knowledge", command.knowledge_id)
        deleted = await service.delete_knowledge(command.knowledge_id)
        if not deleted:
            raise EntityNotFoundError("ideation_knowledge", command.knowledge_id)
        await commit(uow)
        return DeleteIdeationKnowledgeResult()


# ===========================================================================
# Ideation Q&A. Each use case wraps the EXISTING IdeationQAService method; the
# per-endpoint 404 detail lives in the adapter, keyed off the typed
# EntityNotFoundError entity_type (``ideation`` → "Ideation not found",
# ``ideation_qa`` → "Q&A item not found").
# ===========================================================================


class ListIdeationQACommand:
    __slots__ = ("ideation_id",)

    def __init__(self, ideation_id: str) -> None:
        self.ideation_id = ideation_id


class ListIdeationQAResult:
    __slots__ = ("items",)

    def __init__(self, items: list[Any]) -> None:
        self.items = items


class ListIdeationQAUseCase:
    """List an ideation's Q&A items (read, no commit). Mirrors the legacy endpoint:
    no existence check — an unknown ideation yields an empty list."""

    async def execute(
        self, command: ListIdeationQACommand, *, actor: ActorContext, uow: Any
    ) -> ListIdeationQAResult:
        items = await IdeationQAService(session_of(uow)).list_qa(command.ideation_id)
        return ListIdeationQAResult(items)


class CreateIdeationQuestionCommand:
    __slots__ = ("ideation_id", "data")

    def __init__(self, ideation_id: str, data: Any) -> None:
        self.ideation_id = ideation_id
        self.data = data


class CreateIdeationQuestionResult:
    __slots__ = ("qa",)

    def __init__(self, qa: Any) -> None:
        self.qa = qa


class CreateIdeationQuestionUseCase:
    """Ask a question on an ideation (write). ``create_question`` returns ``None``
    when the ideation is missing → ``EntityNotFoundError("ideation")`` (404
    "Ideation not found"); commits after the service mutation."""

    async def execute(
        self, command: CreateIdeationQuestionCommand, *, actor: ActorContext, uow: Any
    ) -> CreateIdeationQuestionResult:
        qa = await IdeationQAService(session_of(uow)).create_question(
            command.ideation_id, actor.actor_id, command.data
        )
        if not qa:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        return CreateIdeationQuestionResult(qa)


class AnswerIdeationQuestionCommand:
    __slots__ = ("qa_id", "data")

    def __init__(self, qa_id: str, data: Any) -> None:
        self.qa_id = qa_id
        self.data = data


class AnswerIdeationQuestionResult:
    __slots__ = ("qa",)

    def __init__(self, qa: Any) -> None:
        self.qa = qa


class AnswerIdeationQuestionUseCase:
    """Answer an ideation Q&A question (write). Calls ``answer_question`` with the
    REST surface/actor_type. Preserves the legacy self-answer semantics EXACTLY: on
    ``QASelfAnsweringNotAllowedError`` the transaction is COMMITTED (the
    authorization audit side-effect persists) and the error re-raised for the
    adapter to map to 403 with its ``{reason, message}`` detail; ``None`` (no such
    Q&A or nothing persisted) → ``EntityNotFoundError("ideation_qa")`` (404 "Q&A
    item not found"); a successful answer commits."""

    async def execute(
        self, command: AnswerIdeationQuestionCommand, *, actor: ActorContext, uow: Any
    ) -> AnswerIdeationQuestionResult:
        from okto_pulse.core.services import QASelfAnsweringNotAllowedError

        service = IdeationQAService(session_of(uow))
        try:
            qa = await service.answer_question(
                command.qa_id, actor.actor_id, command.data,
                actor_type="user", surface="rest",
            )
        except QASelfAnsweringNotAllowedError:
            await commit(uow)
            raise
        if not qa:
            raise EntityNotFoundError("ideation_qa", command.qa_id)
        await commit(uow)
        return AnswerIdeationQuestionResult(qa)


class DeleteIdeationQuestionCommand:
    __slots__ = ("qa_id",)

    def __init__(self, qa_id: str) -> None:
        self.qa_id = qa_id


class DeleteIdeationQuestionResult:
    __slots__ = ()


class DeleteIdeationQuestionUseCase:
    """Delete an ideation Q&A item (write). ``delete_question`` returns ``False``
    when the item is missing → ``EntityNotFoundError("ideation_qa")`` (404 "Q&A item
    not found"); commits after the delete."""

    async def execute(
        self, command: DeleteIdeationQuestionCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteIdeationQuestionResult:
        deleted = await IdeationQAService(session_of(uow)).delete_question(command.qa_id)
        if not deleted:
            raise EntityNotFoundError("ideation_qa", command.qa_id)
        await commit(uow)
        return DeleteIdeationQuestionResult()
