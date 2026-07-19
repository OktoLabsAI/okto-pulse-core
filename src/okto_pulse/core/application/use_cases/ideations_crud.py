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

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.scope import ActorScope, QueryScope


_WRITE_SHARE_PERMISSIONS = {"editor", "admin"}


def _query_scope_for_actor(
    actor: ActorContext,
    *,
    board_id: str | None = None,
    board_access_granted: bool = False,
) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(
        target_board_id=board_id,
        allowed_board_ids={board_id} if board_access_granted and board_id else None,
        require_ownership=not board_access_granted,
    )


async def _require_accessible_board(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
    *,
    write: bool = False,
) -> Any:
    board = await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions=_WRITE_SHARE_PERMISSIONS if write else None,
    )
    if board is None:
        raise EntityNotFoundError("board", board_id)
    return board


async def _require_accessible_ideation(
    uow: PulseUnitOfWork,
    ideation_id: str,
    actor: ActorContext,
    *,
    write: bool = False,
) -> Any:
    """Resolve an ideation without exposing records outside the actor's board.

    MCP actors already carry the authenticated board. REST actors do not, so the
    parent board must be owned by the user before any child read or write runs.
    """

    ideation = await uow.services.ideations.get_ideation(ideation_id)
    if ideation is None:
        raise EntityNotFoundError("ideation", ideation_id)
    if actor.board_id is not None and ideation.board_id != actor.board_id:
        raise EntityNotFoundError("ideation", ideation_id)
    if actor.source == "mcp" and actor.board_id == ideation.board_id:
        return ideation
    board = await load_accessible_board(
        uow,
        ideation.board_id,
        actor,
        allowed_share_permissions=_WRITE_SHARE_PERMISSIONS if write else None,
    )
    if board is None:
        raise EntityNotFoundError("ideation", ideation_id)
    return ideation


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
        self, command: CreateIdeationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateIdeationResult:
        await _require_accessible_board(uow, command.board_id, actor, write=True)
        service = uow.services.ideations
        ideation = await service.create_ideation(
            command.board_id,
            actor.actor_id,
            command.data,
            query_scope=_query_scope_for_actor(
                actor,
                board_id=command.board_id,
                board_access_granted=True,
            ),
        )
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
        self, command: ListIdeationsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListIdeationsResult:
        await _require_accessible_board(uow, command.board_id, actor)
        ideations = await uow.services.ideations.list_ideations(
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
    """Fetch an accessible ideation with nested data; denied and missing are 404."""

    async def execute(
        self, command: GetIdeationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetIdeationResult:
        ideation = await _require_accessible_ideation(uow, command.ideation_id, actor)
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
        self, command: UpdateIdeationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> UpdateIdeationResult:
        service = uow.services.ideations
        await _require_accessible_ideation(
            uow, command.ideation_id, actor, write=True
        )
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
        self, command: SetIdeationAmbiguityGateSkipCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> SetIdeationAmbiguityGateSkipResult:
        service = uow.services.ideations
        await _require_accessible_ideation(
            uow, command.ideation_id, actor, write=True
        )
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
        self, command: DeleteIdeationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteIdeationResult:
        await _require_accessible_ideation(
            uow, command.ideation_id, actor, write=True
        )
        deleted = await uow.services.ideations.delete_ideation(
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
        self, command: EvaluateComplexityCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> EvaluateComplexityResult:
        from okto_pulse.core.services.persistence_mutation import (
            mark_mutable_field_modified,
        )

        service = uow.services.ideations
        ideation = await _require_accessible_ideation(
            uow, command.ideation_id, actor, write=True
        )

        body = command.body
        scope = ideation.scope_assessment or {}
        for dim in ("domains", "ambiguity", "dependencies"):
            if dim in body:
                scope[dim] = int(body[dim])
            if f"{dim}_justification" in body:
                scope[f"{dim}_justification"] = body[f"{dim}_justification"]

        ideation.scope_assessment = scope
        mark_mutable_field_modified(ideation, "scope_assessment")
        # evaluate_complexity reads the ideation again through the persistence
        # port. Flush the detached-record delta first so REST and MCP classify the
        # same submitted scope values within this transaction.
        await uow.synchronize()

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
        self, command: DeriveSpecCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeriveSpecResult:
        ideation = await _require_accessible_ideation(
            uow, command.ideation_id, actor, write=True
        )
        spec = await uow.services.ideations.derive_spec(
            command.ideation_id,
            actor.actor_id,
            query_scope=_query_scope_for_actor(
                actor,
                board_id=ideation.board_id,
                board_access_granted=True,
            ),
        )
        if not spec:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        return DeriveSpecResult(await uow.services.specs.get_spec(spec.id))


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
    """List snapshots after a non-enumerable parent board-access preflight."""

    async def execute(
        self, command: ListIdeationSnapshotsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListIdeationSnapshotsResult:
        await _require_accessible_ideation(uow, command.ideation_id, actor)
        snapshots = await uow.services.ideations.list_snapshots(command.ideation_id)
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
        self, command: GetIdeationSnapshotCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetIdeationSnapshotResult:
        await _require_accessible_ideation(uow, command.ideation_id, actor)
        snapshot = await uow.services.ideations.get_snapshot(
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
        self, command: ListIdeationHistoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListIdeationHistoryResult:
        await _require_accessible_ideation(uow, command.ideation_id, actor)
        history = await uow.services.ideations.list_history(
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
    """List knowledge metadata after parent board-access preflight."""

    async def execute(
        self, command: ListIdeationKnowledgeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListIdeationKnowledgeResult:
        await _require_accessible_ideation(uow, command.ideation_id, actor)
        items = await uow.services.ideation_knowledge.list_knowledge(
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
        self, command: GetIdeationKnowledgeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetIdeationKnowledgeResult:
        await _require_accessible_ideation(uow, command.ideation_id, actor)
        kb = await uow.services.ideation_knowledge.get_knowledge(
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
        self, command: CreateIdeationKnowledgeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateIdeationKnowledgeResult:
        await _require_accessible_ideation(
            uow, command.ideation_id, actor, write=True
        )
        kb = await uow.services.ideation_knowledge.create_knowledge(
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
        self, command: DeleteIdeationKnowledgeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteIdeationKnowledgeResult:
        await _require_accessible_ideation(
            uow, command.ideation_id, actor, write=True
        )
        service = uow.services.ideation_knowledge
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
    """List Q&A after parent board-access preflight (read, no commit)."""

    async def execute(
        self, command: ListIdeationQACommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListIdeationQAResult:
        await _require_accessible_ideation(uow, command.ideation_id, actor)
        items = await uow.services.ideation_qa.list_qa(command.ideation_id)
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
        self, command: CreateIdeationQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateIdeationQuestionResult:
        await _require_accessible_ideation(
            uow, command.ideation_id, actor, write=True
        )
        qa = await uow.services.ideation_qa.create_question(
            command.ideation_id, actor.actor_id, command.data
        )
        if not qa:
            raise EntityNotFoundError("ideation", command.ideation_id)
        await commit(uow)
        return CreateIdeationQuestionResult(qa)


class AnswerIdeationQuestionCommand:
    __slots__ = ("ideation_id", "qa_id", "data")

    def __init__(self, ideation_id: str, qa_id: str, data: Any) -> None:
        self.ideation_id = ideation_id
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
    item not found"); the Q&A item must belong to the path parent before the writer
    runs; a successful answer commits."""

    async def execute(
        self, command: AnswerIdeationQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> AnswerIdeationQuestionResult:
        from okto_pulse.core.services import QASelfAnsweringNotAllowedError

        await _require_accessible_ideation(
            uow, command.ideation_id, actor, write=True
        )
        service = uow.services.ideation_qa
        existing = await service.get_question(command.qa_id)
        if existing is None or existing.ideation_id != command.ideation_id:
            raise EntityNotFoundError("ideation_qa", command.qa_id)
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
    __slots__ = ("ideation_id", "qa_id")

    def __init__(self, ideation_id: str, qa_id: str) -> None:
        self.ideation_id = ideation_id
        self.qa_id = qa_id


class DeleteIdeationQuestionResult:
    __slots__ = ()


class DeleteIdeationQuestionUseCase:
    """Delete an ideation Q&A item (write). ``delete_question`` returns ``False``
    when the item is missing → ``EntityNotFoundError("ideation_qa")`` (404 "Q&A item
    not found"); commits after the delete."""

    async def execute(
        self, command: DeleteIdeationQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteIdeationQuestionResult:
        await _require_accessible_ideation(
            uow, command.ideation_id, actor, write=True
        )
        service = uow.services.ideation_qa
        existing = await service.get_question(command.qa_id)
        if existing is None or existing.ideation_id != command.ideation_id:
            raise EntityNotFoundError("ideation_qa", command.qa_id)
        deleted = await service.delete_question(command.qa_id)
        if not deleted:
            raise EntityNotFoundError("ideation_qa", command.qa_id)
        await commit(uow)
        return DeleteIdeationQuestionResult()
