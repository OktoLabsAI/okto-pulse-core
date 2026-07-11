"""Refinement CRUD + Q&A + snapshot + knowledge use cases (SaaS Refactor spec
R01A REST-FU6-S3).

Transport-free reimplementations of every ``api/refinements.py`` endpoint that
still bound ``get_db`` — the refinement CRUD / move / derive-spec / history
family, the Q&A family, the snapshot readers and the knowledge-base family. Each
use case delegates to the EXISTING ``RefinementService`` / ``RefinementQAService``
/ ``RefinementKnowledgeService`` / ``IdeationService`` / ``SpecService`` method so
the SQL inline, the refinement transition + content/critical-context gates, the
activity logging, the history recording and the artifact propagation all stay in
the service; only the transport envelope (ideation/board-ownership 404,
not-found, mutate, commit, re-fetch) moves here.

Behavioral fidelity to the legacy endpoints:

* ``RefinementService.create_refinement`` returns ``None`` when the ideation is
  missing OR the board is not owned by the user → ``EntityNotFoundError`` with
  entity_type ``refinement_ideation_owner`` (the adapter maps it to the legacy
  ``"Ideation not found or board not owned by user"`` 404). Its ``ValueError``
  (ideation not 'done') propagates UNCAUGHT for the adapter to map to 400 — the
  refinement creation gate lives in the service.
* ``list_refinements`` first loads the parent ideation (missing →
  ``EntityNotFoundError("ideation")`` → "Ideation not found").
* ``update_refinement`` mirrors the legacy endpoint EXACTLY: it is NOT
  ValueError-wrapped, so the archived / non-draft ``ValueError`` raised inside the
  service propagates unchanged (the legacy adapter did not catch it).
* ``move_refinement`` / ``derive_spec`` / ``create_refinement`` ``ValueError``
  (transition / content / in_scope / done gates) propagates for the adapter to
  map to 400. The ``move_refinement`` critical-context authorization error raised
  inside the service propagates unchanged exactly as the legacy endpoint relied
  on (it caught only ``ValueError``).
* ``answer_question`` preserves the legacy self-answer semantics EXACTLY: on
  ``QASelfAnsweringNotAllowedError`` the transaction is COMMITTED (the
  authorization audit side-effect persists) and the error re-raised for the
  adapter to map to 403 with its ``{reason, message}`` detail.
* Reads do not commit; writes ``commit(uow)`` after the service mutation. The
  CRUD writes re-fetch via ``get_refinement`` (so the response carries the loaded
  relationships) and ``derive_spec`` re-fetches the new spec via
  ``SpecService.get_spec`` — exactly as the legacy endpoints did.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.scope import ActorScope, QueryScope


def _query_scope_for_actor(actor: ActorContext, *, board_id: str | None = None) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(target_board_id=board_id)


# ===========================================================================
# Refinement CRUD / move / derive-spec / history
# ===========================================================================


# --- create -----------------------------------------------------------------


class CreateRefinementCommand:
    __slots__ = ("ideation_id", "data")

    def __init__(self, ideation_id: str, data: Any) -> None:
        self.ideation_id = ideation_id
        self.data = data


class CreateRefinementResult:
    __slots__ = ("refinement",)

    def __init__(self, refinement: Any) -> None:
        self.refinement = refinement


class CreateRefinementUseCase:
    """Create a refinement for a 'done' ideation (write). ``create_refinement``
    returns ``None`` when the ideation is missing OR the board is not owned by the
    user → ``EntityNotFoundError("refinement_ideation_owner")`` (adapter → 404
    "Ideation not found or board not owned by user"); its ``ValueError`` (ideation
    not 'done') propagates for the adapter to map to 400. Re-fetches via
    ``get_refinement`` after commit so the relationships are loaded."""

    async def execute(
        self, command: CreateRefinementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateRefinementResult:
        service = uow.services.refinements
        refinement = await service.create_refinement(
            command.ideation_id,
            actor.actor_id,
            command.data,
            query_scope=_query_scope_for_actor(actor),
        )
        if not refinement:
            raise EntityNotFoundError("refinement_ideation_owner", command.ideation_id)
        await commit(uow)
        return CreateRefinementResult(await service.get_refinement(refinement.id))


# --- list -------------------------------------------------------------------


class ListRefinementsCommand:
    __slots__ = ("ideation_id", "status_filter", "include_archived")

    def __init__(
        self,
        ideation_id: str,
        *,
        status_filter: str | None = None,
        include_archived: bool = False,
    ) -> None:
        self.ideation_id = ideation_id
        self.status_filter = status_filter
        self.include_archived = include_archived


class ListRefinementsResult:
    __slots__ = ("refinements",)

    def __init__(self, refinements: list[Any]) -> None:
        self.refinements = refinements


class ListRefinementsUseCase:
    """List an ideation's refinements (read, no commit). A missing parent ideation
    is ``EntityNotFoundError("ideation")`` (adapter → 404 "Ideation not found")."""

    async def execute(
        self, command: ListRefinementsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListRefinementsResult:
        ideation = await uow.services.ideations.get_ideation(command.ideation_id)
        if not ideation:
            raise EntityNotFoundError("ideation", command.ideation_id)
        refinements = await uow.services.refinements.list_refinements(
            command.ideation_id,
            command.status_filter,
            include_archived=command.include_archived,
        )
        return ListRefinementsResult(refinements)


# --- get --------------------------------------------------------------------


class GetRefinementCommand:
    __slots__ = ("refinement_id",)

    def __init__(self, refinement_id: str) -> None:
        self.refinement_id = refinement_id


class GetRefinementResult:
    __slots__ = ("refinement",)

    def __init__(self, refinement: Any) -> None:
        self.refinement = refinement


class GetRefinementUseCase:
    """Fetch a refinement with nested data (read, no commit). 404 when missing."""

    async def execute(
        self, command: GetRefinementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetRefinementResult:
        refinement = await uow.services.refinements.get_refinement(
            command.refinement_id
        )
        if not refinement:
            raise EntityNotFoundError("refinement", command.refinement_id)
        return GetRefinementResult(refinement)


# --- update -----------------------------------------------------------------


class UpdateRefinementCommand:
    __slots__ = ("refinement_id", "data")

    def __init__(self, refinement_id: str, data: Any) -> None:
        self.refinement_id = refinement_id
        self.data = data


class UpdateRefinementResult:
    __slots__ = ("refinement",)

    def __init__(self, refinement: Any) -> None:
        self.refinement = refinement


class UpdateRefinementUseCase:
    """Update a refinement, bumping version on content changes (write). Mirrors the
    legacy endpoint EXACTLY: ``update_refinement``'s ``ValueError`` (archived /
    non-draft) is NOT caught and propagates unchanged; a ``None`` result is
    ``EntityNotFoundError("refinement")`` (→ 404 "Refinement not found"). Re-fetches
    via ``get_refinement`` after commit."""

    async def execute(
        self, command: UpdateRefinementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> UpdateRefinementResult:
        service = uow.services.refinements
        refinement = await service.update_refinement(
            command.refinement_id, actor.actor_id, command.data
        )
        if not refinement:
            raise EntityNotFoundError("refinement", command.refinement_id)
        await commit(uow)
        return UpdateRefinementResult(await service.get_refinement(command.refinement_id))


# --- move -------------------------------------------------------------------


class MoveRefinementCommand:
    __slots__ = ("refinement_id", "data")

    def __init__(self, refinement_id: str, data: Any) -> None:
        self.refinement_id = refinement_id
        self.data = data


class MoveRefinementResult:
    __slots__ = ("refinement",)

    def __init__(self, refinement: Any) -> None:
        self.refinement = refinement


class MoveRefinementUseCase:
    """Change a refinement's status (write). The transition / content (in_scope)
    gate ``ValueError`` raised by ``move_refinement`` propagates for the adapter to
    map to 400; the critical-context authorization error propagates unchanged (the
    legacy endpoint caught only ``ValueError``); a missing refinement is
    ``EntityNotFoundError("refinement")`` (→ 404). Re-fetches after commit."""

    async def execute(
        self, command: MoveRefinementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> MoveRefinementResult:
        service = uow.services.refinements
        refinement = await service.move_refinement(
            command.refinement_id, actor.actor_id, command.data
        )
        if not refinement:
            raise EntityNotFoundError("refinement", command.refinement_id)
        await commit(uow)
        return MoveRefinementResult(await service.get_refinement(command.refinement_id))


# --- delete -----------------------------------------------------------------


class DeleteRefinementCommand:
    __slots__ = ("refinement_id",)

    def __init__(self, refinement_id: str) -> None:
        self.refinement_id = refinement_id


class DeleteRefinementResult:
    __slots__ = ()


class DeleteRefinementUseCase:
    """Delete a refinement (write). ``delete_refinement`` returns ``False`` when the
    refinement is missing → ``EntityNotFoundError("refinement")`` (→ 404); commits
    after the delete."""

    async def execute(
        self, command: DeleteRefinementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteRefinementResult:
        deleted = await uow.services.refinements.delete_refinement(
            command.refinement_id, actor.actor_id
        )
        if not deleted:
            raise EntityNotFoundError("refinement", command.refinement_id)
        await commit(uow)
        return DeleteRefinementResult()


# --- derive spec ------------------------------------------------------------


class DeriveSpecFromRefinementCommand:
    __slots__ = ("refinement_id",)

    def __init__(self, refinement_id: str) -> None:
        self.refinement_id = refinement_id


class DeriveSpecFromRefinementResult:
    __slots__ = ("spec",)

    def __init__(self, spec: Any) -> None:
        self.spec = spec


class DeriveSpecFromRefinementUseCase:
    """Derive a Spec draft from a 'done' refinement (write). ``derive_spec``'s
    ``ValueError`` (refinement not 'done') propagates for the adapter to map to
    400; a ``None`` result is ``EntityNotFoundError("refinement")`` (→ 404).
    Re-fetches the new spec via ``SpecService.get_spec`` after commit, exactly as
    the legacy endpoint did."""

    async def execute(
        self, command: DeriveSpecFromRefinementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeriveSpecFromRefinementResult:
        spec = await uow.services.refinements.derive_spec(
            command.refinement_id,
            actor.actor_id,
            query_scope=_query_scope_for_actor(actor),
        )
        if not spec:
            raise EntityNotFoundError("refinement", command.refinement_id)
        await commit(uow)
        return DeriveSpecFromRefinementResult(await uow.services.specs.get_spec(spec.id))


# --- history ----------------------------------------------------------------


class ListRefinementHistoryCommand:
    __slots__ = ("refinement_id", "limit")

    def __init__(self, refinement_id: str, *, limit: int = 50) -> None:
        self.refinement_id = refinement_id
        self.limit = limit


class ListRefinementHistoryResult:
    __slots__ = ("history",)

    def __init__(self, history: list[Any]) -> None:
        self.history = history


class ListRefinementHistoryUseCase:
    """Read a refinement's change history (read, no commit)."""

    async def execute(
        self, command: ListRefinementHistoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListRefinementHistoryResult:
        history = await uow.services.refinements.list_history(
            command.refinement_id, command.limit
        )
        return ListRefinementHistoryResult(history)


# ===========================================================================
# Refinement Q&A
# ===========================================================================


# --- list -------------------------------------------------------------------


class ListRefinementQACommand:
    __slots__ = ("refinement_id",)

    def __init__(self, refinement_id: str) -> None:
        self.refinement_id = refinement_id


class ListRefinementQAResult:
    __slots__ = ("items",)

    def __init__(self, items: list[Any]) -> None:
        self.items = items


class ListRefinementQAUseCase:
    """List a refinement's Q&A items (read, no commit). Mirrors the legacy endpoint:
    no existence check — an unknown refinement simply yields an empty list."""

    async def execute(
        self, command: ListRefinementQACommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListRefinementQAResult:
        items = await uow.services.refinement_qa.list_qa(command.refinement_id)
        return ListRefinementQAResult(items)


# --- create question --------------------------------------------------------


class CreateRefinementQuestionCommand:
    __slots__ = ("refinement_id", "data")

    def __init__(self, refinement_id: str, data: Any) -> None:
        self.refinement_id = refinement_id
        self.data = data


class CreateRefinementQuestionResult:
    __slots__ = ("qa",)

    def __init__(self, qa: Any) -> None:
        self.qa = qa


class CreateRefinementQuestionUseCase:
    """Ask a question on a refinement (write). ``create_question`` returns ``None``
    when the refinement is missing → ``EntityNotFoundError("refinement")`` (→ 404
    "Refinement not found"); commits after the service mutation."""

    async def execute(
        self, command: CreateRefinementQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateRefinementQuestionResult:
        qa = await uow.services.refinement_qa.create_question(
            command.refinement_id, actor.actor_id, command.data
        )
        if not qa:
            raise EntityNotFoundError("refinement", command.refinement_id)
        await commit(uow)
        return CreateRefinementQuestionResult(qa)


# --- answer question --------------------------------------------------------


class AnswerRefinementQuestionCommand:
    __slots__ = ("qa_id", "data")

    def __init__(self, qa_id: str, data: Any) -> None:
        self.qa_id = qa_id
        self.data = data


class AnswerRefinementQuestionResult:
    __slots__ = ("qa",)

    def __init__(self, qa: Any) -> None:
        self.qa = qa


class AnswerRefinementQuestionUseCase:
    """Answer a refinement Q&A question (write). Calls ``answer_question`` with the
    REST surface/actor_type. Preserves the legacy self-answer semantics EXACTLY: on
    ``QASelfAnsweringNotAllowedError`` the transaction is COMMITTED (the
    authorization audit side-effect persists) and the error re-raised for the
    adapter to map to 403 with its ``{reason, message}`` detail; ``None`` (no such
    Q&A or nothing persisted) → ``EntityNotFoundError("refinement_qa")`` (404 "Q&A
    item not found"); a successful answer commits."""

    async def execute(
        self, command: AnswerRefinementQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> AnswerRefinementQuestionResult:
        from okto_pulse.core.services import QASelfAnsweringNotAllowedError

        service = uow.services.refinement_qa
        try:
            qa = await service.answer_question(
                command.qa_id, actor.actor_id, command.data,
                actor_type="user", surface="rest",
            )
        except QASelfAnsweringNotAllowedError:
            await commit(uow)
            raise
        if not qa:
            raise EntityNotFoundError("refinement_qa", command.qa_id)
        await commit(uow)
        return AnswerRefinementQuestionResult(qa)


# --- delete question --------------------------------------------------------


class DeleteRefinementQuestionCommand:
    __slots__ = ("qa_id",)

    def __init__(self, qa_id: str) -> None:
        self.qa_id = qa_id


class DeleteRefinementQuestionResult:
    __slots__ = ()


class DeleteRefinementQuestionUseCase:
    """Delete a refinement Q&A item (write). ``delete_question`` returns ``False``
    when the item is missing → ``EntityNotFoundError("refinement_qa")`` (404 "Q&A
    item not found"); commits after the delete."""

    async def execute(
        self, command: DeleteRefinementQuestionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteRefinementQuestionResult:
        deleted = await uow.services.refinement_qa.delete_question(command.qa_id)
        if not deleted:
            raise EntityNotFoundError("refinement_qa", command.qa_id)
        await commit(uow)
        return DeleteRefinementQuestionResult()


# ===========================================================================
# Refinement snapshots
# ===========================================================================


# --- list -------------------------------------------------------------------


class ListRefinementSnapshotsCommand:
    __slots__ = ("refinement_id",)

    def __init__(self, refinement_id: str) -> None:
        self.refinement_id = refinement_id


class ListRefinementSnapshotsResult:
    __slots__ = ("snapshots",)

    def __init__(self, snapshots: list[Any]) -> None:
        self.snapshots = snapshots


class ListRefinementSnapshotsUseCase:
    """List a refinement's version snapshots (read, no commit)."""

    async def execute(
        self, command: ListRefinementSnapshotsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListRefinementSnapshotsResult:
        snapshots = await uow.services.refinements.list_snapshots(
            command.refinement_id
        )
        return ListRefinementSnapshotsResult(snapshots)


# --- get --------------------------------------------------------------------


class GetRefinementSnapshotCommand:
    __slots__ = ("refinement_id", "version")

    def __init__(self, refinement_id: str, version: int) -> None:
        self.refinement_id = refinement_id
        self.version = version


class GetRefinementSnapshotResult:
    __slots__ = ("snapshot",)

    def __init__(self, snapshot: Any) -> None:
        self.snapshot = snapshot


class GetRefinementSnapshotUseCase:
    """Fetch one version snapshot of a refinement (read, no commit). A missing
    snapshot is ``EntityNotFoundError("refinement_snapshot", str(version))`` — the
    adapter maps it to the legacy ``f"Snapshot v{version} not found"`` 404."""

    async def execute(
        self, command: GetRefinementSnapshotCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetRefinementSnapshotResult:
        snapshot = await uow.services.refinements.get_snapshot(
            command.refinement_id, command.version
        )
        if not snapshot:
            raise EntityNotFoundError("refinement_snapshot", str(command.version))
        return GetRefinementSnapshotResult(snapshot)


# ===========================================================================
# Refinement knowledge base
# ===========================================================================


# --- list -------------------------------------------------------------------


class ListRefinementKnowledgeCommand:
    __slots__ = ("refinement_id",)

    def __init__(self, refinement_id: str) -> None:
        self.refinement_id = refinement_id


class ListRefinementKnowledgeResult:
    __slots__ = ("items",)

    def __init__(self, items: list[Any]) -> None:
        self.items = items


class ListRefinementKnowledgeUseCase:
    """List a refinement's knowledge base items without content (read, no commit).
    Mirrors the legacy endpoint: no existence check — an unknown refinement simply
    yields an empty list."""

    async def execute(
        self, command: ListRefinementKnowledgeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListRefinementKnowledgeResult:
        items = await uow.services.refinement_knowledge.list_knowledge(
            command.refinement_id
        )
        return ListRefinementKnowledgeResult(items)


# --- get --------------------------------------------------------------------


class GetRefinementKnowledgeCommand:
    __slots__ = ("refinement_id", "knowledge_id")

    def __init__(self, refinement_id: str, knowledge_id: str) -> None:
        self.refinement_id = refinement_id
        self.knowledge_id = knowledge_id


class GetRefinementKnowledgeResult:
    __slots__ = ("knowledge",)

    def __init__(self, knowledge: Any) -> None:
        self.knowledge = knowledge


class GetRefinementKnowledgeUseCase:
    """Fetch one knowledge base item with full content (read, no commit).
    ``EntityNotFoundError("refinement_knowledge")`` when the item is missing OR
    belongs to a different refinement — the adapter maps it to the legacy 404
    "Knowledge base item not found"."""

    async def execute(
        self, command: GetRefinementKnowledgeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetRefinementKnowledgeResult:
        kb = await uow.services.refinement_knowledge.get_knowledge(
            command.knowledge_id
        )
        if not kb or kb.refinement_id != command.refinement_id:
            raise EntityNotFoundError("refinement_knowledge", command.knowledge_id)
        return GetRefinementKnowledgeResult(kb)


# --- create -----------------------------------------------------------------


class CreateRefinementKnowledgeCommand:
    __slots__ = ("refinement_id", "data")

    def __init__(self, refinement_id: str, data: Any) -> None:
        self.refinement_id = refinement_id
        self.data = data


class CreateRefinementKnowledgeResult:
    __slots__ = ("knowledge",)

    def __init__(self, knowledge: Any) -> None:
        self.knowledge = knowledge


class CreateRefinementKnowledgeUseCase:
    """Add a knowledge base item to a refinement (write). ``create_knowledge``
    returns ``None`` when the refinement is missing →
    ``EntityNotFoundError("refinement")`` (adapter → 404 "Refinement not found");
    commits after the service mutation."""

    async def execute(
        self, command: CreateRefinementKnowledgeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateRefinementKnowledgeResult:
        kb = await uow.services.refinement_knowledge.create_knowledge(
            command.refinement_id, actor.actor_id, command.data
        )
        if not kb:
            raise EntityNotFoundError("refinement", command.refinement_id)
        await commit(uow)
        return CreateRefinementKnowledgeResult(kb)


# --- delete -----------------------------------------------------------------


class DeleteRefinementKnowledgeCommand:
    __slots__ = ("refinement_id", "knowledge_id")

    def __init__(self, refinement_id: str, knowledge_id: str) -> None:
        self.refinement_id = refinement_id
        self.knowledge_id = knowledge_id


class DeleteRefinementKnowledgeResult:
    __slots__ = ()


class DeleteRefinementKnowledgeUseCase:
    """Delete a knowledge base item from a refinement (write). ``delete_knowledge``
    returns ``False`` when the item is missing →
    ``EntityNotFoundError("refinement_knowledge")`` (404 "Knowledge base item not
    found"); commits after the delete. Mirrors the legacy endpoint, which keyed the
    delete on the ``knowledge_id`` alone (no refinement cross-check)."""

    async def execute(
        self, command: DeleteRefinementKnowledgeCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteRefinementKnowledgeResult:
        deleted = await uow.services.refinement_knowledge.delete_knowledge(
            command.knowledge_id
        )
        if not deleted:
            raise EntityNotFoundError("refinement_knowledge", command.knowledge_id)
        await commit(uow)
        return DeleteRefinementKnowledgeResult()
