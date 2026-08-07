"""Sprint CRUD use cases (SaaS Refactor spec R01A REST-FU7-S2).

Transport-free reimplementations of every ``api/sprints.py`` endpoint that still
bound ``get_db`` — list-by-board / create / list-by-spec / get / update / move /
delete / submit-evaluation / assign-tasks / unassign-tasks / history / suggest.
Each use case delegates to the EXISTING ``SprintService`` methods so the SQL
inline, the sprint state-machine gates (draft→active→review→closed), the test /
evaluation / threshold coverage gates, the activity logging and the history
recording all stay in the service; only the transport envelope (not-found 404,
mutate, commit, re-fetch) moves here.

Authorization and behavioral fidelity:

* Every read resolves the real parent board and accepts the owner or any board
  share. Every write requires the owner or an ``editor``/``admin`` share. Missing
  and inaccessible resources deliberately use the same not-found result.
* Parent identifiers are containment checked before a service call. A verified
  share is then represented by a one-board ``QueryScope`` for create operations;
  this does not weaken the default owner-only service scope.
* ``SprintOperationError`` (a ``ValueError`` subclass: hotfix-lane eligibility,
  not-found, card-type) and the plain ``ValueError`` raised by the service
  (state-machine / coverage gates) propagate UNCAUGHT so the adapter maps them to
  the legacy status/detail (``SprintOperationError`` → 400 ``to_dict()``,
  ``ValueError`` → 400 ``str``). Order matters in the adapter: ``SprintOperationError``
  before ``ValueError``.
* A ``None`` service result is ``EntityNotFoundError`` (adapter → 404): create
  maps it to the legacy ``"Spec or board not found"``; get / update / move / delete /
  submit-evaluation map it to ``"Sprint not found"``.
* Reads (list-by-board / list-by-spec / get / history / suggest) do not commit.
  Writes ``commit(uow)`` after the service mutation, then re-fetch via
  ``get_sprint`` (create / update / move / assign) exactly as the legacy endpoints
  did, so the response carries the loaded cards/qa/history relationships.

``unassign-tasks`` had no ``SprintService`` method — the legacy endpoint mutated
``Card.sprint_id`` inline. To keep the relational ratchet clean (no ORM import /
``select`` / ``session.get`` in this layer) the use case loads each card through
the EXISTING ``CardService.get_card`` reader and clears ``sprint_id`` on the
session-attached object, preserving the legacy semantics exactly (a card is
cleared only when it exists AND currently belongs to this sprint; the returned
count is the number actually cleared).
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    require_all,
    require_authorization,
)
from okto_pulse.core.application.use_cases.mutation_permissions import (
    entity_state,
    sprint_requirement,
    sprint_update_permission_requirements,
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


async def _require_board_access(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
    *,
    write: bool = False,
    entity_type: str = "board",
    entity_id: str | None = None,
) -> Any:
    board = await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions=_WRITE_SHARE_PERMISSIONS if write else None,
    )
    if board is None:
        raise EntityNotFoundError(entity_type, entity_id or board_id)
    return board


async def _require_spec_access(
    uow: PulseUnitOfWork,
    spec_id: str,
    actor: ActorContext,
    *,
    expected_board_id: str | None = None,
    write: bool = False,
    entity_type: str = "spec",
) -> Any:
    spec = await uow.services.specs.get_spec(spec_id)
    if spec is None or (
        expected_board_id is not None and spec.board_id != expected_board_id
    ):
        raise EntityNotFoundError(entity_type, spec_id)
    await _require_board_access(
        uow,
        spec.board_id,
        actor,
        write=write,
        entity_type=entity_type,
        entity_id=spec_id,
    )
    return spec


async def _require_sprint_access(
    uow: PulseUnitOfWork,
    sprint_id: str,
    actor: ActorContext,
    *,
    write: bool = False,
) -> Any:
    sprint = await uow.services.sprints.get_sprint(sprint_id)
    if sprint is None:
        raise EntityNotFoundError("sprint", sprint_id)
    await _require_board_access(
        uow,
        sprint.board_id,
        actor,
        write=write,
        entity_type="sprint",
        entity_id=sprint_id,
    )
    return sprint


# ===========================================================================
# Reads
# ===========================================================================


# --- list board sprints -----------------------------------------------------


class ListBoardSprintsCommand:
    __slots__ = ("board_id", "status_filter", "spec_id", "include_archived")

    def __init__(
        self,
        board_id: str,
        *,
        status_filter: str | None = None,
        spec_id: str | None = None,
        include_archived: bool = False,
    ) -> None:
        self.board_id = board_id
        self.status_filter = status_filter
        self.spec_id = spec_id
        self.include_archived = include_archived


class ListBoardSprintsResult:
    __slots__ = ("sprints",)

    def __init__(self, sprints: list[Any]) -> None:
        self.sprints = sprints


class ListBoardSprintsUseCase:
    """List every sprint for a board (read, no commit), optionally filtered by
    status and/or spec. A board-bound actor cannot query another board."""

    async def preflight(
        self, command: ListBoardSprintsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> None:
        """Authorize board/spec anchors without materializing sprints."""
        await _require_board_access(uow, command.board_id, actor)
        if command.spec_id is not None:
            repository = getattr(uow, "specs", None)
            spec = (
                await repository.get(command.spec_id)
                if repository is not None
                else await uow.services.specs.get_spec(command.spec_id)
            )
            if spec is None or spec.board_id != command.board_id:
                raise EntityNotFoundError("spec", command.spec_id)

    async def execute(
        self, command: ListBoardSprintsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListBoardSprintsResult:
        await self.preflight(command, actor=actor, uow=uow)
        sprints = await uow.services.sprints.list_board_sprints(
            command.board_id,
            command.status_filter,
            command.spec_id,
            command.include_archived,
        )
        return ListBoardSprintsResult(sprints)


# --- list sprints for a spec ------------------------------------------------


class ListSprintsCommand:
    __slots__ = ("spec_id",)

    def __init__(self, spec_id: str) -> None:
        self.spec_id = spec_id


class ListSprintsResult:
    __slots__ = ("sprints",)

    def __init__(self, sprints: list[Any]) -> None:
        self.sprints = sprints


class ListSprintsUseCase:
    """List a spec's sprints (read, no commit) within the actor's board."""

    async def preflight(
        self, command: ListSprintsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> Any:
        """Authorize the spec anchor without materializing its sprints."""
        repository = getattr(uow, "specs", None)
        spec = (
            await repository.get(command.spec_id)
            if repository is not None
            else await uow.services.specs.get_spec(command.spec_id)
        )
        if spec is None:
            raise EntityNotFoundError("spec", command.spec_id)
        await _require_board_access(
            uow,
            spec.board_id,
            actor,
            entity_type="spec",
            entity_id=command.spec_id,
        )
        return spec

    async def execute(
        self, command: ListSprintsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListSprintsResult:
        spec = await self.preflight(command, actor=actor, uow=uow)
        sprints = await uow.services.sprints.list_sprints(command.spec_id)
        return ListSprintsResult(
            [sprint for sprint in sprints if sprint.board_id == spec.board_id]
        )


# --- get sprint -------------------------------------------------------------


class GetSprintCommand:
    __slots__ = ("sprint_id",)

    def __init__(self, sprint_id: str) -> None:
        self.sprint_id = sprint_id


class GetSprintResult:
    __slots__ = ("sprint",)

    def __init__(self, sprint: Any) -> None:
        self.sprint = sprint


class GetSprintUseCase:
    """Get a sprint with full details (read, no commit). A missing sprint is
    ``EntityNotFoundError("sprint")`` (adapter → 404 "Sprint not found"). A
    board-bound actor receives the same not-found result for a cross-board ID."""

    async def execute(
        self, command: GetSprintCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetSprintResult:
        sprint = await _require_sprint_access(uow, command.sprint_id, actor)
        return GetSprintResult(sprint)


# --- list sprint history ----------------------------------------------------


class ListSprintHistoryCommand:
    __slots__ = ("sprint_id",)

    def __init__(self, sprint_id: str) -> None:
        self.sprint_id = sprint_id


class ListSprintHistoryResult:
    __slots__ = ("history",)

    def __init__(self, history: list[Any]) -> None:
        self.history = history


class ListSprintHistoryUseCase:
    """List a sprint's history (read, no commit) within the actor's board."""

    async def execute(
        self, command: ListSprintHistoryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListSprintHistoryResult:
        await _require_sprint_access(uow, command.sprint_id, actor)

        history = await uow.services.sprints.list_history(command.sprint_id)
        return ListSprintHistoryResult(history)


# --- suggest sprints --------------------------------------------------------


class SuggestSprintsCommand:
    __slots__ = ("spec_id", "threshold", "board_id")

    def __init__(
        self,
        spec_id: str,
        threshold: int = 8,
        *,
        board_id: str | None = None,
    ) -> None:
        self.spec_id = spec_id
        self.threshold = threshold
        self.board_id = board_id


class SuggestSprintsResult:
    __slots__ = ("suggestions",)

    def __init__(self, suggestions: list[Any]) -> None:
        self.suggestions = suggestions


class SuggestSprintsUseCase:
    """Suggest a sprint breakdown for a spec (read, no commit). ``ValueError``
    (e.g. spec not found / not ready) propagates for the adapter → 400."""

    async def execute(
        self, command: SuggestSprintsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> SuggestSprintsResult:
        expected_board_id = command.board_id or actor.board_id
        await _require_spec_access(
            uow,
            command.spec_id,
            actor,
            expected_board_id=expected_board_id,
        )

        suggestions = await uow.services.sprints.suggest_sprints(
            command.spec_id, command.threshold
        )
        return SuggestSprintsResult(suggestions)


# ===========================================================================
# Writes
# ===========================================================================


# --- create sprint ----------------------------------------------------------


class CreateSprintCommand:
    __slots__ = ("board_id", "data", "spec_id")

    def __init__(self, board_id: str, data: Any, *, spec_id: str | None = None) -> None:
        self.board_id = board_id
        self.data = data
        self.spec_id = spec_id


class CreateSprintResult:
    __slots__ = ("sprint",)

    def __init__(self, sprint: Any) -> None:
        self.sprint = sprint


class CreateSprintUseCase:
    """Create a sprint for a spec (write). ``SprintOperationError`` (hotfix-lane
    eligibility → 400) and ``ValueError`` (invalid TS/BR ids → 400) propagate; a
    ``None`` result (missing spec or board) is ``EntityNotFoundError("spec_or_board")``
    (adapter → 404 "Spec or board not found"). Re-fetches via ``get_sprint`` after
    commit so the response carries the loaded relationships, exactly as the legacy
    endpoint. The path and payload spec identifiers must agree before the service
    is called."""

    async def execute(
        self, command: CreateSprintCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateSprintResult:
        payload_spec_id = command.data.spec_id
        if command.spec_id is not None and command.spec_id != payload_spec_id:
            raise EntityNotFoundError("spec_or_board", command.spec_id)
        await _require_board_access(
            uow,
            command.board_id,
            actor,
            write=True,
            entity_type="spec_or_board",
        )
        await _require_spec_access(
            uow,
            payload_spec_id,
            actor,
            expected_board_id=command.board_id,
            write=True,
            entity_type="spec_or_board",
        )
        await require_authorization(
            actor,
            sprint_requirement(
                "sprint.entity.create",
                legacy_operation="specs:create",
            ),
            uow=uow,
            board_id=command.board_id,
        )

        service = uow.services.sprints
        sprint = await service.create_sprint(
            command.board_id,
            actor.actor_id,
            command.data,
            query_scope=_query_scope_for_actor(
                actor,
                board_id=command.board_id,
                board_access_granted=True,
            ),
        )
        if not sprint:
            raise EntityNotFoundError("spec_or_board", command.board_id)
        await commit(uow)
        return CreateSprintResult(await service.get_sprint(sprint.id))


# --- update sprint ----------------------------------------------------------


class UpdateSprintCommand:
    __slots__ = ("sprint_id", "data")

    def __init__(self, sprint_id: str, data: Any) -> None:
        self.sprint_id = sprint_id
        self.data = data


class UpdateSprintResult:
    __slots__ = ("sprint",)

    def __init__(self, sprint: Any) -> None:
        self.sprint = sprint


class UpdateSprintUseCase:
    """Update a sprint (write). ``ValueError`` (invalid TS/BR ids → 400) propagates;
    a ``None`` result is ``EntityNotFoundError("sprint")`` (adapter → 404 "Sprint not
    found"). Re-fetches via ``get_sprint`` after commit."""

    async def execute(
        self, command: UpdateSprintCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> UpdateSprintResult:
        service = uow.services.sprints
        existing = await _require_sprint_access(
            uow, command.sprint_id, actor, write=True
        )
        await require_all(
            actor,
            *sprint_update_permission_requirements(
                command.data,
                state=entity_state(existing),
            ),
            uow=uow,
            board_id=existing.board_id,
        )

        sprint = await service.update_sprint(command.sprint_id, actor.actor_id, command.data)
        if not sprint:
            raise EntityNotFoundError("sprint", command.sprint_id)
        await commit(uow)
        return UpdateSprintResult(await service.get_sprint(sprint.id))


# --- move sprint ------------------------------------------------------------


class MoveSprintCommand:
    __slots__ = ("sprint_id", "data")

    def __init__(self, sprint_id: str, data: Any) -> None:
        self.sprint_id = sprint_id
        self.data = data


class MoveSprintResult:
    __slots__ = ("sprint",)

    def __init__(self, sprint: Any) -> None:
        self.sprint = sprint


class MoveSprintUseCase:
    """Move a sprint through its state machine (write). ``ValueError`` (illegal
    transition / unmet coverage / no-evaluation / below-threshold / reject /
    archived → 400) propagates; a ``None`` result is ``EntityNotFoundError("sprint")``
    (adapter → 404 "Sprint not found"). Re-fetches via ``get_sprint`` after commit.
    The state/coverage/evaluation gates live entirely in ``SprintService.move_sprint``."""

    async def execute(
        self, command: MoveSprintCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> MoveSprintResult:
        service = uow.services.sprints
        await _require_sprint_access(uow, command.sprint_id, actor, write=True)

        sprint = await service.move_sprint(command.sprint_id, actor.actor_id, command.data)
        if not sprint:
            raise EntityNotFoundError("sprint", command.sprint_id)
        await commit(uow)
        return MoveSprintResult(await service.get_sprint(sprint.id))


# --- delete sprint ----------------------------------------------------------


class DeleteSprintCommand:
    __slots__ = ("sprint_id",)

    def __init__(self, sprint_id: str) -> None:
        self.sprint_id = sprint_id


class DeleteSprintResult:
    __slots__ = ()


class DeleteSprintUseCase:
    """Delete a sprint (write). A falsey service result is
    ``EntityNotFoundError("sprint")`` (adapter → 404 "Sprint not found"); otherwise
    commit, mirroring the legacy 204 endpoint."""

    async def execute(
        self, command: DeleteSprintCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DeleteSprintResult:
        service = uow.services.sprints
        existing = await _require_sprint_access(
            uow, command.sprint_id, actor, write=True
        )
        await require_authorization(
            actor,
            sprint_requirement(
                "sprint.entity.delete",
                state=entity_state(existing),
                legacy_operation="specs:delete",
            ),
            uow=uow,
            board_id=existing.board_id,
        )

        deleted = await service.delete_sprint(command.sprint_id, actor.actor_id)
        if not deleted:
            raise EntityNotFoundError("sprint", command.sprint_id)
        await commit(uow)
        return DeleteSprintResult()


# --- submit evaluation ------------------------------------------------------


class SubmitSprintEvaluationCommand:
    __slots__ = ("sprint_id", "evaluation")

    def __init__(self, sprint_id: str, evaluation: dict) -> None:
        self.sprint_id = sprint_id
        self.evaluation = evaluation


class SubmitSprintEvaluationResult:
    __slots__ = ("sprint",)

    def __init__(self, sprint: Any) -> None:
        self.sprint = sprint


class SubmitSprintEvaluationUseCase:
    """Submit a qualitative evaluation for a sprint (write). ``ValueError`` (not in
    review status → 400) propagates; a ``None`` result is
    ``EntityNotFoundError("sprint")`` (adapter → 404 "Sprint not found"). Returns the
    mutated sprint (carrying ``evaluations``) for the adapter to shape the
    ``evaluation_id`` envelope, exactly as the legacy endpoint."""

    async def execute(
        self, command: SubmitSprintEvaluationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> SubmitSprintEvaluationResult:
        service = uow.services.sprints
        existing = await _require_sprint_access(
            uow, command.sprint_id, actor, write=True
        )
        await require_authorization(
            actor,
            sprint_requirement(
                "sprint.evaluations.submit",
                state=entity_state(existing),
                legacy_operation="specs:evaluate",
            ),
            uow=uow,
            board_id=existing.board_id,
        )

        sprint = await service.submit_evaluation(
            command.sprint_id, actor.actor_id, command.evaluation
        )
        if not sprint:
            raise EntityNotFoundError("sprint", command.sprint_id)
        await commit(uow)
        return SubmitSprintEvaluationResult(sprint)


# --- assign tasks -----------------------------------------------------------


class AssignSprintTasksCommand:
    __slots__ = ("sprint_id", "card_ids")

    def __init__(self, sprint_id: str, card_ids: list[str]) -> None:
        self.sprint_id = sprint_id
        self.card_ids = card_ids


class AssignSprintTasksResult:
    __slots__ = ("assigned", "sprint")

    def __init__(self, assigned: int, sprint: Any) -> None:
        self.assigned = assigned
        self.sprint = sprint


class AssignSprintTasksUseCase:
    """Assign cards to a sprint (write). ``SprintOperationError`` (sprint not found /
    hotfix card-type → 400) and ``ValueError`` (cross-spec card → 400) propagate; the
    adapter must catch ``SprintOperationError`` before ``ValueError``. Re-fetches via
    ``get_sprint`` after commit so the adapter can shape the lane envelope
    (``lane_type`` / ``accepted_card_types``), exactly as the legacy endpoint."""

    async def execute(
        self, command: AssignSprintTasksCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> AssignSprintTasksResult:
        service = uow.services.sprints
        existing = await _require_sprint_access(
            uow, command.sprint_id, actor, write=True
        )
        await require_authorization(
            actor,
            sprint_requirement(
                "sprint.entity.assign",
                state=entity_state(existing),
            ),
            uow=uow,
            board_id=existing.board_id,
        )

        count = await service.assign_tasks(command.sprint_id, command.card_ids, actor.actor_id)
        await commit(uow)
        sprint = await service.get_sprint(command.sprint_id)
        return AssignSprintTasksResult(count, sprint)


# --- unassign tasks ---------------------------------------------------------


class UnassignSprintTasksCommand:
    __slots__ = ("sprint_id", "card_ids")

    def __init__(self, sprint_id: str, card_ids: list[str]) -> None:
        self.sprint_id = sprint_id
        self.card_ids = card_ids


class UnassignSprintTasksResult:
    __slots__ = ("unassigned",)

    def __init__(self, unassigned: int) -> None:
        self.unassigned = unassigned


class UnassignSprintTasksUseCase:
    """Remove cards through the canonical sprint writer.

    The writer records history/activity and bumps ``Sprint.version`` so the
    version-keyed ``SprintScopeResolver`` cannot serve stale scope.
    """

    async def execute(
        self, command: UnassignSprintTasksCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> UnassignSprintTasksResult:
        service = uow.services.sprints
        existing = await _require_sprint_access(
            uow, command.sprint_id, actor, write=True
        )
        await require_authorization(
            actor,
            sprint_requirement(
                "sprint.entity.assign",
                state=entity_state(existing),
            ),
            uow=uow,
            board_id=existing.board_id,
        )

        count = await service.unassign_tasks(
            command.sprint_id,
            command.card_ids,
            actor.actor_id,
        )
        await commit(uow)
        return UnassignSprintTasksResult(count)
