"""Sprint CRUD use cases (SaaS Refactor spec R01A REST-FU7-S2).

Transport-free reimplementations of every ``api/sprints.py`` endpoint that still
bound ``get_db`` — list-by-board / create / list-by-spec / get / update / move /
delete / submit-evaluation / assign-tasks / unassign-tasks / history / suggest.
Each use case delegates to the EXISTING ``SprintService`` methods so the SQL
inline, the sprint state-machine gates (draft→active→review→closed), the test /
evaluation / threshold coverage gates, the activity logging and the history
recording all stay in the service; only the transport envelope (not-found 404,
mutate, commit, re-fetch) moves here.

Behavioral fidelity to the legacy endpoints:

* The legacy sprint endpoints carried NO permission gate (no ``_require_permissions``),
  so these use cases carry none either — they delegate straight to ``SprintService``.
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

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
    session_of,
)
from okto_pulse.core.application.scope import ActorScope, QueryScope
from okto_pulse.core.services.main import CardService, SprintService


def _query_scope_for_actor(actor: ActorContext, *, board_id: str | None = None) -> QueryScope:
    return ActorScope.from_context(actor).query_scope(target_board_id=board_id)


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
    status and/or spec — the legacy endpoint had no ownership/permission gate."""

    async def execute(
        self, command: ListBoardSprintsCommand, *, actor: ActorContext, uow: Any
    ) -> ListBoardSprintsResult:
        session = session_of(uow)
        sprints = await SprintService(session).list_board_sprints(
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
    """List a spec's sprints (read, no commit)."""

    async def execute(
        self, command: ListSprintsCommand, *, actor: ActorContext, uow: Any
    ) -> ListSprintsResult:
        session = session_of(uow)
        sprints = await SprintService(session).list_sprints(command.spec_id)
        return ListSprintsResult(sprints)


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
    ``EntityNotFoundError("sprint")`` (adapter → 404 "Sprint not found")."""

    async def execute(
        self, command: GetSprintCommand, *, actor: ActorContext, uow: Any
    ) -> GetSprintResult:
        session = session_of(uow)
        sprint = await SprintService(session).get_sprint(command.sprint_id)
        if not sprint:
            raise EntityNotFoundError("sprint", command.sprint_id)
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
    """List a sprint's history (read, no commit). The legacy endpoint returned the
    service result directly with no not-found gate."""

    async def execute(
        self, command: ListSprintHistoryCommand, *, actor: ActorContext, uow: Any
    ) -> ListSprintHistoryResult:
        session = session_of(uow)
        history = await SprintService(session).list_history(command.sprint_id)
        return ListSprintHistoryResult(history)


# --- suggest sprints --------------------------------------------------------


class SuggestSprintsCommand:
    __slots__ = ("spec_id", "threshold")

    def __init__(self, spec_id: str, threshold: int = 8) -> None:
        self.spec_id = spec_id
        self.threshold = threshold


class SuggestSprintsResult:
    __slots__ = ("suggestions",)

    def __init__(self, suggestions: list[Any]) -> None:
        self.suggestions = suggestions


class SuggestSprintsUseCase:
    """Suggest a sprint breakdown for a spec (read, no commit). ``ValueError``
    (e.g. spec not found / not ready) propagates for the adapter → 400."""

    async def execute(
        self, command: SuggestSprintsCommand, *, actor: ActorContext, uow: Any
    ) -> SuggestSprintsResult:
        session = session_of(uow)
        suggestions = await SprintService(session).suggest_sprints(
            command.spec_id, command.threshold
        )
        return SuggestSprintsResult(suggestions)


# ===========================================================================
# Writes
# ===========================================================================


# --- create sprint ----------------------------------------------------------


class CreateSprintCommand:
    __slots__ = ("board_id", "data")

    def __init__(self, board_id: str, data: Any) -> None:
        self.board_id = board_id
        self.data = data


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
    endpoint. The spec id is taken from the payload (``data.spec_id``), mirroring
    the legacy service call — the path ``spec_id`` was unused there too."""

    async def execute(
        self, command: CreateSprintCommand, *, actor: ActorContext, uow: Any
    ) -> CreateSprintResult:
        session = session_of(uow)
        service = SprintService(session)
        sprint = await service.create_sprint(
            command.board_id,
            actor.actor_id,
            command.data,
            query_scope=_query_scope_for_actor(actor, board_id=command.board_id),
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
        self, command: UpdateSprintCommand, *, actor: ActorContext, uow: Any
    ) -> UpdateSprintResult:
        session = session_of(uow)
        service = SprintService(session)
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
        self, command: MoveSprintCommand, *, actor: ActorContext, uow: Any
    ) -> MoveSprintResult:
        session = session_of(uow)
        service = SprintService(session)
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
        self, command: DeleteSprintCommand, *, actor: ActorContext, uow: Any
    ) -> DeleteSprintResult:
        session = session_of(uow)
        deleted = await SprintService(session).delete_sprint(command.sprint_id, actor.actor_id)
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
        self, command: SubmitSprintEvaluationCommand, *, actor: ActorContext, uow: Any
    ) -> SubmitSprintEvaluationResult:
        session = session_of(uow)
        sprint = await SprintService(session).submit_evaluation(
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
        self, command: AssignSprintTasksCommand, *, actor: ActorContext, uow: Any
    ) -> AssignSprintTasksResult:
        session = session_of(uow)
        service = SprintService(session)
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
    """Remove cards from a sprint (write). Reproduces the legacy inline mutation
    without coupling to the ORM: each card id is loaded through the EXISTING
    ``CardService.get_card`` reader, and ``sprint_id`` is cleared on the
    session-attached object only when the card exists AND currently belongs to this
    sprint — the returned count is the number actually cleared, exactly as the
    legacy endpoint."""

    async def execute(
        self, command: UnassignSprintTasksCommand, *, actor: ActorContext, uow: Any
    ) -> UnassignSprintTasksResult:
        session = session_of(uow)
        service = CardService(session)
        count = 0
        for card_id in command.card_ids:
            card = await service.get_card(card_id)
            if card and card.sprint_id == command.sprint_id:
                card.sprint_id = None
                count += 1
        await commit(uow)
        return UnassignSprintTasksResult(count)
