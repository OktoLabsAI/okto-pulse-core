"""MCP-scoped card CRUD use cases (SaaS Refactor spec R01A MCP-FU6, family: card).

The MCP card tools diverge from the REST ``card_crud`` use cases in three ways
the REST endpoints never had, so they cannot be reused as-is (would drift):

1. **Board ownership scoping** — every MCP card tool fetches the card and rejects
   a cross-board id with the legacy ``{"error": "Card not found"}``. The REST use
   cases operate by ``card_id`` only.
2. **Atomic MCP activity log** — the use case passes the resolved actor context
   to ``CardService``, the single canonical producer, and commits that one row
   in the SAME transaction as the mutation.
3. **MCP-specific JSON envelopes / except order** — handled by the tool adapter,
   NOT here.

These use cases are MCP-orchestration-specific but stay transport-free: they do
the board-scope, call ``CardService`` with the actor metadata, and commit a
SINGLE UoW transaction. They return domain objects — the adapter owns the exact
``json.dumps`` envelope and the
``CardOperationError``/``GateContractError``/``ResourceGateError``/``ValueError``
except order (per Codex decision: option A with adapter envelope).
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
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.mutation_permissions import (
    card_create_permission_requirement,
    card_requirement,
    card_update_permission_requirements,
    entity_state,
    transition_permission_requirement,
)
from okto_pulse.core.domain.enums import CardStatus


def _require_actor_board(actor: ActorContext, board_id: str) -> None:
    """Fail closed when a direct caller spoofs the command board."""

    if actor.board_id is None or actor.board_id != board_id:
        raise EntityNotFoundError("card", board_id)


def _activity_actor_type(actor: ActorContext) -> str:
    """Map transport source to the persisted activity actor taxonomy."""

    if actor.source == "mcp":
        return "agent"
    if actor.source == "rest":
        return "user"
    return actor.source


async def _get_card_in_scope(
    service: Any,
    card_id: str,
    board_id: str,
    actor: ActorContext,
) -> Any:
    _require_actor_board(actor, board_id)
    card = await service.get_card(card_id)
    if not card or card.board_id != board_id:
        raise EntityNotFoundError("card", card_id)
    return card


# --- create (board create + scenario backlink + activity log) ---------------


class McpCreateCardCommand:
    __slots__ = ("board_id", "spec_id", "data", "scenario_ids_list", "activity_details")

    def __init__(
        self,
        board_id: str,
        spec_id: str,
        data: Any,
        scenario_ids_list: list[str] | None,
        activity_details: dict,
    ) -> None:
        self.board_id = board_id
        self.spec_id = spec_id
        self.data = data
        self.scenario_ids_list = scenario_ids_list
        self.activity_details = activity_details


class McpCreateCardResult:
    __slots__ = ("card", "knowledge_mutation")

    def __init__(self, card: Any, knowledge_mutation: Any = None) -> None:
        self.card = card
        self.knowledge_mutation = knowledge_mutation


class McpCreateCardUseCase:
    """Create card, traceability backlinks and one activity in one transaction.

    ``CardService.create_card`` invokes the shared idempotent traceability writer
    for scenario/FR/BR targets and owns the canonical activity row. The use case
    supplies MCP actor metadata and performs the one commit.
    """

    async def execute(
        self,
        command: McpCreateCardCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpCreateCardResult:
        board = await load_accessible_board(uow, command.board_id, actor)
        if board is None:
            raise ValueError("Board not found")

        await require_authorization(
            actor,
            card_create_permission_requirement(command.data),
            uow=uow,
            board_id=command.board_id,
        )

        from okto_pulse.core.application.use_cases.knowledge_propagation import (
            CreateCardKnowledgeV2Command,
            CreateCardKnowledgeV2UseCase,
        )
        mutation = await CreateCardKnowledgeV2UseCase().execute(
            CreateCardKnowledgeV2Command(
                command.board_id,
                command.data,
                skip_ownership_check=True,
                activity_details=command.activity_details,
            ),
            actor=actor,
            uow=uow,
        )
        return McpCreateCardResult(None, knowledge_mutation=mutation)


# --- get (board-scoped read) ------------------------------------------------


class McpGetCardCommand:
    __slots__ = ("card_id", "board_id")

    def __init__(self, card_id: str, board_id: str) -> None:
        self.card_id = card_id
        self.board_id = board_id


class McpGetCardResult:
    __slots__ = ("card",)

    def __init__(self, card: Any) -> None:
        self.card = card


class McpGetCardUseCase:
    """Fetch a board-scoped card (read). A missing card OR a cross-board card is
    ``EntityNotFoundError`` → the adapter's legacy ``{"error": "Card not found"}``.
    Read: no commit (the legacy tool's ``db.commit()`` was a no-op on a read)."""

    async def execute(
        self, command: McpGetCardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpGetCardResult:
        card = await _get_card_in_scope(
            uow.services.cards, command.card_id, command.board_id, actor
        )
        return McpGetCardResult(card)


# --- update (board-scope + atomic activity log + commit) --------------------


class McpUpdateCardCommand:
    __slots__ = ("card_id", "board_id", "data", "activity_details")

    def __init__(
        self, card_id: str, board_id: str, data: Any, activity_details: dict
    ) -> None:
        self.card_id = card_id
        self.board_id = board_id
        self.data = data
        self.activity_details = activity_details


class McpUpdateCardResult:
    __slots__ = ("card",)

    def __init__(self, card: Any) -> None:
        self.card = card


class McpUpdateCardUseCase:
    """Update a board-scoped card and its one canonical ``card_updated`` row in a
    single transaction. Cross-board/missing → ``EntityNotFoundError``.
    ``CardService.update_card`` gate errors
    (``CardOperationError``/``ValueError``) propagate for the adapter to map."""

    async def execute(
        self,
        command: McpUpdateCardCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpUpdateCardResult:
        service = uow.services.cards
        existing = await _get_card_in_scope(
            service, command.card_id, command.board_id, actor
        )
        await require_all(
            actor,
            *card_update_permission_requirements(
                command.data,
                state=entity_state(existing),
            ),
            uow=uow,
            board_id=existing.board_id,
        )
        updated = await service.update_card(
            command.card_id,
            actor.actor_id,
            command.data,
            actor_type=_activity_actor_type(actor),
            actor_name=actor.actor_name,
            activity_details=command.activity_details,
        )
        await commit(uow)
        return McpUpdateCardResult(updated)


# --- move (board-scope + state machine + commit) ----------------------------


class McpMoveCardCommand:
    __slots__ = ("card_id", "board_id", "data")

    def __init__(self, card_id: str, board_id: str, data: Any) -> None:
        self.card_id = card_id
        self.board_id = board_id
        self.data = data


class McpMoveCardResult:
    __slots__ = ("card",)

    def __init__(self, card: Any) -> None:
        self.card = card


class McpMoveCardUseCase:
    """Move a board-scoped card. Cross-board/missing → ``EntityNotFoundError``
    (adapter ``"Card not found"``). ``CardService.move_card`` gate errors
    (``CardOperationError``/``GateContractError``/``ResourceGateError``/
    ``ValueError``) propagate for the adapter to map in that exact order. If the
    move yields ``None`` (no row updated), commit is SKIPPED and ``card=None`` →
    adapter ``"Failed to move card"`` — preserving the legacy early-return."""

    async def execute(
        self, command: McpMoveCardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpMoveCardResult:
        service = uow.services.cards
        existing = await _get_card_in_scope(
            service, command.card_id, command.board_id, actor
        )
        current_state = entity_state(existing)
        transition_from = existing.status
        target_state = str(getattr(command.data.status, "value", command.data.status))
        learning_submission = getattr(command.data, 'learning_submission', None)
        if learning_submission is not None:
            from okto_pulse.core.application.use_cases.learning_capture import authorize_learning_submission
            from okto_pulse.core.domain.learning_submission import (
                learning_submission_replay, learning_submission_request_digest,
            )

            await authorize_learning_submission(command.data, actor=actor, uow=uow, board_id=existing.board_id)
            replay = learning_submission_replay(existing.conclusions, actor_id=actor.actor_id,
                capture_id=learning_submission.capture_id,
                request_digest=learning_submission_request_digest(board_id=existing.board_id,
                    bug_id=existing.id, actor_id=actor.actor_id, move=command.data))
            if replay is not None:
                current_state = replay.from_status
                transition_from = replay.from_status
        requirement = (
            card_requirement(
                "card.entity.edit_fields",
                state=current_state,
                legacy_operation="cards:move",
            )
            if current_state == target_state
            else transition_permission_requirement(
                "card",
                transition_from,
                command.data.status,
                legacy_operation="cards:move",
            )
        )
        await require_authorization(
            actor,
            requirement,
            uow=uow,
            board_id=existing.board_id,
        )
        try:
            updated = await service.move_card(
                command.card_id, actor.actor_id, command.data, actor.actor_name
            )
            if not updated:
                return McpMoveCardResult(None)
            # Flush and refresh inside the still-open transaction. Refreshing
            # after commit would create a gap where another writer could move
            # the row or a refresh failure could misreport a durable write.
            await uow.synchronize()
            await uow.reload(
                updated,
                fields=("status", "position", "policy_version"),
            )
            await commit(uow)
        except BaseException:
            if learning_submission is not None:
                await uow.rollback()
            raise
        return McpMoveCardResult(updated)


# --- delete (board-scope + atomic activity log + commit) --------------------


class McpDeleteCardCommand:
    __slots__ = ("card_id", "board_id")

    def __init__(self, card_id: str, board_id: str) -> None:
        self.card_id = card_id
        self.board_id = board_id


class McpDeleteCardResult:
    __slots__ = ("deleted", "takedown")

    def __init__(self, deleted: bool, takedown: dict[str, object]) -> None:
        self.deleted = deleted
        self.takedown = takedown


class McpDeleteCardUseCase:
    """Delete a board-scoped card and its one canonical activity atomically.

    The service owns ``card_deleted`` after the governed writer accepts the
    delete. This ordering keeps governed conflicts at zero audit/mutation.
    Cross-board/missing → ``EntityNotFoundError`` (adapter
    ``"Card not found"``).
    """

    async def execute(
        self,
        command: McpDeleteCardCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpDeleteCardResult:
        service = uow.services.cards
        existing = await _get_card_in_scope(
            service, command.card_id, command.board_id, actor
        )
        await require_authorization(
            actor,
            card_requirement(
                "card.entity.delete",
                state=entity_state(existing),
                legacy_operation="cards:delete",
            ),
            uow=uow,
            board_id=existing.board_id,
        )
        delete_result = await service.delete_card(
            command.card_id,
            actor.actor_id,
            return_receipt=True,
            actor_type=_activity_actor_type(actor),
            actor_name=actor.actor_name,
        )
        if not delete_result:
            raise EntityNotFoundError("card", command.card_id)
        receipt_serializer = getattr(delete_result, "to_dict", None)
        if receipt_serializer is None:
            raise RuntimeError("governed_delete_receipt_missing")
        takedown = receipt_serializer()
        if not isinstance(takedown, dict):
            raise RuntimeError("governed_delete_receipt_invalid")
        try:
            await commit(uow)
        except BaseException:
            await service.restore_deleted_card_attachments(delete_result)
            raise
        return McpDeleteCardResult(True, takedown)


# --- dependencies (combined read) -------------------------------------------


class McpGetCardDependenciesCommand:
    __slots__ = ("card_id",)

    def __init__(self, card_id: str) -> None:
        self.card_id = card_id


class McpGetCardDependenciesResult:
    __slots__ = ("dependencies", "dependents", "can_advance", "blocking_titles")

    def __init__(
        self,
        dependencies: list[Any],
        dependents: list[Any],
        can_advance: bool,
        blocking_titles: list[Any],
    ) -> None:
        self.dependencies = dependencies
        self.dependents = dependents
        self.can_advance = can_advance
        self.blocking_titles = blocking_titles


class McpGetCardDependenciesUseCase:
    """List a card's dependencies + dependents + advance-readiness in one read
    (the legacy MCP tool combined three ``CardService`` reads the REST split into
    separate use cases). Missing/cross-board source cards are a non-disclosing
    ``EntityNotFoundError``. Legacy/corrupt cross-board edges are excluded from
    both projections and readiness so titles cannot leak through
    ``blocking_titles``. The legacy read-only ``db.commit()`` is skipped."""

    async def execute(
        self,
        command: McpGetCardDependenciesCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpGetCardDependenciesResult:
        service = uow.services.cards
        await _get_card_in_scope(service, command.card_id, actor.board_id or "", actor)

        deps = [
            dependency
            for dependency in await service.get_dependencies(command.card_id)
            if dependency.board_id == actor.board_id
        ]
        dependents = [
            dependent
            for dependent in await service.get_dependents(command.card_id)
            if dependent.board_id == actor.board_id
        ]
        blocking = [
            dependency.title
            for dependency in deps
            if dependency.status not in (CardStatus.DONE, CardStatus.CANCELLED)
        ]
        deps_met = not blocking
        return McpGetCardDependenciesResult(deps, dependents, deps_met, blocking)


class McpRemoveCardDependencyCommand:
    __slots__ = ("card_id", "depends_on_id")

    def __init__(self, card_id: str, depends_on_id: str) -> None:
        self.card_id = card_id
        self.depends_on_id = depends_on_id


class McpRemoveCardDependencyResult:
    __slots__ = ("removed",)

    def __init__(self, removed: bool) -> None:
        self.removed = removed


class McpRemoveCardDependencyUseCase:
    """Remove a card→depends_on edge (write). Unlike the REST
    ``RemoveCardDependencyUseCase`` (which raises ``EntityNotFoundError`` on a
    no-op), the legacy MCP tool returns ``{"success": removed}`` with the raw
    bool — preserved here (no raise on ``False``). A missing/cross-board source
    card is non-disclosing not-found. A missing/cross-board target remains
    indistinguishable from a missing edge as ``False`` and never reaches the
    writer."""

    async def execute(
        self,
        command: McpRemoveCardDependencyCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpRemoveCardDependencyResult:
        service = uow.services.cards
        card = await _get_card_in_scope(
            service, command.card_id, actor.board_id or "", actor
        )

        depends_on = await service.get_card(command.depends_on_id)
        if (
            not depends_on
            or depends_on.board_id != card.board_id
            or depends_on.board_id != actor.board_id
        ):
            return McpRemoveCardDependencyResult(False)

        await require_authorization(
            actor,
            card_requirement(
                "card.entity.manage_dependencies",
                state=entity_state(card),
            ),
            uow=uow,
            board_id=card.board_id,
        )

        removed = await service.remove_dependency(
            command.card_id, command.depends_on_id
        )
        await commit(uow)
        return McpRemoveCardDependencyResult(removed)
