"""MCP-scoped card CRUD use cases (SaaS Refactor spec R01A MCP-FU6, family: card).

The MCP card tools diverge from the REST ``card_crud`` use cases in three ways
the REST endpoints never had, so they cannot be reused as-is (would drift):

1. **Board ownership scoping** — every MCP card tool fetches the card and rejects
   a cross-board id with the legacy ``{"error": "Card not found"}``. The REST use
   cases operate by ``card_id`` only.
2. **Atomic MCP activity log** — ``update_card``/``delete_card`` write a
   ``board_service._log_activity`` row in the SAME transaction as the mutation
   (REST does not; ``CardService`` emits a DomainEvent, not that row).
3. **MCP-specific JSON envelopes / except order** — handled by the tool adapter,
   NOT here.

These use cases are MCP-orchestration-specific but stay transport-free: they do
the board-scope, call ``CardService``, write the activity log where the legacy
MCP contract did, and commit a SINGLE UoW transaction. They return domain
objects — the adapter owns the exact ``json.dumps`` envelope and the
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
from okto_pulse.core.domain.enums import CardStatus
from okto_pulse.core.services.activity_log import (
    activity_log_changes,
    activity_log_value,
)


def _require_actor_board(actor: ActorContext, board_id: str) -> None:
    """Fail closed when a direct caller spoofs the command board."""

    if actor.board_id is None or actor.board_id != board_id:
        raise EntityNotFoundError("card", board_id)


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
    __slots__ = ("card",)

    def __init__(self, card: Any) -> None:
        self.card = card


class McpCreateCardUseCase:
    """Create card, traceability backlinks and activity in one transaction.

    ``CardService.create_card`` invokes the shared idempotent traceability writer
    for scenario/FR/BR targets.  The MCP-specific activity row is appended before
    the one and only commit, so any failure rolls the entire operation back.
    """

    async def execute(
        self, command: McpCreateCardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpCreateCardResult:
        try:
            _require_actor_board(actor, command.board_id)
        except EntityNotFoundError as exc:
            raise ValueError("Board not found") from exc

        spec = await uow.services.specs.get_spec(command.spec_id)
        if not spec or spec.board_id != command.board_id:
            raise ValueError(f"Spec '{command.spec_id}' not found")

        service = uow.services.cards
        card = await service.create_card(
            command.board_id, actor.actor_id, command.data, skip_ownership_check=True
        )
        if not card:
            return McpCreateCardResult(None)

        await uow.services.boards._log_activity(
            board_id=command.board_id,
            card_id=card.id,
            action="card_created",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            details=command.activity_details,
        )
        await commit(uow)
        return McpCreateCardResult(card)


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
    """Update a board-scoped card AND write the ``card_updated`` activity row in a
    single transaction (the legacy MCP tool's atomic contract). Cross-board/missing
    → ``EntityNotFoundError``. ``CardService.update_card`` gate errors
    (``CardOperationError``/``ValueError``) propagate for the adapter to map."""

    async def execute(
        self, command: McpUpdateCardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpUpdateCardResult:
        service = uow.services.cards
        card = await _get_card_in_scope(
            service, command.card_id, command.board_id, actor
        )
        update_data = command.data.model_dump(exclude_unset=True)
        changes = activity_log_changes(
            {field: getattr(card, field, None) for field in update_data},
            update_data,
            update_data,
        )
        activity_details = {
            field: activity_log_value(value)
            for field, value in command.activity_details.items()
        }
        updated = await service.update_card(
            command.card_id, actor.actor_id, command.data
        )
        await uow.services.boards._log_activity(
            board_id=command.board_id,
            card_id=command.card_id,
            action="card_updated",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            # Keep every legacy MCP metadata field and add the same diff
            # envelope emitted by the canonical CardService producer.
            details={**activity_details, "changes": changes},
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
        await _get_card_in_scope(service, command.card_id, command.board_id, actor)
        updated = await service.move_card(
            command.card_id, actor.actor_id, command.data, actor.actor_name
        )
        if not updated:
            return McpMoveCardResult(None)
        await commit(uow)
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
    """Delete a board-scoped card and atomically write ``card_deleted`` only
    after the canonical writer accepts the delete. This ordering keeps governed
    conflicts at zero audit/mutation. Cross-board/missing →
    ``EntityNotFoundError`` (adapter ``"Card not found"``)."""

    async def execute(
        self, command: McpDeleteCardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpDeleteCardResult:
        service = uow.services.cards
        card = await _get_card_in_scope(
            service, command.card_id, command.board_id, actor
        )
        delete_result = await service.delete_card(
            command.card_id,
            actor.actor_id,
            return_receipt=True,
        )
        if not delete_result:
            raise EntityNotFoundError("card", command.card_id)
        receipt_serializer = getattr(delete_result, "to_dict", None)
        if receipt_serializer is None:
            raise RuntimeError("governed_delete_receipt_missing")
        takedown = receipt_serializer()
        if not isinstance(takedown, dict):
            raise RuntimeError("governed_delete_receipt_invalid")
        await uow.services.boards._log_activity(
            board_id=command.board_id,
            card_id=command.card_id,
            action="card_deleted",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            details={"title": card.title},
        )
        await commit(uow)
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
        self, command: McpGetCardDependenciesCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpGetCardDependenciesResult:
        service = uow.services.cards
        await _get_card_in_scope(
            service, command.card_id, actor.board_id or "", actor
        )

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

        removed = await service.remove_dependency(
            command.card_id, command.depends_on_id
        )
        await commit(uow)
        return McpRemoveCardDependencyResult(removed)


# --- copy knowledge spec→card (R3-IMP2 effective-resource fallback) ----------


class McpCopyKnowledgeToCardCommand:
    __slots__ = ("board_id", "spec_id", "card_id", "id_filter")

    def __init__(
        self,
        board_id: str,
        spec_id: str,
        card_id: str,
        id_filter: list[str] | set[str] | tuple[str, ...] | None,
    ) -> None:
        self.board_id = board_id
        self.spec_id = spec_id
        self.card_id = card_id
        self.id_filter = id_filter


class McpCopyKnowledgeToCardResult:
    """``empty_plan`` set → adapter renders ``_effective_empty_copy_response``;
    otherwise the copy counters/provenance the adapter shapes into JSON."""

    __slots__ = (
        "empty_plan", "copied", "copied_ids", "fallback",
        "src_type", "src_id", "total_on_card",
    )

    def __init__(
        self,
        *,
        empty_plan: dict | None = None,
        copied: int = 0,
        copied_ids: list[str] | None = None,
        fallback: bool = False,
        src_type: str | None = None,
        src_id: str | None = None,
        total_on_card: int = 0,
    ) -> None:
        self.empty_plan = empty_plan
        self.copied = copied
        self.copied_ids = copied_ids or []
        self.fallback = fallback
        self.src_type = src_type
        self.src_id = src_id
        self.total_on_card = total_on_card


class McpCopyKnowledgeToCardUseCase:
    """Copy spec knowledge entries onto a card as inline card KEs, with the
    R3-IMP2 effective-inherited fallback (refinement/ideation) — the full legacy
    MCP orchestration, transport-free. Missing spec/card → ``EntityNotFoundError``
    (adapter maps to ``"Spec not found"``/``"Card not found"`` via ``entity_type``).
    ``ResourceLineageResolutionError`` from the resolver propagates UNCAUGHT for the
    adapter's ``exc.to_error_dict()``. An empty effective plan returns
    ``empty_plan`` for the adapter's ``_effective_empty_copy_response``. Dedup by
    source/card-kb-id and the canonical ``copied_from_<type>:<id>:<kb>`` provenance
    are byte-identical to the legacy tool; a single commit after ``update_card``."""

    async def execute(
        self,
        command: McpCopyKnowledgeToCardCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpCopyKnowledgeToCardResult:
        spec = await uow.services.specs.get_spec(command.spec_id)
        if (
            not spec
            or actor.board_id is None
            or actor.board_id != command.board_id
            or spec.board_id != command.board_id
        ):
            raise EntityNotFoundError("spec", command.spec_id)
        card = await uow.services.cards.get_card(command.card_id)
        if not card or card.board_id != command.board_id:
            raise EntityNotFoundError("card", command.card_id)

        direct_kbs = await uow.services.spec_knowledge.list_knowledge(
            command.spec_id
        )

        fallback = False
        if direct_kbs:
            src_type, src_id = "spec", command.spec_id
            items = [
                {
                    "id": kb.id,
                    "title": kb.title,
                    "description": getattr(kb, "description", None),
                    "content": kb.content,
                    "mime_type": getattr(kb, "mime_type", None) or "text/markdown",
                    "source_kb_id": getattr(kb, "source_kb_id", None),
                    "root_source_kb_id": getattr(kb, "root_source_kb_id", None),
                }
                for kb in direct_kbs
            ]
        else:
            plan = await uow.services.resolve_effective_card_copy_plan(
                board_id=command.board_id,
                spec_id=command.spec_id,
                resource_type="knowledge_base",
            )
            if not plan["fallback"]:
                return McpCopyKnowledgeToCardResult(empty_plan=plan)
            items = await uow.services.load_effective_kb_items(
                plan["source_entity_type"],
                plan["source_entity_id"],
            )
            if not items:
                return McpCopyKnowledgeToCardResult(empty_plan=plan)
            src_type, src_id = plan["source_entity_type"], plan["source_entity_id"]
            fallback = True

        from okto_pulse.core.application.artifact_propagation import (
            artifact_identity_values,
            validate_artifact_selections,
        )

        requested_ids = (
            sorted(command.id_filter)
            if isinstance(command.id_filter, set)
            else list(command.id_filter)
            if command.id_filter is not None
            else None
        )
        validate_artifact_selections(
            source_mockups=None,
            source_knowledge_bases=items,
            mockup_ids=None,
            kb_ids=requested_ids,
            source_type=src_type,
            source_id=src_id,
        )
        if command.id_filter is not None:
            wanted_ids = set(command.id_filter)
            items = [
                item
                for item in items
                if artifact_identity_values(item, "knowledge_base") & wanted_ids
            ]

        from okto_pulse.core.services.application_schemas import CardUpdate

        existing = list(card.knowledge_bases or [])
        existing_sources = {
            str(kb.get("source") or "") for kb in existing if isinstance(kb, dict)
        }
        existing_ids = {
            str(kb.get("id") or "") for kb in existing if isinstance(kb, dict)
        }
        copied = 0
        copied_ids: list[str] = []
        for it in items:
            if src_type == "spec":
                source = f"copied_from_spec:{src_id}:{it['id']}"
            else:
                source = f"copied_from_{src_type}:{src_id}:{it['id']}"
            card_kb_id = f"cardkb_{it['id']}"
            if source in existing_sources or card_kb_id in existing_ids:
                continue
            existing.append(
                {
                    "id": card_kb_id,
                    "title": it["title"],
                    "description": it.get("description"),
                    "content": it["content"],
                    "mime_type": it.get("mime_type") or "text/markdown",
                    "source": source,
                    "source_kb_id": it["id"],
                    "root_source_kb_id": (
                        it.get("root_source_kb_id")
                        or it.get("source_kb_id")
                        or it["id"]
                    ),
                    "immediate_parent_kb_id": it["id"],
                    "author_id": actor.actor_id,
                }
            )
            existing_sources.add(source)
            existing_ids.add(card_kb_id)
            copied_ids.append(card_kb_id)
            copied += 1

        await uow.services.cards.update_card(
            command.card_id,
            actor.actor_id,
            CardUpdate(knowledge_bases=existing),
            allow_card_resource_write=True,
        )
        await commit(uow)
        return McpCopyKnowledgeToCardResult(
            copied=copied,
            copied_ids=copied_ids,
            fallback=fallback,
            src_type=src_type,
            src_id=src_id,
            total_on_card=len(existing),
        )
