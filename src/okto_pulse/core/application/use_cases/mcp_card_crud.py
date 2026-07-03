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

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
    session_of,
)
from okto_pulse.core.services import (
    BoardService,
    CardService,
    SpecKnowledgeService,
    SpecService,
)


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
    """Create a card on a board with the legacy MCP orchestration preserved
    exactly: ``CardService.create_card(skip_ownership_check=True)`` → commit →
    (optional) bidirectional ``test_scenarios.linked_task_ids`` backlink on the
    parent spec → ``card_created`` activity log → commit. Two commits + the flush,
    same as the legacy tool. ``CardOperationError``/``ValueError`` from
    ``create_card`` propagate for the adapter to map; ``card=None`` →
    adapter ``"Failed to create card"`` (no scenario link / no log)."""

    async def execute(
        self, command: McpCreateCardCommand, *, actor: ActorContext, uow: Any
    ) -> McpCreateCardResult:
        session = session_of(uow)
        service = CardService(session)
        card = await service.create_card(
            command.board_id, actor.actor_id, command.data, skip_ownership_check=True
        )
        await commit(uow)

        if not card:
            return McpCreateCardResult(None)

        if command.scenario_ids_list:
            spec_obj = await SpecService(session).get_spec(command.spec_id)
            if spec_obj and spec_obj.test_scenarios:
                from okto_pulse.core.services.persistence_mutation import (
                    mark_mutable_field_modified,
                )

                scenarios = list(spec_obj.test_scenarios)
                changed = False
                for sc in scenarios:
                    if sc.get("id") in command.scenario_ids_list:
                        task_ids = list(sc.get("linked_task_ids") or [])
                        if card.id not in task_ids:
                            task_ids.append(card.id)
                            sc["linked_task_ids"] = task_ids
                            changed = True
                if changed:
                    spec_obj.test_scenarios = scenarios
                    mark_mutable_field_modified(spec_obj, "test_scenarios")
                    await session.flush()

        await BoardService(session)._log_activity(
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
        self, command: McpGetCardCommand, *, actor: ActorContext, uow: Any
    ) -> McpGetCardResult:
        card = await CardService(session_of(uow)).get_card(command.card_id)
        if not card or card.board_id != command.board_id:
            raise EntityNotFoundError("card", command.card_id)
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
        self, command: McpUpdateCardCommand, *, actor: ActorContext, uow: Any
    ) -> McpUpdateCardResult:
        session = session_of(uow)
        service = CardService(session)
        card = await service.get_card(command.card_id)
        if not card or card.board_id != command.board_id:
            raise EntityNotFoundError("card", command.card_id)
        updated = await service.update_card(
            command.card_id, actor.actor_id, command.data
        )
        await BoardService(session)._log_activity(
            board_id=command.board_id,
            card_id=command.card_id,
            action="card_updated",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            details=command.activity_details,
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
        self, command: McpMoveCardCommand, *, actor: ActorContext, uow: Any
    ) -> McpMoveCardResult:
        session = session_of(uow)
        service = CardService(session)
        card = await service.get_card(command.card_id)
        if not card or card.board_id != command.board_id:
            raise EntityNotFoundError("card", command.card_id)
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
    __slots__ = ("deleted",)

    def __init__(self, deleted: bool) -> None:
        self.deleted = deleted


class McpDeleteCardUseCase:
    """Delete a board-scoped card, writing the ``card_deleted`` activity row
    BEFORE the delete in the SAME transaction (legacy MCP order/atomicity).
    Cross-board/missing → ``EntityNotFoundError`` (adapter ``"Card not found"``)."""

    async def execute(
        self, command: McpDeleteCardCommand, *, actor: ActorContext, uow: Any
    ) -> McpDeleteCardResult:
        session = session_of(uow)
        service = CardService(session)
        card = await service.get_card(command.card_id)
        if not card or card.board_id != command.board_id:
            raise EntityNotFoundError("card", command.card_id)
        await BoardService(session)._log_activity(
            board_id=command.board_id,
            card_id=command.card_id,
            action="card_deleted",
            actor_type="agent",
            actor_id=actor.actor_id,
            actor_name=actor.actor_name,
            details={"title": card.title},
        )
        deleted = await service.delete_card(command.card_id, actor.actor_id)
        await commit(uow)
        return McpDeleteCardResult(deleted)


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
    separate use cases). No board-scope (the legacy tool had none here). The
    legacy ``db.commit()`` was a no-op on a read — skipped."""

    async def execute(
        self, command: McpGetCardDependenciesCommand, *, actor: ActorContext, uow: Any
    ) -> McpGetCardDependenciesResult:
        service = CardService(session_of(uow))
        deps = await service.get_dependencies(command.card_id)
        dependents = await service.get_dependents(command.card_id)
        deps_met, blocking = await service.check_dependencies_met(command.card_id)
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
    bool — preserved here (no raise on ``False``)."""

    async def execute(
        self,
        command: McpRemoveCardDependencyCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> McpRemoveCardDependencyResult:
        removed = await CardService(session_of(uow)).remove_dependency(
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
        id_filter: set[str] | None,
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
        uow: Any,
    ) -> McpCopyKnowledgeToCardResult:
        session = session_of(uow)

        spec = await SpecService(session).get_spec(command.spec_id)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)
        card = await CardService(session).get_card(command.card_id)
        if not card:
            raise EntityNotFoundError("card", command.card_id)

        direct_kbs = await SpecKnowledgeService(session).list_knowledge(
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
                }
                for kb in direct_kbs
            ]
        else:
            from okto_pulse.core.services.effective_resource_propagation import (
                load_effective_kb_items,
                resolve_effective_card_copy_plan,
            )

            plan = await resolve_effective_card_copy_plan(
                session,
                board_id=command.board_id,
                spec_id=command.spec_id,
                resource_type="knowledge_base",
            )
            if not plan["fallback"]:
                return McpCopyKnowledgeToCardResult(empty_plan=plan)
            items = await load_effective_kb_items(
                session, plan["source_entity_type"], plan["source_entity_id"]
            )
            if not items:
                return McpCopyKnowledgeToCardResult(empty_plan=plan)
            src_type, src_id = plan["source_entity_type"], plan["source_entity_id"]
            fallback = True

        if command.id_filter is not None:
            items = [it for it in items if it["id"] in command.id_filter]

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
                    "author_id": actor.actor_id,
                }
            )
            existing_sources.add(source)
            existing_ids.add(card_kb_id)
            copied_ids.append(card_kb_id)
            copied += 1

        await CardService(session).update_card(
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
