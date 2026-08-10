"""MCP mockup, copy and consolidated-list use cases for AF35-S4.

Wrappers keep auth, permission checks, parameter parsing and JSON envelope
mapping. This module owns the relational work through the MCP UnitOfWork path.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

import hashlib
import re
import time
from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.mutation_permissions import (
    card_requirement,
    entity_state,
)
from okto_pulse.core.application.use_cases._service_payload import (
    ServicePayload,
    payload,
)
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog
from okto_pulse.core.services.knowledge_governance_projection import (
    serialize_knowledge_base as _serialize_knowledge_base,
)


@dataclass(frozen=True)
class McpPayloadResult:
    payload: Any


def _qa_selected_labels(qa: Any) -> list[str]:
    selected = getattr(qa, "selected", None)
    choices = getattr(qa, "choices", None)
    if isinstance(qa, dict):
        selected = qa.get("selected")
        choices = qa.get("choices")
    selected_ids = [str(item) for item in (selected or [])]
    labels_by_id = {
        str(choice.get("id")): str(choice.get("label"))
        for choice in (choices or [])
        if isinstance(choice, dict) and choice.get("id") is not None
    }
    return [labels_by_id.get(item, item) for item in selected_ids]


def _qa_answer_text(qa: Any) -> str | None:
    answer = getattr(qa, "answer", None)
    if isinstance(qa, dict):
        answer = qa.get("answer")
    if answer:
        return str(answer)
    labels = _qa_selected_labels(qa)
    if labels:
        return ", ".join(labels)
    return None


def _sanitize_html(html: str) -> str:
    sanitized = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    sanitized = re.sub(
        r"\s+on\w+\s*=\s*[\"'][^\"']*[\"']", "", sanitized, flags=re.IGNORECASE
    )
    sanitized = re.sub(r"\s+on\w+\s*=\s*\S+", "", sanitized, flags=re.IGNORECASE)
    return sanitized


def _project_screen_mockup(
    screen: dict[str, Any],
    *,
    include_content: bool,
) -> dict[str, Any]:
    """Return a stable list projection without leaking heavy HTML by default."""

    projected = dict(screen)
    raw_content = screen.get("html_content")
    html_content = "" if raw_content is None else str(raw_content)
    encoded_content = html_content.encode("utf-8")
    projected["has_html_content"] = bool(html_content)
    projected["html_content_bytes"] = len(encoded_content)
    projected["html_content_sha256"] = hashlib.sha256(encoded_content).hexdigest()
    if include_content:
        projected["html_content"] = html_content
    else:
        projected.pop("html_content", None)
    return projected


def _in_board_scope(record: Any, board_id: str, actor: ActorContext) -> bool:
    """Require actor, command, and canonical entity to share one board."""
    return bool(record and actor.board_id == board_id and record.board_id == board_id)


async def _load_entity_mockups(
    services: ApplicationServiceCatalog,
    entity_type: str,
    entity_id: str,
    board_id: str,
    actor: ActorContext,
):
    if entity_type == "spec":
        service = services.specs
        entity = await service.get_spec(entity_id)
        return (
            entity if _in_board_scope(entity, board_id, actor) else None,
            service,
            ServicePayload,
        )
    if entity_type == "ideation":
        service = services.ideations
        entity = await service.get_ideation(entity_id)
        return (
            entity if _in_board_scope(entity, board_id, actor) else None,
            service,
            ServicePayload,
        )
    if entity_type == "refinement":
        service = services.refinements
        entity = await service.get_refinement(entity_id)
        return (
            entity if _in_board_scope(entity, board_id, actor) else None,
            service,
            ServicePayload,
        )
    if entity_type == "card":
        service = services.cards
        entity = await service.get_card(entity_id)
        return (
            entity if _in_board_scope(entity, board_id, actor) else None,
            service,
            ServicePayload,
        )
    if entity_type == "story":
        service = services.stories
        entity = await service.get_story(entity_id)
        return (
            entity if _in_board_scope(entity, board_id, actor) else None,
            service,
            ServicePayload,
        )
    return None, None, None


async def _save_entity_mockups(
    service: Any,
    entity_type: str,
    entity_id: str,
    agent_id: str,
    screens: list[dict[str, Any]],
    update_class: Any,
) -> None:
    if entity_type == "spec":
        await service.update_spec(
            entity_id, agent_id, update_class(screen_mockups=screens)
        )
    elif entity_type == "ideation":
        await service.update_ideation(
            entity_id, agent_id, update_class(screen_mockups=screens)
        )
    elif entity_type == "refinement":
        await service.update_refinement(
            entity_id, agent_id, update_class(screen_mockups=screens)
        )
    elif entity_type == "card":
        await service.update_card(
            entity_id, agent_id, update_class(screen_mockups=screens)
        )
    elif entity_type == "story":
        await service.update_story(
            entity_id, agent_id, update_class(screen_mockups=screens)
        )


@dataclass(frozen=True)
class McpCopyMockupsToCardCommand:
    board_id: str
    spec_id: str
    card_id: str
    screen_ids: list[str] | set[str] | tuple[str, ...] | None


class McpCopyMockupsToCardResult:
    __slots__ = ("empty_plan", "copied", "total_on_card", "fallback")

    def __init__(
        self,
        *,
        empty_plan: dict[str, Any] | None = None,
        copied: int = 0,
        total_on_card: int = 0,
        fallback: bool = False,
    ) -> None:
        self.empty_plan = empty_plan
        self.copied = copied
        self.total_on_card = total_on_card
        self.fallback = fallback


class McpCopyMockupsToCardUseCase:
    async def execute(
        self,
        command: McpCopyMockupsToCardCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpCopyMockupsToCardResult:

        spec = await uow.services.specs.get_spec(command.spec_id)
        if not _in_board_scope(spec, command.board_id, actor):
            raise EntityNotFoundError("spec", command.spec_id)
        card_service = uow.services.cards
        card = await card_service.get_card(command.card_id)
        if not _in_board_scope(card, command.board_id, actor):
            raise EntityNotFoundError("card", command.card_id)
        await require_authorization(
            actor,
            card_requirement(
                "card.copy_from_spec.mockups",
                state=entity_state(card),
            ),
            uow=uow,
            board_id=command.board_id,
        )

        from okto_pulse.core.application.artifact_propagation import (
            artifact_identity_values,
            validate_artifact_selections,
        )

        source_mockups = [m for m in (spec.screen_mockups or []) if isinstance(m, dict)]
        fallback = False
        source_type, source_id = "spec", command.spec_id
        plan = await uow.services.resolve_effective_card_copy_plan(
            board_id=command.board_id,
            spec_id=command.spec_id,
            resource_type="mockup",
        )
        if not source_mockups:
            if not plan["fallback"]:
                return McpCopyMockupsToCardResult(empty_plan=plan)
            fallback_parent, _service, _update_class = await _load_entity_mockups(
                uow.services,
                plan["source_entity_type"],
                plan["source_entity_id"],
                command.board_id,
                actor,
            )
            if not fallback_parent:
                raise EntityNotFoundError("spec", command.spec_id)
            source_mockups = await uow.services.load_effective_mockup_items(
                plan["source_entity_type"],
                plan["source_entity_id"],
            )
            if not source_mockups:
                return McpCopyMockupsToCardResult(empty_plan=plan)
            source_type = plan["source_entity_type"]
            source_id = plan["source_entity_id"]
            fallback = True
        else:
            # A Spec may carry only the mockups selected at derivation time while
            # still inheriting other effective mockups from its Refinement or
            # Ideation.  The Resource Gate evaluates every effective identity, so
            # copy the union.  Direct items win when their lineage identity
            # intersects an inherited item; genuinely distinct inherited items
            # remain available for explicit selection and card coverage.
            inherited_refs = [
                item
                for item in plan.get("inherited_refs", ())
                if isinstance(item, dict)
            ]
            refs_by_parent: dict[tuple[str, str], list[set[str]]] = {}
            for ref in inherited_refs:
                parent_type = str(ref.get("source_entity_type") or "").strip()
                parent_id = str(ref.get("source_entity_id") or "").strip()
                identities = artifact_identity_values(ref, "mockup")
                if parent_type and parent_id and identities:
                    refs_by_parent.setdefault((parent_type, parent_id), []).append(
                        identities
                    )

            seen_identities = [
                artifact_identity_values(item, "mockup") for item in source_mockups
            ]
            for (parent_type, parent_id), wanted_identities in refs_by_parent.items():
                inherited_items = await uow.services.load_effective_mockup_items(
                    parent_type,
                    parent_id,
                )
                for item in inherited_items:
                    if not isinstance(item, dict):
                        continue
                    identities = artifact_identity_values(item, "mockup")
                    if not identities or not any(
                        identities & wanted for wanted in wanted_identities
                    ):
                        continue
                    if any(identities & seen for seen in seen_identities):
                        continue
                    source_mockups.append(item)
                    seen_identities.append(identities)

        requested_ids = (
            sorted(command.screen_ids)
            if isinstance(command.screen_ids, set)
            else list(command.screen_ids)
            if command.screen_ids is not None
            else None
        )
        validate_artifact_selections(
            source_mockups=source_mockups,
            source_knowledge_bases=None,
            mockup_ids=requested_ids,
            kb_ids=None,
            source_type=source_type,
            source_id=source_id,
        )
        if command.screen_ids is not None:
            wanted_ids = set(command.screen_ids)
            source_mockups = [
                item
                for item in source_mockups
                if artifact_identity_values(item, "mockup") & wanted_ids
            ]

        existing = list(card.screen_mockups or [])
        existing_ids = {m.get("id") for m in existing if isinstance(m, dict)}
        copied = 0
        for mockup in source_mockups:
            if mockup.get("id") not in existing_ids:
                existing.append(mockup)
                existing_ids.add(mockup.get("id"))
                copied += 1

        await card_service.update_card(
            command.card_id,
            actor.actor_id,
            payload(screen_mockups=existing),
            allow_card_resource_write=True,
        )
        await commit(uow)
        return McpCopyMockupsToCardResult(
            copied=copied, total_on_card=len(existing), fallback=fallback
        )


@dataclass(frozen=True)
class McpGetCardKnowledgeCommand:
    board_id: str
    card_id: str
    knowledge_id: str


class McpGetCardKnowledgeUseCase:
    async def execute(
        self,
        command: McpGetCardKnowledgeCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:

        card = await uow.services.cards.get_card(command.card_id)
        if not _in_board_scope(card, command.board_id, actor):
            raise EntityNotFoundError("card", command.card_id)
        from okto_pulse.core.application.effective_knowledge_read import (
            load_effective_card_knowledge,
        )

        for kb in await load_effective_card_knowledge(uow.services, card):
            if kb.get("id") == command.knowledge_id:
                return McpPayloadResult(
                    {
                        "success": True,
                        "knowledge": _serialize_knowledge_base(kb),
                    }
                )
        raise EntityNotFoundError("card_knowledge", command.knowledge_id)


@dataclass(frozen=True)
class McpCopyQaToCardCommand:
    board_id: str
    spec_id: str
    card_id: str


class McpCopyQaToCardUseCase:
    async def execute(
        self,
        command: McpCopyQaToCardCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:

        spec = await uow.services.specs.get_spec(command.spec_id)
        if not _in_board_scope(spec, command.board_id, actor):
            raise EntityNotFoundError("spec", command.spec_id)
        card = await uow.services.cards.get_card(command.card_id)
        if not _in_board_scope(card, command.board_id, actor):
            raise EntityNotFoundError("card", command.card_id)
        await require_authorization(
            actor,
            card_requirement(
                "card.copy_from_spec.qa",
                state=entity_state(card),
            ),
            uow=uow,
            board_id=command.board_id,
        )

        qa_items = [qa for qa in (spec.qa_items or []) if _qa_answer_text(qa)]
        if not qa_items:
            return McpPayloadResult(
                {"success": True, "copied": 0, "reason": "no_answered_qa"}
            )

        lines = ["## Spec Q&A Context\n"]
        for qa in qa_items:
            lines.append(f"**Q:** {qa.question}\n**A:** {_qa_answer_text(qa)}\n")
        await uow.services.comments.create_comment(
            command.card_id,
            actor.actor_id,
            payload(
                content="\n".join(lines),
                comment_type="text",
                choices=None,
                allow_free_text=False,
            ),
        )
        await commit(uow)
        return McpPayloadResult({"success": True, "copied": len(qa_items)})


@dataclass(frozen=True)
class McpScreenMockupCommand:
    board_id: str
    entity_id: str
    entity_type: str
    screen_id: str = ""
    title: str = ""
    description: str = ""
    screen_type: str = ""
    html_content: str = ""
    design_system_ref: str = ""
    design_system_version: int | None = None
    design_system_evidence: Any = None
    text: str = ""
    offset: int = 0
    limit: int = 50
    include_content: bool = False


class McpAddScreenMockupUseCase:
    async def execute(
        self,
        command: McpScreenMockupCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                f"{command.entity_type}.mockups.create",
                legacy_operation="specs:update",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        from okto_pulse.core.services.design_system import (
            DesignSystemError,
            normalize_design_system_ref,
        )

        entity, service, update_class = await _load_entity_mockups(
            uow.services,
            command.entity_type,
            command.entity_id,
            command.board_id,
            actor,
        )
        if not entity:
            raise EntityNotFoundError(command.entity_type, command.entity_id)

        screen_id = (
            "sm_"
            + hashlib.md5(
                f"{command.entity_id}{command.title}{time.time()}".encode()
            ).hexdigest()[:8]
        )
        screen = {
            "id": screen_id,
            "title": command.title,
            "description": command.description or None,
            "screen_type": command.screen_type or "page",
            "html_content": _sanitize_html(command.html_content),
            "annotations": [],
            "order": 0,
            "design_system_ref": normalize_design_system_ref(
                command.design_system_ref, command.design_system_version
            ),
            "design_system_evidence": command.design_system_evidence,
        }
        try:
            gate_outcome = await uow.services.mockup_design_gate.evaluate_screen(
                command.board_id,
                screen,
                entity_type=command.entity_type,
                entity_id=command.entity_id,
            )
        except DesignSystemError as exc:
            return McpPayloadResult(exc.to_dict())
        screens = list(entity.screen_mockups or [])
        screen["order"] = len(screens)
        screens.append(screen)
        await _save_entity_mockups(
            service,
            command.entity_type,
            command.entity_id,
            actor.actor_id,
            screens,
            update_class,
        )
        await commit(uow)
        return McpPayloadResult(
            {
                "success": True,
                "entity_type": command.entity_type,
                "screen": screen,
                "design_system_gate": gate_outcome,
            }
        )


class McpUpdateScreenMockupUseCase:
    async def execute(
        self,
        command: McpScreenMockupCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                f"{command.entity_type}.mockups.edit",
                legacy_operation="specs:update",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        from okto_pulse.core.services.design_system import (
            DesignSystemError,
            normalize_design_system_ref,
        )

        entity, service, update_class = await _load_entity_mockups(
            uow.services,
            command.entity_type,
            command.entity_id,
            command.board_id,
            actor,
        )
        if not entity:
            raise EntityNotFoundError(command.entity_type, command.entity_id)
        screens = list(entity.screen_mockups or [])
        screen = next((s for s in screens if s.get("id") == command.screen_id), None)
        if not screen:
            raise EntityNotFoundError("screen", command.screen_id)
        original = dict(screen)
        if command.title:
            screen["title"] = command.title
        if command.description:
            screen["description"] = command.description
        if command.screen_type:
            screen["screen_type"] = command.screen_type
        if command.html_content:
            screen["html_content"] = _sanitize_html(command.html_content)
        if command.design_system_ref:
            screen["design_system_ref"] = normalize_design_system_ref(
                command.design_system_ref, command.design_system_version
            )
        if command.design_system_evidence is not None:
            screen["design_system_evidence"] = command.design_system_evidence
        try:
            outcomes = await uow.services.mockup_design_gate.gate_delta(
                command.board_id,
                [original],
                [screen],
                entity_type=command.entity_type,
                entity_id=command.entity_id,
            )
        except DesignSystemError as exc:
            return McpPayloadResult(exc.to_dict())
        await _save_entity_mockups(
            service,
            command.entity_type,
            command.entity_id,
            actor.actor_id,
            screens,
            update_class,
        )
        await commit(uow)
        return McpPayloadResult(
            {
                "success": True,
                "screen": screen,
                "design_system_gate": outcomes[0]
                if outcomes
                else {"outcome": "not_applicable"},
            }
        )


class McpAnnotateMockupUseCase:
    async def execute(
        self,
        command: McpScreenMockupCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                f"{command.entity_type}.mockups.annotate",
                legacy_operation="specs:update",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        entity, service, update_class = await _load_entity_mockups(
            uow.services,
            command.entity_type,
            command.entity_id,
            command.board_id,
            actor,
        )
        if not entity:
            raise EntityNotFoundError(command.entity_type, command.entity_id)
        annotation = {
            "id": "an_"
            + hashlib.md5(
                f"{command.screen_id}{command.text}{time.time()}".encode()
            ).hexdigest()[:8],
            "text": command.text,
            "author_id": actor.actor_id,
        }
        screens = list(entity.screen_mockups or [])
        screen = next((s for s in screens if s.get("id") == command.screen_id), None)
        if not screen:
            raise EntityNotFoundError("screen", command.screen_id)
        anns = screen.get("annotations") or []
        anns.append(annotation)
        screen["annotations"] = anns
        await _save_entity_mockups(
            service,
            command.entity_type,
            command.entity_id,
            actor.actor_id,
            screens,
            update_class,
        )
        await commit(uow)
        return McpPayloadResult({"success": True, "annotation": annotation})


class McpListScreenMockupsUseCase:
    async def execute(
        self,
        command: McpScreenMockupCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                f"{command.entity_type}.mockups.read",
                legacy_operation="board:read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        entity, _service, _update_class = await _load_entity_mockups(
            uow.services,
            command.entity_type,
            command.entity_id,
            command.board_id,
            actor,
        )
        if not entity:
            raise EntityNotFoundError(command.entity_type, command.entity_id)
        screens = list(entity.screen_mockups or [])
        if command.screen_type:
            screens = [
                s for s in screens if s.get("screen_type") == command.screen_type
            ]
        total = len(screens)
        paginated = screens[command.offset : command.offset + command.limit]
        projected = [
            _project_screen_mockup(screen, include_content=command.include_content)
            for screen in paginated
        ]
        return McpPayloadResult(
            {
                "entity_type": command.entity_type,
                "entity_id": command.entity_id,
                "total": total,
                "offset": command.offset,
                "limit": command.limit,
                "screens": projected,
            }
        )


class McpDeleteScreenMockupUseCase:
    async def execute(
        self,
        command: McpScreenMockupCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                f"{command.entity_type}.mockups.delete",
                legacy_operation="specs:update",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        entity, service, update_class = await _load_entity_mockups(
            uow.services,
            command.entity_type,
            command.entity_id,
            command.board_id,
            actor,
        )
        if not entity:
            raise EntityNotFoundError(command.entity_type, command.entity_id)
        screens = list(entity.screen_mockups or [])
        original_len = len(screens)
        screens = [s for s in screens if s.get("id") != command.screen_id]
        if len(screens) == original_len:
            raise EntityNotFoundError("screen", command.screen_id)
        await _save_entity_mockups(
            service,
            command.entity_type,
            command.entity_id,
            actor.actor_id,
            screens,
            update_class,
        )
        await commit(uow)
        return McpPayloadResult({"success": True, "screen_id": command.screen_id})


# Canonical Q&A answer-state filter tokens (okto_pulse_list_qa filters.status).
# "answered" ⇔ answered_at IS NOT NULL; "unanswered"/"open" ⇔ answered_at IS NULL —
# the only reliable predicate, because choice/multi_choice answers leave `answer`
# NULL but always stamp answered_at (see services/main.py _attach_open_qa_counts).
QA_STATUS_ANSWERED = "answered"
QA_STATUS_UNANSWERED = frozenset({"unanswered", "open"})
QA_STATUS_VALUES = frozenset({QA_STATUS_ANSWERED}) | QA_STATUS_UNANSWERED


@dataclass(frozen=True)
class McpListQaCommand:
    board_id: str
    entity_type: str
    entity_id: str
    filters: dict[str, Any]


class McpListQaUseCase:
    async def execute(
        self, command: McpListQaCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpPayloadResult:
        parent, _service, _update_class = await _load_entity_mockups(
            uow.services,
            command.entity_type,
            command.entity_id,
            command.board_id,
            actor,
        )
        if not parent:
            raise EntityNotFoundError(command.entity_type, command.entity_id)
        if command.entity_type == "spec":
            items = await uow.services.spec_qa.list_qa(command.entity_id)
        elif command.entity_type == "ideation":
            items = await uow.services.ideation_qa.list_qa(command.entity_id)
        else:
            items = await uow.services.refinement_qa.list_qa(command.entity_id)
        if command.filters.get("asked_by"):
            items = [q for q in items if q.asked_by == command.filters["asked_by"]]
        status_filter = command.filters.get("status")
        if status_filter is not None:
            normalized = str(status_filter).strip().lower()
            if normalized == QA_STATUS_ANSWERED:
                items = [q for q in items if q.answered_at is not None]
            elif normalized in QA_STATUS_UNANSWERED:
                items = [q for q in items if q.answered_at is None]
            else:
                # Fail-closed domain guard (the MCP handler pre-validates the same
                # vocabulary; this keeps a direct use-case call honest too).
                raise ValueError(
                    f"status='{status_filter}' is not a valid Q&A answer-state "
                    f"filter; expected one of {sorted(QA_STATUS_VALUES)}"
                )
        return McpPayloadResult(
            {
                "entity_type": command.entity_type,
                "entity_id": command.entity_id,
                "count": len(items),
                "qa_items": [
                    {
                        "id": qa.id,
                        "question": qa.question,
                        "question_type": qa.question_type,
                        "choices": qa.choices,
                        "allow_free_text": getattr(qa, "allow_free_text", None),
                        "answer": qa.answer,
                        "selected": qa.selected,
                        "asked_by": qa.asked_by,
                        "answered_by": qa.answered_by,
                        "created_at": qa.created_at.isoformat(),
                        "answered_at": qa.answered_at.isoformat()
                        if qa.answered_at
                        else None,
                    }
                    for qa in items
                ],
            }
        )


@dataclass(frozen=True)
class McpListKnowledgeCommand:
    board_id: str
    entity_type: str
    entity_id: str
    filters: dict[str, Any]


class McpListKnowledgeUseCase:
    async def execute(
        self,
        command: McpListKnowledgeCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:
        parent, _service, _update_class = await _load_entity_mockups(
            uow.services,
            command.entity_type,
            command.entity_id,
            command.board_id,
            actor,
        )
        if not parent:
            raise EntityNotFoundError(command.entity_type, command.entity_id)

        mime_filter: str | None = command.filters.get("mime_type")
        if command.entity_type == "spec":
            from okto_pulse.core.application.effective_knowledge_read import (
                load_effective_spec_knowledge,
            )

            items = await load_effective_spec_knowledge(uow.services, parent)
            if mime_filter:
                items = [kb for kb in items if kb.get("mime_type") == mime_filter]
            return McpPayloadResult(
                {
                    "entity_type": command.entity_type,
                    "entity_id": command.entity_id,
                    "count": len(items),
                    "knowledge_bases": [
                        _serialize_knowledge_base(kb, include_content=False)
                        for kb in items
                    ],
                }
            )
        if command.entity_type == "ideation":
            items = await uow.services.ideation_knowledge.list_knowledge(
                command.entity_id
            )
            if mime_filter:
                items = [
                    kb for kb in items if getattr(kb, "mime_type", None) == mime_filter
                ]
            return McpPayloadResult(
                {
                    "entity_type": command.entity_type,
                    "entity_id": command.entity_id,
                    "count": len(items),
                    "knowledge_bases": [
                        _serialize_knowledge_base(
                            kb,
                            include_content=False,
                        )
                        for kb in items
                    ],
                }
            )
        if command.entity_type == "refinement":
            items = await uow.services.refinement_knowledge.list_knowledge(
                command.entity_id
            )
            if mime_filter:
                items = [
                    kb for kb in items if getattr(kb, "mime_type", None) == mime_filter
                ]
            return McpPayloadResult(
                {
                    "entity_type": command.entity_type,
                    "entity_id": command.entity_id,
                    "count": len(items),
                    "knowledge_bases": [
                        _serialize_knowledge_base(kb, include_content=False)
                        for kb in items
                    ],
                }
            )
        card = parent
        from okto_pulse.core.application.effective_knowledge_read import (
            load_effective_card_knowledge,
        )

        kbs = await load_effective_card_knowledge(uow.services, card)
        if mime_filter:
            kbs = [kb for kb in kbs if kb.get("mime_type") == mime_filter]
        return McpPayloadResult(
            {
                "entity_type": command.entity_type,
                "entity_id": command.entity_id,
                "count": len(kbs),
                # Bounded projection (same serializer as the spec branch): omit
                # `content` from a card knowledge LISTING while preserving governance
                # + metadata/lineage. `with_knowledge_governance(dict(kb), kb)` copied
                # the whole effective dict, leaking `content` — read the body via the
                # single-item get, not the list.
                "knowledge_bases": [
                    _serialize_knowledge_base(kb, include_content=False) for kb in kbs
                ],
            }
        )


@dataclass(frozen=True)
class McpListSnapshotsCommand:
    board_id: str
    entity_type: str
    entity_id: str


class McpListSnapshotsUseCase:
    async def execute(
        self,
        command: McpListSnapshotsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> McpPayloadResult:
        parent, _service, _update_class = await _load_entity_mockups(
            uow.services,
            command.entity_type,
            command.entity_id,
            command.board_id,
            actor,
        )
        if not parent:
            raise EntityNotFoundError(command.entity_type, command.entity_id)
        if command.entity_type == "ideation":
            snapshots = await uow.services.ideations.list_snapshots(command.entity_id)
            return McpPayloadResult(
                {
                    "entity_type": command.entity_type,
                    "entity_id": command.entity_id,
                    "count": len(snapshots),
                    "snapshots": [
                        {
                            "version": s.version,
                            "title": s.title,
                            "complexity": s.complexity,
                            "created_by": s.created_by,
                            "created_at": s.created_at.isoformat(),
                        }
                        for s in snapshots
                    ],
                }
            )
        snapshots = await uow.services.refinements.list_snapshots(command.entity_id)
        return McpPayloadResult(
            {
                "entity_type": command.entity_type,
                "entity_id": command.entity_id,
                "count": len(snapshots),
                "snapshots": [
                    {
                        "version": s.version,
                        "title": s.title,
                        "created_by": s.created_by,
                        "created_at": s.created_at.isoformat(),
                    }
                    for s in snapshots
                ],
            }
        )
