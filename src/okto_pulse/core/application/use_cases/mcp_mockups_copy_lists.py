"""MCP mockup, copy and consolidated-list use cases for AF35-S4.

Wrappers keep auth, permission checks, parameter parsing and JSON envelope
mapping. This module owns the relational work through the MCP UnitOfWork path.
"""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
    session_of,
)
from okto_pulse.core.application.use_cases._service_payload import ServicePayload, payload


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
    sanitized = re.sub(r"\s+on\w+\s*=\s*[\"'][^\"']*[\"']", "", sanitized, flags=re.IGNORECASE)
    sanitized = re.sub(r"\s+on\w+\s*=\s*\S+", "", sanitized, flags=re.IGNORECASE)
    return sanitized


def _serialize_knowledge_base(kb: Any, *, include_content: bool = True) -> dict[str, Any]:
    if isinstance(kb, dict):
        data = {
            "id": kb.get("id"),
            "title": kb.get("title") or kb.get("name"),
            "description": kb.get("description"),
            "mime_type": kb.get("mime_type") or kb.get("content_type") or "text/markdown",
        }
        for attr in (
            "ideation_id",
            "refinement_id",
            "spec_id",
            "source",
            "source_type",
            "source_id",
            "source_title",
            "source_version",
            "source_kb_id",
        ):
            if kb.get(attr):
                data[attr] = kb[attr]
        if include_content:
            data["content"] = kb.get("content")
        for attr in ("created_by", "created_at", "updated_at"):
            if kb.get(attr):
                data[attr] = kb[attr]
        return data

    data: dict[str, Any] = {
        "id": getattr(kb, "id", None),
        "title": getattr(kb, "title", None),
        "description": getattr(kb, "description", None),
        "mime_type": getattr(kb, "mime_type", "text/markdown"),
    }
    for attr in (
        "ideation_id",
        "refinement_id",
        "spec_id",
        "source_type",
        "source_id",
        "source_title",
        "source_version",
        "source_kb_id",
    ):
        value = getattr(kb, attr, None)
        if value:
            data[attr] = value
    if include_content:
        data["content"] = getattr(kb, "content", None)
    if getattr(kb, "created_by", None):
        data["created_by"] = kb.created_by
    if getattr(kb, "created_at", None):
        data["created_at"] = kb.created_at.isoformat()
    if getattr(kb, "updated_at", None):
        data["updated_at"] = kb.updated_at.isoformat()
    return data


async def _load_entity_mockups(session: Any, entity_type: str, entity_id: str):
    from okto_pulse.core.services import (
        CardService,
        IdeationService,
        RefinementService,
        SpecService,
        StoryService,
    )

    if entity_type == "spec":
        service = SpecService(session)
        return await service.get_spec(entity_id), service, ServicePayload
    if entity_type == "ideation":
        service = IdeationService(session)
        return await service.get_ideation(entity_id), service, ServicePayload
    if entity_type == "refinement":
        service = RefinementService(session)
        return await service.get_refinement(entity_id), service, ServicePayload
    if entity_type == "card":
        service = CardService(session)
        return await service.get_card(entity_id), service, ServicePayload
    if entity_type == "story":
        service = StoryService(session)
        return await service.get_story(entity_id), service, ServicePayload
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
        await service.update_spec(entity_id, agent_id, update_class(screen_mockups=screens))
    elif entity_type == "ideation":
        await service.update_ideation(entity_id, agent_id, update_class(screen_mockups=screens))
    elif entity_type == "refinement":
        await service.update_refinement(entity_id, agent_id, update_class(screen_mockups=screens))
    elif entity_type == "card":
        await service.update_card(entity_id, agent_id, update_class(screen_mockups=screens))
    elif entity_type == "story":
        await service.update_story(entity_id, agent_id, update_class(screen_mockups=screens))


@dataclass(frozen=True)
class McpCopyMockupsToCardCommand:
    board_id: str
    spec_id: str
    card_id: str
    screen_ids: set[str] | None


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
        uow: Any,
    ) -> McpCopyMockupsToCardResult:
        from okto_pulse.core.services import CardService, SpecService

        session = session_of(uow)
        spec = await SpecService(session).get_spec(command.spec_id)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)
        card_service = CardService(session)
        card = await card_service.get_card(command.card_id)
        if not card:
            raise EntityNotFoundError("card", command.card_id)

        source_mockups = [m for m in (spec.screen_mockups or []) if isinstance(m, dict)]
        fallback = False
        if not source_mockups:
            from okto_pulse.core.services.effective_resource_propagation import (
                load_effective_mockup_items,
                resolve_effective_card_copy_plan,
            )

            plan = await resolve_effective_card_copy_plan(
                session,
                board_id=command.board_id,
                spec_id=command.spec_id,
                resource_type="mockup",
            )
            if not plan["fallback"]:
                return McpCopyMockupsToCardResult(empty_plan=plan)
            source_mockups = await load_effective_mockup_items(
                session, plan["source_entity_type"], plan["source_entity_id"]
            )
            if not source_mockups:
                return McpCopyMockupsToCardResult(empty_plan=plan)
            fallback = True

        if command.screen_ids:
            source_mockups = [
                item for item in source_mockups if item.get("id") in command.screen_ids
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
        uow: Any,
    ) -> McpPayloadResult:
        from okto_pulse.core.services import CardService

        card = await CardService(session_of(uow)).get_card(command.card_id)
        if not card or card.board_id != command.board_id:
            raise EntityNotFoundError("card", command.card_id)
        for kb in card.knowledge_bases or []:
            if kb.get("id") == command.knowledge_id:
                return McpPayloadResult({"success": True, "knowledge": kb})
        raise EntityNotFoundError("card_knowledge", command.knowledge_id)


@dataclass(frozen=True)
class McpCopyQaToCardCommand:
    spec_id: str
    card_id: str


class McpCopyQaToCardUseCase:
    async def execute(
        self,
        command: McpCopyQaToCardCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> McpPayloadResult:
        from okto_pulse.core.services import CardService, SpecService
        from okto_pulse.core.services.main import CommentService

        session = session_of(uow)
        spec = await SpecService(session).get_spec(command.spec_id)
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)
        card = await CardService(session).get_card(command.card_id)
        if not card:
            raise EntityNotFoundError("card", command.card_id)

        qa_items = [qa for qa in (spec.qa_items or []) if _qa_answer_text(qa)]
        if not qa_items:
            return McpPayloadResult({"error": "No answered Q&A to copy"})

        lines = ["## Spec Q&A Context\n"]
        for qa in qa_items:
            lines.append(f"**Q:** {qa.question}\n**A:** {_qa_answer_text(qa)}\n")
        await CommentService(session).create_comment(
            command.card_id,
            actor.actor_id,
            payload(content="\n".join(lines)),
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


class McpAddScreenMockupUseCase:
    async def execute(
        self, command: McpScreenMockupCommand, *, actor: ActorContext, uow: Any
    ) -> McpPayloadResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemError,
            MockupDesignSystemGate,
            normalize_design_system_ref,
        )

        session = session_of(uow)
        screen_id = "sm_" + hashlib.md5(
            f"{command.entity_id}{command.title}{time.time()}".encode()
        ).hexdigest()[:8]
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
        entity, service, update_class = await _load_entity_mockups(
            session, command.entity_type, command.entity_id
        )
        if not entity:
            raise EntityNotFoundError(command.entity_type, command.entity_id)
        try:
            gate_outcome = await MockupDesignSystemGate(session).evaluate_screen(
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
            service, command.entity_type, command.entity_id, actor.actor_id, screens, update_class
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
        self, command: McpScreenMockupCommand, *, actor: ActorContext, uow: Any
    ) -> McpPayloadResult:
        from okto_pulse.core.services.design_system import (
            DesignSystemError,
            MockupDesignSystemGate,
            normalize_design_system_ref,
        )

        session = session_of(uow)
        entity, service, update_class = await _load_entity_mockups(
            session, command.entity_type, command.entity_id
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
            outcomes = await MockupDesignSystemGate(session).gate_delta(
                command.board_id,
                [original],
                [screen],
                entity_type=command.entity_type,
                entity_id=command.entity_id,
            )
        except DesignSystemError as exc:
            return McpPayloadResult(exc.to_dict())
        await _save_entity_mockups(
            service, command.entity_type, command.entity_id, actor.actor_id, screens, update_class
        )
        await commit(uow)
        return McpPayloadResult(
            {
                "success": True,
                "screen": screen,
                "design_system_gate": outcomes[0] if outcomes else {"outcome": "not_applicable"},
            }
        )


class McpAnnotateMockupUseCase:
    async def execute(
        self, command: McpScreenMockupCommand, *, actor: ActorContext, uow: Any
    ) -> McpPayloadResult:
        session = session_of(uow)
        annotation = {
            "id": "an_" + hashlib.md5(
                f"{command.screen_id}{command.text}{time.time()}".encode()
            ).hexdigest()[:8],
            "text": command.text,
            "author_id": actor.actor_id,
        }
        entity, service, update_class = await _load_entity_mockups(
            session, command.entity_type, command.entity_id
        )
        if not entity:
            raise EntityNotFoundError(command.entity_type, command.entity_id)
        screens = list(entity.screen_mockups or [])
        screen = next((s for s in screens if s.get("id") == command.screen_id), None)
        if not screen:
            raise EntityNotFoundError("screen", command.screen_id)
        anns = screen.get("annotations") or []
        anns.append(annotation)
        screen["annotations"] = anns
        await _save_entity_mockups(
            service, command.entity_type, command.entity_id, actor.actor_id, screens, update_class
        )
        await commit(uow)
        return McpPayloadResult({"success": True, "annotation": annotation})


class McpListScreenMockupsUseCase:
    async def execute(
        self, command: McpScreenMockupCommand, *, actor: ActorContext, uow: Any
    ) -> McpPayloadResult:
        entity, _service, _update_class = await _load_entity_mockups(
            session_of(uow), command.entity_type, command.entity_id
        )
        if not entity:
            raise EntityNotFoundError(command.entity_type, command.entity_id)
        screens = list(entity.screen_mockups or [])
        if command.screen_type:
            screens = [s for s in screens if s.get("screen_type") == command.screen_type]
        total = len(screens)
        paginated = screens[command.offset:command.offset + command.limit]
        return McpPayloadResult(
            {
                "entity_type": command.entity_type,
                "entity_id": command.entity_id,
                "total": total,
                "offset": command.offset,
                "limit": command.limit,
                "screens": paginated,
            }
        )


class McpDeleteScreenMockupUseCase:
    async def execute(
        self, command: McpScreenMockupCommand, *, actor: ActorContext, uow: Any
    ) -> McpPayloadResult:
        entity, service, update_class = await _load_entity_mockups(
            session_of(uow), command.entity_type, command.entity_id
        )
        if not entity:
            raise EntityNotFoundError(command.entity_type, command.entity_id)
        screens = list(entity.screen_mockups or [])
        original_len = len(screens)
        screens = [s for s in screens if s.get("id") != command.screen_id]
        if len(screens) == original_len:
            raise EntityNotFoundError("screen", command.screen_id)
        await _save_entity_mockups(
            service, command.entity_type, command.entity_id, actor.actor_id, screens, update_class
        )
        await commit(uow)
        return McpPayloadResult({"success": True, "screen_id": command.screen_id})


@dataclass(frozen=True)
class McpListQaCommand:
    entity_type: str
    entity_id: str
    filters: dict[str, Any]


class McpListQaUseCase:
    async def execute(
        self, command: McpListQaCommand, *, actor: ActorContext, uow: Any
    ) -> McpPayloadResult:
        from okto_pulse.core.services import (
            IdeationQAService,
            RefinementQAService,
            SpecQAService,
        )

        session = session_of(uow)
        if command.entity_type == "spec":
            items = await SpecQAService(session).list_qa(command.entity_id)
        elif command.entity_type == "ideation":
            items = await IdeationQAService(session).list_qa(command.entity_id)
        else:
            items = await RefinementQAService(session).list_qa(command.entity_id)
        await commit(uow)
        if command.filters.get("asked_by"):
            items = [q for q in items if q.asked_by == command.filters["asked_by"]]
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
                        "answered_at": qa.answered_at.isoformat() if qa.answered_at else None,
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
        self, command: McpListKnowledgeCommand, *, actor: ActorContext, uow: Any
    ) -> McpPayloadResult:
        from okto_pulse.core.services import (
            CardService,
            IdeationKnowledgeService,
            IdeationService,
            RefinementKnowledgeService,
            SpecKnowledgeService,
        )

        session = session_of(uow)
        mime_filter: str | None = command.filters.get("mime_type")
        if command.entity_type == "spec":
            items = await SpecKnowledgeService(session).list_knowledge(command.entity_id)
            await commit(uow)
            if mime_filter:
                items = [kb for kb in items if getattr(kb, "mime_type", None) == mime_filter]
            return McpPayloadResult(
                {
                    "entity_type": command.entity_type,
                    "entity_id": command.entity_id,
                    "count": len(items),
                    "knowledge_bases": [
                        {
                            "id": kb.id,
                            "title": kb.title,
                            "description": kb.description,
                            "mime_type": kb.mime_type,
                            "created_at": kb.created_at.isoformat(),
                        }
                        for kb in items
                    ],
                }
            )
        if command.entity_type == "ideation":
            ideation = await IdeationService(session).get_ideation(command.entity_id)
            if not ideation or ideation.board_id != command.board_id:
                raise EntityNotFoundError("ideation", command.entity_id)
            items = await IdeationKnowledgeService(session).list_knowledge(command.entity_id)
            await commit(uow)
            if mime_filter:
                items = [kb for kb in items if getattr(kb, "mime_type", None) == mime_filter]
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
        if command.entity_type == "refinement":
            items = await RefinementKnowledgeService(session).list_knowledge(command.entity_id)
            await commit(uow)
            if mime_filter:
                items = [kb for kb in items if getattr(kb, "mime_type", None) == mime_filter]
            return McpPayloadResult(
                {
                    "entity_type": command.entity_type,
                    "entity_id": command.entity_id,
                    "count": len(items),
                    "knowledge_bases": [
                        {
                            "id": kb.id,
                            "title": kb.title,
                            "description": kb.description,
                            "mime_type": kb.mime_type,
                            "created_at": kb.created_at.isoformat(),
                        }
                        for kb in items
                    ],
                }
            )
        card = await CardService(session).get_card(command.entity_id)
        if not card or card.board_id != command.board_id:
            raise EntityNotFoundError("card", command.entity_id)
        kbs = list(card.knowledge_bases or [])
        if mime_filter:
            kbs = [kb for kb in kbs if kb.get("mime_type") == mime_filter]
        return McpPayloadResult(
            {
                "entity_type": command.entity_type,
                "entity_id": command.entity_id,
                "count": len(kbs),
                "knowledge_bases": kbs,
            }
        )


@dataclass(frozen=True)
class McpListSnapshotsCommand:
    entity_type: str
    entity_id: str


class McpListSnapshotsUseCase:
    async def execute(
        self, command: McpListSnapshotsCommand, *, actor: ActorContext, uow: Any
    ) -> McpPayloadResult:
        from okto_pulse.core.services import IdeationService, RefinementService

        if command.entity_type == "ideation":
            snapshots = await IdeationService(session_of(uow)).list_snapshots(
                command.entity_id
            )
            await commit(uow)
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
        snapshots = await RefinementService(session_of(uow)).list_snapshots(
            command.entity_id
        )
        await commit(uow)
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
