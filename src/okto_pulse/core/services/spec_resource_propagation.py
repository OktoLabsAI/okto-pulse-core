"""Spec resource auto-propagation into linked cards."""

from __future__ import annotations

import copy
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.models.db import ActivityLog, Board, Card, Spec, SpecKnowledgeBase
from okto_pulse.core.services.architecture import (
    ArchitectureDesignRepository,
    ArchitecturePropagationService,
)


SUPPORTED_RESOURCE_TYPES = ("knowledge_base", "architecture", "mockup")


class SpecResourcePropagationService:
    """Copy selected Spec resources to a card according to board settings."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def propagate_for_card(
        self,
        *,
        board_id: str,
        spec_id: str,
        card_id: str,
        actor_id: str,
        trigger: str,
    ) -> dict[str, Any]:
        board = await self.db.get(Board, board_id)
        if not board:
            return {"enabled": False, "reason": "board_not_found"}

        resource_types = self._resolve_resource_types(board)
        if not resource_types:
            return {"enabled": False, "reason": "disabled"}

        spec = await self.db.get(Spec, spec_id)
        card = await self.db.get(Card, card_id)
        if not spec or not card or spec.board_id != board_id or card.board_id != board_id:
            return {"enabled": True, "reason": "spec_or_card_not_found"}

        results: dict[str, dict[str, Any]] = {}
        for resource_type in resource_types:
            if resource_type == "knowledge_base":
                results[resource_type] = await self._copy_knowledge(spec, card, actor_id)
            elif resource_type == "mockup":
                results[resource_type] = await self._copy_mockups(spec, card)
            elif resource_type == "architecture":
                results[resource_type] = await self._copy_architecture(spec, card, actor_id)

        await self._log_audit(
            board_id=board_id,
            card_id=card_id,
            actor_id=actor_id,
            trigger=trigger,
            spec_id=spec_id,
            resource_types=resource_types,
            results=results,
        )

        return {
            "enabled": True,
            "trigger": trigger,
            "spec_id": spec_id,
            "card_id": card_id,
            "resource_types": resource_types,
            "results": results,
        }

    def _resolve_resource_types(self, board: Board) -> list[str]:
        settings = board.settings or {}
        if not settings.get("auto_derive_spec_resources_enabled", False):
            return []
        resource_types = settings.get("auto_derive_spec_resource_types") or []
        normalized: list[str] = []
        for value in resource_types:
            resource_type = getattr(value, "value", value)
            if resource_type in SUPPORTED_RESOURCE_TYPES and resource_type not in normalized:
                normalized.append(resource_type)
        return normalized

    async def _copy_knowledge(
        self,
        spec: Spec,
        card: Card,
        actor_id: str,
    ) -> dict[str, Any]:
        result = await self.db.execute(
            select(SpecKnowledgeBase)
            .where(SpecKnowledgeBase.spec_id == spec.id)
            .order_by(SpecKnowledgeBase.created_at.asc(), SpecKnowledgeBase.title.asc())
        )
        source_items = list(result.scalars().all())
        existing = list(card.knowledge_bases or [])
        existing_sources = {
            str(item.get("source") or "")
            for item in existing
            if isinstance(item, dict)
        }
        existing_ids = {
            str(item.get("id") or "")
            for item in existing
            if isinstance(item, dict)
        }

        copied = 0
        ignored = 0
        copied_ids: list[str] = []
        for kb in source_items:
            source = f"copied_from_spec:{spec.id}:{kb.id}"
            card_kb_id = f"cardkb_{kb.id}"
            if source in existing_sources or card_kb_id in existing_ids:
                ignored += 1
                continue
            existing.append(
                {
                    "id": card_kb_id,
                    "title": kb.title,
                    "description": getattr(kb, "description", None),
                    "content": kb.content,
                    "mime_type": getattr(kb, "mime_type", None) or "text/markdown",
                    "source": source,
                    "author_id": actor_id,
                }
            )
            existing_sources.add(source)
            existing_ids.add(card_kb_id)
            copied_ids.append(card_kb_id)
            copied += 1

        if copied:
            card.knowledge_bases = existing
            flag_modified(card, "knowledge_bases")
            await self.db.flush()

        return {
            "source_count": len(source_items),
            "copied_count": copied,
            "ignored_count": ignored,
            "copied_ids": copied_ids,
            "warnings": [],
        }

    async def _copy_mockups(self, spec: Spec, card: Card) -> dict[str, Any]:
        source_items = [
            item
            for item in list(spec.screen_mockups or [])
            if isinstance(item, dict) and item.get("id")
        ]
        existing = list(card.screen_mockups or [])
        existing_source_ids: set[str] = set()
        for item in existing:
            if not isinstance(item, dict):
                continue
            for key in ("id", "origin_id", "source_mockup_id"):
                value = item.get(key)
                if value:
                    existing_source_ids.add(str(value))

        copied = 0
        ignored = 0
        copied_ids: list[str] = []
        for mockup in source_items:
            source_id = str(mockup.get("id"))
            if source_id in existing_source_ids:
                ignored += 1
                continue
            copied_mockup = copy.deepcopy(mockup)
            existing.append(copied_mockup)
            existing_source_ids.add(source_id)
            copied_ids.append(source_id)
            copied += 1

        if copied:
            card.screen_mockups = existing
            flag_modified(card, "screen_mockups")
            await self.db.flush()

        return {
            "source_count": len(source_items),
            "copied_count": copied,
            "ignored_count": ignored,
            "copied_ids": copied_ids,
            "warnings": [],
        }

    async def _copy_architecture(
        self,
        spec: Spec,
        card: Card,
        actor_id: str,
    ) -> dict[str, Any]:
        repository = ArchitectureDesignRepository(self.db)
        source_designs = await repository.list("spec", spec.id, include_payloads=False)
        existing_designs = await repository.list("card", card.id, include_payloads=False)
        existing_refs = {
            design.source_ref
            for design in existing_designs
            if getattr(design, "source_ref", None)
        }
        copied_count = 0
        ignored_count = 0
        for design in source_designs:
            source_ref = repository.source_ref_for(design)
            if source_ref in existing_refs:
                ignored_count += 1
            else:
                copied_count += 1

        copied_designs = []
        if source_designs:
            copied_designs = await ArchitecturePropagationService(
                self.db,
                repository=repository,
            ).copy_spec_to_card(spec.id, card.id, actor_id)

        return {
            "source_count": len(source_designs),
            "copied_count": copied_count,
            "ignored_count": ignored_count,
            "copied_ids": [design.id for design in copied_designs],
            "warnings": [],
        }

    async def _log_audit(
        self,
        *,
        board_id: str,
        card_id: str,
        actor_id: str,
        trigger: str,
        spec_id: str,
        resource_types: list[str],
        results: dict[str, dict[str, Any]],
    ) -> None:
        self.db.add(
            ActivityLog(
                board_id=board_id,
                card_id=card_id,
                action="spec_resources_auto_propagated",
                actor_type="user",
                actor_id=actor_id,
                actor_name=actor_id[:20],
                details={
                    "trigger": trigger,
                    "spec_id": spec_id,
                    "card_id": card_id,
                    "resource_types": resource_types,
                    "results": results,
                },
            )
        )
        await self.db.flush()
