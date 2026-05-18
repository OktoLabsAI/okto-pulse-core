"""Spec resource auto-propagation into linked cards."""

from __future__ import annotations

import copy
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.models.db import ActivityLog, Board, Card, Spec, SpecKnowledgeBase
from okto_pulse.core.models.schemas import ArchitectureDesignUpdate
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
        removed_kb_ids: set[str] | None = None,
        removed_architecture_design_ids: set[str] | None = None,
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
                if removed_kb_ids:
                    results[resource_type] = await self._remove_knowledge(spec, card, removed_kb_ids)
                else:
                    results[resource_type] = await self._copy_knowledge(spec, card, actor_id)
            elif resource_type == "mockup":
                results[resource_type] = await self._copy_mockups(spec, card)
            elif resource_type == "architecture":
                if removed_architecture_design_ids:
                    results[resource_type] = await self._remove_architecture(
                        spec, card, removed_architecture_design_ids
                    )
                else:
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

    async def propagate_for_spec(
        self,
        *,
        board_id: str,
        spec_id: str,
        actor_id: str,
        trigger: str,
        removed_kb_ids: set[str] | None = None,
        removed_architecture_design_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        """Copy selected Spec resources to every linked non-archived card."""
        board = await self.db.get(Board, board_id)
        if not board:
            return {"enabled": False, "reason": "board_not_found", "cards": []}

        resource_types = self._resolve_resource_types(board)
        if not resource_types:
            return {"enabled": False, "reason": "disabled", "cards": []}

        spec = await self.db.get(Spec, spec_id)
        if not spec or spec.board_id != board_id:
            return {"enabled": True, "reason": "spec_not_found", "cards": []}

        result = await self.db.execute(
            select(Card)
            .where(
                Card.board_id == board_id,
                Card.spec_id == spec_id,
                Card.archived.is_(False),
            )
            .order_by(Card.created_at.asc(), Card.title.asc())
        )
        cards = list(result.scalars().all())
        card_results = []
        for card in cards:
            card_results.append(
                await self.propagate_for_card(
                    board_id=board_id,
                    spec_id=spec_id,
                    card_id=card.id,
                    actor_id=actor_id,
                    trigger=trigger,
                    removed_kb_ids=removed_kb_ids,
                    removed_architecture_design_ids=removed_architecture_design_ids,
                )
            )

        return {
            "enabled": True,
            "trigger": trigger,
            "board_id": board_id,
            "spec_id": spec_id,
            "resource_types": resource_types,
            "cards": card_results,
        }

    async def propagate_for_board(
        self,
        *,
        board_id: str,
        actor_id: str,
        trigger: str,
    ) -> dict[str, Any]:
        """Backfill selected Spec resources for all specs in a board."""
        board = await self.db.get(Board, board_id)
        if not board:
            return {"enabled": False, "reason": "board_not_found", "specs": []}

        resource_types = self._resolve_resource_types(board)
        if not resource_types:
            return {"enabled": False, "reason": "disabled", "specs": []}

        result = await self.db.execute(
            select(Spec.id)
            .where(Spec.board_id == board_id, Spec.archived.is_(False))
            .order_by(Spec.created_at.asc(), Spec.title.asc())
        )
        spec_ids = [row[0] for row in result.all()]
        spec_results = []
        for spec_id in spec_ids:
            spec_results.append(
                await self.propagate_for_spec(
                    board_id=board_id,
                    spec_id=spec_id,
                    actor_id=actor_id,
                    trigger=trigger,
                )
            )

        return {
            "enabled": True,
            "trigger": trigger,
            "board_id": board_id,
            "resource_types": resource_types,
            "specs": spec_results,
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

        def _kb_index_by_source() -> dict[str, int]:
            return {
                str(item.get("source") or ""): idx
                for idx, item in enumerate(existing)
                if isinstance(item, dict)
            }

        def _kb_index_by_id() -> dict[str, int]:
            return {
                str(item.get("id") or ""): idx
                for idx, item in enumerate(existing)
                if isinstance(item, dict)
            }

        source_index = _kb_index_by_source()
        id_index = _kb_index_by_id()

        copied = 0
        ignored = 0
        copied_ids: list[str] = []
        mutated = False
        for kb in source_items:
            source = f"copied_from_spec:{spec.id}:{kb.id}"
            card_kb_id = f"cardkb_{kb.id}"
            target_idx = source_index.get(source)
            if target_idx is None:
                target_idx = id_index.get(card_kb_id)
            new_payload = {
                "id": card_kb_id,
                "title": kb.title,
                "description": getattr(kb, "description", None),
                "content": kb.content,
                "mime_type": getattr(kb, "mime_type", None) or "text/markdown",
                "source": source,
                "author_id": actor_id,
            }
            if target_idx is not None:
                current = existing[target_idx]
                preserved_author = (
                    current.get("author_id") if isinstance(current, dict) else None
                ) or actor_id
                refreshed = {**new_payload, "author_id": preserved_author}
                if isinstance(current, dict) and all(
                    current.get(key) == refreshed.get(key)
                    for key in ("title", "description", "content", "mime_type", "source")
                ):
                    ignored += 1
                    continue
                existing[target_idx] = refreshed
                source_index[source] = target_idx
                id_index[card_kb_id] = target_idx
                copied_ids.append(card_kb_id)
                copied += 1
                mutated = True
                continue
            existing.append(new_payload)
            new_idx = len(existing) - 1
            source_index[source] = new_idx
            id_index[card_kb_id] = new_idx
            copied_ids.append(card_kb_id)
            copied += 1
            mutated = True

        if mutated:
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

    async def _remove_knowledge(
        self,
        spec: Spec,
        card: Card,
        kb_ids: set[str],
    ) -> dict[str, Any]:
        existing = list(card.knowledge_bases or [])
        sources_to_remove = {f"copied_from_spec:{spec.id}:{kb_id}" for kb_id in kb_ids}
        ids_to_remove = {f"cardkb_{kb_id}" for kb_id in kb_ids}
        removed_ids: list[str] = []
        kept: list[Any] = []
        for item in existing:
            if isinstance(item, dict):
                source = str(item.get("source") or "")
                item_id = str(item.get("id") or "")
                if source in sources_to_remove or item_id in ids_to_remove:
                    removed_ids.append(item_id or source)
                    continue
            kept.append(item)
        if removed_ids:
            card.knowledge_bases = kept
            flag_modified(card, "knowledge_bases")
            await self.db.flush()
        return {
            "source_count": len(kb_ids),
            "copied_count": 0,
            "ignored_count": 0,
            "removed_count": len(removed_ids),
            "removed_ids": removed_ids,
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
        existing_by_ref = {
            getattr(design, "source_ref", None): design
            for design in existing_designs
            if getattr(design, "source_ref", None)
        }
        copied_count = 0
        ignored_count = 0
        copied_ids: list[str] = []
        refreshed = False
        for design in source_designs:
            source_ref = repository.source_ref_for(design)
            target = existing_by_ref.get(source_ref)
            if target is None:
                copied_count += 1
                refreshed = True
                continue
            target_version = getattr(target, "source_version", None)
            spec_version = getattr(design, "version", None)
            if target_version == spec_version:
                ignored_count += 1
                continue
            patch = ArchitectureDesignUpdate(
                title=design.title,
                global_description=design.global_description,
                entities=design.entities,
                interfaces=design.interfaces,
                diagrams=design.diagrams or [],
                source_ref=source_ref,
                source_version=spec_version,
                source_design_id=design.id,
                change_summary=f"Refresh from spec design {design.id} v{spec_version}",
            )
            await repository.update(target.id, patch, actor_id)
            copied_count += 1
            copied_ids.append(target.id)
            refreshed = True

        if refreshed:
            propagation_service = ArchitecturePropagationService(
                self.db,
                repository=repository,
            )
            new_designs = await propagation_service.copy_spec_to_card(spec.id, card.id, actor_id)
            copied_ids.extend(design.id for design in new_designs if design.id not in copied_ids)

        return {
            "source_count": len(source_designs),
            "copied_count": copied_count,
            "ignored_count": ignored_count,
            "copied_ids": copied_ids,
            "warnings": [],
        }

    async def _remove_architecture(
        self,
        spec: Spec,
        card: Card,
        spec_design_ids: set[str],
    ) -> dict[str, Any]:
        repository = ArchitectureDesignRepository(self.db)
        existing_designs = await repository.list("card", card.id, include_payloads=False)
        refs_to_remove = {f"architecture_design:{design_id}" for design_id in spec_design_ids}
        removed_ids: list[str] = []
        for design in existing_designs:
            source_ref = getattr(design, "source_ref", None)
            if source_ref in refs_to_remove:
                await repository.delete(design.id, None)
                removed_ids.append(design.id)
        return {
            "source_count": len(spec_design_ids),
            "copied_count": 0,
            "ignored_count": 0,
            "removed_count": len(removed_ids),
            "removed_ids": removed_ids,
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
