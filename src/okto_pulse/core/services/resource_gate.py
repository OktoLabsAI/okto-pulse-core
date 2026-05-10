"""Resource Gate domain service.

The gate intentionally persists only explicit N/A marks. Provided resources
remain inferred from the existing Architecture, Mockup and Knowledge artifacts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Literal

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from okto_pulse.core.models.db import (
    ArchitectureDesign,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationKnowledgeBase,
    Refinement,
    RefinementKnowledgeBase,
    ResourceNotApplicable,
    Spec,
    SpecKnowledgeBase,
)

EntityType = Literal["ideation", "refinement", "spec", "card"]
ResourceType = Literal["architecture", "mockup", "knowledge_base"]
ResourceState = Literal["provided", "not_applicable", "missing"]
SourceChannel = Literal["ui", "api", "mcp"]

ENTITY_TYPES: tuple[str, ...] = ("ideation", "refinement", "spec", "card")
RESOURCE_TYPES: tuple[str, ...] = ("architecture", "mockup", "knowledge_base")
SOURCE_CHANNELS: tuple[str, ...] = ("ui", "api", "mcp")


class ResourceGateError(ValueError):
    """Base exception carrying a stable machine-readable error code."""

    def __init__(self, code: str, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


class ResourceGateNotFound(ResourceGateError):
    """Raised when the target entity does not exist on the requested board."""

    def __init__(self, entity_type: str, entity_id: str, board_id: str):
        super().__init__(
            "entity_not_found",
            f"{entity_type} '{entity_id}' was not found on board '{board_id}'.",
            details={"entity_type": entity_type, "entity_id": entity_id, "board_id": board_id},
        )


class ResourceGateJustificationRequired(ResourceGateError):
    """Raised when API/MCP attempts to mark N/A without justification."""

    def __init__(self, source_channel: str):
        super().__init__(
            "justification_required",
            f"Justification is required when source_channel is '{source_channel}'.",
            details={"source_channel": source_channel},
        )


class ResourceGateViolation(ResourceGateError):
    """Raised when a Resource Gate blocks a transition."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(code, message, details=details)


@dataclass(frozen=True)
class _EntityRef:
    entity_type: str
    entity_id: str
    title: str | None
    entity: Any


class ResourceGateService:
    """Resolve Resource Gate state for SDLC entities."""

    warning_message = (
        "WARNING: marking this resource as N/A may lead to partial or incorrect "
        "solutions if the resource is actually needed."
    )

    def __init__(self, db: AsyncSession):
        self.db = db

    @staticmethod
    def is_spec_resource_task_coverage_required(board: Any | None) -> bool:
        """Return the board-level Level 2 gate setting, defaulting to enabled."""
        board_settings = (getattr(board, "settings", None) or {}) if board else {}
        return bool(board_settings.get("require_spec_resource_task_coverage", True))

    async def get_summary(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
    ) -> dict[str, Any]:
        """Return Provided/N/A/Missing summary for the entity."""
        self._validate_entity_type(entity_type)
        root = await self._load_entity_ref(board_id, entity_type, entity_id)
        direct_refs = await self._collect_refs(root)
        inherited_refs: dict[str, list[dict[str, Any]]] = {
            resource_type: [] for resource_type in RESOURCE_TYPES
        }
        for parent in await self._load_parent_refs(board_id, root):
            parent_refs = await self._collect_refs(parent)
            for resource_type, refs in parent_refs.items():
                inherited_refs[resource_type].extend(refs)

        active_marks = await self._load_active_marks(board_id, entity_type, entity_id)
        resources: list[dict[str, Any]] = []
        for resource_type in RESOURCE_TYPES:
            direct = direct_refs[resource_type]
            inherited = inherited_refs[resource_type]
            na_mark = active_marks.get(resource_type)
            state = self._resolve_state(direct, inherited, na_mark)
            resources.append(
                self._serialize_resource_state(
                    resource_type=resource_type,
                    state=state,
                    direct=direct,
                    inherited=inherited,
                    na_mark=na_mark,
                )
            )

        missing_resources = [
            item for item in resources if item["state"] == "missing"
        ]
        return {
            "board_id": board_id,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "resources": resources,
            "blocking": bool(missing_resources),
            "missing_resources": missing_resources,
            "warnings": [],
        }

    async def mark_not_applicable(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
        resource_type: ResourceType | str,
        actor_id: str,
        *,
        justification: str | None = None,
        source_channel: SourceChannel | str = "ui",
    ) -> dict[str, Any]:
        """Persist an explicit N/A mark and return the updated summary."""
        self._validate_resource_type(resource_type)
        self._validate_source_channel(source_channel)
        if source_channel in {"api", "mcp"} and not (justification or "").strip():
            raise ResourceGateJustificationRequired(source_channel)

        await self._load_entity_ref(board_id, entity_type, entity_id)
        await self._deactivate_marks(
            board_id,
            entity_type,
            entity_id,
            resource_type,
            actor_id=actor_id,
            reason="superseded by new N/A mark",
        )
        mark = ResourceNotApplicable(
            board_id=board_id,
            entity_type=str(entity_type),
            entity_id=entity_id,
            resource_type=str(resource_type),
            justification=(justification or None),
            source_channel=str(source_channel),
            created_by=actor_id,
        )
        self.db.add(mark)
        await self.db.flush()

        summary = await self.get_summary(board_id, entity_type, entity_id)
        warning = self.warning_message if source_channel in {"api", "mcp"} else None
        if warning:
            summary["warnings"].append(
                {
                    "code": "resource_not_applicable_risk",
                    "message": warning,
                    "resource_type": resource_type,
                }
            )
        return {"success": True, "mark_id": mark.id, "summary": summary, "warning": warning}

    async def clear_not_applicable(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
        resource_type: ResourceType | str,
        actor_id: str,
        *,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """Deactivate the active N/A mark for an entity/resource type."""
        self._validate_resource_type(resource_type)
        await self._load_entity_ref(board_id, entity_type, entity_id)
        affected = await self._deactivate_marks(
            board_id,
            entity_type,
            entity_id,
            resource_type,
            actor_id=actor_id,
            reason=reason or "cleared",
        )
        summary = await self.get_summary(board_id, entity_type, entity_id)
        return {"success": True, "cleared": affected, "summary": summary}

    async def validate_entity_completion(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
    ) -> dict[str, Any]:
        """Return whether Level 1 completion is allowed."""
        summary = await self.get_summary(board_id, entity_type, entity_id)
        blocking_resources = [
            resource
            for resource in summary["resources"]
            if resource["state"] == "missing"
        ]
        return {
            "allowed": not blocking_resources,
            "blocking_resources": blocking_resources,
            "summary": summary,
        }

    async def validate_or_raise_entity_completion(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
        *,
        phase: str = "completion",
    ) -> dict[str, Any]:
        """Validate Level 1 completion and raise a structured violation on failure."""
        result = await self.validate_entity_completion(board_id, entity_type, entity_id)
        if result["allowed"]:
            return result

        labels = ", ".join(
            self._resource_label(item["resource_type"])
            for item in result["blocking_resources"]
        )
        raise ResourceGateViolation(
            "resource_gate_missing_resources",
            (
                f"Cannot complete {entity_type} '{entity_id}': "
                f"missing mandatory resource(s): {labels}. "
                "Attach the resource(s) or mark each one as N/A before completing."
            ),
            details={
                "board_id": board_id,
                "entity_type": str(entity_type),
                "entity_id": entity_id,
                "phase": phase,
                "blocking_resources": result["blocking_resources"],
                "summary": result["summary"],
            },
        )

    async def validate_spec_resource_task_coverage(
        self,
        board_id: str,
        spec_id: str,
        *,
        enabled: bool = True,
    ) -> dict[str, Any]:
        """Validate that every effective spec resource is covered by a task.

        Level 2 only applies to resources actually provided to the spec, either
        directly or inherited. N/A and Missing states remain Level 1 signals and
        are not represented as task coverage obligations.
        """
        summary = await self.get_summary(board_id, "spec", spec_id)
        provided_refs: list[dict[str, Any]] = []
        for resource in summary["resources"]:
            if resource["state"] != "provided":
                continue
            for ref in self._coverage_obligation_refs(resource):
                provided_refs.append(
                    {
                        **ref,
                        "resource_type": resource["resource_type"],
                    }
                )

        if not enabled or not provided_refs:
            return {
                "allowed": True,
                "enabled": enabled,
                "board_id": board_id,
                "spec_id": spec_id,
                "required_resources": provided_refs,
                "uncovered_resources": [],
                "summary": summary,
            }

        task_cards = await self._load_spec_task_cards(spec_id)
        coverage_ids = await self._collect_task_resource_id_coverage(task_cards)
        uncovered: list[dict[str, Any]] = []

        for ref in provided_refs:
            resource_type = ref["resource_type"]
            identities = self._resource_identity_values(ref)
            eligible_ids = coverage_ids[resource_type]["eligible"]
            cancelled_ids = coverage_ids[resource_type]["cancelled"]
            if identities and identities.intersection(eligible_ids):
                continue

            if identities and identities.intersection(cancelled_ids):
                reason = "covered_only_by_cancelled_task"
                remediation = (
                    "Attach or copy this resource to at least one non-cancelled task. "
                    "Cancelled tasks do not count as coverage."
                )
            else:
                reason = "uncovered"
                remediation = (
                    "Attach or copy this resource directly to at least one non-cancelled task."
                )
            uncovered.append(
                {
                    "resource_type": resource_type,
                    "resource_id": ref.get("id"),
                    "resource_title": ref.get("title"),
                    "source_entity_type": ref.get("source_entity_type"),
                    "source_entity_id": ref.get("source_entity_id"),
                    "source_entity_title": ref.get("source_entity_title"),
                    "reason": reason,
                    "remediation": remediation,
                }
            )

        return {
            "allowed": not uncovered,
            "enabled": enabled,
            "board_id": board_id,
            "spec_id": spec_id,
            "required_resources": provided_refs,
            "uncovered_resources": uncovered,
            "summary": summary,
        }

    async def validate_or_raise_spec_resource_task_coverage(
        self,
        board_id: str,
        spec_id: str,
        *,
        phase: str,
        enabled: bool = True,
    ) -> dict[str, Any]:
        """Validate Level 2 coverage and raise a structured violation on failure."""
        result = await self.validate_spec_resource_task_coverage(
            board_id,
            spec_id,
            enabled=enabled,
        )
        if result["allowed"]:
            return result

        label_items = []
        for item in result["uncovered_resources"][:5]:
            label = self._resource_label(item["resource_type"])
            if item.get("resource_title"):
                label = f"{label} ({item['resource_title']})"
            label_items.append(label)
        labels = ", ".join(label_items)
        extra = (
            f" and {len(result['uncovered_resources']) - 5} more"
            if len(result["uncovered_resources"]) > 5
            else ""
        )
        raise ResourceGateViolation(
            "resource_gate_spec_task_coverage",
            (
                "Cannot advance spec: mandatory spec resource(s) are not covered "
                f"by non-cancelled task cards: {labels}{extra}. "
                "Copy or attach every effective spec Architecture, Mockup and "
                "Knowledge Base resource to at least one task, or disable the "
                "board setting 'require_spec_resource_task_coverage'."
            ),
            details={
                "board_id": board_id,
                "spec_id": spec_id,
                "phase": phase,
                "uncovered_resources": result["uncovered_resources"],
                "required_resources": result["required_resources"],
                "summary": result["summary"],
            },
        )

    async def _load_active_marks(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
    ) -> dict[str, ResourceNotApplicable]:
        result = await self.db.execute(
            select(ResourceNotApplicable)
            .where(
                ResourceNotApplicable.board_id == board_id,
                ResourceNotApplicable.entity_type == entity_type,
                ResourceNotApplicable.entity_id == entity_id,
                ResourceNotApplicable.active.is_(True),
            )
            .order_by(ResourceNotApplicable.created_at.desc())
        )
        marks: dict[str, ResourceNotApplicable] = {}
        for mark in result.scalars().all():
            marks.setdefault(mark.resource_type, mark)
        return marks

    async def _deactivate_marks(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
        resource_type: str,
        *,
        actor_id: str,
        reason: str,
    ) -> int:
        result = await self.db.execute(
            update(ResourceNotApplicable)
            .where(
                ResourceNotApplicable.board_id == board_id,
                ResourceNotApplicable.entity_type == entity_type,
                ResourceNotApplicable.entity_id == entity_id,
                ResourceNotApplicable.resource_type == resource_type,
                ResourceNotApplicable.active.is_(True),
            )
            .values(
                active=False,
                cleared_by=actor_id,
                cleared_at=datetime.now(timezone.utc),
                clear_reason=reason,
            )
        )
        return int(result.rowcount or 0)

    async def _load_entity_ref(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
    ) -> _EntityRef:
        self._validate_entity_type(entity_type)
        model, options = self._model_options(entity_type)
        result = await self.db.execute(
            select(model)
            .options(*options)
            .where(model.id == entity_id, model.board_id == board_id)
        )
        entity = result.scalar_one_or_none()
        if entity is None:
            raise ResourceGateNotFound(entity_type, entity_id, board_id)
        return _EntityRef(
            entity_type=entity_type,
            entity_id=entity_id,
            title=getattr(entity, "title", None),
            entity=entity,
        )

    async def _load_parent_refs(self, board_id: str, root: _EntityRef) -> list[_EntityRef]:
        entity = root.entity
        parents: list[_EntityRef] = []
        seen: set[tuple[str, str]] = set()

        async def add_parent(entity_type: str, entity_id: str | None) -> None:
            if not entity_id or (entity_type, entity_id) in seen:
                return
            ref = await self._load_entity_ref(board_id, entity_type, entity_id)
            seen.add((entity_type, entity_id))
            parents.append(ref)

        if root.entity_type == "refinement":
            await add_parent("ideation", getattr(entity, "ideation_id", None))
        elif root.entity_type == "spec":
            await add_parent("refinement", getattr(entity, "refinement_id", None))
            await add_parent("ideation", getattr(entity, "ideation_id", None))
            if getattr(entity, "refinement_id", None):
                refinement = parents[0].entity if parents and parents[0].entity_type == "refinement" else None
                await add_parent("ideation", getattr(refinement, "ideation_id", None))
        elif root.entity_type == "card":
            await add_parent("spec", getattr(entity, "spec_id", None))
            spec_ref = next((p for p in parents if p.entity_type == "spec"), None)
            spec = spec_ref.entity if spec_ref else None
            await add_parent("refinement", getattr(spec, "refinement_id", None))
            await add_parent("ideation", getattr(spec, "ideation_id", None))
            refinement_ref = next((p for p in parents if p.entity_type == "refinement"), None)
            refinement = refinement_ref.entity if refinement_ref else None
            await add_parent("ideation", getattr(refinement, "ideation_id", None))

        return parents

    async def _collect_refs(self, ref: _EntityRef) -> dict[str, list[dict[str, Any]]]:
        return {
            "architecture": await self._architecture_refs(ref),
            "mockup": self._mockup_refs(ref),
            "knowledge_base": await self._knowledge_refs(ref),
        }

    async def _load_spec_task_cards(self, spec_id: str) -> list[Card]:
        result = await self.db.execute(
            select(Card).where(
                Card.spec_id == spec_id,
                Card.card_type == CardType.NORMAL,
                Card.archived.is_(False),
            )
        )
        return list(result.scalars().all())

    async def _collect_task_resource_id_coverage(
        self,
        cards: list[Card],
    ) -> dict[str, dict[str, set[str]]]:
        coverage: dict[str, dict[str, set[str]]] = {
            resource_type: {"eligible": set(), "cancelled": set()}
            for resource_type in RESOURCE_TYPES
        }
        if not cards:
            return coverage

        cards_by_id = {card.id: card for card in cards}
        card_ids = list(cards_by_id)

        for card in cards:
            bucket = "cancelled" if card.status == CardStatus.CANCELLED else "eligible"
            for item in (card.screen_mockups or []):
                coverage["mockup"][bucket].update(self._resource_identity_values(item))
            for item in (card.knowledge_bases or []):
                coverage["knowledge_base"][bucket].update(self._resource_identity_values(item))

        result = await self.db.execute(
            select(
                ArchitectureDesign.card_id.label("card_id"),
                ArchitectureDesign.id.label("id"),
                ArchitectureDesign.source_design_id.label("source_design_id"),
                ArchitectureDesign.source_ref.label("source_ref"),
            ).where(ArchitectureDesign.card_id.in_(card_ids))
        )
        for row in result.mappings().all():
            card_id = row.get("card_id")
            if not card_id or card_id not in cards_by_id:
                continue
            bucket = (
                "cancelled"
                if cards_by_id[card_id].status == CardStatus.CANCELLED
                else "eligible"
            )
            coverage["architecture"][bucket].update(
                self._resource_identity_values(dict(row))
            )

        return coverage

    async def _architecture_refs(self, ref: _EntityRef) -> list[dict[str, Any]]:
        result = await self.db.execute(
            select(ArchitectureDesign.id, ArchitectureDesign.title)
            .where(
                ArchitectureDesign.board_id == getattr(ref.entity, "board_id"),
                ArchitectureDesign.parent_type == ref.entity_type,
                self._architecture_parent_column(ref.entity_type) == ref.entity_id,
            )
            .order_by(ArchitectureDesign.created_at.asc())
        )
        return [
            self._artifact_ref(ref, artifact_id=row[0], title=row[1])
            for row in result.all()
        ]

    def _mockup_refs(self, ref: _EntityRef) -> list[dict[str, Any]]:
        mockups = getattr(ref.entity, "screen_mockups", None) or []
        return [
            self._artifact_ref(
                ref,
                artifact_id=(item.get("id") if isinstance(item, dict) else None),
                title=(
                    item.get("title") or item.get("name")
                    if isinstance(item, dict)
                    else None
                ),
            )
            for item in mockups
        ]

    async def _knowledge_refs(self, ref: _EntityRef) -> list[dict[str, Any]]:
        entity = ref.entity
        if ref.entity_type == "card":
            return [
                self._artifact_ref(
                    ref,
                    artifact_id=item.get("id") if isinstance(item, dict) else None,
                    title=item.get("title") if isinstance(item, dict) else None,
                )
                for item in (getattr(entity, "knowledge_bases", None) or [])
            ]

        kb_model, fk_column = {
            "ideation": (IdeationKnowledgeBase, IdeationKnowledgeBase.ideation_id),
            "refinement": (RefinementKnowledgeBase, RefinementKnowledgeBase.refinement_id),
            "spec": (SpecKnowledgeBase, SpecKnowledgeBase.spec_id),
        }[ref.entity_type]
        result = await self.db.execute(
            select(kb_model.id, kb_model.title)
            .where(fk_column == ref.entity_id)
            .order_by(kb_model.created_at.asc())
        )
        return [
            self._artifact_ref(ref, artifact_id=row[0], title=row[1])
            for row in result.all()
        ]

    @staticmethod
    def _artifact_ref(
        ref: _EntityRef,
        *,
        artifact_id: str | None,
        title: str | None,
    ) -> dict[str, Any]:
        return {
            "id": artifact_id,
            "title": title,
            "source_entity_type": ref.entity_type,
            "source_entity_id": ref.entity_id,
            "source_entity_title": ref.title,
        }

    @staticmethod
    def _coverage_obligation_refs(resource: dict[str, Any]) -> list[dict[str, Any]]:
        """Return the resource refs that must be permeated to task cards.

        Direct spec resources are snapshots of the formalized parent context. When
        they exist, requiring the parent inherited copies as separate task
        obligations double-counts the same artifact and makes the gate impossible
        to satisfy with the public copy tools.
        """
        direct = list(resource.get("direct_refs") or [])
        if direct:
            return direct
        return list(resource.get("inherited_refs") or [])

    @staticmethod
    def _resource_identity_values(item: Any) -> set[str]:
        values: set[str] = set()
        if isinstance(item, dict):
            for key in (
                "id",
                "origin_id",
                "source_id",
                "source_mockup_id",
                "source_kb_id",
                "source_design_id",
            ):
                value = item.get(key)
                if value:
                    text = str(value)
                    values.add(text)
                    if text.startswith("cardkb_") and len(text) > len("cardkb_"):
                        values.add(text[len("cardkb_") :])
            values.update(ResourceGateService._source_ref_values(item.get("source_ref")))
            values.update(ResourceGateService._source_ref_values(item.get("origin_ref")))
            values.update(ResourceGateService._source_ref_values(item.get("source")))
        elif item:
            values.add(str(item))
        return values

    @staticmethod
    def _source_ref_values(source_ref: Any) -> set[str]:
        if not source_ref:
            return set()
        text = str(source_ref)
        values = {text}
        for sep in (":", "/", "\\"):
            if sep in text:
                values.add(text.rsplit(sep, 1)[-1])
        return values

    @staticmethod
    def _resolve_state(
        direct: Iterable[dict[str, Any]],
        inherited: Iterable[dict[str, Any]],
        na_mark: ResourceNotApplicable | None,
    ) -> ResourceState:
        if list(direct) or list(inherited):
            return "provided"
        if na_mark is not None:
            return "not_applicable"
        return "missing"

    def _serialize_resource_state(
        self,
        *,
        resource_type: str,
        state: ResourceState,
        direct: list[dict[str, Any]],
        inherited: list[dict[str, Any]],
        na_mark: ResourceNotApplicable | None,
    ) -> dict[str, Any]:
        return {
            "resource_type": resource_type,
            "state": state,
            "direct_count": len(direct),
            "inherited_count": len(inherited),
            "total_count": len(direct) + len(inherited),
            "direct_refs": direct,
            "inherited_refs": inherited,
            "na_mark": self._serialize_na_mark(na_mark, effective=state == "not_applicable"),
            "blocking": state == "missing",
            "reason": None if state != "missing" else "missing",
            "remediation": self._remediation(resource_type, state),
        }

    @staticmethod
    def _serialize_na_mark(
        mark: ResourceNotApplicable | None,
        *,
        effective: bool,
    ) -> dict[str, Any] | None:
        if mark is None:
            return None
        return {
            "id": mark.id,
            "active": bool(mark.active),
            "effective": effective,
            "justification": mark.justification,
            "source_channel": mark.source_channel,
            "created_by": mark.created_by,
            "created_at": mark.created_at.isoformat() if mark.created_at else None,
        }

    @staticmethod
    def _remediation(resource_type: str, state: ResourceState) -> str | None:
        if state != "missing":
            return None
        labels = {
            "architecture": "Architecture",
            "mockup": "Mockup",
            "knowledge_base": "Knowledge Base",
        }
        return f"Attach a {labels[resource_type]} resource or mark it as N/A."

    @staticmethod
    def _resource_label(resource_type: str) -> str:
        return {
            "architecture": "Architecture",
            "mockup": "Mockup",
            "knowledge_base": "Knowledge Base",
        }.get(resource_type, resource_type)

    @staticmethod
    def _model_options(entity_type: str) -> tuple[type[Any], list[Any]]:
        if entity_type == "ideation":
            return Ideation, [selectinload(Ideation.architecture_designs)]
        if entity_type == "refinement":
            return Refinement, [selectinload(Refinement.architecture_designs)]
        if entity_type == "spec":
            return Spec, [selectinload(Spec.architecture_designs)]
        if entity_type == "card":
            return Card, [selectinload(Card.architecture_designs)]
        raise AssertionError(entity_type)

    @staticmethod
    def _architecture_parent_column(entity_type: str):
        return {
            "ideation": ArchitectureDesign.ideation_id,
            "refinement": ArchitectureDesign.refinement_id,
            "spec": ArchitectureDesign.spec_id,
            "card": ArchitectureDesign.card_id,
        }[entity_type]

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        if entity_type not in ENTITY_TYPES:
            raise ResourceGateError(
                "invalid_entity_type",
                f"Invalid entity_type '{entity_type}'. Expected one of: {', '.join(ENTITY_TYPES)}.",
            )

    @staticmethod
    def _validate_resource_type(resource_type: str) -> None:
        if resource_type not in RESOURCE_TYPES:
            raise ResourceGateError(
                "invalid_resource_type",
                f"Invalid resource_type '{resource_type}'. Expected one of: {', '.join(RESOURCE_TYPES)}.",
            )

    @staticmethod
    def _validate_source_channel(source_channel: str) -> None:
        if source_channel not in SOURCE_CHANNELS:
            raise ResourceGateError(
                "invalid_source_channel",
                f"Invalid source_channel '{source_channel}'. Expected one of: {', '.join(SOURCE_CHANNELS)}.",
            )
