"""Architecture Design domain services.

This module owns the storage boundary for first-class Architecture Designs.
Small structured metadata stays in the architecture envelope, while diagram
payloads go through ``ArchitectureDiagramStore`` so a future SaaS edition can
swap database storage for blob storage without changing REST/MCP contracts.
"""

from __future__ import annotations

import copy
import hashlib
import json
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified, set_committed_value

from okto_pulse.core.models.db import (
    ArchitectureDesign,
    ArchitectureDesignVersion,
    ArchitectureDiagramPayload,
    Card,
    Ideation,
    Refinement,
    Spec,
)
from okto_pulse.core.models.schemas import (
    ArchitectureDesignCreate,
    ArchitectureDesignResponse,
    ArchitectureDesignSummary,
    ArchitectureDesignUpdate,
    ArchitectureDiffResponse,
)

PARENT_MODELS = {
    "ideation": (Ideation, "ideation_id"),
    "refinement": (Refinement, "refinement_id"),
    "spec": (Spec, "spec_id"),
    "card": (Card, "card_id"),
}

SEMANTIC_PATCH_FIELDS = {
    "title",
    "global_description",
    "entities",
    "interfaces",
    "diagrams",
}


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _hash_payload(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _payload_size(value: Any) -> int:
    return len(_stable_json(value).encode("utf-8"))


def _new_scoped_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _dump_model_or_dict(value: Any, *, exclude_unset: bool = False) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_unset=exclude_unset)
    return dict(value or {})


class ArchitectureDiagramAdapter:
    """Base adapter contract for diagram formats."""

    format: str = "raw"

    def validate(self, payload: Any) -> None:
        if payload is None:
            raise ValueError("diagram payload is required")

    def normalize(self, payload: Any) -> Any:
        self.validate(payload)
        return copy.deepcopy(payload)

    def dump(self, payload: Any) -> str:
        return payload if isinstance(payload, str) else _stable_json(payload)

    def render(self, payload: Any, mode: str = "raw") -> str:
        return self.dump(payload)

    def import_excalidraw(self, payload: Any) -> Any:
        return self.normalize(payload)

    def summarize_semantics(self, payload: Any) -> dict[str, Any]:
        return {
            "content_hash": _hash_payload(payload),
            "semantic_hash": _hash_payload(payload),
            "semantic_change": True,
        }


class ExcalidrawArchitectureDiagramAdapter(ArchitectureDiagramAdapter):
    """Adapter for Excalidraw JSON scene payloads."""

    format = "excalidraw_json"

    def validate(self, payload: Any) -> None:
        super().validate(payload)
        if not isinstance(payload, dict):
            raise ValueError("Excalidraw payload must be a JSON object")
        elements = payload.get("elements")
        if not isinstance(elements, list):
            raise ValueError("Excalidraw payload must contain an elements array")

    def summarize_semantics(self, payload: Any) -> dict[str, Any]:
        self.validate(payload)
        elements = payload.get("elements") or []
        semantic_elements = [
            {
                "id": item.get("id"),
                "type": item.get("type"),
                "text": item.get("text"),
                "link": item.get("link"),
                "boundElements": item.get("boundElements"),
                "customData": item.get("customData"),
            }
            for item in elements
            if isinstance(item, dict)
        ]
        semantic = {
            "elements": semantic_elements,
            "files": sorted((payload.get("files") or {}).keys()),
        }
        return {
            "content_hash": _hash_payload(payload),
            "semantic_hash": _hash_payload(semantic),
            "semantic_change": True,
        }


class RawArchitectureDiagramAdapter(ArchitectureDiagramAdapter):
    """Fallback adapter for raw/text-like diagram payloads."""

    format = "raw"

    def validate(self, payload: Any) -> None:
        super().validate(payload)
        if not isinstance(payload, (str, dict, list)):
            raise ValueError("raw diagram payload must be string, object, or array")


class ArchitectureDiagramAdapterRegistry:
    """Registry that resolves diagram adapters by format."""

    def __init__(self) -> None:
        raw = RawArchitectureDiagramAdapter()
        self._adapters: dict[str, ArchitectureDiagramAdapter] = {
            raw.format: raw,
            "mermaid": raw,
            "svg": raw,
            "plantuml": raw,
            "c4": raw,
            ExcalidrawArchitectureDiagramAdapter.format: ExcalidrawArchitectureDiagramAdapter(),
        }

    def register(self, adapter: ArchitectureDiagramAdapter) -> None:
        self._adapters[adapter.format] = adapter

    def get(self, format_name: str | None) -> ArchitectureDiagramAdapter:
        if not format_name:
            return self._adapters["raw"]
        try:
            return self._adapters[format_name]
        except KeyError as exc:
            raise ValueError(f"unsupported architecture diagram format: {format_name}") from exc


class ArchitectureDiagramStore:
    """Database-backed diagram payload store.

    The public contract intentionally uses opaque payload refs instead of table
    details. A blob-storage adapter can later keep the same methods.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def save_payload(
        self,
        board_id: str,
        design_id: str,
        diagram_id: str,
        format: str,
        payload: Any,
    ) -> ArchitectureDiagramPayload:
        result = await self.db.execute(
            select(ArchitectureDiagramPayload).where(
                ArchitectureDiagramPayload.design_id == design_id,
                ArchitectureDiagramPayload.diagram_id == diagram_id,
            )
        )
        row = result.scalar_one_or_none()
        content_hash = _hash_payload(payload)
        size_bytes = _payload_size(payload)
        payload_json = None if isinstance(payload, str) else payload
        payload_text = payload if isinstance(payload, str) else None
        if row is None:
            row = ArchitectureDiagramPayload(
                board_id=board_id,
                design_id=design_id,
                diagram_id=diagram_id,
                storage_backend="database",
                storage_key=f"architecture_diagram_payloads/{design_id}/{diagram_id}",
                format=format,
                adapter_payload_json=payload_json,
                payload_text=payload_text,
                content_hash=content_hash,
                size_bytes=size_bytes,
            )
            self.db.add(row)
        else:
            row.format = format
            row.adapter_payload_json = payload_json
            row.payload_text = payload_text
            row.content_hash = content_hash
            row.size_bytes = size_bytes
        await self.db.flush()
        return row

    async def load_payload(self, ref: str) -> Any:
        row = await self._get_payload_row(ref)
        if row is None:
            raise KeyError(f"diagram payload not found: {ref}")
        return row.payload_text if row.payload_text is not None else row.adapter_payload_json

    async def delete_payload(self, ref: str) -> None:
        row = await self._get_payload_row(ref)
        if row is not None:
            await self.db.delete(row)
            await self.db.flush()

    async def copy_payload(self, source_ref: str, target_design_id: str, target_diagram_id: str) -> ArchitectureDiagramPayload:
        row = await self._get_payload_row(source_ref)
        if row is None:
            raise KeyError(f"diagram payload not found: {source_ref}")
        payload = row.payload_text if row.payload_text is not None else row.adapter_payload_json
        return await self.save_payload(
            board_id=row.board_id,
            design_id=target_design_id,
            diagram_id=target_diagram_id,
            format=row.format,
            payload=copy.deepcopy(payload),
        )

    async def exists(self, ref: str) -> bool:
        return await self._get_payload_row(ref) is not None

    async def stat(self, ref: str) -> dict[str, Any]:
        row = await self._get_payload_row(ref)
        if row is None:
            raise KeyError(f"diagram payload not found: {ref}")
        return {
            "id": row.id,
            "storage_backend": row.storage_backend,
            "storage_key": row.storage_key,
            "format": row.format,
            "content_hash": row.content_hash,
            "size_bytes": row.size_bytes,
        }

    async def _get_payload_row(self, ref: str) -> ArchitectureDiagramPayload | None:
        result = await self.db.execute(
            select(ArchitectureDiagramPayload).where(
                (ArchitectureDiagramPayload.id == ref) | (ArchitectureDiagramPayload.storage_key == ref)
            )
        )
        return result.scalar_one_or_none()


class ArchitectureDesignRepository:
    """Repository for Architecture Design envelopes and versions."""

    def __init__(
        self,
        db: AsyncSession,
        *,
        diagram_store: ArchitectureDiagramStore | None = None,
        adapter_registry: ArchitectureDiagramAdapterRegistry | None = None,
    ):
        self.db = db
        self.diagram_store = diagram_store or ArchitectureDiagramStore(db)
        self.adapter_registry = adapter_registry or ArchitectureDiagramAdapterRegistry()

    async def list(self, parent_type: str, parent_id: str, include_payloads: bool = False) -> list[ArchitectureDesign]:
        _, parent_field = self._parent_config(parent_type)
        result = await self.db.execute(
            select(ArchitectureDesign)
            .where(ArchitectureDesign.parent_type == parent_type, getattr(ArchitectureDesign, parent_field) == parent_id)
            .order_by(ArchitectureDesign.updated_at.desc(), ArchitectureDesign.title.asc())
        )
        designs = list(result.scalars().all())
        if include_payloads:
            for design in designs:
                await self._attach_payloads(design)
        return designs

    async def get(self, design_id: str, include_payloads: bool = False) -> ArchitectureDesign | None:
        design = await self.db.get(ArchitectureDesign, design_id)
        if design is not None and include_payloads:
            await self._attach_payloads(design)
        return design

    async def create(self, parent_type: str, parent_id: str, data: ArchitectureDesignCreate | dict[str, Any], actor_id: str) -> ArchitectureDesign:
        parent_model, parent_field = self._parent_config(parent_type)
        parent = await self.db.get(parent_model, parent_id)
        if parent is None:
            raise ValueError(f"{parent_type} parent not found: {parent_id}")
        payload = _dump_model_or_dict(data)
        board_id = getattr(parent, "board_id")
        design = ArchitectureDesign(
            board_id=board_id,
            parent_type=parent_type,
            **{parent_field: parent_id},
            title=payload["title"],
            global_description=payload["global_description"],
            entities=payload.get("entities") or [],
            interfaces=payload.get("interfaces") or [],
            diagrams=[],
            source_ref=payload.get("source_ref"),
            source_version=payload.get("source_version"),
            source_design_id=payload.get("source_design_id"),
            stale=False,
            breaking_change_flag=False,
            requires_arch_review=False,
            created_by=actor_id,
        )
        self.db.add(design)
        await self.db.flush()
        design.diagrams = await self._normalize_diagrams(board_id, design.id, payload.get("diagrams") or [])
        flag_modified(design, "diagrams")
        await self.snapshot(design.id, actor_id, "Initial architecture design")
        await self._publish_parent_semantic_change(design, actor_id)
        await self.db.flush()
        await self.db.refresh(design)
        return design

    async def update(self, design_id: str, patch: ArchitectureDesignUpdate | dict[str, Any], actor_id: str) -> ArchitectureDesign:
        design = await self.get(design_id)
        if design is None:
            raise ValueError(f"architecture design not found: {design_id}")
        payload = _dump_model_or_dict(patch, exclude_unset=True)
        change_summary = payload.pop("change_summary", None)
        semantic_change = bool(SEMANTIC_PATCH_FIELDS & payload.keys())
        if "title" in payload:
            design.title = payload["title"]
        if "global_description" in payload:
            design.global_description = payload["global_description"]
        for field_name in ("entities", "interfaces"):
            if field_name in payload:
                setattr(design, field_name, payload[field_name] or [])
                flag_modified(design, field_name)
        if "diagrams" in payload:
            design.diagrams = await self._normalize_diagrams(design.board_id, design.id, payload["diagrams"] or [])
            flag_modified(design, "diagrams")
        for field_name in ("source_ref", "source_version", "source_design_id"):
            if field_name in payload:
                setattr(design, field_name, payload[field_name])
        design.stale = False
        design.breaking_change_flag = False
        design.requires_arch_review = False
        design.version += 1
        await self.snapshot(design.id, actor_id, change_summary or "Architecture design updated")
        if semantic_change:
            await self._publish_parent_semantic_change(design, actor_id)
        await self.db.flush()
        await self.db.refresh(design)
        return design

    async def delete(self, design_id: str, actor_id: str | None = None) -> bool:
        design = await self.get(design_id)
        if design is None:
            return False
        await self._publish_parent_semantic_change(design, actor_id)
        await self.db.delete(design)
        await self.db.flush()
        return True

    async def snapshot(self, design_id: str, actor_id: str, reason: str | None = None) -> ArchitectureDesignVersion:
        design = await self.get(design_id)
        if design is None:
            raise ValueError(f"architecture design not found: {design_id}")
        envelope = self._envelope_snapshot(design)
        version = ArchitectureDesignVersion(
            design_id=design.id,
            version=design.version,
            envelope_snapshot=envelope,
            diagram_refs_snapshot=[
                {
                    "id": diagram.get("id"),
                    "adapter_payload_ref": diagram.get("adapter_payload_ref"),
                    "content_hash": diagram.get("content_hash"),
                    "format": diagram.get("format"),
                }
                for diagram in design.diagrams
            ],
            created_by=actor_id,
            change_summary=reason,
        )
        self.db.add(version)
        await self.db.flush()
        return version

    async def diff(self, design_id: str, from_version: int, to_version: int) -> ArchitectureDiffResponse:
        result = await self.db.execute(
            select(ArchitectureDesignVersion).where(
                ArchitectureDesignVersion.design_id == design_id,
                ArchitectureDesignVersion.version.in_([from_version, to_version]),
            )
        )
        versions = {item.version: item for item in result.scalars().all()}
        if from_version not in versions or to_version not in versions:
            raise ValueError("architecture design version not found")
        before = versions[from_version].envelope_snapshot
        after = versions[to_version].envelope_snapshot
        changed_fields = [
            field
            for field in ("title", "global_description", "entities", "interfaces", "diagrams")
            if before.get(field) != after.get(field)
        ]
        semantic_fields = [field for field in changed_fields if field != "diagrams"]
        layout_fields = ["diagrams"] if "diagrams" in changed_fields else []
        return ArchitectureDiffResponse(
            design_id=design_id,
            from_version=from_version,
            to_version=to_version,
            changed_fields=changed_fields,
            semantic_changes=[{"field": field} for field in semantic_fields],
            layout_changes=[{"field": field} for field in layout_fields],
            breaking_change_flag=False,
            requires_arch_review=False,
        )

    def to_summary(self, design: ArchitectureDesign) -> ArchitectureDesignSummary:
        diagrams = list(design.diagrams or [])
        return ArchitectureDesignSummary(
            id=design.id,
            board_id=design.board_id,
            parent_type=design.parent_type,
            parent_id=self.parent_id_for(design),
            title=design.title,
            version=design.version,
            source_ref=design.source_ref,
            source_version=design.source_version,
            stale=False,
            breaking_change_flag=False,
            requires_arch_review=False,
            diagrams_count=len(diagrams),
            adapter_payload_refs=[d["adapter_payload_ref"] for d in diagrams if d.get("adapter_payload_ref")],
            created_at=design.created_at,
            updated_at=design.updated_at,
        )

    def to_response(self, design: ArchitectureDesign) -> ArchitectureDesignResponse:
        return ArchitectureDesignResponse(
            id=design.id,
            board_id=design.board_id,
            parent_type=design.parent_type,
            parent_id=self.parent_id_for(design),
            title=design.title,
            global_description=design.global_description,
            entities=design.entities or [],
            interfaces=design.interfaces or [],
            diagrams=design.diagrams or [],
            version=design.version,
            source_ref=design.source_ref,
            source_version=design.source_version,
            source_design_id=design.source_design_id,
            stale=False,
            breaking_change_flag=False,
            requires_arch_review=False,
            created_by=design.created_by,
            created_at=design.created_at,
            updated_at=design.updated_at,
        )

    def parent_id_for(self, design: ArchitectureDesign) -> str:
        _, parent_field = self._parent_config(design.parent_type)
        parent_id = getattr(design, parent_field)
        if not parent_id:
            raise ValueError(f"architecture design has no {parent_field}")
        return parent_id

    def source_ref_for(self, design: ArchitectureDesign) -> str:
        return f"architecture_design:{design.id}"

    async def _normalize_diagrams(self, board_id: str, design_id: str, diagrams: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized_diagrams: list[dict[str, Any]] = []
        for raw_diagram in diagrams:
            diagram = dict(raw_diagram)
            diagram_id = diagram.get("id") or _new_scoped_id("diag")
            diagram["id"] = diagram_id
            format_name = diagram.get("format") or "raw"
            diagram["format"] = format_name
            payload = diagram.pop("adapter_payload", None)
            if payload is not None:
                adapter = self.adapter_registry.get(format_name)
                normalized_payload = adapter.normalize(payload)
                payload_row = await self.diagram_store.save_payload(
                    board_id=board_id,
                    design_id=design_id,
                    diagram_id=diagram_id,
                    format=format_name,
                    payload=normalized_payload,
                )
                diagram["adapter_payload_ref"] = payload_row.id
                diagram["content_hash"] = payload_row.content_hash
                diagram["size_bytes"] = payload_row.size_bytes
            normalized_diagrams.append(diagram)
        return normalized_diagrams

    async def _attach_payloads(self, design: ArchitectureDesign) -> None:
        diagrams = []
        for diagram in design.diagrams or []:
            enriched = dict(diagram)
            ref = enriched.get("adapter_payload_ref")
            if ref:
                try:
                    enriched["adapter_payload"] = await self.diagram_store.load_payload(ref)
                except KeyError:
                    enriched["adapter_payload_missing"] = True
            diagrams.append(enriched)
        set_committed_value(design, "diagrams", diagrams)

    def _envelope_snapshot(self, design: ArchitectureDesign) -> dict[str, Any]:
        return {
            "id": design.id,
            "board_id": design.board_id,
            "parent_type": design.parent_type,
            "parent_id": self.parent_id_for(design),
            "title": design.title,
            "global_description": design.global_description,
            "entities": copy.deepcopy(design.entities or []),
            "interfaces": copy.deepcopy(design.interfaces or []),
            "diagrams": copy.deepcopy(design.diagrams or []),
            "version": design.version,
            "source_ref": design.source_ref,
            "source_version": design.source_version,
            "source_design_id": design.source_design_id,
            "stale": False,
            "breaking_change_flag": False,
            "requires_arch_review": False,
        }

    def _parent_config(self, parent_type: str):
        try:
            return PARENT_MODELS[parent_type]
        except KeyError as exc:
            raise ValueError(f"unsupported architecture parent type: {parent_type}") from exc

    async def _publish_parent_semantic_change(
        self,
        design: ArchitectureDesign,
        actor_id: str | None,
    ) -> None:
        """Emit the existing semantic events that feed KG consolidation.

        Architecture is a child artifact, but spec/refinement consolidation is
        parent-scoped today. Reusing these event types keeps the KG pipeline
        light and avoids a new Kuzu schema or dispatcher branch for v1.
        """
        if design.parent_type == "spec" and design.spec_id:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecSemanticChanged

            await event_publish(
                SpecSemanticChanged(
                    board_id=design.board_id,
                    actor_id=actor_id,
                    spec_id=design.spec_id,
                    changed_fields=["architecture_designs"],
                ),
                session=self.db,
            )
        elif design.parent_type == "refinement" and design.refinement_id:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import RefinementSemanticChanged

            await event_publish(
                RefinementSemanticChanged(
                    board_id=design.board_id,
                    actor_id=actor_id,
                    refinement_id=design.refinement_id,
                    changed_fields=["architecture_designs"],
                ),
                session=self.db,
            )


class ArchitecturePropagationService:
    """Copy architecture snapshots between ceremony artifacts."""

    def __init__(
        self,
        db: AsyncSession,
        *,
        repository: ArchitectureDesignRepository | None = None,
    ):
        self.db = db
        self.repository = repository or ArchitectureDesignRepository(db)

    async def copy_from_parent(
        self,
        source_parent_type: str,
        source_parent_id: str,
        target_parent_type: str,
        target_parent_id: str,
        actor_id: str,
        design_ids: list[str] | None = None,
    ) -> list[ArchitectureDesign]:
        source_parent = await self._get_parent(source_parent_type, source_parent_id)
        target_parent = await self._get_parent(target_parent_type, target_parent_id)
        if source_parent is None:
            raise ValueError(f"{source_parent_type} parent not found: {source_parent_id}")
        if target_parent is None:
            raise ValueError(f"{target_parent_type} parent not found: {target_parent_id}")
        if getattr(source_parent, "board_id") != getattr(target_parent, "board_id"):
            raise ValueError("source and target parents must belong to the same board")

        source_designs = await self.repository.list(source_parent_type, source_parent_id, include_payloads=True)
        if design_ids is not None:
            wanted = set(design_ids)
            source_designs = [design for design in source_designs if design.id in wanted]

        copied: list[ArchitectureDesign] = []
        for source_design in source_designs:
            source_ref = self.repository.source_ref_for(source_design)
            existing = await self._find_existing_copy(target_parent_type, target_parent_id, source_ref)
            payload = self._payload_from_source(source_design, source_ref)
            if existing is None:
                copied_design = await self.repository.create(
                    target_parent_type,
                    target_parent_id,
                    ArchitectureDesignCreate(**payload),
                    actor_id,
                )
            else:
                copied_design = await self.repository.update(
                    existing.id,
                    ArchitectureDesignUpdate(
                        title=payload["title"],
                        global_description=payload["global_description"],
                        entities=payload["entities"],
                        interfaces=payload["interfaces"],
                        diagrams=payload["diagrams"],
                        source_ref=payload["source_ref"],
                        source_version=payload["source_version"],
                        source_design_id=payload["source_design_id"],
                        stale=False,
                        breaking_change_flag=False,
                        requires_arch_review=False,
                        change_summary=f"Re-synced from {source_parent_type} architecture",
                    ),
                    actor_id,
                )
            copied.append(copied_design)
        await self.db.flush()
        return copied

    async def copy_spec_to_card(
        self,
        spec_id: str,
        card_id: str,
        actor_id: str,
        design_ids: list[str] | None = None,
    ) -> list[ArchitectureDesign]:
        return await self.copy_from_parent("spec", spec_id, "card", card_id, actor_id, design_ids)

    async def _get_parent(self, parent_type: str, parent_id: str) -> Any | None:
        parent_model, _ = self.repository._parent_config(parent_type)
        return await self.db.get(parent_model, parent_id)

    async def _find_existing_copy(
        self,
        target_parent_type: str,
        target_parent_id: str,
        source_ref: str,
    ) -> ArchitectureDesign | None:
        _, target_field = self.repository._parent_config(target_parent_type)
        result = await self.db.execute(
            select(ArchitectureDesign).where(
                ArchitectureDesign.parent_type == target_parent_type,
                getattr(ArchitectureDesign, target_field) == target_parent_id,
                ArchitectureDesign.source_ref == source_ref,
            )
        )
        return result.scalar_one_or_none()

    def _payload_from_source(self, design: ArchitectureDesign, source_ref: str) -> dict[str, Any]:
        diagrams = []
        for diagram in design.diagrams or []:
            copied = copy.deepcopy(diagram)
            source_payload_ref = copied.pop("adapter_payload_ref", None)
            copied["source_diagram_id"] = copied.get("id")
            copied["source_payload_ref"] = source_payload_ref
            copied.pop("content_hash", None)
            copied.pop("size_bytes", None)
            diagrams.append(copied)
        return {
            "title": design.title,
            "global_description": design.global_description,
            "entities": copy.deepcopy(design.entities or []),
            "interfaces": copy.deepcopy(design.interfaces or []),
            "diagrams": diagrams,
            "source_ref": source_ref,
            "source_version": design.version,
            "source_design_id": design.id,
            "stale": False,
            "breaking_change_flag": False,
            "requires_arch_review": False,
        }
