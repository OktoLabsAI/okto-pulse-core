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
import re
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

ALLOWED_INTERFACE_DIRECTIONS = {
    "source_to_target",
    "target_to_source",
    "bidirectional",
    "none",
}

ALLOWED_CONNECTION_TYPES = {
    "direct",
    "elbow",
}

ARCHITECTURE_DESIGN_SCHEMA_VERSION = "2026-05-01"


def architecture_design_payload_schema() -> dict[str, Any]:
    """Machine-readable authoring contract for Architecture Design payloads."""
    return copy.deepcopy(
        {
            "schema_version": ARCHITECTURE_DESIGN_SCHEMA_VERSION,
            "allowed_values": {
                "parent_type": ["ideation", "refinement", "spec", "card"],
                "interface.direction": sorted(ALLOWED_INTERFACE_DIRECTIONS),
                "diagram.format": ["excalidraw_json", "mermaid", "plantuml", "c4", "svg", "raw"],
                "excalidraw.connectionType": sorted(ALLOWED_CONNECTION_TYPES),
            },
            "root_contract": {
                "required": ["title", "global_description"],
                "recommended": ["entities", "interfaces", "diagrams"],
                "rules": [
                    "title should name the architecture slice being described",
                    "global_description should explain architecture intent, boundaries, responsibilities, and important constraints",
                    "entities, interfaces, and diagrams should be present whenever they reduce ambiguity for implementers or validators",
                ],
            },
            "entity_type_examples": [
                "web_app",
                "mobile_app",
                "api",
                "service",
                "worker",
                "agent",
                "cli",
                "database",
                "cache",
                "blob_store",
                "file_store",
                "vector_store",
                "graph_store",
                "queue",
                "topic",
                "event_bus",
                "stream",
                "external_service",
                "identity_provider",
                "payment_gateway",
                "model_provider",
                "mcp_server",
                "mcp_client",
                "scheduler",
                "gateway",
                "load_balancer",
            ],
            "entity_contract": {
                "required": ["id", "name", "entity_type"],
                "recommended": ["responsibility", "boundaries", "technologies", "relationships", "notes"],
                "rules": [
                    "name must be a concrete component name, not just the category",
                    "name comparison is normalized by removing spaces, underscores and hyphens",
                    "entity_type is the category, for example api, web_app, database, queue, worker, external_service",
                    "responsibility should say what the component owns or guarantees",
                    "boundaries should say what runtime, data, team, or external boundary the entity represents",
                ],
                "bad_examples": [
                    {"id": "entity-api", "name": "API", "entity_type": "api"},
                    {"id": "entity-db", "name": "Database", "entity_type": "database"},
                    {"id": "entity-mcp", "name": "MCP Server", "entity_type": "mcp_server"},
                ],
                "good_examples": [
                    {
                        "id": "entity-checkout-api",
                        "name": "Checkout API",
                        "entity_type": "api",
                        "responsibility": "Validates checkout commands and orchestrates payment authorization.",
                        "boundaries": "Backend application boundary",
                        "technologies": ["FastAPI", "SQLAlchemy"],
                    },
                    {
                        "id": "entity-orders-db",
                        "name": "Orders DB",
                        "entity_type": "database",
                        "responsibility": "Persists orders, payment state and idempotency keys.",
                        "technologies": ["PostgreSQL"],
                    },
                    {
                        "id": "entity-okto-mcp-endpoint",
                        "name": "Okto Pulse MCP Endpoint",
                        "entity_type": "mcp_server",
                        "responsibility": "Exposes Okto Pulse tools to MCP clients.",
                        "technologies": ["FastMCP", "ASGI"],
                    },
                ],
                "anti_patterns": [
                    {
                        "pattern": "generic name or name equals entity_type",
                        "example": {"name": "API", "entity_type": "api"},
                        "consequence": "ownership and task boundaries become ambiguous; implementations can land in the wrong component",
                    },
                    {
                        "pattern": "missing responsibility",
                        "consequence": "implementers must infer what the component owns, guarantees, persists, or delegates",
                    },
                    {
                        "pattern": "missing boundaries",
                        "consequence": "runtime, data, tenancy, external-system, and security boundaries may be crossed accidentally",
                    },
                ],
            },
            "interface_contract": {
                "required": ["id", "name"],
                "recommended": [
                    "endpoint",
                    "description",
                    "participants",
                    "direction",
                    "protocol",
                    "contract_type",
                    "request_schema",
                    "response_schema",
                    "event_schema",
                    "error_contract",
                ],
                "rules": [
                    "endpoint is optional but recommended for API paths, RPC methods, event names, queue names, or operation names",
                    "participants must contain exactly two entity ids or names when endpoints are known",
                    "when an interface is linked from a diagram edge, participants must match the edge source/target linked entities",
                    "direction must be source_to_target, target_to_source, bidirectional, or none",
                    "protocol and contract_type should be present whenever schemas or payload contracts are present",
                    "include request_schema, response_schema, event_schema, or error_contract when implementation depends on payload shape",
                    "multiple interfaces between the same two entities are valid; distinguish them with id, name, and preferably endpoint",
                ],
                "example": {
                    "id": "interface-create-order",
                    "name": "Create order",
                    "endpoint": "POST /orders",
                    "description": "Customer Portal sends checkout details to Checkout API.",
                    "participants": ["entity-customer-portal", "entity-checkout-api"],
                    "direction": "source_to_target",
                    "protocol": "REST",
                    "contract_type": "OpenAPI",
                    "request_schema": {"type": "object", "required": ["cart_id"]},
                    "response_schema": {"type": "object", "required": ["order_id"]},
                    "error_contract": {"400": "Invalid checkout payload", "409": "Duplicate idempotency key"},
                },
                "anti_patterns": [
                    {
                        "pattern": "participants omitted when endpoints are known",
                        "consequence": "diagram edges and tasks cannot trace the interaction to two concrete entities",
                    },
                    {
                        "pattern": "duplicating entities or diagrams to model several API endpoints between the same two components",
                        "consequence": "architecture becomes noisy and ownership appears split across false components",
                    },
                    {
                        "pattern": "several same-pair contracts without endpoint or descriptive operation name",
                        "consequence": "implementers cannot distinguish which request, event, or contract belongs to which task",
                    },
                    {
                        "pattern": "schemas supplied without protocol or contract_type",
                        "consequence": "agents may invent transport or contract details during implementation",
                    },
                    {
                        "pattern": "missing request/response/event/error schema for an implementation-critical contract",
                        "consequence": "payload shape, validation rules, and failure behavior are guessed later",
                    },
                ],
            },
            "excalidraw_adapter_payload_contract": {
                "root": {
                    "type": "excalidraw",
                    "version": 2,
                    "elements": [],
                    "appState": {},
                    "files": {},
                },
                "node_element": {
                    "id": "node-checkout-api",
                    "type": "rectangle",
                    "x": 360,
                    "y": 120,
                    "width": 220,
                    "height": 88,
                    "label": "Checkout API\\napi",
                    "linkedEntityId": "entity-checkout-api",
                },
                "edge_element": {
                    "id": "edge-create-order",
                    "type": "arrow",
                    "sourceElementId": "node-customer-portal",
                    "targetElementId": "node-checkout-api",
                    "linkedInterfaceIds": ["interface-create-order", "interface-get-order"],
                    "connectionType": "elbow",
                },
                "rules": [
                    "Each element needs a stable id.",
                    "Nodes should set linkedEntityId to an entity id or name.",
                    "Edges should set sourceElementId, targetElementId, linkedInterfaceIds and connectionType.",
                    "Use linkedInterfaceIds for one or more contracts on the same connection; linkedInterfaceId remains accepted for legacy single-contract edges.",
                    "Every linked interface with participants must match the source and target linkedEntityId values of the edge.",
                    "connectionType accepts only direct or elbow. Use elbow for routed/orthogonal connections. Do not use curved.",
                ],
            },
            "complete_minimal_payload_example": {
                "title": "Checkout runtime architecture",
                "global_description": "Customer Portal calls Checkout API, which persists orders in Orders DB.",
                "entities": [
                    {
                        "id": "entity-customer-portal",
                        "name": "Customer Portal",
                        "entity_type": "web_app",
                        "responsibility": "Collects checkout input.",
                    },
                    {
                        "id": "entity-checkout-api",
                        "name": "Checkout API",
                        "entity_type": "api",
                        "responsibility": "Validates checkout and creates orders.",
                    },
                ],
                "interfaces": [
                    {
                        "id": "interface-create-order",
                        "name": "Create order",
                        "endpoint": "POST /orders",
                        "description": "Customer Portal sends checkout data to Checkout API.",
                        "participants": ["entity-customer-portal", "entity-checkout-api"],
                        "direction": "source_to_target",
                        "protocol": "REST",
                        "contract_type": "OpenAPI",
                        "request_schema": {"type": "object", "required": ["cart_id"]},
                        "response_schema": {"type": "object", "required": ["order_id"]},
                    }
                ],
                "diagrams": [
                    {
                        "id": "diagram-runtime",
                        "title": "Runtime context",
                        "diagram_type": "context",
                        "format": "excalidraw_json",
                        "adapter_payload": {
                            "type": "excalidraw",
                            "version": 2,
                            "elements": [
                                {
                                    "id": "node-customer-portal",
                                    "type": "rectangle",
                                    "label": "Customer Portal\\nweb_app",
                                    "linkedEntityId": "entity-customer-portal",
                                },
                                {
                                    "id": "node-checkout-api",
                                    "type": "rectangle",
                                    "label": "Checkout API\\napi",
                                    "linkedEntityId": "entity-checkout-api",
                                },
                                {
                                    "id": "edge-create-order",
                                    "type": "arrow",
                                    "sourceElementId": "node-customer-portal",
                                    "targetElementId": "node-checkout-api",
                                    "linkedInterfaceIds": ["interface-create-order"],
                                    "connectionType": "elbow",
                                },
                            ],
                            "appState": {},
                            "files": {},
                        },
                    }
                ],
            },
        }
    )


class ArchitecturePayloadValidationError(ValueError):
    """Semantic validation failure for an Architecture Design payload."""

    def __init__(self, issues: list[str]):
        self.issues = issues
        issue_count = len(issues)
        super().__init__(f"Architecture payload rejected: {issue_count} issue(s): " + "; ".join(issues))


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


def _canonical_ref(value: Any) -> str:
    return str(value or "").strip().casefold()


def _canonical_label(value: Any) -> str:
    return re.sub(r"[\s_\-]+", "", _canonical_ref(value))


def _compact_known_refs(refs: dict[str, str], limit: int = 8) -> str:
    values = sorted(set(refs.values()))
    if not values:
        return "none"
    suffix = "" if len(values) <= limit else f", ... (+{len(values) - limit} more)"
    return ", ".join(values[:limit]) + suffix


def _custom_or_top_level(item: dict[str, Any], key: str) -> Any:
    if key in item:
        return item.get(key)
    custom_data = item.get("customData")
    if isinstance(custom_data, dict):
        return custom_data.get(key)
    return None


def _linked_interface_refs(item: dict[str, Any], path: str, issues: list[str] | None = None) -> list[Any]:
    """Return legacy and multi-interface refs from a diagram connection element."""
    refs: list[Any] = []
    legacy_ref = _custom_or_top_level(item, "linkedInterfaceId")
    if legacy_ref not in (None, ""):
        refs.append(legacy_ref)

    multi_ref = _custom_or_top_level(item, "linkedInterfaceIds")
    if multi_ref in (None, ""):
        return refs
    if not isinstance(multi_ref, list):
        if issues is not None:
            issues.append(f"{path}.linkedInterfaceIds must be a JSON array of interface ids or names.")
        return refs

    legacy_keys: set[str] = set(_canonical_ref(ref) for ref in refs if _canonical_ref(ref))
    seen: set[str] = set(legacy_keys)
    for index, ref in enumerate(multi_ref):
        ref_key = _canonical_ref(ref)
        if not ref_key:
            if issues is not None:
                issues.append(f"{path}.linkedInterfaceIds[{index}] must be a non-empty interface id or name.")
            continue
        if ref_key in seen:
            if issues is not None:
                if ref_key not in legacy_keys:
                    issues.append(
                        f"{path}.linkedInterfaceIds[{index}] duplicates another linked interface on the same connection."
                    )
            continue
        seen.add(ref_key)
        refs.append(ref)
    return refs


def _same_ordered_refs(left: list[Any], right: list[Any]) -> bool:
    return [_canonical_ref(item) for item in left] == [_canonical_ref(item) for item in right]


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
        self._validate_payload(payload)
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
        candidate_payload = {
            "title": payload.get("title", design.title),
            "global_description": payload.get("global_description", design.global_description),
            "entities": payload.get("entities", design.entities or []),
            "interfaces": payload.get("interfaces", design.interfaces or []),
            "diagrams": payload.get("diagrams", design.diagrams or []),
        }
        self._validate_payload(candidate_payload)
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

    def critique_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Return semantic validation issues and non-blocking authoring warnings."""
        issues: list[str] = []
        warnings: list[str] = []
        if not isinstance(payload, dict):
            issues.append(f"payload must be a JSON object; received {type(payload).__name__}.")
            return {
                "valid": False,
                "issues": issues,
                "warnings": warnings,
                "suggested_fixes": self._suggest_fixes(issues),
                "summary": self._payload_summary({}),
            }

        if not str(payload.get("title") or "").strip():
            issues.append("title is required. Name the architecture slice being described.")
        if not str(payload.get("global_description") or "").strip():
            issues.append(
                "global_description is required. Explain architecture intent, boundaries, responsibilities, and important constraints."
            )

        entities = self._validate_entities(payload.get("entities"), issues, warnings)
        entity_refs = self._ref_index(entities)
        interfaces = self._validate_interfaces(payload.get("interfaces"), entity_refs, issues, warnings)
        interface_refs = self._ref_index(interfaces)
        interface_items = self._ref_item_index(interfaces)
        self._validate_diagrams(payload.get("diagrams"), entity_refs, interface_refs, interface_items, issues, warnings)
        return {
            "valid": not issues,
            "issues": issues,
            "warnings": warnings,
            "suggested_fixes": self._suggest_fixes(issues + warnings),
            "summary": self._payload_summary(payload),
        }

    def validate_payload(self, payload: dict[str, Any]) -> None:
        """Raise ArchitecturePayloadValidationError when a payload cannot be persisted."""
        critique = self.critique_payload(payload)
        issues = list(critique.get("issues") or [])
        if issues:
            raise ArchitecturePayloadValidationError(issues)

    def _validate_payload(self, payload: dict[str, Any]) -> None:
        self.validate_payload(payload)

    def _validate_entities(
        self,
        raw_entities: Any,
        issues: list[str],
        warnings: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if raw_entities is None:
            return []
        if not isinstance(raw_entities, list):
            issues.append(f"entities must be a JSON array; received {type(raw_entities).__name__}.")
            return []

        entities: list[dict[str, Any]] = []
        seen_ids: dict[str, int] = {}
        seen_names: dict[str, int] = {}
        for index, raw_entity in enumerate(raw_entities):
            path = f"entities[{index}]"
            if not isinstance(raw_entity, dict):
                issues.append(f"{path} must be a JSON object; received {type(raw_entity).__name__}.")
                continue
            entities.append(raw_entity)
            entity_id = _canonical_ref(raw_entity.get("id"))
            entity_name = str(raw_entity.get("name") or "").strip()
            entity_type = str(raw_entity.get("entity_type") or "").strip()
            if warnings is not None:
                if not entity_id:
                    warnings.append(
                        f"{path}.id is empty. Diagram links and future updates become fragile without stable ids."
                    )
                if not entity_name:
                    warnings.append(
                        f"{path}.name is empty. Implementers cannot identify the component or assign ownership."
                    )
                if not entity_type:
                    warnings.append(
                        f"{path}.entity_type is empty. The UI cannot choose contextual icons/presets and agents lose category signal."
                    )
                if not str(raw_entity.get("responsibility") or "").strip():
                    warnings.append(
                        f"{path}.responsibility is empty. Tasks may implement behavior in the wrong component or skip owned duties."
                    )
                if not str(raw_entity.get("boundaries") or "").strip():
                    warnings.append(
                        f"{path}.boundaries is empty. Runtime, data, tenant, or external boundaries may be crossed accidentally."
                    )
            if entity_id:
                if entity_id in seen_ids:
                    issues.append(f"{path}.id duplicates entities[{seen_ids[entity_id]}].id '{raw_entity.get('id')}'. Use stable unique ids.")
                else:
                    seen_ids[entity_id] = index
            if entity_name:
                name_key = _canonical_ref(entity_name)
                if name_key in seen_names:
                    issues.append(
                        f"{path}.name duplicates entities[{seen_names[name_key]}].name '{entity_name}'. "
                        "Use unique component names so diagram links and task ownership stay unambiguous."
                    )
                else:
                    seen_names[name_key] = index
            if entity_name and entity_type and _canonical_label(entity_name) == _canonical_label(entity_type):
                issues.append(
                    f"{path}.name duplicates entity_type '{entity_type}' after normalization. Use a concrete component "
                    "name such as 'Checkout API', 'Orders DB', 'Billing Worker', or 'Okto Pulse MCP Endpoint', "
                    "and keep entity_type as the category."
                )
        return entities

    def _validate_interfaces(
        self,
        raw_interfaces: Any,
        entity_refs: dict[str, str],
        issues: list[str],
        warnings: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if raw_interfaces is None:
            return []
        if not isinstance(raw_interfaces, list):
            issues.append(f"interfaces must be a JSON array; received {type(raw_interfaces).__name__}.")
            return []

        interfaces: list[dict[str, Any]] = []
        seen_ids: dict[str, int] = {}
        seen_names: dict[str, int] = {}
        for index, raw_interface in enumerate(raw_interfaces):
            path = f"interfaces[{index}]"
            if not isinstance(raw_interface, dict):
                issues.append(f"{path} must be a JSON object; received {type(raw_interface).__name__}.")
                continue
            interfaces.append(raw_interface)
            interface_id = _canonical_ref(raw_interface.get("id"))
            interface_name = str(raw_interface.get("name") or "").strip()
            if warnings is not None:
                if not interface_id:
                    warnings.append(
                        f"{path}.id is empty. Diagram edges cannot link deterministically to this interface."
                    )
                if not interface_name:
                    warnings.append(
                        f"{path}.name is empty. Reviewers cannot understand what interaction the edge represents."
                    )
                if not str(raw_interface.get("description") or "").strip():
                    warnings.append(
                        f"{path}.description is empty. Implementers may miss why the integration exists or what behavior it carries."
                    )
            if interface_id:
                if interface_id in seen_ids:
                    issues.append(
                        f"{path}.id duplicates interfaces[{seen_ids[interface_id]}].id '{raw_interface.get('id')}'. "
                        "Use stable unique ids."
                    )
                else:
                    seen_ids[interface_id] = index
            if interface_name:
                name_key = _canonical_ref(interface_name)
                if name_key in seen_names:
                    issues.append(
                        f"{path}.name duplicates interfaces[{seen_names[name_key]}].name '{interface_name}'. "
                        "Use unique interface names so edges and contracts resolve deterministically."
                    )
                else:
                    seen_names[name_key] = index

            direction = raw_interface.get("direction")
            if direction not in (None, "") and direction not in ALLOWED_INTERFACE_DIRECTIONS:
                allowed = ", ".join(sorted(ALLOWED_INTERFACE_DIRECTIONS))
                issues.append(f"{path}.direction='{direction}' is invalid. Allowed values: {allowed}.")

            participants = raw_interface.get("participants") or []
            if participants:
                if not isinstance(participants, list):
                    issues.append(f"{path}.participants must be a JSON array with exactly two entity ids or names.")
                else:
                    normalized_participants = [_canonical_ref(item) for item in participants if str(item or "").strip()]
                    if len(normalized_participants) != 2:
                        issues.append(
                            f"{path}.participants must contain exactly two entities when provided; "
                            f"received {len(normalized_participants)}."
                        )
                    if len(set(normalized_participants)) != len(normalized_participants):
                        issues.append(f"{path}.participants must reference two distinct entities.")
                    for participant_index, participant in enumerate(participants):
                        participant_key = _canonical_ref(participant)
                        if participant_key and participant_key not in entity_refs:
                            issues.append(
                                f"{path}.participants[{participant_index}] references '{participant}', but it does not "
                                f"match any entity id or name. Known entities: {_compact_known_refs(entity_refs)}."
                            )
            elif warnings is not None:
                warnings.append(
                    f"{path}.participants is empty. The relationship will not be traceable to two concrete entities in diagrams or tasks."
                )

            has_schema_payload = any(
                raw_interface.get(field_name) not in (None, "", {}, [])
                for field_name in ("request_schema", "response_schema", "event_schema", "error_contract", "schema_ref")
            )
            if has_schema_payload and not raw_interface.get("protocol") and not raw_interface.get("contract_type"):
                issues.append(
                    f"{path} defines schema/contract data but leaves both protocol and contract_type empty. "
                    "Set protocol (for example REST, gRPC, Kafka, SQS) or contract_type (for example OpenAPI, JSON Schema, protobuf)."
                )
            elif warnings is not None:
                if not raw_interface.get("protocol") and not raw_interface.get("contract_type"):
                    warnings.append(
                        f"{path} leaves both protocol and contract_type empty. Agents may invent transport or contract details later."
                    )
                if not has_schema_payload:
                    warnings.append(
                        f"{path} has no request_schema, response_schema, event_schema, error_contract, or schema_ref. "
                        "Payload shape and failure modes may be guessed during implementation."
                    )
        return interfaces

    def _validate_diagrams(
        self,
        raw_diagrams: Any,
        entity_refs: dict[str, str],
        interface_refs: dict[str, str],
        interface_items: dict[str, dict[str, Any]],
        issues: list[str],
        warnings: list[str] | None = None,
    ) -> None:
        if raw_diagrams is None:
            return
        if not isinstance(raw_diagrams, list):
            issues.append(f"diagrams must be a JSON array; received {type(raw_diagrams).__name__}.")
            return

        seen_ids: dict[str, int] = {}
        seen_titles: dict[str, int] = {}
        for index, raw_diagram in enumerate(raw_diagrams):
            path = f"diagrams[{index}]"
            if not isinstance(raw_diagram, dict):
                issues.append(f"{path} must be a JSON object; received {type(raw_diagram).__name__}.")
                continue
            diagram_id = _canonical_ref(raw_diagram.get("id"))
            diagram_title = str(raw_diagram.get("title") or "").strip()
            if diagram_id:
                if diagram_id in seen_ids:
                    issues.append(f"{path}.id duplicates diagrams[{seen_ids[diagram_id]}].id '{raw_diagram.get('id')}'.")
                else:
                    seen_ids[diagram_id] = index
            if diagram_title:
                title_key = _canonical_ref(diagram_title)
                if title_key in seen_titles:
                    issues.append(f"{path}.title duplicates diagrams[{seen_titles[title_key]}].title '{diagram_title}'.")
                else:
                    seen_titles[title_key] = index

            format_name = raw_diagram.get("format") or "raw"
            payload_present = "adapter_payload" in raw_diagram and raw_diagram.get("adapter_payload") is not None
            if not payload_present:
                continue
            payload = raw_diagram.get("adapter_payload")
            try:
                adapter = self.adapter_registry.get(format_name)
                adapter.validate(payload)
            except Exception as exc:
                issues.append(f"{path}.adapter_payload invalid for format '{format_name}': {exc}")
                continue
            if format_name == "excalidraw_json" and isinstance(payload, dict):
                self._validate_excalidraw_links(path, payload, entity_refs, interface_refs, interface_items, issues, warnings)

    def _validate_excalidraw_links(
        self,
        path: str,
        payload: dict[str, Any],
        entity_refs: dict[str, str],
        interface_refs: dict[str, str],
        interface_items: dict[str, dict[str, Any]],
        issues: list[str],
        warnings: list[str] | None = None,
    ) -> None:
        elements = payload.get("elements") or []
        element_refs: dict[str, str] = {}
        element_entity_refs: dict[str, str] = {}
        seen_element_ids: dict[str, int] = {}
        for index, raw_element in enumerate(elements):
            element_path = f"{path}.adapter_payload.elements[{index}]"
            if not isinstance(raw_element, dict):
                issues.append(f"{element_path} must be a JSON object; received {type(raw_element).__name__}.")
                continue
            element_id = _canonical_ref(raw_element.get("id"))
            if not element_id:
                issues.append(f"{element_path}.id is required so diagram links can target the element.")
                continue
            if element_id in seen_element_ids:
                issues.append(f"{element_path}.id duplicates elements[{seen_element_ids[element_id]}].id '{raw_element.get('id')}'.")
            else:
                seen_element_ids[element_id] = index
                element_refs[element_id] = str(raw_element.get("id"))
                linked_entity_id = _custom_or_top_level(raw_element, "linkedEntityId")
                if linked_entity_id not in (None, ""):
                    element_entity_refs[element_id] = str(linked_entity_id)

        for index, raw_element in enumerate(elements):
            if not isinstance(raw_element, dict):
                continue
            element_path = f"{path}.adapter_payload.elements[{index}]"
            linked_entity_id = _custom_or_top_level(raw_element, "linkedEntityId")
            if linked_entity_id not in (None, "") and _canonical_ref(linked_entity_id) not in entity_refs:
                issues.append(
                    f"{element_path}.linkedEntityId references '{linked_entity_id}', but it does not match any "
                    f"entity id or name. Known entities: {_compact_known_refs(entity_refs)}."
                )
            linked_interface_ids = _linked_interface_refs(raw_element, element_path, issues)
            for linked_interface_id in linked_interface_ids:
                if _canonical_ref(linked_interface_id) not in interface_refs:
                    issues.append(
                        f"{element_path}.linkedInterfaceIds references '{linked_interface_id}', but it does not match any "
                        f"interface id or name. Known interfaces: {_compact_known_refs(interface_refs)}."
                    )

            connection_type = _custom_or_top_level(raw_element, "connectionType")
            if connection_type not in (None, "") and connection_type not in ALLOWED_CONNECTION_TYPES:
                allowed = ", ".join(sorted(ALLOWED_CONNECTION_TYPES))
                issues.append(
                    f"{element_path}.connectionType='{connection_type}' is invalid. Allowed values: {allowed}. "
                    "Suggested fix: use 'elbow' for routed/orthogonal connections or 'direct' for straight connections; do not use 'curved'."
                )

            source_id = _custom_or_top_level(raw_element, "sourceElementId")
            target_id = _custom_or_top_level(raw_element, "targetElementId")
            if source_id or target_id:
                if not source_id or not target_id:
                    issues.append(f"{element_path} must set both sourceElementId and targetElementId for a connection.")
                if source_id and _canonical_ref(source_id) not in element_refs:
                    issues.append(
                        f"{element_path}.sourceElementId references '{source_id}', but no diagram element with that id exists."
                    )
                if target_id and _canonical_ref(target_id) not in element_refs:
                    issues.append(
                        f"{element_path}.targetElementId references '{target_id}', but no diagram element with that id exists."
                    )
                source_entity_ref = element_entity_refs.get(_canonical_ref(source_id))
                target_entity_ref = element_entity_refs.get(_canonical_ref(target_id))
                if source_entity_ref and target_entity_ref:
                    edge_participants = [source_entity_ref, target_entity_ref]
                    for linked_interface_id in linked_interface_ids:
                        raw_interface = interface_items.get(_canonical_ref(linked_interface_id))
                        if not raw_interface:
                            continue
                        participants = raw_interface.get("participants") or []
                        normalized_participants = [
                            participant for participant in participants if str(participant or "").strip()
                        ] if isinstance(participants, list) else []
                        if len(normalized_participants) == 2 and not _same_ordered_refs(normalized_participants, edge_participants):
                            issues.append(
                                f"{element_path} links interface '{linked_interface_id}', but its participants "
                                f"{normalized_participants!r} do not match the connection endpoints {edge_participants!r}. "
                                "The connection defines the two endpoint entities; update interface participants or link a different contract."
                            )
                        elif warnings is not None and not normalized_participants:
                            warnings.append(
                                f"{element_path} links interface '{linked_interface_id}' without participants. "
                                f"Suggested derivation: set participants to {edge_participants!r}."
                            )
                        if warnings is not None and len(linked_interface_ids) > 1 and not str(raw_interface.get("endpoint") or "").strip():
                            warnings.append(
                                f"{element_path} links multiple interfaces; interface '{linked_interface_id}' has no endpoint. "
                                "Set endpoint to distinguish API paths, RPC methods, events, queues, or operations on the same connector."
                            )
                if warnings is not None and not linked_interface_ids:
                    warnings.append(
                        f"{element_path}.linkedInterfaceIds is empty. The diagram connection will not open any interface contract."
                    )
            elif warnings is not None and raw_element.get("type") not in ("arrow", "line"):
                if linked_entity_id in (None, ""):
                    warnings.append(
                        f"{element_path}.linkedEntityId is empty. The diagram node will not open the entity description."
                    )

    def _ref_index(self, items: list[dict[str, Any]]) -> dict[str, str]:
        refs: dict[str, str] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            item_id = item.get("id")
            item_name = item.get("name")
            if str(item_id or "").strip():
                refs[_canonical_ref(item_id)] = str(item_id)
            if str(item_name or "").strip():
                refs[_canonical_ref(item_name)] = str(item_name)
        return refs

    def _ref_item_index(self, items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        refs: dict[str, dict[str, Any]] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            item_id = item.get("id")
            item_name = item.get("name")
            if str(item_id or "").strip():
                refs[_canonical_ref(item_id)] = item
            if str(item_name or "").strip():
                refs[_canonical_ref(item_name)] = item
        return refs

    def _payload_summary(self, payload: dict[str, Any]) -> dict[str, Any]:
        diagrams = payload.get("diagrams") or []
        elements_count = 0
        linked_entity_elements = 0
        linked_interface_elements = 0
        if isinstance(diagrams, list):
            for diagram in diagrams:
                if not isinstance(diagram, dict):
                    continue
                adapter_payload = diagram.get("adapter_payload") or {}
                if not isinstance(adapter_payload, dict):
                    continue
                elements = adapter_payload.get("elements") or []
                if not isinstance(elements, list):
                    continue
                for element in elements:
                    if not isinstance(element, dict):
                        continue
                    elements_count += 1
                    if _custom_or_top_level(element, "linkedEntityId") not in (None, ""):
                        linked_entity_elements += 1
                    if _linked_interface_refs(element, "payload_summary"):
                        linked_interface_elements += 1
        return {
            "entities_count": len(payload.get("entities") or []) if isinstance(payload.get("entities") or [], list) else 0,
            "interfaces_count": len(payload.get("interfaces") or []) if isinstance(payload.get("interfaces") or [], list) else 0,
            "diagrams_count": len(diagrams) if isinstance(diagrams, list) else 0,
            "diagram_elements_count": elements_count,
            "linked_entity_elements_count": linked_entity_elements,
            "linked_interface_elements_count": linked_interface_elements,
        }

    def _suggest_fixes(self, messages: list[str]) -> list[str]:
        fixes: list[str] = []
        for message in messages:
            lower = message.casefold()
            if "connectiontype" in lower:
                fixes.append("Use connectionType='direct' for straight edges or connectionType='elbow' for routed/orthogonal edges; never use 'curved'.")
            elif "duplicates entity_type" in lower:
                fixes.append("Rename the entity to a concrete component name and keep entity_type as the category, e.g. 'Okto Pulse MCP Endpoint' + 'mcp_server'.")
            elif "participants" in lower:
                fixes.append("Set participants to exactly the two linkedEntityId values on the diagram edge source and target nodes.")
            elif ".direction" in lower:
                fixes.append("Use one of direction='source_to_target', 'target_to_source', 'bidirectional', or 'none'.")
            elif "protocol" in lower or "contract_type" in lower:
                fixes.append("Set protocol and/or contract_type, then include request_schema, response_schema, event_schema, or error_contract when payload shape matters.")
            elif "linkedentityid" in lower:
                fixes.append("Set linkedEntityId on diagram nodes to an existing entity id or name.")
            elif "linkedinterfaceid" in lower:
                fixes.append("Set linkedInterfaceIds on diagram edges to one or more existing interface ids or names.")
            elif "endpoint" in lower:
                fixes.append("Set endpoint on same-connector interfaces to distinguish API paths, RPC methods, events, queues, or operations.")
            elif "title is required" in lower:
                fixes.append("Set title to a concrete architecture slice name, e.g. 'Checkout runtime architecture'.")
            elif "global_description is required" in lower:
                fixes.append("Set global_description to a concise narrative of architecture intent, boundaries, responsibilities, and constraints.")
            elif "responsibility" in lower:
                fixes.append("Add a concise responsibility that states what the component owns, guarantees, or persists.")
            elif "boundaries" in lower:
                fixes.append("Add boundaries describing runtime, data, tenancy, external-system, or ownership limits.")
        deduped: list[str] = []
        for fix in fixes:
            if fix not in deduped:
                deduped.append(fix)
        return deduped

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
