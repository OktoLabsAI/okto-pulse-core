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
import logging
import uuid
from collections import Counter
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from okto_pulse.core.ports.architecture_persistence import (
    ArchitectureFilter,
    ArchitectureOperator,
    ArchitectureQuery,
    ArchitectureRecord,
    get_architecture_persistence_port,
)
from okto_pulse.core.models.schemas import (
    ArchitectureDesignCreate,
    ArchitectureDesignResponse,
    ArchitectureDesignSummary,
    ArchitectureDesignUpdate,
    ArchitectureDiffResponse,
    ArchitectureWarningAcknowledgementRequest,
)
from okto_pulse.core.services.architecture_observability import (
    observe_architecture_finding_run_persist,
    observe_architecture_gate_eval,
    observe_architecture_projection,
    observe_architecture_warning_ack,
)

PARENT_MODELS = {
    "ideation": ("ideation", "ideation_id"),
    "refinement": ("refinement", "refinement_id"),
    "spec": ("spec", "spec_id"),
    "card": ("card", "card_id"),
}


def _arf(
    field: str,
    operator: ArchitectureOperator,
    value: Any = None,
) -> ArchitectureFilter:
    return ArchitectureFilter(field, operator, value)


async def _architecture_list(
    context: Any,
    entity: str,
    *,
    filters: tuple[ArchitectureFilter, ...] = (),
    any_filters: tuple[ArchitectureFilter, ...] = (),
    order_by: tuple[tuple[str, bool], ...] = (),
    limit: int | None = None,
) -> list[ArchitectureRecord]:
    rows = await get_architecture_persistence_port().list(
        context,
        ArchitectureQuery(
            entity=entity,
            filters=filters,
            any_filters=any_filters,
            order_by=order_by,
            limit=limit,
        ),
    )
    return list(rows)


async def _architecture_get(
    context: Any,
    entity: str,
    record_id: str,
) -> ArchitectureRecord | None:
    return await get_architecture_persistence_port().get(
        context,
        entity=entity,
        record_id=record_id,
    )


async def _architecture_create(
    context: Any,
    entity: str,
    **values: Any,
) -> ArchitectureRecord:
    return await get_architecture_persistence_port().create(
        context,
        entity=entity,
        values=values,
    )


async def _architecture_flush(context: Any) -> None:
    await get_architecture_persistence_port().flush(context)

SEMANTIC_PATCH_FIELDS = {
    "title",
    "global_description",
    "entities",
    "interfaces",
    "diagrams",
}

CARD_ARCHITECTURE_READ_ONLY_MESSAGE = (
    "Card Architecture Designs are read-only governed snapshots. "
    "Copy architecture from the parent spec to refresh card context, and edit "
    "the source ideation, refinement, or spec Architecture Design instead."
)


class CardArchitectureReadOnlyError(ValueError):
    """Raised when a caller tries to author Architecture directly on a card."""


def _ensure_card_architecture_write_allowed(parent_type: str, *, allow: bool) -> None:
    if parent_type == "card" and not allow:
        raise CardArchitectureReadOnlyError(CARD_ARCHITECTURE_READ_ONLY_MESSAGE)


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

# Spec cc497a0d — Semantic registry that maps entity_type -> canonical Excalidraw
# node metadata. Used to (a) normalize linkedEntityId-only nodes safely before
# persistence, (b) reject ambiguous/divergent payloads, (c) expose the contract
# to MCP agents via okto_pulse_get_architecture_design_schema.
SEMANTIC_NODE_REGISTRY: dict[str, dict[str, str]] = {
    # Application & runtime
    "web_app": {"displayType": "Web App", "architectureKind": "frontend", "iconName": "monitor"},
    "mobile_app": {"displayType": "Mobile App", "architectureKind": "frontend", "iconName": "smartphone"},
    "api": {"displayType": "API", "architectureKind": "service", "iconName": "server"},
    "service": {"displayType": "Service", "architectureKind": "service", "iconName": "server"},
    "worker": {"displayType": "Worker", "architectureKind": "compute", "iconName": "cpu"},
    "agent": {"displayType": "Agent", "architectureKind": "service", "iconName": "workflow"},
    "cli": {"displayType": "CLI", "architectureKind": "interface", "iconName": "terminal"},
    # Data
    "database": {"displayType": "Database", "architectureKind": "data", "iconName": "database"},
    "cache": {"displayType": "Cache", "architectureKind": "data", "iconName": "hard_drive"},
    "blob_store": {"displayType": "Blob Storage", "architectureKind": "data", "iconName": "hard_drive"},
    "file_store": {"displayType": "File Storage", "architectureKind": "data", "iconName": "hard_drive"},
    "vector_store": {"displayType": "Vector Store", "architectureKind": "data", "iconName": "database"},
    "graph_store": {"displayType": "Graph Store", "architectureKind": "data", "iconName": "database"},
    # Messaging
    "queue": {"displayType": "Queue", "architectureKind": "messaging", "iconName": "message"},
    "topic": {"displayType": "Topic", "architectureKind": "messaging", "iconName": "message"},
    "event_bus": {"displayType": "Event Bus", "architectureKind": "messaging", "iconName": "workflow"},
    "stream": {"displayType": "Stream", "architectureKind": "messaging", "iconName": "workflow"},
    # External & identity
    "external_service": {"displayType": "External Service", "architectureKind": "external", "iconName": "globe"},
    "identity_provider": {"displayType": "Identity Provider", "architectureKind": "security", "iconName": "lock"},
    "payment_gateway": {"displayType": "Payment Gateway", "architectureKind": "external", "iconName": "lock"},
    "model_provider": {"displayType": "Model Provider", "architectureKind": "external", "iconName": "cpu"},
    "mcp_server": {"displayType": "MCP Server", "architectureKind": "service", "iconName": "server"},
    "mcp_client": {"displayType": "MCP Client", "architectureKind": "service", "iconName": "terminal"},
    # Infrastructure
    "scheduler": {"displayType": "Scheduler", "architectureKind": "workflow", "iconName": "workflow"},
    "gateway": {"displayType": "Gateway", "architectureKind": "network", "iconName": "network"},
    "load_balancer": {"displayType": "Load Balancer", "architectureKind": "network", "iconName": "network"},
}

ALLOWED_NODE_ICON_NAMES: frozenset[str] = frozenset({
    "boxes", "server", "database", "message", "user", "network", "cloud", "cpu",
    "globe", "hard_drive", "lock", "monitor", "package", "smartphone", "terminal", "workflow",
})

REQUIRED_LINKED_NODE_FIELDS: tuple[str, ...] = (
    "text", "displayType", "architectureKind", "iconName", "linkedEntityId",
)

logger = logging.getLogger("okto_pulse.architecture")

ARCHITECTURE_DESIGN_SCHEMA_VERSION = "2026-05-04"
SUPPORTED_ARCHITECTURE_DIAGRAM_FORMAT = "excalidraw_json"

TOPOLOGY_WARNING_CODES: frozenset[str] = frozenset({
    "isolated_entity_node",
    "disconnected_subgraph",
    "dangling_connector",
    "entity_without_diagram",
    "conceptual_justification_invalid",
})

TOPOLOGY_SUGGESTED_FIXES: dict[str, str] = {
    "isolated_entity_node": "Connect '{label}' to a producer/consumer, remove it from this diagram, or add a valid conceptual justification.",
    "disconnected_subgraph": "Connect the disconnected subgraph to the main flow, split it into a separate diagram, or justify it when the diagram is conceptual.",
    "dangling_connector": "Set both sourceElementId and targetElementId to existing diagram elements or remove the connector.",
    "entity_without_diagram": "Add a linked diagram node for entity '{entity_name}' or document why the entity is intentionally not visualized.",
    "conceptual_justification_invalid": "Replace the placeholder justification with a specific reason for why this element is intentionally isolated.",
}

CONCEPTUAL_CONNECTIVITY_WARNING_CODES: frozenset[str] = frozenset({
    "isolated_entity_node",
    "disconnected_subgraph",
})

CONCEPTUAL_JUSTIFICATION_PLACEHOLDERS: frozenset[str] = frozenset({
    "todo",
    "tbd",
    "n/a",
    "na",
    "none",
    "fix later",
})

CONCEPTUAL_JUSTIFICATION_MIN_CHARS = 12
CONCEPTUAL_JUSTIFICATION_MAX_CHARS = 500


@dataclass(frozen=True)
class ArchitectureWarningRecord:
    """Structured non-blocking architecture validation warning."""

    code: str
    message: str
    path: str
    suggested_fix: str
    severity: str = "warning"
    diagram_id: str | None = None
    diagram_type: str | None = None
    element_id: str | None = None
    entity_id: str | None = None
    node_ref: str | None = None
    justification: str | None = None

    def __post_init__(self) -> None:
        if self.code not in TOPOLOGY_WARNING_CODES:
            raise ValueError(f"unknown architecture warning code: {self.code}")
        if self.severity != "warning":
            raise ValueError("ArchitectureWarningRecord severity must be 'warning'")
        target_count = sum(1 for value in (self.element_id, self.entity_id, self.node_ref) if value)
        if target_count > 1:
            raise ValueError("ArchitectureWarningRecord accepts at most one primary target")
        if not self.path:
            raise ValueError("ArchitectureWarningRecord.path is required")

    def to_dict(self) -> dict[str, Any]:
        data = {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "path": self.path,
            "suggested_fix": self.suggested_fix,
            "diagram_id": self.diagram_id,
            "diagram_type": self.diagram_type,
            "element_id": self.element_id,
            "entity_id": self.entity_id,
            "node_ref": self.node_ref,
            "justification": self.justification,
        }
        return {key: value for key, value in data.items() if value not in (None, "")}


@dataclass(frozen=True)
class TopologyWarningEvaluation:
    """Result produced by the pure architecture topology analyzer."""

    warnings: tuple[ArchitectureWarningRecord, ...]
    suppressed_warnings: tuple[ArchitectureWarningRecord, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "warnings": [warning.to_dict() for warning in self.warnings],
            "suppressed_warnings": [warning.to_dict() for warning in self.suppressed_warnings],
        }


@dataclass(frozen=True)
class _TopologyNode:
    ref: str
    element_id: str | None
    path: str
    label: str
    linked_entity_ref: str | None


class TopologyWarningEngine:
    """Pure, adapter-neutral topology checks for Architecture Design diagrams."""

    EDGE_TYPES = frozenset({"arrow", "line"})

    def evaluate(
        self,
        *,
        entities: list[dict[str, Any]],
        diagrams: list[dict[str, Any]],
        existing_issues: list[str] | None = None,
    ) -> TopologyWarningEvaluation:
        """Return deterministic non-blocking topology and coverage warnings.

        ``existing_issues`` is accepted for the later payload-critic wiring path.
        The engine never downgrades or removes existing blocking issues.
        """
        _ = existing_issues
        if not isinstance(entities, list) or not isinstance(diagrams, list):
            return TopologyWarningEvaluation(warnings=())

        declared_entities, entity_ref_to_declared = self._declared_entities(entities)
        covered_entities: set[str] = set()
        warnings: list[ArchitectureWarningRecord] = []
        suppressed_warnings: list[ArchitectureWarningRecord] = []

        for diagram_index, raw_diagram in enumerate(diagrams):
            if not isinstance(raw_diagram, dict):
                continue
            if (raw_diagram.get("format") or SUPPORTED_ARCHITECTURE_DIAGRAM_FORMAT) != SUPPORTED_ARCHITECTURE_DIAGRAM_FORMAT:
                continue
            payload = raw_diagram.get("adapter_payload")
            if not isinstance(payload, dict):
                continue
            diagram_warnings, diagram_covered = self._evaluate_diagram(
                raw_diagram,
                payload,
                diagram_index,
                entity_ref_to_declared,
            )
            active_warnings, suppressed, justification_warnings = self._apply_conceptual_justifications(
                raw_diagram,
                diagram_index,
                diagram_warnings,
            )
            warnings.extend(active_warnings)
            warnings.extend(justification_warnings)
            suppressed_warnings.extend(suppressed)
            covered_entities.update(diagram_covered)

        for declared in declared_entities:
            if declared["declared_ref"] in covered_entities:
                continue
            entity_name = declared["name"] or declared["entity_id"]
            warnings.append(
                ArchitectureWarningRecord(
                    code="entity_without_diagram",
                    diagram_id=None,
                    diagram_type=None,
                    entity_id=declared["entity_id"],
                    path=declared["path"],
                    message=f"Architecture entity '{entity_name}' is not represented by any linked diagram node.",
                    suggested_fix=TOPOLOGY_SUGGESTED_FIXES["entity_without_diagram"].format(entity_name=entity_name),
                )
            )

        return TopologyWarningEvaluation(
            warnings=tuple(self._sort_warnings(warnings)),
            suppressed_warnings=tuple(self._sort_warnings(suppressed_warnings)),
        )

    def _evaluate_diagram(
        self,
        diagram: dict[str, Any],
        payload: dict[str, Any],
        diagram_index: int,
        entity_ref_to_declared: dict[str, str],
    ) -> tuple[list[ArchitectureWarningRecord], set[str]]:
        diagram_id = str(diagram.get("id") or f"diagram[{diagram_index}]")
        diagram_type = str(diagram.get("diagram_type") or "")
        elements = payload.get("elements") or []
        if not isinstance(elements, list):
            return [], set()

        nodes: dict[str, _TopologyNode] = {}
        covered_entities: set[str] = set()
        warnings: list[ArchitectureWarningRecord] = []
        edge_candidates: list[tuple[int, dict[str, Any]]] = []

        diagram_path = f"diagrams[{diagram_index}].adapter_payload"
        for element_index, raw_element in enumerate(elements):
            if not isinstance(raw_element, dict):
                continue
            element_type = str(raw_element.get("type") or "")
            if element_type in self.EDGE_TYPES:
                edge_candidates.append((element_index, raw_element))
                continue
            element_id = str(raw_element.get("id") or "").strip() or None
            element_ref = self._node_ref(raw_element, element_id)
            if not element_ref:
                continue
            linked_entity_raw = _custom_or_top_level(raw_element, "linkedEntityId")
            linked_entity_ref = _canonical_ref(linked_entity_raw)
            declared_ref = entity_ref_to_declared.get(linked_entity_ref) if linked_entity_ref else None
            if declared_ref:
                covered_entities.add(declared_ref)
            nodes[element_ref] = _TopologyNode(
                ref=element_ref,
                element_id=element_id,
                path=f"{diagram_path}.elements[{element_index}]",
                label=self._element_label(raw_element, element_id or element_ref),
                linked_entity_ref=declared_ref,
            )

        adjacency: dict[str, set[str]] = {node_ref: set() for node_ref in nodes}
        for element_index, raw_edge in edge_candidates:
            edge_path = f"{diagram_path}.elements[{element_index}]"
            source_id = _custom_or_top_level(raw_edge, "sourceElementId")
            target_id = _custom_or_top_level(raw_edge, "targetElementId")
            source_ref = _canonical_ref(source_id)
            target_ref = _canonical_ref(target_id)
            if not source_ref or not target_ref or source_ref not in nodes or target_ref not in nodes:
                element_id = str(raw_edge.get("id") or "").strip() or None
                warnings.append(
                    ArchitectureWarningRecord(
                        code="dangling_connector",
                        diagram_id=diagram_id,
                        diagram_type=diagram_type,
                        element_id=element_id,
                        path=edge_path,
                        message="Diagram connector does not resolve both sourceElementId and targetElementId to existing nodes.",
                        suggested_fix=TOPOLOGY_SUGGESTED_FIXES["dangling_connector"],
                    )
                )
                continue
            adjacency[source_ref].add(target_ref)
            adjacency[target_ref].add(source_ref)

        for node_ref, node in nodes.items():
            if node.linked_entity_ref and not adjacency[node_ref]:
                warnings.append(
                    ArchitectureWarningRecord(
                        code="isolated_entity_node",
                        diagram_id=diagram_id,
                        diagram_type=diagram_type,
                        element_id=node.element_id,
                        node_ref=None if node.element_id else node.ref,
                        path=node.path,
                        message=f"Diagram entity node '{node.label}' has no incident connector.",
                        suggested_fix=TOPOLOGY_SUGGESTED_FIXES["isolated_entity_node"].format(label=node.label),
                    )
                )

        connected_components = self._connected_components(adjacency)
        connected_components = [component for component in connected_components if any(adjacency[node_ref] for node_ref in component)]
        if len(connected_components) > 1:
            primary = connected_components[0]
            for component in connected_components:
                if component == primary:
                    continue
                focus_ref = sorted(component)[0]
                focus = nodes[focus_ref]
                warnings.append(
                    ArchitectureWarningRecord(
                        code="disconnected_subgraph",
                        diagram_id=diagram_id,
                        diagram_type=diagram_type,
                        element_id=focus.element_id,
                        node_ref=None if focus.element_id else focus.ref,
                        path=focus.path,
                        message=f"Diagram contains a disconnected subgraph with {len(component)} node(s).",
                        suggested_fix=TOPOLOGY_SUGGESTED_FIXES["disconnected_subgraph"],
                    )
                )

        return warnings, covered_entities

    def _apply_conceptual_justifications(
        self,
        diagram: dict[str, Any],
        diagram_index: int,
        warnings: list[ArchitectureWarningRecord],
    ) -> tuple[list[ArchitectureWarningRecord], list[ArchitectureWarningRecord], list[ArchitectureWarningRecord]]:
        raw_justifications = diagram.get("connectivity_justifications")
        if not isinstance(raw_justifications, dict) or not raw_justifications:
            return warnings, [], []

        valid_justifications: dict[str, str] = {}
        invalid_warnings: list[ArchitectureWarningRecord] = []
        for raw_key, raw_value in raw_justifications.items():
            key = str(raw_key or "").strip()
            path = f"diagrams[{diagram_index}].connectivity_justifications[{key!r}]"
            if not key:
                invalid_warnings.append(self._invalid_justification_warning(diagram, diagram_index, path, None, None))
                continue
            justification, invalid_reason = self._validate_connectivity_justification(raw_value)
            if invalid_reason:
                target_warning = self._warning_for_justification_key(warnings, key)
                invalid_warnings.append(
                    self._invalid_justification_warning(
                        diagram,
                        diagram_index,
                        path,
                        target_warning,
                        invalid_reason,
                    )
                )
                continue
            valid_justifications[_canonical_ref(key)] = justification

        if not self._is_conceptual_diagram(diagram):
            return warnings, [], invalid_warnings

        active_warnings: list[ArchitectureWarningRecord] = []
        suppressed_warnings: list[ArchitectureWarningRecord] = []
        for warning in warnings:
            if warning.code not in CONCEPTUAL_CONNECTIVITY_WARNING_CODES:
                active_warnings.append(warning)
                continue
            justification = self._matching_justification(warning, valid_justifications)
            if not justification:
                active_warnings.append(warning)
                continue
            suppressed_warnings.append(
                ArchitectureWarningRecord(
                    code=warning.code,
                    message=warning.message,
                    path=warning.path,
                    suggested_fix=warning.suggested_fix,
                    severity=warning.severity,
                    diagram_id=warning.diagram_id,
                    diagram_type=warning.diagram_type,
                    element_id=warning.element_id,
                    entity_id=warning.entity_id,
                    node_ref=warning.node_ref,
                    justification=justification,
                )
            )
        return active_warnings, suppressed_warnings, invalid_warnings

    def _invalid_justification_warning(
        self,
        diagram: dict[str, Any],
        diagram_index: int,
        path: str,
        target_warning: ArchitectureWarningRecord | None,
        reason: str | None,
    ) -> ArchitectureWarningRecord:
        reason_suffix = f" {reason}" if reason else ""
        return ArchitectureWarningRecord(
            code="conceptual_justification_invalid",
            diagram_id=str(diagram.get("id") or f"diagram[{diagram_index}]"),
            diagram_type=str(diagram.get("diagram_type") or ""),
            element_id=target_warning.element_id if target_warning else None,
            node_ref=(None if target_warning and target_warning.element_id else target_warning.node_ref) if target_warning else None,
            path=path,
            message=f"Conceptual connectivity justification is invalid.{reason_suffix}",
            suggested_fix=TOPOLOGY_SUGGESTED_FIXES["conceptual_justification_invalid"],
        )

    def _validate_connectivity_justification(self, value: Any) -> tuple[str, str | None]:
        justification = str(value or "").strip()
        if not justification:
            return "", "Provide a non-empty justification."
        length = len(justification)
        if length < CONCEPTUAL_JUSTIFICATION_MIN_CHARS:
            return justification, f"Use at least {CONCEPTUAL_JUSTIFICATION_MIN_CHARS} characters after trimming."
        if length > CONCEPTUAL_JUSTIFICATION_MAX_CHARS:
            return justification, f"Use at most {CONCEPTUAL_JUSTIFICATION_MAX_CHARS} characters after trimming."
        lowered = justification.casefold()
        compacted = re.sub(r"[\s/._-]+", "", lowered)
        if lowered in CONCEPTUAL_JUSTIFICATION_PLACEHOLDERS or compacted in {"todo", "tbd", "na", "none", "fixlater"}:
            return justification, "Replace placeholder text with a specific reason."
        if lowered.startswith(("todo", "tbd", "fix later")):
            return justification, "Replace placeholder text with a specific reason."
        return justification, None

    def _warning_for_justification_key(
        self,
        warnings: list[ArchitectureWarningRecord],
        key: str,
    ) -> ArchitectureWarningRecord | None:
        key_ref = _canonical_ref(key)
        for warning in warnings:
            if warning.element_id and _canonical_ref(warning.element_id) == key_ref:
                return warning
        for warning in warnings:
            if not warning.element_id and warning.node_ref and _canonical_ref(warning.node_ref) == key_ref:
                return warning
        return None

    def _matching_justification(
        self,
        warning: ArchitectureWarningRecord,
        valid_justifications: dict[str, str],
    ) -> str | None:
        if warning.element_id:
            return valid_justifications.get(_canonical_ref(warning.element_id))
        if warning.node_ref:
            return valid_justifications.get(_canonical_ref(warning.node_ref))
        return None

    def _is_conceptual_diagram(self, diagram: dict[str, Any]) -> bool:
        if diagram.get("is_conceptual") is True:
            return True
        diagram_type = _canonical_ref(diagram.get("diagram_type"))
        return "concept" in diagram_type

    def _declared_entities(self, entities: list[dict[str, Any]]) -> tuple[list[dict[str, str]], dict[str, str]]:
        declared: list[dict[str, str]] = []
        ref_to_declared: dict[str, str] = {}
        for index, entity in enumerate(entities):
            if not isinstance(entity, dict):
                continue
            entity_id = str(entity.get("id") or entity.get("name") or f"entities[{index}]").strip()
            entity_name = str(entity.get("name") or "").strip()
            declared_ref = _canonical_ref(entity.get("id") or entity.get("name") or entity_id)
            if not declared_ref:
                continue
            declared.append(
                {
                    "declared_ref": declared_ref,
                    "entity_id": entity_id,
                    "name": entity_name,
                    "path": f"entities[{index}]",
                }
            )
            for ref_value in (entity.get("id"), entity.get("name")):
                ref = _canonical_ref(ref_value)
                if ref:
                    ref_to_declared[ref] = declared_ref
        return declared, ref_to_declared

    def _connected_components(self, adjacency: dict[str, set[str]]) -> list[set[str]]:
        visited: set[str] = set()
        components: list[set[str]] = []
        for node_ref in adjacency:
            if node_ref in visited:
                continue
            stack = [node_ref]
            component: set[str] = set()
            while stack:
                current = stack.pop()
                if current in visited:
                    continue
                visited.add(current)
                component.add(current)
                stack.extend(sorted(adjacency[current] - visited, reverse=True))
            components.append(component)
        return components

    def _element_label(self, element: dict[str, Any], fallback: str) -> str:
        for key in ("text", "label", "name"):
            value = str(element.get(key) or "").strip()
            if value:
                return value.splitlines()[0].strip()
        return fallback

    def _node_ref(self, element: dict[str, Any], element_id: str | None) -> str:
        for value in (
            element_id,
            _custom_or_top_level(element, "node_ref"),
            _custom_or_top_level(element, "nodeRef"),
        ):
            ref = _canonical_ref(value)
            if ref:
                return ref
        return ""

    def _sort_warnings(self, warnings: list[ArchitectureWarningRecord]) -> list[ArchitectureWarningRecord]:
        return sorted(
            warnings,
            key=lambda warning: (
                warning.severity,
                warning.code,
                warning.diagram_id or "",
                warning.path,
                warning.element_id or "",
                warning.entity_id or "",
                warning.node_ref or "",
            ),
        )


def architecture_design_payload_schema() -> dict[str, Any]:
    """Machine-readable authoring contract for Architecture Design payloads."""
    return copy.deepcopy(
        {
            "schema_version": ARCHITECTURE_DESIGN_SCHEMA_VERSION,
            "allowed_values": {
                "parent_type": ["ideation", "refinement", "spec", "card"],
                "interface.direction": sorted(ALLOWED_INTERFACE_DIRECTIONS),
                "diagram.format": [SUPPORTED_ARCHITECTURE_DIAGRAM_FORMAT],
                "excalidraw.connectionType": sorted(ALLOWED_CONNECTION_TYPES),
                "topology_warning.code": sorted(TOPOLOGY_WARNING_CODES),
                "topology_warning.severity": ["warning"],
            },
            "topology_warning_contract": {
                "record_shape": {
                    "required": ["code", "severity", "message", "path", "suggested_fix"],
                    "target_fields": ["element_id", "entity_id", "node_ref"],
                    "target_rule": "At most one primary target field is set; node_ref is legacy/import fallback.",
                },
                "connectivity_justifications": {
                    "location": "diagram.connectivity_justifications",
                    "scope": "conceptual diagrams only",
                    "key_preference": "Use element_id keys; use node_ref only for legacy/imported diagrams without stable ids.",
                    "value_constraints": {
                        "min_trimmed_chars": CONCEPTUAL_JUSTIFICATION_MIN_CHARS,
                        "max_trimmed_chars": CONCEPTUAL_JUSTIFICATION_MAX_CHARS,
                        "placeholder_values": sorted(CONCEPTUAL_JUSTIFICATION_PLACEHOLDERS),
                    },
                    "suppressible_codes": sorted(CONCEPTUAL_CONNECTIVITY_WARNING_CODES),
                    "non_suppressible_codes": ["dangling_connector", "entity_without_diagram"],
                },
                "suggested_fix_templates": copy.deepcopy(TOPOLOGY_SUGGESTED_FIXES),
            },
            "root_contract": {
                "required": ["title", "global_description"],
                "recommended": ["entities", "interfaces", "diagrams"],
                "rules": [
                    "title should name the architecture slice being described",
                    "global_description should explain architecture intent, boundaries, responsibilities, and important constraints",
                    "entities, interfaces, and diagrams should be present whenever they reduce ambiguity for implementers or validators",
                    "diagram.format must be excalidraw_json; Mermaid, PlantUML, C4, SVG, and raw snippets belong only in descriptive text such as entity responsibility, boundaries, notes, or global_description",
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
                    "interfaces do not own source/target; diagram connections define endpoint entities through sourceElementId and targetElementId",
                    "participants is accepted only as optional legacy/derived metadata and must contain exactly two entity ids or names when provided",
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
                    "direction": "source_to_target",
                    "protocol": "REST",
                    "contract_type": "OpenAPI",
                    "request_schema": {"type": "object", "required": ["cart_id"]},
                    "response_schema": {"type": "object", "required": ["order_id"]},
                    "error_contract": {"400": "Invalid checkout payload", "409": "Duplicate idempotency key"},
                },
                "anti_patterns": [
                    {
                        "pattern": "interface owns source/target instead of using diagram connection endpoints",
                        "consequence": "the contract and diagram can drift; tasks may wire the wrong components",
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
            "semantic_node_registry": {
                "description": (
                    "Spec cc497a0d — canonical map from entity_type to "
                    "{displayType, architectureKind, iconName} applied to Excalidraw nodes "
                    "before persistence. Linked nodes (linkedEntityId set) must either provide "
                    "all four fields explicitly or rely on this registry to fill them safely."
                ),
                "required_node_fields_for_linked": list(REQUIRED_LINKED_NODE_FIELDS),
                "allowed_icon_names": sorted(ALLOWED_NODE_ICON_NAMES),
                "mappings": copy.deepcopy(SEMANTIC_NODE_REGISTRY),
                "rules": [
                    "Linked nodes (linkedEntityId set) must carry text, displayType, architectureKind, and iconName.",
                    "Missing fields are auto-filled deterministically when entity.entity_type is in the registry.",
                    "displayType divergence from the registry is rejected with suggested_fixes; align or change entity_type.",
                    "iconName outside the allowed set is rejected; remove the field to inherit the canonical icon.",
                    "When entity_type has no canonical mapping AND no explicit metadata is provided, payload is rejected.",
                ],
                "validation_flow_for_agents": [
                    "1. okto_pulse_get_architecture_design_schema — fetch this registry.",
                    "2. Build payload: prefer relying on registry by setting only linkedEntityId + entity_type.",
                    "3. POST /api/v1/architecture/validate — check valid=true, surface warnings to user.",
                    "4. add/update_architecture_design — backend re-applies normalization and rejection.",
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
                    "text": "Checkout API",
                    "displayType": "API",
                    "architectureKind": "service",
                    "iconName": "server",
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
                    "For conceptual diagrams, diagram.connectivity_justifications may suppress isolated_entity_node and disconnected_subgraph warnings only with valid explicit reasons.",
                    "Use linkedInterfaceIds for one or more contracts on the same connection; linkedInterfaceId remains accepted for legacy single-contract edges.",
                    "The source and target linkedEntityId values of the edge define the two endpoint entities for linked interfaces.",
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


class ArchitectureWarningAcknowledgementRequired(ValueError):
    """Raised when a valid architecture payload has active warnings without explicit acknowledgement."""

    def __init__(
        self,
        *,
        design_id: str,
        warning_keys: list[str],
        warnings: list[dict[str, Any]],
        reason: str = "architecture_warning_acknowledgement_required",
    ):
        self.reason = reason
        self.design_id = design_id
        self.warning_keys = warning_keys
        self.warnings = warnings
        super().__init__(reason)

    def to_payload(self) -> dict[str, Any]:
        return {
            "error": self.reason,
            "code": "architecture_warning_acknowledgement_required",
            "design_id": self.design_id,
            "warning_keys": self.warning_keys,
            "structured_warnings": self.warnings,
            "message": (
                "Architecture design has active warnings. Explicitly acknowledge the warning keys "
                "to save; acknowledgement is audit-only and does not clear the warnings."
            ),
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


def _semantic_registry_key(entity_type: Any) -> str:
    return re.sub(r"[\s\-]+", "_", str(entity_type or "").strip().lower())


def _resolve_semantic_canonical(registry_key: str) -> dict[str, str] | None:
    """Resolve the canonical semantic metadata for an entity_type.

    Exact match takes precedence. Falls back to family matching so
    specialized types (``database_table``, ``api_route``, ``worker_pool``) inherit
    their parent's canonical metadata when the suffix is just a refinement.
    """
    if not registry_key:
        return None
    canonical = SEMANTIC_NODE_REGISTRY.get(registry_key)
    if canonical is not None:
        return canonical
    # Family inheritance: <known>_<suffix> falls back to <known>.
    parts = registry_key.split("_")
    for cutoff in range(len(parts) - 1, 0, -1):
        candidate = "_".join(parts[:cutoff])
        if candidate in SEMANTIC_NODE_REGISTRY:
            return SEMANTIC_NODE_REGISTRY[candidate]
    return None


def normalize_excalidraw_semantics(
    payload: dict[str, Any],
    entities: list[dict[str, Any]],
    *,
    path_prefix: str = "diagrams[*].adapter_payload",
) -> tuple[dict[str, Any], list[str], list[str]]:
    """Apply the semantic node registry to a linked Excalidraw payload.

    For every node element with ``linkedEntityId`` pointing to an entity whose
    ``entity_type`` exists in ``SEMANTIC_NODE_REGISTRY``, deterministically fill
    the canonical ``displayType``/``architectureKind``/``iconName``/``text`` when
    missing. Reject the payload when metadata diverges from the registry, the
    icon is outside the allowlist, or the entity_type has no canonical mapping
    AND the node was supplied with only ``linkedEntityId``.

    Returns:
        Tuple ``(normalized_payload, warnings, issues)``.
          - ``normalized_payload``: deep-copied payload with safe fills applied.
          - ``warnings``: ``semantic_metadata_normalized`` entries (one per element with auto-fill).
          - ``issues``: blocking validation messages. When non-empty, the caller
            MUST reject the payload before persistence (FR3 / br_semantic_reject_ambiguous_payload).
    """
    if not isinstance(payload, dict):
        return payload, [], []
    elements = payload.get("elements")
    if not isinstance(elements, list):
        return payload, [], []

    entity_lookup: dict[str, dict[str, Any]] = {}
    for entity in entities or []:
        if not isinstance(entity, dict):
            continue
        ent_id = _canonical_ref(entity.get("id"))
        ent_name = _canonical_ref(entity.get("name"))
        if ent_id:
            entity_lookup[ent_id] = entity
        if ent_name:
            entity_lookup.setdefault(ent_name, entity)

    normalized_payload = copy.deepcopy(payload)
    normalized_elements = normalized_payload.get("elements") or []
    warnings: list[str] = []
    issues: list[str] = []

    for index, element in enumerate(normalized_elements):
        if not isinstance(element, dict):
            continue
        linked_entity_id = _custom_or_top_level(element, "linkedEntityId")
        # Skip edges/lines — semantic registry applies to entity-bound nodes only.
        if linked_entity_id in (None, "") or element.get("type") in ("arrow", "line"):
            continue
        entity = entity_lookup.get(_canonical_ref(linked_entity_id))
        if entity is None:
            # Dangling reference is already caught by _validate_excalidraw_links.
            continue
        entity_type_raw = str(entity.get("entity_type") or "").strip()
        registry_key = _semantic_registry_key(entity_type_raw)
        canonical = _resolve_semantic_canonical(registry_key)
        path = f"{path_prefix}.elements[{index}]"

        if canonical is None:
            has_explicit_metadata = any(
                str(element.get(field) or "").strip()
                for field in ("text", "displayType", "architectureKind", "iconName")
            )
            if not has_explicit_metadata:
                if entity_type_raw:
                    issues.append(
                        f"{path}.linkedEntityId='{linked_entity_id}' resolves to entity_type='{entity_type_raw}', "
                        f"but no canonical mapping exists in SEMANTIC_NODE_REGISTRY. Either set "
                        f"text/displayType/architectureKind/iconName explicitly, or rename the entity_type to one "
                        f"of: {sorted(SEMANTIC_NODE_REGISTRY.keys())}."
                    )
                else:
                    issues.append(
                        f"{path}.linkedEntityId='{linked_entity_id}' resolves to an entity without entity_type. "
                        f"Set the entity's entity_type so the diagram node can inherit semantic metadata."
                    )
            else:
                current_icon = element.get("iconName")
                if current_icon not in (None, "") and current_icon not in ALLOWED_NODE_ICON_NAMES:
                    issues.append(
                        f"{path}.iconName='{current_icon}' is not in the allowed icon set. "
                        f"Allowed: {sorted(ALLOWED_NODE_ICON_NAMES)}."
                    )
            continue

        normalized_anywhere = False
        normalized_fields: list[str] = []

        current_icon = element.get("iconName")
        if current_icon in (None, ""):
            element["iconName"] = canonical["iconName"]
            normalized_fields.append("iconName")
            normalized_anywhere = True
        elif current_icon not in ALLOWED_NODE_ICON_NAMES:
            issues.append(
                f"{path}.iconName='{current_icon}' is not in the allowed icon set. "
                f"Allowed: {sorted(ALLOWED_NODE_ICON_NAMES)}. Remove the field to inherit "
                f"'{canonical['iconName']}' from the registry, or pick an allowed name."
            )
            continue

        current_display = str(element.get("displayType") or "").strip()
        if not current_display:
            element["displayType"] = canonical["displayType"]
            normalized_fields.append("displayType")
            normalized_anywhere = True
        elif current_display.casefold() != canonical["displayType"].casefold():
            issues.append(
                f"{path}.displayType='{current_display}' diverges from entity_type='{entity_type_raw}' "
                f"(registry expects '{canonical['displayType']}'). Remove the field to inherit the canonical "
                f"value, align it to '{canonical['displayType']}', or change the entity's entity_type."
            )
            continue

        current_kind = str(element.get("architectureKind") or "").strip()
        if not current_kind:
            element["architectureKind"] = canonical["architectureKind"]
            normalized_fields.append("architectureKind")
            normalized_anywhere = True

        current_text = str(element.get("text") or "").strip()
        if not current_text:
            element["text"] = str(entity.get("name") or canonical["displayType"]).strip()
            normalized_fields.append("text")
            normalized_anywhere = True

        if normalized_anywhere:
            warnings.append(
                f"{path} semantic_metadata_normalized: filled {normalized_fields} from registry "
                f"for entity_type='{entity_type_raw}' (linkedEntityId='{linked_entity_id}')."
            )

    return normalized_payload, warnings, issues


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

    def __init__(self, db: Any):
        self.db = db

    async def save_payload(
        self,
        board_id: str,
        design_id: str,
        diagram_id: str,
        format: str,
        payload: Any,
    ) -> ArchitectureRecord:
        rows = await _architecture_list(
            self.db,
            "architecture_diagram_payload",
            filters=(
                _arf("design_id", "eq", design_id),
                _arf("diagram_id", "eq", diagram_id),
            ),
            limit=1,
        )
        row = rows[0] if rows else None
        content_hash = _hash_payload(payload)
        size_bytes = _payload_size(payload)
        payload_json = None if isinstance(payload, str) else payload
        payload_text = payload if isinstance(payload, str) else None
        if row is None:
            row = await _architecture_create(
                self.db,
                "architecture_diagram_payload",
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
        else:
            row.format = format
            row.adapter_payload_json = payload_json
            row.payload_text = payload_text
            row.content_hash = content_hash
            row.size_bytes = size_bytes
        await _architecture_flush(self.db)
        return row

    async def load_payload(self, ref: str) -> Any:
        row = await self._get_payload_row(ref)
        if row is None:
            raise KeyError(f"diagram payload not found: {ref}")
        return row.payload_text if row.payload_text is not None else row.adapter_payload_json

    async def delete_payload(self, ref: str) -> None:
        row = await self._get_payload_row(ref)
        if row is not None:
            await get_architecture_persistence_port().delete(self.db, row)

    async def copy_payload(
        self, source_ref: str, target_design_id: str, target_diagram_id: str
    ) -> ArchitectureRecord:
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

    async def _get_payload_row(self, ref: str) -> ArchitectureRecord | None:
        rows = await _architecture_list(
            self.db,
            "architecture_diagram_payload",
            any_filters=(
                _arf("id", "eq", ref),
                _arf("storage_key", "eq", ref),
            ),
            limit=1,
        )
        return rows[0] if rows else None


ARCHITECTURE_FINDING_ACTIVE = "active"
ARCHITECTURE_FINDING_RESOLVED = "resolved"
ARCHITECTURE_FINDING_SUPERSEDED = "superseded"


class ArchitectureFindingRunError(ValueError):
    """Base error for architecture finding run persistence."""


class ArchitectureInvalidDesignNotPersisted(ArchitectureFindingRunError):
    """Raised when a caller attempts to persist findings for an invalid design."""


class ArchitectureFindingKeyCollision(ArchitectureFindingRunError):
    """Raised when one critic run produces conflicting payloads for one key."""


def _warning_as_dict(warning: ArchitectureWarningRecord | dict[str, Any]) -> dict[str, Any]:
    if isinstance(warning, ArchitectureWarningRecord):
        return warning.to_dict()
    if isinstance(warning, dict):
        return dict(warning)
    raise TypeError(f"unsupported architecture warning type: {type(warning).__name__}")


def architecture_warning_target(warning: ArchitectureWarningRecord | dict[str, Any]) -> tuple[str, str, str]:
    """Return normalized target kind, target ref, and path for a warning.

    Priority is intentionally identical to TR3: element_id, then entity_id,
    then node_ref, then path. Path is required by ArchitectureWarningRecord and
    by the persisted finding contract.
    """

    data = _warning_as_dict(warning)
    path = str(data.get("path") or "").strip()
    if not path:
        raise ValueError("architecture warning path is required")
    for field_name, kind in (
        ("element_id", "element"),
        ("entity_id", "entity"),
        ("node_ref", "node"),
    ):
        value = data.get(field_name)
        if value not in (None, ""):
            return kind, str(value), path
    return "path", path, path


def stable_architecture_finding_key(
    design_id: str,
    warning: ArchitectureWarningRecord | dict[str, Any],
) -> str:
    """Compute the stable key that drives finding lifecycle transitions."""

    data = _warning_as_dict(warning)
    kind, target_ref, _path = architecture_warning_target(data)
    payload = {
        "design_id": design_id,
        "warning_code": str(data.get("code") or ""),
        "diagram_id": str(data.get("diagram_id") or ""),
        "target_ref": target_ref,
        "normalized_target_kind": kind,
    }
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


class ArchitectureFindingRunStore:
    """Persistence boundary for Architecture Design finding runs.

    The store consumes existing backend critic output. It never calls
    ``TopologyWarningEngine`` and never creates persisted error findings for
    invalid payloads.
    """

    def __init__(self, db: Any):
        self.db = db

    async def upsert_latest_run(
        self,
        *,
        board_id: str,
        design_id: str,
        design_version: int,
        critic_run_id: str,
        actor: dict[str, Any],
        validator_summary: dict[str, Any] | None,
        structured_warnings: list[ArchitectureWarningRecord | dict[str, Any]],
    ) -> dict[str, Any]:
        design = await _architecture_get(
            self.db,
            "architecture_design",
            design_id,
        )
        if design is None:
            raise ValueError(f"architecture design not found: {design_id}")
        if design.board_id != board_id:
            raise ValueError("architecture design does not belong to board")

        summary = dict(validator_summary or {})
        if summary.get("valid") is False or summary.get("issues"):
            raise ArchitectureInvalidDesignNotPersisted("invalid_design_not_persisted")

        await self._delete_existing_run(design_id, critic_run_id)

        previous_runs = await _architecture_list(
            self.db,
            "architecture_finding_run",
            filters=(
                _arf("design_id", "eq", design_id),
                _arf("is_current", "is_true"),
            ),
        )
        for run in previous_runs:
            run.is_current = False

        previous_active = await _architecture_list(
            self.db,
            "architecture_finding",
            filters=(
                _arf("design_id", "eq", design_id),
                _arf("lifecycle", "eq", ARCHITECTURE_FINDING_ACTIVE),
            ),
        )
        previous_by_key = {
            finding.finding_key: finding
            for finding in previous_active
        }

        warning_rows = self._dedupe_warnings(design_id, structured_warnings)
        new_keys = set(warning_rows)
        resolved_count = 0
        superseded_count = 0
        for key, finding in previous_by_key.items():
            if key in new_keys:
                finding.lifecycle = ARCHITECTURE_FINDING_SUPERSEDED
                superseded_count += 1
            else:
                finding.lifecycle = ARCHITECTURE_FINDING_RESOLVED
                resolved_count += 1

        run = await _architecture_create(
            self.db,
            "architecture_finding_run",
            board_id=board_id,
            design_id=design_id,
            design_version=design_version,
            critic_run_id=critic_run_id,
            is_current=True,
            active_count=len(warning_rows),
            resolved_count=resolved_count,
            superseded_count=superseded_count,
            validator_summary=summary,
            actor_type=str(actor.get("actor_type") or "user"),
            actor_id=str(actor.get("actor_id") or "system"),
            actor_name=actor.get("actor_name"),
        )
        findings: list[ArchitectureRecord] = []
        for key, warning in warning_rows.items():
            kind, target_ref, path = architecture_warning_target(warning)
            finding = await _architecture_create(
                self.db,
                "architecture_finding",
                run_id=run.id,
                board_id=board_id,
                design_id=design_id,
                design_version=design_version,
                critic_run_id=critic_run_id,
                finding_key=key,
                warning_code=str(warning.get("code") or "unknown"),
                severity=str(warning.get("severity") or "warning"),
                message=str(warning.get("message") or ""),
                suggested_fix=warning.get("suggested_fix"),
                path=path,
                target_ref=target_ref,
                normalized_target_kind=kind,
                diagram_id=warning.get("diagram_id"),
                diagram_type=warning.get("diagram_type"),
                lifecycle=ARCHITECTURE_FINDING_ACTIVE,
                raw_warning=warning,
            )
            findings.append(finding)
        await _architecture_flush(self.db)

        return {
            "design_id": design_id,
            "critic_run_id": critic_run_id,
            "active_count": len(findings),
            "resolved_count": resolved_count,
            "superseded_count": superseded_count,
            "findings": [self._finding_to_dict(finding) for finding in findings],
        }

    async def list_findings(
        self,
        *,
        design_id: str,
        lifecycle: str | None = None,
    ) -> list[ArchitectureRecord]:
        filters = [_arf("design_id", "eq", design_id)]
        if lifecycle is not None:
            filters.append(_arf("lifecycle", "eq", lifecycle))
        return await _architecture_list(
            self.db,
            "architecture_finding",
            filters=tuple(filters),
            order_by=(("created_at", False), ("finding_key", False)),
        )

    async def get_current_run(self, design_id: str) -> ArchitectureRecord | None:
        rows = await _architecture_list(
            self.db,
            "architecture_finding_run",
            filters=(
                _arf("design_id", "eq", design_id),
                _arf("is_current", "is_true"),
            ),
            order_by=(("created_at", True),),
            limit=1,
        )
        return rows[0] if rows else None

    async def record_acknowledgements(
        self,
        *,
        board_id: str,
        design_id: str,
        critic_run_id: str,
        finding_keys: list[str],
        actor: dict[str, Any],
        statement: str | None = None,
    ) -> list[ArchitectureRecord]:
        run = await self._get_run(design_id, critic_run_id)
        if run is None:
            raise ValueError(f"architecture finding run not found: {critic_run_id}")
        active = await self.list_findings(design_id=design_id, lifecycle=ARCHITECTURE_FINDING_ACTIVE)
        active_keys = {finding.finding_key for finding in active if finding.critic_run_id == critic_run_id}
        missing = sorted(set(finding_keys) - active_keys)
        if missing:
            raise ValueError(f"warning acknowledgement references unknown active findings: {missing}")

        acknowledgements: list[ArchitectureRecord] = []
        for finding_key in finding_keys:
            rows = await _architecture_list(
                self.db,
                "architecture_warning_acknowledgement",
                filters=(
                    _arf("design_id", "eq", design_id),
                    _arf("critic_run_id", "eq", critic_run_id),
                    _arf("finding_key", "eq", finding_key),
                    _arf(
                        "actor_id",
                        "eq",
                        str(actor.get("actor_id") or "system"),
                    ),
                ),
                limit=1,
            )
            row = rows[0] if rows else None
            if row is None:
                row = await _architecture_create(
                    self.db,
                    "architecture_warning_acknowledgement",
                    board_id=board_id,
                    design_id=design_id,
                    design_version=run.design_version,
                    critic_run_id=critic_run_id,
                    finding_key=finding_key,
                    actor_type=str(actor.get("actor_type") or "user"),
                    actor_id=str(actor.get("actor_id") or "system"),
                    actor_name=actor.get("actor_name"),
                    statement=statement,
                )
            else:
                row.actor_type = str(actor.get("actor_type") or row.actor_type)
                row.actor_name = actor.get("actor_name")
                row.statement = statement
            acknowledgements.append(row)
        await _architecture_flush(self.db)
        return acknowledgements

    async def _delete_existing_run(self, design_id: str, critic_run_id: str) -> None:
        existing = await self._get_run(design_id, critic_run_id)
        if existing is not None:
            await get_architecture_persistence_port().delete(self.db, existing)

    async def _get_run(
        self, design_id: str, critic_run_id: str
    ) -> ArchitectureRecord | None:
        rows = await _architecture_list(
            self.db,
            "architecture_finding_run",
            filters=(
                _arf("design_id", "eq", design_id),
                _arf("critic_run_id", "eq", critic_run_id),
            ),
            limit=1,
        )
        return rows[0] if rows else None

    def _dedupe_warnings(
        self,
        design_id: str,
        structured_warnings: list[ArchitectureWarningRecord | dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        by_key: dict[str, dict[str, Any]] = {}
        fingerprints: dict[str, str] = {}
        for warning in structured_warnings:
            data = _warning_as_dict(warning)
            key = stable_architecture_finding_key(design_id, data)
            fingerprint = _hash_payload(data)
            if key in by_key:
                if fingerprints[key] != fingerprint:
                    raise ArchitectureFindingKeyCollision("finding_key_collision")
                continue
            by_key[key] = data
            fingerprints[key] = fingerprint
        return by_key

    def _finding_to_dict(self, finding: ArchitectureRecord) -> dict[str, Any]:
        return {
            "finding_key": finding.finding_key,
            "code": finding.warning_code,
            "severity": finding.severity,
            "lifecycle": finding.lifecycle,
            "normalized_target_kind": finding.normalized_target_kind,
            "target_ref": finding.target_ref,
            "path": finding.path,
            "message": finding.message,
            "suggested_fix": finding.suggested_fix,
            "diagram_id": finding.diagram_id,
            "diagram_type": finding.diagram_type,
        }


async def backfill_architecture_finding_runs(
    db: Any,
    *,
    board_id: str | None = None,
    only_missing: bool = False,
) -> dict[str, int]:
    """Materializa finding runs para designs existentes (self-heal).

    Investigação 2026-06-10: o AFG avalia FINDINGS PERSISTIDOS, mas eles só
    nascem num save pós-AFG — 83% dos designs do board 0.2.3 nunca tiveram
    um run (criados antes da feature ou nunca re-salvos), então a tabela
    estava vazia e o gate nunca bloqueou nada. Este sweep re-avalia os
    designs com os payloads RE-HIDRATADOS (o segundo furo: refs
    externalizadas eram puladas pelo engine) e grava o run atual de cada
    um. Idempotente: ``upsert_latest_run`` substitui o run corrente.

    ``only_missing=True`` restringe a designs sem nenhum run (sweep de boot
    barato); o default re-avalia tudo (reconcilia runs imprecisos gravados
    antes do fix da re-hidratação).
    """
    repo = ArchitectureDesignRepository(db)
    filters: tuple[ArchitectureFilter, ...] = ()
    if board_id:
        filters = (_arf("board_id", "eq", board_id),)
    designs = await _architecture_list(
        db,
        "architecture_design",
        filters=filters,
    )

    stats = {"designs": 0, "skipped": 0, "with_findings": 0, "findings": 0, "errors": 0}
    for design in designs:
        if only_missing:
            existing = await _architecture_list(
                db,
                "architecture_finding_run",
                filters=(_arf("design_id", "eq", design.id),),
                limit=1,
            )
            if existing:
                stats["skipped"] += 1
                continue
        try:
            diagrams = await repo._diagrams_for_validation(design.diagrams or [])
            critique = repo.critique_payload(
                {
                    "title": design.title,
                    "global_description": design.global_description,
                    "entities": design.entities or [],
                    "interfaces": design.interfaces or [],
                    "diagrams": diagrams,
                }
            )
            if critique.get("valid") is False:
                # Designs com payload inválido nunca persistem run (contrato
                # da store) — ficam para correção manual.
                stats["skipped"] += 1
                continue
            warnings = repo._structured_warnings_with_keys(
                design.id, list(critique.get("structured_warnings") or [])
            )
            store = ArchitectureFindingRunStore(db)
            await store.upsert_latest_run(
                board_id=design.board_id,
                design_id=design.id,
                design_version=design.version,
                critic_run_id=(
                    f"archcrit-backfill:{design.id}:v{design.version}:"
                    f"{uuid.uuid4().hex[:8]}"
                ),
                actor={
                    "actor_type": "system",
                    "actor_id": "architecture-finding-backfill",
                    "actor_name": "Architecture finding backfill",
                },
                validator_summary={
                    "valid": True,
                    "issues": [],
                    "warnings_count": len(critique.get("warnings") or []),
                    "structured_warnings_count": len(warnings),
                    "suppressed_warnings_count": len(
                        critique.get("suppressed_warnings") or []
                    ),
                    "summary": critique.get("summary") or {},
                },
                structured_warnings=warnings,
            )
            stats["designs"] += 1
            if warnings:
                stats["with_findings"] += 1
                stats["findings"] += len(warnings)
        except Exception:
            logger.exception(
                "architecture.finding_backfill.design_failed design_id=%s",
                design.id,
            )
            stats["errors"] += 1
    await get_architecture_persistence_port().commit(db)
    return stats



@dataclass(frozen=True)
class ArchitectureFindingGate:
    """Evaluate persisted architecture findings for SDLC completion gates.

    The gate consumes Resource Gate-resolved Architecture Design references and
    the persisted finding lifecycle. It intentionally never re-runs topology
    analysis; the backend architecture critic remains the source of truth.
    """

    db: Any

    async def evaluate(
        self,
        *,
        board_id: str,
        owner_type: str,
        owner_id: str,
        architecture_refs: list[dict[str, Any]],
    ) -> dict[str, Any]:
        started = perf_counter()
        store = ArchitectureFindingRunStore(self.db)
        active: list[dict[str, Any]] = []
        resolved_count = 0
        superseded_count = 0
        by_design: list[dict[str, Any]] = []
        seen_design_ids: set[str] = set()

        for ref in architecture_refs:
            design_id = str(ref.get("id") or "").strip()
            if not design_id or design_id in seen_design_ids:
                continue
            seen_design_ids.add(design_id)

            findings = await store.list_findings(design_id=design_id)
            active_findings = [
                finding for finding in findings
                if finding.lifecycle == ARCHITECTURE_FINDING_ACTIVE
            ]
            resolved_findings = [
                finding for finding in findings
                if finding.lifecycle == ARCHITECTURE_FINDING_RESOLVED
            ]
            superseded_findings = [
                finding for finding in findings
                if finding.lifecycle == ARCHITECTURE_FINDING_SUPERSEDED
            ]
            resolved_count += len(resolved_findings)
            superseded_count += len(superseded_findings)

            by_design.append({
                "design_id": design_id,
                "title": ref.get("title"),
                "source_entity_type": ref.get("source_entity_type"),
                "source_entity_id": ref.get("source_entity_id"),
                "source_entity_title": ref.get("source_entity_title"),
                "active_count": len(active_findings),
                "resolved_count": len(resolved_findings),
                "superseded_count": len(superseded_findings),
            })
            active.extend(
                self._finding_to_remediation(finding, ref)
                for finding in active_findings
            )

        by_code = dict(Counter(item["code"] for item in active))
        projection = {
            "owner_type": owner_type,
            "owner_id": owner_id,
            "design_count": len(seen_design_ids),
            "active_count": len(active),
            "resolved_count": resolved_count,
            "superseded_count": superseded_count,
            "by_code": by_code,
            "by_design": by_design,
            "top_remediation": active[:10],
            "blocking": bool(active),
            "remediation": (
                "Resolve active Architecture Design findings by updating the "
                "diagram/design until the backend architecture critic no longer "
                "emits those warning keys. Save acknowledgement is audit-only "
                "and does not bypass Done."
                if active else None
            ),
        }
        outcome = "active" if active else "clear"
        duration_ms = (perf_counter() - started) * 1000
        observe_architecture_gate_eval(
            board_id=board_id,
            owner_type=owner_type,
            duration_ms=duration_ms,
            active_count=len(active),
            design_count=len(seen_design_ids),
            outcome=outcome,
        )
        observe_architecture_projection(
            board_id=board_id,
            owner_type=owner_type,
            active_count=len(active),
            design_count=len(seen_design_ids),
            outcome=outcome,
        )
        return {
            "allowed": not active,
            "board_id": board_id,
            "owner_type": owner_type,
            "owner_id": owner_id,
            "architecture_findings": projection,
        }

    @staticmethod
    def _finding_to_remediation(
        finding: ArchitectureRecord,
        ref: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "design_id": finding.design_id,
            "design_title": ref.get("title"),
            "source_entity_type": ref.get("source_entity_type"),
            "source_entity_id": ref.get("source_entity_id"),
            "source_entity_title": ref.get("source_entity_title"),
            "finding_key": finding.finding_key,
            "critic_run_id": finding.critic_run_id,
            "design_version": finding.design_version,
            "code": finding.warning_code,
            "severity": finding.severity,
            "message": finding.message,
            "normalized_target_kind": finding.normalized_target_kind,
            "target_ref": finding.target_ref,
            "path": finding.path,
            "suggested_fix": finding.suggested_fix,
            "diagram_id": finding.diagram_id,
            "diagram_type": finding.diagram_type,
            "remediation": (
                "Update the Architecture Design so the backend critic resolves "
                "this warning; acknowledgement records are audit-only."
            ),
        }


ARCHITECTURE_PROPAGATION_BLOCKED_CODE = "architecture_propagation_blocked"

# Canonical verdict-status taxonomy for propagation eligibility (Spec A).
PROPAGATION_VERDICT_CURRENT = "current"          # persisted run trusted as-is
PROPAGATION_VERDICT_REVALIDATED = "revalidated"  # persisted run missing/stale -> critic re-run
PROPAGATION_VERDICT_UNAVAILABLE = "unavailable"  # fail-closed: could not decide

# Why a revalidation was triggered (audit detail; None when the persisted verdict is trusted).
PROPAGATION_REVALIDATION_MISSING_RUN = "missing_run"
PROPAGATION_REVALIDATION_VERSION_INCOMPATIBLE = "version_incompatible"
PROPAGATION_REVALIDATION_VERDICT_UNLOADABLE = "verdict_unloadable"


@dataclass(frozen=True)
class ArchitecturePropagationEligibility:
    """Canonical verdict on whether an Architecture Design source may be copied/propagated.

    This is the single serializable contract consumed by every copy/propagation call
    site (wiring is Spec B). It deliberately mirrors the ``ArchitectureFindingGate``
    classification so propagation eligibility and the Architecture Finding Done Gate
    (AFG.01) stay in lockstep: an active finding that blocks Done also blocks copy.
    ``ArchitectureWarningAcknowledgement`` rows are surfaced as audit evidence only and
    never flip ``eligible`` (TR fe6f1935 / db.py:1482).
    """

    design_id: str
    eligible: bool
    verdict_status: str
    source_design_id: str | None
    source_ref: str | None
    source_version: int | None
    parent_type: str | None
    parent_id: str | None
    critic_run_id: str | None
    design_version: int | None
    issues: list[str]
    blocking_warnings: list[dict[str, Any]]
    finding_keys: list[str]
    non_blocking: dict[str, Any]
    acknowledgements_audit: list[dict[str, Any]]
    revalidation_reason: str | None
    remediation: str | None

    @property
    def blocker_counts(self) -> dict[str, int]:
        return {
            "issues": len(self.issues),
            "blocking_warnings": len(self.blocking_warnings),
            "total": len(self.issues) + len(self.blocking_warnings),
        }

    def to_dict(self) -> dict[str, Any]:
        """Stable JSON payload for REST, MCP, and internal services (TR f106b97f).

        Materializes the canonical error/verdict contract — every field the Spec A
        refinement requires for automation (FR b64537ee), not free text.
        """
        return {
            "error": ARCHITECTURE_PROPAGATION_BLOCKED_CODE,
            "code": ARCHITECTURE_PROPAGATION_BLOCKED_CODE,
            "eligible": self.eligible,
            "design_id": self.design_id,
            "source_design_id": self.source_design_id,
            "source_ref": self.source_ref,
            "source_version": self.source_version,
            "parent_source": {"parent_type": self.parent_type, "parent_id": self.parent_id},
            "critic_run_id": self.critic_run_id,
            "design_version": self.design_version,
            "verdict_status": self.verdict_status,
            "revalidation_reason": self.revalidation_reason,
            "blocker_counts": self.blocker_counts,
            "finding_keys": list(self.finding_keys),
            "issues": list(self.issues),
            "warnings": [dict(warning) for warning in self.blocking_warnings],
            "non_blocking": dict(self.non_blocking),
            "acknowledgements_audit": [dict(ack) for ack in self.acknowledgements_audit],
            "remediation": self.remediation,
        }


class ArchitecturePropagationBlocked(ValueError):
    """Raised when a source Architecture Design is ineligible for copy/propagation.

    Carries the full :class:`ArchitecturePropagationEligibility` verdict so callers
    (REST/MCP/services, wired in Spec B) surface a structured, serializable error
    without re-deriving the reason from free text.
    """

    def __init__(self, eligibility: ArchitecturePropagationEligibility):
        self.eligibility = eligibility
        super().__init__(ARCHITECTURE_PROPAGATION_BLOCKED_CODE)

    def to_payload(self) -> dict[str, Any]:
        return self.eligibility.to_dict()

    # Alias for parity with ResourcePropagationError/ResourceLineageResolutionError,
    # whose REST/MCP handlers call ``.to_error_dict``.
    def to_error_dict(self) -> dict[str, Any]:
        return self.eligibility.to_dict()


def _propagation_remediation(
    *, blocked: bool, verdict_status: str, finding_keys: list[str]
) -> str | None:
    if not blocked:
        return None
    if verdict_status == PROPAGATION_VERDICT_UNAVAILABLE:
        return (
            "Propagation is fail-closed: the source Architecture Design, its diagram "
            "payload, or its persisted critic verdict could not be loaded. Restore the "
            "source so the canonical eligibility policy can re-evaluate before copying."
        )
    keys = ", ".join(sorted(finding_keys)) if finding_keys else "(see issues)"
    return (
        "Resolve the blocking architecture findings on the SOURCE design until the "
        "backend critic no longer emits them, then retry. Acknowledgement is audit-only "
        f"and does not authorize propagation. Blocking finding keys: {keys}."
    )


def build_propagation_eligibility(
    *,
    design_id: str,
    source_design_id: str | None,
    source_ref: str | None,
    source_version: int | None,
    parent_type: str | None,
    parent_id: str | None,
    design_version: int | None,
    critic_run_id: str | None,
    verdict_status: str,
    revalidation_reason: str | None,
    issues: list[str],
    blocking_warnings: list[dict[str, Any]],
    suppressed_warnings: list[dict[str, Any]] | None = None,
    resolved_count: int = 0,
    superseded_count: int = 0,
    acknowledgements_audit: list[dict[str, Any]] | None = None,
) -> ArchitecturePropagationEligibility:
    """Pure constructor for the canonical eligibility verdict.

    Centralizes the blocking/non-blocking taxonomy (FR cb209602 / 8f1a6f4c) so it is
    unit-testable without a database: ``issues`` and ``blocking_warnings`` block;
    ``suppressed_warnings`` and resolved/superseded findings are recorded as
    non-blocking and never flip ``eligible``. A ``verdict_status`` of ``unavailable``
    is always fail-closed (ineligible) regardless of counts (TR e4fa4314).
    """

    suppressed = list(suppressed_warnings or [])
    acks = list(acknowledgements_audit or [])
    fail_closed = verdict_status == PROPAGATION_VERDICT_UNAVAILABLE
    eligible = not fail_closed and not issues and not blocking_warnings
    finding_keys = sorted(
        {
            str(warning.get("finding_key"))
            for warning in blocking_warnings
            if warning.get("finding_key")
        }
    )
    non_blocking = {
        "suppressed_warnings": suppressed,
        "suppressed_warnings_count": len(suppressed),
        "resolved_findings_count": int(resolved_count),
        "superseded_findings_count": int(superseded_count),
    }
    remediation = _propagation_remediation(
        blocked=not eligible,
        verdict_status=verdict_status,
        finding_keys=finding_keys,
    )
    return ArchitecturePropagationEligibility(
        design_id=design_id,
        eligible=eligible,
        verdict_status=verdict_status,
        source_design_id=source_design_id,
        source_ref=source_ref,
        source_version=source_version,
        parent_type=parent_type,
        parent_id=parent_id,
        critic_run_id=critic_run_id,
        design_version=design_version,
        issues=list(issues),
        blocking_warnings=[dict(warning) for warning in blocking_warnings],
        finding_keys=finding_keys,
        non_blocking=non_blocking,
        acknowledgements_audit=[dict(ack) for ack in acks],
        revalidation_reason=revalidation_reason,
        remediation=remediation,
    )


@dataclass(frozen=True)
class ArchitecturePropagationEligibilityPolicy:
    """Canonical policy that decides whether an Architecture Design may be propagated.

    Source of truth (Decision A): the persisted ``ArchitectureFindingRun`` verdict,
    revalidated deterministically ONLY when it is missing, version-incompatible, or
    unloadable (FR b7fe702a). Revalidation re-runs the deterministic critic IN MEMORY
    and never mutates persisted state, so an eligibility check is side-effect free and
    the fail-closed paths (TR e4fa4314 / AC 2c06ae77) cannot leave partial writes.

    Spec A scope: this class only DECIDES. Wiring it into copy/propagation call sites
    (ArchitecturePropagationService, REST, MCP, auto-derive) is Spec B.
    """

    db: Any

    async def evaluate(self, design_id: str) -> ArchitecturePropagationEligibility:
        """Return the canonical eligibility verdict for a source design (never mutates)."""
        try:
            repo = ArchitectureDesignRepository(self.db)
            design = await repo.get(design_id)
            if design is None:
                return self._unavailable(design_id, reason="source_design_not_found")
            store = ArchitectureFindingRunStore(self.db)
            run = await store.get_current_run(design_id)
            reason = self._revalidation_reason(design, run)
            if reason is None:
                return await self._evaluate_persisted(design, run, store)
            return await self._evaluate_revalidated(design, run, repo, store, reason)
        except Exception:  # noqa: BLE001 - fail-closed on any load/critic failure
            logger.exception(
                "architecture.propagation_eligibility.failed design_id=%s", design_id
            )
            return self._unavailable(design_id, reason="evaluation_error")

    async def require_eligible(self, design_id: str) -> ArchitecturePropagationEligibility:
        """Raise :class:`ArchitecturePropagationBlocked` unless the source is eligible."""
        eligibility = await self.evaluate(design_id)
        if not eligibility.eligible:
            raise ArchitecturePropagationBlocked(eligibility)
        return eligibility

    @staticmethod
    def _revalidation_reason(
        design: ArchitectureRecord,
        run: ArchitectureRecord | None,
    ) -> str | None:
        if run is None:
            return PROPAGATION_REVALIDATION_MISSING_RUN
        if run.design_version != design.version:
            return PROPAGATION_REVALIDATION_VERSION_INCOMPATIBLE
        summary = run.validator_summary
        if not isinstance(summary, dict) or summary.get("valid") is False or summary.get("issues"):
            return PROPAGATION_REVALIDATION_VERDICT_UNLOADABLE
        return None

    async def _evaluate_persisted(
        self,
        design: ArchitectureRecord,
        run: ArchitectureRecord,
        store: ArchitectureFindingRunStore,
    ) -> ArchitecturePropagationEligibility:
        findings = await store.list_findings(design_id=design.id)
        active = [f for f in findings if f.lifecycle == ARCHITECTURE_FINDING_ACTIVE]
        resolved = sum(1 for f in findings if f.lifecycle == ARCHITECTURE_FINDING_RESOLVED)
        superseded = sum(1 for f in findings if f.lifecycle == ARCHITECTURE_FINDING_SUPERSEDED)
        blocking_warnings = [self._finding_payload(finding) for finding in active]
        acks = await self._acknowledgements_audit(design.id, run.critic_run_id)
        return build_propagation_eligibility(
            design_id=design.id,
            source_design_id=design.source_design_id,
            source_ref=design.source_ref,
            source_version=design.source_version,
            parent_type=design.parent_type,
            parent_id=design.parent_id,
            design_version=design.version,
            critic_run_id=run.critic_run_id,
            verdict_status=PROPAGATION_VERDICT_CURRENT,
            revalidation_reason=None,
            issues=[],
            blocking_warnings=blocking_warnings,
            suppressed_warnings=[],
            resolved_count=resolved,
            superseded_count=superseded,
            acknowledgements_audit=acks,
        )

    async def _evaluate_revalidated(
        self,
        design: ArchitectureRecord,
        run: ArchitectureRecord | None,
        repo: ArchitectureDesignRepository,
        store: ArchitectureFindingRunStore,
        reason: str,
    ) -> ArchitecturePropagationEligibility:
        diagrams = await repo._diagrams_for_validation(list(design.diagrams or []))
        critique = repo.critique_payload(
            {
                "title": design.title,
                "global_description": design.global_description,
                "entities": design.entities or [],
                "interfaces": design.interfaces or [],
                "diagrams": diagrams,
            }
        )
        issues = list(critique.get("issues") or [])
        blocking_warnings = repo._structured_warnings_with_keys(
            design.id, list(critique.get("structured_warnings") or [])
        )
        resolved = superseded = 0
        if run is not None:
            findings = await store.list_findings(design_id=design.id)
            resolved = sum(1 for f in findings if f.lifecycle == ARCHITECTURE_FINDING_RESOLVED)
            superseded = sum(1 for f in findings if f.lifecycle == ARCHITECTURE_FINDING_SUPERSEDED)
        acks = await self._acknowledgements_audit(
            design.id, run.critic_run_id if run is not None else None
        )
        return build_propagation_eligibility(
            design_id=design.id,
            source_design_id=design.source_design_id,
            source_ref=design.source_ref,
            source_version=design.source_version,
            parent_type=design.parent_type,
            parent_id=design.parent_id,
            design_version=design.version,
            critic_run_id=run.critic_run_id if run is not None else None,
            verdict_status=PROPAGATION_VERDICT_REVALIDATED,
            revalidation_reason=reason,
            issues=issues,
            blocking_warnings=blocking_warnings,
            suppressed_warnings=list(critique.get("suppressed_warnings") or []),
            resolved_count=resolved,
            superseded_count=superseded,
            acknowledgements_audit=acks,
        )

    def _unavailable(
        self, design_id: str, *, reason: str
    ) -> ArchitecturePropagationEligibility:
        return build_propagation_eligibility(
            design_id=design_id,
            source_design_id=None,
            source_ref=None,
            source_version=None,
            parent_type=None,
            parent_id=None,
            design_version=None,
            critic_run_id=None,
            verdict_status=PROPAGATION_VERDICT_UNAVAILABLE,
            revalidation_reason=reason,
            issues=[f"architecture propagation eligibility could not be evaluated: {reason}"],
            blocking_warnings=[],
        )

    async def _acknowledgements_audit(
        self, design_id: str, critic_run_id: str | None
    ) -> list[dict[str, Any]]:
        filters = [_arf("design_id", "eq", design_id)]
        if critic_run_id:
            filters.append(_arf("critic_run_id", "eq", critic_run_id))
        rows = await _architecture_list(
            self.db,
            "architecture_warning_acknowledgement",
            filters=tuple(filters),
        )
        return [
            {
                "finding_key": ack.finding_key,
                "critic_run_id": ack.critic_run_id,
                "design_version": ack.design_version,
                "actor_id": ack.actor_id,
                "actor_type": ack.actor_type,
                "statement": ack.statement,
                "audit_only": True,
            }
            for ack in rows
        ]

    @staticmethod
    def _finding_payload(finding: ArchitectureRecord) -> dict[str, Any]:
        return {
            "finding_key": finding.finding_key,
            "code": finding.warning_code,
            "severity": finding.severity,
            "lifecycle": finding.lifecycle,
            "message": finding.message,
            "normalized_target_kind": finding.normalized_target_kind,
            "target_ref": finding.target_ref,
            "path": finding.path,
            "diagram_id": finding.diagram_id,
            "diagram_type": finding.diagram_type,
            "critic_run_id": finding.critic_run_id,
            "design_version": finding.design_version,
        }


class ArchitectureDesignRepository:
    """Repository for Architecture Design envelopes and versions."""

    def __init__(
        self,
        db: Any,
        *,
        diagram_store: ArchitectureDiagramStore | None = None,
        adapter_registry: ArchitectureDiagramAdapterRegistry | None = None,
    ):
        self.db = db
        self.diagram_store = diagram_store or ArchitectureDiagramStore(db)
        self.adapter_registry = adapter_registry or ArchitectureDiagramAdapterRegistry()

    async def list(
        self,
        parent_type: str,
        parent_id: str,
        include_payloads: bool = False,
    ) -> list[ArchitectureRecord]:
        _, parent_field = self._parent_config(parent_type)
        designs = await _architecture_list(
            self.db,
            "architecture_design",
            filters=(
                _arf("parent_type", "eq", parent_type),
                _arf(parent_field, "eq", parent_id),
            ),
            order_by=(("updated_at", True), ("title", False)),
        )
        if include_payloads:
            for design in designs:
                await self._attach_payloads(design)
        return designs

    async def get(
        self, design_id: str, include_payloads: bool = False
    ) -> ArchitectureRecord | None:
        design = await _architecture_get(
            self.db,
            "architecture_design",
            design_id,
        )
        if design is not None and include_payloads:
            await self._attach_payloads(design)
        return design

    async def create(
        self,
        parent_type: str,
        parent_id: str,
        data: ArchitectureDesignCreate | dict[str, Any],
        actor_id: str,
        *,
        allow_card_parent_write: bool = False,
    ) -> ArchitectureRecord:
        _ensure_card_architecture_write_allowed(
            parent_type,
            allow=allow_card_parent_write,
        )
        parent_entity, parent_field = self._parent_config(parent_type)
        parent = await _architecture_get(self.db, parent_entity, parent_id)
        if parent is None:
            raise ValueError(f"{parent_type} parent not found: {parent_id}")
        payload = _dump_model_or_dict(data)
        acknowledgement = payload.pop("architecture_warning_acknowledgement", None)
        design_id = str(uuid.uuid4())
        board_id = getattr(parent, "board_id")
        # Hidrata uma CÓPIA para o critique (refs externalizadas → payload),
        # preservando o payload original para a persistência normal.
        critique_input = dict(payload)
        critique_input["diagrams"] = await self._diagrams_for_validation(
            list(payload.get("diagrams") or [])
        )
        critique = self._critique_for_save(
            design_id=design_id,
            board_id=board_id,
            entity_type=parent_type,
            payload=critique_input,
            acknowledgement=acknowledgement,
        )
        design = await _architecture_create(
            self.db,
            "architecture_design",
            id=design_id,
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
        design.diagrams = await self._normalize_diagrams(
            board_id,
            design.id,
            payload.get("diagrams") or [],
            entities=payload.get("entities") or [],
        )
        await _architecture_flush(self.db)
        await self.snapshot(design.id, actor_id, "Initial architecture design")
        await self._persist_finding_run(
            design=design,
            critique=critique,
            actor_id=actor_id,
            acknowledgement=acknowledgement,
        )
        await self._publish_parent_semantic_change(design, actor_id)
        await _architecture_flush(self.db)
        await get_architecture_persistence_port().refresh(self.db, design)
        if parent_type == "spec":
            await self._propagate_spec_architecture_change(
                design,
                actor_id,
                trigger="spec_architecture_created",
            )
        return design

    async def update(
        self,
        design_id: str,
        patch: ArchitectureDesignUpdate | dict[str, Any],
        actor_id: str,
        *,
        allow_card_parent_write: bool = False,
    ) -> ArchitectureRecord:
        design = await self.get(design_id)
        if design is None:
            raise ValueError(f"architecture design not found: {design_id}")
        _ensure_card_architecture_write_allowed(
            design.parent_type,
            allow=allow_card_parent_write,
        )
        payload = _dump_model_or_dict(patch, exclude_unset=True)
        change_summary = payload.pop("change_summary", None)
        acknowledgement = payload.pop("architecture_warning_acknowledgement", None)
        semantic_change = bool(SEMANTIC_PATCH_FIELDS & payload.keys())
        # Re-hidrata SEMPRE (investigação 2026-06-10): um patch com
        # ``diagrams`` carregando ``adapter_payload_ref`` sem o payload
        # inline (ex.: import_excalidraw re-anexa os diagramas existentes
        # crus) fazia o TopologyWarningEngine pular esses diagramas
        # silenciosamente — entidades cobertas pareciam órfãs (falso
        # entity_without_diagram) e warnings reais de dentro deles se
        # perdiam. O helper só carrega quando há ref sem payload, então o
        # caso inline permanece intocado.
        candidate_diagrams = await self._diagrams_for_validation(
            payload["diagrams"] if "diagrams" in payload else (design.diagrams or [])
        )
        candidate_payload = {
            "title": payload.get("title", design.title),
            "global_description": payload.get("global_description", design.global_description),
            "entities": payload.get("entities", design.entities or []),
            "interfaces": payload.get("interfaces", design.interfaces or []),
            "diagrams": candidate_diagrams,
        }
        critique = self._critique_for_save(
            design_id=design.id,
            board_id=design.board_id,
            entity_type=design.parent_type,
            payload=candidate_payload,
            acknowledgement=acknowledgement,
        )
        if "title" in payload:
            design.title = payload["title"]
        if "global_description" in payload:
            design.global_description = payload["global_description"]
        for field_name in ("entities", "interfaces"):
            if field_name in payload:
                setattr(design, field_name, payload[field_name] or [])
        if "diagrams" in payload:
            design.diagrams = await self._normalize_diagrams(
                design.board_id,
                design.id,
                payload["diagrams"] or [],
                entities=candidate_payload.get("entities") or [],
            )
        for field_name in ("source_ref", "source_version", "source_design_id"):
            if field_name in payload:
                setattr(design, field_name, payload[field_name])
        design.stale = False
        design.breaking_change_flag = False
        design.requires_arch_review = False
        design.version += 1
        await _architecture_flush(self.db)
        await self.snapshot(design.id, actor_id, change_summary or "Architecture design updated")
        await self._persist_finding_run(
            design=design,
            critique=critique,
            actor_id=actor_id,
            acknowledgement=acknowledgement,
        )
        if semantic_change:
            await self._publish_parent_semantic_change(design, actor_id)
        await _architecture_flush(self.db)
        await get_architecture_persistence_port().refresh(self.db, design)
        if design.parent_type == "spec":
            await self._propagate_spec_architecture_change(
                design,
                actor_id,
                trigger="spec_architecture_updated",
            )
        return design

    def _critique_for_save(
        self,
        *,
        design_id: str,
        board_id: str | None,
        entity_type: str | None,
        payload: dict[str, Any],
        acknowledgement: ArchitectureWarningAcknowledgementRequest | dict[str, Any] | None,
    ) -> dict[str, Any]:
        critique = self.critique_payload(payload)
        issues = list(critique.get("issues") or [])
        if issues:
            raise ArchitecturePayloadValidationError(issues)

        warnings = self._structured_warnings_with_keys(
            design_id,
            list(critique.get("structured_warnings") or []),
        )
        critique["structured_warnings"] = warnings
        if warnings:
            self._require_warning_acknowledgement(
                design_id=design_id,
                board_id=board_id,
                entity_type=entity_type,
                warnings=warnings,
                acknowledgement=acknowledgement,
            )
        return critique

    def _structured_warnings_with_keys(
        self,
        design_id: str,
        structured_warnings: list[ArchitectureWarningRecord | dict[str, Any]],
    ) -> list[dict[str, Any]]:
        keyed: list[dict[str, Any]] = []
        for warning in structured_warnings:
            data = _warning_as_dict(warning)
            data["finding_key"] = stable_architecture_finding_key(design_id, data)
            keyed.append(data)
        return keyed

    async def _diagrams_for_validation(self, diagrams: list[dict[str, Any]]) -> list[dict[str, Any]]:
        enriched: list[dict[str, Any]] = []
        for diagram in diagrams:
            item = dict(diagram)
            ref = item.get("adapter_payload_ref")
            if ref and "adapter_payload" not in item:
                try:
                    loaded = await self.diagram_store.load_payload(ref)
                    # payload_text retorna string JSON; o TopologyWarningEngine
                    # exige dict e pularia o diagrama silenciosamente.
                    if isinstance(loaded, str):
                        try:
                            loaded = json.loads(loaded)
                        except (TypeError, ValueError):
                            loaded = None
                    if isinstance(loaded, dict):
                        item["adapter_payload"] = loaded
                except KeyError:
                    pass
            enriched.append(item)
        return enriched

    def _require_warning_acknowledgement(
        self,
        *,
        design_id: str,
        board_id: str | None,
        entity_type: str | None,
        warnings: list[dict[str, Any]],
        acknowledgement: ArchitectureWarningAcknowledgementRequest | dict[str, Any] | None,
    ) -> None:
        data = _dump_model_or_dict(acknowledgement) if acknowledgement is not None else {}
        warning_keys = sorted(str(warning["finding_key"]) for warning in warnings)
        code_count = len({str(warning.get("code") or "unknown") for warning in warnings})
        metric_labels = {
            "board_id": board_id or "unknown",
            "entity_type": entity_type or "unknown",
            "warning_count": len(warnings),
            "code_count": code_count,
        }
        if not data.get("accepted"):
            observe_architecture_warning_ack(
                **metric_labels,
                outcome="required_without_ack",
            )
            raise ArchitectureWarningAcknowledgementRequired(
                design_id=design_id,
                warning_keys=warning_keys,
                warnings=warnings,
            )
        acknowledged_keys = sorted(
            str(key)
            for key in (data.get("warning_keys") or [])
            if str(key).strip()
        )
        if acknowledged_keys and acknowledged_keys != warning_keys:
            observe_architecture_warning_ack(
                **metric_labels,
                outcome="failed_ack_mismatch",
            )
            raise ArchitectureWarningAcknowledgementRequired(
                design_id=design_id,
                warning_keys=warning_keys,
                warnings=warnings,
                reason="architecture_warning_acknowledgement_keys_mismatch",
            )
        observe_architecture_warning_ack(
            **metric_labels,
            outcome="accepted_with_ack",
        )

    async def _persist_finding_run(
        self,
        *,
        design: ArchitectureRecord,
        critique: dict[str, Any],
        actor_id: str,
        acknowledgement: ArchitectureWarningAcknowledgementRequest | dict[str, Any] | None,
    ) -> None:
        structured_warnings = list(critique.get("structured_warnings") or [])
        critic_run_id = f"archcrit:{design.id}:v{design.version}:{uuid.uuid4().hex[:8]}"
        actor = {
            "actor_type": "user",
            "actor_id": actor_id,
            "actor_name": actor_id,
        }
        store = ArchitectureFindingRunStore(self.db)
        try:
            await store.upsert_latest_run(
                board_id=design.board_id,
                design_id=design.id,
                design_version=design.version,
                critic_run_id=critic_run_id,
                actor=actor,
                validator_summary={
                    "valid": True,
                    "issues": [],
                    "warnings_count": len(critique.get("warnings") or []),
                    "structured_warnings_count": len(structured_warnings),
                    "suppressed_warnings_count": len(critique.get("suppressed_warnings") or []),
                    "summary": critique.get("summary") or {},
                },
                structured_warnings=structured_warnings,
            )
        except Exception:
            observe_architecture_finding_run_persist(
                board_id=design.board_id,
                entity_type=design.parent_type,
                warning_count=len(structured_warnings),
                outcome="failed",
            )
            raise
        observe_architecture_finding_run_persist(
            board_id=design.board_id,
            entity_type=design.parent_type,
            warning_count=len(structured_warnings),
            outcome="persisted",
        )
        if structured_warnings and acknowledgement is not None:
            data = _dump_model_or_dict(acknowledgement)
            if data.get("accepted"):
                warning_keys = [
                    str(warning["finding_key"])
                    for warning in structured_warnings
                    if warning.get("finding_key")
                ]
                await store.record_acknowledgements(
                    board_id=design.board_id,
                    design_id=design.id,
                    critic_run_id=critic_run_id,
                    finding_keys=warning_keys,
                    actor=actor,
                    statement=data.get("statement"),
                )

    async def delete(
        self,
        design_id: str,
        actor_id: str | None = None,
        *,
        allow_card_parent_write: bool = False,
    ) -> bool:
        design = await self.get(design_id)
        if design is None:
            return False
        _ensure_card_architecture_write_allowed(
            design.parent_type,
            allow=allow_card_parent_write,
        )
        spec_parent_id = (
            self.parent_id_for(design) if design.parent_type == "spec" else None
        )
        spec_board_id = design.board_id if design.parent_type == "spec" else None
        deleted_design_id = design.id
        await self._publish_parent_semantic_change(design, actor_id)
        await get_architecture_persistence_port().delete(self.db, design)
        if spec_parent_id and spec_board_id:
            from okto_pulse.core.services.spec_resource_propagation import (
                SpecResourcePropagationService,
            )

            await SpecResourcePropagationService(self.db).propagate_for_spec(
                board_id=spec_board_id,
                spec_id=spec_parent_id,
                actor_id=actor_id or "system",
                trigger="spec_architecture_deleted",
                removed_architecture_design_ids={deleted_design_id},
            )
        return True

    async def _propagate_spec_architecture_change(
        self,
        design: ArchitectureRecord,
        actor_id: str | None,
        *,
        trigger: str,
    ) -> None:
        from okto_pulse.core.services.spec_resource_propagation import (
            SpecResourcePropagationService,
        )

        if design.parent_type != "spec":
            return
        parent_id = self.parent_id_for(design)
        if not parent_id:
            return
        resolved_actor = actor_id or "system"
        await SpecResourcePropagationService(self.db).propagate_for_spec(
            board_id=design.board_id,
            spec_id=parent_id,
            actor_id=resolved_actor,
            trigger=trigger,
        )

    async def snapshot(
        self, design_id: str, actor_id: str, reason: str | None = None
    ) -> ArchitectureRecord:
        design = await self.get(design_id)
        if design is None:
            raise ValueError(f"architecture design not found: {design_id}")
        envelope = self._envelope_snapshot(design)
        return await _architecture_create(
            self.db,
            "architecture_design_version",
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

    async def diff(self, design_id: str, from_version: int, to_version: int) -> ArchitectureDiffResponse:
        rows = await _architecture_list(
            self.db,
            "architecture_design_version",
            filters=(
                _arf("design_id", "eq", design_id),
                _arf("version", "in", [from_version, to_version]),
            ),
        )
        versions = {item.version: item for item in rows}
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

    def to_summary(self, design: ArchitectureRecord) -> ArchitectureDesignSummary:
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

    def to_response(self, design: ArchitectureRecord) -> ArchitectureDesignResponse:
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

    def parent_id_for(self, design: ArchitectureRecord) -> str:
        _, parent_field = self._parent_config(design.parent_type)
        parent_id = getattr(design, parent_field)
        if not parent_id:
            raise ValueError(f"architecture design has no {parent_field}")
        return parent_id

    def source_ref_for(self, design: ArchitectureRecord) -> str:
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
                "structured_warnings": [],
                "suppressed_warnings": [],
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

        diagrams_value = payload.get("diagrams")
        topology_evaluation = TopologyWarningEngine().evaluate(
            entities=entities,
            diagrams=diagrams_value if isinstance(diagrams_value, list) else [],
            existing_issues=issues,
        )
        topology_result = topology_evaluation.to_dict()
        structured_warnings = topology_result["warnings"]
        suppressed_warnings = topology_result["suppressed_warnings"]

        # Spec cc497a0d — semantic node normalization. Runs after structural
        # validation so dangling refs surface their own messages first, then
        # checks that linked nodes carry text/displayType/architectureKind/iconName
        # consistent with the entity's entity_type and the canonical registry.
        if isinstance(diagrams_value, list):
            for d_index, raw_diagram in enumerate(diagrams_value):
                if not isinstance(raw_diagram, dict):
                    continue
                adapter_payload = raw_diagram.get("adapter_payload")
                if not isinstance(adapter_payload, dict):
                    continue
                _, sem_warnings, sem_issues = normalize_excalidraw_semantics(
                    adapter_payload,
                    entities,
                    path_prefix=f"diagrams[{d_index}].adapter_payload",
                )
                warnings.extend(sem_warnings)
                issues.extend(sem_issues)

        summary = self._payload_summary(payload)
        summary["structured_warnings_count"] = len(structured_warnings)
        summary["suppressed_warnings_count"] = len(suppressed_warnings)
        summary["structured_warning_codes"] = [warning["code"] for warning in structured_warnings]
        by_code = Counter(str(warning.get("code") or "unknown") for warning in structured_warnings)
        by_code_and_diagram_type = Counter(
            (
                str(warning.get("code") or "unknown"),
                str(warning.get("diagram_type") or "unknown"),
            )
            for warning in structured_warnings
        )
        summary["structured_warning_counts_by_code"] = {
            code: by_code[code]
            for code in sorted(by_code)
        }
        summary["structured_warning_counts_by_code_and_diagram_type"] = [
            {"code": code, "diagram_type": diagram_type, "count": count}
            for (code, diagram_type), count in sorted(by_code_and_diagram_type.items())
        ]

        return {
            "valid": not issues,
            "issues": issues,
            "warnings": warnings,
            "structured_warnings": structured_warnings,
            "suppressed_warnings": suppressed_warnings,
            "suggested_fixes": self._suggest_fixes(issues + warnings),
            "summary": summary,
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

            format_name = raw_diagram.get("format") or SUPPORTED_ARCHITECTURE_DIAGRAM_FORMAT
            if format_name != SUPPORTED_ARCHITECTURE_DIAGRAM_FORMAT:
                issues.append(
                    f"{path}.format='{format_name}' is unsupported. Architecture Design diagrams must use "
                    f"format='{SUPPORTED_ARCHITECTURE_DIAGRAM_FORMAT}'. Mermaid, PlantUML, C4, SVG, and raw "
                    "snippets are accepted only as descriptive text in entity responsibility, boundaries, notes, "
                    "or global_description; they cannot be registered as diagram formats."
                )
                continue
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
            if ".format" in lower and "excalidraw_json" in lower:
                fixes.append(
                    "Use diagrams[].format='excalidraw_json'. Put Mermaid, PlantUML, C4, SVG, or raw snippets "
                    "in entity responsibility, boundaries, notes, or global_description instead of diagram formats."
                )
            elif "connectiontype" in lower:
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
            elif "semantic_node_registry" in lower or "canonical mapping" in lower:
                fixes.append(
                    "Add the entity_type to SEMANTIC_NODE_REGISTRY or rename the entity to a known type. "
                    "Until then, set displayType, architectureKind, iconName and text explicitly on the diagram node."
                )
            elif "iconname" in lower and "allowed icon set" in lower:
                fixes.append(
                    "Use one of the allowed iconName values (boxes, server, database, message, user, network, cloud, "
                    "cpu, globe, hard_drive, lock, monitor, package, smartphone, terminal, workflow) or remove the field "
                    "to inherit the canonical icon from the registry."
                )
            elif "displaytype" in lower and "diverges" in lower:
                fixes.append(
                    "Remove the diverging displayType to inherit the registry value, align it to the canonical one, "
                    "or change the entity's entity_type to match the intended category."
                )
            elif "without entity_type" in lower:
                fixes.append("Set the entity's entity_type so diagram nodes can inherit semantic metadata via the registry.")
        deduped: list[str] = []
        for fix in fixes:
            if fix not in deduped:
                deduped.append(fix)
        return deduped

    async def _normalize_diagrams(
        self,
        board_id: str,
        design_id: str,
        diagrams: list[dict[str, Any]],
        *,
        entities: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        normalized_diagrams: list[dict[str, Any]] = []
        for raw_diagram in diagrams:
            diagram = dict(raw_diagram)
            diagram_id = diagram.get("id") or _new_scoped_id("diag")
            diagram["id"] = diagram_id
            format_name = diagram.get("format") or SUPPORTED_ARCHITECTURE_DIAGRAM_FORMAT
            diagram["format"] = format_name
            payload = diagram.pop("adapter_payload", None)
            if payload is not None:
                adapter = self.adapter_registry.get(format_name)
                normalized_payload = adapter.normalize(payload)
                # Spec cc497a0d FR2: apply semantic registry to fill safe defaults
                # before persistence so renderers/agents see complete metadata. Validation
                # gate already raised on issues; here we just normalize warnings.
                if (
                    format_name == SUPPORTED_ARCHITECTURE_DIAGRAM_FORMAT
                    and isinstance(normalized_payload, dict)
                    and entities is not None
                ):
                    normalized_payload, _, _ = normalize_excalidraw_semantics(
                        normalized_payload, entities,
                    )
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

    async def _attach_payloads(self, design: ArchitectureRecord) -> None:
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
        design.attach("diagrams", diagrams)

    def _envelope_snapshot(self, design: ArchitectureRecord) -> dict[str, Any]:
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
        design: ArchitectureRecord,
        actor_id: str | None,
    ) -> None:
        """Emit the existing semantic events that feed KG consolidation.

        Architecture is a child artifact, but spec/refinement consolidation is
        parent-scoped today. Reusing these event types keeps the KG pipeline
        light and avoids a new graph schema or dispatcher branch for v1.
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
        db: Any,
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
        architecture_warning_acknowledgement: ArchitectureWarningAcknowledgementRequest | dict[str, Any] | None = None,
    ) -> list[ArchitectureRecord]:
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

        # Spec B: enforce canonical propagation eligibility against every REAL source
        # BEFORE any create/update, so a blocking source never yields a partial target
        # snapshot. ``architecture_warning_acknowledgement`` stays audit-only here.
        await self.assert_sources_eligible(source_designs, flow="copy_from_parent")

        copied: list[ArchitectureRecord] = []
        for source_design in source_designs:
            source_ref = self.repository.source_ref_for(source_design)
            existing = await self._find_existing_copy(target_parent_type, target_parent_id, source_ref)
            payload = self._payload_from_source(source_design, source_ref)
            if architecture_warning_acknowledgement is not None:
                payload["architecture_warning_acknowledgement"] = architecture_warning_acknowledgement
            if existing is None:
                copied_design = await self.repository.create(
                    target_parent_type,
                    target_parent_id,
                    ArchitectureDesignCreate(**payload),
                    actor_id,
                    allow_card_parent_write=target_parent_type == "card",
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
                        architecture_warning_acknowledgement=architecture_warning_acknowledgement,
                        change_summary=f"Re-synced from {source_parent_type} architecture",
                    ),
                    actor_id,
                    allow_card_parent_write=target_parent_type == "card",
                )
            copied.append(copied_design)
        await _architecture_flush(self.db)
        return copied

    async def copy_spec_to_card(
        self,
        spec_id: str,
        card_id: str,
        actor_id: str,
        design_ids: list[str] | None = None,
        architecture_warning_acknowledgement: ArchitectureWarningAcknowledgementRequest | dict[str, Any] | None = None,
    ) -> list[ArchitectureRecord]:
        return await self.copy_from_parent(
            "spec",
            spec_id,
            "card",
            card_id,
            actor_id,
            design_ids,
            architecture_warning_acknowledgement=architecture_warning_acknowledgement,
        )

    async def copy_effective_spec_to_card(
        self,
        *,
        board_id: str,
        spec_id: str,
        card_id: str,
        actor_id: str,
        design_ids: list[str] | None = None,
        architecture_warning_acknowledgement: ArchitectureWarningAcknowledgementRequest | dict[str, Any] | None = None,
    ) -> tuple[list[ArchitectureRecord], dict[str, Any]]:
        """Copy every effective spec Architecture obligation to a card.

        Direct spec designs still use the standard spec -> card copy path. When
        a legacy/manual spec only inherits Architecture Designs, every inherited
        reference is copied from its own source parent and the snapshot carries
        ``source_design_id`` so the Resource Gate can match by origin identity.
        """
        from okto_pulse.core.services.effective_resource_propagation import (
            ResourcePropagationError,
            effective_ref_identity_values,
            resolve_effective_card_copy_plan,
        )

        plan = await resolve_effective_card_copy_plan(
            self.db,
            board_id=board_id,
            spec_id=spec_id,
            resource_type="architecture",
        )
        if plan["fallback"]:
            refs = list(plan.get("fallback_refs") or [])
            wanted = set(design_ids or [])
            if wanted:
                # Card 5c43a364: match design_ids against EVERY canonical identity
                # of the effective ref (root + intermediate), not just the immediate
                # id, so design_ids=[root_id] still resolves a multi-hop inherited ref.
                refs = [
                    ref for ref in refs
                    if effective_ref_identity_values(ref) & wanted
                ]
            grouped: dict[tuple[str, str], list[str]] = {}
            for ref in refs:
                source_parent_type = str(ref.get("source_entity_type") or "")
                source_parent_id = str(ref.get("source_entity_id") or "")
                source_design_id = str(ref.get("id") or ref.get("resource_id") or "")
                if not source_parent_type or not source_parent_id or not source_design_id:
                    continue
                grouped.setdefault(
                    (source_parent_type, source_parent_id),
                    [],
                ).append(source_design_id)

            if not grouped and plan["has_obligation"] and not wanted:
                raise ResourcePropagationError(
                    "architecture copy failed: effective inherited Architecture "
                    "references could not be resolved to source parents",
                    failed_resource_type="architecture",
                    coverage_obligation_id=plan.get("coverage_obligation_id"),
                )

            copied: list[ArchitectureRecord] = []
            for (source_parent_type, source_parent_id), source_design_ids in grouped.items():
                copied.extend(
                    await self.copy_from_parent(
                        source_parent_type=source_parent_type,
                        source_parent_id=source_parent_id,
                        target_parent_type="card",
                        target_parent_id=card_id,
                        actor_id=actor_id,
                        design_ids=source_design_ids,
                        architecture_warning_acknowledgement=(
                            architecture_warning_acknowledgement
                        ),
                    )
                )
            return copied, plan

        if (
            not plan["has_direct"]
            and plan["has_obligation"]
            and not plan["not_applicable"]
        ):
            raise ResourcePropagationError(
                "architecture copy failed: Resource Gate reports Architecture "
                "provided but no direct or inherited copy source was resolved",
                failed_resource_type="architecture",
                coverage_obligation_id=plan.get("coverage_obligation_id"),
            )

        return (
            await self.copy_spec_to_card(
                spec_id,
                card_id,
                actor_id,
                design_ids=design_ids,
                architecture_warning_acknowledgement=architecture_warning_acknowledgement,
            ),
            plan,
        )

    async def assert_sources_eligible(
        self,
        source_designs: list[ArchitectureRecord],
        *,
        flow: str,
    ) -> None:
        """Spec B: block copy/propagation of any ineligible source BEFORE mutation.

        Evaluates the canonical propagation eligibility policy (Spec A) against each
        REAL source design (not the assembled target snapshot — TR 79b0469d) and raises
        :class:`ArchitecturePropagationBlocked` with the shared structured error on the
        first blocking source. Called before any ``repository.create``/``update`` so a
        blocking source never yields a partial target snapshot (atomicity, TR 3154a2d1).
        ``architecture_warning_acknowledgement`` is audit-only and never bypasses this
        (FR 67f3545a). Emits a structured, payload-free block log (OR-B1).
        """
        policy = ArchitecturePropagationEligibilityPolicy(self.db)
        for design in source_designs:
            eligibility = await policy.evaluate(design.id)
            if eligibility.eligible:
                continue
            logger.warning(
                "architecture.propagation_blocked flow=%s board_id=%s source_design_id=%s "
                "verdict=%s finding_keys=%s",
                flow,
                design.board_id,
                design.id,
                eligibility.verdict_status,
                ",".join(eligibility.finding_keys),
                extra={
                    "event": "architecture.propagation_blocked",
                    "metric": "architecture_propagation_blocked_total",
                    "flow": flow,
                    "board_id": design.board_id,
                    "source_design_id": design.id,
                    "verdict_status": eligibility.verdict_status,
                    "blocker_counts": eligibility.blocker_counts,
                },
            )
            raise ArchitecturePropagationBlocked(eligibility)

    async def _get_parent(self, parent_type: str, parent_id: str) -> Any | None:
        parent_entity, _ = self.repository._parent_config(parent_type)
        return await _architecture_get(self.db, parent_entity, parent_id)

    async def _find_existing_copy(
        self,
        target_parent_type: str,
        target_parent_id: str,
        source_ref: str,
    ) -> ArchitectureRecord | None:
        _, target_field = self.repository._parent_config(target_parent_type)
        rows = await _architecture_list(
            self.db,
            "architecture_design",
            filters=(
                _arf("parent_type", "eq", target_parent_type),
                _arf(target_field, "eq", target_parent_id),
                _arf("source_ref", "eq", source_ref),
            ),
            limit=1,
        )
        return rows[0] if rows else None

    def _payload_from_source(
        self, design: ArchitectureRecord, source_ref: str
    ) -> dict[str, Any]:
        diagrams = []
        for diagram in design.diagrams or []:
            copied = copy.deepcopy(diagram)
            source_diagram_id = copied.get("id")
            source_payload_ref = copied.pop("adapter_payload_ref", None)
            copied["source_diagram_id"] = source_diagram_id
            copied["source_payload_ref"] = source_payload_ref
            copied["id"] = _new_scoped_id("diag")
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
            # Card 5c43a364: carry the canonical ROOT origin (not the immediate
            # parent) so a multi-hop inherited copy covers the gate obligation.
            "source_design_id": design.source_design_id or design.id,
            "stale": False,
            "breaking_change_flag": False,
            "requires_arch_review": False,
        }
