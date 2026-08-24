"""Canonical, persistence-neutral Project structure contracts.

The aggregate belongs to a Spec and deliberately preserves ``None`` (not
authored) versus ``[]`` (authored and empty).  This module has no persistence
or transport knowledge; adapters persist its JSON projection and use the pure
projection/export helpers for Task, Test and whole-Spec reads.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from enum import Enum
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

PROJECT_STRUCTURE_CONTRACT_VERSION = "project-structure/v1"
PROJECT_STRUCTURE_EXPORT_SCHEMA_VERSION = "project-structure-export/v1"
PROJECT_STRUCTURE_MAX_ACTIVE_NODES = 500
PROJECT_STRUCTURE_MAX_DEPTH = 20
PROJECT_STRUCTURE_MAX_NAME_LENGTH = 255
PROJECT_STRUCTURE_MAX_NOTE_LENGTH = 4_000
PROJECT_STRUCTURE_MAX_IMPACT_ITEMS = 50

_NODE_ID_RE = re.compile(r"^psn_[A-Za-z0-9][A-Za-z0-9_-]{0,123}$")
_REFERENCE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$")


class ProjectStructureError(ValueError):
    """Stable domain error with machine-readable details."""

    def __init__(self, code: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.details = details or {}


class ProjectStructureValidationError(ProjectStructureError):
    def __init__(self, issues: list[dict[str, Any]]) -> None:
        super().__init__(
            "project_structure_validation_failed", details={"issues": issues[:100]}
        )
        self.issues = issues[:100]


class ProjectStructureRemovalBlocked(ProjectStructureError):
    def __init__(self, impact: dict[str, Any]) -> None:
        super().__init__("project_structure_folder_not_empty", details=impact)
        self.impact = impact


class ProjectStructureNodeKind(str, Enum):
    FOLDER = "folder"
    FILE = "file"
    ARTIFACT = "artifact"


class ProjectStructureClassification(str, Enum):
    AS_IS = "as_is"
    TO_BE = "to_be"
    REFERENCE_SCAFFOLD = "reference_scaffold"


class ProjectStructureNodeState(str, Enum):
    EXISTING = "existing"
    PLANNED = "planned"
    MODIFIED = "modified"
    REMOVED = "removed"


class ProjectStructureNodeStatus(str, Enum):
    ACTIVE = "active"
    REVOKED = "revoked"


class ProjectStructureTaskRole(str, Enum):
    CREATE = "create"
    MODIFY = "modify"
    READ = "read"
    REMOVE = "remove"


class ProjectStructureTestRole(str, Enum):
    TARGET = "target"
    TEST_FILE = "test_file"
    FIXTURE = "fixture"
    INTEGRATION_POINT = "integration_point"


def _clean_reference_id(value: str) -> str:
    normalized = value.strip()
    if not _REFERENCE_ID_RE.fullmatch(normalized):
        raise ValueError("reference id must be an opaque same-Spec identifier")
    return normalized


class _ClosedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class ProjectStructureTaskReference(_ClosedModel):
    task_id: str
    role: ProjectStructureTaskRole
    classification_at_link: ProjectStructureClassification | None = None

    _task_id = field_validator("task_id")(_clean_reference_id)


class ProjectStructureTestReference(_ClosedModel):
    test_id: str
    role: ProjectStructureTestRole
    classification_at_link: ProjectStructureClassification | None = None

    _test_id = field_validator("test_id")(_clean_reference_id)


class ProjectStructureNode(_ClosedModel):
    """Closed canonical node stored inside ``Spec.project_structure``."""

    id: str
    parent_id: str | None = None
    position: int = Field(ge=0)
    kind: ProjectStructureNodeKind
    name: str = Field(min_length=1, max_length=PROJECT_STRUCTURE_MAX_NAME_LENGTH)
    note: str = Field(default="", max_length=PROJECT_STRUCTURE_MAX_NOTE_LENGTH)
    classification: ProjectStructureClassification
    state: ProjectStructureNodeState | None = None
    interpretation_limit: str | None = Field(
        default=None,
        min_length=1,
        max_length=PROJECT_STRUCTURE_MAX_NOTE_LENGTH,
    )
    status: ProjectStructureNodeStatus = ProjectStructureNodeStatus.ACTIVE
    task_references: list[ProjectStructureTaskReference] = Field(default_factory=list)
    test_references: list[ProjectStructureTestReference] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)

    @field_validator("id", "parent_id")
    @classmethod
    def _node_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not _NODE_ID_RE.fullmatch(normalized):
            raise ValueError("project structure node id must match psn_*")
        return normalized

    @field_validator("name", mode="before")
    @classmethod
    def _name(cls, value: Any) -> Any:
        if not isinstance(value, str):
            return value
        normalized = value.strip()
        if not normalized:
            raise ValueError("node name must not be blank")
        return normalized

    @field_validator("note", mode="before")
    @classmethod
    def _note(cls, value: Any) -> Any:
        if not isinstance(value, str):
            return value
        return value.strip()

    @field_validator("interpretation_limit", mode="before")
    @classmethod
    def _interpretation_limit(cls, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            return value
        normalized = value.strip()
        if not normalized:
            raise ValueError("interpretation_limit must not be blank")
        return normalized

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids(cls, values: list[str]) -> list[str]:
        normalized = [_clean_reference_id(value) for value in values]
        if len(normalized) != len(set(normalized)):
            raise ValueError("evidence_ids must be unique")
        return sorted(normalized)

    @model_validator(mode="after")
    def _coherent_context(self) -> ProjectStructureNode:
        task_ids = [item.task_id for item in self.task_references]
        test_ids = [item.test_id for item in self.test_references]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError("task_references must contain one role per task")
        if len(test_ids) != len(set(test_ids)):
            raise ValueError("test_references must contain one role per test")
        if (
            self.classification
            == ProjectStructureClassification.REFERENCE_SCAFFOLD.value
        ):
            if self.interpretation_limit is None:
                raise ValueError("reference_scaffold requires interpretation_limit")
        elif self.interpretation_limit is not None:
            raise ValueError(
                "interpretation_limit is only valid for reference_scaffold"
            )
        if (
            self.classification == ProjectStructureClassification.TO_BE.value
            and self.evidence_ids
        ):
            raise ValueError("to_be nodes cannot be linked to Code Evidence")
        for reference in (*self.task_references, *self.test_references):
            if reference.classification_at_link is None:
                reference.classification_at_link = self.classification
        self.task_references.sort(key=lambda item: (item.task_id, str(item.role)))
        self.test_references.sort(key=lambda item: (item.test_id, str(item.role)))
        return self


class ProjectStructureMutation(_ClosedModel):
    operation: Literal[
        "create",
        "update",
        "revoke",
        "restore",
        "reorder",
        "link_task",
        "unlink_task",
        "link_test",
        "unlink_test",
        "link_evidence",
        "unlink_evidence",
    ]
    entity_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    position: int | None = Field(default=None, ge=0)
    task_id: str | None = None
    task_role: ProjectStructureTaskRole | None = None
    test_id: str | None = None
    test_role: ProjectStructureTestRole | None = None
    evidence_id: str | None = None

    @field_validator("entity_id")
    @classmethod
    def _entity_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not _NODE_ID_RE.fullmatch(normalized):
            raise ValueError("entity_id must match psn_*")
        return normalized

    @field_validator("task_id", "test_id", "evidence_id")
    @classmethod
    def _reference_id(cls, value: str | None) -> str | None:
        return None if value is None else _clean_reference_id(value)


class ProjectStructureBatch(_ClosedModel):
    operations: list[ProjectStructureMutation] = Field(min_length=1, max_length=500)


class ProjectStructureProjectionNode(_ClosedModel):
    node: ProjectStructureNode
    depth: int = Field(ge=1, le=PROJECT_STRUCTURE_MAX_DEPTH)
    direct: bool
    context_only: bool
    reference_role: str | None = None


class ProjectStructureAffectedReference(_ClosedModel):
    node_id: str
    state: Literal["unavailable", "classification_changed"]
    reason: str
    classification: ProjectStructureClassification | None = None


class ProjectStructureProjection(_ClosedModel):
    contract_version: Literal["project-structure/v1"] = (
        PROJECT_STRUCTURE_CONTRACT_VERSION
    )
    state: Literal[
        "not_authored",
        "authored_empty",
        "no_direct_references",
        "projected",
    ]
    spec_id: str
    spec_version: int = Field(ge=0)
    authored: bool
    structure_revision: int = Field(ge=0)
    digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    reference_type: Literal["task", "test"]
    reference_id: str
    nodes: list[ProjectStructureProjectionNode] = Field(default_factory=list)
    affected_references: list[ProjectStructureAffectedReference] = Field(
        default_factory=list
    )


class ProjectStructureSnapshot(_ClosedModel):
    contract_version: Literal["project-structure/v1"] = (
        PROJECT_STRUCTURE_CONTRACT_VERSION
    )
    state: Literal["not_authored", "authored_empty", "authored"]
    spec_id: str
    spec_version: int = Field(ge=0)
    authored: bool
    structure_revision: int = Field(ge=0)
    digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    nodes: list[ProjectStructureNode] = Field(default_factory=list)


def _issue(code: str, **details: Any) -> dict[str, Any]:
    return {"code": code, **details}


def _node_payload(node: ProjectStructureNode) -> dict[str, Any]:
    return node.model_dump(mode="json")


def _parse_nodes(
    nodes: list[Any],
) -> tuple[list[ProjectStructureNode], list[dict[str, Any]]]:
    parsed: list[ProjectStructureNode] = []
    issues: list[dict[str, Any]] = []
    for index, raw in enumerate(nodes):
        try:
            parsed.append(ProjectStructureNode.model_validate(raw))
        except ValidationError as exc:
            for error in exc.errors(include_url=False):
                issues.append(
                    _issue(
                        "project_structure_node_invalid",
                        index=index,
                        path=".".join(str(item) for item in error.get("loc", ())),
                        message=error.get("msg", "invalid node"),
                    )
                )
    return parsed, issues


def validate_project_structure(nodes: list[Any] | None) -> list[dict[str, Any]] | None:
    """Validate the complete aggregate before returning canonical JSON."""

    if nodes is None:
        return None
    if not isinstance(nodes, list):
        raise ProjectStructureValidationError(
            [_issue("project_structure_must_be_list_or_null")]
        )
    parsed, issues = _parse_nodes(nodes)
    by_id: dict[str, ProjectStructureNode] = {}
    for node in parsed:
        if node.id in by_id:
            issues.append(_issue("project_structure_duplicate_id", node_id=node.id))
        else:
            by_id[node.id] = node

    active = [
        node
        for node in parsed
        if node.status == ProjectStructureNodeStatus.ACTIVE.value
    ]
    if len(active) > PROJECT_STRUCTURE_MAX_ACTIVE_NODES:
        issues.append(
            _issue(
                "project_structure_node_limit_exceeded",
                maximum=PROJECT_STRUCTURE_MAX_ACTIVE_NODES,
                actual=len(active),
            )
        )

    active_by_id = {node.id: node for node in active}
    siblings: dict[str | None, list[ProjectStructureNode]] = {}
    for node in active:
        if node.parent_id == node.id:
            issues.append(_issue("project_structure_self_parent", node_id=node.id))
        if node.parent_id is not None:
            parent = active_by_id.get(node.parent_id)
            if parent is None:
                issues.append(
                    _issue(
                        "project_structure_parent_missing_or_inactive",
                        node_id=node.id,
                        parent_id=node.parent_id,
                    )
                )
            elif parent.kind != ProjectStructureNodeKind.FOLDER.value:
                issues.append(
                    _issue(
                        "project_structure_parent_not_folder",
                        node_id=node.id,
                        parent_id=node.parent_id,
                    )
                )
        siblings.setdefault(node.parent_id, []).append(node)

    for parent_id, values in siblings.items():
        positions = sorted(node.position for node in values)
        if positions != list(range(len(values))):
            issues.append(
                _issue(
                    "project_structure_sibling_positions_not_contiguous",
                    parent_id=parent_id,
                    positions=positions,
                )
            )

    children = {
        key: sorted(value, key=lambda item: (item.position, item.id))
        for key, value in siblings.items()
    }
    for node in active:
        if node.kind != ProjectStructureNodeKind.FOLDER.value and children.get(node.id):
            issues.append(
                _issue("project_structure_leaf_has_children", node_id=node.id)
            )

    visited: set[str] = set()
    visiting: set[str] = set()

    def walk(node: ProjectStructureNode, depth: int) -> None:
        if node.id in visiting:
            issues.append(_issue("project_structure_cycle", node_id=node.id))
            return
        if node.id in visited:
            issues.append(
                _issue("project_structure_multiple_reachability", node_id=node.id)
            )
            return
        if depth > PROJECT_STRUCTURE_MAX_DEPTH:
            issues.append(
                _issue(
                    "project_structure_depth_exceeded",
                    node_id=node.id,
                    maximum=PROJECT_STRUCTURE_MAX_DEPTH,
                    actual=depth,
                )
            )
            return
        visiting.add(node.id)
        for child in children.get(node.id, []):
            walk(child, depth + 1)
        visiting.remove(node.id)
        visited.add(node.id)

    for root in children.get(None, []):
        walk(root, 1)
    unreachable = sorted(set(active_by_id) - visited)
    for node_id in unreachable:
        issues.append(_issue("project_structure_node_unreachable", node_id=node_id))

    if issues:
        raise ProjectStructureValidationError(issues)
    return [_node_payload(node) for node in parsed]


def canonical_project_structure_digest(nodes: list[Any] | None) -> str | None:
    canonical = validate_project_structure(nodes)
    if canonical is None:
        return None
    payload = json.dumps(
        canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _active_siblings(
    nodes: list[dict[str, Any]], parent_id: str | None
) -> list[dict[str, Any]]:
    return sorted(
        [
            node
            for node in nodes
            if node.get("status", "active") == "active"
            and node.get("parent_id") == parent_id
        ],
        key=lambda item: (int(item.get("position", 0)), str(item.get("id"))),
    )


def _normalize_positions(
    nodes: list[dict[str, Any]], parent_ids: set[str | None]
) -> None:
    for parent_id in parent_ids:
        for position, node in enumerate(_active_siblings(nodes, parent_id)):
            node["position"] = position


def project_structure_removal_impact(
    nodes: list[Any],
    node_id: str,
) -> dict[str, Any]:
    canonical = validate_project_structure(nodes) or []
    by_parent: dict[str, list[dict[str, Any]]] = {}
    for node in canonical:
        parent_id = node.get("parent_id")
        if parent_id and node.get("status", "active") == "active":
            by_parent.setdefault(parent_id, []).append(node)
    descendants: list[dict[str, Any]] = []
    pending = list(by_parent.get(node_id, []))
    while pending:
        current = pending.pop(0)
        descendants.append(current)
        pending.extend(by_parent.get(str(current["id"]), []))
    impacted = [node for node in canonical if node.get("id") == node_id] + descendants
    task_ids = sorted(
        {
            str(ref["task_id"])
            for node in impacted
            for ref in node.get("task_references", [])
        }
    )
    test_ids = sorted(
        {
            str(ref["test_id"])
            for node in impacted
            for ref in node.get("test_references", [])
        }
    )
    evidence_ids = sorted(
        {str(value) for node in impacted for value in node.get("evidence_ids", [])}
    )
    return {
        "node_id": node_id,
        "descendant_count": len(descendants),
        "descendant_ids": [
            str(node["id"]) for node in descendants[:PROJECT_STRUCTURE_MAX_IMPACT_ITEMS]
        ],
        "descendants_truncated": len(descendants) > PROJECT_STRUCTURE_MAX_IMPACT_ITEMS,
        "affected_task_ids": task_ids[:PROJECT_STRUCTURE_MAX_IMPACT_ITEMS],
        "affected_test_ids": test_ids[:PROJECT_STRUCTURE_MAX_IMPACT_ITEMS],
        "affected_evidence_ids": evidence_ids[:PROJECT_STRUCTURE_MAX_IMPACT_ITEMS],
        "affected_references_truncated": any(
            len(values) > PROJECT_STRUCTURE_MAX_IMPACT_ITEMS
            for values in (task_ids, test_ids, evidence_ids)
        ),
    }


def _find(nodes: list[dict[str, Any]], node_id: str | None) -> dict[str, Any]:
    if not node_id:
        raise ProjectStructureError("project_structure_entity_id_required")
    for node in nodes:
        if node.get("id") == node_id:
            return node
    raise ProjectStructureError(
        "project_structure_node_not_found", details={"node_id": node_id}
    )


def _apply_one(
    nodes: list[dict[str, Any]],
    mutation: ProjectStructureMutation,
) -> tuple[list[dict[str, Any]], str]:
    values = [dict(node) for node in nodes]
    operation = mutation.operation
    if operation == "create":
        payload = dict(mutation.payload)
        payload.setdefault("id", f"psn_{uuid.uuid4().hex[:12]}")
        payload.setdefault("parent_id", None)
        payload.setdefault(
            "position", len(_active_siblings(values, payload.get("parent_id")))
        )
        payload.setdefault("status", "active")
        node = ProjectStructureNode.model_validate(payload).model_dump(mode="json")
        if any(item.get("id") == node["id"] for item in values):
            existing = _find(values, node["id"])
            if existing == node:
                return values, str(node["id"])
            raise ProjectStructureError(
                "project_structure_duplicate_id", details={"node_id": node["id"]}
            )
        siblings = _active_siblings(values, node.get("parent_id"))
        if node["position"] > len(siblings):
            raise ProjectStructureError("project_structure_position_out_of_range")
        for sibling in siblings[node["position"] :]:
            sibling["position"] = int(sibling["position"]) + 1
        values.append(node)
        return values, str(node["id"])

    if operation == "reorder":
        parent_id = mutation.payload.get("parent_id")
        ordered_ids = [str(value) for value in mutation.payload.get("ordered_ids", [])]
        siblings = _active_siblings(values, parent_id)
        current_ids = [str(item["id"]) for item in siblings]
        if len(ordered_ids) != len(set(ordered_ids)) or set(ordered_ids) != set(
            current_ids
        ):
            raise ProjectStructureError(
                "project_structure_reorder_membership_mismatch",
                details={"expected_ids": current_ids, "received_ids": ordered_ids},
            )
        by_id = {str(item["id"]): item for item in siblings}
        for index, ordered_id in enumerate(ordered_ids):
            by_id[ordered_id]["position"] = index
        return values, "__collection__"

    target = _find(values, mutation.entity_id)
    node_id = str(target["id"])
    if operation == "update":
        payload = dict(mutation.payload)
        if "id" in payload or "status" in payload:
            raise ProjectStructureError("project_structure_immutable_field")
        previous_parent = target.get("parent_id")
        next_parent = payload.get("parent_id", previous_parent)
        requested_position = payload.pop("position", mutation.position)
        target.update(payload)
        target["parent_id"] = next_parent
        if requested_position is None:
            requested_position = target.get("position", 0)
        desired_position = int(requested_position)
        target["position"] = desired_position
        _normalize_positions(values, {previous_parent, next_parent})
        siblings = _active_siblings(values, next_parent)
        siblings = [item for item in siblings if item["id"] != node_id]
        if desired_position > len(siblings):
            raise ProjectStructureError("project_structure_position_out_of_range")
        siblings.insert(desired_position, target)
        for index, sibling in enumerate(siblings):
            sibling["position"] = index
        return values, node_id
    if operation == "revoke":
        impact = project_structure_removal_impact(values, node_id)
        if impact["descendant_count"]:
            raise ProjectStructureRemovalBlocked(impact)
        target["status"] = "revoked"
        _normalize_positions(values, {target.get("parent_id")})
        return values, node_id
    if operation == "restore":
        parent_id = target.get("parent_id")
        siblings = _active_siblings(values, parent_id)
        desired_position = (
            mutation.position if mutation.position is not None else len(siblings)
        )
        if desired_position > len(siblings):
            raise ProjectStructureError("project_structure_position_out_of_range")
        target["status"] = "active"
        siblings.insert(desired_position, target)
        for index, sibling in enumerate(siblings):
            sibling["position"] = index
        return values, node_id

    if operation in {"link_task", "unlink_task"}:
        if mutation.task_id is None:
            raise ProjectStructureError("project_structure_task_id_required")
        refs = [dict(value) for value in target.get("task_references", [])]
        refs = [value for value in refs if value.get("task_id") != mutation.task_id]
        if operation == "link_task":
            if mutation.task_role is None:
                raise ProjectStructureError("project_structure_task_role_required")
            refs.append({"task_id": mutation.task_id, "role": mutation.task_role})
            refs[-1]["classification_at_link"] = target.get("classification")
        target["task_references"] = refs
        return values, node_id
    if operation in {"link_test", "unlink_test"}:
        if mutation.test_id is None:
            raise ProjectStructureError("project_structure_test_id_required")
        refs = [dict(value) for value in target.get("test_references", [])]
        refs = [value for value in refs if value.get("test_id") != mutation.test_id]
        if operation == "link_test":
            if mutation.test_role is None:
                raise ProjectStructureError("project_structure_test_role_required")
            refs.append({"test_id": mutation.test_id, "role": mutation.test_role})
            refs[-1]["classification_at_link"] = target.get("classification")
        target["test_references"] = refs
        return values, node_id
    if mutation.evidence_id is None:
        raise ProjectStructureError("project_structure_evidence_id_required")
    evidence_ids = [str(value) for value in target.get("evidence_ids", [])]
    if operation == "link_evidence" and mutation.evidence_id not in evidence_ids:
        evidence_ids.append(mutation.evidence_id)
    if operation == "unlink_evidence":
        evidence_ids = [
            value for value in evidence_ids if value != mutation.evidence_id
        ]
    target["evidence_ids"] = evidence_ids
    return values, node_id


def apply_project_structure_batch(
    nodes: list[Any] | None,
    batch: ProjectStructureBatch | dict[str, Any],
) -> tuple[list[dict[str, Any]], list[str], bool]:
    """Apply every operation in-memory and validate once before persistence."""

    canonical = validate_project_structure(nodes) or []
    parsed_batch = (
        batch
        if isinstance(batch, ProjectStructureBatch)
        else ProjectStructureBatch.model_validate(batch)
    )
    working = canonical
    entity_ids: list[str] = []
    try:
        for operation in parsed_batch.operations:
            working, entity_id = _apply_one(working, operation)
            entity_ids.append(entity_id)
        final = validate_project_structure(working) or []
    except ValidationError as exc:
        raise ProjectStructureValidationError(
            [
                _issue(
                    "project_structure_mutation_invalid",
                    path=".".join(str(item) for item in error.get("loc", ())),
                    message=error.get("msg", "invalid mutation"),
                )
                for error in exc.errors(include_url=False)
            ]
        ) from exc
    return final, entity_ids, final != canonical or nodes is None


def _tree_order(
    nodes: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    children: dict[str | None, list[dict[str, Any]]] = {}
    for node in nodes:
        if node.get("status", "active") == "active":
            children.setdefault(node.get("parent_id"), []).append(node)
    for values in children.values():
        values.sort(key=lambda item: (int(item["position"]), str(item["id"])))
    ordered: list[dict[str, Any]] = []
    depths: dict[str, int] = {}

    def walk(node: dict[str, Any], depth: int) -> None:
        ordered.append(node)
        depths[str(node["id"])] = depth
        for child in children.get(str(node["id"]), []):
            walk(child, depth + 1)

    for root in children.get(None, []):
        walk(root, 1)
    return ordered, depths


def project_structure_management_nodes(nodes: list[Any] | None) -> list[dict[str, Any]]:
    """Deterministic all-status ordering for authoring and restore surfaces."""

    canonical = validate_project_structure(nodes) or []
    children: dict[str | None, list[dict[str, Any]]] = {}
    for node in canonical:
        children.setdefault(node.get("parent_id"), []).append(node)
    for values in children.values():
        values.sort(
            key=lambda item: (
                int(item.get("position", 0)),
                str(item.get("status", "active")),
                str(item["id"]),
            )
        )
    ordered: list[dict[str, Any]] = []
    visited: set[str] = set()

    def walk(node: dict[str, Any]) -> None:
        node_id = str(node["id"])
        if node_id in visited:
            return
        visited.add(node_id)
        ordered.append(node)
        for child in children.get(node_id, []):
            walk(child)

    for root in children.get(None, []):
        walk(root)
    for node in sorted(
        canonical,
        key=lambda item: (
            str(item.get("parent_id") or ""),
            int(item.get("position", 0)),
            str(item["id"]),
        ),
    ):
        walk(node)
    return ordered


def project_project_structure(
    nodes: list[Any] | None,
    *,
    structure_revision: int,
    spec_id: str,
    spec_version: int,
    reference_type: Literal["task", "test"],
    reference_id: str,
    previous_nodes: list[Any] | None = None,
) -> ProjectStructureProjection:
    canonical = validate_project_structure(nodes)
    clean_reference = _clean_reference_id(reference_id)
    if canonical is None:
        return ProjectStructureProjection(
            state="not_authored",
            spec_id=_clean_reference_id(spec_id),
            spec_version=spec_version,
            authored=False,
            structure_revision=structure_revision,
            reference_type=reference_type,
            reference_id=clean_reference,
        )
    by_id = {str(node["id"]): node for node in canonical}
    ref_field = "task_references" if reference_type == "task" else "test_references"
    id_field = "task_id" if reference_type == "task" else "test_id"
    direct_roles: dict[str, str] = {}
    affected: list[ProjectStructureAffectedReference] = []
    for node in canonical:
        matching = [
            ref
            for ref in node.get(ref_field, [])
            if ref.get(id_field) == clean_reference
        ]
        if not matching:
            continue
        node_id = str(node["id"])
        if node.get("status", "active") != "active":
            affected.append(
                ProjectStructureAffectedReference(
                    node_id=node_id,
                    state="unavailable",
                    reason="node_revoked",
                    classification=node.get("classification"),
                )
            )
        else:
            direct_roles[node_id] = str(matching[0]["role"])
            linked_classification = matching[0].get("classification_at_link")
            if linked_classification is not None and linked_classification != node.get(
                "classification"
            ):
                affected.append(
                    ProjectStructureAffectedReference(
                        node_id=node_id,
                        state="classification_changed",
                        reason="node_reclassified_since_link",
                        classification=node.get("classification"),
                    )
                )
    if previous_nodes is not None:
        previous = validate_project_structure(previous_nodes) or []
        previous_direct = {
            str(node["id"]): node
            for node in previous
            if any(
                ref.get(id_field) == clean_reference for ref in node.get(ref_field, [])
            )
        }
        affected_ids = {item.node_id for item in affected}
        for node_id, previous_node in previous_direct.items():
            current = by_id.get(node_id)
            if current is None or current.get("status", "active") != "active":
                if node_id not in affected_ids:
                    affected.append(
                        ProjectStructureAffectedReference(
                            node_id=node_id,
                            state="unavailable",
                            reason="node_removed_or_revoked",
                            classification=previous_node.get("classification"),
                        )
                    )
                continue
            if current.get("classification") != previous_node.get("classification"):
                affected.append(
                    ProjectStructureAffectedReference(
                        node_id=node_id,
                        state="classification_changed",
                        reason="node_reclassified",
                        classification=current.get("classification"),
                    )
                )
    included = set(direct_roles)
    for node_id in tuple(direct_roles):
        current = by_id[node_id]
        while current.get("parent_id") is not None:
            parent_id = str(current["parent_id"])
            parent = by_id.get(parent_id)
            if parent is None or parent.get("status", "active") != "active":
                break
            included.add(parent_id)
            current = parent
    ordered, depths = _tree_order(canonical)
    projected = [
        ProjectStructureProjectionNode(
            node=ProjectStructureNode.model_validate(node),
            depth=depths[str(node["id"])],
            direct=str(node["id"]) in direct_roles,
            context_only=str(node["id"]) not in direct_roles,
            reference_role=direct_roles.get(str(node["id"])),
        )
        for node in ordered
        if str(node["id"]) in included
    ]
    return ProjectStructureProjection(
        state=(
            "authored_empty"
            if not canonical
            else ("projected" if projected else "no_direct_references")
        ),
        spec_id=_clean_reference_id(spec_id),
        spec_version=spec_version,
        authored=True,
        structure_revision=structure_revision,
        digest=canonical_project_structure_digest(canonical),
        reference_type=reference_type,
        reference_id=clean_reference,
        nodes=projected,
        affected_references=affected,
    )


def project_structure_export_payload(
    nodes: list[Any] | None,
    *,
    structure_revision: int,
) -> dict[str, Any] | None:
    """Whole-Spec export section; UI collapse state never participates."""

    canonical = validate_project_structure(nodes)
    if canonical is None:
        return None
    ordered, _depths = _tree_order(canonical)
    return {
        "contract_version": PROJECT_STRUCTURE_EXPORT_SCHEMA_VERSION,
        "authored": True,
        "structure_revision": structure_revision,
        "structure_digest": canonical_project_structure_digest(canonical),
        "active_node_count": len(ordered),
        "nodes": ordered,
    }


def project_structure_snapshot(
    nodes: list[Any] | None,
    *,
    spec_id: str,
    spec_version: int,
    structure_revision: int,
) -> ProjectStructureSnapshot:
    """Return the stable all-status management envelope used by REST and MCP."""

    canonical = validate_project_structure(nodes)
    if canonical is None:
        return ProjectStructureSnapshot(
            state="not_authored",
            spec_id=_clean_reference_id(spec_id),
            spec_version=spec_version,
            authored=False,
            structure_revision=structure_revision,
        )
    ordered = project_structure_management_nodes(canonical)
    return ProjectStructureSnapshot(
        state="authored_empty" if not ordered else "authored",
        spec_id=_clean_reference_id(spec_id),
        spec_version=spec_version,
        authored=True,
        structure_revision=structure_revision,
        digest=canonical_project_structure_digest(canonical),
        nodes=[ProjectStructureNode.model_validate(node) for node in ordered],
    )


def project_structure_reference_ids(
    nodes: list[Any] | None,
) -> tuple[set[str], set[str], set[str]]:
    canonical = validate_project_structure(nodes) or []
    task_ids = {
        str(ref["task_id"]) for node in canonical for ref in node["task_references"]
    }
    test_ids = {
        str(ref["test_id"]) for node in canonical for ref in node["test_references"]
    }
    evidence_ids = {str(value) for node in canonical for value in node["evidence_ids"]}
    return task_ids, test_ids, evidence_ids


__all__ = [
    "PROJECT_STRUCTURE_CONTRACT_VERSION",
    "PROJECT_STRUCTURE_EXPORT_SCHEMA_VERSION",
    "PROJECT_STRUCTURE_MAX_ACTIVE_NODES",
    "PROJECT_STRUCTURE_MAX_DEPTH",
    "ProjectStructureAffectedReference",
    "ProjectStructureBatch",
    "ProjectStructureClassification",
    "ProjectStructureError",
    "ProjectStructureMutation",
    "ProjectStructureNode",
    "ProjectStructureNodeKind",
    "ProjectStructureNodeState",
    "ProjectStructureNodeStatus",
    "ProjectStructureProjection",
    "ProjectStructureProjectionNode",
    "ProjectStructureRemovalBlocked",
    "ProjectStructureSnapshot",
    "ProjectStructureTaskReference",
    "ProjectStructureTaskRole",
    "ProjectStructureTestReference",
    "ProjectStructureTestRole",
    "ProjectStructureValidationError",
    "apply_project_structure_batch",
    "canonical_project_structure_digest",
    "project_project_structure",
    "project_structure_export_payload",
    "project_structure_management_nodes",
    "project_structure_reference_ids",
    "project_structure_removal_impact",
    "project_structure_snapshot",
    "validate_project_structure",
]
