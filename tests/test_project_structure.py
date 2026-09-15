from __future__ import annotations

import copy
from datetime import UTC, datetime

import pytest

from okto_pulse.core.domain.permissions import (
    PROJECT_STRUCTURE_ENTITY_OPERATIONS,
    structured_spec_entity_permission_flags,
)
from okto_pulse.core.domain.project_structure import (
    PROJECT_STRUCTURE_MAX_ACTIVE_NODES,
    ProjectStructureBatch,
    ProjectStructureError,
    ProjectStructureRemovalBlocked,
    ProjectStructureValidationError,
    apply_project_structure_batch,
    project_project_structure,
    project_structure_export_payload,
    project_structure_snapshot,
    validate_project_structure,
)
from okto_pulse.core.models.schemas import SpecResponse


def _node(
    node_id: str,
    *,
    parent_id: str | None = None,
    position: int = 0,
    kind: str = "folder",
    classification: str = "as_is",
    **values: object,
) -> dict[str, object]:
    return {
        "id": node_id,
        "parent_id": parent_id,
        "position": position,
        "kind": kind,
        "name": values.pop("name", node_id),
        "classification": classification,
        **values,
    }


def test_optional_aggregate_preserves_not_authored_and_authored_empty() -> None:
    not_authored = project_structure_snapshot(
        None,
        spec_id="spec-1",
        spec_version=4,
        structure_revision=0,
    )
    authored_empty = project_structure_snapshot(
        [],
        spec_id="spec-1",
        spec_version=5,
        structure_revision=1,
    )

    assert not_authored.state == "not_authored"
    assert not_authored.authored is False
    assert not_authored.digest is None
    assert authored_empty.state == "authored_empty"
    assert authored_empty.authored is True
    assert authored_empty.nodes == []
    assert authored_empty.digest is not None
    assert project_structure_export_payload(None, structure_revision=0) is None


def test_collective_reorder_does_not_require_an_entity_id() -> None:
    current = [
        _node("psn_first", position=0, kind="file"),
        _node("psn_second", position=1, kind="file"),
    ]

    reordered, entity_ids, changed = apply_project_structure_batch(
        current,
        {
            "operations": [
                {
                    "operation": "reorder",
                    "payload": {
                        "parent_id": None,
                        "ordered_ids": ["psn_second", "psn_first"],
                    },
                }
            ]
        },
    )

    assert entity_ids == ["__collection__"]
    assert changed is True
    assert [
        node["id"] for node in sorted(reordered, key=lambda node: node["position"])
    ] == ["psn_second", "psn_first"]


def test_spec_response_normalizes_legacy_null_structure_revision() -> None:
    now = datetime.now(UTC)

    response = SpecResponse.model_validate(
        {
            "id": "spec-legacy",
            "board_id": "board-1",
            "title": "Legacy Spec",
            "description": None,
            "context": None,
            "functional_requirements": [],
            "technical_requirements": [],
            "acceptance_criteria": [],
            "project_structure": None,
            "project_structure_revision": None,
            "project_structure_digest": None,
            "status": "draft",
            "edition": 1,
            "version": 1,
            "assignee_id": None,
            "created_by": "agent-1",
            "created_at": now,
            "updated_at": now,
            "labels": None,
        }
    )

    assert response.project_structure_revision == 0


def test_closed_node_validation_normalizes_before_enforcing_limits() -> None:
    canonical = validate_project_structure(
        [
            _node(
                "psn_root",
                name=f"  {'x' * 255}  ",
                note="  one useful note  ",
            )
        ]
    )
    assert canonical is not None
    assert canonical[0]["name"] == "x" * 255
    assert canonical[0]["note"] == "one useful note"

    with pytest.raises(ProjectStructureValidationError):
        validate_project_structure([_node("psn_bad", unknown=True)])
    with pytest.raises(ProjectStructureValidationError):
        validate_project_structure([_node("psn_bad", name=" x " * 256)])
    with pytest.raises(ProjectStructureValidationError):
        validate_project_structure([_node("psn_bad", name=123)])


def test_classification_and_evidence_invariants_are_fail_closed() -> None:
    with pytest.raises(ProjectStructureValidationError):
        validate_project_structure(
            [_node("psn_plan", classification="to_be", evidence_ids=["ev-1"])]
        )
    with pytest.raises(ProjectStructureValidationError):
        validate_project_structure(
            [_node("psn_scaffold", classification="reference_scaffold")]
        )
    with pytest.raises(ProjectStructureValidationError):
        validate_project_structure(
            [_node("psn_as_is", interpretation_limit="not applicable")]
        )

    canonical = validate_project_structure(
        [
            _node(
                "psn_scaffold",
                classification="reference_scaffold",
                interpretation_limit="  Layout only; behavior is not implied.  ",
            )
        ]
    )
    assert canonical is not None
    assert canonical[0]["interpretation_limit"] == (
        "Layout only; behavior is not implied."
    )


def test_tree_limits_and_parent_rules_are_atomic() -> None:
    too_many = [
        _node(f"psn_{index}", position=index)
        for index in range(PROJECT_STRUCTURE_MAX_ACTIVE_NODES + 1)
    ]
    with pytest.raises(ProjectStructureValidationError) as exc_info:
        validate_project_structure(too_many)
    assert any(
        issue["code"] == "project_structure_node_limit_exceeded"
        for issue in exc_info.value.issues
    )

    too_deep = [_node("psn_0")]
    too_deep.extend(
        _node(f"psn_{index}", parent_id=f"psn_{index - 1}") for index in range(1, 21)
    )
    with pytest.raises(ProjectStructureValidationError) as depth_error:
        validate_project_structure(too_deep)
    assert any(
        issue["code"] == "project_structure_depth_exceeded"
        for issue in depth_error.value.issues
    )

    with pytest.raises(ProjectStructureValidationError):
        validate_project_structure(
            [
                _node("psn_file", kind="file"),
                _node("psn_child", parent_id="psn_file"),
            ]
        )


def test_move_to_occupied_position_preserves_requested_order() -> None:
    nodes = [
        _node("psn_a", position=0),
        _node("psn_b", position=1),
        _node("psn_c", position=2),
    ]
    updated, _, changed = apply_project_structure_batch(
        nodes,
        {
            "operations": [
                {
                    "operation": "update",
                    "entity_id": "psn_c",
                    "payload": {"position": 0},
                }
            ]
        },
    )
    assert changed is True
    assert [
        node["id"] for node in sorted(updated, key=lambda item: int(item["position"]))
    ] == ["psn_c", "psn_a", "psn_b"]


def test_batch_failure_does_not_mutate_input_or_apply_partial_change() -> None:
    nodes = [_node("psn_root", note="before")]
    original = copy.deepcopy(nodes)
    batch = ProjectStructureBatch.model_validate(
        {
            "operations": [
                {
                    "operation": "update",
                    "entity_id": "psn_root",
                    "payload": {"note": "after"},
                },
                {
                    "operation": "link_task",
                    "entity_id": "psn_root",
                    "task_id": "task-1",
                },
            ]
        }
    )
    with pytest.raises(ProjectStructureError):
        apply_project_structure_batch(nodes, batch)
    assert nodes == original


def test_non_empty_folder_never_silently_cascades() -> None:
    nodes = [
        _node("psn_root"),
        _node("psn_child", parent_id="psn_root", kind="file"),
    ]
    with pytest.raises(ProjectStructureRemovalBlocked) as exc_info:
        apply_project_structure_batch(
            nodes,
            {"operations": [{"operation": "revoke", "entity_id": "psn_root"}]},
        )
    assert exc_info.value.impact["descendant_ids"] == ["psn_child"]


def test_management_read_includes_revoked_node_and_restore_is_explicit() -> None:
    nodes = [
        _node("psn_a", position=0, kind="file"),
        _node("psn_b", position=1, kind="file"),
    ]
    revoked, _, _ = apply_project_structure_batch(
        nodes,
        {"operations": [{"operation": "revoke", "entity_id": "psn_a"}]},
    )
    snapshot = project_structure_snapshot(
        revoked,
        spec_id="spec-1",
        spec_version=2,
        structure_revision=2,
    )
    assert [(node.id, node.status) for node in snapshot.nodes] == [
        ("psn_b", "active"),
        ("psn_a", "revoked"),
    ]

    restored, _, _ = apply_project_structure_batch(
        revoked,
        {"operations": [{"operation": "restore", "entity_id": "psn_a", "position": 1}]},
    )
    active = sorted(restored, key=lambda item: int(item["position"]))
    assert [(node["id"], node["status"]) for node in active] == [
        ("psn_b", "active"),
        ("psn_a", "active"),
    ]


def test_projection_contains_only_direct_nodes_and_ancestors_with_affected_refs() -> (
    None
):
    nodes = validate_project_structure(
        [
            _node("psn_root"),
            _node(
                "psn_file",
                parent_id="psn_root",
                kind="file",
                task_references=[{"task_id": "task-1", "role": "modify"}],
            ),
            _node("psn_other", position=1, kind="file"),
        ]
    )
    assert nodes is not None
    projected = project_project_structure(
        nodes,
        spec_id="spec-1",
        spec_version=4,
        structure_revision=3,
        reference_type="task",
        reference_id="task-1",
    )
    assert projected.state == "projected"
    assert [
        (item.node.id, item.direct, item.context_only) for item in projected.nodes
    ] == [
        ("psn_root", False, True),
        ("psn_file", True, False),
    ]
    assert projected.nodes[-1].reference_role == "modify"

    reclassified, _, _ = apply_project_structure_batch(
        nodes,
        {
            "operations": [
                {
                    "operation": "update",
                    "entity_id": "psn_file",
                    "payload": {"classification": "to_be"},
                }
            ]
        },
    )
    changed = project_project_structure(
        reclassified,
        spec_id="spec-1",
        spec_version=5,
        structure_revision=4,
        reference_type="task",
        reference_id="task-1",
    )
    assert [(item.node_id, item.state) for item in changed.affected_references] == [
        ("psn_file", "classification_changed")
    ]

    revoked, _, _ = apply_project_structure_batch(
        nodes,
        {"operations": [{"operation": "revoke", "entity_id": "psn_file"}]},
    )
    unavailable = project_project_structure(
        revoked,
        spec_id="spec-1",
        spec_version=5,
        structure_revision=4,
        reference_type="task",
        reference_id="task-1",
    )
    assert unavailable.nodes == []
    assert unavailable.affected_references[0].state == "unavailable"


def test_export_is_active_deterministic_preorder_and_ignores_ui_collapse() -> None:
    nodes = [
        _node("psn_child", parent_id="psn_root", kind="file"),
        _node("psn_root"),
        _node("psn_revoked", position=1, status="revoked", kind="file"),
    ]
    payload = project_structure_export_payload(nodes, structure_revision=7)
    assert payload is not None
    assert payload["active_node_count"] == 2
    assert [node["id"] for node in payload["nodes"]] == [
        "psn_root",
        "psn_child",
    ]


def test_permission_registry_exposes_concrete_leaves_but_no_batch_super_permission() -> (
    None
):
    flags = set(structured_spec_entity_permission_flags())
    assert {
        f"spec.structured_entity.project_structure_node.{operation}"
        for operation in PROJECT_STRUCTURE_ENTITY_OPERATIONS
    }.issubset(flags)
    assert "spec.structured_entity.project_structure_node.batch" not in flags
