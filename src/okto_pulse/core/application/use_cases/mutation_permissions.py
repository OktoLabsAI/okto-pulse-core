"""Pure permission-requirement builders shared by REST and MCP mutations."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from okto_pulse.core.application.use_cases.authorization import PermissionRequirement
from okto_pulse.core.domain.sdlc_registry import transition_permission_flag


_CARD_ASSIGN_FIELDS = {"assignee_id", "sprint_id"}
_CARD_LABEL_FIELDS = {"labels"}
_CARD_LINK_SPEC_FIELDS = {"spec_id"}
_CARD_LINK_TEST_FIELDS = {"test_scenario_ids", "linked_test_task_ids"}
_CARD_BUG_FIELDS = {
    "severity",
    "expected_behavior",
    "observed_behavior",
    "steps_to_reproduce",
    "action_plan",
}
_SPRINT_COVERAGE_FIELDS = {
    "skip_test_coverage",
    "skip_rules_coverage",
    "skip_qualitative_validation",
    "validation_threshold",
    "require_task_validation",
    "validation_min_confidence",
    "validation_min_completeness",
    "validation_max_drift",
}
_SPRINT_LABEL_FIELDS = {"labels"}
_SPRINT_NON_MUTATION_FIELDS = {"expected_version"}


def payload_fields_set(data: Any) -> set[str]:
    """Return explicitly supplied payload fields without depending on Pydantic.

    Pydantic v2/v1 field sets take precedence. Mapping and ``model_dump``/``dict``
    payloads keep this helper useful for transport-free test doubles and future
    DTO implementations.
    """

    for attribute in ("model_fields_set", "__fields_set__"):
        fields = getattr(data, attribute, None)
        if fields is not None:
            return {str(field) for field in fields}
    if isinstance(data, Mapping):
        return {str(field) for field in data}
    for method_name in ("model_dump", "dict"):
        method = getattr(data, method_name, None)
        if not callable(method):
            continue
        try:
            values = method(exclude_unset=True)
        except TypeError:
            values = method()
        if isinstance(values, Mapping):
            return {str(field) for field in values}
    values = getattr(data, "__dict__", None)
    if isinstance(values, Mapping):
        return {str(field) for field in values if not str(field).startswith("_")}
    return set()


def entity_state(entity: Any) -> str | None:
    """Return an entity's canonical status value when it has one."""

    status = getattr(entity, "status", None)
    if status is None:
        return None
    value = getattr(status, "value", status)
    return str(value) if value is not None else None


def _requirement(
    operation: str,
    legacy_operation: str,
    *,
    entity: str | None = None,
    state: str | None = None,
) -> PermissionRequirement:
    if entity is None or state is None:
        return PermissionRequirement(operation, legacy_operation=legacy_operation)
    return PermissionRequirement(
        operation,
        legacy_operation=legacy_operation,
        entity=entity,
        state=state,
    )


def card_requirement(
    operation: str,
    *,
    state: str | None = None,
    legacy_operation: str = "cards:update",
) -> PermissionRequirement:
    return _requirement(
        operation,
        legacy_operation,
        entity="card" if state is not None else None,
        state=state,
    )


def sprint_requirement(
    operation: str,
    *,
    state: str | None = None,
    legacy_operation: str = "specs:update",
) -> PermissionRequirement:
    return _requirement(
        operation,
        legacy_operation,
        entity="sprint" if state is not None else None,
        state=state,
    )


def transition_permission_requirement(
    entity: str,
    current_state: Any,
    target_state: Any,
    *,
    legacy_operation: str | None,
) -> PermissionRequirement:
    """Build the exact state-aware requirement for one registered SDLC edge."""

    current = str(getattr(current_state, "value", current_state))
    target = str(getattr(target_state, "value", target_state))
    return PermissionRequirement(
        transition_permission_flag(entity, current, target),
        legacy_operation=legacy_operation,
        entity=entity,
        state=current,
    )


def card_create_permission_requirement(data: Any) -> PermissionRequirement:
    raw_card_type = (
        data.get("card_type", "normal")
        if isinstance(data, Mapping)
        else getattr(data, "card_type", "normal")
    )
    card_type = str(getattr(raw_card_type, "value", raw_card_type)).strip().lower()
    operation = (
        "card.entity.create_test"
        if card_type == "test"
        else "card.entity.create"
    )
    return card_requirement(operation, legacy_operation="cards:create")


def card_update_permission_requirements(
    data: Any,
    *,
    state: str | None = None,
) -> tuple[PermissionRequirement, ...]:
    fields = payload_fields_set(data)
    operations: set[str] = set()
    if fields & _CARD_ASSIGN_FIELDS:
        operations.add("card.entity.assign")
    if fields & _CARD_LABEL_FIELDS:
        operations.add("card.entity.label")
    if fields & _CARD_LINK_SPEC_FIELDS:
        operations.add("card.entity.link_spec")
    if fields & _CARD_LINK_TEST_FIELDS:
        operations.add("card.entity.link_tests")
    if fields & _CARD_BUG_FIELDS:
        operations.add("card.entity.edit_bug_fields")
    categorized = (
        _CARD_ASSIGN_FIELDS
        | _CARD_LABEL_FIELDS
        | _CARD_LINK_SPEC_FIELDS
        | _CARD_LINK_TEST_FIELDS
        | _CARD_BUG_FIELDS
    )
    if not fields or fields - categorized:
        operations.add("card.entity.edit_fields")
    return tuple(card_requirement(operation, state=state) for operation in sorted(operations))


def sprint_update_permission_requirements(
    data: Any,
    *,
    state: str | None = None,
) -> tuple[PermissionRequirement, ...]:
    supplied_fields = payload_fields_set(data)
    fields = supplied_fields - _SPRINT_NON_MUTATION_FIELDS
    if supplied_fields and not fields:
        return ()
    operations: set[str] = set()
    if fields & _SPRINT_COVERAGE_FIELDS:
        operations.add("sprint.entity.edit_coverage_flags")
    if fields & _SPRINT_LABEL_FIELDS:
        operations.add("sprint.entity.label")
    categorized = _SPRINT_COVERAGE_FIELDS | _SPRINT_LABEL_FIELDS
    if not fields or fields - categorized:
        operations.add("sprint.entity.edit_fields")
    return tuple(sprint_requirement(operation, state=state) for operation in sorted(operations))


__all__ = [
    "card_create_permission_requirement",
    "card_requirement",
    "card_update_permission_requirements",
    "entity_state",
    "payload_fields_set",
    "sprint_requirement",
    "sprint_update_permission_requirements",
    "transition_permission_requirement",
]
