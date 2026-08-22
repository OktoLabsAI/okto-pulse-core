"""Closed public error projection for operational Spec dependencies.

``SpecDependencyOperationError`` deliberately carries rich, transport-neutral
diagnostics inside the application.  Inbound adapters must not serialize that
object directly: callers could otherwise observe a newly introduced error code,
free-form exception text, or unbounded facts before the public contract has been
reviewed.  This module is the single allowlisted REST/MCP projection boundary.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.spec_dependency import SpecDependencyOperationError


class SpecDependencyErrorCategory(str, Enum):
    """Transport-neutral categories with deterministic HTTP semantics."""

    INVALID_ARGUMENT = "invalid_argument"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    INTERNAL_ERROR = "internal_error"


SPEC_DEPENDENCY_HTTP_STATUS_BY_CATEGORY: dict[
    SpecDependencyErrorCategory,
    int,
] = {
    SpecDependencyErrorCategory.INVALID_ARGUMENT: 400,
    SpecDependencyErrorCategory.NOT_FOUND: 404,
    SpecDependencyErrorCategory.CONFLICT: 409,
    SpecDependencyErrorCategory.INTERNAL_ERROR: 500,
}


@dataclass(frozen=True, slots=True)
class _PublicErrorDefinition:
    category: SpecDependencyErrorCategory
    message: str
    retryable: bool = False


_PUBLIC_BY_CODE: dict[str, _PublicErrorDefinition] = {
    "invalid_spec_dependency_request": _PublicErrorDefinition(
        SpecDependencyErrorCategory.INVALID_ARGUMENT,
        "Spec dependency request is invalid.",
    ),
    "invalid_cursor": _PublicErrorDefinition(
        SpecDependencyErrorCategory.INVALID_ARGUMENT,
        "The Spec dependency cursor is invalid for this query.",
    ),
    "spec_dependency_self_reference": _PublicErrorDefinition(
        SpecDependencyErrorCategory.INVALID_ARGUMENT,
        "A Spec cannot depend on itself.",
    ),
    "dependency_target_unavailable": _PublicErrorDefinition(
        SpecDependencyErrorCategory.NOT_FOUND,
        "Dependency target is unavailable.",
    ),
    "spec_dependency_not_found": _PublicErrorDefinition(
        SpecDependencyErrorCategory.NOT_FOUND,
        "Active Spec dependency was not found.",
    ),
    "cross_board_dependency_forbidden": _PublicErrorDefinition(
        SpecDependencyErrorCategory.CONFLICT,
        "Operational Spec dependencies cannot cross board boundaries.",
    ),
    "spec_dependency_state_conflict": _PublicErrorDefinition(
        SpecDependencyErrorCategory.CONFLICT,
        "Spec dependency operation conflicts with the current state.",
    ),
    "spec_dependency_version_conflict": _PublicErrorDefinition(
        SpecDependencyErrorCategory.CONFLICT,
        "Spec changed after the dependency request was prepared.",
        retryable=True,
    ),
    "spec_dependency_cycle": _PublicErrorDefinition(
        SpecDependencyErrorCategory.CONFLICT,
        "This dependency would create a cycle between Specs.",
    ),
    "spec_dependencies_incomplete": _PublicErrorDefinition(
        SpecDependencyErrorCategory.CONFLICT,
        "Spec prerequisites are not complete.",
    ),
}

SPEC_DEPENDENCY_PUBLIC_ERROR_CODES = frozenset(_PUBLIC_BY_CODE)

_INTERNAL_ERROR = _PublicErrorDefinition(
    SpecDependencyErrorCategory.INTERNAL_ERROR,
    "Spec dependency operation could not be completed.",
)
_INTERNAL_ERROR_CODE = "spec_dependency_internal_error"

_PUBLIC_REMEDIATIONS = frozenset(
    {
        "choose_a_non_descendant_prerequisite",
        "choose_a_prerequisite_from_the_same_board",
        "complete_blocking_specs",
        "complete_target_spec_or_return_source_to_draft",
        "refresh_spec",
        "remove_incoming_dependencies",
        "restore_archived_prerequisites_or_remove_dependencies",
        "restore_spec",
        "use_a_new_idempotency_key",
        "use_the_existing_dependency",
    }
)

# Some codes cover several internal state-conflict causes.  A reviewed
# remediation is a bounded discriminator that lets the public message remain
# useful without echoing the exception's free-form message.
_MESSAGE_BY_CODE_AND_REMEDIATION: dict[tuple[str, str], str] = {
    ("spec_dependency_state_conflict", "restore_spec"): (
        "Archived Specs cannot mutate operational dependencies."
    ),
    ("spec_dependency_state_conflict", "refresh_spec"): (
        "The Spec lifecycle edition changed while processing the dependency."
    ),
    ("spec_dependency_state_conflict", "use_a_new_idempotency_key"): (
        "The idempotency key was already used with a different request."
    ),
    ("spec_dependency_state_conflict", "use_the_existing_dependency"): (
        "An active dependency between these Specs already exists."
    ),
    (
        "spec_dependency_state_conflict",
        "complete_target_spec_or_return_source_to_draft",
    ): "After execution starts, a new prerequisite must already be Done.",
    ("spec_dependency_state_conflict", "remove_incoming_dependencies"): (
        "Another active Spec depends on this target."
    ),
    ("spec_dependencies_incomplete", "complete_blocking_specs"): (
        "All prerequisite Specs must be Done before execution can start."
    ),
    (
        "spec_dependencies_incomplete",
        "restore_archived_prerequisites_or_remove_dependencies",
    ): (
        "Restore archived prerequisite Specs or remove their dependencies "
        "before execution can start."
    ),
}

_ID_FACT_KEYS = frozenset(
    {
        "dependency_id",
        "spec_id",
        "source_spec_id",
        "target_spec_id",
    }
)
_COUNT_FACT_KEYS = frozenset(
    {
        "archived_blocking_count",
        "blocking_count",
        "current_spec_edition",
        "current_spec_version",
        "expected_spec_edition",
        "expected_spec_version",
        "incoming_count",
        "incoming_count_lower_bound",
        "limit",
        "spec_edition",
        "unfinished_blocking_count",
    }
)
_BOOL_FACT_KEYS = frozenset(
    {
        "blockers_truncated",
        "incoming_dependencies_truncated",
        "incoming_has_more",
    }
)
_FACT_KEYS_BY_CODE: dict[str, frozenset[str]] = {
    "invalid_spec_dependency_request": frozenset({"limit"}),
    "spec_dependency_self_reference": frozenset({"spec_id"}),
    "spec_dependency_not_found": frozenset({"dependency_id"}),
    "cross_board_dependency_forbidden": frozenset({"spec_id", "target_spec_id"}),
    "spec_dependency_version_conflict": frozenset(
        {
            "spec_id",
            "expected_spec_version",
            "current_spec_version",
        }
    ),
    "spec_dependency_cycle": frozenset({"spec_id", "target_spec_id", "cycle_path"}),
    "spec_dependency_state_conflict": frozenset(
        {
            "conflict_kind",
            "current_spec_edition",
            "dependency_id",
            "expected_spec_edition",
            "incoming_count",
            "incoming_count_lower_bound",
            "incoming_dependencies",
            "incoming_dependencies_truncated",
            "incoming_has_more",
            "operation",
            "spec_edition",
            "spec_id",
            "target_spec_id",
            "target_status",
        }
    ),
    "spec_dependencies_incomplete": frozenset(
        {
            "archived_blocking_count",
            "blocking_count",
            "blocking_dependencies",
            "blockers_truncated",
            "spec_id",
            "unfinished_blocking_count",
        }
    ),
}

_PUBLIC_CONFLICT_KINDS = frozenset({"active_duplicate", "idempotency_key_reuse"})
_PUBLIC_OPERATIONS = frozenset({"archive Spec tree", "delete Spec"})
_PUBLIC_SPEC_STATUSES = frozenset(status.value for status in SpecStatus)
_MAX_IDENTIFIER_LENGTH = 255
_MAX_TITLE_LENGTH = 500
_MAX_DETAIL_ITEMS = 100
_MAX_PUBLIC_INTEGER = (2**63) - 1


def project_spec_dependency_error(error: Exception) -> dict[str, Any]:
    """Return a bounded allowlisted envelope shared by REST and MCP.

    Unknown exception types and unknown ``SpecDependencyOperationError`` codes
    deliberately collapse to one internal error.  Their message, remediation,
    and facts never cross the inbound boundary.
    """

    code = (
        error.code
        if isinstance(error, SpecDependencyOperationError)
        and isinstance(error.code, str)
        else _INTERNAL_ERROR_CODE
    )
    definition = _PUBLIC_BY_CODE.get(code)
    if definition is None:
        return _base_payload(_INTERNAL_ERROR_CODE, _INTERNAL_ERROR)

    remediation = _safe_remediation(error.remediation)
    message = _MESSAGE_BY_CODE_AND_REMEDIATION.get(
        (code, remediation or ""),
        definition.message,
    )
    payload = _base_payload(code, definition, message=message)
    if remediation is not None:
        payload["remediation"] = remediation
    facts = _project_facts(code, error.facts)
    if facts:
        payload["facts"] = facts
    return payload


def spec_dependency_error_category(
    error: Exception,
) -> SpecDependencyErrorCategory:
    """Return the public category, failing closed for an unknown error."""

    if not isinstance(error, SpecDependencyOperationError):
        return SpecDependencyErrorCategory.INTERNAL_ERROR
    definition = _PUBLIC_BY_CODE.get(error.code)
    return (
        definition.category
        if definition is not None
        else SpecDependencyErrorCategory.INTERNAL_ERROR
    )


def spec_dependency_http_status(error: Exception) -> int:
    """Return the intended REST status without importing a web framework."""

    return SPEC_DEPENDENCY_HTTP_STATUS_BY_CATEGORY[
        spec_dependency_error_category(error)
    ]


def _base_payload(
    code: str,
    definition: _PublicErrorDefinition,
    *,
    message: str | None = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "message": message or definition.message,
        "retryable": definition.retryable,
    }


def _safe_remediation(value: object) -> str | None:
    return value if isinstance(value, str) and value in _PUBLIC_REMEDIATIONS else None


def _project_facts(code: str, raw_facts: object) -> dict[str, Any]:
    if not isinstance(raw_facts, Mapping):
        return {}
    allowed_keys = _FACT_KEYS_BY_CODE.get(code, frozenset())
    projected: dict[str, Any] = {}
    for key in allowed_keys:
        if key not in raw_facts:
            continue
        value = raw_facts[key]
        if key in _ID_FACT_KEYS:
            safe_value = _safe_identifier(value)
        elif key in _COUNT_FACT_KEYS:
            safe_value = _safe_non_negative_int(value)
        elif key in _BOOL_FACT_KEYS:
            safe_value = value if isinstance(value, bool) else None
        elif key == "target_status":
            safe_value = value if value in _PUBLIC_SPEC_STATUSES else None
        elif key == "conflict_kind":
            safe_value = value if value in _PUBLIC_CONFLICT_KINDS else None
        elif key == "operation":
            safe_value = value if value in _PUBLIC_OPERATIONS else None
        elif key == "cycle_path":
            safe_value = _project_identifier_list(value)
        elif key == "incoming_dependencies":
            safe_value = _project_dependency_refs(value, incoming=True)
        elif key == "blocking_dependencies":
            safe_value = _project_blocking_dependencies(value)
        else:  # pragma: no cover - exhaustive guard for future allowlist edits
            safe_value = None
        if safe_value is not None:
            projected[key] = safe_value
    return projected


def _safe_identifier(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    if not value or len(value) > _MAX_IDENTIFIER_LENGTH:
        return None
    return value


def _safe_non_negative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if value < 0 or value > _MAX_PUBLIC_INTEGER:
        return None
    return value


def _project_identifier_list(value: object) -> list[str] | None:
    if not isinstance(value, (list, tuple)):
        return None
    projected = [
        safe
        for item in value[:_MAX_DETAIL_ITEMS]
        if (safe := _safe_identifier(item)) is not None
    ]
    return projected


def _project_dependency_refs(
    value: object,
    *,
    incoming: bool,
) -> list[dict[str, str]] | None:
    if not isinstance(value, (list, tuple)):
        return None
    keys = (
        ("dependency_id", "source_spec_id", "target_spec_id")
        if incoming
        else ("dependency_id", "target_spec_id")
    )
    projected: list[dict[str, str]] = []
    for item in value[:_MAX_DETAIL_ITEMS]:
        if not isinstance(item, Mapping):
            continue
        row = {
            key: safe
            for key in keys
            if (safe := _safe_identifier(item.get(key))) is not None
        }
        if row:
            projected.append(row)
    return projected


def _project_blocking_dependencies(
    value: object,
) -> list[dict[str, Any]] | None:
    if not isinstance(value, (list, tuple)):
        return None
    projected: list[dict[str, Any]] = []
    for item in value[:_MAX_DETAIL_ITEMS]:
        if not isinstance(item, Mapping):
            continue
        row: dict[str, Any] = {}
        for key in ("dependency_id", "target_spec_id"):
            safe = _safe_identifier(item.get(key))
            if safe is not None:
                row[key] = safe
        title = item.get("target_title")
        if isinstance(title, str):
            row["target_title"] = title[:_MAX_TITLE_LENGTH]
        status = item.get("target_status")
        if status in _PUBLIC_SPEC_STATUSES:
            row["target_status"] = status
        archived = item.get("target_archived")
        if isinstance(archived, bool):
            row["target_archived"] = archived
        if row:
            projected.append(row)
    return projected


__all__ = [
    "SPEC_DEPENDENCY_HTTP_STATUS_BY_CATEGORY",
    "SPEC_DEPENDENCY_PUBLIC_ERROR_CODES",
    "SpecDependencyErrorCategory",
    "project_spec_dependency_error",
    "spec_dependency_error_category",
    "spec_dependency_http_status",
]
