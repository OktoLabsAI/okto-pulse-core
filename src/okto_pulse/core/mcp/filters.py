"""Server-side validation of filter combinations per entity_type.

Spec P0.B (consolidated list handlers) — TR-B2.

This module is the single source of truth for which filter keys are
accepted by each of the four polymorphic handlers:

* ``okto_pulse_list_by_board``   — scope "by_board"
* ``okto_pulse_list_qa``         — scope "qa"
* ``okto_pulse_list_knowledge``  — scope "knowledge"
* ``okto_pulse_list_snapshots``  — no per-key filter validation (empty
  entity-type map → all filters rejected; snapshots take no filter dict)

Design rationale
----------------
Centralising allowed-key tables here (rather than inline in server.py)
makes it straightforward to extend or audit filter contracts without
touching the 14 000-line server module.  Each handler calls
``validate_filters(entity_type, filters, scope)`` and returns a structured
error immediately when the result is ``(False, reason)``.

Adding a new allowed filter key: add it to the appropriate sub-dict here
and update the matching service layer.  No changes to server.py are needed
unless the service signature changes.
"""

from __future__ import annotations

import json
from typing import Any, Literal, Optional, TypeAlias, TypedDict


BoardEntityType: TypeAlias = Literal[
    "spec", "ideation", "refinement", "sprint", "story", "topic"
]
QaEntityType: TypeAlias = Literal["spec", "ideation", "refinement"]
KnowledgeEntityType: TypeAlias = Literal["spec", "ideation", "refinement", "card"]
SnapshotEntityType: TypeAlias = Literal["ideation", "refinement"]


class SpecBoardFilters(TypedDict, total=False):
    status: str
    labels: list[str]
    assignee_id: str
    include_archived: bool


class IdeationBoardFilters(TypedDict, total=False):
    status: str
    labels: list[str]
    derivation_pending: bool
    include_archived: bool


class RefinementBoardFilters(IdeationBoardFilters, total=False):
    ideation_id: str


class SprintBoardFilters(TypedDict, total=False):
    status: str
    spec_id: str
    include_archived: bool


class StoryBoardFilters(TypedDict, total=False):
    status: str
    topic_id: str
    linked: bool
    converted: bool
    include_archived: bool


class TopicBoardFilters(TypedDict, total=False):
    include_archived: bool


BoardListFilters: TypeAlias = (
    SpecBoardFilters
    | IdeationBoardFilters
    | RefinementBoardFilters
    | SprintBoardFilters
    | StoryBoardFilters
    | TopicBoardFilters
)


class QaFilters(TypedDict, total=False):
    status: str
    asked_by: str


class SprintQaFilters(TypedDict, total=False):
    status: str


QaListFilters: TypeAlias = QaFilters | SprintQaFilters


class KnowledgeFilters(TypedDict, total=False):
    mime_type: str

# ---------------------------------------------------------------------------
# Allowed filter key tables
# ---------------------------------------------------------------------------

# okto_pulse_list_by_board
ALLOWED_FILTERS_BY_BOARD: dict[str, list[str]] = {
    "spec": ["status", "labels", "assignee_id", "include_archived"],
    "ideation": ["status", "labels", "derivation_pending", "include_archived"],
    "refinement": [
        "status",
        "labels",
        "ideation_id",
        "derivation_pending",
        "include_archived",
    ],
    "sprint": ["status", "spec_id", "include_archived"],
    "story": ["status", "topic_id", "linked", "converted", "include_archived"],
    "topic": ["include_archived"],
}

# okto_pulse_list_qa
ALLOWED_FILTERS_QA: dict[str, list[str]] = {
    "spec": ["status", "asked_by"],
    "ideation": ["status", "asked_by"],
    "refinement": ["status", "asked_by"],
    "sprint": ["status"],
}

# okto_pulse_list_knowledge
ALLOWED_FILTERS_KNOWLEDGE: dict[str, list[str]] = {
    "spec": ["mime_type"],
    "ideation": ["mime_type"],
    "refinement": ["mime_type"],
    "card": ["mime_type"],
}

# okto_pulse_list_snapshots — snapshots take no filter dict at all
ALLOWED_FILTERS_SNAPSHOTS: dict[str, list[str]] = {
    "ideation": [],
    "refinement": [],
}

# Aggregate lookup used by validate_filters()
_SCOPE_MAP: dict[str, dict[str, list[str]]] = {
    "by_board": ALLOWED_FILTERS_BY_BOARD,
    "qa": ALLOWED_FILTERS_QA,
    "knowledge": ALLOWED_FILTERS_KNOWLEDGE,
    "snapshots": ALLOWED_FILTERS_SNAPSHOTS,
}

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_filter_payload(raw: dict[str, Any] | str | None) -> dict[str, Any]:
    """Decode the explicit legacy JSON form and require an object payload."""

    if raw is None:
        return {}
    decoded: Any = raw
    if isinstance(raw, str):
        if not raw.strip():
            return {}
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Invalid JSON filter object: {exc.msg} (at pos {exc.pos})"
            ) from exc
    if not isinstance(decoded, dict):
        raise ValueError(
            f"filters must be an object, got {type(decoded).__name__}"
        )
    return dict(decoded)


def validate_filters(
    entity_type: str,
    filters: dict,
    scope: str = "by_board",
) -> tuple[bool, Optional[str]]:
    """Validate that filter keys are allowed for the given entity_type.

    Parameters
    ----------
    entity_type:
        The entity discriminator (e.g. ``"spec"``, ``"ideation"``).
    filters:
        The raw filter dict supplied by the caller.  May be ``None`` or
        empty — both are accepted without any key inspection.
    scope:
        Which handler is validating.  One of ``"by_board"``, ``"qa"``,
        ``"knowledge"``, ``"snapshots"``.

    Returns
    -------
    ``(True, None)``
        All filter keys are in the allow-list (or ``filters`` is empty).
    ``(False, reason)``
        At least one key is not allowed; ``reason`` describes the problem.

    Notes
    -----
    Unknown *scope* values result in an empty allow-list, so every non-empty
    filter dict is rejected.  This is intentional — it prevents silent
    pass-through when a new handler forgets to register its scope.
    """
    if not filters:
        return True, None

    scope_table = _SCOPE_MAP.get(scope)
    if scope_table is None:
        return (
            False,
            f"Unknown validation scope '{scope}'. "
            f"Allowed scopes: {sorted(_SCOPE_MAP.keys())}",
        )

    allowed: list[str] = scope_table.get(entity_type, [])
    invalid_keys = [k for k in filters.keys() if k not in allowed]

    if invalid_keys:
        return (
            False,
            (
                f"Invalid filter keys for entity_type='{entity_type}' "
                f"(scope='{scope}'): {invalid_keys}. "
                f"Allowed: {allowed}"
            ),
        )

    return True, None


def supported_filter_keys(entity_type: str, scope: str = "by_board") -> list[str]:
    """Return the allow-listed filter keys for an entity/scope pair."""
    return list(_SCOPE_MAP.get(scope, {}).get(entity_type, []))


def invalid_filter_keys(
    entity_type: str,
    filters: dict | None,
    scope: str = "by_board",
) -> list[str]:
    """Return caller-supplied keys that are not in the central allow-list."""
    if not filters:
        return []
    allowed = set(supported_filter_keys(entity_type, scope=scope))
    return [key for key in filters.keys() if key not in allowed]


def supported_entity_types(scope: str = "by_board") -> list[str]:
    """Return the entity types recognised by a given handler scope.

    Useful for generating ``SUPPORTED`` lists inside handler bodies
    without hard-coding them.
    """
    return sorted(_SCOPE_MAP.get(scope, {}).keys())
