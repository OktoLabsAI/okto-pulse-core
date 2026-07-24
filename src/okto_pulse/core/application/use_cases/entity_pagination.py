"""ListEntityUseCase + the FR0 pagination executor (spec 8b33f9a8).

The executor lives in the APPLICATION layer, built purely on the
application-persistence port (no service imports — the purity/boundary
contract of ``application/__init__``), and is driven by the SURFACE CATALOG:
each :class:`SurfaceSpec` pins ONE entity, ONE canonical order sequence
(FR3 — applied BY the executor; callers never choose an ordering) and the
surface's typed scope/filter/include whitelists (FR0 "ListEntityUseCase por
superfície"). The two derived queries — ``scope`` alone drives
``total_overall``; ``scope`` + the discretionary filters drive the page
items and ``total_filtered`` — NEVER materialize the full collection.

Every check is fail-closed: unknown surfaces, un-anchored scopes, off-kind
operators, out-of-catalog enum values, blank ids, empty ``in`` lists, empty
``any_groups`` and undeclared includes are all rejected with typed errors.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from okto_pulse.core.domain.enums import (
    CardPriority,
    CardStatus,
    CardType,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
    SprintStatus,
    StoryStatus,
)
from okto_pulse.core.ports.application_persistence import (
    PAGE_LIMIT_MAX,
    PAGE_LIMIT_MIN,
    PAGE_OFFSET_MAX,
    ApplicationFilter,
    ApplicationGroupCount,
    ApplicationGroupCountQuery,
    ApplicationQuery,
    GroupCountRequest,
    PageRequest,
    PageResult,
    get_application_persistence_port,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

_LIST_ORDER: tuple[tuple[str, bool], ...] = (("updated_at", True), ("id", True))
_KANBAN_ORDER: tuple[tuple[str, bool], ...] = (("position", False), ("id", True))
_MCP_CARD_ORDER: tuple[tuple[str, bool], ...] = (
    ("status", False),
    ("position", False),
    ("id", True),
)
_MCP_SPRINT_ORDER: tuple[tuple[str, bool], ...] = (
    ("created_at", False),
    ("id", True),
)
_MCP_TOPIC_ORDER: tuple[tuple[str, bool], ...] = (
    ("name", False),
    ("id", True),
)
#: Lookup tie-break is id ASC — the APPROVED C4 contract (DDL
#: (board_id, title, id) ASC/ASC; EXPLAIN @5k shows ASC/DESC forces a
#: TEMP B-TREE FOR RIGHT PART OF ORDER BY). The specific approved lookup
#: contract governs over FR3's generic id-DESC phrasing.
_LOOKUP_ORDER: tuple[tuple[str, bool], ...] = (("title", False), ("id", False))


@dataclass(frozen=True, slots=True)
class SurfaceSpec:
    """One pagination surface: entity + canonical order + typed whitelists.

    ``required_scope_fields`` must ALL be present in the scope with valid
    eq predicates (e.g. the Kanban column surface requires ``status`` beside
    ``board_id`` so both totals are column-scoped — a board-only scope would
    silently mix column and board counts).
    """

    entity: str
    order_by: tuple[tuple[str, bool], ...]
    scope_fields: frozenset[str]
    anchor_fields: frozenset[str]
    filter_fields: frozenset[str]
    required_scope_fields: frozenset[str] = field(default_factory=frozenset)
    includes: frozenset[str] = field(default_factory=frozenset)
    select_fields: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GroupCountSurfaceSpec:
    """Typed whitelist for one server-side GROUP BY count surface."""

    entity: str
    scope_fields: frozenset[str]
    anchor_fields: frozenset[str]
    filter_fields: frozenset[str]
    group_by_options: frozenset[tuple[str, ...]]
    required_scope_fields: frozenset[str] = field(default_factory=frozenset)


#: The Core surface catalog (FR0). Each surface has EXACTLY ONE canonical
#: order (FR3) — list surfaces page by (updated_at DESC, id DESC), the Kanban
#: column by (position ASC, id DESC) and the lookups by (title ASC, id ASC —
#: the approved C4 contract) — derived and applied by the executor, never
#: caller-chosen.
SURFACES: dict[str, SurfaceSpec] = {
    "story_list": SurfaceSpec(
        entity="story",
        order_by=_LIST_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        filter_fields=frozenset(
            {
                "status",
                "topic_id",
                "linked",
                "converted",
                "title",
                "description",
                "actor",
                "goal",
                "benefit",
            }
        ),
        select_fields=(
            "id",
            "board_id",
            "topic_id",
            "title",
            "description",
            "actor",
            "goal",
            "benefit",
            "labels",
            "status",
            "assignee_id",
            "created_by",
            "created_at",
            "updated_at",
            "archived",
            "screen_mockups_count",
        ),
    ),
    "ideation_list": SurfaceSpec(
        entity="ideation",
        order_by=_LIST_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        filter_fields=frozenset(
            {"status", "derivation_pending", "title", "description", "labels"}
        ),
        select_fields=(
            "id",
            "board_id",
            "title",
            "description",
            "problem_statement",
            "complexity",
            "status",
            "version",
            "assignee_id",
            "created_by",
            "created_at",
            "updated_at",
            "labels",
            "archived",
            "scope_assessment",
        ),
    ),
    "refinement_list": SurfaceSpec(
        entity="refinement",
        order_by=_LIST_ORDER,
        scope_fields=frozenset({"board_id", "ideation_id", "archived"}),
        anchor_fields=frozenset({"board_id", "ideation_id"}),
        filter_fields=frozenset(
            {"status", "derivation_pending", "title", "description", "labels"}
        ),
        select_fields=(
            "id",
            "ideation_id",
            "board_id",
            "title",
            "description",
            "status",
            "version",
            "assignee_id",
            "created_by",
            "created_at",
            "updated_at",
            "labels",
            "archived",
        ),
    ),
    "refinement_board": SurfaceSpec(
        entity="refinement",
        order_by=_LIST_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        filter_fields=frozenset(
            {
                "status",
                "derivation_pending",
                "title",
                "description",
                "labels",
                "ideation_title",
            }
        ),
        select_fields=(
            "id",
            "ideation_id",
            "board_id",
            "title",
            "description",
            "status",
            "version",
            "assignee_id",
            "created_by",
            "created_at",
            "updated_at",
            "labels",
            "archived",
            "ideation_title",
        ),
    ),
    "spec_list": SurfaceSpec(
        entity="spec",
        order_by=_LIST_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        filter_fields=frozenset({"status", "title", "description", "labels"}),
        select_fields=(
            "id",
            "board_id",
            "ideation_id",
            "refinement_id",
            "title",
            "description",
            "status",
            "version",
            "assignee_id",
            "created_by",
            "created_at",
            "updated_at",
            "labels",
            "archived",
        ),
    ),
    "sprint_list": SurfaceSpec(
        entity="sprint",
        order_by=_LIST_ORDER,
        scope_fields=frozenset({"board_id", "spec_id", "archived"}),
        anchor_fields=frozenset({"board_id", "spec_id"}),
        filter_fields=frozenset({"status", "spec_id", "title"}),
        select_fields=(
            "id",
            "spec_id",
            "board_id",
            "title",
            "description",
            "objective",
            "expected_outcome",
            "status",
            "created_by",
            "created_at",
            "updated_at",
            "archived",
        ),
    ),
    "card_list": SurfaceSpec(
        entity="card",
        order_by=_LIST_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        filter_fields=frozenset(
            {
                "status",
                "spec_id",
                "sprint_id",
                "priority",
                "card_type",
                "assignee_id",
                "labels",
                "title",
                "description",
            }
        ),
        select_fields=(
            "id",
            "board_id",
            "spec_id",
            "sprint_id",
            "title",
            "description",
            "status",
            "priority",
            "card_type",
            "position",
            "assignee_id",
            "labels",
            "archived",
            "created_by",
            "due_date",
            "severity",
            "test_scenario_ids",
            "linked_test_task_ids",
            "validations_count",
            "validations_fail_count",
            "validations_has_pass",
            "first_pass_confidence",
            "first_pass_completeness",
            "first_pass_drift",
            "conclusions_count",
            "last_conclusion_completeness",
            "last_conclusion_drift",
            "created_at",
            "updated_at",
            "open_qa_count",
        ),
    ),
    "kanban_column": SurfaceSpec(
        entity="card",
        order_by=_KANBAN_ORDER,
        scope_fields=frozenset({"board_id", "status", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        # ``status`` is REQUIRED SCOPE here (both totals column-scoped) and
        # therefore NOT a discretionary filter of this surface.
        required_scope_fields=frozenset({"board_id", "status"}),
        filter_fields=frozenset(
            {
                "spec_id",
                "card_type",
                "assignee_id",
                "labels",
                "title",
                "description",
            }
        ),
        select_fields=(
            "id",
            "board_id",
            "spec_id",
            "title",
            "description",
            "status",
            "priority",
            "position",
            "assignee_id",
            "created_by",
            "created_at",
            "updated_at",
            "due_date",
            "labels",
            "test_scenario_ids",
            "conclusions",
            "card_type",
            "origin_task_id",
            "severity",
            "linked_test_task_ids",
            "archived",
            "open_qa_count",
        ),
    ),
    "spec_lookup": SurfaceSpec(
        entity="spec",
        order_by=_LOOKUP_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        filter_fields=frozenset(
            {
                "status",
                "title",
                "linked_to_cards",
                "linked_to_active_cards",
            }
        ),
        select_fields=("id", "title", "status"),
    ),
    "ideation_lookup": SurfaceSpec(
        entity="ideation",
        order_by=_LOOKUP_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        filter_fields=frozenset({"status", "title"}),
        select_fields=("id", "title", "status"),
    ),
    # MCP surfaces deliberately remain separate from REST surfaces: MCP has
    # per-tool ordering and payload compatibility contracts (FR13), while REST
    # uses the summary projections and canonical list order above.
    "mcp_card_status_list": SurfaceSpec(
        entity="card",
        order_by=_MCP_CARD_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        required_scope_fields=frozenset({"board_id"}),
        filter_fields=frozenset({"status", "spec_id", "priority", "assignee_id"}),
        select_fields=(
            "id",
            "title",
            "description",
            "status",
            "priority",
            "position",
            "assignee_id",
            "spec_id",
            "test_scenario_ids",
            "due_date",
            "labels",
            "archived",
        ),
    ),
    "mcp_spec_list": SurfaceSpec(
        entity="spec",
        order_by=_LIST_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        filter_fields=frozenset({"status", "labels", "assignee_id"}),
        select_fields=(
            "id",
            "title",
            "description",
            "status",
            "version",
            "assignee_id",
            "labels",
            "created_at",
            "updated_at",
        ),
    ),
    "mcp_ideation_list": SurfaceSpec(
        entity="ideation",
        order_by=_LIST_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        filter_fields=frozenset({"status", "labels", "derivation_pending"}),
        select_fields=(
            "id",
            "title",
            "description",
            "problem_statement",
            "complexity",
            "status",
            "version",
            "assignee_id",
            "labels",
            "created_at",
            "updated_at",
        ),
    ),
    "mcp_refinement_list": SurfaceSpec(
        entity="refinement",
        order_by=_LIST_ORDER,
        scope_fields=frozenset({"board_id", "ideation_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        required_scope_fields=frozenset({"board_id", "ideation_id"}),
        filter_fields=frozenset({"status", "labels", "derivation_pending"}),
        select_fields=(
            "id",
            "title",
            "description",
            "in_scope",
            "out_of_scope",
            "status",
            "version",
            "assignee_id",
            "labels",
            "created_at",
            "updated_at",
        ),
    ),
    "mcp_sprint_list": SurfaceSpec(
        entity="sprint",
        order_by=_MCP_SPRINT_ORDER,
        scope_fields=frozenset({"board_id", "spec_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        required_scope_fields=frozenset({"board_id", "spec_id"}),
        filter_fields=frozenset({"status"}),
        select_fields=(
            "id",
            "title",
            "status",
            "lane_type",
            "origin_sprint_id",
            "origin_bug_id",
            "spec_version",
            "test_scenario_ids",
            "business_rule_ids",
            "labels",
        ),
    ),
    "mcp_story_list": SurfaceSpec(
        entity="story",
        order_by=_LIST_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        filter_fields=frozenset({"status", "topic_id", "linked", "converted"}),
        select_fields=(
            "id",
            "board_id",
            "topic_id",
            "title",
            "description",
            "actor",
            "goal",
            "benefit",
            "labels",
            "status",
            "assignee_id",
            "created_at",
            "updated_at",
            "archived",
            "screen_mockups_count",
            "ideation_links_count",
        ),
    ),
    "mcp_topic_list": SurfaceSpec(
        entity="topic",
        order_by=_MCP_TOPIC_ORDER,
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        filter_fields=frozenset(),
        select_fields=(
            "id",
            "board_id",
            "name",
            "description",
            "archived",
            "created_by",
            "created_at",
            "updated_at",
        ),
    ),
}


#: Aggregate surfaces are separate from pagination surfaces so an aggregate
#: request can never accidentally be used as an unordered page (or vice
#: versa). The tuple order is part of the result-key contract.
GROUP_COUNT_SURFACES: dict[str, GroupCountSurfaceSpec] = {
    "kanban_facets": GroupCountSurfaceSpec(
        entity="card",
        scope_fields=frozenset({"board_id", "status", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        required_scope_fields=frozenset({"board_id"}),
        filter_fields=frozenset(
            {
                "status",
                "spec_id",
                "card_type",
                "assignee_id",
                "labels",
                "title",
                "description",
            }
        ),
        group_by_options=frozenset(
            {
                ("status", "card_type"),
                ("card_type",),
                ("assignee_id",),
            }
        ),
    ),
    "topic_story_counts": GroupCountSurfaceSpec(
        entity="story",
        scope_fields=frozenset({"board_id", "topic_id"}),
        anchor_fields=frozenset({"board_id"}),
        required_scope_fields=frozenset({"board_id"}),
        filter_fields=frozenset(),
        group_by_options=frozenset(
            {
                ("topic_id", "archived"),
                ("archived",),
            }
        ),
    ),
    "mcp_ideation_refinement_counts": GroupCountSurfaceSpec(
        entity="refinement",
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        required_scope_fields=frozenset({"board_id"}),
        filter_fields=frozenset({"ideation_id", "status"}),
        group_by_options=frozenset({("ideation_id",)}),
    ),
    "mcp_spec_child_counts": GroupCountSurfaceSpec(
        entity="spec",
        scope_fields=frozenset({"board_id", "archived"}),
        anchor_fields=frozenset({"board_id"}),
        required_scope_fields=frozenset({"board_id"}),
        filter_fields=frozenset({"ideation_id", "refinement_id", "status"}),
        group_by_options=frozenset({("ideation_id",), ("refinement_id",)}),
    ),
}

#: Field KINDS drive per-field operator/value validation.
FIELD_KINDS: dict[str, str] = {
    "board_id": "id",
    "ideation_id": "id",
    "refinement_id": "id",
    "spec_id": "id",
    "sprint_id": "id",
    "topic_id": "id",
    "assignee_id": "id",
    "archived": "bool",
    "derivation_pending": "bool",
    # Virtual relational field (FR2): resolved by the adapter as a correlated
    # EXISTS against story_ideation_links — never post-fetch.
    "linked": "bool",
    # Virtual lifecycle predicate used by the legacy Story route.  The adapter
    # resolves it to status ==/!= converted without widening enum operators.
    "converted": "bool",
    # Virtual lookup predicates resolved as correlated card EXISTS probes.
    # The active variant excludes archived cards; the all-cards variant keeps
    # the current Kanban options universe, including cancelled specs.
    "linked_to_cards": "bool",
    "linked_to_active_cards": "bool",
    "status": "enum",
    "priority": "enum",
    "card_type": "enum",
    "title": "text",
    "description": "text",
    "actor": "text",
    "goal": "text",
    "benefit": "text",
    "labels": "text",
    # Virtual join projection/filter exposed only by the board-wide
    # refinement surface (C8).
    "ideation_title": "text",
}

#: Operator allowlist per field kind (C5 may extend deliberately).
KIND_OPERATORS: dict[str, frozenset[str]] = {
    "id": frozenset({"eq", "in", "is_none", "not_none"}),
    "bool": frozenset({"eq", "is_true", "is_false"}),
    "enum": frozenset({"eq", "in"}),
    "text": frozenset(
        {"eq", "in", "ilike", "contains", "json_member", "is_none", "not_none"}
    ),
}

#: Enum VALUE catalogs per (entity, field) — eq/in values outside the
#: catalog (including the empty string) are fail-closed.
ENUM_CATALOGS: dict[tuple[str, str], frozenset[str]] = {
    ("story", "status"): frozenset(v.value for v in StoryStatus),
    ("ideation", "status"): frozenset(v.value for v in IdeationStatus),
    ("refinement", "status"): frozenset(v.value for v in RefinementStatus),
    ("spec", "status"): frozenset(v.value for v in SpecStatus),
    ("sprint", "status"): frozenset(v.value for v in SprintStatus),
    ("card", "status"): frozenset(v.value for v in CardStatus),
    ("card", "priority"): frozenset(v.value for v in CardPriority),
    ("card", "card_type"): frozenset(v.value for v in CardType),
}

#: Operators a SCOPE predicate may use (fail-closed): identity equality plus
#: the boolean forms of the archived policy — never ne/contains/not_none.
SCOPE_OPERATORS: frozenset[str] = frozenset({"eq", "is_true", "is_false"})


def _validate_predicate(entity: str, item: ApplicationFilter, error: str) -> None:
    """Per-FIELD operator/value validation (kind + enum-catalog driven)."""
    kind = FIELD_KINDS.get(item.field)
    if kind is None or item.operator not in KIND_OPERATORS[kind]:
        raise ValueError(
            f"{error}: operator '{item.operator}' is not allowed on '{item.field}'"
        )
    value = item.value
    catalog = ENUM_CATALOGS.get((entity, item.field)) if kind == "enum" else None
    if item.operator == "eq":
        if kind == "bool" and not isinstance(value, bool):
            raise ValueError(f"{error}: '{item.field}' eq requires a bool value")
        if kind in ("id", "enum", "text") and (
            not isinstance(value, str) or (kind != "text" and not value.strip())
        ):
            raise ValueError(
                f"{error}: '{item.field}' eq requires a non-empty string value"
            )
        if catalog is not None and value not in catalog:
            raise ValueError(
                f"{error}: '{value}' is not a catalogued '{item.field}' value "
                f"for '{entity}'"
            )
    elif item.operator == "in":
        if (
            not isinstance(value, (list, tuple))
            or not value
            or not all(isinstance(entry, str) and entry.strip() for entry in value)
        ):
            raise ValueError(
                f"{error}: '{item.field}' in requires a non-empty list of "
                "non-empty strings"
            )
        if catalog is not None and not set(value) <= catalog:
            raise ValueError(
                f"{error}: '{item.field}' in contains values outside the "
                f"'{entity}' catalogue"
            )
    elif item.operator in ("ilike", "contains", "json_member"):
        if not isinstance(value, str) or not value:
            raise ValueError(
                f"{error}: '{item.field}' {item.operator} requires a "
                "non-empty string value"
            )
    elif item.operator in ("is_true", "is_false", "is_none", "not_none"):
        if value is not None:
            raise ValueError(f"{error}: '{item.field}' {item.operator} takes no value")


def _validate_fields(
    surface: str,
    filters: tuple[ApplicationFilter, ...],
    allowed: frozenset[str],
    error: str,
) -> None:
    for item in filters:
        if item.field not in allowed:
            raise ValueError(f"{error}: '{item.field}' is not allowed for '{surface}'")


async def list_entities_page(context: Any, request: PageRequest) -> PageResult:
    """Execute one paginated SURFACE list with two exact server-side totals.

    The surface catalog (FR0) resolves the entity, the typed whitelists and
    the SINGLE canonical order (FR3) — the executor derives and APPLIES the
    order; a caller can never choose or bypass it. ``scope`` alone drives
    ``total_overall``; ``scope`` + the discretionary filters drive the page
    items and ``total_filtered`` (KG dec-s05-01). All bounds and typed
    catalogs are ENFORCED fail-closed.
    """
    spec = SURFACES.get(request.surface)
    if spec is None:
        raise ValueError(
            f"page_request_unknown_surface: '{request.surface}' has no "
            "pagination surface"
        )
    # Normalize EVERY container to immutable tuples exactly once, then
    # validate AND query with the normalized copies — a generator would be
    # consumed by validation and reach the port empty (round-7 repro).
    scope = tuple(request.scope)
    filters = tuple(request.filters)
    any_filters = tuple(request.any_filters)
    any_groups = tuple(tuple(group) for group in request.any_groups)
    includes = tuple(request.includes)
    if not scope:
        raise ValueError(
            "page_request_scope_required: the mandatory base scope predicate "
            "(board/ideation/spec + archived policy) is missing"
        )
    _validate_fields(
        request.surface,
        scope,
        spec.scope_fields,
        "page_request_scope_field_not_allowed",
    )
    for item in scope:
        if item.operator not in SCOPE_OPERATORS:
            raise ValueError(
                f"page_request_scope_operator_not_allowed: '{item.operator}' "
                f"on '{item.field}' — scope accepts {sorted(SCOPE_OPERATORS)}"
            )
        _validate_predicate(
            spec.entity, item, "page_request_scope_operator_not_allowed"
        )
    if not any(
        item.field in spec.anchor_fields
        and item.operator == "eq"
        and isinstance(item.value, str)
        and item.value.strip()
        for item in scope
    ):
        raise ValueError(
            "page_request_scope_anchor_required: the scope must pin at least "
            f"one of {sorted(spec.anchor_fields)} with a non-empty eq "
            f"predicate for '{request.surface}'"
        )
    provided_scope_fields = {item.field for item in scope}
    missing_required = spec.required_scope_fields - provided_scope_fields
    if missing_required:
        raise ValueError(
            f"page_request_scope_field_required: '{request.surface}' requires "
            f"{sorted(spec.required_scope_fields)} in the scope; missing "
            f"{sorted(missing_required)}"
        )
    for collection in (filters, any_filters):
        _validate_fields(
            request.surface,
            collection,
            spec.filter_fields,
            "page_request_filter_field_not_allowed",
        )
        for item in collection:
            _validate_predicate(
                spec.entity, item, "page_request_filter_operator_not_allowed"
            )
    for group in any_groups:
        if not group:
            # An empty AND-group vanishes in SQL and stops restricting the OR.
            raise ValueError(
                "page_request_filter_group_empty: every any_group must carry "
                "at least one predicate"
            )
        _validate_fields(
            request.surface,
            group,
            spec.filter_fields,
            "page_request_filter_field_not_allowed",
        )
        for item in group:
            _validate_predicate(
                spec.entity, item, "page_request_filter_operator_not_allowed"
            )
    if not isinstance(request.offset, int) or isinstance(request.offset, bool):
        raise ValueError("page_request_invalid_window: offset must be an int")
    if request.offset < 0:
        raise ValueError("page_request_invalid_window: offset must be >= 0")
    if request.offset > PAGE_OFFSET_MAX:
        # A larger offset overflows SQLite's signed 64-bit OFFSET binding — fail
        # closed at the boundary instead of leaking a raw OverflowError.
        raise ValueError(
            f"page_request_invalid_window: offset must be <= {PAGE_OFFSET_MAX}"
        )
    if (
        not isinstance(request.limit, int)
        or isinstance(request.limit, bool)
        or not (PAGE_LIMIT_MIN <= request.limit <= PAGE_LIMIT_MAX)
    ):
        raise ValueError(
            "page_request_invalid_window: limit must be an int in "
            f"{PAGE_LIMIT_MIN}..{PAGE_LIMIT_MAX}"
        )
    for path in includes:
        if path not in spec.includes:
            raise ValueError(
                f"page_request_include_not_allowed: '{path}' is not an "
                f"allowed projection for '{request.surface}' (fail-closed)"
            )

    filtered = ApplicationQuery(
        entity=spec.entity,
        filters=(*scope, *filters),
        any_filters=any_filters,
        any_groups=any_groups,
        order_by=spec.order_by,
        offset=request.offset,
        limit=request.limit,
        includes=includes,
        select_fields=spec.select_fields,
    )
    overall = ApplicationQuery(entity=spec.entity, filters=scope)
    port = get_application_persistence_port()
    has_discretionary_filters = bool(filters or any_filters or any_groups)

    if not has_discretionary_filters:
        # The filtered and overall predicates are byte-for-byte equivalent.
        # Compute the exact total once instead of issuing two identical COUNTs.
        # This preserves the two envelope values while avoiding redundant work
        # on every unfiltered page (including nested refinement lists).
        total_overall = await port.count(context, overall)
        total_filtered = total_overall
        rows = await port.list(context, filtered)
    else:
        # SQL adapters may collapse the expensive filtered COUNT + page scan
        # into one windowed operation.  Capability detection keeps the pure
        # Core contract compatible with lightweight/in-memory ports.
        list_with_count = getattr(port, "list_with_count", None)
        if callable(list_with_count) and (any_filters or any_groups):
            rows, total_filtered = await list_with_count(context, filtered)
        else:
            total_filtered = await port.count(context, filtered)
            rows = await port.list(context, filtered)
        total_overall = await port.count(context, overall)
    return PageResult(
        items=tuple(rows),
        total_filtered=total_filtered,
        total_overall=total_overall,
        offset=request.offset,
        limit=request.limit,
    )


async def group_count_entities(
    context: Any, request: GroupCountRequest
) -> tuple[ApplicationGroupCount, ...]:
    """Execute one catalog-typed GROUP BY count in the bound transaction.

    Independent disjunction dimensions remain independent all the way to the
    persistence port: dimensions are ANDed, their branches are ORed, and the
    predicates within each branch are ANDed. Every container is normalized
    once before validation so one-shot iterables cannot disappear between
    validation and execution.
    """

    spec = GROUP_COUNT_SURFACES.get(request.surface)
    if spec is None:
        raise ValueError(
            f"group_count_request_unknown_surface: '{request.surface}' has no "
            "aggregate surface"
        )

    scope = tuple(request.scope)
    group_by = tuple(request.group_by)
    filters = tuple(request.filters)
    disjunctions = tuple(
        tuple(tuple(branch) for branch in dimension)
        for dimension in request.disjunctions
    )

    if group_by not in spec.group_by_options:
        raise ValueError(
            f"group_count_request_group_by_not_allowed: {group_by!r} is not "
            f"allowed for '{request.surface}'"
        )
    if not scope:
        raise ValueError(
            "group_count_request_scope_required: the mandatory base scope "
            "predicate is missing"
        )
    _validate_fields(
        request.surface,
        scope,
        spec.scope_fields,
        "group_count_request_scope_field_not_allowed",
    )
    for item in scope:
        if item.operator not in SCOPE_OPERATORS:
            raise ValueError(
                f"group_count_request_scope_operator_not_allowed: "
                f"'{item.operator}' on '{item.field}' — scope accepts "
                f"{sorted(SCOPE_OPERATORS)}"
            )
        _validate_predicate(
            spec.entity,
            item,
            "group_count_request_scope_operator_not_allowed",
        )
    if not any(
        item.field in spec.anchor_fields
        and item.operator == "eq"
        and isinstance(item.value, str)
        and item.value.strip()
        for item in scope
    ):
        raise ValueError(
            "group_count_request_scope_anchor_required: the scope must pin at "
            f"least one of {sorted(spec.anchor_fields)} with a non-empty eq "
            f"predicate for '{request.surface}'"
        )
    provided_scope_fields = {item.field for item in scope}
    missing_required = spec.required_scope_fields - provided_scope_fields
    if missing_required:
        raise ValueError(
            f"group_count_request_scope_field_required: '{request.surface}' "
            f"requires {sorted(spec.required_scope_fields)} in the scope; "
            f"missing {sorted(missing_required)}"
        )

    _validate_fields(
        request.surface,
        filters,
        spec.filter_fields,
        "group_count_request_filter_field_not_allowed",
    )
    for item in filters:
        _validate_predicate(
            spec.entity,
            item,
            "group_count_request_filter_operator_not_allowed",
        )

    for dimension in disjunctions:
        if not dimension:
            raise ValueError(
                "group_count_request_disjunction_empty: every disjunction "
                "dimension must carry at least one branch"
            )
        for branch in dimension:
            if not branch:
                raise ValueError(
                    "group_count_request_branch_empty: every disjunction "
                    "branch must carry at least one predicate"
                )
            _validate_fields(
                request.surface,
                branch,
                spec.filter_fields,
                "group_count_request_filter_field_not_allowed",
            )
            for item in branch:
                _validate_predicate(
                    spec.entity,
                    item,
                    "group_count_request_filter_operator_not_allowed",
                )

    port = get_application_persistence_port()
    return tuple(
        await port.group_count(
            context,
            ApplicationGroupCountQuery(
                entity=spec.entity,
                group_by=group_by,
                filters=(*scope, *filters),
                disjunctions=disjunctions,
            ),
        )
    )


class EntityPageService:
    """Catalog-scoped pagination service (one relational transaction).

    The ``ApplicationServiceCatalog`` exposes this under ``entity_pages`` so
    UnitOfWork-shaped callers reach the executor through the CONTRACT — never
    through a concrete service's private handle.
    """

    def __init__(self, relational_context: Any) -> None:
        self._relational_context = relational_context

    async def list(self, request: PageRequest) -> PageResult:
        return await list_entities_page(self._relational_context, request)

    async def group_count(
        self, request: GroupCountRequest
    ) -> tuple[ApplicationGroupCount, ...]:
        return await group_count_entities(self._relational_context, request)


class ListEntityPageCommand:
    __slots__ = ("request",)

    def __init__(self, request: PageRequest) -> None:
        self.request = request


class ListEntityPageResult:
    __slots__ = ("page",)

    def __init__(self, page: PageResult) -> None:
        self.page = page


class ListEntityUseCase:
    """Execute one paginated SURFACE list (read, no commit).

    UnitOfWork-shaped like every application use case: relational access
    comes exclusively through the catalog contract —
    ``uow.services.entity_pages`` (:class:`EntityPageService`) — never
    through a concrete service's private ``db`` handle. Callers get a
    :class:`PageResult` whose totals are always window-independent
    (KG dec-s05-01) under the enforced FR0/FR3 surface contract.
    """

    async def execute(
        self, command: ListEntityPageCommand, *, uow: PulseUnitOfWork
    ) -> ListEntityPageResult:
        return ListEntityPageResult(
            await uow.services.entity_pages.list(command.request)
        )
