"""Persistence-neutral record and query boundary for legacy application services."""

from __future__ import annotations

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Protocol

ApplicationOperator = Literal[
    "eq",
    "ne",
    "in",
    "not_in",
    "gte",
    "lte",
    "gt",
    "lt",
    "is_true",
    "is_false",
    "is_none",
    "not_none",
    "contains",
    "ilike",
]


@dataclass(frozen=True, slots=True)
class ApplicationFilter:
    field: str
    operator: ApplicationOperator
    value: Any = None


@dataclass(frozen=True, slots=True)
class ApplicationQuery:
    entity: str
    filters: tuple[ApplicationFilter, ...] = ()
    any_filters: tuple[ApplicationFilter, ...] = ()
    any_groups: tuple[tuple[ApplicationFilter, ...], ...] = ()
    order_by: tuple[tuple[str, bool], ...] = ()
    offset: int = 0
    limit: int | None = None
    includes: tuple[str, ...] = ()
    select_fields: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ApplicationGroupCountQuery:
    """One typed aggregate query over application records.

    ``filters`` are combined with AND. ``disjunctions`` model independent
    boolean dimensions: dimensions are combined with AND, branches inside a
    dimension with OR, and predicates inside a branch with AND. This shape is
    deliberately richer than :class:`ApplicationQuery.any_groups`; it can
    express, for example, ``spec/unlinked AND search AND status/type-map``
    without flattening the independent OR dimensions into one predicate.

    The adapter returns one :class:`ApplicationGroupCount` per distinct
    ``group_by`` key. Result ordering is intentionally unspecified.
    """

    entity: str
    group_by: tuple[str, ...]
    filters: tuple[ApplicationFilter, ...] = ()
    disjunctions: tuple[tuple[tuple[ApplicationFilter, ...], ...], ...] = ()


@dataclass(frozen=True, slots=True)
class ApplicationGroupCount:
    """One GROUP BY key and its server-computed row count."""

    values: tuple[Any, ...]
    count: int


@dataclass(slots=True)
class ApplicationRecord:
    """Detached mutable application record synchronized through the port."""

    entity: str
    values: dict[str, Any]
    dirty_fields: set[str] = field(default_factory=set, repr=False)

    def __getattr__(self, name: str) -> Any:
        try:
            return self.values[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        if name in {"entity", "values", "dirty_fields"}:
            object.__setattr__(self, name, value)
            return
        self.values[name] = value
        self.dirty_fields.add(name)

    def mark_dirty(self, name: str) -> None:
        if name not in self.values:
            raise AttributeError(name)
        self.dirty_fields.add(name)

    def attach(self, name: str, value: Any) -> None:
        self.values[name] = value


class ApplicationRecordConflictError(RuntimeError):
    """A persistence insert collided with the same stable application identity.

    Adapters raise this transport-neutral error only after verifying the
    record's primary-key conflict.  Higher-level workflows can then decide
    whether that collision represents an expected idempotent creation race.
    """

    code = "application_record_conflict"

    def __init__(self, entity: str, record_id: str) -> None:
        self.entity = entity
        self.record_id = record_id
        super().__init__(f"{entity}:{record_id} already exists")


#: Core-side pagination window bounds (FR0): the REST adapter further
#: restricts limit to {25, 50, 100}; MCP caps at 200.
PAGE_LIMIT_MIN = 1
PAGE_LIMIT_MAX = 200


@dataclass(frozen=True, slots=True)
class PageRequest:
    """A typed server-side pagination request over one SURFACE (FR0).

    ``surface`` names one entry of the Core surface catalog (``story_list``,
    ``card_list``, ``kanban_column``, ``spec_lookup``, …) — each surface pins
    ONE entity and ONE canonical order sequence (FR3), applied BY the
    executor; callers never choose an ordering. ``scope`` is the MANDATORY
    base predicate — the surface's scope filters plus the archived policy —
    and alone drives ``total_overall``. ``filters``/``any_filters``/
    ``any_groups`` are the discretionary, surface-typed filters; together
    with ``scope`` they drive the page items and ``total_filtered``. Both
    totals are always computed server-side, independently of the window —
    never inferred from ``len(items)`` (KG dec-s05-01).

    Contract (ENFORCED by the Core executor, not merely documented): known
    surface; ``scope`` non-empty, anchored and surface-typed; ``offset >= 0``;
    ``limit`` an int in ``1..200`` (the ``{25, 50, 100}`` menu is
    REST/UX-only); enum values validated against the surface's catalogs.
    """

    surface: str
    scope: tuple[ApplicationFilter, ...]
    offset: int
    limit: int
    filters: tuple[ApplicationFilter, ...] = ()
    any_filters: tuple[ApplicationFilter, ...] = ()
    any_groups: tuple[tuple[ApplicationFilter, ...], ...] = ()
    includes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PageResult:
    """One page of application records plus the two server-computed totals.

    ``total_filtered`` counts the rows matching ``PageRequest.filtered`` BEFORE
    the ``offset``/``limit`` window; ``total_overall`` counts
    ``PageRequest.overall``. ``offset``/``limit`` echo the applied window so a
    caller can render a stable paginator without a second round-trip.
    """

    items: tuple[ApplicationRecord, ...]
    total_filtered: int
    total_overall: int
    offset: int
    limit: int | None


@dataclass(frozen=True, slots=True)
class GroupCountRequest:
    """Catalog-scoped GROUP BY request for an application read surface.

    The Core service validates ``scope``, ``filters``, every nested
    disjunction predicate and the exact ``group_by`` tuple before constructing
    the lower-level :class:`ApplicationGroupCountQuery`.
    """

    surface: str
    scope: tuple[ApplicationFilter, ...]
    group_by: tuple[str, ...]
    filters: tuple[ApplicationFilter, ...] = ()
    disjunctions: tuple[tuple[tuple[ApplicationFilter, ...], ...], ...] = ()


class ApplicationPersistencePort(Protocol):
    async def list(
        self, context: Any, query: ApplicationQuery
    ) -> tuple[ApplicationRecord, ...]: ...

    async def count(self, context: Any, query: ApplicationQuery) -> int:
        """Count rows matching ``query``'s filters, ignoring offset/limit/includes.

        Drives the server-side totals of :class:`PageResult`.  Implementations
        may combine a filtered count with the page query, and the executor may
        reuse one count when filtered/overall predicates are identical; totals
        are never inferred from the materialized page length.
        """
        ...

    async def group_count(
        self, context: Any, query: ApplicationGroupCountQuery
    ) -> tuple[ApplicationGroupCount, ...]:
        """Count rows in each distinct ``group_by`` key in one statement."""
        ...

    async def get(
        self,
        context: Any,
        *,
        entity: str,
        record_id: str,
        includes: tuple[str, ...] = (),
    ) -> ApplicationRecord | None: ...

    async def fence(
        self,
        context: Any,
        *,
        entity: str,
        record_id: str,
        expected_values: Mapping[str, object],
    ) -> bool:
        """Lock one record iff its authoritative scalar facts still match."""
        ...

    async def add(
        self,
        context: Any,
        record: ApplicationRecord,
        *,
        conflict_error: Exception | None = None,
    ) -> ApplicationRecord: ...

    async def delete(self, context: Any, record: ApplicationRecord) -> None: ...

    async def flush(self, context: Any) -> None: ...

    async def refresh(
        self, context: Any, record: ApplicationRecord
    ) -> ApplicationRecord: ...

    async def commit(self, context: Any) -> None: ...

    async def rollback(self, context: Any) -> None: ...

    async def backfill_qa_answered_at(self, context: Any) -> dict[str, int]: ...


_RUNTIME_KEY = "ports.application_persistence.store"


def register_application_persistence_port(store: ApplicationPersistencePort) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_application_persistence_port() -> ApplicationPersistencePort:
    return require_runtime_value(
        _RUNTIME_KEY, "application_persistence_port_not_configured"
    )


def reset_application_persistence_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ApplicationFilter",
    "ApplicationGroupCount",
    "ApplicationGroupCountQuery",
    "ApplicationOperator",
    "ApplicationPersistencePort",
    "ApplicationQuery",
    "ApplicationRecord",
    "ApplicationRecordConflictError",
    "GroupCountRequest",
    "PAGE_LIMIT_MAX",
    "PAGE_LIMIT_MIN",
    "PageRequest",
    "PageResult",
    "get_application_persistence_port",
    "register_application_persistence_port",
    "reset_application_persistence_port_for_tests",
]
