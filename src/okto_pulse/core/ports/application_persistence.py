"""Persistence-neutral record and query boundary for legacy application services."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

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


class ApplicationPersistencePort(Protocol):
    async def list(
        self, context: Any, query: ApplicationQuery
    ) -> tuple[ApplicationRecord, ...]: ...

    async def get(
        self,
        context: Any,
        *,
        entity: str,
        record_id: str,
        includes: tuple[str, ...] = (),
    ) -> ApplicationRecord | None: ...

    async def add(self, context: Any, record: ApplicationRecord) -> ApplicationRecord: ...

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
    return require_runtime_value(_RUNTIME_KEY, "application_persistence_port_not_configured")


def reset_application_persistence_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ApplicationFilter",
    "ApplicationOperator",
    "ApplicationPersistencePort",
    "ApplicationQuery",
    "ApplicationRecord",
    "get_application_persistence_port",
    "register_application_persistence_port",
    "reset_application_persistence_port_for_tests",
]
