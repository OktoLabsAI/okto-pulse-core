"""Persistence boundary for the Architecture Design bounded context."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

ArchitectureOperator = Literal["eq", "ne", "in", "is_true", "is_false"]


@dataclass(frozen=True, slots=True)
class ArchitectureFilter:
    field: str
    operator: ArchitectureOperator
    value: Any = None


@dataclass(frozen=True, slots=True)
class ArchitectureQuery:
    entity: str
    filters: tuple[ArchitectureFilter, ...] = ()
    any_filters: tuple[ArchitectureFilter, ...] = ()
    order_by: tuple[tuple[str, bool], ...] = ()
    limit: int | None = None


@dataclass(slots=True)
class ArchitectureRecord:
    """Detached mutable record; dirty fields are synchronized by the adapter."""

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

    def attach(self, name: str, value: Any) -> None:
        """Attach a transient hydrated value without scheduling persistence."""

        self.values[name] = value


class ArchitecturePersistencePort(Protocol):
    async def list(
        self, context: Any, query: ArchitectureQuery
    ) -> tuple[ArchitectureRecord, ...]: ...

    async def get(
        self, context: Any, *, entity: str, record_id: str
    ) -> ArchitectureRecord | None: ...

    async def create(
        self, context: Any, *, entity: str, values: dict[str, Any]
    ) -> ArchitectureRecord: ...

    async def delete(self, context: Any, record: ArchitectureRecord) -> None: ...

    async def flush(self, context: Any) -> None: ...

    async def refresh(
        self, context: Any, record: ArchitectureRecord
    ) -> ArchitectureRecord: ...

    async def commit(self, context: Any) -> None: ...


_RUNTIME_KEY = "ports.architecture_persistence.store"


def register_architecture_persistence_port(
    store: ArchitecturePersistencePort,
) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_architecture_persistence_port() -> ArchitecturePersistencePort:
    return require_runtime_value(_RUNTIME_KEY, "architecture_persistence_port_not_configured")


def reset_architecture_persistence_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ArchitectureFilter",
    "ArchitectureOperator",
    "ArchitecturePersistencePort",
    "ArchitectureQuery",
    "ArchitectureRecord",
    "get_architecture_persistence_port",
    "register_architecture_persistence_port",
    "reset_architecture_persistence_port_for_tests",
]
