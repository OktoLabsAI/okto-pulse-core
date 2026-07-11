"""Persistence-neutral query specifications for Core analytics."""

from __future__ import annotations

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)

from dataclasses import dataclass
from typing import Any, Literal, Mapping, Protocol

AnalyticsOperator = Literal[
    "eq",
    "ne",
    "in",
    "not_in",
    "gte",
    "lte",
    "is_true",
    "is_false",
    "contains",
]


@dataclass(frozen=True, slots=True)
class AnalyticsFilter:
    field: str
    operator: AnalyticsOperator
    value: Any = None


@dataclass(frozen=True, slots=True)
class AnalyticsQuery:
    entity: str
    filters: tuple[AnalyticsFilter, ...] = ()
    search: str = ""
    search_fields: tuple[str, ...] = ()
    order_by: str | None = None
    descending: bool = False
    offset: int = 0
    limit: int | None = None


@dataclass(frozen=True, slots=True)
class AnalyticsFact:
    """Detached immutable projection of one persistence record."""

    values: Mapping[str, Any]

    def __getattr__(self, name: str) -> Any:
        try:
            return self.values[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class AnalyticsReadPort(Protocol):
    async def list(
        self, context: Any, query: AnalyticsQuery
    ) -> tuple[AnalyticsFact, ...]: ...

    async def count(self, context: Any, query: AnalyticsQuery) -> int: ...


_RUNTIME_KEY = "ports.analytics_read.reader"


def register_analytics_read_port(reader: AnalyticsReadPort) -> None:
    register_runtime_value(_RUNTIME_KEY, reader)


def get_analytics_read_port() -> AnalyticsReadPort:
    return require_runtime_value(_RUNTIME_KEY, "analytics_read_port_not_configured")


def reset_analytics_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "AnalyticsFact",
    "AnalyticsFilter",
    "AnalyticsOperator",
    "AnalyticsQuery",
    "AnalyticsReadPort",
    "get_analytics_read_port",
    "register_analytics_read_port",
    "reset_analytics_read_port_for_tests",
]
