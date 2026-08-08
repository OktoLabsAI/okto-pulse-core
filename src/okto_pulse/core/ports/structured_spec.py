"""Persistence boundary for structured spec child mutations."""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from dataclasses import dataclass
from typing import Any, Protocol, Sequence


@dataclass(slots=True)
class StructuredSpecRecord:
    id: str
    board_id: str
    status: Any
    version: int
    archived: bool
    # Preserve stored NULL versus an authored empty collection.  Requirement
    # lint hashes the exact post-mutation semantic snapshot; normalizing an
    # untouched NULL to [] in this projection would make a structured-write
    # receipt immediately stale when currentness is rederived from persistence.
    functional_requirements: list[Any] | None
    business_rules: list[Any] | None
    technical_requirements: list[Any] | None
    decisions: list[Any] | None
    acceptance_criteria: list[Any] | None
    api_contracts: list[Any] | None
    integration_requirements: list[Any] | None
    observability_requirements: list[Any] | None
    test_scenarios: list[Any] | None
    title: str | None = None
    description: str | None = None
    context: str | None = None


class StructuredSpecStore(Protocol):
    async def get(
        self,
        context: Any,
        *,
        spec_id: str,
    ) -> StructuredSpecRecord | None: ...

    async def save(
        self,
        context: Any,
        record: StructuredSpecRecord,
        *,
        changed_fields: Sequence[str],
    ) -> None: ...


_RUNTIME_KEY = "ports.structured_spec.store"


def register_structured_spec_store(store: StructuredSpecStore) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_structured_spec_store() -> StructuredSpecStore:
    return require_runtime_value(_RUNTIME_KEY, "structured_spec_store_not_configured")


def reset_structured_spec_store_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "StructuredSpecRecord",
    "StructuredSpecStore",
    "get_structured_spec_store",
    "register_structured_spec_store",
    "reset_structured_spec_store_for_tests",
]
