"""Persistence boundary for structured spec child mutations."""

from __future__ import annotations

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)

from dataclasses import dataclass
from enum import Enum
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
    project_structure: list[Any] | None = None
    project_structure_revision: int = 0
    project_structure_digest: str | None = None


@dataclass(frozen=True, slots=True)
class ProjectStructureMutationReceipt:
    """Durable idempotency receipt saved in the same transaction as the tree."""

    spec_id: str
    idempotency_key: str
    request_digest: str
    result: dict[str, Any]


class ProjectStructureMutationPersistenceState(str, Enum):
    APPLIED = "applied"
    REPLAYED = "replayed"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    VERSION_CONFLICT = "version_conflict"


@dataclass(frozen=True, slots=True)
class ProjectStructureMutationPersistenceResult:
    state: ProjectStructureMutationPersistenceState
    receipt: ProjectStructureMutationReceipt | None = None


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
        expected_version: int | None = None,
    ) -> None: ...

    async def get_project_structure_receipt(
        self,
        context: Any,
        *,
        spec_id: str,
        idempotency_key: str,
    ) -> ProjectStructureMutationReceipt | None: ...

    async def save_project_structure_mutation(
        self,
        context: Any,
        record: StructuredSpecRecord,
        *,
        expected_spec_version: int,
        expected_project_structure_revision: int,
        bump_spec_version: bool,
        changed_fields: Sequence[str],
        receipt: ProjectStructureMutationReceipt,
    ) -> ProjectStructureMutationPersistenceResult:
        """Atomically fence Spec version + tree revision and claim the key.

        The adapter must convert a unique-key race into ``replayed`` or
        ``idempotency_conflict`` after comparing request digests.  It must not
        leak an integrity exception or commit either half independently.
        Relation-only writes set ``bump_spec_version=False`` and advance only
        the Project structure revision so validation evidence remains current.
        """
        ...

    async def validate_project_structure_references(
        self,
        context: Any,
        *,
        board_id: str,
        spec_id: str,
        task_ids: Sequence[str],
        test_ids: Sequence[str],
        evidence_ids: Sequence[str],
    ) -> None:
        """Fail closed when a reference is missing, wrong-type or cross-Spec."""
        ...


_RUNTIME_KEY = "ports.structured_spec.store"


def register_structured_spec_store(store: StructuredSpecStore) -> None:
    register_runtime_value(_RUNTIME_KEY, store)


def get_structured_spec_store() -> StructuredSpecStore:
    return require_runtime_value(_RUNTIME_KEY, "structured_spec_store_not_configured")


def reset_structured_spec_store_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ProjectStructureMutationReceipt",
    "ProjectStructureMutationPersistenceResult",
    "ProjectStructureMutationPersistenceState",
    "StructuredSpecRecord",
    "StructuredSpecStore",
    "get_structured_spec_store",
    "register_structured_spec_store",
    "reset_structured_spec_store_for_tests",
]
