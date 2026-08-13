"""Transaction-bound persistence port for operational Spec precedence."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, Sequence, runtime_checkable

from okto_pulse.core.domain.spec_dependency import (
    SpecDependencyBlocker,
    SpecDependencyListQuery,
    SpecDependencyMutationReceipt,
    SpecDependencyPage,
    SpecDependencyReadiness,
    SpecDependencyRecord,
    SpecDependencySpecSnapshot,
)


@runtime_checkable
class SpecDependencyPersistencePort(Protocol):
    """Relational authority scoped to the caller's Unit of Work.

    ``acquire_board_graph_lock`` must serialize dependency mutations for one
    board across processes and hold the lock until the surrounding transaction
    finishes.  The remaining write methods stage changes but never commit.
    """

    async def acquire_board_graph_lock(self, board_id: str) -> None: ...

    async def get_spec_snapshot(
        self,
        *,
        board_id: str,
        spec_id: str,
        for_update: bool = False,
    ) -> SpecDependencySpecSnapshot | None: ...

    async def lookup_mutation_replay(
        self,
        *,
        board_id: str,
        operation: str,
        idempotency_key: str,
        actor_id: str,
        actor_type: str,
    ) -> SpecDependencyMutationReceipt | None: ...

    async def get_dependency(
        self,
        *,
        board_id: str,
        dependency_id: str,
    ) -> SpecDependencyRecord | None: ...

    async def find_active_dependency(
        self,
        *,
        board_id: str,
        source_spec_id: str,
        target_spec_id: str,
    ) -> SpecDependencyRecord | None: ...

    async def list_active_board_edges(
        self,
        *,
        board_id: str,
    ) -> tuple[SpecDependencyRecord, ...]: ...

    async def list_active_blockers(
        self,
        *,
        board_id: str,
        source_spec_id: str,
        limit: int = 100,
    ) -> tuple[SpecDependencyBlocker, ...]: ...

    async def list_incoming_active(
        self,
        *,
        board_id: str,
        target_spec_ids: Sequence[str],
        exclude_source_spec_ids: Sequence[str] = (),
        limit: int = 100,
    ) -> tuple[SpecDependencyRecord, ...]: ...

    async def list_page(self, query: SpecDependencyListQuery) -> SpecDependencyPage: ...

    async def get_readiness(
        self,
        *,
        board_id: str,
        spec_id: str,
        blocker_limit: int = 100,
    ) -> SpecDependencyReadiness:
        """Return exact totals and at most ``blocker_limit`` blocker details.

        Implementations must calculate ``blocking_count``,
        ``archived_blocking_count`` and ``unfinished_blocking_count`` over the
        complete active dependency set. ``blockers`` is only a bounded sample,
        with ``blockers_truncated`` declaring whether details were omitted.
        The supported detail limit is inclusive of zero: a non-positive value
        returns no blocker details while preserving the exact totals.
        """
        ...

    async def list_board_readiness(
        self,
        *,
        board_id: str,
        blocker_limit_per_spec: int = 100,
    ) -> tuple[SpecDependencyReadiness, ...]:
        """Batch readiness with the same exact-total/bounded-sample contract.

        ``blocker_limit_per_spec=0`` returns an empty detail sample for every
        Spec without changing exact totals or truncation reporting.
        """
        ...

    async def insert_dependency(
        self,
        dependency: SpecDependencyRecord,
        *,
        request_digest: str,
    ) -> None: ...

    async def tombstone_dependency(
        self,
        *,
        board_id: str,
        dependency_id: str,
        removed_at: datetime,
        removed_by: str,
        removed_by_type: str,
        removed_by_name: str | None,
        removal_reason: str,
        source_version_on_remove: int,
        source_title_on_remove: str,
        source_edition_on_remove: int,
        target_title_on_remove: str | None,
        target_edition_on_remove: int | None,
        idempotency_key: str,
        request_digest: str,
    ) -> SpecDependencyRecord: ...

    async def bump_source_spec_version(
        self,
        *,
        board_id: str,
        spec_id: str,
        expected_version: int,
        expected_edition: int,
    ) -> SpecDependencySpecSnapshot | None: ...

    async def store_mutation_receipt(
        self,
        receipt: SpecDependencyMutationReceipt,
        *,
        idempotency_key: str,
        actor_id: str,
        actor_type: str,
        actor_name: str | None,
    ) -> None:
        """Stage the immutable replay result after the source CAS succeeds."""
        ...

    async def mark_spec_edition_started(
        self,
        *,
        board_id: str,
        spec_id: str,
        expected_edition: int,
    ) -> SpecDependencySpecSnapshot | None: ...


__all__ = ["SpecDependencyPersistencePort"]
