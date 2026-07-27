"""AuditRepository Protocol — async audit persistence contract."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.kg.interfaces.audit_dtos import (
    AuditRow,
    ConsolidationAuditData,
    NodeRefData,
    OutboxEventData,
)


class AuditPersistenceError(RuntimeError):
    """Backend-neutral failure raised by an audit persistence adapter."""

    code = "audit_persistence_error"
    retryable = False

    def __init__(self, operation: str) -> None:
        self.operation = str(operation)
        super().__init__(f"{self.code}: {self.operation}")


class AuditTransactionContextRequired(AuditPersistenceError):
    """The audit receipt cannot be staged outside its caller-owned UoW."""

    code = "audit_transaction_context_required"


class AuditWriteContention(AuditPersistenceError):
    """A transient relational writer contention interrupted audit staging."""

    code = "audit_write_contention"
    retryable = True


@runtime_checkable
class AuditRepository(Protocol):
    async def get_latest_for_artifact(
        self,
        board_id: str,
        artifact_id: str,
        *,
        artifact_type: str,
    ) -> AuditRow | None:
        """Return the latest committed audit for one artifact identity.

        The canonical identity is ``(board_id, artifact_type, artifact_id)``;
        callers cannot fall back to an ambiguous id-only lookup.
        """
        ...

    async def get_audit_by_session(self, session_id: str) -> AuditRow | None:
        """Return audit row by session_id."""
        ...

    async def get_node_refs_by_session(self, session_id: str) -> list[NodeRefData]:
        """Return the graph-node back-references recorded by a committed
        session (spec MKG-B-S1 FR5/TR4 -- count-only re-attestation)."""
        ...

    async def stage_consolidation_records(
        self,
        transaction_context: object,
        audit: ConsolidationAuditData,
        node_refs: list[NodeRefData],
        outbox_event: OutboxEventData,
    ) -> None:
        """Stage the receipt in the mandatory caller-owned transaction.

        Implementations must not commit or roll back ``transaction_context``.
        """
        ...

    async def mark_audit_undone(self, session_id: str) -> None:
        """Mark a session's audit as undone."""
        ...

    async def purge_by_board(self, board_id: str) -> int:
        """Delete all audit records for a board. Returns count deleted."""
        ...
