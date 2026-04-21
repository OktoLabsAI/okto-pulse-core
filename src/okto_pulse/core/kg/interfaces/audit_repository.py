"""AuditRepository Protocol — async audit persistence contract."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.kg.interfaces.audit_dtos import (
    AuditRow,
    ConsolidationAuditData,
    NodeRefData,
    OutboxEventData,
)


@runtime_checkable
class AuditRepository(Protocol):
    async def get_latest_for_artifact(
        self, board_id: str, artifact_id: str
    ) -> AuditRow | None:
        """Return the most recent committed audit for (board_id, artifact_id)."""
        ...

    async def get_audit_by_session(self, session_id: str) -> AuditRow | None:
        """Return audit row by session_id."""
        ...

    async def commit_consolidation_records(
        self,
        audit: ConsolidationAuditData,
        node_refs: list[NodeRefData],
        outbox_event: OutboxEventData,
    ) -> None:
        """Atomically write audit + node refs + outbox event."""
        ...

    async def mark_audit_undone(self, session_id: str) -> None:
        """Mark a session's audit as undone."""
        ...

    async def purge_by_board(self, board_id: str) -> int:
        """Delete all audit records for a board. Returns count deleted."""
        ...
