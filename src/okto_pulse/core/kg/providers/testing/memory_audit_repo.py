"""InMemoryAuditRepository — satisfies AuditRepository Protocol.

Pure in-memory storage for unit tests without SQLAlchemy/SQLite.
"""

from __future__ import annotations

from okto_pulse.core.kg.interfaces.audit_dtos import (
    AuditRow,
    ConsolidationAuditData,
    NodeRefData,
    OutboxEventData,
)


class InMemoryAuditRepository:
    def __init__(self):
        self.audits: list[AuditRow] = []
        self.node_refs: list[NodeRefData] = []
        self.outbox_events: list[OutboxEventData] = []

    async def get_latest_for_artifact(
        self, board_id: str, artifact_id: str
    ) -> AuditRow | None:
        matching = [
            a
            for a in reversed(self.audits)
            if a.board_id == board_id
            and a.artifact_id == artifact_id
            and a.committed_at is not None
            and a.undo_status == "none"
        ]
        return matching[0] if matching else None

    async def get_audit_by_session(self, session_id: str) -> AuditRow | None:
        for a in self.audits:
            if a.session_id == session_id:
                return a
        return None

    async def commit_consolidation_records(
        self,
        audit: ConsolidationAuditData,
        node_refs: list[NodeRefData],
        outbox_event: OutboxEventData,
    ) -> None:
        self.audits.append(
            AuditRow(
                session_id=audit.session_id,
                board_id=audit.board_id,
                artifact_id=audit.artifact_id,
                artifact_type=audit.artifact_type,
                agent_id=audit.agent_id,
                started_at=audit.started_at,
                committed_at=audit.committed_at,
                nodes_added=audit.nodes_added,
                nodes_updated=audit.nodes_updated,
                nodes_superseded=audit.nodes_superseded,
                edges_added=audit.edges_added,
                summary_text=audit.summary_text,
                content_hash=audit.content_hash,
                undo_status="none",
            )
        )
        self.node_refs.extend(node_refs)
        self.outbox_events.append(outbox_event)

    async def mark_audit_undone(self, session_id: str) -> None:
        for i, a in enumerate(self.audits):
            if a.session_id == session_id:
                self.audits[i] = a.model_copy(update={"undo_status": "undone"})
                return

    async def purge_by_board(self, board_id: str) -> int:
        before = len(self.audits)
        self.audits = [a for a in self.audits if a.board_id != board_id]
        self.node_refs = [r for r in self.node_refs if r.board_id != board_id]
        self.outbox_events = [e for e in self.outbox_events if e.board_id != board_id]
        return before - len(self.audits)

    def clear(self) -> None:
        self.audits.clear()
        self.node_refs.clear()
        self.outbox_events.clear()
