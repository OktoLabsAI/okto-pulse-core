"""InMemoryAuditRepository — satisfies AuditRepository Protocol.

Pure in-memory storage for unit tests without SQLAlchemy/SQLite.
"""

from __future__ import annotations
from okto_pulse.core.ports.projection_findings import (
    ProjectionFindingSnapshot, validate_audit_finding_snapshot,
)

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
        self.reference_findings: dict[str, ProjectionFindingSnapshot] = {}

    async def get_latest_reference_findings(
        self, board_id: str, artifact_id: str, *, artifact_type: str, namespace: str,
    ) -> ProjectionFindingSnapshot | None:
        for audit in reversed(self.audits):
            snapshot = self.reference_findings.get(audit.session_id)
            if (snapshot is not None and audit.committed_at is not None
                    and audit.undo_status == 'none'
                    and (audit.board_id, audit.artifact_type, audit.artifact_id)
                    == (board_id, artifact_type, artifact_id) and snapshot.namespace == namespace):
                return snapshot
        return None

    async def get_latest_for_artifact(
        self,
        board_id: str,
        artifact_id: str,
        *,
        artifact_type: str,
    ) -> AuditRow | None:
        matching = [
            a
            for a in reversed(self.audits)
            if a.board_id == board_id
            and a.artifact_id == artifact_id
            and a.artifact_type == artifact_type
            and a.committed_at is not None
            and a.undo_status == "none"
        ]
        return matching[0] if matching else None

    async def get_audit_by_session(self, session_id: str) -> AuditRow | None:
        for a in self.audits:
            if a.session_id == session_id:
                return a
        return None

    async def get_node_refs_by_session(self, session_id: str) -> list[NodeRefData]:
        return [r for r in self.node_refs if r.session_id == session_id]

    async def stage_consolidation_records(
        self,
        transaction_context: object,
        audit: ConsolidationAuditData,
        node_refs: list[NodeRefData],
        outbox_event: OutboxEventData,
        *,
        reference_findings: ProjectionFindingSnapshot | None = None,
    ) -> None:
        del transaction_context
        validate_audit_finding_snapshot(reference_findings, board_id=audit.board_id,
            artifact_type=audit.artifact_type, artifact_id=audit.artifact_id, agent_id=audit.agent_id)
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
        if reference_findings is not None:
            self.reference_findings[audit.session_id] = reference_findings

    async def mark_audit_undone(self, session_id: str) -> None:
        for i, a in enumerate(self.audits):
            if a.session_id == session_id:
                self.audits[i] = a.model_copy(update={"undo_status": "undone"})
                return

    async def purge_by_board(self, board_id: str) -> int:
        before = len(self.audits)
        self.audits = [a for a in self.audits if a.board_id != board_id]
        self.reference_findings = {key: value for key, value in self.reference_findings.items()
                                   if value.board_id != board_id}
        self.node_refs = [r for r in self.node_refs if r.board_id != board_id]
        self.outbox_events = [e for e in self.outbox_events if e.board_id != board_id]
        return before - len(self.audits)

    def clear(self) -> None:
        self.audits.clear()
        self.node_refs.clear()
        self.outbox_events.clear()
        self.reference_findings.clear()
