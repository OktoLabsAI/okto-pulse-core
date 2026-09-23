"""Test-only SQLAlchemy KG governance store."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.orm.attributes import flag_modified

from sqlalchemy_test_models import (
    Board,
    ConsolidationAudit,
    ConsolidationQueue,
    GlobalUpdateOutbox,
    KuzuNodeRef,
)
from okto_pulse.core.ports.kg_events import HISTORICAL_PROGRESS_SETTINGS_KEY
from okto_pulse.core.ports.kg_governance import (
    BoardErasureJobFact,
    BoostAuditRecord,
    GovernanceUndoFact,
    HistoricalBoardRecord,
)


class TestSqlAlchemyKGGovernanceStore:
    __test__ = False

    def __init__(self) -> None:
        self._board_erasure_jobs: dict[str, BoardErasureJobFact] = {}

    async def get_board(
        self, context: Any, *, board_id: str
    ) -> HistoricalBoardRecord | None:
        row = await context.get(Board, board_id)
        if row is None:
            return None
        return HistoricalBoardRecord(id=str(row.id), settings=dict(row.settings or {}))


    async def queue_counts(self, context: Any, *, board_id: str) -> dict[str, int]:
        rows = (
            await context.execute(
                select(ConsolidationQueue.status, func.count())
                .where(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.source == "historical_backfill",
                )
                .group_by(ConsolidationQueue.status)
            )
        ).all()
        return {str(status): int(count) for status, count in rows}








    async def get_undo_fact(
        self, context: Any, *, board_id: str, session_id: str
    ) -> GovernanceUndoFact | None:
        audit = (
            (
                await context.execute(
                    select(ConsolidationAudit).where(
                        ConsolidationAudit.session_id == session_id,
                        ConsolidationAudit.board_id == board_id,
                    )
                )
            )
            .scalars()
            .first()
        )
        if audit is None:
            return None
        refs = (
            (
                await context.execute(
                    select(KuzuNodeRef).where(KuzuNodeRef.session_id == session_id)
                )
            )
            .scalars()
            .all()
        )
        node_ids = tuple(str(row.kuzu_node_id) for row in refs)
        blockers: tuple[str, ...] = ()
        if node_ids:
            rows = (
                (
                    await context.execute(
                        select(KuzuNodeRef.session_id).where(
                            KuzuNodeRef.kuzu_node_id.in_(node_ids),
                            KuzuNodeRef.session_id != session_id,
                        )
                    )
                )
                .scalars()
                .all()
            )
            blockers = tuple(sorted({str(value) for value in rows}))
        return GovernanceUndoFact(
            session_id, str(audit.undo_status), node_ids, blockers
        )

    async def mark_session_undone(
        self, context: Any, *, session_id: str, undone_at
    ) -> None:
        row = await context.get(ConsolidationAudit, session_id)
        if row is not None:
            row.undo_status = "undone"
            row.undone_at = undone_at

    async def purge_expired_audit(self, context: Any, *, board_id: str, cutoff) -> int:
        result = await context.execute(
            delete(ConsolidationAudit).where(
                ConsolidationAudit.board_id == board_id,
                ConsolidationAudit.committed_at < cutoff,
            )
        )
        return int(result.rowcount or 0)

    async def purge_board_metadata(self, context: Any, *, board_id: str) -> None:
        for model in (
            KuzuNodeRef,
            ConsolidationAudit,
            ConsolidationQueue,
            GlobalUpdateOutbox,
        ):
            await context.execute(delete(model).where(model.board_id == board_id))
        board = await context.get(Board, board_id)
        if board is not None and isinstance(board.settings, dict):
            settings = dict(board.settings)
            settings.pop(HISTORICAL_PROGRESS_SETTINGS_KEY, None)
            board.settings = settings
            flag_modified(board, "settings")

    async def stage_board_erasure_job(
        self,
        context: Any,
        *,
        board_id: str,
        actor_id: str,
    ) -> BoardErasureJobFact:
        del context
        if board_id in self._board_erasure_jobs:
            raise RuntimeError(f"board_erasure_job_conflict:{board_id}")
        job = BoardErasureJobFact(
            board_id=board_id,
            actor_id=actor_id,
            attempts=0,
            last_error=None,
            next_attempt_at=datetime.now(timezone.utc),
        )
        self._board_erasure_jobs[board_id] = job
        return job

    async def get_board_erasure_job(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> BoardErasureJobFact | None:
        del context
        return self._board_erasure_jobs.get(board_id)

    async def list_due_board_erasure_jobs(
        self,
        context: Any,
        *,
        now: datetime,
        limit: int,
    ) -> tuple[BoardErasureJobFact, ...]:
        del context
        due = sorted(
            (
                job
                for job in self._board_erasure_jobs.values()
                if job.next_attempt_at <= now
            ),
            key=lambda job: (job.next_attempt_at, job.board_id),
        )
        return tuple(due[:limit])

    async def record_board_erasure_failure(
        self,
        context: Any,
        *,
        board_id: str,
        error: str,
        next_attempt_at: datetime,
    ) -> None:
        del context
        job = self._board_erasure_jobs.get(board_id)
        if job is None:
            return
        self._board_erasure_jobs[board_id] = BoardErasureJobFact(
            board_id=job.board_id,
            actor_id=job.actor_id,
            attempts=job.attempts + 1,
            last_error=error,
            next_attempt_at=next_attempt_at,
        )

    async def complete_board_erasure_job(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> bool:
        del context
        return self._board_erasure_jobs.pop(board_id, None) is not None

    def add_boost_audit(self, context: Any, audit: BoostAuditRecord) -> None:
        context.add(
            ConsolidationAudit(
                session_id=audit.session_id,
                board_id=audit.board_id,
                artifact_id=audit.artifact_id,
                artifact_type="boost",
                agent_id=audit.agent_id,
                started_at=audit.started_at,
                committed_at=audit.committed_at,
                nodes_added=0,
                edges_added=0,
            )
        )

    async def commit(self, context: Any) -> None:
        await context.commit()


__all__ = ["TestSqlAlchemyKGGovernanceStore"]
