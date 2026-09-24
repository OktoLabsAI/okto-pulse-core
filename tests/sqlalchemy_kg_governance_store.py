"""Test-only SQLAlchemy KG governance store."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import delete
from sqlalchemy.orm.attributes import flag_modified

from sqlalchemy_test_models import (
    Board,
    ConsolidationAudit,
    ConsolidationQueue,
    GlobalUpdateOutbox,
    KuzuNodeRef,
)
from okto_pulse.core.ports.kg_events import HISTORICAL_PROGRESS_SETTINGS_KEY
from okto_pulse.core.ports.kg_governance import BoardErasureJobFact


class TestSqlAlchemyKGGovernanceStore:
    __test__ = False

    def __init__(self) -> None:
        self._board_erasure_jobs: dict[str, BoardErasureJobFact] = {}













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


    async def commit(self, context: Any) -> None:
        await context.commit()


__all__ = ["TestSqlAlchemyKGGovernanceStore"]
