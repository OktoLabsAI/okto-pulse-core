"""Test-only SQLAlchemy KG health read adapter."""

from typing import Sequence

from sqlalchemy import func, select

from sqlalchemy_test_models import (
    Board, CanonicalDebt, ConsolidationAudit, ConsolidationDeadLetter,
    ConsolidationQueue, KGTickRun, KuzuNodeRef,
)
from okto_pulse.core.ports.kg_health import KGHealthQueueSnapshot, KGTickRunFact


class TestSqlAlchemyKGHealthReader:
    __test__ = False

    async def queue_snapshot(self, context, *, board_id: str) -> KGHealthQueueSnapshot:
        active = (
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.status.in_(("pending", "claimed")),
        )
        depth = await context.scalar(select(func.count()).where(*active))
        oldest = await context.scalar(
            select(func.min(ConsolidationQueue.triggered_at)).where(*active)
        )
        dead = await context.scalar(
            select(func.count()).where(ConsolidationDeadLetter.board_id == board_id)
        )
        return KGHealthQueueSnapshot(
            await context.get(Board, board_id) is not None,
            int(depth or 0), oldest, int(dead or 0),
        )

    async def list_tick_runs(self, context) -> tuple[KGTickRunFact, ...]:
        rows = (await context.execute(select(KGTickRun))).scalars().all()
        return tuple(
            KGTickRunFact(
                row.started_at, row.completed_at, int(row.nodes_recomputed or 0),
                int(row.boards_processed or 0), int(row.boards_failed or 0), row.error,
            )
            for row in rows
        )

    async def has_materialized_history(self, context, *, board_id: str) -> bool:
        refs = await context.scalar(
            select(func.count()).where(KuzuNodeRef.board_id == board_id)
        )
        if int(refs or 0) > 0:
            return True
        nodes = await context.scalar(
            select(func.coalesce(func.sum(ConsolidationAudit.nodes_added), 0)).where(
                ConsolidationAudit.board_id == board_id
            )
        )
        return int(nodes or 0) > 0

    async def count_partition_debt(
        self,
        context,
        *,
        board_id: str,
        target_status: str,
        open_states: Sequence[str],
    ) -> int:
        value = await context.scalar(
            select(func.count()).select_from(CanonicalDebt).where(
                CanonicalDebt.board_id == board_id,
                CanonicalDebt.target_status == target_status,
                CanonicalDebt.canonical_state.in_(tuple(open_states)),
            )
        )
        return int(value or 0)


__all__ = ["TestSqlAlchemyKGHealthReader"]
