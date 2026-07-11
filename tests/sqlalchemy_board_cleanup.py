"""Test-only SQLAlchemy board runtime cleanup adapter."""

from sqlalchemy import delete

from okto_pulse.core.infra.database import get_session_factory
from sqlalchemy_test_models import (
    ConsolidationAudit,
    ConsolidationQueue,
    GlobalUpdateOutbox,
)


class TestSqlAlchemyBoardRelationalCleanup:
    __test__ = False

    async def wipe_runtime_rows(self, *, board_id: str) -> dict[str, int]:
        removed: dict[str, int] = {}
        async with get_session_factory()() as session:
            for model, label in (
                (GlobalUpdateOutbox, "outbox"),
                (ConsolidationAudit, "audit"),
                (ConsolidationQueue, "queue"),
            ):
                result = await session.execute(
                    delete(model).where(model.board_id == board_id)
                )
                removed[label] = int(result.rowcount or 0)
            await session.commit()
        return removed


__all__ = ["TestSqlAlchemyBoardRelationalCleanup"]
