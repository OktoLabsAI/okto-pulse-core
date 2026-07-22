"""Test-only SQLAlchemy global outbox store."""

import copy
from typing import Any, Sequence

from sqlalchemy import and_, func, or_, select

from sqlalchemy_test_models import GlobalUpdateOutbox, KuzuNodeRef
from okto_pulse.core.ports.global_outbox import (
    GlobalOutboxDeadLetterCursor,
    GlobalOutboxEventRecord,
    GlobalOutboxNodeRefFact,
)


def _record(row: Any) -> GlobalOutboxEventRecord:
    return GlobalOutboxEventRecord(
        id=str(row.id), event_id=str(row.event_id), board_id=str(row.board_id),
        session_id=str(row.session_id) if row.session_id else None,
        payload=copy.deepcopy(row.payload or {}), retry_count=int(row.retry_count),
        last_error=row.last_error, processed_at=row.processed_at,
        created_at=row.created_at,
        event_type=str(row.event_type),
    )


class TestSqlAlchemyGlobalOutboxStore:
    __test__ = False

    async def materialize_claimed(
        self, context, claimed: Sequence[Any]
    ) -> tuple[GlobalOutboxEventRecord, ...]:
        return tuple(
            item if isinstance(item, GlobalOutboxEventRecord) else _record(item)
            for item in claimed
        )

    async def list_dead_letters(
        self,
        context,
        *,
        limit: int,
        error_markers: Sequence[str],
        after: GlobalOutboxDeadLetterCursor | None = None,
    ) -> tuple[GlobalOutboxEventRecord, ...]:
        predicates = [
            func.instr(
                func.lower(GlobalUpdateOutbox.last_error),
                marker.lower(),
            )
            > 0
            for marker in error_markers
            if marker
        ]
        query = select(GlobalUpdateOutbox).where(
            GlobalUpdateOutbox.processed_at.is_(None),
            GlobalUpdateOutbox.retry_count == -1,
        )
        if predicates:
            query = query.where(or_(*predicates))
        if after is not None:
            query = query.where(
                or_(
                    GlobalUpdateOutbox.created_at > after.created_at,
                    and_(
                        GlobalUpdateOutbox.created_at == after.created_at,
                        GlobalUpdateOutbox.id > after.id,
                    ),
                )
            )
        rows = (
            await context.execute(
                query.order_by(
                    GlobalUpdateOutbox.created_at.asc(),
                    GlobalUpdateOutbox.id.asc(),
                )
                .limit(limit)
            )
        ).scalars().all()
        return tuple(_record(row) for row in rows)

    async def list_added_node_refs(
        self,
        context,
        *,
        session_id: str,
        board_id: str,
        node_types: Sequence[str],
    ) -> tuple[GlobalOutboxNodeRefFact, ...]:
        rows = (
            await context.execute(
                select(KuzuNodeRef).where(
                    KuzuNodeRef.session_id == session_id,
                    KuzuNodeRef.board_id == board_id,
                    KuzuNodeRef.operation == "add",
                    KuzuNodeRef.kuzu_node_type.in_(tuple(node_types)),
                )
            )
        ).scalars().all()
        return tuple(
            GlobalOutboxNodeRefFact(str(row.kuzu_node_id), str(row.kuzu_node_type))
            for row in rows
        )

    async def save_events(
        self, context, events: Sequence[GlobalOutboxEventRecord]
    ) -> None:
        for event in events:
            row = await context.get(GlobalUpdateOutbox, event.id)
            if row is None:
                continue
            row.retry_count = event.retry_count
            row.last_error = event.last_error
            row.processed_at = event.processed_at
        await context.flush()

    async def commit(self, context) -> None:
        await context.commit()


__all__ = ["TestSqlAlchemyGlobalOutboxStore"]
