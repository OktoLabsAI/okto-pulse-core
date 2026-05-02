"""Dead Letter Inspector service (spec ed17b1fe - Wave 2 NC 1ede3471)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.workers.dead_letter import list_dead_letter
from okto_pulse.core.models.db import ConsolidationDeadLetter, ConsolidationQueue


def _normalise_errors(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        normalised: list[dict[str, Any]] = []
        for index, item in enumerate(value, start=1):
            if isinstance(item, dict):
                normalised.append(item)
            else:
                normalised.append({
                    "attempt": index,
                    "occurred_at": "",
                    "error_type": "LegacyError",
                    "message": str(item),
                    "traceback": None,
                })
        return normalised
    if isinstance(value, dict):
        return [value]
    if value:
        return [{
            "attempt": 1,
            "occurred_at": "",
            "error_type": "LegacyError",
            "message": str(value),
            "traceback": None,
        }]
    return []


def _normalise_errors(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        normalised: list[dict[str, Any]] = []
        for index, item in enumerate(value, start=1):
            if isinstance(item, dict):
                normalised.append(item)
            else:
                normalised.append({
                    "attempt": index,
                    "occurred_at": "",
                    "error_type": "LegacyError",
                    "message": str(item),
                    "traceback": None,
                })
        return normalised
    if isinstance(value, dict):
        return [value]
    if value:
        return [{
            "attempt": 1,
            "occurred_at": "",
            "error_type": "LegacyError",
            "message": str(value),
            "traceback": None,
        }]
    return []


def _row_to_dict(row: ConsolidationDeadLetter) -> dict[str, Any]:
    return {
        "id": row.id,
        "board_id": row.board_id,
        "artifact_type": row.artifact_type,
        "artifact_id": row.artifact_id,
        "original_queue_id": row.original_queue_id,
        "attempts": row.attempts,
        "errors": _normalise_errors(row.errors),
        "dead_lettered_at": (
            row.dead_lettered_at.isoformat()
            if row.dead_lettered_at
            else None
        ),
    }


async def list_dead_letter_rows(
    db: AsyncSession,
    board_id: str,
    *,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Paginated list of DLQ rows for a board.

    Returns ``{rows, total, limit, offset}`` matching the REST + MCP
    response shape. ``rows`` is the window ``[offset:offset+limit]``;
    ``total`` is the count of all DLQ rows for the board (capped at
    ``limit + offset`` due to underlying helper signature — fine for
    MVP where max limit is 200).
    """
    rows = await list_dead_letter(db, board_id, limit=limit + offset)
    sliced = rows[offset:offset + limit]
    return {
        "rows": [_row_to_dict(r) for r in sliced],
        "total": len(rows),
        "limit": limit,
        "offset": offset,
    }


async def reprocess_dead_letter_rows(
    db: AsyncSession,
    board_id: str,
    *,
    dead_letter_ids: Iterable[str] | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Move DLQ rows back to ConsolidationQueue for another processing attempt.

    This is intentionally idempotent. If a queue row for the same
    board/artifact already exists, the DLQ row is cleared and counted as
    already_queued instead of inserting a duplicate that would violate the
    queue uniqueness constraint.
    """
    limit = max(1, min(int(limit or 50), 200))
    ids = [str(item) for item in (dead_letter_ids or []) if str(item).strip()]

    query = (
        select(ConsolidationDeadLetter)
        .where(ConsolidationDeadLetter.board_id == board_id)
    )
    if ids:
        query = query.where(ConsolidationDeadLetter.id.in_(ids))
    query = query.order_by(ConsolidationDeadLetter.dead_lettered_at.asc()).limit(limit)

    rows = list((await db.execute(query)).scalars().all())
    requeued: list[dict[str, Any]] = []
    already_queued: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for row in rows:
        existing_result = await db.execute(
            select(ConsolidationQueue).where(
                ConsolidationQueue.board_id == row.board_id,
                ConsolidationQueue.artifact_type == row.artifact_type,
                ConsolidationQueue.artifact_id == row.artifact_id,
            )
        )
        existing = existing_result.scalar_one_or_none()
        if existing is None:
            queue_row = ConsolidationQueue(
                board_id=row.board_id,
                artifact_type=row.artifact_type,
                artifact_id=row.artifact_id,
                priority="high",
                source="dead_letter_reprocess",
                status="pending",
                triggered_at=now,
                triggered_by_event="dlq_reprocess",
                attempts=0,
                last_error=None,
                next_retry_at=None,
                claimed_at=None,
                claim_timeout_at=None,
                worker_id=None,
                claimed_by_session_id=None,
            )
            db.add(queue_row)
            await db.flush()
            requeued.append({
                "dead_letter_id": row.id,
                "queue_id": queue_row.id,
                "artifact_type": row.artifact_type,
                "artifact_id": row.artifact_id,
            })
        else:
            existing.status = "pending"
            existing.attempts = 0
            existing.last_error = None
            existing.next_retry_at = None
            existing.claimed_at = None
            existing.claim_timeout_at = None
            existing.worker_id = None
            existing.claimed_by_session_id = None
            already_queued.append({
                "dead_letter_id": row.id,
                "queue_id": existing.id,
                "artifact_type": row.artifact_type,
                "artifact_id": row.artifact_id,
            })
        await db.delete(row)

    return {
        "success": True,
        "requested": len(ids) if ids else None,
        "selected": len(rows),
        "requeued": requeued,
        "already_queued": already_queued,
        "requeued_count": len(requeued),
        "already_queued_count": len(already_queued),
    }
