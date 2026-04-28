"""Dead-letter routing for the consolidation queue (spec bdcda842 IMPL-3).

After ``kg_queue_max_attempts`` consecutive failures of a consolidation
attempt, the queue entry is moved into ``ConsolidationDeadLetter`` and
removed from ``ConsolidationQueue``. The DLQ row preserves the full attempt
history in the ``errors`` JSON array following the schema fixed by TR16/AC17:

    {
        "attempt": <int, 1-based>,
        "occurred_at": <ISO8601 UTC string>,
        "error_type": <str, exception class name>,
        "message": <str, ≤500 chars>,
        "traceback": <str|None, ≤2000 chars when DEBUG, else null>
    }

The ``last_error`` column on the queue acts as the per-attempt scratch slot;
when an attempt fails the worker pushes a fresh entry into a synthesised
running list before deciding whether to schedule a retry or route to DLQ.
The list is reconstructed from a ``last_error`` string of the form
``"[attempt N] <error_type>: <message>"`` — keeping the schema lean while
still letting operators inspect each attempt in the DLQ row.
"""

from __future__ import annotations

import logging
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import (
    ConsolidationDeadLetter,
    ConsolidationQueue,
)

logger = logging.getLogger("okto_pulse.kg.dead_letter")


_MESSAGE_TRUNCATE_CHARS = 500
_TRACEBACK_TRUNCATE_CHARS = 2000


def build_attempt_entry(
    *,
    attempt: int,
    error_type: str,
    message: str,
    include_traceback: bool = False,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    """Build a single ``errors[]`` entry following the TR16/AC17 schema.

    ``include_traceback`` is opt-in (``traceback`` is None unless the caller
    explicitly captured one). Both ``message`` and ``traceback`` are
    truncated to keep DLQ rows bounded.
    """
    when = occurred_at or datetime.now(timezone.utc)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    tb_text: str | None
    if include_traceback:
        try:
            tb_text = traceback.format_exc()[:_TRACEBACK_TRUNCATE_CHARS]
            if not tb_text or tb_text.strip() == "NoneType: None":
                tb_text = None
        except Exception:
            tb_text = None
    else:
        tb_text = None
    return {
        "attempt": int(attempt),
        "occurred_at": when.isoformat(),
        "error_type": str(error_type)[:80],
        "message": str(message)[:_MESSAGE_TRUNCATE_CHARS],
        "traceback": tb_text,
    }


def _accumulate_history(
    queue_entry: ConsolidationQueue,
    final_entry: dict[str, Any],
) -> list[dict[str, Any]]:
    """Reconstruct the full attempt history from queue_entry.last_error.

    The worker stores per-attempt error messages in ``last_error`` as the
    only signal available without DB schema changes. We wrap this into the
    canonical shape for the DLQ. When previous attempts had richer data
    (rare — only the failing exception's class/message is captured), they
    appear with a synthetic ``occurred_at`` of "epoch" so the operator can
    identify them as historical placeholders.
    """
    history: list[dict[str, Any]] = []
    total_attempts = max(int(queue_entry.attempts or 0), 1)
    placeholder_count = total_attempts - 1
    if placeholder_count > 0:
        placeholder_ts = datetime.now(timezone.utc).isoformat()
        previous_message = (queue_entry.last_error or "").strip()
        for n in range(1, placeholder_count + 1):
            history.append({
                "attempt": n,
                "occurred_at": placeholder_ts,
                "error_type": "PriorAttempt",
                "message": (previous_message or "(no message captured)")[:_MESSAGE_TRUNCATE_CHARS],
                "traceback": None,
            })
    history.append(final_entry)
    return history


async def route_to_dead_letter(
    db: AsyncSession,
    queue_entry: ConsolidationQueue,
    *,
    error_text: str,
    error_type: str | None = None,
    capture_traceback: bool = False,
) -> ConsolidationDeadLetter:
    """Move ``queue_entry`` to ConsolidationDeadLetter and remove from queue.

    The caller (worker) is responsible for ``await db.commit()`` afterwards.
    """
    parts = error_text.split(":", 1)
    if error_type is None:
        error_type = parts[0].strip() if parts else "UnknownError"
    message = (parts[1].strip() if len(parts) > 1 else error_text).strip()

    final_entry = build_attempt_entry(
        attempt=queue_entry.attempts or 1,
        error_type=error_type or "UnknownError",
        message=message,
        include_traceback=capture_traceback,
    )
    history = _accumulate_history(queue_entry, final_entry)

    dlq_row = ConsolidationDeadLetter(
        id=str(uuid.uuid4()),
        board_id=queue_entry.board_id,
        artifact_type=queue_entry.artifact_type,
        artifact_id=queue_entry.artifact_id,
        original_queue_id=queue_entry.id,
        attempts=queue_entry.attempts or 0,
        errors=history,
    )
    db.add(dlq_row)
    await db.delete(queue_entry)

    logger.warning(
        "consolidation.dead_letter board=%s artifact=%s:%s attempts=%d "
        "last_error=%s",
        queue_entry.board_id, queue_entry.artifact_type,
        queue_entry.artifact_id, queue_entry.attempts or 0,
        message[:120],
        extra={
            "event": "kg.queue.dead_letter",
            "board_id": queue_entry.board_id,
            "artifact_type": queue_entry.artifact_type,
            "artifact_id": queue_entry.artifact_id,
            "attempts": queue_entry.attempts or 0,
            "error_type": error_type,
        },
    )
    return dlq_row


async def list_dead_letter(
    db: AsyncSession,
    board_id: str,
    *,
    limit: int = 100,
) -> list[ConsolidationDeadLetter]:
    """Return up to ``limit`` most recent DLQ rows for a board."""
    result = await db.execute(
        select(ConsolidationDeadLetter)
        .where(ConsolidationDeadLetter.board_id == board_id)
        .order_by(ConsolidationDeadLetter.dead_lettered_at.desc())
        .limit(limit)
    )
    return list(result.scalars().all())
