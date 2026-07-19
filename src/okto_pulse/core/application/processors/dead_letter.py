"""Dead-letter routing for the consolidation queue (spec bdcda842 IMPL-3).

After ``kg_queue_max_attempts`` consecutive failures of a consolidation
attempt, the queue entry is moved into the adapter-owned dead-letter store and
removed from the active queue. The DLQ record preserves the full attempt
history in the ``errors`` JSON array following the schema fixed by TR16/AC17:

    {
        "attempt": <int, 1-based>,
        "occurred_at": <ISO8601 UTC string>,
        "error_type": <str, exception class name>,
        "message": <str, ≤500 chars>,
        "traceback": <str|None, ≤2000 chars when DEBUG, else null>,
        "recovery_class": "connectivity"|"invalid_payload"|"true_drift",
        "reason_code": <stable str>,
        "replay_safe": <bool>,
        "correlation_id": <uuid>
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

from okto_pulse.core.ports.kg_operational import (
    KGQueueEntrySnapshot,
    classify_kg_recovery_failure,
    get_kg_worker_queue_port,
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
    correlation_id: str | None = None,
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
    classification = classify_kg_recovery_failure(error_type, message)
    return {
        "attempt": int(attempt),
        "occurred_at": when.isoformat(),
        "error_type": str(error_type)[:80],
        "message": str(message)[:_MESSAGE_TRUNCATE_CHARS],
        "traceback": tb_text,
        "recovery_class": classification.recovery_class,
        "reason_code": classification.reason_code,
        "replay_safe": classification.replay_safe,
        "correlation_id": correlation_id or str(uuid.uuid4()),
    }


def _accumulate_history(
    queue_entry: KGQueueEntrySnapshot,
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
            history.append(
                build_attempt_entry(
                    attempt=n,
                    occurred_at=datetime.fromisoformat(placeholder_ts),
                    error_type="PriorAttempt",
                    message=previous_message or "(no message captured)",
                )
            )
    history.append(final_entry)
    return history


def queue_entry_snapshot(queue_entry: Any) -> KGQueueEntrySnapshot:
    return KGQueueEntrySnapshot(
        id=str(queue_entry.id),
        board_id=str(queue_entry.board_id),
        artifact_type=str(queue_entry.artifact_type),
        artifact_id=str(queue_entry.artifact_id),
        attempts=int(queue_entry.attempts or 0),
        last_error=queue_entry.last_error,
    )


async def route_to_dead_letter(
    context: Any,
    queue_entry: Any,
    *,
    error_text: str,
    error_type: str | None = None,
    capture_traceback: bool = False,
) -> Any:
    """Move ``queue_entry`` to the adapter-owned DLQ and remove it from queue.

    The caller is responsible for committing the surrounding transaction.
    """
    snapshot = (
        queue_entry
        if isinstance(queue_entry, KGQueueEntrySnapshot)
        else queue_entry_snapshot(queue_entry)
    )
    parts = error_text.split(":", 1)
    if error_type is None:
        error_type = parts[0].strip() if parts else "UnknownError"
    message = (parts[1].strip() if len(parts) > 1 else error_text).strip()

    final_entry = build_attempt_entry(
        attempt=snapshot.attempts or 1,
        error_type=error_type or "UnknownError",
        message=message,
        include_traceback=capture_traceback,
    )
    history = _accumulate_history(snapshot, final_entry)
    dlq_row = await get_kg_worker_queue_port().route_to_dead_letter(
        context,
        queue_entry=snapshot,
        errors=history,
    )

    logger.warning(
        "consolidation.dead_letter board=%s artifact=%s:%s attempts=%d "
        "last_error=%s",
        snapshot.board_id, snapshot.artifact_type,
        snapshot.artifact_id, snapshot.attempts or 0,
        message[:120],
        extra={
            "event": "kg.queue.dead_letter",
            "board_id": snapshot.board_id,
            "artifact_type": snapshot.artifact_type,
            "artifact_id": snapshot.artifact_id,
            "attempts": snapshot.attempts or 0,
            "error_type": error_type,
        },
    )
    return dlq_row


async def list_dead_letter(
    context: Any,
    board_id: str,
    *,
    limit: int = 100,
) -> list[Any]:
    """Return up to ``limit`` most recent DLQ rows for a board."""
    rows = await get_kg_worker_queue_port().list_dead_letter(
        context,
        board_id=board_id,
        limit=limit,
    )
    return list(rows)
