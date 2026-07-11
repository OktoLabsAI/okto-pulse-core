"""Dead Letter Inspector service (spec ed17b1fe - Wave 2 NC 1ede3471)."""

from __future__ import annotations

from typing import Any, Iterable

from okto_pulse.core.application.processors.dead_letter import list_dead_letter
from okto_pulse.core.ports.kg_operational import get_kg_worker_queue_port


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


def _row_to_dict(row: Any) -> dict[str, Any]:
    errors = _normalise_errors(row.errors)
    # AC6 / ts_c604a02b: derive last_error/error_text from the most recent
    # attempt, keeping the full errors[] history intact.
    last_error = errors[-1].get("message") if errors else None
    return {
        "id": row.id,
        # FR6/AC6 alias (spec 007d1308): contract field name, kept additive so
        # the existing DLQ Inspector consumer (spec 1ede3471) reading `id`
        # still works.
        "dead_letter_id": row.id,
        "board_id": row.board_id,
        "artifact_type": row.artifact_type,
        "artifact_id": row.artifact_id,
        "original_queue_id": row.original_queue_id,
        "attempts": row.attempts,
        "errors": errors,
        "last_error": last_error,
        "error_text": last_error,
        # SPEC4 (card 2e913ac3, AC ac_26acf1db): bounded suggested next action so
        # the DLQ row is actionable from the drill-down alone. Every DLQ row is a
        # terminal consolidation failure → inspect the error then reprocess.
        "next_action": "inspect_last_error_then_reprocess_via_okto_pulse_kg_dead_letter_reprocess",
        "dead_lettered_at": (
            row.dead_lettered_at.isoformat()
            if row.dead_lettered_at
            else None
        ),
    }


async def list_cognitive_dlq_rows(
    db: object,
    board_id: str,
    *,
    limit: int,
    offset: int,
) -> tuple[int, list[Any]]:
    """Read the board's technical-DLQ rows for the cognitive DLQ surface
    (spec R01A MCP-FU3B): the total count + a page of ``ConsolidationDeadLetter``
    rows ordered by id. Extracted verbatim from the inline query in the
    ``okto_pulse_kg_list_cognitive_dlq`` MCP tool so that tool no longer issues SQL
    directly; the row projection (normalized artifact id, technical_dlq framing)
    stays in the adapter."""
    total, rows = await get_kg_worker_queue_port().list_dead_letter_page(
        db,
        board_id=board_id,
        limit=limit,
        offset=offset,
    )
    return total, list(rows)


async def list_dead_letter_rows(
    db: object,
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
    projected = [_row_to_dict(r) for r in sliced]
    return {
        "rows": projected,
        # FR6/AC6 alias (spec 007d1308): `items` is the contract name; `rows`
        # is preserved for the existing DLQ Inspector consumer.
        "items": projected,
        "total": len(rows),
        "limit": limit,
        "offset": offset,
    }


async def reprocess_dead_letter_rows(
    db: object,
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

    return dict(
        await get_kg_worker_queue_port().reprocess_dead_letter_rows(
            db,
            board_id=board_id,
            dead_letter_ids=ids,
            limit=limit,
        )
    )
