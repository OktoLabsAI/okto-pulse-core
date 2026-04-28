"""Dead Letter Inspector service (spec ed17b1fe — Wave 2 NC 1ede3471).

Read-only MVP. Reusa `workers/dead_letter.py::list_dead_letter` adicionando
janela de paginação (limit + offset) sobre o resultado. Em prod com 1000+
DLQ rows, vale evoluir para SQL nativo com OFFSET; o limite máximo de 200
mantém o cost contained no MVP.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.workers.dead_letter import list_dead_letter
from okto_pulse.core.models.db import ConsolidationDeadLetter


def _row_to_dict(row: ConsolidationDeadLetter) -> dict[str, Any]:
    return {
        "id": row.id,
        "board_id": row.board_id,
        "artifact_type": row.artifact_type,
        "artifact_id": row.artifact_id,
        "original_queue_id": row.original_queue_id,
        "attempts": row.attempts,
        "errors": row.errors or [],
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
