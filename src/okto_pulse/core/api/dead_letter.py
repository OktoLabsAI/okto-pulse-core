"""Dead Letter Inspector REST endpoint (spec ed17b1fe — Wave 2 NC 1ede3471).

GET /api/v1/kg/queue/dead-letter — read-only listing of DLQ rows for a
board. Pagination via ``limit`` (1-200, default 50) + ``offset`` (>=0,
default 0). Filter by board access enforced at auth layer (same pattern
as queue_health endpoint).

Reprocess endpoint deferred to v2 — see spec D1 (MVP read-only — reprocess
deferred).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.services.dead_letter_inspector_service import (
    list_dead_letter_rows,
)

router = APIRouter()


class DeadLetterRow(BaseModel):
    id: str
    board_id: str
    artifact_type: str
    artifact_id: str
    original_queue_id: str | None
    attempts: int
    errors: list[dict[str, Any]]
    dead_lettered_at: str | None


class DeadLetterListResponse(BaseModel):
    rows: list[DeadLetterRow]
    total: int
    limit: int
    offset: int


@router.get(
    "/kg/queue/dead-letter",
    response_model=DeadLetterListResponse,
)
async def get_dead_letter(
    board_id: str = Query(..., description="Board UUID (required)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _user: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> DeadLetterListResponse:
    """List dead-lettered consolidation rows for a board.

    Returns ``{rows, total, limit, offset}``. Each row includes the
    full ``errors[]`` history from the TR16 schema (one entry per
    attempt: error_type, message, occurred_at, traceback).
    """
    data = await list_dead_letter_rows(
        db, board_id, limit=limit, offset=offset,
    )
    return DeadLetterListResponse(**data)
