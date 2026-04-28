"""REST endpoint for the consolidation queue health snapshot.

Spec bdcda842 (FR9): GET /api/v1/kg/queue/health returns 13 live metrics
the EventQueueTab in the frontend polls every 2s. The endpoint is read-only
and never touches Kùzu — only the SQLite app DB + in-process counters.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.services.queue_health_service import get_queue_health

router = APIRouter()


class QueueHealthResponse(BaseModel):
    """Live consolidation queue health snapshot."""

    queue_depth: int
    oldest_pending_age_s: float
    claimed_count: int
    claimed_boards: list[str]
    dead_letter_count: int
    claims_per_min_1m: int
    claims_per_min_5m: int
    alert_threshold: int
    alert_active: bool
    alert_fired_total: int
    workers_active: int
    workers_idle: int
    workers_draining_count: int
    kuzu_lock_retries_5m: int


@router.get("/kg/queue/health", response_model=QueueHealthResponse)
async def get_kg_queue_health(
    _: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> QueueHealthResponse:
    """Return the live snapshot of consolidation queue health.

    Polled by the frontend EventQueueTab at 2s intervals (TR10). Safe to
    invoke at any frequency — counts are SQL aggregates and in-process
    sliding windows; no Kùzu I/O.
    """
    data = await get_queue_health(db)
    return QueueHealthResponse(**data)
