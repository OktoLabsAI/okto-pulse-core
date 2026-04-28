"""REST endpoint for the KG health snapshot.

Spec 20f67c2a (Ideação #5, FR1). GET /api/v1/kg/health?board_id=X returns
10 fields the frontend (and ad-hoc curl) can poll to surface the live
state of a board's knowledge graph. Pattern mirrors queue_health.py.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.services.kg_health_service import (
    BoardNotFoundError,
    get_kg_health,
)

router = APIRouter()


class TopDisconnectedNode(BaseModel):
    id: str
    type: str
    degree: int


class KGHealthResponse(BaseModel):
    """Live KG health snapshot for one board."""

    queue_depth: int
    oldest_pending_age_s: float
    dead_letter_count: int
    total_nodes: int
    default_score_count: int
    default_score_ratio: float
    avg_relevance: float
    top_disconnected_nodes: list[TopDisconnectedNode]
    schema_version: str
    contradict_warn_count: int
    last_decay_tick_at: str | None = None
    nodes_recomputed_in_last_tick: int = 0


@router.get("/kg/health", response_model=KGHealthResponse)
async def get_kg_health_endpoint(
    board_id: str = Query(..., description="Board ID (uuid)"),
    _: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> KGHealthResponse:
    """Return the live KG health snapshot for ``board_id``.

    Compute is in-process: SQL aggregations on the app DB and
    per-node-type queries against the board's Kùzu graph. Kùzu errors
    degrade gracefully (zeros), so the endpoint stays available even when
    Kùzu hasn't been bootstrapped or is under a transient lock.
    """
    try:
        data = await get_kg_health(board_id, db)
    except BoardNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return KGHealthResponse(**data)
