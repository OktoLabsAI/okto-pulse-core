"""Traceability API endpoints for SDLC lineage visualizations."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.database import get_db
from okto_pulse.core.services.traceability import (
    TraceabilityReadError,
    build_lineage_graph,
)

router = APIRouter(prefix="/boards", tags=["traceability"])


@router.get("/{board_id}/lineage-graph")
async def get_lineage_graph(
    board_id: str,
    entity_type: str = Query(..., min_length=1),
    entity_id: str = Query(..., min_length=1),
    include_artifacts: bool = False,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return a UI-only SDLC lineage graph rooted at the selected entity."""
    try:
        return await build_lineage_graph(
            db,
            board_id,
            entity_type=entity_type,
            entity_id=entity_id,
            include_artifacts=include_artifacts,
        )
    except TraceabilityReadError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"code": exc.code, "message": exc.message},
        ) from exc
