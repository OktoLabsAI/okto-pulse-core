"""REST endpoints for CanonicalDebt ledger."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.api.deps import scheduler_control_from_request
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.services.canonical_debt_service import (
    list_canonical_debt,
    schedule_canonical_debt_retry,
)
from okto_pulse.core.services.kg_health_service import get_kg_health

router = APIRouter(prefix="/kg/canonical-debt")


class CanonicalDebtListResponse(BaseModel):
    board_id: str
    items: list[dict[str, Any]]
    counts: dict[str, Any]
    total: int
    limit: int
    offset: int


class CanonicalDebtRetryResponse(BaseModel):
    ok: bool
    attempt_consumed: bool = False
    kg_health_state: str | None = None
    debt: dict[str, Any] | None = None
    error: str | None = None


@router.get("", response_model=CanonicalDebtListResponse)
async def get_canonical_debt(
    board_id: str = Query(..., min_length=1),
    artifact_type: str | None = Query(None),
    state: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> CanonicalDebtListResponse:
    from okto_pulse.core.api.kg_rebuild import _require_board_access

    await _require_board_access(board_id, user_id, db)
    result = await list_canonical_debt(
        db,
        board_id=board_id,
        artifact_type=artifact_type,
        state=state,
        limit=limit,
        offset=offset,
    )
    return CanonicalDebtListResponse(
        board_id=board_id,
        items=result.items,
        counts=result.counts,
        total=result.total,
        limit=limit,
        offset=offset,
    )


@router.post("/{debt_id}/retry", response_model=CanonicalDebtRetryResponse)
async def retry_canonical_debt(
    request: Request,
    debt_id: str = Path(..., min_length=1),
    board_id: str = Query(..., min_length=1),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> CanonicalDebtRetryResponse:
    from okto_pulse.core.api.kg_rebuild import _require_board_access

    await _require_board_access(board_id, user_id, db)
    health = await get_kg_health(
        board_id,
        db,
        scheduler_control=scheduler_control_from_request(request),
    )
    result = await schedule_canonical_debt_retry(
        db,
        board_id=board_id,
        debt_id=debt_id,
        actor_id=user_id,
        kg_health_state=str(health.get("overall_state") or health.get("graph_state")),
    )
    if not result.get("ok") and result.get("error") == "canonical_debt_not_found":
        raise HTTPException(status_code=404, detail="canonical debt item not found")
    return CanonicalDebtRetryResponse(**result)


__all__ = ["router"]
