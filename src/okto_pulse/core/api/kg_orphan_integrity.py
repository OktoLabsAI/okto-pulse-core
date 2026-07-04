"""REST surfaces for KG orphan integrity reports and explicit backfill."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.api.deps import scheduler_control_from_request
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.kg.orphan_integrity import (
    DEFAULT_ORPHAN_SAMPLE_LIMIT,
    MAX_ORPHAN_SAMPLE_LIMIT,
    OrphanBackfillReconciler,
    OrphanNodeScanner,
)
from okto_pulse.core.ports.scheduler import SchedulerControl
from okto_pulse.core.services.kg_health_service import get_kg_health

router = APIRouter()


class OrphanBackfillRequest(BaseModel):
    board_id: str
    generation_id: str | None = None
    dry_run: bool = True
    node_ids: list[str] | None = None
    limit: int = Field(default=DEFAULT_ORPHAN_SAMPLE_LIMIT, ge=0)


async def _ensure_backfill_allowed(
    board_id: str,
    db: AsyncSession,
    *,
    scheduler_control: SchedulerControl | None = None,
) -> None:
    health = await get_kg_health(
        board_id,
        db,
        scheduler_control=scheduler_control,
    )
    state = str(health.get("overall_state") or health.get("graph_state") or "")
    if state in {"recovery_needed", "quarantined"}:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "kg_orphan_backfill_refused_by_health",
                "board_id": board_id,
                "overall_state": health.get("overall_state"),
                "graph_state": health.get("graph_state"),
                "operator_action": "inspect_kg_health_recovery_flow",
            },
        )


def _graph_unavailable_error(board_id: str, exc: Exception) -> HTTPException:
    return HTTPException(
        status_code=503,
        detail={
            "error": "kg_orphan_graph_unavailable",
            "board_id": board_id,
            "error_type": type(exc).__name__,
            "operator_action": "inspect_kg_health",
        },
    )


def _report_payload(
    *,
    board_id: str,
    generation_id: str | None,
    limit: int,
) -> dict[str, Any]:
    report = OrphanNodeScanner().scan(
        board_id=board_id,
        generation_id=generation_id,
        limit=limit,
    )
    payload = report.to_safe_dict()
    payload["backfill_summary"] = {
        "status": "not_run",
        "dry_run": None,
        "detected": None,
        "connected": None,
        "noop": None,
        "unresolved": None,
        "ambiguous": None,
        "semantic_pending": None,
    }
    return payload


@router.get("/kg/orphan-integrity/report")
async def get_kg_orphan_integrity_report(
    board_id: str = Query(..., description="Board ID"),
    generation_id: str | None = Query(None, description="Optional KG generation ID"),
    limit: int = Query(
        DEFAULT_ORPHAN_SAMPLE_LIMIT,
        ge=0,
        le=MAX_ORPHAN_SAMPLE_LIMIT,
        description="Maximum number of safe orphan samples to return",
    ),
    _: str = Depends(require_user),
) -> dict[str, Any]:
    """Return a bounded, safe orphan-node report for a board."""

    try:
        return _report_payload(
            board_id=board_id,
            generation_id=generation_id,
            limit=limit,
        )
    except Exception as exc:
        raise _graph_unavailable_error(board_id, exc) from exc


@router.post("/kg/orphan-integrity/backfill")
async def post_kg_orphan_integrity_backfill(
    body: OrphanBackfillRequest,
    request: Request,
    _: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Run explicit orphan backfill. Defaults to dry-run for safe review."""

    await _ensure_backfill_allowed(
        body.board_id,
        db,
        scheduler_control=scheduler_control_from_request(request),
    )
    limit = max(0, min(int(body.limit), MAX_ORPHAN_SAMPLE_LIMIT))
    try:
        result = OrphanBackfillReconciler().run(
            board_id=body.board_id,
            generation_id=body.generation_id,
            dry_run=body.dry_run,
            node_ids=body.node_ids,
            limit=limit,
        )
    except Exception as exc:
        raise _graph_unavailable_error(body.board_id, exc) from exc

    return {
        "board_id": body.board_id,
        "generation_id": body.generation_id,
        "dry_run": body.dry_run,
        "backfill_summary": result.to_safe_dict(),
        "correlation_id": result.correlation_id,
    }
