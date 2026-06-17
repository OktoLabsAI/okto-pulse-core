"""REST endpoint for the Cognitive Action Center read-model
(S3 / card 38a014f7 / api_8ee8f41b).

GET /api/v1/kg/{board_id}/cognitive-readiness/items — a READ-ONLY projection of
the board's readiness signals (cognitive items + canonical debt + technical DLQ),
reconciled by normalized artifact_id, with each artifact's readiness verdict
OBTAINED from ``CognitiveReadinessService`` (readiness_effect / blocking /
precedence_explanation surfaced verbatim, NEVER recomputed — tr_6bfe98e7 /
tr_0aab9bea / br_ee939fc7). The Action Center owns no store/table/queue/ledger
(fr_7695a7a7 / dec_272906c3).

Errors echo the bounded code verbatim:
  * 400 ``invalid_filter`` — unknown ``signal`` value;
  * 503 ``readiness_source_unavailable`` — a readiness source could not be read.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.kg.cognitive_action_center import (
    CognitiveActionCenterReadModel,
    build_clear_response,
    build_skip_response,
)
from okto_pulse.core.kg.cognitive_readiness import (
    GATE_BLOCKING_TIERS,
    CognitiveReadinessError,
    CognitiveReadinessService,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    default_rebuild_base_dir,
)

router = APIRouter()


def build_default_readiness_service() -> CognitiveReadinessService:
    return CognitiveReadinessService(
        CognitiveConsolidationItemStore(base_dir=default_rebuild_base_dir())
    )


async def _enforcement_active(db, board_id: str) -> bool:
    """Whether the board's done-gate actually enforces readiness (two-key
    rollout). Delegates to the existing helper — never recomputed (S3.1 carry-
    forward). Lazy import avoids a services<->api import cycle."""
    from okto_pulse.core.models.db import Board
    from okto_pulse.core.services.main import _cognitive_readiness_blocking_active

    board = await db.get(Board, board_id)
    return _cognitive_readiness_blocking_active(board)


def _would_block_done(verdict, enforcement_active: bool) -> bool:
    return bool(enforcement_active and verdict.tier in GATE_BLOCKING_TIERS)


class CognitiveSkipRequest(BaseModel):
    """Direct cognitive skip / no_action on an artifact (fr_2007b14a)."""

    source_ref: str = Field(min_length=1)
    reason_code: str = Field(min_length=1)
    justification: str | None = None
    evidence_refs: list[str] | None = None
    revisit_at: str | None = None
    kg_generation_id: str | None = None


class CognitiveClearRequest(BaseModel):
    """Clear / reopen a cognitive skip via the central path."""

    source_ref: str = Field(min_length=1)
    kg_generation_id: str | None = None


@router.get(
    "/kg/{board_id}/cognitive-readiness/items",
    tags=["kg-cognitive-action-center"],
)
async def list_cognitive_readiness_items(
    board_id: str,
    signal: str = Query("all"),
    artifact_id: str | None = Query(None),
    source_ref: str | None = Query(None),
    reason_code: str | None = Query(None),
    status: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    kg_generation_id: str | None = Query(None),
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    read_model = CognitiveActionCenterReadModel(build_default_readiness_service())
    try:
        result = await read_model.list_signals(
            db,
            board_id=board_id,
            signal=signal,
            artifact_id=artifact_id,
            source_ref=source_ref,
            reason_code=reason_code,
            status=status,
            search=search,
            limit=limit,
            offset=offset,
            kg_generation_id=kg_generation_id,
        )
    except CognitiveReadinessError as exc:
        raise HTTPException(status_code=exc.http_status, detail=exc.to_dict()) from exc

    # Enforcement-aware annotation so the UI never says "blocks done" purely from
    # blocking=True (S3.1 carry-forward): would_block_done = enforcement_active
    # AND tier in GATE_BLOCKING_TIERS, delegated to the central helper.
    enforcement_active = await _enforcement_active(db, board_id)
    for item in result["items"]:
        tier = (item.get("precedence_explanation") or {}).get("tier")
        item["would_block_done"] = bool(
            enforcement_active and tier in GATE_BLOCKING_TIERS
        )
    result["summary"]["enforcement_active"] = enforcement_active
    return result


@router.post(
    "/kg/{board_id}/cognitive-readiness/skip",
    tags=["kg-cognitive-action-center"],
)
async def record_cognitive_skip_endpoint(
    board_id: str,
    payload: CognitiveSkipRequest,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    """Thin REST over the CENTRAL skip write-path (same service as the MCP twin).
    400 invalid_reason_code / 400 revisit_at_required / 409
    technical_debt_cannot_be_skipped — a DLQ/open-debt blocker is NEVER masked."""
    service = build_default_readiness_service()
    try:
        item = await service.record_cognitive_skip(
            db,
            board_id=board_id,
            source_ref=payload.source_ref,
            reason_code=payload.reason_code,
            actor=actor,
            justification=payload.justification,
            evidence_refs=payload.evidence_refs,
            revisit_at=payload.revisit_at,
            kg_generation_id=payload.kg_generation_id,
        )
        verdict = await service.evaluate_artifact(
            db, board_id=board_id, source_ref=payload.source_ref,
            kg_generation_id=payload.kg_generation_id,
        )
        enforcement_active = await _enforcement_active(db, board_id)
    except CognitiveReadinessError as exc:
        raise HTTPException(status_code=exc.http_status, detail=exc.to_dict()) from exc
    return build_skip_response(
        item, verdict, actor=actor,
        would_block_done=_would_block_done(verdict, enforcement_active),
    )


@router.post(
    "/kg/{board_id}/cognitive-readiness/clear",
    tags=["kg-cognitive-action-center"],
)
async def clear_cognitive_skip_endpoint(
    board_id: str,
    payload: CognitiveClearRequest,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    """Clear/reopen a cognitive skip via the CENTRAL path (audit preserved).
    404 generation/item_not_found; 409 cognitive_item_not_skipped."""
    service = build_default_readiness_service()
    try:
        item = await service.clear_cognitive_skip(
            db,
            board_id=board_id,
            source_ref=payload.source_ref,
            actor=actor,
            kg_generation_id=payload.kg_generation_id,
        )
        verdict = await service.evaluate_artifact(
            db, board_id=board_id, source_ref=payload.source_ref,
            kg_generation_id=payload.kg_generation_id,
        )
        enforcement_active = await _enforcement_active(db, board_id)
    except CognitiveReadinessError as exc:
        raise HTTPException(status_code=exc.http_status, detail=exc.to_dict()) from exc
    return build_clear_response(
        item, verdict, actor=actor,
        would_block_done=_would_block_done(verdict, enforcement_active),
    )


@router.get(
    "/kg/{board_id}/cognitive-readiness/metrics",
    tags=["kg-cognitive-action-center"],
)
async def get_cognitive_readiness_metrics(
    board_id: str,
    kg_generation_id: str | None = Query(None),
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    """Bounded readiness metrics derived from the read-model (fr_9ae21310).
    Labels are bounded; free-text is never a label (br_3a55b609)."""
    read_model = CognitiveActionCenterReadModel(build_default_readiness_service())
    try:
        return await read_model.metrics(
            db, board_id=board_id, kg_generation_id=kg_generation_id,
        )
    except CognitiveReadinessError as exc:
        raise HTTPException(status_code=exc.http_status, detail=exc.to_dict()) from exc
