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

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.kg.cognitive_action_center import (
    CognitiveActionCenterReadModel,
)
from okto_pulse.core.kg.cognitive_readiness import (
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
        return await read_model.list_signals(
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
