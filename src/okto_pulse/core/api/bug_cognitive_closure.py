"""REST endpoint for bug cognitive-closure evidence evaluation
(S2 / card 13b43f3d / api_8c29ce5d).

POST /api/v1/bugs/{bug_id}/cognitive-closure/evaluate — classifies bug evidence
and returns the readiness verdict OBTAINED from CognitiveReadinessService
(readiness_effect / blocking / precedence_explanation mirrored, NEVER recomputed
— tr_28465cc7). Any resulting skip/no_action goes through the central write-path.
The MCP twin ``okto_pulse_kg_evaluate_bug_cognitive_closure`` shares this exact
core (``bug_cognitive_closure.evaluate_bug_cognitive_closure``) so REST and MCP
never diverge (br_4f1fedd9 / dec_7b75ce29).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.kg.bug_cognitive_closure import evaluate_bug_cognitive_closure
from okto_pulse.core.kg.cognitive_readiness import (
    CognitiveReadinessError,
    CognitiveReadinessService,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    require_rebuild_audit_artifact_store,
)
from okto_pulse.core.models.db import Card

router = APIRouter()


class BugCognitiveClosureEvaluateRequest(BaseModel):
    """api_8c29ce5d request: evidence, requested_action, optional revisit_at."""

    evidence: dict[str, Any] = Field(default_factory=dict)
    requested_action: str = "evaluate"
    reason_code: str | None = None
    justification: str | None = None
    evidence_refs: list[str] | None = None
    revisit_at: str | None = None


def build_default_readiness_service() -> CognitiveReadinessService:
    return CognitiveReadinessService(
        CognitiveConsolidationItemStore(
            artifact_store=require_rebuild_audit_artifact_store()
        )
    )


@router.post(
    "/bugs/{bug_id}/cognitive-closure/evaluate",
    tags=["bug-cognitive-closure"],
)
async def evaluate_bug_cognitive_closure_endpoint(
    bug_id: str,
    payload: BugCognitiveClosureEvaluateRequest,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    card = await db.get(Card, bug_id)
    if card is None or getattr(card, "board_id", None) is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "bug_not_found", "message": f"Bug {bug_id!r} not found."},
        )
    service = build_default_readiness_service()
    try:
        return await evaluate_bug_cognitive_closure(
            service,
            db,
            board_id=card.board_id,
            bug_id=bug_id,
            evidence=payload.evidence,
            requested_action=payload.requested_action,
            reason_code=payload.reason_code,
            actor=actor,
            justification=payload.justification,
            evidence_refs=payload.evidence_refs,
            revisit_at=payload.revisit_at,
        )
    except CognitiveReadinessError as exc:
        raise HTTPException(status_code=exc.http_status, detail=exc.to_dict()) from exc
