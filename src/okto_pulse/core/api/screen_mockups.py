"""Screen mockup REST endpoints with the MockupDesignSystemGate (spec 3a006f65 /
card 0192f58d, contract api_a5e13bc5).

Create/update a ScreenMockup on mockup-bearing source entities
(spec/ideation/refinement/story), carrying the Design System consumption metadata.
Card mockups are read-only governed snapshots and are refreshed through the
spec-to-card copy path.
(design_system_ref/version/evidence). The MockupDesignSystemGate runs BEFORE
persistence: blocking rejects an invalid/missing ref with an actionable structured
error; advisory persists + returns a design_system_gate warning + a queryable audit
row; off / no effective Design System does not block. HTML is sanitized exactly as the
MCP path. This is the REST twin of okto_pulse_add/update_screen_mockup — it shares the
same MockupDesignSystemGate, so REST and MCP never diverge.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, ValidationError

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.schemas import (
    CardUpdate,
    IdeationUpdate,
    RefinementUpdate,
    SpecUpdate,
    StoryUpdate,
)
from okto_pulse.core.services import (
    CARD_RESOURCE_READ_ONLY_MESSAGE,
    CardService,
    IdeationService,
    RefinementService,
    SpecService,
    StoryService,
)
from okto_pulse.core.services.design_system import (
    MockupDesignSystemGate,
    normalize_design_system_ref,
)

router = APIRouter()

_ENTITY_TYPES = ("spec", "ideation", "refinement", "card", "story")
_DISPATCH = {
    "spec": (SpecService, "get_spec", "update_spec", SpecUpdate),
    "ideation": (IdeationService, "get_ideation", "update_ideation", IdeationUpdate),
    "refinement": (RefinementService, "get_refinement", "update_refinement", RefinementUpdate),
    "card": (CardService, "get_card", "update_card", CardUpdate),
    "story": (StoryService, "get_story", "update_story", StoryUpdate),
}


def _sanitize_html(html: str) -> str:
    sanitized = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    sanitized = re.sub(r"\s+on\w+\s*=\s*[\"'][^\"']*[\"']", "", sanitized, flags=re.IGNORECASE)
    sanitized = re.sub(r"\s+on\w+\s*=\s*\S+", "", sanitized, flags=re.IGNORECASE)
    return sanitized


class CreateScreenMockupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str
    description: str | None = None
    screen_type: str = "page"
    html_content: str = ""
    design_system_ref: str | None = None
    design_system_version: int | None = None
    design_system_evidence: Any | None = None


class UpdateScreenMockupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str | None = None
    description: str | None = None
    screen_type: str | None = None
    html_content: str | None = None
    design_system_ref: str | None = None
    design_system_version: int | None = None
    design_system_evidence: Any | None = None


def _invalid_request(exc: ValidationError) -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={
            "error": "invalid_request",
            "code": "invalid_request",
            "message": "Request body has unsupported or invalid fields.",
            "details": exc.errors(include_url=False),
        },
    )


def _validate_entity_type(entity_type: str) -> None:
    if entity_type not in _ENTITY_TYPES:
        raise HTTPException(
            status_code=422,
            detail={"error": "invalid_entity_type", "code": "invalid_entity_type",
                    "message": f"entity_type must be one of: {', '.join(_ENTITY_TYPES)}"},
        )


def _reject_card_resource_write(entity_type: str) -> None:
    if entity_type == "card":
        raise HTTPException(status_code=409, detail=CARD_RESOURCE_READ_ONLY_MESSAGE)


async def _load(db, entity_type: str, entity_id: str):
    ServiceCls, get_name, update_name, UpdateClass = _DISPATCH[entity_type]
    service = ServiceCls(db)
    entity = await getattr(service, get_name)(entity_id)
    return entity, service, update_name, UpdateClass


@router.post("/{entity_type}/{entity_id}/screen-mockups", status_code=201)
async def create_screen_mockup(
    entity_type: str,
    entity_id: str,
    raw: dict[str, Any] = Body(default_factory=dict),
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    _validate_entity_type(entity_type)
    _reject_card_resource_write(entity_type)
    try:
        req = CreateScreenMockupRequest.model_validate(raw)
    except ValidationError as exc:
        raise _invalid_request(exc)

    entity, service, update_name, UpdateClass = await _load(db, entity_type, entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail={"error": "not_found",
                            "message": f"{entity_type} '{entity_id}' not found"})

    screen = {
        "id": "sm_" + hashlib.md5(f"{entity_id}{req.title}{uuid.uuid4()}".encode()).hexdigest()[:8],
        "title": req.title,
        "description": req.description,
        "screen_type": req.screen_type,
        "html_content": _sanitize_html(req.html_content),
        "annotations": [],
        "order": len(entity.screen_mockups or []),
        "design_system_ref": normalize_design_system_ref(req.design_system_ref, req.design_system_version),
        "design_system_evidence": req.design_system_evidence,
    }

    # Gate BEFORE persistence (blocking -> DesignSystemError -> 422 via app handler).
    outcome = await MockupDesignSystemGate(db).evaluate_screen(
        entity.board_id, screen, entity_type=entity_type, entity_id=entity_id
    )
    screens = list(entity.screen_mockups or []) + [screen]
    await getattr(service, update_name)(entity_id, actor, UpdateClass(screen_mockups=screens))
    await db.commit()
    return {"success": True, "screen": screen, "design_system_gate": outcome}


@router.patch("/{entity_type}/{entity_id}/screen-mockups/{screen_id}")
async def update_screen_mockup(
    entity_type: str,
    entity_id: str,
    screen_id: str,
    raw: dict[str, Any] = Body(default_factory=dict),
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    _validate_entity_type(entity_type)
    _reject_card_resource_write(entity_type)
    try:
        req = UpdateScreenMockupRequest.model_validate(raw)
    except ValidationError as exc:
        raise _invalid_request(exc)

    entity, service, update_name, UpdateClass = await _load(db, entity_type, entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail={"error": "not_found",
                            "message": f"{entity_type} '{entity_id}' not found"})

    screens = [dict(s) for s in (entity.screen_mockups or [])]
    screen = next((s for s in screens if s.get("id") == screen_id), None)
    if not screen:
        raise HTTPException(status_code=404, detail={"error": "not_found",
                            "message": f"Screen '{screen_id}' not found"})

    original = dict(screen)
    if req.title is not None:
        screen["title"] = req.title
    if req.description is not None:
        screen["description"] = req.description
    if req.screen_type is not None:
        screen["screen_type"] = req.screen_type
    if req.html_content is not None:
        screen["html_content"] = _sanitize_html(req.html_content)
    if req.design_system_ref is not None:
        screen["design_system_ref"] = normalize_design_system_ref(
            req.design_system_ref, req.design_system_version
        )
    if req.design_system_evidence is not None:
        screen["design_system_evidence"] = req.design_system_evidence

    # delta-only: re-gate this mockup only when a gate-relevant field changed.
    outcomes = await MockupDesignSystemGate(db).gate_delta(
        entity.board_id, [original], [screen], entity_type=entity_type, entity_id=entity_id
    )
    await getattr(service, update_name)(entity_id, actor, UpdateClass(screen_mockups=screens))
    await db.commit()
    return {
        "success": True,
        "screen": screen,
        "design_system_gate": outcomes[0] if outcomes else {"outcome": "not_applicable"},
    }
