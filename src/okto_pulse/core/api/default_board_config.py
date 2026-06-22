"""REST endpoints for administrative DefaultBoardConfiguration (spec 9df814bc /
card 7da43521, FR7).

active template, version history, create/activate/deactivate, and the board
default-config diff. The MCP twin tools share the same orchestrator
(``DefaultBoardConfigApiService``) so REST and MCP never diverge. Request models
forbid extra fields and named bypass intents are rejected with a structured error.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, ValidationError

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.services.amendment_revision_api import AmendmentRevisionApiError
from okto_pulse.core.services.default_board_config_api import (
    DefaultBoardConfigApiService,
    reject_bypass_fields,
)
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationError,
)

router = APIRouter()


class DefaultBoardConfigVersionCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    settings_payload: dict[str, Any] | None = None
    scope: str = "global"
    guideline_default_refs: list[Any] | None = None
    design_system_default_ref: dict[str, Any] | None = None
    activate: bool = False


class UpdateDefaultGuidelineRefsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    guideline_default_refs: list[Any] | None = None


class SetTemplateDesignSystemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    design_system_id: str
    version: int | None = None
    snapshot: dict[str, Any] | None = None
    gate_mode: str = "off"


def _err(exc: DefaultBoardConfigurationError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.to_dict())


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


@router.get("/default-board-config/active")
async def get_active_default_board_config(
    scope: str = "global",
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    try:
        return await DefaultBoardConfigApiService(db).get_active(scope=scope)
    except DefaultBoardConfigurationError as exc:
        raise _err(exc)


@router.get("/default-board-config/versions")
async def list_default_board_config_versions(
    scope: str = "global",
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    try:
        return await DefaultBoardConfigApiService(db).list_versions(scope=scope)
    except DefaultBoardConfigurationError as exc:
        raise _err(exc)


@router.post("/default-board-config/versions")
async def create_default_board_config_version(
    raw: dict[str, Any] = Body(default_factory=dict),
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    try:
        reject_bypass_fields(raw)
        req = DefaultBoardConfigVersionCreateRequest.model_validate(raw)
    except AmendmentRevisionApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.to_dict())
    except ValidationError as exc:
        raise _invalid_request(exc)
    try:
        result = await DefaultBoardConfigApiService(db).create_version(
            actor=actor, **req.model_dump()
        )
        await db.commit()
        return result
    except DefaultBoardConfigurationError as exc:
        raise _err(exc)


@router.post("/default-board-config/versions/{template_id}/activate")
async def activate_default_board_config_version(
    template_id: str,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    try:
        result = await DefaultBoardConfigApiService(db).activate_version(
            template_id=template_id, actor=actor
        )
        await db.commit()
        return result
    except DefaultBoardConfigurationError as exc:
        raise _err(exc)


@router.post("/default-board-config/versions/{template_id}/deactivate")
async def deactivate_default_board_config_version(
    template_id: str,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    try:
        result = await DefaultBoardConfigApiService(db).deactivate_version(
            template_id=template_id, actor=actor
        )
        await db.commit()
        return result
    except DefaultBoardConfigurationError as exc:
        raise _err(exc)


@router.get("/boards/{board_id}/default-config-diff")
async def get_board_default_config_diff(
    board_id: str,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    try:
        return await DefaultBoardConfigApiService(db).get_board_diff(board_id=board_id)
    except DefaultBoardConfigurationError as exc:
        raise _err(exc)


# -- guideline defaults (spec 8a2fad91 / card 5cb88511) ----------------------


@router.get("/guidelines/default-candidates")
async def list_default_guideline_candidates(
    scope: str = "global",
    template_id: str | None = None,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    """Global catalog guidelines with derived eligibility + current default status
    from the umbrella template (api_019810c9)."""
    try:
        return await DefaultBoardConfigApiService(db).list_default_candidates(
            scope=scope, template_id=template_id
        )
    except DefaultBoardConfigurationError as exc:
        raise _err(exc)


@router.post("/default-board-configurations/{template_id}/guidelines")
async def update_default_guideline_refs(
    template_id: str,
    raw: dict[str, Any] = Body(default_factory=dict),
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    """Update guideline_default_refs for a template using only global catalog
    guidelines (api_0845ff2a). Active template => copy-on-write new version."""
    try:
        reject_bypass_fields(raw)
        req = UpdateDefaultGuidelineRefsRequest.model_validate(raw)
    except AmendmentRevisionApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.to_dict())
    except ValidationError as exc:
        raise _invalid_request(exc)
    try:
        result = await DefaultBoardConfigApiService(db).update_template_guidelines(
            template_id=template_id,
            guideline_default_refs=req.guideline_default_refs,
            actor=actor,
        )
        await db.commit()
        return result
    except DefaultBoardConfigurationError as exc:
        raise _err(exc)


@router.post("/default-board-configurations/{template_id}/design-system")
async def set_default_design_system(
    template_id: str,
    raw: dict[str, Any] = Body(default_factory=dict),
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    """Set the Design System default reference + canonical gate mode on a template
    (api_3ed0aee6). Active template => copy-on-write new version. The design_system_id
    must be a real global active DesignSystem (inline/synthetic rejected)."""
    try:
        reject_bypass_fields(raw)
        req = SetTemplateDesignSystemRequest.model_validate(raw)
    except AmendmentRevisionApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.to_dict())
    except ValidationError as exc:
        raise _invalid_request(exc)
    try:
        result = await DefaultBoardConfigApiService(db).set_template_design_system(
            template_id=template_id, actor=actor, **req.model_dump()
        )
        await db.commit()
        return result
    except DefaultBoardConfigurationError as exc:
        raise _err(exc)
