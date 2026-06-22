"""Design System catalog + board-link REST endpoints (spec 3a006f65 / card 1392f59d).

Catalog CRUD (global + board-inline, versioned) and the singular board<->design-system
effective link, sharing ``DesignSystemService`` with the MCP twins so REST and MCP
never diverge. Request models forbid extra fields and named bypass intents are
rejected with a structured error. Default application + gate mode (card #2) and the
mockup gate (card #3) are NOT here.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, ValidationError

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.services.amendment_revision_api import (
    AmendmentRevisionApiError,
    reject_bypass_fields,
)
from okto_pulse.core.services.design_system import (
    DesignSystemError,
    DesignSystemService,
    serialize_design_system,
)

router = APIRouter()


class CreateDesignSystemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str
    scope: str = "global"
    board_id: str | None = None
    payload: dict[str, Any] | None = None
    status: str = "active"


class UpdateDesignSystemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str | None = None
    payload: dict[str, Any] | None = None
    status: str | None = None


class LinkDesignSystemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    design_system_id: str


def _err(exc: DesignSystemError) -> HTTPException:
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


@router.post("/design-systems")
async def create_design_system(
    raw: dict[str, Any] = Body(default_factory=dict),
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    try:
        reject_bypass_fields(raw)
        req = CreateDesignSystemRequest.model_validate(raw)
    except AmendmentRevisionApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.to_dict())
    except ValidationError as exc:
        raise _invalid_request(exc)
    try:
        ds = await DesignSystemService(db).create_design_system(actor, **req.model_dump())
        await db.commit()
        return serialize_design_system(ds)
    except DesignSystemError as exc:
        raise _err(exc)


@router.get("/design-systems")
async def list_design_systems(
    scope: str = "global",
    board_id: str | None = None,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> list[dict[str, Any]]:
    items = await DesignSystemService(db).list_catalog(scope=scope, board_id=board_id)
    return [serialize_design_system(d) for d in items]


@router.get("/design-systems/{design_system_id}")
async def get_design_system(
    design_system_id: str,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    try:
        ds = await DesignSystemService(db).require_design_system(design_system_id)
        return serialize_design_system(ds)
    except DesignSystemError as exc:
        raise _err(exc)


@router.patch("/design-systems/{design_system_id}")
async def update_design_system(
    design_system_id: str,
    raw: dict[str, Any] = Body(default_factory=dict),
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    try:
        reject_bypass_fields(raw)
        req = UpdateDesignSystemRequest.model_validate(raw)
    except AmendmentRevisionApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.to_dict())
    except ValidationError as exc:
        raise _invalid_request(exc)
    try:
        ds = await DesignSystemService(db).update_design_system(
            design_system_id, actor, **req.model_dump(exclude_unset=True)
        )
        await db.commit()
        return serialize_design_system(ds)
    except DesignSystemError as exc:
        raise _err(exc)


@router.delete("/design-systems/{design_system_id}", status_code=204)
async def delete_design_system(
    design_system_id: str,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> None:
    deleted = await DesignSystemService(db).delete_design_system(design_system_id, actor)
    if not deleted:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "design_system_not_found",
                "code": "design_system_not_found",
                "message": "Design System not found.",
            },
        )
    await db.commit()


@router.post("/boards/{board_id}/design-system")
async def link_board_design_system(
    board_id: str,
    raw: dict[str, Any] = Body(default_factory=dict),
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    try:
        reject_bypass_fields(raw)
        req = LinkDesignSystemRequest.model_validate(raw)
    except AmendmentRevisionApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.to_dict())
    except ValidationError as exc:
        raise _invalid_request(exc)
    try:
        link = await DesignSystemService(db).link_design_system_to_board(
            board_id, req.design_system_id
        )
        await db.commit()
        return {
            "board_id": link.board_id,
            "design_system_id": link.design_system_id,
            "design_system_version": link.design_system_version,
        }
    except DesignSystemError as exc:
        raise _err(exc)


@router.delete("/boards/{board_id}/design-system", status_code=204)
async def unlink_board_design_system(
    board_id: str,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> None:
    unlinked = await DesignSystemService(db).unlink_design_system_from_board(board_id)
    if not unlinked:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "design_system_link_not_found",
                "code": "design_system_link_not_found",
                "message": "No Design System is linked to this board.",
            },
        )
    await db.commit()


@router.get("/boards/{board_id}/design-system")
async def get_board_design_system(
    board_id: str,
    db=Depends(get_db),
    actor: str = Depends(require_user),
) -> dict[str, Any]:
    effective = await DesignSystemService(db).get_board_effective_design_system(board_id)
    return {"board_id": board_id, "effective": effective}
