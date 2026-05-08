"""Resource Gate API endpoints."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.db import Board
from okto_pulse.core.services.main import BoardService
from okto_pulse.core.services.resource_gate import (
    ResourceGateError,
    ResourceGateJustificationRequired,
    ResourceGateNotFound,
    ResourceGateService,
)

router = APIRouter()

EntityType = Literal["ideation", "refinement", "spec", "card"]
ResourceType = Literal["architecture", "mockup", "knowledge_base"]
SourceChannel = Literal["ui", "api", "mcp"]


class ResourceNotApplicableRequest(BaseModel):
    resource_type: ResourceType
    source_channel: SourceChannel = "api"
    justification: str | None = None


class ClearResourceNotApplicableRequest(BaseModel):
    source_channel: SourceChannel = "api"
    reason: str | None = None


class ResourceGateBoardSettingsUpdate(BaseModel):
    require_spec_resource_task_coverage: bool


class ResourceGateBoardSettingsResponse(BaseModel):
    board_id: str
    settings: dict[str, Any]


async def _get_board_or_404(
    board_id: str,
    user_id: str,
    db: AsyncSession,
) -> Board:
    board = await BoardService(db).get_board(board_id, user_id)
    if not board:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Board not found",
        )
    return board


def _resource_gate_exception(exc: ResourceGateError) -> HTTPException:
    status_code = status.HTTP_400_BAD_REQUEST
    if isinstance(exc, ResourceGateNotFound):
        status_code = status.HTTP_404_NOT_FOUND
    elif isinstance(exc, ResourceGateJustificationRequired):
        status_code = status.HTTP_400_BAD_REQUEST
    return HTTPException(
        status_code=status_code,
        detail={
            "code": exc.code,
            "message": str(exc),
            "details": exc.details,
        },
    )


@router.get("/resource-gate/specs/{spec_id}/task-coverage")
async def get_spec_resource_task_coverage(
    spec_id: str,
    board_id: str = Query(...),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Validate Resource Gate Level 2 coverage for a spec."""
    board = await _get_board_or_404(board_id, user_id, db)
    service = ResourceGateService(db)
    try:
        return await service.validate_spec_resource_task_coverage(
            board_id,
            spec_id,
            enabled=service.is_spec_resource_task_coverage_required(board),
        )
    except ResourceGateError as exc:
        raise _resource_gate_exception(exc) from exc


@router.get("/resource-gate/{entity_type}/{entity_id}")
async def get_resource_gate_summary(
    entity_type: EntityType,
    entity_id: str,
    board_id: str = Query(...),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Return Provided/N/A/Missing Resource Gate summary for an entity."""
    await _get_board_or_404(board_id, user_id, db)
    try:
        return await ResourceGateService(db).get_summary(board_id, entity_type, entity_id)
    except ResourceGateError as exc:
        raise _resource_gate_exception(exc) from exc


@router.post("/resource-gate/{entity_type}/{entity_id}/not-applicable")
async def mark_resource_not_applicable(
    entity_type: EntityType,
    entity_id: str,
    data: ResourceNotApplicableRequest,
    board_id: str = Query(...),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Mark one mandatory resource type as not applicable."""
    await _get_board_or_404(board_id, user_id, db)
    try:
        result = await ResourceGateService(db).mark_not_applicable(
            board_id,
            entity_type,
            entity_id,
            data.resource_type,
            user_id,
            justification=data.justification,
            source_channel=data.source_channel,
        )
    except ResourceGateError as exc:
        raise _resource_gate_exception(exc) from exc
    await db.commit()
    return result


@router.delete("/resource-gate/{entity_type}/{entity_id}/not-applicable/{resource_type}")
async def clear_resource_not_applicable(
    entity_type: EntityType,
    entity_id: str,
    resource_type: ResourceType,
    data: ClearResourceNotApplicableRequest | None = Body(default=None),
    board_id: str = Query(...),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Clear the active N/A mark for a resource type."""
    await _get_board_or_404(board_id, user_id, db)
    payload = data or ClearResourceNotApplicableRequest()
    try:
        result = await ResourceGateService(db).clear_not_applicable(
            board_id,
            entity_type,
            entity_id,
            resource_type,
            user_id,
            reason=payload.reason,
        )
    except ResourceGateError as exc:
        raise _resource_gate_exception(exc) from exc
    await db.commit()
    return result


@router.patch(
    "/boards/{board_id}/settings/resource-gate",
    response_model=ResourceGateBoardSettingsResponse,
)
async def update_resource_gate_board_settings(
    board_id: str,
    data: ResourceGateBoardSettingsUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update board-level Resource Gate settings."""
    board = await _get_board_or_404(board_id, user_id, db)
    settings = dict(board.settings or {})
    settings["require_spec_resource_task_coverage"] = (
        data.require_spec_resource_task_coverage
    )
    board.settings = settings
    flag_modified(board, "settings")
    await db.commit()
    return {
        "board_id": board_id,
        "settings": settings,
    }
