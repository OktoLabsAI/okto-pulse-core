"""Sprint API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.schemas import (
    SprintCreate,
    SprintHistoryResponse,
    SprintMove,
    SprintResponse,
    SprintSummary,
    SprintUpdate,
)
from okto_pulse.core.services.main import SprintService

router = APIRouter()


@router.post(
    "/boards/{board_id}/specs/{spec_id}/sprints",
    response_model=SprintResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_sprint(
    board_id: str,
    spec_id: str,
    data: SprintCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new sprint for a spec."""
    service = SprintService(db)
    try:
        sprint = await service.create_sprint(board_id, user_id, data)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not sprint:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec or board not found")
    await db.commit()
    sprint = await service.get_sprint(sprint.id)
    return sprint


@router.get("/boards/{board_id}/specs/{spec_id}/sprints", response_model=list[SprintSummary])
async def list_sprints(
    board_id: str,
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List sprints for a spec."""
    service = SprintService(db)
    return await service.list_sprints(spec_id)


@router.get("/sprints/{sprint_id}", response_model=SprintResponse)
async def get_sprint(
    sprint_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a sprint by ID with full details."""
    service = SprintService(db)
    sprint = await service.get_sprint(sprint_id)
    if not sprint:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sprint not found")
    return sprint


@router.patch("/sprints/{sprint_id}", response_model=SprintResponse)
async def update_sprint(
    sprint_id: str,
    data: SprintUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a sprint."""
    service = SprintService(db)
    try:
        sprint = await service.update_sprint(sprint_id, user_id, data)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not sprint:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sprint not found")
    await db.commit()
    sprint = await service.get_sprint(sprint.id)
    return sprint


@router.post("/sprints/{sprint_id}/move", response_model=SprintResponse)
async def move_sprint(
    sprint_id: str,
    data: SprintMove,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Move a sprint to a different status."""
    service = SprintService(db)
    try:
        sprint = await service.move_sprint(sprint_id, user_id, data)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not sprint:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sprint not found")
    await db.commit()
    sprint = await service.get_sprint(sprint.id)
    return sprint


@router.delete("/sprints/{sprint_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_sprint(
    sprint_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a sprint."""
    service = SprintService(db)
    deleted = await service.delete_sprint(sprint_id, user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sprint not found")
    await db.commit()


@router.post("/sprints/{sprint_id}/evaluations")
async def submit_evaluation(
    sprint_id: str,
    evaluation: dict,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Submit an evaluation for a sprint."""
    service = SprintService(db)
    try:
        sprint = await service.submit_evaluation(sprint_id, user_id, evaluation)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not sprint:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sprint not found")
    await db.commit()
    return {"success": True, "evaluation_id": sprint.evaluations[-1]["id"] if sprint.evaluations else None}


@router.get("/sprints/{sprint_id}/history", response_model=list[SprintHistoryResponse])
async def list_history(
    sprint_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List sprint history."""
    service = SprintService(db)
    return await service.list_history(sprint_id)


@router.get("/boards/{board_id}/specs/{spec_id}/sprints/suggest")
async def suggest_sprints(
    board_id: str,
    spec_id: str,
    threshold: int = 8,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Suggest sprint breakdown for a spec."""
    service = SprintService(db)
    try:
        suggestions = await service.suggest_sprints(spec_id, threshold)
        return {"suggestions": suggestions, "count": len(suggestions)}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
