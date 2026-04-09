"""Guideline API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.schemas import (
    BoardGuidelineLinkRequest,
    GuidelineCreate,
    GuidelineResponse,
    GuidelineUpdate,
)
from okto_pulse.core.services import BoardService, GuidelineService

router = APIRouter()


# ============================================================================
# Global Guidelines CRUD
# ============================================================================


@router.get("/guidelines", response_model=list[GuidelineResponse])
async def list_guidelines(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    tag: str | None = Query(None),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List global guidelines for the current user."""
    service = GuidelineService(db)
    return await service.list_guidelines(user_id, offset=offset, limit=limit, tag=tag)


@router.post("/guidelines", response_model=GuidelineResponse, status_code=status.HTTP_201_CREATED)
async def create_guideline(
    data: GuidelineCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new guideline."""
    service = GuidelineService(db)
    guideline = await service.create_guideline(user_id, data)
    await db.commit()
    return guideline


@router.get("/guidelines/{guideline_id}", response_model=GuidelineResponse)
async def get_guideline(
    guideline_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a guideline by ID."""
    service = GuidelineService(db)
    guideline = await service.get_guideline(guideline_id)
    if not guideline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Guideline not found")
    return guideline


@router.patch("/guidelines/{guideline_id}", response_model=GuidelineResponse)
async def update_guideline(
    guideline_id: str,
    data: GuidelineUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a guideline."""
    service = GuidelineService(db)
    guideline = await service.update_guideline(guideline_id, user_id, data)
    if not guideline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Guideline not found or not owned by user")
    await db.commit()
    return guideline


@router.delete("/guidelines/{guideline_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_guideline(
    guideline_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a guideline."""
    service = GuidelineService(db)
    deleted = await service.delete_guideline(guideline_id, user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Guideline not found or not owned by user")
    await db.commit()


# ============================================================================
# Board Guidelines (linked + inline)
# ============================================================================


@router.get("/boards/{board_id}/guidelines")
async def get_board_guidelines(
    board_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get all guidelines for a board (linked globals + inline), sorted by priority."""
    board_service = BoardService(db)
    board = await board_service.get_board(board_id, user_id)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")

    service = GuidelineService(db)
    return await service.get_board_guidelines(board_id)


@router.post("/boards/{board_id}/guidelines", status_code=status.HTTP_201_CREATED)
async def link_or_create_board_guideline(
    board_id: str,
    data: BoardGuidelineLinkRequest,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Link an existing guideline to a board."""
    board_service = BoardService(db)
    board = await board_service.get_board(board_id, user_id)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")

    service = GuidelineService(db)
    guideline = await service.get_guideline(data.guideline_id)
    if not guideline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Guideline not found")

    link = await service.link_guideline_to_board(board_id, data.guideline_id, data.priority)
    await db.commit()
    return {"id": link.id, "board_id": board_id, "guideline_id": data.guideline_id, "priority": link.priority}


@router.delete("/boards/{board_id}/guidelines/{guideline_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unlink_board_guideline(
    board_id: str,
    guideline_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Unlink a guideline from a board."""
    service = GuidelineService(db)
    unlinked = await service.unlink_guideline_from_board(board_id, guideline_id)
    if not unlinked:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link not found")
    await db.commit()


@router.patch("/boards/{board_id}/guidelines/{guideline_id}")
async def update_board_guideline_priority(
    board_id: str,
    guideline_id: str,
    data: dict,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update the priority of a linked guideline on a board."""
    priority = data.get("priority", 0)
    service = GuidelineService(db)
    updated = await service.update_priority(board_id, guideline_id, priority)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link not found")
    await db.commit()
    return {"board_id": board_id, "guideline_id": guideline_id, "priority": priority}
