"""Board API endpoints."""

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user, get_realm_id
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models import (
    BoardCreate,
    BoardResponse,
    BoardShareCreate,
    BoardShareResponse,
    BoardShareUpdate,
    BoardSummary,
    BoardUpdate,
    CardCreate,
    CardResponse,
)
from okto_pulse.core.models.db import CardStatus
from okto_pulse.core.services import AgentService, BoardService, CardService, ShareService

router = APIRouter()


@router.post("", response_model=BoardResponse, status_code=status.HTTP_201_CREATED)
async def create_board(
    data: BoardCreate,
    user_id: str = Depends(require_user),
    realm_id: str | None = Depends(get_realm_id),
    db: AsyncSession = Depends(get_db),
):
    """Create a new board."""
    service = BoardService(db)
    board = await service.create_board(user_id, data, realm_id=realm_id)
    await db.commit()
    # Re-fetch with relationships loaded
    board = await service.get_board(board.id)
    board.__dict__["agents"] = []
    return board


@router.get("", response_model=list[BoardSummary])
async def list_boards(
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    view: Literal["my", "shared", "all"] = Query("my"),
    user_id: str = Depends(require_user),
    realm_id: str | None = Depends(get_realm_id),
    db: AsyncSession = Depends(get_db),
):
    """List boards for the current user. view: my|shared|all."""
    service = BoardService(db)
    boards, _ = await service.list_boards(user_id, offset, limit, realm_id=realm_id, view=view)
    return boards


@router.get("/{board_id}", response_model=BoardResponse)
async def get_board(
    board_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a board by ID with all cards and agents."""
    service = BoardService(db)
    board = await service.get_board(board_id, user_id)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")
    # Populate agents via junction table
    agent_service = AgentService(db)
    board.__dict__["agents"] = await agent_service.list_agents_for_board(board_id)
    return board


@router.patch("/{board_id}", response_model=BoardResponse)
async def update_board(
    board_id: str,
    data: BoardUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a board."""
    service = BoardService(db)
    board = await service.update_board(board_id, user_id, data)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")
    await db.commit()
    # Re-fetch with relationships loaded
    board = await service.get_board(board_id, user_id)
    agent_service = AgentService(db)
    board.__dict__["agents"] = await agent_service.list_agents_for_board(board_id)
    return board


@router.delete("/{board_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_board(
    board_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a board and all its cards."""
    service = BoardService(db)
    deleted = await service.delete_board(board_id, user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")
    await db.commit()


@router.post("/{board_id}/cards", response_model=CardResponse, status_code=status.HTTP_201_CREATED)
async def create_card(
    board_id: str,
    data: CardCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new card in a board."""
    service = CardService(db)
    try:
        card = await service.create_card(board_id, user_id, data)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not card:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Board not found or not owned by user"
        )
    await db.commit()
    # Re-fetch with relationships loaded
    card = await CardService(db).get_card(card.id)
    return card


@router.get("/{board_id}/columns")
async def get_board_columns(
    board_id: str,
    include_archived: bool = Query(False, alias="include_archived"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get board cards grouped by status/column."""

    board_service = BoardService(db)
    board = await board_service.get_board(board_id, user_id)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")

    # Group cards by status (exclude archived unless requested)
    columns = {s.value: [] for s in CardStatus}
    for card in board.cards:
        if not include_archived and getattr(card, "archived", False):
            continue
        columns[card.status.value].append({
            "id": card.id,
            "board_id": card.board_id,
            "spec_id": card.spec_id,
            "title": card.title,
            "description": card.description,
            "status": card.status.value,
            "priority": card.priority.value if card.priority else "none",
            "position": card.position,
            "assignee_id": card.assignee_id,
            "created_by": card.created_by,
            "created_at": card.created_at.isoformat(),
            "updated_at": card.updated_at.isoformat(),
            "due_date": card.due_date.isoformat() if card.due_date else None,
            "labels": card.labels or [],
            "test_scenario_ids": card.test_scenario_ids,
            "conclusions": card.conclusions,
            # Bug card fields
            "card_type": getattr(card, "card_type", "normal") or "normal",
            "origin_task_id": getattr(card, "origin_task_id", None),
            "severity": getattr(card, "severity", None),
            "linked_test_task_ids": getattr(card, "linked_test_task_ids", None),
            "archived": getattr(card, "archived", False),
        })

    return {"board_id": board_id, "columns": columns}


# ==================== ARCHIVE ====================


@router.post("/{board_id}/archive/{entity_type}/{entity_id}")
async def archive_tree(
    board_id: str,
    entity_type: str,
    entity_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Archive an entity and all its descendants in cascade."""
    from okto_pulse.core.services.main import ArchiveService
    service = ArchiveService(db)
    try:
        counts = await service.archive_tree(entity_type, entity_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    await db.commit()
    return {"success": True, "archived_count": counts}


@router.post("/{board_id}/restore/{entity_type}/{entity_id}")
async def restore_tree(
    board_id: str,
    entity_type: str,
    entity_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Restore an archived entity and all its descendants."""
    from okto_pulse.core.services.main import ArchiveService
    service = ArchiveService(db)
    try:
        counts = await service.restore_tree(entity_type, entity_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    await db.commit()
    return {"success": True, "restored_count": counts}


# ==================== SHARES ====================


@router.post(
    "/{board_id}/shares",
    response_model=BoardShareResponse,
    status_code=status.HTTP_201_CREATED,
)
async def share_board(
    board_id: str,
    data: BoardShareCreate,
    user_id: str = Depends(require_user),
    realm_id: str | None = Depends(get_realm_id),
    db: AsyncSession = Depends(get_db),
):
    """Share a board with another user (owner/admin only)."""
    service = ShareService(db)
    share = await service.share_board(board_id, user_id, realm_id or "", data)
    if not share:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to share this board or invalid target user",
        )
    await db.commit()
    return share


@router.get("/{board_id}/shares", response_model=list[BoardShareResponse])
async def list_board_shares(
    board_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all shares for a board."""
    service = ShareService(db)
    # Verify caller has access
    perm = await service.get_user_permission(board_id, user_id)
    if not perm:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")
    return await service.list_shares(board_id)


@router.patch("/{board_id}/shares/{share_id}", response_model=BoardShareResponse)
async def update_board_share(
    board_id: str,
    share_id: str,
    data: BoardShareUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a share's permission (owner/admin only)."""
    service = ShareService(db)
    share = await service.update_share(share_id, user_id, data)
    if not share:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to update this share",
        )
    await db.commit()
    return share


@router.delete("/{board_id}/shares/{share_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_board_share(
    board_id: str,
    share_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Revoke a share (owner/admin can revoke, shared user can leave)."""
    service = ShareService(db)
    revoked = await service.revoke_share(share_id, user_id)
    if not revoked:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to revoke this share",
        )
    await db.commit()
