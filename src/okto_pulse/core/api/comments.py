"""Comment API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models import CommentCreate, CommentUpdate, CommentResponse
from okto_pulse.core.services import BoardService, CommentService

router = APIRouter()


async def _log(db: AsyncSession, card_id: str, action: str, user_id: str, details: dict | None = None):
    """Log card-level activity."""
    from okto_pulse.core.models.db import Card
    from okto_pulse.core.services.main import resolve_actor_name
    card = await db.get(Card, card_id)
    if card:
        actor_name = await resolve_actor_name(db, user_id, card.board_id)
        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=card.board_id, card_id=card_id,
            action=action, actor_type="user", actor_id=user_id, actor_name=actor_name,
            details=details,
        )


@router.post("/card/{card_id}", response_model=CommentResponse, status_code=status.HTTP_201_CREATED)
async def create_comment(
    card_id: str,
    data: CommentCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new comment on a card."""
    service = CommentService(db)
    comment = await service.create_comment(card_id, user_id, data)
    if not comment:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")
    await _log(db, card_id, "comment_added", user_id, {"content": data.content[:100]})
    await db.commit()
    await db.refresh(comment, attribute_names=["id", "card_id", "content", "author_id", "created_at", "updated_at"])
    return comment


@router.patch("/{comment_id}", response_model=CommentResponse)
async def update_comment(
    comment_id: str,
    data: CommentUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a comment."""
    service = CommentService(db)
    comment = await service.update_comment(comment_id, user_id, data)
    if not comment:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Comment not found or not owned by user")
    await _log(db, comment.card_id, "comment_updated", user_id, {"comment_id": comment_id})
    await db.commit()
    await db.refresh(comment, attribute_names=["id", "card_id", "content", "author_id", "created_at", "updated_at"])
    return comment


@router.post("/{comment_id}/respond", response_model=CommentResponse)
async def respond_to_choice(
    comment_id: str,
    data: dict,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Respond to a choice board comment."""
    from okto_pulse.core.services.main import resolve_actor_name
    from okto_pulse.core.models.db import Comment as CommentModel

    comment_obj = await db.get(CommentModel, comment_id)
    if not comment_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Comment not found")

    actor_name = await resolve_actor_name(db, user_id, comment_obj.card.board_id if comment_obj.card else "")

    service = CommentService(db)
    comment = await service.respond_to_choice(
        comment_id=comment_id,
        responder_id=user_id,
        responder_name=actor_name or user_id,
        selected=data.get("selected", []),
        free_text=data.get("free_text"),
    )
    if not comment:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid choice or comment not found")
    await db.commit()
    await db.refresh(comment)
    return comment


@router.delete("/{comment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_comment(
    comment_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a comment."""
    service = CommentService(db)
    # Get card_id before deletion
    from okto_pulse.core.models.db import Comment as CommentModel
    comment = await db.get(CommentModel, comment_id)
    card_id = comment.card_id if comment else None

    deleted = await service.delete_comment(comment_id, user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Comment not found or not owned by user")
    if card_id:
        await _log(db, card_id, "comment_deleted", user_id, {"comment_id": comment_id})
    await db.commit()
