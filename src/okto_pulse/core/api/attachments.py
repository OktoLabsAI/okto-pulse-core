"""Attachment API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models import AttachmentResponse
from okto_pulse.core.services import AttachmentService, BoardService

router = APIRouter()


async def _log(db: AsyncSession, card_id: str, action: str, user_id: str, details: dict | None = None):
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


async def _validate_card_belongs_to_board(db: AsyncSession, board_id: str, card_id: str):
    """Validate that the card belongs to the specified board."""
    from okto_pulse.core.models.db import Card
    card = await db.get(Card, card_id)
    if not card or card.board_id != board_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found in this board")
    return card


@router.post("/{board_id}/{card_id}", response_model=AttachmentResponse, status_code=status.HTTP_201_CREATED)
async def upload_attachment(
    board_id: str,
    card_id: str,
    file: UploadFile = File(...),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Upload a file attachment to a card."""
    await _validate_card_belongs_to_board(db, board_id, card_id)

    service = AttachmentService(db)
    content = await file.read()

    attachment = await service.upload_attachment(
        card_id=card_id,
        user_id=user_id,
        filename=file.filename or "unnamed",
        content=content,
        mime_type=file.content_type or "application/octet-stream",
    )
    if not attachment:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")
    await _log(db, card_id, "attachment_uploaded", user_id, {"filename": file.filename, "size": len(content)})
    await db.commit()
    await db.refresh(attachment, attribute_names=["id", "card_id", "filename", "original_filename", "mime_type", "size", "uploaded_by", "created_at"])
    return attachment


@router.get("/{board_id}/{card_id}/{attachment_id}")
async def download_attachment(
    board_id: str,
    card_id: str,
    attachment_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Download an attachment."""
    await _validate_card_belongs_to_board(db, board_id, card_id)

    service = AttachmentService(db)
    attachment = await service.get_attachment(attachment_id)
    if not attachment or attachment.card_id != card_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attachment not found")
    return FileResponse(
        path=attachment.path,
        filename=attachment.original_filename,
        media_type=attachment.mime_type,
    )


@router.delete("/{board_id}/{card_id}/{attachment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_attachment(
    board_id: str,
    card_id: str,
    attachment_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete an attachment."""
    await _validate_card_belongs_to_board(db, board_id, card_id)

    service = AttachmentService(db)
    attachment = await service.get_attachment(attachment_id)
    if not attachment or attachment.card_id != card_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attachment not found")

    deleted = await service.delete_attachment(attachment_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attachment not found")
    await _log(db, card_id, "attachment_deleted", user_id, {"attachment_id": attachment_id})
    await db.commit()
