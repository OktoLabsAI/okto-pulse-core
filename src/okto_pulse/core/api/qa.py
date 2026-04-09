"""Q&A API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models import QACreate, QAAnswer, QAResponse
from okto_pulse.core.services import BoardService, QAService

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


@router.post("/card/{card_id}", response_model=QAResponse, status_code=status.HTTP_201_CREATED)
async def create_question(
    card_id: str,
    data: QACreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new question on a card."""
    service = QAService(db)
    qa = await service.create_question(card_id, user_id, data)
    if not qa:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")
    await _log(db, card_id, "question_added", user_id, {"question": data.question[:100]})
    await db.commit()
    await db.refresh(qa, attribute_names=["id", "card_id", "question", "answer", "asked_by", "answered_by", "created_at", "answered_at"])
    return qa


@router.post("/{qa_id}/answer", response_model=QAResponse)
async def answer_question(
    qa_id: str,
    data: QAAnswer,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Answer a question."""
    service = QAService(db)
    qa = await service.answer_question(qa_id, user_id, data)
    if not qa:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Q&A item not found")
    await _log(db, qa.card_id, "question_answered", user_id, {"qa_id": qa_id, "answer": data.answer[:100]})
    await db.commit()
    await db.refresh(qa, attribute_names=["id", "card_id", "question", "answer", "asked_by", "answered_by", "created_at", "answered_at"])
    return qa


@router.delete("/{qa_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_question(
    qa_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a Q&A item."""
    from okto_pulse.core.models.db import QAItem
    qa = await db.get(QAItem, qa_id)
    card_id = qa.card_id if qa else None

    service = QAService(db)
    deleted = await service.delete_question(qa_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Q&A item not found")
    if card_id:
        await _log(db, card_id, "question_deleted", user_id, {"qa_id": qa_id})
    await db.commit()
