"""Stories and Topics API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.schemas import (
    StoryConversionRequest,
    StoryConversionResponse,
    StoryCreate,
    StoryLinkCreate,
    StoryMove,
    StoryResponse,
    StorySummary,
    StoryUpdate,
    TopicCreate,
    TopicResponse,
    TopicSummary,
    TopicUpdate,
)
from okto_pulse.core.services import BoardService, StoryService

router = APIRouter()


class IdeationStoriesLinkRequest(BaseModel):
    """Request body for linking one or more Stories to an Ideation."""

    story_ids: list[str] = Field(..., min_length=1)


async def _ensure_board(db: AsyncSession, board_id: str, user_id: str) -> None:
    board = await BoardService(db).get_board(board_id, user_id)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")


def _ideation_payload(ideation) -> dict:
    return {
        "id": ideation.id,
        "board_id": ideation.board_id,
        "title": ideation.title,
        "description": ideation.description,
        "problem_statement": ideation.problem_statement,
        "complexity": ideation.complexity.value if ideation.complexity else None,
        "status": ideation.status.value,
        "version": ideation.version,
        "assignee_id": ideation.assignee_id,
        "created_by": ideation.created_by,
        "created_at": ideation.created_at.isoformat() if ideation.created_at else None,
        "updated_at": ideation.updated_at.isoformat() if ideation.updated_at else None,
        "labels": ideation.labels,
        "archived": getattr(ideation, "archived", False),
        "pre_archive_status": getattr(ideation, "pre_archive_status", None),
    }


@router.post("/boards/{board_id}/topics", response_model=TopicResponse, status_code=status.HTTP_201_CREATED)
async def create_topic(
    board_id: str,
    data: TopicCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a board-scoped Story Topic."""
    service = StoryService(db)
    try:
        topic = await service.create_topic(board_id, user_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if not topic:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")
    await db.commit()
    return topic


@router.get("/boards/{board_id}/topics", response_model=list[TopicSummary])
async def list_topics(
    board_id: str,
    include_archived: bool = Query(False, alias="include_archived"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List Story Topics for a board."""
    await _ensure_board(db, board_id, user_id)
    return await StoryService(db).list_topics(board_id, include_archived=include_archived)


@router.patch("/topics/{topic_id}", response_model=TopicResponse)
async def update_topic(
    topic_id: str,
    data: TopicUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a Story Topic."""
    service = StoryService(db)
    try:
        topic = await service.update_topic(topic_id, user_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if not topic:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found")
    await db.commit()
    return topic


@router.post("/boards/{board_id}/stories", response_model=StoryResponse, status_code=status.HTTP_201_CREATED)
async def create_story(
    board_id: str,
    data: StoryCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a lightweight Story before Ideation."""
    service = StoryService(db)
    try:
        story = await service.create_story(board_id, user_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if not story:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")
    await db.commit()
    return await service.get_story(story.id)


@router.get("/boards/{board_id}/stories", response_model=list[StorySummary])
async def list_stories(
    board_id: str,
    status_filter: str | None = Query(None, alias="status"),
    topic_id: str | None = Query(None),
    search: str | None = Query(None),
    linked: bool | None = Query(None),
    converted: bool | None = Query(None),
    include_archived: bool = Query(False, alias="include_archived"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List Stories for a board."""
    await _ensure_board(db, board_id, user_id)
    try:
        return await StoryService(db).list_stories(
            board_id,
            status_filter=status_filter,
            topic_id=topic_id,
            search=search,
            linked=linked,
            converted=converted,
            include_archived=include_archived,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/stories/{story_id}", response_model=StoryResponse)
async def get_story(
    story_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a Story with Topic and Ideation links."""
    story = await StoryService(db).get_story(story_id)
    if not story:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story not found")
    await _ensure_board(db, story.board_id, user_id)
    return story


@router.patch("/stories/{story_id}", response_model=StoryResponse)
async def update_story(
    story_id: str,
    data: StoryUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a Story."""
    service = StoryService(db)
    try:
        story = await service.update_story(story_id, user_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if not story:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story not found")
    await db.commit()
    return await service.get_story(story_id)


@router.post("/stories/{story_id}/move", response_model=StoryResponse)
async def move_story(
    story_id: str,
    data: StoryMove,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Change Story lifecycle status."""
    service = StoryService(db)
    try:
        story = await service.move_story(story_id, user_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if not story:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story not found")
    await db.commit()
    return await service.get_story(story_id)


@router.delete("/stories/{story_id}", response_model=StoryResponse)
async def archive_story(
    story_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Archive a Story without deleting historical lineage."""
    service = StoryService(db)
    story = await service.archive_story(story_id, user_id, archived=True)
    if not story:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story not found")
    await db.commit()
    return await service.get_story(story_id)


@router.post("/stories/{story_id}/restore", response_model=StoryResponse)
async def restore_story(
    story_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Restore an archived Story."""
    service = StoryService(db)
    story = await service.archive_story(story_id, user_id, archived=False)
    if not story:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story not found")
    await db.commit()
    return await service.get_story(story_id)


@router.post("/stories/{story_id}/ideations", response_model=StoryResponse)
async def link_story_to_ideation(
    story_id: str,
    data: StoryLinkCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Link a Story to an existing Ideation."""
    service = StoryService(db)
    link = await service.link_story_to_ideation(story_id, data.ideation_id, user_id)
    if not link:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story or Ideation not found")
    await db.commit()
    return await service.get_story(story_id)


@router.post("/ideations/{ideation_id}/stories")
async def link_stories_to_ideation(
    ideation_id: str,
    data: IdeationStoriesLinkRequest,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Contract-compatible endpoint for linking Stories to an existing Ideation."""
    service = StoryService(db)
    linked_story_ids: list[str] = []
    for story_id in data.story_ids:
        link = await service.link_story_to_ideation(story_id, ideation_id, user_id)
        if not link:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story or Ideation not found")
        linked_story_ids.append(story_id)
    await db.commit()
    return {"success": True, "ideation_id": ideation_id, "story_ids": linked_story_ids}


@router.post("/boards/{board_id}/stories/convert-to-ideation", response_model=StoryConversionResponse)
@router.post("/boards/{board_id}/stories/convert", response_model=StoryConversionResponse)
async def convert_stories(
    board_id: str,
    data: StoryConversionRequest,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create or link an Ideation from selected Stories."""
    service = StoryService(db)
    try:
        result = await service.convert_stories(board_id, user_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")
    ideation, links, propagated = result
    await db.commit()
    return {
        "success": True,
        "ideation": _ideation_payload(ideation),
        "links": links,
        "propagated_mockups": propagated,
    }
