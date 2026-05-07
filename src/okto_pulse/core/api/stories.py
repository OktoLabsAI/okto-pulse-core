"""Stories and Topics API endpoints."""

import json
from collections.abc import Iterable

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.infra.permissions import (
    PermissionSet,
    check_permission,
    map_legacy_permissions,
    resolve_permissions,
)
from okto_pulse.core.models.db import Agent, AgentBoard, PermissionPreset, Topic
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
from okto_pulse.core.services.story_permissions import (
    story_move_permission,
    story_state,
    story_update_permissions,
    topic_update_permissions,
)

router = APIRouter()


class IdeationStoriesLinkRequest(BaseModel):
    """Request body for linking one or more Stories to an Ideation."""

    story_ids: list[str] = Field(..., min_length=1)


async def _ensure_board(db: AsyncSession, board_id: str, user_id: str) -> None:
    board = await BoardService(db).get_board(board_id, user_id)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")


async def _resolve_user_permissions(db: AsyncSession, user_id: str, board_id: str):
    """Resolve the same best-effort permission model exposed by /me/permissions."""
    result = await db.execute(select(Agent).where(Agent.created_by == user_id).limit(1))
    agent = result.scalar_one_or_none()
    if agent is None:
        return None

    agent_flags: dict | None = None
    preset_flags: dict | None = None
    board_overrides: dict | None = None

    if isinstance(agent.permission_flags, dict) and agent.permission_flags:
        agent_flags = agent.permission_flags
    elif isinstance(agent.permissions, list) and agent.permissions:
        agent_flags = map_legacy_permissions(agent.permissions)

    if agent.preset_id:
        preset = await db.get(PermissionPreset, agent.preset_id)
        if preset and preset.flags:
            preset_flags = preset.flags

    grant_result = await db.execute(
        select(AgentBoard).where(
            AgentBoard.agent_id == agent.id,
            AgentBoard.board_id == board_id,
        )
    )
    agent_board = grant_result.scalar_one_or_none()
    if agent_board and isinstance(agent_board.permission_overrides, dict):
        board_overrides = agent_board.permission_overrides

    return resolve_permissions(agent_flags, preset_flags, board_overrides)


def _permission_detail(message: str):
    try:
        return json.loads(message)
    except json.JSONDecodeError:
        return message


def _raise_permission_denied(message: str) -> None:
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=_permission_detail(message),
    )


async def _require_permissions(
    db: AsyncSession,
    user_id: str,
    board_id: str,
    permissions: str | Iterable[str | None] | None,
    *,
    entity: str | None = None,
    entity_status: str | None = None,
) -> None:
    if permissions is None:
        return
    permission_set = await _resolve_user_permissions(db, user_id, board_id)
    permission_names = [permissions] if isinstance(permissions, str) else list(permissions)
    for permission in permission_names:
        if not permission:
            continue
        if isinstance(permission_set, PermissionSet) and entity and entity_status:
            error = permission_set.check_with_state(permission, entity, entity_status)
        else:
            error = check_permission(permission_set, permission)
        if error:
            _raise_permission_denied(error)


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
    await _ensure_board(db, board_id, user_id)
    await _require_permissions(db, user_id, board_id, "topic.entity.create")
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
    await _require_permissions(db, user_id, board_id, "topic.entity.read")
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
    topic = await db.get(Topic, topic_id)
    if not topic:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found")
    await _ensure_board(db, topic.board_id, user_id)
    update_data = data.model_dump(exclude_unset=True)
    await _require_permissions(
        db,
        user_id,
        topic.board_id,
        topic_update_permissions(update_data, current_archived=bool(topic.archived)),
    )
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
    await _ensure_board(db, board_id, user_id)
    await _require_permissions(db, user_id, board_id, "story.entity.create")
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
    await _require_permissions(db, user_id, board_id, "story.entity.read")
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
    await _require_permissions(db, user_id, story.board_id, "story.entity.read")
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
    existing = await service.get_story(story_id)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story not found")
    await _ensure_board(db, existing.board_id, user_id)
    update_data = data.model_dump(exclude_unset=True)
    await _require_permissions(
        db,
        user_id,
        existing.board_id,
        story_update_permissions(update_data),
        entity="story",
        entity_status=story_state(existing.status, archived=bool(existing.archived)),
    )
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
    existing = await service.get_story(story_id)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story not found")
    await _ensure_board(db, existing.board_id, user_id)
    await _require_permissions(
        db,
        user_id,
        existing.board_id,
        story_move_permission(existing.status, data.status),
        entity="story",
        entity_status=story_state(existing.status, archived=bool(existing.archived)),
    )
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
    existing = await service.get_story(story_id)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story not found")
    await _ensure_board(db, existing.board_id, user_id)
    await _require_permissions(
        db,
        user_id,
        existing.board_id,
        "story.entity.archive",
        entity="story",
        entity_status=story_state(existing.status, archived=bool(existing.archived)),
    )
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
    existing = await service.get_story(story_id)
    if not existing:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story not found")
    await _ensure_board(db, existing.board_id, user_id)
    await _require_permissions(
        db,
        user_id,
        existing.board_id,
        "story.entity.restore",
        entity="story",
        entity_status=story_state(existing.status, archived=bool(existing.archived)),
    )
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
    story = await service.get_story(story_id)
    if not story:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story or Ideation not found")
    await _ensure_board(db, story.board_id, user_id)
    await _require_permissions(
        db,
        user_id,
        story.board_id,
        "story.links.ideation",
        entity="story",
        entity_status=story_state(story.status, archived=bool(story.archived)),
    )
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
        story = await service.get_story(story_id)
        if not story:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Story or Ideation not found")
        await _ensure_board(db, story.board_id, user_id)
        await _require_permissions(
            db,
            user_id,
            story.board_id,
            "story.links.ideation",
            entity="story",
            entity_status=story_state(story.status, archived=bool(story.archived)),
        )
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
    await _ensure_board(db, board_id, user_id)
    await _require_permissions(db, user_id, board_id, "story.conversion.to_ideation")
    service = StoryService(db)
    for story_id in data.story_ids:
        story = await service.get_story(story_id)
        if story and story.board_id == board_id:
            await _require_permissions(
                db,
                user_id,
                board_id,
                "story.conversion.to_ideation",
                entity="story",
                entity_status=story_state(story.status, archived=bool(story.archived)),
            )
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
