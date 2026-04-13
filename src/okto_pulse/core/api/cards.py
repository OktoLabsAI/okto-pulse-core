"""Card API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models import (
    ActivityLogResponse,
    CardCreate,
    CardMove,
    CardResponse,
    CardUpdate,
)
from okto_pulse.core.models.db import ActivityLog, Agent, AgentSeenItem
from okto_pulse.core.services import CardService

router = APIRouter()


@router.get("/{card_id}", response_model=CardResponse)
async def get_card(
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a card by ID with all attachments, Q&A, and comments."""
    service = CardService(db)
    card = await service.get_card(card_id)
    if not card:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")
    return card


@router.patch("/{card_id}", response_model=CardResponse)
async def update_card(
    card_id: str,
    data: CardUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a card."""
    service = CardService(db)
    card = await service.update_card(card_id, user_id, data)
    if not card:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")
    await db.commit()
    # Re-fetch with relationships loaded
    card = await service.get_card(card_id)
    return card


@router.post("/{card_id}/move", response_model=CardResponse)
async def move_card(
    card_id: str,
    data: CardMove,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Move a card to a different column/position."""
    service = CardService(db)
    try:
        card = await service.move_card(card_id, user_id, data)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    if not card:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")
    await db.commit()
    card = await service.get_card(card_id)
    return card


# ---- Dependencies ----

@router.get("/{card_id}/dependencies")
async def get_dependencies(
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get cards this card depends on."""
    service = CardService(db)
    deps = await service.get_dependencies(card_id)
    return [
        {"id": d.id, "title": d.title, "status": d.status.value}
        for d in deps
    ]


@router.get("/{card_id}/dependents")
async def get_dependents(
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get cards that depend on this card."""
    service = CardService(db)
    deps = await service.get_dependents(card_id)
    return [
        {"id": d.id, "title": d.title, "status": d.status.value}
        for d in deps
    ]


@router.post("/{card_id}/dependencies/{depends_on_id}", status_code=status.HTTP_201_CREATED)
async def add_dependency(
    card_id: str,
    depends_on_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Add a dependency: card_id depends on depends_on_id."""
    service = CardService(db)
    dep = await service.add_dependency(card_id, depends_on_id)
    if not dep:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Dependência circular detectada ou auto-referência",
        )
    await db.commit()
    return {"id": dep.id, "card_id": card_id, "depends_on_id": depends_on_id}


@router.delete("/{card_id}/dependencies/{depends_on_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_dependency(
    card_id: str,
    depends_on_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove a dependency."""
    service = CardService(db)
    removed = await service.remove_dependency(card_id, depends_on_id)
    if not removed:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dependency not found")
    await db.commit()


@router.get("/{card_id}/activity", response_model=list[ActivityLogResponse])
async def get_card_activity(
    card_id: str,
    limit: int = Query(50, ge=1, le=200),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get activity log for a specific card."""
    query = (
        select(ActivityLog)
        .where(ActivityLog.card_id == card_id)
        .order_by(ActivityLog.created_at.desc())
        .limit(limit)
    )
    result = await db.execute(query)
    return list(result.scalars().all())


@router.get("/{card_id}/seen")
async def get_card_seen_status(
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get seen status for all items in a card (comments, QA) by agents."""
    from okto_pulse.core.models.db import Comment, QAItem

    # Collect item IDs from this card
    comment_ids_q = select(Comment.id).where(Comment.card_id == card_id)
    qa_ids_q = select(QAItem.id).where(QAItem.card_id == card_id)

    comment_ids = [r[0] for r in (await db.execute(comment_ids_q)).all()]
    qa_ids = [r[0] for r in (await db.execute(qa_ids_q)).all()]
    all_ids = set(comment_ids + qa_ids)

    if not all_ids:
        return {"items": {}}

    # Get seen records for these items
    seen_query = (
        select(AgentSeenItem, Agent.name)
        .join(Agent, Agent.id == AgentSeenItem.agent_id)
        .where(AgentSeenItem.item_id.in_(all_ids))
        .order_by(AgentSeenItem.seen_at)
    )
    seen_results = (await db.execute(seen_query)).all()

    # Group by item_id: {item_id: [{agent_name, seen_at}]}
    items: dict[str, list] = {}
    for seen, agent_name in seen_results:
        if seen.item_id not in items:
            items[seen.item_id] = []
        items[seen.item_id].append({
            "agent_id": seen.agent_id,
            "agent_name": agent_name,
            "seen_at": seen.seen_at.isoformat(),
        })

    return {"items": items}


# ---- Bug card: link test tasks ----

@router.post("/{card_id}/test-tasks", status_code=status.HTTP_201_CREATED)
async def link_test_task_to_bug(
    card_id: str,
    body: dict,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Link a test task to a bug card. Validates same spec and new scenario."""
    from okto_pulse.core.models.db import Card as CardModel, Spec
    from sqlalchemy.orm import selectinload

    test_task_id = body.get("test_task_id")
    if not test_task_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="test_task_id is required")

    service = CardService(db)
    bug_card = await service.get_card(card_id)
    if not bug_card:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")

    if getattr(bug_card, "card_type", "normal") != "bug":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Card is not a bug card")

    test_task = await service.get_card(test_task_id)
    if not test_task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Test task not found")

    # Validate same spec
    if test_task.spec_id != bug_card.spec_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Test task does not belong to the same spec as the bug",
        )

    # Validate test task references a scenario created AFTER the bug
    if bug_card.spec_id and test_task.test_scenario_ids:
        spec = await db.get(Spec, bug_card.spec_id)
        if spec:
            all_scenarios = {s["id"]: s for s in (spec.test_scenarios or [])}
            bug_created = bug_card.created_at.isoformat() if bug_card.created_at else ""
            for sid in test_task.test_scenario_ids:
                sc = all_scenarios.get(sid)
                if sc:
                    sc_created = sc.get("created_at", "")
                    if sc_created and sc_created < bug_created:
                        raise HTTPException(
                            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="Test task references a scenario created before the bug card — only new scenarios are accepted",
                        )

    # Add test task to linked_test_task_ids
    from sqlalchemy.orm.attributes import flag_modified
    linked = list(bug_card.linked_test_task_ids or [])
    if test_task_id not in linked:
        linked.append(test_task_id)
        bug_card.linked_test_task_ids = linked
        flag_modified(bug_card, "linked_test_task_ids")
        await db.commit()

    return {
        "success": True,
        "bug_card_id": card_id,
        "test_task_id": test_task_id,
        "is_unblocked": len(linked) >= 1,
    }


@router.delete("/{card_id}/test-tasks/{test_task_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unlink_test_task_from_bug(
    card_id: str,
    test_task_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Unlink a test task from a bug card."""
    from sqlalchemy.orm.attributes import flag_modified

    service = CardService(db)
    bug_card = await service.get_card(card_id)
    if not bug_card:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")

    linked = list(bug_card.linked_test_task_ids or [])
    if test_task_id in linked:
        linked.remove(test_task_id)
        bug_card.linked_test_task_ids = linked
        flag_modified(bug_card, "linked_test_task_ids")
        await db.commit()


@router.delete("/{card_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_card(
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a card."""
    service = CardService(db)
    deleted = await service.delete_card(card_id, user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")
    await db.commit()


# ---- Task Validation Endpoints ----


@router.post("/{card_id}/validate", status_code=status.HTTP_201_CREATED)
async def submit_task_validation(
    card_id: str,
    data: dict,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Submit a task validation for a card in 'validation' status."""
    # Validate required fields
    required = [
        "confidence", "confidence_justification",
        "estimated_completeness", "completeness_justification",
        "estimated_drift", "drift_justification",
        "general_justification", "recommendation",
    ]
    missing = [f for f in required if f not in data or data[f] is None]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Missing required fields: {', '.join(missing)}",
        )
    if data.get("recommendation") not in ("approve", "reject"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="recommendation must be 'approve' or 'reject'",
        )

    # Resolve reviewer name via CardService helper
    service = CardService(db)
    try:
        from okto_pulse.core.services.main import resolve_actor_name
        # Get card first to know the board
        card = await service.get_card(card_id)
        if not card:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")
        reviewer_name = await resolve_actor_name(db, user_id, card.board_id)
    except HTTPException:
        raise
    except Exception:
        reviewer_name = user_id

    try:
        result = await service.submit_task_validation(
            card_id=card_id,
            reviewer_id=user_id,
            reviewer_name=reviewer_name,
            data=data,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    await db.commit()
    return result


@router.get("/{card_id}/validations")
async def list_task_validations(
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all validations for a card (reverse chronological)."""
    service = CardService(db)
    try:
        validations = await service.list_task_validations(card_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return {"card_id": card_id, "total": len(validations), "validations": validations}


@router.get("/{card_id}/validations/{validation_id}")
async def get_task_validation(
    card_id: str,
    validation_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a single validation by ID."""
    service = CardService(db)
    try:
        validation = await service.get_task_validation(card_id, validation_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    if not validation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Validation not found")
    return validation


@router.delete("/{card_id}/validations/{validation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_task_validation(
    card_id: str,
    validation_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a validation entry."""
    service = CardService(db)
    try:
        deleted = await service.delete_task_validation(card_id, validation_id, user_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Validation not found")
    await db.commit()
