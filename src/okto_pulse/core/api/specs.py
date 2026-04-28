"""Spec API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.schemas import (
    SpecCreate,
    SpecKnowledgeCreate,
    SpecKnowledgeResponse,
    SpecKnowledgeSummary,
    SpecKnowledgeUpdate,
    SpecMove,
    SpecResponse,
    SpecSkillCreate,
    SpecSkillResponse,
    SpecSkillUpdate,
    SpecSummary,
    SpecUpdate,
)
from okto_pulse.core.models.schemas import SpecHistoryResponse, SpecQAAnswer, SpecQACreate, SpecQAResponse
from okto_pulse.core.services import BoardService, SpecKnowledgeService, SpecQAService, SpecService, SpecSkillService

router = APIRouter()


@router.post(
    "/boards/{board_id}/specs",
    response_model=SpecResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_spec(
    board_id: str,
    data: SpecCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new spec in a board."""
    service = SpecService(db)
    spec = await service.create_spec(board_id, user_id, data)
    if not spec:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Board not found or not owned by user",
        )
    await db.commit()
    spec = await service.get_spec(spec.id)
    return spec


@router.get("/boards/{board_id}/specs", response_model=list[SpecSummary])
async def list_specs(
    board_id: str,
    status_filter: str | None = Query(None, alias="status"),
    include_archived: bool = Query(False, alias="include_archived"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List specs for a board, optionally filtered by status."""
    board_service = BoardService(db)
    board = await board_service.get_board(board_id, user_id)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")

    service = SpecService(db)
    return await service.list_specs(board_id, status_filter, include_archived=include_archived)


@router.get("/specs/{spec_id}", response_model=SpecResponse)
async def get_spec(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a spec by ID with its derived cards."""
    service = SpecService(db)
    spec = await service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    return spec


@router.patch("/specs/{spec_id}", response_model=SpecResponse)
async def update_spec(
    spec_id: str,
    data: SpecUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a spec. Bumps version when content fields change.
    Rejects orphan `linked_*` references with 422 — see
    `_validate_spec_linked_refs` in services/main.py for the exact rules.
    """
    service = SpecService(db)
    try:
        spec = await service.update_spec(spec_id, user_id, data)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e),
        )
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await db.commit()
    spec = await service.get_spec(spec_id)
    return spec


@router.post("/specs/{spec_id}/move", response_model=SpecResponse)
async def move_spec(
    spec_id: str,
    data: SpecMove,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Change spec status."""
    service = SpecService(db)
    try:
        spec = await service.move_spec(spec_id, user_id, data)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await db.commit()
    spec = await service.get_spec(spec_id)
    return spec


@router.delete("/specs/{spec_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_spec(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a spec. Unlinks derived cards but doesn't delete them."""
    service = SpecService(db)
    deleted = await service.delete_spec(spec_id, user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await db.commit()


@router.get("/specs/{spec_id}/history", response_model=list[SpecHistoryResponse])
async def list_spec_history(
    spec_id: str,
    limit: int = Query(50, ge=1, le=200),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get detailed change history for a spec."""
    service = SpecService(db)
    return await service.list_history(spec_id, limit)


@router.post("/specs/{spec_id}/link-card/{card_id}", status_code=status.HTTP_200_OK)
async def link_card_to_spec(
    spec_id: str,
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Link an existing card to a spec."""
    service = SpecService(db)
    linked = await service.link_card(spec_id, card_id, user_id=user_id)
    if not linked:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Spec or card not found, or they belong to different boards",
        )
    await db.commit()
    return {"success": True, "spec_id": spec_id, "card_id": card_id}


@router.post("/specs/{spec_id}/unlink-card/{card_id}", status_code=status.HTTP_200_OK)
async def unlink_card_from_spec(
    spec_id: str,
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Unlink a card from a spec."""
    service = SpecService(db)
    unlinked = await service.unlink_card(card_id, user_id=user_id)
    if not unlinked:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Card not found or not linked to any spec",
        )
    await db.commit()
    return {"success": True, "spec_id": spec_id, "card_id": card_id}


# ==================== LINK TASK TO SCENARIO ====================


@router.post("/specs/{spec_id}/scenarios/{scenario_id}/link-task/{card_id}", status_code=status.HTTP_200_OK)
async def link_task_to_scenario(
    spec_id: str,
    scenario_id: str,
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Link a task card to a test scenario (bidirectional).
    Validates upfront that both scenario and card exist before mutating
    either side, so a typo in card_id no longer leaves an orphan link.
    """
    from okto_pulse.core.services import CardService

    spec_service = SpecService(db)
    spec = await spec_service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")

    card_service = CardService(db)
    card = await card_service.get_card(card_id)
    if not card:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Card '{card_id}' not found — cannot link a non-existent card.",
        )

    scenarios = list(spec.test_scenarios or [])
    found = False
    for s in scenarios:
        if s.get("id") == scenario_id:
            task_ids = list(s.get("linked_task_ids") or [])
            if card_id not in task_ids:
                task_ids.append(card_id)
            s["linked_task_ids"] = task_ids
            found = True
            break

    if not found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scenario '{scenario_id}' not found in spec.",
        )

    try:
        await spec_service.update_spec(spec_id, user_id, SpecUpdate(test_scenarios=scenarios))
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e),
        )

    existing = list(card.test_scenario_ids or [])
    if scenario_id not in existing:
        existing.append(scenario_id)
    from okto_pulse.core.models.schemas import CardUpdate as CU
    await card_service.update_card(card_id, user_id, CU(test_scenario_ids=existing))

    await db.commit()
    return {"success": True, "spec_id": spec_id, "scenario_id": scenario_id, "card_id": card_id}


@router.post("/specs/{spec_id}/scenarios/{scenario_id}/unlink-task/{card_id}", status_code=status.HTTP_200_OK)
async def unlink_task_from_scenario(
    spec_id: str,
    scenario_id: str,
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Unlink a task card from a test scenario (bidirectional)."""
    from okto_pulse.core.services import CardService

    spec_service = SpecService(db)
    spec = await spec_service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")

    scenarios = list(spec.test_scenarios or [])
    found = False
    for s in scenarios:
        if s.get("id") == scenario_id:
            task_ids = list(s.get("linked_task_ids") or [])
            if card_id in task_ids:
                task_ids.remove(card_id)
            s["linked_task_ids"] = task_ids
            found = True
            break

    if not found:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scenario not found")

    await spec_service.update_spec(spec_id, user_id, SpecUpdate(test_scenarios=scenarios))

    card_service = CardService(db)
    card = await card_service.get_card(card_id)
    if card:
        existing = list(card.test_scenario_ids or [])
        if scenario_id in existing:
            existing.remove(scenario_id)
        from okto_pulse.core.models.schemas import CardUpdate as CU
        await card_service.update_card(card_id, user_id, CU(test_scenario_ids=existing))

    await db.commit()
    return {"success": True, "spec_id": spec_id, "scenario_id": scenario_id, "card_id": card_id}


# ==================== SPEC SKILLS ====================


@router.get("/specs/{spec_id}/skills", response_model=list[SpecSkillResponse])
async def list_spec_skills(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all skills for a spec."""
    service = SpecSkillService(db)
    return await service.list_skills(spec_id)


@router.post("/specs/{spec_id}/skills", response_model=SpecSkillResponse, status_code=status.HTTP_201_CREATED)
async def create_spec_skill(
    spec_id: str,
    data: SpecSkillCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a skill on a spec."""
    service = SpecSkillService(db)
    skill = await service.create_skill(spec_id, user_id, data)
    if not skill:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found or duplicate skill_id")
    await db.commit()
    return skill


@router.patch("/specs/{spec_id}/skills/{skill_id}", response_model=SpecSkillResponse)
async def update_spec_skill(
    spec_id: str,
    skill_id: str,
    data: SpecSkillUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a skill."""
    service = SpecSkillService(db)
    skill = await service.update_skill(spec_id, skill_id, data)
    if not skill:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Skill not found")
    await db.commit()
    return skill


@router.delete("/specs/{spec_id}/skills/{skill_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_spec_skill(
    spec_id: str,
    skill_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a skill."""
    service = SpecSkillService(db)
    deleted = await service.delete_skill(spec_id, skill_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Skill not found")
    await db.commit()


# ==================== SPEC KNOWLEDGE BASE ====================


@router.get("/specs/{spec_id}/knowledge", response_model=list[SpecKnowledgeSummary])
async def list_spec_knowledge(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all knowledge base items for a spec (without content)."""
    service = SpecKnowledgeService(db)
    return await service.list_knowledge(spec_id)


@router.get("/specs/{spec_id}/knowledge/{knowledge_id}", response_model=SpecKnowledgeResponse)
async def get_spec_knowledge(
    spec_id: str,
    knowledge_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a knowledge base item with full content."""
    service = SpecKnowledgeService(db)
    kb = await service.get_knowledge(knowledge_id)
    if not kb or kb.spec_id != spec_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge base item not found")
    return kb


@router.post("/specs/{spec_id}/knowledge", response_model=SpecKnowledgeResponse, status_code=status.HTTP_201_CREATED)
async def create_spec_knowledge(
    spec_id: str,
    data: SpecKnowledgeCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Add a knowledge base item to a spec."""
    service = SpecKnowledgeService(db)
    kb = await service.create_knowledge(spec_id, user_id, data)
    if not kb:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await db.commit()
    return kb


@router.delete("/specs/{spec_id}/knowledge/{knowledge_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_spec_knowledge(
    spec_id: str,
    knowledge_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a knowledge base item."""
    service = SpecKnowledgeService(db)
    kb = await service.get_knowledge(knowledge_id)
    if not kb or kb.spec_id != spec_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge base item not found")
    await service.delete_knowledge(knowledge_id)
    await db.commit()


# ==================== SPEC Q&A ====================


@router.get("/specs/{spec_id}/qa", response_model=list[SpecQAResponse])
async def list_spec_qa(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all Q&A items for a spec."""
    service = SpecQAService(db)
    return await service.list_qa(spec_id)


@router.post("/specs/{spec_id}/qa", response_model=SpecQAResponse, status_code=status.HTTP_201_CREATED)
async def create_spec_question(
    spec_id: str,
    data: SpecQACreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Ask a question on a spec."""
    service = SpecQAService(db)
    qa = await service.create_question(spec_id, user_id, data)
    if not qa:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    await db.commit()
    return qa


@router.post("/specs/{spec_id}/qa/{qa_id}/answer", response_model=SpecQAResponse)
async def answer_spec_question(
    spec_id: str,
    qa_id: str,
    data: SpecQAAnswer,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Answer a spec Q&A question."""
    service = SpecQAService(db)
    qa = await service.answer_question(qa_id, user_id, data)
    if not qa:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Q&A item not found")
    await db.commit()
    return qa


@router.delete("/specs/{spec_id}/qa/{qa_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_spec_question(
    spec_id: str,
    qa_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a spec Q&A item."""
    service = SpecQAService(db)
    deleted = await service.delete_question(qa_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Q&A item not found")
    await db.commit()


# ---- Spec Validation Gate Endpoints ----


@router.post("/specs/{spec_id}/validation", status_code=status.HTTP_201_CREATED)
async def submit_spec_validation(
    spec_id: str,
    data: dict,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Submit a Spec Validation Gate record for a spec in 'approved' status.

    Runs deterministic coverage gates as pre-requisite. If they pass, computes
    outcome from thresholds + recommendation: failed if any threshold violated
    or recommendation=reject; success only if all thresholds OK and approve.
    On success, atomically promotes spec.status to validated.
    """
    # Validate required fields (mirror SpecValidationSubmit schema)
    required = [
        "completeness", "completeness_justification",
        "assertiveness", "assertiveness_justification",
        "ambiguity", "ambiguity_justification",
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
    # Min length checks (schema-level guarantee, but fail fast here too)
    for dim in ("completeness", "assertiveness", "ambiguity"):
        jf = data.get(f"{dim}_justification", "")
        if not isinstance(jf, str) or len(jf.strip()) < 10:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{dim}_justification must be at least 10 characters",
            )
    if len((data.get("general_justification") or "").strip()) < 20:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="general_justification must be at least 20 characters",
        )

    service = SpecService(db)
    spec = await service.get_spec(spec_id)
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")

    # Resolve reviewer name
    try:
        from okto_pulse.core.services.main import resolve_actor_name
        reviewer_name = await resolve_actor_name(db, user_id, spec.board_id)
    except Exception:
        reviewer_name = user_id

    try:
        result = await service.submit_spec_validation(
            spec_id=spec_id,
            reviewer_id=user_id,
            reviewer_name=reviewer_name,
            data=data,
        )
    except ValueError as e:
        # Could be: state guard, opt-in guard, coverage gate failure, or input validation
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

    await db.commit()
    return result


@router.get("/specs/{spec_id}/validations")
async def list_spec_validations(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all Spec Validation Gate records in reverse chronological order.

    Returns current_validation_id and the validations array with an 'active'
    flag on each record indicating if it's the currently-active pointer.
    """
    service = SpecService(db)
    try:
        result = await service.list_spec_validations(spec_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return {"spec_id": spec_id, **result}
