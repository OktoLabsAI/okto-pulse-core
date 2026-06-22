"""Ideation API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.schemas import (
    IdeationAmbiguityGateSkipUpdate,
    IdeationCreate,
    IdeationHistoryResponse,
    IdeationKnowledgeCreate,
    IdeationKnowledgeResponse,
    IdeationKnowledgeSummary,
    IdeationMove,
    IdeationQAAnswer,
    IdeationQACreate,
    IdeationQAResponse,
    IdeationResponse,
    IdeationSnapshotResponse,
    IdeationSnapshotSummary,
    IdeationSummary,
    IdeationUpdate,
    SpecResponse,
)
from okto_pulse.core.services import (
    AmbiguityGateError,
    BoardService,
    IdeationKnowledgeService,
    IdeationQAService,
    IdeationService,
    QASelfAnsweringNotAllowedError,
    SpecService,
)

router = APIRouter()


@router.post(
    "/boards/{board_id}/ideations",
    response_model=IdeationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_ideation(
    board_id: str,
    data: IdeationCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new ideation in a board."""
    service = IdeationService(db)
    ideation = await service.create_ideation(board_id, user_id, data)
    if not ideation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Board not found or not owned by user",
        )
    await db.commit()
    ideation = await service.get_ideation(ideation.id)
    return ideation


@router.get("/boards/{board_id}/ideations", response_model=list[IdeationSummary])
async def list_ideations(
    board_id: str,
    status_filter: str | None = Query(None, alias="status"),
    include_archived: bool = Query(False, alias="include_archived"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List ideations for a board, optionally filtered by status."""
    board_service = BoardService(db)
    board = await board_service.get_board(board_id, user_id)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")

    service = IdeationService(db)
    return await service.list_ideations(board_id, status_filter, include_archived=include_archived)


@router.get("/ideations/{ideation_id}", response_model=IdeationResponse)
async def get_ideation(
    ideation_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get an ideation by ID with nested data."""
    service = IdeationService(db)
    ideation = await service.get_ideation(ideation_id)
    if not ideation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")
    return ideation


@router.patch("/ideations/{ideation_id}", response_model=IdeationResponse)
async def update_ideation(
    ideation_id: str,
    data: IdeationUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update an ideation. Bumps version when content fields change."""
    service = IdeationService(db)
    ideation = await service.update_ideation(ideation_id, user_id, data)
    if not ideation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")
    await db.commit()
    ideation = await service.get_ideation(ideation_id)
    return ideation


@router.post("/ideations/{ideation_id}/move", response_model=IdeationResponse)
async def move_ideation(
    ideation_id: str,
    data: IdeationMove,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Change ideation status."""
    service = IdeationService(db)
    try:
        ideation = await service.move_ideation(ideation_id, user_id, data)
    except AmbiguityGateError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    if not ideation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")
    await db.commit()
    ideation = await service.get_ideation(ideation_id)
    return ideation


@router.patch("/ideations/{ideation_id}/ambiguity-gate-skip", response_model=IdeationResponse)
async def set_ideation_ambiguity_gate_skip(
    ideation_id: str,
    data: IdeationAmbiguityGateSkipUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Persist the per-ideation Max ambiguity gate skip override (spec 2485780b).

    Dedicated write path that works while the ideation is in evaluating status
    without opening the generic update_ideation draft-only guard to other
    fields. Rejects archived ideations and emits an auditable activity entry.
    """
    service = IdeationService(db)
    try:
        ideation = await service.set_ambiguity_gate_skip(
            ideation_id, user_id, data.skip_ambiguity_gate, source="rest"
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    if not ideation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")
    await db.commit()
    ideation = await service.get_ideation(ideation_id)
    return ideation


@router.delete("/ideations/{ideation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_ideation(
    ideation_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete an ideation."""
    service = IdeationService(db)
    deleted = await service.delete_ideation(ideation_id, user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")
    await db.commit()


@router.post("/ideations/{ideation_id}/evaluate", response_model=IdeationResponse)
async def evaluate_complexity(
    ideation_id: str,
    request: Request,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Evaluate ideation complexity. Accepts scores + justifications in body."""
    body = await request.json()
    service = IdeationService(db)

    # Build scope_assessment from body
    ideation = await service.get_ideation(ideation_id)
    if not ideation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")

    scope = ideation.scope_assessment or {}
    for dim in ["domains", "ambiguity", "dependencies"]:
        if dim in body:
            scope[dim] = int(body[dim])
        if f"{dim}_justification" in body:
            scope[f"{dim}_justification"] = body[f"{dim}_justification"]

    # Update scope_assessment directly (bypasses draft-only edit guard since evaluation
    # requires writing scores while in 'evaluating' status)
    from sqlalchemy.orm.attributes import flag_modified
    ideation.scope_assessment = scope
    flag_modified(ideation, "scope_assessment")
    ideation = await service.evaluate_complexity(ideation_id, user_id)
    if not ideation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")
    await db.commit()
    ideation = await service.get_ideation(ideation_id)
    return ideation


@router.post("/ideations/{ideation_id}/derive-spec", response_model=SpecResponse)
async def derive_spec(
    ideation_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a spec draft from a done ideation."""
    service = IdeationService(db)
    try:
        spec = await service.derive_spec(ideation_id, user_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")
    await db.commit()
    spec_service = SpecService(db)
    spec = await spec_service.get_spec(spec.id)
    return spec


@router.get("/ideations/{ideation_id}/snapshots", response_model=list[IdeationSnapshotSummary])
async def list_ideation_snapshots(
    ideation_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all version snapshots for an ideation."""
    service = IdeationService(db)
    return await service.list_snapshots(ideation_id)


@router.get("/ideations/{ideation_id}/snapshots/{version}", response_model=IdeationSnapshotResponse)
async def get_ideation_snapshot(
    ideation_id: str,
    version: int,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a specific version snapshot of an ideation."""
    service = IdeationService(db)
    snapshot = await service.get_snapshot(ideation_id, version)
    if not snapshot:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Snapshot v{version} not found")
    return snapshot


@router.get("/ideations/{ideation_id}/history", response_model=list[IdeationHistoryResponse])
async def list_ideation_history(
    ideation_id: str,
    limit: int = Query(50, ge=1, le=200),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get detailed change history for an ideation."""
    service = IdeationService(db)
    return await service.list_history(ideation_id, limit)


# ==================== IDEATION KNOWLEDGE BASE ====================


@router.get("/ideations/{ideation_id}/knowledge", response_model=list[IdeationKnowledgeSummary])
async def list_ideation_knowledge(
    ideation_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all knowledge base items for an ideation."""
    service = IdeationKnowledgeService(db)
    return await service.list_knowledge(ideation_id)


@router.get("/ideations/{ideation_id}/knowledge/{knowledge_id}", response_model=IdeationKnowledgeResponse)
async def get_ideation_knowledge(
    ideation_id: str,
    knowledge_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a knowledge base item with full content."""
    service = IdeationKnowledgeService(db)
    kb = await service.get_knowledge(knowledge_id)
    if not kb or kb.ideation_id != ideation_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge base item not found")
    return kb


@router.post(
    "/ideations/{ideation_id}/knowledge",
    response_model=IdeationKnowledgeResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_ideation_knowledge(
    ideation_id: str,
    data: IdeationKnowledgeCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a knowledge base item on an ideation."""
    service = IdeationKnowledgeService(db)
    kb = await service.create_knowledge(ideation_id, user_id, data)
    if not kb:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")
    await db.commit()
    return kb


@router.delete("/ideations/{ideation_id}/knowledge/{knowledge_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_ideation_knowledge(
    ideation_id: str,
    knowledge_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a knowledge base item from an ideation."""
    service = IdeationKnowledgeService(db)
    kb = await service.get_knowledge(knowledge_id)
    if not kb or kb.ideation_id != ideation_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge base item not found")
    deleted = await service.delete_knowledge(knowledge_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge base item not found")
    await db.commit()


# ==================== IDEATION Q&A ====================


@router.get("/ideations/{ideation_id}/qa", response_model=list[IdeationQAResponse])
async def list_ideation_qa(
    ideation_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all Q&A items for an ideation."""
    service = IdeationQAService(db)
    return await service.list_qa(ideation_id)


@router.post("/ideations/{ideation_id}/qa", response_model=IdeationQAResponse, status_code=status.HTTP_201_CREATED)
async def create_ideation_question(
    ideation_id: str,
    data: IdeationQACreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Ask a question on an ideation."""
    service = IdeationQAService(db)
    qa = await service.create_question(ideation_id, user_id, data)
    if not qa:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")
    await db.commit()
    return qa


@router.post("/ideations/{ideation_id}/qa/{qa_id}/answer", response_model=IdeationQAResponse)
async def answer_ideation_question(
    ideation_id: str,
    qa_id: str,
    data: IdeationQAAnswer,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Answer an ideation Q&A question."""
    service = IdeationQAService(db)
    try:
        qa = await service.answer_question(
            qa_id, user_id, data, actor_type="user", surface="rest"
        )
    except QASelfAnsweringNotAllowedError as exc:
        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"reason": exc.reason, "message": str(exc)},
        ) from exc
    if not qa:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Q&A item not found")
    await db.commit()
    return qa


@router.delete("/ideations/{ideation_id}/qa/{qa_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_ideation_question(
    ideation_id: str,
    qa_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete an ideation Q&A item."""
    service = IdeationQAService(db)
    deleted = await service.delete_question(qa_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Q&A item not found")
    await db.commit()
