"""Refinement API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.schemas import (
    RefinementCreate,
    RefinementHistoryResponse,
    RefinementKnowledgeCreate,
    RefinementKnowledgeResponse,
    RefinementKnowledgeSummary,
    RefinementMove,
    RefinementQAAnswer,
    RefinementQACreate,
    RefinementQAResponse,
    RefinementResponse,
    RefinementSnapshotResponse,
    RefinementSnapshotSummary,
    RefinementSummary,
    RefinementUpdate,
    SpecResponse,
)
from okto_pulse.core.services import IdeationService, RefinementKnowledgeService, RefinementQAService, RefinementService, SpecService

router = APIRouter()


@router.post(
    "/ideations/{ideation_id}/refinements",
    response_model=RefinementResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_refinement(
    ideation_id: str,
    data: RefinementCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new refinement for a done ideation."""
    service = RefinementService(db)
    try:
        refinement = await service.create_refinement(ideation_id, user_id, data)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not refinement:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Ideation not found or board not owned by user",
        )
    await db.commit()
    refinement = await service.get_refinement(refinement.id)
    return refinement


@router.get("/ideations/{ideation_id}/refinements", response_model=list[RefinementSummary])
async def list_refinements(
    ideation_id: str,
    status_filter: str | None = Query(None, alias="status"),
    include_archived: bool = Query(False, alias="include_archived"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List refinements for an ideation, optionally filtered by status."""
    ideation_service = IdeationService(db)
    ideation = await ideation_service.get_ideation(ideation_id)
    if not ideation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")

    service = RefinementService(db)
    return await service.list_refinements(ideation_id, status_filter, include_archived=include_archived)


@router.get("/refinements/{refinement_id}", response_model=RefinementResponse)
async def get_refinement(
    refinement_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a refinement by ID with nested data."""
    service = RefinementService(db)
    refinement = await service.get_refinement(refinement_id)
    if not refinement:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Refinement not found")
    return refinement


@router.patch("/refinements/{refinement_id}", response_model=RefinementResponse)
async def update_refinement(
    refinement_id: str,
    data: RefinementUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a refinement. Bumps version when content fields change."""
    service = RefinementService(db)
    refinement = await service.update_refinement(refinement_id, user_id, data)
    if not refinement:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Refinement not found")
    await db.commit()
    refinement = await service.get_refinement(refinement_id)
    return refinement


@router.post("/refinements/{refinement_id}/move", response_model=RefinementResponse)
async def move_refinement(
    refinement_id: str,
    data: RefinementMove,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Change refinement status."""
    service = RefinementService(db)
    try:
        refinement = await service.move_refinement(refinement_id, user_id, data)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not refinement:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Refinement not found")
    await db.commit()
    refinement = await service.get_refinement(refinement_id)
    return refinement


@router.delete("/refinements/{refinement_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_refinement(
    refinement_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a refinement."""
    service = RefinementService(db)
    deleted = await service.delete_refinement(refinement_id, user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Refinement not found")
    await db.commit()


@router.post("/refinements/{refinement_id}/derive-spec", response_model=SpecResponse)
async def derive_spec(
    refinement_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Derive a spec from a refinement."""
    service = RefinementService(db)
    try:
        spec = await service.derive_spec(refinement_id, user_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Refinement not found")
    await db.commit()
    spec_service = SpecService(db)
    spec = await spec_service.get_spec(spec.id)
    return spec


@router.get("/refinements/{refinement_id}/history", response_model=list[RefinementHistoryResponse])
async def list_refinement_history(
    refinement_id: str,
    limit: int = Query(50, ge=1, le=200),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get detailed change history for a refinement."""
    service = RefinementService(db)
    return await service.list_history(refinement_id, limit)


# ==================== REFINEMENT Q&A ====================


@router.get("/refinements/{refinement_id}/qa", response_model=list[RefinementQAResponse])
async def list_refinement_qa(
    refinement_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all Q&A items for a refinement."""
    service = RefinementQAService(db)
    return await service.list_qa(refinement_id)


@router.post("/refinements/{refinement_id}/qa", response_model=RefinementQAResponse, status_code=status.HTTP_201_CREATED)
async def create_refinement_question(
    refinement_id: str,
    data: RefinementQACreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Ask a question on a refinement."""
    service = RefinementQAService(db)
    qa = await service.create_question(refinement_id, user_id, data)
    if not qa:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Refinement not found")
    await db.commit()
    return qa


@router.post("/refinements/{refinement_id}/qa/{qa_id}/answer", response_model=RefinementQAResponse)
async def answer_refinement_question(
    refinement_id: str,
    qa_id: str,
    data: RefinementQAAnswer,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Answer a refinement Q&A question."""
    service = RefinementQAService(db)
    qa = await service.answer_question(qa_id, user_id, data)
    if not qa:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Q&A item not found")
    await db.commit()
    return qa


@router.delete("/refinements/{refinement_id}/qa/{qa_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_refinement_question(
    refinement_id: str,
    qa_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a refinement Q&A item."""
    service = RefinementQAService(db)
    deleted = await service.delete_question(qa_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Q&A item not found")
    await db.commit()


# ==================== REFINEMENT SNAPSHOTS ====================


@router.get("/refinements/{refinement_id}/snapshots", response_model=list[RefinementSnapshotSummary])
async def list_refinement_snapshots(
    refinement_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all version snapshots for a refinement."""
    service = RefinementService(db)
    return await service.list_snapshots(refinement_id)


@router.get("/refinements/{refinement_id}/snapshots/{version}", response_model=RefinementSnapshotResponse)
async def get_refinement_snapshot(
    refinement_id: str,
    version: int,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a specific version snapshot of a refinement."""
    service = RefinementService(db)
    snapshot = await service.get_snapshot(refinement_id, version)
    if not snapshot:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Snapshot v{version} not found")
    return snapshot


# ==================== REFINEMENT KNOWLEDGE BASE ====================


@router.get("/refinements/{refinement_id}/knowledge", response_model=list[RefinementKnowledgeSummary])
async def list_refinement_knowledge(
    refinement_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all knowledge base items for a refinement."""
    service = RefinementKnowledgeService(db)
    return await service.list_knowledge(refinement_id)


@router.get("/refinements/{refinement_id}/knowledge/{knowledge_id}", response_model=RefinementKnowledgeResponse)
async def get_refinement_knowledge(
    refinement_id: str,
    knowledge_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a knowledge base item with full content."""
    service = RefinementKnowledgeService(db)
    kb = await service.get_knowledge(knowledge_id)
    if not kb or kb.refinement_id != refinement_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge base item not found")
    return kb


@router.post(
    "/refinements/{refinement_id}/knowledge",
    response_model=RefinementKnowledgeResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_refinement_knowledge(
    refinement_id: str,
    data: RefinementKnowledgeCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a knowledge base item on a refinement."""
    service = RefinementKnowledgeService(db)
    kb = await service.create_knowledge(refinement_id, user_id, data)
    if not kb:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Refinement not found")
    await db.commit()
    return kb


@router.delete("/refinements/{refinement_id}/knowledge/{knowledge_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_refinement_knowledge(
    refinement_id: str,
    knowledge_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a knowledge base item from a refinement."""
    service = RefinementKnowledgeService(db)
    deleted = await service.delete_knowledge(knowledge_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Knowledge base item not found")
    await db.commit()
