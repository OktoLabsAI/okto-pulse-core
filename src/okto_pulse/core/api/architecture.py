"""Architecture Design API endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.db import ArchitectureDesign, Card, Ideation, Refinement, Spec
from okto_pulse.core.models.schemas import (
    ArchitectureDesignCreate,
    ArchitectureDesignResponse,
    ArchitectureDesignSummary,
    ArchitectureDesignUpdate,
    ArchitectureDiagramFormat,
    ArchitectureDiagramPayloadResponse,
    ArchitectureDiagramType,
    ArchitectureDiffResponse,
)
from okto_pulse.core.services.architecture import (
    ArchitectureDesignRepository,
    ArchitectureDiagramStore,
    ArchitecturePayloadValidationError,
    ArchitecturePropagationService,
)

router = APIRouter()

PARENT_MODELS = {
    "ideation": Ideation,
    "refinement": Refinement,
    "spec": Spec,
    "card": Card,
}


class DiagramPayloadUpdate(BaseModel):
    """Payload replacement for an existing diagram."""

    payload: dict[str, Any] | list[Any] | str
    format: ArchitectureDiagramFormat | None = None
    change_summary: str | None = None


class ExcalidrawImportRequest(BaseModel):
    """Import an Excalidraw scene into an architecture design."""

    title: str = Field(..., min_length=1, max_length=255)
    payload: dict[str, Any]
    diagram_type: ArchitectureDiagramType = "other"
    description: str | None = None
    order_index: int = 0
    replace_diagram_id: str | None = None
    change_summary: str | None = None


class CopyArchitectureRequest(BaseModel):
    """Optional filter for copy architecture operations."""

    design_ids: list[str] | None = None


async def _get_parent(db: AsyncSession, parent_type: str, parent_id: str) -> Any | None:
    model = PARENT_MODELS[parent_type]
    return await db.get(model, parent_id)


async def _ensure_parent(db: AsyncSession, parent_type: str, parent_id: str) -> Any:
    parent = await _get_parent(db, parent_type, parent_id)
    if parent is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"{parent_type} not found")
    return parent


async def _ensure_spec_architecture_unlocked(db: AsyncSession, spec_id: str) -> None:
    spec = await db.get(Spec, spec_id)
    if spec is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    current_id = getattr(spec, "current_validation_id", None)
    validations = getattr(spec, "validations", None) or []
    current = next((item for item in validations if item.get("id") == current_id), None)
    if current_id and current and current.get("outcome") == "success":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Spec is locked because validation passed. Move it back to draft or approved to edit architecture.",
        )


async def _ensure_design_mutable(db: AsyncSession, design_id: str) -> Any:
    design = await db.get(ArchitectureDesign, design_id)
    if design is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Architecture design not found")
    if design.parent_type == "spec":
        await _ensure_spec_architecture_unlocked(db, design.spec_id)
    return design


def _http_error_from_value(error: ValueError) -> HTTPException:
    message = str(error)
    status_code = (
        status.HTTP_422_UNPROCESSABLE_CONTENT
        if isinstance(error, ArchitecturePayloadValidationError)
        else status.HTTP_404_NOT_FOUND
        if "not found" in message
        else status.HTTP_422_UNPROCESSABLE_CONTENT
    )
    return HTTPException(status_code=status_code, detail=message)


async def _list_architecture(
    parent_type: str,
    parent_id: str,
    db: AsyncSession,
) -> list[ArchitectureDesignSummary]:
    await _ensure_parent(db, parent_type, parent_id)
    repo = ArchitectureDesignRepository(db)
    designs = await repo.list(parent_type, parent_id)
    return [repo.to_summary(design) for design in designs]


async def _create_architecture(
    parent_type: str,
    parent_id: str,
    data: ArchitectureDesignCreate,
    user_id: str,
    db: AsyncSession,
) -> ArchitectureDesignResponse:
    await _ensure_parent(db, parent_type, parent_id)
    if parent_type == "spec":
        await _ensure_spec_architecture_unlocked(db, parent_id)
    repo = ArchitectureDesignRepository(db)
    try:
        design = await repo.create(parent_type, parent_id, data, user_id)
    except ValueError as error:
        raise _http_error_from_value(error)
    response = repo.to_response(design)
    await db.commit()
    return response


@router.get("/ideations/{ideation_id}/architecture", response_model=list[ArchitectureDesignSummary])
async def list_ideation_architecture(
    ideation_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    return await _list_architecture("ideation", ideation_id, db)


@router.post(
    "/ideations/{ideation_id}/architecture",
    response_model=ArchitectureDesignResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_ideation_architecture(
    ideation_id: str,
    data: ArchitectureDesignCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    return await _create_architecture("ideation", ideation_id, data, user_id, db)


@router.get("/refinements/{refinement_id}/architecture", response_model=list[ArchitectureDesignSummary])
async def list_refinement_architecture(
    refinement_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    return await _list_architecture("refinement", refinement_id, db)


@router.post(
    "/refinements/{refinement_id}/architecture",
    response_model=ArchitectureDesignResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_refinement_architecture(
    refinement_id: str,
    data: ArchitectureDesignCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    return await _create_architecture("refinement", refinement_id, data, user_id, db)


@router.get("/specs/{spec_id}/architecture", response_model=list[ArchitectureDesignSummary])
async def list_spec_architecture(
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    return await _list_architecture("spec", spec_id, db)


@router.post(
    "/specs/{spec_id}/architecture",
    response_model=ArchitectureDesignResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_spec_architecture(
    spec_id: str,
    data: ArchitectureDesignCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    return await _create_architecture("spec", spec_id, data, user_id, db)


@router.get("/cards/{card_id}/architecture", response_model=list[ArchitectureDesignSummary])
async def list_card_architecture(
    card_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    return await _list_architecture("card", card_id, db)


@router.post(
    "/cards/{card_id}/architecture",
    response_model=ArchitectureDesignResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_card_architecture(
    card_id: str,
    data: ArchitectureDesignCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    return await _create_architecture("card", card_id, data, user_id, db)


@router.get("/architecture/{design_id}", response_model=ArchitectureDesignResponse)
async def get_architecture_design(
    design_id: str,
    include_payloads: bool = Query(False),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    repo = ArchitectureDesignRepository(db)
    design = await repo.get(design_id, include_payloads=include_payloads)
    if design is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Architecture design not found")
    return repo.to_response(design)


@router.patch("/architecture/{design_id}", response_model=ArchitectureDesignResponse)
async def update_architecture_design(
    design_id: str,
    data: ArchitectureDesignUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    await _ensure_design_mutable(db, design_id)
    repo = ArchitectureDesignRepository(db)
    try:
        design = await repo.update(design_id, data, user_id)
    except ValueError as error:
        raise _http_error_from_value(error)
    response = repo.to_response(design)
    await db.commit()
    return response


@router.delete("/architecture/{design_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_architecture_design(
    design_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    await _ensure_design_mutable(db, design_id)
    repo = ArchitectureDesignRepository(db)
    deleted = await repo.delete(design_id, user_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Architecture design not found")
    await db.commit()


@router.get(
    "/architecture/{design_id}/diagrams/{diagram_id}/payload",
    response_model=ArchitectureDiagramPayloadResponse,
)
async def get_architecture_diagram_payload(
    design_id: str,
    diagram_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    repo = ArchitectureDesignRepository(db)
    design = await repo.get(design_id)
    if design is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Architecture design not found")
    diagram = next((item for item in design.diagrams or [] if item.get("id") == diagram_id), None)
    if not diagram or not diagram.get("adapter_payload_ref"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Diagram payload not found")
    store = ArchitectureDiagramStore(db)
    try:
        payload = await store.load_payload(diagram["adapter_payload_ref"])
        stat_info = await store.stat(diagram["adapter_payload_ref"])
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Diagram payload not found")
    return ArchitectureDiagramPayloadResponse(
        design_id=design_id,
        diagram_id=diagram_id,
        format=stat_info["format"],
        content_hash=stat_info["content_hash"],
        size_bytes=stat_info["size_bytes"],
        payload=payload,
    )


@router.put("/architecture/{design_id}/diagrams/{diagram_id}/payload", response_model=ArchitectureDesignResponse)
async def update_architecture_diagram_payload(
    design_id: str,
    diagram_id: str,
    data: DiagramPayloadUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    design = await _ensure_design_mutable(db, design_id)
    diagrams = [dict(item) for item in design.diagrams or []]
    target = next((item for item in diagrams if item.get("id") == diagram_id), None)
    if target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Diagram not found")
    target["format"] = data.format or target.get("format") or "raw"
    target["adapter_payload"] = data.payload
    repo = ArchitectureDesignRepository(db)
    try:
        updated = await repo.update(
            design_id,
            ArchitectureDesignUpdate(
                diagrams=diagrams,
                change_summary=data.change_summary or f"Updated diagram payload {diagram_id}",
            ),
            user_id,
        )
    except ValueError as error:
        raise _http_error_from_value(error)
    response = repo.to_response(updated)
    await db.commit()
    return response


@router.post("/architecture/{design_id}/diagrams/import-excalidraw", response_model=ArchitectureDesignResponse)
async def import_excalidraw_architecture_diagram(
    design_id: str,
    data: ExcalidrawImportRequest,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    design = await _ensure_design_mutable(db, design_id)
    diagrams = [dict(item) for item in design.diagrams or []]
    imported = {
        "id": data.replace_diagram_id or None,
        "title": data.title,
        "diagram_type": data.diagram_type,
        "format": "excalidraw_json",
        "description": data.description,
        "order_index": data.order_index,
        "adapter_payload": data.payload,
    }
    if data.replace_diagram_id:
        index = next((idx for idx, item in enumerate(diagrams) if item.get("id") == data.replace_diagram_id), -1)
        if index < 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Diagram not found")
        diagrams[index] = {**diagrams[index], **imported, "id": data.replace_diagram_id}
    else:
        imported.pop("id")
        diagrams.append(imported)
    repo = ArchitectureDesignRepository(db)
    try:
        updated = await repo.update(
            design_id,
            ArchitectureDesignUpdate(
                diagrams=diagrams,
                change_summary=data.change_summary or "Imported Excalidraw diagram",
            ),
            user_id,
        )
    except ValueError as error:
        raise _http_error_from_value(error)
    response = repo.to_response(updated)
    await db.commit()
    return response


@router.get("/architecture/{design_id}/diff", response_model=ArchitectureDiffResponse)
async def get_architecture_diff(
    design_id: str,
    from_version: int = Query(..., ge=1),
    to_version: int = Query(..., ge=1),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    repo = ArchitectureDesignRepository(db)
    try:
        return await repo.diff(design_id, from_version, to_version)
    except ValueError as error:
        raise _http_error_from_value(error)


@router.post("/cards/{card_id}/copy-architecture-from-spec/{spec_id}", response_model=list[ArchitectureDesignResponse])
async def copy_architecture_from_spec_to_card(
    card_id: str,
    spec_id: str,
    data: CopyArchitectureRequest | None = None,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    service = ArchitecturePropagationService(db)
    try:
        designs = await service.copy_spec_to_card(spec_id, card_id, user_id, design_ids=(data.design_ids if data else None))
    except ValueError as error:
        raise _http_error_from_value(error)
    repo = ArchitectureDesignRepository(db)
    response = [repo.to_response(design) for design in designs]
    await db.commit()
    return response
