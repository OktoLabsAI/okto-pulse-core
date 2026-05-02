"""Permission preset API endpoints."""

import copy
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.db import PermissionPreset

router = APIRouter()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class PresetCreate(BaseModel):
    name: str
    description: str = ""
    flags: dict | None = None


class PresetUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    flags: dict | None = None


class PresetResponse(BaseModel):
    id: str
    owner_id: str | None
    name: str
    description: str | None
    is_builtin: bool
    base_preset_id: str | None
    flags: dict | None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=list[PresetResponse])
async def list_presets(
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all presets: built-in + custom owned by the user."""
    query = select(PermissionPreset).where(
        (PermissionPreset.is_builtin.is_(True))
        | (PermissionPreset.owner_id == user_id)
    ).order_by(PermissionPreset.is_builtin.desc(), PermissionPreset.name)
    result = await db.execute(query)
    return list(result.scalars().all())


@router.post("", response_model=PresetResponse, status_code=status.HTTP_201_CREATED)
async def create_preset(
    data: PresetCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a custom preset."""
    preset = PermissionPreset(
        id=str(uuid.uuid4()),
        owner_id=user_id,
        name=data.name,
        description=data.description or None,
        is_builtin=False,
        flags=data.flags,
    )
    db.add(preset)
    await db.commit()
    await db.refresh(preset)
    return preset


@router.post("/{preset_id}/clone", response_model=PresetResponse, status_code=status.HTTP_201_CREATED)
async def clone_preset(
    preset_id: str,
    data: PresetCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Clone an existing preset (built-in or custom) as a new custom preset."""
    source = await db.get(PermissionPreset, preset_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source preset not found")

    cloned_flags = copy.deepcopy(source.flags) if source.flags else {}
    # Apply any flag overrides from the request
    if data.flags:
        from okto_pulse.core.infra.permissions import _flatten_registry, _get_nested, _set_nested
        for path in _flatten_registry(data.flags):
            value = _get_nested(data.flags, path)
            if value is not None:
                _set_nested(cloned_flags, path, value)

    preset = PermissionPreset(
        id=str(uuid.uuid4()),
        owner_id=user_id,
        name=data.name,
        description=data.description or source.description,
        is_builtin=False,
        base_preset_id=preset_id,
        flags=cloned_flags,
    )
    db.add(preset)
    await db.commit()
    await db.refresh(preset)
    return preset


@router.put("/{preset_id}", response_model=PresetResponse)
async def update_preset(
    preset_id: str,
    data: PresetUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a custom preset. Built-in presets cannot be modified."""
    preset = await db.get(PermissionPreset, preset_id)
    if not preset:
        raise HTTPException(status_code=404, detail="Preset not found")
    if preset.is_builtin:
        raise HTTPException(status_code=403, detail="Built-in presets cannot be modified or deleted")
    if preset.owner_id != user_id:
        raise HTTPException(status_code=403, detail="You can only modify your own presets")

    if data.name is not None:
        preset.name = data.name
    if data.description is not None:
        preset.description = data.description
    if data.flags is not None:
        preset.flags = data.flags

    await db.commit()
    await db.refresh(preset)
    return preset


@router.delete("/{preset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_preset(
    preset_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a custom preset. Built-in presets cannot be deleted."""
    preset = await db.get(PermissionPreset, preset_id)
    if not preset:
        raise HTTPException(status_code=404, detail="Preset not found")
    if preset.is_builtin:
        raise HTTPException(status_code=403, detail="Built-in presets cannot be modified or deleted")
    if preset.owner_id != user_id:
        raise HTTPException(status_code=403, detail="You can only delete your own presets")

    await db.delete(preset)
    await db.commit()
