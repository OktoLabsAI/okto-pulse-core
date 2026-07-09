"""Permission preset API endpoints."""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from okto_pulse.core.api.deps import get_unit_of_work
from okto_pulse.core.application.use_cases import (
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.permission_presets import (
    ClonePermissionPresetCommand,
    ClonePermissionPresetUseCase,
    CreatePermissionPresetCommand,
    CreatePermissionPresetUseCase,
    DeletePermissionPresetCommand,
    DeletePermissionPresetUseCase,
    ListPermissionPresetsCommand,
    ListPermissionPresetsUseCase,
    UpdatePermissionPresetCommand,
    UpdatePermissionPresetUseCase,
)
from okto_pulse.core.inbound.rest_adapter import RESTAdapterContract
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.repositories import PulseUnitOfWork

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
    uow: PulseUnitOfWork = Depends(get_unit_of_work),
):
    """List all presets: built-in + custom owned by the user."""
    result = await ListPermissionPresetsUseCase().execute(
        ListPermissionPresetsCommand(),
        actor=RESTAdapterContract.actor(user_id),
        uow=uow,
    )
    return result.presets


@router.post("", response_model=PresetResponse, status_code=status.HTTP_201_CREATED)
async def create_preset(
    data: PresetCreate,
    user_id: str = Depends(require_user),
    uow: PulseUnitOfWork = Depends(get_unit_of_work),
):
    """Create a custom preset."""
    result = await CreatePermissionPresetUseCase().execute(
        CreatePermissionPresetCommand(
            name=data.name,
            description=data.description,
            flags=data.flags,
        ),
        actor=RESTAdapterContract.actor(user_id),
        uow=uow,
    )
    return result.preset


@router.post("/{preset_id}/clone", response_model=PresetResponse, status_code=status.HTTP_201_CREATED)
async def clone_preset(
    preset_id: str,
    data: PresetCreate,
    user_id: str = Depends(require_user),
    uow: PulseUnitOfWork = Depends(get_unit_of_work),
):
    """Clone an existing preset (built-in or custom) as a new custom preset."""
    try:
        result = await ClonePermissionPresetUseCase().execute(
            ClonePermissionPresetCommand(
                preset_id=preset_id,
                name=data.name,
                description=data.description,
                flags=data.flags,
            ),
            actor=RESTAdapterContract.actor(user_id),
            uow=uow,
        )
    except EntityNotFoundError:
        raise HTTPException(status_code=404, detail="Source preset not found")
    return result.preset


@router.put("/{preset_id}", response_model=PresetResponse)
async def update_preset(
    preset_id: str,
    data: PresetUpdate,
    user_id: str = Depends(require_user),
    uow: PulseUnitOfWork = Depends(get_unit_of_work),
):
    """Update a custom preset. Built-in presets cannot be modified."""
    try:
        result = await UpdatePermissionPresetUseCase().execute(
            UpdatePermissionPresetCommand(
                preset_id=preset_id,
                name=data.name,
                description=data.description,
                flags=data.flags,
            ),
            actor=RESTAdapterContract.actor(user_id),
            uow=uow,
        )
    except EntityNotFoundError:
        raise HTTPException(status_code=404, detail="Preset not found")
    except PermissionDeniedError as exc:
        raise HTTPException(status_code=403, detail=exc.message)
    return result.preset


@router.delete("/{preset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_preset(
    preset_id: str,
    user_id: str = Depends(require_user),
    uow: PulseUnitOfWork = Depends(get_unit_of_work),
):
    """Delete a custom preset. Built-in presets cannot be deleted."""
    try:
        await DeletePermissionPresetUseCase().execute(
            DeletePermissionPresetCommand(preset_id=preset_id),
            actor=RESTAdapterContract.actor(user_id),
            uow=uow,
        )
    except EntityNotFoundError:
        raise HTTPException(status_code=404, detail="Preset not found")
    except PermissionDeniedError as exc:
        raise HTTPException(status_code=403, detail=exc.message)
