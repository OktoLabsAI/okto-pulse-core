"""Endpoints for the currently authenticated user/agent.

Exposes effective permissions for frontend gating (`GET /me/permissions`).
Backend still enforces authorization via 403 — this endpoint only lets the
frontend reflect the same intent in UI (hide/disable buttons that would be
rejected anyway).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.infra.permissions import (
    _match_builtin_preset_name,
    map_legacy_permissions,
    resolve_permissions,
)
from okto_pulse.core.models.db import Agent, PermissionPreset

router = APIRouter()


class PermissionsResponse(BaseModel):
    board_id: str
    preset_name: str | None
    flags: dict

    model_config = {"from_attributes": True}


@router.get("/me/permissions", response_model=PermissionsResponse)
async def get_my_permissions(
    board_id: str = Query(..., description="Board to resolve permissions against"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> PermissionsResponse:
    """Return the authenticated user's effective permission flags for a board.

    Flags are resolved via ``resolve_permissions(agent_flags, preset_flags,
    board_overrides)`` — the exact same path used by runtime permission
    checks. Board overrides that restrict flags (ceiling model) appear as
    ``False``.

    Legacy agents (``permissions`` column non-null, flat list) are mapped
    through ``map_legacy_permissions`` first. Agents with no granular data
    at all default to the full registry (True for everything), matching the
    historical "full access" compat path.
    """
    # Best-effort lookup: find an Agent associated with this user + board.
    # Community edition may not have the Agent <-> user coupling fully
    # populated, so we fall back to a permissive default on miss.
    result = await db.execute(
        select(Agent).where(Agent.created_by == user_id).limit(1)
    )
    agent = result.scalar_one_or_none()

    agent_flags: dict | None = None
    preset_flags: dict | None = None
    preset_name: str | None = None

    if agent is not None:
        if isinstance(agent.permission_flags, dict) and agent.permission_flags:
            agent_flags = agent.permission_flags
        elif isinstance(agent.permissions, list) and agent.permissions:
            agent_flags = map_legacy_permissions(agent.permissions)

        # Load preset flags if preset_id points at one.
        if agent.preset_id:
            preset_row = await db.get(PermissionPreset, agent.preset_id)
            if preset_row and preset_row.flags:
                preset_flags = preset_row.flags
                preset_name = preset_row.name

    # Fallback: if no agent/preset resolved yet, leave the ceiling model
    # wide open — resolve_permissions with None preset_flags yields the full
    # registry (True for everything). This matches the legacy behaviour.
    permission_set = resolve_permissions(
        agent_flags=agent_flags,
        preset_flags=preset_flags,
        board_overrides=None,
    )

    # If no explicit preset_name and flags match a built-in, surface the name.
    if preset_name is None:
        preset_name = _match_builtin_preset_name(permission_set.flags)

    return PermissionsResponse(
        board_id=board_id,
        preset_name=preset_name,
        flags=permission_set.flags,
    )
