"""Permission and preset service helpers for REST use cases."""

from __future__ import annotations

import copy
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.permissions import (
    _flatten_registry,
    _get_nested,
    _match_builtin_preset_name,
    _set_nested,
    map_legacy_permissions,
    resolve_permissions,
)
from okto_pulse.core.models.db import Agent, PermissionPreset


@dataclass(frozen=True)
class EffectivePermissions:
    board_id: str
    preset_name: str | None
    flags: dict[str, Any]


class PermissionPresetRestService:
    """Relational implementation behind the me/presets use cases."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_effective_permissions(
        self,
        *,
        user_id: str,
        board_id: str,
    ) -> EffectivePermissions:
        result = await self.db.execute(
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

            if agent.preset_id:
                preset_row = await self.db.get(PermissionPreset, agent.preset_id)
                if preset_row and preset_row.flags:
                    preset_flags = preset_row.flags
                    preset_name = preset_row.name

        permission_set = resolve_permissions(
            agent_flags=agent_flags,
            preset_flags=preset_flags,
            board_overrides=None,
        )

        if preset_name is None:
            preset_name = _match_builtin_preset_name(permission_set.flags)

        return EffectivePermissions(
            board_id=board_id,
            preset_name=preset_name,
            flags=permission_set.flags,
        )

    async def list_presets(self, *, user_id: str) -> list[PermissionPreset]:
        query = (
            select(PermissionPreset)
            .where(
                (PermissionPreset.is_builtin.is_(True))
                | (PermissionPreset.owner_id == user_id)
            )
            .order_by(PermissionPreset.is_builtin.desc(), PermissionPreset.name)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def create_preset(
        self,
        *,
        user_id: str,
        name: str,
        description: str,
        flags: dict | None,
    ) -> PermissionPreset:
        preset = PermissionPreset(
            id=str(uuid.uuid4()),
            owner_id=user_id,
            name=name,
            description=description or None,
            is_builtin=False,
            flags=flags,
        )
        self.db.add(preset)
        await self.db.flush()
        return preset

    async def clone_preset(
        self,
        *,
        source_preset_id: str,
        user_id: str,
        name: str,
        description: str,
        flags: dict | None,
    ) -> PermissionPreset | None:
        source = await self.db.get(PermissionPreset, source_preset_id)
        if not source:
            return None

        cloned_flags = copy.deepcopy(source.flags) if source.flags else {}
        if flags:
            for path in _flatten_registry(flags):
                value = _get_nested(flags, path)
                if value is not None:
                    _set_nested(cloned_flags, path, value)

        preset = PermissionPreset(
            id=str(uuid.uuid4()),
            owner_id=user_id,
            name=name,
            description=description or source.description,
            is_builtin=False,
            base_preset_id=source_preset_id,
            flags=cloned_flags,
        )
        self.db.add(preset)
        await self.db.flush()
        return preset

    async def update_preset(
        self,
        *,
        preset_id: str,
        user_id: str,
        name: str | None,
        description: str | None,
        flags: dict | None,
    ) -> PermissionPreset | None:
        preset = await self.db.get(PermissionPreset, preset_id)
        if not preset:
            return None
        if preset.is_builtin:
            raise PermissionError("Built-in presets cannot be modified or deleted")
        if preset.owner_id != user_id:
            raise PermissionError("You can only modify your own presets")

        if name is not None:
            preset.name = name
        if description is not None:
            preset.description = description
        if flags is not None:
            preset.flags = flags

        await self.db.flush()
        return preset

    async def delete_preset(self, *, preset_id: str, user_id: str) -> bool:
        preset = await self.db.get(PermissionPreset, preset_id)
        if not preset:
            return False
        if preset.is_builtin:
            raise PermissionError("Built-in presets cannot be modified or deleted")
        if preset.owner_id != user_id:
            raise PermissionError("You can only delete your own presets")

        await self.db.delete(preset)
        await self.db.flush()
        return True

    async def refresh(self, preset: PermissionPreset) -> PermissionPreset:
        await self.db.refresh(preset)
        return preset
