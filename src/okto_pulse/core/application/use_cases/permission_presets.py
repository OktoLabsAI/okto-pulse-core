"""Transport-neutral use cases for current-user permissions and presets."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
    session_of,
)
from okto_pulse.core.services.permission_presets import (
    EffectivePermissions,
    PermissionPresetRestService,
)


class GetMyPermissionsCommand:
    __slots__ = ("board_id",)

    def __init__(self, *, board_id: str) -> None:
        self.board_id = board_id


class GetMyPermissionsResult:
    __slots__ = ("permissions",)

    def __init__(self, permissions: EffectivePermissions) -> None:
        self.permissions = permissions


class GetMyPermissionsUseCase:
    async def execute(
        self,
        command: GetMyPermissionsCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> GetMyPermissionsResult:
        service = PermissionPresetRestService(session_of(uow))
        permissions = await service.get_effective_permissions(
            user_id=actor.actor_id,
            board_id=command.board_id,
        )
        return GetMyPermissionsResult(permissions)


class ListPermissionPresetsCommand:
    __slots__ = ()


class ListPermissionPresetsResult:
    __slots__ = ("presets",)

    def __init__(self, presets: list[Any]) -> None:
        self.presets = presets


class ListPermissionPresetsUseCase:
    async def execute(
        self,
        command: ListPermissionPresetsCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> ListPermissionPresetsResult:
        service = PermissionPresetRestService(session_of(uow))
        return ListPermissionPresetsResult(
            await service.list_presets(user_id=actor.actor_id)
        )


class CreatePermissionPresetCommand:
    __slots__ = ("name", "description", "flags")

    def __init__(
        self,
        *,
        name: str,
        description: str,
        flags: dict | None,
    ) -> None:
        self.name = name
        self.description = description
        self.flags = flags


class PermissionPresetResult:
    __slots__ = ("preset",)

    def __init__(self, preset: Any) -> None:
        self.preset = preset


class CreatePermissionPresetUseCase:
    async def execute(
        self,
        command: CreatePermissionPresetCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> PermissionPresetResult:
        service = PermissionPresetRestService(session_of(uow))
        preset = await service.create_preset(
            user_id=actor.actor_id,
            name=command.name,
            description=command.description,
            flags=command.flags,
        )
        await commit(uow)
        return PermissionPresetResult(await service.refresh(preset))


class ClonePermissionPresetCommand:
    __slots__ = ("preset_id", "name", "description", "flags")

    def __init__(
        self,
        *,
        preset_id: str,
        name: str,
        description: str,
        flags: dict | None,
    ) -> None:
        self.preset_id = preset_id
        self.name = name
        self.description = description
        self.flags = flags


class ClonePermissionPresetUseCase:
    async def execute(
        self,
        command: ClonePermissionPresetCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> PermissionPresetResult:
        service = PermissionPresetRestService(session_of(uow))
        preset = await service.clone_preset(
            source_preset_id=command.preset_id,
            user_id=actor.actor_id,
            name=command.name,
            description=command.description,
            flags=command.flags,
        )
        if preset is None:
            raise EntityNotFoundError("source_preset", command.preset_id)
        await commit(uow)
        return PermissionPresetResult(await service.refresh(preset))


class UpdatePermissionPresetCommand:
    __slots__ = ("preset_id", "name", "description", "flags")

    def __init__(
        self,
        *,
        preset_id: str,
        name: str | None,
        description: str | None,
        flags: dict | None,
    ) -> None:
        self.preset_id = preset_id
        self.name = name
        self.description = description
        self.flags = flags


class UpdatePermissionPresetUseCase:
    async def execute(
        self,
        command: UpdatePermissionPresetCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> PermissionPresetResult:
        service = PermissionPresetRestService(session_of(uow))
        try:
            preset = await service.update_preset(
                preset_id=command.preset_id,
                user_id=actor.actor_id,
                name=command.name,
                description=command.description,
                flags=command.flags,
            )
        except PermissionError as exc:
            raise PermissionDeniedError(str(exc)) from exc
        if preset is None:
            raise EntityNotFoundError("preset", command.preset_id)
        await commit(uow)
        return PermissionPresetResult(await service.refresh(preset))


class DeletePermissionPresetCommand:
    __slots__ = ("preset_id",)

    def __init__(self, *, preset_id: str) -> None:
        self.preset_id = preset_id


class DeletePermissionPresetUseCase:
    async def execute(
        self,
        command: DeletePermissionPresetCommand,
        *,
        actor: ActorContext,
        uow: Any,
    ) -> None:
        service = PermissionPresetRestService(session_of(uow))
        try:
            deleted = await service.delete_preset(
                preset_id=command.preset_id,
                user_id=actor.actor_id,
            )
        except PermissionError as exc:
            raise PermissionDeniedError(str(exc)) from exc
        if not deleted:
            raise EntityNotFoundError("preset", command.preset_id)
        await commit(uow)
