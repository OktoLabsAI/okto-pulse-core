"""Transport-neutral use cases for current-user permissions and presets."""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
)
from okto_pulse.core.ports.relational_application import (
    EffectivePermissions,
)


def _gateway(uow: PulseUnitOfWork):
    return uow.services.permission_presets


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
        uow: PulseUnitOfWork,
    ) -> GetMyPermissionsResult:
        if await load_accessible_board(uow, command.board_id, actor) is None:
            raise EntityNotFoundError("board", command.board_id)
        permissions = await _gateway(uow).get_effective_permissions(
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
        uow: PulseUnitOfWork,
    ) -> ListPermissionPresetsResult:
        return ListPermissionPresetsResult(
            await _gateway(uow).list_presets(user_id=actor.actor_id)
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
        uow: PulseUnitOfWork,
    ) -> PermissionPresetResult:
        preset = await _gateway(uow).create_preset(
            user_id=actor.actor_id,
            name=command.name,
            description=command.description,
            flags=command.flags,
        )
        await commit(uow)
        return PermissionPresetResult(preset)


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
        uow: PulseUnitOfWork,
    ) -> PermissionPresetResult:
        preset = await _gateway(uow).clone_preset(
            source_preset_id=command.preset_id,
            user_id=actor.actor_id,
            name=command.name,
            description=command.description,
            flags=command.flags,
        )
        if preset is None:
            raise EntityNotFoundError("source_preset", command.preset_id)
        await commit(uow)
        return PermissionPresetResult(preset)


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
        uow: PulseUnitOfWork,
    ) -> PermissionPresetResult:
        try:
            preset = await _gateway(uow).update_preset(
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
        return PermissionPresetResult(preset)


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
        uow: PulseUnitOfWork,
    ) -> None:
        try:
            deleted = await _gateway(uow).delete_preset(
                preset_id=command.preset_id,
                user_id=actor.actor_id,
            )
        except PermissionError as exc:
            raise PermissionDeniedError(str(exc)) from exc
        if not deleted:
            raise EntityNotFoundError("preset", command.preset_id)
        await commit(uow)
