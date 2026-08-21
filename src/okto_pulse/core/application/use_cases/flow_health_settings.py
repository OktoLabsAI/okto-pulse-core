"""Governed read/save/restore operations for board Flow Health policy."""

from __future__ import annotations

from dataclasses import dataclass

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.models.schemas import (
    BoardSettings,
    FlowHealthSettings,
    FlowHealthSettingsRestore,
    FlowHealthSettingsUpdate,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.services.flow_health_settings import (
    FlowHealthSettingsVersionConflict,
)


@dataclass(frozen=True, slots=True)
class GetFlowHealthSettingsCommand:
    board_id: str


@dataclass(frozen=True, slots=True)
class SaveFlowHealthSettingsCommand:
    board_id: str
    update: FlowHealthSettingsUpdate


@dataclass(frozen=True, slots=True)
class RestoreFlowHealthSettingsCommand:
    board_id: str
    restore: FlowHealthSettingsRestore


@dataclass(frozen=True, slots=True)
class FlowHealthSettingsResult:
    board_id: str
    settings: FlowHealthSettings

    def canonical_dict(self) -> dict[str, object]:
        return {
            "board_id": self.board_id,
            "settings": self.settings.model_dump(mode="json"),
        }


def _settings_for_board(board: object) -> FlowHealthSettings:
    root = BoardSettings.model_validate(getattr(board, "settings", None) or {})
    return root.analytics.flow_health


async def _load_board(
    board_id: str,
    *,
    actor: ActorContext,
    uow: PulseUnitOfWork,
    write: bool,
) -> object:
    board = await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions={"admin"} if write else None,
    )
    if board is None:
        raise EntityNotFoundError("board", board_id)
    if write:
        await require_authorization(
            actor,
            PermissionRequirement(
                "board.admin.edit",
                legacy_operation="board.read",
            ),
            uow=uow,
            board_id=board_id,
        )
    return board


class GetFlowHealthSettingsUseCase:
    async def execute(
        self,
        command: GetFlowHealthSettingsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> FlowHealthSettingsResult:
        board = await _load_board(command.board_id, actor=actor, uow=uow, write=False)
        return FlowHealthSettingsResult(command.board_id, _settings_for_board(board))


class SaveFlowHealthSettingsUseCase:
    async def execute(
        self,
        command: SaveFlowHealthSettingsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> FlowHealthSettingsResult:
        await _load_board(command.board_id, actor=actor, uow=uow, write=True)
        next_settings = await uow.services.boards.compare_and_swap_flow_health_settings(
            command.board_id,
            actor.actor_id,
            expected_version=command.update.expected_version,
            update=command.update,
        )
        if next_settings is None:
            raise EntityNotFoundError("board", command.board_id)
        await commit(uow)
        return FlowHealthSettingsResult(command.board_id, next_settings)


class RestoreFlowHealthSettingsUseCase:
    async def execute(
        self,
        command: RestoreFlowHealthSettingsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> FlowHealthSettingsResult:
        await _load_board(command.board_id, actor=actor, uow=uow, write=True)
        next_settings = await uow.services.boards.compare_and_swap_flow_health_settings(
            command.board_id,
            actor.actor_id,
            expected_version=command.restore.expected_version,
            update=None,
        )
        if next_settings is None:
            raise EntityNotFoundError("board", command.board_id)
        await commit(uow)
        return FlowHealthSettingsResult(command.board_id, next_settings)


__all__ = [
    "FlowHealthSettingsResult",
    "FlowHealthSettingsVersionConflict",
    "GetFlowHealthSettingsCommand",
    "GetFlowHealthSettingsUseCase",
    "RestoreFlowHealthSettingsCommand",
    "RestoreFlowHealthSettingsUseCase",
    "SaveFlowHealthSettingsCommand",
    "SaveFlowHealthSettingsUseCase",
]
