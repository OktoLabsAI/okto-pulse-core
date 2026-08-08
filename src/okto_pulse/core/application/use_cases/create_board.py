"""create_board use case (spec #09 first cut, REST-only).

Behavior-preserving transport-free reimplementation of
``POST /api/v1/boards`` (``api/boards.py:create_board``). Delegates to the
existing ``BoardService`` so the Community semantics, audit and default-config
snapshot are unchanged; the inbound adapter (spec #09 card 77188b6f) builds the
command/actor and serializes the result via ``BoardResponse``.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.domain.checklist import (
    SPECIFY_CHECKLIST_TEMPLATE_VERSION,
    ChecklistContractError,
    ChecklistMode,
)
from okto_pulse.core.models import BoardCreate
from okto_pulse.core.services.board_governance import BoardGovernanceService
from okto_pulse.core.services.checklist import ChecklistService


class CreateBoardCommand:
    """Input for :class:`CreateBoardUseCase`."""

    __slots__ = ("data",)

    def __init__(self, data: BoardCreate) -> None:
        self.data = data


class CreateBoardResult:
    """Output of :class:`CreateBoardUseCase` — the persisted board, re-fetched
    with relationships and effective settings, ready for ``BoardResponse``."""

    __slots__ = ("board",)

    def __init__(self, board: Any) -> None:
        self.board = board


class CreateBoardUseCase:
    """Create a board without any transport dependency."""

    @staticmethod
    def _checklist_mode_from_snapshot(board: Any) -> ChecklistMode:
        snapshot = getattr(board, "default_config_snapshot", None)
        if snapshot is None:
            return ChecklistMode.ADVISORY
        if not isinstance(snapshot, dict):
            raise ChecklistContractError(
                "invalid_default_checklist_snapshot",
                "The applied default configuration snapshot must be an object.",
            )
        if "spec_checklist" not in snapshot:
            # No active template and historical snapshots both preserve the
            # established forward default without retroactive mutation.
            return ChecklistMode.ADVISORY

        checklist = snapshot["spec_checklist"]
        if not isinstance(checklist, dict):
            raise ChecklistContractError(
                "invalid_default_checklist_snapshot",
                "The applied default checklist snapshot must be an object.",
            )
        try:
            mode = ChecklistMode(checklist.get("mode"))
        except (TypeError, ValueError) as exc:
            raise ChecklistContractError(
                "invalid_default_checklist_snapshot",
                "The applied default checklist snapshot has an invalid mode.",
            ) from exc
        if (
            checklist.get("template_version_id")
            != SPECIFY_CHECKLIST_TEMPLATE_VERSION
        ):
            raise ChecklistContractError(
                "invalid_default_checklist_snapshot",
                "The applied default checklist snapshot has an unsupported template version.",
            )
        return mode

    async def execute(
        self, command: CreateBoardCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CreateBoardResult:
        service = uow.services.boards
        await require_authorization(
            actor,
            PermissionRequirement(
                "board.admin.create",
                legacy_operation="board.read",
            ),
            uow=uow,
        )
        board = await service.create_board(
            actor.actor_id, command.data, realm_id=actor.realm_id
        )
        # TR10: only boards created after A3 activation receive a snapshotted
        # advisory /specify/v1 binding. Legacy boards deliberately remain
        # binding-less and resolve to effective OFF without retropropagation.
        checklist_service = ChecklistService()
        checklist_binding = checklist_service.prepare_binding(
            board_id=board.id,
            mode=self._checklist_mode_from_snapshot(board),
            current_binding=None,
        )
        await checklist_service.apply_binding(
            checklist_binding,
            previous_binding=None,
            persistence=uow.services.checklists,
        )
        await service.record_checklist_binding_change(
            board_id=board.id,
            actor_id=actor.actor_id,
            binding=checklist_binding,
            previous_binding=None,
            change_source="board_bootstrap",
        )
        await commit(uow)
        # Re-fetch with relationships loaded (mirrors api/boards.py:create_board).
        board = await service.get_board(board.id)
        values = {
            "agents": [],
            "settings": BoardGovernanceService.normalize_settings(
                getattr(board, "settings", None)
            ),
        }
        attach = getattr(board, "attach", None)
        if callable(attach):
            for name, value in values.items():
                attach(name, value)
        else:
            board.__dict__.update(values)
        return CreateBoardResult(board=board)
