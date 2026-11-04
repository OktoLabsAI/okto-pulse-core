"""Authorized native preview/apply boundary for proven legacy allocations."""

from dataclasses import dataclass

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import EntityNotFoundError, commit
from okto_pulse.core.application.use_cases.board_access import load_accessible_board


@dataclass(frozen=True)
class BugAllocationRepairCommand:
    board_id: str
    spec_id: str
    bug_id: str
    expected_spec_version: int | None = None
    expected_scope_digest: str | None = None
    selected_link_ids: tuple[str, ...] = ()
    idempotency_key: str | None = None


class BugAllocationRepairUseCase:
    async def execute(self, command, *, actor, uow, apply=False):
        board = await load_accessible_board(
            uow,
            command.board_id,
            actor,
            allowed_share_permissions={"editor", "admin"} if apply else None,
        )
        if board is None:
            raise EntityNotFoundError("board", command.board_id)
        await require_authorization(
            actor,
            PermissionRequirement(
                "spec.entity.edit_fields" if apply else "spec.entity.read"
            ),
            uow=uow,
            board_id=command.board_id,
        )
        common = {
            "board_id": command.board_id,
            "spec_id": command.spec_id,
            "bug_id": command.bug_id,
        }
        if not apply:
            return await uow.services.specs.preview_bug_allocation_repair(**common)
        from okto_pulse.core.services.bug_allocation_repair import (
            BugAllocationRepairError,
        )

        if (
            type(command.expected_spec_version) is not int
            or not command.expected_scope_digest
            or not command.idempotency_key
        ):
            raise BugAllocationRepairError(
                "repair_preview_required",
                "Apply requires the exact version, digest and selection from a current preview.",
            )
        result = await uow.services.specs.apply_bug_allocation_repair(
            **common,
            actor=actor,
            expected_spec_version=command.expected_spec_version,
            expected_scope_digest=command.expected_scope_digest,
            selected_link_ids=command.selected_link_ids,
            idempotency_key=command.idempotency_key,
        )
        await commit(uow)
        return result
