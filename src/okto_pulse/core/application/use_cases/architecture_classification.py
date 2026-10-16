"""Authorized all-or-nothing classification across REST and MCP."""

from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_all,
    resolve_actor_permissions,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.architecture_classification import (
    ArchitectureClassificationBatch,
)
from okto_pulse.core.domain.permissions import ALL_FLAGS
from okto_pulse.core.ports.application_persistence import (
    ApplicationFilter,
    ApplicationQuery,
)
from okto_pulse.core.ports.permission_policy import PermissionSet, set_permission_flag
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


@dataclass(frozen=True, slots=True)
class ClassifyArchitectureCandidatesCommand:
    board_id: str
    spec_id: str
    batch: ArchitectureClassificationBatch


class ClassifyArchitectureCandidatesUseCase:
    async def execute(
        self,
        command: ClassifyArchitectureCandidatesCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> dict[str, Any]:
        # Detach nested authored payloads and revalidate before any UoW writes.
        batch = ArchitectureClassificationBatch.model_validate(
            command.batch.model_dump(mode="json")
        )
        await uow.begin_write()
        try:
            if await load_accessible_board(uow, command.board_id, actor) is None:
                raise EntityNotFoundError("spec", command.spec_id)
            # Authorize the entire batch using scoped metadata only. includes=()
            # would still load scalar requirement JSON before its read authority.
            matches = await uow.services.list_application_records(
                ApplicationQuery(
                    entity="spec",
                    filters=(
                        ApplicationFilter("id", "eq", command.spec_id),
                        ApplicationFilter("board_id", "eq", command.board_id),
                    ),
                    select_fields=("id", "board_id", "status", "archived"),
                    limit=1,
                )
            )
            spec = matches[0] if matches else None
            if (
                spec is None
                or spec.board_id != command.board_id
                or getattr(spec, "archived", False)
            ):
                raise EntityNotFoundError("spec", command.spec_id)
            status = str(getattr(spec.status, "value", spec.status))
            operations = {
                "spec.entity.read",
                "spec.architecture.read",
                "spec.integration_requirements.read",
                "spec.entity.edit_fields",
            }
            for item in batch.decisions:
                if item.disposition == "promote_to_ir":
                    operations.add(
                        "spec.structured_entity.integration_requirement.create"
                    )
                elif item.disposition == "associate_existing_ir":
                    operations.add(
                        "spec.structured_entity.integration_requirement.update"
                    )
            permissions = await resolve_actor_permissions(actor, uow, command.board_id)
            await require_all(
                actor,
                *(
                    PermissionRequirement(operation, entity="spec", state=status)
                    for operation in sorted(operations)
                ),
                uow=uow,
                board_id=command.board_id,
                permissions=permissions,
            )
            if not isinstance(permissions, PermissionSet):
                # Legacy MCP/system claims are evaluated by the canonical
                # policy above. Reify only those granted leaves for the old
                # structured service; never persist or broaden a policy.
                flags: dict[str, Any] = {}
                for flag in ALL_FLAGS:
                    set_permission_flag(flags, flag, False)
                for flag in operations | {f"spec.interact_in.{status}"}:
                    set_permission_flag(flags, flag, True)
                permissions = PermissionSet(flags)
            result = await uow.services.architecture_classifications.apply(
                board_id=command.board_id,
                spec_id=command.spec_id,
                batch=batch,
                actor_id=actor.actor_id,
                actor_name=actor.actor_name,
                actor_kind=actor.actor_kind,
                permissions=permissions,
            )
            if result["replayed"]:
                await uow.rollback()
            else:
                await commit(uow)
            return result
        except BaseException:
            await uow.rollback()
            raise
