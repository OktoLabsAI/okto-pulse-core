"""Transport-neutral Project structure reads and the governed batch write."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.card_crud import _get_card_for_actor
from okto_pulse.core.application.use_cases.spec_crud import _require_actor_board_spec
from okto_pulse.core.domain.project_structure import (
    ProjectStructureProjection,
    ProjectStructureSnapshot,
    project_project_structure,
    project_structure_snapshot,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.services.spec_structured_entities import (
    StructuredSpecEntityCommand,
)


class GetProjectStructureCommand:
    __slots__ = ("board_id", "spec_id")

    def __init__(self, board_id: str, spec_id: str) -> None:
        self.board_id = board_id
        self.spec_id = spec_id


class GetProjectStructureResult:
    __slots__ = ("structure",)

    def __init__(self, structure: ProjectStructureSnapshot) -> None:
        self.structure = structure


class GetProjectStructureUseCase:
    async def execute(
        self,
        command: GetProjectStructureCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetProjectStructureResult:
        spec = await _require_actor_board_spec(uow, command.spec_id, actor)
        if spec.board_id != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)
        await require_authorization(
            actor,
            PermissionRequirement("spec.entity.read"),
            uow=uow,
            board_id=command.board_id,
        )
        return GetProjectStructureResult(
            project_structure_snapshot(
                getattr(spec, "project_structure", None),
                spec_id=spec.id,
                spec_version=int(spec.version),
                structure_revision=int(
                    getattr(spec, "project_structure_revision", 0) or 0
                ),
            )
        )


class GetCardProjectStructureProjectionCommand:
    __slots__ = ("board_id", "card_id")

    def __init__(self, board_id: str, card_id: str) -> None:
        self.board_id = board_id
        self.card_id = card_id


class GetCardProjectStructureProjectionResult:
    __slots__ = ("projection",)

    def __init__(self, projection: ProjectStructureProjection) -> None:
        self.projection = projection


class GetCardProjectStructureProjectionUseCase:
    async def execute(
        self,
        command: GetCardProjectStructureProjectionCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> GetCardProjectStructureProjectionResult:
        card = await _get_card_for_actor(
            uow,
            command.card_id,
            actor,
            expected_board_id=command.board_id,
        )
        await require_authorization(
            actor,
            PermissionRequirement("card.entity.read"),
            uow=uow,
            board_id=command.board_id,
        )
        spec_id = getattr(card, "spec_id", None)
        if not spec_id:
            raise EntityNotFoundError("spec", "")
        spec = await _require_actor_board_spec(uow, str(spec_id), actor)
        if spec.board_id != command.board_id:
            raise EntityNotFoundError("spec", str(spec_id))
        await require_authorization(
            actor,
            PermissionRequirement("spec.entity.read"),
            uow=uow,
            board_id=command.board_id,
        )
        raw_card_type = getattr(card, "card_type", "normal")
        card_type = str(getattr(raw_card_type, "value", raw_card_type)).lower()
        if card_type not in {"normal", "test"}:
            raise CommandValidationError(
                f"project_structure_projection_unsupported_card_type:{card_type}"
            )
        reference_type = "test" if card_type == "test" else "task"
        return GetCardProjectStructureProjectionResult(
            project_project_structure(
                getattr(spec, "project_structure", None),
                spec_id=spec.id,
                spec_version=int(spec.version),
                structure_revision=int(
                    getattr(spec, "project_structure_revision", 0) or 0
                ),
                reference_type=reference_type,
                reference_id=card.id,
            )
        )


class MutateProjectStructureCommand:
    __slots__ = (
        "board_id",
        "spec_id",
        "operations",
        "expected_spec_version",
        "expected_structure_revision",
        "expected_spec_edition",
        "idempotency_key",
    )

    def __init__(
        self,
        board_id: str,
        spec_id: str,
        *,
        operations: list[dict[str, Any]],
        expected_spec_version: int,
        expected_structure_revision: int,
        idempotency_key: str,
        expected_spec_edition: int | None = None,
    ) -> None:
        self.board_id = board_id
        self.spec_id = spec_id
        self.operations = operations
        self.expected_spec_version = expected_spec_version
        self.expected_structure_revision = expected_structure_revision
        self.expected_spec_edition = expected_spec_edition
        self.idempotency_key = idempotency_key


class MutateProjectStructureResult:
    __slots__ = ("structured_result",)

    def __init__(self, structured_result: Any) -> None:
        self.structured_result = structured_result


class MutateProjectStructureUseCase:
    """Route every REST/UI batch through the same single-writer service as MCP."""

    async def execute(
        self,
        command: MutateProjectStructureCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> MutateProjectStructureResult:
        spec = await _require_actor_board_spec(
            uow,
            command.spec_id,
            actor,
            write=True,
        )
        if spec.board_id != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)
        permission_set = await uow.services.resolve_user_permissions(
            actor.actor_id,
            command.board_id,
        )
        result = await uow.services.structured_specs.apply(
            StructuredSpecEntityCommand(
                board_id=command.board_id,
                spec_id=command.spec_id,
                actor_id=actor.actor_id,
                entity_type="project_structure_node",
                operation="batch",
                # Keep validation behind the structured service's concrete-leaf
                # authorization check.  Unauthorized callers must not learn
                # payload-shape details from this transport-neutral wrapper.
                payload={"operations": command.operations},
                expected_spec_version=command.expected_spec_version,
                expected_structure_revision=command.expected_structure_revision,
                expected_spec_edition=command.expected_spec_edition,
                idempotency_key=command.idempotency_key,
                permission_set=permission_set,
            )
        )
        if not result.success:
            await uow.rollback()
        else:
            # A successful no-op still claimed a durable idempotency key.
            await commit(uow)
        return MutateProjectStructureResult(result)


__all__ = [
    "GetCardProjectStructureProjectionCommand",
    "GetCardProjectStructureProjectionResult",
    "GetCardProjectStructureProjectionUseCase",
    "GetProjectStructureCommand",
    "GetProjectStructureResult",
    "GetProjectStructureUseCase",
    "MutateProjectStructureCommand",
    "MutateProjectStructureResult",
    "MutateProjectStructureUseCase",
]
