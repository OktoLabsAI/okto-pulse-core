"""Authorized classification review without exposing IRs to architecture-only readers."""

from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_all,
)
from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.architecture_classification_review import (
    ArchitectureReviewState,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.ports.application_persistence import (
    ApplicationFilter,
    ApplicationQuery,
)


@dataclass(frozen=True, slots=True)
class GetArchitectureClassificationsCommand:
    board_id: str
    spec_id: str
    offset: int = 0
    limit: int = 25
    candidate_id: str | None = None
    source_digest: str | None = None
    state: ArchitectureReviewState | None = None


class GetArchitectureClassificationsUseCase:
    async def execute(
        self,
        command: GetArchitectureClassificationsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> dict[str, Any]:
        await uow.begin_consistent_read()
        if await load_accessible_board(uow, command.board_id, actor) is None:
            raise EntityNotFoundError("spec", command.spec_id)
        await require_all(
            actor,
            PermissionRequirement("spec.entity.read"),
            PermissionRequirement("spec.architecture.read"),
            PermissionRequirement("spec.integration_requirements.read"),
            uow=uow,
            board_id=command.board_id,
        )
        # includes=() suppresses relationships, not scalar JSON bodies. Use
        # the existing public field projection and scope both keys in SQL.
        matches = await uow.services.list_application_records(
            ApplicationQuery(
                entity="spec",
                filters=(
                    ApplicationFilter("id", "eq", command.spec_id),
                    ApplicationFilter("board_id", "eq", command.board_id),
                ),
                select_fields=("id", "board_id"),
                limit=1,
            )
        )
        if not matches:
            raise EntityNotFoundError("spec", command.spec_id)
        return await uow.services.architecture_classifications.review(
            board_id=command.board_id,
            spec_id=command.spec_id,
            offset=command.offset,
            limit=command.limit,
            candidate_id=command.candidate_id,
            source_digest=command.source_digest,
            state=command.state,
        )
