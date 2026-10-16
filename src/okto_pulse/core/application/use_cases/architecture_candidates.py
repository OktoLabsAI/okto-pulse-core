"""Authorized, consistent reads of the Spec's adopted architecture contracts."""

from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.use_cases.authorization import PermissionRequirement, require_all
from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.architecture_candidates import architecture_candidate_read_projection
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


@dataclass(frozen=True, slots=True)
class GetArchitectureCandidatesCommand:
    board_id: str
    spec_id: str
    offset: int = 0
    limit: int = 25
    candidate_id: str | None = None
    source_digest: str | None = None


class GetArchitectureCandidatesUseCase:
    async def execute(
        self, command: GetArchitectureCandidatesCommand, *,
        actor: ActorContext, uow: PulseUnitOfWork,
    ) -> dict[str, Any]:
        # One snapshot includes ownership, permissions, lineage and bodies.
        await uow.begin_consistent_read()
        if await load_accessible_board(uow, command.board_id, actor) is None:
            raise EntityNotFoundError("spec", command.spec_id)
        # The generic rich Spec reader eagerly loads Design bodies. Resolve
        # just the subject record until ownership and read leaves are checked.
        spec = await uow.services.get_application_record(
            entity="spec", record_id=command.spec_id, includes=(),
        )
        if spec is None or spec.board_id != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)
        await require_all(
            actor,
            PermissionRequirement("spec.entity.read"),
            PermissionRequirement("spec.architecture.read"),
            uow=uow, board_id=command.board_id,
        )
        population = await uow.services.load_spec_architecture_candidates(
            board_id=command.board_id, spec_id=command.spec_id,
        )
        return architecture_candidate_read_projection(
            population, board_id=command.board_id, spec_id=command.spec_id,
            spec_version=spec.version, spec_edition=spec.edition,
            offset=command.offset, limit=command.limit,
            candidate_id=command.candidate_id, source_digest=command.source_digest,
        )
