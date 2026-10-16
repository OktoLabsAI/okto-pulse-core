"""Shared REST/MCP entry points; inspection is an authenticated judgment."""

from okto_pulse.core.application.use_cases.authorization import PermissionRequirement, require_authorization
from okto_pulse.core.application.use_cases.base import EntityNotFoundError, commit
from okto_pulse.core.application.use_cases.mutation_permissions import entity_state
from okto_pulse.core.application.use_cases.spec_crud import _require_actor_board_spec


async def authorize(command, actor, uow, *, write):
    spec = await _require_actor_board_spec(uow, command.spec_id, actor, write=write)
    if spec.board_id != command.board_id:
        raise EntityNotFoundError("spec_not_found")
    for operation in ("spec.entity.read", "code_traceability.evidence.read"):
        await require_authorization(actor, PermissionRequirement(operation), uow=uow, board_id=command.board_id)
    if write:
        await require_authorization(actor, PermissionRequirement("spec.validation.submit",
            entity="spec", state=entity_state(spec)), uow=uow, board_id=command.board_id)


class GetDecisionReviewsUseCase:
    async def execute(self, command, *, actor, uow):
        await authorize(command, actor, uow, write=False)
        return await uow.services.delivery_evidence.decision_reviews(command, actor_id=actor.actor_id)


class RecordDecisionReviewsUseCase:
    async def execute(self, command, *, actor, uow):
        await authorize(command, actor, uow, write=True)
        try:
            result = await uow.services.delivery_evidence.record_decision_reviews(
                command, actor_id=actor.actor_id, actor_kind=actor.actor_kind)
            await commit(uow)
            return result
        except BaseException:
            await uow.rollback()
            raise
