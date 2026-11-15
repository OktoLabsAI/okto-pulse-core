"""Authenticated read preparation and human-only responsibility mutation."""
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement, require_authorization, resolve_actor_permissions, decide_authorization,
)
from okto_pulse.core.application.use_cases.base import PermissionDeniedError, commit


async def _read_authority(command, actor, uow):
    permissions = await resolve_actor_permissions(actor, uow=uow, board_id=command.board_id)
    for operation in ('spec.entity.read', 'card.entity.read', 'code_traceability.evidence.read'):
        await require_authorization(actor, PermissionRequirement(operation), permissions=permissions,
            uow=uow, board_id=command.board_id)
    return permissions


class GetTaskRequirementOptionsUseCase:
    async def execute(self, command, *, actor, uow):
        permissions = await _read_authority(command, actor, uow)
        allowed = []
        if actor.actor_kind in {'user', 'human'}:
            for decision, permission in [('waived', 'code_traceability.waiver.create'),
                                         ('optional', 'code_traceability.waiver.create'),
                                         ('required', 'code_traceability.waiver.clear')]:
                if decide_authorization(actor, PermissionRequirement(permission), permissions=permissions).allowed:
                    allowed.append(decision)
        return await uow.services.delivery_evidence.task_requirement_options(command,
            actor_id=actor.actor_id, allowed_decisions=tuple(allowed))


class RecordTaskRequirementDecisionUseCase:
    async def execute(self, command, *, actor, uow):
        if actor.actor_kind not in {'user', 'human'}:
            raise PermissionDeniedError('task_requirement_human_authorization_required')
        permissions = await _read_authority(command, actor, uow)
        operation = 'code_traceability.waiver.clear' if command.decision == 'required' else 'code_traceability.waiver.create'
        await require_authorization(actor, PermissionRequirement(operation), permissions=permissions,
            uow=uow, board_id=command.board_id)
        result = await uow.services.delivery_evidence.record_task_requirement_decision(command,
            actor_id=actor.actor_id, actor_kind=actor.actor_kind)
        await commit(uow)
        return result


class GetTaskRequirementHistoryUseCase:
    async def execute(self, query, *, actor, uow):
        await _read_authority(query, actor, uow)
        return await uow.services.delivery_evidence.task_requirement_history(query, actor_id=actor.actor_id)
