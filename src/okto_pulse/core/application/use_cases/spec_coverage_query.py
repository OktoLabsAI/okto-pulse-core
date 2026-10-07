"""Authorize source coverage and proof visibility before reading any facts."""
from dataclasses import dataclass

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement, require_all, resolve_actor_permissions, decide_authorization,
)
from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.ports.kg_query_policy import KGQueryPolicy
from okto_pulse.core.ports.spec_coverage_query import SpecCoverageQuery


@dataclass(frozen=True, slots=True)
class SpecCoverageCommand:
    board_id: str
    spec_id: str
    limit: int = 200
    cursor: str | None = None
    timeout_ms: int | None = None


class SpecCoverageUseCase:
    async def execute(self, command, *, actor, uow: PulseUnitOfWork):
        board = await load_accessible_board(uow, command.board_id, actor)
        if board is None:
            raise EntityNotFoundError('board', command.board_id)
        await require_all(actor, *(PermissionRequirement(flag) for flag in (
            'board.read', 'spec.entity.read', 'card.entity.read', 'spec.tests.read',
            'spec.integration_requirements.read', 'spec.observability_requirements.read',
        )), uow=uow, board_id=command.board_id)
        permissions = await resolve_actor_permissions(actor, uow, command.board_id)
        read_delivery = decide_authorization(actor, PermissionRequirement('code_traceability.evidence.read'),
            permissions=permissions).allowed
        read_graph = decide_authorization(actor, PermissionRequirement('kg.query.related_context'),
            permissions=permissions).allowed
        query = SpecCoverageQuery(command.board_id, command.spec_id,
            f'{actor.realm_id or LOCAL_REALM_ID}:{actor.actor_kind}:{actor.source}:{actor.actor_id}',
            command.limit, command.cursor, read_delivery, read_graph)
        policy = KGQueryPolicy.from_settings(getattr(board, 'settings', None))
        return await uow.services.analytics.spec_coverage(query,
            timeout_ms=policy.effective_timeout(command.timeout_ms))
