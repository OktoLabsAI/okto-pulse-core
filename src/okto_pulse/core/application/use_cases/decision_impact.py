"""Authorize Decision impact before collecting its bounded Spec scope."""
from dataclasses import dataclass
from okto_pulse.core.application.use_cases.authorization import PermissionRequirement, require_all
from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.ports.decision_impact import DecisionImpactQuery
from okto_pulse.core.ports.kg_query_policy import KGQueryPolicy


@dataclass(frozen=True, slots=True)
class DecisionImpactCommand:
    board_id: str
    spec_id: str
    decision_id: str
    limit: int = 200
    cursor: str | None = None
    max_depth: int = 3
    timeout_ms: int | None = None


class DecisionImpactUseCase:
    async def execute(self, command, *, actor, uow):
        board = await load_accessible_board(uow, command.board_id, actor)
        if board is None:
            raise EntityNotFoundError('board', command.board_id)
        await require_all(actor, *(PermissionRequirement(flag) for flag in (
            'board.read', 'spec.entity.read', 'card.entity.read', 'spec.tests.read',
            'spec.integration_requirements.read', 'spec.observability_requirements.read',
            'kg.query.related_context',
        )), uow=uow, board_id=command.board_id)
        query = DecisionImpactQuery(command.board_id, command.spec_id, command.decision_id,
            f'{actor.realm_id or LOCAL_REALM_ID}:{actor.actor_kind}:{actor.source}:{actor.actor_id}',
            command.limit, command.cursor, command.max_depth)
        policy = KGQueryPolicy.from_settings(getattr(board, 'settings', None))
        return await uow.services.analytics.decision_impact(query,
            timeout_ms=policy.effective_timeout(command.timeout_ms))
