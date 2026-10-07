"""Authorize every source family before reading a composed lineage snapshot."""
from dataclasses import dataclass
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.application.use_cases.authorization import PermissionRequirement, require_all
from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.ports.kg_query_policy import KGQueryPolicy
from okto_pulse.core.ports.lineage_query import LineageQuery


@dataclass(frozen=True, slots=True)
class LineageCommand:
    board_id: str
    subject_ref: str
    limit: int = 200
    cursor: str | None = None
    max_depth: int = 3
    timeout_ms: int | None = None


class LineageUseCase:
    async def execute(self, command, *, actor, uow: PulseUnitOfWork):
        board = await load_accessible_board(uow, command.board_id, actor)
        if board is None:
            raise EntityNotFoundError('board', command.board_id)
        await require_all(actor, *(PermissionRequirement(flag) for flag in (
            'board.read', 'spec.entity.read', 'card.entity.read', 'ideation.entity.read',
            'refinement.entity.read', 'story.entity.read', 'amendment.revision.read',
        )), uow=uow, board_id=command.board_id)
        query = LineageQuery(command.board_id, command.subject_ref,
            f'{actor.realm_id or LOCAL_REALM_ID}:{actor.actor_kind}:{actor.source}:{actor.actor_id}',
            command.limit, command.cursor, command.max_depth)
        policy = KGQueryPolicy.from_settings(getattr(board, 'settings', None))
        return await uow.services.analytics.lineage(query, timeout_ms=policy.effective_timeout(command.timeout_ms))
