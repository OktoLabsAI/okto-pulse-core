"""Authorize each closed Bug grouping before any source or graph aggregation."""
from dataclasses import dataclass

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.application.use_cases.authorization import PermissionRequirement, require_all
from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.ports.analytics_foundation import AnalyticsUtcWindow
from okto_pulse.core.ports.bug_clusters import BugClustersQuery, BugClusterGrouping
from okto_pulse.core.ports.kg_query_policy import KGQueryPolicy


@dataclass(frozen=True, slots=True)
class BugClustersCommand:
    board_id: str
    window: AnalyticsUtcWindow
    group_by: BugClusterGrouping = 'proxy'
    status: str | None = None
    severity: str | None = None
    limit: int = 200
    cursor: str | None = None
    timeout_ms: int | None = None


class BugClustersUseCase:
    async def execute(self, command, *, actor, uow: PulseUnitOfWork):
        board = await load_accessible_board(uow, command.board_id, actor)
        if board is None:
            raise EntityNotFoundError('board', command.board_id)
        flags = ['board.read', 'card.entity.read']
        if command.group_by in {'spec', 'proxy'}:
            flags.append('spec.entity.read')
        if command.group_by == 'proxy':
            flags.append('kg.query.related_context')
        if command.group_by == 'learning':
            flags.append('kg.query.learning_from_bugs')
        await require_all(actor, *(PermissionRequirement(flag) for flag in flags),
                          uow=uow, board_id=command.board_id)
        query = BugClustersQuery(command.board_id,
            f'{actor.realm_id or LOCAL_REALM_ID}:{actor.actor_kind}:{actor.source}:{actor.actor_id}',
            command.window, command.group_by, command.status, command.severity, command.limit, command.cursor)
        policy = KGQueryPolicy.from_settings(getattr(board, 'settings', None))
        return await uow.services.analytics.bug_clusters(query, timeout_ms=policy.effective_timeout(command.timeout_ms))
