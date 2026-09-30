"""Read query policy through the existing scoped Board repository."""

from okto_pulse.core.application.use_cases.authorization import PermissionRequirement, require_authorization
from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.ports.kg_query_policy import KGQueryPolicy
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


class ReadKGQueryPolicyUseCase:
    async def execute(self, board_id: str, *, actor: ActorContext, uow: PulseUnitOfWork) -> KGQueryPolicy:
        await require_authorization(actor, PermissionRequirement("board.read"), uow=uow, board_id=board_id)
        board = await load_accessible_board(uow, board_id, actor)
        if board is None:
            raise EntityNotFoundError("board", board_id)
        return KGQueryPolicy.from_settings(getattr(board, "settings", None))
