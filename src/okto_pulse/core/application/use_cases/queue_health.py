"""queue_health use cases (SaaS Refactor spec R01A IMP4, REST read-only).

Behavior-preserving, transport-free reimplementation of the two read-only
consolidation-queue endpoints (``api/queue_health.py``):
``GET /api/v1/kg/queue/health`` and ``GET /api/v1/kg/queue/drilldown``. Both
delegate to the existing ``queue_health_service`` readers so the live snapshot /
drilldown payloads are byte-identical. Read-only: no commit. The public
signatures are transport-neutral — they never expose ``AsyncSession``/``get_db``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


_GLOBAL_QUEUE_READ_PERMISSIONS = (
    "queue.read.all",
    "kg.queue.read.all",
    "board.read.all",
    "boards.read.all",
    "board.global",
    "boards.global",
    "kg.admin.historical_consolidation",
)


class QueueBoardNotFoundError(EntityNotFoundError):
    def __init__(self, board_id: str) -> None:
        super().__init__("board", board_id)


def _permission_enabled(permissions: Any, required: str) -> bool:
    if isinstance(permissions, Mapping):
        if permissions.get("*") is True or permissions.get(required) is True:
            return True
        cursor: Any = permissions
        for part in required.split("."):
            if not isinstance(cursor, Mapping) or part not in cursor:
                return False
            cursor = cursor[part]
        return cursor is True
    checker = getattr(permissions, "check", None)
    if callable(checker):
        try:
            return checker(required) is None
        except Exception:
            return False
    if isinstance(permissions, (list, tuple, set, frozenset)):
        return required in permissions or "*" in permissions
    return False


def _require_global_queue_access(actor: ActorContext) -> None:
    roles = {str(role).lower() for role in actor.roles}
    if roles.intersection({"admin", "operator"}):
        return
    if any(
        _permission_enabled(actor.permissions, permission)
        for permission in _GLOBAL_QUEUE_READ_PERMISSIONS
    ):
        return
    raise PermissionDeniedError(
        "Global queue visibility requires an admin or operator capability"
    )


class GetQueueHealthCommand:
    """Input for :class:`GetQueueHealthUseCase` (no parameters)."""

    __slots__ = ()


class GetQueueHealthResult:
    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class GetQueueHealthUseCase:
    """Live consolidation-queue health snapshot, transport-free."""

    async def execute(
        self, command: GetQueueHealthCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetQueueHealthResult:
        _require_global_queue_access(actor)
        return GetQueueHealthResult(data=await uow.services.kg.queue_health())


class GetQueueDrilldownCommand:
    """Input for :class:`GetQueueDrilldownUseCase`."""

    __slots__ = ("board_id",)

    def __init__(self, board_id: str | None = None) -> None:
        self.board_id = board_id


class GetQueueDrilldownResult:
    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class GetQueueDrilldownUseCase:
    """Active-queue drilldown (board-scoped or global), transport-free."""

    async def execute(
        self, command: GetQueueDrilldownCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetQueueDrilldownResult:
        if command.board_id is None:
            _require_global_queue_access(actor)
        elif await load_accessible_board(uow, command.board_id, actor) is None:
            raise QueueBoardNotFoundError(command.board_id)
        return GetQueueDrilldownResult(
            data=await uow.services.kg.queue_drilldown(command.board_id)
        )
