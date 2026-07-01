"""queue_health use cases (SaaS Refactor spec R01A IMP4, REST read-only).

Behavior-preserving, transport-free reimplementation of the two read-only
consolidation-queue endpoints (``api/queue_health.py``):
``GET /api/v1/kg/queue/health`` and ``GET /api/v1/kg/queue/drilldown``. Both
delegate to the existing ``queue_health_service`` readers so the live snapshot /
drilldown payloads are byte-identical. Read-only: no commit. The public
signatures are transport-neutral — they never expose ``AsyncSession``/``get_db``.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import ActorContext, session_of
from okto_pulse.core.services.queue_health_service import (
    get_active_queue_drilldown,
    get_queue_health,
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
        self, command: GetQueueHealthCommand, *, actor: ActorContext, uow: Any
    ) -> GetQueueHealthResult:
        return GetQueueHealthResult(data=await get_queue_health(session_of(uow)))


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
        self, command: GetQueueDrilldownCommand, *, actor: ActorContext, uow: Any
    ) -> GetQueueDrilldownResult:
        return GetQueueDrilldownResult(
            data=await get_active_queue_drilldown(session_of(uow), command.board_id)
        )
