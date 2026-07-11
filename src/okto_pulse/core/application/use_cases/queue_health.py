"""queue_health use cases (SaaS Refactor spec R01A IMP4, REST read-only).

Behavior-preserving, transport-free reimplementation of the two read-only
consolidation-queue endpoints (``api/queue_health.py``):
``GET /api/v1/kg/queue/health`` and ``GET /api/v1/kg/queue/drilldown``. Both
delegate to the existing ``queue_health_service`` readers so the live snapshot /
drilldown payloads are byte-identical. Read-only: no commit. The public
signatures are transport-neutral — they never expose ``AsyncSession``/``get_db``.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import ActorContext


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
        return GetQueueDrilldownResult(
            data=await uow.services.kg.queue_drilldown(command.board_id)
        )
