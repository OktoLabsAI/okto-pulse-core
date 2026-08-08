"""kg health read use cases (SaaS Refactor spec R01A MCP-FU4, read-only).

Transport-free reimplementations of the two read-only KG health diagnostics
behind the MCP tools ``okto_pulse_kg_health`` and
``okto_pulse_kg_health_readiness`` (gemelar of REST ``GET /api/v1/kg/health`` and
``GET /api/v1/kg/health-readiness``). Each delegates to the existing
``kg_health_service`` / ``kg_health_readiness_service`` reader so the payload is
byte-identical to the legacy path; the service errors (``BoardNotFoundError`` /
``InvalidProfileError``) propagate unchanged for the adapter to map. Read-only: no
commit. The public signatures are transport-neutral — they never expose
``AsyncSession``/``get_db``, and the MCP-specific slim/full projection stays in the
adapter.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.errors import BoardNotFoundError
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.ports.scheduler import SchedulerControl


async def _require_health_board_access(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
) -> None:
    if await load_accessible_board(uow, board_id, actor) is None:
        raise BoardNotFoundError("Board not found")


class GetKgHealthCommand:
    """Input for :class:`GetKgHealthUseCase`."""

    __slots__ = ("board_id", "scheduler_control")

    def __init__(
        self,
        board_id: str,
        *,
        scheduler_control: SchedulerControl | None = None,
    ) -> None:
        self.board_id = board_id
        self.scheduler_control = scheduler_control


class GetKgHealthResult:
    """Output — the raw health mapping (pre any surface projection)."""

    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class GetKgHealthUseCase:
    """Board KG health snapshot, transport-free. ``BoardNotFoundError`` propagates."""

    async def execute(
        self, command: GetKgHealthCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetKgHealthResult:
        await _require_health_board_access(uow, command.board_id, actor)
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.health.read",
                legacy_operation="kg.admin.settings_read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        data = await uow.services.kg.health(
            command.board_id,
            scheduler_control=command.scheduler_control,
        )
        return GetKgHealthResult(data=data)


class GetKgHealthReadinessCommand:
    """Input for :class:`GetKgHealthReadinessUseCase`.

    ``surface`` is a request parameter of ``build_health_readiness`` (it tunes the
    non-maskable projection per caller); the adapter supplies its own value.
    """

    __slots__ = ("board_id", "profile", "artifact_ref", "surface", "scheduler_control")

    def __init__(
        self,
        board_id: str,
        *,
        profile: str = "summary",
        artifact_ref: str | None = None,
        surface: str = "mcp",
        scheduler_control: SchedulerControl | None = None,
    ) -> None:
        self.board_id = board_id
        self.profile = profile
        self.artifact_ref = artifact_ref
        self.surface = surface
        self.scheduler_control = scheduler_control


class GetKgHealthReadinessResult:
    """Output — the non-maskable readiness mapping."""

    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class GetKgHealthReadinessUseCase:
    """Non-maskable KG health/readiness, transport-free.

    ``InvalidProfileError`` / ``BoardNotFoundError`` propagate unchanged.
    """

    async def execute(
        self, command: GetKgHealthReadinessCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GetKgHealthReadinessResult:
        await _require_health_board_access(uow, command.board_id, actor)
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.health.read",
                legacy_operation="kg.admin.settings_read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        data = await uow.services.kg.health_readiness(
            command.board_id,
            profile=command.profile,
            surface=command.surface,
            artifact_ref=command.artifact_ref,
            scheduler_control=command.scheduler_control,
        )
        return GetKgHealthReadinessResult(data=data)
