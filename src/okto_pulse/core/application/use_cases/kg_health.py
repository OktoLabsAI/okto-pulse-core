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

from typing import Any

from okto_pulse.core.application.use_cases.base import ActorContext, session_of
from okto_pulse.core.ports.scheduler import SchedulerControl


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
        self, command: GetKgHealthCommand, *, actor: ActorContext, uow: Any
    ) -> GetKgHealthResult:
        from okto_pulse.core.services.kg_health_service import get_kg_health

        data = await get_kg_health(
            command.board_id,
            session_of(uow),
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
        self, command: GetKgHealthReadinessCommand, *, actor: ActorContext, uow: Any
    ) -> GetKgHealthReadinessResult:
        from okto_pulse.core.services.kg_health_readiness_service import (
            build_health_readiness,
        )

        data = await build_health_readiness(
            command.board_id,
            session_of(uow),
            profile=command.profile,
            surface=command.surface,
            artifact_ref=command.artifact_ref,
            scheduler_control=command.scheduler_control,
        )
        return GetKgHealthReadinessResult(data=data)
