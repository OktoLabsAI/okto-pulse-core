"""Analytics helper-endpoint use cases (SaaS Refactor spec R01A REST-FU2a).

Transport-free reimplementations of the four ``api/analytics.py`` endpoints whose
work is already a pure ``compute_*`` helper over the session — blockers, funnel,
velocity, coverage. Each guards board ownership via the transport-free
``board_is_owned_by`` reader (404 mapped by the adapter) and delegates to the
existing ``compute_*`` so the payload is byte-identical. Read-only — no commit.
Date parsing and the velocity granularity 400 stay in the REST adapter.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    session_of,
)


async def _ensure_board_owner(uow: Any, board_id: str, user_id: str):
    from okto_pulse.core.services.analytics_service import board_is_owned_by

    session = session_of(uow)
    if not await board_is_owned_by(session, board_id, user_id):
        raise EntityNotFoundError("board", board_id)
    return session


# --- blockers ---------------------------------------------------------------


class BoardBlockersCommand:
    __slots__ = ("board_id", "stale_hours", "filter_type")

    def __init__(
        self, board_id: str, *, stale_hours: int = 72, filter_type: str | None = None
    ) -> None:
        self.board_id = board_id
        self.stale_hours = stale_hours
        self.filter_type = filter_type


class BoardBlockersResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardBlockersUseCase:
    async def execute(
        self, command: BoardBlockersCommand, *, actor: ActorContext, uow: Any
    ) -> BoardBlockersResult:
        from okto_pulse.core.services.analytics_service import compute_blockers

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        return BoardBlockersResult(
            await compute_blockers(
                session,
                command.board_id,
                stale_hours=command.stale_hours,
                filter_type=command.filter_type,
            )
        )


# --- funnel -----------------------------------------------------------------


class BoardFunnelCommand:
    __slots__ = ("board_id", "dt_from", "dt_to")

    def __init__(
        self, board_id: str, *, dt_from: datetime | None = None, dt_to: datetime | None = None
    ) -> None:
        self.board_id = board_id
        self.dt_from = dt_from
        self.dt_to = dt_to


class BoardFunnelResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardFunnelUseCase:
    async def execute(
        self, command: BoardFunnelCommand, *, actor: ActorContext, uow: Any
    ) -> BoardFunnelResult:
        from okto_pulse.core.services.analytics_service import compute_funnel

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        return BoardFunnelResult(
            await compute_funnel(
                session, command.board_id, dt_from=command.dt_from, dt_to=command.dt_to
            )
        )


# --- velocity ---------------------------------------------------------------


class BoardVelocityCommand:
    __slots__ = ("board_id", "granularity", "weeks", "days", "dt_from", "dt_to")

    def __init__(
        self,
        board_id: str,
        *,
        granularity: str = "week",
        weeks: int = 12,
        days: int = 30,
        dt_from: datetime | None = None,
        dt_to: datetime | None = None,
    ) -> None:
        self.board_id = board_id
        self.granularity = granularity
        self.weeks = weeks
        self.days = days
        self.dt_from = dt_from
        self.dt_to = dt_to


class BoardVelocityResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardVelocityUseCase:
    async def execute(
        self, command: BoardVelocityCommand, *, actor: ActorContext, uow: Any
    ) -> BoardVelocityResult:
        from okto_pulse.core.services.analytics_service import compute_velocity

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        return BoardVelocityResult(
            await compute_velocity(
                session,
                command.board_id,
                granularity=command.granularity,
                weeks=command.weeks,
                days=command.days,
                dt_from=command.dt_from,
                dt_to=command.dt_to,
            )
        )


# --- coverage ---------------------------------------------------------------


class BoardCoverageCommand:
    __slots__ = ("board_id", "dt_from", "dt_to")

    def __init__(
        self, board_id: str, *, dt_from: datetime | None = None, dt_to: datetime | None = None
    ) -> None:
        self.board_id = board_id
        self.dt_from = dt_from
        self.dt_to = dt_to


class BoardCoverageResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardCoverageUseCase:
    async def execute(
        self, command: BoardCoverageCommand, *, actor: ActorContext, uow: Any
    ) -> BoardCoverageResult:
        from okto_pulse.core.services.analytics_service import compute_coverage

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        return BoardCoverageResult(
            await compute_coverage(
                session, command.board_id, dt_from=command.dt_from, dt_to=command.dt_to
            )
        )


# --- cross-board overview (REST-FU2b) ---------------------------------------


class AnalyticsOverviewCommand:
    __slots__ = ("dt_from", "dt_to")

    def __init__(
        self, *, dt_from: datetime | None = None, dt_to: datetime | None = None
    ) -> None:
        self.dt_from = dt_from
        self.dt_to = dt_to


class AnalyticsOverviewResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class AnalyticsOverviewUseCase:
    """Cross-board KPI overview for the actor's own boards (read-only). No board
    guard — ``compute_overview`` filters strictly by ``owner_id == actor``."""

    async def execute(
        self, command: AnalyticsOverviewCommand, *, actor: ActorContext, uow: Any
    ) -> AnalyticsOverviewResult:
        from okto_pulse.core.services.analytics_service import compute_overview

        return AnalyticsOverviewResult(
            await compute_overview(
                session_of(uow), actor.actor_id,
                dt_from=command.dt_from, dt_to=command.dt_to,
            )
        )


# --- board quality scatters (REST-FU2c) -------------------------------------


class BoardQualityCommand:
    __slots__ = ("board_id", "dt_from", "dt_to")

    def __init__(
        self, board_id: str, *, dt_from: datetime | None = None, dt_to: datetime | None = None
    ) -> None:
        self.board_id = board_id
        self.dt_from = dt_from
        self.dt_to = dt_to


class BoardQualityResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardQualityUseCase:
    async def execute(
        self, command: BoardQualityCommand, *, actor: ActorContext, uow: Any
    ) -> BoardQualityResult:
        from okto_pulse.core.services.analytics_service import compute_quality

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        return BoardQualityResult(
            await compute_quality(
                session, command.board_id, dt_from=command.dt_from, dt_to=command.dt_to
            )
        )


class BoardValidationsCommand:
    __slots__ = ("board_id", "dt_from", "dt_to")

    def __init__(
        self, board_id: str, *, dt_from: datetime | None = None, dt_to: datetime | None = None
    ) -> None:
        self.board_id = board_id
        self.dt_from = dt_from
        self.dt_to = dt_to


class BoardValidationsResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardValidationsUseCase:
    async def execute(
        self, command: BoardValidationsCommand, *, actor: ActorContext, uow: Any
    ) -> BoardValidationsResult:
        from okto_pulse.core.services.analytics_service import compute_validations

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        return BoardValidationsResult(
            await compute_validations(
                session, command.board_id, dt_from=command.dt_from, dt_to=command.dt_to
            )
        )


class BoardSpecAnalyticsCommand:
    __slots__ = ("board_id", "spec_id")

    def __init__(self, board_id: str, spec_id: str) -> None:
        self.board_id = board_id
        self.spec_id = spec_id


class BoardSpecAnalyticsResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardSpecAnalyticsUseCase:
    """Per-spec analytics (read). 404 "Board not found" then "Spec not found"."""

    async def execute(
        self, command: BoardSpecAnalyticsCommand, *, actor: ActorContext, uow: Any
    ) -> BoardSpecAnalyticsResult:
        from okto_pulse.core.services.analytics_service import compute_spec_analytics

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        data = await compute_spec_analytics(session, command.board_id, command.spec_id)
        if data is None:
            raise EntityNotFoundError("spec", command.spec_id)
        return BoardSpecAnalyticsResult(data)


class BoardSprintAnalyticsCommand:
    __slots__ = ("board_id", "sprint_id")

    def __init__(self, board_id: str, sprint_id: str) -> None:
        self.board_id = board_id
        self.sprint_id = sprint_id


class BoardSprintAnalyticsResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardSprintAnalyticsUseCase:
    """Per-sprint analytics (read). 404 "Board not found" then "Sprint not found"."""

    async def execute(
        self, command: BoardSprintAnalyticsCommand, *, actor: ActorContext, uow: Any
    ) -> BoardSprintAnalyticsResult:
        from okto_pulse.core.services.analytics_service import compute_sprint_analytics

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        data = await compute_sprint_analytics(session, command.board_id, command.sprint_id)
        if data is None:
            raise EntityNotFoundError("sprint", command.sprint_id)
        return BoardSprintAnalyticsResult(data)


# --- board sprints summary + agents (REST-FU2d) -----------------------------


class BoardSprintsAnalyticsCommand:
    __slots__ = ("board_id", "dt_from", "dt_to")

    def __init__(
        self, board_id: str, *, dt_from: datetime | None = None, dt_to: datetime | None = None
    ) -> None:
        self.board_id = board_id
        self.dt_from = dt_from
        self.dt_to = dt_to


class BoardSprintsAnalyticsResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardSprintsAnalyticsUseCase:
    async def execute(
        self, command: BoardSprintsAnalyticsCommand, *, actor: ActorContext, uow: Any
    ) -> BoardSprintsAnalyticsResult:
        from okto_pulse.core.services.analytics_service import compute_sprints_analytics

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        return BoardSprintsAnalyticsResult(
            await compute_sprints_analytics(
                session, command.board_id, dt_from=command.dt_from, dt_to=command.dt_to
            )
        )


class BoardAgentsCommand:
    __slots__ = ("board_id", "dt_from", "dt_to")

    def __init__(
        self, board_id: str, *, dt_from: datetime | None = None, dt_to: datetime | None = None
    ) -> None:
        self.board_id = board_id
        self.dt_from = dt_from
        self.dt_to = dt_to


class BoardAgentsResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardAgentsUseCase:
    async def execute(
        self, command: BoardAgentsCommand, *, actor: ActorContext, uow: Any
    ) -> BoardAgentsResult:
        from okto_pulse.core.services.analytics_service import compute_agents

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        return BoardAgentsResult(
            await compute_agents(
                session, command.board_id, dt_from=command.dt_from, dt_to=command.dt_to
            )
        )


# --- entity list (REST-FU2e; dispatch by type) ------------------------------


class BoardEntitiesCommand:
    __slots__ = ("board_id", "type", "offset", "limit", "search", "dt_from", "dt_to")

    def __init__(
        self,
        board_id: str,
        *,
        type: str,
        offset: int = 0,
        limit: int = 50,
        search: str = "",
        dt_from: datetime | None = None,
        dt_to: datetime | None = None,
    ) -> None:
        self.board_id = board_id
        self.type = type
        self.offset = offset
        self.limit = limit
        self.search = search
        self.dt_from = dt_from
        self.dt_to = dt_to


class BoardEntitiesResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardEntitiesUseCase:
    """Paginated entity list dispatched by ``type`` (read). Board 404 is checked
    first; an unknown type raises ``CommandValidationError`` (adapter → 400)."""

    async def execute(
        self, command: BoardEntitiesCommand, *, actor: ActorContext, uow: Any
    ) -> BoardEntitiesResult:
        from okto_pulse.core.application.use_cases.base import CommandValidationError
        from okto_pulse.core.services.analytics_service import (
            _list_card_entities,
            _list_ideation_entities,
            _list_spec_entities,
        )

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        readers = {
            "ideation": _list_ideation_entities,
            "spec": _list_spec_entities,
            "card": _list_card_entities,
        }
        reader = readers.get(command.type)
        if reader is None:
            raise CommandValidationError("type must be one of: ideation, spec, card")
        data = await reader(
            session, command.board_id, command.offset, command.limit,
            command.dt_from, command.dt_to, command.search,
        )
        return BoardEntitiesResult(data)


# --- entity detail (REST-FU2f; dispatch by entity_type) ---------------------


class BoardEntityDetailCommand:
    __slots__ = ("board_id", "entity_type", "entity_id")

    def __init__(self, board_id: str, entity_type: str, entity_id: str) -> None:
        self.board_id = board_id
        self.entity_type = entity_type
        self.entity_id = entity_id


class BoardEntityDetailResult:
    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class BoardEntityDetailUseCase:
    """Single-entity detail dispatched by ``entity_type`` (read). Board 404 first;
    unknown entity_type → ``CommandValidationError`` (400); a missing entity →
    ``EntityNotFoundError(entity_type)`` (adapter → 404 "<Type> not found")."""

    async def execute(
        self, command: BoardEntityDetailCommand, *, actor: ActorContext, uow: Any
    ) -> BoardEntityDetailResult:
        from okto_pulse.core.application.use_cases.base import CommandValidationError
        from okto_pulse.core.services.analytics_service import (
            _card_detail,
            _ideation_detail,
            _refinement_detail,
            _spec_detail,
            _sprint_detail,
        )

        session = await _ensure_board_owner(uow, command.board_id, actor.actor_id)
        readers = {
            "spec": _spec_detail,
            "ideation": _ideation_detail,
            "card": _card_detail,
            "refinement": _refinement_detail,
            "sprint": _sprint_detail,
        }
        reader = readers.get(command.entity_type)
        if reader is None:
            raise CommandValidationError(
                "entity_type must be one of: spec, ideation, card, refinement, sprint"
            )
        data = await reader(session, command.board_id, command.entity_id)
        if data is None:
            raise EntityNotFoundError(command.entity_type, command.entity_id)
        return BoardEntityDetailResult(data)
