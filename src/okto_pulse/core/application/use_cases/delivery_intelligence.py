"""Permission-scoped Delivery Intelligence application boundary."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsFilterClause,
    AnalyticsFoundationQuery,
    AnalyticsUtcWindow,
    require_utc_datetime,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


_CURSOR = re.compile(r"^offset:(0|[1-9][0-9]*)$")
_FILTER_OPERATORS = {
    "sprint_id": frozenset({"eq", "ne", "in", "not_in"}),
    "lane": frozenset({"eq", "ne", "in", "not_in"}),
    "role": frozenset({"eq", "ne", "in", "not_in"}),
    # contribution_view configures one projection shape rather than filtering
    # a population; negative/multi-value forms would be ambiguous.
    "contribution_view": frozenset({"eq"}),
}


@dataclass(frozen=True, slots=True)
class DeliveryIntelligenceCommand:
    board_id: str
    window: AnalyticsUtcWindow
    as_of: datetime
    filters: tuple[AnalyticsFilterClause, ...] = ()
    cursor: str | None = None
    limit: int = 50
    minimum_sample_size: int = 5

    def __post_init__(self) -> None:
        if not isinstance(self.board_id, str) or not self.board_id.strip():
            raise ValueError("delivery_intelligence_board_id_required")
        if not isinstance(self.window, AnalyticsUtcWindow):
            raise ValueError("delivery_intelligence_window_required")
        object.__setattr__(
            self,
            "as_of",
            require_utc_datetime(self.as_of, field="delivery_intelligence_as_of"),
        )
        if not isinstance(self.filters, tuple) or any(
            not isinstance(item, AnalyticsFilterClause) for item in self.filters
        ):
            raise ValueError("delivery_intelligence_filters_invalid")
        contribution_view_count = 0
        for clause in self.filters:
            allowed_operators = _FILTER_OPERATORS.get(clause.field)
            if allowed_operators is None:
                raise ValueError("delivery_intelligence_filter_field_unsupported")
            if clause.operator not in allowed_operators:
                raise ValueError("delivery_intelligence_filter_operator_unsupported")
            values = clause.value if isinstance(clause.value, tuple) else (clause.value,)
            if any(not isinstance(value, str) or not value.strip() for value in values):
                raise ValueError("delivery_intelligence_filter_value_invalid")
            if clause.field == "contribution_view":
                contribution_view_count += 1
        if contribution_view_count > 1:
            raise ValueError("delivery_intelligence_contribution_view_ambiguous")
        if self.cursor is not None and not _CURSOR.fullmatch(self.cursor):
            raise ValueError("delivery_intelligence_cursor_invalid")
        if isinstance(self.limit, bool) or not 1 <= self.limit <= 100:
            raise ValueError("delivery_intelligence_limit_invalid")
        if (
            isinstance(self.minimum_sample_size, bool)
            or not 2 <= self.minimum_sample_size <= 100
        ):
            raise ValueError("delivery_intelligence_minimum_sample_invalid")

    @property
    def cursor_offset(self) -> int:
        return int(self.cursor.split(":", 1)[1]) if self.cursor else 0


@dataclass(frozen=True, slots=True)
class DeliveryIntelligenceResult:
    data: dict[str, object]


class DeliveryIntelligenceUseCase:
    async def execute(
        self,
        command: DeliveryIntelligenceCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DeliveryIntelligenceResult:
        board = await load_accessible_board(uow, command.board_id, actor)
        if board is None:
            raise EntityNotFoundError("board", command.board_id)
        query = AnalyticsFoundationQuery(
            board_id=command.board_id,
            actor_scope_ref=f"actor:{actor.actor_id}",
            window=command.window,
            filters=command.filters,
            as_of=command.as_of,
        )
        payload = await uow.services.analytics.delivery_intelligence(
            query=query,
            actor_id=actor.actor_id,
            operator_visibility=getattr(board, "owner_id", None) == actor.actor_id,
            cursor_offset=command.cursor_offset,
            limit=command.limit,
            minimum_sample_size=command.minimum_sample_size,
        )
        return DeliveryIntelligenceResult(payload)


__all__ = [
    "DeliveryIntelligenceCommand",
    "DeliveryIntelligenceResult",
    "DeliveryIntelligenceUseCase",
]
