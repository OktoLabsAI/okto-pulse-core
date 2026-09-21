"""Same-population contribution characterization, captured before F5 retirement."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from okto_pulse.core.domain.enums import CardStatus
from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsFilterClause, AnalyticsFoundationQuery, AnalyticsUtcWindow,
)

NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)


def cards():
    rows = []
    for actor, count in (("owner", 7), ("large", 6), ("small", 1)):
        for i in range(count):
            rows.append(SimpleNamespace(
                id=f"{actor}-{i}", sprint_id="legacy", created_by=actor,
                status=CardStatus.DONE, created_at=NOW - timedelta(hours=2+i),
                updated_at=NOW,
                validations=([
                    {"outcome": "failed", "reviewer_id": "reviewer"},
                    {"outcome": "success", "evaluator_id": "reviewer"},
                ] if i % 2 else [{"outcome": "pass", "reviewer_id": actor}]),
            ))
    return rows


def cases():
    for actor in ("owner", "absent"):
        for operator in (False, True):
            for view in ("self", "aggregates", "self_and_aggregates", "operator"):
                for role in ("all", "validation_agent", "implementation_+_validation"):
                    key = f"{actor}/{operator}/{view}/{role}"
                    query = AnalyticsFoundationQuery(
                        board_id="board-1", actor_scope_ref=f"actor:{actor}",
                        window=AnalyticsUtcWindow(NOW-timedelta(days=30), NOW),
                        filters=(AnalyticsFilterClause("contribution_view", "eq", view),
                                 AnalyticsFilterClause("role", "eq", role)), as_of=NOW,
                    )
                    yield key, dict(query=query, actor_id=actor, operator_visibility=operator)
