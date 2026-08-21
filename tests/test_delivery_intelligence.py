from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.delivery_intelligence import (
    DeliveryIntelligenceCommand,
    DeliveryIntelligenceUseCase,
)
from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsFilterClause,
    AnalyticsUtcWindow,
)


NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)


def _command(**overrides) -> DeliveryIntelligenceCommand:
    values = {
        "board_id": "board-1",
        "window": AnalyticsUtcWindow(NOW - timedelta(days=30), NOW),
        "as_of": NOW,
        "filters": (
            AnalyticsFilterClause("lane", "in", ("normal", "hotfix")),
            AnalyticsFilterClause("contribution_view", "eq", "self_and_aggregates"),
        ),
        "cursor": "offset:25",
        "limit": 25,
        "minimum_sample_size": 5,
    }
    values.update(overrides)
    return DeliveryIntelligenceCommand(**values)


def test_delivery_intelligence_command_uses_bounded_deterministic_cursor() -> None:
    command = _command()

    assert command.cursor_offset == 25
    assert command.limit == 25

    with pytest.raises(ValueError, match="cursor_invalid"):
        _command(cursor="page:2")
    with pytest.raises(ValueError, match="limit_invalid"):
        _command(limit=101)


@pytest.mark.asyncio
async def test_delivery_intelligence_use_case_preserves_actor_scope_and_filters() -> (
    None
):
    calls: list[dict[str, object]] = []

    class Boards:
        async def get(self, board_id: str):
            assert board_id == "board-1"
            return SimpleNamespace(id=board_id, owner_id="owner-1", realm_id=None)

    class Analytics:
        async def delivery_intelligence(self, **kwargs):
            calls.append(kwargs)
            return {"contract_version": "1", "result_state": "empty"}

    uow = SimpleNamespace(
        boards=Boards(),
        services=SimpleNamespace(analytics=Analytics(), shares=object()),
    )

    result = await DeliveryIntelligenceUseCase().execute(
        _command(),
        actor=ActorContext("owner-1", "rest", board_id="board-1"),
        uow=uow,
    )

    assert result.data["contract_version"] == "1"
    assert len(calls) == 1
    call = calls[0]
    assert call["actor_id"] == "owner-1"
    assert call["operator_visibility"] is True
    assert call["cursor_offset"] == 25
    assert call["minimum_sample_size"] == 5
    query = call["query"]
    assert query.actor_scope_ref == "actor:owner-1"
    assert [item.canonical_dict() for item in query.filters] == [
        {"field": "lane", "operator": "in", "value": ["normal", "hotfix"]},
        {
            "field": "contribution_view",
            "operator": "eq",
            "value": "self_and_aggregates",
        },
    ]


@pytest.mark.asyncio
async def test_delivery_intelligence_enforces_privacy_floor_per_aggregate_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.domain.enums import CardStatus, CardType
    from okto_pulse.core.ports.analytics_foundation import AnalyticsFoundationQuery
    from okto_pulse.core.services import analytics_service

    sprint_row = {
        "sprint_id": "sprint-1",
        "title": "Sprint 1",
        "status": "active",
        "lane_type": "normal",
        "done_cards": 5,
        "commitment": {
            "state": "available",
            "original_member_count": 5,
            "added_count": 0,
            "removed_count": 0,
        },
        "completed_committed_count": 5,
    }

    async def fake_sprints(*_args, **_kwargs):
        return {"sprints": [sprint_row]}

    cards = []
    for index in range(5):
        cards.append(
            SimpleNamespace(
                id=f"card-{index}",
                sprint_id="sprint-1",
                created_by="other-agent",
                status=CardStatus.DONE,
                card_type=CardType.NORMAL,
                created_at=NOW - timedelta(hours=2),
                updated_at=NOW,
                validations=(
                    [
                        {
                            "outcome": "success",
                            "reviewer_id": "other-agent",
                        }
                    ]
                    if index == 0
                    else []
                ),
            )
        )

    async def fake_list(_db, entity: str, **_kwargs):
        assert entity == "card"
        return cards

    monkeypatch.setattr(analytics_service, "compute_sprints_analytics", fake_sprints)
    monkeypatch.setattr(analytics_service, "_analytics_list", fake_list)
    query = AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:owner-1",
        window=AnalyticsUtcWindow(NOW - timedelta(days=30), NOW),
        filters=(
            AnalyticsFilterClause(
                "contribution_view", "eq", "self_and_aggregates"
            ),
        ),
        as_of=NOW,
    )

    payload = await analytics_service.compute_delivery_intelligence(
        object(),
        query=query,
        actor_id="owner-1",
        operator_visibility=False,
        minimum_sample_size=5,
    )

    aggregate = payload["contributions"][0]
    assert aggregate["visibility"] == "aggregate"
    assert aggregate["done_count"] == 5
    assert aggregate["median_cycle_hours"]["state"] == "available"
    assert aggregate["median_cycle_hours"]["sample_size"] == 5
    assert aggregate["first_pass"] == {
        "state": "restricted",
        "value": None,
        "numerator": None,
        "denominator": None,
        "sample_size": 0,
        "reason": "minimum_sample_not_met",
        "unit": "percent",
    }
    assert aggregate["validation_success"]["state"] == "restricted"
    assert aggregate["rework_introduced"] is None
    assert aggregate["rework_resolved"] is None
    assert payload["exclusions"] == {
        "restricted_count": 1,
        "excluded_count": 1,
        "reasons": [{"reason": "minimum_sample_not_met", "count": 1}],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("role_filter", "expected_role"),
    (
        ("implementation_agent", "Implementation agent"),
        ("validation_agent", "Validation agent"),
    ),
)
async def test_delivery_intelligence_filters_before_role_aggregation(
    monkeypatch: pytest.MonkeyPatch,
    role_filter: str,
    expected_role: str,
) -> None:
    from okto_pulse.core.domain.enums import CardStatus, CardType
    from okto_pulse.core.ports.analytics_foundation import AnalyticsFoundationQuery
    from okto_pulse.core.services import analytics_service

    async def fake_sprints(*_args, **_kwargs):
        return {
            "sprints": [
                {
                    "sprint_id": "sprint-1",
                    "title": "Sprint 1",
                    "status": "active",
                    "lane_type": "normal",
                    "done_cards": 5,
                    "commitment": {
                        "state": "available",
                        "original_member_count": 5,
                        "added_count": 0,
                        "removed_count": 0,
                    },
                    "completed_committed_count": 5,
                }
            ]
        }

    cards = [
        SimpleNamespace(
            id=f"card-{index}",
            sprint_id="sprint-1",
            created_by="implementation-agent",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            created_at=NOW - timedelta(hours=2),
            updated_at=NOW,
            validations=[
                {"outcome": "success", "reviewer_id": "validation-agent"}
            ],
        )
        for index in range(5)
    ]

    async def fake_list(_db, entity: str, **_kwargs):
        assert entity == "card"
        return cards

    monkeypatch.setattr(analytics_service, "compute_sprints_analytics", fake_sprints)
    monkeypatch.setattr(analytics_service, "_analytics_list", fake_list)
    query = AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:owner-1",
        window=AnalyticsUtcWindow(NOW - timedelta(days=30), NOW),
        filters=(
            AnalyticsFilterClause("role", "in", (role_filter,)),
            AnalyticsFilterClause("contribution_view", "eq", "aggregates"),
        ),
        as_of=NOW,
    )

    payload = await analytics_service.compute_delivery_intelligence(
        object(),
        query=query,
        actor_id="owner-1",
        operator_visibility=False,
        minimum_sample_size=5,
    )

    assert len(payload["contributions"]) == 1
    assert payload["contributions"][0]["role"] == expected_role


@pytest.mark.asyncio
async def test_sprint_commitment_counts_done_baseline_member_after_scope_removal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.domain.enums import (
        CardStatus,
        CardType,
        SprintLaneType,
        SprintStatus,
    )
    from okto_pulse.core.ports import sprint_activation_baseline as baseline_port
    from okto_pulse.core.ports.sprint_activation_baseline import (
        SprintActivationBaseline,
        SprintActivationMember,
    )
    from okto_pulse.core.services import analytics_service

    sprint = SimpleNamespace(
        id="sprint-1",
        board_id="board-1",
        spec_id="spec-1",
        title="Sprint 1",
        status=SprintStatus.ACTIVE,
        lane_type=SprintLaneType.NORMAL,
        created_at=NOW - timedelta(days=10),
        evaluations=[],
    )
    removed_done_card = SimpleNamespace(
        id="card-removed",
        sprint_id=None,
        created_by="agent-1",
        status=CardStatus.DONE,
        card_type=CardType.NORMAL,
        policy_version=1,
        created_at=NOW - timedelta(days=9),
        updated_at=NOW - timedelta(days=1),
        validations=[],
        conclusions=[],
    )
    baseline = SprintActivationBaseline(
        board_id="board-1",
        sprint_id="sprint-1",
        spec_id="spec-1",
        sprint_version=1,
        activated_at=NOW - timedelta(days=8),
        activated_by="agent-1",
        members=(SprintActivationMember("card-removed", "normal", 1),),
    )

    async def fake_list(_db, entity: str, **_kwargs):
        return {
            "sprint": [sprint],
            "card": [removed_done_card],
            "spec": [],
        }[entity]

    class BaselineStore:
        async def get(self, _context, *, board_id: str, sprint_id: str):
            assert (board_id, sprint_id) == ("board-1", "sprint-1")
            return baseline

    monkeypatch.setattr(analytics_service, "_analytics_list", fake_list)
    monkeypatch.setattr(
        baseline_port,
        "get_sprint_activation_baseline_store",
        lambda: BaselineStore(),
    )

    payload = await analytics_service.compute_sprints_analytics(object(), "board-1")

    row = payload["sprints"][0]
    assert row["total_cards"] == 0
    assert row["commitment"]["removed_count"] == 1
    assert row["completed_committed_count"] == 1
