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
            AnalyticsFilterClause("contribution_view", "eq", "self_and_aggregates"),
        ),
        "cursor": "contributions-v2:offset:25",
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


def test_delivery_intelligence_command_rejects_unknown_or_ambiguous_filters() -> None:
    with pytest.raises(ValueError, match="filter_field_unsupported"):
        _command(filters=(AnalyticsFilterClause("unknown", "eq", "value"),))
    with pytest.raises(ValueError, match="filter_operator_unsupported"):
        _command(
            filters=(AnalyticsFilterClause("contribution_view", "ne", "self"),)
        )
    with pytest.raises(ValueError, match="contribution_view_ambiguous"):
        _command(
            filters=(
                AnalyticsFilterClause("contribution_view", "eq", "self"),
                AnalyticsFilterClause("contribution_view", "eq", "aggregates"),
            )
        )


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
            return {"contract_version": "2", "result_state": "empty"}

    uow = SimpleNamespace(
        boards=Boards(),
        services=SimpleNamespace(analytics=Analytics(), shares=object()),
    )

    result = await DeliveryIntelligenceUseCase().execute(
        _command(),
        actor=ActorContext("owner-1", "rest", board_id="board-1"),
        uow=uow,
    )

    assert result.data["contract_version"] == "2"
    assert len(calls) == 1
    call = calls[0]
    assert call["actor_id"] == "owner-1"
    assert call["operator_visibility"] is True
    assert call["cursor_offset"] == 25
    assert call["minimum_sample_size"] == 5
    query = call["query"]
    assert query.actor_scope_ref == "actor:owner-1"
    assert [item.canonical_dict() for item in query.filters] == [
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
@pytest.mark.parametrize(
    "wildcard_filters",
    (
        (AnalyticsFilterClause("role", "in", ("all",)),),
        (
            AnalyticsFilterClause("role", "eq", "ALL"),
        ),
    ),
)
async def test_delivery_intelligence_keeps_all_as_positive_filter_wildcard(
    monkeypatch: pytest.MonkeyPatch,
    wildcard_filters: tuple[AnalyticsFilterClause, ...],
) -> None:
    """``role=all`` must match every row, not the literal text."""

    from okto_pulse.core.domain.enums import CardStatus, CardType
    from okto_pulse.core.ports.analytics_foundation import AnalyticsFoundationQuery
    from okto_pulse.core.services import analytics_service


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

    monkeypatch.setattr(analytics_service, "_analytics_list", fake_list)

    async def _payload(filters: tuple[AnalyticsFilterClause, ...]):
        query = AnalyticsFoundationQuery(
            board_id="board-1",
            actor_scope_ref="actor:owner-1",
            window=AnalyticsUtcWindow(NOW - timedelta(days=30), NOW),
            filters=(
                *filters,
                AnalyticsFilterClause("contribution_view", "eq", "aggregates"),
            ),
            as_of=NOW,
        )
        return await analytics_service.compute_delivery_intelligence(
            object(),
            query=query,
            actor_id="owner-1",
            operator_visibility=False,
            minimum_sample_size=5,
        )

    unfiltered = await _payload(())
    wildcard = await _payload(wildcard_filters)

    # Only the echoed query metadata may differ; every projected value must be
    # identical to the unfiltered read.
    query_echo = {"filters", "query_fingerprint"}
    assert unfiltered["contributions"]
    assert {k: v for k, v in wildcard.items() if k not in query_echo} == {
        k: v for k, v in unfiltered.items() if k not in query_echo
    }

