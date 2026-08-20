from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsFoundationQuery,
    AnalyticsUtcWindow,
)
from okto_pulse.core.services.flow_health_read_model import (
    build_flow_health_projection,
)


NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _query() -> AnalyticsFoundationQuery:
    return AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:user-1",
        window=AnalyticsUtcWindow(NOW - timedelta(days=30), NOW),
        as_of=NOW,
    )


def _board(settings=None):
    return SimpleNamespace(id="board-1", settings=settings)


def _card(*, status="in_progress", archived=False, updated_at=None):
    return SimpleNamespace(
        id="card-1",
        status=status,
        archived=archived,
        updated_at=updated_at or NOW,
        spec_id=None,
    )


def _event(event_id, event_type, age_hours, **payload):
    return SimpleNamespace(
        id=event_id,
        event_type=event_type,
        payload_json=payload,
        occurred_at=NOW - timedelta(hours=age_hours),
    )


def test_card_episode_uses_governed_event_time_and_ignores_updated_at():
    projection = build_flow_health_projection(
        query=_query(),
        as_of=NOW,
        board=_board(),
        specs=(),
        cards=(_card(updated_at=NOW),),
        domain_events=(
            _event("created", "card.created", 200, card_id="card-1"),
            _event(
                "moved",
                "card.moved",
                80,
                card_id="card-1",
                from_status="not_started",
                to_status="in_progress",
            ),
        ),
    )

    assert projection.summary.stale == 1
    assert projection.items[0].current_episode.entry_event_id == "moved"
    assert projection.items[0].current_episode.age_seconds == 80 * 3600


def test_missing_or_contradictory_event_authority_never_guesses_health():
    missing = build_flow_health_projection(
        query=_query(),
        as_of=NOW,
        board=_board(),
        specs=(),
        cards=(_card(),),
        domain_events=(),
    )
    contradictory = build_flow_health_projection(
        query=_query(),
        as_of=NOW,
        board=_board(),
        specs=(),
        cards=(_card(status="done"),),
        domain_events=(_event("created", "card.created", 2, card_id="card-1"),),
    )

    assert missing.items[0].state.value == "unavailable"
    assert contradictory.items[0].state.value == "inconsistent"


def test_rejection_companion_preserves_actionable_rework_detail():
    projection = build_flow_health_projection(
        query=_query(),
        as_of=NOW,
        board=_board(),
        specs=(),
        cards=(_card(status="rejected"),),
        domain_events=(
            _event("created", "card.created", 10, card_id="card-1"),
            _event(
                "moved",
                "card.moved",
                2,
                card_id="card-1",
                from_status="not_started",
                to_status="rejected",
            ),
            _event(
                "rejected",
                "card.completion_rejected",
                1,
                card_id="card-1",
                cause_kind="completion_gate",
                cause_code="missing_evidence",
                cause_summary="Evidence is incomplete",
            ),
        ),
    )

    assert projection.items[0].current_episode.state.value == "rejected"
    assert projection.items[0].rework[0].rejection_code == "missing_evidence"


def test_cancelled_rows_are_excluded_and_policy_override_is_board_owned():
    projection = build_flow_health_projection(
        query=_query(),
        as_of=NOW,
        board=_board(
            {
                "flow_health": {
                    "version": 2,
                    "overrides": {"in_progress": 24},
                }
            }
        ),
        specs=(),
        cards=(_card(status="cancelled"),),
        domain_events=(),
    )

    assert projection.items == ()
    assert projection.exclusions.excluded_count == 1
    assert projection.policy.version == 2
    assert projection.policy.overrides[0].stale_hours == 24
