from __future__ import annotations

from datetime import UTC, datetime

import pytest

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsFilterClause,
    AnalyticsFoundationProjection,
    AnalyticsFoundationQuery,
    AnalyticsUtcWindow,
)
from okto_pulse.core.ports.analytics_workspace import (
    AnalyticsPanelEnvelope,
    AnalyticsPanelState,
    AnalyticsSortClause,
    AnalyticsSortDirection,
    AnalyticsWorkspaceLevel,
    AnalyticsWorkspaceQuery,
)
from okto_pulse.core.services.analytics_workspace import AnalyticsWorkspaceService


def _foundation(*, actor: str = "actor:user-1") -> AnalyticsFoundationQuery:
    return AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref=actor,
        window=AnalyticsUtcWindow(
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 9, 1, tzinfo=UTC),
        ),
        filters=(
            AnalyticsFilterClause("status", "in", ("done", "approved")),
            AnalyticsFilterClause("type", "eq", "spec"),
        ),
        as_of=datetime(2026, 8, 20, 12, 30, tzinfo=UTC),
    )


def _query(*, cursor: str | None = "cursor-2") -> AnalyticsWorkspaceQuery:
    return AnalyticsWorkspaceQuery(
        foundation=_foundation(),
        level=AnalyticsWorkspaceLevel.ENTITY,
        entity_id="spec-7",
        sort=AnalyticsSortClause("cycle_time", AnalyticsSortDirection.DESC),
        cursor=cursor,
    )


def _projection(query: AnalyticsWorkspaceQuery) -> AnalyticsFoundationProjection:
    return AnalyticsFoundationProjection(
        contract_version=ANALYTICS_FOUNDATION_CONTRACT_VERSION,
        query_fingerprint=query.foundation.fingerprint,
        filters=query.foundation.filters,
        as_of=query.foundation.as_of,
        metrics=(),
    )


def test_workspace_url_round_trips_to_one_canonical_form() -> None:
    query = _query()
    encoded = AnalyticsWorkspaceService.to_query_string(query)
    restored = AnalyticsWorkspaceService.from_query_string(
        "?" + encoded,
        board_id=query.foundation.board_id,
        actor_scope_ref=query.foundation.actor_scope_ref,
    )

    assert AnalyticsWorkspaceService.to_query_string(restored) == encoded
    assert restored.foundation.board_id == query.foundation.board_id
    assert restored.foundation.actor_scope_ref == query.foundation.actor_scope_ref
    assert restored.foundation.window == query.foundation.window
    assert {item.field for item in restored.foundation.filters} == {"status", "type"}
    assert restored.foundation.as_of == query.foundation.as_of
    assert restored.level == query.level
    assert restored.entity_id == query.entity_id
    assert restored.sort == query.sort
    assert restored.cursor == query.cursor


@pytest.mark.parametrize(
    "raw,code",
    [
        ("level=board&from=x", "analytics_url_field_missing"),
        (
            "level=board&from=2026-08-01T00%3A00%3A00Z&to=2026-09-01T00%3A00%3A00Z"
            "&filters=%5B%5D&sort=metric_id%3Aasc&future=true",
            "analytics_url_field_unsupported",
        ),
        (
            "level=board&level=entity&from=2026-08-01T00%3A00%3A00Z"
            "&to=2026-09-01T00%3A00%3A00Z&filters=%5B%5D&sort=metric_id%3Aasc",
            "analytics_url_field_duplicate",
        ),
        (
            "level=board&from=2026-08-01T00%3A00%3A00Z"
            "&to=2026-09-01T00%3A00%3A00Z&filters=%7B%7D&sort=metric_id%3Aasc",
            "analytics_url_filters_invalid",
        ),
    ],
)
def test_workspace_url_rejects_invalid_state(raw: str, code: str) -> None:
    with pytest.raises(ValueError, match=code):
        AnalyticsWorkspaceService.from_query_string(
            raw, board_id="board-1", actor_scope_ref="actor:user-1"
        )


def test_logical_fingerprint_ignores_cursor_but_request_fingerprint_does_not() -> None:
    first = _query(cursor="cursor-1")
    second = _query(cursor="cursor-2")

    assert first.query_fingerprint == second.query_fingerprint
    assert first.request_fingerprint != second.request_fingerprint
    assert second.without_cursor().query_fingerprint == second.query_fingerprint


def test_actor_scope_is_bound_into_both_fingerprints() -> None:
    first = _query()
    other = AnalyticsWorkspaceQuery(
        foundation=_foundation(actor="actor:user-2"),
        level=first.level,
        entity_id=first.entity_id,
        sort=first.sort,
        cursor=first.cursor,
    )

    assert first.query_fingerprint != other.query_fingerprint
    assert first.request_fingerprint != other.request_fingerprint


def test_late_panel_response_cannot_overwrite_current_request() -> None:
    old_query = _query(cursor="old")
    new_query = _query(cursor="new")
    current = AnalyticsPanelEnvelope(
        panel_id="cycle_time",
        state=AnalyticsPanelState.LOADING,
        query_fingerprint=new_query.query_fingerprint,
        request_fingerprint=new_query.request_fingerprint,
        foundation_fingerprint=new_query.foundation.fingerprint,
    )
    late = AnalyticsPanelEnvelope(
        panel_id="cycle_time",
        state=AnalyticsPanelState.AVAILABLE,
        query_fingerprint=old_query.query_fingerprint,
        request_fingerprint=old_query.request_fingerprint,
        foundation_fingerprint=old_query.foundation.fingerprint,
        result=_projection(old_query),
    )

    assert (
        AnalyticsWorkspaceService.accept_panel_update(
            current=current,
            incoming=late,
            current_request_fingerprint=new_query.request_fingerprint,
        )
        is current
    )


def test_stale_panel_requires_last_result_as_of_error_and_retry() -> None:
    query = _query()
    result = _projection(query)

    stale = AnalyticsPanelEnvelope(
        panel_id="flow_health",
        state=AnalyticsPanelState.STALE,
        query_fingerprint=query.query_fingerprint,
        request_fingerprint=query.request_fingerprint,
        foundation_fingerprint=query.foundation.fingerprint,
        result=result,
        stale_as_of=result.as_of,
        retryable=True,
        error_code="analytics_source_stale",
    )
    assert stale.result is result

    with pytest.raises(ValueError, match="analytics_stale_panel_shape_invalid"):
        AnalyticsPanelEnvelope(
            panel_id="flow_health",
            state=AnalyticsPanelState.STALE,
            query_fingerprint=query.query_fingerprint,
            request_fingerprint=query.request_fingerprint,
            foundation_fingerprint=query.foundation.fingerprint,
            result=result,
            stale_as_of=result.as_of,
            retryable=False,
            error_code="analytics_source_stale",
        )


def test_workspace_projection_rejects_cross_query_panel() -> None:
    query = _query()
    other = _query(cursor="other")
    panel = AnalyticsPanelEnvelope(
        panel_id="cycle_time",
        state=AnalyticsPanelState.LOADING,
        query_fingerprint=query.query_fingerprint,
        request_fingerprint=other.request_fingerprint,
        foundation_fingerprint=query.foundation.fingerprint,
    )

    with pytest.raises(
        ValueError, match="analytics_workspace_panel_fingerprint_mismatch"
    ):
        AnalyticsWorkspaceService.projection(
            query=query,
            as_of=query.foundation.as_of,
            panels=(panel,),
        )


def test_canonical_projection_preserves_transport_parity_fields() -> None:
    query = _query(cursor=None)
    result = _projection(query)
    panel = AnalyticsPanelEnvelope(
        panel_id="cycle_time",
        state=AnalyticsPanelState.AVAILABLE,
        query_fingerprint=query.query_fingerprint,
        request_fingerprint=query.request_fingerprint,
        foundation_fingerprint=query.foundation.fingerprint,
        result=result,
    )
    projection = AnalyticsWorkspaceService.projection(
        query=query,
        as_of=query.foundation.as_of,
        panels=(panel,),
    )

    payload = projection.canonical_dict()
    assert payload["query_fingerprint"] == query.query_fingerprint
    assert payload["request_fingerprint"] == query.request_fingerprint
    assert payload["as_of"] == "2026-08-20T12:30:00.000000Z"
    assert payload["panels"][0]["result"] == result.canonical_dict()
    assert query.canonical_dict()["filters"] == [
        {"field": "status", "operator": "in", "value": ["done", "approved"]},
        {"field": "type", "operator": "eq", "value": "spec"},
    ]
