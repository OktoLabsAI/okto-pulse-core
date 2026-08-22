from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsEvidenceCurrentness,
    AnalyticsExclusion,
    AnalyticsExclusionSummary,
    AnalyticsFilterClause,
    AnalyticsFoundationQuery,
    AnalyticsMetricFamily,
    AnalyticsPopulationScope,
    AnalyticsResultState,
    AnalyticsSourceAuthority,
    AnalyticsTemporalSemantics,
    AnalyticsUtcWindow,
)
from okto_pulse.core.services.analytics_foundation import (
    AnalyticsEvidenceBinding,
    AnalyticsFoundationService,
    AnalyticsLifecycleEvent,
    classify_evidence_currentness,
    governed_episode_duration,
)


UTC = timezone.utc


def _window() -> AnalyticsUtcWindow:
    return AnalyticsUtcWindow(
        datetime(2026, 8, 1, tzinfo=UTC),
        datetime(2026, 9, 1, tzinfo=UTC),
    )


def _filters() -> tuple[AnalyticsFilterClause, ...]:
    return (AnalyticsFilterClause("status", "in", ("done", "approved")),)


def _source() -> AnalyticsSourceAuthority:
    return AnalyticsSourceAuthority(
        "lifecycle_events", "board-events/v1", "occurred_at"
    )


def _population(*, accessible: int = 8, excluded: int = 2) -> AnalyticsPopulationScope:
    return AnalyticsPopulationScope("actor:principal-1", accessible, excluded)


def _exclusions(*, excluded: int = 2) -> AnalyticsExclusionSummary:
    return AnalyticsExclusionSummary(
        restricted_count=excluded,
        excluded_count=excluded,
        reasons=(AnalyticsExclusion("permission_denied", excluded),)
        if excluded
        else (),
    )


def _flow_semantics() -> AnalyticsTemporalSemantics:
    return AnalyticsTemporalSemantics(
        family=AnalyticsMetricFamily.EVENT_FLOW,
        authoritative_timestamp_field="occurred_at",
        window=_window(),
        event_source="card.lifecycle/v1",
    )


def test_zero_denominator_is_not_applicable_and_never_fabricates_value():
    metric = AnalyticsFoundationService.ratio_metric(
        metric_id="delivery.coverage",
        numerator=0,
        denominator=0,
        n=0,
        exclusions=_exclusions(excluded=0),
        as_of=datetime(2026, 8, 20, tzinfo=UTC),
        filters=_filters(),
        source=_source(),
        population_scope=_population(accessible=0, excluded=0),
        temporal_semantics=_flow_semantics(),
        zero_denominator_reason="no_applicable_obligation",
    )

    assert metric.state is AnalyticsResultState.NOT_APPLICABLE
    assert metric.numerator == metric.denominator == 0
    assert metric.value is None
    assert metric.n == 0


def test_utc_window_is_half_open_at_both_boundaries():
    window = _window()

    assert window.contains(window.from_inclusive) is True
    assert window.contains(window.to_exclusive - timedelta(microseconds=1)) is True
    assert window.contains(window.to_exclusive) is False


@pytest.mark.parametrize(
    "start,end,error",
    [
        (
            datetime(2026, 8, 1),
            datetime(2026, 9, 1, tzinfo=UTC),
            "analytics_window_from_must_be_timezone_aware_utc",
        ),
        (
            datetime(2026, 8, 1, tzinfo=timezone(timedelta(hours=1))),
            datetime(2026, 9, 1, tzinfo=UTC),
            "analytics_window_from_must_be_utc",
        ),
        (
            datetime(2026, 9, 1, tzinfo=UTC),
            datetime(2026, 9, 1, tzinfo=UTC),
            "analytics_temporal_query_invalid",
        ),
    ],
)
def test_invalid_temporal_queries_fail_before_projection(start, end, error):
    with pytest.raises(ValueError, match=error):
        AnalyticsUtcWindow(start, end)


def test_query_fingerprint_is_filter_order_independent_but_actor_bound():
    first = AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:a",
        window=_window(),
        filters=(
            AnalyticsFilterClause("type", "eq", "spec"),
            AnalyticsFilterClause("status", "in", ("done", "approved")),
        ),
    )
    reordered = AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:a",
        window=_window(),
        filters=tuple(reversed(first.filters)),
    )
    other_actor = AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:b",
        window=_window(),
        filters=first.filters,
    )

    assert first.fingerprint == reordered.fingerprint
    assert first.fingerprint != other_actor.fingerprint


def test_currentness_uses_subject_edition_result_and_authority_not_status():
    expected = AnalyticsEvidenceBinding("spec-1", "edition-7", "validation", "auth-7")
    same_edition_after_status_movement = AnalyticsEvidenceBinding(
        "spec-1", "edition-7", "validation", "auth-7"
    )
    previous = AnalyticsEvidenceBinding("spec-1", "edition-6", "validation", "auth-6")

    assert (
        classify_evidence_currentness(
            expected=expected,
            evidence=same_edition_after_status_movement,
        )
        is AnalyticsEvidenceCurrentness.CURRENT
    )
    assert (
        classify_evidence_currentness(
            expected=expected,
            evidence=previous,
            accepted_previous_authority_refs=frozenset({"auth-6"}),
        )
        is AnalyticsEvidenceCurrentness.PREVIOUS
    )
    assert (
        classify_evidence_currentness(expected=expected, evidence=None)
        is AnalyticsEvidenceCurrentness.MISSING
    )
    assert (
        classify_evidence_currentness(expected=expected, evidence=previous)
        is AnalyticsEvidenceCurrentness.STALE
    )


def test_flow_duration_uses_ordered_episode_events_only():
    events = (
        AnalyticsLifecycleEvent(
            "episode-1", "started", datetime(2026, 8, 2, 10, tzinfo=UTC)
        ),
        AnalyticsLifecycleEvent("other", "ended", datetime(2026, 8, 2, 11, tzinfo=UTC)),
        AnalyticsLifecycleEvent(
            "episode-1", "ended", datetime(2026, 8, 2, 13, tzinfo=UTC)
        ),
    )

    assert governed_episode_duration(
        events,
        episode_id="episode-1",
        start_event_type="started",
        end_event_type="ended",
    ) == timedelta(hours=3)


def test_restricted_metric_has_no_numeric_payload_or_identity_leak():
    metric = AnalyticsFoundationService.non_numeric_metric(
        metric_id="quality.pass_rate",
        family=AnalyticsMetricFamily.EVENT_FLOW,
        state=AnalyticsResultState.RESTRICTED,
        reason="population_not_authorized",
        exclusions=_exclusions(),
        as_of=datetime(2026, 8, 20, tzinfo=UTC),
        filters=_filters(),
        source=_source(),
        population_scope=_population(),
        temporal_semantics=_flow_semantics(),
    )

    assert (metric.numerator, metric.denominator, metric.value, metric.n) == (
        None,
        None,
        None,
        0,
    )
    assert metric.exclusions.reasons == (AnalyticsExclusion("permission_denied", 2),)


def test_projection_binds_contract_fingerprint_filters_window_and_cut():
    query = AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:a",
        window=_window(),
        filters=_filters(),
    )
    cut = datetime(2026, 8, 20, tzinfo=UTC)
    metric = AnalyticsFoundationService.ratio_metric(
        metric_id="delivery.coverage",
        numerator=6,
        denominator=8,
        n=8,
        exclusions=_exclusions(),
        as_of=cut,
        filters=query.filters,
        source=_source(),
        population_scope=_population(),
        temporal_semantics=_flow_semantics(),
        zero_denominator_reason="no_applicable_obligation",
    )

    result = AnalyticsFoundationService.projection(
        query=query,
        as_of=cut,
        metrics=(metric,),
    )

    assert result.contract_version == ANALYTICS_FOUNDATION_CONTRACT_VERSION == "3"
    assert result.query_fingerprint == query.fingerprint
    assert result.metrics[0].value == 0.75
    payload = result.canonical_dict()
    assert payload["as_of"] == "2026-08-20T00:00:00.000000Z"
    assert payload["metrics"][0]["state"] == "available"
    assert payload["metrics"][0]["filters"] == payload["filters"]


def test_snapshot_and_cohort_semantics_require_their_authorities():
    with pytest.raises(ValueError, match="analytics_snapshot_semantics_invalid"):
        AnalyticsTemporalSemantics(
            family=AnalyticsMetricFamily.SNAPSHOT,
            authoritative_timestamp_field="captured_at",
            window=_window(),
        )
    with pytest.raises(ValueError, match="analytics_cohort_semantics_invalid"):
        AnalyticsTemporalSemantics(
            family=AnalyticsMetricFamily.COHORT,
            authoritative_timestamp_field="joined_at",
            window=_window(),
            cohort_id="sprint-7",
        )
