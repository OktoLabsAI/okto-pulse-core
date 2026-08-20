from datetime import UTC, datetime, timedelta

import pytest

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusion,
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    AnalyticsSourceAuthority,
    AnalyticsUtcWindow,
)
from okto_pulse.core.ports.flow_health import (
    FLOW_HEALTH_DEFAULT_GENERAL_STALE_HOURS,
    FLOW_HEALTH_DEFAULT_REJECTED_STALE_HOURS,
    FlowAuthorityState,
    FlowBlockerCode,
    FlowBlockerFact,
    FlowHealthPolicy,
    FlowHealthState,
    FlowLifecycleEvent,
    FlowLifecycleState,
    FlowPolicyOverride,
    FlowSubjectRef,
    FlowSubjectType,
    FlowThresholdProvenance,
)
from okto_pulse.core.services.flow_health import (
    FlowHealthPolicyResolver,
    FlowHealthService,
    FlowSubjectFacts,
)


NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _query() -> AnalyticsFoundationQuery:
    return AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:user-1",
        window=AnalyticsUtcWindow(NOW - timedelta(days=30), NOW + timedelta(seconds=1)),
        as_of=NOW,
    )


def _policy(*overrides: FlowPolicyOverride) -> FlowHealthPolicy:
    return FlowHealthPolicy(
        version=3,
        authority_ref="board-policy:3",
        overrides=tuple(sorted(overrides, key=lambda item: item.state.value)),
    )


def _subject(subject_id: str = "card-1") -> FlowSubjectRef:
    return FlowSubjectRef(FlowSubjectType.CARD, subject_id)


def _event(
    event_id: str,
    sequence: int,
    to_state: FlowLifecycleState,
    *,
    age_hours: int,
    from_state: FlowLifecycleState | None = None,
    subject: FlowSubjectRef | None = None,
) -> FlowLifecycleEvent:
    detail = (
        {
            "rejection_kind": "quality",
            "rejection_code": "missing_evidence",
            "rejection_summary": "Evidence is incomplete",
        }
        if to_state is FlowLifecycleState.REJECTED
        else {}
    )
    return FlowLifecycleEvent(
        event_id=event_id,
        subject=subject or _subject(),
        sequence=sequence,
        from_state=from_state,
        to_state=to_state,
        occurred_at=NOW - timedelta(hours=age_hours),
        authority_ref=f"lifecycle:{event_id}",
        **detail,
    )


def _facts(
    *events: FlowLifecycleEvent,
    blockers: tuple[FlowBlockerFact, ...] = (),
    authority_state: FlowAuthorityState = FlowAuthorityState.CURRENT,
    subject: FlowSubjectRef | None = None,
) -> FlowSubjectFacts:
    return FlowSubjectFacts(
        subject=subject or _subject(),
        authority_state=authority_state,
        source_authority=AnalyticsSourceAuthority(
            "governed_lifecycle", "lifecycle:board-1", "occurred_at"
        ),
        events=events,
        blockers=blockers,
    )


def test_default_and_override_threshold_policy_is_versioned():
    policy = _policy(FlowPolicyOverride(FlowLifecycleState.IN_PROGRESS, 24))

    pending = FlowHealthPolicyResolver.threshold(policy, FlowLifecycleState.PENDING)
    rejected = FlowHealthPolicyResolver.threshold(policy, FlowLifecycleState.REJECTED)
    progress = FlowHealthPolicyResolver.threshold(
        policy, FlowLifecycleState.IN_PROGRESS
    )

    assert pending.stale_hours == FLOW_HEALTH_DEFAULT_GENERAL_STALE_HOURS
    assert rejected.stale_hours == FLOW_HEALTH_DEFAULT_REJECTED_STALE_HOURS
    assert pending.provenance is FlowThresholdProvenance.DEFAULT
    assert progress.stale_hours == 24
    assert progress.provenance is FlowThresholdProvenance.OVERRIDE
    assert progress.policy_version == 3


@pytest.mark.parametrize("value", [0, -1, 1.5, True])
def test_invalid_threshold_hours_fail_closed(value):
    with pytest.raises(ValueError, match="policy_override_hours_invalid"):
        FlowPolicyOverride(FlowLifecycleState.PENDING, value)  # type: ignore[arg-type]


def test_current_episode_age_uses_latest_transition_not_created_or_updated_time():
    facts = _facts(
        _event("created", 1, FlowLifecycleState.PENDING, age_hours=200),
        _event(
            "started",
            2,
            FlowLifecycleState.IN_PROGRESS,
            age_hours=10,
            from_state=FlowLifecycleState.PENDING,
        ),
    )

    item = FlowHealthService.item(facts, policy=_policy(), as_of=NOW)

    assert item is not None
    assert item.current_episode is not None
    assert item.current_episode.entry_event_id == "started"
    assert item.current_episode.age_seconds == 10 * 3600
    assert item.state is FlowHealthState.HEALTHY


def test_rejected_progress_done_and_repeat_rejection_preserve_attempts():
    facts = _facts(
        _event("pending", 1, FlowLifecycleState.PENDING, age_hours=100),
        _event(
            "reject-1",
            2,
            FlowLifecycleState.REJECTED,
            age_hours=90,
            from_state=FlowLifecycleState.PENDING,
        ),
        _event(
            "resume-1",
            3,
            FlowLifecycleState.IN_PROGRESS,
            age_hours=80,
            from_state=FlowLifecycleState.REJECTED,
        ),
        _event(
            "done-1",
            4,
            FlowLifecycleState.DONE,
            age_hours=70,
            from_state=FlowLifecycleState.IN_PROGRESS,
        ),
        _event(
            "reject-2",
            5,
            FlowLifecycleState.REJECTED,
            age_hours=12,
            from_state=FlowLifecycleState.DONE,
        ),
    )

    episodes = FlowHealthService.rework_episodes(facts)

    assert tuple(item.attempt for item in episodes) == (1, 2)
    assert episodes[0].resumed_at is not None
    assert episodes[0].completed_at is not None
    assert episodes[1].resumed_at is None
    item = FlowHealthService.item(facts, policy=_policy(), as_of=NOW)
    assert item is not None and item.current_episode is not None
    assert item.current_episode.state is FlowLifecycleState.REJECTED
    assert episodes[1].rejection_code == "missing_evidence"


def test_current_validation_blocks_but_effective_skip_does_not_claim_coverage():
    blockers = (
        FlowBlockerFact(
            FlowBlockerCode.SPEC_PENDING_VALIDATION,
            FlowAuthorityState.CURRENT,
            "validation:current",
        ),
        FlowBlockerFact(
            FlowBlockerCode.UNCOVERED_TEST,
            FlowAuthorityState.CURRENT,
            "skip:effective",
            effective_skip=True,
        ),
    )
    facts = _facts(
        _event("pending", 1, FlowLifecycleState.PENDING, age_hours=1),
        blockers=blockers,
    )

    item = FlowHealthService.item(facts, policy=_policy(), as_of=NOW)

    assert item is not None
    assert item.state is FlowHealthState.BLOCKED
    assert tuple(blocker.code for blocker in item.blockers) == (
        FlowBlockerCode.SPEC_PENDING_VALIDATION,
        FlowBlockerCode.UNCOVERED_TEST,
    )
    assert item.blockers[1].effective_skip is True
    assert "uncovered_test" not in item.reason_codes


@pytest.mark.parametrize(
    ("authority_state", "expected"),
    [
        (FlowAuthorityState.MISSING, FlowHealthState.UNAVAILABLE),
        (FlowAuthorityState.STALE, FlowHealthState.UNAVAILABLE),
        (FlowAuthorityState.INCONSISTENT, FlowHealthState.INCONSISTENT),
    ],
)
def test_missing_contradictory_and_restricted_authority_never_guess_healthy(
    authority_state, expected
):
    item = FlowHealthService.item(
        _facts(authority_state=authority_state), policy=_policy(), as_of=NOW
    )

    assert item is not None
    assert item.state is expected
    assert item.current_episode is None
    assert not item.rework
    assert not item.blockers


def test_restricted_work_is_aggregate_only_and_never_leaks_subject_facts():
    restricted = _facts(authority_state=FlowAuthorityState.RESTRICTED)

    assert FlowHealthService.item(restricted, policy=_policy(), as_of=NOW) is None

    result = FlowHealthService.projection(
        query=_query(),
        as_of=NOW,
        policy=_policy(),
        population_scope=AnalyticsPopulationScope("actor:user-1", 0, 1),
        exclusions=AnalyticsExclusionSummary(
            restricted_count=1,
            excluded_count=1,
            reasons=(AnalyticsExclusion("restricted", 1),),
        ),
        subjects=(restricted,),
    )
    assert result.items == ()
    assert result.summary.restricted == 1


def test_cancelled_and_archived_work_are_excluded_from_active_projection():
    active = _facts(_event("active", 1, FlowLifecycleState.PENDING, age_hours=1))
    archived_subject = _subject("card-archived")
    archived = _facts(
        _event(
            "archived",
            1,
            FlowLifecycleState.ARCHIVED,
            age_hours=1,
            subject=archived_subject,
        ),
        subject=archived_subject,
    )

    result = FlowHealthService.projection(
        query=_query(),
        as_of=NOW,
        policy=_policy(),
        population_scope=AnalyticsPopulationScope("actor:user-1", 1, 1),
        exclusions=AnalyticsExclusionSummary(
            excluded_count=1,
            reasons=(AnalyticsExclusion("inactive_work", 1),),
        ),
        subjects=(active, archived),
    )

    assert tuple(item.subject.subject_id for item in result.items) == ("card-1",)
    assert result.summary.healthy == 1
    assert result.summary.total == 1
    assert result.canonical_dict()["query_fingerprint"] == _query().fingerprint


def test_lifecycle_gap_or_wrong_predecessor_is_inconsistent_not_silently_aged():
    facts = _facts(
        _event("pending", 1, FlowLifecycleState.PENDING, age_hours=10),
        _event(
            "done",
            3,
            FlowLifecycleState.DONE,
            age_hours=1,
            from_state=FlowLifecycleState.REJECTED,
        ),
    )

    with pytest.raises(ValueError, match="lifecycle_sequence_gap"):
        FlowHealthService.item(facts, policy=_policy(), as_of=NOW)


def test_board_totals_reconcile_to_actor_accessible_rows_and_echo_policy():
    subjects = (
        _facts(_event("healthy", 1, FlowLifecycleState.PENDING, age_hours=1)),
        _facts(
            _event(
                "stale",
                1,
                FlowLifecycleState.IN_PROGRESS,
                age_hours=80,
                subject=_subject("card-2"),
            ),
            subject=_subject("card-2"),
        ),
    )

    result = FlowHealthService.projection(
        query=_query(),
        as_of=NOW,
        policy=_policy(),
        population_scope=AnalyticsPopulationScope("actor:user-1", 2),
        exclusions=AnalyticsExclusionSummary(),
        subjects=subjects,
    )

    assert result.summary.healthy == 1
    assert result.summary.stale == 1
    assert result.policy.version == 3
    assert result.as_of == NOW
    assert result.population_scope.accessible_count == result.summary.total == 2
