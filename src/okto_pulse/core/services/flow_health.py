"""Pure policy, lifecycle, blocker and rework semantics for flow health."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    AnalyticsSourceAuthority,
    require_utc_datetime,
)
from okto_pulse.core.ports.flow_health import (
    FLOW_HEALTH_CONTRACT_VERSION,
    EffectiveFlowThreshold,
    FlowAuthorityState,
    FlowBlockerFact,
    FlowCurrentEpisode,
    FlowHealthItem,
    FlowHealthPolicy,
    FlowHealthProjection,
    FlowHealthState,
    FlowHealthSummary,
    FlowLifecycleEvent,
    FlowLifecycleState,
    FlowReworkEpisode,
    FlowSubjectRef,
    FlowThresholdProvenance,
)


@dataclass(frozen=True, slots=True)
class FlowSubjectFacts:
    subject: FlowSubjectRef
    authority_state: FlowAuthorityState
    source_authority: AnalyticsSourceAuthority
    events: tuple[FlowLifecycleEvent, ...] = ()
    blockers: tuple[FlowBlockerFact, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.subject, FlowSubjectRef):
            raise ValueError("flow_health_subject_facts_subject_invalid")
        if not isinstance(self.authority_state, FlowAuthorityState):
            raise ValueError("flow_health_subject_authority_state_invalid")
        if not isinstance(self.source_authority, AnalyticsSourceAuthority):
            raise ValueError("flow_health_subject_source_authority_invalid")
        if not isinstance(self.events, tuple) or any(
            not isinstance(item, FlowLifecycleEvent) for item in self.events
        ):
            raise ValueError("flow_health_subject_events_invalid")
        if not isinstance(self.blockers, tuple) or any(
            not isinstance(item, FlowBlockerFact) for item in self.blockers
        ):
            raise ValueError("flow_health_subject_blockers_invalid")
        if self.authority_state is not FlowAuthorityState.CURRENT and (
            self.events or self.blockers
        ):
            raise ValueError("flow_health_nonavailable_subject_leaks_facts")


class FlowHealthPolicyResolver:
    """Resolve Board-owned stale thresholds without request-level overrides."""

    @staticmethod
    def threshold(
        policy: FlowHealthPolicy, state: FlowLifecycleState
    ) -> EffectiveFlowThreshold:
        if not isinstance(policy, FlowHealthPolicy):
            raise ValueError("flow_health_policy_invalid")
        if not isinstance(state, FlowLifecycleState):
            raise ValueError("flow_health_threshold_state_invalid")
        if state in {FlowLifecycleState.CANCELLED, FlowLifecycleState.ARCHIVED}:
            raise ValueError("flow_health_inactive_state_has_no_threshold")
        by_state = {item.state: item.stale_hours for item in policy.overrides}
        if state in by_state:
            hours = by_state[state]
            provenance = FlowThresholdProvenance.OVERRIDE
        elif state is FlowLifecycleState.REJECTED:
            hours = policy.rejected_stale_hours
            provenance = FlowThresholdProvenance.DEFAULT
        else:
            hours = policy.general_stale_hours
            provenance = FlowThresholdProvenance.DEFAULT
        return EffectiveFlowThreshold(
            state=state,
            stale_hours=hours,
            provenance=provenance,
            policy_version=policy.version,
            authority_ref=policy.authority_ref,
        )


class FlowHealthService:
    """Build deterministic actor-scoped flow facts from native authorities."""

    @staticmethod
    def _ordered_events(facts: FlowSubjectFacts) -> tuple[FlowLifecycleEvent, ...]:
        events = facts.events
        if not events:
            raise ValueError("flow_health_lifecycle_authority_missing")
        if len({item.event_id for item in events}) != len(events):
            raise ValueError("flow_health_lifecycle_event_duplicate")
        if tuple(sorted(events, key=lambda item: item.sequence)) != events:
            raise ValueError("flow_health_lifecycle_events_out_of_order")
        for index, event in enumerate(events):
            if event.subject != facts.subject:
                raise ValueError("flow_health_lifecycle_subject_mismatch")
            if index and event.sequence != events[index - 1].sequence + 1:
                raise ValueError("flow_health_lifecycle_sequence_gap")
            if index and event.from_state is not events[index - 1].to_state:
                raise ValueError("flow_health_lifecycle_chain_inconsistent")
            if index and event.occurred_at < events[index - 1].occurred_at:
                raise ValueError("flow_health_lifecycle_time_regressed")
        return events

    @staticmethod
    def current_episode(
        facts: FlowSubjectFacts, *, as_of: datetime
    ) -> FlowCurrentEpisode:
        observed_at = require_utc_datetime(as_of, field="flow_health_episode_as_of")
        current = FlowHealthService._ordered_events(facts)[-1]
        if current.occurred_at > observed_at:
            raise ValueError("flow_health_lifecycle_event_after_as_of")
        return FlowCurrentEpisode(
            state=current.to_state,
            entered_at=current.occurred_at,
            age_seconds=int((observed_at - current.occurred_at).total_seconds()),
            entry_event_id=current.event_id,
            authority_ref=current.authority_ref,
        )

    @staticmethod
    def rework_episodes(facts: FlowSubjectFacts) -> tuple[FlowReworkEpisode, ...]:
        episodes: list[FlowReworkEpisode] = []
        for event in FlowHealthService._ordered_events(facts):
            if event.to_state is FlowLifecycleState.REJECTED:
                episodes.append(
                    FlowReworkEpisode(
                        attempt=len(episodes) + 1,
                        rejected_at=event.occurred_at,
                        rejection_event_id=event.event_id,
                        rejection_kind=event.rejection_kind or "",
                        rejection_code=event.rejection_code or "",
                        rejection_summary=event.rejection_summary or "",
                    )
                )
                continue
            if not episodes:
                continue
            current = episodes[-1]
            if (
                event.to_state is FlowLifecycleState.IN_PROGRESS
                and current.resumed_at is None
            ):
                episodes[-1] = replace(current, resumed_at=event.occurred_at)
            elif (
                event.to_state is FlowLifecycleState.DONE
                and current.resumed_at is not None
                and current.completed_at is None
            ):
                episodes[-1] = replace(current, completed_at=event.occurred_at)
        return tuple(episodes)

    @staticmethod
    def _nonavailable_item(facts: FlowSubjectFacts) -> FlowHealthItem:
        state_by_authority = {
            FlowAuthorityState.INCONSISTENT: FlowHealthState.INCONSISTENT,
            FlowAuthorityState.MISSING: FlowHealthState.UNAVAILABLE,
            FlowAuthorityState.STALE: FlowHealthState.UNAVAILABLE,
        }
        state = state_by_authority[facts.authority_state]
        return FlowHealthItem(
            subject=facts.subject,
            state=state,
            reason_codes=(f"lifecycle_authority_{facts.authority_state.value}",),
            threshold=None,
            current_episode=None,
            rework=(),
            blockers=(),
            source_authority=facts.source_authority,
        )

    @staticmethod
    def item(
        facts: FlowSubjectFacts,
        *,
        policy: FlowHealthPolicy,
        as_of: datetime,
    ) -> FlowHealthItem | None:
        if facts.authority_state is FlowAuthorityState.RESTRICTED:
            return None
        if facts.authority_state is not FlowAuthorityState.CURRENT:
            return FlowHealthService._nonavailable_item(facts)
        episode = FlowHealthService.current_episode(facts, as_of=as_of)
        if episode.state in {FlowLifecycleState.CANCELLED, FlowLifecycleState.ARCHIVED}:
            return None
        threshold = FlowHealthPolicyResolver.threshold(policy, episode.state)
        reasons: set[str] = set()
        blocker_rows: list[FlowBlockerFact] = []
        active_blockers: list[FlowBlockerFact] = []
        unavailable = False
        inconsistent = False
        restricted = False
        for blocker in facts.blockers:
            if blocker.authority_state is FlowAuthorityState.RESTRICTED:
                restricted = True
            elif blocker.authority_state is FlowAuthorityState.INCONSISTENT:
                inconsistent = True
            elif blocker.authority_state in {
                FlowAuthorityState.MISSING,
                FlowAuthorityState.STALE,
            }:
                unavailable = True
            else:
                blocker_rows.append(blocker)
                if not blocker.effective_skip:
                    active_blockers.append(blocker)
                    reasons.add(blocker.code.value)
        if restricted:
            state = FlowHealthState.RESTRICTED
            reasons.add("blocker_authority_restricted")
        elif inconsistent:
            state = FlowHealthState.INCONSISTENT
            reasons.add("blocker_authority_inconsistent")
        elif unavailable:
            state = FlowHealthState.UNAVAILABLE
            reasons.add("blocker_authority_unavailable")
        elif active_blockers:
            state = FlowHealthState.BLOCKED
        elif episode.age_seconds >= threshold.stale_hours * 3600:
            state = FlowHealthState.STALE
            reasons.add("stale_current_episode")
        elif episode.age_seconds * 5 >= threshold.stale_hours * 3600 * 4:
            state = FlowHealthState.AT_RISK
            reasons.add("approaching_stale_threshold")
        else:
            state = FlowHealthState.HEALTHY
        if state in {
            FlowHealthState.RESTRICTED,
            FlowHealthState.UNAVAILABLE,
            FlowHealthState.INCONSISTENT,
        }:
            return FlowHealthItem(
                subject=facts.subject,
                state=state,
                reason_codes=tuple(sorted(reasons)),
                threshold=None,
                current_episode=None,
                rework=(),
                blockers=(),
                source_authority=facts.source_authority,
            )
        return FlowHealthItem(
            subject=facts.subject,
            state=state,
            reason_codes=tuple(sorted(reasons)),
            threshold=threshold,
            current_episode=episode,
            rework=FlowHealthService.rework_episodes(facts),
            blockers=tuple(sorted(blocker_rows, key=lambda item: item.code.value)),
            source_authority=facts.source_authority,
        )

    @staticmethod
    def projection(
        *,
        query: AnalyticsFoundationQuery,
        as_of: datetime,
        policy: FlowHealthPolicy,
        population_scope: AnalyticsPopulationScope,
        exclusions: AnalyticsExclusionSummary,
        subjects: tuple[FlowSubjectFacts, ...],
        next_cursor: str | None = None,
    ) -> FlowHealthProjection:
        observed_at = require_utc_datetime(as_of, field="flow_health_projection_as_of")
        if query.as_of is not None and query.as_of != observed_at:
            raise ValueError("flow_health_as_of_mismatch")
        if population_scope.scope_ref != query.actor_scope_ref:
            raise ValueError("flow_health_population_scope_mismatch")
        if not isinstance(subjects, tuple) or any(
            not isinstance(item, FlowSubjectFacts) for item in subjects
        ):
            raise ValueError("flow_health_subjects_invalid")
        identities = tuple(item.subject for item in subjects)
        if len(set(identities)) != len(identities):
            raise ValueError("flow_health_subject_duplicate")
        items = tuple(
            sorted(
                (
                    item
                    for facts in subjects
                    if (
                        item := FlowHealthService.item(
                            facts, policy=policy, as_of=observed_at
                        )
                    )
                    is not None
                ),
                key=lambda item: item.subject.sort_key,
            )
        )
        counts = {state: 0 for state in FlowHealthState}
        for item in items:
            counts[item.state] += 1
        return FlowHealthProjection(
            contract_version=FLOW_HEALTH_CONTRACT_VERSION,
            foundation_version=ANALYTICS_FOUNDATION_CONTRACT_VERSION,
            query_fingerprint=query.fingerprint,
            filters=query.filters,
            as_of=observed_at,
            policy=policy,
            population_scope=population_scope,
            exclusions=exclusions,
            summary=FlowHealthSummary(
                healthy=counts[FlowHealthState.HEALTHY],
                at_risk=counts[FlowHealthState.AT_RISK],
                blocked=counts[FlowHealthState.BLOCKED],
                stale=counts[FlowHealthState.STALE],
                restricted=exclusions.restricted_count,
                unavailable=counts[FlowHealthState.UNAVAILABLE],
                inconsistent=counts[FlowHealthState.INCONSISTENT],
            ),
            items=items,
            next_cursor=next_cursor,
        )


__all__ = [
    "FlowHealthPolicyResolver",
    "FlowHealthService",
    "FlowSubjectFacts",
]
