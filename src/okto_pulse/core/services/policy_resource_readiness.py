"""Pure composition helpers for policy and Resource Gate readiness."""

from __future__ import annotations

from datetime import datetime

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    require_utc_datetime,
)
from okto_pulse.core.ports.policy_resource_readiness import (
    POLICY_RESOURCE_READINESS_CONTRACT_VERSION,
    GovernanceCurrentness,
    PolicyExceptionFact,
    PolicyExceptionKind,
    PolicyReadinessFact,
    PolicyReadinessTotals,
    PolicyResourceReadinessProjection,
    PolicyResourceReadinessRow,
    ResourceL1Fact,
    ResourceL2Fact,
    ResourceL2State,
    ResourceType,
)


class PolicyResourceReadinessService:
    """Compose already-authoritative facts without exposing mutation seams."""

    @staticmethod
    def policy_totals(
        policies: tuple[PolicyReadinessFact, ...],
    ) -> PolicyReadinessTotals:
        return PolicyReadinessTotals.from_facts(policies)

    @staticmethod
    def exception_fact(
        *,
        kind: PolicyExceptionKind,
        authority_ref: str,
        reason_code: str,
        currentness: GovernanceCurrentness,
        effective_at: datetime,
        as_of: datetime,
        expires_at: datetime | None = None,
        currentness_reason: str | None = None,
        impact_backlog_count: int = 0,
    ) -> PolicyExceptionFact:
        effective = require_utc_datetime(
            effective_at, field="policy_exception_effective_at"
        )
        observed_at = require_utc_datetime(as_of, field="policy_exception_as_of")
        if observed_at < effective:
            raise ValueError("policy_resource_exception_as_of_precedes_effective")
        if expires_at is not None:
            expiry = require_utc_datetime(
                expires_at, field="policy_exception_expires_at"
            )
            if currentness is GovernanceCurrentness.CURRENT and expiry <= observed_at:
                raise ValueError("policy_resource_exception_currentness_contradiction")
        return PolicyExceptionFact(
            kind=kind,
            authority_ref=authority_ref,
            reason_code=reason_code,
            currentness=currentness,
            effective_at=effective,
            age_seconds=int((observed_at - effective).total_seconds()),
            expires_at=expires_at,
            currentness_reason=currentness_reason,
            impact_backlog_count=impact_backlog_count,
        )

    @staticmethod
    def resource_l2_from_authority(
        *,
        resource_type: ResourceType,
        eligible_card_ids: tuple[str, ...],
        covered_eligible_card_ids: tuple[str, ...],
        cancelled_or_archived_card_ids: tuple[str, ...],
        evidence_refs: tuple[str, ...] = (),
    ) -> ResourceL2Fact:
        populations = (
            eligible_card_ids,
            covered_eligible_card_ids,
            cancelled_or_archived_card_ids,
        )
        if any(
            not isinstance(values, tuple)
            or any(not isinstance(value, str) or not value for value in values)
            or len(set(values)) != len(values)
            for values in populations
        ):
            raise ValueError("policy_resource_l2_population_invalid")
        eligible = set(eligible_card_ids)
        covered = set(covered_eligible_card_ids)
        historical = set(cancelled_or_archived_card_ids)
        if not covered.issubset(eligible) or eligible.intersection(historical):
            raise ValueError("policy_resource_l2_population_binding_invalid")
        if covered:
            state = ResourceL2State.COVERED
        elif eligible:
            state = ResourceL2State.UNCOVERED
        else:
            state = ResourceL2State.NOT_APPLICABLE
        return ResourceL2Fact(
            resource_type=resource_type,
            state=state,
            eligible_card_count=len(eligible),
            covered_card_count=len(covered),
            cancelled_or_archived_link_count=len(historical),
            evidence_refs=evidence_refs if covered else (),
        )

    @staticmethod
    def row(
        *,
        spec_id: str,
        edition: int,
        policies: tuple[PolicyReadinessFact, ...],
        resources_l1: tuple[ResourceL1Fact, ...],
        resources_l2: tuple[ResourceL2Fact, ...],
    ) -> PolicyResourceReadinessRow:
        return PolicyResourceReadinessRow(
            spec_id=spec_id,
            edition=edition,
            policies=policies,
            policy_totals=PolicyResourceReadinessService.policy_totals(policies),
            resources_l1=resources_l1,
            resources_l2=resources_l2,
        )

    @staticmethod
    def projection(
        *,
        query: AnalyticsFoundationQuery,
        as_of: datetime,
        population_scope: AnalyticsPopulationScope,
        exclusions: AnalyticsExclusionSummary,
        specs: tuple[PolicyResourceReadinessRow, ...],
        next_cursor: str | None = None,
    ) -> PolicyResourceReadinessProjection:
        return PolicyResourceReadinessProjection(
            contract_version=POLICY_RESOURCE_READINESS_CONTRACT_VERSION,
            foundation_version=ANALYTICS_FOUNDATION_CONTRACT_VERSION,
            query_fingerprint=query.fingerprint,
            filters=query.filters,
            as_of=as_of,
            population_scope=population_scope,
            exclusions=exclusions,
            specs=specs,
            next_cursor=next_cursor,
        )


__all__ = ["PolicyResourceReadinessService"]
