from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusion,
    AnalyticsExclusionSummary,
    AnalyticsFilterClause,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    AnalyticsUtcWindow,
)
from okto_pulse.core.ports.policy_resource_readiness import (
    GovernanceCurrentness,
    PolicyAuthority,
    PolicyExceptionKind,
    PolicyReadinessFact,
    PolicyReadinessState,
    PolicyReadinessTotals,
    PolicyResourceReadinessRow,
    ResourceL1Fact,
    ResourceL1State,
    ResourceL2Fact,
    ResourceL2State,
    ResourceType,
)
from okto_pulse.core.services.policy_resource_readiness import (
    PolicyResourceReadinessService,
)


NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _query() -> AnalyticsFoundationQuery:
    return AnalyticsFoundationQuery(
        "board-1",
        "actor:user-1",
        AnalyticsUtcWindow(NOW - timedelta(days=30), NOW + timedelta(seconds=1)),
        (AnalyticsFilterClause("policy_state", "eq", "waived"),),
        NOW,
    )


def _exception(kind: PolicyExceptionKind = PolicyExceptionKind.WAIVER):
    return PolicyResourceReadinessService.exception_fact(
        kind=kind,
        authority_ref=f"{kind.value}-7",
        reason_code="accepted_risk",
        currentness=GovernanceCurrentness.CURRENT,
        effective_at=NOW - timedelta(days=2),
        as_of=NOW,
        expires_at=NOW + timedelta(days=5),
        impact_backlog_count=3,
    )


def test_policy_states_reconcile_without_counting_exceptions_as_native_pass() -> None:
    policies = (
        PolicyReadinessFact(
            "native",
            PolicyReadinessState.NATIVE_PASS,
            PolicyAuthority.BLOCKING,
            "a1",
            "e1",
        ),
        PolicyReadinessFact(
            "waived",
            PolicyReadinessState.WAIVED,
            PolicyAuthority.BLOCKING,
            "a2",
            "e2",
            exception=_exception(),
        ),
        PolicyReadinessFact(
            "skipped",
            PolicyReadinessState.SKIPPED,
            PolicyAuthority.ADVISORY,
            "a3",
            exception=_exception(PolicyExceptionKind.SKIP),
        ),
        PolicyReadinessFact(
            "pending",
            PolicyReadinessState.BLOCKING_PENDING,
            PolicyAuthority.BLOCKING,
            "a4",
        ),
        PolicyReadinessFact(
            "restricted",
            PolicyReadinessState.RESTRICTED,
            PolicyAuthority.ADVISORY,
        ),
    )
    totals = PolicyResourceReadinessService.policy_totals(policies)

    assert totals.native_pass == 1
    assert totals.waived == 1
    assert totals.skipped == 1
    assert totals.completed == 3
    assert totals.applicable == 4
    assert totals.restricted == 1


def test_waiver_age_expiry_reason_and_backlog_are_preserved() -> None:
    fact = _exception()

    assert fact.age_seconds == 172_800
    assert fact.expires_at == NOW + timedelta(days=5)
    assert fact.impact_backlog_count == 3
    assert fact.canonical_dict()["authority_ref"] == "waiver-7"


def test_stale_exception_requires_native_currentness_reason() -> None:
    with pytest.raises(
        ValueError, match="policy_resource_exception_currentness_reason_required"
    ):
        PolicyResourceReadinessService.exception_fact(
            kind=PolicyExceptionKind.WAIVER,
            authority_ref="waiver-7",
            reason_code="accepted_risk",
            currentness=GovernanceCurrentness.STALE,
            effective_at=NOW - timedelta(days=1),
            as_of=NOW,
        )


def test_native_pass_cannot_be_forged_from_a_waiver() -> None:
    with pytest.raises(
        ValueError, match="policy_resource_native_pass_exception_forbidden"
    ):
        PolicyReadinessFact(
            "policy-1",
            PolicyReadinessState.NATIVE_PASS,
            PolicyAuthority.BLOCKING,
            "assessment-1",
            exception=_exception(),
        )


def test_blocking_and_advisory_states_preserve_native_authority() -> None:
    with pytest.raises(
        ValueError, match="policy_resource_blocking_state_authority_mismatch"
    ):
        PolicyReadinessFact(
            "policy-1",
            PolicyReadinessState.BLOCKING_FAILED,
            PolicyAuthority.ADVISORY,
            "assessment-1",
        )
    with pytest.raises(
        ValueError, match="policy_resource_advisory_state_authority_mismatch"
    ):
        PolicyReadinessFact(
            "policy-2",
            PolicyReadinessState.ADVISORY_FAILED,
            PolicyAuthority.BLOCKING,
            "assessment-2",
        )


def test_expired_waiver_cannot_claim_current_authority() -> None:
    with pytest.raises(
        ValueError, match="policy_resource_exception_currentness_contradiction"
    ):
        PolicyResourceReadinessService.exception_fact(
            kind=PolicyExceptionKind.WAIVER,
            authority_ref="waiver-7",
            reason_code="accepted_risk",
            currentness=GovernanceCurrentness.CURRENT,
            effective_at=NOW - timedelta(days=2),
            expires_at=NOW - timedelta(seconds=1),
            as_of=NOW,
        )


def test_resource_l1_preserves_canonical_state_and_authority() -> None:
    provided = ResourceL1Fact(
        ResourceType.ARCHITECTURE,
        ResourceL1State.PROVIDED,
        PolicyAuthority.BLOCKING,
        "resource-authority-1",
        "architecture-7",
    )
    not_applicable = ResourceL1Fact(
        ResourceType.MOCKUP,
        ResourceL1State.NOT_APPLICABLE,
        PolicyAuthority.ADVISORY,
        "resource-authority-2",
        currentness_reason="not_required",
    )

    assert provided.canonical_dict()["evidence_ref"] == "architecture-7"
    assert not_applicable.canonical_dict()["authority"] == "advisory"


def test_restricted_resource_does_not_leak_or_zero_fill_evidence() -> None:
    restricted_l1 = ResourceL1Fact(
        ResourceType.KNOWLEDGE_BASE,
        ResourceL1State.RESTRICTED,
        PolicyAuthority.BLOCKING,
    )
    restricted_l2 = ResourceL2Fact(
        ResourceType.KNOWLEDGE_BASE,
        ResourceL2State.RESTRICTED,
        None,
        None,
        None,
    )

    assert restricted_l1.authority_ref is None
    assert restricted_l2.covered_only_by_cancelled_task is None
    assert restricted_l2.canonical_dict()["eligible_card_count"] is None


def test_cancelled_only_resource_never_satisfies_active_l2() -> None:
    result = PolicyResourceReadinessService.resource_l2_from_authority(
        resource_type=ResourceType.ARCHITECTURE,
        eligible_card_ids=("active-card",),
        covered_eligible_card_ids=(),
        cancelled_or_archived_card_ids=("cancelled-card",),
    )

    assert result.state is ResourceL2State.UNCOVERED
    assert result.covered_only_by_cancelled_task is True
    assert result.covered_card_count == 0


def test_one_eligible_active_link_satisfies_l2() -> None:
    result = PolicyResourceReadinessService.resource_l2_from_authority(
        resource_type=ResourceType.MOCKUP,
        eligible_card_ids=("active-1", "active-2"),
        covered_eligible_card_ids=("active-2",),
        cancelled_or_archived_card_ids=("archived-1",),
        evidence_refs=("mockup-link-1",),
    )

    assert result.state is ResourceL2State.COVERED
    assert result.covered_only_by_cancelled_task is False


def test_l2_rejects_cancelled_card_inside_active_population() -> None:
    with pytest.raises(
        ValueError, match="policy_resource_l2_population_binding_invalid"
    ):
        PolicyResourceReadinessService.resource_l2_from_authority(
            resource_type=ResourceType.MOCKUP,
            eligible_card_ids=("card-1",),
            covered_eligible_card_ids=(),
            cancelled_or_archived_card_ids=("card-1",),
        )


def test_projection_binds_foundation_population_and_read_only_rows() -> None:
    policies = (
        PolicyReadinessFact(
            "native",
            PolicyReadinessState.NATIVE_PASS,
            PolicyAuthority.BLOCKING,
            "a1",
            "e1",
        ),
    )
    row = PolicyResourceReadinessService.row(
        spec_id="spec-1",
        edition=7,
        policies=policies,
        resources_l1=tuple(
            ResourceL1Fact(
                resource_type,
                ResourceL1State.PROVIDED,
                PolicyAuthority.BLOCKING,
                f"resource-authority-{resource_type.value}",
                f"evidence-{resource_type.value}",
            )
            for resource_type in ResourceType
        ),
        resources_l2=tuple(
            PolicyResourceReadinessService.resource_l2_from_authority(
                resource_type=resource_type,
                eligible_card_ids=("card-1",),
                covered_eligible_card_ids=("card-1",),
                cancelled_or_archived_card_ids=(),
                evidence_refs=(f"evidence-{resource_type.value}",),
            )
            for resource_type in ResourceType
        ),
    )
    population = AnalyticsPopulationScope("actor:user-1", 1, 2)
    exclusions = AnalyticsExclusionSummary(
        restricted_count=2,
        excluded_count=2,
        reasons=(AnalyticsExclusion("permission_denied", 2),),
    )
    projection = PolicyResourceReadinessService.projection(
        query=_query(),
        as_of=NOW,
        population_scope=population,
        exclusions=exclusions,
        specs=(row,),
    )
    payload = projection.canonical_dict()

    assert projection.query_fingerprint == _query().fingerprint
    assert payload["foundation_version"] == "3"
    assert payload["specs"][0]["policy"]["totals"]["native_pass"] == 1
    assert not any(
        name.startswith(("assess", "waive", "skip", "mutate"))
        for name in projection.__dataclass_fields__
    )


def test_projection_rejects_population_that_differs_from_accessible_specs() -> None:
    with pytest.raises(ValueError, match="policy_resource_population_count_mismatch"):
        PolicyResourceReadinessService.projection(
            query=_query(),
            as_of=NOW,
            population_scope=AnalyticsPopulationScope("actor:user-1", 1, 0),
            exclusions=AnalyticsExclusionSummary(),
            specs=(),
        )


def test_row_rejects_missing_resource_authority() -> None:
    policies = (
        PolicyReadinessFact(
            "native",
            PolicyReadinessState.NATIVE_PASS,
            PolicyAuthority.BLOCKING,
            "a1",
            "e1",
        ),
    )
    with pytest.raises(ValueError, match="policy_resource_l1_authority_set_incomplete"):
        PolicyResourceReadinessService.row(
            spec_id="spec-1",
            edition=7,
            policies=policies,
            resources_l1=(),
            resources_l2=tuple(
                ResourceL2Fact(
                    resource_type,
                    ResourceL2State.NOT_APPLICABLE,
                    0,
                    0,
                    0,
                )
                for resource_type in ResourceType
            ),
        )


def test_row_rejects_forged_policy_totals() -> None:
    policy = PolicyReadinessFact(
        "native",
        PolicyReadinessState.NATIVE_PASS,
        PolicyAuthority.BLOCKING,
        "a1",
        "e1",
    )
    resources_l1 = tuple(
        ResourceL1Fact(
            resource_type,
            ResourceL1State.MISSING,
            PolicyAuthority.BLOCKING,
            f"authority-{resource_type.value}",
        )
        for resource_type in ResourceType
    )
    resources_l2 = tuple(
        ResourceL2Fact(
            resource_type,
            ResourceL2State.NOT_APPLICABLE,
            0,
            0,
            0,
        )
        for resource_type in ResourceType
    )
    zero = PolicyReadinessTotals(*(0 for _ in range(12)))

    with pytest.raises(ValueError, match="policy_resource_policy_totals_mismatch"):
        PolicyResourceReadinessRow(
            "spec-1",
            7,
            (policy,),
            zero,
            resources_l1,
            resources_l2,
        )
