from __future__ import annotations

from datetime import UTC, datetime

import pytest

from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceSpecRelationType,
    CodeEvidenceType,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityWaiverEntityType,
    CodeTraceabilityWaiverReason,
    CodeTraceabilityWaiverScope,
    ImplementationTargetExecutionDisposition,
    ImplementationTargetResolutionState,
)
from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusion,
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    AnalyticsUtcWindow,
)
from okto_pulse.core.ports.coverage_traceability import (
    CodeEvidenceExecutionFact,
    CodeEvidenceMatrixState,
    CodeEvidenceResolutionFact,
    CodeEvidenceTargetFact,
    CodeEvidenceWaiverFact,
    CoverageAggregateState,
    CoverageAuthorityState,
    CoverageCounts,
    CoverageCurrentness,
    CoverageDeliveryState,
    CoverageEvidenceEligibility,
    CoverageEvidenceFact,
    CoverageFactState,
    CoverageObligationFact,
    CoverageObligationIdentity,
    CoverageObligationType,
    CoverageSkipMetadata,
    CoverageSkipState,
    CoverageTypeProjection,
)
from okto_pulse.core.services.coverage_traceability import (
    CoverageTraceabilityService,
)


NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
WINDOW = AnalyticsUtcWindow(
    datetime(2026, 8, 1, tzinfo=UTC),
    datetime(2026, 9, 1, tzinfo=UTC),
)


def _query() -> AnalyticsFoundationQuery:
    return AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:user-1",
        window=WINDOW,
        as_of=NOW,
    )


def _identity(
    obligation_type: CoverageObligationType = CoverageObligationType.ACCEPTANCE_CRITERION,
    *,
    obligation_id: str | None = None,
) -> CoverageObligationIdentity:
    return CoverageObligationIdentity(
        spec_id="spec-1",
        obligation_type=obligation_type,
        obligation_id=obligation_id or f"{obligation_type.value}-1",
        edition=7,
        currentness=CoverageCurrentness.CURRENT,
    )


def _obligation(
    identity: CoverageObligationIdentity | None = None,
    *,
    applicable: bool = True,
    skip: CoverageSkipMetadata = CoverageSkipMetadata(),
) -> CoverageObligationFact:
    return CoverageObligationFact(
        identity=identity or _identity(),
        applicable=applicable,
        authority_state=CoverageAuthorityState.AVAILABLE,
        authority_ref="spec-entity-authority-7",
        skip=skip,
    )


def _evidence(
    identity: CoverageObligationIdentity | None = None,
    *,
    evidence_id: str = "evidence-1",
    delivery_state: CoverageDeliveryState = CoverageDeliveryState.ACTIVE,
    lifecycle_status: CodeTraceabilityLifecycleStatus = (
        CodeTraceabilityLifecycleStatus.ACTIVE
    ),
    currentness: CoverageCurrentness = CoverageCurrentness.CURRENT,
) -> CoverageEvidenceFact:
    return CoverageEvidenceFact(
        evidence_id=evidence_id,
        evidence_type=CodeEvidenceType.BEHAVIOR,
        source_ref="source-1",
        obligation=identity or _identity(),
        relation_type=CodeEvidenceSpecRelationType.IMPLEMENTS,
        evidence_content_sha256="a" * 64,
        parent_card_id="card-1",
        delivery_state=delivery_state,
        lifecycle_status=lifecycle_status,
        currentness=currentness,
        currentness_reason=(
            None
            if currentness is CoverageCurrentness.CURRENT
            else "prior_edition_evidence"
        ),
        authority_ref="code-evidence-link-1",
    )


def _matrix():
    target = CodeEvidenceTargetFact(
        target_id="target-1",
        card_id="card-1",
        source_ref="source-1",
        revision=3,
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        delivery_state=CoverageDeliveryState.ACTIVE,
        currentness=CoverageCurrentness.CURRENT,
        currentness_reason=None,
        current_resolution_id="resolution-1",
    )
    resolution = CodeEvidenceResolutionFact(
        resolution_id="resolution-1",
        target_id="target-1",
        target_revision=3,
        state=ImplementationTargetResolutionState.RESOLVED,
        currentness=CoverageCurrentness.CURRENT,
        currentness_reason=None,
        authority_ref="resolution-authority-1",
    )
    execution = CodeEvidenceExecutionFact(
        execution_id="execution-1",
        target_id="target-1",
        target_revision=3,
        disposition=ImplementationTargetExecutionDisposition.TOUCHED,
        currentness=CoverageCurrentness.CURRENT,
        currentness_reason=None,
        authority_ref="execution-authority-1",
    )
    return CoverageTraceabilityService.code_evidence_matrix(
        authority_state=CodeEvidenceMatrixState.AVAILABLE,
        targets=(target,),
        resolutions=(resolution,),
        executions=(execution,),
    )


def test_all_obligation_types_reconcile_to_board_totals() -> None:
    obligations = tuple(_obligation(_identity(kind)) for kind in CoverageObligationType)
    evidence = tuple(
        _evidence(
            fact.identity,
            evidence_id=f"evidence-{fact.identity.obligation_type.value}",
        )
        for fact in obligations
    )

    projection = CoverageTraceabilityService.projection(
        query=_query(),
        as_of=NOW,
        population_scope=AnalyticsPopulationScope("actor:user-1", 9),
        exclusions=AnalyticsExclusionSummary(),
        evidence_population_scope=AnalyticsPopulationScope("actor:user-1", 12),
        evidence_exclusions=AnalyticsExclusionSummary(),
        obligations=obligations,
        evidence=evidence,
        code_evidence=_matrix(),
    )

    assert projection.totals.applicable == 9
    assert projection.totals.covered == 9
    assert projection.totals.value == 100.0
    assert len(projection.coverage) == 9
    assert all(item.counts.covered == 1 for item in projection.coverage)
    assert projection.query_fingerprint == _query().fingerprint


def test_one_evidence_covers_only_each_explicit_canonical_link() -> None:
    acceptance = _identity(CoverageObligationType.ACCEPTANCE_CRITERION)
    requirement = _identity(CoverageObligationType.FUNCTIONAL_REQUIREMENT)
    projection = CoverageTraceabilityService.projection(
        query=_query(),
        as_of=NOW,
        population_scope=AnalyticsPopulationScope("actor:user-1", 2),
        exclusions=AnalyticsExclusionSummary(),
        evidence_population_scope=AnalyticsPopulationScope("actor:user-1", 5),
        evidence_exclusions=AnalyticsExclusionSummary(),
        obligations=(_obligation(acceptance), _obligation(requirement)),
        evidence=(
            _evidence(acceptance, evidence_id="shared-evidence"),
            _evidence(requirement, evidence_id="shared-evidence"),
        ),
        code_evidence=_matrix(),
    )

    assert projection.totals.covered == 2
    assert (
        sum(len(row.evidence) for group in projection.coverage for row in group.rows)
        == 2
    )


def test_skip_never_changes_factual_numerator_or_denominator() -> None:
    identity = _identity()
    skipped = CoverageSkipMetadata(
        state=CoverageSkipState.SKIPPED,
        authority_ref="skip-1",
        reason_code="accepted_scope_exception",
        currentness=CoverageCurrentness.CURRENT,
    )
    plain_row = CoverageTraceabilityService.obligation_row(_obligation(identity), ())
    skipped_row = CoverageTraceabilityService.obligation_row(
        _obligation(identity, skip=skipped), ()
    )

    assert plain_row.state is skipped_row.state is CoverageFactState.UNCOVERED
    assert plain_row.covered is skipped_row.covered is False
    assert CoverageTraceabilityService.counts((plain_row,)).applicable == 1
    skipped_counts = CoverageTraceabilityService.counts((skipped_row,))
    assert skipped_counts.applicable == 1
    assert skipped_counts.covered == 0
    assert skipped_counts.skipped == 1


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        (
            {"delivery_state": CoverageDeliveryState.CANCELLED},
            CoverageEvidenceEligibility.INELIGIBLE_CANCELLED_OR_ARCHIVED,
        ),
        (
            {"delivery_state": CoverageDeliveryState.ARCHIVED},
            CoverageEvidenceEligibility.INELIGIBLE_CANCELLED_OR_ARCHIVED,
        ),
        (
            {"lifecycle_status": CodeTraceabilityLifecycleStatus.REVOKED},
            CoverageEvidenceEligibility.INELIGIBLE_REVOKED_OR_SUPERSEDED,
        ),
        (
            {"lifecycle_status": CodeTraceabilityLifecycleStatus.SUPERSEDED},
            CoverageEvidenceEligibility.INELIGIBLE_REVOKED_OR_SUPERSEDED,
        ),
        (
            {"currentness": CoverageCurrentness.PREVIOUS},
            CoverageEvidenceEligibility.INELIGIBLE_PRIOR_EDITION,
        ),
    ],
)
def test_historical_or_inactive_evidence_is_drillable_but_ineligible(
    changes: dict[str, object], expected: CoverageEvidenceEligibility
) -> None:
    row = CoverageTraceabilityService.obligation_row(
        _obligation(),
        (_evidence(**changes),),
    )

    assert row.state is CoverageFactState.UNCOVERED
    assert row.evidence[0].eligibility is expected


def test_non_satisfying_traceability_relation_does_not_cover() -> None:
    evidence = CoverageEvidenceFact(
        evidence_id="evidence-contradiction",
        evidence_type=CodeEvidenceType.BEHAVIOR,
        source_ref="source-1",
        obligation=_identity(),
        relation_type=CodeEvidenceSpecRelationType.CONTRADICTS,
        evidence_content_sha256="b" * 64,
        parent_card_id="card-1",
        delivery_state=CoverageDeliveryState.ACTIVE,
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        currentness=CoverageCurrentness.CURRENT,
        currentness_reason=None,
        authority_ref="code-evidence-link-contradiction",
    )
    row = CoverageTraceabilityService.obligation_row(_obligation(), (evidence,))

    assert row.state is CoverageFactState.UNCOVERED
    assert (
        row.evidence[0].eligibility
        is CoverageEvidenceEligibility.INELIGIBLE_NON_SATISFYING_RELATION
    )


def test_zero_applicable_is_explicit_without_percentage() -> None:
    row = CoverageTraceabilityService.obligation_row(_obligation(applicable=False), ())
    counts = CoverageTraceabilityService.counts((row,))

    assert row.state is CoverageFactState.NOT_APPLICABLE
    assert counts.state is CoverageAggregateState.NOT_APPLICABLE
    assert (counts.applicable, counts.covered, counts.uncovered, counts.n) == (
        0,
        0,
        0,
        0,
    )
    assert counts.value is None


def test_restricted_authority_never_leaks_evidence_or_zero_counts() -> None:
    fact = CoverageObligationFact(
        identity=_identity(),
        applicable=True,
        authority_state=CoverageAuthorityState.RESTRICTED,
        authority_ref=None,
        authority_reason="permission_denied",
    )
    row = CoverageTraceabilityService.obligation_row(fact, ())
    counts = CoverageTraceabilityService.counts((row,))

    assert row.state is CoverageFactState.RESTRICTED
    assert row.covered is None
    assert row.evidence == ()
    assert counts.state is CoverageAggregateState.RESTRICTED
    assert counts.covered is None

    with pytest.raises(
        ValueError, match="coverage_traceability_non_available_evidence_forbidden"
    ):
        CoverageTraceabilityService.obligation_row(fact, (_evidence(),))


def test_current_code_evidence_matrix_binds_target_resolution_and_execution() -> None:
    matrix = _matrix()

    assert matrix.state is CodeEvidenceMatrixState.AVAILABLE
    assert (
        matrix.targets[0].current_resolution_id == matrix.resolutions[0].resolution_id
    )
    assert matrix.executions[0].target_revision == matrix.targets[0].revision


def test_missing_current_resolution_is_unavailable_not_guessed() -> None:
    target = CodeEvidenceTargetFact(
        target_id="target-1",
        card_id="card-1",
        source_ref="source-1",
        revision=3,
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        delivery_state=CoverageDeliveryState.ACTIVE,
        currentness=CoverageCurrentness.CURRENT,
        currentness_reason=None,
        current_resolution_id=None,
    )
    matrix = CoverageTraceabilityService.code_evidence_matrix(
        authority_state=CodeEvidenceMatrixState.AVAILABLE,
        targets=(target,),
    )

    assert matrix.state is CodeEvidenceMatrixState.UNAVAILABLE
    assert matrix.reason == "code_evidence_current_resolution_missing"


def test_contradictory_resolution_pointer_fails_closed() -> None:
    target = CodeEvidenceTargetFact(
        target_id="target-1",
        card_id="card-1",
        source_ref="source-1",
        revision=3,
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        delivery_state=CoverageDeliveryState.ACTIVE,
        currentness=CoverageCurrentness.CURRENT,
        currentness_reason=None,
        current_resolution_id="resolution-1",
    )
    wrong = CodeEvidenceResolutionFact(
        resolution_id="resolution-1",
        target_id="target-other",
        target_revision=3,
        state=ImplementationTargetResolutionState.RESOLVED,
        currentness=CoverageCurrentness.CURRENT,
        currentness_reason=None,
        authority_ref="resolution-authority-1",
    )
    matrix = CoverageTraceabilityService.code_evidence_matrix(
        authority_state=CodeEvidenceMatrixState.AVAILABLE,
        targets=(target,),
        resolutions=(wrong,),
    )

    assert matrix.state is CodeEvidenceMatrixState.INCONSISTENT
    assert matrix.reason == "code_evidence_resolution_pointer_contradictory"


def test_current_stale_resolution_is_unavailable_with_explicit_reason() -> None:
    target = CodeEvidenceTargetFact(
        target_id="target-1",
        card_id="card-1",
        source_ref="source-1",
        revision=3,
        lifecycle_status=CodeTraceabilityLifecycleStatus.ACTIVE,
        delivery_state=CoverageDeliveryState.ACTIVE,
        currentness=CoverageCurrentness.CURRENT,
        currentness_reason=None,
        current_resolution_id="resolution-1",
    )
    stale = CodeEvidenceResolutionFact(
        resolution_id="resolution-1",
        target_id="target-1",
        target_revision=3,
        state=ImplementationTargetResolutionState.STALE,
        currentness=CoverageCurrentness.CURRENT,
        currentness_reason=None,
        authority_ref="resolution-authority-1",
    )
    matrix = CoverageTraceabilityService.code_evidence_matrix(
        authority_state=CodeEvidenceMatrixState.AVAILABLE,
        targets=(target,),
        resolutions=(stale,),
    )

    assert matrix.state is CodeEvidenceMatrixState.UNAVAILABLE
    assert matrix.reason == "code_evidence_current_resolution_unavailable"


def test_waiver_is_visible_but_never_fabricates_target_or_coverage() -> None:
    waiver = CodeEvidenceWaiverFact(
        waiver_id="waiver-1",
        entity_type=CodeTraceabilityWaiverEntityType.SPEC,
        entity_id="spec-1",
        scope=CodeTraceabilityWaiverScope.CODE_EVIDENCE,
        reason_code=CodeTraceabilityWaiverReason.NO_CODE_CHANGE,
        active=True,
        currentness=CoverageCurrentness.CURRENT,
        currentness_reason=None,
        authority_ref="waiver-authority-1",
    )
    matrix = CoverageTraceabilityService.code_evidence_matrix(
        authority_state=CodeEvidenceMatrixState.AVAILABLE,
        waivers=(waiver,),
    )

    assert matrix.state is CodeEvidenceMatrixState.NOT_APPLICABLE
    assert matrix.targets == ()
    assert matrix.waivers == (waiver,)


def test_prior_target_history_remains_drillable_without_current_applicability() -> None:
    historical = CodeEvidenceTargetFact(
        target_id="target-old",
        card_id="card-archived",
        source_ref="source-1",
        revision=2,
        lifecycle_status=CodeTraceabilityLifecycleStatus.SUPERSEDED,
        delivery_state=CoverageDeliveryState.ARCHIVED,
        currentness=CoverageCurrentness.PREVIOUS,
        currentness_reason="superseded_target",
        current_resolution_id="resolution-old",
    )
    historical_resolution = CodeEvidenceResolutionFact(
        resolution_id="resolution-old",
        target_id="target-old",
        target_revision=2,
        state=ImplementationTargetResolutionState.STALE,
        currentness=CoverageCurrentness.PREVIOUS,
        currentness_reason="prior_target_revision",
        authority_ref="resolution-authority-old",
    )
    matrix = CoverageTraceabilityService.code_evidence_matrix(
        authority_state=CodeEvidenceMatrixState.AVAILABLE,
        targets=(historical,),
        resolutions=(historical_resolution,),
    )

    assert matrix.state is CodeEvidenceMatrixState.NOT_APPLICABLE
    assert matrix.targets == (historical,)
    assert matrix.resolutions == (historical_resolution,)


def test_projection_preserves_separate_permission_exclusions() -> None:
    projection = CoverageTraceabilityService.projection(
        query=_query(),
        as_of=NOW,
        population_scope=AnalyticsPopulationScope("actor:user-1", 1, 2),
        exclusions=AnalyticsExclusionSummary(
            restricted_count=2,
            excluded_count=2,
            reasons=(AnalyticsExclusion("permission_denied", 2),),
        ),
        evidence_population_scope=AnalyticsPopulationScope("actor:user-1", 4, 3),
        evidence_exclusions=AnalyticsExclusionSummary(
            restricted_count=3,
            excluded_count=3,
            reasons=(AnalyticsExclusion("evidence_permission_denied", 3),),
        ),
        obligations=(_obligation(),),
        evidence=(_evidence(),),
        code_evidence=_matrix(),
    )
    payload = projection.canonical_dict()

    assert payload["exclusions"]["excluded_count"] == 2
    assert payload["evidence_exclusions"]["excluded_count"] == 3
    assert payload["foundation_version"] == "3"


def test_orphan_evidence_is_rejected_instead_of_covering_unrelated_obligation() -> None:
    with pytest.raises(ValueError, match="coverage_traceability_orphan_evidence"):
        CoverageTraceabilityService.projection(
            query=_query(),
            as_of=NOW,
            population_scope=AnalyticsPopulationScope("actor:user-1", 1),
            exclusions=AnalyticsExclusionSummary(),
            evidence_population_scope=AnalyticsPopulationScope("actor:user-1", 1),
            evidence_exclusions=AnalyticsExclusionSummary(),
            obligations=(_obligation(),),
            evidence=(_evidence(_identity(obligation_id="ac-other")),),
            code_evidence=_matrix(),
        )


def test_type_projection_rejects_forged_counts() -> None:
    row = CoverageTraceabilityService.obligation_row(_obligation(), (_evidence(),))
    forged = CoverageCounts(
        state=CoverageAggregateState.AVAILABLE,
        applicable=1,
        covered=0,
        uncovered=1,
        skipped=0,
        value=0.0,
        n=1,
    )

    with pytest.raises(ValueError, match="coverage_traceability_type_counts_mismatch"):
        CoverageTypeProjection(
            obligation_type=CoverageObligationType.ACCEPTANCE_CRITERION,
            counts=forged,
            rows=(row,),
        )
