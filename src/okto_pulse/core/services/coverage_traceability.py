"""Pure factual-coverage and Code Evidence Matrix composition semantics."""

from __future__ import annotations

from datetime import datetime

from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceSpecRelationType,
    CodeTraceabilityLifecycleStatus,
    ImplementationTargetResolutionState,
)
from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    require_utc_datetime,
)
from okto_pulse.core.ports.coverage_traceability import (
    COVERAGE_TRACEABILITY_CONTRACT_VERSION,
    MAX_CODE_EVIDENCE_MATRIX_ROWS,
    MAX_COVERAGE_EVIDENCE_ROWS,
    MAX_COVERAGE_OBLIGATION_ROWS,
    CodeEvidenceExecutionFact,
    CodeEvidenceMatrix,
    CodeEvidenceMatrixState,
    CodeEvidenceOverlapFact,
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
    CoverageEvidenceRow,
    CoverageFactState,
    CoverageObligationFact,
    CoverageObligationIdentity,
    CoverageObligationRow,
    CoverageObligationType,
    CoverageTraceabilityProjection,
    CoverageTypeProjection,
)


_AUTHORITY_TO_FACT_STATE = {
    CoverageAuthorityState.RESTRICTED: CoverageFactState.RESTRICTED,
    CoverageAuthorityState.UNAVAILABLE: CoverageFactState.UNAVAILABLE,
    CoverageAuthorityState.INCONSISTENT: CoverageFactState.INCONSISTENT,
}

_FACT_TO_AGGREGATE_STATE = {
    CoverageFactState.RESTRICTED: CoverageAggregateState.RESTRICTED,
    CoverageFactState.UNAVAILABLE: CoverageAggregateState.UNAVAILABLE,
    CoverageFactState.INCONSISTENT: CoverageAggregateState.INCONSISTENT,
}

_AGGREGATE_PRECEDENCE = (
    CoverageAggregateState.INCONSISTENT,
    CoverageAggregateState.RESTRICTED,
    CoverageAggregateState.UNAVAILABLE,
)

_SATISFYING_RELATIONS = frozenset(
    {
        CodeEvidenceSpecRelationType.SUPPORTS,
        CodeEvidenceSpecRelationType.IMPLEMENTS,
        CodeEvidenceSpecRelationType.TESTS,
    }
)


class CoverageTraceabilityService:
    """Compose authority facts without persistence reach-ins or mutation seams."""

    @staticmethod
    def evidence_row(fact: CoverageEvidenceFact) -> CoverageEvidenceRow:
        if not isinstance(fact, CoverageEvidenceFact):
            raise ValueError("coverage_traceability_evidence_fact_invalid")
        if fact.relation_type not in _SATISFYING_RELATIONS:
            eligibility = CoverageEvidenceEligibility.INELIGIBLE_NON_SATISFYING_RELATION
        elif fact.delivery_state is not CoverageDeliveryState.ACTIVE:
            eligibility = CoverageEvidenceEligibility.INELIGIBLE_CANCELLED_OR_ARCHIVED
        elif fact.lifecycle_status is not CodeTraceabilityLifecycleStatus.ACTIVE:
            eligibility = CoverageEvidenceEligibility.INELIGIBLE_REVOKED_OR_SUPERSEDED
        elif fact.currentness is not CoverageCurrentness.CURRENT:
            eligibility = CoverageEvidenceEligibility.INELIGIBLE_PRIOR_EDITION
        else:
            eligibility = CoverageEvidenceEligibility.ELIGIBLE
        return CoverageEvidenceRow(fact=fact, eligibility=eligibility)

    @staticmethod
    def obligation_row(
        fact: CoverageObligationFact,
        evidence: tuple[CoverageEvidenceFact, ...],
    ) -> CoverageObligationRow:
        if not isinstance(fact, CoverageObligationFact):
            raise ValueError("coverage_traceability_obligation_fact_invalid")
        if not isinstance(evidence, tuple) or any(
            not isinstance(item, CoverageEvidenceFact) for item in evidence
        ):
            raise ValueError("coverage_traceability_evidence_facts_invalid")
        if any(item.obligation != fact.identity for item in evidence):
            raise ValueError("coverage_traceability_evidence_target_mismatch")
        if fact.identity.currentness is not CoverageCurrentness.CURRENT:
            raise ValueError("coverage_traceability_current_obligation_required")

        if fact.authority_state is not CoverageAuthorityState.AVAILABLE:
            if evidence:
                raise ValueError(
                    "coverage_traceability_non_available_evidence_forbidden"
                )
            return CoverageObligationRow(
                identity=fact.identity,
                state=_AUTHORITY_TO_FACT_STATE[fact.authority_state],
                applicable=fact.applicable,
                covered=None,
                skip=fact.skip,
                evidence=(),
                reason=fact.authority_reason,
            )

        evidence_rows = tuple(
            CoverageTraceabilityService.evidence_row(item) for item in evidence
        )
        eligible = any(
            item.eligibility is CoverageEvidenceEligibility.ELIGIBLE
            for item in evidence_rows
        )
        if not fact.applicable:
            if eligible:
                raise ValueError(
                    "coverage_traceability_not_applicable_has_current_evidence"
                )
            return CoverageObligationRow(
                identity=fact.identity,
                state=CoverageFactState.NOT_APPLICABLE,
                applicable=False,
                covered=None,
                skip=fact.skip,
                evidence=evidence_rows,
                authority_ref=fact.authority_ref,
                reason="obligation_not_applicable",
            )
        return CoverageObligationRow(
            identity=fact.identity,
            state=(
                CoverageFactState.COVERED if eligible else CoverageFactState.UNCOVERED
            ),
            applicable=True,
            covered=eligible,
            skip=fact.skip,
            evidence=evidence_rows,
            authority_ref=fact.authority_ref,
        )

    @staticmethod
    def counts(rows: tuple[CoverageObligationRow, ...]) -> CoverageCounts:
        if not isinstance(rows, tuple) or any(
            not isinstance(item, CoverageObligationRow) for item in rows
        ):
            raise ValueError("coverage_traceability_rows_invalid")
        aggregate_states = {
            _FACT_TO_AGGREGATE_STATE[item.state]
            for item in rows
            if item.state in _FACT_TO_AGGREGATE_STATE
        }
        for state in _AGGREGATE_PRECEDENCE:
            if state in aggregate_states:
                return CoverageCounts(
                    state=state,
                    applicable=None,
                    covered=None,
                    uncovered=None,
                    skipped=None,
                    value=None,
                    n=None,
                    reason=f"coverage_{state.value}",
                )

        applicable_rows = tuple(item for item in rows if item.applicable)
        if not applicable_rows:
            return CoverageCounts(
                state=CoverageAggregateState.NOT_APPLICABLE,
                applicable=0,
                covered=0,
                uncovered=0,
                skipped=0,
                value=None,
                n=0,
                reason="zero_applicable_obligations",
            )
        covered = sum(item.covered is True for item in applicable_rows)
        applicable = len(applicable_rows)
        return CoverageCounts(
            state=CoverageAggregateState.AVAILABLE,
            applicable=applicable,
            covered=covered,
            uncovered=applicable - covered,
            skipped=sum(item.skip.effective for item in applicable_rows),
            value=round((covered / applicable) * 100, 6),
            n=applicable,
        )

    @staticmethod
    def code_evidence_matrix(
        *,
        authority_state: CodeEvidenceMatrixState,
        targets: tuple[CodeEvidenceTargetFact, ...] = (),
        resolutions: tuple[CodeEvidenceResolutionFact, ...] = (),
        executions: tuple[CodeEvidenceExecutionFact, ...] = (),
        overlaps: tuple[CodeEvidenceOverlapFact, ...] = (),
        waivers: tuple[CodeEvidenceWaiverFact, ...] = (),
        reason: str | None = None,
    ) -> CodeEvidenceMatrix:
        typed_collections = (
            (targets, CodeEvidenceTargetFact),
            (resolutions, CodeEvidenceResolutionFact),
            (executions, CodeEvidenceExecutionFact),
            (overlaps, CodeEvidenceOverlapFact),
            (waivers, CodeEvidenceWaiverFact),
        )
        if any(
            not isinstance(items, tuple)
            or any(not isinstance(item, item_type) for item in items)
            for items, item_type in typed_collections
        ):
            raise ValueError("coverage_traceability_matrix_rows_invalid")
        if (
            sum(len(items) for items, _ in typed_collections)
            > MAX_CODE_EVIDENCE_MATRIX_ROWS
        ):
            raise ValueError("coverage_traceability_matrix_rows_too_many")
        targets = tuple(sorted(targets, key=lambda item: item.target_id))
        resolutions = tuple(sorted(resolutions, key=lambda item: item.resolution_id))
        executions = tuple(sorted(executions, key=lambda item: item.execution_id))
        overlaps = tuple(sorted(overlaps, key=lambda item: item.overlap_id))
        waivers = tuple(sorted(waivers, key=lambda item: item.waiver_id))
        collections = (targets, resolutions, executions, overlaps, waivers)
        if authority_state in {
            CodeEvidenceMatrixState.RESTRICTED,
            CodeEvidenceMatrixState.UNAVAILABLE,
            CodeEvidenceMatrixState.INCONSISTENT,
        }:
            if authority_state is CodeEvidenceMatrixState.RESTRICTED:
                if any(collections):
                    raise ValueError(
                        "coverage_traceability_restricted_matrix_rows_forbidden"
                    )
            return CodeEvidenceMatrix(
                state=authority_state,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason=reason or f"code_evidence_{authority_state.value}",
            )
        if authority_state is CodeEvidenceMatrixState.NOT_APPLICABLE:
            if any((targets, resolutions, executions, overlaps)):
                raise ValueError(
                    "coverage_traceability_matrix_not_applicable_with_rows"
                )
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.NOT_APPLICABLE,
                waivers=waivers,
                reason=reason or "zero_code_evidence_targets",
            )
        if authority_state is not CodeEvidenceMatrixState.AVAILABLE:
            raise ValueError("coverage_traceability_matrix_authority_state_invalid")
        if not targets:
            if any((resolutions, executions, overlaps)):
                return CodeEvidenceMatrix(
                    state=CodeEvidenceMatrixState.INCONSISTENT,
                    targets=targets,
                    resolutions=resolutions,
                    executions=executions,
                    overlaps=overlaps,
                    waivers=waivers,
                    reason="code_evidence_orphan_facts",
                )
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.NOT_APPLICABLE,
                waivers=waivers,
                reason="zero_code_evidence_targets",
            )

        current_targets = {
            item.target_id: item
            for item in targets
            if item.currentness is CoverageCurrentness.CURRENT
        }
        current_resolutions = {
            item.resolution_id: item
            for item in resolutions
            if item.currentness is CoverageCurrentness.CURRENT
        }
        if not current_targets:
            if any(
                item.currentness is CoverageCurrentness.CURRENT
                for items in (resolutions, executions, overlaps)
                for item in items
            ):
                return CodeEvidenceMatrix(
                    state=CodeEvidenceMatrixState.INCONSISTENT,
                    targets=targets,
                    resolutions=resolutions,
                    executions=executions,
                    overlaps=overlaps,
                    waivers=waivers,
                    reason="code_evidence_current_fact_without_current_target",
                )
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.NOT_APPLICABLE,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason="zero_current_code_evidence_targets",
            )
        if any(
            item.lifecycle_status is not CodeTraceabilityLifecycleStatus.ACTIVE
            or item.delivery_state is not CoverageDeliveryState.ACTIVE
            for item in current_targets.values()
        ):
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.INCONSISTENT,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason="code_evidence_current_target_inactive",
            )
        if any(item.current_resolution_id is None for item in current_targets.values()):
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.UNAVAILABLE,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason="code_evidence_current_resolution_missing",
            )

        pointed_resolution_ids = {
            item.current_resolution_id for item in current_targets.values()
        }
        if not pointed_resolution_ids.issubset(current_resolutions):
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.UNAVAILABLE,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason="code_evidence_resolution_authority_missing",
            )
        if set(current_resolutions) != pointed_resolution_ids:
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.INCONSISTENT,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason="code_evidence_resolution_authority_contradictory",
            )
        if any(
            resolution.target_id != target.target_id
            or resolution.target_revision != target.revision
            for target in current_targets.values()
            for resolution in (current_resolutions[target.current_resolution_id],)
        ):
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.INCONSISTENT,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason="code_evidence_resolution_pointer_contradictory",
            )
        unavailable_resolution_states = {
            ImplementationTargetResolutionState.MISSING,
            ImplementationTargetResolutionState.UNAVAILABLE,
            ImplementationTargetResolutionState.STALE,
        }
        if any(
            item.state in unavailable_resolution_states
            for item in current_resolutions.values()
        ):
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.UNAVAILABLE,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason="code_evidence_current_resolution_unavailable",
            )
        if any(
            item.state is ImplementationTargetResolutionState.AMBIGUOUS
            for item in current_resolutions.values()
        ):
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.INCONSISTENT,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason="code_evidence_current_resolution_ambiguous",
            )
        if any(
            item.target_id not in current_targets
            or item.target_revision != current_targets[item.target_id].revision
            for item in executions
            if item.currentness is CoverageCurrentness.CURRENT
        ):
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.INCONSISTENT,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason="code_evidence_execution_authority_contradictory",
            )
        if any(
            item.target_a_id not in current_targets
            or item.target_b_id not in current_targets
            or item.resolution_a_id
            != current_targets[item.target_a_id].current_resolution_id
            or item.resolution_b_id
            != current_targets[item.target_b_id].current_resolution_id
            for item in overlaps
            if item.currentness is CoverageCurrentness.CURRENT
        ):
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.INCONSISTENT,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason="code_evidence_overlap_authority_contradictory",
            )
        all_targets = {item.target_id: item for item in targets}
        all_resolutions = {item.resolution_id: item for item in resolutions}
        if (
            any(
                item.target_id not in all_targets
                or item.target_revision > all_targets[item.target_id].revision
                for item in resolutions
            )
            or any(
                item.target_id not in all_targets
                or item.target_revision > all_targets[item.target_id].revision
                for item in executions
            )
            or any(
                item.target_a_id not in all_targets
                or item.target_b_id not in all_targets
                or item.resolution_a_id not in all_resolutions
                or item.resolution_b_id not in all_resolutions
                for item in overlaps
            )
        ):
            return CodeEvidenceMatrix(
                state=CodeEvidenceMatrixState.INCONSISTENT,
                targets=targets,
                resolutions=resolutions,
                executions=executions,
                overlaps=overlaps,
                waivers=waivers,
                reason="code_evidence_historical_authority_contradictory",
            )
        return CodeEvidenceMatrix(
            state=CodeEvidenceMatrixState.AVAILABLE,
            targets=targets,
            resolutions=resolutions,
            executions=executions,
            overlaps=overlaps,
            waivers=waivers,
        )

    @staticmethod
    def projection(
        *,
        query: AnalyticsFoundationQuery,
        as_of: datetime,
        population_scope: AnalyticsPopulationScope,
        exclusions: AnalyticsExclusionSummary,
        evidence_population_scope: AnalyticsPopulationScope,
        evidence_exclusions: AnalyticsExclusionSummary,
        obligations: tuple[CoverageObligationFact, ...],
        evidence: tuple[CoverageEvidenceFact, ...],
        code_evidence: CodeEvidenceMatrix,
        next_cursor: str | None = None,
    ) -> CoverageTraceabilityProjection:
        observed_at = require_utc_datetime(
            as_of, field="coverage_traceability_projection_as_of"
        )
        if query.as_of is not None and query.as_of != observed_at:
            raise ValueError("coverage_traceability_as_of_mismatch")
        if (
            population_scope.scope_ref != query.actor_scope_ref
            or evidence_population_scope.scope_ref != query.actor_scope_ref
        ):
            raise ValueError("coverage_traceability_actor_scope_mismatch")
        if not isinstance(obligations, tuple) or any(
            not isinstance(item, CoverageObligationFact) for item in obligations
        ):
            raise ValueError("coverage_traceability_obligations_invalid")
        if len(obligations) > MAX_COVERAGE_OBLIGATION_ROWS:
            raise ValueError("coverage_traceability_rows_too_many")
        if not isinstance(evidence, tuple) or any(
            not isinstance(item, CoverageEvidenceFact) for item in evidence
        ):
            raise ValueError("coverage_traceability_evidence_invalid")
        if len(evidence) > MAX_COVERAGE_EVIDENCE_ROWS:
            raise ValueError("coverage_traceability_evidence_rows_too_many")
        identities = tuple(item.identity for item in obligations)
        if len(set(identities)) != len(identities):
            raise ValueError("coverage_traceability_obligation_duplicate")
        if len({(item.evidence_id, item.obligation) for item in evidence}) != len(
            evidence
        ):
            raise ValueError("coverage_traceability_evidence_duplicate")
        identity_set = set(identities)
        if any(item.obligation not in identity_set for item in evidence):
            raise ValueError("coverage_traceability_orphan_evidence")

        grouped_evidence: dict[
            CoverageObligationIdentity, list[CoverageEvidenceFact]
        ] = {identity: [] for identity in identities}
        for item in evidence:
            grouped_evidence[item.obligation].append(item)
        evidence_by_obligation = {
            identity: tuple(items) for identity, items in grouped_evidence.items()
        }
        rows = tuple(
            CoverageTraceabilityService.obligation_row(
                fact,
                tuple(
                    sorted(
                        evidence_by_obligation[fact.identity],
                        key=lambda item: (item.evidence_id, item.authority_ref),
                    )
                ),
            )
            for fact in sorted(
                obligations,
                key=lambda item: (
                    item.identity.obligation_type.value,
                    item.identity.spec_id,
                    item.identity.edition,
                    item.identity.obligation_id,
                ),
            )
        )
        coverage = tuple(
            CoverageTypeProjection(
                obligation_type=obligation_type,
                counts=CoverageTraceabilityService.counts(
                    tuple(
                        item
                        for item in rows
                        if item.identity.obligation_type is obligation_type
                    )
                ),
                rows=tuple(
                    item
                    for item in rows
                    if item.identity.obligation_type is obligation_type
                ),
            )
            for obligation_type in CoverageObligationType
        )
        return CoverageTraceabilityProjection(
            contract_version=COVERAGE_TRACEABILITY_CONTRACT_VERSION,
            foundation_version=ANALYTICS_FOUNDATION_CONTRACT_VERSION,
            query_fingerprint=query.fingerprint,
            filters=query.filters,
            as_of=observed_at,
            population_scope=population_scope,
            exclusions=exclusions,
            evidence_population_scope=evidence_population_scope,
            evidence_exclusions=evidence_exclusions,
            totals=CoverageTraceabilityService.counts(rows),
            coverage=coverage,
            code_evidence=code_evidence,
            next_cursor=next_cursor,
        )


__all__ = ["CoverageTraceabilityService"]
