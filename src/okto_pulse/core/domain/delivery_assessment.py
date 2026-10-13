"""Immutable vocabulary for delivery readiness; no transport or infrastructure.

Authentication belongs to edition adapters. Effective coverage owns the shared
proof predicates; these values describe their result, not a second proof engine.
"""

from dataclasses import dataclass, replace
from enum import StrEnum

from okto_pulse.core.domain.delivery_evidence import (
    DeliveryBinding, DeliveryScope, DeliveryWaiverFact, DeliveryPhase, delivery_waiver_current,
)
from okto_pulse.core.domain.enums import CardType


class ReadinessPhase(StrEnum):
    IMPLEMENTATION = "implementation"
    VERIFICATION = "verification"
    LIFECYCLE = "lifecycle"


class UnitState(StrEnum):
    SATISFIED = "satisfied"
    MISSING = "missing"
    PARTIAL = "partial"
    FAILED = "failed"
    STALE = "stale"
    CONFLICT = "conflict"
    WAIVED = "waived"
    NOT_APPLICABLE = "not_applicable"
    UNKNOWN = "unknown"


class MetricStatus(StrEnum):
    KNOWN = "known"
    UNKNOWN = "unknown"
    NOT_APPLICABLE = "not_applicable"


class DeliveryState(StrEnum):
    UNKNOWN = "unknown"
    IMPLEMENTATION_PENDING = "implementation_pending"
    VERIFICATION_PENDING = "verification_pending"
    VERIFICATION_FAILED = "verification_failed"
    READY = "ready"
    DELIVERED = "delivered"
    READY_WITH_EXCEPTIONS = "ready_with_exceptions"
    DELIVERED_WITH_EXCEPTIONS = "delivered_with_exceptions"
    NOT_APPLICABLE = "not_applicable"


class ReadinessReason(StrEnum):
    SCOPE_UNAVAILABLE = "scope_unavailable"
    SCOPE_ALLOCATION_UNRESOLVED = "scope_allocation_unresolved"
    SCOPE_CONTRACT_CHANGED = "scope_contract_changed"
    IMPLEMENTATION_MISSING = "implementation_missing"
    IMPLEMENTATION_PARTIAL = "implementation_partial"
    IMPLEMENTATION_ASSOCIATION_MISSING = "implementation_association_missing"
    IMPLEMENTATION_PROOF_REVOKED = "implementation_proof_revoked"
    IMPLEMENTATION_PROOF_STALE = "implementation_proof_stale"
    MATERIAL_PROGRESS_UNRESOLVED = "material_progress_unresolved"
    VERIFICATION_PLAN_MISSING = "verification_plan_missing"
    VERIFICATION_NOT_RUN = "verification_not_run"
    VERIFICATION_FAILED = "verification_failed"
    VERIFICATION_INCONCLUSIVE = "verification_inconclusive"
    VERIFICATION_CONFLICT = "verification_conflict"
    VERIFICATION_ASSOCIATION_MISSING = "verification_association_missing"
    VERIFICATION_BASE_MISMATCH = "verification_base_mismatch"
    VERIFICATION_BASE_UNRESOLVED = "verification_base_unresolved"
    VERIFICATION_RECEIPT_INVALID = "verification_receipt_invalid"
    VERIFICATION_METHOD_UNSUPPORTED = "verification_method_unsupported"
    VERIFICATION_DEPENDENCY_PENDING = "verification_dependency_pending"
    DECISION_VERIFICATION_PENDING = "decision_verification_pending"
    APPLICABLE_OBLIGATION_WAIVED = "applicable_obligation_waived"
    LIFECYCLE_CARD_OPEN = "lifecycle_card_open"
    LIFECYCLE_GATE_PENDING = "lifecycle_gate_pending"
    ASSESSMENT_UNAVAILABLE = "assessment_unavailable"
    HISTORICAL_COMPLETION_DIVERGED = "historical_completion_diverged"


@dataclass(frozen=True, slots=True)
class AssessmentCompleteness:
    """Population, allocation, evaluation and response pagination are independent."""

    population_known: bool
    scope_resolved: bool
    evaluation_complete: bool
    page_complete: bool


@dataclass(frozen=True, slots=True)
class FactualReason:
    code: ReadinessReason
    # Native codes are retained without parsing their human-facing messages.
    native_reason_codes: tuple[str, ...] = ()
    source_ids: tuple[str, ...] = ()
    criterion_ids: tuple[str, ...] = ()
    scenario_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class UnitPhaseAssessment:
    state: UnitState
    intrinsic_state: UnitState
    proof_satisfied: bool | None
    lifecycle_satisfied: bool | None
    reasons: tuple[FactualReason, ...] = ()
    evidence_ids: tuple[str, ...] = ()
    exception_ids: tuple[str, ...] = ()
    unused_exception_ids: tuple[str, ...] = ()

    def __post_init__(self):
        if self.state == UnitState.SATISFIED and self.proof_satisfied is not True:
            raise ValueError("readiness_satisfied_without_proof")
        if self.state == UnitState.WAIVED and (not self.exception_ids or self.proof_satisfied is True):
            raise ValueError("readiness_waiver_without_effective_exception")
        if self.intrinsic_state == UnitState.WAIVED:
            raise ValueError("readiness_waiver_is_not_intrinsic")


@dataclass(frozen=True, slots=True)
class ObligationAssessment:
    binding: DeliveryBinding
    spec_edition: int
    contribution_card_ids: tuple[str, ...]
    implementation: UnitPhaseAssessment
    verification: UnitPhaseAssessment


@dataclass(frozen=True, slots=True)
class ScenarioAssessment:
    scenario_id: str
    semantic_sha256: str
    spec_edition: int
    card_id: str
    verification: UnitPhaseAssessment


@dataclass(frozen=True, slots=True)
class DeliveryAssessment:
    scope: DeliveryScope
    completeness: AssessmentCompleteness
    obligations: tuple[ObligationAssessment, ...]
    reasons: tuple[FactualReason, ...] = ()
    card_id: str | None = None
    card_type: CardType | None = None
    scenarios: tuple[ScenarioAssessment, ...] = ()


def apply_delivery_exceptions(result: UnitPhaseAssessment, *, scope: DeliveryScope,
                              binding: DeliveryBinding, phase: DeliveryPhase,
                              waivers: tuple[DeliveryWaiverFact, ...]) -> UnitPhaseAssessment:
    """Authority is factual; prose and old absent fields never imply N/A.

    N/A is resolved first. Accepted risk changes qualification, not proof. If
    proof is sufficient the exception is retained as unused, not administrative
    work the caller must revoke to obtain full credit.
    """
    valid = tuple(item for item in waivers if delivery_waiver_current(item, scope, binding, phase))
    not_applicable = tuple(sorted({item.id for item in valid if item.disposition == "not_applicable"}))
    accepted_risk = tuple(sorted({item.id for item in valid if item.disposition in {None, "risk_accepted"}}))
    if not_applicable:
        return replace(result, state=UnitState.NOT_APPLICABLE,
            exception_ids=not_applicable, unused_exception_ids=accepted_risk)
    if result.proof_satisfied is True or result.state == UnitState.NOT_APPLICABLE:
        return replace(result, unused_exception_ids=accepted_risk)
    if accepted_risk:
        return replace(result, state=UnitState.WAIVED, exception_ids=accepted_risk,
            reasons=(*result.reasons, FactualReason(ReadinessReason.APPLICABLE_OBLIGATION_WAIVED,
                source_ids=accepted_risk)))
    return result


@dataclass(frozen=True, slots=True)
class PhaseMetricCounts:
    satisfied: int = 0
    missing: int = 0
    partial: int = 0
    failed: int = 0
    stale: int = 0
    conflict: int = 0
    waived: int = 0
    not_applicable: int = 0
    unknown: int = 0


@dataclass(frozen=True, slots=True)
class PhaseMetric:
    unit: str
    status: MetricStatus
    percent: int | None
    total: int | None
    applicable: int | None
    satisfied: int
    not_applicable: int
    waived: int
    unresolved: int | None
    unknown: int
    counts_by_state: PhaseMetricCounts
    reason_codes: tuple[ReadinessReason, ...]
    known_counts: PhaseMetricCounts | None


@dataclass(frozen=True, slots=True)
class DeliveryMetrics:
    implementation: PhaseMetric
    verification: PhaseMetric


def _phase_metric(phases, *, unit, population_known, evaluation_complete, reason_codes=()):
    """Aggregate already evaluated canonical units, never infer proof from status."""
    counts = {state.value: 0 for state in UnitState}
    reasons = set(reason_codes)
    for phase in phases:
        counts[phase.state.value] += 1
        reasons.update(reason.code for reason in phase.reasons)
    values = PhaseMetricCounts(**counts)
    total = len(phases) if population_known else None
    applicable = total - values.not_applicable if total is not None else None
    unresolved = applicable - values.satisfied if applicable is not None else None
    if not population_known or not evaluation_complete or values.unknown:
        status, percent = MetricStatus.UNKNOWN, None
        if not population_known:
            reasons.add(ReadinessReason.SCOPE_UNAVAILABLE)
        if not evaluation_complete or values.unknown:
            reasons.add(ReadinessReason.ASSESSMENT_UNAVAILABLE)
    elif not applicable:
        status, percent = MetricStatus.NOT_APPLICABLE, None
    else:
        status = MetricStatus.KNOWN
        percent = 100 * values.satisfied // applicable
    return PhaseMetric(unit, status, percent, total, applicable, values.satisfied,
        values.not_applicable, values.waived, unresolved, values.unknown, values,
        tuple(sorted(reasons)), values if not population_known else None)


def aggregate_delivery_metrics(assessment: DeliveryAssessment) -> DeliveryMetrics:
    """Independent metrics over unique canonical obligations or assigned scenarios.

    Deduplication is by full canonical identity. A contradictory duplicate,
    second digest for the same ref, or foreign edition is uncertain population,
    never another unit to count. Scope resolution remains explicit on the
    assessment; a known unallocated obligation must still be present as pending.
    """
    is_test = assessment.card_type == CardType.TEST
    source = assessment.scenarios if is_test else assessment.obligations
    identities = {}
    by_ref = {}
    ambiguous = set()
    population_known = assessment.completeness.population_known and bool(source)
    for row in source:
        ref = row.scenario_id if is_test else row.binding.obligation_ref
        digest = row.semantic_sha256 if is_test else row.binding.semantic_sha256
        key = (ref, digest, row.spec_edition)
        if key in identities and identities[key] != row:
            ambiguous.add(key)
        if (row.spec_edition != assessment.scope.edition
            or (ref in by_ref and by_ref[ref] != key)
            or (key in identities and identities[key] != row)
            or (is_test and (assessment.card_id is None or row.card_id != assessment.card_id))):
            population_known = False
        by_ref[ref] = key
        identities.setdefault(key, row)
    unknown = UnitPhaseAssessment(UnitState.UNKNOWN, UnitState.UNKNOWN, None, None,
        reasons=(FactualReason(ReadinessReason.ASSESSMENT_UNAVAILABLE),))
    rows = tuple(
        replace(identities[key], **({"verification": unknown} if is_test else
                {"implementation": unknown, "verification": unknown}))
        if key in ambiguous else identities[key]
        for key in sorted(identities)
    )
    options = dict(population_known=population_known,
        evaluation_complete=assessment.completeness.evaluation_complete,
        reason_codes=tuple(reason.code for reason in assessment.reasons))
    implementation = (
        _phase_metric((), unit="obligation", population_known=True, evaluation_complete=True)
        if is_test else _phase_metric(tuple(row.implementation for row in rows), unit="obligation", **options)
    )
    verification = _phase_metric(tuple(row.verification for row in rows),
        unit="scenario" if is_test else "obligation", **options)
    return DeliveryMetrics(implementation, verification)
