"""Immutable vocabulary for delivery readiness; no transport or infrastructure.

Authentication belongs to edition adapters. Effective coverage owns the shared
proof predicates; these values describe their result, not a second proof engine.
"""

from dataclasses import dataclass
from enum import StrEnum

from okto_pulse.core.domain.delivery_evidence import DeliveryBinding, DeliveryScope


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
class DeliveryAssessment:
    scope: DeliveryScope
    completeness: AssessmentCompleteness
    obligations: tuple[ObligationAssessment, ...]
    reasons: tuple[FactualReason, ...] = ()
