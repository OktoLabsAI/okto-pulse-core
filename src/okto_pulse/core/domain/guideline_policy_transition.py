"""Pure policy-compliance decision for canonical SDLC transitions.

The registry owns which lifecycle edges require compliance.  This module owns
only the deterministic decision over an immutable receipt plus its live fence;
it performs no I/O, locking, persistence, clock access, or status mutation.
Preview and mutation therefore consume the same snapshot contract and can
compare the resulting digest to close time-of-check/time-of-use drift.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from okto_pulse.core.domain.guideline_compliance import (
    PolicyComplianceCurrentSnapshot,
    PolicyCurrentnessReason,
    assess_policy_receipt_currentness,
)
from okto_pulse.core.domain.guideline_policy import (
    GuidelineEnforcement,
    GuidelinePolicyContractError,
    PolicyComplianceReasonCode,
    PolicyComplianceReceipt,
    PolicyComplianceState,
    PolicyCurrentness,
    PolicyEntityType,
    PolicyEvaluationOutcome,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.domain.sdlc_registry import (
    is_transition_allowed,
    transition_contracts,
    transition_requires_policy_compliance,
)


POLICY_TRANSITION_CONTRACT_VERSION = "policy-transition/v1"


class PolicyTransitionReasonCode(str, Enum):
    """Closed aggregate explanations shared by preview and mutation."""

    TRANSITION_NOT_ALLOWED = "transition_not_allowed"
    POLICY_COMPLIANCE_NOT_REQUIRED = "policy_compliance_not_required"
    POLICY_COMPLIANCE_RECEIPT_MISSING = "policy_compliance_receipt_missing"
    POLICY_COMPLIANCE_RECEIPT_STALE = "policy_compliance_receipt_stale"
    POLICY_COMPLIANCE_BLOCKED = "policy_compliance_blocked"
    POLICY_EVALUATION_UNAVAILABLE = "policy_evaluation_unavailable"
    POLICY_EVALUATION_DEGRADED = "policy_evaluation_degraded"
    POLICY_COMPLIANCE_READY = "policy_compliance_ready"
    POLICY_COMPLIANCE_READY_WITH_WAIVERS = "policy_compliance_ready_with_waivers"
    POLICY_COMPLIANCE_NOT_APPLICABLE = "policy_compliance_not_applicable"
    POLICY_COMPLIANCE_ADVISORY_ONLY = "policy_compliance_advisory_only"


class PolicyTransitionRejected(GuidelinePolicyContractError):
    """Stable fail-closed error projected by mutation services."""

    def __init__(self, decision: PolicyTransitionDecision) -> None:
        if not isinstance(decision, PolicyTransitionDecision):
            raise GuidelinePolicyContractError("policy_transition_decision_invalid")
        self.decision = decision
        self.reason_codes = decision.reason_codes
        self.decision_digest = decision.decision_digest
        self.fence_digest = decision.fence_digest
        self.receipt_id = decision.receipt_id
        self.currentness = decision.currentness
        self.currentness_reasons = decision.currentness_reasons
        self.applicable_rule_count = decision.applicable_rule_count
        self.applicable_blocking_rule_count = decision.applicable_blocking_rule_count
        self.blocking_rule_count = decision.blocking_rule_count
        super().__init__(decision.reason_code.value)


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GuidelinePolicyContractError(code)
    return value.strip()


def _optional_text(value: object, code: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, code)


def _identity(
    snapshot: PolicyComplianceCurrentSnapshot,
) -> tuple[str, PolicyEntityType, str]:
    return snapshot.identity


def _receipt_identity(
    receipt: PolicyComplianceReceipt,
) -> tuple[str, PolicyEntityType, str]:
    return (
        receipt.subject.board_id,
        receipt.subject.entity_type,
        receipt.subject.subject_id,
    )


def _subject_payload(subject: object) -> dict[str, object]:
    return {
        "board_id": subject.board_id,
        "entity_type": subject.entity_type.value,
        "subject_id": subject.subject_id,
        "subject_version": subject.subject_version,
    }


def _current_snapshot_payload(
    snapshot: PolicyComplianceCurrentSnapshot | None,
) -> dict[str, object] | None:
    if snapshot is None:
        return None
    return {
        "subject": _subject_payload(snapshot.subject),
        "subject_content_digest": snapshot.subject_content_digest,
        "input_digest": snapshot.input_digest,
        "policy_set_digest": snapshot.policy_set_digest,
        "binding_head_digest": snapshot.binding_head_digest,
        "catalog_version": snapshot.catalog_version,
        "ruleset_version": snapshot.ruleset_version,
    }


def _receipt_payload(
    receipt: PolicyComplianceReceipt | None,
) -> dict[str, object] | None:
    if receipt is None:
        return None
    return {
        "receipt_id": receipt.receipt_id,
        "subject": _subject_payload(receipt.subject),
        "subject_content_digest": receipt.subject_content_digest,
        "input_digest": receipt.input_digest,
        "policy_set_digest": receipt.policy_set_digest,
        "binding_head_digest": receipt.binding_head_digest,
        "catalog_version": receipt.catalog_version,
        "ruleset_version": receipt.ruleset_version,
        "outcome": receipt.outcome.value,
        "state": receipt.state.value,
        "reason_codes": tuple(reason.value for reason in receipt.reason_codes),
        "adopted_revisions": tuple(
            {
                "binding_id": revision.binding_id,
                "binding_revision": revision.binding_revision,
                "guideline_id": revision.guideline_id,
                "revision_id": revision.revision_id,
                "semantic_version": revision.semantic_version,
                "revision_digest": revision.revision_digest,
            }
            for revision in receipt.adopted_revisions
        ),
        "rule_results": tuple(
            {
                "guideline_id": result.guideline_id,
                "revision_id": result.revision_id,
                "rule_id": result.rule_id,
                "outcome": result.outcome.value,
                "enforcement": result.enforcement.value,
                "waiver_id": result.waiver_id,
            }
            for result in receipt.rule_results
        ),
    }


@dataclass(frozen=True, slots=True)
class PolicyTransitionSnapshot:
    """Transaction-bound evidence consumed by one transition decision.

    Adapters resolve this value under their caller-owned unit of work.  Mutation
    adapters additionally guarantee the board/subject locking discipline; core
    deliberately does not prescribe a database lock primitive.
    """

    board_id: str
    entity_type: PolicyEntityType
    subject_id: str
    expected_from_status: str
    applicable_rule_count: int
    applicable_blocking_rule_count: int
    receipt: PolicyComplianceReceipt | None
    current_snapshot: PolicyComplianceCurrentSnapshot | None
    evaluation_available: bool = True
    evaluation_error_code: str | None = None
    currentness: PolicyCurrentness | None = field(init=False)
    currentness_reasons: tuple[PolicyCurrentnessReason, ...] = field(init=False)
    blocking_rule_count: int = field(init=False)
    waived_rule_count: int = field(init=False)
    advisory_issue_count: int = field(init=False)
    fence_digest: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "policy_transition_board_id_required"),
        )
        if not isinstance(self.entity_type, PolicyEntityType):
            raise GuidelinePolicyContractError("policy_transition_entity_type_invalid")
        object.__setattr__(
            self,
            "subject_id",
            _required_text(
                self.subject_id,
                "policy_transition_subject_id_required",
            ),
        )
        object.__setattr__(
            self,
            "expected_from_status",
            _required_text(
                self.expected_from_status,
                "policy_transition_from_status_required",
            ),
        )
        for field_name in (
            "applicable_rule_count",
            "applicable_blocking_rule_count",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise GuidelinePolicyContractError(
                    f"policy_transition_{field_name}_invalid"
                )
        if self.applicable_blocking_rule_count > self.applicable_rule_count:
            raise GuidelinePolicyContractError(
                "policy_transition_applicable_rule_counts_invalid"
            )
        # Validates the status against the same lifecycle used for mutation.
        transition_contracts(
            self.entity_type.value,
            self.expected_from_status,
        )
        if self.receipt is not None and not isinstance(
            self.receipt,
            PolicyComplianceReceipt,
        ):
            raise GuidelinePolicyContractError("policy_transition_receipt_invalid")
        if self.current_snapshot is not None and not isinstance(
            self.current_snapshot,
            PolicyComplianceCurrentSnapshot,
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_current_snapshot_invalid"
            )
        expected_identity = (
            self.board_id,
            self.entity_type,
            self.subject_id,
        )
        if (
            self.receipt is not None
            and _receipt_identity(self.receipt) != expected_identity
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_receipt_scope_mismatch"
            )
        if (
            self.current_snapshot is not None
            and _identity(self.current_snapshot) != expected_identity
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_current_snapshot_scope_mismatch"
            )
        if not isinstance(self.evaluation_available, bool):
            raise GuidelinePolicyContractError(
                "policy_transition_evaluation_availability_invalid"
            )
        error_code = _optional_text(
            self.evaluation_error_code,
            "policy_transition_evaluation_error_code_invalid",
        )
        if self.evaluation_available and error_code is not None:
            raise GuidelinePolicyContractError("policy_transition_available_with_error")
        if not self.evaluation_available and error_code is None:
            raise GuidelinePolicyContractError(
                "policy_transition_unavailable_error_required"
            )
        object.__setattr__(self, "evaluation_error_code", error_code)

        currentness: PolicyCurrentness | None = None
        currentness_reasons: tuple[PolicyCurrentnessReason, ...] = ()
        if self.receipt is not None:
            assessment = assess_policy_receipt_currentness(
                self.receipt,
                self.current_snapshot,
            )
            currentness = assessment.currentness
            currentness_reasons = assessment.reasons
        object.__setattr__(self, "currentness", currentness)
        object.__setattr__(
            self,
            "currentness_reasons",
            currentness_reasons,
        )

        receipt = self.receipt
        if receipt is not None and currentness is PolicyCurrentness.CURRENT:
            receipt_blocking_rule_count = sum(
                1
                for result in receipt.rule_results
                if result.enforcement is GuidelineEnforcement.BLOCKING
            )
            if (
                receipt.rule_count != self.applicable_rule_count
                or receipt_blocking_rule_count != self.applicable_blocking_rule_count
            ):
                raise GuidelinePolicyContractError(
                    "policy_transition_current_receipt_rule_counts_mismatch"
                )
        object.__setattr__(
            self,
            "blocking_rule_count",
            receipt.blocking_rule_count if receipt is not None else 0,
        )
        object.__setattr__(
            self,
            "waived_rule_count",
            receipt.waived_rule_count if receipt is not None else 0,
        )
        object.__setattr__(
            self,
            "advisory_issue_count",
            (
                sum(
                    1
                    for result in receipt.rule_results
                    if result.enforcement is GuidelineEnforcement.ADVISORY
                    and result.outcome
                    in {
                        PolicyEvaluationOutcome.FAIL,
                        PolicyEvaluationOutcome.ERROR,
                    }
                )
                if receipt is not None
                else 0
            ),
        )
        object.__setattr__(
            self,
            "fence_digest",
            canonical_sha256(
                {
                    "contract": POLICY_TRANSITION_CONTRACT_VERSION,
                    "board_id": self.board_id,
                    "entity_type": self.entity_type.value,
                    "subject_id": self.subject_id,
                    "expected_from_status": self.expected_from_status,
                    "applicable_rule_count": self.applicable_rule_count,
                    "applicable_blocking_rule_count": (
                        self.applicable_blocking_rule_count
                    ),
                    "evaluation_available": self.evaluation_available,
                    "evaluation_error_code": error_code,
                    "receipt": _receipt_payload(receipt),
                    "current_snapshot": _current_snapshot_payload(
                        self.current_snapshot
                    ),
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class PolicyTransitionDecision:
    """Deterministic aggregate answer for one requested lifecycle edge."""

    entity_type: PolicyEntityType
    subject_id: str
    from_status: str
    to_status: str
    transition_allowed: bool
    policy_compliance_required: bool
    allowed: bool
    reason_codes: tuple[PolicyTransitionReasonCode, ...]
    receipt_id: str | None
    currentness: PolicyCurrentness | None
    currentness_reasons: tuple[PolicyCurrentnessReason, ...]
    applicable_rule_count: int
    applicable_blocking_rule_count: int
    blocking_rule_count: int
    waived_rule_count: int
    advisory_issue_count: int
    fence_digest: str
    decision_digest: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.entity_type, PolicyEntityType):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_entity_type_invalid"
            )
        for field_name in ("subject_id", "from_status", "to_status"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"policy_transition_decision_{field_name}_required",
                ),
            )
        for field_name in (
            "transition_allowed",
            "policy_compliance_required",
            "allowed",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise GuidelinePolicyContractError(
                    f"policy_transition_decision_{field_name}_invalid"
                )
        if self.allowed and not self.transition_allowed:
            raise GuidelinePolicyContractError(
                "policy_transition_decision_illegal_edge_allowed"
            )
        if self.policy_compliance_required and not self.transition_allowed:
            raise GuidelinePolicyContractError(
                "policy_transition_decision_illegal_edge_requires_policy"
            )
        if (
            not isinstance(self.reason_codes, tuple)
            or not self.reason_codes
            or any(
                not isinstance(reason, PolicyTransitionReasonCode)
                for reason in self.reason_codes
            )
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_reason_codes_invalid"
            )
        if len(set(self.reason_codes)) != len(self.reason_codes):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_reason_codes_duplicate"
            )
        object.__setattr__(
            self,
            "receipt_id",
            _optional_text(
                self.receipt_id,
                "policy_transition_decision_receipt_id_invalid",
            ),
        )
        if self.currentness is not None and not isinstance(
            self.currentness,
            PolicyCurrentness,
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_currentness_invalid"
            )
        if not isinstance(self.currentness_reasons, tuple) or any(
            not isinstance(reason, PolicyCurrentnessReason)
            for reason in self.currentness_reasons
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_currentness_reasons_invalid"
            )
        for field_name in (
            "applicable_rule_count",
            "applicable_blocking_rule_count",
            "blocking_rule_count",
            "waived_rule_count",
            "advisory_issue_count",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise GuidelinePolicyContractError(
                    f"policy_transition_decision_{field_name}_invalid"
                )
        if self.applicable_blocking_rule_count > self.applicable_rule_count:
            raise GuidelinePolicyContractError(
                "policy_transition_decision_rule_counts_invalid"
            )
        if self.currentness is PolicyCurrentness.CURRENT and (
            self.blocking_rule_count > self.applicable_blocking_rule_count
            or self.waived_rule_count > self.applicable_rule_count
            or self.advisory_issue_count > self.applicable_rule_count
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_current_rule_counts_invalid"
            )
        object.__setattr__(
            self,
            "fence_digest",
            _required_text(
                self.fence_digest,
                "policy_transition_decision_fence_digest_required",
            ),
        )
        object.__setattr__(
            self,
            "decision_digest",
            canonical_sha256(
                {
                    "contract": POLICY_TRANSITION_CONTRACT_VERSION,
                    "entity_type": self.entity_type.value,
                    "subject_id": self.subject_id,
                    "from_status": self.from_status,
                    "to_status": self.to_status,
                    "transition_allowed": self.transition_allowed,
                    "policy_compliance_required": (self.policy_compliance_required),
                    "allowed": self.allowed,
                    "reason_codes": tuple(reason.value for reason in self.reason_codes),
                    "receipt_id": self.receipt_id,
                    "currentness": (
                        self.currentness.value if self.currentness is not None else None
                    ),
                    "currentness_reasons": tuple(
                        reason.value for reason in self.currentness_reasons
                    ),
                    "applicable_rule_count": self.applicable_rule_count,
                    "applicable_blocking_rule_count": (
                        self.applicable_blocking_rule_count
                    ),
                    "blocking_rule_count": self.blocking_rule_count,
                    "waived_rule_count": self.waived_rule_count,
                    "advisory_issue_count": self.advisory_issue_count,
                    "fence_digest": self.fence_digest,
                }
            ),
        )

    @property
    def reason_code(self) -> PolicyTransitionReasonCode:
        return self.reason_codes[0]


def _decision(
    snapshot: PolicyTransitionSnapshot,
    *,
    to_status: str,
    transition_allowed: bool,
    policy_compliance_required: bool,
    allowed: bool,
    reason_code: PolicyTransitionReasonCode,
) -> PolicyTransitionDecision:
    receipt = snapshot.receipt
    return PolicyTransitionDecision(
        entity_type=snapshot.entity_type,
        subject_id=snapshot.subject_id,
        from_status=snapshot.expected_from_status,
        to_status=to_status,
        transition_allowed=transition_allowed,
        policy_compliance_required=policy_compliance_required,
        allowed=allowed,
        reason_codes=(reason_code,),
        receipt_id=receipt.receipt_id if receipt is not None else None,
        currentness=snapshot.currentness,
        currentness_reasons=snapshot.currentness_reasons,
        applicable_rule_count=snapshot.applicable_rule_count,
        applicable_blocking_rule_count=(snapshot.applicable_blocking_rule_count),
        blocking_rule_count=snapshot.blocking_rule_count,
        waived_rule_count=snapshot.waived_rule_count,
        advisory_issue_count=snapshot.advisory_issue_count,
        fence_digest=snapshot.fence_digest,
    )


def evaluate_policy_transition(
    snapshot: PolicyTransitionSnapshot,
    target_status: str,
) -> PolicyTransitionDecision:
    """Evaluate one edge with fail-closed policy semantics.

    Recovery/backward/cancellation edges return before inspecting evaluator
    availability or historical receipts.  Only registry edges explicitly
    marked ``policy_compliance`` can be blocked by this gate.
    """

    if not isinstance(snapshot, PolicyTransitionSnapshot):
        raise GuidelinePolicyContractError("policy_transition_snapshot_invalid")
    target_status = _required_text(
        target_status,
        "policy_transition_target_status_required",
    )
    no_op = (
        snapshot.entity_type is PolicyEntityType.TEST_SCENARIO
        and snapshot.expected_from_status == target_status
    )
    transition_allowed = no_op or is_transition_allowed(
        snapshot.entity_type.value,
        snapshot.expected_from_status,
        target_status,
    )
    if not transition_allowed:
        return _decision(
            snapshot,
            to_status=target_status,
            transition_allowed=False,
            policy_compliance_required=False,
            allowed=False,
            reason_code=PolicyTransitionReasonCode.TRANSITION_NOT_ALLOWED,
        )

    policy_required = (
        False
        if no_op
        else transition_requires_policy_compliance(
            snapshot.entity_type.value,
            snapshot.expected_from_status,
            target_status,
        )
    )
    if not policy_required:
        return _decision(
            snapshot,
            to_status=target_status,
            transition_allowed=True,
            policy_compliance_required=False,
            allowed=True,
            reason_code=(PolicyTransitionReasonCode.POLICY_COMPLIANCE_NOT_REQUIRED),
        )
    if snapshot.applicable_blocking_rule_count == 0:
        if snapshot.applicable_rule_count == 0:
            ready_reason = PolicyTransitionReasonCode.POLICY_COMPLIANCE_NOT_APPLICABLE
        elif (
            not snapshot.evaluation_available
            or snapshot.receipt is None
            or snapshot.currentness is not PolicyCurrentness.CURRENT
        ):
            # Advisory-only policy may degrade observability, never admission.
            ready_reason = PolicyTransitionReasonCode.POLICY_EVALUATION_DEGRADED
        elif snapshot.receipt.state is PolicyComplianceState.READY_WITH_WAIVERS:
            ready_reason = (
                PolicyTransitionReasonCode.POLICY_COMPLIANCE_READY_WITH_WAIVERS
            )
        elif snapshot.advisory_issue_count:
            ready_reason = PolicyTransitionReasonCode.POLICY_COMPLIANCE_ADVISORY_ONLY
        else:
            ready_reason = PolicyTransitionReasonCode.POLICY_COMPLIANCE_READY
        return _decision(
            snapshot,
            to_status=target_status,
            transition_allowed=True,
            policy_compliance_required=True,
            allowed=True,
            reason_code=ready_reason,
        )
    if not snapshot.evaluation_available:
        return _decision(
            snapshot,
            to_status=target_status,
            transition_allowed=True,
            policy_compliance_required=True,
            allowed=False,
            reason_code=(PolicyTransitionReasonCode.POLICY_EVALUATION_UNAVAILABLE),
        )
    receipt = snapshot.receipt
    if receipt is None:
        return _decision(
            snapshot,
            to_status=target_status,
            transition_allowed=True,
            policy_compliance_required=True,
            allowed=False,
            reason_code=(PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_MISSING),
        )
    if snapshot.currentness is not PolicyCurrentness.CURRENT:
        return _decision(
            snapshot,
            to_status=target_status,
            transition_allowed=True,
            policy_compliance_required=True,
            allowed=False,
            reason_code=(PolicyTransitionReasonCode.POLICY_COMPLIANCE_RECEIPT_STALE),
        )
    if receipt.state is PolicyComplianceState.BLOCKED:
        unavailable = (
            PolicyComplianceReasonCode.POLICY_EVALUATION_UNAVAILABLE
            in receipt.reason_codes
        )
        return _decision(
            snapshot,
            to_status=target_status,
            transition_allowed=True,
            policy_compliance_required=True,
            allowed=False,
            reason_code=(
                PolicyTransitionReasonCode.POLICY_EVALUATION_UNAVAILABLE
                if unavailable
                else PolicyTransitionReasonCode.POLICY_COMPLIANCE_BLOCKED
            ),
        )
    if receipt.state is PolicyComplianceState.NOT_APPLICABLE:
        ready_reason = PolicyTransitionReasonCode.POLICY_COMPLIANCE_NOT_APPLICABLE
    elif receipt.state is PolicyComplianceState.READY_WITH_WAIVERS:
        ready_reason = PolicyTransitionReasonCode.POLICY_COMPLIANCE_READY_WITH_WAIVERS
    elif snapshot.advisory_issue_count:
        ready_reason = PolicyTransitionReasonCode.POLICY_COMPLIANCE_ADVISORY_ONLY
    else:
        ready_reason = PolicyTransitionReasonCode.POLICY_COMPLIANCE_READY
    return _decision(
        snapshot,
        to_status=target_status,
        transition_allowed=True,
        policy_compliance_required=True,
        allowed=True,
        reason_code=ready_reason,
    )


def raise_for_policy_transition(
    decision: PolicyTransitionDecision,
) -> None:
    """Raise a typed error carrying the exact preview/mutation decision."""

    if not isinstance(decision, PolicyTransitionDecision):
        raise GuidelinePolicyContractError("policy_transition_decision_invalid")
    if not decision.allowed:
        raise PolicyTransitionRejected(decision)


def require_policy_transition_decision_match(
    expected_decision_digest: str,
    actual: PolicyTransitionDecision,
) -> None:
    """Reject a mutation when its locked decision differs from preview."""

    expected = _required_text(
        expected_decision_digest,
        "policy_transition_expected_decision_digest_required",
    )
    if not isinstance(actual, PolicyTransitionDecision):
        raise GuidelinePolicyContractError("policy_transition_decision_invalid")
    if expected != actual.decision_digest:
        raise GuidelinePolicyContractError("policy_transition_decision_changed")


__all__ = [
    "POLICY_TRANSITION_CONTRACT_VERSION",
    "PolicyTransitionDecision",
    "PolicyTransitionRejected",
    "PolicyTransitionReasonCode",
    "PolicyTransitionSnapshot",
    "evaluate_policy_transition",
    "raise_for_policy_transition",
    "require_policy_transition_decision_match",
]
