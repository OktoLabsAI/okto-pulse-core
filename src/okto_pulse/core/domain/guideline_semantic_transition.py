"""Native SDLC gate over admitted semantic guideline assessments.

Agents own every cognitive score.  This module never invokes a model or
re-evaluates prose: it consumes immutable assessment evidence, verifies the
live subject/guideline/binding fences, applies exact waivers or human skips,
and deterministically decides whether one registered lifecycle edge may run.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_BINDING_ID_MAX_LENGTH,
    GUIDELINE_ID_MAX_LENGTH,
    GuidelineEnforcement,
    GuidelinePolicyContractError,
    PolicyCurrentness,
    PolicyEntityType,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentInadmissibilityCause,
    SemanticGuidelineAssessmentReceipt,
    SemanticMetricOutcome,
)
from okto_pulse.core.domain.guideline_semantic_currentness import (
    SemanticAssessmentCurrentSnapshot,
    SemanticAssessmentCurrentnessReason,
    assess_semantic_assessment_currentness,
)
from okto_pulse.core.domain.guideline_semantic_exceptions import (
    SemanticMetricWaiver,
    SemanticPolicySkip,
    SemanticPolicySkipStatus,
)
from okto_pulse.core.domain.guideline_semantic_findings import (
    SemanticMetricFinding,
    project_semantic_metric_findings,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.domain.sdlc_registry import (
    is_transition_allowed,
    transition_contracts,
    transition_requires_policy_compliance,
)


POLICY_TRANSITION_CONTRACT_VERSION = "semantic-policy-transition/v2"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class PolicyTransitionReasonCode(str, Enum):
    """Closed top-level states shared by preview, mutation, REST, and MCP."""

    TRANSITION_NOT_ALLOWED = "transition_not_allowed"
    POLICY_COMPLIANCE_NOT_REQUIRED = "policy_compliance_not_required"
    POLICY_SUBJECT_REQUIRED = "policy_subject_required"
    POLICY_COMPLIANCE_RECEIPT_MISSING = "policy_compliance_receipt_missing"
    POLICY_COMPLIANCE_RECEIPT_STALE = "policy_compliance_receipt_stale"
    POLICY_COMPLIANCE_BLOCKED = "policy_compliance_blocked"
    POLICY_ASSESSMENT_UNAVAILABLE = "policy_assessment_unavailable"
    POLICY_COMPLIANCE_READY = "policy_compliance_ready"
    POLICY_COMPLIANCE_READY_WITH_WAIVERS = (
        "policy_compliance_ready_with_waivers"
    )
    POLICY_COMPLIANCE_NOT_APPLICABLE = (
        "policy_compliance_not_applicable"
    )
    POLICY_COMPLIANCE_ADVISORY_ONLY = (
        "policy_compliance_advisory_only"
    )


class PolicyTransitionDiagnosticCode(str, Enum):
    """Closed binding-level diagnostics beneath one top-level state."""

    POLICY_COMPLIANCE_RECEIPT_MISSING = "policy_compliance_receipt_missing"
    POLICY_COMPLIANCE_RECEIPT_STALE = "policy_compliance_receipt_stale"
    POLICY_ASSESSMENT_UNAVAILABLE = "policy_assessment_unavailable"
    POLICY_ASSESSMENT_INADMISSIBLE = "policy_assessment_inadmissible"
    POLICY_METRIC_THRESHOLD_FAILED = "policy_metric_threshold_failed"


_REASON_ORDER = tuple(PolicyTransitionReasonCode)
_DIAGNOSTIC_ORDER = tuple(PolicyTransitionDiagnosticCode)
_CURRENTNESS_REASON_ORDER = tuple(SemanticAssessmentCurrentnessReason)


def _required_text(
    value: object,
    code: str,
    *,
    max_length: int = 4096,
) -> str:
    if not isinstance(value, str):
        raise GuidelinePolicyContractError(code)
    normalized = value.strip()
    if not normalized or len(normalized) > max_length:
        raise GuidelinePolicyContractError(code)
    return normalized


def _optional_text(
    value: object,
    code: str,
    *,
    max_length: int = 4096,
) -> str | None:
    if value is None:
        return None
    return _required_text(value, code, max_length=max_length)


def _sha256(value: object, code: str) -> str:
    normalized = _required_text(value, code).lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise GuidelinePolicyContractError(code)
    return normalized


def _non_negative_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise GuidelinePolicyContractError(code)
    return value


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise GuidelinePolicyContractError(code)
    return value.astimezone(timezone.utc)


def _ordered_unique(
    values: set[Enum],
    order: tuple[Enum, ...],
) -> tuple:
    return tuple(item for item in order if item in values)


def _same_subject(
    *,
    board_id: str,
    entity_type: PolicyEntityType,
    subject_id: str,
    snapshot: SemanticAssessmentCurrentSnapshot,
) -> bool:
    return (
        snapshot.subject.board_id == board_id
        and snapshot.subject.entity_type is entity_type
        and snapshot.subject.subject_id == subject_id
    )


def _skip_matches_current(
    skip: SemanticPolicySkip,
    current: SemanticAssessmentCurrentSnapshot,
) -> bool:
    scope = skip.scope
    return (
        skip.status is SemanticPolicySkipStatus.ACTIVE
        and scope.subject == current.subject
        and scope.subject_content_digest == current.subject_content_digest
        and scope.guideline_id == current.guideline_id
        and scope.guideline_revision_id == current.guideline_revision_id
        and (
            scope.guideline_revision_digest
            == current.guideline_revision_digest
        )
        and scope.binding_id == current.binding_id
        and scope.binding_revision == current.binding_revision
        and (
            scope.binding_configuration_digest
            == current.binding_configuration_digest
        )
    )


def _current_snapshot_payload(
    snapshot: SemanticAssessmentCurrentSnapshot,
) -> dict[str, object]:
    return {
        "subject": {
            "board_id": snapshot.subject.board_id,
            "entity_type": snapshot.subject.entity_type.value,
            "subject_id": snapshot.subject.subject_id,
            "subject_version": snapshot.subject.subject_version,
            **(
                {"subject_edition": snapshot.subject.subject_edition}
                if snapshot.subject.subject_edition is not None
                else {}
            ),
        },
        "subject_content_digest": snapshot.subject_content_digest,
        "guideline_id": snapshot.guideline_id,
        "guideline_revision_id": snapshot.guideline_revision_id,
        "guideline_revision_digest": snapshot.guideline_revision_digest,
        "binding_id": snapshot.binding_id,
        "binding_revision": snapshot.binding_revision,
        "binding_configuration_digest": (
            snapshot.binding_configuration_digest
        ),
        "policy_set_digest": snapshot.policy_set_digest,
        "binding_head_digest": snapshot.binding_head_digest,
        "input_digest": snapshot.input_digest,
    }


@dataclass(frozen=True, slots=True)
class SemanticBindingComplianceSnapshot:
    """Immutable evidence for one applicable board binding."""

    binding_id: str
    guideline_id: str
    enforcement: GuidelineEnforcement
    applicable_metric_count: int
    current_snapshot: SemanticAssessmentCurrentSnapshot
    receipt: SemanticGuidelineAssessmentReceipt | None = None
    findings: tuple[SemanticMetricFinding, ...] = ()
    waivers: tuple[SemanticMetricWaiver, ...] = ()
    skip: SemanticPolicySkip | None = None
    assessment_available: bool = True
    assessment_error_code: str | None = None
    inadmissibility_cause: SemanticAssessmentInadmissibilityCause | None = None
    currentness: PolicyCurrentness | None = field(init=False)
    currentness_reasons: tuple[
        SemanticAssessmentCurrentnessReason, ...
    ] = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "binding_id",
            normalize_policy_bounded_text(
                self.binding_id,
                max_length=GUIDELINE_BINDING_ID_MAX_LENGTH,
                code="semantic_transition_binding_id_required",
            ),
        )
        object.__setattr__(
            self,
            "guideline_id",
            normalize_policy_bounded_text(
                self.guideline_id,
                max_length=GUIDELINE_ID_MAX_LENGTH,
                code="semantic_transition_guideline_id_required",
            ),
        )
        if not isinstance(self.enforcement, GuidelineEnforcement):
            raise GuidelinePolicyContractError(
                "semantic_transition_enforcement_invalid"
            )
        object.__setattr__(
            self,
            "applicable_metric_count",
            _non_negative_int(
                self.applicable_metric_count,
                "semantic_transition_applicable_metric_count_invalid",
            ),
        )
        if not isinstance(
            self.current_snapshot,
            SemanticAssessmentCurrentSnapshot,
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_current_snapshot_invalid"
            )
        if (
            self.current_snapshot.binding_id != self.binding_id
            or self.current_snapshot.guideline_id != self.guideline_id
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_current_snapshot_scope_mismatch"
            )
        if self.receipt is not None and not isinstance(
            self.receipt,
            SemanticGuidelineAssessmentReceipt,
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_receipt_invalid"
            )
        if self.receipt is not None and (
            self.receipt.binding_id != self.binding_id
            or self.receipt.guideline_id != self.guideline_id
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_receipt_scope_mismatch"
            )
        findings = tuple(self.findings)
        if any(not isinstance(item, SemanticMetricFinding) for item in findings):
            raise GuidelinePolicyContractError(
                "semantic_transition_findings_invalid"
            )
        if len({item.finding_id for item in findings}) != len(findings):
            raise GuidelinePolicyContractError(
                "semantic_transition_findings_duplicate"
            )
        if self.receipt is None and findings:
            raise GuidelinePolicyContractError(
                "semantic_transition_findings_without_receipt"
            )
        if self.receipt is not None and any(
            item.binding_id != self.binding_id
            or item.guideline_id != self.guideline_id
            or item.receipt_id != self.receipt.receipt_id
            for item in findings
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_finding_scope_mismatch"
            )
        if self.receipt is not None:
            canonical_findings = tuple(
                sorted(
                    project_semantic_metric_findings(self.receipt),
                    key=lambda item: item.finding_id,
                )
            )
            if tuple(
                sorted(findings, key=lambda item: item.finding_id)
            ) != canonical_findings:
                raise GuidelinePolicyContractError(
                    "semantic_transition_findings_projection_mismatch"
                )
        object.__setattr__(
            self,
            "findings",
            tuple(sorted(findings, key=lambda item: item.finding_id)),
        )
        waivers = tuple(self.waivers)
        if any(not isinstance(item, SemanticMetricWaiver) for item in waivers):
            raise GuidelinePolicyContractError(
                "semantic_transition_waivers_invalid"
            )
        if len({item.waiver_id for item in waivers}) != len(waivers):
            raise GuidelinePolicyContractError(
                "semantic_transition_waivers_duplicate"
            )
        if any(
            item.anchor.binding_id != self.binding_id
            or item.anchor.guideline_id != self.guideline_id
            for item in waivers
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_waiver_scope_mismatch"
            )
        object.__setattr__(
            self,
            "waivers",
            tuple(sorted(waivers, key=lambda item: item.waiver_id)),
        )
        if self.skip is not None and not isinstance(
            self.skip,
            SemanticPolicySkip,
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_skip_invalid"
            )
        if self.skip is not None and (
            self.skip.scope.binding_id != self.binding_id
            or self.skip.scope.guideline_id != self.guideline_id
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_skip_scope_mismatch"
            )
        if not isinstance(self.assessment_available, bool):
            raise GuidelinePolicyContractError(
                "semantic_transition_assessment_availability_invalid"
            )
        error_code = _optional_text(
            self.assessment_error_code,
            "semantic_transition_assessment_error_invalid",
        )
        if self.assessment_available == (error_code is not None):
            raise GuidelinePolicyContractError(
                "semantic_transition_assessment_availability_inconsistent"
            )
        object.__setattr__(self, "assessment_error_code", error_code)
        if not self.assessment_available and (
            self.receipt is not None or findings
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_unavailable_evidence_invalid"
            )
        if self.inadmissibility_cause is not None and not isinstance(
            self.inadmissibility_cause,
            SemanticAssessmentInadmissibilityCause,
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_inadmissibility_cause_invalid"
            )
        if (
            self.inadmissibility_cause is not None
            and (
                not self.assessment_available
                or self.receipt is not None
                or self.applicable_metric_count == 0
            )
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_inadmissibility_inconsistent"
            )

        currentness: PolicyCurrentness | None = None
        currentness_reasons: tuple[
            SemanticAssessmentCurrentnessReason, ...
        ] = ()
        if self.receipt is not None:
            assessment = assess_semantic_assessment_currentness(
                self.receipt,
                self.current_snapshot,
            )
            currentness = assessment.currentness
            currentness_reasons = assessment.reasons
            if (
                currentness is PolicyCurrentness.CURRENT
                and self.receipt.metric_count
                != self.applicable_metric_count
            ):
                raise GuidelinePolicyContractError(
                    "semantic_transition_current_metric_count_mismatch"
                )
        object.__setattr__(self, "currentness", currentness)
        object.__setattr__(
            self,
            "currentness_reasons",
            currentness_reasons,
        )


@dataclass(frozen=True, slots=True)
class SemanticBindingComplianceDecision:
    """Deterministic result for one binding under one transaction fence."""

    binding_id: str
    guideline_id: str
    enforcement: GuidelineEnforcement
    applicable_metric_count: int
    allowed: bool
    assessment_available: bool
    receipt_id: str | None
    currentness: PolicyCurrentness | None
    currentness_reasons: tuple[
        SemanticAssessmentCurrentnessReason, ...
    ]
    inadmissibility_cause: SemanticAssessmentInadmissibilityCause | None
    failed_metric_count: int
    waived_metric_count: int
    blocking_metric_count: int
    advisory_issue_count: int
    skipped: bool
    diagnostic_codes: tuple[PolicyTransitionDiagnosticCode, ...]

    def __post_init__(self) -> None:
        for field_name, max_length in (
            ("binding_id", GUIDELINE_BINDING_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"semantic_transition_decision_{field_name}_required",
                ),
            )
        if not isinstance(self.enforcement, GuidelineEnforcement):
            raise GuidelinePolicyContractError(
                "semantic_transition_decision_enforcement_invalid"
            )
        for field_name in (
            "applicable_metric_count",
            "failed_metric_count",
            "waived_metric_count",
            "blocking_metric_count",
            "advisory_issue_count",
        ):
            object.__setattr__(
                self,
                field_name,
                _non_negative_int(
                    getattr(self, field_name),
                    f"semantic_transition_decision_{field_name}_invalid",
                ),
            )
        for field_name in (
            "allowed",
            "assessment_available",
            "skipped",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise GuidelinePolicyContractError(
                    f"semantic_transition_decision_{field_name}_invalid"
                )
        object.__setattr__(
            self,
            "receipt_id",
            _optional_text(
                self.receipt_id,
                "semantic_transition_decision_receipt_id_invalid",
            ),
        )
        if self.currentness is not None and not isinstance(
            self.currentness,
            PolicyCurrentness,
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_decision_currentness_invalid"
            )
        if any(
            not isinstance(item, SemanticAssessmentCurrentnessReason)
            for item in self.currentness_reasons
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_decision_currentness_reasons_invalid"
            )
        if self.currentness_reasons != _ordered_unique(
            set(self.currentness_reasons),
            _CURRENTNESS_REASON_ORDER,
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_decision_currentness_reasons_order_invalid"
            )
        if self.inadmissibility_cause is not None and not isinstance(
            self.inadmissibility_cause,
            SemanticAssessmentInadmissibilityCause,
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_decision_inadmissibility_invalid"
            )
        if any(
            not isinstance(item, PolicyTransitionDiagnosticCode)
            for item in self.diagnostic_codes
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_decision_diagnostics_invalid"
            )
        if self.diagnostic_codes != _ordered_unique(
            set(self.diagnostic_codes),
            _DIAGNOSTIC_ORDER,
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_decision_diagnostics_order_invalid"
            )
        if (
            self.failed_metric_count > self.applicable_metric_count
            or self.waived_metric_count > self.failed_metric_count
            or self.blocking_metric_count > self.failed_metric_count
            or self.advisory_issue_count > self.applicable_metric_count
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_decision_metric_counts_invalid"
            )
        if (
            self.enforcement is GuidelineEnforcement.ADVISORY
            and self.blocking_metric_count
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_advisory_binding_blocks"
            )
        _evidence_gap_diagnostics = {
            PolicyTransitionDiagnosticCode.POLICY_ASSESSMENT_UNAVAILABLE,
            PolicyTransitionDiagnosticCode.POLICY_ASSESSMENT_INADMISSIBLE,
            PolicyTransitionDiagnosticCode.POLICY_COMPLIANCE_RECEIPT_MISSING,
            PolicyTransitionDiagnosticCode.POLICY_COMPLIANCE_RECEIPT_STALE,
        }
        if (
            self.enforcement is GuidelineEnforcement.ADVISORY
            and not self.allowed
            and not (set(self.diagnostic_codes) & _evidence_gap_diagnostics)
        ):
            # Advisory scores never block; only ABSENT/STALE/INADMISSIBLE
            # evidence rejects an advisory binding (evidence presence is
            # mandatory at governed transitions regardless of enforcement).
            raise GuidelinePolicyContractError(
                "semantic_transition_advisory_binding_rejected"
            )

        if self.applicable_metric_count == 0:
            if (
                not self.allowed
                or self.inadmissibility_cause is not None
                or self.failed_metric_count
                or self.waived_metric_count
                or self.blocking_metric_count
                or self.advisory_issue_count
                or self.skipped
                or self.diagnostic_codes
            ):
                raise GuidelinePolicyContractError(
                    "semantic_transition_context_only_decision_invalid"
                )
            return

        if self.skipped:
            expected_diagnostics = (
                (
                    PolicyTransitionDiagnosticCode
                    .POLICY_METRIC_THRESHOLD_FAILED,
                )
                if self.failed_metric_count
                else ()
            )
            if (
                not self.allowed
                or self.waived_metric_count
                or self.blocking_metric_count
                or self.advisory_issue_count
                or self.diagnostic_codes != expected_diagnostics
            ):
                raise GuidelinePolicyContractError(
                    "semantic_transition_skipped_decision_invalid"
                )
            return

        is_blocking = self.enforcement is GuidelineEnforcement.BLOCKING
        expected_allowed_without_evidence = False
        expected_advisory_issues_without_evidence = (
            0 if is_blocking else self.applicable_metric_count
        )
        if not self.assessment_available:
            if (
                self.receipt_id is not None
                or self.currentness is not None
                or self.currentness_reasons
                or self.inadmissibility_cause is not None
                or self.failed_metric_count
                or self.waived_metric_count
                or self.blocking_metric_count
                or self.advisory_issue_count
                != expected_advisory_issues_without_evidence
                or self.allowed != expected_allowed_without_evidence
                or self.diagnostic_codes
                != (
                    PolicyTransitionDiagnosticCode
                    .POLICY_ASSESSMENT_UNAVAILABLE,
                )
            ):
                raise GuidelinePolicyContractError(
                    "semantic_transition_unavailable_decision_invalid"
                )
            return

        if self.inadmissibility_cause is not None:
            if (
                self.receipt_id is not None
                or self.currentness is not None
                or self.currentness_reasons
                or self.failed_metric_count
                or self.waived_metric_count
                or self.blocking_metric_count
                or self.advisory_issue_count
                != expected_advisory_issues_without_evidence
                or self.allowed != expected_allowed_without_evidence
                or self.diagnostic_codes
                != (
                    PolicyTransitionDiagnosticCode
                    .POLICY_ASSESSMENT_INADMISSIBLE,
                )
            ):
                raise GuidelinePolicyContractError(
                    "semantic_transition_inadmissible_decision_invalid"
                )
            return

        if self.receipt_id is None:
            if (
                self.currentness is not None
                or self.currentness_reasons
                or self.failed_metric_count
                or self.waived_metric_count
                or self.blocking_metric_count
                or self.advisory_issue_count
                != expected_advisory_issues_without_evidence
                or self.allowed != expected_allowed_without_evidence
                or self.diagnostic_codes
                != (
                    PolicyTransitionDiagnosticCode
                    .POLICY_COMPLIANCE_RECEIPT_MISSING,
                )
            ):
                raise GuidelinePolicyContractError(
                    "semantic_transition_missing_receipt_decision_invalid"
                )
            return

        if self.currentness is PolicyCurrentness.STALE:
            if (
                not self.currentness_reasons
                or self.failed_metric_count
                or self.waived_metric_count
                or self.blocking_metric_count
                or self.advisory_issue_count
                != expected_advisory_issues_without_evidence
                or self.allowed != expected_allowed_without_evidence
                or self.diagnostic_codes
                != (
                    PolicyTransitionDiagnosticCode
                    .POLICY_COMPLIANCE_RECEIPT_STALE,
                )
            ):
                raise GuidelinePolicyContractError(
                    "semantic_transition_stale_receipt_decision_invalid"
                )
            return

        if (
            self.currentness is not PolicyCurrentness.CURRENT
            or self.currentness_reasons
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_current_receipt_decision_invalid"
            )
        unwaived_count = self.failed_metric_count - self.waived_metric_count
        expected_blocking_count = unwaived_count if is_blocking else 0
        expected_advisory_count = 0 if is_blocking else unwaived_count
        expected_diagnostics = (
            (
                PolicyTransitionDiagnosticCode
                .POLICY_METRIC_THRESHOLD_FAILED,
            )
            if self.failed_metric_count
            else ()
        )
        if (
            self.blocking_metric_count != expected_blocking_count
            or self.advisory_issue_count != expected_advisory_count
            or self.allowed != (not is_blocking or unwaived_count == 0)
            or self.diagnostic_codes != expected_diagnostics
        ):
            raise GuidelinePolicyContractError(
                "semantic_transition_current_receipt_decision_invalid"
            )

    def to_payload(self) -> dict[str, object]:
        return {
            "binding_id": self.binding_id,
            "guideline_id": self.guideline_id,
            "enforcement": self.enforcement.value,
            "applicable_metric_count": self.applicable_metric_count,
            "allowed": self.allowed,
            "assessment_available": self.assessment_available,
            "receipt_id": self.receipt_id,
            "currentness": (
                self.currentness.value
                if self.currentness is not None
                else None
            ),
            "currentness_reasons": [
                item.value for item in self.currentness_reasons
            ],
            "inadmissibility_cause": (
                self.inadmissibility_cause.value
                if self.inadmissibility_cause is not None
                else None
            ),
            "failed_metric_count": self.failed_metric_count,
            "waived_metric_count": self.waived_metric_count,
            "blocking_metric_count": self.blocking_metric_count,
            "advisory_issue_count": self.advisory_issue_count,
            "skipped": self.skipped,
            "diagnostic_codes": [
                item.value for item in self.diagnostic_codes
            ],
        }


def _evaluate_binding(
    snapshot: SemanticBindingComplianceSnapshot,
    *,
    evaluated_at: datetime,
) -> SemanticBindingComplianceDecision:
    applicable = snapshot.applicable_metric_count
    if applicable == 0:
        return SemanticBindingComplianceDecision(
            binding_id=snapshot.binding_id,
            guideline_id=snapshot.guideline_id,
            enforcement=snapshot.enforcement,
            applicable_metric_count=0,
            allowed=True,
            assessment_available=snapshot.assessment_available,
            receipt_id=(
                snapshot.receipt.receipt_id
                if snapshot.receipt is not None
                else None
            ),
            currentness=snapshot.currentness,
            currentness_reasons=snapshot.currentness_reasons,
            inadmissibility_cause=snapshot.inadmissibility_cause,
            failed_metric_count=0,
            waived_metric_count=0,
            blocking_metric_count=0,
            advisory_issue_count=0,
            skipped=False,
            diagnostic_codes=(),
        )

    if snapshot.skip is not None and _skip_matches_current(
        snapshot.skip,
        snapshot.current_snapshot,
    ):
        failed_count = (
            sum(
                1
                for result in snapshot.receipt.metric_results
                if result.outcome is SemanticMetricOutcome.FAIL
            )
            if (
                snapshot.receipt is not None
                and snapshot.currentness is PolicyCurrentness.CURRENT
            )
            else 0
        )
        return SemanticBindingComplianceDecision(
            binding_id=snapshot.binding_id,
            guideline_id=snapshot.guideline_id,
            enforcement=snapshot.enforcement,
            applicable_metric_count=applicable,
            allowed=True,
            assessment_available=snapshot.assessment_available,
            receipt_id=(
                snapshot.receipt.receipt_id
                if snapshot.receipt is not None
                else None
            ),
            currentness=snapshot.currentness,
            currentness_reasons=snapshot.currentness_reasons,
            inadmissibility_cause=snapshot.inadmissibility_cause,
            failed_metric_count=failed_count,
            waived_metric_count=0,
            blocking_metric_count=0,
            advisory_issue_count=0,
            skipped=True,
            diagnostic_codes=(
                (
                    PolicyTransitionDiagnosticCode
                    .POLICY_METRIC_THRESHOLD_FAILED,
                )
                if failed_count
                else ()
            ),
        )

    is_blocking = snapshot.enforcement is GuidelineEnforcement.BLOCKING
    if not snapshot.assessment_available:
        return SemanticBindingComplianceDecision(
            binding_id=snapshot.binding_id,
            guideline_id=snapshot.guideline_id,
            enforcement=snapshot.enforcement,
            applicable_metric_count=applicable,
            allowed=False,
            assessment_available=False,
            receipt_id=(
                snapshot.receipt.receipt_id
                if snapshot.receipt is not None
                else None
            ),
            currentness=snapshot.currentness,
            currentness_reasons=snapshot.currentness_reasons,
            inadmissibility_cause=None,
            failed_metric_count=0,
            waived_metric_count=0,
            blocking_metric_count=0,
            advisory_issue_count=(0 if is_blocking else applicable),
            skipped=False,
            diagnostic_codes=(
                PolicyTransitionDiagnosticCode
                .POLICY_ASSESSMENT_UNAVAILABLE,
            ),
        )
    if snapshot.inadmissibility_cause is not None:
        return SemanticBindingComplianceDecision(
            binding_id=snapshot.binding_id,
            guideline_id=snapshot.guideline_id,
            enforcement=snapshot.enforcement,
            applicable_metric_count=applicable,
            allowed=False,
            assessment_available=True,
            receipt_id=None,
            currentness=None,
            currentness_reasons=(),
            inadmissibility_cause=snapshot.inadmissibility_cause,
            failed_metric_count=0,
            waived_metric_count=0,
            blocking_metric_count=0,
            advisory_issue_count=(0 if is_blocking else applicable),
            skipped=False,
            diagnostic_codes=(
                PolicyTransitionDiagnosticCode
                .POLICY_ASSESSMENT_INADMISSIBLE,
            ),
        )
    if snapshot.receipt is None:
        return SemanticBindingComplianceDecision(
            binding_id=snapshot.binding_id,
            guideline_id=snapshot.guideline_id,
            enforcement=snapshot.enforcement,
            applicable_metric_count=applicable,
            allowed=False,
            assessment_available=True,
            receipt_id=None,
            currentness=None,
            currentness_reasons=(),
            inadmissibility_cause=None,
            failed_metric_count=0,
            waived_metric_count=0,
            blocking_metric_count=0,
            advisory_issue_count=(0 if is_blocking else applicable),
            skipped=False,
            diagnostic_codes=(
                PolicyTransitionDiagnosticCode
                .POLICY_COMPLIANCE_RECEIPT_MISSING,
            ),
        )
    if snapshot.currentness is not PolicyCurrentness.CURRENT:
        return SemanticBindingComplianceDecision(
            binding_id=snapshot.binding_id,
            guideline_id=snapshot.guideline_id,
            enforcement=snapshot.enforcement,
            applicable_metric_count=applicable,
            allowed=False,
            assessment_available=True,
            receipt_id=snapshot.receipt.receipt_id,
            currentness=snapshot.currentness,
            currentness_reasons=snapshot.currentness_reasons,
            inadmissibility_cause=None,
            failed_metric_count=0,
            waived_metric_count=0,
            blocking_metric_count=0,
            advisory_issue_count=(0 if is_blocking else applicable),
            skipped=False,
            diagnostic_codes=(
                PolicyTransitionDiagnosticCode
                .POLICY_COMPLIANCE_RECEIPT_STALE,
            ),
        )

    canonical_findings = project_semantic_metric_findings(snapshot.receipt)
    supplied_findings = {item.finding_id: item for item in snapshot.findings}
    waived_count = 0
    for finding in canonical_findings:
        supplied = supplied_findings.get(finding.finding_id)
        if supplied != finding:
            continue
        if any(
            waiver.is_active_for(
                supplied,
                currentness=PolicyCurrentness.CURRENT,
                at=evaluated_at,
            )
            for waiver in snapshot.waivers
        ):
            waived_count += 1
    failed_count = sum(
        1
        for result in snapshot.receipt.metric_results
        if result.outcome is SemanticMetricOutcome.FAIL
    )
    unwaived_count = failed_count - waived_count
    diagnostics = (
        (
            PolicyTransitionDiagnosticCode
            .POLICY_METRIC_THRESHOLD_FAILED,
        )
        if failed_count
        else ()
    )
    return SemanticBindingComplianceDecision(
        binding_id=snapshot.binding_id,
        guideline_id=snapshot.guideline_id,
        enforcement=snapshot.enforcement,
        applicable_metric_count=applicable,
        allowed=not is_blocking or unwaived_count == 0,
        assessment_available=True,
        receipt_id=snapshot.receipt.receipt_id,
        currentness=PolicyCurrentness.CURRENT,
        currentness_reasons=(),
        inadmissibility_cause=None,
        failed_metric_count=failed_count,
        waived_metric_count=waived_count,
        blocking_metric_count=(unwaived_count if is_blocking else 0),
        advisory_issue_count=(unwaived_count if not is_blocking else 0),
        skipped=False,
        diagnostic_codes=diagnostics,
    )


def _binding_snapshot_payload(
    snapshot: SemanticBindingComplianceSnapshot,
) -> dict[str, object]:
    return {
        "binding_id": snapshot.binding_id,
        "guideline_id": snapshot.guideline_id,
        "enforcement": snapshot.enforcement.value,
        "applicable_metric_count": snapshot.applicable_metric_count,
        "current_snapshot": _current_snapshot_payload(
            snapshot.current_snapshot
        ),
        "receipt": (
            {
                "receipt_id": snapshot.receipt.receipt_id,
                "receipt_digest": snapshot.receipt.receipt_digest,
            }
            if snapshot.receipt is not None
            else None
        ),
        "findings": [
            {
                "finding_id": item.finding_id,
                "finding_digest": item.finding_digest,
            }
            for item in snapshot.findings
        ],
        "waivers": [
            {
                "waiver_id": item.waiver_id,
                "waiver_revision": item.waiver_revision,
                "head_digest": item.head_digest,
            }
            for item in snapshot.waivers
        ],
        "skip": (
            {
                "skip_id": snapshot.skip.skip_id,
                "skip_revision": snapshot.skip.skip_revision,
                "skip_digest": snapshot.skip.skip_digest,
            }
            if snapshot.skip is not None
            else None
        ),
        "assessment_available": snapshot.assessment_available,
        "assessment_error_code": snapshot.assessment_error_code,
        "inadmissibility_cause": (
            snapshot.inadmissibility_cause.value
            if snapshot.inadmissibility_cause is not None
            else None
        ),
    }


@dataclass(frozen=True, slots=True)
class PolicyTransitionSnapshot:
    """Transaction-bound semantic evidence for one lifecycle transition."""

    board_id: str
    entity_type: PolicyEntityType
    subject_id: str
    expected_from_status: str
    bindings: tuple[SemanticBindingComplianceSnapshot, ...]
    evaluated_at: datetime
    subject_available: bool = True
    binding_decisions: tuple[
        SemanticBindingComplianceDecision, ...
    ] = field(init=False)
    applicable_metric_count: int = field(init=False)
    applicable_blocking_metric_count: int = field(init=False)
    failed_metric_count: int = field(init=False)
    blocking_metric_count: int = field(init=False)
    waived_metric_count: int = field(init=False)
    advisory_issue_count: int = field(init=False)
    skipped_binding_count: int = field(init=False)
    fence_digest: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "board_id",
            _required_text(
                self.board_id,
                "policy_transition_board_id_required",
            ),
        )
        if not isinstance(self.entity_type, PolicyEntityType):
            raise GuidelinePolicyContractError(
                "policy_transition_entity_type_invalid"
            )
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
        transition_contracts(
            self.entity_type.value,
            self.expected_from_status,
        )
        if not isinstance(self.subject_available, bool):
            raise GuidelinePolicyContractError(
                "policy_transition_subject_availability_invalid"
            )
        bindings = tuple(self.bindings)
        if any(
            not isinstance(item, SemanticBindingComplianceSnapshot)
            for item in bindings
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_bindings_invalid"
            )
        if len({item.binding_id for item in bindings}) != len(bindings):
            raise GuidelinePolicyContractError(
                "policy_transition_bindings_duplicate"
            )
        if not self.subject_available and bindings:
            raise GuidelinePolicyContractError(
                "policy_transition_bindings_without_subject"
            )
        for binding in bindings:
            if not _same_subject(
                board_id=self.board_id,
                entity_type=self.entity_type,
                subject_id=self.subject_id,
                snapshot=binding.current_snapshot,
            ):
                raise GuidelinePolicyContractError(
                    "policy_transition_binding_subject_scope_mismatch"
                )
        bindings = tuple(sorted(bindings, key=lambda item: item.binding_id))
        object.__setattr__(self, "bindings", bindings)
        evaluated_at = _aware_utc(
            self.evaluated_at,
            "policy_transition_evaluated_at_invalid",
        )
        object.__setattr__(self, "evaluated_at", evaluated_at)
        decisions = tuple(
            _evaluate_binding(item, evaluated_at=evaluated_at)
            for item in bindings
        )
        object.__setattr__(self, "binding_decisions", decisions)
        count_fields = {
            "applicable_metric_count": sum(
                item.applicable_metric_count for item in decisions
            ),
            "applicable_blocking_metric_count": sum(
                item.applicable_metric_count
                for item in decisions
                if item.enforcement is GuidelineEnforcement.BLOCKING
            ),
            "failed_metric_count": sum(
                item.failed_metric_count for item in decisions
            ),
            "blocking_metric_count": sum(
                item.blocking_metric_count for item in decisions
            ),
            "waived_metric_count": sum(
                item.waived_metric_count for item in decisions
            ),
            "advisory_issue_count": sum(
                item.advisory_issue_count for item in decisions
            ),
            "skipped_binding_count": sum(
                1 for item in decisions if item.skipped
            ),
        }
        for field_name, value in count_fields.items():
            object.__setattr__(self, field_name, value)
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
                    "subject_available": self.subject_available,
                    "evaluated_at": evaluated_at.isoformat(),
                    "bindings": [
                        _binding_snapshot_payload(item) for item in bindings
                    ],
                    "binding_decisions": [
                        item.to_payload() for item in decisions
                    ],
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
    diagnostic_codes: tuple[PolicyTransitionDiagnosticCode, ...]
    binding_decisions: tuple[SemanticBindingComplianceDecision, ...]
    receipt_ids: tuple[str, ...]
    applicable_metric_count: int
    applicable_blocking_metric_count: int
    failed_metric_count: int
    blocking_metric_count: int
    waived_metric_count: int
    advisory_issue_count: int
    skipped_binding_count: int
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
        if self.reason_codes != _ordered_unique(
            set(self.reason_codes),
            _REASON_ORDER,
        ) or not self.reason_codes:
            raise GuidelinePolicyContractError(
                "policy_transition_decision_reason_codes_invalid"
            )
        if self.diagnostic_codes != _ordered_unique(
            set(self.diagnostic_codes),
            _DIAGNOSTIC_ORDER,
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_diagnostics_invalid"
            )
        if any(
            not isinstance(item, SemanticBindingComplianceDecision)
            for item in self.binding_decisions
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_bindings_invalid"
            )
        binding_ids = tuple(
            item.binding_id for item in self.binding_decisions
        )
        if (
            binding_ids != tuple(sorted(binding_ids))
            or len(set(binding_ids)) != len(binding_ids)
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_bindings_order_invalid"
            )
        receipt_ids = tuple(
            _required_text(
                item,
                "policy_transition_decision_receipt_id_invalid",
            )
            for item in self.receipt_ids
        )
        if (
            receipt_ids != tuple(sorted(receipt_ids))
            or len(set(receipt_ids)) != len(receipt_ids)
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_receipt_ids_invalid"
            )
        object.__setattr__(self, "receipt_ids", receipt_ids)
        for field_name in (
            "applicable_metric_count",
            "applicable_blocking_metric_count",
            "failed_metric_count",
            "blocking_metric_count",
            "waived_metric_count",
            "advisory_issue_count",
            "skipped_binding_count",
        ):
            object.__setattr__(
                self,
                field_name,
                _non_negative_int(
                    getattr(self, field_name),
                    f"policy_transition_decision_{field_name}_invalid",
                ),
            )
        if (
            self.applicable_blocking_metric_count
            > self.applicable_metric_count
            or self.failed_metric_count > self.applicable_metric_count
            or self.blocking_metric_count > self.failed_metric_count
            or self.waived_metric_count > self.failed_metric_count
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_metric_counts_invalid"
            )
        expected_counts = {
            "applicable_metric_count": sum(
                item.applicable_metric_count
                for item in self.binding_decisions
            ),
            "applicable_blocking_metric_count": sum(
                item.applicable_metric_count
                for item in self.binding_decisions
                if item.enforcement is GuidelineEnforcement.BLOCKING
            ),
            "failed_metric_count": sum(
                item.failed_metric_count
                for item in self.binding_decisions
            ),
            "blocking_metric_count": sum(
                item.blocking_metric_count
                for item in self.binding_decisions
            ),
            "waived_metric_count": sum(
                item.waived_metric_count
                for item in self.binding_decisions
            ),
            "advisory_issue_count": sum(
                item.advisory_issue_count
                for item in self.binding_decisions
            ),
            "skipped_binding_count": sum(
                1 for item in self.binding_decisions if item.skipped
            ),
        }
        if any(
            getattr(self, field_name) != expected
            for field_name, expected in expected_counts.items()
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_aggregate_counts_mismatch"
            )
        expected_receipt_ids = tuple(
            sorted(
                {
                    item.receipt_id
                    for item in self.binding_decisions
                    if item.receipt_id is not None
                }
            )
        )
        if self.receipt_ids != expected_receipt_ids:
            raise GuidelinePolicyContractError(
                "policy_transition_decision_receipt_ids_mismatch"
            )
        expected_diagnostics = _ordered_unique(
            {
                diagnostic
                for item in self.binding_decisions
                for diagnostic in item.diagnostic_codes
            },
            _DIAGNOSTIC_ORDER,
        )
        if self.diagnostic_codes != expected_diagnostics:
            raise GuidelinePolicyContractError(
                "policy_transition_decision_diagnostics_mismatch"
            )

        if not self.transition_allowed:
            expected_allowed = False
            expected_required = False
            expected_reasons = (
                PolicyTransitionReasonCode.TRANSITION_NOT_ALLOWED,
            )
        elif not self.policy_compliance_required:
            expected_allowed = True
            expected_required = False
            expected_reasons = (
                PolicyTransitionReasonCode
                .POLICY_COMPLIANCE_NOT_REQUIRED,
            )
        elif (
            self.reason_codes
            == (PolicyTransitionReasonCode.POLICY_SUBJECT_REQUIRED,)
        ):
            if self.binding_decisions:
                raise GuidelinePolicyContractError(
                    "policy_transition_decision_subject_required_invalid"
                )
            expected_allowed = False
            expected_required = True
            expected_reasons = (
                PolicyTransitionReasonCode.POLICY_SUBJECT_REQUIRED,
            )
        else:
            blocking = tuple(
                item
                for item in self.binding_decisions
                if (
                    item.applicable_metric_count > 0
                    and not item.allowed
                )
            )
            if blocking:
                present: set[PolicyTransitionReasonCode] = set()
                for item in blocking:
                    diagnostics = set(item.diagnostic_codes)
                    if (
                        PolicyTransitionDiagnosticCode
                        .POLICY_COMPLIANCE_RECEIPT_MISSING
                        in diagnostics
                    ):
                        present.add(
                            PolicyTransitionReasonCode
                            .POLICY_COMPLIANCE_RECEIPT_MISSING
                        )
                    if (
                        PolicyTransitionDiagnosticCode
                        .POLICY_COMPLIANCE_RECEIPT_STALE
                        in diagnostics
                    ):
                        present.add(
                            PolicyTransitionReasonCode
                            .POLICY_COMPLIANCE_RECEIPT_STALE
                        )
                    if (
                        PolicyTransitionDiagnosticCode
                        .POLICY_ASSESSMENT_UNAVAILABLE
                        in diagnostics
                    ):
                        present.add(
                            PolicyTransitionReasonCode
                            .POLICY_ASSESSMENT_UNAVAILABLE
                        )
                    if diagnostics & {
                        (
                            PolicyTransitionDiagnosticCode
                            .POLICY_ASSESSMENT_INADMISSIBLE
                        ),
                        (
                            PolicyTransitionDiagnosticCode
                            .POLICY_METRIC_THRESHOLD_FAILED
                        ),
                    }:
                        present.add(
                            PolicyTransitionReasonCode
                            .POLICY_COMPLIANCE_BLOCKED
                        )
                expected_allowed = False
                expected_required = True
                expected_reasons = _ordered_unique(
                    present,
                    _REASON_ORDER,
                )
            else:
                if self.applicable_metric_count == 0:
                    ready = (
                        PolicyTransitionReasonCode
                        .POLICY_COMPLIANCE_NOT_APPLICABLE
                    )
                elif (
                    self.waived_metric_count > 0
                    or self.skipped_binding_count > 0
                ):
                    ready = (
                        PolicyTransitionReasonCode
                        .POLICY_COMPLIANCE_READY_WITH_WAIVERS
                    )
                elif (
                    self.applicable_blocking_metric_count == 0
                    or self.advisory_issue_count > 0
                ):
                    ready = (
                        PolicyTransitionReasonCode
                        .POLICY_COMPLIANCE_ADVISORY_ONLY
                    )
                else:
                    ready = (
                        PolicyTransitionReasonCode.POLICY_COMPLIANCE_READY
                    )
                expected_allowed = True
                expected_required = True
                expected_reasons = (ready,)
        if (
            self.allowed != expected_allowed
            or self.policy_compliance_required != expected_required
            or self.reason_codes != expected_reasons
        ):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_outcome_mismatch"
            )
        object.__setattr__(
            self,
            "fence_digest",
            _sha256(
                self.fence_digest,
                "policy_transition_decision_fence_digest_invalid",
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
                    "policy_compliance_required": (
                        self.policy_compliance_required
                    ),
                    "allowed": self.allowed,
                    "reason_codes": [
                        item.value for item in self.reason_codes
                    ],
                    "diagnostic_codes": [
                        item.value for item in self.diagnostic_codes
                    ],
                    "binding_decisions": [
                        item.to_payload() for item in self.binding_decisions
                    ],
                    "receipt_ids": list(self.receipt_ids),
                    "counts": {
                        "applicable_metrics": self.applicable_metric_count,
                        "applicable_blocking_metrics": (
                            self.applicable_blocking_metric_count
                        ),
                        "failed_metrics": self.failed_metric_count,
                        "blocking_metrics": self.blocking_metric_count,
                        "waived_metrics": self.waived_metric_count,
                        "advisory_issues": self.advisory_issue_count,
                        "skipped_bindings": self.skipped_binding_count,
                    },
                    "fence_digest": self.fence_digest,
                }
            ),
        )

    @property
    def reason_code(self) -> PolicyTransitionReasonCode:
        return self.reason_codes[0]

    @property
    def receipt_id(self) -> str | None:
        """Deprecated single-receipt compatibility seam."""

        return self.receipt_ids[0] if len(self.receipt_ids) == 1 else None

    @property
    def currentness(self) -> PolicyCurrentness | None:
        """Deprecated aggregate compatibility seam."""

        values = {
            item.currentness
            for item in self.binding_decisions
            if item.receipt_id is not None
        }
        if not values:
            return None
        if PolicyCurrentness.STALE in values:
            return PolicyCurrentness.STALE
        return PolicyCurrentness.CURRENT

    @property
    def currentness_reasons(
        self,
    ) -> tuple[SemanticAssessmentCurrentnessReason, ...]:
        present = {
            reason
            for item in self.binding_decisions
            for reason in item.currentness_reasons
        }
        return _ordered_unique(present, _CURRENTNESS_REASON_ORDER)

class PolicyTransitionRejected(GuidelinePolicyContractError):
    """Stable fail-closed error carrying the exact locked decision."""

    def __init__(self, decision: PolicyTransitionDecision) -> None:
        if not isinstance(decision, PolicyTransitionDecision):
            raise GuidelinePolicyContractError(
                "policy_transition_decision_invalid"
            )
        self.decision = decision
        self.reason_codes = decision.reason_codes
        self.diagnostic_codes = decision.diagnostic_codes
        self.decision_digest = decision.decision_digest
        self.fence_digest = decision.fence_digest
        self.receipt_ids = decision.receipt_ids
        self.applicable_metric_count = decision.applicable_metric_count
        self.applicable_blocking_metric_count = (
            decision.applicable_blocking_metric_count
        )
        self.failed_metric_count = decision.failed_metric_count
        self.blocking_metric_count = decision.blocking_metric_count
        super().__init__(decision.reason_code.value)

    @property
    def receipt_id(self) -> str | None:
        return self.decision.receipt_id

    @property
    def currentness(self) -> PolicyCurrentness | None:
        return self.decision.currentness

    @property
    def currentness_reasons(
        self,
    ) -> tuple[SemanticAssessmentCurrentnessReason, ...]:
        return self.decision.currentness_reasons

def _decision(
    snapshot: PolicyTransitionSnapshot,
    *,
    to_status: str,
    transition_allowed: bool,
    policy_compliance_required: bool,
    allowed: bool,
    reason_codes: tuple[PolicyTransitionReasonCode, ...],
) -> PolicyTransitionDecision:
    diagnostics = _ordered_unique(
        {
            diagnostic
            for item in snapshot.binding_decisions
            for diagnostic in item.diagnostic_codes
        },
        _DIAGNOSTIC_ORDER,
    )
    receipt_ids = tuple(
        sorted(
            {
                item.receipt_id
                for item in snapshot.binding_decisions
                if item.receipt_id is not None
            }
        )
    )
    return PolicyTransitionDecision(
        entity_type=snapshot.entity_type,
        subject_id=snapshot.subject_id,
        from_status=snapshot.expected_from_status,
        to_status=to_status,
        transition_allowed=transition_allowed,
        policy_compliance_required=policy_compliance_required,
        allowed=allowed,
        reason_codes=reason_codes,
        diagnostic_codes=diagnostics,
        binding_decisions=snapshot.binding_decisions,
        receipt_ids=receipt_ids,
        applicable_metric_count=snapshot.applicable_metric_count,
        applicable_blocking_metric_count=(
            snapshot.applicable_blocking_metric_count
        ),
        failed_metric_count=snapshot.failed_metric_count,
        blocking_metric_count=snapshot.blocking_metric_count,
        waived_metric_count=snapshot.waived_metric_count,
        advisory_issue_count=snapshot.advisory_issue_count,
        skipped_binding_count=snapshot.skipped_binding_count,
        fence_digest=snapshot.fence_digest,
    )


def evaluate_policy_transition(
    snapshot: PolicyTransitionSnapshot,
    target_status: str,
) -> PolicyTransitionDecision:
    """Evaluate one native edge without invoking cognitive work."""

    if not isinstance(snapshot, PolicyTransitionSnapshot):
        raise GuidelinePolicyContractError(
            "policy_transition_snapshot_invalid"
        )
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
            reason_codes=(
                PolicyTransitionReasonCode.TRANSITION_NOT_ALLOWED,
            ),
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
            reason_codes=(
                PolicyTransitionReasonCode
                .POLICY_COMPLIANCE_NOT_REQUIRED,
            ),
        )
    if not snapshot.subject_available:
        return _decision(
            snapshot,
            to_status=target_status,
            transition_allowed=True,
            policy_compliance_required=True,
            allowed=False,
            reason_codes=(
                PolicyTransitionReasonCode.POLICY_SUBJECT_REQUIRED,
            ),
        )

    blocking = tuple(
        item
        for item in snapshot.binding_decisions
        if (
            item.applicable_metric_count > 0
            and not item.allowed
        )
    )
    if blocking:
        present: set[PolicyTransitionReasonCode] = set()
        for item in blocking:
            diagnostics = set(item.diagnostic_codes)
            if (
                PolicyTransitionDiagnosticCode
                .POLICY_COMPLIANCE_RECEIPT_MISSING
                in diagnostics
            ):
                present.add(
                    PolicyTransitionReasonCode
                    .POLICY_COMPLIANCE_RECEIPT_MISSING
                )
            if (
                PolicyTransitionDiagnosticCode
                .POLICY_COMPLIANCE_RECEIPT_STALE
                in diagnostics
            ):
                present.add(
                    PolicyTransitionReasonCode
                    .POLICY_COMPLIANCE_RECEIPT_STALE
                )
            if (
                PolicyTransitionDiagnosticCode
                .POLICY_ASSESSMENT_UNAVAILABLE
                in diagnostics
            ):
                present.add(
                    PolicyTransitionReasonCode
                    .POLICY_ASSESSMENT_UNAVAILABLE
                )
            if diagnostics & {
                (
                    PolicyTransitionDiagnosticCode
                    .POLICY_ASSESSMENT_INADMISSIBLE
                ),
                (
                    PolicyTransitionDiagnosticCode
                    .POLICY_METRIC_THRESHOLD_FAILED
                ),
            }:
                present.add(
                    PolicyTransitionReasonCode.POLICY_COMPLIANCE_BLOCKED
                )
        return _decision(
            snapshot,
            to_status=target_status,
            transition_allowed=True,
            policy_compliance_required=True,
            allowed=False,
            reason_codes=_ordered_unique(present, _REASON_ORDER),
        )

    if snapshot.applicable_metric_count == 0:
        ready = (
            PolicyTransitionReasonCode
            .POLICY_COMPLIANCE_NOT_APPLICABLE
        )
    elif (
        snapshot.waived_metric_count > 0
        or snapshot.skipped_binding_count > 0
    ):
        ready = (
            PolicyTransitionReasonCode
            .POLICY_COMPLIANCE_READY_WITH_WAIVERS
        )
    elif (
        snapshot.applicable_blocking_metric_count == 0
        or snapshot.advisory_issue_count > 0
    ):
        ready = (
            PolicyTransitionReasonCode
            .POLICY_COMPLIANCE_ADVISORY_ONLY
        )
    else:
        ready = PolicyTransitionReasonCode.POLICY_COMPLIANCE_READY
    return _decision(
        snapshot,
        to_status=target_status,
        transition_allowed=True,
        policy_compliance_required=True,
        allowed=True,
        reason_codes=(ready,),
    )


def raise_for_policy_transition(
    decision: PolicyTransitionDecision,
) -> None:
    if not isinstance(decision, PolicyTransitionDecision):
        raise GuidelinePolicyContractError(
            "policy_transition_decision_invalid"
        )
    if not decision.allowed:
        raise PolicyTransitionRejected(decision)


def require_policy_transition_decision_match(
    expected_decision_digest: str,
    actual: PolicyTransitionDecision,
) -> None:
    expected = _sha256(
        expected_decision_digest,
        "policy_transition_expected_decision_digest_invalid",
    )
    if not isinstance(actual, PolicyTransitionDecision):
        raise GuidelinePolicyContractError(
            "policy_transition_decision_invalid"
        )
    if expected != actual.decision_digest:
        raise GuidelinePolicyContractError(
            "policy_transition_decision_changed"
        )


__all__ = [
    "POLICY_TRANSITION_CONTRACT_VERSION",
    "PolicyTransitionDecision",
    "PolicyTransitionDiagnosticCode",
    "PolicyTransitionReasonCode",
    "PolicyTransitionRejected",
    "PolicyTransitionSnapshot",
    "SemanticBindingComplianceDecision",
    "SemanticBindingComplianceSnapshot",
    "evaluate_policy_transition",
    "raise_for_policy_transition",
    "require_policy_transition_decision_match",
]
