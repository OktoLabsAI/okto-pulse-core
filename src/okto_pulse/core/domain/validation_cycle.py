"""Human-readable validation-cycle contracts.

The summary contract deliberately separates product state from technical
evidence. Initial UI reads contain only the current lifecycle edition, a small
result summary, counts, and submission fences. Immutable receipts, digests and
technical versions are available only through the result-scoped audit read.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from okto_pulse.core.domain.quality_assessment import AssessmentSubjectType


VALIDATION_CYCLE_CONTRACT_VERSION = "human-validation-cycle/v1"

_STATUS_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TECHNICAL_SUMMARY_KEYS = frozenset(
    {
        "receipt_id",
        "head_revision",
        "subject_version",
        "digests",
        "stale_reasons",
    }
)


class ValidationCycleContractError(ValueError):
    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.message)


def _required_text(value: object, code: str, *, maximum: int = 4096) -> str:
    if not isinstance(value, str):
        raise ValidationCycleContractError(code)
    normalized = value.strip()
    if not normalized or len(normalized) > maximum:
        raise ValidationCycleContractError(code)
    return normalized


def _positive_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValidationCycleContractError(code)
    return value


def _non_negative_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValidationCycleContractError(code)
    return value


def _status(value: object, code: str) -> str:
    normalized = _required_text(value, code, maximum=64).lower()
    if not _STATUS_RE.fullmatch(normalized):
        raise ValidationCycleContractError(code)
    return normalized


def _sha256(value: object, code: str) -> str:
    normalized = _required_text(value, code, maximum=64).lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ValidationCycleContractError(code)
    return normalized


def _freeze_summary_value(value: object, code: str) -> object:
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str) or not raw_key.strip():
                raise ValidationCycleContractError(code)
            key = raw_key.strip()
            if key in _TECHNICAL_SUMMARY_KEYS:
                raise ValidationCycleContractError(
                    "validation_cycle_summary_contains_technical_audit"
                )
            normalized[key] = _freeze_summary_value(raw_value, code)
        return MappingProxyType(normalized)
    if isinstance(value, (tuple, list)):
        return tuple(_freeze_summary_value(item, code) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ValidationCycleContractError(code)


def _summary(value: object, code: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValidationCycleContractError(code)
    frozen = _freeze_summary_value(value, code)
    if not isinstance(frozen, Mapping):  # pragma: no cover - guarded above
        raise ValidationCycleContractError(code)
    return frozen


class ValidationCycleState(str, Enum):
    NOT_STARTED = "not_started"
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


@dataclass(frozen=True, slots=True)
class ValidationCycleSubjectRef:
    subject_type: AssessmentSubjectType
    subject_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.subject_type, AssessmentSubjectType):
            raise ValidationCycleContractError("validation_cycle_subject_type_invalid")
        object.__setattr__(
            self,
            "subject_id",
            _required_text(
                self.subject_id,
                "validation_cycle_subject_id_required",
                maximum=255,
            ),
        )


class ValidationCycleResultType(str, Enum):
    AMBIGUITY_ASSESSMENT = "ambiguity_assessment"
    SPEC_VALIDATION = "spec_validation"
    REQUIREMENT_LINT = "requirement_lint"
    CURATED_CHECKLIST = "curated_checklist"
    POLICY_COMPLIANCE = "policy_compliance"


class ValidationEditionExceptionType(str, Enum):
    AMBIGUITY_GATE_SKIP = "ambiguity_gate_skip"
    POLICY_SKIP = "policy_skip"
    POLICY_WAIVER = "policy_waiver"


VALIDATION_CHECK_RESULT_TYPES = (
    ValidationCycleResultType.REQUIREMENT_LINT,
    ValidationCycleResultType.CURATED_CHECKLIST,
    ValidationCycleResultType.POLICY_COMPLIANCE,
)

_SPEC_VALIDATION_SECTIONS = (
    ValidationCycleResultType.SPEC_VALIDATION,
    *VALIDATION_CHECK_RESULT_TYPES,
)

_ACTION_SECTION = {
    "record_requirement_lint": ValidationCycleResultType.REQUIREMENT_LINT,
    "complete_requirement_lint": ValidationCycleResultType.REQUIREMENT_LINT,
    "complete_curated_checklist": ValidationCycleResultType.CURATED_CHECKLIST,
    "complete_policy_compliance": ValidationCycleResultType.POLICY_COMPLIANCE,
    "submit_spec_validation": ValidationCycleResultType.SPEC_VALIDATION,
}


@dataclass(frozen=True, slots=True)
class ValidationSubmissionFence:
    expected_validation_edition: int
    expected_subject_version: int
    expected_head_revision: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "expected_validation_edition",
            _positive_int(
                self.expected_validation_edition,
                "validation_cycle_expected_edition_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expected_subject_version",
            _positive_int(
                self.expected_subject_version,
                "validation_cycle_expected_subject_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expected_head_revision",
            _non_negative_int(
                self.expected_head_revision,
                "validation_cycle_expected_head_revision_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class ValidationCycleResultSummary:
    result_id: str
    result_type: ValidationCycleResultType
    # Legacy evidence intentionally remains NULL/history-only.  Never infer or
    # backfill an edition merely to make an old record appear current.
    subject_edition: int | None
    status: str
    summary: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "result_id",
            _required_text(
                self.result_id,
                "validation_cycle_result_id_required",
                maximum=255,
            ),
        )
        if not isinstance(self.result_type, ValidationCycleResultType):
            raise ValidationCycleContractError("validation_cycle_result_type_invalid")
        if self.subject_edition is not None:
            object.__setattr__(
                self,
                "subject_edition",
                _positive_int(
                    self.subject_edition,
                    "validation_cycle_result_edition_invalid",
                ),
            )
        object.__setattr__(
            self,
            "status",
            _status(self.status, "validation_cycle_result_status_invalid"),
        )
        object.__setattr__(
            self,
            "summary",
            _summary(self.summary, "validation_cycle_result_summary_invalid"),
        )


@dataclass(frozen=True, slots=True)
class ValidationCycleCheckSummary:
    result_type: ValidationCycleResultType
    status: str
    summary: str

    def __post_init__(self) -> None:
        if self.result_type not in VALIDATION_CHECK_RESULT_TYPES:
            raise ValidationCycleContractError("validation_cycle_check_type_invalid")
        object.__setattr__(
            self,
            "status",
            _status(self.status, "validation_cycle_check_status_invalid"),
        )
        object.__setattr__(
            self,
            "summary",
            _required_text(
                self.summary,
                "validation_cycle_check_summary_invalid",
                maximum=4096,
            ),
        )


@dataclass(frozen=True, slots=True)
class ValidationCycleSummary:
    subject_type: AssessmentSubjectType
    subject_id: str
    edition: int
    status: str
    cycle_state: ValidationCycleState | None
    current_result: ValidationCycleResultSummary | None
    previous_result_count: int | None
    submission_fence: ValidationSubmissionFence | None
    visible_sections: tuple[ValidationCycleResultType, ...]
    previous_results: tuple[ValidationCycleResultSummary, ...] = ()
    checks: tuple[ValidationCycleCheckSummary, ...] = ()
    remaining_actions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.subject_type, AssessmentSubjectType):
            raise ValidationCycleContractError("validation_cycle_subject_type_invalid")
        object.__setattr__(
            self,
            "subject_id",
            _required_text(
                self.subject_id,
                "validation_cycle_subject_id_required",
                maximum=255,
            ),
        )
        edition = _positive_int(
            self.edition,
            "validation_cycle_edition_invalid",
        )
        object.__setattr__(self, "edition", edition)
        object.__setattr__(
            self,
            "status",
            _status(self.status, "validation_cycle_subject_status_invalid"),
        )
        default_sections = (
            _SPEC_VALIDATION_SECTIONS
            if self.subject_type is AssessmentSubjectType.SPEC
            else (ValidationCycleResultType.AMBIGUITY_ASSESSMENT,)
        )
        visible_sections = tuple(self.visible_sections)
        allowed_sections = set(default_sections)
        if (
            not visible_sections
            or len(set(visible_sections)) != len(visible_sections)
            or any(
                not isinstance(item, ValidationCycleResultType)
                or item not in allowed_sections
                for item in visible_sections
            )
            or tuple(item for item in default_sections if item in visible_sections)
            != visible_sections
        ):
            raise ValidationCycleContractError(
                "validation_cycle_visible_sections_invalid"
            )
        object.__setattr__(self, "visible_sections", visible_sections)

        primary_result_type = (
            ValidationCycleResultType.SPEC_VALIDATION
            if self.subject_type is AssessmentSubjectType.SPEC
            else ValidationCycleResultType.AMBIGUITY_ASSESSMENT
        )
        primary_visible = primary_result_type in visible_sections
        if primary_visible:
            if not isinstance(self.cycle_state, ValidationCycleState):
                raise ValidationCycleContractError("validation_cycle_state_invalid")
        elif self.cycle_state is not None:
            raise ValidationCycleContractError("validation_cycle_hidden_state_present")
        if self.current_result is not None:
            if not primary_visible:
                raise ValidationCycleContractError(
                    "validation_cycle_hidden_current_result_present"
                )
            if not isinstance(
                self.current_result,
                ValidationCycleResultSummary,
            ):
                raise ValidationCycleContractError(
                    "validation_cycle_current_result_invalid"
                )
            expected_type = (
                ValidationCycleResultType.SPEC_VALIDATION
                if self.subject_type is AssessmentSubjectType.SPEC
                else ValidationCycleResultType.AMBIGUITY_ASSESSMENT
            )
            if (
                self.current_result.subject_edition != edition
                or self.current_result.result_type is not expected_type
            ):
                raise ValidationCycleContractError(
                    "validation_cycle_current_result_scope_mismatch"
                )
        previous_results = tuple(self.previous_results)
        if any(
            not isinstance(item, ValidationCycleResultSummary)
            for item in previous_results
        ):
            raise ValidationCycleContractError(
                "validation_cycle_previous_result_invalid"
            )
        if any(
            item.result_type is not primary_result_type for item in previous_results
        ):
            raise ValidationCycleContractError(
                "validation_cycle_previous_result_scope_mismatch"
            )
        if not primary_visible and previous_results:
            raise ValidationCycleContractError(
                "validation_cycle_hidden_previous_results_present"
            )
        object.__setattr__(self, "previous_results", previous_results)
        if primary_visible:
            count = _non_negative_int(
                self.previous_result_count,
                "validation_cycle_previous_count_invalid",
            )
            if count < len(previous_results):
                raise ValidationCycleContractError(
                    "validation_cycle_previous_count_mismatch"
                )
            object.__setattr__(self, "previous_result_count", count)
            if not isinstance(self.submission_fence, ValidationSubmissionFence):
                raise ValidationCycleContractError(
                    "validation_cycle_submission_fence_invalid"
                )
            if self.submission_fence.expected_validation_edition != edition:
                raise ValidationCycleContractError(
                    "validation_cycle_submission_fence_edition_mismatch"
                )
        elif self.previous_result_count is not None:
            raise ValidationCycleContractError(
                "validation_cycle_hidden_previous_count_present"
            )
        elif self.submission_fence is not None:
            raise ValidationCycleContractError(
                "validation_cycle_hidden_submission_fence_present"
            )
        checks = tuple(self.checks)
        if any(not isinstance(item, ValidationCycleCheckSummary) for item in checks):
            raise ValidationCycleContractError("validation_cycle_check_invalid")
        if self.subject_type is AssessmentSubjectType.SPEC:
            expected_checks = tuple(
                item
                for item in VALIDATION_CHECK_RESULT_TYPES
                if item in visible_sections
            )
            if tuple(item.result_type for item in checks) != expected_checks:
                raise ValidationCycleContractError(
                    "validation_cycle_spec_checks_visibility_mismatch"
                )
        elif checks:
            raise ValidationCycleContractError("validation_cycle_checks_not_supported")
        object.__setattr__(self, "checks", checks)
        remaining_actions = tuple(
            _required_text(
                item,
                "validation_cycle_remaining_action_invalid",
                maximum=255,
            )
            for item in self.remaining_actions
        )
        if len(set(remaining_actions)) != len(remaining_actions):
            raise ValidationCycleContractError(
                "validation_cycle_remaining_action_duplicate"
            )
        if self.subject_type is not AssessmentSubjectType.SPEC and remaining_actions:
            raise ValidationCycleContractError(
                "validation_cycle_remaining_actions_not_supported"
            )
        if any(
            _ACTION_SECTION.get(action) not in visible_sections
            for action in remaining_actions
        ):
            raise ValidationCycleContractError(
                "validation_cycle_remaining_action_hidden"
            )
        object.__setattr__(self, "remaining_actions", remaining_actions)


@dataclass(frozen=True, slots=True)
class ValidationEditionExceptionAudit:
    """Immutable, edition-bound human override/exception evidence."""

    exception_id: str
    exception_type: ValidationEditionExceptionType
    subject_edition: int
    status: str
    reason: str
    actor_id: str
    recorded_at: datetime

    def __post_init__(self) -> None:
        for field_name in ("exception_id", "reason", "actor_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"validation_exception_{field_name}_required",
                    maximum=4096 if field_name == "reason" else 255,
                ),
            )
        if not isinstance(self.exception_type, ValidationEditionExceptionType):
            raise ValidationCycleContractError("validation_exception_type_invalid")
        object.__setattr__(
            self,
            "subject_edition",
            _positive_int(
                self.subject_edition,
                "validation_exception_subject_edition_invalid",
            ),
        )
        object.__setattr__(
            self,
            "status",
            _status(self.status, "validation_exception_status_invalid"),
        )
        if (
            not isinstance(self.recorded_at, datetime)
            or self.recorded_at.tzinfo is None
            or self.recorded_at.utcoffset() is None
        ):
            raise ValidationCycleContractError(
                "validation_exception_recorded_at_invalid"
            )
        object.__setattr__(
            self,
            "recorded_at",
            self.recorded_at.astimezone(timezone.utc),
        )


@dataclass(frozen=True, slots=True)
class ValidationTechnicalAuditDetails:
    receipt_id: str
    subject_version: int
    head_revision: int
    digests: Mapping[str, str]
    visible_exception_types: tuple[ValidationEditionExceptionType, ...]
    exceptions: tuple[ValidationEditionExceptionAudit, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "receipt_id",
            _required_text(
                self.receipt_id,
                "validation_audit_receipt_id_required",
                maximum=255,
            ),
        )
        object.__setattr__(
            self,
            "subject_version",
            _positive_int(
                self.subject_version,
                "validation_audit_subject_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "head_revision",
            _positive_int(
                self.head_revision,
                "validation_audit_head_revision_invalid",
            ),
        )
        if not isinstance(self.digests, Mapping):
            raise ValidationCycleContractError("validation_audit_digests_invalid")
        digests = {
            _required_text(
                key,
                "validation_audit_digest_name_invalid",
                maximum=64,
            ): _sha256(value, "validation_audit_digest_invalid")
            for key, value in self.digests.items()
        }
        object.__setattr__(self, "digests", MappingProxyType(digests))
        visible_exception_types = tuple(self.visible_exception_types)
        if (
            len(set(visible_exception_types)) != len(visible_exception_types)
            or any(
                not isinstance(item, ValidationEditionExceptionType)
                for item in visible_exception_types
            )
            or tuple(
                item
                for item in ValidationEditionExceptionType
                if item in visible_exception_types
            )
            != visible_exception_types
        ):
            raise ValidationCycleContractError(
                "validation_audit_visible_exception_types_invalid"
            )
        object.__setattr__(
            self,
            "visible_exception_types",
            visible_exception_types,
        )
        exceptions = tuple(self.exceptions)
        if any(
            not isinstance(item, ValidationEditionExceptionAudit) for item in exceptions
        ):
            raise ValidationCycleContractError("validation_audit_exception_invalid")
        if len({item.exception_id for item in exceptions}) != len(exceptions):
            raise ValidationCycleContractError("validation_audit_exception_duplicate")
        if any(
            item.exception_type not in visible_exception_types for item in exceptions
        ):
            raise ValidationCycleContractError(
                "validation_audit_hidden_exception_present"
            )
        object.__setattr__(self, "exceptions", exceptions)


@dataclass(frozen=True, slots=True)
class ValidationTechnicalAudit:
    subject_type: AssessmentSubjectType
    subject_id: str
    result_id: str
    result_type: ValidationCycleResultType
    subject_edition: int | None
    technical_audit: ValidationTechnicalAuditDetails

    def __post_init__(self) -> None:
        if not isinstance(self.subject_type, AssessmentSubjectType):
            raise ValidationCycleContractError("validation_audit_subject_type_invalid")
        object.__setattr__(
            self,
            "subject_id",
            _required_text(
                self.subject_id,
                "validation_audit_subject_id_required",
                maximum=255,
            ),
        )
        object.__setattr__(
            self,
            "result_id",
            _required_text(
                self.result_id,
                "validation_audit_result_id_required",
                maximum=255,
            ),
        )
        if not isinstance(self.result_type, ValidationCycleResultType):
            raise ValidationCycleContractError("validation_audit_result_type_invalid")
        if self.subject_edition is not None:
            object.__setattr__(
                self,
                "subject_edition",
                _positive_int(
                    self.subject_edition,
                    "validation_audit_subject_edition_invalid",
                ),
            )
        if not isinstance(self.technical_audit, ValidationTechnicalAuditDetails):
            raise ValidationCycleContractError("validation_audit_details_invalid")
        if any(
            item.subject_edition != self.subject_edition
            for item in self.technical_audit.exceptions
        ):
            raise ValidationCycleContractError(
                "validation_audit_exception_edition_mismatch"
            )


@dataclass(frozen=True, slots=True)
class RequirementLintAnchor:
    anchor_type: str
    anchor_ref: str | None = None
    excerpt_hash: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "anchor_type",
            _status(self.anchor_type, "requirement_lint_anchor_type_invalid"),
        )
        if self.anchor_ref is not None:
            object.__setattr__(
                self,
                "anchor_ref",
                _required_text(
                    self.anchor_ref,
                    "requirement_lint_anchor_ref_invalid",
                    maximum=4096,
                ),
            )
        if self.excerpt_hash is not None:
            object.__setattr__(
                self,
                "excerpt_hash",
                _sha256(
                    self.excerpt_hash,
                    "requirement_lint_anchor_excerpt_hash_invalid",
                ),
            )


@dataclass(frozen=True, slots=True)
class RequirementLintPreflight:
    subject_edition: int
    subject_status: str
    ruleset_digest: str
    requirement_anchors: tuple[RequirementLintAnchor, ...]
    submission_fence: ValidationSubmissionFence
    assessment_kind: str = "requirement_lint"

    def __post_init__(self) -> None:
        if self.assessment_kind != "requirement_lint":
            raise ValidationCycleContractError(
                "requirement_lint_assessment_kind_invalid"
            )
        edition = _positive_int(
            self.subject_edition,
            "requirement_lint_subject_edition_invalid",
        )
        object.__setattr__(self, "subject_edition", edition)
        if self.subject_status != "approved":
            raise ValidationCycleContractError("requirement_lint_subject_not_approved")
        object.__setattr__(
            self,
            "ruleset_digest",
            _sha256(
                self.ruleset_digest,
                "requirement_lint_ruleset_digest_invalid",
            ),
        )
        anchors = tuple(self.requirement_anchors)
        if any(not isinstance(item, RequirementLintAnchor) for item in anchors):
            raise ValidationCycleContractError("requirement_lint_anchor_invalid")
        if len(
            {(item.anchor_type, item.anchor_ref, item.excerpt_hash) for item in anchors}
        ) != len(anchors):
            raise ValidationCycleContractError("requirement_lint_anchor_duplicate")
        object.__setattr__(self, "requirement_anchors", anchors)
        if not isinstance(self.submission_fence, ValidationSubmissionFence):
            raise ValidationCycleContractError(
                "requirement_lint_submission_fence_invalid"
            )
        if self.submission_fence.expected_validation_edition != edition:
            raise ValidationCycleContractError(
                "requirement_lint_submission_fence_edition_mismatch"
            )


__all__ = [
    "VALIDATION_CHECK_RESULT_TYPES",
    "VALIDATION_CYCLE_CONTRACT_VERSION",
    "RequirementLintAnchor",
    "RequirementLintPreflight",
    "ValidationCycleCheckSummary",
    "ValidationCycleContractError",
    "ValidationCycleResultSummary",
    "ValidationCycleResultType",
    "ValidationCycleState",
    "ValidationCycleSubjectRef",
    "ValidationCycleSummary",
    "ValidationEditionExceptionAudit",
    "ValidationEditionExceptionType",
    "ValidationSubmissionFence",
    "ValidationTechnicalAudit",
    "ValidationTechnicalAuditDetails",
]
