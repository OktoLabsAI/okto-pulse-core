"""Pure currentness for immutable semantic guideline assessments.

Currentness is determined exclusively from normative subject, guideline, and
binding fences.  Assessor and model metadata remain auditable on the receipt
but deliberately do not participate in this comparison.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_BINDING_ID_MAX_LENGTH,
    GUIDELINE_ID_MAX_LENGTH,
    GUIDELINE_REVISION_ID_MAX_LENGTH,
    BoardGuidelineBinding,
    GuidelineBindingState,
    GuidelineRevision,
    PolicyCurrentness,
    PolicySubjectRef,
    PolicySubjectSnapshot,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentContractError,
    SemanticGuidelineAssessmentContext,
    SemanticGuidelineAssessmentReceipt,
    semantic_assessment_input_digest_v1,
)
from okto_pulse.core.domain.guideline_semantic_findings_v2 import (
    SemanticAssessmentReceiptProjectionV2,
)


SEMANTIC_ASSESSMENT_CURRENTNESS_CONTRACT_VERSION = (
    "semantic-guideline-assessment-currentness/v1"
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _sha256(value: object, code: str) -> str:
    if not isinstance(value, str):
        raise SemanticAssessmentContractError(code)
    normalized = value.strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise SemanticAssessmentContractError(code)
    return normalized


def _positive_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise SemanticAssessmentContractError(code)
    return value


def _subject_identity(subject: PolicySubjectRef) -> tuple[str, object, str]:
    return (subject.board_id, subject.entity_type, subject.subject_id)


class SemanticAssessmentCurrentnessReason(str, Enum):
    """Ordered, closed reasons why one recorded receipt is stale."""

    CURRENT_SNAPSHOT_MISSING = "current_snapshot_missing"
    SUBJECT_EDITION_CHANGED = "subject_edition_changed"
    SUBJECT_VERSION_CHANGED = "subject_version_changed"
    SUBJECT_CONTENT_CHANGED = "subject_content_changed"
    GUIDELINE_REVISION_CHANGED = "guideline_revision_changed"
    GUIDELINE_REVISION_DIGEST_CHANGED = "guideline_revision_digest_changed"
    BINDING_REVISION_CHANGED = "binding_revision_changed"
    BINDING_CONFIGURATION_CHANGED = "binding_configuration_changed"
    POLICY_SET_CHANGED = "policy_set_changed"
    BINDING_HEAD_CHANGED = "binding_head_changed"
    INPUT_DIGEST_CHANGED = "input_digest_changed"


_CURRENTNESS_REASON_ORDER = tuple(SemanticAssessmentCurrentnessReason)


@dataclass(frozen=True, slots=True)
class SemanticAssessmentCurrentSnapshot:
    """Authoritative live fences for one subject×binding assessment."""

    subject: PolicySubjectRef
    subject_content_digest: str
    guideline_id: str
    guideline_revision_id: str
    guideline_revision_digest: str
    binding_id: str
    binding_revision: int
    binding_configuration_digest: str
    policy_set_digest: str
    binding_head_digest: str
    input_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_current_snapshot_subject_invalid"
            )
        for field_name, max_length in (
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("guideline_revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
            ("binding_id", GUIDELINE_BINDING_ID_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"semantic_current_snapshot_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "binding_revision",
            _positive_int(
                self.binding_revision,
                "semantic_current_snapshot_binding_revision_invalid",
            ),
        )
        for field_name in (
            "subject_content_digest",
            "guideline_revision_digest",
            "binding_configuration_digest",
            "policy_set_digest",
            "binding_head_digest",
            "input_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"semantic_current_snapshot_{field_name}_invalid",
                ),
            )


@dataclass(frozen=True, slots=True)
class SemanticAssessmentCurrentness:
    """Derived current/stale state with deterministic ordered reasons."""

    receipt_id: str
    currentness: PolicyCurrentness
    reasons: tuple[SemanticAssessmentCurrentnessReason, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "receipt_id",
            normalize_policy_bounded_text(
                self.receipt_id,
                max_length=64,
                code="semantic_currentness_receipt_id_required",
            ),
        )
        if not isinstance(self.currentness, PolicyCurrentness):
            raise SemanticAssessmentContractError(
                "semantic_currentness_state_invalid"
            )
        if not isinstance(self.reasons, tuple) or any(
            not isinstance(reason, SemanticAssessmentCurrentnessReason)
            for reason in self.reasons
        ):
            raise SemanticAssessmentContractError(
                "semantic_currentness_reasons_invalid"
            )
        if len(set(self.reasons)) != len(self.reasons):
            raise SemanticAssessmentContractError(
                "semantic_currentness_reasons_duplicate"
            )
        ordered = tuple(
            reason for reason in _CURRENTNESS_REASON_ORDER if reason in self.reasons
        )
        if self.reasons != ordered:
            raise SemanticAssessmentContractError(
                "semantic_currentness_reasons_order_invalid"
            )
        if (self.currentness is PolicyCurrentness.CURRENT) != (not self.reasons):
            raise SemanticAssessmentContractError(
                "semantic_currentness_state_inconsistent"
            )

    @property
    def is_current(self) -> bool:
        return self.currentness is PolicyCurrentness.CURRENT


def semantic_assessment_current_snapshot_from_context(
    context: SemanticGuidelineAssessmentContext,
) -> SemanticAssessmentCurrentSnapshot:
    """Project one admission context into the normative live fence contract."""

    if not isinstance(context, SemanticGuidelineAssessmentContext):
        raise SemanticAssessmentContractError(
            "semantic_assessment_context_invalid"
        )
    return SemanticAssessmentCurrentSnapshot(
        subject=context.subject_snapshot.subject,
        subject_content_digest=context.subject_snapshot.content_digest,
        guideline_id=context.revision.guideline_id,
        guideline_revision_id=context.revision.revision_id,
        guideline_revision_digest=context.revision.revision_digest,
        binding_id=context.binding.binding_id,
        binding_revision=context.binding.binding_revision,
        binding_configuration_digest=context.binding.configuration_digest,
        policy_set_digest=context.policy_set_digest,
        binding_head_digest=context.binding_head_digest,
        input_digest=semantic_assessment_input_digest_v1(context),
    )


def assess_semantic_assessment_currentness(
    receipt: SemanticGuidelineAssessmentReceipt,
    current: SemanticAssessmentCurrentSnapshot | None,
) -> SemanticAssessmentCurrentness:
    """Compare every normative fence without consulting model metadata."""

    if not isinstance(receipt, SemanticGuidelineAssessmentReceipt):
        raise SemanticAssessmentContractError(
            "semantic_currentness_receipt_invalid"
        )
    if current is None:
        return SemanticAssessmentCurrentness(
            receipt_id=receipt.receipt_id,
            currentness=PolicyCurrentness.STALE,
            reasons=(
                SemanticAssessmentCurrentnessReason.CURRENT_SNAPSHOT_MISSING,
            ),
        )
    if not isinstance(current, SemanticAssessmentCurrentSnapshot):
        raise SemanticAssessmentContractError(
            "semantic_current_snapshot_invalid"
        )
    if _subject_identity(receipt.subject) != _subject_identity(current.subject):
        raise SemanticAssessmentContractError(
            "semantic_currentness_subject_scope_mismatch"
        )
    if (
        receipt.guideline_id != current.guideline_id
        or receipt.binding_id != current.binding_id
    ):
        raise SemanticAssessmentContractError(
            "semantic_currentness_binding_scope_mismatch"
        )

    if current.subject.subject_edition is not None:
        reasons = (
            ()
            if receipt.subject.subject_edition == current.subject.subject_edition
            else (
                SemanticAssessmentCurrentnessReason.SUBJECT_EDITION_CHANGED,
            )
        )
        return SemanticAssessmentCurrentness(
            receipt_id=receipt.receipt_id,
            currentness=(
                PolicyCurrentness.CURRENT
                if not reasons
                else PolicyCurrentness.STALE
            ),
            reasons=reasons,
        )

    present: set[SemanticAssessmentCurrentnessReason] = set()
    if receipt.subject.subject_version != current.subject.subject_version:
        present.add(
            SemanticAssessmentCurrentnessReason.SUBJECT_VERSION_CHANGED
        )
    if receipt.subject_content_digest != current.subject_content_digest:
        present.add(
            SemanticAssessmentCurrentnessReason.SUBJECT_CONTENT_CHANGED
        )
    if receipt.guideline_revision_id != current.guideline_revision_id:
        present.add(
            SemanticAssessmentCurrentnessReason.GUIDELINE_REVISION_CHANGED
        )
    if (
        receipt.guideline_revision_digest
        != current.guideline_revision_digest
    ):
        present.add(
            SemanticAssessmentCurrentnessReason
            .GUIDELINE_REVISION_DIGEST_CHANGED
        )
    if receipt.binding_revision != current.binding_revision:
        present.add(
            SemanticAssessmentCurrentnessReason.BINDING_REVISION_CHANGED
        )
    if (
        receipt.binding_configuration_digest
        != current.binding_configuration_digest
    ):
        present.add(
            SemanticAssessmentCurrentnessReason
            .BINDING_CONFIGURATION_CHANGED
        )
    if receipt.policy_set_digest != current.policy_set_digest:
        present.add(
            SemanticAssessmentCurrentnessReason.POLICY_SET_CHANGED
        )
    if receipt.binding_head_digest != current.binding_head_digest:
        present.add(
            SemanticAssessmentCurrentnessReason.BINDING_HEAD_CHANGED
        )
    if receipt.input_digest != current.input_digest:
        present.add(
            SemanticAssessmentCurrentnessReason.INPUT_DIGEST_CHANGED
        )
    reasons = tuple(
        reason for reason in _CURRENTNESS_REASON_ORDER if reason in present
    )
    return SemanticAssessmentCurrentness(
        receipt_id=receipt.receipt_id,
        currentness=(
            PolicyCurrentness.STALE
            if reasons
            else PolicyCurrentness.CURRENT
        ),
        reasons=reasons,
    )


def assess_native_semantic_assessment_currentness(
    receipt: SemanticAssessmentReceiptProjectionV2,
    *,
    subject: PolicySubjectSnapshot | None,
    binding: BoardGuidelineBinding | None = None,
    revision: GuidelineRevision | None = None,
) -> SemanticAssessmentCurrentness:
    """Apply native edition authority, or the technical fences of an uneditioned subject."""
    if not isinstance(receipt, SemanticAssessmentReceiptProjectionV2):
        raise SemanticAssessmentContractError("semantic_currentness_receipt_invalid")
    present: set[SemanticAssessmentCurrentnessReason] = set()
    reason = SemanticAssessmentCurrentnessReason
    if subject is None:
        present.add(reason.CURRENT_SNAPSHOT_MISSING)
    else:
        if _subject_identity(receipt.subject) != _subject_identity(subject.subject):
            raise SemanticAssessmentContractError("semantic_currentness_subject_scope_mismatch")
        if subject.subject.subject_edition is not None:
            if receipt.subject.subject_edition != subject.subject.subject_edition:
                present.add(reason.SUBJECT_EDITION_CHANGED)
        else:
            if receipt.subject.subject_version != subject.subject.subject_version:
                present.add(reason.SUBJECT_VERSION_CHANGED)
            if receipt.subject_content_digest != subject.content_digest:
                present.add(reason.SUBJECT_CONTENT_CHANGED)
            if binding is None or revision is None or binding.state is not GuidelineBindingState.ACTIVE:
                present.add(reason.CURRENT_SNAPSHOT_MISSING)
            else:
                if (binding.board_id != receipt.subject.board_id
                    or binding.binding_id != receipt.binding_id
                    or binding.guideline_id != receipt.guideline_id
                    or revision.guideline_id != receipt.guideline_id):
                    raise SemanticAssessmentContractError("semantic_currentness_binding_scope_mismatch")
                if receipt.guideline_revision_id != revision.revision_id:
                    present.add(reason.GUIDELINE_REVISION_CHANGED)
                if receipt.guideline_revision_digest != revision.revision_digest:
                    present.add(reason.GUIDELINE_REVISION_DIGEST_CHANGED)
                if receipt.binding_revision != binding.binding_revision:
                    present.add(reason.BINDING_REVISION_CHANGED)
                if receipt.binding_configuration_digest != binding.configuration_digest:
                    present.add(reason.BINDING_CONFIGURATION_CHANGED)
    return SemanticAssessmentCurrentness(
        receipt_id=receipt.receipt_id,
        currentness=PolicyCurrentness.STALE if present else PolicyCurrentness.CURRENT,
        reasons=tuple(item for item in _CURRENTNESS_REASON_ORDER if item in present),
    )


__all__ = [
    "SEMANTIC_ASSESSMENT_CURRENTNESS_CONTRACT_VERSION",
    "SemanticAssessmentCurrentSnapshot",
    "SemanticAssessmentCurrentness",
    "SemanticAssessmentCurrentnessReason",
    "assess_semantic_assessment_currentness",
    "assess_native_semantic_assessment_currentness",
    "semantic_assessment_current_snapshot_from_context",
]
