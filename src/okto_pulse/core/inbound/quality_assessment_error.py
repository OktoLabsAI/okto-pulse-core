"""Shared REST/MCP error projection for SK-A quality assessments."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from okto_pulse.core.domain.quality_assessment import (
    QualityAssessmentContractError,
)
from okto_pulse.core.ports.quality_assessment import (
    AssessmentAuthorityConflict,
    AssessmentHeadRevisionConflict,
    AssessmentIdempotencyConflict,
    AssessmentInputDigestConflict,
    AssessmentReadAccessDenied,
    AssessmentReceiptNotFound,
    AssessmentSubjectLifecycleConflict,
    AssessmentSubjectNotFound,
    AssessmentSubjectStatusConflict,
    AssessmentSubjectVersionConflict,
    QualityAssessmentPersistenceError,
)
from okto_pulse.core.services.quality_assessment import (
    QualityAssessmentError,
    QualityAssessmentErrorCategory,
)

_NEXT_ACTION_BY_CATEGORY: dict[str, str] = {
    QualityAssessmentErrorCategory.INVALID_ARGUMENT.value: "fix_input",
    QualityAssessmentErrorCategory.UNPROCESSABLE.value: "fix_input",
    QualityAssessmentErrorCategory.FORBIDDEN.value: "request_authority",
    QualityAssessmentErrorCategory.NOT_FOUND.value: "verify_reference",
    QualityAssessmentErrorCategory.CONFLICT.value: "refresh_and_retry",
    QualityAssessmentErrorCategory.INTERNAL.value: "retry_or_report",
}
_PUBLIC_CODE_BY_CATEGORY: dict[str, str] = {
    QualityAssessmentErrorCategory.INVALID_ARGUMENT.value: "validation_failed",
    QualityAssessmentErrorCategory.UNPROCESSABLE.value: "validation_failed",
    QualityAssessmentErrorCategory.FORBIDDEN.value: "forbidden",
    QualityAssessmentErrorCategory.NOT_FOUND.value: "not_found",
    QualityAssessmentErrorCategory.CONFLICT.value: "version_conflict",
    QualityAssessmentErrorCategory.INTERNAL.value: "internal_error",
}
_PUBLIC_REASON_CODES = frozenset(
    {
        "invalid_pagination",
        "question_budget_exceeded",
    }
)


def project_quality_assessment_error(error: Exception) -> dict[str, Any]:
    """Return the canonical Quality error envelope shared by REST and MCP."""

    reason_code, message, category, retryable, details = _quality_error_fields(error)
    code = (
        reason_code
        if reason_code in _PUBLIC_REASON_CODES
        else _PUBLIC_CODE_BY_CATEGORY[category]
    )
    details = {"reason_code": reason_code, **details}
    return {
        "outcome": "error",
        "error": code,
        "code": code,
        "error_code": code,
        "message": message,
        "category": category,
        "retryable": retryable,
        "next_action": _NEXT_ACTION_BY_CATEGORY[category],
        "details": details,
    }


def _quality_error_fields(
    error: Exception,
) -> tuple[str, str, str, bool, dict[str, Any]]:
    if isinstance(error, QualityAssessmentError):
        raw_details = error.details
        return (
            error.code,
            error.message,
            error.category.value,
            error.retryable,
            dict(raw_details),
        )
    if isinstance(error, QualityAssessmentContractError):
        return (
            error.code,
            str(error),
            QualityAssessmentErrorCategory.UNPROCESSABLE.value,
            False,
            {},
        )
    if isinstance(error, (AssessmentSubjectNotFound, AssessmentReceiptNotFound)):
        return (
            error.code,
            str(error),
            QualityAssessmentErrorCategory.NOT_FOUND.value,
            False,
            {},
        )
    if isinstance(error, AssessmentReadAccessDenied):
        return (
            error.code,
            str(error),
            QualityAssessmentErrorCategory.FORBIDDEN.value,
            False,
            {},
        )
    if isinstance(error, AssessmentAuthorityConflict):
        return (
            error.code,
            str(error),
            QualityAssessmentErrorCategory.FORBIDDEN.value,
            False,
            {},
        )
    if isinstance(
        error,
        (
            AssessmentHeadRevisionConflict,
            AssessmentSubjectVersionConflict,
            AssessmentIdempotencyConflict,
            AssessmentInputDigestConflict,
            AssessmentSubjectStatusConflict,
            AssessmentSubjectLifecycleConflict,
        ),
    ):
        return (
            error.code,
            str(error),
            QualityAssessmentErrorCategory.CONFLICT.value,
            True,
            {},
        )
    if isinstance(error, QualityAssessmentPersistenceError):
        raw_details = getattr(error, "details", {})
        details = (
            dict(raw_details)
            if isinstance(raw_details, Mapping)
            else {}
        )
        return (
            error.code,
            str(error),
            QualityAssessmentErrorCategory.INTERNAL.value,
            True,
            details,
        )
    if isinstance(error, (TypeError, ValueError)):
        raw_reason = str(error).strip()
        reason_code = (
            raw_reason
            if (
                0 < len(raw_reason) <= 96
                and raw_reason == raw_reason.lower()
                and raw_reason.replace("_", "").isalnum()
            )
            else "quality_assessment_request_invalid"
        )
        return (
            reason_code,
            raw_reason,
            QualityAssessmentErrorCategory.INVALID_ARGUMENT.value,
            False,
            {},
        )
    raise TypeError("quality_assessment_error_type_unsupported")


__all__ = ["project_quality_assessment_error"]
