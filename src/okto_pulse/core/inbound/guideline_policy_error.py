"""Closed REST/MCP error projection for guideline-policy operations.

The projector exposes semantic status categories instead of HTTP primitives.
REST may map those categories to 400/403/404/409/503 while MCP preserves the
same code and retry guidance.  Messages are deliberately fixed and details are
bounded so exception text, database values, secrets, and stack information
cannot cross the inbound boundary.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any

from okto_pulse.core.application.use_cases.base import (
    CommandValidationError,
    ConflictError,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.policy_governance import (
    GuidelineRevisionUnderBump,
)
from okto_pulse.core.domain.guideline_import_export import (
    GuidelineImportExportError,
)
from okto_pulse.core.domain.guideline_policy import GuidelinePolicyContractError
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentInadmissibleError,
    SemanticAssessmentInadmissibilityCause,
)
from okto_pulse.core.inbound.guideline_policy_cursor import (
    GuidelinePolicyCursorConfigurationError,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyAdapterMissing,
    GuidelinePolicyBindingConflict,
    GuidelinePolicyCasConflict,
    GuidelinePolicyCursorConflict,
    GuidelinePolicyDigestConflict,
    GuidelinePolicyEditionConflict,
    GuidelinePolicyHeadConflict,
    GuidelinePolicyIdempotencyConflict,
    GuidelinePolicyLifecycleConflict,
    GuidelinePolicyPersistenceError,
    GuidelinePolicyRevisionConflict,
    GuidelinePolicySubjectConflict,
    GuidelinePolicyVersionConflict,
)
from okto_pulse.core.ports.semantic_subject_projection import (
    SemanticAssessmentV2WriterUnavailable,
    SemanticSubjectProjectionError,
    SemanticSubjectProjectionFailure,
)


class GuidelinePolicyErrorCategory(str, Enum):
    """Transport-neutral categories with one deterministic HTTP mapping."""

    INVALID_ARGUMENT = "invalid_argument"
    PERMISSION_DENIED = "permission_denied"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    UNPROCESSABLE_ENTITY = "unprocessable_entity"
    SERVICE_UNAVAILABLE = "service_unavailable"


GUIDELINE_POLICY_HTTP_STATUS_BY_CATEGORY: dict[
    GuidelinePolicyErrorCategory,
    int,
] = {
    GuidelinePolicyErrorCategory.INVALID_ARGUMENT: 400,
    GuidelinePolicyErrorCategory.PERMISSION_DENIED: 403,
    GuidelinePolicyErrorCategory.NOT_FOUND: 404,
    GuidelinePolicyErrorCategory.CONFLICT: 409,
    GuidelinePolicyErrorCategory.UNPROCESSABLE_ENTITY: 422,
    GuidelinePolicyErrorCategory.SERVICE_UNAVAILABLE: 503,
}

_SAFE_REASON_CODE = re.compile(r"^[a-z][a-z0-9_]{0,95}$")
_SAFE_SEMANTIC_VERSION = re.compile(r"^[0-9A-Za-z][0-9A-Za-z.+-]{0,63}$")
_CONFLICT_ERRORS = (
    ConflictError,
    GuidelinePolicyBindingConflict,
    GuidelinePolicyCasConflict,
    GuidelinePolicyDigestConflict,
    GuidelinePolicyEditionConflict,
    GuidelinePolicyHeadConflict,
    GuidelinePolicyIdempotencyConflict,
    GuidelinePolicyLifecycleConflict,
    GuidelinePolicyRevisionConflict,
    GuidelinePolicySubjectConflict,
    GuidelinePolicyVersionConflict,
)
_PUBLIC_BY_CATEGORY: dict[
    GuidelinePolicyErrorCategory,
    tuple[str, str, bool, str],
] = {
    GuidelinePolicyErrorCategory.INVALID_ARGUMENT: (
        "validation_failed",
        "Guideline policy request validation failed.",
        False,
        "fix_input",
    ),
    GuidelinePolicyErrorCategory.PERMISSION_DENIED: (
        "permission_denied",
        "Permission denied for this guideline policy operation.",
        False,
        "request_authority",
    ),
    GuidelinePolicyErrorCategory.NOT_FOUND: (
        "not_found",
        "The requested guideline policy resource was not found.",
        False,
        "verify_reference",
    ),
    GuidelinePolicyErrorCategory.CONFLICT: (
        "conflict",
        "The guideline policy operation conflicts with current state.",
        True,
        "refresh_and_retry",
    ),
    GuidelinePolicyErrorCategory.UNPROCESSABLE_ENTITY: (
        "semantic_anchor_missing",
        "The semantic anchor could not be resolved on the current subject.",
        False,
        "refresh_subject_and_pinpoint",
    ),
    GuidelinePolicyErrorCategory.SERVICE_UNAVAILABLE: (
        "service_unavailable",
        "Guideline policy service is unavailable.",
        True,
        "retry_or_report",
    ),
}


def project_guideline_policy_error(error: Exception) -> dict[str, Any]:
    """Return the canonical bounded envelope shared by REST and MCP."""

    category, reason_code, extra_details = _classify(error)
    code, message, retryable, next_action = _PUBLIC_BY_CATEGORY[category]
    if isinstance(error, SemanticSubjectProjectionError):
        if error.reason is SemanticSubjectProjectionFailure.FORBIDDEN:
            code = "semantic_anchor_forbidden"
            message = "The semantic anchor is not accessible to this actor."
            next_action = "request_authority"
        elif error.reason is SemanticSubjectProjectionFailure.MALFORMED:
            code = "semantic_assessment_contract_invalid"
            message = "The semantic assessment anchor contract is invalid."
            next_action = "fix_input"
    if isinstance(error, SemanticAssessmentV2WriterUnavailable):
        code = error.code
        message = (
            "Semantic assessment v2 writes are disabled."
            if error.code == "unsupported_contract_version"
            else "Semantic assessment v2 writer prerequisites are not ready."
        )
        retryable = error.code == "v2_writer_not_ready"
        next_action = (
            "enable_contract_version"
            if error.code == "unsupported_contract_version"
            else "complete_readers_first_rollout"
        )
    if isinstance(error, GuidelineRevisionUnderBump):
        code = "under_bump"
        message = "The declared semantic version is below the required minimum."
        retryable = False
        next_action = "increase_semantic_version"
    if isinstance(error, SemanticAssessmentInadmissibleError):
        code = "policy_assessment_inadmissible"
        retryable = False
        if error.cause == (
            SemanticAssessmentInadmissibilityCause.ASSESSOR_SEPARATION_REQUIRED.value
        ):
            message = (
                "A blocking assessment requires an assessor independent from "
                "the subject's last semantic editor."
            )
            next_action = "request_independent_assessor"
        else:
            message = "Assessment confidence is below the active binding minimum."
            next_action = "reassess_with_sufficient_confidence"
    if isinstance(error, GuidelinePolicyCursorConflict) or reason_code == (
        "invalid_cursor"
    ):
        code = "invalid_cursor"
        message = "The cursor is invalid or does not match this request."
        retryable = False
        next_action = "restart_pagination"
    if reason_code == "guideline_impact_preview_required":
        code = "guideline_impact_preview_required"
        message = (
            "Preview guideline impact before adopting a revision or changing "
            "its board priority."
        )
        retryable = False
        next_action = "preview_then_adopt"
    elif reason_code == "guideline_impact_no_changes":
        code = "guideline_impact_no_changes"
        message = (
            "This guideline is already configured with the requested revision "
            "and board settings."
        )
        retryable = False
        next_action = "no_action_required"
    details: dict[str, str] = {}
    if reason_code is not None:
        details["reason_code"] = reason_code
    details.update(extra_details)
    return {
        "outcome": "error",
        "error": code,
        "code": code,
        "error_code": code,
        "message": message,
        "category": category.value,
        "status_category": category.value,
        "http_status": GUIDELINE_POLICY_HTTP_STATUS_BY_CATEGORY[category],
        "retryable": retryable,
        "next_action": next_action,
        "details": details,
    }


def guideline_policy_http_status(error: Exception) -> int:
    """Return the intended REST status without importing a web framework."""

    category, _, _ = _classify(error)
    return GUIDELINE_POLICY_HTTP_STATUS_BY_CATEGORY[category]


def _classify(
    error: Exception,
) -> tuple[GuidelinePolicyErrorCategory, str | None, dict[str, str]]:
    reason_code = _safe_reason_code(getattr(error, "code", None))
    if reason_code is None and isinstance(error, ValueError):
        reason_code = _safe_reason_code(str(error))

    if isinstance(error, SemanticSubjectProjectionError):
        if error.reason is SemanticSubjectProjectionFailure.FORBIDDEN:
            return (
                GuidelinePolicyErrorCategory.PERMISSION_DENIED,
                "semantic_anchor_forbidden",
                {},
            )
        if error.reason is SemanticSubjectProjectionFailure.MISSING:
            return (
                GuidelinePolicyErrorCategory.UNPROCESSABLE_ENTITY,
                "semantic_anchor_missing",
                {},
            )
        return (
            GuidelinePolicyErrorCategory.INVALID_ARGUMENT,
            "semantic_assessment_contract_invalid",
            {},
        )

    if isinstance(error, GuidelinePolicyCursorConflict) or reason_code == (
        "invalid_cursor"
    ):
        return (
            GuidelinePolicyErrorCategory.INVALID_ARGUMENT,
            "invalid_cursor",
            {},
        )
    if isinstance(error, PermissionDeniedError):
        return (
            GuidelinePolicyErrorCategory.PERMISSION_DENIED,
            "guideline_policy_permission_denied",
            {},
        )
    if isinstance(error, GuidelineRevisionUnderBump):
        details = {"minimum_bump": error.minimum_bump.name.lower()}
        minimum = _safe_semantic_version(error.minimum_semantic_version)
        declared = _safe_semantic_version(error.declared_semantic_version)
        if minimum is not None:
            details["minimum_semantic_version"] = minimum
        if declared is not None:
            details["declared_semantic_version"] = declared
        return GuidelinePolicyErrorCategory.INVALID_ARGUMENT, None, details
    if isinstance(error, SemanticAssessmentInadmissibleError):
        cause = _safe_reason_code(error.cause)
        details = {"inadmissibility_cause": cause} if cause is not None else {}
        return (
            GuidelinePolicyErrorCategory.INVALID_ARGUMENT,
            "policy_assessment_inadmissible",
            details,
        )
    if isinstance(error, EntityNotFoundError):
        details = {}
        entity_type = _safe_reason_code(error.entity_type)
        if entity_type is not None:
            details["entity_type"] = entity_type
        return GuidelinePolicyErrorCategory.NOT_FOUND, "entity_not_found", details
    if isinstance(error, _CONFLICT_ERRORS):
        details = {}
        if isinstance(error, ConflictError):
            entity_type = _safe_reason_code(error.entity_type)
            if entity_type is not None:
                details["entity_type"] = entity_type
        if isinstance(error, GuidelinePolicyPersistenceError):
            semantic_reason = _safe_reason_code(str(error))
            if semantic_reason is not None:
                reason_code = semantic_reason
            for key, value in error.details:
                safe_key = _safe_reason_code(key)
                if safe_key != "stale_reasons":
                    continue
                stale_reasons = tuple(
                    reason
                    for item in value.split(",")
                    if (reason := _safe_reason_code(item)) is not None
                )
                if stale_reasons:
                    details["stale_reasons"] = ",".join(stale_reasons)
        return GuidelinePolicyErrorCategory.CONFLICT, reason_code, details
    if isinstance(error, SemanticAssessmentV2WriterUnavailable):
        return (
            GuidelinePolicyErrorCategory.SERVICE_UNAVAILABLE,
            error.code,
            dict(error.details),
        )
    if isinstance(
        error,
        (
            GuidelinePolicyAdapterMissing,
            GuidelinePolicyCursorConfigurationError,
        ),
    ):
        return (
            GuidelinePolicyErrorCategory.SERVICE_UNAVAILABLE,
            reason_code,
            {},
        )
    if isinstance(
        error,
        (
            CommandValidationError,
            GuidelineImportExportError,
            GuidelinePolicyContractError,
            ValueError,
        ),
    ):
        return (
            GuidelinePolicyErrorCategory.INVALID_ARGUMENT,
            reason_code,
            {},
        )
    if isinstance(error, GuidelinePolicyPersistenceError):
        return (
            GuidelinePolicyErrorCategory.SERVICE_UNAVAILABLE,
            reason_code,
            {},
        )
    raise TypeError("guideline_policy_error_type_unsupported")


def _safe_reason_code(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized if _SAFE_REASON_CODE.fullmatch(normalized) else None


def _safe_semantic_version(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return (
        normalized
        if _SAFE_SEMANTIC_VERSION.fullmatch(normalized) is not None
        else None
    )


__all__ = [
    "GUIDELINE_POLICY_HTTP_STATUS_BY_CATEGORY",
    "GuidelinePolicyErrorCategory",
    "guideline_policy_http_status",
    "project_guideline_policy_error",
]
