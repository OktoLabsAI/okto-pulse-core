"""Safe, shared classification error projection for REST and MCP adapters."""

from dataclasses import dataclass

from pydantic import ValidationError

from okto_pulse.core.application.use_cases.base import (
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.domain.architecture_classification import (
    ArchitectureClassificationError,
)
from okto_pulse.core.domain.architecture_classification_review import (
    ArchitectureClassificationReadError,
)
from okto_pulse.core.domain.human_validation_cycle import SubjectEditRequiresDraftError
from okto_pulse.core.ports.guideline_policy import GuidelinePolicyVersionConflict

__all__ = [
    "CLASSIFICATION_REQUEST_ERRORS",
    "ClassificationErrorProjection",
    "classification_error",
]


CLASSIFICATION_REQUEST_ERRORS = (
    ArchitectureClassificationError,
    ArchitectureClassificationReadError,
    EntityNotFoundError,
    PermissionDeniedError,
    SubjectEditRequiresDraftError,
    GuidelinePolicyVersionConflict,
    ValidationError,
)

_ERRORS = {
    "architecture_classification_scope_unavailable": (404, "Spec not found."),
    "architecture_classification_history_unavailable": (
        409,
        "Classification history could not be resolved.",
    ),
    "architecture_classification_invalid_offset": (
        422,
        "Offset must be a nonnegative integer.",
    ),
    "architecture_classification_invalid_limit": (
        422,
        "Limit must be an integer between 1 and 100.",
    ),
    "architecture_classification_invalid_state": (
        422,
        "Unknown classification review state.",
    ),
    "architecture_classification_identity_and_digest_required": (
        422,
        "Candidate identity and source digest must be supplied together.",
    ),
    "architecture_classification_payload_too_large": (
        413,
        "Classification batch exceeds 256 KiB.",
    ),
    "architecture_classification_idempotency_conflict": (
        409,
        "Idempotency key is already bound to another request.",
    ),
    "architecture_classification_version_conflict": (
        409,
        "Spec version or edition changed; refresh before retrying.",
    ),
    "architecture_classification_spec_locked": (
        409,
        "Spec content is locked; use an authorized revision.",
    ),
    "architecture_classification_spec_unavailable": (404, "Spec not found."),
    "architecture_classification_spec_not_found": (404, "Spec not found."),
    "architecture_classification_authorization_denied": (
        403,
        "Required classification permission is missing.",
    ),
    "architecture_candidate_source_changed": (
        409,
        "Architecture source changed; review the current contract.",
    ),
    "architecture_candidate_unresolved": (
        409,
        "Architecture candidate is missing or unresolved.",
    ),
    "architecture_sources_unavailable": (
        409,
        "The adopted architecture population could not be resolved.",
    ),
    "architecture_classification_scope_unresolved": (
        422,
        "A selected contract scope could not be resolved.",
    ),
    "architecture_classification_ir_not_active_in_spec": (
        422,
        "Referenced IR must be uniquely identified and active in this Spec.",
    ),
    "architecture_classification_link_target_invalid": (
        422,
        "An authored IR contains an invalid linked reference.",
    ),
    "architecture_classification_validation_failed": (
        422,
        "Authored IR validation failed.",
    ),
}


@dataclass(frozen=True, slots=True)
class ClassificationErrorProjection:
    status_code: int
    error: str
    message: str

    def payload(self) -> dict[str, str]:
        return {"error": self.error, "message": self.message}


def classification_error(exc: Exception) -> ClassificationErrorProjection:
    """Never echo contracts, validation input or provider exception messages."""
    if isinstance(exc, EntityNotFoundError):
        return ClassificationErrorProjection(
            404, "architecture_classification_spec_unavailable", "Spec not found."
        )
    if isinstance(exc, PermissionDeniedError):
        return ClassificationErrorProjection(
            403, "permission_denied", "Required classification permission is missing."
        )
    if isinstance(exc, SubjectEditRequiresDraftError):
        return ClassificationErrorProjection(
            409, exc.code, "Classification requires a Draft Spec."
        )
    if isinstance(exc, GuidelinePolicyVersionConflict):
        code = "architecture_classification_version_conflict"
    elif isinstance(exc, ValidationError):
        code = "architecture_classification_invalid"
        if any(
            str(item.get("ctx", {}).get("error"))
            == "architecture_classification_payload_too_large"
            for item in exc.errors(include_input=False, include_url=False)
        ):
            code = "architecture_classification_payload_too_large"
    elif isinstance(
        exc, (ArchitectureClassificationError, ArchitectureClassificationReadError)
    ):
        code = str(exc)
    else:
        raise TypeError("unsupported_classification_error") from None
    if code not in _ERRORS:
        return ClassificationErrorProjection(
            422,
            "architecture_classification_invalid",
            "Classification request validation failed.",
        )
    status_code, message = _ERRORS[code]
    return ClassificationErrorProjection(status_code, code, message)
