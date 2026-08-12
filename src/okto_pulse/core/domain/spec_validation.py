"""Stable admission conflicts for the human Spec Validation cycle."""

from __future__ import annotations

from typing import Any


class SpecValidationConflictError(ValueError):
    code = "spec_validation_gate_not_ready"

    def __init__(self, message: str | None = None, *, details: dict[str, Any] | None = None) -> None:
        self.details = dict(details or {})
        super().__init__(message or self.code)

    def to_error_dict(self) -> dict[str, Any]:
        return {
            "outcome": "error",
            "error": self.code,
            "code": self.code,
            "error_code": self.code,
            "message": str(self),
            "category": "conflict",
            "retryable": True,
            "details": dict(self.details),
        }


class SpecValidationEditionConflict(SpecValidationConflictError):
    code = "spec_validation_edition_conflict"


class SpecValidationVersionConflict(SpecValidationConflictError):
    code = "spec_validation_version_conflict"


class SpecValidationGateNotReady(SpecValidationConflictError):
    code = "spec_validation_gate_not_ready"


class RequirementLintRequired(SpecValidationConflictError):
    """No accepted Requirement Lint result exists for the current edition."""

    code = "requirement_lint_required"


__all__ = [
    "RequirementLintRequired",
    "SpecValidationConflictError",
    "SpecValidationEditionConflict",
    "SpecValidationGateNotReady",
    "SpecValidationVersionConflict",
]
