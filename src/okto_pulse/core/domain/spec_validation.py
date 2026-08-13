"""Stable admission conflicts for the human Spec Validation cycle."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class SpecValidationMetric(str, Enum):
    """Closed quality dimensions for a canonical Spec validation."""

    CONFIDENCE = "confidence"
    CLARITY = "clarity"
    ASSERTIVENESS = "assertiveness"
    DECIDABILITY = "decidability"
    AMBIGUITY = "ambiguity"


class SpecValidationPinpointAnchorType(str, Enum):
    """Stable semantic locations supported by validation pinpoints."""

    WHOLE_ARTIFACT = "whole_artifact"
    FIELD = "field"
    STRUCTURED_CHILD = "structured_child"
    QA = "qa"


@dataclass(frozen=True, slots=True)
class SpecValidationPinpoint:
    """A metric-tagged problem location supplied by the evaluator.

    Pulse validates and stores this evidence but never performs the assessment.
    """

    metric: SpecValidationMetric
    anchor_type: SpecValidationPinpointAnchorType
    detail: str
    anchor_ref: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.metric, SpecValidationMetric):
            raise ValueError("spec_validation_pinpoint_metric_invalid")
        if not isinstance(self.anchor_type, SpecValidationPinpointAnchorType):
            raise ValueError("spec_validation_pinpoint_anchor_type_invalid")
        detail = self.detail.strip() if isinstance(self.detail, str) else ""
        if not detail or len(detail) > 4096:
            raise ValueError("spec_validation_pinpoint_detail_invalid")
        object.__setattr__(self, "detail", detail)
        if self.anchor_type is SpecValidationPinpointAnchorType.WHOLE_ARTIFACT:
            if self.anchor_ref is not None:
                raise ValueError("spec_validation_pinpoint_anchor_ref_forbidden")
            object.__setattr__(self, "anchor_ref", None)
        else:
            anchor_ref = (
                self.anchor_ref.strip() if isinstance(self.anchor_ref, str) else None
            )
            if not anchor_ref or len(anchor_ref) > 4096:
                raise ValueError("spec_validation_pinpoint_anchor_ref_required")
            object.__setattr__(self, "anchor_ref", anchor_ref)

    def to_dict(self) -> dict[str, str]:
        payload = {
            "metric": self.metric.value,
            "anchor_type": self.anchor_type.value,
            "detail": self.detail,
        }
        if self.anchor_ref is not None:
            payload["anchor_ref"] = self.anchor_ref
        return payload


class SpecValidationConflictError(ValueError):
    code = "spec_validation_gate_not_ready"

    def __init__(
        self, message: str | None = None, *, details: dict[str, Any] | None = None
    ) -> None:
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
    "SpecValidationMetric",
    "SpecValidationPinpoint",
    "SpecValidationPinpointAnchorType",
    "SpecValidationConflictError",
    "SpecValidationEditionConflict",
    "SpecValidationGateNotReady",
    "SpecValidationVersionConflict",
]
