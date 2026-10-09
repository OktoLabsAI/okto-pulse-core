"""Stable admission conflicts for the human Spec Validation cycle."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from okto_pulse.core.domain.guideline_semantic_v2 import (
    AnchorSnapshot,
    SemanticAnchorAvailability,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


SPEC_VALIDATION_PINPOINT_SNAPSHOT_VERSION = (
    "spec-validation-pinpoint-snapshot/v1"
)


def require_spec_validation_edition(record: Mapping[str, Any]) -> int:
    """Refuse incompatible history without assigning or converting an edition."""
    edition = record.get("edition")
    reported = record.get("validation_edition")
    if (
        type(edition) is not int or edition < 1
        or type(reported) is not int or reported != edition
    ):
        raise ValueError("spec_validation_edition_required")
    return edition


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


class SpecValidationAnchorSnapshotAvailability(str, Enum):
    AVAILABLE = "available"


@dataclass(frozen=True, slots=True)
class SpecValidationAnchorSnapshot:
    """Human-readable anchor evidence sealed with a validation record.

    Stored evidence is never re-resolved against mutable current Spec content.
    """

    availability_at_seal: SpecValidationAnchorSnapshotAvailability
    label: str | None = None
    text: str | None = None
    excerpt: str | None = None
    source_digest: str | None = None
    source_version: str | None = None
    contract_version: str = SPEC_VALIDATION_PINPOINT_SNAPSHOT_VERSION

    def __post_init__(self) -> None:
        if not isinstance(
            self.availability_at_seal,
            SpecValidationAnchorSnapshotAvailability,
        ):
            raise ValueError("spec_validation_anchor_snapshot_availability_invalid")
        if self.contract_version != SPEC_VALIDATION_PINPOINT_SNAPSHOT_VERSION:
            raise ValueError("spec_validation_anchor_snapshot_version_invalid")
        maxima = {
            "label": 4096,
            "text": 65_536,
            "excerpt": 8192,
            "source_version": 1024,
        }
        for field_name, maximum in maxima.items():
            value = getattr(self, field_name)
            if value is None:
                continue
            text = value.strip() if isinstance(value, str) else ""
            if not text or len(text) > maximum:
                raise ValueError(
                    f"spec_validation_anchor_snapshot_{field_name}_invalid"
                )
            object.__setattr__(self, field_name, text)
        if self.source_digest is not None:
            digest = (
                self.source_digest.strip().lower()
                if isinstance(self.source_digest, str)
                else ""
            )
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise ValueError("spec_validation_anchor_snapshot_digest_invalid")
            object.__setattr__(self, "source_digest", digest)
        if (
            self.availability_at_seal
            is SpecValidationAnchorSnapshotAvailability.AVAILABLE
        ):
            if any(
                value is None
                for value in (
                    self.label,
                    self.text,
                    self.source_digest,
                    self.source_version,
                )
            ):
                raise ValueError("spec_validation_anchor_snapshot_content_required")
            expected_digest = canonical_sha256(
                {
                    "label": self.label,
                    "text": self.text,
                    "excerpt": self.excerpt,
                    "source_version": self.source_version,
                }
            )
            if self.source_digest != expected_digest:
                raise ValueError("spec_validation_anchor_snapshot_digest_mismatch")
    @classmethod
    def seal(cls, snapshot: AnchorSnapshot) -> "SpecValidationAnchorSnapshot":
        if not isinstance(snapshot, AnchorSnapshot):
            raise ValueError("spec_validation_anchor_snapshot_invalid")
        if snapshot.availability_at_seal is not SemanticAnchorAvailability.AVAILABLE:
            raise ValueError("spec_validation_anchor_snapshot_unavailable")
        text = snapshot.excerpt or snapshot.label
        digest_payload = {
            "label": snapshot.label,
            "text": text,
            "excerpt": snapshot.excerpt,
            "source_version": snapshot.source_version,
        }
        return cls(
            availability_at_seal=SpecValidationAnchorSnapshotAvailability.AVAILABLE,
            label=snapshot.label,
            text=text,
            excerpt=snapshot.excerpt,
            source_digest=canonical_sha256(digest_payload),
            source_version=snapshot.source_version,
        )

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
    ) -> "SpecValidationAnchorSnapshot":
        if not isinstance(value, Mapping):
            raise ValueError("spec_validation_anchor_snapshot_invalid")
        allowed = {
            "availability_at_seal",
            "label",
            "text",
            "excerpt",
            "source_digest",
            "source_version",
            "contract_version",
        }
        if not set(value).issubset(allowed):
            raise ValueError("spec_validation_anchor_snapshot_invalid")
        return cls(
            availability_at_seal=SpecValidationAnchorSnapshotAvailability(
                value.get("availability_at_seal")
            ),
            label=value.get("label"),
            text=value.get("text"),
            excerpt=value.get("excerpt"),
            source_digest=value.get("source_digest"),
            source_version=value.get("source_version"),
            contract_version=value.get(
                "contract_version",
                SPEC_VALIDATION_PINPOINT_SNAPSHOT_VERSION,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "contract_version": self.contract_version,
            "availability_at_seal": self.availability_at_seal.value,
        }
        for field_name in (
            "label",
            "text",
            "excerpt",
            "source_digest",
            "source_version",
        ):
            value = getattr(self, field_name)
            if value is not None:
                payload[field_name] = value
        return payload


@dataclass(frozen=True, slots=True)
class SpecValidationPinpoint:
    """One actionable evaluator finding. Pulse verifies its literal source quote."""

    metrics: tuple[SpecValidationMetric, ...]
    kind: str
    severity: str
    excerpt: str
    detail: str
    recommendation: str
    anchor_type: SpecValidationPinpointAnchorType
    anchor_ref: str | None = None
    anchor_snapshot: SpecValidationAnchorSnapshot | None = None
    rationale: str | None = None

    def __post_init__(self) -> None:
        if (not self.metrics or len(self.metrics) > 5
            or any(not isinstance(m, SpecValidationMetric) for m in self.metrics)
            or len(set(self.metrics)) != len(self.metrics)):
            raise ValueError("spec_validation_pinpoint_metrics_invalid")
        object.__setattr__(self, "metrics", tuple(sorted(self.metrics, key=lambda m: m.value)))
        if self.kind not in {"problem", "opportunity"}:
            raise ValueError("spec_validation_pinpoint_kind_invalid")
        if self.severity not in {"low", "medium", "high", "critical"}:
            raise ValueError("spec_validation_pinpoint_severity_invalid")
        for name in ("excerpt", "detail", "recommendation"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip() or len(value) > 1000:
                raise ValueError(f"spec_validation_pinpoint_{name}_invalid")
        if self.rationale is not None and (
            not isinstance(self.rationale, str) or not self.rationale.strip()
            or len(self.rationale) > 1000
        ):
            raise ValueError("spec_validation_pinpoint_rationale_invalid")
        if not isinstance(self.anchor_type, SpecValidationPinpointAnchorType):
            raise ValueError("spec_validation_pinpoint_anchor_type_invalid")
        if self.anchor_type is SpecValidationPinpointAnchorType.WHOLE_ARTIFACT:
            if self.anchor_ref is not None:
                raise ValueError("spec_validation_pinpoint_anchor_ref_forbidden")
        else:
            ref = self.anchor_ref.strip() if isinstance(self.anchor_ref, str) else ""
            if not ref or len(ref) > 4096:
                raise ValueError("spec_validation_pinpoint_anchor_ref_required")
            object.__setattr__(self, "anchor_ref", ref)
        if self.anchor_snapshot is not None:
            if not isinstance(self.anchor_snapshot, SpecValidationAnchorSnapshot):
                raise ValueError("spec_validation_anchor_snapshot_invalid")
            if self.excerpt not in (self.anchor_snapshot.text or ""):
                raise ValueError("spec_validation_pinpoint_excerpt_not_verbatim")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SpecValidationPinpoint":
        allowed = {"metrics", "kind", "severity", "excerpt", "detail", "recommendation",
                   "anchor_type", "anchor_ref", "anchor_snapshot", "rationale"}
        if not isinstance(value, Mapping) or not set(value).issubset(allowed):
            raise ValueError("spec_validation_pinpoint_invalid")
        metrics = value.get("metrics")
        if not isinstance(metrics, (list, tuple)):
            raise ValueError("spec_validation_pinpoint_metrics_invalid")
        raw_snapshot = value.get("anchor_snapshot")
        return cls(
            metrics=tuple(SpecValidationMetric(m) for m in metrics),
            kind=value.get("kind"), severity=value.get("severity"),
            excerpt=value.get("excerpt"), detail=value.get("detail"),
            recommendation=value.get("recommendation"),
            rationale=value.get("rationale"),
            anchor_type=SpecValidationPinpointAnchorType(value.get("anchor_type")),
            anchor_ref=value.get("anchor_ref"),
            anchor_snapshot=SpecValidationAnchorSnapshot.from_dict(raw_snapshot) if raw_snapshot is not None else None,
        )

    def seal(self, snapshot: AnchorSnapshot) -> "SpecValidationPinpoint":
        return self.from_dict({**self.to_dict(), "anchor_snapshot": SpecValidationAnchorSnapshot.seal(snapshot).to_dict()})

    def to_dict(self) -> dict[str, Any]:
        payload = {"metrics": [m.value for m in self.metrics], "kind": self.kind,
                   "severity": self.severity, "excerpt": self.excerpt, "detail": self.detail,
                   "recommendation": self.recommendation, "anchor_type": self.anchor_type.value}
        if self.anchor_ref is not None:
            payload["anchor_ref"] = self.anchor_ref
        if self.rationale is not None:
            payload["rationale"] = self.rationale
        if self.anchor_snapshot is not None:
            payload["anchor_snapshot"] = self.anchor_snapshot.to_dict()
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
    "SPEC_VALIDATION_PINPOINT_SNAPSHOT_VERSION",
    "SpecValidationAnchorSnapshot",
    "SpecValidationAnchorSnapshotAvailability",
    "SpecValidationMetric",
    "SpecValidationPinpoint",
    "SpecValidationPinpointAnchorType",
    "SpecValidationConflictError",
    "SpecValidationEditionConflict",
    "SpecValidationGateNotReady",
    "SpecValidationVersionConflict",
]
