"""Lifecycle-edition selector for quality summaries projected into the KG.

The selector is edition-neutral and deliberately accepts only the assessment
identities whose normative inputs can be rederived by the current runtime.
Unknown or impossible kind/subject/origin/source combinations fail closed
instead of being projected as current by assumption.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from okto_pulse.core.domain.quality_assessment import (
    AssessmentCurrentness,
    AssessmentDigestSet,
    AssessmentKind,
    AssessmentOrigin,
    AssessmentSource,
    AssessmentSubjectRef,
    AssessmentSubjectType,
    evaluate_assessment_input_currentness,
)
from okto_pulse.core.services.ambiguity_assessment import (
    ambiguity_digest_set,
    resolve_ambiguity_gate_configuration,
)

class QualityProjectionCurrentnessError(ValueError):
    """A projected quality head has no supported normative currentness path."""


def current_quality_projection_digests(
    *,
    subject_type: AssessmentSubjectType | str,
    assessment_kind: AssessmentKind | str,
    origin: AssessmentOrigin | str,
    source: AssessmentSource | str,
    subject: object,
    qa_items: Sequence[object] | None,
    board_settings: Mapping[str, object] | None,
) -> AssessmentDigestSet:
    """Dispatch one supported assessment identity to its normative builder."""

    try:
        resolved_type = AssessmentSubjectType(subject_type)
        resolved_kind = AssessmentKind(assessment_kind)
        resolved_origin = AssessmentOrigin(origin)
        resolved_source = AssessmentSource(source)
    except (TypeError, ValueError) as exc:
        raise QualityProjectionCurrentnessError(
            "quality_projection_identity_unsupported"
        ) from exc

    if resolved_kind is AssessmentKind.AMBIGUITY:
        if resolved_type not in {
            AssessmentSubjectType.IDEATION,
            AssessmentSubjectType.REFINEMENT,
        } or (resolved_origin, resolved_source) not in {
            (AssessmentOrigin.HUMAN_OR_AGENT, AssessmentSource.NATIVE),
        }:
            raise QualityProjectionCurrentnessError(
                "quality_projection_identity_unsupported"
            )
        configuration = resolve_ambiguity_gate_configuration(
            resolved_type,
            board_settings,
        )
        return ambiguity_digest_set(
            subject_type=resolved_type,
            subject=subject,
            qa_items=qa_items,
            configuration=configuration,
        )

    raise QualityProjectionCurrentnessError(
        "quality_projection_identity_unsupported"
    )


def evaluate_quality_projection_currentness(
    *,
    board_id: str,
    subject_type: AssessmentSubjectType | str,
    subject_id: str,
    assessed_subject_version: int,
    assessed_digests: AssessmentDigestSet,
    assessment_kind: AssessmentKind | str,
    origin: AssessmentOrigin | str,
    source: AssessmentSource | str,
    current_subject: object,
    assessed_subject_edition: int,
) -> AssessmentCurrentness:
    """Project only native, explicitly edition-scoped quality evidence."""

    try:
        resolved_type = AssessmentSubjectType(subject_type)
        AssessmentKind(assessment_kind)
        AssessmentOrigin(origin)
        AssessmentSource(source)
        current_id = (
            current_subject.get("id") if isinstance(current_subject, Mapping)
            else getattr(current_subject, "id")
        )
        current_version = (
            current_subject.get("version") if isinstance(current_subject, Mapping)
            else getattr(current_subject, "version")
        )
        current_edition = (
            current_subject.get("edition") if isinstance(current_subject, Mapping)
            else getattr(current_subject, "edition", None)
        )
        for edition in (assessed_subject_edition, current_edition):
            if type(edition) is not int or edition < 1:
                raise QualityProjectionCurrentnessError("quality_projection_edition_required")
        if current_id != subject_id:
            raise QualityProjectionCurrentnessError("quality_projection_subject_invalid")
        assessed = AssessmentSubjectRef(
            board_id=board_id, subject_type=resolved_type, subject_id=subject_id,
            subject_version=assessed_subject_version, subject_edition=assessed_subject_edition,
        )
        current = AssessmentSubjectRef(
            board_id=board_id, subject_type=resolved_type, subject_id=current_id,
            subject_version=current_version, subject_edition=current_edition,
        )
    except QualityProjectionCurrentnessError:
        raise
    except (AttributeError, TypeError, ValueError) as exc:
        raise QualityProjectionCurrentnessError("quality_projection_subject_invalid") from exc
    return evaluate_assessment_input_currentness(
        assessed, assessed_digests, current_subject=current,
        current_digests=assessed_digests,
    )


__all__ = [
    "QualityProjectionCurrentnessError",
    "current_quality_projection_digests",
    "evaluate_quality_projection_currentness",
]
