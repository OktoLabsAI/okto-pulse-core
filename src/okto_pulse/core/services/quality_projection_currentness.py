"""Closed currentness selector for quality summaries projected into the KG.

The selector is edition-neutral and deliberately accepts only the assessment
identities whose normative inputs can be rederived by the current runtime.
Unknown or impossible kind/subject/origin/source combinations fail closed
instead of being projected as current by assumption.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from okto_pulse.core.domain.quality_assessment import (
    AssessmentCurrentness,
    AssessmentDigestSet,
    AssessmentKind,
    AssessmentOrigin,
    AssessmentSource,
    AssessmentSubjectRef,
    AssessmentSubjectType,
    AssessmentVersionSet,
    evaluate_assessment_input_currentness,
)
from okto_pulse.core.domain.quality_canonicalization import (
    canonical_sha256,
    clarification_digest_v1,
    policy_digest_v1,
    ruleset_digest_v1,
    semantic_content_digest_v1,
)
from okto_pulse.core.domain.requirement_lint import RequirementLocale
from okto_pulse.core.services.ambiguity_assessment import (
    ambiguity_digest_set,
    ambiguity_qa_payload,
    ambiguity_subject_payload,
    resolve_ambiguity_gate_configuration,
)
from okto_pulse.core.services.requirement_lint_assessment import (
    requirement_lint_normative_digests_v1,
    resolve_lint_language_profile,
)

LEGACY_SPEC_VALIDATION_RULESET_VERSION = (
    "legacy-spec-validation-ruleset/v1"
)
LEGACY_SPEC_VALIDATION_TAXONOMY_VERSION = (
    "legacy-spec-validation-taxonomy/v1"
)
LEGACY_SPEC_VALIDATION_ANALYZER_VERSION = (
    "legacy-spec-validation-analyzer/v1"
)
LEGACY_SPEC_VALIDATION_POLICY_VERSION = (
    "legacy-spec-validation-policy/v1"
)
LEGACY_SPEC_VALIDATION_RULESET_DIGEST = ruleset_digest_v1(
    LEGACY_SPEC_VALIDATION_RULESET_VERSION,
    {
        "source": "current successful legacy SpecValidation",
        "score_field": "ambiguity",
        "range": [0, 100],
    },
)
LEGACY_SPEC_VALIDATION_TAXONOMY_DIGEST = canonical_sha256(
    {
        "version": LEGACY_SPEC_VALIDATION_TAXONOMY_VERSION,
        "assessment_kind": AssessmentKind.SPEC_VALIDATION.value,
    }
)
LEGACY_SPEC_VALIDATION_POLICY_DIGEST = policy_digest_v1(
    LEGACY_SPEC_VALIDATION_POLICY_VERSION,
    {
        "current_validation_required": True,
        "outcome": "success",
        "scope_match_required": True,
    },
)


class QualityProjectionCurrentnessError(ValueError):
    """A projected quality head has no supported normative currentness path."""


def legacy_spec_validation_digest_set_v1(
    *,
    subject: object,
    qa_items: Sequence[object] | None,
) -> AssessmentDigestSet:
    """Rederive the frozen legacy SpecValidation migration input identity."""

    return AssessmentDigestSet(
        content_digest=semantic_content_digest_v1(
            AssessmentSubjectType.SPEC,
            ambiguity_subject_payload(AssessmentSubjectType.SPEC, subject),
        ),
        clarification_digest=clarification_digest_v1(
            ambiguity_qa_payload(qa_items),
        ),
        ruleset_digest=LEGACY_SPEC_VALIDATION_RULESET_DIGEST,
        taxonomy_digest=LEGACY_SPEC_VALIDATION_TAXONOMY_DIGEST,
        policy_digest=LEGACY_SPEC_VALIDATION_POLICY_DIGEST,
    )


def legacy_spec_validation_versions_v1() -> AssessmentVersionSet:
    return AssessmentVersionSet(
        ruleset_version=LEGACY_SPEC_VALIDATION_RULESET_VERSION,
        taxonomy_version=LEGACY_SPEC_VALIDATION_TAXONOMY_VERSION,
        analyzer_version=LEGACY_SPEC_VALIDATION_ANALYZER_VERSION,
        policy_version=LEGACY_SPEC_VALIDATION_POLICY_VERSION,
    )


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
            (AssessmentOrigin.LEGACY_IMPORT, AssessmentSource.LEGACY_MIGRATION),
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

    if resolved_kind is AssessmentKind.REQUIREMENT_LINT:
        if (
            resolved_type is not AssessmentSubjectType.SPEC
            or resolved_origin is not AssessmentOrigin.SEMANTIC_WRITER
            or resolved_source is not AssessmentSource.NATIVE
        ):
            raise QualityProjectionCurrentnessError(
                "quality_projection_identity_unsupported"
            )
        return requirement_lint_normative_digests_v1(
            content_digest=semantic_content_digest_v1(
                resolved_type,
                ambiguity_subject_payload(resolved_type, subject),
            ),
            clarification_digest=clarification_digest_v1(
                ambiguity_qa_payload(qa_items),
            ),
            default_locale=RequirementLocale.UNKNOWN,
            # The write-side hook seals the board language profile into the
            # policy digest; the read side MUST resolve the same profile or
            # every profiled receipt is evaluated as policy_changed/stale.
            default_locales=resolve_lint_language_profile(board_settings),
        )

    if (
        resolved_kind is AssessmentKind.SPEC_VALIDATION
        and resolved_type is AssessmentSubjectType.SPEC
        and resolved_origin is AssessmentOrigin.LEGACY_IMPORT
        and resolved_source is AssessmentSource.LEGACY_MIGRATION
    ):
        return legacy_spec_validation_digest_set_v1(
            subject=subject,
            qa_items=qa_items,
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
    qa_items: Sequence[object] | None,
    board_settings: Mapping[str, object] | None,
) -> AssessmentCurrentness:
    """Return current/stale for one head using a closed normative dispatch."""

    try:
        resolved_type = AssessmentSubjectType(subject_type)
        current_id = str(
            (
                current_subject.get("id")
                if isinstance(current_subject, Mapping)
                else getattr(current_subject, "id")
            )
            or ""
        ).strip()
        current_version_raw: Any = (
            current_subject.get("version")
            if isinstance(current_subject, Mapping)
            else getattr(current_subject, "version")
        )
        current_version = int(current_version_raw)
    except (AttributeError, TypeError, ValueError) as exc:
        raise QualityProjectionCurrentnessError(
            "quality_projection_subject_invalid"
        ) from exc
    if (
        not board_id
        or not subject_id
        or current_id != subject_id
        or current_version < 1
        or not isinstance(assessed_subject_version, int)
        or isinstance(assessed_subject_version, bool)
        or assessed_subject_version < 1
    ):
        raise QualityProjectionCurrentnessError(
            "quality_projection_subject_invalid"
        )
    current_digests = current_quality_projection_digests(
        subject_type=resolved_type,
        assessment_kind=assessment_kind,
        origin=origin,
        source=source,
        subject=current_subject,
        qa_items=qa_items,
        board_settings=board_settings,
    )
    return evaluate_assessment_input_currentness(
        AssessmentSubjectRef(
            board_id=board_id,
            subject_type=resolved_type,
            subject_id=subject_id,
            subject_version=assessed_subject_version,
        ),
        assessed_digests,
        current_subject=AssessmentSubjectRef(
            board_id=board_id,
            subject_type=resolved_type,
            subject_id=subject_id,
            subject_version=current_version,
        ),
        current_digests=current_digests,
    )


__all__ = [
    "LEGACY_SPEC_VALIDATION_ANALYZER_VERSION",
    "LEGACY_SPEC_VALIDATION_POLICY_DIGEST",
    "LEGACY_SPEC_VALIDATION_POLICY_VERSION",
    "LEGACY_SPEC_VALIDATION_RULESET_DIGEST",
    "LEGACY_SPEC_VALIDATION_RULESET_VERSION",
    "LEGACY_SPEC_VALIDATION_TAXONOMY_DIGEST",
    "LEGACY_SPEC_VALIDATION_TAXONOMY_VERSION",
    "QualityProjectionCurrentnessError",
    "current_quality_projection_digests",
    "evaluate_quality_projection_currentness",
    "legacy_spec_validation_digest_set_v1",
    "legacy_spec_validation_versions_v1",
]
