"""Current semantic assessment values, authority and admissibility."""

from __future__ import annotations


from dataclasses import dataclass


from enum import Enum

from okto_pulse.core.domain.guideline_policy import (
    POLICY_ACTOR_ID_MAX_LENGTH,
    POLICY_VERSION_MAX_LENGTH,
    BoardGuidelineBinding,
    GuidelineBindingState,
    GuidelineEnforcement,
    GuidelinePolicyContractError,
    GuidelineRevision,
    PolicySubjectSnapshot,
    normalize_policy_bounded_text,
)


from okto_pulse.core.domain.quality_canonicalization import canonical_sha256

SEMANTIC_BINDING_HEAD_DIGEST_VERSION = "semantic-binding-head/v1"

SEMANTIC_POLICY_SET_DIGEST_VERSION = "semantic-policy-set/v1"

class SemanticAssessmentInadmissibilityCause(str, Enum):
    """Closed structural causes beneath the canonical inadmissible state."""

    CONFIDENCE_BELOW_MINIMUM = "confidence_below_minimum"
    ASSESSOR_SEPARATION_REQUIRED = "assessor_separation_required"


class SemanticAssessmentContractError(GuidelinePolicyContractError):
    """One closed semantic assessment value or fence is invalid."""


class SemanticAssessmentInadmissibleError(SemanticAssessmentContractError):
    """An assessment rejected before evidence construction, with a closed cause."""

    def __init__(
        self,
        cause: SemanticAssessmentInadmissibilityCause,
    ) -> None:
        if not isinstance(cause, SemanticAssessmentInadmissibilityCause):
            raise TypeError("semantic_assessment_inadmissibility_cause_invalid")
        self.cause = cause.value
        super().__init__("policy_assessment_inadmissible")


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SemanticAssessmentContractError(code)
    return value.strip()


def _optional_text(value: object, code: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, code)


def _typed_tuple(
    value: object,
    expected_type: type,
    code: str,
    *,
    allow_empty: bool = True,
) -> tuple:
    if not isinstance(value, tuple | list):
        raise SemanticAssessmentContractError(code)
    resolved = tuple(value)
    if any(not isinstance(item, expected_type) for item in resolved):
        raise SemanticAssessmentContractError(code)
    if not allow_empty and not resolved:
        raise SemanticAssessmentContractError(code)
    return resolved


class SemanticMetricOutcome(str, Enum):
    PASS = "pass"
    FAIL = "fail"


class SemanticThresholdSource(str, Enum):
    DEFAULT = "default"
    OVERRIDE = "override"


class SemanticAssessmentState(str, Enum):
    """Closed aggregate state of one structurally admitted assessment."""

    PASSED = "passed"
    METRIC_THRESHOLD_FAILED = "metric_threshold_failed"


@dataclass(frozen=True, slots=True)
class SemanticAssessmentAssessor:
    agent_id: str
    model_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "agent_id",
            normalize_policy_bounded_text(
                self.agent_id,
                max_length=POLICY_ACTOR_ID_MAX_LENGTH,
                code="semantic_assessment_assessor_agent_id_required",
            ),
        )
        model_id = _optional_text(
            self.model_id,
            "semantic_assessment_assessor_model_id_invalid",
        )
        if model_id is not None and len(model_id) > POLICY_VERSION_MAX_LENGTH:
            raise SemanticAssessmentContractError(
                "semantic_assessment_assessor_model_id_invalid"
            )
        object.__setattr__(self, "model_id", model_id)


def _canonical_bindings(
    value: object,
) -> tuple[BoardGuidelineBinding, ...]:
    bindings = _typed_tuple(
        value,
        BoardGuidelineBinding,
        "semantic_assessment_bindings_invalid",
    )
    if len({binding.binding_id for binding in bindings}) != len(bindings):
        raise SemanticAssessmentContractError(
            "semantic_assessment_bindings_duplicate"
        )
    if len({binding.guideline_id for binding in bindings}) != len(bindings):
        raise SemanticAssessmentContractError(
            "semantic_assessment_guideline_bindings_duplicate"
        )
    if len({binding.board_id for binding in bindings}) > 1:
        raise SemanticAssessmentContractError(
            "semantic_assessment_bindings_board_mismatch"
        )
    return tuple(
        sorted(
            bindings,
            key=lambda binding: (
                binding.priority,
                binding.binding_id,
            ),
        )
    )


def _binding_digest_payload(
    binding: BoardGuidelineBinding,
) -> dict[str, object]:
    return {
        "binding_id": binding.binding_id,
        "binding_revision": binding.binding_revision,
        "board_id": binding.board_id,
        "guideline_id": binding.guideline_id,
        "revision_id": binding.revision_id,
        "semantic_version": binding.semantic_version,
        "revision_digest": binding.revision_digest,
        "priority": binding.priority,
        "enforcement": binding.enforcement.value,
        "minimum_confidence": binding.minimum_confidence,
        "metric_threshold_overrides": dict(
            binding.metric_threshold_overrides
        ),
        "configuration_digest": binding.configuration_digest,
        "state": binding.state.value,
        "source_kind": binding.source_kind.value,
    }


def semantic_binding_head_digest_v1(
    bindings: tuple[BoardGuidelineBinding, ...]
    | list[BoardGuidelineBinding],
) -> str:
    """Digest every exact binding head, including unlinked state."""

    canonical_bindings = _canonical_bindings(bindings)
    return canonical_sha256(
        {
            "contract": SEMANTIC_BINDING_HEAD_DIGEST_VERSION,
            "bindings": [
                _binding_digest_payload(binding)
                for binding in canonical_bindings
            ],
        }
    )


def semantic_policy_set_digest_v1(
    bindings: tuple[BoardGuidelineBinding, ...]
    | list[BoardGuidelineBinding],
    revisions: tuple[GuidelineRevision, ...] | list[GuidelineRevision],
) -> str:
    """Digest the exact active semantic revisions and their ordered metrics."""

    canonical_bindings = _canonical_bindings(bindings)
    active_bindings = tuple(
        binding
        for binding in canonical_bindings
        if binding.state is GuidelineBindingState.ACTIVE
    )
    revision_values = _typed_tuple(
        revisions,
        GuidelineRevision,
        "semantic_assessment_revisions_invalid",
    )
    identities = tuple(
        (revision.guideline_id, revision.revision_id)
        for revision in revision_values
    )
    if len(set(identities)) != len(identities):
        raise SemanticAssessmentContractError(
            "semantic_assessment_revisions_duplicate"
        )
    expected_identities = {
        (binding.guideline_id, binding.revision_id)
        for binding in active_bindings
    }
    if set(identities) != expected_identities:
        raise SemanticAssessmentContractError(
            "semantic_assessment_revision_set_mismatch"
        )
    revision_by_identity = dict(
        zip(identities, revision_values, strict=True)
    )
    adopted: list[dict[str, object]] = []
    for binding in sorted(
        active_bindings,
        key=lambda item: (
            item.guideline_id,
            item.revision_id,
            item.revision_digest,
        ),
    ):
        revision = revision_by_identity[
            (binding.guideline_id, binding.revision_id)
        ]
        if (
            binding.revision_digest != revision.revision_digest
            or binding.semantic_version != revision.semantic_version
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_revision_binding_mismatch"
            )
        adopted.append(
            {
                "guideline_id": binding.guideline_id,
                "revision": {
                    "semantic_version": revision.semantic_version,
                    "title": revision.title,
                    "content": revision.content,
                    "revision_digest": revision.revision_digest,
                    "metrics": [
                        metric.digest_payload()
                        for metric in revision.metrics
                    ],
                    "tags": list(revision.tags),
                },
            }
        )
    return canonical_sha256(
        {
            "contract": SEMANTIC_POLICY_SET_DIGEST_VERSION,
            "adopted": adopted,
        }
    )


def validate_semantic_assessment_admissibility(
    *,
    assessor: SemanticAssessmentAssessor,
    confidence: int,
    binding: BoardGuidelineBinding,
    subject_snapshot: PolicySubjectSnapshot,
) -> None:
    """Fail before constructing any receipt/result evidence."""

    if confidence < binding.minimum_confidence:
        raise SemanticAssessmentInadmissibleError(
            SemanticAssessmentInadmissibilityCause.CONFIDENCE_BELOW_MINIMUM
        )
    last_editor_id = subject_snapshot.last_semantic_editor_id
    if (
        binding.enforcement is GuidelineEnforcement.BLOCKING
        and assessor.agent_id == last_editor_id
    ):
        # Separation failures are a closed structural cause of
        # inadmissibility, not an additional persisted state or public
        # top-level diagnostic.
        raise SemanticAssessmentInadmissibleError(
            SemanticAssessmentInadmissibilityCause.ASSESSOR_SEPARATION_REQUIRED
        )


def validate_semantic_assessment_authority(
    *, subject_snapshot: PolicySubjectSnapshot,
    binding: BoardGuidelineBinding, revision: GuidelineRevision,
) -> None:
    """Validate the current authority before evaluating any metric."""
    if not isinstance(subject_snapshot, PolicySubjectSnapshot):
        raise SemanticAssessmentContractError("semantic_assessment_context_subject_invalid")
    if not isinstance(binding, BoardGuidelineBinding):
        raise SemanticAssessmentContractError("semantic_assessment_context_binding_invalid")
    if not isinstance(revision, GuidelineRevision):
        raise SemanticAssessmentContractError("semantic_assessment_context_revision_invalid")
    if binding.state is not GuidelineBindingState.ACTIVE:
        raise SemanticAssessmentContractError("semantic_assessment_binding_inactive")
    if binding.board_id != subject_snapshot.subject.board_id:
        raise SemanticAssessmentContractError("semantic_assessment_binding_board_mismatch")
    if (binding.guideline_id != revision.guideline_id
        or binding.revision_id != revision.revision_id
        or binding.revision_digest != revision.revision_digest
        or binding.semantic_version != revision.semantic_version):
        raise SemanticAssessmentContractError("semantic_assessment_revision_binding_mismatch")
    if set(binding.metric_threshold_overrides) - {metric.code for metric in revision.metrics}:
        raise SemanticAssessmentContractError("semantic_assessment_threshold_override_unknown")


__all__ = ['SemanticAssessmentAssessor', 'SemanticAssessmentContractError', 'SemanticAssessmentInadmissibilityCause', 'SemanticAssessmentInadmissibleError', 'SemanticAssessmentState', 'SemanticMetricOutcome', 'SemanticThresholdSource', 'semantic_binding_head_digest_v1', 'semantic_policy_set_digest_v1', 'validate_semantic_assessment_admissibility', 'validate_semantic_assessment_authority', 'SEMANTIC_BINDING_HEAD_DIGEST_VERSION', 'SEMANTIC_POLICY_SET_DIGEST_VERSION']
