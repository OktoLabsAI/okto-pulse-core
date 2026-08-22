"""Pure semantic guideline assessment admission and immutable evidence.

Agents own cognition: they submit one bounded score, rationale, evidence set,
and stable pinpoint set for every applicable metric.  Pulse owns only closed
validation, authoritative fence checks, threshold comparison, conjunctive
aggregation, and immutable receipt sealing.  This module deliberately has no
clock, persistence, transport, network, cache, or model-provider dependency.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_BINDING_ID_MAX_LENGTH,
    GUIDELINE_ID_MAX_LENGTH,
    GUIDELINE_REVISION_ID_MAX_LENGTH,
    POLICY_ACTOR_ID_MAX_LENGTH,
    POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
    POLICY_METRIC_CODE_MAX_LENGTH,
    POLICY_METRIC_ID_MAX_LENGTH,
    POLICY_RECEIPT_ID_MAX_LENGTH,
    POLICY_VERSION_MAX_LENGTH,
    BoardGuidelineBinding,
    GuidelineBindingState,
    GuidelineEnforcement,
    GuidelineMetric,
    GuidelineMetricDirection,
    GuidelinePolicyContractError,
    GuidelineRevision,
    PolicyCurrentness,
    PolicySubjectRef,
    PolicySubjectSnapshot,
    normalize_policy_bounded_text,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingAnchorType,
    UnboundFindingAnchor,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


SEMANTIC_GUIDELINE_ASSESSMENT_CONTRACT_VERSION = (
    "semantic-guideline-assessment/v1"
)
SEMANTIC_ASSESSMENT_INPUT_DIGEST_VERSION = (
    "semantic-guideline-assessment-input/v1"
)
SEMANTIC_ASSESSMENT_REQUEST_DIGEST_VERSION = (
    "semantic-guideline-assessment-request/v1"
)
SEMANTIC_ASSESSMENT_RECEIPT_DIGEST_VERSION = (
    "semantic-guideline-assessment-receipt/v1"
)
SEMANTIC_BINDING_HEAD_DIGEST_VERSION = "semantic-binding-head/v1"
SEMANTIC_POLICY_SET_DIGEST_VERSION = "semantic-policy-set/v1"
LEGACY_UNKNOWN_SEMANTIC_EDITOR_ID = "legacy_unknown"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


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


def _sha256(value: object, code: str) -> str:
    normalized = _required_text(value, code).lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise SemanticAssessmentContractError(code)
    return normalized


def _score(value: object, code: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= 100
    ):
        raise SemanticAssessmentContractError(code)
    return value


def _positive_int(value: object, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise SemanticAssessmentContractError(code)
    return value


def _aware_utc(value: object, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise SemanticAssessmentContractError(code)
    return value.astimezone(timezone.utc)


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


@dataclass(frozen=True, slots=True)
class SemanticMetricAssessment:
    """Agent-produced evidence for exactly one semantic metric."""

    metric_id: str
    score: int
    rationale: str
    evidence_refs: tuple[EvidenceRef, ...]
    pinpoints: tuple[UnboundFindingAnchor, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "metric_id",
            normalize_policy_bounded_text(
                self.metric_id,
                max_length=POLICY_METRIC_ID_MAX_LENGTH,
                code="semantic_metric_assessment_metric_id_required",
            ),
        )
        object.__setattr__(
            self,
            "score",
            _score(
                self.score,
                "semantic_metric_assessment_score_invalid",
            ),
        )
        object.__setattr__(
            self,
            "rationale",
            _required_text(
                self.rationale,
                "semantic_metric_assessment_rationale_required",
            ),
        )
        evidence_refs = _typed_tuple(
            self.evidence_refs,
            EvidenceRef,
            "semantic_metric_assessment_evidence_refs_invalid",
            allow_empty=False,
        )
        if len(set(evidence_refs)) != len(evidence_refs):
            raise SemanticAssessmentContractError(
                "semantic_metric_assessment_evidence_refs_duplicate"
            )
        object.__setattr__(
            self,
            "evidence_refs",
            tuple(
                sorted(
                    evidence_refs,
                    key=lambda item: (
                        item.source_type,
                        item.source_id,
                        item.source_version,
                        item.content_hash,
                    ),
                )
            ),
        )
        pinpoints = _typed_tuple(
            self.pinpoints,
            UnboundFindingAnchor,
            "semantic_metric_assessment_pinpoints_invalid",
            allow_empty=False,
        )
        if len(set(pinpoints)) != len(pinpoints):
            raise SemanticAssessmentContractError(
                "semantic_metric_assessment_pinpoints_duplicate"
            )
        object.__setattr__(
            self,
            "pinpoints",
            tuple(
                sorted(
                    pinpoints,
                    key=lambda item: (
                        item.anchor_type.value,
                        item.anchor_ref or "",
                        item.excerpt_hash or "",
                    ),
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class SemanticGuidelineAssessmentSubmission:
    """Closed external assessment command before authoritative re-resolution."""

    subject: PolicySubjectRef
    binding_id: str
    expected_binding_revision: int
    guideline_revision_id: str
    idempotency_key: str
    confidence: int
    assessor: SemanticAssessmentAssessor
    metric_results: tuple[SemanticMetricAssessment, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_assessment_subject_invalid"
            )
        if not isinstance(self.assessor, SemanticAssessmentAssessor):
            raise SemanticAssessmentContractError(
                "semantic_assessment_assessor_invalid"
            )
        object.__setattr__(
            self,
            "binding_id",
            normalize_policy_bounded_text(
                self.binding_id,
                max_length=GUIDELINE_BINDING_ID_MAX_LENGTH,
                code="semantic_assessment_binding_id_required",
            ),
        )
        object.__setattr__(
            self,
            "expected_binding_revision",
            _positive_int(
                self.expected_binding_revision,
                "semantic_assessment_expected_binding_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "guideline_revision_id",
            normalize_policy_bounded_text(
                self.guideline_revision_id,
                max_length=GUIDELINE_REVISION_ID_MAX_LENGTH,
                code="semantic_assessment_guideline_revision_id_required",
            ),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            normalize_policy_bounded_text(
                self.idempotency_key,
                max_length=POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
                code="semantic_assessment_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "confidence",
            _score(
                self.confidence,
                "semantic_assessment_confidence_invalid",
            ),
        )
        metric_results = _typed_tuple(
            self.metric_results,
            SemanticMetricAssessment,
            "semantic_assessment_metric_results_invalid",
            allow_empty=False,
        )
        metric_ids = tuple(item.metric_id for item in metric_results)
        if len(set(metric_ids)) != len(metric_ids):
            raise SemanticAssessmentContractError(
                "semantic_assessment_metric_result_duplicate"
            )
        object.__setattr__(
            self,
            "metric_results",
            tuple(
                sorted(
                    metric_results,
                    key=lambda item: item.metric_id,
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class SemanticGuidelineAssessmentContext:
    """Server-resolved authority used to admit one external submission."""

    subject_snapshot: PolicySubjectSnapshot
    binding: BoardGuidelineBinding
    revision: GuidelineRevision
    policy_set_digest: str
    binding_head_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.subject_snapshot, PolicySubjectSnapshot):
            raise SemanticAssessmentContractError(
                "semantic_assessment_context_subject_invalid"
            )
        if not isinstance(self.binding, BoardGuidelineBinding):
            raise SemanticAssessmentContractError(
                "semantic_assessment_context_binding_invalid"
            )
        if not isinstance(self.revision, GuidelineRevision):
            raise SemanticAssessmentContractError(
                "semantic_assessment_context_revision_invalid"
            )
        if self.binding.state is not GuidelineBindingState.ACTIVE:
            raise SemanticAssessmentContractError(
                "semantic_assessment_binding_inactive"
            )
        if self.binding.board_id != self.subject_snapshot.subject.board_id:
            raise SemanticAssessmentContractError(
                "semantic_assessment_binding_board_mismatch"
            )
        if (
            self.binding.guideline_id != self.revision.guideline_id
            or self.binding.revision_id != self.revision.revision_id
            or self.binding.revision_digest != self.revision.revision_digest
            or self.binding.semantic_version != self.revision.semantic_version
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_revision_binding_mismatch"
            )
        known_codes = {metric.code for metric in self.revision.metrics}
        unknown_overrides = (
            set(self.binding.metric_threshold_overrides) - known_codes
        )
        if unknown_overrides:
            raise SemanticAssessmentContractError(
                "semantic_assessment_threshold_override_unknown"
            )
        object.__setattr__(
            self,
            "policy_set_digest",
            _sha256(
                self.policy_set_digest,
                "semantic_assessment_policy_set_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "binding_head_digest",
            _sha256(
                self.binding_head_digest,
                "semantic_assessment_binding_head_digest_invalid",
            ),
        )

    @property
    def applicable_metrics(self) -> tuple[GuidelineMetric, ...]:
        subject_type = self.subject_snapshot.subject.entity_type
        return tuple(
            metric
            for metric in self.revision.metrics
            if metric.applies_to(subject_type)
        )


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


@dataclass(frozen=True, slots=True)
class SemanticAssessmentPinpoint:
    """Stable pinpoint bound to the exact subject/input sealed by a receipt."""

    subject: PolicySubjectRef
    input_digest: str
    anchor_type: FindingAnchorType
    anchor_ref: str | None = None
    excerpt_hash: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_assessment_pinpoint_subject_invalid"
            )
        object.__setattr__(
            self,
            "input_digest",
            _sha256(
                self.input_digest,
                "semantic_assessment_pinpoint_input_digest_invalid",
            ),
        )
        try:
            normalized = UnboundFindingAnchor(
                anchor_type=self.anchor_type,
                anchor_ref=self.anchor_ref,
                excerpt_hash=self.excerpt_hash,
            )
        except (TypeError, ValueError) as exc:
            raise SemanticAssessmentContractError(
                "semantic_assessment_pinpoint_invalid"
            ) from exc
        object.__setattr__(self, "anchor_type", normalized.anchor_type)
        object.__setattr__(self, "anchor_ref", normalized.anchor_ref)
        object.__setattr__(self, "excerpt_hash", normalized.excerpt_hash)


@dataclass(frozen=True, slots=True)
class SemanticMetricResult:
    """Deterministic threshold result plus the complete cognitive evidence."""

    metric_result_id: str
    receipt_id: str
    subject: PolicySubjectRef
    binding_id: str
    guideline_id: str
    revision_id: str
    metric_id: str
    metric_code: str
    metric_definition_digest: str
    score: int
    direction: GuidelineMetricDirection
    default_threshold: int
    effective_threshold: int
    threshold_source: SemanticThresholdSource
    outcome: SemanticMetricOutcome
    rationale: str
    evidence_refs: tuple[EvidenceRef, ...]
    pinpoints: tuple[SemanticAssessmentPinpoint, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_metric_result_subject_invalid"
            )
        if not isinstance(self.direction, GuidelineMetricDirection):
            raise SemanticAssessmentContractError(
                "semantic_metric_result_direction_invalid"
            )
        if not isinstance(self.threshold_source, SemanticThresholdSource):
            raise SemanticAssessmentContractError(
                "semantic_metric_result_threshold_source_invalid"
            )
        if not isinstance(self.outcome, SemanticMetricOutcome):
            raise SemanticAssessmentContractError(
                "semantic_metric_result_outcome_invalid"
            )
        for field_name, max_length in (
            ("metric_result_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("receipt_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("binding_id", GUIDELINE_BINDING_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
            ("metric_id", POLICY_METRIC_ID_MAX_LENGTH),
            ("metric_code", POLICY_METRIC_CODE_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"semantic_metric_result_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "metric_definition_digest",
            _sha256(
                self.metric_definition_digest,
                "semantic_metric_result_definition_digest_invalid",
            ),
        )
        for field_name in (
            "score",
            "default_threshold",
            "effective_threshold",
        ):
            object.__setattr__(
                self,
                field_name,
                _score(
                    getattr(self, field_name),
                    f"semantic_metric_result_{field_name}_invalid",
                ),
            )
        expected_outcome = (
            SemanticMetricOutcome.PASS
            if (
                self.score >= self.effective_threshold
                if self.direction is GuidelineMetricDirection.MINIMUM
                else self.score <= self.effective_threshold
            )
            else SemanticMetricOutcome.FAIL
        )
        if self.outcome is not expected_outcome:
            raise SemanticAssessmentContractError(
                "semantic_metric_result_outcome_inconsistent"
            )
        object.__setattr__(
            self,
            "rationale",
            _required_text(
                self.rationale,
                "semantic_metric_result_rationale_required",
            ),
        )
        evidence_refs = _typed_tuple(
            self.evidence_refs,
            EvidenceRef,
            "semantic_metric_result_evidence_refs_invalid",
            allow_empty=False,
        )
        pinpoints = _typed_tuple(
            self.pinpoints,
            SemanticAssessmentPinpoint,
            "semantic_metric_result_pinpoints_invalid",
            allow_empty=False,
        )
        if any(
            pinpoint.subject != self.subject for pinpoint in pinpoints
        ):
            raise SemanticAssessmentContractError(
                "semantic_metric_result_pinpoint_subject_mismatch"
            )
        object.__setattr__(self, "evidence_refs", evidence_refs)
        object.__setattr__(self, "pinpoints", pinpoints)

    @property
    def passed(self) -> bool:
        return self.outcome is SemanticMetricOutcome.PASS


@dataclass(frozen=True, slots=True)
class SemanticGuidelineAssessmentReceipt:
    """Complete immutable evidence for one subject×binding assessment."""

    receipt_id: str
    subject: PolicySubjectRef
    subject_content_digest: str
    last_semantic_editor_id: str
    binding_id: str
    binding_revision: int
    guideline_id: str
    guideline_revision_id: str
    guideline_revision_digest: str
    binding_configuration_digest: str
    policy_set_digest: str
    binding_head_digest: str
    input_digest: str
    request_digest: str
    idempotency_key: str
    enforcement: GuidelineEnforcement
    assessor: SemanticAssessmentAssessor
    assessor_independent: bool
    confidence: int
    minimum_confidence: int
    confidence_admissible: bool
    state: SemanticAssessmentState
    currentness: PolicyCurrentness
    metric_results: tuple[SemanticMetricResult, ...]
    recorded_at: datetime
    receipt_digest: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.subject, PolicySubjectRef):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_subject_invalid"
            )
        if not isinstance(self.assessor, SemanticAssessmentAssessor):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_assessor_invalid"
            )
        if not isinstance(self.enforcement, GuidelineEnforcement):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_enforcement_invalid"
            )
        if not isinstance(self.state, SemanticAssessmentState):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_state_invalid"
            )
        if self.currentness is not PolicyCurrentness.CURRENT:
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_currentness_invalid"
            )
        for field_name, max_length in (
            ("receipt_id", POLICY_RECEIPT_ID_MAX_LENGTH),
            ("last_semantic_editor_id", POLICY_ACTOR_ID_MAX_LENGTH),
            ("binding_id", GUIDELINE_BINDING_ID_MAX_LENGTH),
            ("guideline_id", GUIDELINE_ID_MAX_LENGTH),
            ("guideline_revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
            ("idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_policy_bounded_text(
                    getattr(self, field_name),
                    max_length=max_length,
                    code=f"semantic_assessment_receipt_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "binding_revision",
            _positive_int(
                self.binding_revision,
                "semantic_assessment_receipt_binding_revision_invalid",
            ),
        )
        for field_name in (
            "subject_content_digest",
            "guideline_revision_digest",
            "binding_configuration_digest",
            "policy_set_digest",
            "binding_head_digest",
            "input_digest",
            "request_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"semantic_assessment_receipt_{field_name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "confidence",
            _score(
                self.confidence,
                "semantic_assessment_receipt_confidence_invalid",
            ),
        )
        object.__setattr__(
            self,
            "minimum_confidence",
            _score(
                self.minimum_confidence,
                "semantic_assessment_receipt_minimum_confidence_invalid",
            ),
        )
        if not isinstance(self.confidence_admissible, bool):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_confidence_admissible_invalid"
            )
        if self.confidence_admissible != (
            self.confidence >= self.minimum_confidence
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_confidence_inconsistent"
            )
        if not self.confidence_admissible:
            raise SemanticAssessmentContractError(
                "policy_assessment_inadmissible"
            )
        if not isinstance(self.assessor_independent, bool):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_assessor_independent_invalid"
            )
        if self.assessor_independent != (
            self.assessor.agent_id != self.last_semantic_editor_id
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_assessor_independence_inconsistent"
            )
        metric_results = _typed_tuple(
            self.metric_results,
            SemanticMetricResult,
            "semantic_assessment_receipt_metric_results_invalid",
            allow_empty=False,
        )
        if len({result.metric_id for result in metric_results}) != len(
            metric_results
        ):
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_metric_result_duplicate"
            )
        for result in metric_results:
            if (
                result.receipt_id != self.receipt_id
                or result.subject != self.subject
                or result.binding_id != self.binding_id
                or result.guideline_id != self.guideline_id
                or result.revision_id != self.guideline_revision_id
                or any(
                    pinpoint.input_digest != self.input_digest
                    for pinpoint in result.pinpoints
                )
            ):
                raise SemanticAssessmentContractError(
                    "semantic_assessment_receipt_metric_result_scope_mismatch"
                )
        object.__setattr__(self, "metric_results", metric_results)
        expected_state = (
            SemanticAssessmentState.PASSED
            if all(result.passed for result in metric_results)
            else SemanticAssessmentState.METRIC_THRESHOLD_FAILED
        )
        if self.state is not expected_state:
            raise SemanticAssessmentContractError(
                "semantic_assessment_receipt_state_inconsistent"
            )
        object.__setattr__(
            self,
            "recorded_at",
            _aware_utc(
                self.recorded_at,
                "semantic_assessment_receipt_recorded_at_invalid",
            ),
        )
        expected_receipt_digest = semantic_assessment_receipt_digest_v1(self)
        if self.receipt_digest is not None:
            supplied_digest = _sha256(
                self.receipt_digest,
                "semantic_assessment_receipt_digest_invalid",
            )
            if supplied_digest != expected_receipt_digest:
                raise SemanticAssessmentContractError(
                    "semantic_assessment_receipt_digest_mismatch"
                )
        object.__setattr__(
            self,
            "receipt_digest",
            expected_receipt_digest,
        )

    @property
    def metric_count(self) -> int:
        return len(self.metric_results)

    @property
    def failed_metric_count(self) -> int:
        return sum(1 for result in self.metric_results if not result.passed)


@dataclass(frozen=True, slots=True)
class SemanticGuidelineAssessmentResult:
    input_digest: str
    request_digest: str
    receipt: SemanticGuidelineAssessmentReceipt
    replayed: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "request_digest",
            _sha256(
                self.request_digest,
                "semantic_assessment_result_request_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "input_digest",
            _sha256(
                self.input_digest,
                "semantic_assessment_result_input_digest_invalid",
            ),
        )
        if not isinstance(self.receipt, SemanticGuidelineAssessmentReceipt):
            raise SemanticAssessmentContractError(
                "semantic_assessment_result_receipt_invalid"
            )
        if self.receipt.input_digest != self.input_digest:
            raise SemanticAssessmentContractError(
                "semantic_assessment_result_input_digest_mismatch"
            )
        if self.receipt.request_digest != self.request_digest:
            raise SemanticAssessmentContractError(
                "semantic_assessment_result_request_digest_mismatch"
            )
        if not isinstance(self.replayed, bool):
            raise SemanticAssessmentContractError(
                "semantic_assessment_result_replayed_invalid"
            )


def _evidence_payload(evidence_ref: EvidenceRef) -> dict[str, object]:
    return {
        "source_type": evidence_ref.source_type,
        "source_id": evidence_ref.source_id,
        "source_version": evidence_ref.source_version,
        "content_hash": evidence_ref.content_hash,
    }


def _unbound_pinpoint_payload(
    pinpoint: UnboundFindingAnchor,
) -> dict[str, object]:
    return {
        "anchor_type": pinpoint.anchor_type.value,
        "anchor_ref": pinpoint.anchor_ref,
        "excerpt_hash": pinpoint.excerpt_hash,
    }


def _bound_pinpoint_payload(
    pinpoint: SemanticAssessmentPinpoint,
) -> dict[str, object]:
    return {
        "anchor_type": pinpoint.anchor_type.value,
        "anchor_ref": pinpoint.anchor_ref,
        "excerpt_hash": pinpoint.excerpt_hash,
        "board_id": pinpoint.subject.board_id,
        "subject_type": pinpoint.subject.entity_type.value,
        "subject_id": pinpoint.subject.subject_id,
        "subject_version": pinpoint.subject.subject_version,
        **(
            {"subject_edition": pinpoint.subject.subject_edition}
            if pinpoint.subject.subject_edition is not None
            else {}
        ),
        "input_digest": pinpoint.input_digest,
    }


def semantic_assessment_input_digest_v1(
    context: SemanticGuidelineAssessmentContext,
) -> str:
    """Digest only the authoritative inputs used for receipt currentness.

    Assessor and model metadata, idempotency keys, scores, rationale, evidence,
    and pinpoints are audit/request data.  They deliberately do not participate
    in this digest, so a model change alone can never stale semantic evidence.
    """

    if not isinstance(context, SemanticGuidelineAssessmentContext):
        raise SemanticAssessmentContractError(
            "semantic_assessment_context_invalid"
        )
    return canonical_sha256(
        {
            "contract": SEMANTIC_ASSESSMENT_INPUT_DIGEST_VERSION,
            "subject": {
                "board_id": context.subject_snapshot.subject.board_id,
                "subject_type": (
                    context.subject_snapshot.subject.entity_type.value
                ),
                "subject_id": context.subject_snapshot.subject.subject_id,
                "subject_version": (
                    context.subject_snapshot.subject.subject_version
                ),
                **(
                    {
                        "subject_edition": (
                            context.subject_snapshot.subject.subject_edition
                        )
                    }
                    if context.subject_snapshot.subject.subject_edition is not None
                    else {}
                ),
                "content_digest": context.subject_snapshot.content_digest,
                "last_semantic_editor_id": (
                    context.subject_snapshot.last_semantic_editor_id
                ),
            },
            "binding": {
                "binding_id": context.binding.binding_id,
                "binding_revision": context.binding.binding_revision,
                "configuration_digest": context.binding.configuration_digest,
                "binding_head_digest": context.binding_head_digest,
            },
            "guideline": {
                "revision_id": context.revision.revision_id,
                "revision_digest": context.revision.revision_digest,
            },
            "policy_set_digest": context.policy_set_digest,
        }
    )


def semantic_assessment_request_digest_v1(
    submission: SemanticGuidelineAssessmentSubmission,
    context: SemanticGuidelineAssessmentContext,
) -> str:
    """Digest the complete auditable request, including assessor/model data."""

    if not isinstance(submission, SemanticGuidelineAssessmentSubmission):
        raise SemanticAssessmentContractError(
            "semantic_assessment_submission_invalid"
        )
    if not isinstance(context, SemanticGuidelineAssessmentContext):
        raise SemanticAssessmentContractError(
            "semantic_assessment_context_invalid"
        )
    return canonical_sha256(
        {
            "contract": SEMANTIC_ASSESSMENT_REQUEST_DIGEST_VERSION,
            "input_digest": semantic_assessment_input_digest_v1(context),
            "idempotency_key": submission.idempotency_key,
            "assessor": {
                "agent_id": submission.assessor.agent_id,
                "model_id": submission.assessor.model_id,
            },
            "confidence": submission.confidence,
            "metric_results": [
                {
                    "metric_id": result.metric_id,
                    "score": result.score,
                    "rationale": result.rationale,
                    "evidence_refs": [
                        _evidence_payload(item)
                        for item in result.evidence_refs
                    ],
                    "pinpoints": [
                        _unbound_pinpoint_payload(item)
                        for item in result.pinpoints
                    ],
                }
                for result in submission.metric_results
            ],
        }
    )


def semantic_assessment_receipt_digest_v1(
    receipt: SemanticGuidelineAssessmentReceipt,
) -> str:
    """Seal all immutable assessment evidence except the digest itself."""

    return canonical_sha256(
        {
            "contract": SEMANTIC_ASSESSMENT_RECEIPT_DIGEST_VERSION,
            "receipt_id": receipt.receipt_id,
            "subject": {
                "board_id": receipt.subject.board_id,
                "subject_type": receipt.subject.entity_type.value,
                "subject_id": receipt.subject.subject_id,
                "subject_version": receipt.subject.subject_version,
                **(
                    {"subject_edition": receipt.subject.subject_edition}
                    if receipt.subject.subject_edition is not None
                    else {}
                ),
                "content_digest": receipt.subject_content_digest,
                "last_semantic_editor_id": receipt.last_semantic_editor_id,
            },
            "binding": {
                "binding_id": receipt.binding_id,
                "binding_revision": receipt.binding_revision,
                "configuration_digest": (
                    receipt.binding_configuration_digest
                ),
                "enforcement": receipt.enforcement.value,
                "minimum_confidence": receipt.minimum_confidence,
            },
            "guideline": {
                "guideline_id": receipt.guideline_id,
                "revision_id": receipt.guideline_revision_id,
                "revision_digest": receipt.guideline_revision_digest,
            },
            "policy_set_digest": receipt.policy_set_digest,
            "binding_head_digest": receipt.binding_head_digest,
            "input_digest": receipt.input_digest,
            "request_digest": receipt.request_digest,
            "idempotency_key": receipt.idempotency_key,
            "assessor": {
                "agent_id": receipt.assessor.agent_id,
                "model_id": receipt.assessor.model_id,
                "independent": receipt.assessor_independent,
            },
            "confidence": receipt.confidence,
            "confidence_admissible": receipt.confidence_admissible,
            "state": receipt.state.value,
            "currentness": receipt.currentness.value,
            "metric_results": [
                {
                    "metric_result_id": result.metric_result_id,
                    "metric_id": result.metric_id,
                    "metric_code": result.metric_code,
                    "metric_definition_digest": (
                        result.metric_definition_digest
                    ),
                    "score": result.score,
                    "direction": result.direction.value,
                    "default_threshold": result.default_threshold,
                    "effective_threshold": result.effective_threshold,
                    "threshold_source": result.threshold_source.value,
                    "outcome": result.outcome.value,
                    "rationale": result.rationale,
                    "evidence_refs": [
                        _evidence_payload(item)
                        for item in result.evidence_refs
                    ],
                    "pinpoints": [
                        _bound_pinpoint_payload(item)
                        for item in result.pinpoints
                    ],
                }
                for result in receipt.metric_results
            ],
            "recorded_at": receipt.recorded_at.isoformat(),
        }
    )


def _validate_exact_fences_and_metric_set(
    submission: SemanticGuidelineAssessmentSubmission,
    context: SemanticGuidelineAssessmentContext,
) -> tuple[GuidelineMetric, ...]:
    if submission.subject != context.subject_snapshot.subject:
        raise SemanticAssessmentContractError(
            "semantic_assessment_subject_stale"
        )
    if (
        submission.binding_id != context.binding.binding_id
        or submission.expected_binding_revision
        != context.binding.binding_revision
    ):
        raise SemanticAssessmentContractError(
            "semantic_assessment_binding_stale"
        )
    if submission.guideline_revision_id != context.revision.revision_id:
        raise SemanticAssessmentContractError(
            "semantic_assessment_guideline_revision_stale"
        )
    applicable_metrics = context.applicable_metrics
    if not applicable_metrics:
        raise SemanticAssessmentContractError(
            "semantic_assessment_no_applicable_metrics"
        )
    expected_ids = {metric.metric_id for metric in applicable_metrics}
    submitted_ids = {
        result.metric_id for result in submission.metric_results
    }
    unknown_ids = submitted_ids - {
        metric.metric_id for metric in context.revision.metrics
    }
    if unknown_ids:
        raise SemanticAssessmentContractError(
            "semantic_assessment_metric_result_unknown"
        )
    non_applicable_ids = submitted_ids - expected_ids
    if non_applicable_ids:
        raise SemanticAssessmentContractError(
            "semantic_assessment_metric_result_not_applicable"
        )
    if submitted_ids != expected_ids:
        raise SemanticAssessmentContractError(
            "semantic_assessment_metric_results_incomplete"
        )
    return applicable_metrics


def _validate_assessment_admissibility(
    submission: SemanticGuidelineAssessmentSubmission,
    context: SemanticGuidelineAssessmentContext,
) -> None:
    """Fail before constructing any receipt/result evidence."""

    if submission.confidence < context.binding.minimum_confidence:
        raise SemanticAssessmentInadmissibleError(
            SemanticAssessmentInadmissibilityCause.CONFIDENCE_BELOW_MINIMUM
        )
    last_editor_id = context.subject_snapshot.last_semantic_editor_id
    if (
        context.binding.enforcement is GuidelineEnforcement.BLOCKING
        and (
            last_editor_id == LEGACY_UNKNOWN_SEMANTIC_EDITOR_ID
            or submission.assessor.agent_id == last_editor_id
        )
    ):
        # Separation failures are a closed structural cause of
        # inadmissibility, not an additional persisted state or public
        # top-level diagnostic.
        raise SemanticAssessmentInadmissibleError(
            SemanticAssessmentInadmissibilityCause.ASSESSOR_SEPARATION_REQUIRED
        )


def record_semantic_guideline_assessment(
    submission: SemanticGuidelineAssessmentSubmission,
    context: SemanticGuidelineAssessmentContext,
    *,
    receipt_id: str,
    recorded_at: datetime,
) -> SemanticGuidelineAssessmentResult:
    """Admit, aggregate, and seal one complete externally-produced assessment."""

    if not isinstance(submission, SemanticGuidelineAssessmentSubmission):
        raise SemanticAssessmentContractError(
            "semantic_assessment_submission_invalid"
        )
    if not isinstance(context, SemanticGuidelineAssessmentContext):
        raise SemanticAssessmentContractError(
            "semantic_assessment_context_invalid"
        )
    receipt_id = normalize_policy_bounded_text(
        receipt_id,
        max_length=POLICY_RECEIPT_ID_MAX_LENGTH,
        code="semantic_assessment_receipt_id_required",
    )
    recorded_at = _aware_utc(
        recorded_at,
        "semantic_assessment_recorded_at_invalid",
    )
    applicable_metrics = _validate_exact_fences_and_metric_set(
        submission,
        context,
    )
    _validate_assessment_admissibility(submission, context)
    input_digest = semantic_assessment_input_digest_v1(context)
    request_digest = semantic_assessment_request_digest_v1(
        submission,
        context,
    )
    submitted_by_metric_id = {
        result.metric_id: result for result in submission.metric_results
    }
    metric_results: list[SemanticMetricResult] = []
    for metric in applicable_metrics:
        submitted = submitted_by_metric_id[metric.metric_id]
        if metric.code in context.binding.metric_threshold_overrides:
            effective_threshold = (
                context.binding.metric_threshold_overrides[metric.code]
            )
            threshold_source = SemanticThresholdSource.OVERRIDE
        else:
            effective_threshold = metric.default_threshold
            threshold_source = SemanticThresholdSource.DEFAULT
        passed = (
            submitted.score >= effective_threshold
            if metric.direction is GuidelineMetricDirection.MINIMUM
            else submitted.score <= effective_threshold
        )
        bound_pinpoints = tuple(
            SemanticAssessmentPinpoint(
                subject=submission.subject,
                input_digest=input_digest,
                anchor_type=pinpoint.anchor_type,
                anchor_ref=pinpoint.anchor_ref,
                excerpt_hash=pinpoint.excerpt_hash,
            )
            for pinpoint in submitted.pinpoints
        )
        metric_result_id = canonical_sha256(
            {
                "contract": SEMANTIC_GUIDELINE_ASSESSMENT_CONTRACT_VERSION,
                "receipt_id": receipt_id,
                "binding_id": context.binding.binding_id,
                "metric_id": metric.metric_id,
                "input_digest": input_digest,
            }
        )
        metric_results.append(
            SemanticMetricResult(
                metric_result_id=metric_result_id,
                receipt_id=receipt_id,
                subject=submission.subject,
                binding_id=context.binding.binding_id,
                guideline_id=context.revision.guideline_id,
                revision_id=context.revision.revision_id,
                metric_id=metric.metric_id,
                metric_code=metric.code,
                metric_definition_digest=canonical_sha256(
                    metric.digest_payload()
                ),
                score=submitted.score,
                direction=metric.direction,
                default_threshold=metric.default_threshold,
                effective_threshold=effective_threshold,
                threshold_source=threshold_source,
                outcome=(
                    SemanticMetricOutcome.PASS
                    if passed
                    else SemanticMetricOutcome.FAIL
                ),
                rationale=submitted.rationale,
                evidence_refs=submitted.evidence_refs,
                pinpoints=bound_pinpoints,
            )
        )
    state = (
        SemanticAssessmentState.PASSED
        if all(result.passed for result in metric_results)
        else SemanticAssessmentState.METRIC_THRESHOLD_FAILED
    )
    receipt = SemanticGuidelineAssessmentReceipt(
        receipt_id=receipt_id,
        subject=submission.subject,
        subject_content_digest=context.subject_snapshot.content_digest,
        last_semantic_editor_id=(
            context.subject_snapshot.last_semantic_editor_id
        ),
        binding_id=context.binding.binding_id,
        binding_revision=context.binding.binding_revision,
        guideline_id=context.revision.guideline_id,
        guideline_revision_id=context.revision.revision_id,
        guideline_revision_digest=context.revision.revision_digest,
        binding_configuration_digest=context.binding.configuration_digest,
        policy_set_digest=context.policy_set_digest,
        binding_head_digest=context.binding_head_digest,
        input_digest=input_digest,
        request_digest=request_digest,
        idempotency_key=submission.idempotency_key,
        enforcement=context.binding.enforcement,
        assessor=submission.assessor,
        assessor_independent=(
            submission.assessor.agent_id
            != context.subject_snapshot.last_semantic_editor_id
        ),
        confidence=submission.confidence,
        minimum_confidence=context.binding.minimum_confidence,
        confidence_admissible=True,
        state=state,
        currentness=PolicyCurrentness.CURRENT,
        metric_results=tuple(metric_results),
        recorded_at=recorded_at,
    )
    return SemanticGuidelineAssessmentResult(
        input_digest=input_digest,
        request_digest=request_digest,
        receipt=receipt,
    )


__all__ = [
    "LEGACY_UNKNOWN_SEMANTIC_EDITOR_ID",
    "SEMANTIC_ASSESSMENT_INPUT_DIGEST_VERSION",
    "SEMANTIC_ASSESSMENT_REQUEST_DIGEST_VERSION",
    "SEMANTIC_ASSESSMENT_RECEIPT_DIGEST_VERSION",
    "SEMANTIC_BINDING_HEAD_DIGEST_VERSION",
    "SEMANTIC_GUIDELINE_ASSESSMENT_CONTRACT_VERSION",
    "SEMANTIC_POLICY_SET_DIGEST_VERSION",
    "SemanticAssessmentAssessor",
    "SemanticAssessmentContractError",
    "SemanticAssessmentInadmissibilityCause",
    "SemanticAssessmentInadmissibleError",
    "SemanticAssessmentPinpoint",
    "SemanticAssessmentState",
    "SemanticGuidelineAssessmentContext",
    "SemanticGuidelineAssessmentReceipt",
    "SemanticGuidelineAssessmentResult",
    "SemanticGuidelineAssessmentSubmission",
    "SemanticMetricAssessment",
    "SemanticMetricOutcome",
    "SemanticMetricResult",
    "SemanticThresholdSource",
    "record_semantic_guideline_assessment",
    "semantic_assessment_request_digest_v1",
    "semantic_assessment_input_digest_v1",
    "semantic_assessment_receipt_digest_v1",
    "semantic_binding_head_digest_v1",
    "semantic_policy_set_digest_v1",
]
