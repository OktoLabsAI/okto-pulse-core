"""Pure SK-A quality-assessment domain contracts.

Receipts, findings, heads, and question materializations are immutable values.
Concrete persistence and transport concerns live behind Core ports.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import ClassVar, Generic, TypeVar

from okto_pulse.core.domain.quality_canonicalization import (
    CANONICAL_JSON_VERSION,
    assessment_input_digest_v1,
)

MAX_PROPOSED_QUESTIONS_PER_ASSESSMENT_V1 = 5
QUALITY_PAGE_LIMIT_MAX = 200
QUALITY_ASSESSMENT_CONTRACT_VERSION = "quality-assessment/v1"
QUALITY_ASSESSMENT_CHANNELS_V1 = frozenset(
    {"rest:quality_assess", "mcp:quality_assess"}
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STABLE_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:-]*$")
_MUTABLE_BRACKET_INDEX_RE = re.compile(r"\[\d+\]")
_MUTABLE_SEGMENT_INDEX_RE = re.compile(r"(?:^|[./])\d+(?:$|[./])")


class QualityAssessmentContractError(ValueError):
    """A domain value violates the frozen quality-assessment contract."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.message)


def _required_text(value: str, code: str) -> str:
    if not isinstance(value, str):
        raise QualityAssessmentContractError(code)
    normalized = value.strip()
    if not normalized:
        raise QualityAssessmentContractError(code)
    return normalized


def _strict_non_negative_int(value: int, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise QualityAssessmentContractError(code)
    return value


def _strict_positive_int(value: int, code: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise QualityAssessmentContractError(code)
    return value


def _sha256(value: str, code: str) -> str:
    if not isinstance(value, str):
        raise QualityAssessmentContractError(code)
    normalized = value.strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise QualityAssessmentContractError(code)
    return normalized


def _aware_utc(value: datetime, code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise QualityAssessmentContractError(code)
    return value.astimezone(timezone.utc)


def _enum_instance(value: object, enum_type: type[Enum], code: str) -> None:
    if not isinstance(value, enum_type):
        raise QualityAssessmentContractError(code)


def _instance(value: object, expected_type: type, code: str) -> None:
    if not isinstance(value, expected_type):
        raise QualityAssessmentContractError(code)


def _typed_tuple(
    value: object,
    expected_type: type,
    code: str,
) -> tuple:
    if not isinstance(value, tuple | list):
        raise QualityAssessmentContractError(code)
    resolved = tuple(value)
    if any(not isinstance(item, expected_type) for item in resolved):
        raise QualityAssessmentContractError(code)
    return resolved


class AssessmentSubjectType(str, Enum):
    IDEATION = "ideation"
    REFINEMENT = "refinement"
    SPEC = "spec"


class AssessmentKind(str, Enum):
    AMBIGUITY = "ambiguity"
    SPEC_VALIDATION = "spec_validation"
    REQUIREMENT_LINT = "requirement_lint"


class AssessmentOrigin(str, Enum):
    HUMAN_OR_AGENT = "human_or_agent"
    SPEC_VALIDATION = "spec_validation"
    SEMANTIC_WRITER = "semantic_writer"
    LEGACY_IMPORT = "legacy_import"


class AssessmentSource(str, Enum):
    NATIVE = "native"
    LEGACY_MIGRATION = "legacy_migration"


class AssessmentOutcome(str, Enum):
    """Threshold-independent result frozen on an assessment receipt."""

    RECORDED = "recorded"
    ADVISORY = "advisory"


class AssessmentScaleKind(str, Enum):
    AMBIGUITY_SCORE = "ambiguity_score"
    PERCENTAGE = "percentage"
    FINDING_COUNT = "finding_count"


class ScoreDirection(str, Enum):
    LOWER_BETTER = "lower_better"
    HIGHER_BETTER = "higher_better"


class FindingSeverity(str, Enum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class FindingLifecycle(str, Enum):
    OPEN = "open"
    RESOLVED = "resolved"
    SUPERSEDED = "superseded"


class FindingAnchorType(str, Enum):
    WHOLE_ARTIFACT = "whole_artifact"
    FIELD = "field"
    STRUCTURED_CHILD = "structured_child"
    QA = "qa"


class AssessmentStaleReason(str, Enum):
    SUBJECT_EDITION_CHANGED = "subject_edition_changed"
    CONTENT_CHANGED = "content_changed"
    CLARIFICATION_CHANGED = "clarification_changed"
    RULESET_CHANGED = "ruleset_changed"
    TAXONOMY_CHANGED = "taxonomy_changed"
    POLICY_CHANGED = "policy_changed"
    SUBJECT_VERSION_CHANGED = "subject_version_changed"


class AssessmentReceiptState(str, Enum):
    CURRENT = "current"
    PREVIOUS = "previous"
    # Technical compatibility only. Human lifecycle projections use PREVIOUS.
    STALE = "stale"
    SUPERSEDED = "superseded"


ASSESSMENT_STALE_REASON_ORDER: tuple[AssessmentStaleReason, ...] = (
    AssessmentStaleReason.SUBJECT_EDITION_CHANGED,
    AssessmentStaleReason.SUBJECT_VERSION_CHANGED,
    AssessmentStaleReason.CONTENT_CHANGED,
    AssessmentStaleReason.CLARIFICATION_CHANGED,
    AssessmentStaleReason.RULESET_CHANGED,
    AssessmentStaleReason.TAXONOMY_CHANGED,
    AssessmentStaleReason.POLICY_CHANGED,
)


@dataclass(frozen=True, slots=True)
class AssessmentScale:
    kind: AssessmentScaleKind
    minimum: float
    maximum: float
    direction: ScoreDirection

    def __post_init__(self) -> None:
        _enum_instance(
            self.kind,
            AssessmentScaleKind,
            "assessment_scale_kind_invalid",
        )
        _enum_instance(
            self.direction,
            ScoreDirection,
            "assessment_scale_direction_invalid",
        )
        if any(
            isinstance(value, bool)
            or not isinstance(value, int | float)
            or not math.isfinite(float(value))
            for value in (self.minimum, self.maximum)
        ):
            raise QualityAssessmentContractError("assessment_scale_invalid")
        if float(self.minimum) >= float(self.maximum):
            raise QualityAssessmentContractError("assessment_scale_invalid")
        object.__setattr__(self, "minimum", float(self.minimum))
        object.__setattr__(self, "maximum", float(self.maximum))

    @property
    def best_score(self) -> float:
        return (
            self.minimum
            if self.direction is ScoreDirection.LOWER_BETTER
            else self.maximum
        )

    def validate_score(self, score: float) -> float:
        if (
            isinstance(score, bool)
            or not isinstance(score, int | float)
            or not math.isfinite(float(score))
        ):
            raise QualityAssessmentContractError("assessment_score_invalid")
        value = float(score)
        if value < self.minimum or value > self.maximum:
            raise QualityAssessmentContractError("assessment_score_out_of_scale")
        return value


@dataclass(frozen=True, slots=True)
class AssessmentSubjectIdentity:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str

    def __post_init__(self) -> None:
        _enum_instance(
            self.subject_type,
            AssessmentSubjectType,
            "assessment_subject_type_invalid",
        )
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "assessment_board_id_required"),
        )
        object.__setattr__(
            self,
            "subject_id",
            _required_text(self.subject_id, "assessment_subject_id_required"),
        )


@dataclass(frozen=True, slots=True)
class AssessmentSubjectRef:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    subject_version: int
    # ``None`` is accepted only to deserialize evidence created before lifecycle
    # editions existed. Live subject snapshots and all new writes carry >= 1.
    subject_edition: int | None = None

    def __post_init__(self) -> None:
        _enum_instance(
            self.subject_type,
            AssessmentSubjectType,
            "assessment_subject_type_invalid",
        )
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "assessment_board_id_required"),
        )
        object.__setattr__(
            self,
            "subject_id",
            _required_text(self.subject_id, "assessment_subject_id_required"),
        )
        object.__setattr__(
            self,
            "subject_version",
            _strict_positive_int(
                self.subject_version,
                "assessment_subject_version_invalid",
            ),
        )
        if self.subject_edition is not None:
            object.__setattr__(
                self,
                "subject_edition",
                _strict_positive_int(
                    self.subject_edition,
                    "assessment_subject_edition_invalid",
                ),
            )

    @property
    def identity(self) -> AssessmentSubjectIdentity:
        return AssessmentSubjectIdentity(
            board_id=self.board_id,
            subject_type=self.subject_type,
            subject_id=self.subject_id,
        )


@dataclass(frozen=True, slots=True)
class AssessmentPreflightRequest:
    """Server-owned preflight identity for a governed assessment command.

    Transport adapters may select the literal subject and optimistic fences,
    but they cannot supply the assessment scale, input digests, authority
    digest, version catalog, or complete finding-anchor identity.
    """

    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    assessment_kind: AssessmentKind
    expected_subject_version: int
    expected_head_revision: int
    channel: str
    # Optional at the pure-domain boundary for old integrations; application
    # commands for lifecycle-managed subjects require and validate it.
    expected_subject_edition: int | None = None
    evaluated_rule_count: int | None = None

    def __post_init__(self) -> None:
        _enum_instance(
            self.subject_type,
            AssessmentSubjectType,
            "assessment_subject_type_invalid",
        )
        _enum_instance(
            self.assessment_kind,
            AssessmentKind,
            "assessment_kind_invalid",
        )
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "assessment_board_id_required"),
        )
        object.__setattr__(
            self,
            "subject_id",
            _required_text(self.subject_id, "assessment_subject_id_required"),
        )
        object.__setattr__(
            self,
            "expected_subject_version",
            _strict_positive_int(
                self.expected_subject_version,
                "assessment_subject_version_invalid",
            ),
        )
        if self.expected_subject_edition is not None:
            object.__setattr__(
                self,
                "expected_subject_edition",
                _strict_positive_int(
                    self.expected_subject_edition,
                    "assessment_subject_edition_invalid",
                ),
            )
        if (
            self.assessment_kind is AssessmentKind.REQUIREMENT_LINT
            and self.evaluated_rule_count is not None
        ):
            object.__setattr__(
                self,
                "evaluated_rule_count",
                _strict_positive_int(
                    self.evaluated_rule_count,
                    "requirement_lint_evaluated_rule_count_invalid",
                ),
            )
        elif (
            self.assessment_kind is not AssessmentKind.REQUIREMENT_LINT
            and self.evaluated_rule_count is not None
        ):
            raise QualityAssessmentContractError(
                "assessment_evaluated_rule_count_forbidden"
            )
        object.__setattr__(
            self,
            "expected_head_revision",
            _strict_non_negative_int(
                self.expected_head_revision,
                "assessment_head_revision_invalid",
            ),
        )
        channel = _required_text(
            self.channel,
            "assessment_channel_required",
        )
        if channel not in QUALITY_ASSESSMENT_CHANNELS_V1:
            raise QualityAssessmentContractError(
                "assessment_channel_unsupported"
            )
        object.__setattr__(self, "channel", channel)


@dataclass(frozen=True, slots=True)
class AssessmentDigestSet:
    content_digest: str
    clarification_digest: str
    ruleset_digest: str
    taxonomy_digest: str
    policy_digest: str
    input_digest: str | None = None
    canonicalization_version: str = CANONICAL_JSON_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "content_digest",
            "clarification_digest",
            "ruleset_digest",
            "taxonomy_digest",
            "policy_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"assessment_{field_name}_invalid",
                ),
            )
        expected = assessment_input_digest_v1(
            content_digest=self.content_digest,
            clarification_digest=self.clarification_digest,
            ruleset_digest=self.ruleset_digest,
            taxonomy_digest=self.taxonomy_digest,
            policy_digest=self.policy_digest,
        )
        if self.input_digest is None:
            object.__setattr__(self, "input_digest", expected)
        else:
            normalized_input_digest = _sha256(
                self.input_digest,
                "assessment_input_digest_invalid",
            )
            if normalized_input_digest != expected:
                raise QualityAssessmentContractError(
                    "assessment_input_digest_mismatch"
                )
            object.__setattr__(self, "input_digest", normalized_input_digest)
        if self.canonicalization_version != CANONICAL_JSON_VERSION:
            raise QualityAssessmentContractError(
                "assessment_canonicalization_version_unsupported"
            )


@dataclass(frozen=True, slots=True)
class AssessmentVersionSet:
    ruleset_version: str
    taxonomy_version: str
    analyzer_version: str
    policy_version: str

    def __post_init__(self) -> None:
        for field_name in (
            "ruleset_version",
            "taxonomy_version",
            "analyzer_version",
            "policy_version",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"assessment_{field_name}_required",
                ),
            )


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    source_type: str
    source_id: str
    source_version: int
    content_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_type",
            _required_text(self.source_type, "evidence_source_type_required"),
        )
        object.__setattr__(
            self,
            "source_id",
            _required_text(self.source_id, "evidence_source_id_required"),
        )
        object.__setattr__(
            self,
            "source_version",
            _strict_positive_int(
                self.source_version,
                "evidence_source_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "content_hash",
            _sha256(self.content_hash, "evidence_content_hash_invalid"),
        )


@dataclass(frozen=True, slots=True)
class FindingAnchor:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    subject_version: int
    input_digest: str
    anchor_type: FindingAnchorType
    anchor_ref: str | None = None
    excerpt_hash: str | None = None

    def __post_init__(self) -> None:
        _enum_instance(
            self.subject_type,
            AssessmentSubjectType,
            "finding_anchor_subject_type_invalid",
        )
        _enum_instance(
            self.anchor_type,
            FindingAnchorType,
            "finding_anchor_type_invalid",
        )
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "finding_anchor_board_id_required"),
        )
        object.__setattr__(
            self,
            "subject_id",
            _required_text(self.subject_id, "finding_anchor_subject_id_required"),
        )
        object.__setattr__(
            self,
            "subject_version",
            _strict_positive_int(
                self.subject_version,
                "finding_anchor_subject_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "input_digest",
            _sha256(self.input_digest, "finding_anchor_input_digest_invalid"),
        )
        if self.anchor_type is FindingAnchorType.WHOLE_ARTIFACT:
            if self.anchor_ref is not None:
                raise QualityAssessmentContractError(
                    "finding_whole_artifact_ref_forbidden"
                )
        else:
            normalized_ref = _required_text(
                self.anchor_ref or "",
                "finding_anchor_ref_required",
            )
            if (
                _MUTABLE_BRACKET_INDEX_RE.search(normalized_ref)
                or _MUTABLE_SEGMENT_INDEX_RE.search(normalized_ref)
            ):
                raise QualityAssessmentContractError(
                    "finding_mutable_index_anchor_forbidden"
                )
            if (
                self.anchor_type is FindingAnchorType.FIELD
                and not _STABLE_FIELD_RE.fullmatch(normalized_ref)
            ):
                raise QualityAssessmentContractError("finding_field_anchor_invalid")
            object.__setattr__(self, "anchor_ref", normalized_ref)
        if self.excerpt_hash is not None:
            object.__setattr__(
                self,
                "excerpt_hash",
                _sha256(self.excerpt_hash, "finding_excerpt_hash_invalid"),
            )


@dataclass(frozen=True, slots=True)
class UnboundFindingAnchor:
    """Client-visible anchor selector before server identity is attached."""

    anchor_type: FindingAnchorType
    anchor_ref: str | None = None
    excerpt_hash: str | None = None

    def __post_init__(self) -> None:
        _enum_instance(
            self.anchor_type,
            FindingAnchorType,
            "finding_anchor_type_invalid",
        )
        if self.anchor_type is FindingAnchorType.WHOLE_ARTIFACT:
            if self.anchor_ref is not None:
                raise QualityAssessmentContractError(
                    "finding_whole_artifact_ref_forbidden"
                )
        else:
            normalized_ref = _required_text(
                self.anchor_ref or "",
                "finding_anchor_ref_required",
            )
            if (
                _MUTABLE_BRACKET_INDEX_RE.search(normalized_ref)
                or _MUTABLE_SEGMENT_INDEX_RE.search(normalized_ref)
            ):
                raise QualityAssessmentContractError(
                    "finding_mutable_index_anchor_forbidden"
                )
            if (
                self.anchor_type is FindingAnchorType.FIELD
                and not _STABLE_FIELD_RE.fullmatch(normalized_ref)
            ):
                raise QualityAssessmentContractError(
                    "finding_field_anchor_invalid"
                )
            object.__setattr__(self, "anchor_ref", normalized_ref)
        if self.excerpt_hash is not None:
            object.__setattr__(
                self,
                "excerpt_hash",
                _sha256(self.excerpt_hash, "finding_excerpt_hash_invalid"),
            )


@dataclass(frozen=True, slots=True)
class QualityFindingDraft:
    finding_key: str
    category_code: str
    severity: FindingSeverity
    confidence: float
    deterministic: bool
    blocking_eligible: bool
    title: str
    detail: str
    anchor: FindingAnchor
    remediation: str | None = None
    rule_code: str | None = None
    evidence_refs: tuple[EvidenceRef, ...] = ()

    def __post_init__(self) -> None:
        _instance(self.anchor, FindingAnchor, "finding_anchor_invalid")
        _enum_instance(
            self.severity,
            FindingSeverity,
            "finding_severity_invalid",
        )
        if (
            isinstance(self.confidence, bool)
            or not isinstance(self.confidence, int | float)
            or not math.isfinite(float(self.confidence))
            or not 0 <= float(self.confidence) <= 1
        ):
            raise QualityAssessmentContractError("finding_confidence_invalid")
        object.__setattr__(self, "confidence", float(self.confidence))
        if not isinstance(self.deterministic, bool):
            raise QualityAssessmentContractError("finding_determinism_invalid")
        if not isinstance(self.blocking_eligible, bool):
            raise QualityAssessmentContractError(
                "finding_blocking_eligibility_invalid"
            )
        if self.blocking_eligible:
            raise QualityAssessmentContractError(
                "finding_blocking_eligibility_forbidden"
            )
        object.__setattr__(
            self,
            "finding_key",
            _required_text(self.finding_key, "finding_key_required"),
        )
        object.__setattr__(
            self,
            "category_code",
            _required_text(self.category_code, "finding_category_required"),
        )
        object.__setattr__(
            self,
            "title",
            _required_text(self.title, "finding_title_required"),
        )
        object.__setattr__(
            self,
            "detail",
            _required_text(self.detail, "finding_detail_required"),
        )
        for field_name in ("remediation", "rule_code"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _required_text(value, f"finding_{field_name}_invalid"),
                )
        object.__setattr__(
            self,
            "evidence_refs",
            _typed_tuple(
                self.evidence_refs,
                EvidenceRef,
                "finding_evidence_refs_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class UnboundQualityFindingDraft:
    """Assessment finding input with no caller-owned subject/digest fields."""

    finding_key: str
    category_code: str
    severity: FindingSeverity
    confidence: float
    deterministic: bool
    title: str
    detail: str
    anchor: UnboundFindingAnchor
    remediation: str | None = None
    rule_code: str | None = None
    evidence_refs: tuple[EvidenceRef, ...] = ()

    def __post_init__(self) -> None:
        _instance(
            self.anchor,
            UnboundFindingAnchor,
            "finding_anchor_invalid",
        )
        _enum_instance(
            self.severity,
            FindingSeverity,
            "finding_severity_invalid",
        )
        if (
            isinstance(self.confidence, bool)
            or not isinstance(self.confidence, int | float)
            or not math.isfinite(float(self.confidence))
            or not 0 <= float(self.confidence) <= 1
        ):
            raise QualityAssessmentContractError("finding_confidence_invalid")
        object.__setattr__(self, "confidence", float(self.confidence))
        if not isinstance(self.deterministic, bool):
            raise QualityAssessmentContractError("finding_determinism_invalid")
        for field_name in (
            "finding_key",
            "category_code",
            "title",
            "detail",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"finding_{field_name}_required",
                ),
            )
        for field_name in ("remediation", "rule_code"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _required_text(value, f"finding_{field_name}_invalid"),
                )
        object.__setattr__(
            self,
            "evidence_refs",
            _typed_tuple(
                self.evidence_refs,
                EvidenceRef,
                "finding_evidence_refs_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class QualityFinding:
    id: str
    receipt_id: str
    assessment_kind: AssessmentKind
    finding_key: str
    category_code: str
    taxonomy_version: str
    severity: FindingSeverity
    confidence: float
    deterministic: bool
    blocking_eligible: bool
    title: str
    detail: str
    anchor: FindingAnchor
    evidence_refs: tuple[EvidenceRef, ...]
    lifecycle: FindingLifecycle
    created_at: datetime
    remediation: str | None = None
    rule_code: str | None = None

    def __post_init__(self) -> None:
        _instance(self.anchor, FindingAnchor, "finding_anchor_invalid")
        _enum_instance(
            self.assessment_kind,
            AssessmentKind,
            "assessment_kind_invalid",
        )
        _enum_instance(
            self.severity,
            FindingSeverity,
            "finding_severity_invalid",
        )
        _enum_instance(
            self.lifecycle,
            FindingLifecycle,
            "finding_lifecycle_invalid",
        )
        if (
            isinstance(self.confidence, bool)
            or not isinstance(self.confidence, int | float)
            or not math.isfinite(float(self.confidence))
            or not 0 <= float(self.confidence) <= 1
        ):
            raise QualityAssessmentContractError("finding_confidence_invalid")
        object.__setattr__(self, "confidence", float(self.confidence))
        if not isinstance(self.deterministic, bool):
            raise QualityAssessmentContractError("finding_determinism_invalid")
        if not isinstance(self.blocking_eligible, bool):
            raise QualityAssessmentContractError(
                "finding_blocking_eligibility_invalid"
            )
        if self.blocking_eligible:
            raise QualityAssessmentContractError(
                "finding_blocking_eligibility_forbidden"
            )
        for field_name in (
            "id",
            "receipt_id",
            "finding_key",
            "category_code",
            "taxonomy_version",
            "title",
            "detail",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"finding_{field_name}_required",
                ),
            )
        for field_name in ("remediation", "rule_code"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    _required_text(value, f"finding_{field_name}_invalid"),
                )
        object.__setattr__(
            self,
            "evidence_refs",
            _typed_tuple(
                self.evidence_refs,
                EvidenceRef,
                "finding_evidence_refs_invalid",
            ),
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(self.created_at, "finding_created_at_must_be_aware"),
        )


@dataclass(frozen=True, slots=True)
class ProposedQuestionDraft:
    client_key: str
    question: str
    question_type: str
    choices: tuple[str, ...] = ()
    allow_free_text: bool = True
    category_code: str | None = None
    finding_keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "client_key",
            _required_text(self.client_key, "question_client_key_required"),
        )
        object.__setattr__(
            self,
            "question",
            _required_text(self.question, "question_text_required"),
        )
        object.__setattr__(
            self,
            "question_type",
            _required_text(self.question_type, "question_type_required"),
        )
        if not isinstance(self.choices, tuple | list):
            raise QualityAssessmentContractError("question_choices_invalid")
        choices = tuple(
            _required_text(choice, "question_choice_empty") for choice in self.choices
        )
        if len(set(choices)) != len(choices):
            raise QualityAssessmentContractError("question_choices_duplicate")
        object.__setattr__(self, "choices", choices)
        if not isinstance(self.allow_free_text, bool):
            raise QualityAssessmentContractError("question_allow_free_text_invalid")
        if self.category_code is not None:
            object.__setattr__(
                self,
                "category_code",
                _required_text(
                    self.category_code,
                    "question_category_code_invalid",
                ),
            )
        if not isinstance(self.finding_keys, tuple | list):
            raise QualityAssessmentContractError(
                "question_finding_keys_invalid"
            )
        keys = tuple(
            _required_text(key, "question_finding_key_empty")
            for key in self.finding_keys
        )
        if len(set(keys)) != len(keys):
            raise QualityAssessmentContractError(
                "question_finding_keys_duplicate"
            )
        object.__setattr__(self, "finding_keys", keys)


@dataclass(frozen=True, slots=True)
class ProposedQuestion:
    qa_id: str
    receipt_id: str
    client_key: str
    question: str
    question_type: str
    choices: tuple[str, ...]
    allow_free_text: bool
    category_code: str | None
    created_at: datetime

    def __post_init__(self) -> None:
        for field_name in ("qa_id", "receipt_id", "client_key", "question", "question_type"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"question_{field_name}_required",
                ),
            )
        if not isinstance(self.choices, tuple | list) or any(
            not isinstance(choice, str) for choice in self.choices
        ):
            raise QualityAssessmentContractError("question_choices_invalid")
        object.__setattr__(self, "choices", tuple(self.choices))
        if not isinstance(self.allow_free_text, bool):
            raise QualityAssessmentContractError("question_allow_free_text_invalid")
        if self.category_code is not None:
            object.__setattr__(
                self,
                "category_code",
                _required_text(
                    self.category_code,
                    "question_category_code_invalid",
                ),
            )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(self.created_at, "question_created_at_must_be_aware"),
        )


@dataclass(frozen=True, slots=True)
class FindingQaLink:
    finding_id: str
    qa_id: str
    receipt_id: str

    def __post_init__(self) -> None:
        for field_name in ("finding_id", "qa_id", "receipt_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"finding_qa_link_{field_name}_required",
                ),
            )


@dataclass(frozen=True, slots=True)
class AssessmentReceipt:
    id: str
    subject: AssessmentSubjectRef
    assessment_kind: AssessmentKind
    origin: AssessmentOrigin
    source: AssessmentSource
    channel: str
    outcome: AssessmentOutcome
    scale: AssessmentScale
    score: float
    justification: str
    digests: AssessmentDigestSet
    versions: AssessmentVersionSet
    run_identity_digest: str
    authority_digest: str
    idempotency_key: str
    request_digest: str
    created_by: str
    created_at: datetime
    predecessor_receipt_id: str | None = None
    contract_version: str = QUALITY_ASSESSMENT_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _instance(
            self.subject,
            AssessmentSubjectRef,
            "assessment_subject_invalid",
        )
        _instance(self.scale, AssessmentScale, "assessment_scale_invalid")
        _instance(
            self.digests,
            AssessmentDigestSet,
            "assessment_digests_invalid",
        )
        _instance(
            self.versions,
            AssessmentVersionSet,
            "assessment_versions_invalid",
        )
        _enum_instance(
            self.assessment_kind,
            AssessmentKind,
            "assessment_kind_invalid",
        )
        _enum_instance(
            self.origin,
            AssessmentOrigin,
            "assessment_origin_invalid",
        )
        _enum_instance(
            self.source,
            AssessmentSource,
            "assessment_source_invalid",
        )
        _enum_instance(
            self.outcome,
            AssessmentOutcome,
            "assessment_outcome_invalid",
        )
        object.__setattr__(
            self,
            "id",
            _required_text(self.id, "assessment_receipt_id_required"),
        )
        object.__setattr__(
            self,
            "channel",
            _required_text(self.channel, "assessment_channel_required"),
        )
        object.__setattr__(self, "score", self.scale.validate_score(self.score))
        object.__setattr__(
            self,
            "justification",
            _required_text(
                self.justification,
                "assessment_justification_required",
            ),
        )
        object.__setattr__(
            self,
            "run_identity_digest",
            _sha256(
                self.run_identity_digest,
                "assessment_run_identity_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "authority_digest",
            _sha256(
                self.authority_digest,
                "assessment_authority_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(
                self.idempotency_key,
                "assessment_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "request_digest",
            _sha256(
                self.request_digest,
                "assessment_request_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "created_by",
            _required_text(self.created_by, "assessment_created_by_required"),
        )
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "assessment_created_at_must_be_aware",
            ),
        )
        if self.predecessor_receipt_id is not None:
            predecessor = _required_text(
                self.predecessor_receipt_id,
                "assessment_predecessor_receipt_id_invalid",
            )
            if predecessor == self.id:
                raise QualityAssessmentContractError(
                    "assessment_predecessor_self_reference"
                )
            object.__setattr__(self, "predecessor_receipt_id", predecessor)
        if (
            self.origin is AssessmentOrigin.LEGACY_IMPORT
            and self.source is not AssessmentSource.LEGACY_MIGRATION
        ) or (
            self.origin is not AssessmentOrigin.LEGACY_IMPORT
            and self.source is not AssessmentSource.NATIVE
        ):
            raise QualityAssessmentContractError(
                "assessment_source_origin_mismatch"
            )
        expected_outcome = (
            AssessmentOutcome.ADVISORY
            if self.assessment_kind is AssessmentKind.REQUIREMENT_LINT
            else AssessmentOutcome.RECORDED
        )
        if self.outcome is not expected_outcome:
            raise QualityAssessmentContractError(
                "assessment_outcome_kind_mismatch"
            )
        if self.contract_version != QUALITY_ASSESSMENT_CONTRACT_VERSION:
            raise QualityAssessmentContractError(
                "assessment_contract_version_unsupported"
            )


@dataclass(frozen=True, slots=True)
class AssessmentAuditIntent:
    """Typed history/event/outbox intent committed with an assessment bundle."""

    event_type: ClassVar[str] = "quality.assessment_recorded.v1"
    event_schema_version: ClassVar[int] = 1
    history_action: ClassVar[str] = "quality_assessment_recorded"

    event_id: str
    history_id: str
    outbox_id: str
    subject: AssessmentSubjectRef
    assessment_kind: AssessmentKind
    receipt_id: str
    input_digest: str
    request_fingerprint: str
    head_revision: int
    actor_id: str
    authority_digest: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        _instance(
            self.subject,
            AssessmentSubjectRef,
            "assessment_audit_subject_invalid",
        )
        _enum_instance(
            self.assessment_kind,
            AssessmentKind,
            "assessment_kind_invalid",
        )
        for field_name in (
            "event_id",
            "history_id",
            "outbox_id",
            "receipt_id",
            "actor_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"assessment_audit_{field_name}_required",
                ),
            )
        for field_name in (
            "input_digest",
            "request_fingerprint",
            "authority_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(
                    getattr(self, field_name),
                    f"assessment_audit_{field_name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "head_revision",
            _strict_positive_int(
                self.head_revision,
                "assessment_audit_head_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(
                self.occurred_at,
                "assessment_audit_occurred_at_must_be_aware",
            ),
        )


@dataclass(frozen=True, slots=True)
class AssessmentReceiptDetail:
    """Read aggregate that preserves finding-to-Q&A traceability."""

    receipt: AssessmentReceipt
    findings: tuple[QualityFinding, ...]
    proposed_questions: tuple[ProposedQuestion, ...]
    finding_qa_links: tuple[FindingQaLink, ...]

    def __post_init__(self) -> None:
        _instance(
            self.receipt,
            AssessmentReceipt,
            "assessment_detail_receipt_invalid",
        )
        object.__setattr__(
            self,
            "findings",
            _typed_tuple(
                self.findings,
                QualityFinding,
                "assessment_detail_findings_invalid",
            ),
        )
        object.__setattr__(
            self,
            "proposed_questions",
            _typed_tuple(
                self.proposed_questions,
                ProposedQuestion,
                "assessment_detail_questions_invalid",
            ),
        )
        object.__setattr__(
            self,
            "finding_qa_links",
            _typed_tuple(
                self.finding_qa_links,
                FindingQaLink,
                "assessment_detail_links_invalid",
            ),
        )
        finding_ids = {finding.id for finding in self.findings}
        qa_ids = {question.qa_id for question in self.proposed_questions}
        if len(finding_ids) != len(self.findings) or len(qa_ids) != len(
            self.proposed_questions
        ):
            raise QualityAssessmentContractError(
                "assessment_detail_duplicate_child"
            )
        for finding in self.findings:
            if (
                finding.receipt_id != self.receipt.id
                or finding.assessment_kind is not self.receipt.assessment_kind
            ):
                raise QualityAssessmentContractError(
                    "assessment_detail_finding_mismatch"
                )
        for question in self.proposed_questions:
            if question.receipt_id != self.receipt.id:
                raise QualityAssessmentContractError(
                    "assessment_detail_question_mismatch"
                )
        seen_links: set[tuple[str, str]] = set()
        for link in self.finding_qa_links:
            key = (link.finding_id, link.qa_id)
            if (
                link.receipt_id != self.receipt.id
                or link.finding_id not in finding_ids
                or link.qa_id not in qa_ids
                or key in seen_links
            ):
                raise QualityAssessmentContractError(
                    "assessment_detail_link_mismatch"
                )
            seen_links.add(key)


@dataclass(frozen=True, slots=True)
class AssessmentSubjectHead:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    assessment_kind: AssessmentKind
    receipt_id: str
    revision: int
    updated_at: datetime

    def __post_init__(self) -> None:
        _enum_instance(
            self.subject_type,
            AssessmentSubjectType,
            "assessment_head_subject_type_invalid",
        )
        _enum_instance(
            self.assessment_kind,
            AssessmentKind,
            "assessment_head_kind_invalid",
        )
        for field_name in ("board_id", "subject_id", "receipt_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"assessment_head_{field_name}_required",
                ),
            )
        object.__setattr__(
            self,
            "revision",
            _strict_positive_int(
                self.revision,
                "assessment_head_revision_invalid",
            ),
        )
        object.__setattr__(
            self,
            "updated_at",
            _aware_utc(
                self.updated_at,
                "assessment_head_updated_at_must_be_aware",
            ),
        )


@dataclass(frozen=True, slots=True)
class AssessmentCurrentness:
    current: bool
    stale_reasons: tuple[AssessmentStaleReason, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.current, bool):
            raise QualityAssessmentContractError(
                "assessment_currentness_flag_invalid"
            )
        reasons = tuple(self.stale_reasons)
        if any(not isinstance(reason, AssessmentStaleReason) for reason in reasons):
            raise QualityAssessmentContractError(
                "assessment_stale_reason_invalid"
            )
        if self.current and reasons:
            raise QualityAssessmentContractError(
                "current_assessment_cannot_have_stale_reasons"
            )
        if not self.current and not reasons:
            raise QualityAssessmentContractError(
                "stale_assessment_requires_reason"
            )
        if len(set(reasons)) != len(reasons):
            raise QualityAssessmentContractError(
                "assessment_stale_reasons_duplicate"
            )
        canonical = tuple(
            reason for reason in ASSESSMENT_STALE_REASON_ORDER if reason in reasons
        )
        if reasons != canonical:
            raise QualityAssessmentContractError(
                "assessment_stale_reasons_not_canonical"
            )
        object.__setattr__(self, "stale_reasons", reasons)


@dataclass(frozen=True, slots=True)
class AssessmentReceiptView:
    receipt: AssessmentReceipt
    is_head: bool
    freshness: AssessmentCurrentness
    state: AssessmentReceiptState

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, AssessmentReceipt):
            raise QualityAssessmentContractError(
                "assessment_receipt_view_receipt_invalid"
            )
        if not isinstance(self.is_head, bool):
            raise QualityAssessmentContractError(
                "assessment_receipt_view_head_invalid"
            )
        if not isinstance(self.freshness, AssessmentCurrentness):
            raise QualityAssessmentContractError(
                "assessment_receipt_view_freshness_invalid"
            )
        _enum_instance(
            self.state,
            AssessmentReceiptState,
            "assessment_receipt_state_invalid",
        )
        expected = (
            AssessmentReceiptState.SUPERSEDED
            if not self.is_head
            else (
                AssessmentReceiptState.CURRENT
                if self.freshness.current
                else AssessmentReceiptState.STALE
            )
        )
        if self.state is not expected:
            raise QualityAssessmentContractError(
                "assessment_receipt_state_mismatch"
            )


@dataclass(frozen=True, slots=True)
class AssessmentAnchorCatalog:
    fields: frozenset[str] = field(default_factory=frozenset)
    structured_child_ids: frozenset[str] = field(default_factory=frozenset)
    qa_ids: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "fields",
            frozenset(
                _required_text(item, "anchor_catalog_field_empty")
                for item in self.fields
            ),
        )
        object.__setattr__(
            self,
            "structured_child_ids",
            frozenset(
                _required_text(item, "anchor_catalog_child_empty")
                for item in self.structured_child_ids
            ),
        )
        object.__setattr__(
            self,
            "qa_ids",
            frozenset(
                _required_text(item, "anchor_catalog_qa_empty")
                for item in self.qa_ids
            ),
        )


@dataclass(frozen=True, slots=True)
class AssessmentAuthoritySnapshot:
    domain_write: bool
    quality_assess: bool
    authority_digest: str
    qa_ask: bool = False
    spec_validation_submit: bool = False
    reviewer_separation_satisfied: bool = True

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, bool)
            for value in (
                self.domain_write,
                self.quality_assess,
                self.qa_ask,
                self.spec_validation_submit,
                self.reviewer_separation_satisfied,
            )
        ):
            raise QualityAssessmentContractError(
                "assessment_authority_snapshot_invalid"
            )
        object.__setattr__(
            self,
            "authority_digest",
            _sha256(
                self.authority_digest,
                "assessment_authority_digest_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class AssessmentPreflight:
    subject: AssessmentSubjectRef
    status: str
    current_head_revision: int
    current_head_receipt_id: str | None
    channel: str
    expected_scale: AssessmentScale
    digests: AssessmentDigestSet
    versions: AssessmentVersionSet
    anchors: AssessmentAnchorCatalog
    allowed_category_codes: frozenset[str]
    authority: AssessmentAuthoritySnapshot
    origin: AssessmentOrigin
    subject_archived: bool = False

    def __post_init__(self) -> None:
        _instance(
            self.subject,
            AssessmentSubjectRef,
            "assessment_subject_invalid",
        )
        _instance(
            self.expected_scale,
            AssessmentScale,
            "assessment_scale_invalid",
        )
        _instance(
            self.digests,
            AssessmentDigestSet,
            "assessment_digests_invalid",
        )
        _instance(
            self.versions,
            AssessmentVersionSet,
            "assessment_versions_invalid",
        )
        _instance(
            self.anchors,
            AssessmentAnchorCatalog,
            "assessment_anchor_catalog_invalid",
        )
        if not isinstance(
            self.allowed_category_codes,
            frozenset | set | tuple | list,
        ):
            raise QualityAssessmentContractError(
                "assessment_category_catalog_invalid"
            )
        category_codes = frozenset(
            _required_text(
                code,
                "assessment_category_code_invalid",
            )
            for code in self.allowed_category_codes
        )
        object.__setattr__(
            self,
            "allowed_category_codes",
            category_codes,
        )
        _instance(
            self.authority,
            AssessmentAuthoritySnapshot,
            "assessment_authority_snapshot_invalid",
        )
        _enum_instance(
            self.origin,
            AssessmentOrigin,
            "assessment_origin_invalid",
        )
        object.__setattr__(
            self,
            "status",
            _required_text(self.status, "assessment_subject_status_required"),
        )
        object.__setattr__(
            self,
            "current_head_revision",
            _strict_non_negative_int(
                self.current_head_revision,
                "assessment_head_revision_invalid",
            ),
        )
        if self.current_head_receipt_id is not None:
            object.__setattr__(
                self,
                "current_head_receipt_id",
                _required_text(
                    self.current_head_receipt_id,
                    "assessment_head_receipt_id_invalid",
                ),
            )
        if (self.current_head_revision == 0) != (
            self.current_head_receipt_id is None
        ):
            raise QualityAssessmentContractError(
                "assessment_head_identity_mismatch"
            )
        object.__setattr__(
            self,
            "channel",
            _required_text(self.channel, "assessment_channel_required"),
        )
        if not isinstance(self.subject_archived, bool):
            raise QualityAssessmentContractError(
                "assessment_subject_archived_invalid"
            )


@dataclass(frozen=True, slots=True)
class AssessmentSubmission:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    assessment_kind: AssessmentKind
    idempotency_key: str
    expected_subject_version: int
    expected_head_revision: int
    score: float
    justification: str
    scale: AssessmentScale
    findings: tuple[QualityFindingDraft, ...] = ()
    proposed_questions: tuple[ProposedQuestionDraft, ...] = ()
    expected_subject_edition: int | None = None

    def __post_init__(self) -> None:
        _instance(self.scale, AssessmentScale, "assessment_scale_invalid")
        _enum_instance(
            self.subject_type,
            AssessmentSubjectType,
            "assessment_subject_type_invalid",
        )
        _enum_instance(
            self.assessment_kind,
            AssessmentKind,
            "assessment_kind_invalid",
        )
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "assessment_board_id_required"),
        )
        object.__setattr__(
            self,
            "subject_id",
            _required_text(self.subject_id, "assessment_subject_id_required"),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(
                self.idempotency_key,
                "assessment_idempotency_key_required",
            ),
        )
        if self.expected_subject_edition is not None:
            object.__setattr__(
                self,
                "expected_subject_edition",
                _strict_positive_int(
                    self.expected_subject_edition,
                    "assessment_subject_edition_invalid",
                ),
            )
        object.__setattr__(
            self,
            "expected_subject_version",
            _strict_positive_int(
                self.expected_subject_version,
                "assessment_subject_version_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expected_head_revision",
            _strict_non_negative_int(
                self.expected_head_revision,
                "assessment_head_revision_invalid",
            ),
        )
        object.__setattr__(self, "score", self.scale.validate_score(self.score))
        object.__setattr__(
            self,
            "justification",
            _required_text(
                self.justification,
                "assessment_justification_required",
            ),
        )
        object.__setattr__(
            self,
            "findings",
            _typed_tuple(
                self.findings,
                QualityFindingDraft,
                "assessment_findings_invalid",
            ),
        )
        object.__setattr__(
            self,
            "proposed_questions",
            _typed_tuple(
                self.proposed_questions,
                ProposedQuestionDraft,
                "assessment_questions_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class AssessmentWriteBundle:
    idempotency_key: str
    request_fingerprint: str
    expected_subject_version: int
    expected_head_revision: int
    expected_head_receipt_id: str | None
    expected_subject_status: str
    expected_subject_archived: bool
    expected_input_digest: str
    expected_authority_digest: str
    receipt: AssessmentReceipt
    findings: tuple[QualityFinding, ...]
    proposed_questions: tuple[ProposedQuestion, ...]
    finding_qa_links: tuple[FindingQaLink, ...]
    next_head: AssessmentSubjectHead
    audit_intent: AssessmentAuditIntent
    expected_subject_edition: int | None = None

    def __post_init__(self) -> None:
        _instance(
            self.receipt,
            AssessmentReceipt,
            "assessment_bundle_receipt_invalid",
        )
        _instance(
            self.next_head,
            AssessmentSubjectHead,
            "assessment_bundle_head_invalid",
        )
        _instance(
            self.audit_intent,
            AssessmentAuditIntent,
            "assessment_bundle_audit_intent_invalid",
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(
                self.idempotency_key,
                "assessment_idempotency_key_required",
            ),
        )
        object.__setattr__(
            self,
            "request_fingerprint",
            _sha256(
                self.request_fingerprint,
                "assessment_request_fingerprint_invalid",
            ),
        )
        expected_subject_version = _strict_positive_int(
            self.expected_subject_version,
            "assessment_subject_version_invalid",
        )
        expected_subject_edition = self.expected_subject_edition
        if expected_subject_edition is not None:
            expected_subject_edition = _strict_positive_int(
                expected_subject_edition,
                "assessment_subject_edition_invalid",
            )
            object.__setattr__(
                self,
                "expected_subject_edition",
                expected_subject_edition,
            )
        expected_head_revision = _strict_non_negative_int(
            self.expected_head_revision,
            "assessment_head_revision_invalid",
        )
        object.__setattr__(
            self,
            "expected_subject_version",
            expected_subject_version,
        )
        object.__setattr__(
            self,
            "expected_head_revision",
            expected_head_revision,
        )
        if self.expected_head_receipt_id is not None:
            object.__setattr__(
                self,
                "expected_head_receipt_id",
                _required_text(
                    self.expected_head_receipt_id,
                    "assessment_head_receipt_id_invalid",
                ),
            )
        if (expected_head_revision == 0) != (
            self.expected_head_receipt_id is None
        ):
            raise QualityAssessmentContractError(
                "assessment_head_identity_mismatch"
            )
        object.__setattr__(
            self,
            "expected_subject_status",
            _required_text(
                self.expected_subject_status,
                "assessment_subject_status_required",
            ),
        )
        if not isinstance(self.expected_subject_archived, bool):
            raise QualityAssessmentContractError(
                "assessment_subject_archived_invalid"
            )
        object.__setattr__(
            self,
            "expected_input_digest",
            _sha256(
                self.expected_input_digest,
                "assessment_input_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "expected_authority_digest",
            _sha256(
                self.expected_authority_digest,
                "assessment_authority_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "findings",
            _typed_tuple(
                self.findings,
                QualityFinding,
                "assessment_bundle_findings_invalid",
            ),
        )
        object.__setattr__(
            self,
            "proposed_questions",
            _typed_tuple(
                self.proposed_questions,
                ProposedQuestion,
                "assessment_bundle_questions_invalid",
            ),
        )
        object.__setattr__(
            self,
            "finding_qa_links",
            _typed_tuple(
                self.finding_qa_links,
                FindingQaLink,
                "assessment_bundle_links_invalid",
            ),
        )
        if len(self.proposed_questions) > MAX_PROPOSED_QUESTIONS_PER_ASSESSMENT_V1:
            raise QualityAssessmentContractError("question_budget_exceeded")
        subject = self.receipt.subject
        head = self.next_head
        if subject.subject_version != expected_subject_version:
            raise QualityAssessmentContractError(
                "assessment_bundle_subject_version_mismatch"
            )
        if (
            expected_subject_edition is not None
            and subject.subject_edition != expected_subject_edition
        ):
            raise QualityAssessmentContractError(
                "assessment_bundle_subject_edition_mismatch"
            )
        if self.receipt.digests.input_digest != self.expected_input_digest:
            raise QualityAssessmentContractError(
                "assessment_bundle_input_digest_mismatch"
            )
        if (
            self.receipt.idempotency_key != self.idempotency_key
            or self.receipt.request_digest != self.request_fingerprint
            or self.receipt.predecessor_receipt_id
            != self.expected_head_receipt_id
        ):
            raise QualityAssessmentContractError(
                "assessment_bundle_request_provenance_mismatch"
            )
        if self.expected_subject_archived:
            raise QualityAssessmentContractError(
                "assessment_bundle_archived_subject_forbidden"
            )
        if (
            head.revision != expected_head_revision + 1
            or head.board_id != subject.board_id
            or head.subject_type is not subject.subject_type
            or head.subject_id != subject.subject_id
            or head.assessment_kind is not self.receipt.assessment_kind
            or head.receipt_id != self.receipt.id
        ):
            raise QualityAssessmentContractError(
                "assessment_bundle_head_mismatch"
            )
        audit = self.audit_intent
        if (
            audit.subject != subject
            or audit.assessment_kind is not self.receipt.assessment_kind
            or audit.receipt_id != self.receipt.id
            or audit.input_digest != self.receipt.digests.input_digest
            or audit.request_fingerprint != self.request_fingerprint
            or audit.head_revision != head.revision
            or audit.actor_id != self.receipt.created_by
            or audit.authority_digest != self.expected_authority_digest
            or audit.authority_digest != self.receipt.authority_digest
            or audit.occurred_at != self.receipt.created_at
        ):
            raise QualityAssessmentContractError(
                "assessment_bundle_audit_intent_mismatch"
            )

        finding_ids: set[str] = set()
        finding_keys: set[str] = set()
        for finding in self.findings:
            if finding.id in finding_ids or finding.finding_key in finding_keys:
                raise QualityAssessmentContractError(
                    "assessment_bundle_duplicate_finding"
                )
            finding_ids.add(finding.id)
            finding_keys.add(finding.finding_key)
            anchor = finding.anchor
            if (
                finding.receipt_id != self.receipt.id
                or finding.assessment_kind is not self.receipt.assessment_kind
                or anchor.board_id != subject.board_id
                or anchor.subject_type is not subject.subject_type
                or anchor.subject_id != subject.subject_id
                or anchor.subject_version != subject.subject_version
                or anchor.input_digest != self.receipt.digests.input_digest
            ):
                raise QualityAssessmentContractError(
                    "assessment_bundle_finding_mismatch"
                )

        qa_ids: set[str] = set()
        client_keys: set[str] = set()
        for question in self.proposed_questions:
            if question.qa_id in qa_ids or question.client_key in client_keys:
                raise QualityAssessmentContractError(
                    "assessment_bundle_duplicate_question"
                )
            if question.receipt_id != self.receipt.id:
                raise QualityAssessmentContractError(
                    "assessment_bundle_question_mismatch"
                )
            qa_ids.add(question.qa_id)
            client_keys.add(question.client_key)

        link_keys: set[tuple[str, str]] = set()
        for link in self.finding_qa_links:
            key = (link.finding_id, link.qa_id)
            if (
                link.receipt_id != self.receipt.id
                or link.finding_id not in finding_ids
                or link.qa_id not in qa_ids
                or key in link_keys
            ):
                raise QualityAssessmentContractError(
                    "assessment_bundle_finding_qa_link_mismatch"
                )
            link_keys.add(key)


@dataclass(frozen=True, slots=True)
class AssessmentCommitResult:
    board_id: str
    subject_type: AssessmentSubjectType
    subject_id: str
    subject_version: int
    assessment_kind: AssessmentKind
    request_fingerprint: str
    receipt_id: str
    head_revision: int
    event_id: str
    history_id: str
    outbox_id: str
    qa_id_map: tuple[tuple[str, str], ...]
    replayed: bool = False
    subject_edition: int | None = None

    def __post_init__(self) -> None:
        _enum_instance(
            self.subject_type,
            AssessmentSubjectType,
            "assessment_subject_type_invalid",
        )
        _enum_instance(
            self.assessment_kind,
            AssessmentKind,
            "assessment_kind_invalid",
        )
        object.__setattr__(
            self,
            "board_id",
            _required_text(self.board_id, "assessment_board_id_required"),
        )
        object.__setattr__(
            self,
            "subject_id",
            _required_text(self.subject_id, "assessment_subject_id_required"),
        )
        object.__setattr__(
            self,
            "subject_version",
            _strict_positive_int(
                self.subject_version,
                "assessment_subject_version_invalid",
            ),
        )
        if self.subject_edition is not None:
            object.__setattr__(
                self,
                "subject_edition",
                _strict_positive_int(
                    self.subject_edition,
                    "assessment_subject_edition_invalid",
                ),
            )
        object.__setattr__(
            self,
            "request_fingerprint",
            _sha256(
                self.request_fingerprint,
                "assessment_request_fingerprint_invalid",
            ),
        )
        object.__setattr__(
            self,
            "receipt_id",
            _required_text(self.receipt_id, "assessment_receipt_id_required"),
        )
        object.__setattr__(
            self,
            "head_revision",
            _strict_positive_int(
                self.head_revision,
                "assessment_head_revision_invalid",
            ),
        )
        for field_name in ("event_id", "history_id", "outbox_id"):
            object.__setattr__(
                self,
                field_name,
                _required_text(
                    getattr(self, field_name),
                    f"assessment_{field_name}_required",
                ),
            )
        if not isinstance(self.qa_id_map, tuple | list):
            raise QualityAssessmentContractError(
                "assessment_qa_id_map_invalid"
            )
        mapping_items: list[tuple[str, str]] = []
        for item in self.qa_id_map:
            if not isinstance(item, tuple | list) or len(item) != 2:
                raise QualityAssessmentContractError(
                    "assessment_qa_id_map_invalid"
                )
            mapping_items.append(
                (
                    _required_text(item[0], "question_client_key_required"),
                    _required_text(item[1], "question_qa_id_required"),
                )
            )
        mapping = tuple(mapping_items)
        if len({key for key, _ in mapping}) != len(mapping):
            raise QualityAssessmentContractError(
                "assessment_qa_id_map_duplicate_key"
            )
        object.__setattr__(self, "qa_id_map", mapping)
        if not isinstance(self.replayed, bool):
            raise QualityAssessmentContractError(
                "assessment_replayed_flag_invalid"
            )

    @property
    def qa_ids(self) -> dict[str, str]:
        return dict(self.qa_id_map)


_PageItemT = TypeVar("_PageItemT")


@dataclass(frozen=True, slots=True)
class QualityPage(Generic[_PageItemT]):
    items: tuple[_PageItemT, ...]
    total_filtered: int
    total_overall: int
    offset: int
    limit: int

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple | list):
            raise QualityAssessmentContractError("quality_page_items_invalid")
        object.__setattr__(self, "items", tuple(self.items))
        for field_name in ("total_filtered", "total_overall", "offset"):
            object.__setattr__(
                self,
                field_name,
                _strict_non_negative_int(
                    getattr(self, field_name),
                    f"quality_page_{field_name}_invalid",
                ),
            )
        object.__setattr__(
            self,
            "limit",
            _strict_positive_int(self.limit, "quality_page_limit_invalid"),
        )
        if self.limit > QUALITY_PAGE_LIMIT_MAX:
            raise QualityAssessmentContractError("quality_page_limit_invalid")
        if self.total_filtered > self.total_overall:
            raise QualityAssessmentContractError(
                "quality_page_filtered_exceeds_overall"
            )
        if len(self.items) > self.limit:
            raise QualityAssessmentContractError(
                "quality_page_item_count_exceeds_limit"
            )
        if self.items and self.offset + len(self.items) > self.total_filtered:
            raise QualityAssessmentContractError(
                "quality_page_items_exceed_filtered_total"
            )
        expected_item_count = min(
            self.limit,
            max(self.total_filtered - self.offset, 0),
        )
        if len(self.items) != expected_item_count:
            raise QualityAssessmentContractError(
                "quality_page_underfilled"
            )


@dataclass(frozen=True, slots=True)
class QualityPageCursor:
    """Stable keyset boundary plus its absolute result position."""

    created_at: datetime
    item_id: str
    offset: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "created_at",
            _aware_utc(
                self.created_at,
                "quality_cursor_created_at_must_be_aware",
            ),
        )
        object.__setattr__(
            self,
            "item_id",
            _required_text(self.item_id, "quality_cursor_item_id_required"),
        )
        object.__setattr__(
            self,
            "offset",
            _strict_non_negative_int(
                self.offset,
                "quality_cursor_offset_invalid",
            ),
        )


def evaluate_assessment_input_currentness(
    assessed_subject: AssessmentSubjectRef,
    assessed_digests: AssessmentDigestSet,
    *,
    current_subject: AssessmentSubjectRef,
    current_digests: AssessmentDigestSet,
) -> AssessmentCurrentness:
    """Evaluate currentness from the immutable input identity alone.

    Relational projections and rebuild readers intentionally use this smaller
    contract: they need to decide whether a head is current without loading
    unrelated receipt audit fields. The complete receipt helper below
    delegates here so both paths retain the same frozen six-reason order.
    """

    if (
        assessed_subject.board_id != current_subject.board_id
        or assessed_subject.subject_type is not current_subject.subject_type
        or assessed_subject.subject_id != current_subject.subject_id
    ):
        raise QualityAssessmentContractError("assessment_subject_mismatch")

    # Human currentness is lifecycle-edition scoped. Once a live edition is
    # available, technical version and digest drift remain audit/CAS evidence
    # and cannot turn an accepted result into a human-facing stale result.
    if current_subject.subject_edition is not None:
        if assessed_subject.subject_edition == current_subject.subject_edition:
            return AssessmentCurrentness(current=True, stale_reasons=())
        return AssessmentCurrentness(
            current=False,
            stale_reasons=(AssessmentStaleReason.SUBJECT_EDITION_CHANGED,),
        )

    # Compatibility for callers not yet upgraded to lifecycle editions.
    reasons: list[AssessmentStaleReason] = []
    if assessed_subject.subject_version != current_subject.subject_version:
        reasons.append(AssessmentStaleReason.SUBJECT_VERSION_CHANGED)
    comparisons = (
        (
            assessed_digests.content_digest,
            current_digests.content_digest,
            AssessmentStaleReason.CONTENT_CHANGED,
        ),
        (
            assessed_digests.clarification_digest,
            current_digests.clarification_digest,
            AssessmentStaleReason.CLARIFICATION_CHANGED,
        ),
        (
            assessed_digests.ruleset_digest,
            current_digests.ruleset_digest,
            AssessmentStaleReason.RULESET_CHANGED,
        ),
        (
            assessed_digests.taxonomy_digest,
            current_digests.taxonomy_digest,
            AssessmentStaleReason.TAXONOMY_CHANGED,
        ),
        (
            assessed_digests.policy_digest,
            current_digests.policy_digest,
            AssessmentStaleReason.POLICY_CHANGED,
        ),
    )
    reasons.extend(reason for previous, current, reason in comparisons if previous != current)
    ordered = tuple(reason for reason in ASSESSMENT_STALE_REASON_ORDER if reason in reasons)
    return AssessmentCurrentness(current=not ordered, stale_reasons=ordered)


def evaluate_assessment_currentness(
    receipt: AssessmentReceipt,
    *,
    current_subject: AssessmentSubjectRef,
    current_digests: AssessmentDigestSet,
) -> AssessmentCurrentness:
    """Evaluate version-bound currentness using the frozen six-reason set."""

    return evaluate_assessment_input_currentness(
        receipt.subject,
        receipt.digests,
        current_subject=current_subject,
        current_digests=current_digests,
    )


def project_assessment_receipt_view(
    receipt: AssessmentReceipt,
    *,
    head_receipt_id: str | None,
    current_subject: AssessmentSubjectRef,
    current_digests: AssessmentDigestSet,
) -> AssessmentReceiptView:
    """Project freshness separately from append-only head lifecycle."""

    freshness = evaluate_assessment_currentness(
        receipt,
        current_subject=current_subject,
        current_digests=current_digests,
    )
    is_head = receipt.id == head_receipt_id
    state = (
        AssessmentReceiptState.SUPERSEDED
        if not is_head
        else (
            AssessmentReceiptState.CURRENT
            if freshness.current
            else AssessmentReceiptState.STALE
        )
    )
    return AssessmentReceiptView(
        receipt=receipt,
        is_head=is_head,
        freshness=freshness,
        state=state,
    )


__all__ = [
    "ASSESSMENT_STALE_REASON_ORDER",
    "MAX_PROPOSED_QUESTIONS_PER_ASSESSMENT_V1",
    "QUALITY_PAGE_LIMIT_MAX",
    "QUALITY_ASSESSMENT_CONTRACT_VERSION",
    "QUALITY_ASSESSMENT_CHANNELS_V1",
    "AssessmentAnchorCatalog",
    "AssessmentAuditIntent",
    "AssessmentAuthoritySnapshot",
    "AssessmentCommitResult",
    "AssessmentCurrentness",
    "AssessmentDigestSet",
    "AssessmentKind",
    "AssessmentOrigin",
    "AssessmentOutcome",
    "AssessmentPreflight",
    "AssessmentPreflightRequest",
    "AssessmentReceipt",
    "AssessmentReceiptDetail",
    "AssessmentReceiptState",
    "AssessmentReceiptView",
    "AssessmentScale",
    "AssessmentScaleKind",
    "AssessmentSource",
    "AssessmentStaleReason",
    "AssessmentSubjectHead",
    "AssessmentSubjectIdentity",
    "AssessmentSubjectRef",
    "AssessmentSubjectType",
    "AssessmentSubmission",
    "AssessmentVersionSet",
    "AssessmentWriteBundle",
    "EvidenceRef",
    "FindingAnchor",
    "FindingAnchorType",
    "FindingLifecycle",
    "FindingQaLink",
    "FindingSeverity",
    "ProposedQuestion",
    "ProposedQuestionDraft",
    "QualityAssessmentContractError",
    "QualityFinding",
    "QualityFindingDraft",
    "QualityPage",
    "QualityPageCursor",
    "ScoreDirection",
    "UnboundFindingAnchor",
    "UnboundQualityFindingDraft",
    "evaluate_assessment_currentness",
    "project_assessment_receipt_view",
]
