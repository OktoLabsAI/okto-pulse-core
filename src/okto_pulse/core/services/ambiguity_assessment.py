"""Shared SK-A ambiguity-assessment and gate contracts.

The immutable receipt still records the technical input digests, but human gate
currentness is scoped to the subject lifecycle edition.  Editing within Draft
does not manufacture a human-facing stale state; returning to Draft opens the
next edition and makes every earlier receipt historical.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any

from okto_pulse.core.domain.quality_assessment import (
    AssessmentAnchorCatalog,
    AssessmentAuthoritySnapshot,
    AssessmentCurrentness,
    AssessmentDigestSet,
    AssessmentKind,
    AssessmentReceipt,
    AssessmentScale,
    AssessmentScaleKind,
    AssessmentSubjectRef,
    AssessmentSubjectType,
    AssessmentVersionSet,
    ScoreDirection,
    evaluate_assessment_currentness,
)
from okto_pulse.core.domain.quality_canonicalization import (
    SEMANTIC_FIELD_MANIFEST_V1,
    authority_digest_v1,
    clarification_digest_v1,
    policy_digest_v1,
    ruleset_digest_v1,
    semantic_content_digest_v1,
)
from okto_pulse.core.domain.quality_taxonomy import (
    AMBIGUITY_TAXONOMY_CATEGORY_ID_SET,
    AMBIGUITY_TAXONOMY_DIGEST,
    AMBIGUITY_TAXONOMY_VERSION,
)
from okto_pulse.core.ports.quality_assessment import (
    QualityAssessmentPersistencePort,
)

AMBIGUITY_RULESET_VERSION = "ambiguity-assessment-ruleset/v1"
AMBIGUITY_ANALYZER_VERSION = "ambiguity-assessment-analyzer/v1"
AMBIGUITY_POLICY_VERSION = "ambiguity-assessment-policy/v1"
AMBIGUITY_GATE_CONTRACT_VERSION = "ambiguity-gate/v1"
AMBIGUITY_AUTHORITY_VERSION = "quality-authority/v1"

AMBIGUITY_SCALE = AssessmentScale(
    kind=AssessmentScaleKind.AMBIGUITY_SCORE,
    minimum=0,
    maximum=5,
    direction=ScoreDirection.LOWER_BETTER,
)

AMBIGUITY_RULESET_MANIFEST_V1: Mapping[str, object] = MappingProxyType(
    {
        "assessment_kind": AssessmentKind.AMBIGUITY.value,
        "input": "human_or_agent",
        "score_scale": {
            "kind": AssessmentScaleKind.AMBIGUITY_SCORE.value,
            "minimum": 0,
            "maximum": 5,
            "direction": ScoreDirection.LOWER_BETTER.value,
        },
        "question_budget": 5,
        "finding_blocking_eligible": False,
    }
)
AMBIGUITY_RULESET_DIGEST = ruleset_digest_v1(
    AMBIGUITY_RULESET_VERSION,
    AMBIGUITY_RULESET_MANIFEST_V1,
)


def ambiguity_authority_snapshot_v1(
    *,
    actor_id: str,
    board_id: str,
    subject_type: AssessmentSubjectType,
    subject_id: str,
    subject_version: int,
    channel: str,
    domain_write: bool,
    quality_assess: bool,
    qa_ask: bool,
) -> AssessmentAuthoritySnapshot:
    """Build the single authority identity used by preflight and CAS."""

    manifest = {
        "authority_kind": "human_or_agent_ambiguity_assessment",
        "actor_id": actor_id.strip(),
        "board_id": board_id.strip(),
        "subject_type": subject_type.value,
        "subject_id": subject_id.strip(),
        "subject_version": subject_version,
        "channel": channel.strip(),
        "domain_write": domain_write,
        "quality_assess": quality_assess,
        "qa_ask": qa_ask,
        "spec_validation_submit": False,
        "reviewer_separation_satisfied": True,
    }
    return AssessmentAuthoritySnapshot(
        domain_write=domain_write,
        quality_assess=quality_assess,
        qa_ask=qa_ask,
        spec_validation_submit=False,
        reviewer_separation_satisfied=True,
        authority_digest=authority_digest_v1(
            AMBIGUITY_AUTHORITY_VERSION,
            manifest,
        ),
    )


class AmbiguityGateReason(str, Enum):
    DISABLED = "ambiguity_gate_disabled"
    SKIPPED = "ambiguity_gate_skipped"
    ASSESSMENT_MISSING = "ambiguity_assessment_missing"
    ASSESSMENT_STALE = "ambiguity_assessment_stale"
    SCORE_EXCEEDS_THRESHOLD = "ambiguity_score_exceeds_threshold"
    READY = "ambiguity_gate_ready"


class AmbiguityGateError(ValueError):
    """Stable, actionable failure returned by ambiguity-gated transitions."""

    def __init__(
        self,
        code: str | AmbiguityGateReason,
        message: str | None = None,
        *,
        details: Mapping[str, object] | None = None,
    ) -> None:
        self.code = str(getattr(code, "value", code))
        self.details = dict(details or {})
        super().__init__(message or self.code)


@dataclass(frozen=True, slots=True)
class AmbiguityGateConfiguration:
    required: bool
    maximum_score: int

    def __post_init__(self) -> None:
        if not isinstance(self.required, bool):
            raise ValueError("ambiguity_gate_required_invalid")
        if (
            not isinstance(self.maximum_score, int)
            or isinstance(self.maximum_score, bool)
            or not 1 <= self.maximum_score <= 5
        ):
            raise ValueError("ambiguity_gate_threshold_invalid")


@dataclass(frozen=True, slots=True)
class AmbiguityGateDecision:
    allowed: bool
    reason_code: AmbiguityGateReason
    threshold: int
    score: float | None = None
    receipt_id: str | None = None
    currentness: AssessmentCurrentness | None = None


def resolve_ambiguity_gate_configuration(
    subject_type: AssessmentSubjectType | str,
    board_settings: Mapping[str, object] | None,
) -> AmbiguityGateConfiguration:
    """Resolve legacy-safe settings for one supported gated subject."""

    resolved_type = AssessmentSubjectType(subject_type)
    if resolved_type not in {
        AssessmentSubjectType.IDEATION,
        AssessmentSubjectType.REFINEMENT,
    }:
        raise ValueError("ambiguity_gate_subject_not_supported")
    prefix = resolved_type.value
    settings = board_settings or {}
    raw_required = settings.get(f"require_{prefix}_ambiguity_gate", False)
    if isinstance(raw_required, bool):
        required = raw_required
    elif isinstance(raw_required, str):
        normalized_required = raw_required.strip().lower()
        if normalized_required in {"0", "false", "no", "off", ""}:
            required = False
        else:
            # Unknown legacy values remain fail-closed.
            required = True
    else:
        required = bool(raw_required)
    raw_threshold = settings.get(f"max_{prefix}_ambiguity", 3)
    try:
        threshold = int(raw_threshold)
    except (TypeError, ValueError):
        threshold = 3
    # Defensive read normalization for legacy rows.  Writes are rejected by
    # BoardSettings instead of silently clamped.
    threshold = max(1, min(5, threshold))
    return AmbiguityGateConfiguration(
        required=required,
        maximum_score=threshold,
    )


def ambiguity_policy_manifest_v1(
    *,
    subject_type: AssessmentSubjectType | str,
    configuration: AmbiguityGateConfiguration,
) -> Mapping[str, object]:
    resolved_type = AssessmentSubjectType(subject_type)
    if not isinstance(configuration, AmbiguityGateConfiguration):
        raise ValueError("ambiguity_gate_configuration_invalid")
    # Gate enablement and the tolerated score are evaluated against the
    # board's current settings. They are deliberately not part of receipt
    # identity: changing only either setting must not stale an otherwise
    # current assessment.
    return {
        "contract_version": AMBIGUITY_GATE_CONTRACT_VERSION,
        "subject_type": resolved_type.value,
        "question_budget": 5,
        "blocking_predicates": (
            AmbiguityGateReason.ASSESSMENT_MISSING.value,
            AmbiguityGateReason.ASSESSMENT_STALE.value,
            AmbiguityGateReason.SCORE_EXCEEDS_THRESHOLD.value,
        ),
    }


def _record_mapping(value: object, fields: Sequence[str]) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {field: value.get(field) for field in fields}
    return {field: getattr(value, field, None) for field in fields}


def _qa_mapping(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "revision",
        "question",
        "question_type",
        "choices",
        "allow_free_text",
        "answer",
        "selected",
        "answered_at",
        "lifecycle",
        "tombstoned",
    )
    payload = _record_mapping(value, fields)
    payload["revision"] = payload.get("revision") or 1
    payload["question_type"] = payload.get("question_type") or "text"
    payload["choices"] = payload.get("choices") or []
    if payload.get("allow_free_text") is None:
        payload["allow_free_text"] = True
    payload["selected"] = payload.get("selected") or []
    payload["lifecycle"] = payload.get("lifecycle") or (
        "tombstoned" if payload.get("tombstoned") else "active"
    )
    payload["tombstoned"] = bool(payload.get("tombstoned", False))
    return payload


def ambiguity_subject_payload(
    subject_type: AssessmentSubjectType | str,
    subject: object,
) -> dict[str, Any]:
    resolved_type = AssessmentSubjectType(subject_type)
    return _record_mapping(
        subject,
        SEMANTIC_FIELD_MANIFEST_V1[resolved_type.value],
    )


def ambiguity_qa_payload(qa_items: Sequence[object] | None) -> list[dict[str, Any]]:
    return [_qa_mapping(item) for item in (qa_items or ())]


def ambiguity_anchor_catalog(
    *,
    subject_type: AssessmentSubjectType | str,
    subject: object,
    qa_items: Sequence[object] | None,
) -> AssessmentAnchorCatalog:
    resolved_type = AssessmentSubjectType(subject_type)
    payload = ambiguity_subject_payload(resolved_type, subject)
    structured_child_ids: set[str] = set()
    for field_name in SEMANTIC_FIELD_MANIFEST_V1[resolved_type.value]:
        value = payload.get(field_name)
        if not isinstance(value, Sequence) or isinstance(
            value,
            str | bytes | bytearray,
        ):
            continue
        for item in value:
            raw_id = (
                item.get("id")
                if isinstance(item, Mapping)
                else getattr(item, "id", None)
            )
            if isinstance(raw_id, str) and raw_id.strip():
                structured_child_ids.add(raw_id.strip())
    qa_ids = {
        str(item["id"]).strip()
        for item in ambiguity_qa_payload(qa_items)
        if isinstance(item.get("id"), str) and str(item["id"]).strip()
    }
    return AssessmentAnchorCatalog(
        fields=frozenset(SEMANTIC_FIELD_MANIFEST_V1[resolved_type.value]),
        structured_child_ids=frozenset(structured_child_ids),
        qa_ids=frozenset(qa_ids),
    )


def ambiguity_digest_set(
    *,
    subject_type: AssessmentSubjectType | str,
    subject: object,
    qa_items: Sequence[object] | None,
    configuration: AmbiguityGateConfiguration,
) -> AssessmentDigestSet:
    resolved_type = AssessmentSubjectType(subject_type)
    return AssessmentDigestSet(
        content_digest=semantic_content_digest_v1(
            resolved_type,
            ambiguity_subject_payload(resolved_type, subject),
        ),
        clarification_digest=clarification_digest_v1(
            ambiguity_qa_payload(qa_items),
        ),
        ruleset_digest=AMBIGUITY_RULESET_DIGEST,
        taxonomy_digest=AMBIGUITY_TAXONOMY_DIGEST,
        policy_digest=policy_digest_v1(
            AMBIGUITY_POLICY_VERSION,
            ambiguity_policy_manifest_v1(
                subject_type=resolved_type,
                configuration=configuration,
            ),
        ),
    )


def ambiguity_versions_v1() -> AssessmentVersionSet:
    return AssessmentVersionSet(
        ruleset_version=AMBIGUITY_RULESET_VERSION,
        taxonomy_version=AMBIGUITY_TAXONOMY_VERSION,
        analyzer_version=AMBIGUITY_ANALYZER_VERSION,
        policy_version=AMBIGUITY_POLICY_VERSION,
    )


class AmbiguityGateService:
    """Evaluate the canonical ambiguity predicate without mutating state."""

    def __init__(self, persistence: QualityAssessmentPersistencePort) -> None:
        self._persistence = persistence

    async def evaluate(
        self,
        *,
        board_id: str,
        subject_type: AssessmentSubjectType | str,
        subject: object,
        board_settings: Mapping[str, object] | None,
        qa_items: Sequence[object] | None = None,
        skipped: bool = False,
    ) -> AmbiguityGateDecision:
        resolved_type = AssessmentSubjectType(subject_type)
        configuration = resolve_ambiguity_gate_configuration(
            resolved_type,
            board_settings,
        )
        if not configuration.required:
            return AmbiguityGateDecision(
                allowed=True,
                reason_code=AmbiguityGateReason.DISABLED,
                threshold=configuration.maximum_score,
            )
        if skipped:
            return AmbiguityGateDecision(
                allowed=True,
                reason_code=AmbiguityGateReason.SKIPPED,
                threshold=configuration.maximum_score,
            )

        subject_id = getattr(subject, "id", None)
        subject_version = getattr(subject, "version", None)
        subject_edition = getattr(subject, "edition", None)
        if (
            not isinstance(subject_id, str)
            or not subject_id.strip()
            or not isinstance(subject_version, int)
            or isinstance(subject_version, bool)
            or subject_version < 1
            or not isinstance(subject_edition, int)
            or isinstance(subject_edition, bool)
            or subject_edition < 1
        ):
            raise AmbiguityGateError(
                "ambiguity_gate_subject_invalid",
            )
        try:
            resolved = await self._persistence.get_current(
                board_id=board_id,
                subject_type=resolved_type,
                subject_id=subject_id,
                assessment_kind=AssessmentKind.AMBIGUITY,
                subject_edition=subject_edition,
            )
        except TypeError:
            # Temporary compatibility for third-party adapters while the
            # edition-aware port is rolled out.  The defensive check below
            # keeps a legacy head history-only.
            resolved = await self._persistence.get_current(
                board_id=board_id,
                subject_type=resolved_type,
                subject_id=subject_id,
                assessment_kind=AssessmentKind.AMBIGUITY,
            )
        if resolved is None:
            raise AmbiguityGateError(
                AmbiguityGateReason.ASSESSMENT_MISSING,
                "Ambiguity gate failed: no current ambiguity assessment exists. "
                "Run the ambiguity assessment and resolve its pinpointed Q&A. "
                "If a skip is required, request a human decision.",
                details={"threshold": configuration.maximum_score},
            )
        receipt, _head = resolved
        if receipt.subject.subject_edition != subject_edition:
            raise AmbiguityGateError(
                AmbiguityGateReason.ASSESSMENT_MISSING,
                "Ambiguity gate failed: this edition has not been assessed. "
                "Run the ambiguity assessment for the current validation cycle.",
                details={"threshold": configuration.maximum_score},
            )
        current_digests = ambiguity_digest_set(
            subject_type=resolved_type,
            subject=subject,
            qa_items=qa_items,
            configuration=configuration,
        )
        currentness = evaluate_assessment_currentness(
            receipt,
            current_subject=AssessmentSubjectRef(
                board_id=board_id,
                subject_type=resolved_type,
                subject_id=subject_id,
                subject_version=subject_version,
                subject_edition=subject_edition,
            ),
            current_digests=current_digests,
        )
        if not currentness.current:
            raise AmbiguityGateError(
                AmbiguityGateReason.ASSESSMENT_MISSING,
                "Ambiguity gate failed: this edition has not been assessed. "
                "Run the ambiguity assessment for the current validation cycle.",
                details={
                    "threshold": configuration.maximum_score,
                },
            )
        if receipt.score > configuration.maximum_score:
            raise AmbiguityGateError(
                AmbiguityGateReason.SCORE_EXCEEDS_THRESHOLD,
                f"Ambiguity gate failed: score {receipt.score:g} exceeds "
                f"configured max {configuration.maximum_score}. Resolve the "
                "pinpointed findings/Q&A or request a human skip.",
                details={
                    "receipt_id": receipt.id,
                    "score": receipt.score,
                    "threshold": configuration.maximum_score,
                },
            )
        return AmbiguityGateDecision(
            allowed=True,
            reason_code=AmbiguityGateReason.READY,
            threshold=configuration.maximum_score,
            score=receipt.score,
            receipt_id=receipt.id,
            currentness=currentness,
        )


def require_ambiguity_receipt(receipt: AssessmentReceipt) -> AssessmentReceipt:
    """Small fail-closed helper shared by inbound projectors."""

    if not isinstance(receipt, AssessmentReceipt):
        raise ValueError("ambiguity_receipt_invalid")
    if receipt.assessment_kind is not AssessmentKind.AMBIGUITY:
        raise ValueError("ambiguity_receipt_kind_invalid")
    return receipt


__all__ = [
    "AMBIGUITY_ANALYZER_VERSION",
    "AMBIGUITY_AUTHORITY_VERSION",
    "AMBIGUITY_GATE_CONTRACT_VERSION",
    "AMBIGUITY_POLICY_VERSION",
    "AMBIGUITY_RULESET_DIGEST",
    "AMBIGUITY_RULESET_MANIFEST_V1",
    "AMBIGUITY_RULESET_VERSION",
    "AMBIGUITY_SCALE",
    "AMBIGUITY_TAXONOMY_CATEGORY_ID_SET",
    "AmbiguityGateConfiguration",
    "AmbiguityGateDecision",
    "AmbiguityGateError",
    "AmbiguityGateReason",
    "AmbiguityGateService",
    "ambiguity_authority_snapshot_v1",
    "ambiguity_anchor_catalog",
    "ambiguity_digest_set",
    "ambiguity_policy_manifest_v1",
    "ambiguity_qa_payload",
    "ambiguity_subject_payload",
    "ambiguity_versions_v1",
    "require_ambiguity_receipt",
    "resolve_ambiguity_gate_configuration",
]
